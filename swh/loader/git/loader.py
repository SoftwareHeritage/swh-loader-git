# Copyright (C) 2016-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import binascii
import collections
from collections import defaultdict
from dataclasses import dataclass
import datetime
import json
import logging
import os
import pickle
import tempfile
import time
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
)

import urllib3.util

from swh.core.statsd import Statsd
from swh.loader.exception import NotFound
from swh.loader.git.utils import raise_not_found_repository
from swh.model import hashutil
from swh.model.model import (
    BaseContent,
    Directory,
    RawExtrinsicMetadata,
    Release,
    Revision,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
)
from swh.model.swhids import ExtendedObjectType
from swh.storage.algos.snapshot import snapshot_get_latest
from swh.storage.interface import StorageInterface

from . import converters, utils
from .base import BaseGitLoader
from .utils import LOGGING_INTERVAL

logger = logging.getLogger(__name__)
heads_logger = logger.getChild("refs")
remote_logger = logger.getChild("remote")
fetch_pack_logger = logger.getChild("fetch_pack")


def split_lines_and_remainder(buf: bytes) -> Tuple[List[bytes], bytes]:
    """Get newline-terminated (``b"\\r"`` or ``b"\\n"``) lines from `buf`,
    and the beginning of the last line if it isn't terminated."""

    lines = buf.splitlines(keepends=True)
    if not lines:
        return [], b""

    if buf.endswith((b"\r", b"\n")):
        # The buffer ended with a newline, everything can be sent as lines
        return lines, b""
    else:
        # The buffer didn't end with a newline, we need to keep the
        # last bit as the beginning of the next line
        return lines[:-1], lines[-1]


class RepoRepresentation:
    """Repository representation for a Software Heritage origin."""

    def __init__(
        self,
        storage,
        base_snapshots: Optional[List[Snapshot]] = None,
        incremental: bool = True,
        statsd: Optional[Statsd] = None,
    ):
        self.storage = storage
        self.incremental = incremental
        self.statsd = statsd

        if base_snapshots and incremental:
            self.base_snapshots: List[Snapshot] = base_snapshots
        else:
            self.base_snapshots = []

        # Cache existing heads
        self.local_heads: Set[bytes] = set()
        heads_logger.debug("Heads known in the archive:")
        for base_snapshot in self.base_snapshots:
            for branch_name, branch in base_snapshot.branches.items():
                if not branch or branch.target_type == SnapshotTargetType.ALIAS:
                    continue
                heads_logger.debug("    %r: %s", branch_name, branch.target.hex())
                self.local_heads.add(binascii.hexlify(branch.target))

    def determine_wants(
        self, refs: Mapping[bytes, bytes], depth: Optional[int] = None
    ) -> List[bytes]:
        """Get the list of bytehex sha1s that the git loader should fetch.

        This compares the remote refs sent by the server with the base snapshot
        provided by the loader.

        """
        if not refs:
            return []

        if heads_logger.isEnabledFor(logging.DEBUG):
            heads_logger.debug("Heads returned by the git remote:")
            for name, value in refs.items():
                heads_logger.debug("    %r: %s", name, value.decode())

        # Get the remote heads that we want to fetch
        remote_heads: Set[bytes] = set()
        for ref_name, ref_target in refs.items():
            if utils.ignore_branch_name(ref_name):
                continue
            remote_heads.add(ref_target)

        logger.debug("local_heads_count=%s", len(self.local_heads))
        logger.debug("remote_heads_count=%s", len(remote_heads))
        wanted_refs = list(remote_heads - self.local_heads)

        logger.debug("wanted_refs_count=%s", len(wanted_refs))
        if self.statsd is not None:
            self.statsd.histogram(
                "git_ignored_refs_percent",
                len(remote_heads - set(refs.values())) / len(refs),
                tags={},
            )
            self.statsd.histogram(
                "git_known_refs_percent",
                len(self.local_heads & remote_heads) / len(remote_heads),
                tags={},
            )
        return wanted_refs


@dataclass
class FetchPackReturn:
    remote_refs: Dict[bytes, bytes]
    symbolic_refs: Dict[bytes, bytes]
    pack_path: str
    pack_size: int


class GitLoader(BaseGitLoader):
    """A bulk loader for a git repository

    Emits the following statsd stats:

    * increments ``swh_loader_git``
    * histogram ``swh_loader_git_ignored_refs_percent`` is the ratio of refs ignored
      over all refs of the remote repository
    * histogram ``swh_loader_git_known_refs_percent`` is the ratio of (non-ignored)
      remote heads that are already local over all non-ignored remote heads

    All three are tagged with ``{{"incremental": "<incremental_mode>"}}`` where
    ``incremental_mode`` is one of:

    * ``from_same_origin`` when the origin was already loaded
    * ``from_parent_origin`` when the origin was not already loaded,
      but it was detected as a forge-fork of an origin that was already loaded
    * ``no_previous_snapshot`` when the origin was not already loaded,
      and it was detected as a forge-fork of origins that were not already loaded either
    * ``no_parent_origin`` when the origin was no already loaded, and it was not
      detected as a forge-fork of any other origin
    * ``disabled`` when incremental loading is disabled by configuration
    """

    visit_type = "git"

    def __init__(
        self,
        storage: StorageInterface,
        url: str,
        incremental: bool = True,
        repo_representation: Type[RepoRepresentation] = RepoRepresentation,
        pack_size_bytes: int = 4 * 1024 * 1024 * 1024,
        temp_file_cutoff: int = 100 * 1024 * 1024,
        parallel_pack_threshold_bytes: int = 100 * 1000 * 1000,
        connect_timeout: float = 120,
        read_timeout: float = 600,
        verify_certs: bool = True,
        urllib3_extra_kwargs: Dict[str, Any] = {},
        **kwargs: Any,
    ):
        """Initialize the bulk updater.

        Args:
            repo_representation: swh's repository representation
            which is in charge of filtering between known and remote
            data.
            ...

            incremental: If True, the default, this starts from the last known snapshot
                (if any) references. Otherwise, this loads the full repository.

        """
        super().__init__(storage=storage, origin_url=url, **kwargs)
        self.incremental = incremental
        self.repo_representation = repo_representation
        self.pack_size_bytes = pack_size_bytes
        # Accepted for configuration compatibility with the dulwich-based
        # loader (which spooled packs in memory below this cutoff), but
        # unused: the gix engine streams every pack to disk.
        self.temp_file_cutoff = temp_file_cutoff
        self.parallel_pack_threshold_bytes = parallel_pack_threshold_bytes
        # state initialized in fetch_data
        self.remote_refs: Dict[bytes, bytes] = {}
        self.symbolic_refs: Dict[bytes, bytes] = {}
        self.ref_object_types: Dict[bytes, Optional[SnapshotTargetType]] = {}
        self.repo_pack_size_bytes = 0
        # Raw values for the gix fetch path (curl-side timeouts); the
        # urllib3 Timeout below serves the dulwich paths (file:// and the
        # dumb-HTTP fallback in fetch_pack_from_origin).
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.urllib3_extra_kwargs = urllib3_extra_kwargs
        self.urllib3_extra_kwargs["timeout"] = urllib3.util.Timeout(
            connect=connect_timeout, read=read_timeout
        )
        if not verify_certs:
            self.urllib3_extra_kwargs["cert_reqs"] = "CERT_NONE"

    def fetch_pack_from_origin(
        self,
        origin_url: str,
        base_repo: RepoRepresentation,
        do_activity: Callable[[bytes], None],
    ) -> FetchPackReturn:
        """Fetch a pack from the origin using gitoxide (gix).

        The pack is written directly to a temporary file on disk — never
        held entirely in Python memory.  This allows handling arbitrarily
        large packs (e.g. 32 GB Chromium) without OOM.

        Two origin categories route through dulwich instead of gix (see
        :meth:`_fetch_pack_via_dulwich`):

        - ``file://`` URLs, up front: the gix HTTP backend handles
          ``http(s)://`` correctly, but its ``connect()`` for ``file://``
          URLs spawns ``git-upload-pack`` as a subprocess and blocks
          indefinitely on its stdin/stdout (observed under py-spy).
          ``file://`` URLs do not exist in production; they are a test-rig
          convention used by ``prepare_repository_from_archive``.
        - dumb-HTTP servers, on fallback: gix implements only the smart
          protocol and fails the handshake with a typed error on
          dumb-only servers.  Master's dulwich engine handled those
          transparently, and some production origins (static-file hosting)
          still speak only dumb HTTP, so we keep that capability by
          catching the handshake error and retrying via dulwich.
        """
        if origin_url.startswith("file://"):
            return self._fetch_pack_via_dulwich(origin_url, base_repo, do_activity)

        from swh.loader.git._gix import GixFatalError
        from swh.loader.git._gix import fetch_pack as gix_fetch_pack
        from swh.loader.git._gix import fetch_pack_to_file as gix_fetch_to_file

        logger.debug("Transport url to communicate with server: %s", origin_url)

        # Step 1: list remote refs (wants=[] → no pack transferred).
        dumb_http = False
        try:
            with utils.raise_not_found_repository():
                remote_refs_raw, symbolic_refs_raw, _ = gix_fetch_pack(
                    origin_url,
                    [],
                    [],
                    connect_timeout=int(self.connect_timeout),
                    read_timeout=int(self.read_timeout),
                )
        except GixFatalError as e:
            if "dumb" not in str(e).lower():
                raise
            # Dumb-HTTP-only server: gix speaks only the smart protocol
            # and rejects the handshake in under a second ("the 'dumb'
            # protocol is not supported").  Retry the whole fetch via
            # dulwich, which implements dumb HTTP.  The retry happens
            # OUTSIDE this handler so that any later failure (e.g. the
            # pack-size limit) is not chained onto the handshake error —
            # Sentry and logs must report the real failure first.
            dumb_http = True
        if dumb_http:
            logger.debug(
                "dumb-HTTP origin, routing fetch through dulwich: %s",
                origin_url,
            )
            return self._fetch_pack_via_dulwich(origin_url, base_repo, do_activity)

        remote_refs_hex: Dict[bytes, bytes] = {
            name: sha.encode() for name, sha in remote_refs_raw.items()
        }

        # Step 2: compute wants.
        wants_hex: List[bytes] = base_repo.determine_wants(remote_refs_hex)
        logger.debug("fetching %d objects", len(wants_hex))

        # Step 3: fetch pack directly to a temp file on disk.
        wants_bin = [binascii.unhexlify(sha) for sha in wants_hex]
        haves_bin = [binascii.unhexlify(sha) for sha in base_repo.local_heads]

        self._pack_tmp = tempfile.NamedTemporaryFile(suffix=".pack", delete=True)
        pack_path = self._pack_tmp.name

        with utils.raise_not_found_repository():
            _, _, pack_size = gix_fetch_to_file(
                origin_url,
                wants_bin,
                haves_bin,
                self.pack_size_bytes,
                pack_path,
                connect_timeout=int(self.connect_timeout),
                read_timeout=int(self.read_timeout),
            )

        logger.debug("fetched_pack_size=%s", pack_size)

        return FetchPackReturn(
            remote_refs=utils.filter_refs(remote_refs_hex),
            symbolic_refs=utils.filter_symbolic_refs(symbolic_refs_raw),
            pack_path=pack_path,
            pack_size=pack_size,
        )

    def _fetch_pack_via_dulwich(
        self,
        origin_url: str,
        base_repo: RepoRepresentation,
        do_activity: Callable[[bytes], None],
    ) -> FetchPackReturn:
        """Fetch a pack via dulwich, for the origins gix cannot serve.

        Two cases route here (see :meth:`fetch_pack_from_origin`):
        ``file://`` URLs (gix spawns a blocking subprocess for those) and
        dumb-HTTP-only servers (gix implements only the smart protocol).
        The pack is written to a temp file on disk so the rest of the
        gix-loader pipeline (which reads from ``pack_path`` via
        ``_gix.iter_pack_objects``) consumes it without a special case
        downstream.  The configured ``pack_size_bytes`` limit is enforced
        chunk-by-chunk during the download, exactly as on the gix path.
        """
        import dulwich.client
        from dulwich.object_store import ObjectStoreGraphWalker

        transport_kwargs: Dict[str, Any] = {"thin_packs": False}
        if origin_url.startswith(("http://", "https://")):
            # Inject urllib3 kwargs (timeouts, cert handling) into the
            # pool manager, as master's dulwich engine did.
            transport_kwargs["pool_manager"] = dulwich.client.default_urllib3_manager(
                config=None,
                **self.urllib3_extra_kwargs,
            )

        client, path = dulwich.client.get_transport_and_path(
            location=origin_url,
            config=None,
            operation="pull",
            **transport_kwargs,
        )
        logger.debug("Client %s to fetch pack at %s", client, path)

        self._pack_tmp = tempfile.NamedTemporaryFile(suffix=".pack", delete=True)
        pack_path = self._pack_tmp.name
        pack_size = 0

        pack_writer = utils.PackWriter(
            pack_buffer=self._pack_tmp,
            size_limit=self.pack_size_bytes,
            origin_url=origin_url,
            fetch_pack_logger=fetch_pack_logger,
        )

        def write_chunk(data: bytes) -> int:
            # PackWriter enforces the pack_size_bytes limit (raises
            # IOError → visit failed) and logs download progress.  The
            # byte count is returned for dulwich's progress accounting.
            nonlocal pack_size
            pack_writer.write(data)
            pack_size += len(data)
            return len(data)

        # The gix RepoRepresentation does not provide a ``graph_walker``
        # method (that is dulwich-specific).  Build one inline from the
        # ``local_heads`` set, which has the same hex-bytes shape both
        # dulwich and gix use.  ``get_parents`` returning [] keeps the
        # walker shallow — adequate because the smart-protocol negotiation
        # only needs a bounded set of haves to negotiate from.
        graph_walker = ObjectStoreGraphWalker(
            base_repo.local_heads,  # type: ignore[arg-type]
            get_parents=lambda commit: [],
        )

        # Adapt the gix RepoRepresentation's determine_wants signature to
        # whatever dulwich's DetermineWantsFunc expects in the installed
        # version — older dulwich passes `(refs)`, newer passes
        # `(refs, depth)`.  Accept any trailing args.
        def _determine_wants(
            refs: Mapping[bytes, bytes], *_args: Any, **_kw: Any
        ) -> List[bytes]:
            return base_repo.determine_wants(refs)

        with raise_not_found_repository():
            pack_result = client.fetch_pack(
                path.encode() if isinstance(path, str) else path,
                _determine_wants,  # type: ignore[arg-type]
                graph_walker,
                write_chunk,
                progress=do_activity,
            )

        self._pack_tmp.flush()
        remote_refs_raw = pack_result.refs or {}
        symbolic_refs_raw = pack_result.symrefs or {}

        return FetchPackReturn(
            remote_refs=utils.filter_refs(remote_refs_raw),  # type: ignore[arg-type]
            symbolic_refs=utils.filter_symbolic_refs(symbolic_refs_raw),  # type: ignore[arg-type]
            pack_path=pack_path,
            pack_size=pack_size,
        )

    def get_full_snapshot(self, origin_url) -> Optional[Snapshot]:
        return snapshot_get_latest(
            self.storage,
            origin_url,
            visit_type=self.visit_type,
        )

    def load_metadata_objects(
        self, metadata_objects: List[RawExtrinsicMetadata]
    ) -> None:
        for metadata in metadata_objects:
            if (
                metadata.target.object_type == ExtendedObjectType.ORIGIN
                and metadata.format == "application/vnd.github.v3+json"
            ):
                try:
                    json_metadata = json.loads(metadata.metadata)
                    self.repo_pack_size_bytes = json_metadata.get("size", 0) * 1024
                except json.JSONDecodeError:
                    logger.warning(
                        "JSON metadata for origin %s could not be parsed.",
                        self.origin.url,
                    )
        super().load_metadata_objects(metadata_objects)

    def prepare(self) -> None:
        assert self.origin is not None

        self.prev_snapshot = Snapshot(branches={})
        """Last snapshot of this origin if any; empty snapshot otherwise"""
        self.base_snapshots = []
        """Last snapshot of this origin and all its parents, if any."""

        self.statsd.constant_tags["incremental_enabled"] = self.incremental
        self.statsd.constant_tags["has_parent_origins"] = bool(self.parent_origins)

        # May be set to True later
        self.statsd.constant_tags["has_parent_snapshot"] = False

        if self.incremental:
            prev_snapshot = self.get_full_snapshot(self.origin.url)
            self.statsd.constant_tags["has_previous_snapshot"] = bool(prev_snapshot)
            if prev_snapshot:
                self.prev_snapshot = prev_snapshot
                self.base_snapshots.append(prev_snapshot)

            if self.parent_origins is not None:
                # If this origin is a forge fork, load incrementally from the
                # origins it was forked from
                for parent_origin in self.parent_origins:
                    parent_snapshot = self.get_full_snapshot(parent_origin.url)
                    if parent_snapshot is not None:
                        self.statsd.constant_tags["has_parent_snapshot"] = True
                        self.base_snapshots.append(parent_snapshot)

        # Increments a metric with full name 'swh_loader_git'; which is useful to
        # count how many runs of the loader are with each incremental mode
        self.statsd.increment("git_total", tags={})

    def fetch_data(self) -> bool:
        assert self.origin is not None

        if not self.base_snapshots and self.repo_pack_size_bytes > self.pack_size_bytes:
            raise IOError(
                f"Pack file too big for repository {self.origin.url}, "
                f"limit is {self.pack_size_bytes} bytes, "
                f"current size is {self.repo_pack_size_bytes}"
            )

        base_repo = self.repo_representation(
            storage=self.storage,
            base_snapshots=self.base_snapshots,
            incremental=self.incremental,
            statsd=self.statsd,
        )

        # Remote logging utilities

        # Number of lines (ending with a carriage return) elided when debug
        # logging is not enabled
        remote_lines_elided = 0

        # Timestamp where the last elision was logged
        last_elision_logged = time.monotonic()

        def maybe_log_elision(force: bool = False):
            nonlocal remote_lines_elided
            nonlocal last_elision_logged

            if remote_lines_elided and (
                force
                # Always log at least every LOGGING_INTERVAL
                or time.monotonic() > last_elision_logged + LOGGING_INTERVAL
            ):
                remote_logger.info(
                    "%s remote line%s elided",
                    remote_lines_elided,
                    "s" if remote_lines_elided > 1 else "",
                )
                remote_lines_elided = 0
                last_elision_logged = time.monotonic()

        def log_remote_message(line: bytes):
            nonlocal remote_lines_elided

            do_debug = remote_logger.isEnabledFor(logging.DEBUG)

            if not line.endswith(b"\n"):
                # This is a verbose line, ending with a carriage return only
                if do_debug:
                    if stripped := line.strip():
                        remote_logger.debug(
                            "remote: %s", stripped.decode("utf-8", "backslashreplace")
                        )
                else:
                    remote_lines_elided += 1
                    maybe_log_elision()
            else:
                # This is the last line in the current section, we will always log it
                maybe_log_elision(force=True)
                if stripped := line.strip():
                    remote_logger.info(
                        "remote: %s", stripped.decode("utf-8", "backslashreplace")
                    )

        # This buffer keeps the end of what do_remote has received, across
        # calls, if it happens to be unterminated
        next_line_buf = b""

        def do_remote(msg: bytes) -> None:
            nonlocal next_line_buf

            lines, next_line_buf = split_lines_and_remainder(next_line_buf + msg)

            for line in lines:
                log_remote_message(line)

        try:
            with raise_not_found_repository():
                fetch_info = self.fetch_pack_from_origin(
                    self.origin.url, base_repo, do_remote
                )
        except NotFound:
            # NotFound inherits from ValueError and should not be caught
            # by the next exception handler
            raise
        else:
            # Always log what remains in the next_line_buf, if it's not empty
            maybe_log_elision(force=True)
            log_remote_message(next_line_buf)

        self.pack_path = fetch_info.pack_path
        self.pack_size = fetch_info.pack_size
        self.remote_refs = fetch_info.remote_refs
        self.symbolic_refs = fetch_info.symbolic_refs
        self.ref_object_types = {sha1: None for sha1 in self.remote_refs.values()}

        logger.info(
            "Listed %d refs for repo %s",
            len(self.remote_refs),
            self.origin.url,
            extra={
                "swh_type": "git_repo_list_refs",
                "swh_repo": self.origin.url,
                "swh_num_refs": len(self.remote_refs),
            },
        )

        # No more data to fetch
        return False

    def save_data(self) -> None:
        """Store a pack for archival"""
        assert isinstance(self.visit_date, datetime.datetime)

        pack_dir = self.get_save_data_path()

        pack_name = "%s.pack" % self.visit_date.isoformat()
        refs_name = "%s.refs" % self.visit_date.isoformat()

        import shutil

        with (
            open(self.pack_path, "rb") as src,
            open(os.path.join(pack_dir, pack_name), "xb") as dst,
        ):
            shutil.copyfileobj(src, dst)

        with open(os.path.join(pack_dir, refs_name), "xb") as f:
            pickle.dump(self.remote_refs, f)

    # ── Single-pass store_data (overrides BaseGitLoader.store_data) ─────

    _STORE_BATCH_SIZE = 1000

    def _convert_object(self, obj_tuple):
        """Convert a PackReader tuple to a SWH model object and update
        ref_object_types.  Returns ``(type_name, model_object)``."""
        type_num = obj_tuple[0]

        # Trees from PackReader / ParallelPackReader can come pre-built
        # as a Directory object in a 2-tuple ``(2, directory)``.  Handle
        # this fast path before any byte-level extraction: ``obj_tuple[1]``
        # is the Directory itself (no buffer interface), so calling
        # ``binascii.hexlify`` on it would crash with ``TypeError: a
        # bytes-like object is required, not 'Directory'``.
        if type_num == 2 and isinstance(obj_tuple[1], Directory):
            directory = obj_tuple[1]
            sha_hex = binascii.hexlify(directory.id)
            if sha_hex in self.ref_object_types:
                self.ref_object_types[sha_hex] = SnapshotTargetType.DIRECTORY
            return ("directory", directory)

        # All other shapes: ``obj_tuple[1]`` is the raw sha1_git bytes.
        sha1_git = obj_tuple[1]
        sha_hex = binascii.hexlify(sha1_git)

        if type_num == 3:  # blob
            _, _, sha1, sha256, blake2s256, data = obj_tuple
            if sha_hex in self.ref_object_types:
                self.ref_object_types[sha_hex] = SnapshotTargetType.CONTENT
            obj = converters.blob_to_content_precomputed(
                sha1_git,
                sha1,
                sha256,
                blake2s256,
                data,
                max_content_size=self.max_content_size,
            )
            if isinstance(obj, SkippedContent):
                return ("skipped_content", obj)
            return ("content", obj)
        elif (
            type_num == 2
        ):  # tree (raw fields shape: (2, sha, raw, entries, hash_match))
            if sha_hex in self.ref_object_types:
                self.ref_object_types[sha_hex] = SnapshotTargetType.DIRECTORY
            _, _, raw_data, entries, hash_match = obj_tuple
            return (
                "directory",
                converters.tree_to_directory_preparsed(
                    sha1_git, raw_data, entries, hash_match
                ),
            )
        elif type_num == 1:  # commit
            # The tuple's 4th element is the Rust round-trip flag; it is
            # deliberately NOT forwarded: parse-order byte equality does
            # not imply canonical-order hash equality for commits/tags
            # (see converters.commit_to_revision), so the converter
            # always verifies.
            _, _, data, _ = obj_tuple
            if sha_hex in self.ref_object_types:
                self.ref_object_types[sha_hex] = SnapshotTargetType.REVISION
            return (
                "revision",
                converters.commit_to_revision(sha1_git, data),
            )
        elif type_num == 4:  # tag
            _, _, data, _ = obj_tuple
            if sha_hex in self.ref_object_types:
                self.ref_object_types[sha_hex] = SnapshotTargetType.RELEASE
            return (
                "release",
                converters.tag_to_release(sha1_git, data),
            )
        else:
            raise ValueError(f"Unknown object type: {type_num}")

    def store_data(self) -> None:
        """Store fetched data with a single pass through the pack file.

        Overrides :meth:`BaseGitLoader.store_data` to iterate the pack
        exactly once, dispatching each object to a type-specific batch.
        Batches are flushed to storage every :attr:`_STORE_BATCH_SIZE`
        objects per type.

        This is safe because:

        - Storage ``*_add()`` methods are idempotent.
        - No FK constraints between object types in PostgreSQL or Cassandra.

        Ordering caveat: packs are typically commits-first, so revisions
        can reach storage (and the journal) before the directories and
        contents they reference.  BufferingProxyStorage, if configured,
        type-orders each flush it performs, but only over what is
        currently buffered — it does NOT provide visit-wide topological
        ordering across flushes.  A visit interrupted mid-pack can
        therefore leave revisions whose root directory arrives only on a
        later successful visit.  The dulwich engine's four-pass walk
        (contents, then directories, revisions, releases) did guarantee
        that order; whether the stream-order write is acceptable is an
        explicit sign-off item for the storage owners in the design
        review, not an assumption of this code.
        """
        assert self.origin
        if self.save_data_path:
            self.save_data()

        counts: Dict[str, int] = collections.defaultdict(int)
        storage_summary: Dict[str, int] = collections.Counter()

        def sum_counts():
            return sum(counts.values())

        def sum_storage():
            return sum(storage_summary[f"{t}:add"] for t in counts)

        def maybe_log_summary(msg, force=False):
            self.maybe_log(
                msg + ": processed %s objects, %s are new",
                sum_counts,
                sum_storage,
                force=force,
            )

        # Explicit Callable annotation so mypy can typecheck the
        # ``_ADD[type_name](batches[type_name])`` call below: the dict
        # value types from the StorageInterface methods are slightly
        # different per row (their argument types disagree), so without
        # this annotation mypy unifies them as ``object`` and reports
        # "Cannot call function of unknown type [operator]".
        _ADD: Dict[str, Callable[[List[Any]], Dict[str, int]]] = {
            "content": self.storage.content_add,
            "skipped_content": self.storage.skipped_content_add,
            "directory": self.storage.directory_add,
            "revision": self.storage.revision_add,
            "release": self.storage.release_add,
        }
        batches: Dict[str, list] = {k: [] for k in _ADD}
        batch_size = self._STORE_BATCH_SIZE

        def flush_batch(type_name: str) -> None:
            if batches[type_name]:
                storage_summary.update(_ADD[type_name](batches[type_name]))
                batches[type_name] = []

        def flush_all() -> None:
            for t in ("content", "skipped_content", "directory", "revision", "release"):
                flush_batch(t)
            storage_summary.update(self.flush())

        if self.pack_size > 0:
            from swh.loader.git._gix import PackReader, ParallelPackReader

            # Use parallel inflation for packs above the configured threshold
            pack_reader: Iterable
            if self.pack_size > self.parallel_pack_threshold_bytes:
                pack_reader = ParallelPackReader(self.pack_path, channel_bound=4096)
            else:
                pack_reader = PackReader(self.pack_path)

            # Time spent inside the reader's __next__ is the pack-inflation
            # cost (Rust-side decode + delta resolution).  Emitted under the
            # same metric name as the previous dulwich implementation for
            # dashboard continuity; there it measured the PackInflater
            # passes, here the single-pass streaming equivalent.  The two
            # monotonic() calls per object are ~50 ns against >1 us of
            # per-object processing.
            total_inflate_time = 0.0
            reader_iter = iter(pack_reader)
            while True:
                t0 = time.monotonic()
                try:
                    obj_tuple = next(reader_iter)
                except StopIteration:
                    total_inflate_time += time.monotonic() - t0
                    break
                total_inflate_time += time.monotonic() - t0

                type_name, model_obj = self._convert_object(obj_tuple)
                counts[type_name] += 1
                batches[type_name].append(model_obj)

                if len(batches[type_name]) >= batch_size:
                    flush_batch(type_name)

                maybe_log_summary("Processing pack")

            self.statsd_timing("inflate_git_packfile", total_inflate_time * 1000.0)

        flush_all()
        maybe_log_summary("After pack objects", force=True)

        snapshot = self.get_snapshot()
        counts["snapshot"] += 1
        storage_summary.update(self.storage.snapshot_add([snapshot]))
        storage_summary.update(self.flush())
        self.loaded_snapshot_id = snapshot.id

        # Fixed iteration order (not pack-encounter order, which is
        # typically commits-first): keeps the emitted metric sequence
        # deterministic and identical to the dulwich engine's.
        for object_type in (
            "content",
            "skipped_content",
            "directory",
            "revision",
            "release",
            "snapshot",
        ):
            total = counts[object_type]
            filtered = total - storage_summary[f"{object_type}:add"]
            assert 0 <= filtered <= total, (filtered, total)
            if total == 0:
                continue
            tags = {"object_type": object_type}
            self.statsd.histogram(
                "filtered_objects_percent", filtered / total, tags=tags
            )
            self.statsd.increment("filtered_objects_total_sum", filtered, tags=tags)
            self.statsd.increment("filtered_objects_total_count", total, tags=tags)

        maybe_log_summary("After snapshot", force=True)

    # Keep get_* methods for from_disk.py compatibility (they use the base
    # store_data which calls them sequentially).

    def get_contents(self) -> Iterable[BaseContent]:
        if self.pack_size <= 0:
            return
        from swh.loader.git._gix import PackReader

        for obj_tuple in PackReader(self.pack_path):
            if obj_tuple[0] == 3:
                _, sha1_git, sha1, sha256, blake2s256, data = obj_tuple
                sha_hex = binascii.hexlify(sha1_git)
                if sha_hex in self.ref_object_types:
                    self.ref_object_types[sha_hex] = SnapshotTargetType.CONTENT
                yield converters.blob_to_content_precomputed(
                    sha1_git,
                    sha1,
                    sha256,
                    blake2s256,
                    data,
                    max_content_size=self.max_content_size,
                )

    def get_directories(self) -> Iterable[Directory]:
        if self.pack_size <= 0:
            return
        from swh.loader.git._gix import PackReader

        for obj_tuple in PackReader(self.pack_path):
            if obj_tuple[0] == 2:
                if isinstance(obj_tuple[1], Directory):
                    dir_obj = obj_tuple[1]
                    sha_hex = binascii.hexlify(dir_obj.id)
                    if sha_hex in self.ref_object_types:
                        self.ref_object_types[sha_hex] = SnapshotTargetType.DIRECTORY
                    yield dir_obj
                else:
                    _, sha1_git, raw_data, entries, hash_match = obj_tuple
                    sha_hex = binascii.hexlify(sha1_git)
                    if sha_hex in self.ref_object_types:
                        self.ref_object_types[sha_hex] = SnapshotTargetType.DIRECTORY
                    yield converters.tree_to_directory_preparsed(
                        sha1_git, raw_data, entries, hash_match
                    )

    def get_revisions(self) -> Iterable[Revision]:
        if self.pack_size <= 0:
            return
        from swh.loader.git._gix import PackReader

        for obj_tuple in PackReader(self.pack_path):
            if obj_tuple[0] == 1:
                _, sha1_git, data, _ = obj_tuple
                sha_hex = binascii.hexlify(sha1_git)
                if sha_hex in self.ref_object_types:
                    self.ref_object_types[sha_hex] = SnapshotTargetType.REVISION
                yield converters.commit_to_revision(sha1_git, data)

    def get_releases(self) -> Iterable[Release]:
        if self.pack_size <= 0:
            return
        from swh.loader.git._gix import PackReader

        for obj_tuple in PackReader(self.pack_path):
            if obj_tuple[0] == 4:
                _, sha1_git, data, _ = obj_tuple
                sha_hex = binascii.hexlify(sha1_git)
                if sha_hex in self.ref_object_types:
                    self.ref_object_types[sha_hex] = SnapshotTargetType.RELEASE
                yield converters.tag_to_release(sha1_git, data)

    def get_snapshot(self) -> Snapshot:
        """Get the snapshot for the current visit.

        The main complexity of this function is mapping target objects to their
        types, as the `refs` dictionaries returned by the git server only give
        us the identifiers for the target objects, and not their types.

        The loader itself only knows the types of the objects that it has
        fetched from the server (as it has parsed them while loading them to
        the archive). As we only fetched an increment between the previous
        snapshot and the current state of the server, we are missing the type
        information for the objects that would already have been referenced by
        the previous snapshot, and that the git server didn't send us. We infer
        the type of these objects from the previous snapshot.

        """
        branches: Dict[bytes, Optional[SnapshotBranch]] = {}

        unfetched_refs: Dict[bytes, bytes] = {}

        # Retrieve types from the objects loaded by the current loader
        for ref_name, ref_object in self.remote_refs.items():
            if ref_name in self.symbolic_refs:
                continue
            ref_target = hashutil.hash_to_bytes(ref_object.decode())
            target_type = self.ref_object_types.get(ref_object)
            if target_type:
                branches[ref_name] = SnapshotBranch(
                    target=ref_target, target_type=target_type
                )
            else:
                # The object pointed at by this ref was not fetched, supposedly
                # because it existed in the base snapshot. We record it here,
                # and we can get it from the base snapshot later.
                unfetched_refs[ref_name] = ref_target

        dangling_branches = {}
        # Handle symbolic references as alias branches
        for sym_ref_name, sym_ref_target in self.symbolic_refs.items():
            branches[sym_ref_name] = SnapshotBranch(
                target_type=SnapshotTargetType.ALIAS,
                target=sym_ref_target,
            )
            if sym_ref_target not in branches and sym_ref_target not in unfetched_refs:
                # This handles the case where the pointer is "dangling".
                # There's a chance that a further symbolic reference
                # override this default value, which is totally fine.
                dangling_branches[sym_ref_target] = sym_ref_name
                branches[sym_ref_target] = None

        if unfetched_refs:
            # Handle inference of object types from the contents of the
            # previous snapshot
            unknown_objects = {}

            base_snapshot_reverse_branches = {
                branch.target: branch
                for base_snapshot in reversed(self.base_snapshots)
                for branch in base_snapshot.branches.values()
                if branch and branch.target_type != SnapshotTargetType.ALIAS
            }
            assert all(
                base_snapshot_reverse_branches[branch.target] == branch
                for branch in self.prev_snapshot.branches.values()
                if branch and branch.target_type != SnapshotTargetType.ALIAS
            ), "base_snapshot_reverse_branches is not a superset of prev_snapshot"

            for unfetched_ref_name, target in unfetched_refs.items():
                branch = base_snapshot_reverse_branches.get(target)
                branches[unfetched_ref_name] = branch
                if not branch:
                    unknown_objects[unfetched_ref_name] = target

            if unknown_objects and self.base_snapshots:
                # The remote has sent us a partial packfile. It will have skipped
                # objects that it knows are ancestors of the heads we have sent as
                # known. We can look these objects up in the archive, as they should
                # have had all their ancestors loaded when the previous snapshot was
                # loaded.
                refs_for_target = defaultdict(list)
                for unfetched_ref_name, target in unknown_objects.items():
                    refs_for_target[target].append(unfetched_ref_name)

                targets_unknown = set(refs_for_target)

                for method, target_type in (
                    (self.storage.revision_missing, SnapshotTargetType.REVISION),
                    (self.storage.release_missing, SnapshotTargetType.RELEASE),
                    (self.storage.directory_missing, SnapshotTargetType.DIRECTORY),
                    (
                        self.storage.content_missing_per_sha1_git,
                        SnapshotTargetType.CONTENT,
                    ),
                ):
                    missing = set(method(list(targets_unknown)))
                    known = targets_unknown - missing

                    for target in known:
                        for unfetched_ref_name in refs_for_target[target]:
                            logger.debug(
                                "Inferred type %s for branch %r pointing at unfetched %s",
                                target_type.name,
                                unfetched_ref_name,
                                hashutil.hash_to_hex(target),
                                extra={
                                    "swh_type": "swh_loader_git_inferred_target_type"
                                },
                            )
                            branches[unfetched_ref_name] = SnapshotBranch(
                                target=target, target_type=target_type
                            )
                            del unknown_objects[unfetched_ref_name]

                    targets_unknown = missing
                    if not targets_unknown:
                        break

            if unknown_objects:
                # This object was referenced by the server; We did not fetch
                # it, and we do not know it from the previous snapshot. This is
                # likely a bug in the loader.
                raise RuntimeError(
                    "Unknown objects referenced by remote refs: %s"
                    % (
                        ", ".join(
                            f"{name!r}: {hashutil.hash_to_hex(obj)}"
                            for name, obj in unknown_objects.items()
                        )
                    )
                )

        utils.warn_dangling_branches(
            branches, dangling_branches, logger, self.origin.url
        )

        self.snapshot = Snapshot(branches=branches)
        return self.snapshot

    def load_status(self) -> Dict[str, Any]:
        """The load was eventful if the current snapshot is different to
        the one we retrieved at the beginning of the run"""
        eventful = False
        if self.prev_snapshot and self.snapshot:
            eventful = self.snapshot.id != self.prev_snapshot.id
        elif self.snapshot:
            eventful = bool(self.snapshot.branches)

        return {"status": ("eventful" if eventful else "uneventful")}


if __name__ == "__main__":
    import click

    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(process)d %(message)s"
    )

    from deprecated import deprecated

    @deprecated(version="1.1", reason="Use `swh loader run git --help` instead")
    @click.command()
    @click.option("--origin-url", help="Origin url", required=True)
    @click.option("--base-url", default=None, help="Optional Base url")
    @click.option(
        "--ignore-history/--no-ignore-history",
        help="Ignore the repository history",
        default=False,
    )
    def main(origin_url: str, incremental: bool) -> Dict[str, Any]:
        from swh.storage import get_storage

        storage = get_storage(cls="memory")
        loader = GitLoader(
            storage,
            origin_url,
            incremental=incremental,
        )
        return loader.load()

    main()
