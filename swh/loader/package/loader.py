# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
import tempfile
import os
import sys
from typing import (
    Any,
    Dict,
    Iterator,
    Generic,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

import attr
import sentry_sdk

from swh.core.tarball import uncompress
from swh.core.config import SWHConfig
from swh.model import from_disk
from swh.model.collections import ImmutableDict
from swh.model.hashutil import hash_to_hex
from swh.model.model import (
    BaseModel,
    Sha1Git,
    Revision,
    TargetType,
    Snapshot,
    Origin,
    OriginVisit,
    OriginVisitStatus,
    MetadataAuthority,
    MetadataFetcher,
    MetadataTargetType,
    RawExtrinsicMetadata,
)
from swh.model.identifiers import SWHID
from swh.storage import get_storage
from swh.storage.interface import StorageInterface
from swh.storage.utils import now
from swh.storage.algos.snapshot import snapshot_get_latest

from swh.loader.package.utils import download


logger = logging.getLogger(__name__)


@attr.s
class RawExtrinsicMetadataCore:
    """Contains the core of the metadata extracted by a loader, that will be
    used to build a full RawExtrinsicMetadata object by adding object identifier,
    context, and provenance information."""

    format = attr.ib(type=str)
    metadata = attr.ib(type=bytes)
    discovery_date = attr.ib(type=Optional[datetime.datetime], default=None)
    """Defaults to the visit date."""


@attr.s
class BasePackageInfo:
    """Compute the primary key for a dict using the id_keys as primary key
       composite.

    Args:
        d: A dict entry to compute the primary key on
        id_keys: Sequence of keys to use as primary key

    Returns:
        The identity for that dict entry

    """

    url = attr.ib(type=str)
    filename = attr.ib(type=Optional[str])

    # The following attribute has kw_only=True in order to allow subclasses
    # to add attributes. Without kw_only, attributes without default values cannot
    # go after attributes with default values.
    # See <https://github.com/python-attrs/attrs/issues/38>

    revision_extrinsic_metadata = attr.ib(
        type=List[RawExtrinsicMetadataCore], default=[], kw_only=True,
    )

    # TODO: add support for metadata for directories and contents

    @property
    def ID_KEYS(self):
        raise NotImplementedError(f"{self.__class__.__name__} is missing ID_KEYS")

    def artifact_identity(self):
        return [getattr(self, k) for k in self.ID_KEYS]


TPackageInfo = TypeVar("TPackageInfo", bound=BasePackageInfo)


class PackageLoader(Generic[TPackageInfo]):
    # Origin visit type (str) set by the loader
    visit_type = ""

    DEFAULT_CONFIG = {
        "create_authorities": ("bool", True),
        "create_fetchers": ("bool", True),
    }

    def __init__(self, url):
        """Loader's constructor. This raises exception if the minimal required
           configuration is missing (cf. fn:`check` method).

        Args:
            url (str): Origin url to load data from

        """
        # This expects to use the environment variable SWH_CONFIG_FILENAME
        self.config = SWHConfig.parse_config_file()
        self._check_configuration()
        self.storage: StorageInterface = get_storage(**self.config["storage"])
        self.url = url
        self.visit_date = datetime.datetime.now(tz=datetime.timezone.utc)
        self.max_content_size = self.config["max_content_size"]

    def _check_configuration(self):
        """Checks the minimal configuration required is set for the loader.

        If some required configuration is missing, exception detailing the
        issue is raised.

        """
        if "storage" not in self.config:
            raise ValueError("Misconfiguration, at least the storage key should be set")

    def get_versions(self) -> Sequence[str]:
        """Return the list of all published package versions.

        Returns:
            Sequence of published versions

        """
        return []

    def get_package_info(self, version: str) -> Iterator[Tuple[str, TPackageInfo]]:
        """Given a release version of a package, retrieve the associated
           package information for such version.

        Args:
            version: Package version

        Returns:
            (branch name, package metadata)

        """
        yield from {}

    def build_revision(
        self, p_info: TPackageInfo, uncompressed_path: str, directory: Sha1Git
    ) -> Optional[Revision]:
        """Build the revision from the archive metadata (extrinsic
        artifact metadata) and the intrinsic metadata.

        Args:
            p_info: Package information
            uncompressed_path: Artifact uncompressed path on disk

        Returns:
            SWH data dict

        """
        raise NotImplementedError("build_revision")

    def get_default_version(self) -> str:
        """Retrieve the latest release version if any.

        Returns:
            Latest version

        """
        return ""

    def last_snapshot(self) -> Optional[Snapshot]:
        """Retrieve the last snapshot out of the last visit.

        """
        return snapshot_get_latest(self.storage, self.url)

    def known_artifacts(self, snapshot: Optional[Snapshot]) -> Dict[Sha1Git, BaseModel]:
        """Retrieve the known releases/artifact for the origin.

        Args
            snapshot: snapshot for the visit

        Returns:
            Dict of keys revision id (bytes), values a metadata Dict.

        """
        if not snapshot:
            return {}

        # retrieve only revisions (e.g the alias we do not want here)
        revs = [
            rev.target
            for rev in snapshot.branches.values()
            if rev and rev.target_type == TargetType.REVISION
        ]
        known_revisions = self.storage.revision_get(revs)

        return {
            revision["id"]: revision["metadata"]
            for revision in known_revisions
            if revision
        }

    def resolve_revision_from(
        self, known_artifacts: Dict, p_info: TPackageInfo,
    ) -> Optional[bytes]:
        """Resolve the revision from a snapshot and an artifact metadata dict.

        If the artifact has already been downloaded, this will return the
        existing revision targeting that uncompressed artifact directory.
        Otherwise, this returns None.

        Args:
            snapshot: Snapshot
            p_info: Package information

        Returns:
            None or revision identifier

        """
        return None

    def download_package(
        self, p_info: TPackageInfo, tmpdir: str
    ) -> List[Tuple[str, Mapping]]:
        """Download artifacts for a specific package. All downloads happen in
        in the tmpdir folder.

        Default implementation expects the artifacts package info to be
        about one artifact per package.

        Note that most implementation have 1 artifact per package. But some
        implementation have multiple artifacts per package (debian), some have
        none, the package is the artifact (gnu).

        Args:
            artifacts_package_info: Information on the package artifacts to
                download (url, filename, etc...)
            tmpdir: Location to retrieve such artifacts

        Returns:
            List of (path, computed hashes)

        """
        return [download(p_info.url, dest=tmpdir, filename=p_info.filename)]

    def uncompress(
        self, dl_artifacts: List[Tuple[str, Mapping[str, Any]]], dest: str
    ) -> str:
        """Uncompress the artifact(s) in the destination folder dest.

        Optionally, this could need to use the p_info dict for some more
        information (debian).

        """
        uncompressed_path = os.path.join(dest, "src")
        for a_path, _ in dl_artifacts:
            uncompress(a_path, dest=uncompressed_path)
        return uncompressed_path

    def extra_branches(self) -> Dict[bytes, Mapping[str, Any]]:
        """Return an extra dict of branches that are used to update the set of
        branches.

        """
        return {}

    def load(self) -> Dict:
        """Load for a specific origin the associated contents.

        for each package version of the origin

        1. Fetch the files for one package version By default, this can be
           implemented as a simple HTTP request. Loaders with more specific
           requirements can override this, e.g.: the PyPI loader checks the
           integrity of the downloaded files; the Debian loader has to download
           and check several files for one package version.

        2. Extract the downloaded files By default, this would be a universal
           archive/tarball extraction.

           Loaders for specific formats can override this method (for instance,
           the Debian loader uses dpkg-source -x).

        3. Convert the extracted directory to a set of Software Heritage
           objects Using swh.model.from_disk.

        4. Extract the metadata from the unpacked directories This would only
           be applicable for "smart" loaders like npm (parsing the
           package.json), PyPI (parsing the PKG-INFO file) or Debian (parsing
           debian/changelog and debian/control).

           On "minimal-metadata" sources such as the GNU archive, the lister
           should provide the minimal set of metadata needed to populate the
           revision/release objects (authors, dates) as an argument to the
           task.

        5. Generate the revision/release objects for the given version. From
           the data generated at steps 3 and 4.

        end for each

        6. Generate and load the snapshot for the visit

        Using the revisions/releases collected at step 5., and the branch
        information from step 0., generate a snapshot and load it into the
        Software Heritage archive

        """
        status_load = "uneventful"  # either: eventful, uneventful, failed
        status_visit = "full"  # either: partial, full
        tmp_revisions = {}  # type: Dict[str, List]
        snapshot = None

        def finalize_visit() -> Dict[str, Any]:
            """Finalize the visit:

            - flush eventual unflushed data to storage
            - update origin visit's status
            - return the task's status

            """
            self.storage.flush()

            snapshot_id: Optional[bytes] = None
            if snapshot and snapshot.id:  # to prevent the snapshot.id to b""
                snapshot_id = snapshot.id
            assert visit.visit
            visit_status = OriginVisitStatus(
                origin=self.url,
                visit=visit.visit,
                date=now(),
                status=status_visit,
                snapshot=snapshot_id,
            )
            self.storage.origin_visit_status_add([visit_status])
            result: Dict[str, Any] = {
                "status": status_load,
            }
            if snapshot_id:
                result["snapshot_id"] = hash_to_hex(snapshot_id)
            return result

        # Prepare origin and origin_visit
        origin = Origin(url=self.url)
        try:
            self.storage.origin_add([origin])
            visit = list(
                self.storage.origin_visit_add(
                    [
                        OriginVisit(
                            origin=self.url, date=self.visit_date, type=self.visit_type,
                        )
                    ]
                )
            )[0]
        except Exception as e:
            logger.exception("Failed to initialize origin_visit for %s", self.url)
            sentry_sdk.capture_exception(e)
            return {"status": "failed"}

        try:
            last_snapshot = self.last_snapshot()
            logger.debug("last snapshot: %s", last_snapshot)
            known_artifacts = self.known_artifacts(last_snapshot)
            logger.debug("known artifacts: %s", known_artifacts)
        except Exception as e:
            logger.exception("Failed to get previous state for %s", self.url)
            sentry_sdk.capture_exception(e)
            status_visit = "partial"
            status_load = "failed"
            return finalize_visit()

        load_exceptions: List[Exception] = []

        for version in self.get_versions():  # for each
            logger.debug("version: %s", version)
            tmp_revisions[version] = []
            # `p_` stands for `package_`
            for branch_name, p_info in self.get_package_info(version):
                logger.debug("package_info: %s", p_info)
                revision_id = self.resolve_revision_from(known_artifacts, p_info)
                if revision_id is None:
                    try:
                        revision_id = self._load_revision(p_info, origin)
                        if revision_id:
                            self._load_extrinsic_revision_metadata(p_info, revision_id)
                        self.storage.flush()
                        status_load = "eventful"
                    except Exception as e:
                        self.storage.clear_buffers()
                        load_exceptions.append(e)
                        sentry_sdk.capture_exception(e)
                        logger.exception(
                            "Failed loading branch %s for %s", branch_name, self.url
                        )
                        continue

                    if revision_id is None:
                        continue

                tmp_revisions[version].append((branch_name, revision_id))

        if load_exceptions:
            status_visit = "partial"

        if not tmp_revisions:
            # We could not load any revisions; fail completely
            status_visit = "partial"
            status_load = "failed"
            return finalize_visit()

        try:
            # Retrieve the default release version (the "latest" one)
            default_version = self.get_default_version()
            logger.debug("default version: %s", default_version)
            # Retrieve extra branches
            extra_branches = self.extra_branches()
            logger.debug("extra branches: %s", extra_branches)

            snapshot = self._load_snapshot(
                default_version, tmp_revisions, extra_branches
            )
            self.storage.flush()
        except Exception as e:
            logger.exception("Failed to build snapshot for origin %s", self.url)
            sentry_sdk.capture_exception(e)
            status_visit = "partial"
            status_load = "failed"

        if snapshot:
            try:
                metadata_objects = self.build_extrinsic_snapshot_metadata(snapshot.id)
                self._load_metadata_objects(metadata_objects)
            except Exception as e:
                logger.exception(
                    "Failed to load extrinsic snapshot metadata for %s", self.url
                )
                sentry_sdk.capture_exception(e)
                status_visit = "partial"
                status_load = "failed"

        try:
            metadata_objects = self.build_extrinsic_origin_metadata()
            self._load_metadata_objects(metadata_objects)
        except Exception as e:
            logger.exception(
                "Failed to load extrinsic origin metadata for %s", self.url
            )
            sentry_sdk.capture_exception(e)
            status_visit = "partial"
            status_load = "failed"

        return finalize_visit()

    def _load_revision(self, p_info: TPackageInfo, origin) -> Optional[Sha1Git]:
        """Does all the loading of a revision itself:

        * downloads a package and uncompresses it
        * loads it from disk
        * adds contents, directories, and revision to self.storage
        * returns (revision_id, loaded)

        Raises
            exception when unable to download or uncompress artifacts

        """
        with tempfile.TemporaryDirectory() as tmpdir:
            dl_artifacts = self.download_package(p_info, tmpdir)

            uncompressed_path = self.uncompress(dl_artifacts, dest=tmpdir)
            logger.debug("uncompressed_path: %s", uncompressed_path)

            directory = from_disk.Directory.from_disk(
                path=uncompressed_path.encode("utf-8"),
                max_content_length=self.max_content_size,
            )

            contents, skipped_contents, directories = from_disk.iter_directory(
                directory
            )

            logger.debug("Number of skipped contents: %s", len(skipped_contents))
            self.storage.skipped_content_add(skipped_contents)
            logger.debug("Number of contents: %s", len(contents))
            self.storage.content_add(contents)

            logger.debug("Number of directories: %s", len(directories))
            self.storage.directory_add(directories)

            # FIXME: This should be release. cf. D409
            revision = self.build_revision(
                p_info, uncompressed_path, directory=directory.hash
            )
            if not revision:
                # Some artifacts are missing intrinsic metadata
                # skipping those
                return None

        extra_metadata: Tuple[str, Any] = (
            "original_artifact",
            [hashes for _, hashes in dl_artifacts],
        )
        if revision.metadata is not None:
            full_metadata = list(revision.metadata.items()) + [extra_metadata]
        else:
            full_metadata = [extra_metadata]

        revision = attr.evolve(revision, metadata=ImmutableDict(full_metadata))

        logger.debug("Revision: %s", revision)

        self.storage.revision_add([revision])
        return revision.id

    def _load_snapshot(
        self,
        default_version: str,
        revisions: Dict[str, List[Tuple[str, bytes]]],
        extra_branches: Dict[bytes, Mapping[str, Any]],
    ) -> Optional[Snapshot]:
        """Build snapshot out of the current revisions stored and extra branches.
           Then load it in the storage.

        """
        logger.debug("revisions: %s", revisions)
        # Build and load the snapshot
        branches = {}  # type: Dict[bytes, Mapping[str, Any]]
        for version, branch_name_revisions in revisions.items():
            if version == default_version and len(branch_name_revisions) == 1:
                # only 1 branch (no ambiguity), we can create an alias
                # branch 'HEAD'
                branch_name, _ = branch_name_revisions[0]
                # except for some corner case (deposit)
                if branch_name != "HEAD":
                    branches[b"HEAD"] = {
                        "target_type": "alias",
                        "target": branch_name.encode("utf-8"),
                    }

            for branch_name, target in branch_name_revisions:
                branches[branch_name.encode("utf-8")] = {
                    "target_type": "revision",
                    "target": target,
                }

        # Deal with extra-branches
        for name, branch_target in extra_branches.items():
            if name in branches:
                logger.error("Extra branch '%s' has been ignored", name)
            else:
                branches[name] = branch_target

        snapshot_data = {"branches": branches}
        logger.debug("snapshot: %s", snapshot_data)
        snapshot = Snapshot.from_dict(snapshot_data)
        logger.debug("snapshot: %s", snapshot)
        self.storage.snapshot_add([snapshot])

        return snapshot

    def get_loader_name(self) -> str:
        """Returns a fully qualified name of this loader."""
        return f"{self.__class__.__module__}.{self.__class__.__name__}"

    def get_loader_version(self) -> str:
        """Returns the version of the current loader."""
        module_name = self.__class__.__module__ or ""
        module_name_parts = module_name.split(".")

        # Iterate rootward through the package hierarchy until we find a parent of this
        # loader's module with a __version__ attribute.
        for prefix_size in range(len(module_name_parts), 0, -1):
            package_name = ".".join(module_name_parts[0:prefix_size])
            module = sys.modules[package_name]
            if hasattr(module, "__version__"):
                return module.__version__  # type: ignore

        # If this loader's class has no parent package with a __version__,
        # it should implement it itself.
        raise NotImplementedError(
            f"Could not dynamically find the version of {self.get_loader_name()}."
        )

    def get_metadata_fetcher(self) -> MetadataFetcher:
        """Returns a MetadataFetcher instance representing this package loader;
        which is used to for adding provenance information to extracted
        extrinsic metadata, if any."""
        return MetadataFetcher(
            name=self.get_loader_name(), version=self.get_loader_version(), metadata={},
        )

    def get_metadata_authority(self) -> MetadataAuthority:
        """For package loaders that get extrinsic metadata, returns the authority
        the metadata are coming from.
        """
        raise NotImplementedError("get_metadata_authority")

    def get_extrinsic_origin_metadata(self) -> List[RawExtrinsicMetadataCore]:
        """Returns metadata items, used by build_extrinsic_origin_metadata."""
        return []

    def build_extrinsic_origin_metadata(self) -> List[RawExtrinsicMetadata]:
        """Builds a list of full RawExtrinsicMetadata objects, using
        metadata returned by get_extrinsic_origin_metadata."""
        metadata_items = self.get_extrinsic_origin_metadata()
        if not metadata_items:
            # If this package loader doesn't write metadata, no need to require
            # an implementation for get_metadata_authority.
            return []

        authority = self.get_metadata_authority()
        fetcher = self.get_metadata_fetcher()

        metadata_objects = []

        for item in metadata_items:
            metadata_objects.append(
                RawExtrinsicMetadata(
                    type=MetadataTargetType.ORIGIN,
                    id=self.url,
                    discovery_date=item.discovery_date or self.visit_date,
                    authority=authority,
                    fetcher=fetcher,
                    format=item.format,
                    metadata=item.metadata,
                )
            )

        return metadata_objects

    def get_extrinsic_snapshot_metadata(self) -> List[RawExtrinsicMetadataCore]:
        """Returns metadata items, used by build_extrinsic_snapshot_metadata."""
        return []

    def build_extrinsic_snapshot_metadata(
        self, snapshot_id: Sha1Git
    ) -> List[RawExtrinsicMetadata]:
        """Builds a list of full RawExtrinsicMetadata objects, using
        metadata returned by get_extrinsic_snapshot_metadata."""
        metadata_items = self.get_extrinsic_snapshot_metadata()
        if not metadata_items:
            # If this package loader doesn't write metadata, no need to require
            # an implementation for get_metadata_authority.
            return []

        authority = self.get_metadata_authority()
        fetcher = self.get_metadata_fetcher()

        metadata_objects = []

        for item in metadata_items:
            metadata_objects.append(
                RawExtrinsicMetadata(
                    type=MetadataTargetType.SNAPSHOT,
                    id=SWHID(object_type="snapshot", object_id=snapshot_id),
                    discovery_date=item.discovery_date or self.visit_date,
                    authority=authority,
                    fetcher=fetcher,
                    format=item.format,
                    metadata=item.metadata,
                    origin=self.url,
                )
            )

        return metadata_objects

    def build_extrinsic_revision_metadata(
        self, p_info: TPackageInfo, revision_id: Sha1Git
    ) -> List[RawExtrinsicMetadata]:
        if not p_info.revision_extrinsic_metadata:
            # If this package loader doesn't write metadata, no need to require
            # an implementation for get_metadata_authority.
            return []

        authority = self.get_metadata_authority()
        fetcher = self.get_metadata_fetcher()

        metadata_objects = []

        for item in p_info.revision_extrinsic_metadata:
            metadata_objects.append(
                RawExtrinsicMetadata(
                    type=MetadataTargetType.REVISION,
                    id=SWHID(object_type="revision", object_id=revision_id),
                    discovery_date=item.discovery_date or self.visit_date,
                    authority=authority,
                    fetcher=fetcher,
                    format=item.format,
                    metadata=item.metadata,
                    origin=self.url,
                )
            )

        return metadata_objects

    def _load_extrinsic_revision_metadata(
        self, p_info: TPackageInfo, revision_id: Sha1Git
    ) -> None:
        metadata_objects = self.build_extrinsic_revision_metadata(p_info, revision_id)
        self._load_metadata_objects(metadata_objects)

    def _load_metadata_objects(
        self, metadata_objects: List[RawExtrinsicMetadata]
    ) -> None:
        if not metadata_objects:
            # If this package loader doesn't write metadata, no need to require
            # an implementation for get_metadata_authority.
            return

        self._create_authorities(mo.authority for mo in metadata_objects)
        self._create_fetchers(mo.fetcher for mo in metadata_objects)

        self.storage.raw_extrinsic_metadata_add(metadata_objects)

    def _create_authorities(self, authorities: Iterable[MetadataAuthority]) -> None:
        deduplicated_authorities = {
            (authority.type, authority.url): authority for authority in authorities
        }
        if authorities:
            self.storage.metadata_authority_add(list(deduplicated_authorities.values()))

    def _create_fetchers(self, fetchers: Iterable[MetadataFetcher]) -> None:
        deduplicated_fetchers = {
            (fetcher.name, fetcher.version): fetcher for fetcher in fetchers
        }
        if fetchers:
            self.storage.metadata_fetcher_add(list(deduplicated_fetchers.values()))
