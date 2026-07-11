# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Fetch-to-pack and load-from-pack helpers for mass ingestion.

Consolidates the github-ingestion pipeline's download-side fetch (formerly
``custom_swh_components/batch_git_loader/gix_fetch.py``) and its ingest-side
loader (formerly ``BatchGixLoader``) onto the upstream gix ``GitLoader``, so
the pipeline holds no git logic:

- :func:`save_pack` fetches a repository's complete, non-thin pack by REUSING
  the loader's own :meth:`GitLoader.fetch_pack_from_origin` (so the dulwich
  ``file://`` / dumb-HTTP fallbacks come for free) and returns the captured
  refs as a :class:`PackRefs`.
- :class:`GitLoaderFromPack` ingests a local pack + :class:`PackRefs` with no
  network, for the compute-node (two-phase) or streaming ingest.
- :class:`PackRefs` is the single (de)serializer for the refs that travel
  with each pack (e.g. in the pipeline's ``manifest.json``).

Non-thin packs are required because the downstream sink (SWH ``OrcStorage`` in
the mass pipeline) cannot resolve out-of-pack delta bases: the Rust
``PackReader`` resolves deltas only within the pack.  ``incremental=False``
gives an empty ``haves`` set, hence a complete pack.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional

from swh.storage import get_storage

from . import utils
from .loader import FetchPackReturn, GitLoader, RepoRepresentation

__all__ = ["PackRefs", "save_pack", "GitLoaderFromPack"]

_PULL_REF_PREFIXES = (b"refs/pull/", b"refs/merge-requests/")


def _is_pull_ref(name: bytes) -> bool:
    """True for forge pull-/merge-request refs (GitHub ``refs/pull/*``,
    GitLab ``refs/merge-requests/*``)."""
    return name.startswith(_PULL_REF_PREFIXES)


@dataclass
class PackRefs:
    """Refs captured for one fetched repository, plus provenance.

    ``remote_refs`` maps refname (bytes) -> target object id as 40-byte hex
    (bytes); ``symbolic_refs`` maps symbolic name (bytes) -> the refname it
    points at (bytes), e.g. ``{b"HEAD": b"refs/heads/main"}``.  The JSON form
    encodes refnames as latin-1 (lossless for arbitrary bytes) and object ids
    as ascii hex — the single canonical serialization for the manifest.
    """

    remote_refs: Dict[bytes, bytes]
    symbolic_refs: Dict[bytes, bytes] = field(default_factory=dict)
    origin_url: str = ""
    visit_date: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "origin_url": self.origin_url,
            "visit_date": (self.visit_date.isoformat() if self.visit_date else None),
            "refs": {
                n.decode("latin-1"): s.decode("ascii")
                for n, s in self.remote_refs.items()
            },
            "symbolic_refs": {
                n.decode("latin-1"): t.decode("latin-1")
                for n, t in self.symbolic_refs.items()
            },
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "PackRefs":
        vd = d.get("visit_date")
        return cls(
            remote_refs={
                k.encode("latin-1"): v.encode("ascii")
                for k, v in (d.get("refs") or {}).items()
            },
            symbolic_refs={
                k.encode("latin-1"): v.encode("latin-1")
                for k, v in (d.get("symbolic_refs") or {}).items()
            },
            origin_url=str(d.get("origin_url", "")),
            visit_date=datetime.fromisoformat(vd) if vd else None,
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, s: str) -> "PackRefs":
        return cls.from_dict(json.loads(s))


class _PrDroppingRepoRepresentation(RepoRepresentation):
    """Base repo whose ``determine_wants`` also drops forge PR/MR refs, so
    objects reachable only from ``refs/pull/*`` / ``refs/merge-requests/*``
    are never requested (transfer-level skip, not just output filtering)."""

    def determine_wants(
        self, refs: Mapping[bytes, bytes], depth: Optional[int] = None
    ) -> List[bytes]:
        kept = {n: t for n, t in refs.items() if not _is_pull_ref(n)}
        return super().determine_wants(kept, depth)


def save_pack(
    url: str,
    dest: str,
    *,
    drop_forge_pr_refs: bool = False,
    size_limit: int = 0,
    connect_timeout: float = 120,
    read_timeout: float = 600,
) -> PackRefs:
    """Fetch ``url``'s complete non-thin pack to ``dest``; return its refs.

    Reuses :meth:`GitLoader.fetch_pack_from_origin` with ``incremental=False``
    (=> ``haves=[]`` => a complete, non-thin pack).  No storage reads occur;
    the memory storage only exists to construct the loader.  With
    ``drop_forge_pr_refs`` set, PR/MR refs are dropped from the fetch *wants*
    (via :class:`_PrDroppingRepoRepresentation`) AND from the returned carrier
    refs, so those objects are neither transferred nor recorded.

    Note: an empty repository (no advertised refs) is not special-cased here;
    the upstream MR adds that guard to ``fetch_pack_from_origin`` itself.
    """
    # gix reads ``size_limit=0`` as "unlimited", but the dulwich fallback
    # (file:// and dumb-HTTP origins) reads 0 as a literal 0-byte cap and
    # rejects every pack.  Translate an unlimited request to a very large
    # finite cap that BOTH paths accept (no real repo approaches 1 PiB).
    effective_limit = size_limit if size_limit > 0 else (1 << 50)
    storage = get_storage(cls="memory")
    loader = GitLoader(
        storage=storage,
        url=url,
        incremental=False,
        pack_size_bytes=effective_limit,
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
    )
    rr_cls = _PrDroppingRepoRepresentation if drop_forge_pr_refs else RepoRepresentation
    base_repo = rr_cls(storage, base_snapshots=[], incremental=False)

    fetched: FetchPackReturn = loader.fetch_pack_from_origin(
        url, base_repo, lambda _msg: None
    )
    shutil.copyfile(fetched.pack_path, dest)

    remote_refs = fetched.remote_refs
    symbolic_refs = fetched.symbolic_refs
    if drop_forge_pr_refs:
        remote_refs = {n: s for n, s in remote_refs.items() if not _is_pull_ref(n)}
        symbolic_refs = {
            n: t
            for n, t in symbolic_refs.items()
            if not _is_pull_ref(n) and not _is_pull_ref(t)
        }
    return PackRefs(
        remote_refs=remote_refs,
        symbolic_refs=symbolic_refs,
        origin_url=url,
        visit_date=datetime.now(timezone.utc),
    )


class GitLoaderFromPack(GitLoader):
    """Ingest one repository from a local pack + captured :class:`PackRefs`.

    The upstream, reusable form of the pipeline's former ``BatchGixLoader``:
    overrides :meth:`fetch_pack_from_origin` to serve the local pack and the
    pre-captured refs, so no network access occurs.  Keeps
    ``incremental=False`` (the sink may be write-only / read-absent, e.g.
    ``OrcStorage``).
    """

    def __init__(
        self,
        storage: Any,
        url: str,
        pack_path: str,
        refs: PackRefs,
        *,
        visit_date: Optional[datetime] = None,
        **kwargs: Any,
    ) -> None:
        kwargs["incremental"] = False
        super().__init__(storage=storage, url=url, **kwargs)
        self._local_pack_path = pack_path
        self._local_refs = refs
        vd = visit_date or refs.visit_date
        if vd is not None:
            self.visit_date = vd

    def fetch_pack_from_origin(
        self,
        origin_url: str,
        base_repo: RepoRepresentation,
        do_activity: Any,
    ) -> FetchPackReturn:
        if not os.path.exists(self._local_pack_path):
            raise FileNotFoundError(f"Packfile not found: {self._local_pack_path}")
        # Refs were already filtered at fetch time; filter_* is idempotent.
        return FetchPackReturn(
            remote_refs=utils.filter_refs(self._local_refs.remote_refs),
            symbolic_refs=utils.filter_symbolic_refs(self._local_refs.symbolic_refs),
            pack_path=self._local_pack_path,
            pack_size=os.path.getsize(self._local_pack_path),
        )
