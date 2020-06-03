# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
import requests

from typing import Dict, Optional, Any, Mapping

from swh.model import hashutil
from swh.model.model import (
    Revision,
    RevisionType,
    TargetType,
    Snapshot,
    BaseModel,
    Sha1Git,
)

from swh.loader.package.utils import EMPTY_AUTHOR
from swh.loader.package.loader import PackageLoader


logger = logging.getLogger(__name__)


class NixGuixLoader(PackageLoader):
    """Load sources from a sources.json file. This loader is used to load
    sources used by functional package manager (eg. Nix and Guix).

    """

    visit_type = "nixguix"

    def __init__(self, url):
        super().__init__(url=url)
        raw = retrieve_sources(url)
        clean = clean_sources(raw)
        self.sources = clean["sources"]
        self.provider_url = url

        self._integrityByUrl = {s["urls"][0]: s["integrity"] for s in self.sources}

        # The revision used to create the sources.json file. For Nix,
        # this revision belongs to the github.com/nixos/nixpkgs
        # repository
        self.revision = clean["revision"]

    # Note: this could be renamed get_artifacts in the PackageLoader
    # base class.
    def get_versions(self):
        """The first mirror of the mirror list is used as branch name in the
        snapshot.

        """
        return self._integrityByUrl.keys()

    # Note: this could be renamed get_artifact_info in the PackageLoader
    # base class.
    def get_package_info(self, url):
        # TODO: try all mirrors and not only the first one. A source
        # can be fetched from several urls, called mirrors. We
        # currently only use the first one, but if the first one
        # fails, we should try the second one and so on.
        integrity = self._integrityByUrl[url]
        yield url, {"url": url, "raw": {"url": url, "integrity": integrity}}

    def known_artifacts(self, snapshot: Optional[Snapshot]) -> Dict[Sha1Git, BaseModel]:
        """Almost same implementation as the default one except it filters out the extra
        "evaluation" branch which does not have the right metadata structure.

        """
        if not snapshot:
            return {}

        # Skip evaluation revision which has no metadata
        revs = [
            rev.target
            for branch_name, rev in snapshot.branches.items()
            if (
                rev
                and rev.target_type == TargetType.REVISION
                and branch_name != b"evaluation"
            )
        ]
        known_revisions = self.storage.revision_get(revs)

        ret = {}
        for revision in known_revisions:
            if not revision:  # revision_get can return None
                continue
            ret[revision["id"]] = revision["metadata"]
        return ret

    def resolve_revision_from(
        self, known_artifacts: Dict, artifact_metadata: Dict
    ) -> Optional[bytes]:
        for rev_id, known_artifact in known_artifacts.items():
            try:
                known_integrity = known_artifact["extrinsic"]["raw"]["integrity"]
            except KeyError as e:
                logger.exception(
                    "Unexpected metadata revision structure detected: %(context)s",
                    {
                        "context": {
                            "revision": hashutil.hash_to_hex(rev_id),
                            "reason": str(e),
                            "known_artifact": known_artifact,
                        }
                    },
                )
                # metadata field for the revision is not as expected by the loader
                # nixguix. We consider this not the right revision and continue checking
                # the other revisions
                continue
            else:
                if artifact_metadata["integrity"] == known_integrity:
                    return rev_id
        return None

    def extra_branches(self) -> Dict[bytes, Mapping[str, Any]]:
        """We add a branch to the snapshot called 'evaluation' pointing to the
        revision used to generate the sources.json file. This revision
        is specified in the sources.json file itself. For the nixpkgs
        origin, this revision is coming from the
        github.com/nixos/nixpkgs repository.

        Note this repository is not loaded explicitly. So, this
        pointer can target a nonexistent revision for a time. However,
        the github and gnu loaders are supposed to load this revision
        and should create the revision pointed by this branch.

        This branch can be used to identify the snapshot associated to
        a Nix/Guix evaluation.

        """
        return {
            b"evaluation": {
                "target_type": "revision",
                "target": hashutil.hash_to_bytes(self.revision),
            }
        }

    def build_revision(
        self, a_metadata: Dict, uncompressed_path: str, directory: Sha1Git
    ) -> Optional[Revision]:
        return Revision(
            type=RevisionType.TAR,
            message=b"",
            author=EMPTY_AUTHOR,
            date=None,
            committer=EMPTY_AUTHOR,
            committer_date=None,
            parents=(),
            directory=directory,
            synthetic=True,
            metadata={
                "extrinsic": {
                    "provider": self.provider_url,
                    "when": self.visit_date.isoformat(),
                    "raw": a_metadata,
                },
            },
        )


def retrieve_sources(url: str) -> Dict[str, Any]:
    response = requests.get(url, allow_redirects=True)
    if response.status_code != 200:
        raise ValueError("Got %d HTTP code on %s", response.status_code, url)

    return json.loads(response.content.decode("utf-8"))


def clean_sources(sources: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and clean the sources structure. First, it ensures all top
    level keys are presents. Then, it walks on the sources list
    and removes sources that don't contain required keys.

    Raises:
      ValueError: if a top level key is missing

    """
    # Required top level keys
    required_keys = ["version", "revision", "sources"]
    missing_keys = []
    for required_key in required_keys:
        if required_key not in sources:
            missing_keys.append(required_key)

    if missing_keys != []:
        raise ValueError(
            "sources structure invalid, missing: %s", ",".join(missing_keys)
        )

    # Only the version 1 is currently supported
    if sources["version"] != 1:
        raise ValueError(
            "The sources structure version '%d' is not supported", sources["version"]
        )

    # If a source doesn't contain required attributes, this source is
    # skipped but others could still be archived.
    verified_sources = []
    for source in sources["sources"]:
        valid = True
        required_keys = ["urls", "integrity", "type"]
        for required_key in required_keys:
            if required_key not in source:
                logger.info(
                    "Skip source '%s' because key '%s' is missing", source, required_key
                )
                valid = False
        if source["type"] != "url":
            logger.info(
                "Skip source '%s' because the type %s is not supported",
                source,
                source["type"],
            )
            valid = False
        if not isinstance(source["urls"], list):
            logger.info(
                "Skip source '%s' because the urls attribute is not a list", source
            )
            valid = False
        if valid:
            verified_sources.append(source)

    sources["sources"] = verified_sources
    return sources
