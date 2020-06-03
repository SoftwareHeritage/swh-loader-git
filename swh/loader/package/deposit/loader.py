# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
import requests
import types

from typing import Any, Dict, Generator, List, Mapping, Optional, Sequence, Tuple, Union

from swh.model.hashutil import hash_to_hex, hash_to_bytes
from swh.model.model import (
    Person,
    Revision,
    RevisionType,
    TimestampWithTimezone,
    Sha1Git,
)
from swh.loader.package.loader import PackageLoader
from swh.loader.package.utils import download


logger = logging.getLogger(__name__)


class DepositLoader(PackageLoader):
    """Load pypi origin's artifact releases into swh archive.

    """

    visit_type = "deposit"

    def __init__(self, url: str, deposit_id: str):
        """Constructor

        Args:
            url: Origin url to associate the artifacts/metadata to
            deposit_id: Deposit identity

        """
        super().__init__(url=url)

        config_deposit = self.config["deposit"]
        self.deposit_id = deposit_id
        self.client = ApiClient(url=config_deposit["url"], auth=config_deposit["auth"])
        self.metadata: Dict[str, Any] = {}

    def get_versions(self) -> Sequence[str]:
        # only 1 branch 'HEAD' with no alias since we only have 1 snapshot
        # branch
        return ["HEAD"]

    def get_package_info(
        self, version: str
    ) -> Generator[Tuple[str, Mapping[str, Any]], None, None]:
        p_info = {
            "filename": "archive.zip",
            "raw": self.metadata,
        }
        yield "HEAD", p_info

    def download_package(
        self, p_info: Mapping[str, Any], tmpdir: str
    ) -> List[Tuple[str, Mapping]]:
        """Override to allow use of the dedicated deposit client

        """
        return [self.client.archive_get(self.deposit_id, tmpdir, p_info["filename"])]

    def build_revision(
        self, a_metadata: Dict, uncompressed_path: str, directory: Sha1Git
    ) -> Optional[Revision]:
        depo = a_metadata.pop("deposit")

        # Note:
        # `date` and `committer_date` are always transmitted by the deposit read api
        # which computes itself the values. The loader needs to use those to create the
        # revision.

        # date: codemeta:dateCreated if any, deposit completed_date otherwise
        date = TimestampWithTimezone.from_dict(depo["author_date"])
        # commit_date: codemeta:datePublished if any, deposit completed_date otherwise
        commit_date = TimestampWithTimezone.from_dict(depo["committer_date"])

        client, id, collection = [depo[k] for k in ["client", "id", "collection"]]
        message = f"{client}: Deposit {id} in collection {collection}".encode("utf-8")

        author = parse_author(depo["author"])
        committer = parse_author(depo["committer"])

        return Revision(
            type=RevisionType.TAR,
            message=message,
            author=author,
            date=date,
            committer=committer,
            committer_date=commit_date,
            parents=tuple([hash_to_bytes(p) for p in depo["revision_parents"]]),
            directory=directory,
            synthetic=True,
            metadata={
                "extrinsic": {
                    "provider": self.client.metadata_url(self.deposit_id),
                    "when": self.visit_date.isoformat(),
                    "raw": a_metadata,
                },
            },
        )

    def load(self) -> Dict:
        # First making sure the deposit is known prior to trigger a loading
        try:
            self.metadata = self.client.metadata_get(self.deposit_id)
        except ValueError:
            logger.error(f"Unknown deposit {self.deposit_id}, ignoring")
            return {"status": "failed"}
        # Then usual loading
        r = super().load()
        success = r["status"] != "failed"

        if success:
            # Update archive with metadata information
            origin_metadata = self.metadata["origin_metadata"]
            logger.debug("origin_metadata: %s", origin_metadata)

            provider = origin_metadata["provider"]
            authority = {
                "type": provider["provider_type"],
                "url": provider["provider_url"],
                "metadata": {
                    "name": provider["provider_name"],
                    **(provider["metadata"] or {}),
                },
            }
            self.storage.metadata_authority_add(**authority)

            tool = origin_metadata["tool"]
            fetcher = {
                "name": tool["name"],
                "version": tool["version"],
                "metadata": tool["configuration"],
            }
            self.storage.metadata_fetcher_add(**fetcher)

            metadata = origin_metadata["metadata"]
            format = "sword-v2-atom-codemeta-v2-in-json"
            self.storage.origin_metadata_add(
                self.url,
                self.visit_date,
                {"type": authority["type"], "url": authority["url"]},
                {"name": fetcher["name"], "version": fetcher["version"]},
                format,
                json.dumps(metadata).encode(),
            )

        # Update deposit status
        try:
            if not success:
                self.client.status_update(self.deposit_id, status="failed")
                return r

            snapshot_id = hash_to_bytes(r["snapshot_id"])
            branches = self.storage.snapshot_get(snapshot_id)["branches"]
            logger.debug("branches: %s", branches)
            if not branches:
                return r
            rev_id = branches[b"HEAD"]["target"]

            revisions = self.storage.revision_get([rev_id])
            # FIXME: inconsistency between tests and production code
            if isinstance(revisions, types.GeneratorType):
                revisions = list(revisions)
            revision = revisions[0]

            # Retrieve the revision identifier
            dir_id = revision["directory"]

            # update the deposit's status to success with its
            # revision-id and directory-id
            self.client.status_update(
                self.deposit_id,
                status="done",
                revision_id=hash_to_hex(rev_id),
                directory_id=hash_to_hex(dir_id),
                snapshot_id=r["snapshot_id"],
                origin_url=self.url,
            )
        except Exception:
            logger.exception("Problem when trying to update the deposit's status")
            return {"status": "failed"}
        return r


def parse_author(author) -> Person:
    """See prior fixme

    """
    return Person(
        fullname=author["fullname"].encode("utf-8"),
        name=author["name"].encode("utf-8"),
        email=author["email"].encode("utf-8"),
    )


class ApiClient:
    """Private Deposit Api client

    """

    def __init__(self, url, auth: Optional[Mapping[str, str]]):
        self.base_url = url.rstrip("/")
        self.auth = None if not auth else (auth["username"], auth["password"])

    def do(self, method: str, url: str, *args, **kwargs):
        """Internal method to deal with requests, possibly with basic http
           authentication.

        Args:
            method (str): supported http methods as in get/post/put

        Returns:
            The request's execution output

        """
        method_fn = getattr(requests, method)
        if self.auth:
            kwargs["auth"] = self.auth
        return method_fn(url, *args, **kwargs)

    def archive_get(
        self, deposit_id: Union[int, str], tmpdir: str, filename: str
    ) -> Tuple[str, Dict]:
        """Retrieve deposit's archive artifact locally

        """
        url = f"{self.base_url}/{deposit_id}/raw/"
        return download(url, dest=tmpdir, filename=filename, auth=self.auth)

    def metadata_url(self, deposit_id: Union[int, str]) -> str:
        return f"{self.base_url}/{deposit_id}/meta/"

    def metadata_get(self, deposit_id: Union[int, str]) -> Dict[str, Any]:
        """Retrieve deposit's metadata artifact as json

        """
        url = self.metadata_url(deposit_id)
        r = self.do("get", url)
        if r.ok:
            return r.json()

        msg = f"Problem when retrieving deposit metadata at {url}"
        logger.error(msg)
        raise ValueError(msg)

    def status_update(
        self,
        deposit_id: Union[int, str],
        status: str,
        revision_id: Optional[str] = None,
        directory_id: Optional[str] = None,
        snapshot_id: Optional[str] = None,
        origin_url: Optional[str] = None,
    ):
        """Update deposit's information including status, and persistent
           identifiers result of the loading.

        """
        url = f"{self.base_url}/{deposit_id}/update/"
        payload = {"status": status}
        if revision_id:
            payload["revision_id"] = revision_id
        if directory_id:
            payload["directory_id"] = directory_id
        if snapshot_id:
            payload["snapshot_id"] = snapshot_id
        if origin_url:
            payload["origin_url"] = origin_url

        self.do("put", url, json=payload)
