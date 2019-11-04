# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from typing import Any, Dict, Generator, Mapping, Sequence, Tuple

from swh.model.hashutil import hash_to_hex
from swh.loader.package.loader import PackageLoader
from swh.deposit.client import PrivateApiDepositClient as ApiClient


logger = logging.getLogger(__name__)


class DepositLoader(PackageLoader):
    """Load pypi origin's artifact releases into swh archive.

    """
    visit_type = 'deposit'

    def __init__(self, url: str, deposit_id: str):
        """Constructor

        Args:
            url: Origin url to associate the artifacts/metadata to
            deposit_id: Deposit identity

        """
        super().__init__(url=url)

        # For now build back existing api urls
        # archive_url: Private api url to retrieve archive artifact
        self.archive_url = '/%s/raw/' % deposit_id
        # metadata_url: Private api url to retrieve the deposit metadata
        self.metadata_url = '/%s/meta/' % deposit_id
        # deposit_update_url: Private api to push pids and status update on the
        #     deposit id
        self.deposit_update_url = '/%s/update/' % deposit_id
        self.client = ApiClient()
        self._metadata = None

    @property
    def metadata(self):
        if self._metadata is None:
            self._metadata = self.client.metadata_get(self.metadata_url)
        return self._metadata

    def get_versions(self) -> Sequence[str]:
        # only 1 branch 'HEAD' with no alias since we only have 1 snapshot
        # branch
        return ['HEAD']

    def get_package_info(self, version: str) -> Generator[
            Tuple[str, Mapping[str, Any]], None, None]:
        p_info = {
            'url': self.client.base_url + self.archive_url,
            'filename': 'archive.zip',
            'raw': self.metadata,
        }
        yield 'HEAD', p_info

    def build_revision(
            self, a_metadata: Dict, uncompressed_path: str) -> Dict:
        revision = a_metadata.pop('revision')
        metadata = {
            'extrinsic': {
                'provider': '%s/%s' % (
                    self.client.base_url, self.metadata_url),
                'when': self.visit_date.isoformat(),
                'raw': a_metadata,
            },
        }

        # FIXME: the deposit no longer needs to build the revision
        revision['metadata'].update(metadata)
        revision['author'] = parse_author(revision['author'])
        revision['committer'] = parse_author(revision['committer'])
        revision['message'] = revision['message'].encode('utf-8')
        revision['type'] = 'tar'

        return revision

    def load(self) -> Dict:
        # Usual loading
        r = super().load()
        success = r['status'] != 'failed'

        if success:
            # Update archive with metadata information
            origin_metadata = self.metadata['origin_metadata']

            logger.debug('origin_metadata: %s', origin_metadata)
            tools = self.storage.tool_add([origin_metadata['tool']])
            logger.debug('tools: %s', tools)
            tool_id = tools[0]['id']

            provider = origin_metadata['provider']
            # FIXME: Shall we delete this info?
            provider_id = self.storage.metadata_provider_add(
                provider['provider_name'],
                provider['provider_type'],
                provider['provider_url'],
                metadata=None)

            metadata = origin_metadata['metadata']
            self.storage.origin_metadata_add(
                self.url, self.visit_date, provider_id, tool_id, metadata)

        # Update deposit status
        try:
            if not success:
                self.client.status_update(
                    self.deposit_update_url, status='failed')
                return r

            snapshot_id = r['snapshot_id']
            branches = self.storage.snapshot_get(snapshot_id)['branches']
            logger.debug('branches: %s', branches)
            if not branches:
                return r
            rev_id = branches[b'HEAD']['target']

            revision = next(self.storage.revision_get([rev_id]))

            # Retrieve the revision identifier
            dir_id = revision['directory']

            # update the deposit's status to success with its
            # revision-id and directory-id
            self.client.status_update(
                self.deposit_update_url,
                status='done',
                revision_id=hash_to_hex(rev_id),
                directory_id=hash_to_hex(dir_id),
                origin_url=self.url)
        except Exception:
            logger.exception(
                'Problem when trying to update the deposit\'s status')
            return {'status': 'failed'}
        return r


def parse_author(author):
    """See prior fixme

    """
    return {
        'fullname': author['fullname'].encode('utf-8'),
        'name': author['name'].encode('utf-8'),
        'email': author['email'].encode('utf-8'),
    }
