# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Generator, Dict, Tuple, Sequence

from swh.loader.package.loader import PackageLoader
from swh.deposit.client import PrivateApiDepositClient as ApiClient


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

    def get_versions(self) -> Sequence[str]:
        # only 1 branch 'HEAD' with no alias since we only have 1 snapshot
        # branch
        return ['HEAD']

    def get_artifacts(self, version: str) -> Generator[
            Tuple[str, str, Dict], None, None]:
        meta = self.client.metadata_get(self.metadata_url)
        filename = 'archive.zip'  # do not care about it here
        url = self.client.base_url + self.archive_url
        yield filename, url, meta

    def build_revision(
            self, a_metadata: Dict, a_uncompressed_path: str,
            visit_date: str) -> Dict:
        revision = a_metadata.pop('revision')
        metadata = {
            'extrinsic': {
                'provider': '%s/%s' % (
                    self.client.base_url, self.metadata_url),
                'when': visit_date,
                'raw': a_metadata,
            },
        }

        # FIXME: the deposit no longer needs to build the revision
        revision['metadata'].update(metadata)
        revision['author'] = parse_author(revision['author'])
        revision['committer'] = parse_author(revision['committer'])
        revision['message'] = revision['message'].encode('utf-8')

        return revision


def parse_author(author):
    """See prior fixme

    """
    return {
        'fullname': author['fullname'].encode('utf-8'),
        'name': author['name'].encode('utf-8'),
        'email': author['email'].encode('utf-8'),
    }
