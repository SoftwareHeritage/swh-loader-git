# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import os.path

RESOURCES_PATH = os.path.join(os.path.dirname(__file__), 'resources')

package = '8sync'

package_url = 'https://ftp.gnu.org/gnu/8sync/'

tarball = [{'date': '944729610',
            'archive': 'https://ftp.gnu.org/gnu/8sync/8sync-0.1.0.tar.gz'}]


def init_test_data(mock_tarball_request):
    """Initialize the loader with the mock of the tarballs

    """
    for version in tarball:
        tarball_url = version['archive']
        tarball_filename = tarball_url.split('/')[-1]
        tarball_filepath = os.path.join(RESOURCES_PATH, 'tarballs',
                                        tarball_filename)
        with open(tarball_filepath, mode='rb') as tarball_file:
            tarball_content = tarball_file.read()
            mock_tarball_request.get(
                tarball_url, content=tarball_content,
                headers={'content-length': str(len(tarball_content))})
