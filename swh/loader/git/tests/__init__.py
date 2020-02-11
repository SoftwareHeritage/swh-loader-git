# Copyright (C) 2018-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

TEST_LOADER_CONFIG = {
    'storage': {
        'cls': 'pipeline',
        'steps': [
            {
                'cls': 'filter'
            },
            {
                'cls': 'buffer',
                'min_batch_size': {
                    'content': 10,
                    'content_bytes': 100 * 1024 * 1024,
                    'directory': 10,
                    'revision': 10,
                    'release': 10,
                },
            },
            {
                'cls': 'validate',
            },
            {
                'cls': 'memory'
            },
        ]
    },
    'max_content_size': 100 * 1024 * 1024,
    'pack_size_bytes': 4 * 1024 * 1024 * 1024,
    'save_data': False,
}
