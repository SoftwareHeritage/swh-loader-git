# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os


from swh import file


def create_dir_from_hash(file_content_storage_dir, hashv):
    """Create directory in folder file_content_storage_dir from a given hash value.
    """
    folder_in_storage = file.folder_path(file_content_storage_dir, hashv)
    os.makedirs(folder_in_storage, exist_ok=True)
    return folder_in_storage


def add_blob_to_storage(db_conn, file_content_storage_dir, data, hashkey):
    """Add blob in the file content storage (on disk).

    """
    folder_in_storage = create_dir_from_hash(file_content_storage_dir, hashkey)
    filepath = os.path.join(folder_in_storage, hashkey)
    logging.debug('Injecting blob %s in file content storage.' % filepath)
    file.write_data(data, filepath)
