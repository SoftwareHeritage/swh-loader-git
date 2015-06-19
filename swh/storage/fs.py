# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os


from swh import file


def create_dir_from_hash(file_content_storage_dir, hashv, folder_depth):
    """Create directory in folder file_content_storage_dir from a given hash value.
    """
    folder_in_storage = file.folder_path(file_content_storage_dir, hashv,
                                         folder_depth)
    os.makedirs(folder_in_storage, exist_ok=True)
    return folder_in_storage


def write_object(dataset_dir, hashv, data, folder_depth, compress_flag=None):
    """Write object with data and hashv on disk in dataset_dir.
    """
    folder_in_storage = create_dir_from_hash(dataset_dir, hashv, folder_depth)
    filepath = os.path.join(folder_in_storage, hashv)
    logging.debug('write object %s' % filepath)
    file.write_data(data, filepath, compress_flag)
