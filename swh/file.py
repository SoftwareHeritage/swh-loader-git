# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import gzip


def folder_path(prefix_dir, hash):
    """Compute the folder prefix from a hash key.
    """
    # FIXME: find some split function
    return os.path.join(prefix_dir,
                        hash[0:2],
                        hash[2:4],
                        hash[4:6],
                        hash[6:8])


def write_data(data, path, comp_flag=None):
    """Write data to path.
    If compress_path is not None, gzip the data.
    """
    with (gzip.open(path, 'wb') if comp_flag else open(path, 'wb')) as f:
        f.write(data)
