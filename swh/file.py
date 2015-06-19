# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import gzip


def folder_path(prefix_dir, hexhash, depth=4):
    """Compute the folder prefix from a hexhash key.
    The depth determines the number of subfolder from prefix_dir.
    Default to 4.
    """
    hexhashes = [hexhash[x:x+2] for x in range(0, 2*depth, 2)]
    return os.path.join(prefix_dir, *hexhashes)


def write_data(data, path, comp_flag=None):
    """Write data to path.
    If compress_path is not None, gzip the data.
    """
    with (gzip.open(path, 'wb') if comp_flag else open(path, 'wb')) as f:
        f.write(bytes(data, 'UTF-8'))
