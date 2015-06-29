# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import gzip

from retrying import retry
from swh.retry import policy


def folder_path(prefix_dir, hexhash, depth=4):
    """Compute the folder prefix from a hexhash key.
       The depth determines the number of subfolder from prefix_dir.
       Default to 4.
       Example:
       - prefix_dir: /some/path
       - depth:      2
       - hash:       aabbccddeeffgghhii
       -> folder:    /some/path/aa/bb/
    """
    hexhashes = [hexhash[x:x+2] for x in range(0, 2*depth, 2)]
    return os.path.join(prefix_dir, *hexhashes)


@retry(retry_on_exception=policy.retry_if_io_error, wrap_exception=True)
def write_data(data, path, comp_flag=None):
    """Write data (expected string) to path.
       If compress_path is not None, gzip the data.

       If an IOError is raised, this function will be triggered immediately
       again.
       Otherwise, if any other error is raised, the error will be wrapped in
       RetryError.
    """
    with (gzip.open(path, 'wb') if comp_flag else open(path, 'wb')) as f:
        return f.write(data.encode('utf-8'))
