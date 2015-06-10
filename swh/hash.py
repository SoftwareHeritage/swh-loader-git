# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib
import binascii


def sha1_bin(hexsha1):
    """Compute the sha1's binary format from an hexadecimal format string.
    """
    return binascii.unhexlify(hexsha1)


def hashkey_sha1(data):
    """Given some data, compute the hash ready object of such data.
    Return the reference but not the computation.
    """
    sha1 = hashlib.sha1()
    sha1.update(data)
    return sha1
