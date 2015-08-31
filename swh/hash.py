# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib
import binascii


def sha1_bin(hexsha1):
    """Compute the sha1's binary format from an hexadecimal format string.
    """
    return binascii.unhexlify(hexsha1)


def sha1_hex(binsha1):
    """Compute the sha1's binary format from an hexadecimal format string.
    """
    return binascii.hexlify(binsha1)


def hash1(data):
    """Given some data, compute the hash ready object of such data.
    Return the reference object but not the computation.
    """
    sha1 = hashlib.sha1()
    sha1.update(data)
    return sha1


def hash256(data):
    """Given some data, compute the hash ready object of such data.
    Return the reference object but not the computation.
    """
    sha2 = hashlib.sha256()
    sha2.update(data)
    return sha2


def blob_sha1(blob_data):
    """Compute the sha1 of the blob's data.
    blob_data is the blob's data uncompressed.
    """
    return sha1('blob', blob_data)


def sha1(type, data):
    """Compute the sha1 of a data.
    `type` must be git compliant: tree, blob, commit, tag.
    `data` must be uncompressed adequate data for the corresponding type.

    Inspired by pygit2's test utils code.
    https://github.com/libgit2/pygit2/blob/74b81bf18076555fb12369d5f20e4282214116d3/test/utils.py#L50-L56
    http://stackoverflow.com/questions/552659/assigning-git-sha1s-without-git
    """
    git_format_data = ('%s %d\0%s' % (type, len(data), data)).encode()

    return hash1(git_format_data)
