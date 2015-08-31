#!/usr/bin/env python3
# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file_or_handle at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file_or_handle for more information

import pickle
from io import BytesIO

MIMETYPE="application/octet-stream"


def load(file_or_handle):
    """Read a pickled object from the opened file_or_handle object.
    """
    return pickle.load(file_or_handle)


def loads(obj):
    """Read a pickled object from bytes object.
    """
    if obj == b'':
        return obj
    return pickle.loads(obj)


def dumps(obj):
     """Return the pickle representation of the obj.
     """
     return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def dumps_as_stream(obj):
    """Return the pickle representation of the obj as stream.
    """
    return pickle.dump(obj, BytesIO(), protocol=pickle.HIGHEST_PROTOCOL)
