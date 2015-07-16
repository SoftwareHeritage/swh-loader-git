# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


def make(type, obj):
    """Create an swh object.
    """
    return {'type': type, 'raw': obj}


def get_type(obj):
    """Retrieve the type.
    """
    return obj['type']


def get_obj(obj):
    """Retrieve the raw obj.
    """
    return obj['raw']

# Map of sha1s
class SWHMap():
    def __init__(self):
        self.sha1s_hex = []
        self.sha1s_map = {}

    def add(self, obj, sha1=None):
        sha1 = sha1 if sha1 is not None else get_obj(obj).hex
        self.sha1s_hex.append(sha1)
        self.sha1s_map[sha1] = obj

    def get_all_sha1s(self):
        return self.sha1s_hex

    def get_sha1(self, sha1):
        return self.sha1s_map.get(sha1)
