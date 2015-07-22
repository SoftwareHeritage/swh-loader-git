# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# Wrapper of pygit2 object
class SWHObj():
    def __init__(self, obj_type, obj):
        """"""
        self.obj_type = obj_type
        self.obj = obj

    def type(self):
        """Return the current obj's swh type.
        """
        return self.obj_type

    def read_raw(self):
        """Return the wrapped pygit2 object.
        """
        return self.obj.read_raw()

    def sha1(self):
       """Return the current object's sha1.
       """
       return self.obj.hex

    def size(self):
       """Return the current object's size.
       """
       return self.obj.size

    def data(self):
        """Return the current object's data."""
        return self.obj.data


# Structure with:
# - list of sha1s
# - swh objects map (indexed by sha1)
class SWHMap():
    def __init__(self):
        self.sha1s_hex = []
        self.sha1s_map = {}

    def add(self, obj_type, obj, sha1=None):
        """Add obj with type obj_type.
           If sha1 is specified, use it otherwise takes the obj's sha1.
        """
        sha1 = sha1 if sha1 is not None else obj.hex
        self.sha1s_hex.append(sha1)
        self.sha1s_map[sha1] = SWHObj(obj_type, obj)

    def get_all_sha1s(self):
        """Return the current list of sha1s.
        """
        return self.sha1s_hex

    def get_obj(self, sha1):
        """Return the detailed object with sha1.
        """
        return self.sha1s_map.get(sha1)
