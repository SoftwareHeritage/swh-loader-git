# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from datetime import datetime


# Wrapper of pygit2 object
class SWHObj():
    """Wrapper object around pygit2.
    """
    def __init__(self, obj_type, obj):
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

    def __str__(self):
        return """SWHObj({
type: %s,
sha1: %s,
})""" % (self.type(),
         self.sha1())


class SWHMap():
    """Structure with:
    - sha1s as list (FIXME: set)
    - swh objects map (indexed by sha1)
    """
    def __init__(self):
        self.sha1s_hex = []
        self.sha1s_map = {}
        self.origin = {}
        self.releases = []
        self.occurrences = []

    def add_origin(self, type, url):
        self.origin = {'type': type,
                       'url': url}

    def get_origin(self):
        return self.origin

    def add_release(self, revision, name):
        self.releases.append({'revision': revision,
                              'name': name,
                              'date': datetime.utcnow()})

    def get_releases(self):
        return self.releases

    def add_occurrence(self, revision, name):
        self.occurrences.append({'revision': revision,
                                 'reference': name,
                                 'date': datetime.utcnow()})

    def get_occurrences(self):
        return self.occurrences

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

    def __str__(self):
        return """SWHMap({
origin: %s,
occurrences: %s,
releases: %s,
map: %s
})""" % (self.origin,
         self.occurrences,
         self.releases,
         self.sha1s_map)
