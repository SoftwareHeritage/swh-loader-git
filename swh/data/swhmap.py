# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pygit2

from datetime import datetime

from swh import hash


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
    """Data structure that ensures easy access to current keys.
    """
    def __init__(self):
        self.sha1s_hex = []
        self.sha1s_map = {}

    def add(self, sha1, obj):
        """Add obj with type obj_type and sha1.
        """
        self.sha1s_hex.append(sha1)
        self.sha1s_map[sha1] = obj

    def keys(self):
        return self.sha1s_hex

    def objects(self):
        return self.sha1s_map

    def __str__(self):
        return """SWHMap({
sha1s: %s,
map: %s})""" % (self.sha1s_hex.__str__(), self.sha1s_map.__str__())


class SWHRepo():
    """Structure with:
    - sha1s as list
    - swh objects map (indexed by sha1)
    """
    def __init__(self):
        self.origin = {}
        self.releases = []
        self.occurrences = []
        self.contents = SWHMap()
        self.directories = SWHMap()
        self.revisions = SWHMap()

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

    def add_content(self, content_ref):
        self.contents.add(content_ref['sha1'], content_ref)

    def get_contents(self):
        return self.contents

    def add_directory(self, directory):
        self.directories.add(directory['sha1'], directory)

    def get_directories(self):
        return self.directories

    def add_revision(self, revision):
        self.revisions.add(revision['sha1'], revision)

    def get_revisions(self):
        return self.revisions

    def __str__(self):
        return """SWHRepo({
origin: %s,
occurrences: %s,
releases: %s,
contents: %s,
directories: %s,
revisions: %s,
})""" % (self.origin,
         self.occurrences,
         self.releases,
         self.contents.__str__(),
         self.directories.__str__(),
         self.revisions.__str__())
