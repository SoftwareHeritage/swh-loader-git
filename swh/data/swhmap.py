# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

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
        self.visited = set()

    def add_origin(self, origin):
        self.origin = origin

    def get_origin(self):
        return self.origin

    def add_release(self, release):
        self.releases.append(release)

    def get_releases(self):
        return self.releases

    def add_occurrence(self, occurrence):
        self.occurrences.append(occurrence)

    def get_occurrences(self):
        return self.occurrences

    def add_content(self, content_ref):
        sha1 = content_ref['sha1']
        self.contents.add(sha1, content_ref)
        self.visited.add(sha1)

    def get_contents(self):
        return self.contents

    def add_directory(self, directory):
        sha1 = directory['sha1']
        self.directories.add(sha1, directory)
        self.visited.add(sha1)

    def get_directories(self):
        return self.directories

    def add_revision(self, revision):
        sha1 = revision['sha1']
        self.revisions.add(sha1, revision)
        self.visited.add(sha1)

    def already_visited(self, sha1):
        return sha1 in self.visited

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
