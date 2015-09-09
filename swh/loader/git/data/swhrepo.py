# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

class SWHRepo():
    """Structure with:
       - sha1s as list
       - map indexed by sha1
    """
    def __init__(self, visited=set()):
        self.origin = {}
        self.releases = []
        self.occurrences = []
        self.contents = {}
        self.directories = {}
        self.revisions = {}
        self.persons = {}
        self.visited = visited

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
        sha1 = content_ref['id']
        self.contents[sha1] = content_ref
        self.visited.add(sha1)

    def get_contents(self):
        return self.contents

    def add_directory(self, directory):
        sha1 = directory['id']
        self.directories[sha1] = directory
        self.visited.add(sha1)

    def get_directories(self):
        return self.directories

    def add_revision(self, revision):
        sha1 = revision['id']
        self.revisions[sha1] = revision
        self.visited.add(sha1)

    def add_person(self, id, person):
        self.persons[id] = person

    def get_persons(self):
        return self.persons.values()

    def already_visited(self, sha1):
        return sha1 in self.visited

    def get_revisions(self):
        return self.revisions
