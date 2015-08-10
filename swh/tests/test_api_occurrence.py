# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.storage import db, models
from swh.protocols import serial
from test_utils import now, app_client


@attr('slow')
class OccurrenceTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

        with db.connect(db_url) as db_conn:
            self.directory_sha1_hex = 'directory-sha16ee476a8be155ab049994f717e'
            models.add_directory(db_conn, self.directory_sha1_hex)

            self.revision_sha1_hex = 'revision-sha1-to-test-existence9994f717e'
            models.add_revision(db_conn,
                                self.revision_sha1_hex,
                                now(),
                                self.directory_sha1_hex,
                                "revision message",
                                "ardumont",
                                "ardumont")

            self.origin_url = "https://github.com/user/repo"
            models.add_origin(db_conn, self.origin_url, 'git')

            models.add_occurrence(db_conn,
                                 self.origin_url,
                                 'master',
                                 self.revision_sha1_hex)

            self.revision_sha1_hex_2 = '2-revision-sha1-to-test-existence9994f71'
            models.add_revision(db_conn,
                                self.revision_sha1_hex_2,
                                now(),
                                self.directory_sha1_hex,
                                "revision message 2",
                                "ardumont",
                                "ardumont")


    @istest
    def get_occurrence_ok(self):
        # when
        rv = self.app.get('/vcs/occurrences/%s' % self.revision_sha1_hex)

        # then
        assert rv.status_code == 200
        # assert rv.data.decode('utf-8') == '{\n  "names": ["master"]\n}'  # return the list of references pointing to the revision -> do not care for the moment

    @istest
    def get_occurrence_not_found(self):
        # when
        rv = self.app.get('/vcs/occurrences/inexistant-sha1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def get_occurrence_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/vcs/occurrences/1')
        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

    @istest
    def put_occurrence_create_and_update(self):
        occ_revision_sha1_hex = self.revision_sha1_hex_2

        rv = self.app.get('/vcs/occurrences/%s' % occ_revision_sha1_hex)

        # then
        assert rv.status_code == 404
        assert rv.data == b'Not found!'

        # we create it
        body = serial.dumps({'content': 'occurrence content',
                           'reference': 'master',
                           'url-origin': self.origin_url})

        rv = self.app.put('/vcs/occurrences/%s' % occ_revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/occurrences/%s' % occ_revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == occ_revision_sha1_hex

        # we update it
        rv = self.app.put('/vcs/occurrences/%s' % occ_revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 200
        assert rv.data == b'Successful update!'

        # still the same
        rv = self.app.get('/vcs/occurrences/%s' % occ_revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data)['id'] == occ_revision_sha1_hex
