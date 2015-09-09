# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.loader.git.storage import db, models
from swh.loader.git.protocols import serial
from test_utils import now, app_client, app_client_teardown


@attr('slow')
class OccurrenceTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url, self.content_storage_dir = app_client()

        with db.connect(db_url) as db_conn:
            self.directory_sha1_hex = 'directory-sha16ee476a8be155ab049994f717e'
            models.add_directory(db_conn, self.directory_sha1_hex)

            authorAndCommitter = {'name': 'some-name', 'email': 'some-email'}
            models.add_person(db_conn, authorAndCommitter['name'], authorAndCommitter['email'])

            self.revision_sha1_hex = 'revision-sha1-to-test-existence9994f717e'
            models.add_revision(db_conn,
                                self.revision_sha1_hex,
                                now(),
                                self.directory_sha1_hex,
                                "revision message",
                                authorAndCommitter,
                                authorAndCommitter)

            self.origin_url = "https://github.com/user/repo"
            models.add_origin(db_conn, self.origin_url, 'git')

            self.reference_name = 'master'
            models.add_occurrence(db_conn,
                                 self.origin_url,
                                 self.reference_name,
                                 self.revision_sha1_hex)

            self.reference_name2 = 'master2'
            models.add_occurrence(db_conn,
                                 self.origin_url,
                                 self.reference_name2,
                                 self.revision_sha1_hex)

            self.revision_sha1_hex_2 = '2-revision-sha1-to-test-existence9994f71'
            models.add_revision(db_conn,
                                self.revision_sha1_hex_2,
                                now(),
                                self.directory_sha1_hex,
                                "revision message 2",
                                authorAndCommitter,
                                authorAndCommitter)

    def tearDown(self):
        app_client_teardown(self.content_storage_dir)

    @istest
    def get_occurrence_ok(self):
        # when
        rv = self.app.get('/vcs/occurrences/%s' % self.revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data) == [self.reference_name, self.reference_name2]

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
        body = serial.dumps({'revision': occ_revision_sha1_hex,  # FIXME: redundant with the one from uri..
                             'reference': 'master',
                             'url-origin': self.origin_url})

        rv = self.app.put('/vcs/occurrences/%s' % occ_revision_sha1_hex,  # ... here
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # now it exists
        rv = self.app.get('/vcs/occurrences/%s' % occ_revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data) == ['master']

        # we update it
        rv = self.app.put('/vcs/occurrences/%s' % occ_revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        assert rv.status_code == 204
        assert rv.data == b''

        # still the same
        rv = self.app.get('/vcs/occurrences/%s' % occ_revision_sha1_hex)

        # then
        assert rv.status_code == 200
        assert serial.loads(rv.data) == ['master']
