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
from swh.core import hashutil

@attr('slow')
class OccurrenceTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app, db_url, self.content_storage_dir = app_client()

        with db.connect(db_url) as db_conn:
            self.directory_sha1_hex = '0876886dc3b49ebe1043e116727ae781be7c8583'
            self.directory_sha1_bin = hashutil.hex_to_hash(self.directory_sha1_hex)
            models.add_directory(db_conn, self.directory_sha1_bin)

            authorAndCommitter = {'name': 'some-name', 'email': 'some-email'}
            models.add_person(db_conn, authorAndCommitter['name'], authorAndCommitter['email'])

            self.revision_sha1_hex = '1876886dc3b49ebe1043e116727ae781be7c8583'
            self.revision_sha1_bin = hashutil.hex_to_hash(self.revision_sha1_hex)
            models.add_revision(db_conn,
                                self.revision_sha1_bin,
                                now(),
                                now(),
                                self.directory_sha1_bin,
                                "revision message",
                                authorAndCommitter,
                                authorAndCommitter)

            self.origin_url = "https://github.com/user/repo"
            models.add_origin(db_conn, self.origin_url, 'git')

            self.branch_name = 'master'
            models.add_occurrence_history(db_conn,
                                          self.origin_url,
                                          self.branch_name,
                                          self.revision_sha1_bin,
                                          'softwareheritage')

            self.branch_name2 = 'master2'
            models.add_occurrence_history(db_conn,
                                          self.origin_url,
                                          self.branch_name2,
                                          self.revision_sha1_bin,
                                          'softwareheritage')

            self.revision_sha1_hex_2 = '2876886dc3b49ebe1043e116727ae781be7c8583'
            self.revision_sha1_bin_2 = hashutil.hex_to_hash(self.revision_sha1_hex_2)
            models.add_revision(db_conn,
                                self.revision_sha1_bin_2,
                                now(),
                                now(),
                                self.directory_sha1_bin,
                                "revision message 2",
                                authorAndCommitter,
                                authorAndCommitter)

    @classmethod
    def tearDownClass(self):
        app_client_teardown(self.content_storage_dir)

    @istest
    def get_occurrence_ok(self):
        # when
        rv = self.app.get('/vcs/occurrences/%s' % self.revision_sha1_hex)

        # then
        assert rv.status_code == 200
        branches = serial.loads(rv.data)
        assert len(branches) == 2
        assert self.branch_name in branches
        assert self.branch_name2 in branches

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
        body = serial.dumps({'revision': hashutil.hex_to_hash(occ_revision_sha1_hex),  # FIXME: redundant with the one from uri..
                             'branch': 'master',
                             'authority': 'softwareheritage',
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
        # rv = self.app.put('/vcs/occurrences/%s' % occ_revision_sha1_hex,
        #                   data=body,
        #                   headers={'Content-Type': serial.MIMETYPE})

        # assert rv.status_code == 204
        # assert rv.data == b''

        # # still the same
        # rv = self.app.get('/vcs/occurrences/%s' % occ_revision_sha1_hex)

        # # then
        # occs = serial.loads(rv.data)
        # assert rv.status_code == 200
        # assert occs == ['master']
