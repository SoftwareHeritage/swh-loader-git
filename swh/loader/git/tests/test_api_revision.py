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
class RevisionTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app, db_url, self.content_storage_dir = app_client()

        with db.connect(db_url) as db_conn:
            directory_sha1_hex = '13d2a9739ac02431681c317ce449909a46c59554'
            self.directory_sha1_bin = hashutil.hex_to_hash(directory_sha1_hex)
            models.add_directory(db_conn, self.directory_sha1_bin)

            self.authorAndCommitter = {'name': 'some-name', 'email': 'some-email'}
            models.add_person(db_conn, self.authorAndCommitter['name'], self.authorAndCommitter['email'])

            self.revision_parent_sha1_hex = '23d2a9739ac02431681c317ce449909a46c59554'
            self.revision_parent_sha1_bin = hashutil.hex_to_hash(self.revision_parent_sha1_hex)
            models.add_revision(db_conn,
                                self.revision_parent_sha1_bin,
                                now(),
                                now(),
                                self.directory_sha1_bin,
                                "revision message",
                                self.authorAndCommitter,
                                self.authorAndCommitter)

            revision_parent_2_sha1_hex = '33d2a9739ac02431681c317ce449909a46c59554'
            self.revision_parent_2_sha1_bin = hashutil.hex_to_hash(revision_parent_2_sha1_hex)
            models.add_revision(db_conn,
                                self.revision_parent_2_sha1_bin,
                                now(),
                                now(),
                                self.directory_sha1_bin,
                                "revision message 2",
                                self.authorAndCommitter,
                                self.authorAndCommitter)

            revision_parent_3_sha1_hex = '43d2a9739ac02431681c317ce449909a46c59554'
            self.revision_parent_3_sha1_bin = hashutil.hex_to_hash(revision_parent_3_sha1_hex)
            models.add_revision(db_conn,
                                self.revision_parent_3_sha1_bin,
                                now(),
                                now(),
                                self.directory_sha1_bin,
                                "revision message 3",
                                self.authorAndCommitter,
                                self.authorAndCommitter)

    @classmethod
    def tearDownClass(self):
        app_client_teardown(self.content_storage_dir)

    @istest
    def get_revision_ok(self):
        # when
        rv = self.app.get('/vcs/revisions/%s' % self.revision_parent_sha1_hex)

        # then
        self.assertEquals(rv.status_code, 200)
        self.assertEquals(serial.loads(rv.data)['id'], self.revision_parent_sha1_hex)

    @istest
    def get_revision_not_found(self):
        # when
        rv = self.app.get('/vcs/revisions/inexistant-sha1')
        # then
        self.assertEquals(rv.status_code, 404)
        self.assertEquals(rv.data, b'Not found!')

    @istest
    def get_revision_not_found_with_bad_format(self):
        # when
        rv = self.app.get('/vcs/revisions/1')
        # then
        self.assertEquals(rv.status_code, 404)
        self.assertEquals(rv.data, b'Not found!')

    @istest
    def put_revision_create_and_update(self):
        revision_sha1_hex = '53d2a9739ac02431681c317ce449909a46c59554'

        rv = self.app.get('/vcs/revisions/%s' % revision_sha1_hex)

        # then
        self.assertEquals(rv.status_code, 404)
        self.assertEquals(rv.data, b'Not found!')

        # we create it
        body = serial.dumps({'date': now(),
                             'committer-date': now(),
                             'directory': self.directory_sha1_bin,
                             'message': 'revision message describing it',
                             'committer': self.authorAndCommitter,
                             'author': self.authorAndCommitter,
                             'parent-sha1s': [self.revision_parent_sha1_bin,
                                              self.revision_parent_3_sha1_bin,
                                              self.revision_parent_2_sha1_bin]})

        rv = self.app.put('/vcs/revisions/%s' % revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        self.assertEquals(rv.status_code, 204)
        self.assertEquals(rv.data, b'')

        # now it exists
        rv = self.app.get('/vcs/revisions/%s' % revision_sha1_hex)

        # then
        self.assertEquals(rv.status_code, 200)
        self.assertEquals(serial.loads(rv.data)['id'], revision_sha1_hex)

        # we update it
        rv = self.app.put('/vcs/revisions/%s' % revision_sha1_hex,
                          data=body,
                          headers={'Content-Type': serial.MIMETYPE})

        self.assertEquals(rv.status_code, 204)
        self.assertEquals(rv.data, b'')

        # still the same
        rv = self.app.get('/vcs/revisions/%s' % revision_sha1_hex)

        # then
        self.assertEquals(rv.status_code, 200)
        self.assertEquals(serial.loads(rv.data)['id'], revision_sha1_hex)
