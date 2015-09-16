# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest
from nose.plugins.attrib import attr

from swh.loader.git.storage import db, models
from swh.loader.git.protocols import serial
from test_utils import app_client


@attr('slow')
class PersonTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.app, db_url, _ = app_client()

        with db.connect(db_url) as db_conn:
            self.person_name = 'some-name'
            self.person_email = 'some@mail.git'
            self.person_id = models.add_person(db_conn, self.person_name, self.person_email)

    @istest
    def get_person_ok(self):
        # when
        person = {'name': self.person_name,
                  'email': self.person_email}
        rv = self.app.post('/vcs/persons/',
                           data=serial.dumps(person),
                           headers={'Content-Type': serial.MIMETYPE})

        # then
        self.assertEquals(rv.status_code, 200)
        self.assertEquals(serial.loads(rv.data)['id'], self.person_id)

    @istest
    def get_person_not_found(self):
        # when
        person = {'name': 'unknown',
                  'email': 'blah'}
        rv = self.app.post('/vcs/persons/',
                           data=serial.dumps(person),
                           headers={'Content-Type': serial.MIMETYPE})
        # then
        self.assertEquals(rv.status_code, 404)
        self.assertEquals(rv.data, b'Person not found!')

    @istest
    def get_person_not_found_with_bad_format(self):
        # when
        rv = self.app.post('/vcs/persons/',
                           data=serial.dumps({'name': 'unknown'}),
                           headers={'Content-Type': serial.MIMETYPE})
        # then
        self.assertEquals(rv.status_code, 400)

    @istest
    def put_person(self):
        # when
        person = {'name': 'unknown',
                  'email': 'blah'}
        rv = self.app.post('/vcs/persons/',
                           data=serial.dumps(person),
                           headers={'Content-Type': serial.MIMETYPE})
        # then
        self.assertEquals(rv.status_code, 404)
        self.assertEquals(rv.data, b'Person not found!')

        # when
        rv = self.app.put('/vcs/persons/',
                          data=serial.dumps([person]),
                          headers={'Content-Type': serial.MIMETYPE})

        # then
        self.assertEquals(rv.status_code, 204)
        self.assertEquals(rv.data, b'')

        person = {'name': 'unknown',
                  'email': 'blah'}
        rv = self.app.post('/vcs/persons/',
                           data=serial.dumps(person),
                           headers={'Content-Type': serial.MIMETYPE})
        # then
        self.assertEquals(rv.status_code, 200)
        person_id = serial.loads(rv.data)['id']
        self.assertIsNotNone(person_id)

        # when
        rv = self.app.put('/vcs/persons/',
                          data=serial.dumps([person, person]),
                          headers={'Content-Type': serial.MIMETYPE})

        # then
        self.assertEquals(rv.status_code, 204)
        self.assertEquals(rv.data, b'')
