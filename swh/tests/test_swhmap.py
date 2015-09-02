# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.data import swhrepo
from test_utils import app_client


class SWHMapTestCase(unittest.TestCase):
    @istest
    def new_swhmap(self):
        # when
        m = swhrepo.SWHMap()

        # then
        assert m.keys() == set()
        assert m.objects() == {}

    @istest
    def add_first(self):
        # given
        m = swhrepo.SWHMap()

        # when
        m.add('some-sha1', {'sha1': 'some-sha1', 'type': 'something'})

        # then

        keys = m.keys()
        assert len(keys) == 1
        assert 'some-sha1' in keys
        assert m.objects()['some-sha1'] == {'sha1': 'some-sha1', 'type': 'something'}

    @istest
    def add_second_time_can_update(self):
        # given
        m = swhrepo.SWHMap()
        m.add('some-sha1', {'sha1': 'some-sha1', 'type': 'something'})

        # when
        m.add('some-sha1', {'sha1': 'some-sha1', 'type': 'something-else'})

        # then
        keys = m.keys()
        assert len(keys) == 1
        assert 'some-sha1' in keys
        assert m.objects()['some-sha1'] == {'sha1': 'some-sha1', 'type': 'something-else'}


class SWHRepoTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

    @istest
    def new_swhrepo(self):
        # when
        r = swhrepo.SWHRepo()

        r.add_origin({'url': 'foobar'})
        r.add_content({'sha1': 'some-con-sha1'})
        r.add_directory({'sha1': 'some-dir-sha1'})
        r.add_revision({'sha1': 'some-rev-sha1'})
        r.add_occurrence({'sha1': 'some-occ-sha1'})
        r.add_release({'sha1': 'some-rel-sha1'})

        # then
        assert r.get_origin() == {'url': 'foobar'}
        assert r.get_releases() == [{'sha1': 'some-rel-sha1'}]
        assert r.get_occurrences() == [{'sha1': 'some-occ-sha1'}]

        assert r.already_visited('some-con-sha1') is True
        assert r.already_visited('some-dir-sha1') is True
        assert r.already_visited('some-rev-sha1') is True
        assert r.already_visited('some-occ-sha1') is False
        assert r.already_visited('some-rel-sha1') is False

        assert 'some-con-sha1' in r.get_contents().keys()
        assert 'some-dir-sha1' in r.get_directories().keys()
        assert 'some-rev-sha1' in r.get_revisions().keys()
