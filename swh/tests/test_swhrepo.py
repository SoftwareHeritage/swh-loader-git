# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from nose.tools import istest

from swh.data import swhrepo
from test_utils import app_client


class SWHRepoTestCase(unittest.TestCase):
    def setUp(self):
        self.app, db_url = app_client()

    @istest
    def new_swhrepo(self):
        # when
        r = swhrepo.SWHRepo()

        r.add_origin({'url': 'foobar'})
        
        r.add_content({'sha1': 'some-con-sha1'})
        r.add_content({'sha1': 'some-con-sha1-2','stuff': 'some-stuff'})
        r.add_directory({'sha1': 'some-dir-sha1'})
        r.add_directory({'sha1': 'some-dir-sha1-2'})
        r.add_revision({'sha1': 'some-rev-sha1'})
        r.add_revision({'sha1': 'some-rev-sha1-2'})
        r.add_person('id0', {'name': 'the one'})
        r.add_person('id1', {'name': 'another one'})

        r.add_occurrence({'sha1': 'some-occ-sha1'})
        r.add_release({'sha1': 'some-rel-sha1'})

        # then
        assert r.get_origin() == {'url': 'foobar'}
        assert r.get_releases() == [{'sha1': 'some-rel-sha1'}]
        assert r.get_occurrences() == [{'sha1': 'some-occ-sha1'}]

        for sha in ['some-con-sha1', 'some-con-sha1-2',
                    'some-dir-sha1', 'some-dir-sha1-2',
                    'some-rev-sha1', 'some-rev-sha1-2']:
            assert r.already_visited(sha) is True
            
        assert r.already_visited('some-occ-sha1') is False
        assert r.already_visited('some-rel-sha1') is False

        assert r.get_contents() == {'some-con-sha1': {'sha1': 'some-con-sha1'},
                                    'some-con-sha1-2': {'sha1': 'some-con-sha1-2','stuff': 'some-stuff'}}
        assert r.get_directories() == {'some-dir-sha1': {'sha1': 'some-dir-sha1'},
                                       'some-dir-sha1-2': {'sha1': 'some-dir-sha1-2'}}
        assert r.get_revisions() == {'some-rev-sha1': {'sha1': 'some-rev-sha1'},
                                     'some-rev-sha1-2': {'sha1': 'some-rev-sha1-2'}}

        assert len(r.get_persons()) == 2
