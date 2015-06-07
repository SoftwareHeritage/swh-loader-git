import unittest

from nose.tools import istest
#from nose.plugins.attrib import attr

class TestingLearning(unittest.TestCase):
    def setUp(self):
        print("setup")

    def tearDown(self):
        print("teardown")

    @istest
    def tryout(self):
        print("test")
