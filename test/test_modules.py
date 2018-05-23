#--*-- coding:utf-8 --*--
from unittest import TestCase
import unittest

class TestModule(TestCase):
    def setUp(self):
        #do something before test
        pass

    def test_user(self):
        self.assertNotEqual()

    def test_event(self):
        pass


    def tearDown(self):
        #do some thing after test
        pass

if __name__ == '__main__':
    unittest.main()

