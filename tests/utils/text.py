'''Tests the tools and utilities in pulsar.utils.'''
from pulsar.utils import text
from pulsar.apps.test import unittest

class TestTextUtils(unittest.TestCase):
    
    def testLazy(self):
        @text.lazy_string
        def blabla(n):
            return 'AAAAAAAAAAAAAAAAAAAA %s' % n
        r = blabla(3)
        self.assertEqual(r._value, None)
        v = str(r)
        self.assertEqual(v, 'AAAAAAAAAAAAAAAAAAAA 3')
        self.assertEqual(r._value, v)