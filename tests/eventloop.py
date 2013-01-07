import time
from threading import current_thread

import pulsar
from pulsar.apps.test import unittest


class TestEventLoop(unittest.TestCase):
    
    def testIOloop(self):
        ioloop = pulsar.get_event_loop()
        self.assertTrue(ioloop)
        self.assertNotEqual(ioloop.tid, current_thread().ident)
        
    def test_call_soon(self):
        ioloop = pulsar.get_event_loop()
        d = pulsar.Deferred()
        ioloop.call_soon_threadsafe(lambda: d.callback(current_thread().ident))
        # we should be able to wait less than a second
        yield d
        self.assertEqual(d.result, ioloop.tid)
        
    def test_call_later(self):
        ioloop = pulsar.get_event_loop()
        d = pulsar.Deferred()
        timeout1 = ioloop.call_later(20,
                            lambda: d.callback(current_thread().ident))
        timeout2 = ioloop.call_later(10,
                            lambda: d.callback(current_thread().ident))
        # lets wake the ioloop
        ioloop.wake()
        self.assertTrue(timeout1 in ioloop._scheduled)
        self.assertTrue(timeout2 in ioloop._scheduled)
        ioloop.remove_timeout(timeout1)
        ioloop.remove_timeout(timeout2)
        self.assertFalse(timeout1 in ioloop._scheduled)
        self.assertFalse(timeout2 in ioloop._scheduled)
        timeout1 = ioloop.call_later(0.1,
                            lambda: d.callback(current_thread().ident))
        ioloop.wake()
        time.sleep(0.2)
        self.assertTrue(d.called)
        self.assertEqual(d.result, ioloop.tid)
        self.assertFalse(timeout1 in ioloop._scheduled)
        
    def test_periodic(self):
        ioloop = pulsar.get_event_loop()
        d = pulsar.Deferred()
        class p:
            def __init__(self):
                self.c = 0
            def __call__(self, periodic):
                self.c += 1
                if self.c == 2:
                    raise ValueError()
                elif self.c == 3:
                    periodic.stop()
                    d.callback(self.c)
        periodic = ioloop.add_periodic(p(), 1)
        yield d
        self.assertEqual(d.result, 3)
        self.assertFalse(periodic._running)
        
        
        
        