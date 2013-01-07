'''Deferred and asynchronous tools.'''
import sys
from functools import reduce

from pulsar import AlreadyCalledError, Deferred, is_async,\
                     make_async, IOLoop, is_failure, MultiDeferred,\
                     maybe_async
from pulsar.apps.test import unittest


class Cbk(Deferred):
    '''A deferred object'''
    def __init__(self, r = None):
        super(Cbk, self).__init__()
        if r is not None:
            self.r = (r,)
        else:
            self.r = ()
            
    def add(self, result):
        self.r += (result,)
        return self
        
    def set_result(self, result):
        self.add(result)
        self.callback(self.r)
    
    def set_error(self):
        try:
            raise ValueError('Bad callback')
        except Exception as e:
            self.callback(e)

def async_pair():
    c = Deferred()
    d = Deferred().add_both(c.callback)
    return d, c
    
    
class TestDeferred(unittest.TestCase):
    
    def testSimple(self):
        d = Deferred()
        self.assertFalse(d.called)
        self.assertFalse(d.running)
        self.assertEqual(str(d), 'Deferred')
        d.callback('ciao')
        self.assertTrue(d.called)
        self.assertTrue(' (called)' in str(d))
        self.assertEqual(d.result, 'ciao')
        self.assertRaises(AlreadyCalledError, d.callback, 'bla')
        
    def testBadCallback(self):
        d = Deferred()
        self.assertRaises(TypeError, d.add_callback, 3)
        self.assertRaises(TypeError, d.add_callback, lambda r: r, 4)
        
    def testWrongOperations(self):
        d = Deferred()
        self.assertRaises(RuntimeError, d.callback, Deferred())

    def testCallbacks(self):
        d, cbk = async_pair()
        self.assertFalse(d.called)
        d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(cbk.result, 'ciao')
        
    def testError(self):
        d, cbk = async_pair()
        self.assertFalse(d.called)
        try:
            raise Exception('blabla exception')
        except Exception as e:
            trace = sys.exc_info()
            d.callback(e)
        self.assertTrue(d.called)
        self.assertTrue(cbk.called)
        self.assertEqual(cbk.result[-1],trace)
        
    def testDeferredCallback(self):
        d = Deferred()
        d.add_callback(lambda r : Cbk(r))
        self.assertFalse(d.called)
        result = d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(d.paused,1)
        self.assertTrue(is_async(result))
        self.assertEqual(len(result.callbacks), 1)
        self.assertFalse(result.called)
        result.set_result('luca')
        self.assertTrue(result.called)
        self.assertEqual(result.result,('ciao','luca'))
        self.assertEqual(d.paused,0)
        
    def testDeferredCallbackInGenerator(self):
        d = Deferred()
        ioloop = IOLoop()
        # Add a callback which returns a deferred
        rd = Cbk()
        d.add_callback(lambda r : rd.add(r))
        d.add_callback(lambda r : r + ('second',))
        def _gen():
            yield d
        a = make_async(_gen())
        result = d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(d.paused,1)
        self.assertEqual(len(d.callbacks), 2)
        self.assertEqual(len(rd.callbacks), 1)
        #
        self.assertEqual(rd.r, ('ciao',))
        self.assertFalse(a.called)
        #
        # set callback
        rd.set_result('luca')
        self.assertTrue(a.called)
        self.assertFalse(d.paused)
        self.assertEqual(d.result,('ciao','luca','second'))
        
    def testDeferredErrorbackInGenerator(self):
        d = Deferred()
        ioloop = IOLoop()
        # Add a callback which returns a deferred
        rd = Cbk()
        d.add_callback(lambda r : rd.add(r))
        d.add_callback(lambda r : r + ('second',))
        def _gen():
            yield d
        a = make_async(_gen())
        result = d.callback('ciao')
        self.assertTrue(d.called)
        self.assertEqual(d.paused, 1)
        # The generator has added its consume callback
        self.assertEqual(len(d.callbacks), 2)
        self.assertEqual(len(rd.callbacks), 1)
        #
        self.assertEqual(rd.r, ('ciao',))
        self.assertFalse(a.called)
        #
        # set Error back
        rd.set_error()
        self.assertFalse(d.paused)
        self.assertTrue(a.called)
        self.assertTrue(is_failure(d.result))
        
    def testSafeAsync(self):
        pass
        
        
class TestMultiDeferred(unittest.TestCase):
    
    def testSimple(self):
        d = MultiDeferred()
        self.assertFalse(d.called)
        self.assertFalse(d._locked)
        self.assertFalse(d._deferred)
        self.assertFalse(d._stream)
        d.lock()
        self.assertTrue(d.called)
        self.assertTrue(d._locked)
        self.assertEqual(d.result,[])
        self.assertRaises(RuntimeError, d.lock)
        self.assertRaises(AlreadyCalledError, d._finish)
        
    def testMulti(self):
        d = MultiDeferred()
        d1 = Deferred()
        d2 = Deferred()
        d.append(d1)
        d.append(d2)
        d.append('bla')
        self.assertRaises(RuntimeError, d._finish)
        d.lock()
        self.assertRaises(RuntimeError, d._finish)
        self.assertRaises(RuntimeError, d.lock)
        self.assertRaises(RuntimeError, d.append, d1)
        self.assertFalse(d.called)
        d2.callback('first')
        self.assertFalse(d.called)
        d1.callback('second')
        self.assertTrue(d.called)
        self.assertEqual(d.result,['second', 'first', 'bla'])
        
    def testUpdate(self):
        d1 = Deferred()
        d2 = Deferred()
        d = MultiDeferred()
        d.update((d1,d2)).lock()
        d1.callback('first')
        d2.callback('second')
        self.assertTrue(d.called)
        self.assertEqual(d.result,['first','second'])
        
    def testNested(self):
        d = MultiDeferred()
        # add a generator
        d.append((a for a in range(1,11)))
        r = maybe_async(d.lock())
        self.assertTrue(d.locked)
        self.assertFalse(is_async(r))
        self.assertEqual(r, [[1,2,3,4,5,6,7,8,9,10]])
        
    def testNestedhandle(self):
        handle = lambda value : reduce(lambda x,y: x+y, value)\
                     if isinstance(value, list) else value 
        d = MultiDeferred(handle_value=handle)
        d.append((a for a in range(1,11)))
        r = maybe_async(d.lock())
        self.assertFalse(is_async(r))
        self.assertEqual(r, [55])
        handle = lambda value: 'c'*value
        d = MultiDeferred(handle_value=handle)
        d.append((a for a in range(1,11)))
        r = maybe_async(d.lock())
        self.assertFalse(is_async(r))
        self.assertTrue(is_failure(r[0]))
    
        
        
        
        


        
        
        
        
        


