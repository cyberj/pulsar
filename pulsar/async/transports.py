import sys
import io
import time
import logging
import socket
import errno
from collections import deque
from threading import current_thread

from pulsar import create_socket, server_socket, create_client_socket,\
                     wrap_socket, defaults, create_connection, CouldNotParse,\
                     get_socket_timeout, Timeout, BaseSocket
from pulsar.utils.httpurl import IOClientRead
from .defer import Deferred, is_async, is_failure, async, maybe_async,\
                        safe_async, log_failure, NOT_DONE, range
from .access import PulsarThread, get_actor
from .iostream import AsyncIOStream
from .eventloop import IOLoop

__all__ = ['ProtocolSocket',
           'ClientSocket',
           'Client',
           'AsyncConnection',
           'AsyncResponse',
           'AsyncSocketServer']


LOGGER = logging.getLogger('pulsar.transports')

class EchoParser:
    '''A simple echo protocol'''
    def encode(self, data):
        return data
    
    def decode(self, data):
        return bytes(data), bytearray()
    
    
class ProtocolSocket(BaseSocket):
    '''A :class:`BaseSocket` with a protocol for encoding and decoding
messages. This is the base class for :class:`AsyncSocketServer`,
:class:`AsyncConnection` and :class:`ClientSocketHandler`.

.. attribute:: on_closed

    A :class:`Deferred` which receives a callback once the
    :meth:`close` method is invoked.
 
'''
    protocol_factory = EchoParser
    '''A callable for building the protocol to encode and decode messages. This
    attribute can be specified as class attribute or via the constructor.
    A :ref:`protocol <socket-protocol>` must be an instance of a class
    exposing the ``encode`` and ``decode`` methods.'''
    _closing_socket = False
    def __new__(cls, *args, **kwargs):
        o = super(ProtocolSocket, cls).__new__(cls)
        o.on_closed = Deferred(
                        description='on_closed %s callback' % cls.__name__)
        o.time_started = time.time()
        o.time_last = o.time_started
        f = kwargs.get('protocol_factory')
        if f:
            o.protocol_factory = f
        o.received = 0
        return o

    @property
    def closed(self):
        '''True if the socket is closed.'''
        return self.sock.closed

    def on_close(self, failure=None):
        '''Callback just before closing the socket'''
        pass

    def close(self, msg=None):
        '''Close this socket and log the failure if there was one.'''
        if self._closing_socket:
            return msg
        self._closing_socket = True
        if is_failure(msg):
            if isinstance(msg.trace[1], Timeout):
                if not msg.logged:
                    msg.logged = True
                    LOGGER.info('Closing %s on timeout.', self)
            else:
                log_failure(msg)
        self.on_close(msg)
        self.sock.close()
        return self.on_closed.callback(msg)
    
    
class ClientSocketHandler(ProtocolSocket):
    '''A :class:`ProtocolSocket` for clients.
This class can be used with synchronous and asynchronous socket
for both a "client" socket and a server connection socket (the socket
obtained from a server socket via the ``connect`` function).

.. attribute:: protocol

    An object obtained by the :attr:`ProtocolSocket.protocol_factory` method.
    It must implement the ``encode`` and ``decode`` methods used, respectively,
    by clients and servers.
    
.. attribute:: buffer

    A bytearray containing data received from the socket. This buffer
    is not empty when more data is required to form a message.
    
.. attribute:: remote_address

    The remote address for the socket communicating with this
    :class:`ClientSocketHandler`.
'''
    def __init__(self, socket, address, timeout=None, read_timeout=None,
                 **kwargs):
        '''Create a client or client-connection socket.

:parameter socket: a client or client-connection socket
:parameter address: The address of the remote client/server
:parameter protocol_factory: A callable used for creating
    :attr:`ProtoSocket.protocol` instance.
:parameter timeout: A timeout in seconds for the socket. Same rules as
    the ``socket.settimeout`` method in the standard library. A value of 0
    indicates an asynchronous socket.
:parameter read_timeout: A timeout in seconds for asynchronous operations. This
    value is only used when *timeout* is 0 in the constructor
    of a :class:`AsyncIOStream`.
'''
        self._socket_timeout = get_socket_timeout(timeout)
        self._set_socket(socket, read_timeout)
        self.remote_address = address
        self.protocol = self.protocol_factory()
        self.buffer = bytearray()

    def __repr__(self):
        return str(self.remote_address)
    __str__ = __repr__

    def _set_socket(self, sock, read_timeout=None):
        if not isinstance(sock, AsyncIOStream):
            if self._socket_timeout == 0:
                sock = AsyncIOStream(sock)
            else:
                sock = wrap_socket(sock)
                sock.settimeout(self._socket_timeout)
        self.sock = sock
        if self.async:
            close_callback = Deferred().add_callback(self.close)
            self.sock.set_close_callback(close_callback)
            self.read_timeout = read_timeout
            
    def _get_read_timeout(self):
        if self.async:
            return self.sock._read_callback_timeout
        else:
            return self._socket_timeout
    def _set_read_timeout(self, value):
        if self.async:
            self.sock._read_callback_timeout = value
        else:
            self._socket_timeout = value
            self.sock.settimeout(self._socket_timeout)
    read_timeout = property(_get_read_timeout, _set_read_timeout) 


class AsyncRead(object):
        
    def async_read(self):
        return self.keep_reading().add_errback(self.close)
        
    @async(max_errors=1)
    def keep_reading(self):
        msg = None
        while not self.sock.closed:
            future = self.sock.read()
            yield future
            msg = self.parsedata(future.outcome)
            if msg is not None:
                break
        yield msg
        
        
class ClientResponse(AsyncRead, IOClientRead):
    
    def __init__(self, client):
        self.client = client
        self.sock = client.sock
        self.finished = Deferred()
    
    def parsedata(self, data):
        return self.client.parsedata(data)
    
    def begin(self, data):
        self.client.send(data)
        return self.async_read().add_both(self.finished.callback)
    
    def close(self, result=None):
        self.client.close()
    
    
class ClientSocket(ClientSocketHandler):
    '''Synchronous/Asynchronous client for a remote socket server. This client
maintain a connection with remote server and exchange data by writing and
reading from the same socket connection.'''
    response_class = ClientResponse
    def __init__(self, *args, **kwargs):
        super(ClientSocket, self).__init__(*args, **kwargs)
        self.exec_queue = deque()
        self.processing = False
        
    @classmethod
    def connect(cls, address, **kwargs):
        '''Create a new :class:`ClientSocket` connected at *address*.'''
        sock = create_connection(address)
        return cls(sock, address, **kwargs)

    def send(self, data):
        '''Send data to remote server'''
        self.time_last = time.time()
        data = self.protocol.encode(data)
        return self.sock.write(data)
        
    def execute(self, data):
        '''Send and read data from socket. It makes sure commands are queued
and executed in an orderly fashion. For asynchronous connection it returns
a :class:`Deferred` called once data has been received from the server and
parsed.'''
        if data:
            response = self.response_class(self)
            if self.async:
                self.sock.ioloop.call_soon_threadsafe(response.begin, data)
                return response.finished
            else:
                self.send(data)
                return response.read()
        
    def parsedata(self, data):
        '''We got some data to parse'''
        parsed_data = self._parsedata(data)
        if parsed_data:
            self.received += 1
            return parsed_data
        
    def _consume_next(self):
        # This function is always called in the socket IOloop
        if not self.processing and self.exec_queue:
            self.processing = True
            data, cbk = self.exec_queue.popleft()
            response = self.response_class()
            response.connection = self
            
            msg = safe_async(self.send, (data,))\
                    .add_callback(self._read, self.close)
            msg = maybe_async(msg)
            if is_async(msg):
                msg.add_both(cbk.callback)
            else:
                cbk.callback(msg)
            
    def _got_result(self, result):
        # This callback is always executed in the socket IOloop
        self.processing = False
        # keep on consuming if needed
        self._consume_next()
        return result

    def _parsedata(self, data):
        buffer = self.buffer
        if data:
            # extend the buffer
            buffer.extend(data)
        elif not buffer:
            return
        try:
            parsed_data, buffer = self.protocol.decode(buffer)
        except CouldNotParse:
            LOGGER.warn('Could not parse data', exc_info=True)
            parsed_data = None
            buffer = bytearray()
        self.buffer = buffer
        return parsed_data


class Client(ClientSocket):

    def reconnect(self):
        if self.closed:
            sock = create_connection(self.remote_address, blocking=True)
            self._set_socket(sock)


class ReconnectingClient(Client):

    def send(self, data):
        self.reconnect()
        return super(ReconnectingClient, self).send(data)
        
        
class AsyncResponse(object):
    '''An asynchronous server response is created once an
:class:`AsyncConnection` has available parsed data from a read operation.
Instances of this class are iterable and produce chunk of data to send back
to the remote client.

The ``__iter__`` is the only method which **needs** to be implemented by
derived classes. If an empty byte is yielded, the asynchronous engine
will resume the iteration after one loop in the actor event loop.

.. attribute:: connection

    The :class:`AsyncConnection` for this response
    
.. attribute:: server

    The :class:`AsyncSocketServer` for this response

.. attribute:: parsed_data

    Parsed data from remote client
'''
    def __init__(self, connection, parsed_data):
        self.connection = connection
        self.parsed_data = parsed_data

    @property
    def server(self):
        return self.connection.server

    @property
    def protocol(self):
        return self.connection.protocol

    @property
    def sock(self):
        return self.connection.sock

    def __iter__(self):
        # by default it echos the client message
        yield self.protocol.encode(self.parsed_data)


class AsyncConnection(ClientSocketHandler):
    '''An asynchronous client connection for a :class:`AsyncSocketServer`.
The connection maintains the client socket open for as long as it is required.
A connection can handle several request/responses until it is closed.

.. attribute:: server

    The class :class:`AsyncSocketServer` which created this
    :class:`AsyncConnection`.

.. attribute:: response_class

    Class or callable for building an :class:`AsyncResponse` object. It is
    initialised by the :class:`AsyncSocketServer.response_class` but it can be
    changed at runtime when upgrading connections to new protocols. An example
    is the websocket protocol.
'''
    def __init__(self, sock, address, server, **kwargs):
        if not isinstance(sock, AsyncIOStream):
            sock = AsyncIOStream(sock, timeout=server.timeout)
        super(AsyncConnection, self).__init__(sock, address, **kwargs)
        self.server = server
        self.response_class = server.response_class
        server.connections.add(self)
        self.handle().add_errback(self.close)

    @async(max_errors=1, description='Asynchronous client connection generator')
    def handle(self):
        while not self.closed:
            # Read the socket
            outcome = self.sock.read()
            yield outcome
            # if we are here it means no errors occurred so far and
            # data is available to process (max_errors=1)
            self.time_last = time.time()
            buffer = self.buffer
            buffer.extend(outcome.result)
            parsed_data = True
            # IMPORTANT! Consume all data until the protocol returns nothing.
            while parsed_data:
                parsed_data = self.request_data()
                if parsed_data:
                    self.received += 1
                    # The response is an iterable (same as WSGI response)
                    for data in self.response_class(self, parsed_data):
                        if data:
                            yield self.write(data)
                        else: # The response is not ready. release the loop
                            yield NOT_DONE
    
    def request(self, response=None):
        if self._current_request is None:
            self._current_request = AsyncRequest(self)
        request = self._current_request
        if response is not None:
            self._current_request = None
            request.callback(response)
        return request

    def request_data(self):
        '''This function is called when data to parse is available on the
:attr:`ClientSocket.buffer`. It should return parsed data or ``None`` if
more data in the buffer is required.'''
        buffer = self.buffer
        if not buffer:
            return
        self.protocol.connection = self
        try:
            parsed_data, buffer = self.protocol.decode(buffer)
        except:
            LOGGER.error('Could not parse data', exc_info=True)
            raise
        self.buffer = buffer
        return parsed_data

    @property
    def actor(self):
        return self.server.actor

    def on_close(self, failure=None):
        self.server.connections.discard(self)

    def write(self, data):
        '''Write data to socket.'''
        return self.sock.write(data)


class AsyncSocketServer(ProtocolSocket):
    '''A :class:`ProtocolSocket` for asynchronous servers which listen
for requests on a socket.

.. attribute:: actor

    The :class:`Actor` running this :class:`AsyncSocketServer`.

.. attribute:: ioloop

    The :class:`IOLoop` used by this :class:`AsyncSocketServer` for
    asynchronously sending and receiving data.

.. attribute:: connections

    The set of all open :class:`AsyncConnection`

.. attribute:: onthread

    If ``True`` the server has its own :class:`IOLoop` running on a separate
    thread of execution. Otherwise it shares the :attr:`actor.requestloop`

.. attribute:: timeout

    The timeout when reading data in an asynchronous way.
    
'''
    thread = None
    _started = False
    connection_class = AsyncConnection
    '''A subclass of :class:`AsyncConnection`. A new instance of this class is
constructued each time a new connection has been established by the
:meth:`accept` method.'''
    response_class = AsyncResponse
    '''A subclass of :class:`AsyncResponse` for handling responses to clients
    once data has been received and processed.'''
    
    def __init__(self, actor, socket, onthread=False, connection_class=None,
                 response_class=None, timeout=None, **kwargs):
        self.actor = actor
        self.connection_class = connection_class or self.connection_class
        self.response_class = response_class or self.response_class
        self.sock = wrap_socket(socket)
        self.connections = set()
        self.timeout = timeout
        self.on_connection_callbacks = []
        if onthread:
            # Create a pulsar thread and starts it
            self.__ioloop = IOLoop()
            self.thread = PulsarThread(name=self.name, target=self._run)
            self.thread.actor = actor
            actor.requestloop.call_soon_threadsafe(self.thread.start)
        else:
            self.__ioloop = actor.requestloop
            actor.requestloop.call_soon_threadsafe(self._run)
        
    @classmethod
    def make(cls, actor=None, bind=None, backlog=None, **kwargs):
        if actor is None:
            actor = get_actor()
        if not backlog:
            if actor:
                backlog = actor.cfg.backlog
            else:
                backlog = defaults.BACKLOG
        if bind:
            socket = create_socket(bind, backlog=backlog)
        else:
            socket = server_socket(backlog=backlog)
        return cls(actor, socket, **kwargs)

    @property
    def onthread(self):
        return self.thread is not None
    
    @property
    def name(self):
        return '%s %s' % (self.__class__.__name__, self.address)

    def __repr__(self):
        return self.name
    __str__ = __repr__

    @property
    def ioloop(self):
        return self.__ioloop

    @property
    def active_connections(self):
        return len(self.connections)

    def quit_connections(self):
        for c in list(self.connections):
            c.close()
            
    def on_close(self, failure=None):
        self.quit_connections()
        self.ioloop.remove_handler(self)
        if self.onthread:
            self.__ioloop.stop()
            # We join the thread
            if current_thread() != self.thread:
                try:
                    self.thread.join()
                except RuntimeError:
                    pass

    ############################################################## INTERNALS
    def _run(self):
        # add new_connection READ handler to the eventloop and starts the
        # eventloop if it was not already started.
        self.actor.logger.debug('Registering %s with event loop.', self)
        self.ioloop.add_reader(self, self._on_connection)
        self.ioloop.start()

    def _on_connection(self):
        '''Called when a new connection is available.'''
        # obtain the client connection
        for callback in self.on_connection_callbacks:
            c = callback()
            if c is not None:
                return c
        return self.accept()

    def accept(self):
        '''Accept a new connection from a remote client'''
        client, client_address = self.sock.accept()
        if client:
            self.received += 1
            return self.connection_class(client, client_address, self,
                                         protocol_factory=self.protocol_factory)
