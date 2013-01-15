from inspect import isgenerator

from .defer import Deferred

__all__ = ['Protocol', 'ProtocolConsumer', 'Connection', 'ProtocolError']

    
class ProtocolError(Exception):
    '''Raised when the protocol encounter unexpected data. It will close
the socket connection.'''
    
        
class Protocol(object):
    '''Pulsar :class:`Protocol` conforming with pep-3156_.
It can be used for both client and server sockets.

* a *client* protocol is for clients connecting to a remote server.
* a *server* protocol is for socket created from an **accept**
  on a :class:`Server`.

.. attribute:: address

    Address of the client, if this is a server, or of the remote
    server if this is a client.
    
.. attribute:: transport

    The :class:`Transport` for this :class:`Protocol`. This is obtained once
    the :meth:`connection_made` is invoked.

.. attribute:: on_connection

    a :class:`Deferred` called once the :attr:`transport` is connected.
        
.. attribute:: on_connection_lost

    a :class:`Deferred` called once the :attr:`transport` loses the connection
    with the endpoint.
    
**METHODS**
'''
    _transport = None
    response_factory = None
    
    def __init__(self, address):
        self._processed = 0
        self._address = address
        self.consumer = None
        self.on_connection = Deferred()
        self.on_connection_lost = Deferred()
    
    def __repr__(self):
        return str(self._address)
    
    def __str__(self):
        return self.__repr__()
        
    @property
    def processed(self):
        return self._processed
    
    @property
    def address(self):
        return self._address
    
    @property
    def transport(self):
        return self._transport
        
    @property
    def event_loop(self):
        if self.transport:
            return self.transport.event_loop
    
    @property
    def sock(self):
        if self.transport:
            return self.transport.sock
        
    @property
    def closed(self):
        return self._transport.closed if self._transport else True
    
    ############################################################################
    ###    PEP 3156 METHODS
    def connection_made(self, transport):
        """Called when a connection is made. The argument is the
:class:`Transport` representing the connection.
To send data, call its :meth:`Transport.write` or
:meth:`Transport.writelines` method.
To receive data, wait for :meth:`data_received` calls.
When the connection is closed, :meth:`connection_lost` is called."""
        self._transport = transport
        self.on_connection.callback(self)

    def data_received(self, data):
        """Called by the :attr:`transport` when data is received.
By default it feeds the *data*, a bytes object, into the
:attr:`current_response` attribute."""
        if self.consumer:
            self.consumer(data)
            
    def eof_received(self):
        """Called when the other end calls write_eof() or equivalent."""

    def connection_lost(self, exc):
        """Called when the connection is lost or closed.

        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """
        self.on_connection_lost.callback(exc)
        
    ############################################################################
    ###    PULSAR METHODS
    def set_response(self, response):
        '''Set a new response instance on this protocol. If a response is
already available it raises an exception.'''
        assert self._current_response is None, "protocol already in response"
        self._current_response = response
        if self._transport is not None:
            self._current_response.begin()
            
    ############################################################################
    ###    TRANSPORT METHODS SHORTCUT
    def close(self):
        if self._transport:
            self._transport.close()
    
    def abort(self):
        if self._transport:
            self._transport.abort()
    

class ProtocolConsumer(object):
    '''A :class:`Protocol` response is responsible for parsing incoming data
and,  producing no more than one response.'''
    def __init__(self, connection):
        self._connection = connection
            
    @property
    def event_loop(self):
        return self._connection.event_loop
    
    @property
    def sock(self):
        return self._connection.sock
    
    @property
    def protocol(self):
        return self._connection.protocol
    
    @property
    def transport(self):
        return self._connection.transport
    
    def on_connect(self):
        pass
    
    def begin(self):
        raise NotImplementedError
        
    def feed(self, data):
        '''Feed new data into the this :class:`ProtocolResponse`. This method
should return `None` unless it has finished the response and the returned bytes
can be used for the next response.'''
        raise NotImplementedError
    
    def finished(self):
        '''`True` if this response has finished and a new response can start.'''
        return self._connection.finished(self)
    
    ############################################################################
    ###    TRANSPORT SHURTCUTS
    def write(self, data):
        self.transport.write(data)
            
    def writelines(self, lines):
        '''Write an iterable of bytes. It is a proxy to
:meth:`Transport.writelines`'''
        self.transport.writelines(lines)
        
        
class Connection:
    '''A client or server connection. It contains the :class:`Protocol`, the
transport producer (:class:`Server` or :class:`Client`) and a session
number.

.. attribute:: protocol

    The :class:`Protocol` of this connection
    
.. attribute:: producer

    The producer of this connection
    
.. attribute:: response_factory

    A factory of :class:`ProtocolResponse` instances for this :class:`Protocol`
    
.. attribute:: session

    Connection session number. Created by the :attr:`producer`
    
.. attribute:: processed

    Number of separate requests processed by this connection.
    
.. attribute:: current_response

    The :class:`Consumer` currently handling incoming data.
'''
    def __init__(self, protocol, producer, session):
        self._protocol = protocol
        self._producer = producer
        self._session = session 
        self._processed = 0
        self._current_response = None
        self._response_factory = producer.response_factory
        protocol.consumer = self.consume
        
    def __repr__(self):
        return '%s session %s' % (self.protocol, self._session)
    
    def __str__(self):
        return self.__repr__()
    
    @property
    def transport(self):
        return self.protocol.transport
    
    @property
    def protocol(self):
        return self._protocol
    
    @property
    def sock(self):
        return self._protocol.sock
    
    @property
    def producer(self):
        return self._producer
    
    @property
    def session(self):
        return self._session
    
    @property
    def response_factory(self):
        return self._response_factory
    
    @property
    def current_response(self):
        self._current_response
    
    def consume(self, data):
        raise NotImplementedError
    
    def upgrade(self, response_factory):
        '''Update the :attr:`response_factory` attribute with a new
:class:`ProtocolResponse`. This function can be used when the protocol
specification changes during a response (an example is a WebSocket
response).'''
        self._response_factory = response_factory
        
    def finished(self, response):
        raise NotImplementedError