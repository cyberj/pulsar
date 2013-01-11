from .transports import ServerTransport

__all__ = ['Protocol', 'ProtocolResponse', 'ClientResponse',
           'ClientProtocol', 'ServerProtocol', 'ProtocolError',
           'ConcurrentServer']

class ConcurrentServer(object):
    
    def __new__(cls, *args, **kwargs):
        o = super(ConcurrentServer, cls).__new__(cls)
        o.received = 0
        o.concurrent_requests = set()
        return o
        
    @property
    def concurrent_request(self):
        return len(self.concurrent_requests)
    
    
class ProtocolError(Exception):
    '''Raised when the protocol encounter unexpected data. It will close
the socket connection.'''


class ProtocolResponse(object):
    '''A :class:`Protocol` response is responsible for parsing incoming data.'''
    def __init__(self, protocol):
        self._protocol = protocol
        self._finished = False
            
    @property
    def event_loop(self):
        return self._protocol.event_loop
    
    @property
    def sock(self):
        return self._protocol.sock
    
    @property
    def protocol(self):
        return self._protocol
    
    @property
    def transport(self):
        return self._protocol.transport
        
    def feed(self, data):
        '''Feed new data into the :class:`ProtocolResponse`'''
        raise NotImplementedError
    
    def finished(self):
        '''`True` if this response has finished.'''
        return self._finished
    
    def write(self, data):
        if is_async(data):
            self.event_loop.call_soon(self.write, data)
        else:
            self.protocol.transport.write(data)
            
    def writelines(self, lines):
        '''Write an iterable of bytes. It is a proxy to
:meth:`Transport.writelines`'''
        self.transport.writelines(self._generate(lines))
        
    def _generate(self, lines):
        self.transport.pause()
        try:
            for data in lines:
                yield data
        finally:
            self.transport.resume()


class ClientResponse(ProtocolResponse):
    
    def __init__(self, protocol, request):
        super(ClientResponse, self).__init__(protocol)
        self.request = request
        self._protocol.set_response(self)
    
    def on_connect(self):
        pass
    
    def begin(self):
        raise NotImplementedError
        
        
class Protocol(object):
    '''Base class for a pulsar :class:`Protocol` conforming with pep-3156_.
    
.. attribute:: transport

    The :class:`Transport` for this :class:`Protocol`. This is obtained once
    the :meth:`connection_made` is invoked.
    
.. attribute:: response

    The :class:`ProtocolResponse` factory for this :class:`Protocol`
'''
    _transport = None
    response = None
    
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
    
    def __repr__(self):
        if self.sock:
            return repr(self.sock)
        else:
            return '<closed>'
    
    def __str__(self):
        return '%s @ %s' % (self.__class__.__name__, repr(self))
    
    def connection_made(self, transport):
        """Called when a connection is made.

        The argument is the :class:`Transport` representing the connection.
        To send data, call its :meth:`Transport.write` or
        :meth:`Transport.writelines` method.
        To receive data, wait for :meth:`data_received` calls.
        When the connection is closed, :meth:`connection_lost` is called.
        """
        self._transport = transport

    def data_received(self, data):
        """Called by the :attr:`transport` when some data is received.
The argument is a bytes object."""
            
    def eof_received(self):
        """Called when the other end calls write_eof() or equivalent."""

    def connection_lost(self, exc):
        """Called when the connection is lost or closed.

        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """
        
    ############################################################################
    ###    TRANSPORT METHODS SHORTCUT
    def close(self):
        if self._transport:
            self._transport.close()
    
    def abort(self):
        if self._transport:
            self._transport.abort()
    

class ClientProtocol(Protocol):
    '''Base :class:`Protocol` for clients/server sockets.

* a *client* protocol is for clients connecting to a remote server.
* a *server* protocol is for socket created from an **accept** on a
  server socket.

.. attribute:: address

    Address of the client, if this is a server, or of the remote
    server if this is a client.
    
.. attribute:: processed

    Number of separate requests processed by this protocol.
    
.. attribute:: response_factory

    A :class:`ProtocolResponse` class for handling a single request.
'''
    def __init__(self, address, response_factory=None):
        self._processed = 0
        self._address = address
        self._current_response = None
        self._response_factory = response_factory
    
    def __repr__(self):
        return str(self._address)
        
    @property
    def processed(self):
        return self._processed
    
    @property
    def address(self):
        return self._address
    
    def connect(self, sock):
        '''Connect the client to the remote server.'''
        raise NotImplementedError
    
    def connection_made(self, transport):
        self._transport = transport
        if self._current_response is not None:
            self._current_response.on_connect()
            self._current_response.begin()
        
    def set_response(self, response):
        assert self._current_response is None, "protocol already in response"
        self._current_response = response
        if self._transport is not None:
            self._current_response.begin()
    
    def data_received(self, data):
        while data:
            response = self._current_response
            if response is not None and response.finished():
                response = None
            if response is None:
                self._processed += 1
                self._current_response = response = self._response_factory(self)
            data = response.feed(data)
            if data:
                if not response.finished():
                    raise ProtocolError
            
            
class ServerProtocol(Protocol, ConcurrentServer):
    '''Base class for all Server's protocols.
    
.. attribute:: protocol

    The :class:`Protocol` for a socket created from a connection of a remote
    client with this server. It is usually a subclass of
    :class:`ClientProtocol`.
'''
    protocol = ClientProtocol
    
    def create_transport(self, event_loop, sock):
        ServerTransport(event_loop, sock, self)
    
    @property
    def address(self):
        if self.transport:
            return self.transport.address