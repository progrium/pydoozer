import os
import struct

import gevent
import gevent.event
import gevent.socket
import google.protobuf.message

from msg_pb2 import Response
from msg_pb2 import Request

CONNECT_TIMEOUT = 10
DEFAULT_URI = "doozerd:?%s" % "&".join([
    "ca=127.0.0.1:8046",
    "ca=127.0.0.1:8041",
    "ca=127.0.0.1:8042",
    "ca=127.0.0.1:8043",
    ])

def to_dict(message):
    return dict([(field.name, value) for field, value in message.ListFields()])

def parse_uri(uri):
    if uri.startswith("doozerd:?"):
        before, params = uri.split("?", 1)
        addrs = []
        for param in params.split("&"):
            key, value = param.split("=", 1)
            if key == "ca":
                addrs.append(value)
        return addrs
    else:
        raise ValueError("invalid doozerd uri")

def connect(uri=None):
    uri = uri or os.environ.get("DOOZER_URI", DEFAULT_URI)
    addrs = parse_uri(uri)
    if not addrs:
        raise ValueError("there were no addrs supplied in the uri (%s)" % uri)
    return Client(addrs)

class OutOfNodes(Exception): pass
class ResponseError(Exception):
    def __init__(self, response):
        self.code = response.err_code
        self.detail = response.err_detail
        self.response = response
    
    def __str__(self):
        return str(to_dict(response))

class Connection(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        with gevent.Timeout(CONNECT_TIMEOUT) as timeout:
            self.sock = gevent.socket.create_connection((host, int(port)))
        self.pending = {}
        gevent.spawn(self.recv)
    
    @property
    def address(self):
        return "%s:%s" % (self.host, self.port)
    
    def disconnect(self):
        self.sock.close()
    
    def send(self, request):
        request.tag = 0
        while request.tag in self.pending:
            request.tag += 1
            request.tag %= 2**31
        self.pending[request.tag] = gevent.event.AsyncResult()
        data = request.SerializeToString()
        head = struct.pack(">I", len(data))
        self.sock.send(''.join([head, data]))
        response = self.pending[request.tag].get()
        del self.pending[request.tag]
        if 'err_code' in [field.name for field, value in response.ListFields()]:
            raise ResponseError(response)
        return response
    
    def recv(self):
        while True:
            head = self.sock.recv(4)
            length = struct.unpack(">I", head)[0]
            data = self.sock.recv(length)
            response = Response()
            response.ParseFromString(data)
            if response.tag in self.pending:
                self.pending[response.tag].set(response)

class Client(object):
    def __init__(self, addrs=None):
        if addrs is None:
            addrs = []
        self.addrs = addrs
        self.connection = None
        self.connect()
    
    def rev(self):
        request = Request(verb=Request.REV)
        return self._send(request)
        
    def set(self, path, value, rev):
        request = Request(path=path, value=value, rev=rev, verb=Request.SET)
        return self._send(request)
        
    def get(self, path, rev=None):
        request = Request(path=path, verb=Request.GET)
        if rev:
            request.rev = rev
        return self._send(request)
    
    def delete(self, path, rev):
        request = Request(path=path, rev=rev, verb=Request.DEL)
        return self._send(request)
    
    def wait(self, path, rev):
        request = Request(path=path, rev=rev, verb=Request.WAIT)
        return self._send(request)
    
    def stat(self, path, rev):
        request = Request(path=path, rev=rev, verb=Request.STAT)
        return self._send(request)
    
    def access(self, secret):
        request = Request(value=secret, verb=Request.ACCESS)
        return self._send(request)
    
    def _getdir(self, path, offset=0, rev=None):
        request = Request(path=path, offset=offset, verb=Request.GETDIR)
        if rev:
            request.rev = rev
        return self._send(request)
        
    def _walk(self, path, offset=0, rev=None):
        request = Request(path=path, offset=offset, verb=Request.WALK)
        if rev:
            request.rev = rev
        return self._send(request)
    
    def watch(self, path, rev):
        raise NotImplementedError()
    
    def _list(self, method, path, offset=None, rev=None):
        offset = offset or 0
        entities = []
        try:
            while True:
                response = getattr(self, method)(path, offset, rev)
                entities.append(response)
                offset += 1
        except ResponseError, e:
            if e.code == Response.RANGE:
                return entities
            else:
                raise e
            
    def walk(self, path, offset=None, rev=None):
        return self._list('_walk', path, offset, rev)
    
    def getdir(self, path, offset=None, rev=None):
        return self._list('_getdir', path, offset, rev)
        
    
    def disconnect(self):
        self.connection.disconnect()
    
    def reconnect(self):
        self.disconnect()
        self.connect()
        
    def connect(self):
        while len(self.addrs):
            try:
                host, port = self.addrs.pop(0).split(':')
                self.connection = self.connection_to(host, port)
                return
            except IOError:
                pass
        raise OutOfNodes("where did they go?")
        
    def connection_to(self, host, port):
        return Connection(host, port)
                
    def _send(self, request):
        return self.connection.send(request)