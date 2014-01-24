import gevent
import gevent.socket as socket
import gevent.server

import json


class WorkerGateway(object):
    """ Accepts tcp connections from workers and 
        puts connection objects to a queue 
    """

    def __init__(self, endpoint, new_conn_queue):
        self._address, self._port = endpoint.split(":")
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((address, port))
        self._socket.listen(1)
        self._server = gevent.server.StreamServer(self._socket, self._accept)
        
    def _accept(self, socket, address):
        socket.settimeout(None)
        conn = WorkerConnection(socket, address)
        new_conn_queue.put_nowait(conn)

    def start(self):
        self._server_coro = gevent.spawn(self._server.serve_forever)

    def stop(self, timeout=None):
        if self._server_coro:
            self._server_coro.kill(timeout=timeout)


class WorkerConnection(object):
    """ Connection adapter for a worker """

    def __init__(self, socket, address):
        self._socket = socket
        self._address = address
    
    @property
    def address(self):
        return "{0}:{1}".format(self._address[0], self._address[1])

    def send(self, data):
        self._socket.sendall(data)

    def recv(self, count):
        received = ""
        need = count
        while need > 0:
            chunk = self._socket.recv(need)
            need -= len(chunk)
            received += chunk
        return received
    
    def send_json(self, data, chunk_size=1024):
        payload = json.dumps(data)
        #FIXME chunk_size < len(payload)
        return self.send(payload + " " * (chunk_size - len(payload)))
        
    def recv_json(self, chunk_size=1024):
        raw = self.recv(chunk_size).rstrip()
        return json.loads(raw)
    
    def close(self):
        self._socket.close()
