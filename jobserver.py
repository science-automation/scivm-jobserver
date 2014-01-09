from gevent.server import StreamServer
from gevent import socket, spawn, monkey
from gevent.queue import Queue
import gevent
monkey.patch_all()

import os
import random
import sys
import time
import pickle
import redis
import zlib

from play import play


class QueueHandler(object):
    
    def __init__(self):
        self.connected = gevent.event.Event()
        self.connected.clear()
        
        self.to_push = gevent.queue.Queue()
        self.to_fetch = gevent.queue.Queue()
        self.abandoned_q = gevent.queue.Queue()

        self.cli = None
        self.fetcher_coro = None
        self.pusher_coro = None
        self.abandoned_coro = None

    def _get_client(self):
        while True:
            print "trying to connect redis..."
            try:
                cli = redis.StrictRedis(host="localhost", port=6379, db=0)
                cli.set("dummy", "dummy")
                print cli.get("dummy")
                print "redis connected"
                return cli
            except:
                print "redis is offline, trying to connect..."
                gevent.sleep(5)
    
    def connect(self):
        self.connected.clear()
        self.cli = self._get_client()
        self.connected.set()
    
    def _pop(self):
        while True:
            try:
                self.connected.wait()
                queue, payload = self.cli.blpop("noq.jobs.queued")
                return payload
            except:
                print "redis is unavailable"
                self.connect()
    
    def _push(self, data):
        while True:
            try:
                self.connected.wait()
                self.cli.lpush("noq.jobs.finished", data)
                #print "pushed"
                return
            except:
                print "redis is unavailable"
                self.connect()
    
    def push(self, data):
        #print "qpush"
        self.to_push.put(zlib.compress(pickle.dumps(data)))

    def pop(self):
        asyncr = gevent.event.AsyncResult()
        self.to_fetch.put(asyncr)
        #print "wait pop" 
        raw, data = asyncr.get()
        #print "popped"
        if raw:
            return pickle.loads(zlib.decompress(data))
        else:
            return data

    def to_abandoned(self, job_data):
        print "abandoned job {0}".format(job_data["pk"])
        self.abandoned_q.put(job_data)

    def _fetcher(self):
        while True:
            for res in self.to_fetch:
                data = self._pop()
                res.set((True, data))
    
    def _abandoned_fetcher(self):
        while True:
            self.abandoned_q.peek()
            res = self.to_fetch.get() 
            job_data = self.abandoned_q.get()
            res.set((False, job_data))

    def _pusher(self):
        while True:
            for payload in self.to_push:
                data = self._push(payload)

    def start(self):
        self.connect()
        self.fetcher_coro = gevent.spawn(self._fetcher)
        self.pusher_coro = gevent.spawn(self._pusher)
        self.abandoned_coro = gevent.spawn(self._abandoned_fetcher)
    
    def kill(self):
        if self.fetcher_coro:
            self.fetcher_coro.kill()
        if self.pusher_coro:
            self.pusher_coro.kill()
        if self.abandoned_coro:
            self.abandoned_coro.kill()


class Worker(object):
    
    def __init__(self, jm, id, socket):
        self.id = id
        self.socket = socket
        self.jm = jm
        self.q = jm.q
        self.coro = None
        self.cur_job = None

    def spawn(self):
        self.coro = gevent.spawn(self.do)
        return self.coro

    def kill(self):
        self.coro.kill()

    def do(self):
        while True:
            print 'worker {0} waiting for job'.format(self.id)
            job_data = self.q.pop()
            self.cur_job = job_data
            print 'worker {0} got job {1}'.format(self.id, job_data["pk"])
            try:
                update = play(job_data, self.socket)
            except IOError, e:
                self.q.to_abandoned(job_data) #FIXME restartable?
                self.jm.deregister_worker(self)
                self.kill()
                return 
            except Exception, e:
                print e
                update = {"runtime": 0.0, "exception": "system error? {0}".format(str(e))} 
            print 'worker {0} got result of job {1}'.format(self.id, job_data["pk"])
            
            if 'exception' in update:
                print 'worker {0} got exception for job {1}: {2}'.format(self.id, job_data["pk"], update["exception"])
            
            update["finished_at"] = time.time()
            update["pk"] = job_data["pk"]
            update["group_id"] = job_data["group_id"]
            self.q.push(update)
            self.cur_job = None


class JobServer(object):

    def __init__(self):
        self.q = QueueHandler()
        self.q.start()
        
        self.workers = {}
        self._next_worker_id = 0

        self.sockets = gevent.queue.Queue()
    
    @property
    def next_worker_id(self):
        try:
            return self._next_worker_id
        finally:
            self._next_worker_id += 1    

    def add_worker(self, socket):
        worker = Worker(self, self.next_worker_id, socket)
        self.workers[worker.id] = worker
        print "new worker {0}".format(worker.id)
        worker.spawn()
    
    def deregister_worker(self, worker):
        print "worker {0} went away".format(worker.id)
        del self.workers[worker.id]

    def kill(self):
        for worker in self.workers.itervalues():
            worker.kill()
        self.q.kill()



if __name__ == '__main__':
    try:
        SERVER_ADDRESS = os.environ["SERVER_ADDRESS"]
        SERVER_PORT = int(os.environ["SERVER_PORT"])
    except KeyError:
        print "error: please set SERVER_ADDRESS and SERVER_PORT environment variables"
        sys.exit(1)
    except ValueError:
        print "error: SERVER_PORT environment variable must be an integer"
        sys.exit(1)
    
    jserver = JobServer()
    
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((SERVER_ADDRESS, SERVER_PORT))
    listener.listen(1)
    
    def echo(socket, address):
        socket.settimeout(None)
        jserver.add_worker(socket)
    server = StreamServer(listener, echo)
    print "accepting connections...."
    
    try:
        server.serve_forever()
    except Exception, e:
        print e
    finally:
        jserver.kill()
        listener.close()
