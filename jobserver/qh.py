import gevent
import gevent.event
import gevent.queue

import redis 

import zlib
import pickle

import logging


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
            except redis.exceptions.RedisError, e:
                print "redis is offline, trying to connect..."
                gevent.sleep(5)
    
    def connect(self):
        self.connected.clear()
        if self.cli:
            try:
                self.cli.close()
            except Exception, e:
                print "exception at closing redis client", e
        self.cli = self._get_client()
        self.connected.set()
    
    def _pop(self):
        while True:
            try:
                self.connected.wait()
                queue, payload = self.cli.blpop("noq.jobs.queued")
                return payload
            except redis.exceptions.RedisError, e:
                print "redis is unavailable"
                self.connect()
    
    def _push(self, data):
        while True:
            try:
                self.connected.wait()
                self.cli.lpush("noq.jobs.finished", data)
                #print "pushed"
                return
            except redis.exceptions.RedisError, e:
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
