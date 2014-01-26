import gevent
import gevent.monkey
gevent.monkey.patch_all()

from gevent.server import StreamServer
from gevent import socket, spawn
from gevent.queue import Queue

import os
import random
import sys
import time
import pickle
import redis
import zlib

import zerorpc

import logging
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

from workergateway import WorkerConnection, WorkerGateway, WorkerGone
from worker import WorkerInterface
#from qh import QueueHandler, Queues
from qmanager import JobQueueManager

class Worker(object):
    
    def __init__(self, id, qm, conn):
        self.id = id
        self.conn = conn
        self._worker = WorkerInterface(self.conn)
        self.qm = qm
        self.coro = None
    
    @property
    def qdesc(self):
        return self._worker.qdesc

    def spawn(self):
        self.coro = gevent.spawn(self.do)
        return self.coro

    def kill(self):
        print "killing worker {0}".format(self.id)
        self.coro.kill()

    def do(self):
        worker = self._worker 
        print worker.qdesc
        self.q = self.qm.register_worker(self)
        c = 0
        while True:
            print 'worker {0} waiting for job'.format(self.id)
            job_data = self.q.pop()
            self.cur_job = job_data
            print 'worker {0} got job {1}'.format(self.id, job_data["pk"])
            try:
                if c == 0:
                    worker.setup(job_data) 
                updates = worker.assign(job_data)
                for update in updates:
                    print update
                    self.q.push(update)
            except (WorkerGone, IOError), e:
                self.q.abandon(job_data) #FIXME restartable?
                self.qm.deregister_worker(self)
                self.kill()
                return 
            #except Exception, e:
            #    print e
            #    update = {"runtime": 0.0, "exception": "system error? {0}".format(str(e))} 
            print 'worker {0} got result of job {1}'.format(self.id, job_data["pk"])
            
            if 'exception' in update:
                print 'worker {0} got exception for job {1}: {2}'.format(self.id, job_data["pk"], update["exception"])
            
            #self.q.push(update)
            c += 1


class JobServer(object):

    def __init__(self, qm):
        self.qm = qm
        self._next_worker_id = 0

    @property
    def next_worker_id(self):
        try:
            return self._next_worker_id
        finally:
            self._next_worker_id += 1    

    def add_worker_connection(self, conn):
        worker = Worker( self.next_worker_id, self.qm, conn)
        print "new worker {0}".format(worker.id)
        worker.spawn()
    
    def start(self):
        pass

    def stop(self):
        pass


if __name__ == '__main__':
    try:
        GATEWAY = os.environ["GATEWAY"]
        ENDPOINT = os.environ["ENDPOINT"]
    except KeyError:
        print "error: please set GATEWAY and ENDPOINT environment variables"
        sys.exit(1)
    
    qm = JobQueueManager()
    qm.start()

    jserver = JobServer(qm)
    jserver.start()
    
    worker_gateway = WorkerGateway(GATEWAY, jserver.add_worker_connection)
    worker_gateway.start()

    class Service():
        
        def ping(self):
            return "pong"
        
    server = zerorpc.Server(Service())
    server.bind(ENDPOINT)
    try:
        server.run()
    except Exception, e:
        print e
    finally:
        print "shuting down..."
        worker_gateway.stop()
        jserver.stop()
        qm.stop()
