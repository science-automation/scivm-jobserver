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
logging.basicConfig(format="%(levelname)s %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


from workergateway import WorkerConnection, WorkerGateway, WorkerGone
from worker import WorkerInterface
#from qh import QueueHandler, Queues
from qmanager import JobQueueManager


class Worker(object):
    
    def __init__(self, id, qm, conn):
        self.id = id
        self._conn = conn
        self._worker = WorkerInterface(self._conn)
        self.qm = qm
        self.coro = None
    
    @property
    def qdesc(self):
        return self._worker.qdesc

    def spawn(self):
        self.coro = gevent.spawn(self.do)
        return self.coro

    def stop(self):
        logger.debug("stopping worker {0}".format(self.id))
        self._worker.stop()

    def do(self):
        worker = self._worker 
        self.q = self.qm.register_worker(self)
        if not self.q:
            return
        worker.setup({}) 
        while True:
            logger.debug('worker {0} waiting for job'.format(self.id))
            job_data = self.q.take()
            if job_data is None:
                self.qm.deregister_worker(self)
                return 
            self.cur_job = job_data
            logger.debug('worker {0} got job {1}'.format(self.id, job_data["pk"]))
            try:
                updates = worker.assign(job_data)
                for update in updates:
                    self.q.update(update)
            except (WorkerGone, IOError), e:
                self.q.abandon(job_data) #FIXME restartable?
                self.qm.deregister_worker(self)
                return 
            #except Exception, e:
            #    print e
            #    update = {"runtime": 0.0, "exception": "system error? {0}".format(str(e))} 
            logger.debug('worker {0} got result of job {1}'.format(self.id, job_data["pk"]))
            
            if 'exception' in update:
                logger.debug('worker {0} got exception for job {1}: {2}'.format(self.id, job_data["pk"], update["exception"]))
            

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
        logger.debug("new worker {0}".format(worker.id))
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
