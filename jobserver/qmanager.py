import gevent
import logging
import zerorpc
import redis
import pickle
import zlib

logger = logging.getLogger()


class JobQueue(object):
    
    def __init__(self, qname):
        self._qname = qname
        self._workers = set()
        self._cli = redis.StrictRedis()
        self._zcli = zerorpc.Client("tcp://127.0.0.1:9999")

    def pop(self):
        logger.debug("popping job from {0}".format(self._qname))
        qname, payload = self._cli.brpop(self._qname)
        job_data = pickle.loads(zlib.decompress(payload))
        return job_data

    def push(self, update):
        logger.debug("pushing update for job {0}".format(update["pk"]))
        payload = zlib.compress(pickle.dumps(update))
        return self._cli.lpush("noq.jobs.updates", payload)
    
    def abandon(self, job_data):
        logger.debug("requeueing job {0}".format(job_data["pk"]))
        payload = zlib.compress(pickle.dumps(job_data))
        return self._cli.rpush(self._qname, payload)
    
    def start(self):
        self._loop_coro = gevent.spawn(self.loop)
    
    def stop(self):
        if self._loop_coro:
            self._loop_coro.kill()

    def loop(self):
        while True:
            if self._cli.llen(self._qname):
                if not len(self._workers):
                    self._start_worker(1)
            gevent.sleep(3)
    
    def _start_worker(self, count):
        logger.debug("asking for workers for {0}".format(self._qname))
        try:
            print self._zcli.start_worker("127.0.0.1", 4444, self._qname)
        except:
            logger.debug("error asking")
        

class JobQueueManager(object):
    
    def __init__(self):
        self._queues = {}
        self._cli = redis.StrictRedis(host="localhost", port=6379, db=0)
        self._loop_coro = None

    def get_queue(self, qname):
        q = None
        if qname not in self._queues:
            logger.debug("starting queue {0}".format(qname))
            q = JobQueue(qname)
            self._queues[qname] = q
            q.start()
        return q or self._queues[qname]

    
    def register_worker(self, worker):
        logger.debug("registering worker {0}".format(worker.id))
        q = self.get_queue(worker.qdesc)
        q._workers.add(worker)
        return q

    def deregister_worker(self, worker):
        logger.debug("deregistering worker {0}".format(worker.id))
        q = self.get_queue(worker.qdesc)
        q._workers.remove(worker)
        return
    
    def stop(self):
        if self.loop_coro:
            self._loop_coro.kill()
        
        for q in self._queues.values():
            q.kill()

    def start(self):
        self._loop_coro = gevent.spawn(self.loop)

    def loop(self):
        while True:
            logger.debug("waiting for queued jobs")
            queue, payload = self._cli.brpop("noq.jobs.queued")
            data = pickle.loads(zlib.decompress(payload))
            logger.info("get queued job {0}".format(data["pk"],))
            qname = "noq.jobs.queued.{0}".format(data["apikey_id"])
            self._cli.lpush(qname, payload)
            logger.info("get queued job {0} to {1}".format(data["pk"], qname))
            self.get_queue(qname)

