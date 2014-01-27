import gevent
import logging
import zerorpc
import redis
import pickle
import zlib

logger = logging.getLogger()


class JobQueueManager(object):
    
    def __init__(self):
        self._queues = {}
        self._cli = redis.StrictRedis(host="localhost", port=6379, db=0)
        self._loop_coro = None
        self._updater_coro = None
        self._updater_q = gevent.queue.Queue()
        self._qlock = gevent.coros.RLock()

    def get_or_create_queue(self, qname):
        q = None
        with self._qlock:
           if qname not in self._queues:
               logger.info("starting queue {0}".format(qname))
               q = JobQueue(self, qname)
               self._queues[qname] = q
               q.start()
           return q or self._queues[qname]
    
    def remove_queue(self, qname):
        with self._qlock:
            if qname in self._queues:
                logger.info("removing {0}".format(qname))
                q = self._queues[qname] 
                q.stop()
                del self._queues[qname]
            else:
                logger.info("cant remove {0}".format(qname))
            
    def get_queue(self, qname):
        return self._queues.get(qname, None)
    
    def register_worker(self, worker):
        logger.debug("registering worker {0}".format(worker.id))
        q = self.get_queue(worker.qdesc)
        if q is None:
            logger.error("no matching queue for worker {0}".format(worker.id))
            worker.stop()
            return None
        q._workers.add(worker)
        return q

    def deregister_worker(self, worker):
        logger.debug("deregistering worker {0}".format(worker.id))
        q = self.get_queue(worker.qdesc)
        q._workers.remove(worker)
        worker.stop()
        return
    
    def stop(self):
        self._updater_q.put(None)
        if self._loop_coro:
            gevent.kill(self._loop_coro)
        
        if self._updater_coro:
            gevent.kill(self._updater_coro)
        
        for qname in self._queues.keys():
            self.remove_queue(qname)

    def start(self):
        self._loop_coro = gevent.spawn(self.loop)
        self._updater_coro = gevent.spawn(self._updater)

    def loop(self):
        while True:
            logger.debug("waiting for queued jobs")
            queue, payload = self._cli.brpop("noq.jobs.queued")
            data = pickle.loads(zlib.decompress(payload))
            logger.debug("get queued job {0}".format(data["pk"],))
            
            apikey_id = data["apikey_id"]
            env = data.get("envrionment", "default")
            group_id = data.get("group_id", "default")

            qname = "noq.jobs.queued.{0}.{1}.{2}".format(apikey_id, env, group_id)
            #self._cli.lpush(qname, payload)
            logger.debug("get queued job {0} to {1}".format(data["pk"], qname))
            q = self.get_or_create_queue(qname)
            q.queue(data)
    
    def _updater(self):
        while True:
            logger.info("\n-------------------------------")
            logger.info("QUEUES:")
            for q in self._queues.itervalues():
                logger.info(q)
            gevent.sleep(3)

        """
        for update in self._updater_q:
            if update is None:
                break
            logger.debug("pushing update for job {0}".format(update["pk"]))
            payload = zlib.compress(pickle.dumps(update))
            self._cli.lpush("noq.jobs.updates", payload)
        """ 


class JobQueue(object):
    
    def __init__(self, qm, qname):
        self._qname = qname
        self._qm = qm
        self._q = gevent.queue.Queue()
        
        self._workers = set()

        self._queued = set()
        self._taken = set()
        self._processing = set()
        self._finished = 0
        
        self._stopped = False

        self._zcli = zerorpc.Client("tcp://127.0.0.1:9999")
        self._cli = redis.StrictRedis()
    
    @classmethod
    def load_from_redis(cls, qdesc):
        raise NotImplementedError
    
    def _move(self, from_, to, pk):
        from_.remove(pk)
        to.add(pk)

    def _drop(self, from_, pk):
        from_.remove(pk)

    def queue(self, job_desc):
        pk = job_desc["pk"]
        logger.debug("queuing job {0} in {1}".format(pk, self._qname))
        self._queued.add(job_desc["pk"])
        self._q.put_nowait(job_desc)

    def take(self):
        try:
            job_desc = self._q.get(timeout=10)
        except gevent.queue.Empty:
            return None
        pk = job_desc["pk"]
        self._move(self._queued, self._taken, pk)
        logger.debug("assigning job {0} from {1}".format(pk, self._qname))
        return job_desc
    
    def abandon(self, job_desc):
        pk = job_desc["pk"]
        logger.debug("job {0} is returned back to {1}".format(pk, self._qname))
        if pk in self._taken:
            self._move(self._taken, self._queued, pk)
            self._q.put(job_desc) #TODO to the end of the q??
            return

        if pk in self._processing:
            # TODO if not restartable ?
            self._move(self._processing, self._queued, pk)
            self._q.put(job_desc) #TODO to the end of the q??
            return

        logger.error("job {0} is dropped ?!") #TODO

    def update(self, data):
        pk = data["pk"]
        
        if data["type"] == "processing":
            self._move(self._taken, self._processing, pk)

        if data["type"] == "finished":
            self._drop(self._processing, pk)
            self._finished += 1
        
        payload = zlib.compress(pickle.dumps(data))
        return self._cli.lpush("noq.jobs.updates", payload)
        self.qm._updater_q.put_nowait(data)
    
    def start(self):
        self._loop_coro = gevent.spawn(self.loop)
    
    def stop(self):
        self._stopped = True
        if self._loop_coro:
            gevent.kill(self._loop_coro)
        del self._zcli

    def __str__(self):
        return "{0}".format(self._qname).ljust(50) + "q:{0} t:{1} p:{2} f:{3} --  w:{4}".format(
                    len(self._queued),
                    len(self._taken),
                    len(self._processing),
                    self._finished,
                    len(self._workers),
                )

    def loop(self):
        self._idle = 0 
        try:
            while True:
                gevent.sleep(3)
                if self._stopped:
                    break
                ql = len(self._queued)
                if ql != 0:
                    wc = len(self._workers) or 1
                    v = ql / wc
                    #print v, ql, wc
                    if v > 0:
                        self._start_worker(1)
                if len(self._queued) == len(self._taken) == len(self._processing) == len(self._workers) == 0:
                    self._idle +=1 
                
                if self._idle > 3:
                    self._qm.remove_queue(self._qname)
                    break
        except gevent.GreenletExit:
            pass

    def _start_worker(self, count):
        logger.debug("asking for {0} workers for {1}".format(count, self._qname))
        for i in xrange(0, count):
           try:
               self._zcli.start_worker("127.0.0.1", 4444, self._qname, timeout=3)
           except:
               logger.debug("error asking")

"""
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
            gevent.kill(self._loop_coro)

    def loop(self):
        while True:
            if self._cli.llen(self._qname):
                if not len(self._workers):
                    self._start_worker(1)
            gevent.sleep(3)
    
    def _start_worker(self, count):
        logger.debug("asking for workers for {0}".format(self._qname))
        try:
            self._zcli.start_worker("127.0.0.1", 4444, self._qname)
        except:
            logger.debug("error asking")
"""        
