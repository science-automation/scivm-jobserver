import gevent
import logging
import zerorpc
import pickle
import zlib
import rediscli
import os


logger = logging.getLogger("queue")
slogger = logging.getLogger("queue.status")

_rcli = None
def get_rcli():
    global _rcli
    if _rcli is None:
        _rcli = rediscli.StrictRedis(host=os.environ["REDIS_HOST"], port=int(os.environ["REDIS_PORT"]), db=int(os.environ["REDIS_DB"]))
    return _rcli


class Component(object):
    
    def __init__(self):
        self._stopped = gevent.event.Event()
        self._stopped.clear()

        self._coros = []

    def start(self):
        pass

    def stop(self, timeout=None):
        self._stopped.set()
        if timeout:
            gevent.sleep(timeout)
        self._kill()

    def _kill(self):
        for c in self._coros:
            gevent.kill(c)
        del self._coros


class JobQueueManager(Component):
    
    def __init__(self, scaler_cli, worker_class):
        super(JobQueueManager, self).__init__()

        self._next_worker_id = 0
        self._queues = {}
        self._updater_q = gevent.queue.Queue()
        self._qlock = gevent.coros.RLock()
        self._rcli = get_rcli()
        self._scli = scaler_cli
        self._worker_class = worker_class

    def get_or_create_queue(self, qname):
        q = None
        if qname not in self._queues:
            with self._qlock:
                logger.info("starting queue {0}".format(qname))
                q = JobQueue(self, qname)
                self._queues[qname] = q
                q.start()
        return q or self._queues[qname]
    
    def get_queue(self, qname):
        return self._queues.get(qname, None)
    
    def remove_queue(self, qname, timeout=None):
        if qname in self._queues:
            with self._qlock:
                logger.info("removing {0}".format(qname))
                q = self._queues[qname] 
                q.stop(timeout=timeout)
                del self._queues[qname]
        else:
            logger.info("cant remove {0}".format(qname))
    
    @property
    def next_worker_id(self):
        try:
            return self._next_worker_id
        finally:
            self._next_worker_id += 1    

    def _add_worker_connection(self, conn):
        worker = self._worker_class(self.next_worker_id, self, conn)
        logger.debug("new worker {0}".format(worker.id))
        worker.spawn()
            
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
    
    def stop(self, timeout=None):
        super(JobQueueManager, self).stop(timeout)
        for qname in self._queues.keys():
            self.remove_queue(qname)

    def start(self):
        self._coros = [ gevent.spawn(self._queued_fetcher), gevent.spawn(self._status_logger) ]

    def _queued_fetcher(self):
        logger.debug("waiting for queued jobs")
        while True:
            if self._stopped.is_set():
                break
            
            resp = self._rcli.brpop("noq.jobs.queued", timeout=3)
            if resp is None:
                continue

            queue, payload = resp
            data = pickle.loads(zlib.decompress(payload))
            logger.debug("get queued job {0}".format(data["pk"],))
            
            apikey_id = data["apikey_id"]
            env = data.get("env", "default")
            group_id = data.get("group_id", "default")

            qname = "noq.jobs.queued.{0}.{1}.{2}".format(apikey_id, env, group_id)
            #self._rcli.lpush(qname, payload)
            logger.debug("get queued job {0} to {1}".format(data["pk"], qname))
            q = self.get_or_create_queue(qname)
            q.queue(data)
    

    def _status_logger(self):
        while True:
            if self._stopped.is_set():
                break
            
            slogger.info("-------------------------------")
            slogger.info("QUEUES:")
            for q in self._queues.itervalues():
                slogger.info(q)
            gevent.sleep(3)


class JobQueue(Component):
    
    def __init__(self, qm, qname):
        super(JobQueue, self).__init__()

        self._qname = qname
        self._qm = qm
        self._q = gevent.queue.Queue()
        
        self._workers = set()

        self._queued = set()
        self._taken = set()
        self._processing = set()
        self._finished = 0
        
        self._rcli = get_rcli()
    
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
        # ops
        try:
            job_desc = self._q.get(timeout=3)
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
        return self._rcli.lpush("noq.jobs.updates", payload)
    
    def start(self):
        self._coros = [ gevent.spawn(self._watcher), ]
    
    def stop(self, timeout=None):
        super(JobQueue, self).stop(timeout)

    def __str__(self):
        return "{0}".format(self._qname).ljust(50) + "q:{0} t:{1} p:{2} f:{3} --  w:{4}".format(
                    len(self._queued),
                    len(self._taken),
                    len(self._processing),
                    self._finished,
                    len(self._workers),
                )

    def _watcher(self):
        self._idle = 0 
        while True:
            if self._stopped.is_set():
                break
            gevent.sleep(3)

            ql = len(self._queued)
            if ql != 0:
                wc = len(self._workers) or 1
                v = ql / wc
                if v > 0 and wc < 3:
                    self._qm._scli.start_worker(self._qname, 1)
            
            if len(self._queued) == len(self._taken) == len(self._processing) == len(self._workers) == 0:
                self._idle +=1 
            
            if self._idle > 3:
                self._qm.remove_queue(self._qname)
                break


class ScalerClient(object):
    
    def __init__(self, gateway, endpoint):
        self._gateway = gateway
        self._zcli = zerorpc.Client(endpoint)
        
    def start_worker(self, qname, count):
        logger.debug("asking for {0} workers for {1}".format(count, qname))
        try:
            self._zcli.start_worker(self._gateway, qname, count, timeout=5)
        except:
            logger.debug("error asking")
"""
class WaitingJobQueueManager(Component):
    
    def __init__(self):
        super(JobQueueManager, self).__init__()
        self._rcli = get_rcli()
    
    def start(self):
        self._coros.append(gevent.spawn(self._incoming_fetcher))
    
    def _incoming_fetcher(self):
        logger.debug("waiting for incoming jobs")
        while True:
            if self.stopped.is_set():
                break

            resp = self._rcli.brpop("noq.jobs.incoming", timeout=3)
            if resp is None:
                continue
            
            _, payload = resp
            data = pickle.loads(zlib.decompress(payload))
             
            self._rcli.lpush("noq.jobs.queued")
    

class WaintingJobQueue(Component):
    
    def __init__(self, qm, qname):
        super(WaitingJobQueue, self).__init__()

        self._qname = qname
        self._qm = qm
        self._q = gevent.queue.Queue()
        self._queued = set()
        
        self._rcli = get_rcli()
    
    @classmethod
    def load_from_redis(cls, qdesc):
        raise NotImplementedError
    
    def queue(self, job_desc):
        pk = job_desc["pk"]
        logger.debug("queuing job {0} in {1}".format(pk, self._qname))
        self._queued.add(job_desc["pk"])
        self._q.put_nowait(job_desc)

    def start(self):
        self._coros = [ gevent.spawn(self._watcher), ]
    
    def stop(self, timeout=None):
        super(JobQueue, self).stop(timeout)
    
    def __str__(self):
        return "{0}".format(self._qname).ljust(50) + "q:{0}".format(len(self._queued),)
"""
