import gevent
import gevent.monkey
gevent.monkey.patch_all()

from gevent.queue import Queue

import os
import sys

import zerorpc

import logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


from workergateway import WorkerGateway
from worker import Worker
from qmanager import JobQueueManager, ScalerClient
from logstream import Service

if __name__ == '__main__':
    try:
        REDIS_HOST = os.environ["REDIS_HOST"]
        REDIS_PORT = int(os.environ["REDIS_PORT"])
        REDIS_DB = int(os.environ["REDIS_DB"])
        GATEWAY = os.environ["GATEWAY"]
        ENDPOINT = os.environ["ENDPOINT"]
        SCALER_ENDPOINT = os.environ["SCALER_ENDPOINT"]
    except KeyError:
        print "error: please set GATEWAY, ENDPOINT, SCALER_ENDPOINT, REDIS_HOST, REDIS_PORT, REDIS_DB environment variables"
        sys.exit(1)
    except ValueError:
        print "error: REDIS_PORT or REDIS_DB is not an integer"
        
    scaler_cli = ScalerClient(gateway=GATEWAY, endpoint=SCALER_ENDPOINT)
    qm = JobQueueManager(scaler_cli=scaler_cli, worker_class=Worker)

    worker_gateway = WorkerGateway(GATEWAY, qm._add_worker_connection)
    worker_gateway.start()

    qm.start()

    server = zerorpc.Server(Service())
    server.bind(ENDPOINT)
    try:
        server.run()
    except Exception, e:
        print e
    finally:
        print "shuting down..."
        worker_gateway.stop()
        qm.stop()
