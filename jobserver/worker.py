
class WorkerInterface(object):
    
    def __init__(self, conn):
        self._conn = conn
        self._ap_version = None

    def setup(self, job_data):
        """ Initializes the worker. 
            
            Should be sent as the first command and only once 
            in the lifetime of the worker. 
        """
        assert self._ap_version is None
        
        data = {
            "ap_version": "", #FIXME job_data["ap_version"],
            "ap_path": "", #FIXME
            "archive_path": "",#FIXME
            "hostname": "", # hostname to harcode in worker (hostname is part of all cloud api request)
            "type": "setup",
            "fileno": 1, # stdout for now
        }
        self._ap_version = data["ap_version"]
        self._send_msg(data)
    
    def assign(self, job_data):
        """ Sends a job to the worker and yields all updates sent back.
            
            Update types:

                "processing"    kind of "ack" for assign
                "profile"       optional, only if job_data["profile"] is set
                "finished"      result/tracback of execution

            Worker starts to execute the job after sending back a "processing" update.
            The type of the last update is "finished".
        """
        fpickled = job_data["func_obj_pickled"]
        fargspickled = job_data["func_args_pickled"]
        fkwargspickled = job_data["func_kwargs_pickled"]
        
        payload = fpickled
        if fargspickled:
            payload += fargspickled
        if fkwargspickled:
            payload += fkwargspickled

        payload_parts = [len(fpickled), len(fargspickled)]
        payload_length = len(payload)
    
        data = {
            "type": "assign",
            "cores": job_data["cores"],
            "core_type": job_data["core_type"],
            "jid": job_data["pk"],
            "payload_parts": payload_parts,
            "payload_length": payload_length,
            "api_key": job_data["apikey_id"] ,
            "api_secretkey": job_data["api_secretkey"],
            "server_url": job_data["server_url"],
            "ujid": None, #job_data["jid"],
            "job_type": job_data["job_type"],
            "profile": job_data["profile"],
            "fast_serialization": job_data["fast_serialization"],
        }
        self._send_msg(data, payload)
        
        while True:
            message, payload = self._recv_msg()
            assert message["type"] in ('finished', 'processing', 'profile')

            # convert response to our data schema
            if message["type"] == "finished":
                if "traceback" in data:
                    message.pop("traceback")
                    message["exception"] = payload
                else:
                    message["result_pickled"] = payload
            
            if message["type"] == "profile":
                message["profile"] = payload
            
            message["pk"] = job_data["pk"]
            message["group_id"] = job_data["group_id"]
            
            yield message
            
            # stop iteration if executin is finished
            if message["type"] == "finished":
                return
    
    def die(self):
        """ Shuts the worker down.
            
            Effective only when worker is waiting for a new command.
            This *will not kill* the worker if it's busy.
        """
        self._send_msg({"type": "die"})

    def _recv_msg(self):
        data = self._conn.recv_json()
        if "payload_length" in data:
            payload = self._conn.recv(data["payload_length"])
            data.pop("payload_length")
        else:
            payload = None

        return (data, payload)
    
    def _send_msg(self, data, payload=None):
        if "payload_length" in data:
            assert payload is not None
        
        self._conn.send_json(data)
        if "payload_length" in data:
            self._conn.send(payload)
        return 
