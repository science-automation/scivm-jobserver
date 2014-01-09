import time
import pickle
import json


def play(job_data, socket):
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
    
    def send(socket, payload, size=1024):
        payload = json.dumps(payload)
        return socket.send(payload + " " * (size - len(payload)))

    def receive_json(socket):
        rets = [ x.strip() for x in socket.recv(1024).rsplit(' ', 1) ]
        #print rets
        return json.loads(rets[0])
    
    def receive(socket, payload_len): 
        return socket.recv(payload_len)
    
    # -- send setup params
    send(socket, {
            "ap_version": "",
            "ap_path": "",
            "archive_path": "",
            "hostname": "rekk",
            "type": "setup",
            "fileno": 0, # stdin
    })


    # -- send job
    send(socket, {
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
    })


    # -- send job data 
    socket.send(payload)


    # -- get status update (processing)
    while True:
        data = receive_json(socket)
        if data["type"] == "finished":
            if "traceback" in data:
                data["exception"] = receive(socket, data.pop("payload_length"))
            else:
                data["result_pickled"] = receive(socket, data.pop("payload_length"))
            data.pop("type")
            return data
    
    #send(socket, {
    #    "type": "die" 
    #i)
