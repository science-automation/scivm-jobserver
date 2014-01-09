import time
import pickle
import json


def play(job_data, socket):
    API_KEY = 2
    API_SECRETKEY = "1c928172c799744fda4886adb555876bbfbb5525"

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
        "cores": 1,
        "core_type": "c1",
        "jid": 1,
        "payload_parts": payload_parts,
        "payload_length": payload_length,
        "api_key": API_KEY,
        "api_secretkey": API_SECRETKEY,
        "server_url": "http://127.0.0.1:8000/api/cloud/",
        "ujid": None,
        "job_type": "?",
        "profile": False,
        "fast_serialization": 0,
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
