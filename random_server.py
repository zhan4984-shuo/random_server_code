#!/usr/bin/env python3
"""
Flask service that reports detailed host/container information and runs a
simple CPU-stress benchmark.
"""

import os
import sys
import uuid
import time
import math
import subprocess
import traceback
import platform
import json
import multiprocessing as mp
from typing import List, Dict, Any

import boto3
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
container_uuid = str(uuid.uuid4())
request_count = 0


import threading

class AtomicInteger:
    def __init__(self, initial_value=0):
        self._value = initial_value
        self._lock = threading.Lock()

    def increment(self, n=1):
        with self._lock:
            self._value += n
            return self._value

    def decrement(self, n=1):
        with self._lock:
            self._value -= n
            return self._value

    def get_value(self):
        with self._lock:
            return self._value
        
class TimeoutCache:
    def __init__(self):
        self.store = {}

    def get(self, key):
        now = time.time()
        if key in self.store.keys():
            timestamp = self.store[key]
            if now <= timestamp:
                self.store.pop(key, None)
                return True 
        return False
    
    def put(self, key, ttl):
        now = time.time()
        self.store[key] = ttl + now
        return ttl + now
    
    def remove(self, key):
        self.store.pop(key, None)
    

@app.route("/rlb/reserve", methods=["POST"])
def pre_occupy():
    global capacity
    request_data = request.get_json()
    ttl = int(request_data["reserve_timeout_sec"])
    if capacity.get_value() > 0:
        remaining = capacity.decrement()
        if remaining < 0:
            capacity.increment()
            print()
            return {"status":"fail"}
        else:
            new_uuid = str(uuid.uuid4())
            # add expire later
            expired_time = pass_token_map.put(new_uuid, ttl)
            return {
                    "status":"success",
                    "token":new_uuid,
                    "expired_time": expired_time
                    }
    return {"status": "fail"}


# Should we return error if no space
@app.route("/rlb/execute", methods=["POST"])
def execute():
    try:
        url = "http://localhost:8080/2015-03-31/functions/function/invocations"
        request_data = request.get_json()
        data = request_data["payload"]
        token = request_data["token"]
        if not pass_token_map.get(token):
            return {"status": "fail", "message": "You do not get the token to run this function"}
        response = requests.post(url, json=data)
        return response.json()
    except Exception as e:
        print(e)
        return {"status": "fail"}
    finally:
        pass_token_map.remove(token)
        capacity.increment()
        
# Should we return error if no space
@app.route("/rlb/execute_withou_reserve", methods=["POST"])
def execute_withou_reserve():
    try:
        global capacity
        if capacity.get_value() > 0:
            remaining = capacity.decrement()
            if remaining < 0:
                print("no remaining capacity when decr" + str(remaining))
                capacity.increment()
                return {"status":"fail"}
            else:
                new_uuid = str(uuid.uuid4())
                # add expire later
                pass_token_map.put(new_uuid, 2)
                url = "http://localhost:8080/2015-03-31/functions/function/invocations"
                request_data = request.get_json()
                data = request_data["payload"]
                response = requests.post(url, json=data)
                pass_token_map.remove(new_uuid)
                capacity.increment()
                return response.json()
        print("no remaining capacity when check" + str(remaining))
    except Exception as e:
        print(e)
        pass_token_map.remove(new_uuid)
        capacity.increment()
        return {"status": "fail"}
        
def updateInstanceStatus(instance_id, local_ip):
    dynamodb = boto3.resource("dynamodb", region_name="ca-west-1")
    decision_table = dynamodb.Table("localibou_active_global_vms_table")

    response = decision_table.update_item(
        Key={"key": instance_id},
        UpdateExpression="SET #s = :update_status, public_ip = :public_ip",
        ExpressionAttributeNames={
            "#s": "status"
        },
        ConditionExpression="#s = :original_status",
        ExpressionAttributeValues={
            ":update_status": "running",
            ":original_status": "init",
            ":public_ip": local_ip
        },
        ReturnValues="ALL_NEW",
    )
    return response

def get_instance_id():
    try:
        # Get token
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=1
        ).text

        # Get instance-id
        instance_id = requests.get(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=1
        ).text

        return instance_id

    except Exception as e:
        return f"Error getting instance ID: {e}"

def get_public_ip():
    try:
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=1
        ).text

        public_ip = requests.get(
            "http://169.254.169.254/latest/meta-data/public-ipv4",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=1
        ).text

        return public_ip
    except Exception as e:
        return f"Error getting public IP: {e}"

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    capacity = AtomicInteger(int(sys.argv[1]))
    pass_token_map = TimeoutCache()
    port = int(os.environ.get("PORT", 9000))
    instance_id = get_instance_id()
    local_ip = get_public_ip()
    print(local_ip)
    res = updateInstanceStatus(instance_id, local_ip)
    app.run(host="0.0.0.0", port=port)
