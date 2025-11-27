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

@app.route("/pre_occupy", methods=["POST"])
def pre_occupy():
    global capacity
    if capacity.get_value() > 0:
        remaining = capacity.decrement()
        if remaining < 0:
            capacity.increment()
            return {"status":"fail"}
        else:
            new_uuid = str(uuid.uuid4())
            # add expire later
            pass_token_map[new_uuid] = ""
            return {
                    "status":"success",
                    "token":new_uuid,
                    }
    return {"status": "fail"}


# Should we return error if no space
@app.route("/test_routing", methods=["POST"])
def test_routing():
    try:
        url = "http://localhost:8080/2015-03-31/functions/function/invocations"
        request_data = request.get_json()
        data = request_data["data"]
        token = request_data["token"]
        if token not in pass_token_map.keys():
            return {"status": "fail", "message": "You do not get the token to run this function"}
        response = requests.post(url, json=data)
        return response.json()
    except Exception as e:
        print(e)
        return {"status": "fail"}
    finally:
        capacity.increment()
        
def updateInstanceStatus(instance_id):
    dynamodb = boto3.resource("dynamodb", region_name="ca-west-1")
    decision_table = dynamodb.Table("localibou_active_global_vms_table")

    response = decision_table.update_item(
        Key={"key": instance_id},
        UpdateExpression="SET #s = :update_status",
        ExpressionAttributeNames={
            "#s": "status"
        },
        ConditionExpression="#s = :original_status",
        ExpressionAttributeValues={
            ":update_status": "running",
            ":original_status": "init",
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

# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    capacity = AtomicInteger(int(sys.argv[1]))
    pass_token_map = {}
    port = int(os.environ.get("PORT", 8080))
    instance_id = get_instance_id()
    print("-----------------------------------")
    print(instance_id)
    res = updateInstanceStatus(instance_id)
    print("===================================")
    print(res)
    app.run(host="0.0.0.0", port=port)
