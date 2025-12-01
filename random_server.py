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
import threading

app = Flask(__name__)
container_uuid = str(uuid.uuid4())
request_count = 0


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
        """Return True if key is valid (and consume it), False otherwise."""
        now = time.time()
        expiry = self.store.get(key)
        if expiry is None:
            return False
        if now > expiry:
            # expired: clean it up
            self.store.pop(key, None)
            return False
        return True

    def put(self, key, ttl):
        now = time.time()
        expiry = now + float(ttl)
        self.store[key] = expiry
        return expiry

    def remove(self, key):
        self.store.pop(key, None)

    def clear(self):
        """Remove expired keys and give capacity back."""
        now = time.time()
        # copy items first to avoid dict-size-change error
        for key, expiry in list(self.store.items()):
            if now > expiry:
                self.store.pop(key, None)
                # capacity is a global AtomicInteger
                capacity.increment()


@app.route("/rlb/reserve", methods=["POST"])
def pre_occupy():
    request_data = request.get_json()
    pass_token_map.clear()
    ttl = int(request_data["reserve_timeout_sec"])
    if capacity.get_value() > 0:
        remaining = capacity.decrement()
        if remaining < 0:
            capacity.increment()
            print()
            return {"status": "fail"}
        else:
            new_uuid = str(uuid.uuid4())
            # add expire later
            expired_time = pass_token_map.put(new_uuid, ttl)
            return {
                "status": "success",
                "token": new_uuid,
                "expired_time": expired_time,
            }
    return {"status": "fail"}


# Should we return error if no space
@app.route("/rlb/execute", methods=["POST"])
def execute():
    request_data = request.get_json()
    token = request_data["token"]
    eventType = request_data["eventType"]
    try:
        url = "http://localhost:8080/2015-03-31/functions/function/invocations"
        data = request_data["payload"]
        if not pass_token_map.get(token):
            return {
                "status": "fail",
                "message": "You do not get the token to run this function",
            }
        if eventType == "sync":
            response = requests.post(url, json=data)
            return response.json()
        t = threading.Thread(target=post_and_return, args=(url, data, token), daemon=False)
        t.start()
        return {"status": "success"}
    except Exception as e:
        print(e)
        return {"status": "fail"}
    finally:
        # safe even if already removed
        if eventType == "sync":
            pass_token_map.remove(token)
            capacity.increment()

def post_and_return(url, data, token):
    response = requests.post(url, json=data)
    pass_token_map.remove(token)
    capacity.increment()
    return response.json()

# Should we return error if no space
@app.route("/rlb/execute_without_reserve", methods=["POST"])
def execute_without_reserve():
    new_uuid = None
    eventType = request_data["eventType"]
    try:
        pass_token_map.clear()
        current_capacity = capacity.get_value()
        if current_capacity > 0:
            remaining = capacity.decrement()
            if remaining < 0:
                print("no remaining capacity when decr " + str(remaining))
                capacity.increment()
                return {"status": "fail"}
            else:
                new_uuid = str(uuid.uuid4())
                # add expire later
                pass_token_map.put(new_uuid, 2)
                url = "http://localhost:8080/2015-03-31/functions/function/invocations"
                request_data = request.get_json()
                data = request_data["payload"]
                if eventType == "sync":
                    response = requests.post(url, json=data)
                    pass_token_map.remove(new_uuid)
                    capacity.increment()
                    return response.json()
                else:
                    t = threading.Thread(target=post_and_return, args=(url, data, new_uuid), daemon=False)
                    t.start()
                    return {"status": "success"}
        print("no remaining capacity when check " + str(current_capacity))
        return {"status": "fail"}
    except Exception as e:
        print(e)
        if new_uuid is not None:
            pass_token_map.remove(new_uuid)
        capacity.increment()
        return {"status": "fail"}


def updateInstanceStatus(instance_id, local_ip):
    dynamodb = boto3.resource("dynamodb", region_name="ca-west-1")
    decision_table = dynamodb.Table("localibou_active_global_vms_table")

    response = decision_table.update_item(
        Key={"key": instance_id},
        UpdateExpression="SET #s = :update_status, public_ip = :public_ip",
        ExpressionAttributeNames={"#s": "status"},
        ConditionExpression="#s = :original_status",
        ExpressionAttributeValues={
            ":update_status": "running",
            ":original_status": "init",
            ":public_ip": local_ip,
        },
        ReturnValues="ALL_NEW",
    )
    return response

def update_running_new_machine_history(instance_id):
    ms = time.time_ns() // 1_000_000
    try: 
        dynamodb = boto3.resource("dynamodb", region_name="ca-west-1")
        table = dynamodb.Table("localibou_ec2_status_history_log")
        response = table.update_item(
            Key={"ec2_id": instance_id},            # your primary key
            UpdateExpression="SET running_time = :t",
            ExpressionAttributeValues={
                ":t": ms
            },
        )
        return response
    except Exception as e:
        print(e)
        return None


def get_instance_id():
    try:
        # Get token
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=1,
        ).text

        # Get instance-id
        instance_id = requests.get(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=1,
        ).text

        return instance_id

    except Exception as e:
        return f"Error getting instance ID: {e}"


def get_public_ip():
    try:
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=1,
        ).text

        public_ip = requests.get(
            "http://169.254.169.254/latest/meta-data/public-ipv4",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=1,
        ).text

        return public_ip
    except Exception as e:
        return f"Error getting public IP: {e}"


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python random_server.py <capacity>")

    capacity = AtomicInteger(int(sys.argv[1]))
    pass_token_map = TimeoutCache()
    port = int(os.environ.get("PORT", 9000))
    instance_id = get_instance_id()
    print("instance_id: " + str(instance_id))
    local_ip = get_public_ip()
    print(local_ip)
    res = updateInstanceStatus(instance_id, local_ip)
    if res is not None:
        attrs = res["Attributes"]
        region = attrs["region"]
        workflow_id = attrs["workflow_id"]
        update_running_new_machine_history(instance_id)
    print(res)
    app.run(host="0.0.0.0", port=port)
