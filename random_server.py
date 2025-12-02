#!/usr/bin/env python3
"""
Random server that fronts multiple Lambda containers (RIE) and forwards
requests to them with capacity + token control.
"""

import os
import sys
import uuid
import time
import json
from typing import List, Dict, Any

import boto3
import requests
from flask import Flask, jsonify, request
import threading  # NEW: 用于各种锁

app = Flask(__name__)
container_uuid = str(uuid.uuid4())
request_count = 0


# ─────────────────────────────────────────
# 基础并发原语
# ─────────────────────────────────────────

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


class BackendSlot:
    """
    NEW: 每个容器对应一个 backend slot：
    - url: 这个容器的 invocations URL（带 host:port）
    - lock: 保证单容器单并发
    - busy: 是否已被某个 token 占用
    """
    def __init__(self, url: str):
        self.url = url
        self.lock = threading.Lock()
        self.busy = False


# 全局：backend 列表 + token -> backend 映射
backends: List[BackendSlot] = []          # NEW
token_backend_map: Dict[str, int] = {}    # NEW: token -> backend 索引
token_backend_lock = threading.Lock()     # NEW: 保护 map
capacity: AtomicInteger                   # 在 main 里初始化


def assign_backend_for_token(token: str) -> BackendSlot:
    """
    NEW: 为一个 token 选一个空闲 backend，并标记 busy。
    如果没有空闲 backend，返回 None。
    """
    with token_backend_lock:
        for idx, backend in enumerate(backends):
            if not backend.busy:
                backend.busy = True
                token_backend_map[token] = idx
                return backend
    return None


def get_backend_for_token(token: str) -> BackendSlot:
    """NEW: 根据 token 找到对应 backend，如果没有则返回 None。"""
    with token_backend_lock:
        idx = token_backend_map.get(token)
    if idx is None or not (0 <= idx < len(backends)):
        return None
    return backends[idx]


def release_backend_for_token(token: str) -> None:
    """NEW: 释放 token 关联的 backend，让它可以重新被使用。"""
    with token_backend_lock:
        idx = token_backend_map.pop(token, None)
    if idx is not None and 0 <= idx < len(backends):
        backends[idx].busy = False


class TimeoutCache:
    """
    带 TTL 的 token 存储：
    - get(key): 检查 token 是否还有效（未过期）
    - put(key, ttl): 写入 token 和过期时间
    - remove(key) -> bool: 删除 token，返回是否真的存在
    - clear(): 清理过期 token，并归还 capacity + 释放 backend
    """
    def __init__(self):
        self.store = {}
        self._lock = threading.Lock()  # NEW: 线程安全

    def get(self, key):
        """Return True if key is valid (not expired), False otherwise."""
        now = time.time()
        with self._lock:
            expiry = self.store.get(key)
            if expiry is None:
                return False
            if now > expiry:
                # expired: 清理但不在这里还 capacity，集中在 clear() 做
                self.store.pop(key, None)
                return False
            return True

    def put(self, key, ttl):
        now = time.time()
        expiry = now + float(ttl)
        with self._lock:
            self.store[key] = expiry
        return expiry

    def remove(self, key) -> bool:
        """删除 key，返回是否真的存在，用于避免 double free。"""
        with self._lock:
            existed = key in self.store
            if existed:
                self.store.pop(key, None)
            return existed

    def clear(self):
        """
        清理已过期 token：
        - 从 store 删除
        - 归还 capacity
        - 释放 backend
        """
        now = time.time()
        expired_keys = []
        with self._lock:
            for key, expiry in list(self.store.items()):
                if now > expiry:
                    self.store.pop(key, None)
                    expired_keys.append(key)

        # 在锁外释放资源，避免长时间持锁
        for key in expired_keys:
            capacity.increment()
            release_backend_for_token(key)


# 这两个会在 main 里初始化
pass_token_map: TimeoutCache  # type: ignore


def cleanup_token(token: str):
    """
    NEW: 统一 token 清理逻辑：
    - 从 TimeoutCache 删除
    - capacity +1
    - 释放 backend
    """
    removed = pass_token_map.remove(token)
    if removed:
        capacity.increment()
        release_backend_for_token(token)


# ─────────────────────────────────────────
# Flask handlers
# ─────────────────────────────────────────

@app.route("/rlb/reserve", methods=["POST"])
def pre_occupy():
    """
    预留一个执行 slot：
    - 如果有空闲 capacity + backend，就返回 token
    - 否则返回 fail
    """
    request_data = request.get_json()
    pass_token_map.clear()
    ttl = int(request_data["reserve_timeout_sec"])

    # 先看 capacity
    if capacity.get_value() > 0:
        remaining = capacity.decrement()
        if remaining < 0:
            capacity.increment()
            return {"status": "fail"}
        else:
            new_uuid = str(uuid.uuid4())

            # NEW: 为这个 token 分配一个 backend
            backend = assign_backend_for_token(new_uuid)
            if backend is None:
                # 理论上不应该发生，如果发生说明 backend 状态脏了，回滚 capacity
                capacity.increment()
                return {"status": "fail"}

            # 写入 TTL
            expired_time = pass_token_map.put(new_uuid, ttl)
            return {
                "status": "success",
                "token": new_uuid,
                "expired_time": expired_time,
            }
    return {"status": "fail"}


@app.route("/rlb/execute", methods=["POST"])
def execute():
    """
    使用预留的 token 执行：
    - sync: 阻塞直到 Lambda 返回，返回结果
    - async: 后台线程执行，立即返回 {"status": "success"}
    """
    request_data = request.get_json()
    token = request_data["token"]
    eventType = request_data["eventType"]
    try:
        if not pass_token_map.get(token):
            return {
                "status": "fail",
                "message": "You do not get the token to run this function",
            }

        backend = get_backend_for_token(token)  # NEW: 找到具体哪一个容器
        if backend is None:
            return {
                "status": "fail",
                "message": "No backend associated with this token",
            }

        data = request_data["payload"]

        if eventType == "sync":
            # NEW: 对这个 backend 加锁，保证单容器单并发
            with backend.lock:
                response = requests.post(backend.url, json=data)
            result = response.json()
            cleanup_token(token)  # 同步调用结束后释放
            return result

        # async: 起线程执行，但还是按 backend.lock 串行这个容器
        t = threading.Thread(
            target=post_and_return,
            args=(token, data),
            daemon=False,
        )
        t.start()
        return {"status": "success"}
    except Exception as e:
        print(e)
        # sync 出异常也要释放资源
        if eventType == "sync":
            cleanup_token(token)
        return {"status": "fail"}


def post_and_return(token, data):
    """
    异步执行：
    - 根据 token 找 backend
    - 在 backend.lock 下调用 Lambda
    - 最后 cleanup_token
    """
    try:
        backend = get_backend_for_token(token)
        if backend is None:
            return
        with backend.lock:
            response = requests.post(backend.url, json=data)
        _ = response.json()  # 目前没用到结果，保留接口
    except Exception as e:
        print(e)
    finally:
        cleanup_token(token)


@app.route("/rlb/execute_without_reserve", methods=["POST"])
def execute_without_reserve():
    """
    不经过预留，直接抢占一个 slot 执行：
    - 成功时内部也生成一个 token，绑定 backend，用完后释放
    """
    new_uuid = None
    try:
        request_data = request.get_json()  # CHANGED: 先读 request_data
        eventType = request_data["eventType"]
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

                # NEW: 为这个 token 分配 backend
                backend = assign_backend_for_token(new_uuid)
                if backend is None:
                    capacity.increment()
                    return {"status": "fail"}

                # TTL 固定写个 2 秒（保留你原来逻辑）
                pass_token_map.put(new_uuid, 2)
                data = request_data["payload"]

                if eventType == "sync":
                    with backend.lock:
                        response = requests.post(backend.url, json=data)
                    result = response.json()
                    cleanup_token(new_uuid)
                    return result
                else:
                    t = threading.Thread(
                        target=post_and_return,
                        args=(new_uuid, data),
                        daemon=False,
                    )
                    t.start()
                    return {"status": "success"}
        print("no remaining capacity when check " + str(current_capacity))
        return {"status": "fail"}
    except Exception as e:
        print(e)
        if new_uuid is not None:
            cleanup_token(new_uuid)
        return {"status": "fail"}


# ─────────────────────────────────────────
# DynamoDB / 实例状态相关逻辑（基本保持不变）
# ─────────────────────────────────────────

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
            Key={"ec2_id": instance_id},  # your primary key
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


# ─────────────────────────────────────────
# main：初始化 capacity / backends / pass_token_map
# ─────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python random_server.py <capacity>")

    capacity_value = int(sys.argv[1])               # capacity = 容器数
    capacity = AtomicInteger(capacity_value)
    pass_token_map = TimeoutCache()

    # NEW: 根据环境变量 BACKEND_BASE_PORT & BACKEND_HOST 构造多个 backend
    base_port = int(os.environ.get("BACKEND_BASE_PORT", "8080"))
    backend_host = os.environ.get("BACKEND_HOST", "localhost")

    for i in range(capacity_value):
        port = base_port + i
        url = f"http://{backend_host}:{port}/2015-03-31/functions/function/invocations"
        backends.append(BackendSlot(url))

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
