#!/usr/bin/env python3
"""
Random server that fronts multiple Lambda containers (RIE) and forwards
requests to them with capacity + token control.

- 启动方式: python3 random_server.py <capacity>
    capacity = 本机容器数量（replicas）
- 需要环境变量:
    BACKEND_BASE_PORT: 第一个容器映射的 host 端口 (默认 8080)
    BACKEND_HOST: 容器 host 地址 (默认 localhost)
"""

import os
import sys
import uuid
import time
import json
from typing import List, Dict, Any

import boto3
import requests
from flask import Flask, request
import threading
import logging
from logging.handlers import RotatingFileHandler
from typing import Tuple


RESERVE = "Reserve_count"
EXECUTE = "Execute_count"
EXECUTE_NO_RESERVE = "Execute_no_reserve_count"

# ─────────────────────────────────────────
# Logging setup
# ─────────────────────────────────────────

def setup_logging():
    """
    初始化 logging:
    - 优先写到 /var/log/random_server.log
    - 如果失败则写到 /tmp/random_server.log
    - 再失败就退回 stdout
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    log_paths = [
        "/var/log/random_server_print.log",
    ]

    handler = None
    for path in log_paths:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            handler = RotatingFileHandler(
                path,
                maxBytes=10 * 1024 * 1024,  # 10 MB
                backupCount=5,
            )
            break
        except Exception as e:
            # 不能写这个 path，尝试下一个
            # 这里不能用 logger（还没完全配置），直接用 basicConfig 输出一下
            logging.basicConfig(level=logging.INFO)
            logging.getLogger(__name__).warning(
                "Failed to init file handler at %s: %s", path, e
            )

    if handler is None:
        # 全部失败，退回 stdout
        handler = logging.StreamHandler(sys.stdout)

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # 降低 werkzeug 的噪音（但还保留基本 info）
    logging.getLogger("werkzeug").setLevel(logging.INFO)


setup_logging()
logger = logging.getLogger(__name__)


app = Flask(__name__)
container_uuid = str(uuid.uuid4())
request_count = 0

# ====== 关机相关全局变量 ======
is_shutting_down = False          # 是否进入 shutdown 状态
total_capacity = 0                # 初始总 capacity（在 main 里赋值）
shutdown_lock = threading.Lock()  # 防止多线程重复 terminate
termination_started = False       # 只允许 terminate_instances 调用一次
# ============================

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
    每个容器对应一个 backend slot：
    - url: 这个容器的 invocations URL（带 host:port）
    - lock: 保证该容器单并发
    - busy: 是否已被某个 token 占用
    """
    def __init__(self, url: str):
        self.url = url
        self.lock = threading.Lock()
        self.busy = False


# 全局：backend 列表 + token -> backend 映射
backends: List[BackendSlot] = []
token_backend_map: Dict[str, int] = {}
token_backend_lock = threading.Lock()
capacity: AtomicInteger  # 在 main 里初始化
num_of_req_map = {}
time_of_exec_map = {}

# round-robin 起点
next_backend_index = 0


def terminate_self(instance_id: str):
    region = get_current_region_from_imds()
    ec2 = boto3.client("ec2", region_name=region)
    logger.info(
        "[SHUTDOWN] calling terminate_instances on self: %s in %s",
        instance_id,
        region,
    )
    ec2.terminate_instances(InstanceIds=[instance_id])


def assign_backend_for_token(token: str) -> BackendSlot:
    """
    为一个 token 选一个 backend（round-robin）：
    - 从 next_backend_index 开始绕一圈
    - 找到第一个 not busy 的 backend
    - 标记为 busy，并更新 next_backend_index
    """
    global next_backend_index
    with token_backend_lock:
        n = len(backends)
        if n == 0:
            logger.error("No backends available when assigning token %s", token)
            return None

        start = next_backend_index
        for offset in range(n):
            idx = (start + offset) % n
            backend = backends[idx]
            if not backend.busy:
                backend.busy = True
                token_backend_map[token] = idx
                # 下次从这个 backend 的下一个开始
                next_backend_index = (idx + 1) % n
                logger.debug(
                    "Assigned backend index %d to token %s (url=%s)",
                    idx,
                    token,
                    backend.url,
                )
                return backend

        # 没有空闲 backend
        logger.info("All backends busy when assigning token %s", token)
        return None


def get_backend_for_token(token: str) -> Tuple[BackendSlot, int]:
    """根据 token 找到对应 backend，如果没有则返回 None。"""
    with token_backend_lock:
        idx = token_backend_map.get(token)
    if idx is None or not (0 <= idx < len(backends)):
        logger.warning("No backend found for token %s", token)
        return None
    return backends[idx], idx


def release_backend_for_token(token: str) -> None:
    """释放 token 关联的 backend，让它可以重新被使用。"""
    with token_backend_lock:
        idx = token_backend_map.pop(token, None)
    if idx is not None and 0 <= idx < len(backends):
        backends[idx].busy = False
        logger.debug("Released backend index %d for token %s", idx, token)


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
        self._lock = threading.Lock()

    def get(self, key):
        """Return True if key is valid (not expired), False otherwise."""
        now = time.time()
        with self._lock:
            expiry = self.store.get(key)
            if expiry is None:
                return False
            if now > expiry:
                # 过期了，只在这里删除，不在这里还 capacity
                self.store.pop(key, None)
                logger.info("Token %s expired (TimeoutCache.get)", key)
                return False
            return True

    def put(self, key, ttl):
        now = time.time()
        expiry = now + float(ttl)
        with self._lock:
            self.store[key] = expiry
        logger.debug("Token %s put with ttl=%s, expiry=%s", key, ttl, expiry)
        return expiry

    def remove(self, key) -> bool:
        """删除 key，返回是否真的存在，用于避免 double free。"""
        with self._lock:
            existed = key in self.store
            if existed:
                self.store.pop(key, None)
                logger.debug("Token %s removed from TimeoutCache", key)
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

        # 在锁外做释放
        for key in expired_keys:
            logger.info("Token %s expired (TimeoutCache.clear)", key)
            capacity.increment()
            release_backend_for_token(key)


pass_token_map: TimeoutCache  # 在 main 里初始化


def try_shutdown_if_idle():
    """
    如果已经进入 shutdown 且所有 capacity 都空闲，
    先更新 DynamoDB 为 terminated，然后调用 EC2 API 终止当前实例。
    """
    global termination_started

    # 1. 还没进入 shutdown，直接返回
    if not is_shutting_down:
        return

    # 2. 还有容量在用，不能关机
    if capacity.get_value() != total_capacity:
        return

    # 3. 防止多线程重复终止
    with shutdown_lock:
        if termination_started:
            return
        termination_started = True

    # 4. 先更新 DynamoDB，再 terminate 实例
    try:
        instance_id = get_instance_id()
        logger.info(
            "[SHUTDOWN] all capacity free for instance %s, updating status to terminated",
            instance_id,
        )

        # 写 terminated_time + status = terminated
        update_terminated_time(instance_id)
        update_terminated_instance_status(instance_id)

        region = (
            os.environ.get("AWS_REGION")
            or os.environ.get("AWS_DEFAULT_REGION")
            or "ca-west-1"
        )
        logger.info("[SHUTDOWN] Terminating instance %s in region %s", instance_id, region)
        ec2 = boto3.client("ec2", region_name=region)
        ec2.terminate_instances(InstanceIds=[instance_id])
    except Exception as e:
        logger.exception("[SHUTDOWN] Error when terminating instance: %s", e)


def cleanup_token(token: str):
    """
    统一 token 清理逻辑：
    - 从 TimeoutCache 删除
    - capacity +1
    - 释放 backend
    - 如果 shutdown 且空闲，则尝试关机
    """
    removed = pass_token_map.remove(token)
    if removed:
        new_cap = capacity.increment()
        logger.debug(
            "Cleanup token %s: capacity incremented to %s", token, new_cap
        )
        release_backend_for_token(token)
        try_shutdown_if_idle()


# ─────────────────────────────────────────
# Flask handlers
# ─────────────────────────────────────────

@app.route("/rlb/reserve", methods=["POST"])
def pre_occupy():
    if is_shutting_down:
        return {"status": "shutting-down"}
    """
    预留一个执行 slot：
    - 如果有空闲 capacity + backend，就返回 token
    - 否则返回 fail
    """
    num_of_req_map[RESERVE].increment()
    request_data = request.get_json()
    pass_token_map.clear()
    ttl = int(request_data["reserve_timeout_sec"])

    current_cap = capacity.get_value()
    if current_cap > 0:
        remaining = capacity.decrement()
        if remaining < 0:
            capacity.increment()
            logger.warning(
                "[RESERVE] capacity negative after decrement, rollback. current=%s",
                current_cap,
            )
            return {"status": "fail"}
        else:
            new_uuid = str(uuid.uuid4())

            backend = assign_backend_for_token(new_uuid)
            if backend is None:
                # backend 不够，回滚 capacity
                capacity.increment()
                logger.warning(
                    "[RESERVE] no backend available, rollback capacity. token=%s",
                    new_uuid,
                )
                return {"status": "fail"}

            expired_time = pass_token_map.put(new_uuid, ttl)
            logger.info(
                "[RESERVE] success token=%s, ttl=%s, expiry=%s, remaining_capacity=%s",
                new_uuid,
                ttl,
                expired_time,
                remaining,
            )
            return {
                "status": "success",
                "token": new_uuid,
                "expired_time": expired_time,
            }
    logger.info(
        "[RESERVE] no capacity available, current_capacity=%s", current_cap
    )
    return {"status": "fail"}


@app.route("/rlb/execute", methods=["POST"])
def execute():
    if is_shutting_down:
        return {"status": "shutting-down"}
    """
    使用预留的 token 执行：
    - sync: 阻塞直到 Lambda 返回，返回结果
    - async: 后台线程执行，立刻返回 {"status": "success"}
    """
    num_of_req_map[EXECUTE].increment()
    request_data = request.get_json()
    token = request_data["token"]
    eventType = request_data["eventType"]
    try:
        if not pass_token_map.get(token):
            logger.warning(
                "[EXECUTE] invalid or expired token=%s", token
            )
            return {
                "status": "fail",
                "message": "You do not get the token to run this function",
            }

        backend, idx = get_backend_for_token(token)
        if backend is None:
            logger.warning(
                "[EXECUTE] no backend associated with token=%s", token
            )
            return {
                "status": "fail",
                "message": "No backend associated with this token",
            }

        data = request_data["payload"]

        if eventType == "sync":
            with backend.lock:
                logger.info(
                    "[EXECUTE] sync request routing to url: %s (token=%s)",
                    backend.url,
                    token,
                )
                start_execute_time = time.time_ns() // 1_000_000
                response = requests.post(backend.url, json=data)
                time_of_exec_map[idx].increment(
                    (time.time_ns() // 1_000_000) - start_execute_time
                )
            result = response.json()
            cleanup_token(token)
            return result

        # async
        t = threading.Thread(
            target=post_and_return,
            args=(token, data),
            daemon=False,
        )
        t.start()
        logger.info("[EXECUTE] async request accepted for token=%s", token)
        return {"status": "success"}
    except Exception as e:
        logger.exception("[EXECUTE] error with token=%s: %s", token, e)
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
        backend, idx = get_backend_for_token(token)
        if backend is None:
            logger.warning(
                "[ASYNC] no backend found for token=%s in post_and_return", token
            )
            return
        with backend.lock:
            logger.info(
                "[ASYNC] routing async request to url: %s (token=%s)",
                backend.url,
                token,
            )
            start_execute_time = time.time_ns() // 1_000_000
            response = requests.post(backend.url, json=data)
            time_of_exec_map[idx].increment(
                    (time.time_ns() // 1_000_000) - start_execute_time
            )
        _ = response.json()
    except Exception as e:
        logger.exception("[ASYNC] error for token=%s: %s", token, e)
    finally:
        cleanup_token(token)


@app.route("/rlb/execute_without_reserve", methods=["POST"])
def execute_without_reserve():
    if is_shutting_down:
        return {"status": "shutting-down"}
    """
    不经过预留，直接抢占一个 slot 执行：
    - 成功时内部也生成一个 token，绑定 backend，用完后释放
    """
    num_of_req_map[EXECUTE_NO_RESERVE].increment()
    new_uuid = None
    try:
        request_data = request.get_json()
        logger.info("[EXECUTE_NO_RESERVE] request_data=%s", request_data)
        eventType = request_data["eventType"]
        pass_token_map.clear()
        current_capacity = capacity.get_value()
        if current_capacity > 0:
            remaining = capacity.decrement()
            if remaining < 0:
                logger.warning(
                    "[EXECUTE_NO_RESERVE] no remaining capacity when decr, remaining=%s",
                    remaining,
                )
                capacity.increment()
                return {"status": "fail"}
            else:
                new_uuid = str(uuid.uuid4())

                backend = assign_backend_for_token(new_uuid)
                if backend is None:
                    capacity.increment()
                    logger.warning(
                        "[EXECUTE_NO_RESERVE] no backend available, rollback capacity"
                    )
                    return {"status": "fail"}

                # 固定 TTL=2 秒，保持你原来的逻辑
                pass_token_map.put(new_uuid, 2)
                data = request_data["payload"]

                if eventType == "sync":
                    with backend.lock:
                        logger.info(
                            "[EXECUTE_NO_RESERVE] sync routing to url=%s (token=%s)",
                            backend.url,
                            new_uuid,
                        )
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
                    logger.info(
                        "[EXECUTE_NO_RESERVE] async accepted (token=%s)", new_uuid
                    )
                    return {"status": "success"}
        logger.info(
            "[EXECUTE_NO_RESERVE] no remaining capacity when check, current=%s",
            current_capacity,
        )
        return {"status": "fail"}
    except Exception as e:
        logger.exception("[EXECUTE_NO_RESERVE] error: %s", e)
        if new_uuid is not None:
            cleanup_token(new_uuid)
        return {"status": "fail"}


@app.route("/rlb/begin_shutdown", methods=["POST"])
def begin_shutdown():
    """
    标记当前实例进入关机状态：
    - 更新 DynamoDB status 为 'shutting-down'
    - 在 history 表里记录 shutdown_time
    - 把全局 is_shutting_down 设为 True
    - 之后所有 /rlb/reserve /rlb/execute /rlb/execute_without_reserve
      都会直接返回 {"status": "shutting-down"}

    额外：
    - 如果 total_capacity == current_capacity（说明没有在跑的请求）
      -> 在 active 表里把 status 记成 'terminated'
      -> 在 history 表里写 terminated_timestamp
      -> 直接调用 EC2 terminate 自己
    """
    global is_shutting_down, total_capacity, capacity

    instance_id = get_instance_id()

    # 1. 标记为 shutting-down + 记录 shutdown_time
    update_shutting_down_instance_status(instance_id)
    update_shutdown_time(instance_id)

    is_shutting_down = True
    logger.info("[SHUTDOWN] instance %s is entering shutting-down state", instance_id)

    # 2. 用现成的 total_capacity / current_capacity 做一次检查
    current_cap = capacity.get_value()
    logger.info(
        "[SHUTDOWN] capacity check: total_capacity=%s, current_capacity=%s",
        total_capacity,
        current_cap,
    )

    will_terminate_now = False

    if total_capacity == current_cap:
        # 说明已经没在跑的活了，可以直接认为“空闲可删”
        will_terminate_now = True
        logger.info(
            "[SHUTDOWN] no inflight work; marking terminated & terminating self"
        )

        ddb = boto3.resource("dynamodb", region_name="ca-west-1")
        
        time_exec_map = {}
        num_req_map = {}
        for i in time_of_exec_map.keys():
            time_exec_map[i] = time_of_exec_map[i].get_value()
        for i in num_of_req_map.keys():
            num_req_map[i] = num_of_req_map[i].get_value()
        

        # 2.1 更新 active 表里的状态 -> terminated
        active_table = ddb.Table("localibou_active_global_vms_table")
        active_table.update_item(
            Key={"key": instance_id},
            UpdateExpression=(
                "SET #s = :terminated"
            ),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":terminated": "terminated",
            },
        )

        # 2.2 在 history 表里写 terminated_timestamp
        history_table = ddb.Table("localibou_ec2_status_history_log")
        ms = time.time_ns() // 1_000_000
        
        history_table.update_item(
            Key={"ec2_id": instance_id},
            UpdateExpression=(
                "SET terminated_timestamp = :ts, "
                "exec_time_map = :exec_map, "
                "req_num_map = :req_map"
                              
            ),
            ExpressionAttributeValues={
                ":ts": ms,
                ":exec_map": str(time_exec_map),
                ":req_map": str(num_of_req_map),
            },
        )

        # 2.3 调用 EC2 terminate self
        terminate_self(instance_id)

    else:
        logger.info(
            "[SHUTDOWN] still have inflight work, stay in shutting-down (total=%s, current=%s)",
            total_capacity,
            current_cap,
        )

    return {
        "status": "ok",
        "is_shutting_down": True,
        "total_capacity": total_capacity,
        "current_capacity": current_cap,
        "will_terminate_now": will_terminate_now,
    }


# ─────────────────────────────────────────
# DynamoDB / 实例状态相关逻辑
# ─────────────────────────────────────────

def updateInstanceStatus(instance_id, local_ip):
    """
    启动时：从 init -> running，同时写入 public_ip
    """
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
    logger.info(
        "[STATUS] instance %s moved to 'running' with public_ip=%s", instance_id, local_ip
    )
    return response


def update_shutting_down_instance_status(instance_id):
    """
    进入 shutting-down 阶段时：status = 'shutting-down'
    """
    try:
        dynamodb = boto3.resource("dynamodb", region_name="ca-west-1")
        decision_table = dynamodb.Table("localibou_active_global_vms_table")

        response = decision_table.update_item(
            Key={"key": instance_id},
            UpdateExpression="SET #s = :sd",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":sd": "shutting-down"},
            ReturnValues="UPDATED_NEW",
        )
        logger.info(
            "[SHUTDOWN] Updated DynamoDB status to 'shutting-down' for %s",
            instance_id,
        )
        return response
    except Exception as e:
        logger.exception(
            "[SHUTDOWN] Failed to update shutting-down status in DynamoDB for %s: %s",
            instance_id,
            e,
        )
        return None


def update_terminated_instance_status(instance_id):
    """
    即将 terminate 前：status = 'terminated'
    """
    try:
        dynamodb = boto3.resource("dynamodb", region_name="ca-west-1")
        decision_table = dynamodb.Table("localibou_active_global_vms_table")

        response = decision_table.update_item(
            Key={"key": instance_id},
            UpdateExpression="SET #s = :terminated",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":terminated": "terminated"},
            ReturnValues="UPDATED_NEW",
        )
        logger.info(
            "[SHUTDOWN] Updated DynamoDB status to 'terminated' for %s", instance_id
        )
        return response
    except Exception as e:
        # 打 log，但不要阻止真正的 terminate
        logger.exception(
            "[SHUTDOWN] Failed to update terminated status in DynamoDB for %s: %s",
            instance_id,
            e,
        )
        return None


def update_running_new_machine_history(instance_id):
    """
    在 history 表里记录 running_time（毫秒时间戳）
    """
    ms = time.time_ns() // 1_000_000
    try:
        dynamodb = boto3.resource("dynamodb", region_name="ca-west-1")
        table = dynamodb.Table("localibou_ec2_status_history_log")
        response = table.update_item(
            Key={"ec2_id": instance_id},
            UpdateExpression="SET running_time = :t",
            ExpressionAttributeValues={":t": ms},
        )
        logger.info(
            "[HISTORY] Recorded running_time for instance %s: %s", instance_id, ms
        )
        return response
    except Exception as e:
        logger.exception(
            "[HISTORY] Failed to record running_time for %s: %s", instance_id, e
        )
        return None


def update_shutdown_time(instance_id):
    """
    在 history 表里记录 shutdown_time（毫秒时间戳）
    """
    ms = time.time_ns() // 1_000_000
    try:
        dynamodb = boto3.resource("dynamodb", region_name="ca-west-1")
        table = dynamodb.Table("localibou_ec2_status_history_log")
        response = table.update_item(
            Key={"ec2_id": instance_id},
            UpdateExpression="SET shutdown_time = :t",
            ExpressionAttributeValues={":t": ms},
        )
        logger.info(
            "[HISTORY] Recorded shutdown_time for %s: %s", instance_id, ms
        )
        return response
    except Exception as e:
        logger.exception(
            "[HISTORY] Failed to record shutdown_time for %s: %s", instance_id, e
        )
        return None


def update_terminated_time(instance_id):
    """
    在 history 表里记录 terminated_time（毫秒时间戳）
    """
    ms = time.time_ns() // 1_000_000
    try:
        dynamodb = boto3.resource("dynamodb", region_name="ca-west-1")
        table = dynamodb.Table("localibou_ec2_status_history_log")
        response = table.update_item(
            Key={"ec2_id": instance_id},
            UpdateExpression="SET terminated_time = :t",
            ExpressionAttributeValues={":t": ms},
        )
        logger.info(
            "[HISTORY] Recorded terminated_time for %s: %s", instance_id, ms
        )
        return response
    except Exception as e:
        logger.exception(
            "[HISTORY] Failed to record terminated_time for %s: %s", instance_id, e
        )
        return None


def get_instance_id():
    try:
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=1,
        ).text

        instance_id = requests.get(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=1,
        ).text

        return instance_id

    except Exception as e:
        logger.exception("Error getting instance ID: %s", e)
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
        logger.exception("Error getting public IP: %s", e)
        return f"Error getting public IP: {e}"
    
import requests

def get_current_region_from_imds(timeout: float = 1.0) -> str:
    """
    使用 EC2 Instance Metadata Service (IMDSv2) 获取当前实例所在的 AWS Region。
    只在 EC2 上可用。

    :param timeout: 每次 HTTP 请求的超时时间（秒）
    :return: 形如 'us-west-2' 的 region 字符串
    :raises RuntimeError: 如果无法从 IMDS 取到 region
    """
    try:
        # 1) 获取 IMDSv2 token（等价于 shell 的第一条 curl）
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=timeout,
        ).text

        # 2) 用 token 去拿 instance-identity document（等价于第二条 curl）
        resp = requests.get(
            "http://169.254.169.254/latest/dynamic/instance-identity/document",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=timeout,
        )
        resp.raise_for_status()
        doc = resp.json()

        region = doc.get("region")
        if not region:
            raise RuntimeError(f"No 'region' field in identity document: {doc}")
        return region

    except Exception as e:
        raise RuntimeError(f"Unable to determine region from IMDS: {e}")



# ─────────────────────────────────────────
# main：初始化 capacity / backends / pass_token_map
# ─────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python random_server.py <capacity>")


    capacity_value = int(sys.argv[1])  # capacity = 容器数（replicas）
    capacity = AtomicInteger(capacity_value)
    pass_token_map = TimeoutCache()

    # 初始状态不在 shutdown
    is_shutting_down = False
    # 记录总 capacity，用于判断“是否完全空闲”
    total_capacity = capacity_value

    # BACKEND_BASE_PORT: 第一个容器的 host 端口，默认 8080
    base_port = int(os.environ.get("BACKEND_BASE_PORT", "8080"))
    backend_host = os.environ.get("BACKEND_HOST", "localhost")

    # 根据 capacity 建立 backend 列表
    for i in range(capacity_value):
        port = base_port + i
        url = f"http://{backend_host}:{port}/2015-03-31/functions/function/invocations"
        backends.append(BackendSlot(url))

    port = int(os.environ.get("PORT", 9000))
    instance_id = get_instance_id()
    logger.info("instance_id: %s", instance_id)
    local_ip = get_public_ip()
    logger.info("public_ip: %s", local_ip)
    
    num_of_req_map = {
        RESERVE: AtomicInteger(0),
        EXECUTE: AtomicInteger(0),
        EXECUTE_NO_RESERVE: AtomicInteger(0),
    }
    
    for i in range(total_capacity):
        time_of_exec_map = {
            i: AtomicInteger(0),
        }
    
    res = updateInstanceStatus(instance_id, local_ip)
    if res is not None:
        attrs = res["Attributes"]
        region = attrs["region"]
        workflow_id = attrs["workflow_id"]
        update_running_new_machine_history(instance_id)
    logger.info("updateInstanceStatus response: %s", res)
    logger.info(
        "random_server starting on 0.0.0.0:%s with capacity=%s, backends=%s",
        port,
        capacity_value,
        [b.url for b in backends],
    )
    app.run(host="0.0.0.0", port=port, threaded=True)
