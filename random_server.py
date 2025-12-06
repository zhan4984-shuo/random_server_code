#!/usr/bin/env python3
"""
Random server that fronts multiple Lambda containers (RIE) and forwards
requests to them with capacity + token control.

- Start: python3 random_server.py <capacity>
    capacity = number of local containers (replicas)
- Required environment variables:
    BACKEND_BASE_PORT: host port mapped to the first container (default 8080)
    BACKEND_HOST: container host address (default localhost)
"""

import os
import sys
import uuid
import time
import json
import subprocess
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
    Initialize logging:
    - Prefer writing to /var/log/random_server.log
    - If that fails, write to /tmp/random_server.log
    - If that still fails, fall back to stdout
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
            # Cannot write to this path, try the next one.
            # We cannot use the logger yet (not fully configured), so use basicConfig directly.
            logging.basicConfig(level=logging.INFO)
            logging.getLogger(__name__).warning(
                "Failed to init file handler at %s: %s", path, e
            )

    if handler is None:
        # All paths failed, fall back to stdout
        handler = logging.StreamHandler(sys.stdout)

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Reduce werkzeug noise (but keep basic info)
    logging.getLogger("werkzeug").setLevel(logging.INFO)


setup_logging()
logger = logging.getLogger(__name__)


app = Flask(__name__)
container_uuid = str(uuid.uuid4())
request_count = 0

# ====== Shutdown-related global variables ======
is_shutting_down = False          # Whether we have entered shutdown state
total_capacity = 0                # Initial total capacity (assigned in main)
shutdown_lock = threading.Lock()  # Prevent multiple threads from terminating twice
termination_started = False       # Ensure terminate_instances is only called once
# ==============================================

# ─────────────────────────────────────────
# Basic concurrency primitives
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
    Each container corresponds to one backend slot:
    - url: this container's invocation URL (with host:port)
    - lock: ensures single concurrency per container
    - busy: whether this container is currently occupied by a token
    """
    def __init__(self, url: str):
        self.url = url
        self.lock = threading.Lock()
        self.busy = False


# Globals: backend list + token -> backend mapping
backends: List[BackendSlot] = []
token_backend_map: Dict[str, int] = {}
token_backend_lock = threading.Lock()
capacity: AtomicInteger  # initialized in main
num_of_req_map = {}
time_of_exec_map = {}

# Round-robin starting index
next_backend_index = 0

def get_container_id_by_host_port(port: int) -> str:
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.ID}} {{.Ports}}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )
    for line in result.stdout.splitlines():
        cid, ports = line.split(" ", 1)
        if f":{port}->" in ports:
            return cid
    return ""


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
    Choose a backend for a token (round-robin):
    - Start from next_backend_index and wrap around once
    - Find the first backend that is not busy
    - Mark it busy and update next_backend_index
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
                # Next time, start from the next backend
                next_backend_index = (idx + 1) % n
                logger.debug(
                    "Assigned backend index %d to token %s (url=%s)",
                    idx,
                    token,
                    backend.url,
                )
                return backend

        # No free backend
        logger.info("All backends busy when assigning token %s", token)
        return None


def get_backend_for_token(token: str) -> Tuple[BackendSlot, int]:
    """Find the backend and index for a token. If not found, return None."""
    with token_backend_lock:
        idx = token_backend_map.get(token)
    if idx is None or not (0 <= idx < len(backends)):
        logger.warning("No backend found for token %s", token)
        return None
    return backends[idx], idx


def release_backend_for_token(token: str) -> None:
    """Release the backend associated with a token so it can be reused."""
    with token_backend_lock:
        idx = token_backend_map.pop(token, None)
    if idx is not None and 0 <= idx < len(backends):
        backends[idx].busy = False
        logger.debug("Released backend index %d for token %s", idx, token)


class TimeoutCache:
    """
    TTL-based token store:
    - get(key): check whether token is still valid (not expired)
    - put(key, ttl): write token with its expiry time
    - remove(key) -> bool: delete token, return whether it existed (to avoid double free)
    - clear(): clean expired tokens, return capacity, and release backend
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
                # Expired; delete here but do not restore capacity here
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
        """Delete key and return whether it existed (used to avoid double free)."""
        with self._lock:
            existed = key in self.store
            if existed:
                self.store.pop(key, None)
                logger.debug("Token %s removed from TimeoutCache", key)
            return existed

    def clear(self):
        """
        Clean expired tokens:
        - Remove from store
        - Return capacity
        - Release backend
        """
        now = time.time()
        expired_keys = []
        with self._lock:
            for key, expiry in list(self.store.items()):
                if now > expiry:
                    self.store.pop(key, None)
                    expired_keys.append(key)

        # Do capacity/backend release outside the lock
        for key in expired_keys:
            logger.info("Token %s expired (TimeoutCache.clear)", key)
            capacity.increment()
            release_backend_for_token(key)


pass_token_map: TimeoutCache  # initialized in main


def try_shutdown_if_idle():
    """
    If we are already in shutdown state and all capacity is free,
    first update DynamoDB as terminated, then call EC2 API to terminate this instance.
    """
    global termination_started

    # 1. Not in shutdown yet, return early
    if not is_shutting_down:
        return

    # 2. Some capacity still in use, cannot shut down
    if capacity.get_value() != total_capacity:
        return

    # 3. Avoid multiple threads terminating at the same time
    with shutdown_lock:
        if termination_started:
            return
        termination_started = True

    # 4. Update DynamoDB first, then terminate the instance
    try:
        instance_id = get_instance_id()
        logger.info(
            "[SHUTDOWN] all capacity free for instance %s, updating status to terminated",
            instance_id,
        )

        # Write terminated_time + status = terminated
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
    Unified token cleanup logic:
    - Remove token from TimeoutCache
    - capacity +1
    - Release backend
    - If we're shutting down and idle, try to shut down
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
    Reserve an execution slot:
    - If there is free capacity + backend, return a token
    - Otherwise return fail
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
                # No backend available, rollback capacity
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
    Execute with a reserved token:
    - sync: block until Lambda returns, then return result
    - async: run in background thread, immediately return {"status": "success"}
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
    Asynchronous execution:
    - Find backend by token
    - Call Lambda under backend.lock
    - Finally cleanup_token
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
    Execute without reserving:
    - Directly grab a slot and run
    - On success, internally generate a token, bind to backend, and release after use
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

                # Fixed TTL=2 seconds, keeping your original logic
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
    Mark current instance as entering shutdown state:
    - Update DynamoDB status to 'shutting-down'
    - Record shutdown_time in history table
    - Set global is_shutting_down to True
    - After this, all /rlb/reserve /rlb/execute /rlb/execute_without_reserve
      will directly return {"status": "shutting-down"}

    Additionally:
    - If total_capacity == current_capacity (no in-flight requests)
      -> Set status in active table to 'terminated'
      -> Write terminated_timestamp in history table
      -> Directly call EC2 terminate on this instance
    """
    global is_shutting_down, total_capacity, capacity

    instance_id = get_instance_id()

    # 1. Mark as shutting-down + record shutdown_time
    update_shutting_down_instance_status(instance_id)
    update_shutdown_time(instance_id)

    is_shutting_down = True
    logger.info("[SHUTDOWN] instance %s is entering shutting-down state", instance_id)

    # 2. Use existing total_capacity / current_capacity for one check
    current_cap = capacity.get_value()
    logger.info(
        "[SHUTDOWN] capacity check: total_capacity=%s, current_capacity=%s",
        total_capacity,
        current_cap,
    )

    will_terminate_now = False

    if total_capacity == current_cap:
        # No in-flight work, can treat as idle and safe to delete
        will_terminate_now = True
        logger.info(
            "[SHUTDOWN] no inflight work; marking terminated & terminating self"
        )

        ddb = boto3.resource("dynamodb", region_name="ca-west-1")
        
        time_exec_map = {}
        num_req_map = {}
        for i in time_of_exec_map.keys():
            time_exec_map[get_container_id_by_host_port(8080 + i)] = time_of_exec_map[i].get_value()
        for i in num_of_req_map.keys():
            num_req_map[i] = num_of_req_map[i].get_value()
        

        # 2.1 Update status in active table -> terminated
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

        # 2.2 Write terminated_timestamp into history table
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
                ":req_map": str(num_req_map),
            },
        )

        # 2.3 Call EC2 terminate self
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
# DynamoDB / instance status related logic
# ─────────────────────────────────────────

def updateInstanceStatus(instance_id, local_ip):
    """
    On startup: transition from init -> running, and write public_ip
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
    When entering shutting-down phase: set status = 'shutting-down'
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
    Right before terminate: set status = 'terminated'
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
        # Log the error but do not block actual termination
        logger.exception(
            "[SHUTDOWN] Failed to update terminated status in DynamoDB for %s: %s",
            instance_id,
            e,
        )
        return None


def update_running_new_machine_history(instance_id):
    """
    Record running_time (millisecond timestamp) in history table
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
    Record shutdown_time (millisecond timestamp) in history table
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
    Record terminated_time (millisecond timestamp) in history table
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
    Use EC2 Instance Metadata Service (IMDSv2) to get the current instance's AWS region.
    Only works on EC2.

    :param timeout: Timeout (seconds) for each HTTP request
    :return: Region string like 'us-west-2'
    :raises RuntimeError: If unable to retrieve region from IMDS
    """
    try:
        # 1) Get IMDSv2 token (equivalent to the first curl)
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=timeout,
        ).text

        # 2) Use the token to get the instance-identity document (equivalent to the second curl)
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
# main: initialize capacity / backends / pass_token_map
# ─────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python random_server.py <capacity>")


    capacity_value = int(sys.argv[1])  # capacity = number of containers (replicas)
    capacity = AtomicInteger(capacity_value)
    pass_token_map = TimeoutCache()

    # Initial state: not in shutdown
    is_shutting_down = False
    # Record total capacity, used to determine "fully idle"
    total_capacity = capacity_value

    # BACKEND_BASE_PORT: host port mapped to the first container, default 8080
    base_port = int(os.environ.get("BACKEND_BASE_PORT", "8080"))
    backend_host = os.environ.get("BACKEND_HOST", "localhost")

    # Build backend list according to capacity
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
    logger.info(time_of_exec_map)
    
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
