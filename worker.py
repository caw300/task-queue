"""
worker.py — a task-executing worker process.

Run several of these (in different terminals, or on different machines
pointed at the same --broker URL) to see the "distributed" part: they all
poll the same broker and it guarantees none of them ever double-process
the same task.

Usage:
    python worker.py --broker http://localhost:8000 --id worker-1
"""

import argparse
import time
import traceback
import uuid
import requests

import tasks_example  # the registry of functions workers know how to run

REGISTRY = tasks_example.REGISTRY

POLL_INTERVAL_IDLE = 1.0   # how often to ask "got anything for me?" when idle
LEASE_SECONDS = 30         # how long we tell the broker to hold the task for us


def run_worker(broker_url: str, worker_id: str):
    print(f"[{worker_id}] starting, connected to {broker_url}")
    print(f"[{worker_id}] known task types: {list(REGISTRY.keys())}")

    while True:
        try:
            resp = requests.post(
                f"{broker_url}/tasks/claim",
                json={"worker_id": worker_id, "lease_seconds": LEASE_SECONDS},
                timeout=10,
            )
            resp.raise_for_status()
            task = resp.json()["task"]
        except requests.RequestException as e:
            print(f"[{worker_id}] broker unreachable ({e}); retrying...")
            time.sleep(POLL_INTERVAL_IDLE)
            continue

        if task is None:
            time.sleep(POLL_INTERVAL_IDLE)
            continue

        execute_and_report(broker_url, worker_id, task)


def execute_and_report(broker_url: str, worker_id: str, task: dict):
    task_id = task["id"]
    name = task["name"]
    args = task["args"]
    kwargs = task["kwargs"]

    print(f"[{worker_id}] picked up task {task_id[:8]} -> {name}{tuple(args)} "
          f"(priority={task['priority']}, attempt={task['retries'] + 1})")

    fn = REGISTRY.get(name)
    if fn is None:
        requests.post(f"{broker_url}/tasks/{task_id}/fail",
                       json={"error": f"unknown task type '{name}'"})
        return

    try:
        result = fn(*args, **kwargs)
    except Exception:
        err = traceback.format_exc(limit=3)
        print(f"[{worker_id}] task {task_id[:8]} FAILED:\n{err}")
        r = requests.post(f"{broker_url}/tasks/{task_id}/fail", json={"error": err})
        print(f"[{worker_id}] -> {r.json()}")
        return

    requests.post(f"{broker_url}/tasks/{task_id}/complete", json={"result": result})
    print(f"[{worker_id}] task {task_id[:8]} completed -> {result!r}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default="http://localhost:8000")
    parser.add_argument("--id", default=f"worker-{uuid.uuid4().hex[:6]}")
    args = parser.parse_args()

    try:
        run_worker(args.broker, args.id)
    except KeyboardInterrupt:
        print(f"\n[{args.id}] shutting down")
