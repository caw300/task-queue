"""
client.py — a tiny SDK for submitting tasks and checking on them, so
application code never has to build raw HTTP requests by hand.

Example:
    from client import TaskQueueClient
    q = TaskQueueClient()
    task_id = q.submit("add", args=[2, 3])
    print(q.wait(task_id))   # blocks until done, returns the result
"""

import time
import requests


class TaskQueueClient:
    def __init__(self, broker_url: str = "http://localhost:8000"):
        self.broker_url = broker_url.rstrip("/")

    def submit(self, name: str, args=None, kwargs=None, priority: int = 0,
               delay_seconds: float = 0, max_retries: int = 3) -> str:
        resp = requests.post(
            f"{self.broker_url}/tasks",
            json={
                "name": name,
                "args": args or [],
                "kwargs": kwargs or {},
                "priority": priority,
                "delay_seconds": delay_seconds,
                "max_retries": max_retries,
            },
        )
        resp.raise_for_status()
        return resp.json()["id"]

    def status(self, task_id: str) -> dict:
        resp = requests.get(f"{self.broker_url}/tasks/{task_id}")
        resp.raise_for_status()
        return resp.json()

    def wait(self, task_id: str, poll_interval: float = 0.3, timeout: float = 60):
        """Block until the task reaches a terminal state, then return the
        full task record (check ['status'] for 'completed' vs 'failed')."""
        start = time.time()
        while time.time() - start < timeout:
            t = self.status(task_id)
            if t["status"] in ("completed", "failed"):
                return t
            time.sleep(poll_interval)
        raise TimeoutError(f"task {task_id} did not finish within {timeout}s")

    def stats(self) -> dict:
        resp = requests.get(f"{self.broker_url}/stats")
        resp.raise_for_status()
        return resp.json()

    def list_tasks(self, status: str = None, limit: int = 100) -> list:
        params = {"limit": limit}
        if status:
            params["status"] = status
        resp = requests.get(f"{self.broker_url}/tasks", params=params)
        resp.raise_for_status()
        return resp.json()
