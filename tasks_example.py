"""
tasks_example.py — the registry of task functions a worker knows how to run.

A "task" submitted to the queue is really just a name (a string) plus
args/kwargs. Workers look the name up in REGISTRY to find the actual
Python function to call. This indirection is what lets you submit tasks
from anywhere (a web request handler, a CLI, a cron job) without that
code needing to import the actual implementation.

Add your own functions here and they'll be picked up automatically —
just make sure every worker process importing this file has whatever
dependencies your functions need.
"""

import time
import random


def add(a, b):
    return a + b


def slow_square(n, delay=2):
    """Simulates a task that takes real time, so you can watch it sit in
    'running' state on the dashboard."""
    time.sleep(delay)
    return n * n


def flaky_task(fail_probability=0.6):
    """Fails most of the time on purpose, so you can watch the retry +
    backoff logic kick in on the dashboard (status will flip
    running -> pending -> running a few times before completing)."""
    if random.random() < fail_probability:
        raise RuntimeError("simulated transient failure")
    return "succeeded after retries"


def send_email(to: str, subject: str):
    """Stand-in for a real side-effecting task."""
    time.sleep(0.5)
    return f"email sent to {to} with subject '{subject}'"


def word_count(text: str):
    return len(text.split())


REGISTRY = {
    "add": add,
    "slow_square": slow_square,
    "flaky_task": flaky_task,
    "send_email": send_email,
    "word_count": word_count,
}
