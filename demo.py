"""
demo.py — submits a mix of tasks to show off every feature, then waits and
prints the results. Run this while at least one worker.py is running against
the same broker.

Usage:
    python demo.py
"""

from client import TaskQueueClient

q = TaskQueueClient("http://localhost:8000")

print("Submitting tasks...\n")

ids = {}
ids["basic add"] = q.submit("add", args=[2, 3])
ids["low priority"] = q.submit("word_count", args=["a slow low priority task"], priority=-5)
ids["high priority"] = q.submit("add", args=[100, 200], priority=10)
ids["slow (watch dashboard)"] = q.submit("slow_square", args=[7], kwargs={"delay": 4})
ids["delayed 5s"] = q.submit("send_email", args=["you@example.com", "hi"], delay_seconds=5)
ids["flaky (will retry)"] = q.submit("flaky_task", kwargs={"fail_probability": 0.7}, max_retries=5)

for label, tid in ids.items():
    print(f"  {label:26s} -> {tid}")

print("\nWaiting for results (open http://localhost:8000/dashboard to watch live)...\n")

for label, tid in ids.items():
    try:
        result = q.wait(tid, timeout=30)
        status = result["status"]
        payload = result["result"] if status == "completed" else result["error"]
        print(f"  [{status:9s}] {label:26s} -> {payload}")
    except TimeoutError:
        print(f"  [timeout  ] {label:26s} -> still not done after 30s")

print("\nFinal queue stats:", q.stats())
