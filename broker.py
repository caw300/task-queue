"""
broker.py — the coordinator for the distributed task queue.

Responsibilities:
  1. Accept task submissions (POST /tasks)
  2. Hand out tasks to workers atomically, so two workers never grab the
     same task even if they ask at the exact same instant (POST /tasks/claim)
  3. Record success/failure reported back by workers, with retry + backoff
  4. Reclaim tasks from workers that died mid-task (the "reaper")
  5. Serve a small dashboard + JSON stats endpoint

Persistence is SQLite in WAL mode. WAL mode lets multiple processes read
concurrently while one writes, which is what makes it safe for several
worker processes (potentially on several machines, if you expose this
over the network) to talk to one broker at once.

Run with:
    uvicorn broker:app --host 0.0.0.0 --port 8000
"""

import sqlite3
import json
import time
import uuid
import asyncio
import contextlib
from pathlib import Path
from typing import Optional, Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

DB_PATH = Path(__file__).parent / "queue.db"
REAPER_INTERVAL_SECONDS = 2.0
DEFAULT_LEASE_SECONDS = 30

app = FastAPI(title="Distributed Task Queue Broker")


# --------------------------------------------------------------------------
# Storage layer
# --------------------------------------------------------------------------

def get_conn() -> sqlite3.Connection:
    """A short-lived connection per call. WAL mode makes this cheap and safe
    across processes; we don't need a fancy connection pool for this scale."""
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id              TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            args            TEXT NOT NULL DEFAULT '[]',
            kwargs          TEXT NOT NULL DEFAULT '{}',
            priority        INTEGER NOT NULL DEFAULT 0,
            status          TEXT NOT NULL DEFAULT 'pending',
            created_at      REAL NOT NULL,
            run_at          REAL NOT NULL,
            started_at      REAL,
            completed_at    REAL,
            result          TEXT,
            error           TEXT,
            retries         INTEGER NOT NULL DEFAULT 0,
            max_retries     INTEGER NOT NULL DEFAULT 3,
            worker_id       TEXT,
            lease_expires_at REAL
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status_runat ON tasks(status, run_at);")
    conn.close()


# --------------------------------------------------------------------------
# API models
# --------------------------------------------------------------------------

class SubmitRequest(BaseModel):
    name: str
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    priority: int = 0
    delay_seconds: float = 0
    max_retries: int = 3


class ClaimRequest(BaseModel):
    worker_id: str
    lease_seconds: float = DEFAULT_LEASE_SECONDS


class CompleteRequest(BaseModel):
    result: Any = None


class FailRequest(BaseModel):
    error: str


def row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["args"] = json.loads(d["args"])
    d["kwargs"] = json.loads(d["kwargs"])
    if d["result"] is not None:
        try:
            d["result"] = json.loads(d["result"])
        except (json.JSONDecodeError, TypeError):
            pass
    return d


# --------------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------------

@app.on_event("startup")
def startup():
    init_db()
    asyncio.create_task(reaper_loop())


@app.post("/tasks")
def submit_task(req: SubmitRequest):
    task_id = str(uuid.uuid4())
    now = time.time()
    conn = get_conn()
    conn.execute(
        """INSERT INTO tasks (id, name, args, kwargs, priority, status,
                               created_at, run_at, max_retries)
           VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)""",
        (
            task_id, req.name, json.dumps(req.args), json.dumps(req.kwargs),
            req.priority, now, now + req.delay_seconds, req.max_retries,
        ),
    )
    conn.close()
    return {"id": task_id}


@app.post("/tasks/claim")
def claim_task(req: ClaimRequest):
    """Atomically grab the highest-priority, oldest, ready-to-run task and
    assign it to this worker. The UPDATE...WHERE id=(SELECT...) pattern with
    SQLite's RETURNING clause, inside an IMMEDIATE transaction, is what makes
    this race-free: only one process can win the row-lock at a time, so two
    workers polling simultaneously can never claim the same task."""
    now = time.time()
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE;")
    try:
        row = conn.execute(
            """
            UPDATE tasks
            SET status = 'running',
                worker_id = ?,
                started_at = ?,
                lease_expires_at = ?
            WHERE id = (
                SELECT id FROM tasks
                WHERE status = 'pending' AND run_at <= ?
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            )
            RETURNING *;
            """,
            (req.worker_id, now, now + req.lease_seconds, now),
        ).fetchone()
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise
    finally:
        conn.close()

    if row is None:
        return {"task": None}
    return {"task": row_to_dict(row)}


@app.post("/tasks/{task_id}/complete")
def complete_task(task_id: str, req: CompleteRequest):
    conn = get_conn()
    cur = conn.execute(
        """UPDATE tasks SET status='completed', result=?, completed_at=?
           WHERE id=? AND status='running'""",
        (json.dumps(req.result), time.time(), task_id),
    )
    conn.close()
    if cur.rowcount == 0:
        raise HTTPException(404, "task not found or not in running state")
    return {"ok": True}


@app.post("/tasks/{task_id}/fail")
def fail_task(task_id: str, req: FailRequest):
    """Retry with exponential backoff, up to max_retries, then give up."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    if row is None:
        conn.close()
        raise HTTPException(404, "task not found")

    retries = row["retries"] + 1
    if retries <= row["max_retries"]:
        backoff = min(2 ** retries, 60)  # cap backoff at 60s
        conn.execute(
            """UPDATE tasks SET status='pending', retries=?, error=?,
               run_at=?, worker_id=NULL, lease_expires_at=NULL
               WHERE id=?""",
            (retries, req.error, time.time() + backoff, task_id),
        )
        conn.close()
        return {"ok": True, "status": "retrying", "retry_in": backoff, "attempt": retries}
    else:
        conn.execute(
            """UPDATE tasks SET status='failed', retries=?, error=?, completed_at=?
               WHERE id=?""",
            (retries, req.error, time.time(), task_id),
        )
        conn.close()
        return {"ok": True, "status": "failed"}


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    conn = get_conn()
    row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    conn.close()
    if row is None:
        raise HTTPException(404, "task not found")
    return row_to_dict(row)


@app.get("/tasks")
def list_tasks(status: Optional[str] = None, limit: int = 100):
    conn = get_conn()
    if status:
        rows = conn.execute(
            "SELECT * FROM tasks WHERE status=? ORDER BY created_at DESC LIMIT ?",
            (status, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    conn.close()
    return [row_to_dict(r) for r in rows]


@app.get("/stats")
def stats():
    conn = get_conn()
    rows = conn.execute(
        "SELECT status, COUNT(*) as n FROM tasks GROUP BY status"
    ).fetchall()
    conn.close()
    counts = {r["status"]: r["n"] for r in rows}
    for s in ("pending", "running", "completed", "failed"):
        counts.setdefault(s, 0)
    return counts


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return FileResponse(Path(__file__).parent / "dashboard.html")


# --------------------------------------------------------------------------
# Reaper: reclaims tasks whose worker died mid-execution (lease expired)
# --------------------------------------------------------------------------

async def reaper_loop():
    while True:
        with contextlib.suppress(Exception):
            reap_expired_leases()
        await asyncio.sleep(REAPER_INTERVAL_SECONDS)


def reap_expired_leases():
    """If a worker claims a task but crashes (or loses network) before
    reporting completion, the task would be stuck 'running' forever without
    this. We treat an expired lease as an implicit failure and route it
    through the same retry/backoff logic as an explicit failure."""
    now = time.time()
    conn = get_conn()
    stuck = conn.execute(
        "SELECT id FROM tasks WHERE status='running' AND lease_expires_at < ?",
        (now,),
    ).fetchall()
    conn.close()
    for row in stuck:
        fail_task(row["id"], FailRequest(error="lease expired (worker likely died)"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
