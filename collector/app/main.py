"""FastAPI service: continuous ingestion + query API.

Endpoints
---------
GET  /health                 liveness, counters and active ingest limits
GET  /ingest/progress        per-file progress (offset/size, blocked reason)
POST /ingest/poll-now        force an immediate bounded collection pass
GET  /events                 paginated events, filter by path / status /
                             status range / source code
GET  /events/{event_id}      single event with the raw JSON line
GET  /malformed              quarantined lines that failed JSON parsing
"""

from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from . import storage
from .ingest import (
    CollectorLoop,
    MAX_LINE_BYTES,
    READ_CHUNK,
    poll_once,
    validate_limits,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("collector.api")

DB_PATH = os.getenv("DB_PATH", "/data/warehouse.db")
LOG_DIR = os.getenv("LOG_DIR", "/logs")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "0.5"))
# Bounded, fair polling: at most READ_CHUNK_BYTES are read from any single
# file per tick; a line longer than MAX_LINE_BYTES blocks that file (reported
# on /ingest/progress) instead of being split or growing memory without bound.
READ_CHUNK_BYTES = int(os.getenv("READ_CHUNK_BYTES", str(READ_CHUNK)))
MAX_LINE_BYTES_CFG = int(os.getenv("MAX_LINE_BYTES", str(MAX_LINE_BYTES)))
# Misconfiguration must fail at startup rather than silently stall ingestion.
validate_limits(READ_CHUNK_BYTES, MAX_LINE_BYTES_CFG)


class Health(BaseModel):
    status: str
    log_dir: str
    db_path: str
    poll_interval_seconds: float
    read_chunk_bytes: int
    max_line_bytes: int
    collector_running: bool
    total_polls: int
    total_ingested: int
    counts: dict


class FileProgress(BaseModel):
    source_code: str
    dev: int
    inode: int
    path: str
    offset: int
    size: Optional[int] = None
    complete: bool
    last_seen: str
    # True when a line at `offset` exceeds max_line_bytes. The file is held at
    # that exact offset (not split, skipped or partially recorded); other
    # files keep being collected while this one waits for remediation.
    blocked: bool = False
    blocked_reason: Optional[str] = None
    blocked_at: Optional[str] = None


class ProgressResponse(BaseModel):
    collector_running: bool
    total_polls: int
    total_ingested: int
    read_chunk_bytes: int
    max_line_bytes: int
    blocked_count: int
    last_poll: Optional[dict] = None
    counts: dict
    files: list[FileProgress]


class Event(BaseModel):
    id: int
    ts: Optional[str]
    path: Optional[str]
    status: Optional[int]
    method: Optional[str]
    source_code: str
    dev: int
    inode: int
    byte_offset: int
    raw_line: str
    ingested_at: str


class EventPage(BaseModel):
    total: int
    limit: int
    offset: int
    next_offset: Optional[int]
    items: list[Event]


state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = storage.connect(DB_PATH)
    storage.init_db(conn)

    loop = CollectorLoop(
        conn, LOG_DIR, interval=POLL_INTERVAL,
        read_chunk_bytes=READ_CHUNK_BYTES,
        max_line_bytes=MAX_LINE_BYTES_CFG,
    )
    thread = threading.Thread(target=loop.run_forever, name="collector",
                              daemon=True)
    thread.start()
    state.update(conn=conn, loop=loop, thread=thread)
    log.info("API ready; watching %s, db %s", LOG_DIR, DB_PATH)
    try:
        yield
    finally:
        log.info("shutdown requested, stopping collector loop...")
        loop.request_stop()
        thread.join(timeout=10)
        # A checkpoint persists WAL frames into the main db file on exit.
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            log.exception("checkpoint on shutdown failed")
        conn.close()
        log.info("shutdown complete")


app = FastAPI(
    title="Nginx 访问日志入仓 API",
    description=(
        "持续采集共享只读目录中的 JSON 行 Nginx 日志，按 (设备, inode) "
        "识别文件、行与续采偏移同事务提交，支持按路径与状态码分页查询故障请求。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


def _conn():
    return state["conn"]


@app.get("/health", response_model=Health, tags=["meta"])
def health():
    loop: CollectorLoop = state["loop"]
    return Health(
        status="ok",
        log_dir=LOG_DIR,
        db_path=DB_PATH,
        poll_interval_seconds=POLL_INTERVAL,
        read_chunk_bytes=READ_CHUNK_BYTES,
        max_line_bytes=MAX_LINE_BYTES_CFG,
        collector_running=threading.main_thread().is_alive() and not loop._stop,
        total_polls=loop.total_polls,
        total_ingested=loop.total_ingested,
        counts=storage.counts(_conn()),
    )


@app.get("/ingest/progress", response_model=ProgressResponse, tags=["ingest"])
def progress():
    conn = _conn()
    loop: CollectorLoop = state["loop"]
    rows = storage.all_states(conn)
    # Current sizes from the live directory, read-only.
    current: dict[tuple[int, int], int] = {}
    try:
        for name in os.listdir(LOG_DIR):
            try:
                st = os.stat(os.path.join(LOG_DIR, name))
            except FileNotFoundError:
                continue
            current[(st.st_dev, st.st_ino)] = st.st_size
    except FileNotFoundError:
        pass

    files = []
    blocked_count = 0
    for row in rows:
        size = current.get((row["dev"], row["inode"]))
        is_blocked = row["blocked_reason"] is not None
        if is_blocked:
            blocked_count += 1
        files.append(FileProgress(
            source_code=row["source_code"],
            dev=row["dev"],
            inode=row["inode"],
            path=row["path"],
            offset=row["offset"],
            size=size,
            complete=(not is_blocked and size is not None
                      and row["offset"] >= size),
            last_seen=row["last_seen"],
            blocked=is_blocked,
            blocked_reason=row["blocked_reason"],
            blocked_at=row["blocked_at"],
        ))
    return ProgressResponse(
        collector_running=not loop._stop,
        total_polls=loop.total_polls,
        total_ingested=loop.total_ingested,
        read_chunk_bytes=READ_CHUNK_BYTES,
        max_line_bytes=MAX_LINE_BYTES_CFG,
        blocked_count=blocked_count,
        last_poll=loop.last_result,
        counts=storage.counts(conn),
        files=files,
    )


@app.post("/ingest/poll-now", tags=["ingest"])
def poll_now():
    """Force an immediate scan/ingest pass (useful for tests/demos)."""
    loop: CollectorLoop = state["loop"]
    with loop.write_lock:
        return poll_once(
            _conn(), LOG_DIR,
            read_chunk_bytes=loop.read_chunk_bytes,
            max_line_bytes=loop.max_line_bytes,
            carries=loop._carries,
        )


@app.get("/events", response_model=EventPage, tags=["query"])
def list_events(
    path: Optional[str] = Query(None, description="精确路径，例如 /api/orders"),
    status: Optional[int] = Query(None, description="精确状态码，例如 500"),
    status_min: Optional[int] = Query(None, description="状态码下界，例如 400"),
    status_max: Optional[int] = Query(None, description="状态码上界，例如 599"),
    source_code: Optional[str] = Query(None, description="来源文件代号，例如 access.log.20261113T100000.log"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """按路径、状态码分页取数。查故障请求示例：
    ``/events?status_min=500&status_max=599&limit=50``。"""
    return storage.query_events(
        _conn(),
        path=path,
        status=status,
        status_min=status_min,
        status_max=status_max,
        source_code=source_code,
        limit=limit,
        offset=offset,
    )


@app.get("/events/{event_id}", response_model=Event, tags=["query"])
def get_event(event_id: int):
    row = _conn().execute(
        "SELECT * FROM log_events WHERE id = ?", (event_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="event not found")
    return dict(row)


@app.get("/malformed", tags=["query"])
def list_malformed(limit: int = Query(100, ge=1, le=1000),
                   offset: int = Query(0, ge=0)):
    conn = _conn()
    total = conn.execute(
        "SELECT COUNT(*) c FROM malformed_lines"
    ).fetchone()["c"]
    rows = conn.execute(
        "SELECT * FROM malformed_lines ORDER BY id LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    return {"total": total, "limit": limit, "offset": offset,
            "items": [dict(r) for r in rows]}
