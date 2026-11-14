"""Continuous JSON-lines log collector — bounded, fair, batch-committed.

Correctness model
=================

1. File identity is ``(st_dev, st_ino)`` — never the path/name. When nginx
   rotates, the old inode keeps being written briefly and the path points at
   a brand new inode; both are tracked independently, so an offset from the
   old file is never applied to the new one.
2. **Bounded per poll.** Each file is read for at most ``read_chunk_bytes``
   in a single poll (one ``os.read`` of exactly that size at most). A big
   backlog therefore drains over successive ticks and process memory stays
   flat: the largest live state is one chunk plus one in-memory partial line
   plus one batch of decoded lines, never the whole file.
3. **Fairness.** Every poll visits every visible file once, in a stable
   order, and each file gets the same finite byte budget. A huge file can no
   longer monopolise a tick, so the short logs queued behind it keep getting
   ingested on every pass.
4. Only lines terminated by ``\\n`` are ingested. A line that straddles a
   read boundary is carried in memory to the next poll while the *committed*
   offset stays at its first byte, so a half-written line — including a
   multi-byte UTF-8 sequence straddling a read boundary — can never be torn
   apart, skipped or recorded twice. After a process exit the in-memory carry
   is gone, but the committed offset points exactly at the line's start, so
   the line is simply re-read: no gap, no duplicate.
5. Rows of one batch and the batch's advanced offset are written in one
   SQLite transaction (see :mod:`app.storage`). Any exit — before, during or
   after commit — resumes from the last committed offset.
6. **Oversized lines.** A line longer than ``max_line_bytes`` cannot be
   processed inside a batch. It is never split or skipped: the committed
   offset stays at the line's first byte, the file is marked *blocked* with a
   human-readable reason (visible on ``/ingest/progress``), and every other
   file keeps being collected. The block clears automatically once the line
   (or the file) is fixed so that no line exceeds the limit.
7. Rotated archives are never deleted or truncated; they are simply kept in
   the watched directory and drained to EOF once, one bounded batch per poll.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from . import storage

log = logging.getLogger("collector.ingest")

# Per-file, per-poll byte budget. A single os.read() never asks for more than
# this, so a multi-gigabyte backlog cannot be pulled into memory at once.
READ_CHUNK = 1024 * 1024

# Hard size limit for one physical line (bytes, excluding the terminating
# '\n'). Lines above it cannot fit the batching contract; the file is held at
# the line's start offset and reported as blocked instead of being truncated.
MAX_LINE_BYTES = 256 * 1024


def validate_limits(read_chunk_bytes: int, max_line_bytes: int) -> None:
    """Fail fast on an unusable limit combination.

    The per-poll read budget must be at least as large as the longest
    permitted line. Otherwise a legitimate line could span more bytes than a
    whole turn can read, and a process restart that discards the in-memory
    carry would re-read the same partial line forever without ever reaching
    its newline. Keeping ``max_line_bytes <= read_chunk_bytes`` means any
    line that fits the limit can always be observed completely in one turn
    once reading reaches its first byte.
    """
    if read_chunk_bytes <= 0:
        raise ValueError(
            f"read_chunk_bytes must be positive, got {read_chunk_bytes}"
        )
    if max_line_bytes <= 0:
        raise ValueError(
            f"max_line_bytes must be positive, got {max_line_bytes}"
        )
    if max_line_bytes > read_chunk_bytes:
        raise ValueError(
            "max_line_bytes must not exceed read_chunk_bytes, otherwise a "
            "valid line could never be fully read within one poll "
            f"(read_chunk_bytes={read_chunk_bytes}, "
            f"max_line_bytes={max_line_bytes})"
        )


@dataclass(frozen=True)
class FileIdentity:
    dev: int
    inode: int
    size: int
    path: str
    source_code: str


@dataclass
class DrainResult:
    """Outcome of one bounded read of one file."""

    # (absolute byte offset, decoded line, physical byte width incl. '\n')
    complete_lines: list[tuple[int, str, int]]
    # Offset that may be committed: the first byte not yet part of a complete
    # line. Equals the start of `carry` when a tail is held back.
    committed_offset: int
    # Partial line without a terminating newline, to be re-joined on the next
    # poll. Never persisted to the DB; after a crash it is rebuilt by
    # re-reading from committed_offset.
    carry: bytes = b""
    bytes_read: int = 0
    blocked: bool = False
    blocked_reason: Optional[str] = None


@dataclass(frozen=True)
class _Carry:
    """Cross-poll partial-line state for one (dev, inode)."""

    data: bytes       # bytes already read but not newline-terminated
    start_offset: int  # absolute file offset where `data` begins


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _source_code(path: str) -> str:
    """Stable, human-readable identifier of the source file."""
    return os.path.basename(path)


def list_log_files(directory: str) -> list[FileIdentity]:
    """Snapshot the directory. A file that disappears mid-scan is skipped
    here and picked up (or detected as missing) on the next poll."""
    try:
        names = sorted(os.listdir(directory))
    except FileNotFoundError:
        log.warning("watched directory %s vanished", directory)
        return []

    files: list[FileIdentity] = []
    for name in names:
        full = os.path.join(directory, name)
        try:
            st = os.stat(full)
        except FileNotFoundError:
            continue
        if not os.path.isfile(full) or os.path.islink(full):
            continue
        files.append(
            FileIdentity(
                dev=st.st_dev,
                inode=st.st_ino,
                size=st.st_size,
                path=full,
                source_code=_source_code(full),
            )
        )
    return files


def parse_line(raw: str) -> tuple[dict[str, Any] | None, str | None]:
    """Return (parsed, error). Never raises — bad lines are quarantined,
    they must not stop the pipeline."""
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid json: {exc.msg} at pos {exc.pos}"
    if not isinstance(obj, dict):
        return None, "top-level JSON value is not an object"
    return obj, None


def _normalise(parsed: dict[str, Any], raw: str) -> dict[str, Any]:
    ts = parsed.get("time") or parsed.get("@timestamp") or parsed.get("ts")
    if not isinstance(ts, str):
        ts = None

    # Prefer the path without query string for filtering, but keep the raw
    # line verbatim for full fidelity.
    request = parsed.get("request")
    path = parsed.get("path") or parsed.get("uri")
    if not isinstance(path, str) and isinstance(request, str):
        parts = request.split(" ")
        if len(parts) >= 2:
            path = parts[1]
    if isinstance(path, str):
        path = path.split("?", 1)[0]
    else:
        path = None

    status = parsed.get("status")
    try:
        status = int(status) if status is not None else None
    except (TypeError, ValueError):
        status = None

    method = parsed.get("method")
    if not isinstance(method, str) and isinstance(request, str):
        method = request.split(" ", 1)[0] or None
    if not isinstance(method, str):
        method = None

    return {"ts": ts, "path": path, "status": status, "method": method}


def drain_file(
    path: str,
    start_offset: int,
    current_size: int,
    read_chunk_bytes: int = READ_CHUNK,
    max_line_bytes: int = MAX_LINE_BYTES,
    carried: Optional[_Carry] = None,
) -> DrainResult:
    """Read at most ``read_chunk_bytes`` from one file this poll and return
    only complete, newline-terminated lines.

    This is a strict per-file, per-poll I/O cap: at most one ``os.read`` of
    ``read_chunk_bytes`` happens here, so a multi-gigabyte backlog cannot be
    pulled into memory in a tick and every file keeps its fair turn. A line
    longer than the remaining budget is not torn: the bytes already read are
    joined with the cross-poll ``carried`` tail and reassembled on following
    polls while the committed offset stays at the line's first byte. After a
    process exit the in-memory carry is gone, but restarting from that
    committed offset rebuilds the line exactly once.

    Because ``max_line_bytes <= read_chunk_bytes`` is enforced at startup,
    any single line that fits the limit can always be read to its newline
    within one turn once its first byte is reached, so a fresh start cannot
    stall on an in-flight line.

    ``carried`` is the partial line left over from the previous poll; its
    ``start_offset`` must equal ``start_offset`` (the committed offset never
    advances past an unterminated line). The returned ``committed_offset``
    stops at the first byte of any remaining partial line (``carry``).

    A line longer than ``max_line_bytes`` blocks the file at that line's
    first byte without splitting, skipping or partially recording it.
    """
    if start_offset > current_size:
        # Truncation (not expected for the archive-style rotation, but
        # handled): the file was replaced/recreated smaller — restart from 0
        # and discard any stale partial line.
        log.warning(
            "file %s shrank (offset %s > size %s); restarting from 0",
            path, start_offset, current_size,
        )
        start_offset = 0
        carried = None

    if carried is not None and carried.start_offset != start_offset:
        # Defensive: carry and committed position can only diverge after a
        # truncation/reset handled above. Drop stale bytes and re-read.
        log.warning("discarding stale carry in %s (carry@%s, offset=%s)",
                    path, carried.start_offset, start_offset)
        carried = None

    out: list[tuple[int, str, int]] = []
    # Working buffer for the line currently being assembled. It starts with
    # any cross-poll carry and grows only until the next newline or the line
    # limit; it can never exceed max_line_bytes without raising a block.
    pending = carried.data if carried is not None else b""
    line_start = start_offset
    bytes_read = 0
    fd = os.open(path, os.O_RDONLY)
    try:
        # Seek past bytes already held in the cross-poll carry: those were
        # read on earlier ticks but have no terminating newline yet, so they
        # must not be read a second time. The committed offset still points
        # at the line's first byte (start_offset).
        os.lseek(fd, start_offset + len(pending), os.SEEK_SET)

        def _block(held: bytes, reason_line: bool) -> DrainResult:
            length = len(held)
            if reason_line:
                reason = (
                    f"line at byte {line_start} is {length} bytes, exceeds "
                    f"max_line_bytes={max_line_bytes}; ingestion of this file "
                    "is held at that offset until the line is shortened or "
                    "the limit is raised"
                )
            else:
                reason = (
                    f"unterminated line starting at byte {line_start} is "
                    f"already {length} bytes, exceeds max_line_bytes="
                    f"{max_line_bytes}; ingestion of this file is held at "
                    "that offset until the line is completed within the "
                    "limit or the limit is raised"
                )
            log.error("oversized line blocks %s: %s", path, reason)
            return DrainResult(
                complete_lines=out,
                committed_offset=line_start,
                bytes_read=bytes_read,
                blocked=True,
                blocked_reason=reason,
            )

        # A carry already over the limit is a block before any further I/O.
        if len(pending) > max_line_bytes:
            return _block(pending, reason_line=True)

        # Strict per-turn I/O cap: exactly one read, never larger than the
        # remaining per-file budget. With max_line_bytes <= read_chunk_bytes
        # (validated at startup), any line within the limit can reach its
        # newline inside a turn once reading reaches its first byte.
        chunk = os.read(fd, read_chunk_bytes)
        if not chunk and not pending:
            return DrainResult(
                complete_lines=out, committed_offset=start_offset,
                bytes_read=0,
            )
        bytes_read = len(chunk)

        # Split on raw bytes: a decoded-character count is not a byte count,
        # and a multi-byte UTF-8 sequence straddling a read boundary must not
        # be torn apart. The trailing piece has no terminating newline yet.
        *byte_parts, pending = (pending + chunk).split(b"\n")

        for piece in byte_parts:
            # An optional '\r' stays part of the physical line width so byte
            # offsets remain exact; it is stripped only from the decoded text.
            width = len(piece) + 1  # +1 for the consumed '\n'
            if len(piece) > max_line_bytes:
                return _block(piece, reason_line=True)
            text = (piece[:-1].decode("utf-8", errors="replace")
                    if piece.endswith(b"\r")
                    else piece.decode("utf-8", errors="replace"))
            out.append((line_start, text, width))
            line_start += width

        if pending:
            if len(pending) > max_line_bytes:
                return _block(pending, reason_line=False)
            # No newline terminates this line yet. It is held back and the
            # committed offset remains at its first byte (`line_start`); the
            # assembled bytes are returned as carry for the next poll.
            log.debug("partial tail of %d bytes held back in %s",
                      len(pending), path)
            return DrainResult(
                complete_lines=out,
                committed_offset=line_start,
                carry=pending,
                bytes_read=bytes_read,
            )

        # Every buffered byte was part of a complete, newline-terminated line.
        return DrainResult(
            complete_lines=out,
            committed_offset=line_start,
            bytes_read=bytes_read,
        )
    finally:
        os.close(fd)


def _process_file(
    conn: sqlite3.Connection,
    identity: FileIdentity,
    read_chunk_bytes: int,
    max_line_bytes: int,
    carries: dict[tuple[int, int], _Carry],
) -> dict[str, Any]:
    key = (identity.dev, identity.inode)
    state = storage.get_state(conn, identity.dev, identity.inode)
    if state is None:
        start_offset = 0
        log.info("new file discovered: %s (dev=%s inode=%s)",
                 identity.source_code, identity.dev, identity.inode)
    else:
        start_offset = state["offset"]
        if state["path"] != identity.path:
            # Same inode at a new path = rotation by rename. The inode is the
            # identity: keep draining from the committed offset, and from now
            # on label new rows with the archive's source code. (Inode-number
            # recycling cannot occur here: archives are retained, never
            # deleted, and nginx workers hold the renamed files open.)
            log.info("file renamed (rotation): inode %s %s -> %s; "
                     "resuming at offset %s",
                     identity.inode, state["path"], identity.path,
                     start_offset)
        if start_offset == identity.size:
            carries.pop(key, None)
            return {"source_code": identity.source_code, "ingested": 0,
                    "malformed": 0, "offset": start_offset,
                    "size": identity.size, "bytes_read": 0,
                    "blocked": False, "blocked_reason": None}

    drained = drain_file(
        identity.path, start_offset, identity.size,
        read_chunk_bytes=read_chunk_bytes,
        max_line_bytes=max_line_bytes,
        carried=carries.get(key),
    )

    # Maintain the in-memory partial-line registry.
    # * A normal partial tail is carried to the next poll so a line spanning
    #   many chunks is reassembled without re-reading bytes.
    # * A blocked file keeps NO carry: every poll re-reads from the held
    #   offset (still bounded by read_chunk_bytes), so the reported reason is
    #   stable, memory cannot grow without bound, and an operator fix is
    #   observed immediately on the next poll rather than masked by stale
    #   bytes. The committed offset already guarantees re-reading is safe.
    if drained.blocked:
        carries.pop(key, None)
    elif drained.carry:
        carries[key] = _Carry(
            data=drained.carry, start_offset=drained.committed_offset
        )
    else:
        carries.pop(key, None)

    ingested = 0
    malformed = 0
    # Persist state when a batch was committed, when a block is (re)asserted,
    # or when a previous block is cleared. A plain idle poll with nothing new
    # and no prior block writes nothing.
    if (drained.complete_lines or drained.blocked
            or (state is not None and state["blocked_reason"] is not None)):
        with storage.transaction(conn):
            for abs_offset, raw, _width in drained.complete_lines:
                parsed, err = parse_line(raw)
                if err is not None:
                    conn.execute(
                        """INSERT INTO malformed_lines
                               (source_code, dev, inode, byte_offset, raw_line, error)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (identity.source_code, identity.dev, identity.inode,
                         abs_offset, raw, err),
                    )
                    malformed += 1
                    continue
                norm = _normalise(parsed, raw)
                conn.execute(
                    """INSERT INTO log_events
                           (ts, path, status, method, source_code,
                            dev, inode, byte_offset, raw_line)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (norm["ts"], norm["path"], norm["status"], norm["method"],
                     identity.source_code, identity.dev, identity.inode,
                     abs_offset, raw),
                )
                ingested += 1

            # Offset, batch rows and the block marker advance in the same
            # transaction. While blocked, committed_offset equals the line's
            # start so a restart re-reads exactly that line — never skipping
            # it. The partial-line carry is intentionally not in the DB: on
            # crash it is rebuilt from this offset.
            storage.upsert_state(
                conn,
                dev=identity.dev,
                inode=identity.inode,
                source_code=identity.source_code,
                path=identity.path,
                offset=drained.committed_offset,
                blocked_reason=drained.blocked_reason,
                blocked_at=(_utc_now() if drained.blocked else None),
            )

    return {
        "source_code": identity.source_code,
        "dev": identity.dev,
        "inode": identity.inode,
        "ingested": ingested,
        "malformed": malformed,
        "offset": drained.committed_offset,
        "size": identity.size,
        "bytes_read": drained.bytes_read,
        "blocked": drained.blocked,
        "blocked_reason": drained.blocked_reason,
    }


def poll_once(
    conn: sqlite3.Connection,
    directory: str,
    read_chunk_bytes: int = READ_CHUNK,
    max_line_bytes: int = MAX_LINE_BYTES,
    carries: Optional[dict[tuple[int, int], _Carry]] = None,
) -> dict[str, Any]:
    """One fair collection pass: every visible file gets one bounded turn."""
    started = time.monotonic()
    files = list_log_files(directory)
    if carries is None:
        # Stateless callers (one-off scripts, most unit tests) simply rebuild
        # any partial line by re-reading from the committed offset each poll.
        carries = {}
    per_file: list[dict[str, Any]] = []
    total_ingested = 0
    total_malformed = 0
    total_bytes_read = 0
    blocked_files: list[dict[str, Any]] = []

    for identity in files:
        try:
            result = _process_file(
                conn, identity,
                read_chunk_bytes=read_chunk_bytes,
                max_line_bytes=max_line_bytes,
                carries=carries,
            )
        except FileNotFoundError:
            # Rotated away between scan and open: next poll resolves it.
            log.info("file %s disappeared during scan", identity.source_code)
            continue
        except OSError as exc:
            log.error("I/O error on %s: %s", identity.source_code, exc)
            continue
        per_file.append(result)
        total_ingested += result["ingested"]
        total_malformed += result["malformed"]
        total_bytes_read += result["bytes_read"]
        if result["blocked"]:
            blocked_files.append({
                "source_code": result["source_code"],
                "offset": result["offset"],
                "reason": result["blocked_reason"],
            })

    known = {(s["dev"], s["inode"]): s for s in storage.all_states(conn)}
    present = {(f.dev, f.inode) for f in files}
    for key, st in known.items():
        if key not in present:
            # Archived file removed from the shared dir: keep its committed
            # offset so a later re-appearance with the same inode cannot be
            # double-read; drop its in-memory carry as well.
            carries.pop(key, None)
            log.info(
                "tracked file %s (inode=%s) no longer present; "
                "keeping committed offset %s",
                st["source_code"], key[1], st["offset"],
            )

    return {
        "polled_at": _utc_now(),
        "duration_ms": round((time.monotonic() - started) * 1000, 3),
        "files_seen": len(files),
        "ingested": total_ingested,
        "malformed": total_malformed,
        "bytes_read": total_bytes_read,
        "read_chunk_bytes": read_chunk_bytes,
        "max_line_bytes": max_line_bytes,
        "blocked_files": blocked_files,
        "files": per_file,
    }


class CollectorLoop:
    def __init__(self, conn: sqlite3.Connection, directory: str,
                 interval: float = 0.5,
                 read_chunk_bytes: int = READ_CHUNK,
                 max_line_bytes: int = MAX_LINE_BYTES):
        validate_limits(read_chunk_bytes, max_line_bytes)
        self.conn = conn
        self.directory = directory
        self.interval = interval
        self.read_chunk_bytes = read_chunk_bytes
        self.max_line_bytes = max_line_bytes
        self._stop = False
        # Partial lines held between polls, keyed by (dev, inode). Memory is
        # bounded by one max_line_bytes-sized tail per tracked file; the
        # durable source of truth remains the committed offset in SQLite.
        self._carries: dict[tuple[int, int], _Carry] = {}
        # Serialises writers (background loop vs. POST /ingest/poll-now).
        self.write_lock = threading.Lock()
        self.last_result: Optional[dict[str, Any]] = None
        self.total_polls = 0
        self.total_ingested = 0

    def request_stop(self) -> None:
        self._stop = True

    def run_forever(self) -> None:
        log.info(
            "collector started on %s (interval=%ss, read_chunk_bytes=%s, "
            "max_line_bytes=%s)",
            self.directory, self.interval,
            self.read_chunk_bytes, self.max_line_bytes,
        )
        while not self._stop:
            try:
                with self.write_lock:
                    result = poll_once(
                        self.conn, self.directory,
                        read_chunk_bytes=self.read_chunk_bytes,
                        max_line_bytes=self.max_line_bytes,
                        carries=self._carries,
                    )
                self.last_result = result
                self.total_polls += 1
                self.total_ingested += result["ingested"]
                if result["ingested"]:
                    log.info("poll #%s ingested %s line(s) from %s file(s), "
                             "read %s byte(s)",
                             self.total_polls, result["ingested"],
                             result["files_seen"], result["bytes_read"])
                if result["blocked_files"]:
                    for blk in result["blocked_files"]:
                        log.warning("blocked: %s @offset %s — %s",
                                    blk["source_code"], blk["offset"],
                                    blk["reason"])
            except Exception:
                log.exception("poll failed; will retry")
            # Interruptible sleep so SIGTERM shuts down promptly.
            slept = 0.0
            while slept < self.interval and not self._stop:
                time.sleep(min(0.1, self.interval - slept))
                slept += 0.1
        log.info("collector stopped after %s polls, %s total lines",
                 self.total_polls, self.total_ingested)
