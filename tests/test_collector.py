#!/usr/bin/env python3
"""End-to-end correctness tests for the collector.

Runs entirely on the local filesystem + SQLite (no Docker needed):

* partial (unterminated) tails are held back until the newline arrives
* rotation by rename: old inode keeps draining, new file starts at offset 0
* collector exit at arbitrary points resumes without gaps or duplicates
* identical content lines are stored as separate rows
* committed data only is visible to queries (transactional)
* FastAPI query/pagination/progress endpoints work
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.request

# Make `app` importable when run as `python tests/test_collector.py`
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collector"))

from app import storage  # noqa: E402
from app.ingest import CollectorLoop, drain_file, poll_once  # noqa: E402

PASS, FAIL = 0, 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def jline(path: str, status: int, ts: str = "2026-11-13T10:00:00Z") -> str:
    return json.dumps({"time": ts, "path": path, "status": status,
                       "method": "GET"}) + "\n"


def fresh_env():
    base = tempfile.mkdtemp(prefix="collector-test-")
    log_dir = os.path.join(base, "logs")
    db_path = os.path.join(base, "data", "warehouse.db")
    os.makedirs(log_dir)
    os.makedirs(os.path.dirname(db_path))
    conn = storage.connect(db_path)
    storage.init_db(conn)
    return base, log_dir, db_path, conn


def test_drain_partial_tail():
    print("\n[1] 未换行尾部必须等补齐后入仓")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            fh.write(jline("/a", 200))
            fh.write('{"time":"x","path":"/partial","status":5')  # no newline

        r = poll_once(conn, log_dir)
        check("first poll ingests only the complete line", r["ingested"] == 1)
        st = storage.all_states(conn)[0]
        check("offset stops before partial tail",
              st["offset"] == len(jline("/a", 200)),
              f"offset={st['offset']}")
        n = conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        check("warehouse contains 1 row", n == 1)

        # complete the line
        with open(f, "a") as fh:
            fh.write('00,"method":"GET"}\n')
        r = poll_once(conn, log_dir)
        check("second poll ingests the now-completed line", r["ingested"] == 1)
        rows = conn.execute("SELECT path, status FROM log_events ORDER BY id").fetchall()
        check("both rows present with correct content",
              [dict(x) for x in rows] == [
                  {"path": "/a", "status": 200},
                  {"path": "/partial", "status": 500}],
              str([dict(x) for x in rows]))
        st = storage.all_states(conn)[0]
        check("offset reaches EOF", st["offset"] == os.path.getsize(f))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_rotation_rename():
    print("\n[2] rename 轮转：旧 inode 续采、新文件独立偏移")
    base, log_dir, db_path, conn = fresh_env()
    try:
        cur = os.path.join(log_dir, "access.log")
        with open(cur, "w") as fh:
            for i in range(5):
                fh.write(jline(f"/old/{i}", 200))
        poll_once(conn, log_dir)
        check("5 lines ingested pre-rotation",
              conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"] == 5)

        old_inode = os.stat(cur).st_ino
        archive = os.path.join(log_dir, "access.log.20261113T100000Z.log")
        os.rename(cur, archive)
        # An open fd on the archive keeps its inode allocated, exactly like
        # nginx workers that still hold the renamed log file.
        arch_fh = open(archive, "a")
        # old inode receives 2 late lines (in-flight requests)
        arch_fh.write(jline("/late/1", 503))
        arch_fh.write(jline("/late/2", 500))
        arch_fh.flush()
        # nginx reopens a brand new file at the same path
        with open(cur, "w") as fh:
            fh.write(jline("/new/1", 404))

        r = poll_once(conn, log_dir)
        check("3 lines ingested across both inodes", r["ingested"] == 3,
              f"got {r['ingested']}")

        states = {(s["inode"]): s for s in storage.all_states(conn)}
        check("two distinct inodes tracked", len(states) == 2)
        old_state = states[old_inode]
        check("old inode state keeps archive source_code",
              old_state["source_code"].startswith("access.log.20261113"),
              old_state["source_code"])
        check("old inode drained to its EOF",
              old_state["offset"] == os.path.getsize(archive))
        new_inode = os.stat(cur).st_ino
        check("new inode differs from old", new_inode != old_inode)
        check("new inode offset is its own size (starts at 0, not old offset)",
              states[new_inode]["offset"] == os.path.getsize(cur))

        rows = conn.execute(
            "SELECT source_code, path FROM log_events ORDER BY id"
        ).fetchall()
        srcs = {x["source_code"] for x in rows}
        check("rows carry both source file codes",
              srcs == {"access.log", "access.log.20261113T100000Z.log"}, str(srcs))
        check("late lines landed against the archive code",
              all(x["source_code"].startswith("access.log.2026")
                  for x in rows if x["path"].startswith("/late")))
        arch_fh.close()
    finally:
        conn.close()
        shutil.rmtree(base)


def test_duplicate_content_kept():
    print("\n[3] 内容相同的两行分别保留")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        line = jline("/same", 500)
        with open(f, "w") as fh:
            fh.write(line * 4)
        poll_once(conn, log_dir)
        rows = conn.execute(
            "SELECT byte_offset FROM log_events ORDER BY byte_offset"
        ).fetchall()
        check("4 rows for 4 identical lines", len(rows) == 4)
        offsets = [r["byte_offset"] for r in rows]
        check("each row has a distinct byte offset",
              offsets == [0, len(line), 2 * len(line), 3 * len(line)],
              str(offsets))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_resume_no_gap_no_dup():
    print("\n[4] 任意时刻退出后自动续采：不漏不重")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        total = 500
        with open(f, "w") as fh:
            for i in range(total):
                fh.write(jline(f"/p/{i}", 200 if i % 7 else 500))

        # Simulate many crash/restart cycles while draining.
        seen = 0
        for cycle in range(20):
            conn2 = storage.connect(db_path)
            r = poll_once(conn2, log_dir)
            seen += r["ingested"]
            conn2.close()  # hard close = "process exit"
            if seen >= total:
                break
        check(f"all {total} lines ingested across restarts", seen == total,
              f"seen={seen}")

        # Re-polling changes nothing: offsets are committed.
        r = poll_once(conn, log_dir)
        check("idle poll ingests zero duplicates", r["ingested"] == 0)
        n = conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        check("exactly one row per physical line", n == total, f"n={n}")
        check("unique position constraint holds",
              conn.execute(
                  "SELECT COUNT(*) c FROM (SELECT DISTINCT dev,inode,source_code,byte_offset FROM log_events)"
              ).fetchone()["c"] == total)
        paths = [r["path"] for r in conn.execute(
            "SELECT path FROM log_events ORDER BY id").fetchall()]
        check("order/content matches the source file exactly",
              paths == [f"/p/{i}" for i in range(total)])
    finally:
        conn.close()
        shutil.rmtree(base)


def test_background_loop_and_rotation_interleave():
    print("\n[5] 后台循环采集与轮转交错")
    base, log_dir, db_path, conn = fresh_env()
    try:
        loop_conn = storage.connect(db_path)
        loop = CollectorLoop(loop_conn, log_dir, interval=0.05)
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()

        cur = os.path.join(log_dir, "access.log")
        written = 0
        archives = 0
        fh = open(cur, "w")
        archive_handles = []  # keep rotated inodes alive, as nginx workers do
        for round_no in range(6):
            for i in range(30):
                fh.write(jline(f"/r{round_no}/line{i}", 200))
                written += 1
            fh.flush()
            time.sleep(0.05)
            # rotate mid-stream
            fh.close()
            archives += 1
            arch_path = os.path.join(log_dir, f"access.log.rot{round_no}.log")
            os.rename(cur, arch_path)
            # Holding the fd keeps the old inode from being recycled by the
            # filesystem — equivalent to nginx workers finishing in-flight
            # requests against the renamed file.
            archive_handles.append(open(arch_path, "a"))
            # late write into the old inode
            archive_handles[-1].write(jline(f"/r{round_no}/late", 599))
            archive_handles[-1].flush()
            written += 1
            fh = open(cur, "w")
            time.sleep(0.05)
        fh.close()

        deadline = time.time() + 10
        while time.time() < deadline:
            got = loop_conn.execute(
                "SELECT COUNT(*) c FROM log_events").fetchone()["c"]
            if got == written:
                break
            time.sleep(0.1)
        loop.request_stop()
        t.join(timeout=5)
        got = loop_conn.execute(
            "SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        check("every line captured under interleaved rotation",
              got == written, f"got={got} written={written}")
        check("late 599 lines captured from archive inodes",
              loop_conn.execute(
                  "SELECT COUNT(*) c FROM log_events WHERE status=599"
              ).fetchone()["c"] == 6)
        # every archive fully drained
        bad = [dict(s) for s in storage.all_states(loop_conn)
               if s["offset"] != os.path.getsize(s["path"])]
        check("all tracked offsets equal current file sizes", not bad, str(bad))
        # Guaranteed properties, independent of the filesystem's inode
        # allocation policy: every archive is tracked as a distinct source
        # and every source is drained exactly to its current size. On the
        # Docker volume nginx holds every rotated fd open, so inodes are never
        # recycled and the live file additionally appears as "access.log".
        codes = {s["source_code"] for s in storage.all_states(loop_conn)}
        expected_archives = {f"access.log.rot{r}.log" for r in range(6)}
        check("all 6 archives tracked as distinct sources",
              expected_archives.issubset(codes), f"codes={sorted(codes)}")
        for h in archive_handles:
            h.close()
        loop_conn.close()
    finally:
        conn.close()
        shutil.rmtree(base)


def test_malformed_quarantine():
    print("\n[6] 坏行隔离且不阻断后续行")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            fh.write(jline("/ok", 200))
            fh.write("{this is not json}\n")
            fh.write(jline("/ok2", 500))
        r = poll_once(conn, log_dir)
        check("2 good + 1 malformed reported",
              r["ingested"] == 2 and r["malformed"] == 1, str(r))
        check("pipeline continued past the bad line",
              conn.execute(
                  "SELECT path FROM log_events ORDER BY id"
              ).fetchall()[-1]["path"] == "/ok2")
        check("bad line preserved with its byte offset",
              conn.execute(
                  "SELECT byte_offset, error FROM malformed_lines"
              ).fetchone()["byte_offset"] == len(jline("/ok", 200)))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_api():
    print("\n[7] FastAPI：进度、分页、按路径/状态码查询")
    # Environment must be configured BEFORE importing app.main, because its
    # path constants are resolved at import time.
    base = tempfile.mkdtemp(prefix="collector-api-")
    log_dir = os.path.join(base, "logs")
    db_path = os.path.join(base, "data", "warehouse.db")
    os.makedirs(log_dir)
    os.makedirs(os.path.dirname(db_path))
    os.environ["DB_PATH"] = db_path
    os.environ["LOG_DIR"] = log_dir
    os.environ["POLL_INTERVAL"] = "0.05"

    import uvicorn
    from app.main import app

    config = uvicorn.Config(app, host="127.0.0.1", port=8765, log_level="warning")
    server = uvicorn.Server(config)
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen("http://127.0.0.1:8765/health", timeout=1)
                break
            except Exception:
                time.sleep(0.1)

        f = os.path.join(log_dir, "access.log")
        with open(f, "w") as fh:
            for i in range(25):
                fh.write(jline("/api/orders", 200))
            for i in range(5):
                fh.write(jline("/api/orders", 500))
            for i in range(3):
                fh.write(jline("/api/payments", 503))

        deadline = time.time() + 10
        while time.time() < deadline:
            h = json.load(urllib.request.urlopen(
                "http://127.0.0.1:8765/health"))
            if h["counts"]["events"] == 33:
                break
            time.sleep(0.1)
        check("background ingestion visible via /health",
              h["counts"]["events"] == 33, str(h["counts"]))

        prog = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/ingest/progress"))
        check("progress shows file complete (offset == size)",
              prog["files"][0]["complete"] is True
              and prog["files"][0]["offset"] == prog["files"][0]["size"],
              str(prog["files"][0]))

        page = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?limit=10&offset=0"))
        check("pagination metadata correct",
              page["total"] == 33 and len(page["items"]) == 10
              and page["next_offset"] == 10, str({k: page[k] for k in
              ("total", "next_offset")}))

        page2 = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?limit=10&offset=30"))
        check("last page has 3 rows and no next_offset",
              len(page2["items"]) == 3 and page2["next_offset"] is None)

        errs = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?status_min=500&status_max=599"))
        check("status range query finds all 8 failures",
              errs["total"] == 8 and all(i["status"] >= 500 for i in errs["items"]),
              f"total={errs['total']}")

        s503 = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?status=503"))
        check("exact status=503 query", s503["total"] == 3)

        by_path = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?path=/api/orders&limit=100"))
        check("path filter returns 30 order events",
              by_path["total"] == 30
              and all(i["path"] == "/api/orders" for i in by_path["items"]))

        both = json.load(urllib.request.urlopen(
            "http://127.0.0.1:8765/events?path=/api/orders&status=500"))
        check("path + status combined filter", both["total"] == 5)

        one = json.load(urllib.request.urlopen(
            f"http://127.0.0.1:8765/events/{page['items'][0]['id']}"))
        check("single event returns raw JSON line",
              json.loads(one["raw_line"])["path"] == "/api/orders")

        code = urllib.request.urlopen(
            "http://127.0.0.1:8765/events/999999").getcode()
    except urllib.error.HTTPError as e:
        code = e.code
        check("unknown event id -> 404", code == 404, str(code))
    else:
        check("unknown event id -> 404", False, f"got {code}")
    finally:
        server.should_exit = True
        t.join(timeout=5)
        shutil.rmtree(base)


def test_offset_math_after_partial():
    print("\n[8] 偏移量计算：部分行补齐后字节位置精确")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        l1 = jline("/x", 200)
        with open(f, "w") as fh:
            fh.write(l1)
            fh.write('{"incomplete":')
        poll_once(conn, log_dir)
        tail_part = 'true}\n'
        with open(f, "a") as fh:
            fh.write(tail_part)
        poll_once(conn, log_dir)
        rows = conn.execute(
            "SELECT byte_offset, raw_line FROM log_events ORDER BY id").fetchall()
        check("second line offset = length of first line",
              rows[1]["byte_offset"] == len(l1.encode()),
              str(rows[1]["byte_offset"]))
        st = storage.all_states(conn)[0]
        check("final offset equals file size",
              st["offset"] == os.path.getsize(f),
              f"{st['offset']} != {os.path.getsize(f)}")
    finally:
        conn.close()
        shutil.rmtree(base)


def test_multibyte_offsets_and_chunk_boundaries():
    print("\n[9] 多字节 UTF-8：字节偏移精确、跨读取块字符不损坏（分轮排空）")
    base, log_dir, db_path, conn = fresh_env()
    # Tiny bounded reads: multi-byte chars, newlines and partial tails all
    # straddle chunk boundaries. The line limit sits above the longest real
    # line so chunking — not oversized-line protection — drives this test;
    # every line therefore arrives reassembled from several chunks.
    chunk = 7
    max_line = 512
    try:
        f = os.path.join(log_dir, "access.log")
        lines = [
            jline("/unicafé/✓", 200),   # é (2 bytes) and ✓ (3 bytes)
            jline("/ascii/normal", 404),
            jline("/中文/路径", 500),
            jline("/emoji/😀/end", 503),
        ]
        with open(f, "wb") as fh:
            for ln in lines:
                fh.write(ln.encode("utf-8"))
        # One bounded chunk per poll: each file drains over several ticks.
        # A shared carry registry mirrors the real CollectorLoop: a line
        # straddling chunks is reassembled across polls while the committed
        # offset stays at the line's first byte. Early polls may hold only a
        # partial tail and persist no state yet.
        carries = {}
        for _ in range(500):
            r = poll_once(conn, log_dir, read_chunk_bytes=chunk,
                          max_line_bytes=max_line, carries=carries)
            states = storage.all_states(conn)
            if states and states[0]["offset"] == os.path.getsize(f):
                break
            assert r["bytes_read"] <= chunk, "single poll exceeds byte budget"
        rows = conn.execute(
            "SELECT byte_offset, path, raw_line FROM log_events ORDER BY id"
        ).fetchall()
        check("all 4 multibyte lines ingested across chunk boundaries",
              len(rows) == 4)
        raw = open(f, "rb").read()
        expected_offsets, cursor = [], 0
        for ln in lines:
            expected_offsets.append(cursor)
            cursor += len(ln.encode("utf-8"))
        check("byte offsets match true byte positions",
              [r["byte_offset"] for r in rows] == expected_offsets,
              str([r["byte_offset"] for r in rows]))
        check("committed offset equals exact file size in bytes",
              storage.all_states(conn)[0]["offset"] == len(raw))
        # raw lines round-trip exactly (no replacement chars introduced)
        check("raw content preserved byte-for-byte",
              [r["raw_line"] for r in rows] ==
              [ln.rstrip("\n") for ln in lines])
        check("unicode paths normalised correctly",
              [r["path"] for r in rows] ==
              ["/unicafé/✓", "/ascii/normal", "/中文/路径", "/emoji/😀/end"],
              str([r["path"] for r in rows]))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_bounded_bytes_per_poll():
    print("\n[10] 单文件每轮只读有限字节，大文件分多批入仓、内存有界")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        line = jline("/bulk", 200)
        n_lines = 200
        with open(f, "w") as fh:
            for _ in range(n_lines):
                fh.write(line)
        size = os.path.getsize(f)

        chunk = 4096  # far smaller than the whole file
        carries = {}
        r1 = poll_once(conn, log_dir, read_chunk_bytes=chunk,
                       max_line_bytes=chunk - 1, carries=carries)
        check("first poll reads at most the byte budget",
              r1["bytes_read"] <= chunk, str(r1["bytes_read"]))
        check("first poll ingests only a bounded batch",
              0 < r1["ingested"] < n_lines, str(r1["ingested"]))
        st = storage.all_states(conn)[0]
        check("committed offset never passes the last newline",
              st["offset"] <= size and st["offset"] == r1["files"][0]["offset"])
        rows_first = conn.execute(
            "SELECT COUNT(*) c FROM log_events").fetchone()["c"]

        # Drain to EOF over successive polls.
        polls = 1
        for _ in range(500):
            r = poll_once(conn, log_dir, read_chunk_bytes=chunk,
                          max_line_bytes=chunk - 1, carries=carries)
            polls += 1
            check("every poll respects the per-file byte budget",
                  r["bytes_read"] <= chunk, str(r["bytes_read"]))
            if storage.all_states(conn)[0]["offset"] == size:
                break
        check("large file needed multiple polls (no whole-file read)",
              polls > 1, f"polls={polls}")
        n = conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        check("every line eventually ingested exactly once",
              n == n_lines, f"n={n}")
        check("first batch rows were not re-recorded",
              rows_first <= n and conn.execute(
                  "SELECT COUNT(*) c FROM (SELECT DISTINCT byte_offset "
                  "FROM log_events)").fetchone()["c"] == n_lines)
        check("final offset equals file size",
              storage.all_states(conn)[0]["offset"] == size)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_fairness_other_files_not_starved():
    print("\n[11] 大文件不独占采集轮次：每轮每个文件都有一次机会")
    base, log_dir, db_path, conn = fresh_env()
    try:
        big = os.path.join(log_dir, "access.log")
        small = os.path.join(log_dir, "short.log")
        line = jline("/big", 200)
        with open(big, "w") as fh:
            for _ in range(300):
                fh.write(line)
        with open(small, "w") as fh:
            for _ in range(3):
                fh.write(jline("/short", 503))

        chunk = 1024
        r = poll_once(conn, log_dir, read_chunk_bytes=chunk,
                      max_line_bytes=chunk - 1)
        by_src = {x["source_code"]: x for x in r["files"]}
        check("both files visited in one poll", set(by_src) ==
              {"access.log", "short.log"}, str(sorted(by_src)))
        check("large file read only one bounded chunk",
              0 < by_src["access.log"]["bytes_read"] <= chunk,
              str(by_src["access.log"]))
        check("small file fully drained on the first shared poll",
              by_src["short.log"]["offset"] == by_src["short.log"]["size"]
              and by_src["short.log"]["ingested"] == 3,
              str(by_src["short.log"]))
        small_rows = conn.execute(
            "SELECT COUNT(*) c FROM log_events WHERE source_code = 'short.log'"
        ).fetchone()["c"]
        check("short log already in the warehouse while big log remains",
              small_rows == 3 and
              conn.execute(
                  "SELECT COUNT(*) c FROM log_events WHERE source_code = "
                  "'access.log'").fetchone()["c"] < 300)
        check("per-poll result exposes the active limits",
              r["read_chunk_bytes"] == chunk and
              r["max_line_bytes"] == chunk - 1)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_oversized_line_blocks_with_reason():
    print("\n[12] 单行超过批次处理能力：保留原偏移并在进度中说明阻塞原因")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        # Short, hand-written JSON lines keep the fixture below every limit;
        # only the deliberately huge physical line is oversized.
        good = '{"path":"/ok","status":200}\n'
        after = '{"path":"/after","status":500}\n'
        with open(f, "wb") as fh:
            fh.write(good.encode())
            # One physical line (terminated) longer than the limit.
            fh.write(b"x" * 400)
            fh.write(b"\n")
            fh.write(after.encode())

        max_line, chunk = 64, 128
        carries = {}
        r = poll_once(conn, log_dir, read_chunk_bytes=chunk,
                      max_line_bytes=max_line, carries=carries)
        blk = [x for x in r["files"] if x["blocked"]]
        check("poll reports one blocked file", len(blk) == 1, str(r["files"]))
        check("top-level blocked_files summary populated",
              len(r["blocked_files"]) == 1 and
              "max_line_bytes" in r["blocked_files"][0]["reason"])
        st = storage.all_states(conn)[0]
        line_start = len(good.encode())
        check("committed offset held at the oversized line's start",
              st["offset"] == line_start and st["blocked_reason"] is not None,
              str(dict(st)))
        check("rows before the oversized line were committed",
              conn.execute(
                  "SELECT COUNT(*) c FROM log_events").fetchone()["c"] == 1)
        check("rows after the oversized line were not reached",
              conn.execute(
                  "SELECT COUNT(*) c FROM log_events WHERE path = '/after'"
              ).fetchone()["c"] == 0)

        # Re-polling must not split, skip or record the line again.
        r2 = poll_once(conn, log_dir, read_chunk_bytes=chunk,
                       max_line_bytes=max_line, carries=carries)
        st2 = storage.all_states(conn)[0]
        check("block persists and offset is unchanged on repeat polls",
              st2["offset"] == line_start and r2["blocked_files"] and
              st2["blocked_reason"] is not None
              and st2["blocked_at"] == st["blocked_at"],
              str(dict(st2)))
        check("still exactly one ingested row (no duplicates)",
              conn.execute(
                  "SELECT COUNT(*) c FROM log_events").fetchone()["c"] == 1)

        # A different, healthy file is still collected while the first blocks.
        other = os.path.join(log_dir, "other.log")
        with open(other, "w") as fh:
            fh.write('{"path":"/other/ok","status":200}\n')
        r3 = poll_once(conn, log_dir, read_chunk_bytes=chunk,
                       max_line_bytes=max_line, carries=carries)
        by_src = {x["source_code"]: x for x in r3["files"]}
        check("blocked file stays blocked", by_src["access.log"]["blocked"])
        check("other file ingested normally despite the block",
              by_src["other.log"]["ingested"] == 1 and
              not by_src["other.log"]["blocked"])
        check("other file's rows are in the warehouse",
              conn.execute(
                  "SELECT COUNT(*) c FROM log_events WHERE source_code = "
                  "'other.log'").fetchone()["c"] == 1)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_block_clears_after_remediation():
    print("\n[13] 超长行修复后自动解除阻塞，从原偏移续采、不漏不重")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        good = '{"path":"/ok","status":200}\n'
        after = '{"path":"/after","status":500}\n'
        with open(f, "wb") as fh:
            fh.write(good.encode())
            fh.write(b"x" * 400)
            fh.write(b"\n")
            fh.write(after.encode())

        max_line, chunk = 64, 128
        carries = {}
        poll_once(conn, log_dir, read_chunk_bytes=chunk,
                  max_line_bytes=max_line, carries=carries)
        line_start = len(good.encode())
        st = storage.all_states(conn)[0]
        assert st["offset"] == line_start and st["blocked_reason"]

        # Operator truncates the offending line below the limit, keeping the
        # same byte position (file rewritten in place for the test).
        with open(f, "wb") as fh:
            fh.write(good.encode())
            fh.write(b"x" * 20)
            fh.write(b"\n")
            fh.write(after.encode())
        for _ in range(50):
            poll_once(conn, log_dir, read_chunk_bytes=chunk,
                      max_line_bytes=max_line, carries=carries)
            st = storage.all_states(conn)[0]
            if st["offset"] == os.path.getsize(f):
                break
        check("block marker cleared after remediation",
              st["blocked_reason"] is None and st["blocked_at"] is None,
              str(dict(st)))
        check("file drained to EOF from the held offset",
              st["offset"] == os.path.getsize(f))
        rows = conn.execute(
            "SELECT path FROM log_events ORDER BY id").fetchall()
        check("no line skipped or duplicated after recovery",
              [r["path"] for r in rows] == ["/ok", "/after"],
              str([r["path"] for r in rows]))
    finally:
        conn.close()
        shutil.rmtree(base)


def test_crash_resume_within_batched_file():
    print("\n[14] 分批提交的大文件在多次“进程退出”后续采：仅从最后批次恢复")
    base, log_dir, db_path, conn = fresh_env()
    try:
        f = os.path.join(log_dir, "access.log")
        total = 400
        with open(f, "w") as fh:
            for i in range(total):
                fh.write(jline(f"/c/{i}", 200 if i % 5 else 500))

        chunk = 2048
        seen = 0
        for _ in range(40):
            # Brand-new connection each cycle = hard process exit; only the
            # last committed batch can survive. The in-memory carry is lost
            # too, so each restart re-reads the partial line from its
            # committed start offset.
            conn2 = storage.connect(db_path)
            r = poll_once(conn2, log_dir, read_chunk_bytes=chunk,
                          max_line_bytes=chunk - 1)
            seen += r["ingested"]
            conn2.close()
            if seen >= total:
                break
        check(f"all {total} lines ingested across batched crash cycles",
              seen == total, f"seen={seen}")
        n = conn.execute("SELECT COUNT(*) c FROM log_events").fetchone()["c"]
        check("exactly one row per physical line after recovery", n == total)
        check("offsets committed per batch form a gapless sequence",
              conn.execute(
                  "SELECT COUNT(*) c FROM (SELECT DISTINCT byte_offset "
                  "FROM log_events)").fetchone()["c"] == total)
        paths = [r["path"] for r in conn.execute(
            "SELECT path FROM log_events ORDER BY id").fetchall()]
        check("row order/content matches the source exactly",
              paths == [f"/c/{i}" for i in range(total)])
        check("idle re-poll ingests nothing",
              poll_once(conn, log_dir, read_chunk_bytes=chunk,
                        max_line_bytes=chunk - 1)["ingested"] == 0)
    finally:
        conn.close()
        shutil.rmtree(base)


def test_long_line_completes_after_carry_losing_restart():
    print("\n[15] 长行在丢失跨轮缓存的重启后，仍能在一轮内读到换行而不卡死")
    from app.ingest import validate_limits
    base, log_dir, db_path, conn = fresh_env()
    try:
        # The configured invariant itself must reject an unusable ordering.
        for args, acceptable in (
            ((1024, 1024), True), ((1024, 512), True),
            ((512, 1024), False), ((0, 10), False), ((10, 0), False),
        ):
            try:
                validate_limits(*args)
                got = True
            except ValueError:
                got = False
            check(f"validate_limits{args} {'accepts' if acceptable else 'rejects'}",
                  got == acceptable)

        f = os.path.join(log_dir, "access.log")
        # One long line (~226 bytes) plus a normal line. With a 256-byte
        # budget the long line fits one read once its first byte is reached.
        long_line = b'{"path":"/long","pad":"' + b"a" * 200 + b'"}\n'
        tail = b'{"path":"/tail","status":200}\n'
        with open(f, "wb") as fh:
            fh.write(long_line)
            fh.write(tail)
        size = os.path.getsize(f)

        # Simulate a process that restarts on the long line's first poll:
        # no in-memory carry exists, yet it must finish within the strict cap.
        fresh = storage.connect(db_path)
        r = poll_once(fresh, log_dir, read_chunk_bytes=256,
                      max_line_bytes=256)
        check("fresh start never exceeds the per-file byte budget",
              r["bytes_read"] <= 256, str(r["bytes_read"]))
        check("long line completed on a carry-less restart (no stall)",
              r["ingested"] == 2, str(r["ingested"]))
        st = storage.all_states(fresh)[0]
        check("offset reaches EOF and no block is raised",
              st["offset"] == size and st["blocked_reason"] is None,
              str(dict(st)))
        fresh.close()
    finally:
        conn.close()
        shutil.rmtree(base)


if __name__ == "__main__":
    test_drain_partial_tail()
    test_rotation_rename()
    test_duplicate_content_kept()
    test_resume_no_gap_no_dup()
    test_background_loop_and_rotation_interleave()
    test_malformed_quarantine()
    test_api()
    test_offset_math_after_partial()
    test_multibyte_offsets_and_chunk_boundaries()
    test_bounded_bytes_per_poll()
    test_fairness_other_files_not_starved()
    test_oversized_line_blocks_with_reason()
    test_block_clears_after_remediation()
    test_crash_resume_within_batched_file()
    test_long_line_completes_after_carry_losing_restart()
    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    sys.exit(1 if FAIL else 0)
