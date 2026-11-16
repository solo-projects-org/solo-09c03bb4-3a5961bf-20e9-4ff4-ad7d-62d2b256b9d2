"""SQLite 存储层：批次、跨度(调用记录)、入仓表、配置。单连接 + 锁，WAL 模式。"""
import json
import os
import sqlite3
import threading
import time

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "orch.db"))

_lock = threading.RLock()
_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
_conn.row_factory = sqlite3.Row
_conn.execute("PRAGMA journal_mode=WAL")
_conn.execute("PRAGMA busy_timeout=5000")

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at REAL NOT NULL,
  sample_ids TEXT NOT NULL,          -- JSON array
  status TEXT NOT NULL DEFAULT 'running',   -- running / done / failed
  finished_at REAL,
  conclusion TEXT                    -- JSON, 完成结论(含关键链/阻断)
);
CREATE TABLE IF NOT EXISTS spans (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  sample_id TEXT NOT NULL,
  stage TEXT NOT NULL,               -- fetch / qc_nonneg / qc_match / warehouse
  side TEXT NOT NULL,                -- client(编排侧) / server(服务端上报)
  service TEXT NOT NULL,             -- orchestrator / source / qc
  attempt INTEGER NOT NULL DEFAULT 1,
  request_id TEXT NOT NULL,          -- 跨服务调用关联
  status TEXT NOT NULL,              -- running/effective/ok/failed/timeout/late
  start_ts REAL NOT NULL,
  end_ts REAL,
  queue_ms REAL,                     -- 服务端等名额(槽位)耗时
  detail TEXT,                       -- JSON
  note TEXT NOT NULL DEFAULT '',
  recovery_id INTEGER              -- 非空表示该调用由启动恢复发起(旧批次接续)
);
CREATE INDEX IF NOT EXISTS idx_spans_batch ON spans(batch_id);
CREATE INDEX IF NOT EXISTS idx_spans_req ON spans(request_id);
CREATE TABLE IF NOT EXISTS warehouse (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  sample_id TEXT NOT NULL,
  concentration REAL NOT NULL,
  stored_at REAL NOT NULL,
  request_id TEXT NOT NULL,          -- 产生该入仓记录的取数尝试
  UNIQUE(batch_id, sample_id)        -- 幂等：迟到/重复结果不得重复入仓
);
CREATE TABLE IF NOT EXISTS kv (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS recoveries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  trigger TEXT NOT NULL,             -- startup
  started_at REAL NOT NULL,
  finished_at REAL,
  status TEXT NOT NULL DEFAULT 'running',   -- running / done / failed / interrupted
  decisions TEXT,                    -- JSON [{sample_id,action,stage,detail,at}]
  resumed_samples TEXT,              -- JSON array：恢复时接续的样品
  preserved_samples TEXT,            -- JSON array：保持原结论的样品
  unconfirmed TEXT,                  -- JSON [{sample_id,stage,detail,warehouse_id}]
  interrupted_spans INTEGER NOT NULL DEFAULT 0,
  note TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_recoveries_batch ON recoveries(batch_id);
"""

with _lock:
    _conn.executescript(SCHEMA)
    # 旧库补列：CREATE TABLE IF NOT EXISTS 不会给已存在的表加列
    _cols = {r[1] for r in _conn.execute("PRAGMA table_info(spans)").fetchall()}
    if "recovery_id" not in _cols:
        _conn.execute("ALTER TABLE spans ADD COLUMN recovery_id INTEGER")
    _conn.commit()


def execute(sql, args=()):
    with _lock:
        cur = _conn.execute(sql, args)
        _conn.commit()
        return cur


def query(sql, args=()):
    with _lock:
        return [dict(r) for r in _conn.execute(sql, args).fetchall()]


def query_one(sql, args=()):
    rows = query(sql, args)
    return rows[0] if rows else None


# ---------- 跨度 ----------

def insert_span(batch_id, sample_id, stage, side, service, attempt, request_id,
                status, start_ts, queue_ms=None, detail=None, recovery_id=None):
    cur = execute(
        "INSERT INTO spans(batch_id,sample_id,stage,side,service,attempt,request_id,"
        "status,start_ts,queue_ms,detail,recovery_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (batch_id, sample_id, stage, side, service, attempt, request_id, status,
         start_ts, queue_ms,
         json.dumps(detail, ensure_ascii=False) if detail is not None else None,
         recovery_id))
    return cur.lastrowid


def finish_span(span_id, end_ts, status, detail=None, note=None):
    sets, args = "end_ts=?, status=?", [end_ts, status]
    if detail is not None:
        sets += ", detail=?"
        args.append(json.dumps(detail, ensure_ascii=False))
    if note:
        sets += ", note=note||?"
        args.append(note)
    args.append(span_id)
    execute(f"UPDATE spans SET {sets} WHERE id=?", args)


def mark_status(span_id, status, note=None, only_if=None):
    sql, args = "UPDATE spans SET status=?", [status]
    if note:
        sql += ", note=note||?"
        args.append(note)
    sql += " WHERE id=?"
    args.append(span_id)
    if only_if:
        sql += " AND status=?"
        args.append(only_if)
    execute(sql, args)


def interrupt_running_client_spans(note, exclude_batches=()):
    """把重启时仍在 running 的编排侧调用如实标为中断，返回被标记的 [(span_id, batch_id)]。
    exclude_batches 内的批次不动(本进程正在跑它们，避免手工触发恢复时误伤活跃调用)。"""
    with _lock:
        if exclude_batches:
            marks = ",".join("?" * len(exclude_batches))
            rows = _conn.execute(
                "SELECT id,batch_id FROM spans WHERE side='client' AND status='running' "
                f"AND batch_id NOT IN ({marks})", exclude_batches).fetchall()
            _conn.execute(
                "UPDATE spans SET status='interrupted', note=note||? "
                "WHERE side='client' AND status='running' "
                f"AND batch_id NOT IN ({marks})", (note, *exclude_batches))
        else:
            rows = _conn.execute(
                "SELECT id,batch_id FROM spans WHERE side='client' "
                "AND status='running'").fetchall()
            _conn.execute(
                "UPDATE spans SET status='interrupted', note=note||? "
                "WHERE side='client' AND status='running'", (note,))
        _conn.commit()
        return [(r["id"], r["batch_id"]) for r in rows]


# ---------- 入仓 ----------

def warehouse_insert(batch_id, sample_id, concentration, request_id):
    """INSERT OR IGNORE + UNIQUE(batch_id,sample_id)：迟到结果不会重复入仓。"""
    cur = execute(
        "INSERT OR IGNORE INTO warehouse(batch_id,sample_id,concentration,stored_at,request_id)"
        " VALUES(?,?,?,?,?)",
        (batch_id, sample_id, concentration, time.time(), request_id))
    return cur.rowcount == 1


# ---------- 恢复 ----------

def recovery_insert(batch_id, trigger, resumed_samples, preserved_samples,
                    interrupted_spans, note=""):
    cur = execute(
        "INSERT INTO recoveries(batch_id,trigger,started_at,status,decisions,resumed_samples,"
        "preserved_samples,unconfirmed,interrupted_spans,note) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (batch_id, trigger, time.time(), "running", "[]",
         json.dumps(resumed_samples, ensure_ascii=False),
         json.dumps(preserved_samples, ensure_ascii=False), "[]",
         interrupted_spans, note))
    return cur.lastrowid


def recovery_append(recovery_id, field, item):
    """把一条明细追加进 recoveries 的 JSON 数组列(decisions/unconfirmed/preserved_samples)。
    读改写在同一把锁内完成，恢复中的多样品并发记录不会互相覆盖。"""
    assert field in ("decisions", "unconfirmed", "preserved_samples", "resumed_samples")
    with _lock:
        row = _conn.execute(f"SELECT {field} FROM recoveries WHERE id=?",
                            (recovery_id,)).fetchone()
        if not row:
            return
        try:
            arr = json.loads(row[field]) if row[field] else []
        except Exception:
            arr = []
        arr.append(item)
        _conn.execute(f"UPDATE recoveries SET {field}=? WHERE id=?",
                      (json.dumps(arr, ensure_ascii=False), recovery_id))
        _conn.commit()


def recovery_finish(recovery_id, status, note=None):
    if note:
        execute("UPDATE recoveries SET status=?, finished_at=?, note=note||? WHERE id=?",
                (status, time.time(), note, recovery_id))
    else:
        execute("UPDATE recoveries SET status=?, finished_at=? WHERE id=?",
                (status, time.time(), recovery_id))


def recoveries_for(batch_id):
    rows = query("SELECT * FROM recoveries WHERE batch_id=? ORDER BY id", (batch_id,))
    for r in rows:
        for k in ("decisions", "unconfirmed", "resumed_samples", "preserved_samples"):
            try:
                r[k] = json.loads(r[k]) if r[k] else []
            except Exception:
                r[k] = []
    return rows


def recent_recoveries(limit=50):
    rows = query("SELECT * FROM recoveries ORDER BY id DESC LIMIT ?", (limit,))
    for r in rows:
        for k in ("decisions", "unconfirmed", "resumed_samples", "preserved_samples"):
            try:
                r[k] = json.loads(r[k]) if r[k] else []
            except Exception:
                r[k] = []
    return rows


def mark_recovery_interrupted(exclude_batches=()):
    """整理上次进程遗留的"运行中"恢复记录：
    - 批次仍未定稿 → interrupted，下一次启动会重新接续；
    - 批次已定稿(定稿后进程才退出) → done，不误报为中断。
    exclude_batches 内由本进程正在恢复，不处理。"""
    with _lock:
        rows = _conn.execute(
            "SELECT r.id, r.batch_id, b.status AS bstatus FROM recoveries r "
            "LEFT JOIN batches b ON b.id=r.batch_id WHERE r.status='running'").fetchall()
        now = time.time()
        for r in rows:
            if r["batch_id"] in exclude_batches:
                continue
            if r["bstatus"] == "running":
                _conn.execute(
                    "UPDATE recoveries SET status='interrupted', finished_at=?, "
                    "note=note||? WHERE id=?",
                    (now, " 编排端在恢复过程中重启，本次恢复未完成，将由下一次启动重新接续。",
                     r["id"]))
            else:
                _conn.execute(
                    "UPDATE recoveries SET status='done', finished_at=?, note=note||? "
                    "WHERE id=?",
                    (now, " 批次已定稿后编排端退出；本次恢复实际已完成。", r["id"]))
        _conn.commit()


# ---------- 配置 ----------

CONFIG_DEFAULTS = {
    "source_delay_ms": 300,      # 源站单次执行延迟
    "source_slots": 3,           # 源站并行名额(槽位)，超出需排队等名额
    "source_fail_first": False,  # 源站每样品首次请求注入 500
    "qc_nonneg_delay_ms": 200,   # 质控①非负值 延迟
    "qc_nonneg_fail_first": False,
    "qc_match_delay_ms": 200,    # 质控②样品号对应 延迟
    "qc_match_fail_first": False,
    "client_timeout_ms": 2000,   # 编排侧取数逻辑超时(超时后重试，旧请求仍可能运行)
    "max_attempts": 3,           # 取数最大尝试次数
}


def get_config():
    row = query_one("SELECT v FROM kv WHERE k='config'")
    cfg = dict(CONFIG_DEFAULTS)
    if row:
        cfg.update(json.loads(row["v"]))
    return cfg


def save_config(cfg):
    execute("INSERT INTO kv(k,v) VALUES('config',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (json.dumps(cfg, ensure_ascii=False),))
