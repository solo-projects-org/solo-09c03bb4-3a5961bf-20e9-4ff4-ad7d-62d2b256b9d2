#!/usr/bin/env python3
"""启动恢复自测：真实启动三服务子进程，在取数/质控/入仓途中杀掉编排端再拉起，
验证接续、保持原结论、不重复入仓、旧请求迟到不作成功依据、再次重启仍可接续。

用法：python scripts/recovery_selftest.py
仅依赖标准库 + httpx(自测脚本用)；服务端代码用本机 python3 启动。
"""
import json
import os
import signal
import socket
import subprocess
import sys
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORK = os.environ.get("RECOVERY_WORK", "/tmp/recovery_selftest")
PY = sys.executable
ORCH = "http://localhost:8000"
SRC = "http://localhost:8001"
QC = "http://localhost:8002"

FAILS = []


def check(name, cond, extra=""):
    print(("PASS " if cond else "FAIL ") + name + (f"  [{extra}]" if extra else ""), flush=True)
    if not cond:
        FAILS.append(name)


def wait_http(url, timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if httpx.get(url, timeout=1).status_code < 500:
                return True
        except Exception:
            pass
        time.sleep(0.1)
    return False


class Services:
    def __init__(self, work):
        self.work = work
        self.procs = {}
        self.logs = {}

    def _spawn(self, name, cwd, env, port):
        log = open(os.path.join(self.work, f"{name}.log"), "ab")
        self.logs.setdefault(name, []).append(log)
        p = subprocess.Popen([PY, "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
                              "--port", str(port)], cwd=cwd, env={**os.environ, **env},
                             stdout=log, stderr=subprocess.STDOUT)
        self.procs[name] = p
        return p

    def start_source(self):
        self._spawn("source", os.path.join(ROOT, "source_app"),
                    {"ORCH_URL": ORCH}, 8001)

    def start_qc(self):
        self._spawn("qc", os.path.join(ROOT, "qc_app"),
                    {"ORCH_URL": ORCH}, 8002)

    def start_orch(self):
        self._spawn("orch", os.path.join(ROOT, "app"),
                    {"SOURCE_URL": SRC, "QC_URL": QC,
                     "DB_PATH": os.path.join(self.work, "orch.db")}, 8000)
        return wait_http(f"{ORCH}/api/health")

    def kill_orch(self, sig=signal.SIGKILL):
        p = self.procs.get("orch")
        if p and p.poll() is None:
            p.send_signal(sig)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
        self.procs.pop("orch", None)

    def stop_all(self):
        for name in list(self.procs):
            p = self.procs.pop(name)
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
        for logs in self.logs.values():
            for f in logs:
                f.close()


def set_config(c, **kw):
    r = c.post(f"{ORCH}/api/config", json=kw, timeout=10)
    r.raise_for_status()
    return r.json()


def submit(c, ids):
    return c.post(f"{ORCH}/api/batches", json={"sample_ids": ids}, timeout=10).json()["id"]


def detail(c, bid):
    return c.get(f"{ORCH}/api/batches/{bid}", timeout=10).json()


def wait_status(c, bid, statuses=("done", "failed"), timeout=90):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            d = detail(c, bid)
            if d["batch"]["status"] in statuses:
                return d
        except Exception:
            pass
        time.sleep(0.2)
    return detail(c, bid)


def wait_until(pred, timeout=30, interval=0.1):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def main():
    os.makedirs(WORK, exist_ok=True)
    for f in ("orch.db", "orch.db-wal", "orch.db-shm"):
        try:
            os.unlink(os.path.join(WORK, f))
        except FileNotFoundError:
            pass

    svc = Services(WORK)
    c = httpx.Client()
    try:
        svc.start_source()
        svc.start_qc()
        assert wait_http(f"{SRC}/health") and wait_http(f"{QC}/health")
        assert svc.start_orch()

        # ============ 场景 A：取数途中重启，旧请求照样跑到迟到上报 ============
        # 源站慢(2.0s) 且取数超时 900ms；杀掉编排端后旧请求仍在源站跑完并向新进程迟到上报
        set_config(c, source_delay_ms=2000, source_slots=8, source_fail_first=False,
                   qc_nonneg_delay_ms=100, qc_match_delay_ms=100,
                   client_timeout_ms=900, max_attempts=3)
        idsA = [f"A-{i}" for i in range(3)]
        bidA = submit(c, idsA)
        # 等到至少一条取数调用在跑
        wait_until(lambda: any(s["stage"] == "fetch" and s["status"] == "running"
                               for s in detail(c, bidA)["spans"]), timeout=10)
        time.sleep(0.3)
        svc.kill_orch()
        # 编排端已死：源站旧请求继续执行完毕(2s)并向新编排端迟到上报
        time.sleep(0.6)
        assert svc.start_orch()

        d = wait_status(c, bidA, timeout=120)
        check("A 取数途中重启后批次最终定稿", d["batch"]["status"] in ("done", "failed"),
              d["batch"]["status"])
        check("A 全部样品入仓", len(d["warehouse"]) == len(idsA), f"{len(d['warehouse'])}/{len(idsA)}")
        check("A 批次报完成", d["batch"]["status"] == "done", d["batch"]["status"])
        check("A 入仓每样品唯一", len({w["sample_id"] for w in d["warehouse"]}) == len(idsA))
        recs = d.get("recoveries") or []
        check("A 记录了启动恢复", len(recs) >= 1, f"{len(recs)} 次")
        # 恢复尝试与旧请求各用不同 request_id：恢复新尝试标记 recovery_id
        fetch_client = [s for s in d["spans"] if s["stage"] == "fetch" and s["side"] == "client"]
        rec_fetch = [s for s in fetch_client if s.get("recovery_id")]
        check("A 恢复发起过新取数尝试", len(rec_fetch) >= 1, f"{len(rec_fetch)} 条")
        old_running = [s for s in fetch_client if s["status"] == "interrupted"]
        check("A 旧的在途调用如实标为中断", len(old_running) >= 1, f"{len(old_running)} 条")
        # 旧请求迟到上报为服务端跨度，不被当成恢复尝试的有效依据
        old_client_reqs = {s["request_id"] for s in old_running}
        srv_late = [s for s in d["spans"] if s["side"] == "server"
                    and s["request_id"] in old_client_reqs]
        check("A 旧请求存在服务端迟到记录", len(srv_late) >= 1, f"{len(srv_late)} 条")
        eff = [s for s in fetch_client if s["status"] == "effective"]
        check("A 每样品恰一个有效取数", len(eff) == len(idsA), f"{len(eff)}/{len(idsA)}")
        check("A 有效尝试均是恢复发起的(旧请求未冒充)",
              all(s.get("recovery_id") for s in eff),
              str([(s["sample_id"], s["status"], s.get("recovery_id")) for s in eff]))
        # 旧请求的 request_id 没有成为任何有效取数的 request_id
        eff_reqs = {s["request_id"] for s in eff}
        check("A 旧请求未被当作新尝试成功依据", not (eff_reqs & old_client_reqs))
        # 结论仍完整
        check("A 结论含关键链", bool(d["conclusion"] and d["conclusion"]["chain"]))

        # ============ 场景 B：质控途中重启；已明确质控不通过的样品保持原结论 ============
        set_config(c, source_delay_ms=200, source_slots=8,
                   qc_nonneg_delay_ms=1500, qc_match_delay_ms=1500,
                   client_timeout_ms=3000, max_attempts=2)
        idsB = [f"B-{i}" for i in range(4)]
        bidB = submit(c, idsB)
        wait_until(lambda: any(s["stage"] == "qc_nonneg" and s["status"] == "running"
                               for s in detail(c, bidB)["spans"]), timeout=15)
        time.sleep(0.2)
        svc.kill_orch()
        assert svc.start_orch()
        # 恢复时把质控调快，让接续尽快完成
        set_config(c, qc_nonneg_delay_ms=100, qc_match_delay_ms=100)
        d = wait_status(c, bidB, timeout=120)
        check("B 质控途中重启后批次定稿", d["batch"]["status"] in ("done", "failed"),
              d["batch"]["status"])
        check("B 全部样品入仓且报完成", d["batch"]["status"] == "done"
              and len(d["warehouse"]) == len(idsB), f"{len(d['warehouse'])}/{len(idsB)}")
        check("B 入仓每样品唯一", len({w["sample_id"] for w in d["warehouse"]}) == len(idsB))
        recs = d.get("recoveries") or []
        check("B 恢复记录里质控沿用/补做均有留痕",
              any(x["action"] in ("reused_qc", "resumed_qc") for r in recs
                  for x in r.get("decisions") or []))
        # 被中断的质控调用如实留痕
        qc_int = [s for s in d["spans"] if s["stage"].startswith("qc_")
                  and s["status"] == "interrupted"]
        check("B 质检中断调用如实记录", len(qc_int) >= 1, f"{len(qc_int)} 条")

        # ============ 场景 C：已明确质控不通过的样品保持原结论，不因恢复翻案 ============
        set_config(c, source_delay_ms=100, qc_nonneg_delay_ms=100, qc_match_delay_ms=100)
        idsC = ["C-NEG-1", "C-OK-1"]  # NEG 必然质控①不通过
        bidC = submit(c, idsC)
        d = wait_status(c, bidC, timeout=60)
        check("C 含质控不通过样品的批次失败", d["batch"]["status"] == "failed")
        check("C 仅正常样品入仓", [w["sample_id"] for w in d["warehouse"]] == ["C-OK-1"],
              str([w["sample_id"] for w in d["warehouse"]]))
        # 手动触发一次恢复：已入仓/已判不通过的样品都保持原结论
        c.post(f"{ORCH}/api/recover", timeout=10)
        time.sleep(1.0)
        d2 = detail(c, bidC)
        check("C 恢复后不重复入仓", len(d2["warehouse"]) == 1, str(len(d2["warehouse"])))
        check("C 恢复后仍为失败(不翻案)", d2["batch"]["status"] == "failed",
              d2["batch"]["status"])
        check("C 阻断仍在质控①",
              any(b["stage"] == "qc_nonneg" for b in d2["conclusion"]["blocking"] or []))

        # ============ 场景 D：再次重启仍可接续(连续两次中断) ============
        set_config(c, source_delay_ms=1800, source_slots=8,
                   qc_nonneg_delay_ms=100, qc_match_delay_ms=100,
                   client_timeout_ms=700, max_attempts=2)
        idsD = [f"D-{i}" for i in range(2)]
        bidD = submit(c, idsD)
        wait_until(lambda: any(s["stage"] == "fetch" and s["status"] == "running"
                               for s in detail(c, bidD)["spans"]), timeout=10)
        svc.kill_orch()
        time.sleep(0.2)
        svc.start_orch()
        # 恢复又处在慢取数中，再次杀掉
        wait_until(lambda: any(s.get("recovery_id") and s["stage"] == "fetch"
                               and s["status"] == "running"
                               for s in detail(c, bidD)["spans"]), timeout=15)
        svc.kill_orch()
        # 第二次中断发生在恢复过程中：库里的恢复记录应停在 running，等待下次启动标为 interrupted
        import sqlite3 as _sq
        _conn = _sq.connect(os.path.join(WORK, "orch.db"))
        d_count = _conn.execute("SELECT COUNT(*) FROM recoveries WHERE batch_id=? AND status='running'",
                                (bidD,)).fetchone()[0]
        _conn.close()
        check("D 第二次中断时恢复记录停在 running", d_count >= 1, f"{d_count} 条")
        svc.start_orch()
        set_config(c, source_delay_ms=150, client_timeout_ms=3000)
        d = wait_status(c, bidD, timeout=120)
        check("D 两次重启后仍能接续完成", d["batch"]["status"] == "done", d["batch"]["status"])
        check("D 最终全部入仓", len(d["warehouse"]) == len(idsD), f"{len(d['warehouse'])}/{len(idsD)}")
        recs = d.get("recoveries") or []
        check("D 至少两次恢复记录", len(recs) >= 2, f"{len(recs)} 次")

        # ============ 场景 E：恢复期间仍可提交并运行其他批次 ============
        set_config(c, source_delay_ms=1200, client_timeout_ms=5000, max_attempts=2)
        idsE = [f"E-{i}" for i in range(2)]
        bidE = submit(c, idsE)
        wait_until(lambda: any(s["stage"] == "fetch" and s["status"] == "running"
                               for s in detail(c, bidE)["spans"]), timeout=10)
        svc.kill_orch()
        svc.start_orch()  # 启动恢复会接续 E(慢取数 1.2s)
        # 立刻提交新批次 F，不应被恢复阻塞
        idsF = [f"F-{i}" for i in range(2)]
        bidF = submit(c, idsF)
        dF = wait_status(c, bidF, timeout=60)
        dE = wait_status(c, bidE, timeout=90)
        check("E 恢复中的批次最终完成", dE["batch"]["status"] == "done", dE["batch"]["status"])
        check("F 恢复期间提交的其他批次照常运行完成", dF["batch"]["status"] == "done",
              dF["batch"]["status"])
        check("F 两批记录不串", all(w["batch_id"] == bidF for w in dF["warehouse"])
              and all(w["batch_id"] == bidE for w in dE["warehouse"]))

        # ============ 场景 G：入仓已落库但调用记录未写完 ============
        # 通过直接构造数据库状态模拟：仓记录存在、入仓 client span 为 interrupted。
        # 用一个独立编排进程 + 同一 DB 更真实：提交后在此样品入仓瞬间难以精确杀。
        # 这里直接写库模拟重启后的现场，再启动服务触发恢复。
        import sqlite3
        dbp = os.path.join(WORK, "orch.db")
        svc.kill_orch()
        time.sleep(0.3)
        conn = sqlite3.connect(dbp)
        conn.row_factory = sqlite3.Row
        now = time.time()
        sid = "G-PARTIAL-1"
        cur = conn.execute("INSERT INTO batches(created_at,sample_ids,status) VALUES(?,?,?)",
                           (now, json.dumps([sid]), "running"))
        gid = cur.lastrowid
        req = "oldfetchreq0001"
        # 取数有效 + 两项质控通过(有调用记录)
        conn.execute("INSERT INTO spans(batch_id,sample_id,stage,side,service,attempt,request_id,"
                     "status,start_ts,end_ts,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                     (gid, sid, "fetch", "client", "orchestrator", 1, req, "effective",
                      now - 3, now - 2,
                      json.dumps({"response": {"sample_id": sid, "concentration": 3.14}})))
        for chk in ("qc_nonneg", "qc_match"):
            conn.execute("INSERT INTO spans(batch_id,sample_id,stage,side,service,attempt,"
                         "request_id,status,start_ts,end_ts,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                         (gid, sid, chk, "client", "orchestrator", 1, chk + "req", "ok",
                          now - 2, now - 1.5, json.dumps({"response": {"pass": True}})))
        # 入仓记录已落库
        conn.execute("INSERT INTO warehouse(batch_id,sample_id,concentration,stored_at,request_id)"
                     " VALUES(?,?,?,?,?)", (gid, sid, 3.14, now - 1, req))
        # 入仓调用记录未写完：仍是 interrupted(没有 end_ts)
        conn.execute("INSERT INTO spans(batch_id,sample_id,stage,side,service,attempt,request_id,"
                     "status,start_ts,end_ts,detail) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                     (gid, sid, "warehouse", "client", "orchestrator", 1, "whreq", "interrupted",
                      now - 1, None, None))
        conn.commit()
        conn.close()

        assert svc.start_orch()
        d = wait_status(c, gid, timeout=60)
        check("G 入仓已落库、调用记录未写完：批次可定稿", d["batch"]["status"] in ("done", "failed"),
              d["batch"]["status"])
        check("G 仓记录未被重复写入", len(d["warehouse"]) == 1, str(len(d["warehouse"])))
        check("G 如实列出仓记录与未确认环节",
              any(w["sample_id"] == sid for w in d["warehouse"])
              and any(u.get("stage") == "warehouse" for u in d["conclusion"].get("unconfirmed") or []),
              json.dumps(d["conclusion"].get("unconfirmed"), ensure_ascii=False)[:200])
        recs = d.get("recoveries") or []
        check("G 恢复记录标注入仓调用未完成",
              any(x["action"] == "warehouse_call_unfinished" for r in recs
                  for x in r.get("decisions") or []))
        # 再次重启：仍可接续，不因已有仓记录而重复
        svc.kill_orch()
        svc.start_orch()
        time.sleep(1.5)
        d2 = detail(c, gid)
        check("G 再次重启后仓记录仍唯一", len(d2["warehouse"]) == 1, str(len(d2["warehouse"])))
        check("G 再次重启不产生重复入仓声明",
              d2["conclusion"]["samples_stored"] == 1, str(d2["conclusion"]["samples_stored"]))

    finally:
        svc.stop_all()
        c.close()

    print()
    if FAILS:
        print(f"共 {len(FAILS)} 项失败: {FAILS}")
        sys.exit(1)
    print("恢复自测全部通过")


if __name__ == "__main__":
    main()
