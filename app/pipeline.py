"""批次流水线：取数(超时重试/迟到归类) -> 并行两项质控 -> 幂等入仓。
全程真实计时，每次调用一个 span，request_id 跨服务关联。
编排端重启后由 recover_pending() 接续未定稿批次：已入仓/已明确质控不通过的样品保持原结论，
其余样品从未完成的环节继续；旧源站请求可能继续执行并迟到上报，恢复尝试不因此重复入仓。"""
import asyncio
import json
import os
import time
import uuid

import httpx

import analysis
import db
import evidence

SOURCE_URL = os.environ.get("SOURCE_URL", "http://localhost:8001")
QC_URL = os.environ.get("QC_URL", "http://localhost:8002")

# httpx 硬超时只是兜底；逻辑超时由 asyncio.wait 实现，超时后旧请求不被取消、继续运行
HARD_TIMEOUT_S = 30.0

_client = None

# batch_id -> 仍在收尾的迟到尝试任务；批次定稿前要等它们结束，结论才完整
_finalizers = {}

# 本进程正在跑的批次/恢复任务，避免同一批次被重复接续
_running_batches = set()
# 后台恢复任务引用保持：避免 asyncio.create_task 的 task 被 GC 提前回收
_background_tasks = set()


def _spawn(coro):
    t = asyncio.create_task(coro)
    _background_tasks.add(t)
    t.add_done_callback(_background_done)
    return t


def _background_done(t):
    _background_tasks.discard(t)
    if t.cancelled():
        return
    try:
        exc = t.exception()
    except Exception:
        exc = None
    if exc is not None:
        print(f"[background] 任务异常: {type(exc).__name__}: {exc}", flush=True)


def start_batch(batch_id, sample_ids):
    """提交批次：登记为运行中并后台执行(供 API 调用，避免任务被 GC)。"""
    _running_batches.add(batch_id)  # 同步登记，避免恢复误伤刚提交的批次
    return _spawn(_run_tracked(batch_id, sample_ids))


async def _run_tracked(batch_id, sample_ids):
    try:
        await run_batch(batch_id, sample_ids)
    except Exception as e:
        print(f"[batch {batch_id}] 运行出错: {type(e).__name__}: {e}", flush=True)
        try:
            await _finish_batch(batch_id)
        except Exception:
            pass
    finally:
        _running_batches.discard(batch_id)


def _track_finalizer(batch_id, task):
    _finalizers.setdefault(batch_id, set()).add(task)
    task.add_done_callback(lambda t: _finalizers.get(batch_id, set()).discard(t))


def http():
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=HARD_TIMEOUT_S)
    return _client


def _rid():
    return uuid.uuid4().hex[:16]


# ---------------- 取数 ----------------

async def call_source(batch_id, sample_id, attempt, req_id, span_id):
    try:
        r = await http().get(f"{SOURCE_URL}/fetch", params={
            "sample_id": sample_id, "request_id": req_id,
            "batch_id": batch_id, "attempt": attempt})
        end = time.time()
        if r.status_code == 200:
            data = r.json()
            db.finish_span(span_id, end, "ok", {"response": data})
            return ("ok", data)
        db.finish_span(span_id, end, "failed",
                       {"status_code": r.status_code, "body": r.text[:200]})
        return ("failed", None)
    except Exception as e:
        db.finish_span(span_id, time.time(), "failed",
                       {"error": f"{type(e).__name__}: {e}"})
        return ("failed", None)


async def fetch_with_retry(batch_id, sample_id, cfg, recovery_id=None, start_attempt=1):
    """首个有效结果胜出。超时只放弃等待、不取消旧请求：
    尝试次数用完后仍在途的旧请求继续等待收尾(可能反超成为有效尝试)，
    其余迟到完成的记为 late，结果绝不参与入仓。
    恢复时(start_attempt>1)新尝试使用全新 request_id 并标记 recovery_id；
    重启前旧请求只会以服务端跨度迟到上报，不能成为新尝试的有效依据。"""
    timeout_s = cfg["client_timeout_ms"] / 1000.0
    max_attempts = int(cfg["max_attempts"])
    pending = {}   # task -> {attempt, req_id, span_id}
    state = {"winner": None, "winner_req": None}

    def collect(done):
        for t in done:
            meta = pending.pop(t)
            status, data = t.result()
            if status == "ok" and state["winner"] is None:
                state["winner"], state["winner_req"] = data, meta["req_id"]
                db.mark_status(meta["span_id"], "effective", note=" 该尝试结果被采用。")
            elif status == "ok":
                db.mark_status(meta["span_id"], "late", note=" 完成时已有有效结果，迟到未采用。")

    attempt = start_attempt - 1
    # 发起阶段：逻辑超时即追加新尝试，旧尝试继续运行
    while state["winner"] is None and attempt < start_attempt - 1 + max_attempts:
        attempt += 1
        req_id = _rid()
        span_id = db.insert_span(batch_id, sample_id, "fetch", "client", "orchestrator",
                                 attempt, req_id, "running", time.time(),
                                 recovery_id=recovery_id)
        task = asyncio.create_task(call_source(batch_id, sample_id, attempt, req_id, span_id))
        pending[task] = {"attempt": attempt, "req_id": req_id, "span_id": span_id}
        done, _ = await asyncio.wait(set(pending), timeout=timeout_s,
                                     return_when=asyncio.FIRST_COMPLETED)
        collect(done)
        if state["winner"] is None and not done:
            # 逻辑超时：标记客户端放弃，但任务继续运行(源站可能仍在执行)
            for t, meta in pending.items():
                db.mark_status(meta["span_id"], "timeout",
                               note=f" 客户端超时(等待超 {cfg['client_timeout_ms']}ms)放弃并发起新尝试；"
                                    f"旧请求可能仍在源站运行。",
                               only_if="running")
    # 收尾阶段：不再发新请求，已在途的尝试仍可能给出首个有效结果
    while state["winner"] is None and pending:
        done, _ = await asyncio.wait(set(pending), timeout=HARD_TIMEOUT_S,
                                     return_when=asyncio.FIRST_COMPLETED)
        if not done:
            break
        collect(done)
    if pending:
        _track_finalizer(batch_id, asyncio.create_task(_finalize_late(dict(pending))))
    if state["winner"] is None:
        return None
    state["winner"]["_req_id"] = state["winner_req"]
    return state["winner"]


async def _finalize_late(pending):
    """胜出/放弃后仍在跑的旧尝试：完成时如实记录为 late，结果不入仓。"""
    for t, meta in pending.items():
        try:
            status, _ = await t
            if status == "ok":
                db.mark_status(meta["span_id"], "late",
                               note=" 迟到完成，结果未采用，不参与入仓。")
        except Exception:
            pass


# ---------------- 质控(两项并行) ----------------

async def qc_call(batch_id, sample_id, check, payload, attempt=1, recovery_id=None):
    stage = f"qc_{check}"
    req_id = _rid()
    span_id = db.insert_span(batch_id, sample_id, stage, "client", "orchestrator",
                             attempt, req_id, "running", time.time(),
                             recovery_id=recovery_id)
    try:
        r = await http().post(f"{QC_URL}/check/{check}",
                              json={**payload, "request_id": req_id, "batch_id": batch_id})
        end = time.time()
        if r.status_code == 200:
            res = r.json()
            ok = bool(res.get("pass"))
            db.finish_span(span_id, end, "ok" if ok else "failed", {"response": res},
                           note=None if ok else " 质控判定未通过。")
            return ok
        db.finish_span(span_id, end, "failed",
                       {"status_code": r.status_code, "body": r.text[:200]})
        return False
    except Exception as e:
        db.finish_span(span_id, time.time(), "failed",
                       {"error": f"{type(e).__name__}: {e}"})
        return False


async def qc_pair(batch_id, sample_id, payload, checks=("nonneg", "match"),
                  attempts=None, recovery_id=None):
    """并行执行指定的质控项(恢复时只补做缺失项，已通过项保持原结论)。"""
    attempts = attempts or {}
    result = await asyncio.gather(*(
        qc_call(batch_id, sample_id, c, payload,
                attempt=attempts.get(c, 1), recovery_id=recovery_id) for c in checks))
    return dict(zip(checks, result))


async def warehouse_step(batch_id, sample_id, concentration, source_req_id,
                         recovery_id=None):
    """幂等入仓：UNIQUE(batch_id,sample_id) 保证迟到/恢复重放不重复写入。"""
    span_id = db.insert_span(batch_id, sample_id, "warehouse", "client", "orchestrator",
                             1, _rid(), "running", time.time(), recovery_id=recovery_id)
    inserted = db.warehouse_insert(batch_id, sample_id, concentration, source_req_id)
    db.finish_span(span_id, time.time(), "ok",
                   {"inserted": inserted, "concentration": concentration,
                    "source_request_id": source_req_id},
                   note=None if inserted else " 唯一约束去重：该样品已有入仓记录，未重复写入。")
    return inserted


# ---------------- 单样品流程 ----------------

async def process_sample(batch_id, sample_id, cfg, recovery_id=None, start_attempt=1):
    data = await fetch_with_retry(batch_id, sample_id, cfg, recovery_id=recovery_id,
                                  start_attempt=start_attempt)
    if data is None:
        return False
    req_id_used = data.pop("_req_id")
    returned_id = str(data.get("sample_id"))
    payload = {"sample_id": returned_id, "expected_sample_id": sample_id,
               "concentration": data.get("concentration")}
    # 两项质控并行：墙钟取较慢者，不相加
    ok = await qc_pair(batch_id, sample_id, payload, recovery_id=recovery_id)
    if not all(ok.values()):
        return False
    await warehouse_step(batch_id, sample_id, data.get("concentration"), req_id_used,
                         recovery_id=recovery_id)
    return True


async def _finish_batch(batch_id):
    """等在途迟到尝试收尾后定稿：只有全部样品证据完整才报完成。"""
    pending = list(_finalizers.get(batch_id, ()))
    if pending:
        await asyncio.wait(pending)
    _finalizers.pop(batch_id, None)
    concl = analysis.analyze(batch_id)
    status = "done" if concl.get("status") == "done" else "failed"
    db.execute("UPDATE batches SET status=?, finished_at=?, conclusion=? WHERE id=?",
               (status, time.time(), json.dumps(concl, ensure_ascii=False), batch_id))
    return status, concl


async def run_batch(batch_id, sample_ids):
    cfg = db.get_config()  # 批次开始时快照配置，批内一致
    try:
        await asyncio.gather(*(process_sample(batch_id, s, cfg) for s in sample_ids),
                             return_exceptions=True)
    except Exception:
        pass
    await _finish_batch(batch_id)


# ---------------- 启动恢复 ----------------

INTERRUPT_NOTE = " 编排端在此调用完成前重启：结果未记录，未作为完成依据。"


def _decision(rec_id, sample_id, action, stage, detail):
    db.recovery_append(rec_id, "decisions", {
        "at": time.time(), "sample_id": sample_id, "action": action,
        "stage": stage, "detail": detail})


async def _resume_sample(batch_id, sample_id, cfg, rec_id, st):
    """接续一个尚未判定的样品：从证据缺失的环节继续，已完成/已判定的环节不重跑。"""
    # 已明确质控不通过：保持原结论，不重跑、不掩盖
    if st["has_qc_failure"]:
        _decision(rec_id, sample_id, "preserved_qc_failed", "qc",
                  "重启前质控已有明确不通过记录，保持原结论，不重新判定。")
        return False
    # 已入仓：保持原结论(只补记未确认环节，不重复入仓)
    if st["wh_row"]:
        _decision(rec_id, sample_id, "preserved_stored", "warehouse",
                  f"重启前已入仓(仓记录#{st['wh_row']['id']})，保持原结论，不重复入仓。")
        return True

    # 1) 取数：复用重启前已完成的取数结果；否则以全新 request_id 重新尝试
    if st["fetch_usable"]:
        fs = st["fetch_usable"]
        resp = dict(evidence.response_of(fs) or {})
        if not resp:
            resp = {}
        req_id_used = fs["request_id"]
        returned_id = str(resp.get("sample_id", sample_id))
        payload = {"sample_id": returned_id, "expected_sample_id": sample_id,
                   "concentration": resp.get("concentration")}
        _decision(rec_id, sample_id, "reused_fetch", "fetch",
                  f"复用重启前已完成的取数结果(请求 {req_id_used[:8]}、尝试#{fs['attempt']})，"
                  f"未重新发起取数。")
    else:
        _decision(rec_id, sample_id, "resumed_fetch", "fetch",
                  f"取数尚无可用结果，从尝试#{st['max_fetch_attempt'] + 1}起用全新请求接续。")
        data = await fetch_with_retry(batch_id, sample_id, cfg, recovery_id=rec_id,
                                      start_attempt=st["max_fetch_attempt"] + 1)
        if data is None:
            _decision(rec_id, sample_id, "fetch_failed", "fetch",
                      "恢复期间的取数尝试均未成功，样品保持未判定。")
            return False
        req_id_used = data.pop("_req_id")
        returned_id = str(data.get("sample_id"))
        payload = {"sample_id": returned_id, "expected_sample_id": sample_id,
                   "concentration": data.get("concentration")}

    # 2) 质控：只补做尚无通过记录的项，已通过项直接沿用
    missing = [c for c in evidence.QC_CHECKS if c not in st["qc_ok"]]
    if missing:
        _decision(rec_id, sample_id, "resumed_qc", "qc",
                  "补做质控:" + "、".join(evidence.QC_LABELS[c] for c in missing) + "。")
        attempts = {c: st["qc_attempts"].get(c, 0) + 1 for c in missing}
        ok = await qc_pair(batch_id, sample_id, payload, checks=missing,
                           attempts=attempts, recovery_id=rec_id)
        if not all(ok.values()):
            bad = [c for c, v in ok.items() if not v]
            _decision(rec_id, sample_id, "qc_failed", "qc_" + bad[0],
                      "恢复期间质控未通过:" + "、".join(evidence.QC_LABELS[c] for c in bad) + "。")
            return False
    else:
        _decision(rec_id, sample_id, "reused_qc", "qc",
                  "重启前两项质控均已通过，沿用原记录，不重复质控。")

    # 3) 入仓：幂等写入；若仓记录在恢复期间出现(竞态)则如实记为去重
    inserted = await warehouse_step(batch_id, sample_id, payload.get("concentration"),
                                    req_id_used, recovery_id=rec_id)
    _decision(rec_id, sample_id, "warehouse_inserted" if inserted else "warehouse_deduped",
              "warehouse",
              "入仓记录已写入。" if inserted else "该样品已有入仓记录，幂等去重，未重复写入。")
    return True


async def recover_pending():
    """编排端启动后接续所有未定稿批次(可被再次重启重复调用，幂等)。

    - 重启时仍 running 的编排侧调用如实标为 interrupted(不伪造成功/结束时间)；
    - 已入仓、已明确质控不通过的样品保持原结论；
    - 其余样品从证据缺失处接续；每次恢复尝试使用全新 request_id，旧请求迟到上报
      只作为服务端跨度记录，绝不计为新尝试的成功依据。
    """
    note = INTERRUPT_NOTE
    interrupted = db.interrupt_running_client_spans(
        note, exclude_batches=tuple(_running_batches))
    by_batch = {}
    for span_id, bid in interrupted:
        by_batch.setdefault(bid, []).append(span_id)
    db.mark_recovery_interrupted(exclude_batches=tuple(_running_batches))
    rows = db.query("SELECT * FROM batches WHERE status='running' ORDER BY id")
    if not rows and not interrupted:
        return []
    ids = []
    for b in rows:
        if b["id"] in _running_batches:
            continue
        ids.append(b["id"])
        _spawn(_recover_batch(b["id"], json.loads(b["sample_ids"]),
                              interrupted_span_ids=by_batch.get(b["id"], [])))
    return ids


def _note_warehouse_gap(batch_id, rec_id, sample_ids):
    """重启打断了入仓调用记录，但仓记录已落库：据实记录仓记录与未确认环节。
    不把被打断的调用改写为成功——仓记录本身是入仓证据，调用记录未写完仍如实展示。"""
    wh = {w["sample_id"]: w for w in
          db.query("SELECT * FROM warehouse WHERE batch_id=?", (batch_id,))}
    spans = db.query("SELECT * FROM spans WHERE batch_id=? AND stage='warehouse' "
                     "AND side='client'", (batch_id,))
    for sid in sample_ids:
        row = wh.get(sid)
        if not row:
            continue
        mine = [s for s in spans if s["sample_id"] == sid]
        if any(s["status"] == "ok" and s["end_ts"] is not None for s in mine):
            continue
        _decision(rec_id, sid, "warehouse_call_unfinished", "warehouse",
                  f"仓记录#{row['id']}已落库(concentration={row['concentration']}、"
                  f"来源请求 {str(row['request_id'])[:8]}、stored_at={row['stored_at']:.3f})，"
                  f"但入仓调用记录未写完；以仓记录为入仓证据，未确认环节见表内明细，"
                  f"不重复入仓、不伪造调用完成。")


async def _recover_batch(batch_id, sample_ids, interrupted_span_ids=()):
    if batch_id in _running_batches:
        return
    _running_batches.add(batch_id)
    rec_id = None
    try:
        cfg = db.get_config()
        spans = db.query("SELECT * FROM spans WHERE batch_id=?", (batch_id,))
        wh = {w["sample_id"]: w for w in
              db.query("SELECT * FROM warehouse WHERE batch_id=?", (batch_id,))}
        client = [s for s in spans if s["side"] == "client"]
        states = {s: evidence.sample_state(client, s, wh.get(s)) for s in sample_ids}

        preserved, resumed = [], []
        for s in sample_ids:
            st = states[s]
            if st["wh_row"]:
                # 已入仓：原结论保持不变，不重复入仓(未确认环节稍后单独核对并展示)
                preserved.append({"sample_id": s, "reason": "已入仓，保持原结论"})
            elif st["has_qc_failure"]:
                # 已明确质控不通过：保持原结论，不重跑掩盖失败
                preserved.append({"sample_id": s, "reason": "质控已明确不通过，保持原结论"})
            else:
                resumed.append(s)
        rec_id = db.recovery_insert(
            batch_id, "startup", resumed_samples=resumed,
            preserved_samples=preserved,
            interrupted_spans=len(interrupted_span_ids),
            note=f"启动恢复：批次#{batch_id} 于重启时未定稿。")
        for p in preserved:
            _decision(rec_id, p["sample_id"], "preserved", "batch", p["reason"] + "，未重新执行。")
        _decision(rec_id, None, "resume_plan", "batch",
                  "接续样品:" + ("、".join(resumed) if resumed else "无")
                  + "；保持原结论:" + ("、".join(p["sample_id"] for p in preserved)
                                      if preserved else "无") + "。")

        # 先据实记录"仓记录已落库但调用记录未完成"的未确认环节，再规划接续
        _note_warehouse_gap(batch_id, rec_id, sample_ids)
        spans = db.query("SELECT * FROM spans WHERE batch_id=?", (batch_id,))
        client = [s for s in spans if s["side"] == "client"]
        states = {s: evidence.sample_state(client, s, wh.get(s)) for s in sample_ids}

        results = await asyncio.gather(*(
            _resume_sample(batch_id, s, cfg, rec_id, states[s]) for s in resumed),
            return_exceptions=True)
        for s, r in zip(resumed, results):
            if isinstance(r, Exception):
                _decision(rec_id, s, "error", "batch", f"恢复该样品时出错:{r}")

        # 恢复结束后重新核对：未确认环节如实写入恢复记录
        spans = db.query("SELECT * FROM spans WHERE batch_id=?", (batch_id,))
        client = [s for s in spans if s["side"] == "client"]
        wh = {w["sample_id"]: w for w in
              db.query("SELECT * FROM warehouse WHERE batch_id=?", (batch_id,))}
        seen = set()
        for s in sample_ids:
            st = evidence.sample_state(client, s, wh.get(s))
            for u in st["unconfirmed"]:
                key = (u["sample_id"], u["stage"], u.get("span_id"), u.get("warehouse_id"),
                       u["detail"])
                if key in seen:
                    continue
                seen.add(key)
                db.recovery_append(rec_id, "unconfirmed", u)

        status, _ = await _finish_batch(batch_id)
        if status == "done":
            db.recovery_finish(rec_id, "done",
                               note=" 恢复完成：全部样品均有完整质控与入仓证据。")
        else:
            db.recovery_finish(rec_id, "failed",
                               note=" 恢复结束但批次未完成：存在阻断环节(见批次结论)。")
    except Exception as e:
        if rec_id is not None:
            db.recovery_finish(rec_id, "failed", note=f" 恢复过程出错:{type(e).__name__}: {e}")
        raise
    finally:
        _running_batches.discard(batch_id)
