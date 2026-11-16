"""样品证据核对：只依据持久化的 spans(真实调用) 与 warehouse(真实入仓记录)。

恢复计划与批次结论共用这里的判定，避免"能重跑"和"能报完成"两套口径不一致。
一个样品"证据完整"指：
  - 有一条已完成的取数调用(有效，或已完成但重启前未及标记为有效)；
  - 两项质控都有已完成的通过记录，且没有任何明确的质控失败记录；
  - 入仓表有该样品的记录，且该记录的来源取数请求能由取数调用或入仓调用确认。
调用记录被重启打断(status=interrupted)不等于判定失败；它只说明该次调用结果未确认。
"""
import json

QC_CHECKS = ("nonneg", "match")
QC_LABELS = {"nonneg": "质控①非负值", "match": "质控②样品号对应"}


def span_detail(s):
    d = s.get("detail")
    if not d:
        return {}
    try:
        j = json.loads(d) if isinstance(d, str) else d
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}


def response_of(s):
    r = span_detail(s).get("response")
    return r if isinstance(r, dict) else None


def is_done(s):
    return s.get("end_ts") is not None


def _usable_fetch(fetches):
    """已完成的取数结果：优先 effective，其次重启前已完成但未及标记的 ok。"""
    for status in ("effective", "ok"):
        cand = [s for s in fetches if s["status"] == status and is_done(s) and response_of(s)]
        if cand:
            return min(cand, key=lambda s: (s["start_ts"], s["id"]))
    return None


def sample_state(spans, sample_id, wh_row):
    sps = [s for s in spans if s["side"] == "client" and s["sample_id"] == sample_id]
    fetches = sorted([s for s in sps if s["stage"] == "fetch"],
                     key=lambda s: (s["start_ts"], s["id"]))
    usable = _usable_fetch(fetches)
    effective = next((s for s in fetches if s["status"] == "effective" and is_done(s)), None)

    qc_ok, qc_failed, qc_interrupted, qc_attempts = {}, {}, {}, {}
    for c in QC_CHECKS:
        arr = [s for s in sps if s["stage"] == f"qc_{c}"]
        qc_attempts[c] = max([s["attempt"] for s in arr], default=0)
        ok = [s for s in arr if s["status"] == "ok" and is_done(s)]
        failed = [s for s in arr if s["status"] == "failed" and is_done(s)]
        interrupted = [s for s in arr if s["status"] == "interrupted"]
        if ok:
            qc_ok[c] = min(ok, key=lambda s: (s["start_ts"], s["id"]))
        if failed:
            qc_failed[c] = min(failed, key=lambda s: (s["start_ts"], s["id"]))
        if interrupted:
            qc_interrupted[c] = interrupted[0]

    wh_spans = [s for s in sps if s["stage"] == "warehouse"]
    wh_ok_span = next((s for s in wh_spans if s["status"] == "ok" and is_done(s)), None)
    wh_bad_span = next((s for s in wh_spans if s["status"] == "failed" and is_done(s)), None)
    wh_interrupted = [s for s in wh_spans if s["status"] == "interrupted"]
    max_fetch_attempt = max([s["attempt"] for s in fetches], default=0)

    # 入仓记录来源确认：仓记录的 request_id 能对上一条已完成的取数调用，
    # 或已完成的入仓调用记录里写明了同一来源请求。
    row_req_ok = False
    wh_span_confirms = False
    if wh_row:
        rid = wh_row.get("request_id")
        row_req_ok = any(
            s["request_id"] == rid and s["status"] in ("effective", "ok")
            and is_done(s) and response_of(s)
            for s in fetches)
        if wh_ok_span:
            d = span_detail(wh_ok_span)
            wh_span_confirms = (d.get("source_request_id") == rid) or bool(d.get("inserted"))
    warehouse_confirmed = bool(wh_row) and (row_req_ok or wh_span_confirms)

    fetch_ok = usable is not None
    qc_ok_both = all(c in qc_ok for c in QC_CHECKS)
    has_qc_failure = bool(qc_failed)
    evidence_complete = bool(wh_row and fetch_ok and qc_ok_both
                             and warehouse_confirmed and not has_qc_failure)

    unconfirmed = []
    for s in sps:
        if s["status"] == "interrupted":
            unconfirmed.append({
                "sample_id": sample_id, "stage": s["stage"], "span_id": s["id"],
                "detail": "编排端重启时该调用仍未完成，实际结果未记录；未作为成功依据。"})
    if wh_row and not wh_ok_span:
        unconfirmed.append({
            "sample_id": sample_id, "stage": "warehouse", "warehouse_id": wh_row["id"],
            "span_id": (wh_interrupted[0]["id"] if wh_interrupted else None),
            "detail": ("入仓记录已落库，但入仓调用记录被重启打断未写完；入仓结果以仓记录为准。"
                       if wh_interrupted else
                       "入仓记录已落库，但没有对应的入仓调用记录；入仓结果以仓记录为准。")})
    if wh_row and not warehouse_confirmed:
        unconfirmed.append({
            "sample_id": sample_id, "stage": "warehouse", "warehouse_id": wh_row["id"],
            "detail": "入仓记录的来源取数请求无法确认，入仓证据不完整。"})
    if wh_row and not fetch_ok:
        unconfirmed.append({
            "sample_id": sample_id, "stage": "fetch",
            "detail": "已入仓但缺少可确认的取数调用记录。"})
    if wh_row and not qc_ok_both:
        missing = [QC_LABELS[c] for c in QC_CHECKS if c not in qc_ok]
        unconfirmed.append({
            "sample_id": sample_id, "stage": "qc",
            "detail": "已入仓但质控证据不完整(缺少:" + "、".join(missing) + ")。"})
    if wh_row and has_qc_failure:
        c = next(iter(qc_failed))
        unconfirmed.append({
            "sample_id": sample_id, "stage": f"qc_{c}",
            "detail": "入仓记录存在，但质控有明确失败记录，结论冲突。"})

    return {
        "sample_id": sample_id,
        "spans": sps,
        "fetches": fetches,
        "fetch_usable": usable,
        "fetch_effective": effective,
        "fetch_ok": fetch_ok,
        "max_fetch_attempt": max_fetch_attempt,
        "qc_ok": qc_ok,
        "qc_failed": qc_failed,
        "qc_interrupted": qc_interrupted,
        "qc_attempts": qc_attempts,
        "qc_ok_both": qc_ok_both,
        "has_qc_failure": has_qc_failure,
        "wh_row": wh_row,
        "wh_ok_span": wh_ok_span,
        "wh_bad_span": wh_bad_span,
        "wh_interrupted": wh_interrupted,
        "row_req_ok": row_req_ok,
        "wh_span_confirms": wh_span_confirms,
        "warehouse_confirmed": warehouse_confirmed,
        "evidence_complete": evidence_complete,
        "decided_stored": wh_row is not None,
        "decided_qc_failure": has_qc_failure,
        "unconfirmed": unconfirmed,
    }


def first_missing_stage(st):
    """返回证据不完整的第一个环节及说明(用于失败批次的阻断定位)。"""
    if st["has_qc_failure"] and st["wh_row"]:
        c = next(iter(st["qc_failed"]))
        return f"qc_{c}", "入仓记录存在，但该环节有明确失败记录，结论冲突"
    if not st["fetch_ok"]:
        return "fetch", "缺少可确认的取数调用记录"
    for c in QC_CHECKS:
        if c not in st["qc_ok"]:
            failed = st["qc_failed"].get(c)
            if failed:
                return f"qc_{c}", "质控调用失败"
            if c in st["qc_interrupted"]:
                return f"qc_{c}", "质控调用被重启打断，结果未确认"
            return f"qc_{c}", "缺少已完成的质控记录"
    if not st["wh_row"]:
        return "warehouse", "质控已通过，但入仓记录缺失"
    if not st["warehouse_confirmed"]:
        return "warehouse", "入仓记录的来源取数请求未确认"
    return "warehouse", "入仓证据不完整"
