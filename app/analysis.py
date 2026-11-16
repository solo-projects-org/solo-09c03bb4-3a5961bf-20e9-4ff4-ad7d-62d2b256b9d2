"""批次结论分析：关键链(决定完成时间的等待与执行)、瓶颈判定、失败阻断定位。
所有结论只依据 spans(真实调用) 与 warehouse(真实入仓记录)。
"完成"要求每个样品都有完整证据：可确认的取数、两项均通过的质控、可追溯来源的入仓记录；
仅有入仓行而缺少入仓调用记录时，如实列出仓记录与未确认环节，不当作完整完成。"""
import json

import db
import evidence
from evidence import QC_LABELS

STAGE_LABELS = {
    "fetch": "取数",
    "qc_nonneg": "质控①非负值",
    "qc_match": "质控②样品号对应",
    "warehouse": "入仓",
}


def _f(x):
    return round(float(x), 3)


def _err_of(span):
    d = span.get("detail")
    if d:
        try:
            j = json.loads(d)
            if isinstance(j, dict):
                if j.get("error"):
                    return str(j["error"])
                if j.get("status_code"):
                    return f"HTTP {j['status_code']}"
                if j.get("response") is not None:
                    return json.dumps(j["response"], ensure_ascii=False)
        except Exception:
            pass
    return span["status"]


def analyze(batch_id):
    batch = db.query_one("SELECT * FROM batches WHERE id=?", (batch_id,))
    if not batch:
        return {"status": "failed", "text": "批次不存在"}
    sample_ids = list(dict.fromkeys(json.loads(batch["sample_ids"])))
    spans = db.query("SELECT * FROM spans WHERE batch_id=? ORDER BY start_ts,id", (batch_id,))
    wh = db.query("SELECT * FROM warehouse WHERE batch_id=? ORDER BY stored_at", (batch_id,))
    client = [s for s in spans if s["side"] == "client"]
    server_by_req = {}
    for s in spans:
        if s["side"] == "server":
            server_by_req.setdefault(s["request_id"], s)

    t0 = min([s["start_ts"] for s in spans], default=batch["created_at"])
    ends = [s["end_ts"] for s in spans if s["end_ts"]] + [w["stored_at"] for w in wh]
    t1 = max(ends) if ends else t0
    wh_by_sample = {w["sample_id"]: w for w in wh}

    # 逐样品核对证据：完成结论与恢复计划共用同一判定
    states = {sid: evidence.sample_state(client, sid, wh_by_sample.get(sid))
              for sid in sample_ids}
    done = bool(sample_ids) and all(states[s]["evidence_complete"] for s in sample_ids)
    stored_count = sum(1 for s in sample_ids if states[s]["wh_row"])
    unconfirmed = [u for s in sample_ids for u in states[s]["unconfirmed"]]

    # 各阶段统计：sum(各行耗时求和) 与 wall(墙钟跨度) 分开，并行/重叠不相加
    per_stage = {}
    for st in ("fetch", "qc_nonneg", "qc_match", "warehouse"):
        ss = [s for s in client if s["stage"] == st and s["end_ts"] is not None]
        if not ss:
            continue
        per_stage[st] = {
            "label": STAGE_LABELS[st],
            "attempts": len(ss),
            "sum_s": _f(sum(s["end_ts"] - s["start_ts"] for s in ss)),
            "wall_s": _f(max(s["end_ts"] for s in ss) - min(s["start_ts"] for s in ss)),
            "effective": sum(1 for s in ss if s["status"] == "effective"),
            "ok": sum(1 for s in ss if s["status"] == "ok"),
            "failed": sum(1 for s in ss if s["status"] == "failed"),
            "timeout": sum(1 for s in ss if s["status"] == "timeout"),
            "late": sum(1 for s in ss if s["status"] == "late"),
            "interrupted": sum(1 for s in client
                               if s["stage"] == st and s["status"] == "interrupted"),
            # 超时放弃是经历而非最终状态(之后会转为有效/迟到/失败)，从备注中统计
            "gave_up": sum(1 for s in ss if "超时" in (s["note"] or "")),
        }

    # 工作量归因：等名额(源站服务端上报的槽位等待) / 源站执行 / 两项质控 / 入仓
    eff_fetch = [s for s in client if s["stage"] == "fetch" and s["status"] == "effective"]
    slot_wait = sum((server_by_req.get(s["request_id"]) or {}).get("queue_ms") or 0
                    for s in eff_fetch) / 1000.0
    fetch_sum = sum(s["end_ts"] - s["start_ts"] for s in eff_fetch)
    parts = {
        "等名额(源站槽位)": slot_wait,
        "源站取数执行": max(fetch_sum - slot_wait, 0.0),
        "质控①非负值": per_stage.get("qc_nonneg", {}).get("sum_s", 0.0),
        "质控②样品号对应": per_stage.get("qc_match", {}).get("sum_s", 0.0),
        "入仓": per_stage.get("warehouse", {}).get("sum_s", 0.0),
    }
    bottleneck = max(parts, key=lambda k: parts[k]) if parts else None

    result = {
        "status": "done" if done else "failed",
        "t0": t0, "t1": t1, "wall_s": _f(t1 - t0),
        "samples_total": len(sample_ids),
        "samples_stored": stored_count,
        "warehouse_rows": len(wh),
        "client_calls": len(client),
        "per_stage": per_stage,
        "work_parts_s": {k: _f(v) for k, v in parts.items()},
        "bottleneck": bottleneck,
        "chain": None,
        "blocking": None,
        "unconfirmed": unconfirmed,
    }

    if done:
        # 关键链：最后完成入仓的样品决定了批次完成时间
        last_w = max(wh, key=lambda w: w["stored_at"])
        sid = last_w["sample_id"]
        sps = [s for s in client if s["sample_id"] == sid]
        fe = next((s for s in sps if s["stage"] == "fetch"
                   and s["status"] in ("effective", "ok") and s["end_ts"]), None)
        wh_span = next((s for s in sps if s["stage"] == "warehouse"
                        and s["end_ts"] and s["status"] == "ok"), None)
        wh_broken = [s for s in sps if s["stage"] == "warehouse"
                     and s["status"] == "interrupted"]
        qcs = [s for s in sps if s["stage"] in ("qc_nonneg", "qc_match") and s["end_ts"]
               and s["status"] == "ok"]
        segs = []
        # 入仓结束时间：有完成的入仓调用用其结束时间；调用记录被重启打断时，
        # 如实采用仓记录的真实写入时间(stored_at)，并注明调用记录未写完。
        wh_start = wh_end = None
        wh_note = f"入仓记录#{last_w['id']}"
        wh_ids = []
        if wh_span:
            wh_start, wh_end = wh_span["start_ts"], wh_span["end_ts"]
            wh_ids = [wh_span["id"]]
        elif wh_broken:
            wh_start = wh_broken[0]["start_ts"]
            wh_end = last_w["stored_at"]
            wh_note += "；入仓调用记录未写完，结束时间取仓记录写入时间"
            wh_ids = [wh_broken[0]["id"]]
        else:
            wh_end = last_w["stored_at"]
            wh_note += "；无入仓调用记录，以仓记录写入时间为据"
        if fe and wh_end is not None:
            srv = server_by_req.get(fe["request_id"])
            note = ""
            if srv and srv.get("queue_ms"):
                note = f"含等名额 {srv['queue_ms']:.0f}ms(源站服务端记录)"
            segs.append({"kind": "exec", "label": f"取数·尝试#{fe['attempt']}(有效)",
                         "start": fe["start_ts"], "end": fe["end_ts"],
                         "span_ids": [fe["id"]], "note": note})
            prev = fe["end_ts"]
            if qcs:
                qs, qe = min(s["start_ts"] for s in qcs), max(s["end_ts"] for s in qcs)
                if qs - prev > 0.0005:
                    segs.append({"kind": "wait", "label": "调度间隙",
                                 "start": prev, "end": qs, "span_ids": []})
                slower = max(qcs, key=lambda s: s["end_ts"] - s["start_ts"])
                segs.append({"kind": "exec",
                             "label": f"并行质控×{len(qcs)}(较慢:{STAGE_LABELS[slower['stage']]})",
                             "start": qs, "end": qe,
                             "span_ids": [s["id"] for s in qcs],
                             "note": "两项并行，耗时取较慢者，不相加"})
                prev = qe
            if wh_start is not None and wh_start - prev > 0.0005:
                segs.append({"kind": "wait", "label": "调度间隙",
                             "start": prev, "end": wh_start, "span_ids": []})
            wh_from = wh_start if wh_start is not None else prev
            segs.append({"kind": "exec", "label": "入仓",
                         "start": wh_from, "end": wh_end,
                         "span_ids": wh_ids, "note": wh_note})
            result["chain"] = {"sample_id": sid, "segments": segs,
                               "total_s": _f(wh_end - fe["start_ts"])}
        bits = []
        for seg in segs:
            d = _f(seg["end"] - seg["start"])
            bits.append(f"{seg['label']} {d:.3f}s" + (f"({seg['note']})" if seg.get("note") else ""))
        fr = per_stage.get("fetch", {})
        result["text"] = (
            f"批次#{batch_id} 已完成:{stored_count}/{len(sample_ids)} 样品入仓"
            f"(入仓记录 {len(wh)} 条、跨服务调用 {len(client)} 次,结论以实际记录为据)。"
            f"墙钟总耗时 {_f(t1 - t0)}s。"
            f"决定完成时间的关键链(样品 {sid}): " + " → ".join(bits) + "。"
            f"瓶颈判定: {bottleneck}(各环节工作量秒: "
            + "、".join(f"{k}={v}" for k, v in result["work_parts_s"].items())
            + ";并行质控与重叠重试按墙钟计,不直接相加)。"
            f"取数尝试 {fr.get('attempts', 0)} 次:有效 {fr.get('effective', 0)}、"
            f"失败 {fr.get('failed', 0)}、迟到 {fr.get('late', 0)}"
            f"(其中曾超时放弃 {fr.get('gave_up', 0)} 次)。")
    else:
        blocking = []
        for sid in sample_ids:
            st = states[sid]
            if st["evidence_complete"]:
                continue
            stage, why = evidence.first_missing_stage(st)
            if stage == "fetch":
                fetches = st["fetches"]
                if fetches and not st["fetch_ok"]:
                    last = max(fetches, key=lambda s: s["start_ts"])
                    tries = "、".join(f"#{s['attempt']}:{s['status']}"
                                     for s in sorted(fetches, key=lambda x: (x["attempt"], x["id"])))
                    detail = (f"取数 {len(fetches)} 次尝试均未成功({tries});"
                              f"最后错误:{_err_of(last)}")
                else:
                    detail = why
            elif stage.startswith("qc_"):
                c = stage[3:]
                failed = st["qc_failed"].get(c)
                if failed:
                    detail = f"{QC_LABELS[c]}未通过/失败:{_err_of(failed)}"
                elif c in st["qc_interrupted"]:
                    detail = f"{QC_LABELS[c]}调用被重启打断，结果未确认；批次未定稿前会接续"
                else:
                    detail = f"{QC_LABELS[c]}尚未完成"
            else:
                detail = why
                if st["wh_row"] and not st["wh_ok_span"]:
                    detail += f"(仓记录#{st['wh_row']['id']}已落库，入仓调用记录未写完)"
            blocking.append({"sample_id": sid, "stage": stage, "detail": detail})
        result["blocking"] = blocking
        bl = "；".join(f"样品 {b['sample_id']} 阻断于{STAGE_LABELS.get(b['stage'], b['stage'])}"
                       f"({b['detail']})" for b in blocking) or "无阻断明细"
        unconf_txt = ""
        if unconfirmed:
            unconf_txt = (" 未确认环节: " + "；".join(
                f"{u['sample_id']} {STAGE_LABELS.get(u['stage'], u['stage'])}:{u['detail'].rstrip('。')}"
                for u in unconfirmed) + "。")
        result["text"] = (
            f"批次#{batch_id} 未完成:{stored_count}/{len(sample_ids)} 样品入仓"
            f"(入仓记录 {len(wh)} 条)。阻断环节: {bl}。{unconf_txt}失败批次不生成成功链。")
    return result
