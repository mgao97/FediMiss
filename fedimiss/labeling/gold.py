"""金标准与盲审工作表：导出给人，收回后算一致性并生成 gold.jsonl。

三个子命令：

``export``  分层抽样 → 双人独立标注表（**不含**任何模型输出，避免锚定）
``audit``   从已标注结果里按标签分层抽样 → 盲审表（隐藏模型标签，用于估计错误率）
``iaa``     回收标注表 → 人工一致性 κ / 分歧清单 / gold.jsonl

示例::

    python3 -m fedimiss.labeling.gold export \
        --evidence data/labeling/evidence.jsonl \
        --out data/labeling/gold_sheet.csv --limit 400

    python3 -m fedimiss.labeling.gold iaa \
        --sheet data/labeling/gold_sheet.csv \
        --write-gold data/labeling/gold.jsonl

    python3 -m fedimiss.labeling.gold audit \
        --labels data/labeling/labels.jsonl \
        --evidence data/labeling/evidence.jsonl \
        --out data/labeling/audit_sheet.csv --per-label 50
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Sequence

from . import LABELS
from .agreement import cohen_kappa, read_jsonl

SHEET_COLUMNS = [
    "account_key", "instance", "home_instance_status", "mapping_confidence",
    "account_created_at", "followers_count", "following_count", "notes_collected",
    "span_days", "reply_rate", "distinct_reply_targets", "renote_rate",
    "gap_median_seconds", "gap_cv", "modal_interval_share",
    "primary_gap_cv", "primary_modal_interval_share",
    "duplicate_rate", "top_text_share", "hour_entropy", "media_rate",
    "is_locked", "interaction_observable", "visibility_top", "is_bot_declared",
    "description", "sample_texts", "sampler_note",
]


def _safe(value: Any) -> str:
    text = "" if value is None else str(value)
    # 防 CSV 公式注入（Excel 会把 = + - @ 开头当公式）
    if text[:1] in ("=", "+", "-", "@"):
        text = "'" + text
    return text.replace("\r", " ").replace("\n", " ")


def _row(pack: Dict[str, Any], note: str = "") -> Dict[str, str]:
    profile = pack.get("profile", {})
    collection = pack.get("collection", {})
    timing = pack.get("timing", {})
    content = pack.get("content", {})
    interaction = pack.get("interaction", {})
    samples = pack.get("samples", [])
    texts = " || ".join(
        "[%s] %s" % (item.get("created_at"), (item.get("text") or "").replace("\n", " "))
        for item in samples[:8]
    )
    primary = timing.get("primary_window") or {}
    visibilities = content.get("visibility") or {}
    visibility_top = ""
    if visibilities:
        top_vis, top_vis_n = max(visibilities.items(), key=lambda kv: kv[1])
        visibility_top = "%s=%s" % (top_vis, top_vis_n)
    return {
        "account_key": _safe(pack.get("account_key")),
        "instance": _safe(pack.get("instance_domain")),
        "home_instance_status": _safe(collection.get("home_instance_status")),
        "mapping_confidence": _safe(collection.get("mapping_confidence")),
        "account_created_at": _safe(profile.get("account_created_at")),
        "followers_count": _safe(profile.get("followers_count")),
        "following_count": _safe(profile.get("following_count")),
        "notes_collected": _safe(collection.get("notes_collected")),
        "span_days": _safe(collection.get("span_days")),
        "reply_rate": _safe(interaction.get("reply_rate")),
        "distinct_reply_targets": _safe(interaction.get("distinct_reply_targets")),
        "renote_rate": _safe(interaction.get("renote_rate")),
        "gap_median_seconds": _safe(timing.get("gap_median_seconds")),
        "gap_cv": _safe(timing.get("gap_cv")),
        "modal_interval_share": _safe(timing.get("modal_interval_share")),
        # 单窗口（主窗口）内的节奏统计，不受采集缺口影响，判节奏优先看这两列
        "primary_gap_cv": _safe(primary.get("gap_cv")),
        "primary_modal_interval_share": _safe(primary.get("modal_interval_share")),
        "duplicate_rate": _safe(content.get("duplicate_rate")),
        "top_text_share": _safe(content.get("top_text_share")),
        "hour_entropy": _safe(timing.get("hour_entropy")),
        "media_rate": _safe((pack.get("media") or {}).get("media_rate")),
        "is_locked": _safe(profile.get("is_locked")),
        "interaction_observable": _safe(interaction.get("interaction_observable")),
        "visibility_top": _safe(visibility_top),
        "is_bot_declared": _safe(profile.get("is_bot_declared")),
        "description": _safe((profile.get("description") or "")[:300]),
        "sample_texts": _safe(texts[:1500]),
        "sampler_note": _safe(note),
    }


def _write_sheet(path: str, rows: List[Dict[str, str]], extra_columns: Sequence[str]) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        # extrasaction="ignore"：拆分表只写自己的标注列，行里多余的键跳过
        writer = csv.DictWriter(fh, fieldnames=SHEET_COLUMNS + list(extra_columns),
                                extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            for column in extra_columns:
                row.setdefault(column, "")
            writer.writerow(row)


def export_sheet(evidence_path: str, out_path: str, limit: int, seed: int,
                 split: bool = False) -> Dict[str, Any]:
    packs = read_jsonl(evidence_path)
    if limit > 0 and len(packs) > limit:
        packs = random.Random(seed).sample(packs, limit)
        packs.sort(key=lambda pack: pack["account_key"])
    rows = [_row(pack) for pack in packs]
    columns = ["annotator_a", "annotator_b", "adjudicated", "notes"]
    _write_sheet(out_path, rows, columns)

    split_files: List[str] = []
    if split:
        # 按人拆成两份独立表：互不可见（B 表行序另用随机种子打乱），
        # 只保留各自一个标注列；合并与仲裁仍以基础表为准
        stem, ext = os.path.splitext(out_path)
        rnd = random.Random(seed + 1)
        shuffled = list(rows)
        rnd.shuffle(shuffled)
        path_a = "%s_annotatorA%s" % (stem, ext)
        path_b = "%s_annotatorB%s" % (stem, ext)
        _write_sheet(path_a, rows, ["annotator_a", "notes"])
        _write_sheet(path_b, shuffled, ["annotator_b", "notes"])
        split_files = [os.path.abspath(path_a), os.path.abspath(path_b)]

    return {"out": os.path.abspath(out_path), "accounts": len(rows),
            "columns": SHEET_COLUMNS + columns, "split_files": split_files}


def audit_sheet(labels_path: str, evidence_path: str, out_path: str,
                per_label: int, seed: int, run_id: Optional[str]) -> Dict[str, Any]:
    packs = {pack["account_key"]: pack for pack in read_jsonl(evidence_path)}
    latest: Dict[str, Dict[str, Any]] = {}
    for record in read_jsonl(labels_path):
        if run_id and record.get("run_id") != run_id:
            continue
        latest[record["account_key"]] = record

    by_label: Dict[str, List[str]] = defaultdict(list)
    for key, record in latest.items():
        if key in packs and record.get("label") in LABELS:
            by_label[record["label"]].append(key)

    rnd = random.Random(seed)
    rows: List[Dict[str, str]] = []
    for label in LABELS:
        keys = sorted(by_label.get(label, []))
        picked = rnd.sample(keys, min(per_label, len(keys)))
        for key in picked:
            rows.append(_row(packs[key], note="LLM 标签已隐藏；抽样层=%s" % label))
    _write_sheet(out_path, rows, ["audit_label", "auditor", "audit_notes"])
    return {"out": os.path.abspath(out_path), "accounts": len(rows),
            "sampled_by_label": {label: min(per_label, len(by_label.get(label, [])))
                                 for label in LABELS}}


def iaa(sheet_path: str, write_gold: Optional[str], min_kappa: float) -> int:
    with open(sheet_path, "r", encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))

    left, right, disagreements = [], [], []
    gold: Dict[str, Dict[str, str]] = {}
    for row in rows:
        key = (row.get("account_key") or "").strip()
        a = (row.get("annotator_a") or "").strip().lower()
        b = (row.get("annotator_b") or "").strip().lower()
        adjudicated = (row.get("adjudicated") or "").strip().lower()
        if key and a in LABELS and b in LABELS:
            left.append(a)
            right.append(b)
            if a != b:
                disagreements.append({"account_key": key, "annotator_a": a,
                                      "annotator_b": b, "notes": row.get("notes", "")})
        final = adjudicated if adjudicated in LABELS else (a if a == b and a in LABELS else None)
        if key and final:
            gold[key] = {"account_key": key, "label": final,
                         "source": "adjudicated" if adjudicated in LABELS else "consensus"}

    kappa = cohen_kappa(left, right)
    summary = {
        "rows": len(rows),
        "double_annotated": len(left),
        "disagreements": len(disagreements),
        "human_kappa": kappa,
        "gold_accounts": len(gold),
        "label_distribution": dict(Counter(item["label"] for item in gold.values())),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    for item in disagreements[:20]:
        print("分歧: %(account_key)s  a=%(annotator_a)s  b=%(annotator_b)s  %(notes)s" % item)

    if write_gold:
        os.makedirs(os.path.dirname(os.path.abspath(write_gold)), exist_ok=True)
        with open(write_gold, "w", encoding="utf-8") as fh:
            for item in gold.values():
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")

    return 0 if (kappa is None or kappa >= min_kappa) else 2


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="金标准工作表 / 盲审 / 一致性")
    sub = parser.add_subparsers(dest="command", required=True)

    p_export = sub.add_parser("export", help="导出双人标注表（盲标，不含模型输出）")
    p_export.add_argument("--evidence", default="data/labeling/evidence.jsonl")
    p_export.add_argument("--out", default="data/labeling/gold_sheet.csv")
    p_export.add_argument("--limit", type=int, default=400)
    p_export.add_argument("--seed", type=int, default=7)
    p_export.add_argument("--split", action="store_true",
                          help="额外按人拆出 _annotatorA / _annotatorB 两份独立表")

    p_audit = sub.add_parser("audit", help="按标签分层抽样盲审表")
    p_audit.add_argument("--labels", default="data/labeling/labels.jsonl")
    p_audit.add_argument("--evidence", default="data/labeling/evidence.jsonl")
    p_audit.add_argument("--out", default="data/labeling/audit_sheet.csv")
    p_audit.add_argument("--per-label", type=int, default=50)
    p_audit.add_argument("--seed", type=int, default=11)
    p_audit.add_argument("--run-id", default=None)

    p_iaa = sub.add_parser("iaa", help="计算人工一致性并生成 gold.jsonl")
    p_iaa.add_argument("--sheet", default="data/labeling/gold_sheet.csv")
    p_iaa.add_argument("--write-gold", default="data/labeling/gold.jsonl")
    p_iaa.add_argument("--min-kappa", type=float, default=0.6)

    args = parser.parse_args(argv)
    if args.command == "export":
        print(json.dumps(export_sheet(args.evidence, args.out, args.limit, args.seed,
                                      args.split),
                         ensure_ascii=False, indent=2))
        return 0
    if args.command == "audit":
        print(json.dumps(audit_sheet(args.labels, args.evidence, args.out,
                                     args.per_label, args.seed, args.run_id),
                         ensure_ascii=False, indent=2))
        return 0
    return iaa(args.sheet, args.write_gold, args.min_kappa)


if __name__ == "__main__":
    sys.exit(main())
