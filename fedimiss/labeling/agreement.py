"""一致性评估：LLM 预标注 vs 人工金标准 / 标注者之间的一致性。

回答三个问题：

1. 模型在金标准上到底准不准（每类 P/R/F1、混淆矩阵、κ）；
2. 模型的置信度能不能用来分流（各置信度档的实际准确率、ECE）；
3. 这批标签能不能放行（``--min-bot-precision`` 等门槛，不达标返回非零退出码）。

金标准输入支持 JSONL（``{"account_key": ..., "label": ...}``）与 CSV
（列 ``account_key`` 与 ``label``，或 ``annotator_a/annotator_b/adjudicated``）。

用法::

    # 人工双标一致性
    python3 -m fedimiss.labeling.agreement --gold gold.csv --human-kappa

    # LLM vs 金标准（取某次 run）
    python3 -m fedimiss.labeling.agreement \
        --labels data/labeling/labels.jsonl --gold gold.jsonl \
        --run-id run-20261104T000000Z \
        --min-bot-precision 0.90 --min-kappa 0.70
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from . import LABELS


# --------------------------------------------------------------------------- #
# 读取
# --------------------------------------------------------------------------- #

def read_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def load_gold(path: str) -> Dict[str, str]:
    """金标准：优先取 ``adjudicated``，其次 ``label``；缺失的账号跳过。"""
    gold: Dict[str, str] = {}
    if path.endswith(".csv"):
        with open(path, "r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                key = (row.get("account_key") or "").strip()
                label = (row.get("adjudicated") or row.get("label") or "").strip().lower()
                if key and label in LABELS:
                    gold[key] = label
        return gold
    rows: List[Dict[str, Any]] = []
    for item in read_jsonl(path):
        if isinstance(item, list):  # 容忍 JSON 数组整行
            rows.extend(x for x in item if isinstance(x, dict))
        elif isinstance(item, dict):
            rows.append(item)
    for row in rows:
        key = row.get("account_key")
        label = (row.get("adjudicated") or row.get("label") or "").strip().lower()
        if key and label in LABELS:
            gold[key] = label
    return gold


def latest_by_account(rows: Iterable[Dict[str, Any]], run_id: Optional[str]) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if run_id and row.get("run_id") != run_id:
            continue
        key = row.get("account_key")
        if key:
            latest[key] = row  # 文件为追加写，后写覆盖前写
    return latest


# --------------------------------------------------------------------------- #
# 指标
# --------------------------------------------------------------------------- #

def cohen_kappa(left: Sequence[str], right: Sequence[str],
                labels: Sequence[str] = LABELS) -> Optional[float]:
    if len(left) != len(right) or not left:
        return None
    n = len(left)
    observed = sum(1 for a, b in zip(left, right) if a == b) / n
    left_counts, right_counts = Counter(left), Counter(right)
    expected = sum(left_counts[label] / n * right_counts[label] / n for label in labels
                   if left_counts[label] and right_counts[label])
    if expected >= 1.0:
        return 1.0 if observed >= 1.0 else 0.0
    return round((observed - expected) / (1 - expected), 4)


def per_label_metrics(pairs: Sequence[Tuple[str, str]],
                      labels: Sequence[str] = LABELS) -> Dict[str, Dict[str, float]]:
    """pairs = [(gold, pred)]。"""
    out: Dict[str, Dict[str, float]] = {}
    for label in labels:
        tp = sum(1 for gold, pred in pairs if gold == label and pred == label)
        fp = sum(1 for gold, pred in pairs if gold != label and pred == label)
        fn = sum(1 for gold, pred in pairs if gold == label and pred != label)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        out[label] = {"support": tp + fn, "precision": round(precision, 4),
                      "recall": round(recall, 4), "f1": round(f1, 4)}
    return out


def binary_bot_metrics(pairs: Sequence[Tuple[str, str]]) -> Dict[str, float]:
    """bot vs 非 bot（对抗性场景最关心的指标）。"""
    tp = sum(1 for gold, pred in pairs if gold == "bot" and pred == "bot")
    fp = sum(1 for gold, pred in pairs if gold != "bot" and pred == "bot")
    fn = sum(1 for gold, pred in pairs if gold == "bot" and pred != "bot")
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"tp": tp, "fp": fp, "fn": fn,
            "precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4)}


NOMINAL = {"high": 0.9, "medium": 0.7, "low": 0.5}


def calibration(pairs: Sequence[Tuple[str, str, Optional[str]]]) -> Dict[str, Any]:
    """按 confidence 档统计实际准确率；ECE 用名义置信度 {high:0.9, medium:0.7, low:0.5}。"""
    buckets: Dict[str, List[int]] = defaultdict(list)
    for gold, pred, confidence in pairs:
        buckets[confidence or "low"].append(1 if gold == pred else 0)
    stats = {name: {"n": len(values), "accuracy": round(sum(values) / len(values), 4)}
             for name, values in buckets.items() if values}
    total = sum(len(values) for values in buckets.values())
    ece = 0.0
    if total:
        for name, values in buckets.items():
            if values:
                ece += len(values) / total * abs(sum(values) / len(values) - NOMINAL.get(name, 0.5))
    return {"bins": stats, "ece": round(ece, 4)}


def wilson_interval(successes: int, n: int, z: float = 1.96) -> Tuple[float, float]:
    if n <= 0:
        return (0.0, 0.0)
    p = successes / n
    denominator = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denominator
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denominator
    return (round(max(0.0, center - margin), 4), round(min(1.0, center + margin), 4))


def confusion_table(pairs: Sequence[Tuple[str, str]],
                    labels: Sequence[str] = LABELS) -> List[List[int]]:
    index = {label: i for i, label in enumerate(labels)}
    matrix = [[0] * len(labels) for _ in labels]
    for gold, pred in pairs:
        matrix[index[gold]][index[pred]] += 1
    return matrix


def format_report(result: Dict[str, Any]) -> str:
    lines: List[str] = []
    labels = result["labels"]
    lines.append("样本数：%d" % result["n"])
    lines.append("准确率：%.4f   κ=%.4f" % (result["accuracy"], result["kappa"] or 0.0))
    lines.append("")
    header = "gold\\pred".ljust(12) + "".join(label.rjust(12) for label in labels)
    lines.append(header)
    for i, label in enumerate(labels):
        lines.append(label.ljust(12) + "".join(str(v).rjust(12) for v in result["confusion"][i]))
    lines.append("")
    lines.append("%-12s %8s %10s %10s %10s" % ("label", "support", "precision", "recall", "f1"))
    for label in labels:
        m = result["per_label"][label]
        lines.append("%-12s %8d %10.4f %10.4f %10.4f" % (
            label, m["support"], m["precision"], m["recall"], m["f1"]))
    lines.append("")
    b = result["binary_bot"]
    lines.append("bot vs 非 bot：precision=%.4f recall=%.4f f1=%.4f (tp=%d fp=%d fn=%d)" % (
        b["precision"], b["recall"], b["f1"], b["tp"], b["fp"], b["fn"]))
    if result.get("calibration"):
        cal = result["calibration"]
        bins = ", ".join("%s: n=%d acc=%.3f" % (k, v["n"], v["accuracy"])
                         for k, v in sorted(cal["bins"].items()))
        lines.append("置信度分档：%s；ECE=%.4f" % (bins, cal["ece"]))
    if result.get("human_kappa") is not None:
        lines.append("人工双标 κ=%.4f（n=%d）" % (result["human_kappa"], result["human_kappa_n"]))
    if result.get("gates"):
        lines.append("")
        for name, passed in result["gates"].items():
            lines.append("门槛 %s：%s" % (name, "PASS" if passed else "FAIL"))
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# 入口
# --------------------------------------------------------------------------- #

def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="标注一致性 / 质量评估")
    parser.add_argument("--labels", default="data/labeling/labels.jsonl")
    parser.add_argument("--gold", default="data/labeling/gold.jsonl")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--human-kappa", action="store_true",
                        help="使用金标准 CSV 的 annotator_a/annotator_b 计算人工一致性")
    parser.add_argument("--json", dest="json_out", default=None, help="同时写出指标 JSON")
    parser.add_argument("--min-bot-precision", type=float, default=None)
    parser.add_argument("--min-kappa", type=float, default=None)
    args = parser.parse_args(argv)

    gold = load_gold(args.gold)
    if not gold:
        raise SystemExit("金标准为空：%s" % args.gold)
    predicted = latest_by_account(read_jsonl(args.labels), args.run_id)

    matched = [(gold[key], row["label"]) for key, row in predicted.items()
               if key in gold and row.get("label") in LABELS]
    if not matched:
        raise SystemExit("没有可比的账号：检查 account_key 是否与金标准对齐")

    pair_with_conf = [(gold[key], row["label"], row.get("confidence"))
                      for key, row in predicted.items() if key in gold]
    result: Dict[str, Any] = {
        "n": len(matched),
        "accuracy": round(sum(1 for g, p in matched if g == p) / len(matched), 4),
        "kappa": cohen_kappa([g for g, _ in matched], [p for _, p in matched]),
        "confusion": confusion_table(matched),
        "labels": list(LABELS),
        "per_label": per_label_metrics(matched),
        "binary_bot": binary_bot_metrics(matched),
        "calibration": calibration(pair_with_conf),
        "label_distribution": dict(Counter(p for _, p in matched)),
        "gold_distribution": dict(Counter(g for g, _ in matched)),
    }

    if args.human_kappa and args.gold.endswith(".csv"):
        left, right = [], []
        with open(args.gold, "r", encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                a = (row.get("annotator_a") or "").strip().lower()
                b = (row.get("annotator_b") or "").strip().lower()
                if a in LABELS and b in LABELS:
                    left.append(a)
                    right.append(b)
        result["human_kappa"] = cohen_kappa(left, right) if left else None
        result["human_kappa_n"] = len(left)

    gates: Dict[str, bool] = {}
    if args.min_bot_precision is not None:
        gates["bot_precision>=%.2f" % args.min_bot_precision] = \
            result["binary_bot"]["precision"] >= args.min_bot_precision
    if args.min_kappa is not None:
        gates["kappa>=%.2f" % args.min_kappa] = (result["kappa"] or 0.0) >= args.min_kappa
    if gates:
        result["gates"] = gates

    print(format_report(result))
    if args.json_out:
        os.makedirs(os.path.dirname(os.path.abspath(args.json_out)), exist_ok=True)
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(result, fh, ensure_ascii=False, indent=2)

    return 2 if gates and not all(gates.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
