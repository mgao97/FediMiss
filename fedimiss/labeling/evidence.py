"""账号级证据包（evidence pack）构建。

把数据库里分散的账号资料、发帖节奏、内容形态、互动与归属信息，固化成
一条账号 = 一个 JSON 的「证据包」，供人工标注与 LLM 预标注共用同一份证据。

为什么要这样做，而不是把帖子原文直接丢给大模型：

- ``docs/annotation_guidelines.md`` 里 ``bot`` 的判据是「间隔方差极小 / 模板化 /
  几乎无互动」，这些是**统计量**，应由代码计算，不能让模型目测；
- 只有共用同一份证据，人工金标准与 LLM 输出才可比、可审计；
- 证据包里保留 ``note_key`` 与统计数值，模型输出的 ``evidence`` 才能被核验。

用法::

    python3 -m fedimiss.labeling.evidence \
        --db data/fedimiss.sqlite3 \
        --out data/labeling/evidence.jsonl \
        --sample 500 --seed 42

    # 只取某个时间窗口内的帖子（默认使用全部已采集帖子）
    python3 -m fedimiss.labeling.evidence ... \
        --window-id longitudinal-01-20250101

    # 指定账号，不做抽样
    python3 -m fedimiss.labeling.evidence ... \
        --account-key syuilo@misskey.io --account-key foo@misskey.io
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from . import EVIDENCE_VERSION

# 抽样分层的活跃度桶：0 条也要抽，作为「证据不足 → uncertain」的对照
ACTIVITY_BUCKETS = ((0, 1, "0"), (1, 20, "1-19"), (20, 100, "20-99"),
                    (100, 500, "100-499"), (500, 10 ** 9, "500+"))

TEXT_SAMPLE_SIZE = 12
TEXT_PREVIEW_CHARS = 240
DESCRIPTION_CHARS = 600
DEFAULT_TOP_INSTANCES = 10


# --------------------------------------------------------------------------- #
# 基础工具
# --------------------------------------------------------------------------- #

def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    """解析 Misskey 时间戳（``2026-10-23T23:58:55.797Z`` 等），失败返回 None。"""
    if not value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(raw, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if dt else None


def _entropy(counts: Iterable[int]) -> Optional[float]:
    """归一化熵（0~1），用于衡量发帖小时/分钟分布的规律性。"""
    values = [c for c in counts if c > 0]
    total = sum(values)
    if total <= 0 or len(values) <= 1:
        return 0.0 if values else None
    h = -sum((c / total) * math.log(c / total) for c in values)
    return round(h / math.log(len(values)), 4)


def _percentile(sorted_values: Sequence[float], p: float) -> Optional[float]:
    if not sorted_values:
        return None
    k = (len(sorted_values) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi:
        return float(sorted_values[lo])
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


def _round(value: Optional[float], digits: int = 3) -> Optional[float]:
    return None if value is None else round(float(value), digits)


def _normalize_text(text: str) -> str:
    """模板/重复检测用的归一化：去 URL、去空白、小写。"""
    out = []
    i = 0
    while i < len(text):
        if text.startswith("http://", i) or text.startswith("https://", i):
            j = i
            while j < len(text) and not text[j].isspace():
                j += 1
            out.append("<url>")
            i = j
            continue
        out.append(text[i])
        i += 1
    return "".join("".join(out).split()).lower()


def _bucket(n_notes: int) -> str:
    for lo, hi, name in ACTIVITY_BUCKETS:
        if lo <= n_notes < hi:
            return name
    return "500+"


def _connect(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        raise SystemExit("数据库不存在：%s" % db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def _chunks(items: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for start in range(0, len(items), size):
        yield items[start:start + size]


# --------------------------------------------------------------------------- #
# 抽样
# --------------------------------------------------------------------------- #

def load_population(conn: sqlite3.Connection, window_id: Optional[str]) -> List[Dict[str, Any]]:
    """候选账号 = 已采集到帖子的账号 ∪ ``notes_status='completed'`` 的账号。

    后者包含「采集完成但确实没有公开帖子」的账号，是 ``uncertain`` 的重要对照。
    """
    note_filter = " AND n.window_id = ?" if window_id else ""
    note_params: Tuple[Any, ...] = (window_id,) if window_id else ()
    count_filter = " WHERE window_id = ?" if window_id else ""
    sql = f"""
        SELECT a.account_key, a.username, a.host, a.instance_domain, a.display_name,
               a.description, a.created_at, a.followers_count, a.following_count,
               a.is_bot_declared, a.is_locked, a.home_instance_status, a.notes_status,
               a.profile_status, a.language, a.location, a.birthday, a.fields_json,
               a.discovery_instance, a.bfs_status,
               COALESCE(c.n, 0) AS collected_notes
        FROM accounts a
        LEFT JOIN (
            SELECT author_account_key, COUNT(*) AS n FROM notes{count_filter}
            GROUP BY author_account_key
        ) c ON c.author_account_key = a.account_key
        WHERE a.notes_status = 'completed'
           OR EXISTS (
                SELECT 1 FROM notes n
                WHERE n.author_account_key = a.account_key{note_filter}
           )
    """
    params: List[Any] = list(note_params) + list(note_params)
    return [dict(row) for row in conn.execute(sql, params)]


def _allocate(sizes: Dict[Tuple[str, str], int], n: int) -> Dict[Tuple[str, str], int]:
    """按层大小比例分配样本量，每层至少 1 个。"""
    if n <= 0 or not sizes:
        return {}
    if n < len(sizes):
        ranked = sorted(sizes.items(), key=lambda kv: (-kv[1], kv[0]))[:n]
        return {key: 1 for key, _ in ranked}

    total = sum(sizes.values())
    quota = {key: n * size / total for key, size in sizes.items()}
    base = {key: max(1, int(math.floor(q))) for key, q in quota.items()}

    while sum(base.values()) > n:  # 每层至少 1 导致超额时，从最大的层回收
        key = max((k for k in base if base[k] > 1), key=lambda k: base[k], default=None)
        if key is None:
            break
        base[key] -= 1
    remainder = n - sum(base.values())
    for key, _ in sorted(quota.items(), key=lambda kv: (-(kv[1] - math.floor(kv[1])), kv[0])):
        if remainder <= 0:
            break
        if base[key] < sizes[key]:
            base[key] += 1
            remainder -= 1
    return base


def sample_accounts(rows: Sequence[Dict[str, Any]], n: int, seed: int,
                    top_instances: int = DEFAULT_TOP_INSTANCES) -> List[Dict[str, Any]]:
    """按「实例 × 活跃度」分层随机抽样，避免只挑看起来像 bot 的账号。"""
    instance_counts = Counter(row["instance_domain"] or "unknown" for row in rows)
    keep_instances = {name for name, _ in instance_counts.most_common(top_instances)}

    strata: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        instance = row["instance_domain"] or "unknown"
        stratum_instance = instance if instance in keep_instances else "other"
        row["_stratum"] = (stratum_instance, _bucket(int(row["collected_notes"] or 0)))
        strata[row["_stratum"]].append(row)

    allocation = _allocate({key: len(members) for key, members in strata.items()}, n)
    picked: List[Dict[str, Any]] = []
    for (instance, bucket), members in sorted(strata.items()):
        k = allocation.get((instance, bucket), 0)
        if k <= 0:
            continue
        rnd = random.Random("%s:%s:%s" % (seed, instance, bucket))
        picked.extend(rnd.sample(members, min(k, len(members))))

    picked.sort(key=lambda row: (row["_stratum"], row["account_key"]))
    for row in picked:
        row["sampling_method"] = "stratified_random"
        row["sampling_stratum"] = "%s|%s" % row["_stratum"]
        row["sampling_seed"] = seed
    return picked


# --------------------------------------------------------------------------- #
# 统计
# --------------------------------------------------------------------------- #

class _Accumulator:
    """单账号的流式累加器：帖子按 created_at 升序喂入。"""

    __slots__ = ("account_key", "times", "n", "n_reply", "n_renote", "n_media",
                 "n_cw", "langs", "visibilities", "reply_targets", "text_lens",
                 "text_hashes", "template_counter", "samples", "seen", "seed",
                 "window_counts", "src_instances")

    def __init__(self, account_key: str, seed: int, sample_size: int) -> None:
        self.account_key = account_key
        self.times: List[datetime] = []
        self.n = self.n_reply = self.n_renote = self.n_media = self.n_cw = 0
        self.langs: Counter = Counter()
        self.visibilities: Counter = Counter()
        self.reply_targets: set = set()
        self.text_lens: List[int] = []
        self.text_hashes: set = set()
        self.template_counter: Counter = Counter()
        self.template_preview: Dict[str, str] = {}
        self.samples: List[Dict[str, Any]] = []
        self.seen = 0
        self.seed = seed
        self.sample_size = sample_size
        self.window_counts: Counter = Counter()
        self.src_instances: Counter = Counter()

    def add(self, row: sqlite3.Row) -> None:
        dt = _parse_ts(row["created_at"])
        text = row["text"] or ""
        self.seen += 1
        if dt is not None:
            self.times.append(dt)

        self.window_counts[row["window_id"] or "unclassified"] += 1
        self.src_instances[row["source_instance"] or "unknown"] += 1
        if row["reply_id"] or row["reply_account_key"]:
            self.n_reply += 1
        if row["reply_account_key"]:
            self.reply_targets.add(row["reply_account_key"])
        if row["renote_id"] or row["renote_account_key"]:
            self.n_renote += 1
        if row["has_media"]:
            self.n_media += 1
        if row["cw"]:
            self.n_cw += 1
        if row["lang"]:
            self.langs[row["lang"]] += 1
        if row["visibility"]:
            self.visibilities[row["visibility"]] += 1
        self.text_lens.append(len(text))

        if text.strip():
            normalized = _normalize_text(text)
            digest = hashlib.blake2b(normalized.encode("utf-8"), digest_size=8).hexdigest()
            self.text_hashes.add(digest)
            self.template_counter[digest] += 1

        # 确定性蓄水池抽样：样本帖子按时间均匀分布，便于人工核验
        if len(self.samples) < self.sample_size:
            self.samples.append(self._sample(dt, row, text))
        else:
            rnd = random.Random("%s:%d" % (self.account_key, self.seen))
            idx = rnd.randrange(self.seen)
            if idx < self.sample_size:
                self.samples[idx] = self._sample(dt, row, text)
        self.n += 1

    @staticmethod
    def _sample(dt: Optional[datetime], row: sqlite3.Row, text: str) -> Dict[str, Any]:
        return {
            "note_key": row["note_key"],
            "created_at": _iso(dt),
            "text": text[:TEXT_PREVIEW_CHARS],
            "has_media": bool(row["has_media"]),
        }

    def finalize(self) -> Dict[str, Any]:
        self.times.sort()
        gaps = [
            (b - a).total_seconds()
            for a, b in zip(self.times, self.times[1:])
            if (b - a).total_seconds() > 0
        ]
        gap_sorted = sorted(gaps)
        hour_counts = Counter(t.hour for t in self.times)
        minute_counts = Counter(t.minute for t in self.times)
        day_counts = Counter(t.date().isoformat() for t in self.times)

        gap_stats: Dict[str, Any] = {
            "n_intervals": len(gaps),
            "median_seconds": _round(statistics.median(gaps), 1) if gaps else None,
            "iqr_seconds": None,
            "cv": None,
            "modal_minutes": None,
            "modal_share": None,
        }
        if gaps:
            q1 = _percentile(gap_sorted, 0.25)
            q3 = _percentile(gap_sorted, 0.75)
            gap_stats["iqr_seconds"] = _round(q3 - q1, 1)
            mean = statistics.fmean(gaps)
            gap_stats["cv"] = _round(statistics.pstdev(gaps) / mean, 4) if mean else None
            modal = Counter(int(round(g / 60)) for g in gaps).most_common(1)[0]
            gap_stats["modal_minutes"] = modal[0]
            gap_stats["modal_share"] = _round(modal[1] / len(gaps), 4)

        burst = 0
        left = 0
        for right, t in enumerate(self.times):
            while t - self.times[left] > timedelta(hours=1):
                left += 1
            burst = max(burst, right - left + 1)

        n_text = sum(1 for length in self.text_lens if length > 0)
        top_hash, top_count = (None, 0)
        if self.template_counter:
            top_hash, top_count = self.template_counter.most_common(1)[0]

        return {
            "notes_collected": self.n,
            "window_counts": dict(self.window_counts.most_common()),
            "source_instances": dict(self.src_instances.most_common(3)),
            "first_note_at": _iso(self.times[0]) if self.times else None,
            "last_note_at": _iso(self.times[-1]) if self.times else None,
            "span_days": _round((self.times[-1] - self.times[0]).total_seconds() / 86400, 2)
            if len(self.times) > 1 else (0.0 if self.times else None),
            "timing": {
                "gap_median_seconds": gap_stats["median_seconds"],
                "gap_iqr_seconds": gap_stats["iqr_seconds"],
                "gap_cv": gap_stats["cv"],
                "modal_interval_minutes": gap_stats["modal_minutes"],
                "modal_interval_share": gap_stats["modal_share"],
                "hour_entropy": _entropy(hour_counts.values()),
                "minute_entropy": _entropy(minute_counts.values()),
                "top_minute_share": _round(minute_counts.most_common(1)[0][1] / self.n, 4)
                if self.n else None,
                "active_hours": len(hour_counts),
                "active_days": len(day_counts),
                "max_notes_per_day": max(day_counts.values()) if day_counts else 0,
                "max_notes_in_1h": burst,
                "posts_per_active_day": _round(self.n / len(day_counts), 2) if day_counts else None,
            },
            "content": {
                "duplicate_rate": _round((n_text - len(self.text_hashes)) / n_text, 4)
                if n_text else None,
                "top_text_share": _round(top_count / n_text, 4) if n_text else None,
                "top_text_preview": self.template_preview.get(top_hash) if top_hash else None,
                "cw_rate": _round(self.n_cw / self.n, 4) if self.n else None,
                "mean_text_chars": _round(statistics.fmean(self.text_lens), 1) if self.text_lens else None,
                "languages": dict(self.langs.most_common(3)),
                "visibility": dict(self.visibilities.most_common()),
            },
            "interaction": {
                "reply_rate": _round(self.n_reply / self.n, 4) if self.n else None,
                "renote_rate": _round(self.n_renote / self.n, 4) if self.n else None,
                "distinct_reply_targets": len(self.reply_targets),
            },
            "media": {
                "media_rate": _round(self.n_media / self.n, 4) if self.n else None,
            },
            "samples": sorted(self.samples, key=lambda s: s["created_at"] or ""),
        }


def compute_stats(conn: sqlite3.Connection, account_keys: Sequence[str],
                  window_id: Optional[str]) -> Dict[str, Dict[str, Any]]:
    """按账号流式计算帖子统计（分块查询，避免一次性载入全库）。"""
    seed = int(hashlib.blake2b(account_keys[0].encode(), digest_size=4).hexdigest(), 16) \
        if account_keys else 0
    accumulators = {
        key: _Accumulator(key, seed, TEXT_SAMPLE_SIZE) for key in account_keys
    }
    condition = " AND window_id = ?" if window_id else ""
    params_tail: Tuple[Any, ...] = (window_id,) if window_id else ()

    for chunk in _chunks(list(account_keys), 200):
        placeholders = ",".join("?" for _ in chunk)
        sql = f"""
            SELECT note_key, author_account_key, created_at, text, cw, visibility, lang,
                   reply_id, renote_id, reply_account_key, renote_account_key,
                   has_media, window_id, source_instance
            FROM notes
            WHERE author_account_key IN ({placeholders}){condition}
            ORDER BY author_account_key, created_at
        """
        for row in conn.execute(sql, tuple(chunk) + params_tail):
            acc = accumulators.get(row["author_account_key"])
            if acc is not None:
                acc.add(row)

    return {key: acc.finalize() for key, acc in accumulators.items()}


def load_edges(conn: sqlite3.Connection, account_keys: Sequence[str]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = defaultdict(lambda: {"following_out": 0, "following_in": 0})
    for chunk in _chunks(list(account_keys), 500):
        placeholders = ",".join("?" for _ in chunk)
        for row in conn.execute(
            f"SELECT source_account_key AS k, COUNT(*) AS n FROM follow_edges "
            f"WHERE source_account_key IN ({placeholders}) GROUP BY 1", tuple(chunk)
        ):
            out[row["k"]]["following_out"] = row["n"]
        for row in conn.execute(
            f"SELECT target_account_key AS k, COUNT(*) AS n FROM follow_edges "
            f"WHERE target_account_key IN ({placeholders}) GROUP BY 1", tuple(chunk)
        ):
            out[row["k"]]["following_in"] = row["n"]
    return dict(out)


def load_instance_api(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    return {
        row["instance_domain"]: {
            "following_api": row["following_api"],
            "followers_api": row["followers_api"],
            "software": row["software_name"],
        }
        for row in conn.execute(
            "SELECT instance_domain, following_api, followers_api, software_name FROM instances"
        )
    }


def load_mapping(conn: sqlite3.Connection, account_keys: Sequence[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    keys = list(account_keys)
    for chunk in _chunks(keys, 500):
        placeholders = ",".join("?" for _ in chunk)
        for row in conn.execute(
            f"SELECT mapping_key, confidence FROM account_id_mapping "
            f"WHERE mapping_key IN ({placeholders})", tuple(chunk)
        ):
            out[row["mapping_key"]] = row["confidence"]
    return out


def _profile(row: Dict[str, Any]) -> Dict[str, Any]:
    fields: List[Dict[str, str]] = []
    try:
        parsed = json.loads(row.get("fields_json") or "[]")
        for item in parsed[:8]:
            if isinstance(item, dict):
                fields.append({"name": str(item.get("name", ""))[:40],
                               "value": str(item.get("value", ""))[:120]})
    except (ValueError, TypeError):
        pass
    created = _parse_ts(row.get("created_at"))
    return {
        "display_name": (row.get("display_name") or "")[:120],
        "description": (row.get("description") or "")[:DESCRIPTION_CHARS],
        "account_created_at": _iso(created),
        "account_age_days": _round((datetime.now(timezone.utc) - created).days, 0) if created else None,
        "followers_count": row.get("followers_count"),
        "following_count": row.get("following_count"),
        "is_bot_declared": bool(row.get("is_bot_declared")),
        "is_locked": bool(row.get("is_locked")),
        "language": row.get("language"),
        "location": row.get("location"),
        "birthday": row.get("birthday"),
        "profile_fields": fields,
    }


def build_pack(row: Dict[str, Any], stats: Dict[str, Any], edges: Dict[str, int],
               instance_api: Dict[str, Any], mapping_confidence: Optional[str],
               window_scope: Optional[str]) -> Dict[str, Any]:
    instance = row.get("instance_domain") or "unknown"
    api = instance_api.get(instance, {})
    interaction_observable = api.get("following_api") == "ok" or api.get("followers_api") == "ok"
    return {
        "evidence_version": EVIDENCE_VERSION,
        "account_key": row["account_key"],
        "account_url": row.get("url"),
        "profile": _profile(row),
        "collection": {
            "home_instance_status": row.get("home_instance_status"),
            "mapping_confidence": mapping_confidence,
            "discovery_instance": row.get("discovery_instance"),
            "notes_status": row.get("notes_status"),
            "bfs_status": row.get("bfs_status"),
            "window_scope": window_scope,
            "notes_collected": stats["notes_collected"],
            "window_counts": stats["window_counts"],
            "first_note_at": stats["first_note_at"],
            "last_note_at": stats["last_note_at"],
            "span_days": stats["span_days"],
        },
        "timing": stats["timing"],
        "content": stats["content"],
        "interaction": {
            **stats["interaction"],
            **edges,
            "following_api": api.get("following_api"),
            "followers_api": api.get("followers_api"),
            # 关键：misskey.io 等实例关系接口 forbidden，不能把「看不到互动」当成「没有互动」
            "interaction_observable": interaction_observable,
        },
        "media": stats["media"],
        "samples": stats["samples"],
        "sampling": {
            "method": row.get("sampling_method"),
            "stratum": row.get("sampling_stratum"),
            "seed": row.get("sampling_seed"),
            "text_sample_size": TEXT_SAMPLE_SIZE,
        },
    }


# --------------------------------------------------------------------------- #
# 入口
# --------------------------------------------------------------------------- #

def build_evidence(db_path: str, out_path: str, sample: int, seed: int,
                   window_id: Optional[str] = None,
                   account_keys: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    conn = _connect(db_path)
    try:
        if account_keys:
            placeholders = ",".join("?" for _ in account_keys)
            rows = [dict(r) for r in conn.execute(
                f"SELECT * FROM accounts WHERE account_key IN ({placeholders})", tuple(account_keys)
            )]
            missing = set(account_keys) - {r["account_key"] for r in rows}
            if missing:
                raise SystemExit("以下账号不存在：%s" % ", ".join(sorted(missing)))
            for row in rows:
                row["collected_notes"] = conn.execute(
                    "SELECT COUNT(*) FROM notes WHERE author_account_key = ?", (row["account_key"],)
                ).fetchone()[0]
                row["sampling_method"] = "manual"
                row["sampling_stratum"] = None
                row["sampling_seed"] = None
            picked = rows
        else:
            population = load_population(conn, window_id)
            picked = sample_accounts(population, sample, seed)

        keys = [row["account_key"] for row in picked]
        stats = compute_stats(conn, keys, window_id)
        edges = load_edges(conn, keys)
        instance_api = load_instance_api(conn)
        mapping = load_mapping(conn, keys)

        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as fh:
            for row in picked:
                pack = build_pack(row, stats[row["account_key"]],
                                  edges.get(row["account_key"], {}),
                                  instance_api, mapping.get(row["account_key"]), window_id)
                fh.write(json.dumps(pack, ensure_ascii=False) + "\n")
    finally:
        conn.close()

    summary = {
        "db": os.path.abspath(db_path),
        "out": os.path.abspath(out_path),
        "accounts": len(picked),
        "window_scope": window_id,
        "seed": seed,
        "evidence_version": EVIDENCE_VERSION,
        "strata": len({row.get("sampling_stratum") for row in picked}),
    }
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="构建账号级证据包（JSONL）")
    parser.add_argument("--db", default="data/fedimiss.sqlite3")
    parser.add_argument("--out", default="data/labeling/evidence.jsonl")
    parser.add_argument("--sample", type=int, default=500, help="抽样账号数；<=0 表示全量")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--window-id", default=None, help="只统计该窗口的帖子")
    parser.add_argument("--account-key", action="append", default=None,
                        help="指定账号（可重复），忽略 --sample")
    args = parser.parse_args(argv)

    summary = build_evidence(args.db, args.out, args.sample, args.seed,
                             args.window_id, args.account_key)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
