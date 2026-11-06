"""LLM 预标注：证据包 → 结构化标签（JSONL，可续跑、可审计）。

只使用标准库（urllib），兼容任意 OpenAI Chat Completions 风格的服务：
本地 vLLM / Ollama（``/v1``）/ LM Studio，或云端 API。**推荐本地部署**：
帖子原文属第三方内容，外发前需确认实例 Content-Signal 与合规要求。

用法::

    # 1) 离线自检：不调用网络，用规则基线验证流水线
    python3 -m fedimiss.labeling.llm_label \
        --input data/labeling/evidence.jsonl \
        --out data/labeling/labels_mock.jsonl --mock

    # 2) 本地模型
    FEDIMISS_LLM_BASE_URL=http://127.0.0.1:8000/v1 \
    python3 -m fedimiss.labeling.llm_label \
        --input data/labeling/evidence.jsonl \
        --out data/labeling/labels.jsonl \
        --model Qwen3-8B --votes 3 --workers 4

    # 3) 人工指定的重点账号
    ... --account-key foo@misskey.io --account-key bar@misskey.io

设计要点：

- ``--votes N`` 做 self-consistency，多数票且要求严格多数，否则降级 ``uncertain``；
- 每条记录带 ``prompt_sha256 / model / evidence_hash``，标签可追溯到具体版本；
- 默认续跑：同一 ``(account_key, prompt, model, votes)`` 已标注则跳过；
- 模型没给证据时自动降级为 ``uncertain``（对应规范 3.3）。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import CONFIDENCES, EVIDENCE_VERSION, LABELS, PROMPT_VERSION

DEFAULT_PROMPT = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))), "prompts", "label_v1.md")


# --------------------------------------------------------------------------- #
# 证据渲染与校验
# --------------------------------------------------------------------------- #

def render_evidence(pack: Dict[str, Any]) -> str:
    """把证据包渲染成紧凑、字段可引用的文本，控制 prompt 体积。"""
    lines: List[str] = []
    lines.append("account_key: %s" % pack.get("account_key"))
    if pack.get("account_url"):
        lines.append("account_url: %s" % pack["account_url"])
    for section in ("profile", "collection", "timing", "content", "interaction", "media"):
        lines.append("%s: %s" % (section, json.dumps(pack.get(section, {}), ensure_ascii=False)))
    lines.append("samples:")
    for idx, sample in enumerate(pack.get("samples", []), start=1):
        text = (sample.get("text") or "").replace("\n", " ")
        lines.append("%d. [%s] %s%s" % (idx, sample.get("created_at"), text,
                                        " [media]" if sample.get("has_media") else ""))
    return "\n".join(lines)


def evidence_hash(pack: Dict[str, Any]) -> str:
    payload = {key: pack.get(key) for key in
               ("account_key", "profile", "collection", "timing", "content",
                "interaction", "media", "samples")}
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]


def extract_json(text: str) -> Optional[Dict[str, Any]]:
    """从模型回复中抽取第一个 JSON 对象（容忍 ```json 包裹与前后缀）。"""
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(text[index:])
        except ValueError:
            continue
        if isinstance(obj, dict):
            return obj
    return None


def normalize_result(raw: Dict[str, Any]) -> Dict[str, Any]:
    label = str(raw.get("label", "")).strip().lower()
    confidence = str(raw.get("confidence", "")).strip().lower()
    if label not in LABELS:
        return {"label": "uncertain", "confidence": "low", "rule_ids": [],
                "evidence": [], "uncertain_reason": "invalid_label:%s" % label,
                "verification": raw.get("verification")}
    if confidence not in CONFIDENCES:
        confidence = "low"
    evidence = [str(item)[:300] for item in (raw.get("evidence") or []) if str(item).strip()][:8]
    rule_ids = [str(item) for item in (raw.get("rule_ids") or [])][:4]

    if label in ("bot", "human") and not evidence:
        return {"label": "uncertain", "confidence": "low", "rule_ids": rule_ids,
                "evidence": [], "uncertain_reason": "model_no_evidence",
                "verification": raw.get("verification")}
    if label == "uncertain" and not raw.get("uncertain_reason"):
        return {"label": "uncertain", "confidence": confidence, "rule_ids": rule_ids,
                "evidence": evidence, "uncertain_reason": "unspecified",
                "verification": raw.get("verification")}
    return {"label": label, "confidence": confidence, "rule_ids": rule_ids,
            "evidence": evidence,
            "uncertain_reason": raw.get("uncertain_reason") if label == "uncertain" else None,
            "verification": raw.get("verification")}


# --------------------------------------------------------------------------- #
# 后端
# --------------------------------------------------------------------------- #

def call_openai(base_url: str, model: str, api_key: Optional[str],
                messages: List[Dict[str, str]], temperature: float,
                max_tokens: int, timeout: float, retries: int = 3) -> str:
    url = base_url.rstrip("/") + "/chat/completions"
    payload = json.dumps({
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = "Bearer %s" % api_key

    last_error: Optional[Exception] = None
    for attempt in range(retries):
        request = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = json.loads(response.read().decode("utf-8"))
            return body["choices"][0]["message"]["content"]
        except urllib.error.HTTPError as exc:  # 429/5xx 退避
            last_error = exc
            if exc.code not in (429, 500, 502, 503, 504):
                raise
            time.sleep((2 ** attempt) + random.random())
        except (urllib.error.URLError, TimeoutError, KeyError, ValueError) as exc:
            last_error = exc
            time.sleep((2 ** attempt) + random.random())
    raise RuntimeError("LLM 调用失败：%s" % last_error)


def _mock_label(pack: Dict[str, Any]) -> Dict[str, Any]:
    """离线规则基线：用于验证流水线，不作为研究结论。"""
    collection = pack.get("collection", {})
    profile = pack.get("profile", {})
    timing = pack.get("timing", {})
    content = pack.get("content", {})
    interaction = pack.get("interaction", {})

    if collection.get("mapping_confidence") == "unresolved":
        return {"label": "uncertain", "confidence": "low", "rule_ids": ["3.3"],
                "evidence": ["collection.mapping_confidence=unresolved"],
                "uncertain_reason": "账号归属不可靠"}
    if int(collection.get("notes_collected") or 0) < 20:
        return {"label": "uncertain", "confidence": "low", "rule_ids": ["3.3"],
                "evidence": ["collection.notes_collected=%s" % collection.get("notes_collected")],
                "uncertain_reason": "样本不足 20 条"}

    reply_rate = float(interaction.get("reply_rate") or 0.0)
    targets = int(interaction.get("distinct_reply_targets") or 0)
    gap_cv = timing.get("gap_cv")
    modal_share = float(timing.get("modal_interval_share") or 0.0)
    duplicate = float(content.get("duplicate_rate") or 0.0)
    declared = bool(profile.get("is_bot_declared"))

    regular = gap_cv is not None and float(gap_cv) < 0.2
    templated = duplicate >= 0.3 or float(content.get("top_text_share") or 0) >= 0.25
    observable = bool(interaction.get("interaction_observable"))
    if (declared and (regular or templated)) or (
            regular and templated and modal_share >= 0.15 and observable and reply_rate < 0.02):
        return {"label": "bot", "confidence": "high" if declared and regular else "medium",
                "rule_ids": ["3.1-1"] if declared else ["3.1-2"],
                "evidence": ["timing.gap_cv=%s" % gap_cv,
                             "content.duplicate_rate=%s" % duplicate,
                             "profile.is_bot_declared=%s" % declared],
                "uncertain_reason": None}
    if reply_rate > 0.05 and targets >= 3 and float(timing.get("hour_entropy") or 1) > 0.6:
        return {"label": "human", "confidence": "medium", "rule_ids": ["3.2-1"],
                "evidence": ["interaction.reply_rate=%s" % reply_rate,
                             "interaction.distinct_reply_targets=%d" % targets],
                "uncertain_reason": None}
    return {"label": "uncertain", "confidence": "low", "rule_ids": ["3.3"],
            "evidence": ["timing.gap_cv=%s" % gap_cv, "content.duplicate_rate=%s" % duplicate],
            "uncertain_reason": "证据不足以区分 bot/human"}


class RateLimiter:
    def __init__(self, qps: float) -> None:
        self.interval = 1.0 / qps if qps > 0 else 0.0
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        if self.interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            delay = self._last + self.interval - now
            if delay > 0:
                time.sleep(delay)
                now = time.monotonic()
            self._last = now


def label_one(pack: Dict[str, Any], prompt: str, args: argparse.Namespace,
              limiter: RateLimiter) -> Dict[str, Any]:
    started = time.monotonic()
    messages = [{"role": "user", "content": prompt.replace("{{EVIDENCE}}", render_evidence(pack))}]
    votes: List[Dict[str, Any]] = []
    errors: List[str] = []

    for vote_index in range(max(1, args.votes)):
        limiter.wait()
        try:
            if args.mock:
                raw = _mock_label(pack)
            else:
                text = call_openai(args.base_url, args.model, args.api_key, messages,
                                   args.temperature if args.votes == 1 else max(args.temperature, 0.7),
                                   args.max_tokens, args.timeout)
                parsed = extract_json(text)
                if parsed is None:
                    raise ValueError("回复中找不到 JSON 对象")
                raw = parsed
            votes.append(normalize_result(raw))
        except Exception as exc:  # 单票失败不丢弃整条账号，记录后重试/降级
            errors.append(str(exc)[:200])

    if not votes:
        return {
            "account_key": pack["account_key"], "label": "uncertain", "confidence": "low",
            "rule_ids": [], "evidence": [], "uncertain_reason": "llm_error",
            "verification": None, "votes": [], "vote_agreement": 0.0,
            "error": "; ".join(errors)[:400],
            "latency_ms": int((time.monotonic() - started) * 1000),
        }

    counts = Counter(v["label"] for v in votes)
    top_label, top_count = counts.most_common(1)[0]
    agreement = top_count / len(votes)
    if top_count * 2 <= len(votes):  # 未过半 → 降级
        final = {"label": "uncertain", "confidence": "low", "rule_ids": ["3.3"],
                 "evidence": [], "uncertain_reason": "模型多票不一致",
                 "verification": None}
    elif agreement >= 0.999:
        final = next(v for v in votes if v["label"] == top_label)
    else:
        final = dict(next(v for v in votes if v["label"] == top_label))
        final["confidence"] = "medium" if final["confidence"] == "high" else "low"

    return {
        "account_key": pack["account_key"],
        "label": final["label"],
        "confidence": final["confidence"],
        "rule_ids": final.get("rule_ids", []),
        "evidence": final.get("evidence", []),
        "uncertain_reason": final.get("uncertain_reason"),
        "verification": final.get("verification"),
        "votes": votes,
        "vote_agreement": round(agreement, 3),
        "error": "; ".join(errors)[:400] if errors else None,
        "latency_ms": int((time.monotonic() - started) * 1000),
    }


# --------------------------------------------------------------------------- #
# 主流程
# --------------------------------------------------------------------------- #

def _resume_index(path: str, key_fields: Tuple[str, ...]) -> set:
    done = set()
    if not os.path.exists(path):
        return done
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            done.add(tuple(record.get(field) for field in key_fields))
    return done


def run(args: argparse.Namespace) -> int:
    with open(args.prompt, "r", encoding="utf-8") as fh:
        template = fh.read()
    prompt_sha = hashlib.sha256(template.encode("utf-8")).hexdigest()[:16]

    packs: List[Dict[str, Any]] = []
    with open(args.input, "r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                packs.append(json.loads(line))
    if args.account_key:
        wanted = set(args.account_key)
        packs = [pack for pack in packs if pack["account_key"] in wanted]
    if args.limit > 0:
        packs = packs[:args.limit]
    if not packs:
        print("没有待标注账号", file=sys.stderr)
        return 1

    run_id = args.run_id or datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")
    resume_keys = ("account_key", "run_id")
    done = set() if args.no_resume else _resume_index(args.out, resume_keys)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    lock = threading.Lock()
    limiter = RateLimiter(args.qps)
    counters = Counter()
    total = 0

    with open(args.out, "a", encoding="utf-8") as out_fh, \
            ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {}
        for pack in packs:
            if (pack["account_key"], run_id) in done:
                counters["skipped"] += 1
                continue
            futures[pool.submit(label_one, pack, template, args, limiter)] = pack
            total += 1

        for future in as_completed(futures):
            pack = futures[future]
            try:
                result = future.result()
            except Exception as exc:  # 兜底，不让单条异常中断批处理
                result = {"account_key": pack["account_key"], "label": "uncertain",
                          "confidence": "low", "uncertain_reason": "llm_error",
                          "error": str(exc)[:400], "votes": [], "vote_agreement": 0.0}
            record = {
                **result,
                "prompt_version": PROMPT_VERSION,
                "prompt_sha256": prompt_sha,
                "evidence_version": pack.get("evidence_version", EVIDENCE_VERSION),
                "evidence_hash": evidence_hash(pack),
                "model": "mock-rules-v1" if args.mock else args.model,
                "votes_requested": max(1, args.votes),
                "run_id": run_id,
                "labeled_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }
            with lock:
                out_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                out_fh.flush()
                counters[record["label"]] += 1
                if record.get("error"):
                    counters["with_error"] += 1

    summary = {
        "run_id": run_id,
        "out": os.path.abspath(args.out),
        "prompt_sha256": prompt_sha,
        "model": "mock-rules-v1" if args.mock else args.model,
        "votes": max(1, args.votes),
        "labeled": total - counters["skipped"],
        "skipped_existing": counters["skipped"],
        "labels": {label: counters[label] for label in LABELS},
        "with_error": counters["with_error"],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="LLM 账号级预标注（OpenAI 兼容接口）")
    parser.add_argument("--input", default="data/labeling/evidence.jsonl")
    parser.add_argument("--out", default="data/labeling/labels.jsonl")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    parser.add_argument("--model", default=os.environ.get("FEDIMISS_LLM_MODEL", "local-model"))
    parser.add_argument("--base-url", default=os.environ.get("FEDIMISS_LLM_BASE_URL",
                                                             "http://127.0.0.1:8000/v1"))
    parser.add_argument("--api-key", default=os.environ.get("FEDIMISS_LLM_API_KEY"))
    parser.add_argument("--votes", type=int, default=1, help="self-consistency 投票次数")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--qps", type=float, default=0.0, help="全局限速（0 表示不限）")
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--max-tokens", type=int, default=600)
    parser.add_argument("--timeout", type=float, default=90.0)
    parser.add_argument("--limit", type=int, default=0, help="只标注前 N 条，用于冒烟")
    parser.add_argument("--account-key", action="append", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--mock", action="store_true", help="不调用网络，用规则基线自检")
    args = parser.parse_args(argv)
    if not args.mock and not args.base_url:
        parser.error("需要 --base-url 或设置 FEDIMISS_LLM_BASE_URL")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
