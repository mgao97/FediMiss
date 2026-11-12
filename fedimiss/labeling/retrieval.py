"""Retrieval-based few-shot：从人工金标中检索相似案例，注入动态 prompt。

纯标准库实现（项目约定：无 pip 依赖）。检索池只允许人工金标数据：

- 文本侧：``display_name + description + 帖子样本`` 的字符 2/3-gram TF-IDF 余弦相似度
  （对日文友好，无需分词器）；
- 行为侧：duplicate_rate / gap_cv / top_text_share 等统计特征，
  用 median/IQR 稳健标准化后算逐维距离（对长尾不敏感）；
- 混合分：``w_text * 文本余弦 + (1-w_text) * 行为相似度``。

防泄漏：检索时永久排除查询账号自身（leave-one-out），金标 400 上的
验收指标不会因为"检索到自己"而虚高。

用法::

    from .retrieval import FewShotRetriever
    r = FewShotRetriever("data/labeling/evidence_gold400.jsonl",
                         "data/labeling/gold.jsonl",
                         "data/labeling/gold_sheet_400.csv")
    blocks, keys = r.examples_for(query_pack)
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

# --------------------------------------------------------------------------- #
# 文本表示：字符 n-gram TF-IDF
# --------------------------------------------------------------------------- #

_WS = re.compile(r"\s+")


def _char_ngrams(text: str, sizes: Sequence[int] = (2, 3)) -> Counter:
    text = _WS.sub("", (text or "").lower())
    grams: Counter = Counter()
    for size in sizes:
        for i in range(max(0, len(text) - size + 1)):
            grams[text[i:i + size]] += 1
    return grams


class TfidfIndex:
    """稀疏 TF-IDF 索引，支持余弦检索。规模（数百文档）下纯 dict 足够。"""

    def __init__(self, sizes: Sequence[int] = (2, 3)) -> None:
        self.sizes = tuple(sizes)
        self.idf: Dict[str, float] = {}
        self.vectors: Dict[str, Dict[str, float]] = {}

    def add_documents(self, keys: List[str], texts: List[str]) -> None:
        n_docs = len(keys)
        df: Counter = Counter()
        counts: Dict[str, Counter] = {}
        for key, text in zip(keys, texts):
            grams = _char_ngrams(text, self.sizes)
            counts[key] = grams
            df.update(grams.keys())
        self.idf = {g: math.log((n_docs + 1) / (c + 1)) + 1.0 for g, c in df.items()}
        for key, grams in counts.items():
            vec: Dict[str, float] = {}
            for gram, tf in grams.items():
                weight = (1.0 + math.log(tf)) * self.idf.get(gram, 0.0)
                if weight > 0:
                    vec[gram] = weight
            norm = math.sqrt(sum(w * w for w in vec.values())) or 1.0
            self.vectors[key] = {g: w / norm for g, w in vec.items()}

    def search(self, text: str, k: int, exclude: Sequence[str] = ()) -> List[Tuple[str, float]]:
        query = _char_ngrams(text, self.sizes)
        qvec: Dict[str, float] = {}
        for gram, tf in query.items():
            idf = self.idf.get(gram)
            if idf is None:
                continue
            qvec[gram] = (1.0 + math.log(tf)) * idf
        qnorm = math.sqrt(sum(w * w for w in qvec.values())) or 1.0
        qvec = {g: w / qnorm for g, w in qvec.items()}
        blocked = set(exclude)
        scored: List[Tuple[str, float]] = []
        for key, dvec in self.vectors.items():
            if key in blocked:
                continue
            small, large = (qvec, dvec) if len(qvec) <= len(dvec) else (dvec, qvec)
            dot = sum(w * large.get(g, 0.0) for g, w in small.items())
            scored.append((key, dot))
        scored.sort(key=lambda item: -item[1])
        return scored[:k]


# --------------------------------------------------------------------------- #
# 行为统计特征：稳健标准化 + 逐维距离
# --------------------------------------------------------------------------- #

_NUMERIC_FIELDS = (
    ("content", "duplicate_rate"),
    ("content", "top_text_share"),
    ("timing", "gap_cv"),
    ("timing", "modal_interval_share"),
    ("timing", "hour_entropy"),
    ("interaction", "reply_rate"),
    ("media", "media_rate"),
)


def _feature_vector(pack: Dict[str, Any]) -> List[float]:
    values: List[float] = []
    collection = pack.get("collection", {}) or {}
    values.append(math.log1p(float(collection.get("notes_collected") or 0)))
    values.append(math.log1p(max(0.0, float(collection.get("span_days") or 0))))
    for section, field in _NUMERIC_FIELDS:
        raw = (pack.get(section, {}) or {}).get(field)
        values.append(float(raw) if raw is not None else float("nan"))
    return values


def _median(sorted_values: List[float]) -> float:
    n = len(sorted_values)
    mid = n // 2
    if n % 2:
        return sorted_values[mid]
    return (sorted_values[mid - 1] + sorted_values[mid]) / 2.0


class NumericScaler:
    """median/IQR 稳健标准化；缺失值映射到中位数（scaled=0）。"""

    def __init__(self) -> None:
        self.medians: List[float] = []
       	self.iqrs: List[float] = []

    def fit(self, vectors: List[List[float]]) -> None:
        n_dims = max(len(v) for v in vectors)
        self.medians, self.iqrs = [], []
        for dim in range(n_dims):
            valid = sorted(v[dim] for v in vectors if dim < len(v)
                           and not math.isnan(v[dim]))
            if not valid:
                self.medians.append(0.0)
                self.iqrs.append(1.0)
                continue
            med = _median(valid)
            q1 = _median(valid[: max(1, len(valid) // 2)])
            q3 = _median(valid[len(valid) // 2:])
            iqr = (q3 - q1) or (max(valid) - min(valid)) or 1.0
            self.medians.append(med)
            self.iqrs.append(iqr if iqr > 0 else 1.0)

    def transform(self, vector: List[float]) -> List[float]:
        scaled: List[float] = []
        for dim, value in enumerate(vector):
            if dim >= len(self.medians) or math.isnan(value):
                scaled.append(0.0)
                continue
            z = (value - self.medians[dim]) / self.iqrs[dim]
            scaled.append(max(-3.0, min(3.0, z)) / 3.0)  # 截断到 [-1, 1]
        return scaled


def _numeric_similarity(a: List[float], b: List[float]) -> float:
    if not a:
        return 0.0
    total = sum(abs(x - y) for x, y in zip(a, b)) / len(a)
    return max(0.0, 1.0 - total / 2.0)


# --------------------------------------------------------------------------- #
# 检索器：混合相似度 + 案例渲染
# --------------------------------------------------------------------------- #

def _clip(text: Optional[str], limit: int) -> str:
    text = _WS.sub(" ", (text or "").strip())
    return text[:limit] + ("…" if len(text) > limit else "")


class FewShotRetriever:
    def __init__(self, packs_path: str, labels_path: str, sheet_path: str,
                 k_bot: int = 2, k_human: int = 2,
                 w_text: float = 0.65) -> None:
        self.k_bot = k_bot
        self.k_human = k_human
        self.w_text = w_text

        packs: Dict[str, Dict[str, Any]] = {}
        with open(packs_path, "r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    pack = json.loads(line)
                    packs[pack["account_key"]] = pack
        labels: Dict[str, str] = {}
        with open(labels_path, "r", encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    record = json.loads(line)
                    labels[record["account_key"]] = record["label"]
        self.notes = self._load_notes(sheet_path)

        # 只保留有人工标签的账号（不许模型预测标签混入）
        self.keys = [k for k in packs if k in labels]
        self.packs = {k: packs[k] for k in self.keys}
        self.labels = {k: labels[k] for k in self.keys}

        index = TfidfIndex()
        index.add_documents(self.keys,
                            [self._doc_text(self.packs[k]) for k in self.keys])
        self.index = index

        scaler = NumericScaler()
        scaler.fit([_feature_vector(self.packs[k]) for k in self.keys])
        self.scaler = scaler
        self.numeric = {k: scaler.transform(_feature_vector(self.packs[k]))
                        for k in self.keys}

    @staticmethod
    def _doc_text(pack: Dict[str, Any]) -> str:
        profile = pack.get("profile", {}) or {}
        parts = [profile.get("display_name") or "", profile.get("description") or ""]
        for sample in pack.get("samples", []) or []:
            parts.append(sample.get("text") or "")
        return "\n".join(parts)

    @staticmethod
    def _load_notes(sheet_path: str) -> Dict[str, str]:
        """从金标工作表取人工判定理由（notes_a / notes_b / notes）。"""
        notes: Dict[str, str] = {}
        try:
            with open(sheet_path, "r", encoding="utf-8-sig") as fh:
                for row in csv.DictReader(fh):
                    key = (row.get("account_key") or "").strip()
                    if not key:
                        continue
                    parts = []
                    for col in ("notes_a", "notes_b"):
                        value = _clip(row.get(col), 110)
                        if value:
                            parts.append(value)
                    extra = _clip(row.get("notes"), 110)
                    if extra:
                        parts.append(extra)
                    if parts:
                        notes[key] = " ｜ ".join(parts)
        except FileNotFoundError:
            pass
        return notes

    def _score(self, query_pack: Dict[str, Any], exclude: Sequence[str]) -> List[Tuple[str, float]]:
        text_scores = dict(self.index.search(self._doc_text(query_pack), len(self.keys), exclude))
        query_numeric = self.scaler.transform(_feature_vector(query_pack))
        scored: List[Tuple[str, float]] = []
        for key in self.keys:
            if key in set(exclude):
                continue
            hybrid = (self.w_text * text_scores.get(key, 0.0)
                      + (1.0 - self.w_text) * _numeric_similarity(query_numeric, self.numeric[key]))
            scored.append((key, hybrid))
        scored.sort(key=lambda item: -item[1])
        return scored

    def retrieve(self, query_pack: Dict[str, Any],
                 k_bot: Optional[int] = None,
                 k_human: Optional[int] = None) -> List[Tuple[str, str, float]]:
        """返回 [(account_key, 人工标签, 混合相似度)]，按类内相似度降序。

        永久排除查询账号自身（leave-one-out），防止金标自检时标签泄漏。
        """
        scored = self._score(query_pack, exclude=[query_pack.get("account_key", "")])
        picked: List[Tuple[str, str, float]] = []
        n_bot, n_human = 0, 0
        for key, score in scored:
            label = self.labels[key]
            if label == "bot" and n_bot < (self.k_bot if k_bot is None else k_bot):
                picked.append((key, label, score))
                n_bot += 1
            elif label == "human" and n_human < (self.k_human if k_human is None else k_human):
                picked.append((key, label, score))
                n_human += 1
            if n_bot >= (self.k_bot if k_bot is None else k_bot) and \
               n_human >= (self.k_human if k_human is None else k_human):
                break
        return picked

    def render_example(self, key: str, label: str, score: float) -> str:
        pack = self.packs[key]
        profile = pack.get("profile", {}) or {}
        collection = pack.get("collection", {}) or {}
        content = pack.get("content", {}) or {}
        timing = pack.get("timing", {}) or {}
        samples = pack.get("samples", []) or []
        lines = [
            "【案例 %s】人工标签: %s（相似度 %.2f）" % (key, label, score),
            "人工理由: %s" % (self.notes.get(key) or "（工作表未填理由）"),
            "简介: %s｜%s" % (_clip(profile.get("display_name"), 40),
                              _clip(profile.get("description"), 110)),
            "帖子样本: %s" % "；".join(_clip(s.get("text"), 70) for s in samples[:2]),
            "统计: notes_collected=%s duplicate_rate=%s top_text_share=%s gap_cv=%s" % (
                collection.get("notes_collected"), content.get("duplicate_rate"),
                content.get("top_text_share"), timing.get("gap_cv")),
        ]
        return "\n".join(lines)

    def examples_for(self, query_pack: Dict[str, Any]) -> Tuple[str, List[str]]:
        """渲染动态 few-shot 块，返回 (prompt片段, 引用的案例 key 列表)。"""
        picked = self.retrieve(query_pack)
        if not picked:
            return "（检索池中无相似案例）", []
        blocks = [self.render_example(key, label, score) for key, label, score in picked]
        return "\n\n".join(blocks), [key for key, _, _ in picked]
