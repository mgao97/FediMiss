"""配置加载。

配置分三层：
  * 顶层：身份与合规（user_agent / purpose / respect_content_signals）
  * ``limits``：采集预算（实例数、账号数、BFS 深度、帖子数、请求数）
  * ``rate`` / ``media`` / ``storage`` / ``workers``：执行参数

所有键都有默认值，缺失即用默认。
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

DEFAULTS: Dict[str, Any] = {
    # 身份与合规
    "user_agent": "FediMissBot/1.0 (+https://example.org; research)",
    "contact": "",
    "purpose": "research",
    "respect_content_signals": True,
    # 实例与账号种子
    "seed_instances": [],
    "seed_accounts": [],
    "exclude_instances": [],
    # 采集预算（阶段 A/B/C 通过覆盖这些值实现扩容）
    "limits": {
        "max_instances": 5,
        "max_accounts_total": 5000,
        "max_accounts_per_instance": 2000,
        "max_bfs_depth": 2,
        "max_relations_per_account": 200,
        "notes_per_account": 40,
        "notes_page_size": 100,
        "max_notes_total": 200000,
        "max_requests_per_instance": 20000,
        # 候选实例上限（种子 + peer 扩展），避免实例校验无界扩散
        "max_candidates": 40,
        # users/following|followers 连续 N 个账号返回 400 FORBIDDEN 才判定为「实例级限制」
        # 实测该错误多数来自个别账号的隐私设置，不应当作实例能力缓存
        "relation_forbidden_threshold": 10,
        # 单实例单轮 BFS 处理的账号上限。BFS 不排空就不进入资料/帖子阶段，
        # 会导致帖子采集被无限推迟；设置上限后每轮都能产出完整快照，剩余账号留给下一轮续采。
        "max_bfs_accounts_per_instance": 500,
    },
    "sampling": {
        "strategy": "mhrw",
        "phase": "relations",
        "mhrw_steps_per_instance": 3000,
        "seed": None,
    },
    # 速率与重试
    "rate": {
        "min_interval_seconds": 1.2,
        "request_timeout": 30,
        "max_retries": 3,
        "breaker_seconds": 300,
    },
    # 媒体
    "media": {
        "download": False,
        "types": ["image/"],
        "max_bytes": 8388608,
        "max_files_per_account": 20,
        "max_total_bytes": 2147483648,
    },
    # 存储
    "storage": {"root": "data", "keep_raw": True},
    # 时间窗口与快照（未填写 longitudinal.start_date 时不得启动正式纵向采集）
    "time_config": {
        "historical": {
            "enabled": True,
            "start": "2024-10-25T00:00:00Z",
            "end": "2024-10-31T00:00:00Z",
        },
        "longitudinal": {
            "enabled": False,
            "start_date": None,
            "days": 90,
            "window_days": 30,
        },
        "snapshots": {
            "account_interval_days": 30,
            "relationship_interval_days": 30,
            "instance_interval_days": 7,
        },
    },
    # 并发：不同 host 并行，同 host 串行（速率控制）
    "workers": {"instance_workers": 4, "account_batch_size": 50},
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


class Config:
    """点号访问的配置容器。"""

    def __init__(self, data: Dict[str, Any] = None):
        self.data = _deep_merge(DEFAULTS, data or {})
        self._normalize()

    # ------------------------------------------------------------------ 规范化
    def _normalize(self) -> None:
        self.data["seed_instances"] = _clean_hosts(self.data.get("seed_instances") or [])
        self.data["exclude_instances"] = _clean_hosts(self.data.get("exclude_instances") or [])
        seeds = []
        for item in self.data.get("seed_accounts") or []:
            acct = str(item).strip().lstrip("@").lower()
            if "@" in acct:
                seeds.append(acct)
        self.data["seed_accounts"] = seeds

    # ------------------------------------------------------------------ 访问
    def get(self, path: str, default: Any = None) -> Any:
        node: Any = self.data
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    @property
    def user_agent(self) -> str:
        ua = self.data["user_agent"]
        contact = (self.data.get("contact") or "").strip()
        if contact and contact not in ua:
            ua = "%s; contact: %s" % (ua, contact)
        return ua

    @property
    def root(self) -> str:
        return os.path.abspath(self.data["storage"]["root"])

    @property
    def db_path(self) -> str:
        return os.path.join(self.root, "fedimiss.sqlite3")

    @property
    def purpose(self) -> str:
        return str(self.data.get("purpose") or "research").lower()

    @property
    def is_ai_training(self) -> bool:
        return self.purpose in ("ai-training", "ai_train", "training")

    @property
    def respect_content_signals(self) -> bool:
        return bool(self.data.get("respect_content_signals", True))

    def limit(self, name: str) -> int:
        return int(self.data["limits"].get(name, 0))

    def rate(self, name: str, default: float = 0.0) -> float:
        return float(self.data["rate"].get(name, default))

    def media(self, name: str, default: Any = None) -> Any:
        return self.data["media"].get(name, default)

    def summary(self) -> Dict[str, Any]:
        return {
            "user_agent": self.user_agent,
            "purpose": self.purpose,
            "respect_content_signals": self.respect_content_signals,
            "seed_instances": len(self.data["seed_instances"]),
            "seed_accounts": len(self.data["seed_accounts"]),
            "limits": dict(self.data["limits"]),
            "rate": dict(self.data["rate"]),
            "media_download": bool(self.data["media"]["download"]),
            "storage_root": self.root,
        }


def _clean_hosts(items: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        host = str(item).strip().lower()
        if host.startswith("https://"):
            host = host[8:]
        elif host.startswith("http://"):
            host = host[7:]
        host = host.strip("/")
        if host and host not in seen:
            seen.add(host)
            out.append(host)
    return out


def load(path: str = None) -> Config:
    data: Dict[str, Any] = {}
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    return Config(data)
