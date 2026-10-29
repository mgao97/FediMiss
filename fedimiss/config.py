"""配置加载与校验。"""

from __future__ import annotations

import copy
import json

DEFAULTS = {
    # 机器人标识：请务必替换为真实可联系的 UA
    "user_agent": "FediMissBot/1.0 (+https://github.com/your-org/FediMiss; research; contact: you@example.org)",
    # 数据用途：research | ai-training
    #   ai-training 时，若目标 host 的 Content-Signal 声明 ai-train=no，将跳过该 host 的内容
    "purpose": "research",
    "respect_content_signals": True,
    # 媒体：仅抓取 image/*，遵守媒体 host 的 robots.txt / Content-Signal
    "download_media": True,
    "download_media_types": ["image/"],
    "max_media_bytes": 8 * 1024 * 1024,
    "max_media_per_account": 200,
    # 帖子
    "notes_per_account": 200,
    "notes_page_size": 100,
    "include_replies": False,
    # 仅抓取该时间之后的帖子（ISO8601，例如 "2026-01-01T00:00:00Z"），留空表示不限制
    "since": None,
    # 限速
    "min_interval_seconds": 1.5,
    "request_timeout": 30,
    "max_retries": 4,
    # 目标：[{"host": "misskey.io", "users": ["syuilo"]}]
    "instances": [],
}

_TRAINING_PURPOSES = {"ai-training", "ai_train", "training", "train"}


class Config:
    def __init__(self, data: dict):
        merged = copy.deepcopy(DEFAULTS)
        merged.update({k: v for k, v in (data or {}).items() if v is not None})
        for key, value in merged.items():
            setattr(self, key, value)
        self.purpose = str(self.purpose).lower()
        self.instances = self._normalize_instances(self.instances)

    @staticmethod
    def _normalize_instances(instances):
        out = []
        for item in instances or []:
            host = str(item.get("host", "")).strip()
            if not host:
                continue
            host = host.replace("https://", "").replace("http://", "").strip("/")
            users = [str(u).lstrip("@").strip() for u in (item.get("users") or []) if str(u).strip()]
            out.append({"host": host, "users": users, "label": item.get("label", host)})
        return out

    @property
    def is_ai_training(self) -> bool:
        return self.purpose in _TRAINING_PURPOSES

    @classmethod
    def load(cls, path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as fh:
            return cls(json.load(fh))

    def summary(self) -> dict:
        return {
            "purpose": self.purpose,
            "download_media": self.download_media,
            "notes_per_account": self.notes_per_account,
            "min_interval_seconds": self.min_interval_seconds,
            "instances": ["%s(%d users)" % (i["host"], len(i["users"])) for i in self.instances],
        }
