"""媒体附件采集（独立模块，对应需求 §9）。

首轮默认 **不下载**（``media.download = false``）：只保留附件元数据。
原因：Misskey 的媒体大多托管在与实例本体不同的 CDN 上
（实测 ``media.misskeyusercontent.jp`` / ``media.misskeyusercontent.com``
的 robots.txt 是 ``Disallow: /``），媒体 host 与实例本体的策略必须分开判定。

下载时（``media.download = true``）：
  * 按「媒体所在 host」单独校验 robots.txt 与 Content-Signal（由 HttpClient 强制）；
  * 按 ``media.types`` 前缀过滤（默认仅 ``image/``）；
  * 单文件上限 ``media.max_bytes``、总量上限 ``media.max_total_bytes``；
  * 每账号最多 ``media.max_files_per_account`` 个附件；
  * 落盘按 sha256 命名去重：``<root>/media/<host>/<前 2 位>/<sha256>.<ext>``；
  * 每个附件独立状态：pending / running / completed / skipped_robots /
    skipped_type / skipped_budget / skipped_disabled / failed，
    失败原因、HTTP 状态与重试次数全部入库，可审计、可续传。
"""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from .http import HttpError, ResponseTooLarge, RobotsDisallowed

# 常见 Content-Type → 扩展名（拿不到文件名后缀时的回退）
_EXT_BY_TYPE = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/avif": ".avif",
    "image/svg+xml": ".svg",
    "video/mp4": ".mp4",
    "video/webm": ".webm",
    "audio/mpeg": ".mp3",
    "audio/ogg": ".ogg",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class MediaCollector:
    """把 media 表中 ``pending`` 的附件按预算、限流、robots 规则下载到本地。"""

    def __init__(self, store, http, robots, config, logger=None, stop_event=None):
        self.store = store
        self.http = http
        self.robots = robots
        self.config = config
        self.logger = logger
        self.stop_event = stop_event
        self.root = os.path.join(config.root, "media")
        self.max_bytes = int(config.media("max_bytes", 8388608) or 8388608)
        self.max_total_bytes = int(config.media("max_total_bytes", 2147483648) or 0)
        self.max_files_per_account = int(config.media("max_files_per_account", 20) or 0)
        self.types = list(config.media("types", ["image/"]) or [])
        self._per_account: Dict[str, int] = {}

    # ------------------------------------------------------------------ 工具
    def _log(self, level: str, message: str, *args) -> None:
        if self.logger:
            getattr(self.logger, level)(message, *args)

    def _stopped(self) -> bool:
        return self.stop_event is not None and self.stop_event.is_set()

    def _used_bytes(self) -> int:
        return int(self.store._scalar("SELECT COALESCE(SUM(downloaded_bytes), 0) FROM media") or 0)

    def _type_allowed(self, media_type: Optional[str]) -> bool:
        if not self.types:
            return True
        value = (media_type or "").lower()
        return any(value.startswith(prefix.lower()) for prefix in self.types)

    def _extension(self, name: Optional[str], content_type: str) -> str:
        if name and "." in name:
            suffix = name.rsplit(".", 1)[-1].strip().lower()
            if 1 <= len(suffix) <= 5 and suffix.isalnum():
                return "." + suffix
        base = (content_type or "").split(";")[0].strip().lower()
        return _EXT_BY_TYPE.get(base, "")

    # ------------------------------------------------------------------ 单条
    def _skip(self, item: Dict[str, Any], status: str, reason: str) -> None:
        self.store.update_media(item["media_key"], download_status=status, error=reason, attempted_at=_now())
        self.store.log_error(
            stage="media", instance_domain=item.get("media_host"), subject=item["media_key"],
            error_type=status, message=reason,
        )

    def _download_one(self, item: Dict[str, Any]) -> str:
        media_key = item["media_key"]
        url = item.get("url")
        if not url:
            self._skip(item, "failed", "附件缺少 url")
            return "failed"
        if not self._type_allowed(item.get("type")):
            self._skip(item, "skipped_type", "类型 %s 不在 media.types 内" % item.get("type"))
            return "skipped_type"

        account_key = item.get("author_account_key") or ""
        if self.max_files_per_account:
            used = self._per_account.get(account_key, 0)
            if used >= self.max_files_per_account:
                self._skip(item, "skipped_budget", "账号附件数达到 max_files_per_account")
                return "skipped_budget"

        if self.max_total_bytes and self._used_bytes() >= self.max_total_bytes:
            self._skip(item, "skipped_budget", "媒体总量达到 max_total_bytes")
            return "skipped_budget"

        # robots.txt 与 Content-Signal：媒体 host 与实例本体分开判定
        if not self.robots.can_fetch(url):
            self._skip(item, "skipped_robots", "robots.txt 禁止抓取该媒体 URL")
            return "skipped_robots"
        if self.config.respect_content_signals and self.config.is_ai_training:
            rules = self.robots.rules_for(url)
            if rules.forbids_ai_training():
                self._skip(item, "skipped_robots", "Content-Signal: ai-train=no")
                return "skipped_robots"

        try:
            body, content_type, _final = self.http.get_bytes(url, max_bytes=self.max_bytes)
        except RobotsDisallowed as exc:
            self._skip(item, "skipped_robots", str(exc))
            return "skipped_robots"
        except ResponseTooLarge:
            self._skip(item, "failed", "文件超过 max_bytes=%d" % self.max_bytes)
            return "failed"
        except HttpError as exc:
            self._skip(item, "failed", "HTTP %s: %s" % (exc.status, exc.message))
            return "failed"
        except Exception as exc:  # noqa: BLE001
            self._skip(item, "failed", repr(exc))
            return "failed"

        if not body:
            self._skip(item, "failed", "空响应体")
            return "failed"

        digest = hashlib.sha256(body).hexdigest()
        host = item.get("media_host") or urlparse(url).netloc or "unknown"
        ext = self._extension(item.get("name"), content_type or item.get("type") or "")
        relative = os.path.join(host, digest[:2], digest + ext)
        target = os.path.join(self.root, relative)

        if not os.path.exists(target):
            os.makedirs(os.path.dirname(target), exist_ok=True)
            tmp = target + ".part"
            with open(tmp, "wb") as handle:
                handle.write(body)
            os.replace(tmp, target)

        self.store.update_media(
            media_key,
            download_status="completed",
            local_path=os.path.relpath(target, self.config.root),
            downloaded_bytes=len(body),
            sha256=digest,
            attempted_at=_now(),
            error=None,
        )
        if account_key:
            self._per_account[account_key] = self._per_account.get(account_key, 0) + 1
        return "completed"

    # ------------------------------------------------------------------ 入口
    def run(self, batch: int = 20) -> Dict[str, int]:
        stats: Dict[str, int] = {}
        if not self.config.media("download", False):
            pending = self.store.count_media("pending")
            self._log("info", "media.download=false，仅保留元数据（pending %d 条不下载）", pending)
            return stats
        self.store.reset_media_running()
        while not self._stopped():
            items = self.store.claim_media(batch)
            if not items:
                break
            for item in items:
                if self._stopped():
                    self.store.update_media(item["media_key"], download_status="pending")
                    continue
                status = self._download_one(item)
                stats[status] = stats.get(status, 0) + 1
            self._log("info", "媒体下载进度 %s", stats)
        return stats


__all__ = ["MediaCollector"]
