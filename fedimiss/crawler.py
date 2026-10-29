"""多实例多模态抓取编排。

流程：
  1. 解析实例 host 的 robots.txt；若 Disallow: /api 则整个实例跳过（合规硬约束）
  2. 若 purpose=ai-training 且该实例 Content-Signal 声明 ai-train=no，则跳过
  3. 抓取实例 meta / stats
  4. 逐个账户：抓取 profile -> 游标分页抓取帖子（含 files 元数据）
  5. 媒体：仅在媒体 host 的 robots.txt 允许时下载 image/*；否则仅保留元数据
  6. 支持断点续爬（已存在的 note id 会被跳过）
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

from .api import MisskeyApi
from .http import HttpClient, HttpError, ResponseTooLarge, RobotsDisallowed
from .robots import RobotsPolicy
from .storage import Storage, now_iso

EXT_BY_TYPE = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/avif": ".avif",
    "image/bmp": ".bmp",
    "image/svg+xml": ".svg",
    "image/tiff": ".tiff",
}


class Crawler:
    def __init__(self, config, storage: Storage, logger, http: HttpClient = None, robots: RobotsPolicy = None):
        self.config = config
        self.storage = storage
        self.logger = logger
        self.robots = robots or RobotsPolicy(
            user_agent=config.user_agent,
            timeout=config.request_timeout,
            logger=logger,
        )
        self.http = http or HttpClient(
            user_agent=config.user_agent,
            robots=self.robots,
            timeout=config.request_timeout,
            min_interval=config.min_interval_seconds,
            max_retries=config.max_retries,
            logger=logger,
        )
        self.stop_requested = False
        self.stats = {"instances_ok": 0, "instances_skipped": 0, "users": 0, "notes": 0, "media": 0}

    # ------------------------------------------------------------------ 入口
    def run(self):
        for inst in self.config.instances:
            if self.stop_requested:
                break
            try:
                self._crawl_instance(inst)
            except Exception as exc:  # noqa: BLE001 - 单实例失败不影响其它实例
                self.logger.exception("实例 %s 抓取失败: %s", inst["host"], exc)
        return self.stats

    # ------------------------------------------------------------------ 实例
    def _crawl_instance(self, inst):
        host = inst["host"]
        api = MisskeyApi(host, self.http)
        api_url = api.api_base

        rules = self.robots.rules_for(api_url)
        compliance = {
            "fetched_at": now_iso(),
            "purpose": self.config.purpose,
            "api_host": host,
            "api_robots_status": rules.status,
            "api_robots_note": rules.note,
            "content_signals": rules.content_signals,
            "crawl_delay": rules.crawl_delay,
            "decisions": [],
        }

        # 1) robots.txt 是否允许 /api
        if not self.robots.can_fetch(api_url):
            compliance["decisions"].append({"action": "skip_instance", "reason": "robots.txt 禁止 /api"})
            self.storage.save_compliance(host, compliance)
            self.stats["instances_skipped"] += 1
            self.logger.warning("[%s] 跳过：robots.txt 禁止访问 /api", host)
            return

        # 2) Content-Signal: ai-train=no
        if self.config.respect_content_signals and self.config.is_ai_training and rules.forbids_ai_training():
            compliance["decisions"].append(
                {"action": "skip_instance", "reason": "Content-Signal: ai-train=no，禁止用于 AI 训练"}
            )
            self.storage.save_compliance(host, compliance)
            self.stats["instances_skipped"] += 1
            self.logger.warning("[%s] 跳过：Content-Signal 声明 ai-train=no", host)
            return

        # 3) 实例元信息
        try:
            meta = api.meta()
        except HttpError as exc:
            compliance["decisions"].append({"action": "skip_instance", "reason": "meta 请求失败: %s" % exc})
            self.storage.save_compliance(host, compliance)
            self.stats["instances_skipped"] += 1
            self.logger.error("[%s] 跳过：获取 meta 失败 %s", host, exc)
            return

        stats = None
        try:
            stats = api.stats()
        except Exception as exc:  # noqa: BLE001 - stats 仅为辅助信息
            self.logger.info("[%s] stats 获取失败（忽略）: %s", host, exc)

        self.storage.save_instance_meta(host, meta, stats)
        version = meta.get("version", "?")
        self.logger.info("[%s] 实例可用，Misskey %s，账户数=%d", host, version, len(inst["users"]))
        compliance["decisions"].append({"action": "crawl_instance", "version": version})
        self.stats["instances_ok"] += 1

        for username in inst["users"]:
            if self.stop_requested:
                break
            try:
                self._crawl_user(api, host, username)
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("[%s@%s] 抓取失败: %s", username, host, exc)
                compliance["decisions"].append({"action": "user_error", "user": username, "reason": str(exc)})

        self.storage.save_compliance(host, compliance)

    # ------------------------------------------------------------------ 账户
    def _crawl_user(self, api, host, username):
        try:
            profile = api.users_show(username=username)
        except HttpError as exc:
            self.logger.error("[%s@%s] 获取 profile 失败: %s", username, host, exc)
            return

        user_id = profile.get("id")
        if not user_id:
            self.logger.error("[%s@%s] profile 缺少 id，跳过", username, host)
            return

        self.storage.save_profile(host, username, profile)
        self.stats["users"] += 1
        self.logger.info(
            "[%s@%s] profile 已保存：name=%s notes=%s followers=%s",
            username, host, profile.get("name"), profile.get("notesCount"), profile.get("followersCount"),
        )

        existing = self.storage.existing_note_ids(host, username)
        media_counter = {"count": 0}
        until_id = None
        fetched = 0
        limit = self.config.notes_per_account
        page_size = self.config.notes_page_size
        since = self.config.since

        while not self.stop_requested and fetched < limit:
            want = min(page_size, limit - fetched)
            try:
                page = api.users_notes(
                    user_id=user_id,
                    limit=want,
                    until_id=until_id,
                    with_files=True,
                    with_replies=self.config.include_replies,
                )
            except HttpError as exc:
                self.logger.error("[%s@%s] 获取帖子失败: %s", username, host, exc)
                break
            if not isinstance(page, list) or not page:
                break

            records = []
            reached_since = False
            for note in page:
                if since and (note.get("createdAt") or "") < since:
                    reached_since = True
                    break
                if note.get("id") in existing:
                    continue
                media = self._process_media(host, username, note, media_counter)
                records.append(
                    {
                        "instance": host,
                        "account": username,
                        "fetched_at": now_iso(),
                        "note": note,
                        "media": media,
                    }
                )
                fetched += 1

            if records:
                self.storage.append_notes(host, username, records)
                self.stats["notes"] += len(records)

            if reached_since or len(page) < want:
                break
            until_id = page[-1].get("id")
            if not until_id:
                break

        self.stats["media"] += media_counter["count"]
        self.logger.info(
            "[%s@%s] 完成：新增帖子 %d，下载媒体 %d（累计媒体上限 %d）",
            username, host, fetched, media_counter["count"], self.config.max_media_per_account,
        )

    # ------------------------------------------------------------------ 媒体
    def _type_allowed(self, mime):
        mime = (mime or "").lower()
        for prefix in self.config.download_media_types:
            if mime.startswith(prefix.lower()):
                return True
        return False

    def _ext(self, file_obj, content_type):
        name = file_obj.get("name") or ""
        ext = os.path.splitext(name)[1]
        if ext:
            return ext
        ctype = (content_type or file_obj.get("type") or "").split(";")[0].strip().lower()
        return EXT_BY_TYPE.get(ctype, ".bin")

    def _process_media(self, host, username, note, counter):
        entries = []
        note_id = note.get("id")
        for file_obj in note.get("files") or []:
            entry = {
                "id": file_obj.get("id"),
                "url": file_obj.get("url"),
                "thumbnailUrl": file_obj.get("thumbnailUrl"),
                "type": file_obj.get("type"),
                "name": file_obj.get("name"),
                "size": file_obj.get("size"),
                "isSensitive": file_obj.get("isSensitive"),
                "blurhash": file_obj.get("blurhash"),
                "local_path": None,
                "skipped": None,
            }
            url = file_obj.get("url")
            if not self.config.download_media:
                entry["skipped"] = "download_disabled"
            elif not self._type_allowed(file_obj.get("type")):
                entry["skipped"] = "type_filtered"
            elif counter["count"] >= self.config.max_media_per_account:
                entry["skipped"] = "account_media_limit"
            elif not url:
                entry["skipped"] = "no_url"
            elif (file_obj.get("size") or 0) > self.config.max_media_bytes:
                entry["skipped"] = "too_large"
            else:
                try:
                    media_rules = self.robots.rules_for(url)
                    if (
                        self.config.respect_content_signals
                        and self.config.is_ai_training
                        and media_rules.forbids_ai_training()
                    ):
                        entry["skipped"] = "content_signal"
                    elif not self.robots.can_fetch(url):
                        entry["skipped"] = "robots"
                    else:
                        data, content_type, _ = self.http.get_bytes(url, max_bytes=self.config.max_media_bytes)
                        filename = "%s-%s%s" % (note_id, file_obj.get("id"), self._ext(file_obj, content_type))
                        path = self.storage.write_media(host, username, filename, data)
                        entry["local_path"] = os.path.relpath(path, self.storage.root)
                        entry["downloaded_bytes"] = len(data)
                        counter["count"] += 1
                except RobotsDisallowed:
                    entry["skipped"] = "robots"
                except ResponseTooLarge:
                    entry["skipped"] = "too_large"
                except HttpError as exc:
                    entry["skipped"] = "http_%s" % exc.status
                    self.logger.warning("媒体下载失败 %s: %s", url, exc)
            entries.append(entry)
        return entries
