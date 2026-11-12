"""Misskey HTTP API 统一适配器。

Misskey 的 HTTP API 全部是 ``POST /api/<endpoint>`` + JSON（不是 REST），
匿名即可读取公开数据；令牌放在请求体 JSON 的 ``i`` 字段（本工具只用匿名端点）。

本类把「请求构造 / 分页 / 响应解析 / 错误处理 / 字段映射」收在一处，
上层的 BFS、采集、身份解析都不直接碰 API 细节。

实测要点（2026-10，见 docs/misskey_api_notes.md）：
  * ``users/notes`` 必须传 ``userId``，传 ``username`` 会 400。
  * 分页是游标式：``untilId`` 向更早翻，``limit`` 上限 100，
    取返回列表最后一个元素的 ``id`` 作为下一页游标。
  * ``users/following`` / ``users/followers`` 返回的是**关系对象**
    （``followerId`` / ``followeeId``），部分实例内嵌完整用户对象
    （``followee`` / ``follower``）；部分实例匿名访问返回 400 FORBIDDEN。
  * ``notes/local-timeline`` 部分实例通过 ``disableLocalTimeline`` 关闭。
  * Misskey 不返回 ``x-ratelimit-*`` 响应头，限流只能靠 429 + Retry-After。
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator, List, Optional

from .http import HttpError, RobotsDisallowed

MAX_LIMIT = 100


class ApiForbidden(Exception):
    """该端点在当前实例上匿名不可用（400 FORBIDDEN / 403 / 401）。"""


class ApiUnavailable(Exception):
    """端点不存在或实例未启用该功能（404 / 501 等）。"""


class MisskeyAdapter:
    def __init__(self, host: str, http, store=None, logger=None):
        self.host = host
        self.http = http
        self.store = store
        self.logger = logger
        self.api_base = "https://%s/api" % host

    # ------------------------------------------------------------------ 基础调用
    def call(self, endpoint: str, payload: Dict[str, Any] = None, max_retries: int = None) -> Any:
        url = "%s/%s" % (self.api_base, endpoint)
        try:
            data = self.http.post_json(url, payload or {}, max_retries=max_retries)
        except RobotsDisallowed:
            raise
        except HttpError as exc:
            if self.store:
                self.store.bump_instance_stats(self.host, ok=False, throttled=(exc.status == 429))
                self.store.log_error(
                    stage="api", instance_domain=self.host, subject=endpoint,
                    error_type="http", http_status=exc.status, message=exc.message,
                )
            if exc.status in (400, 401, 403):
                message = exc.message or ""
                if "FORBIDDEN" in message.upper() or "PERMISSION" in message.upper() or exc.status in (401, 403):
                    raise ApiForbidden("%s -> %s" % (endpoint, message))
                raise
            if exc.status in (404, 501):
                raise ApiUnavailable("%s 不可用: %s" % (endpoint, exc.message))
            raise
        if self.store:
            self.store.bump_instance_stats(self.host, ok=True)
        return data

    def try_call(self, endpoint: str, payload: Dict[str, Any] = None, max_retries: int = None):
        """返回 (data, error_kind)；不抛异常，供能力探测使用。"""
        try:
            return self.call(endpoint, payload, max_retries=max_retries), None
        except ApiForbidden:
            return None, "forbidden"
        except ApiUnavailable:
            return None, "unavailable"
        except RobotsDisallowed:
            return None, "robots"
        except HttpError as exc:
            return None, "http_%s" % exc.status
        except Exception as exc:  # noqa: BLE001
            return None, "exception:%s" % exc

    # ------------------------------------------------------------------ 实例
    def meta(self) -> Dict[str, Any]:
        return self.call("meta")

    def stats(self) -> Dict[str, Any]:
        return self.call("stats", max_retries=0)

    # ------------------------------------------------------------------ 用户
    def users_show(
        self,
        username: str = None,
        user_id: str = None,
        host: Optional[str] = None,
        max_retries: int = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        if user_id:
            payload["userId"] = user_id
        else:
            payload["username"] = username
            if host is not None:
                payload["host"] = host
        return self.call("users/show", payload, max_retries=max_retries)

    # ------------------------------------------------------------------ 分页
    def _paginate(
        self,
        endpoint: str,
        payload: Dict[str, Any],
        limit: int,
        max_pages: int,
        cursor_key: str = "untilId",
    ) -> Iterator[Any]:
        """游标分页生成器：``untilId`` = 上一页最后一条的 ``id``。"""
        page_size = max(1, min(int(limit), MAX_LIMIT))
        cursor: Optional[str] = None
        pages = 0
        while pages < max_pages:
            body = dict(payload)
            body["limit"] = page_size
            if cursor:
                body[cursor_key] = cursor
            page = self.call(endpoint, body)
            if not isinstance(page, list) or not page:
                return
            for item in page:
                yield item
            pages += 1
            if len(page) < page_size:
                return
            last = page[-1]
            cursor = last.get("id") if isinstance(last, dict) else None
            if not cursor:
                return

    def users_notes(
        self,
        user_id: str,
        limit: int = 100,
        max_pages: int = 1,
        with_files: bool = True,
        with_replies: bool = False,
    ) -> Iterator[Dict[str, Any]]:
        return self._paginate(
            "users/notes",
            {
                "userId": user_id,
                "withFiles": bool(with_files),
                "withReplies": bool(with_replies),
            },
            limit,
            max_pages,
        )

    def users_following(self, user_id: str, limit: int = 100, max_pages: int = 2) -> Iterator[Dict[str, Any]]:
        return self._paginate("users/following", {"userId": user_id}, limit, max_pages)

    def users_followers(self, user_id: str, limit: int = 100, max_pages: int = 2) -> Iterator[Dict[str, Any]]:
        return self._paginate("users/followers", {"userId": user_id}, limit, max_pages)

    def local_timeline(
        self,
        limit: int = 100,
        max_pages: int = 1,
        with_files: bool = False,
        with_renotes: bool = False,
    ) -> Iterator[Dict[str, Any]]:
        return self._paginate(
            "notes/local-timeline",
            {"withFiles": bool(with_files), "withRenotes": bool(with_renotes)},
            limit,
            max_pages,
        )

    def search_notes(self, query: str, limit: int = 100, max_pages: int = 1) -> Iterator[Dict[str, Any]]:
        """按关键词/话题标签全文搜索公开帖子（返回帖子内嵌 user 对象）。"""
        return self._paginate("notes/search", {"query": query}, limit, max_pages)

    def search_users_by_username(self, username: str, limit: int = 100) -> List[Dict[str, Any]]:
        """按用户名前缀搜索本实例用户（host=None 表示仅本地用户）。"""
        data = self.call(
            "users/search-by-username-and-host",
            {
                "username": username,
                "host": None,
                "limit": max(1, min(int(limit), MAX_LIMIT)),
            },
        )
        return data if isinstance(data, list) else []


# ---------------------------------------------------------------------- 字段映射
def map_account(user: Dict[str, Any]) -> Dict[str, Any]:
    """把 Misskey 用户对象映射成本工具的账号字段。"""
    if not isinstance(user, dict):
        return {}
    return {
        "username": user.get("username"),
        "host": user.get("host"),
        "local_user_id": user.get("id"),
        "account_uri": user.get("uri"),
        "url": user.get("url"),
        "display_name": user.get("name"),
        "description": user.get("description"),
        "created_at": user.get("createdAt"),
        "followers_count": user.get("followersCount"),
        "following_count": user.get("followingCount"),
        "notes_count": user.get("notesCount"),
        "is_bot_declared": 1 if user.get("isBot") else 0,
        "is_locked": 1 if user.get("isLocked") else 0,
        "avatar_url": user.get("avatarUrl"),
        "banner_url": user.get("bannerUrl"),
        "raw": json.dumps(user, ensure_ascii=False)[:40000],
    }


def map_note(note: Dict[str, Any], source_instance: str, author_account_key: str) -> Dict[str, Any]:
    """把 Misskey 帖子对象映射成 notes 表行。"""
    files = note.get("files") if isinstance(note.get("files"), list) else []
    return {
        "note_key": "%s#%s" % (source_instance, note.get("id")),
        "note_id": note.get("id"),
        "source_instance": source_instance,
        "author_account_key": author_account_key,
        "created_at": note.get("createdAt"),
        "text": note.get("text"),
        "cw": note.get("cw"),
        "visibility": note.get("visibility"),
        "lang": note.get("lang"),
        "reply_id": note.get("replyId"),
        "renote_id": note.get("renoteId"),
        "reply_account_key": None,
        "renote_account_key": None,
        "has_media": bool(files),
        "media_count": len(files),
        "files": json.dumps(files, ensure_ascii=False)[:200000] if files else None,
        "collection_time": None,
    }


def map_media_files(note: Dict[str, Any], source_instance: str, author_account_key: str) -> List[Dict[str, Any]]:
    """把帖子里的 ``files[]`` 展开成 media 表行。"""
    from urllib.parse import urlparse

    files = note.get("files") if isinstance(note.get("files"), list) else []
    note_key = "%s#%s" % (source_instance, note.get("id"))
    out: List[Dict[str, Any]] = []
    for item in files:
        if not isinstance(item, dict) or not item.get("id"):
            continue
        url = item.get("url") or ""
        properties = item.get("properties") or {}
        out.append(
            {
                "media_key": "%s#%s" % (note_key, item["id"]),
                "note_key": note_key,
                "note_id": note.get("id"),
                "source_instance": source_instance,
                "author_account_key": author_account_key,
                "file_id": item.get("id"),
                "url": url,
                "thumbnail_url": item.get("thumbnailUrl"),
                "type": item.get("type"),
                "name": item.get("name"),
                "size": item.get("size"),
                "md5": item.get("md5"),
                "blurhash": item.get("blurhash"),
                "is_sensitive": item.get("isSensitive"),
                "width": properties.get("width"),
                "height": properties.get("height"),
                "media_host": urlparse(url).netloc if url else None,
            }
        )
    return out
