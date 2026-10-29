"""Misskey HTTP API 封装。

Misskey 的 HTTP API 全部为 POST + JSON（并非 REST），匿名即可访问公开数据：
  * POST /api/meta          实例元信息（含软件版本）
  * POST /api/stats         实例统计
  * POST /api/users/show    用户 profile（username 或 userId）
  * POST /api/users/notes   用户帖子列表（userId 必填，支持 untilId 游标分页）
  * POST /api/notes/show    单条帖子
权限不足或参数错误时服务端返回 {"error": {...}}，由 HttpClient 抛出 HttpError。
"""

from __future__ import annotations


class MisskeyApi:
    def __init__(self, host: str, http):
        self.host = host
        self.http = http
        self.api_base = "https://%s/api" % host

    def call(self, endpoint: str, payload: dict = None, max_retries: int = None):
        return self.http.post_json(
            "%s/%s" % (self.api_base, endpoint), payload or {}, max_retries=max_retries
        )

    # ------------------------------------------------------------------ 实例
    def meta(self) -> dict:
        return self.call("meta")

    def stats(self) -> dict:
        # stats 属于可选的辅助信息，失败不阻塞，也不做重试
        return self.call("stats", max_retries=0)

    # ------------------------------------------------------------------ 用户
    def users_show(self, username: str = None, user_id: str = None, host: str = None) -> dict:
        payload = {}
        if user_id:
            payload["userId"] = user_id
        else:
            payload["username"] = username
            if host:
                payload["host"] = host
        return self.call("users/show", payload)

    def users_notes(
        self,
        user_id: str,
        limit: int = 100,
        until_id: str = None,
        since_id: str = None,
        with_files: bool = True,
        with_replies: bool = False,
    ):
        payload = {
            "userId": user_id,
            "limit": max(1, min(int(limit), 100)),
            "withFiles": bool(with_files),
            "withReplies": bool(with_replies),
        }
        if until_id:
            payload["untilId"] = until_id
        if since_id:
            payload["sinceId"] = since_id
        return self.call("users/notes", payload)

    # ------------------------------------------------------------------ 帖子
    def notes_show(self, note_id: str) -> dict:
        return self.call("notes/show", {"noteId": note_id})
