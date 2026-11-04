"""跨实例账号身份与归属解析。

核心约束（需求 §6）：**不能把「发现账号的实例」当成「账号的原属实例」**。

Misskey 用户对象的可用证据：
  * ``host``  —— 为空表示该账号就是返回这个对象的实例的**本地账号**；
                 非空则是该账号的**原属实例域名**（远端缓存视图）。
  * ``url``   —— 形如 ``https://<home>/@<username>``，可用于推出原属实例。
  * ``uri``   —— ActivityPub 对象 ID，形如 ``https://<home>/users/<id>``，
                 可推出原属实例，并**推断**本地 ID（不保证等同 Misskey 的 aidx id）。
  * ``id``    —— **仅在返回该对象的实例上有意义**，不等于原属实例的本地 ID。

置信度：
  verified   在原属实例上查到该账号且 ``host`` 为空 → 本地 ID 可信
  declared   来自远端缓存对象的 ``host`` 字段（可确认原属实例，但本地 ID 未知）
  inferred   仅由 ``uri`` 推断（原属实例与本地 ID 都是推断值）
  unresolved 无法可靠判定，宁可留空也不猜
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

from .api import ApiForbidden, ApiUnavailable, MisskeyAdapter


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_acct(raw: str, default_host: str = None) -> Optional[Tuple[str, str]]:
    """解析 ``user@host`` / ``@user@host`` / ``user``（配合 default_host）。"""
    if not raw:
        return None
    text = str(raw).strip().lstrip("@")
    if "@" in text:
        username, host = text.split("@", 1)
    else:
        username, host = text, (default_host or "")
    username = username.strip()
    host = host.strip().lower()
    if not username or not host:
        return None
    return username, host


def home_of(user: Dict[str, Any], source_instance: str) -> Tuple[Optional[str], str]:
    """判断账号原属实例，返回 (home_host, status)。

    status ∈ {local, declared_from_host, inferred_from_url, unknown}
    """
    if not isinstance(user, dict):
        return None, "unknown"
    host = user.get("host")
    if host is None and user.get("username"):
        # 返回该对象的实例上 host 为空 → 这是该实例的本地账号
        return source_instance, "local"
    if host:
        return str(host).lower(), "declared_from_host"
    url = user.get("url") or ""
    parsed = urlparse(url)
    if parsed.netloc and "/@" in parsed.path:
        return parsed.netloc.lower(), "inferred_from_url"
    return None, "unknown"


def infer_local_id_from_uri(uri: str) -> Optional[str]:
    """从 ``https://host/users/<id>`` 推断本地 ID（仅作推断证据）。"""
    if not uri:
        return None
    parsed = urlparse(uri)
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) == 2 and parts[0] == "users":
        return parts[1]
    return None


def account_key(username: str, host: str) -> str:
    """规范化账号键：``username@host``（host 小写）。"""
    return "%s@%s" % (username.strip(), (host or "").strip().lower())


class IdentityResolver:
    """把从任意实例看到的用户对象，登记为「归属明确」的账号记录。"""

    def __init__(self, store, http, robots, config, logger=None):
        self.store = store
        self.http = http
        self.robots = robots
        self.config = config
        self.logger = logger
        self._adapters: Dict[str, MisskeyAdapter] = {}

    # ------------------------------------------------------------------ 工具
    def adapter(self, host: str) -> MisskeyAdapter:
        if host not in self._adapters:
            self._adapters[host] = MisskeyAdapter(host, self.http, self.store, self.logger)
        return self._adapters[host]

    def _host_allowed(self, host: str) -> bool:
        """该实例的 /api 是否被 robots.txt 允许。"""
        return self.robots.can_fetch("https://%s/api/meta" % host)

    def _log(self, level: str, message: str, *args) -> None:
        if self.logger:
            getattr(self.logger, level)(message, *args)

    # ------------------------------------------------------------------ 登记
    def register(
        self,
        user: Dict[str, Any],
        source_instance: str,
        method: str,
        depth: int = 0,
    ) -> Optional[str]:
        """登记一个用户对象，返回 account_key。已存在则补齐缺失字段。"""
        if not isinstance(user, dict):
            return None
        username = user.get("username")
        if not username:
            return None
        home, status = home_of(user, source_instance)
        if not home:
            # 无法判定归属 → 记入 unresolved，但不建账号（避免污染账号表）
            raw_id = "%s@%s" % (username, source_instance)
            self.store.upsert_mapping(
                raw_id,
                raw_identifier=raw_id,
                normalized_identifier=raw_id.lower(),
                source_instance=source_instance,
                resolved_username=username,
                resolved_host=None,
                resolution_method="none",
                resolution_evidence=json.dumps(
                    {"reason": "无 host / url 证据", "source_instance": source_instance},
                    ensure_ascii=False,
                ),
                confidence="unresolved",
                resolved_at=_now(),
                failure_reason="无法确定原属实例",
            )
            return None

        key = account_key(username, home)
        local_id: Optional[str] = None
        confidence = "declared"

        if status == "local":
            # 对象就是在原属实例上拿到的 → id 即原属实例本地 ID
            local_id = user.get("id")
            confidence = "verified"
        else:
            local_id = infer_local_id_from_uri(user.get("uri") or "")
            confidence = "inferred" if local_id else "declared"

        fields = {
            "username": username,
            "host": None if status == "local" else home,
            "instance_domain": home,
            "local_user_id": local_id,
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
            "discovery_instance": source_instance,
            "discovery_method": method,
            "discovered_at": _now(),
            "bfs_depth": depth,
            "home_instance_status": status,
            "raw": json.dumps(user, ensure_ascii=False)[:40000],
        }

        inserted = self.store.insert_account_if_absent(key, **fields)
        if not inserted:
            # 只补缺失字段，不覆盖已有更可信的值
            existing = self.store.get_account(key) or {}
            patch = {}
            for field, value in fields.items():
                if field in ("discovered_at", "discovery_instance", "discovery_method", "bfs_depth",
                             "home_instance_status", "local_user_id"):
                    continue
                if existing.get(field) in (None, "", 0) and value not in (None, "", 0):
                    patch[field] = value
            if existing.get("local_user_id") in (None, "") and local_id:
                patch["local_user_id"] = local_id
            if patch:
                self.store.upsert_account(key, **patch)
            if (existing.get("home_instance_status") in (None, "unknown")) and status:
                self.store.upsert_account(key, home_instance_status=status)

        self.store.upsert_mapping(
            key,
            raw_identifier="%s@%s" % (username, source_instance),
            normalized_identifier=key.lower(),
            source_instance=source_instance,
            resolved_username=username,
            resolved_host=home,
            resolved_local_user_id=local_id,
            resolution_method=method,
            resolution_evidence=json.dumps(
                {
                    "home_status": status,
                    "source_instance": source_instance,
                    "user_id_on_source": user.get("id"),
                    "uri": user.get("uri"),
                    "url": user.get("url"),
                },
                ensure_ascii=False,
            ),
            confidence=confidence,
            resolved_at=_now(),
        )
        return key

    # ------------------------------------------------------------------ 深解析
    def resolve_home_identity(self, key: str) -> Dict[str, Any]:
        """向**原属实例**发 ``users/show``，拿到可信的本地 ID。

        返回 {"confidence", "local_user_id", "home_host"}。
        失败时保持 unresolved/declared，不猜测。
        """
        account = self.store.get_account(key)
        if not account:
            return {"confidence": "unresolved", "local_user_id": None, "home_host": None}

        username = account.get("username")
        home = account.get("instance_domain")
        if not username or not home:
            return {"confidence": "unresolved", "local_user_id": None, "home_host": home}

        if not self._host_allowed(home):
            self.store.upsert_mapping(
                key,
                resolution_method="home_api",
                confidence="declared",
                resolved_at=_now(),
                failure_reason="原属实例 robots.txt 禁止 /api",
            )
            return {"confidence": "declared", "local_user_id": account.get("local_user_id"),
                    "home_host": home}

        try:
            user = self.adapter(home).users_show(username=username, host=None)
        except ApiForbidden:
            self._mark(key, "declared", "原属实例匿名访问 users/show 被拒绝")
            return {"confidence": "declared", "local_user_id": account.get("local_user_id"),
                    "home_host": home}
        except ApiUnavailable:
            self._mark(key, "declared", "原属实例无 users/show")
            return {"confidence": "declared", "local_user_id": account.get("local_user_id"),
                    "home_host": home}
        except Exception as exc:  # noqa: BLE001
            self.store.log_error(
                stage="identity", instance_domain=home, subject=key,
                error_type="resolve_failed", message=str(exc),
            )
            self._mark(key, "declared", "解析异常：%s" % exc)
            return {"confidence": "declared", "local_user_id": account.get("local_user_id"),
                    "home_host": home}

        if not isinstance(user, dict) or user.get("host") is not None:
            # 原属实例返回的是远端副本 → 说明我们的归属判断有误，不猜
            self._mark(key, "unresolved", "原属实例返回的是远端副本，未确认本地账号")
            return {"confidence": "unresolved", "local_user_id": None, "home_host": home}

        local_id = user.get("id")
        self.store.upsert_account(
            key,
            local_user_id=local_id,
            account_uri=user.get("uri"),
            url=user.get("url"),
            home_instance_status="local",
            raw=json.dumps(user, ensure_ascii=False)[:40000],
        )
        self.store.upsert_mapping(
            key,
            resolved_local_user_id=local_id,
            resolved_host=home,
            resolution_method="home_api_verified",
            resolution_evidence=json.dumps(
                {"verified_on": home, "returned_id": local_id, "returned_host": None},
                ensure_ascii=False,
            ),
            confidence="verified",
            resolved_at=_now(),
            failure_reason=None,
        )
        return {"confidence": "verified", "local_user_id": local_id, "home_host": home}

    def _mark(self, key: str, confidence: str, reason: str) -> None:
        self.store.upsert_mapping(
            key,
            resolution_method="home_api",
            confidence=confidence,
            resolved_at=_now(),
            failure_reason=reason,
        )
