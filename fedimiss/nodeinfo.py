"""实例发现与校验。

三个可核验的公开来源：

1. **NodeInfo**（权威）：``https://<host>/.well-known/nodeinfo`` 给出
   ``/nodeinfo/2.1`` 或 ``/nodeinfo/2.0`` 的地址，内容是软件名/版本、
   注册开放状态、用户数、本地帖数，以及 Misskey 特有的
   ``metadata.disableLocalTimeline`` / ``disableGlobalTimeline``。
2. **实例自身声明的 peer 列表**：``/api/v1/instance/peers``
   （部分 Misskey 实例为兼容而暴露）返回邻居域名数组，可用于扩展候选实例。
3. **实例 API 自检**：``POST /api/meta``、``POST /api/notes/local-timeline``
   验证匿名可用性与时间线开关。

所有请求都走 :class:`fedimiss.http.HttpClient`，因此天然受 robots.txt 约束。
合规判定顺序：先 robots.txt 是否允许 ``/api``，再看 Content-Signal。
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple

from .http import HttpError, RobotsDisallowed

# 与 Misskey 同源、可确认兼容的软件名
MISSKEY_FAMILY = {
    "misskey",
    "sharkey",
    "firefish",
    "calckey",
    "foundkey",
    "meisskey",
    "cherrypick",
    "nekomisskey",
    "misskey-tempura",
}

NODEINFO_SCHEMAS = (
    "http://nodeinfo.diaspora.software/ns/schema/2.1",
    "http://nodeinfo.diaspora.software/ns/schema/2.0",
)


def is_misskey_family(software_name: str) -> bool:
    name = (software_name or "").strip().lower()
    if not name:
        return False
    if name in MISSKEY_FAMILY:
        return True
    return "misskey" in name


class InstanceProbe:
    """对一个候选实例做完整校验，产出可直接入库的记录。"""

    def __init__(self, http, robots, logger=None):
        self.http = http
        self.robots = robots
        self.logger = logger

    # ------------------------------------------------------------------ 工具
    def _get_json(self, url: str, timeout: bool = False) -> Tuple[Optional[int], Any]:
        """GET 并解析 JSON；robots 禁止或网络失败返回 (None, None)。"""
        try:
            status, _headers, raw = self.http.request("GET", url)
        except RobotsDisallowed:
            self._log("info", "%s 被 robots.txt 禁止", url)
            return None, None
        except HttpError as exc:
            return exc.status, None
        if not raw:
            return status, None
        try:
            return status, json.loads(raw.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            return status, None

    def _post(self, host: str, endpoint: str, payload: Dict[str, Any] = None):
        """返回 (ok, status, data_or_error)。"""
        url = "https://%s/api/%s" % (host, endpoint)
        try:
            data = self.http.post_json(url, payload or {}, max_retries=1)
            return True, 200, data
        except RobotsDisallowed:
            return False, -2, "robots_disallowed"
        except HttpError as exc:
            return False, exc.status, exc.message
        except Exception as exc:  # noqa: BLE001
            return False, -1, repr(exc)

    def _log(self, level: str, message: str, *args) -> None:
        if self.logger:
            getattr(self.logger, level)(message, *args)

    # ------------------------------------------------------------------ NodeInfo
    def fetch_nodeinfo(self, host: str) -> Optional[Dict[str, Any]]:
        _status, well_known = self._get_json("https://%s/.well-known/nodeinfo" % host)
        if not isinstance(well_known, dict):
            return None
        links = well_known.get("links") or []
        target = None
        for schema in NODEINFO_SCHEMAS:
            for link in links:
                if link.get("rel") == schema and link.get("href"):
                    target = link["href"]
                    break
            if target:
                break
        if not target:
            return None
        _status, payload = self._get_json(target)
        if not isinstance(payload, dict):
            return None
        payload["_nodeinfo_url"] = target
        return payload

    # ------------------------------------------------------------------ 候选扩展
    def peers_of(self, host: str) -> List[str]:
        """读取实例自带的 peer 域名列表（可用则返回，否则空列表）。"""
        status, payload = self._get_json("https://%s/api/v1/instance/peers" % host)
        if status != 200 or not isinstance(payload, list):
            return []
        out = []
        for item in payload:
            if isinstance(item, str) and "." in item:
                out.append(item.strip().lower())
        return out

    # ------------------------------------------------------------------ 完整校验
    def probe(self, host: str, discovery_source: str = "seed") -> Dict[str, Any]:
        record: Dict[str, Any] = {
            "instance_domain": host,
            "discovery_source": discovery_source,
            "api_status": "unknown",
            "collection_status": "pending",
        }

        nodeinfo = self.fetch_nodeinfo(host)
        if not nodeinfo:
            record["api_status"] = "unreachable"
            record["collection_status"] = "error"
            record["failure_reason"] = "nodeinfo 不可用"
            return record

        software = nodeinfo.get("software") or {}
        metadata = nodeinfo.get("metadata") or {}
        usage = nodeinfo.get("usage") or {}
        users = usage.get("users") or {}

        record.update(
            {
                "software_name": software.get("name"),
                "software_version": software.get("version"),
                "nodeinfo_url": nodeinfo.get("_nodeinfo_url"),
                "users_total": users.get("total"),
                "local_posts": usage.get("localPosts"),
                "open_registrations": 1 if nodeinfo.get("openRegistrations") else 0,
                "disable_local_timeline": 1 if metadata.get("disableLocalTimeline") else 0,
                "disable_global_timeline": 1 if metadata.get("disableGlobalTimeline") else 0,
                "raw": json.dumps(nodeinfo, ensure_ascii=False)[:200000],
            }
        )

        if not is_misskey_family(record["software_name"]):
            record["api_status"] = "not_misskey"
            record["collection_status"] = "skipped"
            record["failure_reason"] = "NodeInfo 软件为 %s，非 Misskey 系" % record["software_name"]
            return record

        # 合规：robots.txt 是否允许 /api
        api_url = "https://%s/api/meta" % host
        rules = self.robots.rules_for(api_url)
        record["api_robots_status"] = rules.status
        record["api_robots_note"] = rules.note
        record["content_signals"] = json.dumps(rules.content_signals, ensure_ascii=False)
        if not self.robots.can_fetch(api_url):
            record["api_status"] = "forbidden_by_robots"
            record["collection_status"] = "skipped"
            record["failure_reason"] = "robots.txt 禁止 /api（%s）" % rules.note
            return record

        ok, status, data = self._post(host, "meta")
        if not ok:
            record["api_status"] = "api_error"
            record["collection_status"] = "error"
            record["failure_reason"] = "POST /api/meta 失败（%s）：%s" % (status, data)
            return record
        record["api_status"] = "ok"
        if isinstance(data, dict):
            version = data.get("version")
            if version:
                record["software_version"] = record.get("software_version") or version
            if record.get("software_name") is None:
                record["software_name"] = data.get("name")
            if data.get("disableLocalTimeline"):
                record["disable_local_timeline"] = 1
            if data.get("disableGlobalTimeline"):
                record["disable_global_timeline"] = 1

        # 能力探测：本地时间线
        ok, status, data = self._post(host, "notes/local-timeline", {"limit": 1})
        record["local_timeline_api"] = "ok" if ok and isinstance(data, list) else "unavailable"

        record["following_api"] = "unknown"
        record["followers_api"] = "unknown"
        return record
