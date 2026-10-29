"""robots.txt 与 Content-Signal 合规处理。

Misskey 各实例的 robots.txt 策略差异很大（实测，2026-10）：

    misskey.io        User-agent: *   Allow: /
    misskey.cloud     user-agent: *   allow: /
    misskey.flowers   User-agent: *   Allow: /   Content-Signal: search=yes,ai-train=no,use=reference
    misskey.art       User-agent: *   Disallow: /api  ...
    misskey.design    User-agent: *   Disallow: /api  ...（Cloudflare 托管，Content-Signal: ai-train=no）
    misskey.niri.la   User-agent: *   Disallow: /api
    media.misskeyusercontent.jp / media.misskeyusercontent.com   Disallow: /

因此抓取前必须逐 host 解析 robots.txt，并在必要时同时读取 Content-Signal
（Cloudflare 的 AI 内容使用声明）。

为什么不用标准库 ``urllib.robotparser``：
    该实现对「同一个 User-agent 出现多个分组」的情况只采用第一个分组。
    misskey.design / mi.mashiro.site 等实例的 robots.txt 恰好是
      第 1 组: User-agent: * / Content-Signal / Allow: /
      第 n 组: User-agent: * / Disallow: /settings ... /api ...
    于是 /api 的 Disallow 会被漏掉。RFC 9309 要求：匹配同一 UA 的多个分组
    必须合并，路径匹配取「最长匹配优先，同长度 Allow 优先」。
    本模块按 RFC 9309 自行实现匹配，避免漏规则。

合规策略：
  * robots.txt 返回 2xx            -> 按内容解析
  * robots.txt 返回 4xx（含 404）  -> 视为「未声明」，允许抓取
  * robots.txt 返回 5xx / 网络错误 -> 视为「不可用」，保守地禁止抓取
"""

from __future__ import annotations

import re
import ssl
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlsplit

_CONTENT_SIGNAL_TOKENS = ("search", "ai-input", "ai-train", "use")
_RULE_NAMES = ("disallow", "allow", "crawl-delay")


@dataclass
class HostRules:
    """单个 host（scheme://host[:port]）的合规信息。"""

    host: str
    status: int
    allow_all: bool = False
    robots_found: bool = False
    crawl_delay: Optional[float] = None
    content_signals: Dict[str, str] = field(default_factory=dict)
    raw: str = ""
    note: str = ""

    @property
    def signal_summary(self) -> str:
        if not self.content_signals:
            return "none"
        return ",".join("%s=%s" % (k, v) for k, v in sorted(self.content_signals.items()))

    def forbids_ai_training(self) -> bool:
        """Content-Signal 是否明确禁止用于 AI 训练。"""
        return self.content_signals.get("ai-train", "").lower() == "no"


@dataclass
class _Group:
    agents: List[str] = field(default_factory=list)
    rules: List[Tuple[bool, str]] = field(default_factory=list)  # (is_allow, pattern)
    crawl_delay: Optional[float] = None
    content_signals: Dict[str, str] = field(default_factory=dict)


class RobotsRules:
    """按 RFC 9309 解析并匹配 robots.txt。"""

    def __init__(self, groups: List[_Group], user_agent: str, allow_all: bool = False):
        self.user_agent = user_agent
        self.allow_all = allow_all
        self.groups = groups
        ua_lower = user_agent.lower()
        specific = [
            g for g in groups
            if any(a and a != "*" and a in ua_lower for a in g.agents)
        ]
        applicable = specific if specific else [g for g in groups if "*" in g.agents]
        self.rules: List[Tuple[bool, str]] = []
        for g in applicable:
            self.rules.extend(g.rules)
        delays = [g.crawl_delay for g in applicable if g.crawl_delay]
        self._crawl_delay = max(delays) if delays else None
        signals: Dict[str, str] = {}
        for g in applicable:
            signals.update(g.content_signals)
        self.content_signals = signals
        self._cache: Dict[str, bool] = {}

    # ------------------------------------------------------------------ 解析
    @classmethod
    def parse(cls, text: str, user_agent: str, allow_all: bool = False) -> "RobotsRules":
        groups: List[_Group] = []
        current: Optional[_Group] = None
        seen_rule = False

        for raw_line in text.splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue
            name, value = line.split(":", 1)
            name = name.strip().lower()
            value = value.strip()

            if name == "user-agent":
                if current is None or seen_rule:
                    current = _Group()
                    groups.append(current)
                    seen_rule = False
                current.agents.append(value.lower())
                continue

            if current is None:
                continue

            if name in _RULE_NAMES:
                seen_rule = True
                if name == "crawl-delay":
                    try:
                        current.crawl_delay = float(value)
                    except ValueError:
                        pass
                else:
                    is_allow = name == "allow"
                    if value == "":
                        # 空 Disallow 表示允许全部；空 Allow 无意义
                        continue
                    current.rules.append((is_allow, value))
            elif name == "content-signal":
                for token in value.split(","):
                    token = token.strip()
                    if "=" not in token:
                        continue
                    k, v = token.split("=", 1)
                    k = k.strip().lower()
                    if k in _CONTENT_SIGNAL_TOKENS:
                        current.content_signals[k] = v.strip().lower()

        return cls(groups, user_agent, allow_all=allow_all)

    # ------------------------------------------------------------------ 匹配
    @staticmethod
    def _compile(pattern: str):
        out = []
        for ch in pattern:
            if ch == "*":
                out.append(".*")
            elif ch == "$":
                out.append("$")
            else:
                out.append(re.escape(ch))
        return re.compile("^" + "".join(out))

    def _path(self, url: str) -> str:
        parts = urlsplit(url)
        path = parts.path or "/"
        if parts.query:
            path += "?" + parts.query
        return path

    def _allowed(self, path: str) -> bool:
        best_len = -1
        best_allow = True
        for is_allow, pattern in self.rules:
            if self._compile(pattern).match(path):
                plen = len(pattern)
                if plen > best_len or (plen == best_len and is_allow):
                    best_len = plen
                    best_allow = is_allow
        return True if best_len < 0 else best_allow

    def can_fetch(self, url: str) -> bool:
        if self.allow_all:
            return True
        path = self._path(url)
        cached = self._cache.get(path)
        if cached is None:
            cached = self._allowed(path)
            self._cache[path] = cached
        return cached

    def crawl_delay(self) -> Optional[float]:
        return self._crawl_delay


class RobotsPolicy:
    """按 host 缓存 robots.txt，并给出抓取许可与 Content-Signal。"""

    def __init__(self, user_agent: str, timeout: float = 15.0, ttl: float = 3600.0, logger=None):
        self.user_agent = user_agent
        self.timeout = timeout
        self.ttl = ttl
        self.logger = logger
        self._cache: Dict[str, Tuple[float, HostRules, RobotsRules]] = {}
        self._lock = threading.Lock()
        self._ctx = ssl.create_default_context()

    # ------------------------------------------------------------------ 内部
    @staticmethod
    def _key(url: str) -> str:
        p = urlparse(url)
        return "%s://%s" % (p.scheme, p.netloc)

    @staticmethod
    def _robots_url(url: str) -> str:
        p = urlparse(url)
        return "%s://%s/robots.txt" % (p.scheme, p.netloc)

    def _raw_get(self, url: str) -> Tuple[int, str]:
        """不带 robots 校验的原始 GET（仅用于获取 robots.txt）。"""
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"User-Agent": self.user_agent, "Accept": "text/plain,*/*", "Connection": "close"},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self._ctx) as resp:
                charset = resp.headers.get_content_charset() or "utf-8"
                return resp.status, resp.read(512 * 1024).decode(charset, errors="replace")
        except urllib.error.HTTPError as exc:
            return exc.code, ""
        except Exception as exc:  # noqa: BLE001 - 网络异常统一保守处理
            if self.logger:
                self.logger.warning("获取 %s 失败: %s", url, exc)
            return -1, ""

    def _load(self, url: str) -> Tuple[HostRules, RobotsRules]:
        key = self._key(url)
        robots_url = self._robots_url(url)
        status, text = self._raw_get(robots_url)

        if status == -1 or 500 <= status < 600:
            rules = HostRules(
                host=key, status=status, allow_all=False, robots_found=False,
                note="robots.txt 不可用，保守地禁止抓取",
            )
            # 构造一个显式的「禁止全部」匹配器
            return rules, RobotsRules([_Group(agents=["*"], rules=[(False, "/")])], self.user_agent)

        if 400 <= status < 500:
            rules = HostRules(
                host=key, status=status, allow_all=True, robots_found=False,
                note="robots.txt 返回 %s，视为未声明，允许抓取" % status,
            )
            return rules, RobotsRules([], self.user_agent, allow_all=True)

        matcher = RobotsRules.parse(text, self.user_agent)
        rules = HostRules(
            host=key,
            status=status,
            allow_all=False,
            robots_found=True,
            crawl_delay=matcher.crawl_delay(),
            content_signals=matcher.content_signals,
            raw=text,
            note="robots.txt 解析成功（%d 个分组）" % len(matcher.groups),
        )
        return rules, matcher

    def _get(self, url: str) -> Tuple[HostRules, RobotsRules]:
        key = self._key(url)
        now = time.time()
        with self._lock:
            cached = self._cache.get(key)
            if cached and now - cached[0] < self.ttl:
                return cached[1], cached[2]
        rules, matcher = self._load(url)
        with self._lock:
            self._cache[key] = (now, rules, matcher)
        if self.logger:
            self.logger.info(
                "robots[%s] status=%s signal=%s crawl_delay=%s %s",
                key, rules.status, rules.signal_summary, rules.crawl_delay, rules.note,
            )
        return rules, matcher

    # ------------------------------------------------------------------ 对外
    def rules_for(self, url: str) -> HostRules:
        return self._get(url)[0]

    def can_fetch(self, url: str) -> bool:
        return self._get(url)[1].can_fetch(url)

    def crawl_delay(self, url: str) -> Optional[float]:
        return self._get(url)[0].crawl_delay

    def content_signals(self, url: str) -> Dict[str, str]:
        return self._get(url)[0].content_signals
