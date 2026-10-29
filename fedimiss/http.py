"""限速 HTTP 客户端。

特性：
  * 每 host 最小请求间隔（与 robots.txt 的 Crawl-delay 取较大值）
  * 429 / 5xx / 网络错误自动重试，支持 Retry-After，指数退避 + 抖动
  * 抓取前强制 robots.txt 校验（可通过 check_robots=False 关闭，仅用于 robots.txt 自身）
  * 响应体大小上限，避免意外拉取超大文件
仅依赖标准库。
"""

from __future__ import annotations

import gzip
import json
import random
import ssl
import time
import urllib.error
import urllib.request
import zlib
from typing import Dict, Optional, Tuple


class RobotsDisallowed(Exception):
    """目标 URL 被 robots.txt 禁止。"""


class HttpError(Exception):
    """HTTP 请求失败（含服务端返回的错误信息）。"""

    def __init__(self, status: int, message: str, url: str = ""):
        super().__init__("HTTP %s %s (%s)" % (status, message, url))
        self.status = status
        self.message = message
        self.url = url


class ResponseTooLarge(Exception):
    """响应体超过上限。"""


class HttpClient:
    def __init__(
        self,
        user_agent: str,
        robots,
        timeout: float = 30.0,
        min_interval: float = 1.0,
        max_retries: int = 4,
        max_bytes: int = 32 * 1024 * 1024,
        logger=None,
    ):
        self.user_agent = user_agent
        self.robots = robots
        self.timeout = timeout
        self.min_interval = min_interval
        self.max_retries = max_retries
        self.max_bytes = max_bytes
        self.logger = logger
        self._ctx = ssl.create_default_context()
        self._last_at: Dict[str, float] = {}

    # ------------------------------------------------------------------ 限速
    def _throttle(self, host: str, crawl_delay: Optional[float]) -> None:
        interval = self.min_interval
        if crawl_delay and crawl_delay > interval:
            interval = crawl_delay
        last = self._last_at.get(host)
        if last is not None:
            wait = interval - (time.time() - last)
            if wait > 0:
                time.sleep(wait)
        self._last_at[host] = time.time()

    @staticmethod
    def _retry_after(headers) -> Optional[float]:
        if not headers:
            return None
        raw = headers.get("Retry-After")
        if not raw:
            return None
        try:
            return max(0.0, float(raw))
        except ValueError:
            return None

    # ------------------------------------------------------------------ 请求
    def request(
        self,
        method: str,
        url: str,
        data: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        check_robots: bool = True,
        max_bytes: Optional[int] = None,
        max_retries: Optional[int] = None,
    ) -> Tuple[int, object, bytes]:
        """发起请求，返回 (status, headers, body_bytes)。"""
        from urllib.parse import urlparse

        if check_robots and self.robots is not None:
            if not self.robots.can_fetch(url):
                raise RobotsDisallowed("robots.txt 禁止抓取: %s" % url)

        host = urlparse(url).netloc
        crawl_delay = self.robots.crawl_delay(url) if self.robots is not None else None
        limit = max_bytes or self.max_bytes
        retries = self.max_retries if max_retries is None else max_retries

        hdrs = {
            "User-Agent": self.user_agent,
            "Accept": "application/json, */*",
            "Connection": "close",
        }
        if headers:
            hdrs.update(headers)

        attempt = 0
        while True:
            attempt += 1
            self._throttle(host, crawl_delay)
            req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout, context=self._ctx) as resp:
                    body = self._read_limited(resp, limit)
                    return resp.status, resp.headers, body
            except urllib.error.HTTPError as exc:
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if retryable and attempt <= retries:
                    delay = self._retry_after(exc.headers)
                    if delay is None:
                        delay = min(60.0, (2 ** (attempt - 1)) * 2.0)
                    delay += random.uniform(0, 0.5)
                    if self.logger:
                        self.logger.warning(
                            "HTTP %s，%.1fs 后重试 (%s/%s): %s",
                            exc.code, delay, attempt, retries, url,
                        )
                    time.sleep(delay)
                    continue
                detail = ""
                try:
                    detail = exc.read(4096).decode("utf-8", errors="replace")
                except Exception:  # noqa: BLE001
                    pass
                raise HttpError(exc.code, detail or exc.reason, url)
            except ResponseTooLarge:
                raise
            except Exception as exc:  # noqa: BLE001 - 网络层异常统一重试
                if attempt <= retries:
                    delay = min(60.0, (2 ** (attempt - 1)) * 2.0) + random.uniform(0, 0.5)
                    if self.logger:
                        self.logger.warning(
                            "请求异常 %s，%.1fs 后重试 (%s/%s): %s",
                            exc, delay, attempt, retries, url,
                        )
                    time.sleep(delay)
                    continue
                raise HttpError(0, str(exc), url)

    @staticmethod
    def _read_limited(resp, limit: int) -> bytes:
        encoding = (resp.headers.get("Content-Encoding") or "").lower()
        chunks = []
        total = 0
        while True:
            chunk = resp.read(65536)
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise ResponseTooLarge("响应体超过 %d 字节上限" % limit)
            chunks.append(chunk)
        raw = b"".join(chunks)
        if encoding == "gzip":
            raw = gzip.decompress(raw)
        elif encoding == "deflate":
            try:
                raw = zlib.decompress(raw)
            except zlib.error:
                raw = zlib.decompress(raw, -zlib.MAX_WBITS)
        return raw

    # ------------------------------------------------------------------ 便捷方法
    def post_json(self, url: str, payload: dict, max_retries: Optional[int] = None) -> dict:
        body = json.dumps(payload).encode("utf-8")
        status, _headers, raw = self.request(
            "POST",
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            max_retries=max_retries,
        )
        if not raw:
            return {}
        try:
            parsed = json.loads(raw.decode("utf-8", errors="replace"))
        except json.JSONDecodeError as exc:
            raise HttpError(status, "响应不是合法 JSON: %s" % exc, url)
        if isinstance(parsed, dict) and "error" in parsed and status >= 400:
            err = parsed["error"]
            raise HttpError(status, str(err), url)
        return parsed

    def get_bytes(self, url: str, max_bytes: Optional[int] = None) -> Tuple[bytes, str, str]:
        """下载二进制内容，返回 (bytes, content_type, final_url)。"""
        status, headers, raw = self.request("GET", url, max_bytes=max_bytes)
        return raw, headers.get("Content-Type", ""), url
