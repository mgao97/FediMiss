"""UTC 时间窗口、窗口归属和 30 天纵向窗口划分。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional, Tuple


def parse_utc(value: Optional[str]) -> datetime:
    if not value:
        raise ValueError("时间值不能为空")
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def in_window(value: Optional[str], start: str, end: str) -> bool:
    if not value:
        return False
    try:
        timestamp = parse_utc(value)
        return parse_utc(start) <= timestamp < parse_utc(end)
    except (TypeError, ValueError, OverflowError):
        return False


def window_label(value: Optional[str], windows: Iterable[Dict[str, Any]]) -> Optional[str]:
    for window in windows:
        if in_window(value, window["start"], window["end"]):
            return window["window_id"]
    return None


def historical_window(config: Any) -> Optional[Dict[str, str]]:
    data = config.get("time_config.historical", {}) if hasattr(config, "get") else {}
    if not isinstance(data, dict) or not data.get("enabled"):
        return None
    start = data.get("start")
    end = data.get("end")
    if start and end:
        parse_utc(start)
        parse_utc(end)
        if parse_utc(end) <= parse_utc(start):
            raise ValueError("historical.start 必须早于 historical.end")
        return {"window_id": "historical-20241025-20241031", "start": start, "end": end}
    raise ValueError("historical 已启用，但 start 或 end 未配置")


def longitudinal_windows(config: Any) -> Tuple[Dict[str, Any], ...]:
    data = config.get("time_config.longitudinal", {}) if hasattr(config, "get") else {}
    if not isinstance(data, dict) or not data.get("enabled"):
        return ()
    start_value = data.get("start_date")
    if not start_value:
        raise ValueError("longitudinal 已启用，必须配置 time_config.longitudinal.start_date")
    start = parse_utc(start_value)
    days = int(data.get("days", 90))
    window_days = int(data.get("window_days", 30))
    if days <= 0 or window_days <= 0 or days % window_days:
        raise ValueError("longitudinal.days 和 window_days 必须为正且整除")
    windows = []
    for index in range(0, days, window_days):
        window_start = start + timedelta(days=index)
        window_end = start + timedelta(days=index + window_days)
        windows.append({
            "window_id": "longitudinal-%02d-%s" % (
                index // window_days + 1, window_start.strftime("%Y%m%d"),
            ),
            "start": window_start.isoformat().replace("+00:00", "Z"),
            "end": window_end.isoformat().replace("+00:00", "Z"),
        })
    return tuple(windows)
