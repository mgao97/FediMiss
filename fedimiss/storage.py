"""本地存储：JSONL 帖子流、profile JSON、媒体文件与合规报告。"""

from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime, timezone

_SAFE_RE = re.compile(r"[^A-Za-z0-9._@-]+")


def _safe(name: str) -> str:
    name = _SAFE_RE.sub("_", str(name)).strip("_")
    return name or "unknown"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Storage:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ 路径
    def instance_dir(self, host: str) -> str:
        path = os.path.join(self.root, _safe(host))
        os.makedirs(path, exist_ok=True)
        return path

    def user_dir(self, host: str, username: str) -> str:
        path = os.path.join(self.instance_dir(host), "users", _safe(username))
        os.makedirs(path, exist_ok=True)
        return path

    def media_dir(self, host: str, username: str) -> str:
        path = os.path.join(self.user_dir(host, username), "media")
        os.makedirs(path, exist_ok=True)
        return path

    def notes_path(self, host: str, username: str) -> str:
        return os.path.join(self.user_dir(host, username), "notes.jsonl")

    # ------------------------------------------------------------------ 写
    def _write_json(self, path: str, obj) -> None:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    def save_instance_meta(self, host: str, meta: dict, stats: dict = None) -> str:
        path = os.path.join(self.instance_dir(host), "instance_meta.json")
        payload = {"fetched_at": now_iso(), "meta": meta}
        if stats is not None:
            payload["stats"] = stats
        self._write_json(path, payload)
        return path

    def save_profile(self, host: str, username: str, profile: dict) -> str:
        path = os.path.join(self.user_dir(host, username), "profile.json")
        self._write_json(
            path,
            {"instance": host, "account": username, "fetched_at": now_iso(), "profile": profile},
        )
        return path

    def append_notes(self, host: str, username: str, records) -> int:
        if not records:
            return 0
        path = self.notes_path(host, username)
        with self._lock:
            with open(path, "a", encoding="utf-8") as fh:
                for rec in records:
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return len(records)

    def existing_note_ids(self, host: str, username: str):
        path = self.notes_path(host, username)
        ids = set()
        if not os.path.exists(path):
            return ids
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                note = rec.get("note") or {}
                if note.get("id"):
                    ids.add(note["id"])
        return ids

    def write_media(self, host: str, username: str, filename: str, data: bytes) -> str:
        path = os.path.join(self.media_dir(host, username), _safe(filename))
        with self._lock:
            with open(path, "wb") as fh:
                fh.write(data)
        return path

    def save_compliance(self, host: str, info: dict) -> str:
        path = os.path.join(self.instance_dir(host), "compliance.json")
        self._write_json(path, info)
        return path

    def append_run_log(self, entry: dict) -> None:
        path = os.path.join(self.root, "crawl_log.jsonl")
        with self._lock:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
