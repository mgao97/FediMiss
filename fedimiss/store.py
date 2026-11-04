"""SQLite 状态存储。

设计沿用 FediData 的「实体分层」思路（实例 / 账号 / 关系 / 内容 / 错误分表），
但落在单机 SQLite 上，以便「单机可恢复、多 worker 共享队列、断点续采」。

表：
  instances          实例清单与校验状态
  accounts           账号（含 BFS / profile / notes / media 四个状态机）
  notes              公开帖子
  follow_edges       关注关系边
  account_id_mapping 跨实例账号归属解析的可审计映射
  media              媒体附件元数据与下载状态
  instance_stats     每实例请求计数、成功率、最近成功时间
  errors             所有失败的可审计记录

线程模型：单连接 + 互斥锁（WAL 模式）。写入集中在主线程/少量 worker，足够。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS instances (
    instance_domain        TEXT PRIMARY KEY,
    software_name          TEXT,
    software_version       TEXT,
    nodeinfo_url           TEXT,
    users_total            INTEGER,
    local_posts            INTEGER,
    open_registrations     INTEGER,
    disable_local_timeline INTEGER,
    disable_global_timeline INTEGER,
    discovery_source       TEXT,
    checked_at             TEXT,
    api_status             TEXT,
    api_robots_status      INTEGER,
    api_robots_note        TEXT,
    content_signals        TEXT,
    following_api          TEXT,
    followers_api          TEXT,
    local_timeline_api     TEXT,
    collection_status      TEXT DEFAULT 'pending',
    failure_reason         TEXT,
    raw                    TEXT
);

CREATE TABLE IF NOT EXISTS accounts (
    account_key         TEXT PRIMARY KEY,
    username            TEXT,
    host                TEXT,
    instance_domain     TEXT,
    local_user_id       TEXT,
    account_uri         TEXT,
    url                 TEXT,
    display_name        TEXT,
    description         TEXT,
    created_at          TEXT,
    followers_count     INTEGER,
    following_count     INTEGER,
    notes_count         INTEGER,
    is_bot_declared     INTEGER,
    is_locked           INTEGER,
    avatar_url          TEXT,
    banner_url          TEXT,
    discovery_instance  TEXT,
    discovery_method    TEXT,
    discovered_at       TEXT,
    bfs_depth           INTEGER,
    home_instance_status TEXT,
    bfs_status          TEXT DEFAULT 'pending',
    profile_status      TEXT DEFAULT 'pending',
    notes_status        TEXT DEFAULT 'pending',
    media_status        TEXT DEFAULT 'pending',
    retry_count         INTEGER DEFAULT 0,
    last_error          TEXT,
    last_collected_at   TEXT,
    raw                 TEXT
);
CREATE INDEX IF NOT EXISTS idx_accounts_bfs      ON accounts(bfs_status, bfs_depth);
CREATE INDEX IF NOT EXISTS idx_accounts_profile  ON accounts(profile_status);
CREATE INDEX IF NOT EXISTS idx_accounts_notes    ON accounts(notes_status);
CREATE INDEX IF NOT EXISTS idx_accounts_instance ON accounts(instance_domain);
CREATE INDEX IF NOT EXISTS idx_accounts_home     ON accounts(home_instance_status);

CREATE TABLE IF NOT EXISTS notes (
    note_key           TEXT PRIMARY KEY,
    note_id            TEXT,
    source_instance    TEXT,
    author_account_key TEXT,
    created_at         TEXT,
    text               TEXT,
    cw                 TEXT,
    visibility         TEXT,
    lang               TEXT,
    reply_id           TEXT,
    renote_id          TEXT,
    reply_account_key  TEXT,
    renote_account_key TEXT,
    has_media          INTEGER,
    media_count        INTEGER,
    files              TEXT,
    collection_time    TEXT
);
CREATE INDEX IF NOT EXISTS idx_notes_author   ON notes(author_account_key);
CREATE INDEX IF NOT EXISTS idx_notes_instance ON notes(source_instance);
CREATE INDEX IF NOT EXISTS idx_notes_created  ON notes(created_at);

CREATE TABLE IF NOT EXISTS follow_edges (
    edge_key              TEXT PRIMARY KEY,
    source_account_key    TEXT,
    target_account_key    TEXT,
    source_instance_domain TEXT,
    target_instance_domain TEXT,
    direction             TEXT,
    collected_at          TEXT,
    collection_method     TEXT,
    source_instance       TEXT
);
CREATE INDEX IF NOT EXISTS idx_edges_source ON follow_edges(source_account_key);
CREATE INDEX IF NOT EXISTS idx_edges_target ON follow_edges(target_account_key);

CREATE TABLE IF NOT EXISTS account_id_mapping (
    mapping_key           TEXT PRIMARY KEY,
    raw_identifier        TEXT,
    normalized_identifier TEXT,
    source_instance       TEXT,
    resolved_username     TEXT,
    resolved_host         TEXT,
    resolved_local_user_id TEXT,
    resolution_method     TEXT,
    resolution_evidence   TEXT,
    confidence            TEXT,
    resolved_at           TEXT,
    failure_reason        TEXT
);
CREATE INDEX IF NOT EXISTS idx_mapping_confidence ON account_id_mapping(confidence);

CREATE TABLE IF NOT EXISTS media (
    media_key         TEXT PRIMARY KEY,
    note_key          TEXT,
    note_id           TEXT,
    source_instance   TEXT,
    author_account_key TEXT,
    file_id           TEXT,
    url               TEXT,
    thumbnail_url     TEXT,
    type              TEXT,
    name              TEXT,
    size              INTEGER,
    md5               TEXT,
    blurhash          TEXT,
    is_sensitive      INTEGER,
    width             INTEGER,
    height            INTEGER,
    media_host        TEXT,
    robots_state      TEXT,
    download_status   TEXT DEFAULT 'pending',
    local_path        TEXT,
    downloaded_bytes  INTEGER,
    sha256            TEXT,
    attempted_at      TEXT,
    error             TEXT
);
CREATE INDEX IF NOT EXISTS idx_media_status ON media(download_status);
CREATE INDEX IF NOT EXISTS idx_media_note   ON media(note_key);

CREATE TABLE IF NOT EXISTS account_snapshots (
    snapshot_key TEXT PRIMARY KEY,
    account_key  TEXT,
    window_id    TEXT,
    snapshot_at  TEXT,
    profile_json TEXT,
    display_name TEXT,
    description  TEXT,
    followers_count INTEGER,
    following_count INTEGER,
    notes_count INTEGER,
    raw_profile TEXT
);
CREATE INDEX IF NOT EXISTS idx_account_snapshots_account
    ON account_snapshots(account_key, snapshot_at);

CREATE TABLE IF NOT EXISTS relationship_snapshots (
    snapshot_key TEXT PRIMARY KEY,
    source_account_key TEXT,
    target_account_key TEXT,
    source_instance_domain TEXT,
    target_instance_domain TEXT,
    direction TEXT,
    window_id TEXT,
    snapshot_at TEXT,
    collection_method TEXT
);
CREATE INDEX IF NOT EXISTS idx_relationship_snapshots_source
    ON relationship_snapshots(source_account_key, snapshot_at);

CREATE TABLE IF NOT EXISTS instance_snapshots (
    snapshot_key TEXT PRIMARY KEY,
    instance_domain TEXT,
    window_id TEXT,
    snapshot_at TEXT,
    metadata_json TEXT,
    software_name TEXT,
    software_version TEXT,
    users_total INTEGER,
    local_posts INTEGER
);
CREATE INDEX IF NOT EXISTS idx_instance_snapshots_domain
    ON instance_snapshots(instance_domain, snapshot_at);

CREATE TABLE IF NOT EXISTS collection_runs (
    run_key TEXT PRIMARY KEY,
    window_id TEXT,
    window_type TEXT,
    start_at TEXT,
    end_at TEXT,
    status TEXT,
    requests INTEGER DEFAULT 0,
    successes INTEGER DEFAULT 0,
    failures INTEGER DEFAULT 0,
    instance_domain TEXT,
    available_range_start TEXT,
    available_range_end TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS instance_stats (
    instance_domain TEXT PRIMARY KEY,
    requests        INTEGER DEFAULT 0,
    ok              INTEGER DEFAULT 0,
    errors          INTEGER DEFAULT 0,
    throttled       INTEGER DEFAULT 0,
    last_success_at TEXT,
    last_error_at   TEXT
);

CREATE TABLE IF NOT EXISTS errors (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              TEXT,
    stage           TEXT,
    instance_domain TEXT,
    subject         TEXT,
    error_type      TEXT,
    http_status     INTEGER,
    message         TEXT,
    retry_count     INTEGER
);
CREATE INDEX IF NOT EXISTS idx_errors_stage ON errors(stage);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


class Store:
    def __init__(self, path: str):
        directory = os.path.dirname(os.path.abspath(path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, timeout=60.0)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA foreign_keys=OFF")
            self._conn.executescript(SCHEMA)
            self._migrate()
            self._conn.commit()

    def _migrate(self) -> None:
        """为已有数据库补充向后兼容的新账号/媒体字段。"""
        account_columns = {
            "avatar_blurhash": "TEXT",
            "banner_blurhash": "TEXT",
            "background_url": "TEXT",
            "is_cat": "INTEGER",
            "is_admin": "INTEGER",
            "is_moderator": "INTEGER",
            "is_indexable": "INTEGER",
            "is_suspended": "INTEGER",
            "is_silenced": "INTEGER",
            "hide_online_status": "INTEGER",
            "no_crawling": "INTEGER",
            "birthday": "TEXT",
            "location": "TEXT",
            "language": "TEXT",
            "fields_json": "TEXT",
            "emojis_json": "TEXT",
            "raw_profile": "TEXT",
            "profile_collected_at": "TEXT",
        }
        existing = {
            row["name"] for row in self._conn.execute("PRAGMA table_info(accounts)").fetchall()
        }
        for name, declaration in account_columns.items():
            if name not in existing:
                self._conn.execute(
                    "ALTER TABLE accounts ADD COLUMN %s %s" % (name, declaration)
                )
        # 帖子和关系保留原字段，并增加可审计的窗口归属和采集时间
        migrations = {
            "notes": {
                "collected_at": "TEXT",
                "window_id": "TEXT",
                "window_type": "TEXT",
                "raw_note": "TEXT",
            },
            "follow_edges": {
                "window_id": "TEXT",
                "snapshot_at": "TEXT",
            },
            "media": {
                "collected_at": "TEXT",
                "window_id": "TEXT",
            },
        }
        for table, columns in migrations.items():
            existing = {row["name"] for row in self._conn.execute(
                "PRAGMA table_info(%s)" % table
            ).fetchall()}
            for name, declaration in columns.items():
                if name not in existing:
                    self._conn.execute(
                        "ALTER TABLE %s ADD COLUMN %s %s" % (table, name, declaration)
                    )

    def close(self) -> None:
        with self._lock:
            self._conn.commit()
            self._conn.close()

    # ------------------------------------------------------------------ 内部
    def _exec(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        with self._lock:
            cur = self._conn.execute(sql, params)
            self._conn.commit()
            return cur

    def _executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> int:
        rows = list(rows)
        if not rows:
            return 0
        with self._lock:
            cur = self._conn.executemany(sql, rows)
            self._conn.commit()
            return cur.rowcount

    def _rows(self, sql: str, params: Sequence[Any] = ()) -> List[sqlite3.Row]:
        with self._lock:
            return list(self._conn.execute(sql, params).fetchall())

    def _one(self, sql: str, params: Sequence[Any] = ()) -> Optional[sqlite3.Row]:
        rows = self._rows(sql, params)
        return rows[0] if rows else None

    # ------------------------------------------------------------------ meta
    def set_meta(self, key: str, value: Any) -> None:
        self._exec(
            "INSERT INTO meta(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value),
        )

    def get_meta(self, key: str, default: Any = None) -> Any:
        row = self._one("SELECT value FROM meta WHERE key=?", (key,))
        return row["value"] if row else default

    # ------------------------------------------------------------------ 实例
    def upsert_instance(self, domain: str, **fields: Any) -> None:
        row = self._one("SELECT instance_domain FROM instances WHERE instance_domain=?", (domain,))
        if row is None:
            self._exec("INSERT INTO instances(instance_domain) VALUES(?)", (domain,))
        if not fields:
            return
        cols = ", ".join("%s=?" % key for key in fields)
        self._exec(
            "UPDATE instances SET %s WHERE instance_domain=?" % cols,
            tuple(fields.values()) + (domain,),
        )

    def get_instance(self, domain: str) -> Optional[Dict[str, Any]]:
        row = self._one("SELECT * FROM instances WHERE instance_domain=?", (domain,))
        return dict(row) if row else None

    def instances(self, status: str = None) -> List[Dict[str, Any]]:
        if status:
            rows = self._rows(
                "SELECT * FROM instances WHERE collection_status=? ORDER BY users_total DESC", (status,)
            )
        else:
            rows = self._rows("SELECT * FROM instances ORDER BY users_total DESC")
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------ 账号
    def upsert_account(self, account_key: str, **fields: Any) -> None:
        row = self._one("SELECT account_key FROM accounts WHERE account_key=?", (account_key,))
        if row is None:
            self._exec("INSERT INTO accounts(account_key) VALUES(?)", (account_key,))
        if fields:
            cols = ", ".join("%s=?" % key for key in fields)
            self._exec(
                "UPDATE accounts SET %s WHERE account_key=?" % cols,
                tuple(fields.values()) + (account_key,),
            )

    def insert_account_if_absent(self, account_key: str, **fields: Any) -> bool:
        """返回 True 表示本次新插入。"""
        row = self._one("SELECT 1 FROM accounts WHERE account_key=?", (account_key,))
        if row is not None:
            return False
        self.upsert_account(account_key, **fields)
        return True

    def get_account(self, account_key: str) -> Optional[Dict[str, Any]]:
        row = self._one("SELECT * FROM accounts WHERE account_key=?", (account_key,))
        return dict(row) if row else None

    def claim_accounts(
        self,
        status_column: str,
        limit: int,
        max_depth: int = None,
        domain: str = None,
    ) -> List[Dict[str, Any]]:
        """原子领取一批 pending 账号（按 bfs_depth 由浅到深）。

        ``domain`` 限定原属实例：每个实例 worker 只处理归属自己的账号，
        否则 worker A 会领到归属实例 B 的账号并对 B 的账号做无意义的本地查询。
        """
        clauses = ["%s='pending'" % status_column]
        params: List[Any] = []
        if max_depth is not None:
            clauses.append("bfs_depth <= ?")
            params.append(max_depth)
        if domain:
            clauses.append("instance_domain = ?")
            params.append(domain)
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(
                "SELECT account_key FROM accounts WHERE %s "
                "ORDER BY bfs_depth ASC, discovered_at ASC LIMIT ?" % " AND ".join(clauses),
                tuple(params),
            ).fetchall()
            keys = [r["account_key"] for r in rows]
            if keys:
                marks = ",".join("?" * len(keys))
                self._conn.execute(
                    "UPDATE accounts SET %s='running' WHERE account_key IN (%s)" % (status_column, marks),
                    tuple(keys),
                )
            self._conn.commit()
        return [self.get_account(k) for k in keys]

    def count_accounts(self, where: str = "", params: Sequence[Any] = ()) -> int:
        sql = "SELECT COUNT(*) AS n FROM accounts"
        if where:
            sql += " WHERE " + where
        row = self._one(sql, params)
        return int(row["n"]) if row else 0

    def accounts_pending(self, status_column: str) -> int:
        return self.count_accounts("%s='pending'" % status_column)

    def accounts_for_instance(self, domain: str) -> List[Dict[str, Any]]:
        return [
            dict(r)
            for r in self._rows(
                "SELECT * FROM accounts WHERE instance_domain=? AND host IS NULL", (domain,)
            )
        ]

    def reset_running(self, status_columns: Sequence[str]) -> int:
        """进程重启时把 in-flight 状态回滚为 pending。"""
        total = 0
        for col in status_columns:
            cur = self._exec(
                "UPDATE accounts SET %s='pending' WHERE %s IN ('running')" % (col, col)
            )
            total += cur.rowcount or 0
        return total

    def iter_accounts(self, where: str = "", params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM accounts"
        if where:
            sql += " WHERE " + where
        return [dict(r) for r in self._rows(sql, params)]

    # ------------------------------------------------------------------ 帖子
    def insert_notes(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = []
        for item in rows:
            payload.append(
                (
                    item["note_key"], item.get("note_id"), item.get("source_instance"),
                    item.get("author_account_key"), item.get("created_at"), item.get("text"),
                    item.get("cw"), item.get("visibility"), item.get("lang"),
                    item.get("reply_id"), item.get("renote_id"),
                    item.get("reply_account_key"), item.get("renote_account_key"),
                    1 if item.get("has_media") else 0, item.get("media_count", 0),
                    item.get("files"), item.get("collection_time"),
                    item.get("collected_at", item.get("collection_time")),
                    item.get("window_id"), item.get("window_type"), item.get("raw_note"),
                )
            )
        return self._executemany(
            "INSERT OR IGNORE INTO notes(note_key, note_id, source_instance, author_account_key, "
            "created_at, text, cw, visibility, lang, reply_id, renote_id, reply_account_key, "
            "renote_account_key, has_media, media_count, files, collection_time, "
            "collected_at, window_id, window_type, raw_note) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            payload,
        )

    def max_note_cursor(self, author_account_key: str) -> Optional[str]:
        row = self._one(
            "SELECT MIN(created_at) AS oldest FROM notes WHERE author_account_key=?",
            (author_account_key,),
        )
        return row["oldest"] if row and row["oldest"] else None

    def count_notes(self) -> int:
        row = self._one("SELECT COUNT(*) AS n FROM notes")
        return int(row["n"]) if row else 0

    # ------------------------------------------------------------------ 关系边
    def insert_edges(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = [
            (
                item["edge_key"], item["source_account_key"], item["target_account_key"],
                item.get("source_instance_domain"), item.get("target_instance_domain"),
                item.get("direction"), item.get("collected_at"),
                item.get("collection_method"), item.get("source_instance"),
                item.get("window_id"), item.get("snapshot_at", item.get("collected_at")),
            )
            for item in rows
        ]
        return self._executemany(
            "INSERT OR IGNORE INTO follow_edges(edge_key, source_account_key, target_account_key, "
            "source_instance_domain, target_instance_domain, direction, collected_at, "
            "collection_method, source_instance, window_id, snapshot_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            payload,
        )

    def count_edges(self) -> int:
        row = self._one("SELECT COUNT(*) AS n FROM follow_edges")
        return int(row["n"]) if row else 0

    # ------------------------------------------------------------------ 归属映射
    def upsert_mapping(self, mapping_key: str, **fields: Any) -> None:
        row = self._one("SELECT mapping_key FROM account_id_mapping WHERE mapping_key=?", (mapping_key,))
        if row is None:
            self._exec("INSERT INTO account_id_mapping(mapping_key) VALUES(?)", (mapping_key,))
        if fields:
            cols = ", ".join("%s=?" % key for key in fields)
            self._exec(
                "UPDATE account_id_mapping SET %s WHERE mapping_key=?" % cols,
                tuple(fields.values()) + (mapping_key,),
            )

    def count_mappings(self, confidence: str = None) -> int:
        if confidence:
            row = self._one(
                "SELECT COUNT(*) AS n FROM account_id_mapping WHERE confidence=?", (confidence,)
            )
        else:
            row = self._one("SELECT COUNT(*) AS n FROM account_id_mapping")
        return int(row["n"]) if row else 0

    # ------------------------------------------------------------------ 媒体
    def upsert_media(self, rows: Iterable[Dict[str, Any]]) -> int:
        payload = [
            (
                item["media_key"], item.get("note_key"), item.get("note_id"),
                item.get("source_instance"), item.get("author_account_key"), item.get("file_id"),
                item.get("url"), item.get("thumbnail_url"), item.get("type"), item.get("name"),
                item.get("size"), item.get("md5"), item.get("blurhash"),
                1 if item.get("is_sensitive") else 0, item.get("width"), item.get("height"),
                item.get("media_host"), item.get("robots_state"),
                item.get("download_status", "pending"),
            )
            for item in rows
        ]
        return self._executemany(
            "INSERT OR IGNORE INTO media(media_key, note_key, note_id, source_instance, "
            "author_account_key, file_id, url, thumbnail_url, type, name, size, md5, blurhash, "
            "is_sensitive, width, height, media_host, robots_state, download_status) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            payload,
        )

    def claim_media(self, limit: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT media_key FROM media WHERE download_status='pending' LIMIT ?", (limit,)
            ).fetchall()
            keys = [r["media_key"] for r in rows]
            if keys:
                marks = ",".join("?" * len(keys))
                self._conn.execute(
                    "UPDATE media SET download_status='running' WHERE media_key IN (%s)" % marks,
                    tuple(keys),
                )
            self._conn.commit()
            return [dict(r) for r in self._conn.execute(
                "SELECT * FROM media WHERE media_key IN (%s)" % ",".join("?" * len(keys)), tuple(keys)
            ).fetchall()] if keys else []

    def update_media(self, media_key: str, **fields: Any) -> None:
        if not fields:
            return
        cols = ", ".join("%s=?" % key for key in fields)
        self._exec(
            "UPDATE media SET %s WHERE media_key=?" % cols, tuple(fields.values()) + (media_key,)
        )

    def count_media(self, status: str = None) -> int:
        if status:
            row = self._one("SELECT COUNT(*) AS n FROM media WHERE download_status=?", (status,))
        else:
            row = self._one("SELECT COUNT(*) AS n FROM media")
        return int(row["n"]) if row else 0

    def reset_media_running(self) -> int:
        cur = self._exec("UPDATE media SET download_status='pending' WHERE download_status='running'")
        return cur.rowcount or 0

    # ------------------------------------------------------------------ 窗口快照与采集日志
    def insert_account_snapshot(self, snapshot_key: str, account_key: str, window_id: str,
                                snapshot_at: str, account: Dict[str, Any]) -> None:
        self._exec(
            "INSERT OR IGNORE INTO account_snapshots("
            "snapshot_key, account_key, window_id, snapshot_at, profile_json, display_name, "
            "description, followers_count, following_count, notes_count, raw_profile"
            ") VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                snapshot_key, account_key, window_id, snapshot_at,
                json.dumps(account, ensure_ascii=False)[:40000],
                account.get("display_name"), account.get("description"),
                account.get("followers_count"), account.get("following_count"),
                account.get("notes_count"), account.get("raw_profile"),
            ),
        )

    def insert_relationship_snapshot(self, snapshot_key: str, edge: Dict[str, Any],
                                     window_id: str, snapshot_at: str) -> None:
        self._exec(
            "INSERT OR IGNORE INTO relationship_snapshots("
            "snapshot_key, source_account_key, target_account_key, source_instance_domain, "
            "target_instance_domain, direction, window_id, snapshot_at, collection_method"
            ") VALUES(?,?,?,?,?,?,?,?,?)",
            (
                snapshot_key, edge.get("source_account_key"), edge.get("target_account_key"),
                edge.get("source_instance_domain"), edge.get("target_instance_domain"),
                edge.get("direction"), window_id, snapshot_at,
                edge.get("collection_method"),
            ),
        )

    def insert_instance_snapshot(self, snapshot_key: str, instance_domain: str, window_id: str,
                                 snapshot_at: str, metadata: Dict[str, Any]) -> None:
        self._exec(
            "INSERT OR IGNORE INTO instance_snapshots("
            "snapshot_key, instance_domain, window_id, snapshot_at, metadata_json, "
            "software_name, software_version, users_total, local_posts"
            ") VALUES(?,?,?,?,?,?,?,?,?)",
            (
                snapshot_key, instance_domain, window_id, snapshot_at,
                json.dumps(metadata, ensure_ascii=False)[:40000],
                metadata.get("software_name"), metadata.get("software_version"),
                metadata.get("users_total"), metadata.get("local_posts"),
            ),
        )

    def upsert_collection_run(self, run_key: str, **fields: Any) -> None:
        row = self._one("SELECT run_key FROM collection_runs WHERE run_key=?", (run_key,))
        if row is None:
            self._exec("INSERT INTO collection_runs(run_key) VALUES(?)", (run_key,))
        if fields:
            cols = ", ".join("%s=?" % key for key in fields)
            self._exec(
                "UPDATE collection_runs SET %s WHERE run_key=?" % cols,
                tuple(fields.values()) + (run_key,),
            )

    # ------------------------------------------------------------------ 统计与错误
    def bump_instance_stats(self, domain: str, ok: bool = True, throttled: bool = False) -> None:
        row = self._one("SELECT instance_domain FROM instance_stats WHERE instance_domain=?", (domain,))
        if row is None:
            self._exec("INSERT INTO instance_stats(instance_domain) VALUES(?)", (domain,))
        self._exec("UPDATE instance_stats SET requests=requests+1 WHERE instance_domain=?", (domain,))
        if ok:
            self._exec(
                "UPDATE instance_stats SET ok=ok+1, last_success_at=datetime('now') "
                "WHERE instance_domain=?",
                (domain,),
            )
        else:
            self._exec(
                "UPDATE instance_stats SET errors=errors+1, last_error_at=datetime('now') "
                "WHERE instance_domain=?",
                (domain,),
            )
        if throttled:
            self._exec("UPDATE instance_stats SET throttled=throttled+1 WHERE instance_domain=?", (domain,))

    def log_error(
        self,
        stage: str,
        instance_domain: str = None,
        subject: str = None,
        error_type: str = None,
        http_status: int = None,
        message: str = None,
        retry_count: int = 0,
    ) -> None:
        self._exec(
            "INSERT INTO errors(ts, stage, instance_domain, subject, error_type, http_status, "
            "message, retry_count) VALUES(datetime('now'),?,?,?,?,?,?,?)",
            (stage, instance_domain, subject, error_type, http_status, (message or "")[:2000], retry_count),
        )

    def instance_stats(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in self._rows("SELECT * FROM instance_stats ORDER BY requests DESC")]

    def errors(self, limit: int = 50) -> List[Dict[str, Any]]:
        return [dict(r) for r in self._rows("SELECT * FROM errors ORDER BY id DESC LIMIT ?", (limit,))]

    def error_counts(self) -> List[Dict[str, Any]]:
        return [
            dict(r)
            for r in self._rows(
                "SELECT stage, error_type, http_status, COUNT(*) AS n FROM errors "
                "GROUP BY stage, error_type, http_status ORDER BY n DESC"
            )
        ]

    def counts(self) -> Dict[str, int]:
        out = {
            "instances": self._scalar("SELECT COUNT(*) FROM instances"),
            "accounts": self._scalar("SELECT COUNT(*) FROM accounts"),
            "notes": self._scalar("SELECT COUNT(*) FROM notes"),
            "follow_edges": self._scalar("SELECT COUNT(*) FROM follow_edges"),
            "media": self._scalar("SELECT COUNT(*) FROM media"),
            "account_snapshots": self._scalar("SELECT COUNT(*) FROM account_snapshots"),
            "relationship_snapshots": self._scalar("SELECT COUNT(*) FROM relationship_snapshots"),
            "instance_snapshots": self._scalar("SELECT COUNT(*) FROM instance_snapshots"),
            "collection_runs": self._scalar("SELECT COUNT(*) FROM collection_runs"),
            "mappings": self._scalar("SELECT COUNT(*) FROM account_id_mapping"),
            "errors": self._scalar("SELECT COUNT(*) FROM errors"),
        }
        return out

    def _scalar(self, sql: str) -> int:
        row = self._one(sql)
        if row is None:
            return 0
        return int(list(row)[0] or 0)

    def distinct_note_media_types(self) -> List[Tuple[str, int]]:
        rows = self._rows(
            "SELECT type, COUNT(*) AS n FROM media GROUP BY type ORDER BY n DESC"
        )
        return [(r["type"], int(r["n"])) for r in rows]
