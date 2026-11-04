"""数据导出：5 个 Parquet 快照 + 采集清单。

交付物（需求 §12）：
  instances.parquet / accounts.parquet / notes.parquet /
  follow_edges.parquet / account_id_mapping.parquet
  collection_manifest.json

额外导出 media.parquet（附件元数据），因为媒体表是实例/账号/帖子之外的
独立实体层，缺了它 UGC 的多模态信息不可追溯。

依赖 vendored pyarrow（本机无 pip）：
    PYTHONPATH=/users/Min/FediMiss/.vendor/lib python3 -m fedimiss export
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Tuple

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as _exc:  # pragma: no cover - 环境缺少 vendored pyarrow
    raise SystemExit(
        "未找到 pyarrow。请用 vendored 版本运行：\n"
        "  PYTHONPATH=<项目>/.vendor/lib python3 -m fedimiss export\n"
        "原始错误：%s" % _exc
    )

from . import __version__

S = pa.string()
I = pa.int64()

# (表名, [(字段, 类型)]) —— 字段顺序即 Parquet 列顺序
SCHEMAS: Dict[str, Tuple[str, List[Tuple[str, Any]]]] = {
    "instances": (
        "instances",
        [
            ("instance_domain", S), ("software_name", S), ("software_version", S),
            ("nodeinfo_url", S), ("users_total", I), ("local_posts", I),
            ("open_registrations", I), ("disable_local_timeline", I),
            ("disable_global_timeline", I), ("discovery_source", S), ("checked_at", S),
            ("api_status", S), ("api_robots_status", I), ("api_robots_note", S),
            ("content_signals", S), ("following_api", S), ("followers_api", S),
            ("local_timeline_api", S), ("collection_status", S), ("failure_reason", S),
        ],
    ),
    "accounts": (
        "accounts",
        [
            ("account_key", S), ("username", S), ("host", S), ("instance_domain", S),
            ("local_user_id", S), ("account_uri", S), ("url", S), ("display_name", S),
            ("description", S), ("created_at", S), ("followers_count", I),
            ("following_count", I), ("notes_count", I), ("is_bot_declared", I),
            ("is_locked", I), ("avatar_url", S), ("banner_url", S),
            ("avatar_blurhash", S), ("banner_blurhash", S), ("background_url", S),
            ("is_cat", I), ("is_admin", I), ("is_moderator", I), ("is_indexable", I),
            ("is_suspended", I), ("is_silenced", I), ("hide_online_status", I),
            ("no_crawling", I), ("birthday", S), ("location", S), ("language", S),
            ("fields_json", S), ("emojis_json", S), ("raw_profile", S),
            ("profile_collected_at", S),
            ("discovery_instance", S), ("discovery_method", S), ("discovered_at", S),
            ("bfs_depth", I), ("home_instance_status", S), ("bfs_status", S),
            ("profile_status", S), ("notes_status", S), ("last_collected_at", S),
        ],
    ),
    "notes": (
        "notes",
        [
            ("note_key", S), ("note_id", S), ("source_instance", S),
            ("author_account_key", S), ("created_at", S), ("text", S), ("cw", S),
            ("visibility", S), ("lang", S), ("reply_id", S), ("renote_id", S),
            ("reply_account_key", S), ("renote_account_key", S), ("has_media", I),
            ("media_count", I), ("collection_time", S), ("collected_at", S),
            ("window_id", S), ("window_type", S), ("raw_note", S),
        ],
    ),
    "follow_edges": (
        "follow_edges",
        [
            ("edge_key", S), ("source_account_key", S), ("target_account_key", S),
            ("source_instance_domain", S), ("target_instance_domain", S),
            ("direction", S), ("collected_at", S), ("collection_method", S),
            ("source_instance", S), ("window_id", S), ("snapshot_at", S),
        ],
    ),
    "account_id_mapping": (
        "account_id_mapping",
        [
            ("mapping_key", S), ("raw_identifier", S), ("normalized_identifier", S),
            ("source_instance", S), ("resolved_username", S), ("resolved_host", S),
            ("resolved_local_user_id", S), ("resolution_method", S),
            ("resolution_evidence", S), ("confidence", S), ("resolved_at", S),
            ("failure_reason", S),
        ],
    ),
    "media": (
        "media",
        [
            ("media_key", S), ("note_key", S), ("note_id", S), ("source_instance", S),
            ("author_account_key", S), ("file_id", S), ("url", S), ("thumbnail_url", S),
            ("type", S), ("name", S), ("size", I), ("md5", S), ("blurhash", S),
            ("is_sensitive", I), ("width", I), ("height", I), ("media_host", S),
            ("robots_state", S), ("download_status", S), ("local_path", S),
            ("downloaded_bytes", I), ("sha256", S), ("attempted_at", S), ("error", S),
            ("collected_at", S), ("window_id", S),
        ],
    ),
    "account_snapshots": (
        "account_snapshots",
        [
            ("snapshot_key", S), ("account_key", S), ("window_id", S), ("snapshot_at", S),
            ("profile_json", S), ("display_name", S), ("description", S),
            ("followers_count", I), ("following_count", I), ("notes_count", I), ("raw_profile", S),
        ],
    ),
    "relationship_snapshots": (
        "relationship_snapshots",
        [
            ("snapshot_key", S), ("source_account_key", S), ("target_account_key", S),
            ("source_instance_domain", S), ("target_instance_domain", S), ("direction", S),
            ("window_id", S), ("snapshot_at", S), ("collection_method", S),
        ],
    ),
    "instance_snapshots": (
        "instance_snapshots",
        [
            ("snapshot_key", S), ("instance_domain", S), ("window_id", S), ("snapshot_at", S),
            ("metadata_json", S), ("software_name", S), ("software_version", S),
            ("users_total", I), ("local_posts", I),
        ],
    ),
    "collection_runs": (
        "collection_runs",
        [
            ("run_key", S), ("window_id", S), ("window_type", S), ("start_at", S),
            ("end_at", S), ("status", S), ("requests", I), ("successes", I),
            ("failures", I), ("instance_domain", S), ("available_range_start", S),
            ("available_range_end", S), ("notes", S),
        ],
    ),
}


def _fetch(store, table: str, fields: Sequence[Tuple[str, Any]]) -> List[Dict[str, Any]]:
    columns = ", ".join(name for name, _ in fields)
    rows = store._rows("SELECT %s FROM %s" % (columns, table))
    return [{name: row[name] for name, _ in fields} for row in rows]


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def export_all(store, config, out_dir: str = None, logger=None) -> Dict[str, Any]:
    out = out_dir or config.root
    os.makedirs(out, exist_ok=True)
    written: List[Dict[str, Any]] = []

    for table, (source, fields) in SCHEMAS.items():
        rows = _fetch(store, source, fields)
        arrow_schema = pa.schema([pa.field(name, typ) for name, typ in fields])
        arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
        path = os.path.join(out, "%s.parquet" % table)
        pq.write_table(arrow_table, path, compression="zstd")
        written.append(
            {
                "file": os.path.basename(path),
                "rows": arrow_table.num_rows,
                "columns": len(fields),
                "bytes": os.path.getsize(path),
                "sha256": _sha256(path),
            }
        )
        if logger:
            logger.info("导出 %-22s %8d 行 -> %s", os.path.basename(path), arrow_table.num_rows, path)

    manifest = _manifest(store, config, written)
    manifest_path = os.path.join(out, "collection_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
    if logger:
        logger.info("导出清单 -> %s", manifest_path)
    return manifest


def _manifest(store, config, written: List[Dict[str, Any]]) -> Dict[str, Any]:
    instances = store.instances()
    return {
        "tool": "FediMiss",
        "tool_version": __version__,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "config_version": store.get_meta("config_version"),
        "config": config.summary(),
        "seed_instances": config.data["seed_instances"],
        "counts": store.counts(),
        "instance_count": len(instances),
        "collected_instances": [
            {
                "instance_domain": row.get("instance_domain"),
                "software_name": row.get("software_name"),
                "software_version": row.get("software_version"),
                "users_total": row.get("users_total"),
                "api_status": row.get("api_status"),
                "collection_status": row.get("collection_status"),
            }
            for row in instances
        ],
        "outputs": written,
        "notes": [
            "所有内容均为公开接口可读数据，抓取前逐 host 校验 robots.txt 与 Content-Signal。",
            "账号归属以原属实例为准（home_instance_status / account_id_mapping.confidence），"
            "无法可靠解析者标为 unresolved，不做猜测。",
            "is_bot_declared 仅为平台自我声明字段，不是最终 bot/human 标签，需人工标注。",
        ],
    }


__all__ = ["export_all", "SCHEMAS"]
