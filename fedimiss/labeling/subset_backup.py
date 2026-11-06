"""把「资料+帖子+媒体」8,775 个标注总体账号的数据与标注产物，单独备份成一套自包含目录。

产物布局（默认 ``data/fedimiss/``）::

    fedimiss_8775.sqlite3          过滤后的完整库（含全部表结构 / 索引 / 原始行）
    parquet/*.parquet              accounts / notes / media / follow_edges / ...
    labeling/                      evidence.jsonl、标签、金标准表、盲审表
    prompts/ label_v1.md           标注 prompt（版本留痕）
    docs/                          标注规范与流水线文档
    PROGRESS.md                    进度与人工待办说明
    manifest.json                  行数统计 + 全部文件 sha256

账号清单来自证据包（``--evidence``，即 ``--require profile,notes,media`` 的产物）。
过滤关系：accounts / notes / media / account_snapshots / account_id_mapping 按账号键，
follow_edges / relationship_snapshots 取任一端在总体内的边，instances 等小表整表保留。

需要 parquet 时用 miss 环境运行（vendored pyarrow 是 cpython-310 构建）::

    /users/Min/miniconda/envs/miss/bin/python \\
        PYTHONPATH=.vendor/lib python3 -m fedimiss.labeling.subset_backup
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from . import EVIDENCE_VERSION

CHUNK = 900

FILTERED_TABLES: Dict[str, str] = {
    # 表名: 过滤列
    "accounts": "account_key",
    "account_snapshots": "account_key",
    "account_id_mapping": "mapping_key",
    "notes": "author_account_key",
    "media": "author_account_key",
}
EDGE_TABLES = ("follow_edges", "relationship_snapshots")
FULL_TABLES = ("instances", "instance_snapshots", "collection_runs", "instance_stats", "meta")

COPY_FILES = (
    ("data/labeling/evidence.jsonl", "labeling/evidence.jsonl"),
    ("data/labeling/labels_mock.jsonl", "labeling/labels_mock.jsonl"),
    ("data/labeling/gold_sheet_sample.csv", "labeling/gold_sheet_sample.csv"),
    ("data/labeling/audit_sheet_sample.csv", "labeling/audit_sheet_sample.csv"),
    ("prompts/label_v1.md", "prompts/label_v1.md"),
    ("docs/annotation_guidelines.md", "docs/annotation_guidelines.md"),
    ("docs/llm_annotation_pipeline.md", "docs/llm_annotation_pipeline.md"),
)

PARQUET_TABLES = ("accounts", "notes", "media", "follow_edges", "account_id_mapping",
                  "account_snapshots", "relationship_snapshots", "instances")

POPULATION_DEF = ("profile_status='completed' 且有帖子且有媒体记录（--require profile,notes,media）；"
                  "账号清单来自 evidence.jsonl")


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def load_account_keys(evidence_path: str) -> List[str]:
    keys = []
    with open(evidence_path, "r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                keys.append(json.loads(line)["account_key"])
    if not keys:
        raise SystemExit("证据包为空：%s" % evidence_path)
    return keys


def _chunks(items: Sequence[str]) -> Iterable[Sequence[str]]:
    for start in range(0, len(items), CHUNK):
        yield items[start:start + CHUNK]


def copy_schema(source: sqlite3.Connection, dest: sqlite3.Connection) -> None:
    for row in source.execute(
        "SELECT type, name, sql FROM sqlite_master "
        "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' "
        "ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END, name"
    ):
        dest.execute(row["sql"])


def copy_rows(source: sqlite3.Connection, dest: sqlite3.Connection,
              keys: Sequence[str]) -> Dict[str, int]:
    counts: Dict[str, int] = {}

    def insert(table: str, rows: Iterable[sqlite3.Row]) -> int:
        n = 0
        buffer: List[Tuple] = []
        first = True
        columns: List[str] = []
        for row in rows:
            if first:
                columns = list(row.keys())
                first = False
            buffer.append(tuple(row))
            if len(buffer) >= 5000:
                dest.executemany(
                    "INSERT OR REPLACE INTO %s (%s) VALUES (%s)"
                    % (table, ",".join(columns), ",".join("?" * len(columns))), buffer)
                n += len(buffer)
                buffer = []
        if first:
            return 0
        if buffer:
            dest.executemany(
                "INSERT OR REPLACE INTO %s (%s) VALUES (%s)"
                % (table, ",".join(columns), ",".join("?" * len(columns))), buffer)
            n += len(buffer)
        return n

    for table, column in FILTERED_TABLES.items():
        total = 0
        for chunk in _chunks(keys):
            placeholders = ",".join("?" for _ in chunk)
            rows = source.execute(
                f"SELECT * FROM {table} WHERE {column} IN ({placeholders})", tuple(chunk))
            total += insert(table, rows)
        counts[table] = total

    for table in EDGE_TABLES:
        total = 0
        for chunk in _chunks(keys):
            placeholders = ",".join("?" for _ in chunk)
            rows = source.execute(
                f"SELECT * FROM {table} WHERE source_account_key IN ({placeholders}) "
                f"OR target_account_key IN ({placeholders})", tuple(chunk) + tuple(chunk))
            total += insert(table, rows)
        counts[table] = total

    for table in FULL_TABLES:
        try:
            counts[table] = insert(table, source.execute(f"SELECT * FROM {table}"))
        except sqlite3.OperationalError:  # 表不存在则跳过
            counts[table] = 0

    # 边表行可能因「两端在不同分块」被访问两次（INSERT OR REPLACE 去重），
    # 最终计数以目标库实际行数为准
    for table in counts:
        try:
            counts[table] = dest.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except sqlite3.OperationalError:
            counts[table] = 0
    return counts


def _unify_column(values: List[Any]) -> Tuple[Any, List[Any]]:
    """SQLite 动态类型会让不同批次的列推断出不同 Arrow 类型（如全 NULL 批次推断成
    double），这里按整列统一：int→int64，含 float→float64，其余→string。"""
    import pyarrow as pa
    types = {type(v) for v in values if v is not None}
    if types <= {int}:
        return pa.int64(), values
    if types <= {int, float}:
        return pa.float64(), [float(v) if v is not None else None for v in values]
    return pa.string(), [None if v is None else str(v) for v in values]


def export_parquet(dest_path: str, out_dir: str) -> Dict[str, int]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)  # 清掉上次失败留下的残缺文件
    os.makedirs(out_dir)
    counts: Dict[str, int] = {}
    conn = sqlite3.connect(dest_path)
    conn.row_factory = sqlite3.Row
    try:
        for table in PARQUET_TABLES:
            cursor = conn.execute(f"SELECT * FROM {table}")
            columns = [d[0] for d in cursor.description]
            writer: Optional[pq.ParquetWriter] = None
            schema: Optional[pa.Schema] = None
            n = 0
            while True:
                rows = cursor.fetchmany(100_000)
                if not rows:
                    break
                arrays: Dict[str, List[Any]] = {col: [row[col] for row in rows]
                                                for col in columns}
                if schema is None:  # 用首批数据 + 整列统一规则确定类型
                    fields = []
                    for col in columns:
                        arrow_type, arrays[col] = _unify_column(arrays[col])
                        fields.append(pa.field(col, arrow_type))
                    schema = pa.schema(fields)
                    writer = pq.ParquetWriter(
                        os.path.join(out_dir, "%s.parquet" % table), schema,
                        compression="zstd")
                else:  # 后续批次按同一 schema 强制转换
                    for col, field in zip(columns, schema):
                        if field.type == pa.int64():
                            arrays[col] = [int(v) if v is not None else None
                                           for v in arrays[col]]
                        elif field.type == pa.float64():
                            arrays[col] = [float(v) if v is not None else None
                                           for v in arrays[col]]
                        else:
                            arrays[col] = [None if v is None else str(v)
                                           for v in arrays[col]]
                batch = pa.Table.from_pydict(arrays, schema=schema)
                writer.write_table(batch)
                n += len(rows)
            if writer is not None:
                writer.close()
            counts[table] = n
    finally:
        conn.close()
    return counts


PROGRESS_TEMPLATE = """# 8,775 标注总体：进度与人工待办

备份时间：{now}
总体口径：{population}
来源数据库：{source_db}

## 当前进度

| 阶段 | 状态 |
| --- | --- |
| 数据采集 | 已完成（8,775 账号全量证据包已构建） |
| 证据包（evidence.jsonl） | {n_accounts} 个账号，evidence_version={ev} |
| LLM 预标注 | **尚未开始**（labels_mock.jsonl 只是规则基线自检，不是模型标签，不可用于研究） |
| 人工金标准 | **尚未开始**（gold_sheet_sample.csv 只是格式样例，待导出正式 400 份双人盲标表） |
| 一致性验收 | 尚未开始（依赖金标准） |

## 还需要人工做的事（无法省略）

1. **双人独立标注金标准**：约 400 个账号（分层抽样），按 docs/annotation_guidelines.md 打
   bot / human / uncertain 标签，填 gold_sheet.csv 的 annotator_a / annotator_b 列。
   这是评估 LLM 的唯一基准，模型输出不能替代。
2. **仲裁分歧**：双人标签不一致的账号由第三人裁决，填 adjudicated 列。
3. **接入本地模型跑预标注**（可选但推荐）：设置 FEDIMISS_LLM_BASE_URL 指向 vLLM/Ollama，
   运行 docs/llm_annotation_pipeline.md 的命令；用 agreement.py 对照金标准验收
   （建议 bot 精确率 ≥ 0.90、κ ≥ 0.70）后才允许全量预标注。
4. **盲审**：全量预标注后按标签分层抽样 ≥10% 人工复核，估计总体错误率。

## 文件说明

- `fedimiss_8775.sqlite3`：过滤后的完整数据库（结构与主库一致）
- `parquet/`：同数据的列式快照
- `labeling/evidence.jsonl`：8,775 个账号的证据包（人工标注与 LLM 共用同一份证据）
- `labeling/labels_mock.jsonl`：规则基线自检输出（勿当真实标签）
- `prompts/label_v1.md` + `docs/`：标注规范与判定规则的版本快照
"""


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="备份 8,775 标注总体的数据与标注产物")
    parser.add_argument("--db", default="data/fedimiss.sqlite3")
    parser.add_argument("--out", default="data/fedimiss")
    parser.add_argument("--evidence", default="data/labeling/evidence.jsonl")
    parser.add_argument("--no-parquet", action="store_true")
    args = parser.parse_args(argv)

    root = os.path.abspath(args.out)
    if os.path.exists(root):
        if any(os.scandir(root)):  # 有内容才拒绝，空目录（可能是残留）允许复用
            raise SystemExit("目标目录已存在且非空，避免覆盖：%s" % root)
    else:
        os.makedirs(root)

    keys = load_account_keys(args.evidence)
    dest_path = os.path.join(root, "fedimiss_8775.sqlite3")

    source = sqlite3.connect(args.db)
    source.row_factory = sqlite3.Row
    source.execute("PRAGMA busy_timeout = 10000")
    dest = sqlite3.connect(dest_path)
    try:
        copy_schema(source, dest)
        counts = copy_rows(source, dest, keys)
        dest.commit()
    finally:
        dest.close()
        source.close()

    parquet_counts: Dict[str, int] = {}
    if not args.no_parquet:
        try:
            parquet_counts = export_parquet(dest_path, os.path.join(root, "parquet"))
        except ImportError as exc:
            print("警告：pyarrow 不可用，跳过 parquet（%s）" % exc, file=sys.stderr)

    copied = []
    for src, dst in COPY_FILES:
        src_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))), src)
        if os.path.exists(src_path):
            target = os.path.join(root, dst)
            os.makedirs(os.path.dirname(target), exist_ok=True)
            shutil.copy2(src_path, target)
            copied.append(dst)

    manifest = {
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_db": os.path.abspath(args.db),
        "population": POPULATION_DEF,
        "population_definition_evidence_version": EVIDENCE_VERSION,
        "n_accounts": len(keys),
        "row_counts": counts,
        "parquet_row_counts": parquet_counts,
        "labeling_status": {
            "evidence": "done",
            "llm_labels": "mock_baseline_only",
            "human_gold": "not_started",
        },
        "copied_artifacts": copied,
        "files": {},
    }
    for base, _dirs, files in os.walk(root):
        for name in sorted(files):
            path = os.path.join(base, name)
            rel = os.path.relpath(path, root)
            if rel != "manifest.json":
                manifest["files"][rel] = {
                    "bytes": os.path.getsize(path),
                    "sha256": sha256_file(path),
                }
    with open(os.path.join(root, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=2)

    progress = PROGRESS_TEMPLATE.format(
        now=manifest["created_at"], population=POPULATION_DEF,
        source_db=manifest["source_db"], n_accounts=len(keys), ev=EVIDENCE_VERSION)
    with open(os.path.join(root, "PROGRESS.md"), "w", encoding="utf-8") as fh:
        fh.write(progress)

    print(json.dumps({"out": root, "n_accounts": len(keys), "row_counts": counts,
                      "parquet": parquet_counts, "artifacts": copied},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
