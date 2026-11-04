"""数据质量与偏差报告（需求 §9）。

每次采集生成 ``data_quality_report.md``，逐项回答需求列出的 8 个问题，
并显式声明「公开时间线发现 + BFS 扩展 + API 可访问性」带来的选择偏差。

原则：**只报可核验的数字**。任何无法从 store 直接算出的指标，宁可写
「未采集」也不估算。
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Sequence

# FediDB 记录的整个 Misskey 网络规模（2026-10 实测），仅作覆盖度分母参照。
FEDIDB_MISSKEY = {
    "source": "https://api.fedidb.org/v1/software/misskey",
    "instance_count": 1171,
    "user_count": 1249820,
    "checked_at": "2026-10",
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _rows(store, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    return [dict(r) for r in store._rows(sql, params)]


def _scalar(store, sql: str, params: Sequence[Any] = ()) -> Any:
    row = store._one(sql, params)
    return list(row)[0] if row else None


def _pct(part: Any, total: Any) -> str:
    try:
        p = float(part or 0)
        t = float(total or 0)
    except (TypeError, ValueError):
        return "n/a"
    if t <= 0:
        return "n/a"
    return "%.2f%%" % (100.0 * p / t)


def _table(headers: Sequence[str], rows: Iterable) -> List[str]:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(c) for c in row) + " |")
    return out


def build_report(store, config) -> str:
    lines: List[str] = []
    add = lines.append

    counts = store.counts()
    instances = store.instances()
    collected = [r for r in instances if r.get("api_status") == "ok"
                 and r.get("collection_status") != "deferred"]

    add("# FediMiss 数据质量与偏差报告")
    add("")
    add("- 生成时间：%s" % _now())
    add("- 配置版本：%s" % (store.get_meta("config_version") or "未记录"))
    add("- 采集范围：%d 个实例（可采集 %d 个）" % (len(instances), len(collected)))
    add("- 用途声明：purpose=%s，respect_content_signals=%s" % (
        config.purpose, config.respect_content_signals))
    add("")

    # ------------------------------------------------------------ 1 实例
    add("## 1. 实例数量、状态与软件版本分布")
    add("")
    add("| 指标 | 值 |")
    add("| --- | --- |")
    add("| 入库实例数 | %d |" % len(instances))
    add("| 本轮可采集实例数 | %d |" % len(collected))
    add("| 采集完成实例数 | %d |" % sum(
        1 for r in instances if r.get("collection_status") == "completed"))
    add("")
    add("**采集状态分布**")
    add("")
    for row in _table(("collection_status", "api_status", "实例数"), [
        (r.get("_s") or "unknown", r["api_status"], r["n"])
        for r in _rows(store,
                       "SELECT COALESCE(collection_status,'unknown') AS _s, "
                       "COALESCE(api_status,'unknown') AS api_status, COUNT(*) AS n "
                       "FROM instances GROUP BY _s, api_status ORDER BY n DESC")
    ]):
        add(row)
    add("")
    add("**软件版本分布**")
    add("")
    for row in _table(("software_name", "software_version", "实例数"), [
        (r["software_name"] or "unknown", r["software_version"] or "unknown", r["n"])
        for r in _rows(store,
                       "SELECT software_name, software_version, COUNT(*) AS n FROM instances "
                       "GROUP BY software_name, software_version ORDER BY n DESC")
    ]):
        add(row)
    add("")
    not_ok = [r for r in instances if r.get("api_status") != "ok"]
    if not_ok:
        add("**未通过校验的实例（不可采集，已标记，不再反复请求）**")
        add("")
        for row in _table(("实例", "api_status", "failure_reason"), [
            (r["instance_domain"], r["api_status"], (r.get("failure_reason") or "")[:120])
            for r in not_ok
        ]):
            add(row)
        add("")

    # ------------------------------------------------------------ 2 账号
    total_accounts = counts["accounts"]
    active = store.count_accounts("profile_status='completed'")
    add("## 2. 发现账号数、去重账号数与活跃账号数")
    add("")
    add("| 指标 | 值 |")
    add("| --- | --- |")
    add("| 账号总数（accounts，主键去重后） | %d |" % total_accounts)
    add("| 本机去重：account_key=username@home_host 唯一 | 是（主键约束） |")
    add("| 已抓取资料（profile_status=completed） | %d |" % active)
    add("| 已抓取帖子（notes_status=completed） | %d |" % store.count_accounts("notes_status='completed'"))
    add("| 已遍历关系（bfs_status=completed） | %d |" % store.count_accounts("bfs_status='completed'"))
    add("| BFS 队列剩余（bfs_status=pending） | %d |" % store.count_accounts("bfs_status='pending'"))
    add("")
    add("说明：**账号表只收录「原属实例可判定」的账号**；无法判定归属的候选只写 "
        "`account_id_mapping`（confidence=unresolved），不进入 accounts，避免污染账号表。")
    add("")

    # ------------------------------------------------------------ 3 归属
    add("## 3. 原属实例可确认率与未知归属比例")
    add("")
    mapping_total = counts["mappings"]
    add("**账号归属证据等级（accounts.home_instance_status）**")
    add("")
    for row in _table(("home_instance_status", "账号数", "占比"), [
        (r["k"] or "unknown", r["n"], _pct(r["n"], total_accounts))
        for r in _rows(store,
                       "SELECT COALESCE(home_instance_status,'unknown') AS k, COUNT(*) AS n "
                       "FROM accounts GROUP BY k ORDER BY n DESC")
    ]):
        add(row)
    add("")
    add("**映射表置信度（account_id_mapping.confidence，共 %d 条）**" % mapping_total)
    add("")
    for row in _table(("confidence", "条数", "占比"), [
        (r["k"] or "unknown", r["n"], _pct(r["n"], mapping_total))
        for r in _rows(store,
                       "SELECT COALESCE(confidence,'unknown') AS k, COUNT(*) AS n "
                       "FROM account_id_mapping GROUP BY k ORDER BY n DESC")
    ]):
        add(row)
    add("")
    verified = store.count_mappings("verified")
    unresolved = store.count_mappings("unresolved")
    add("| 指标 | 值 |")
    add("| --- | --- |")
    add("| verified（在原属实例确认 host=null） | %d（%s） |" % (verified, _pct(verified, mapping_total)))
    add("| unresolved（无法可靠判定，未猜测） | %d（%s） |" % (unresolved, _pct(unresolved, mapping_total)))
    add("| 归属未经原属实例验证的账号比例 | %s |" % _pct(total_accounts - verified, total_accounts))
    add("")
    add("口径：`verified` 表示在**账号原属实例**上查到该账号且返回 `host == null`；"
        "`declared` 表示原属实例来自远端缓存对象的 `host` 字段（归属已知、本地 ID 未知）；"
        "`inferred` 表示仅由 `uri` 推断；`unresolved` 表示无法判定，留空不猜。")
    add("")

    # ------------------------------------------------------------ 4 帖子
    add("## 4. 帖子数量、时间跨度与每账号分布")
    add("")
    span = _rows(store, "SELECT MIN(created_at) AS lo, MAX(created_at) AS hi FROM notes")
    lo = span[0]["lo"] if span else None
    hi = span[0]["hi"] if span else None
    per_account = _rows(store,
                        "SELECT COUNT(*) AS n FROM notes GROUP BY author_account_key")
    values = sorted(int(r["n"]) for r in per_account)
    add("| 指标 | 值 |")
    add("| --- | --- |")
    add("| 帖子总数 | %d |" % counts["notes"])
    add("| 时间跨度 | %s ~ %s |" % (lo or "n/a", hi or "n/a"))
    add("| 有帖子的账号数 | %d |" % len(values))
    if values:
        add("| 每账号帖子数 最小/中位/最大 | %d / %d / %d |" % (
            values[0], values[len(values) // 2], values[-1]))
        add("| 每账号平均帖子数 | %.1f |" % (sum(values) / len(values)))
    add("| account_key 为空（归属未定）的帖子 | %d |" % int(
        _scalar(store, "SELECT COUNT(*) FROM notes WHERE author_account_key IS NULL") or 0))
    add("")
    dist = _rows(store, "SELECT media_count AS k, COUNT(*) AS n FROM notes GROUP BY k ORDER BY k")
    if dist:
        add("**每帖附件数分布**")
        add("")
        for row in _table(("附件数", "帖子数"), [(f"{r['k']}+", r["n"]) for r in dist][:12]):
            add(row)
        add("")

    # ------------------------------------------------------------ 5 关系
    add("## 5. 关注边数量、跨实例比例与关系缺失")
    add("")
    total_edges = counts["follow_edges"]
    cross = int(_scalar(
        store,
        "SELECT COUNT(*) FROM follow_edges WHERE source_instance_domain <> target_instance_domain",
    ) or 0)
    same = total_edges - cross
    unknown_dir = int(_scalar(store, "SELECT COUNT(*) FROM follow_edges WHERE direction IS NULL") or 0)
    add("| 指标 | 值 |")
    add("| --- | --- |")
    add("| 关注边总数 | %d |" % total_edges)
    add("| 同实例边 | %d（%s） |" % (same, _pct(same, total_edges)))
    add("| 跨实例边 | %d（%s） |" % (cross, _pct(cross, total_edges)))
    add("| 方向缺失边 | %d |" % unknown_dir)
    add("| 未完成关系遍历的账号（bfs_status<>completed） | %d |" % store.count_accounts(
        "bfs_status NOT IN ('completed')"))
    add("")
    add("**关系采集能力差异（按实例）**：`users/following` / `users/followers` "
        "匿名可用性因实例而异，被拒绝的实例没有关注边，构成结构性缺失。")
    add("")
    for row in _table(("实例", "following_api", "followers_api", "local_timeline_api"), [
        (r["instance_domain"], r.get("following_api") or "unknown",
         r.get("followers_api") or "unknown", r.get("local_timeline_api") or "unknown")
        for r in instances
    ]):
        add(row)
    add("")

    # ------------------------------------------------------------ 6 API
    add("## 6. API 成功率、字段缺失率、重复率与失败原因")
    add("")
    stats = store.instance_stats()
    req = sum(int(r["requests"] or 0) for r in stats)
    ok = sum(int(r["ok"] or 0) for r in stats)
    err = sum(int(r["errors"] or 0) for r in stats)
    thr = sum(int(r["throttled"] or 0) for r in stats)
    add("| 指标 | 值 |")
    add("| --- | --- |")
    add("| API 请求总数 | %d |" % req)
    add("| 成功 / 失败 | %d / %d |" % (ok, err))
    add("| 请求成功率 | %s |" % _pct(ok, req))
    add("| 429 限流次数 | %d（%s） |" % (thr, _pct(thr, req)))
    add("")
    # 字段缺失率
    def missing(table: str, column: str, label: str):
        total = counts["notes"] if table == "notes" else counts["accounts"]
        n = int(_scalar(store, "SELECT COUNT(*) FROM %s WHERE %s IS NULL OR %s=''" % (
            table, column, column)) or 0)
        return (label, n, _pct(n, total), total)

    add("**关键字段缺失率**")
    add("")
    for row in _table(("字段", "缺失数", "缺失率", "分母"), [
        missing("accounts", "description", "accounts.description"),
        missing("accounts", "created_at", "accounts.created_at"),
        missing("accounts", "followers_count", "accounts.followers_count"),
        missing("notes", "text", "notes.text"),
        missing("notes", "created_at", "notes.created_at"),
    ]):
        add(row)
    add("")
    dup_notes = int(_scalar(store, "SELECT COUNT(*) FROM (SELECT note_id FROM notes "
                                   "GROUP BY note_id HAVING COUNT(*) > 1)") or 0)
    add("| 指标 | 值 |")
    add("| --- | --- |")
    add("| 跨实例重复 note_id 组数 | %d |" % dup_notes)
    add("| 帖子重复率（含转发/跨实例重复） | %s |" % _pct(
        counts["notes"] - int(_scalar(store, "SELECT COUNT(DISTINCT note_id) FROM notes") or 0),
        counts["notes"]))
    add("")
    errs = store.error_counts()
    if errs:
        add("**失败原因分布（errors 表）**")
        add("")
        for row in _table(("阶段", "错误类型", "HTTP", "次数"), [
            (r["stage"], r["error_type"], r["http_status"] if r["http_status"] is not None else "-", r["n"])
            for r in errs[:20]
        ]):
            add(row)
        add("")

    # ------------------------------------------------------------ 7 各实例
    add("## 7. 各实例数据量与账号活跃度分布")
    add("")
    add("| 实例 | 账号数 | 帖子数 | 关注边数 | 已抓资料 | 已抓帖子 |")
    add("| --- | --- | --- | --- | --- | --- |")
    for r in instances:
        domain = r["instance_domain"]
        add("| %s | %d | %d | %d | %d | %d |" % (
            domain,
            store.count_accounts("instance_domain=?", (domain,)),
            int(_scalar(store, "SELECT COUNT(*) FROM notes WHERE source_instance=?", (domain,)) or 0),
            int(_scalar(store, "SELECT COUNT(*) FROM follow_edges WHERE source_instance=?", (domain,)) or 0),
            store.count_accounts("instance_domain=? AND profile_status='completed'", (domain,)),
            store.count_accounts("instance_domain=? AND notes_status='completed'", (domain,)),
        ))
    add("")
    media_types = store.distinct_note_media_types()
    if media_types:
        add("**附件类型分布（元数据）**")
        add("")
        for row in _table(("type", "数量"), media_types[:10]):
            add(row)
        add("")

    # ------------------------------------------------------------ 8 覆盖度
    add("## 8. 样本相对公开实例统计的覆盖情况")
    add("")
    covered_users = sum(int(r.get("users_total") or 0) for r in collected)
    add("| 指标 | 值 |")
    add("| --- | --- |")
    add("| 本轮目标实例声明用户数合计（NodeInfo usage.users.total） | %d |" % covered_users)
    add("| 本轮实际账号数 | %d |" % total_accounts)
    add("| FediDB 记录的全网 Misskey 用户数（参照分母） | %d |" % FEDIDB_MISSKEY["user_count"])
    add("| FediDB 记录的全网 Misskey 实例数（参照分母） | %d |" % FEDIDB_MISSKEY["instance_count"])
    add("| 账号相对全网规模占比 | %s |" % _pct(total_accounts, FEDIDB_MISSKEY["user_count"]))
    add("| 实例相对全网规模占比 | %s |" % _pct(len(collected), FEDIDB_MISSKEY["instance_count"]))
    add("")
    add("参照来源：`%s`（%s 实测，仅作分母，不代表逐实例精确统计）。"
        % (FEDIDB_MISSKEY["source"], FEDIDB_MISSKEY["checked_at"]))
    add("")

    # ------------------------------------------------------------ 偏差
    add("## 选择偏差声明")
    add("")
    add("1. **实例选择偏差**：实例来自配置种子 + 已有实例的 peer 列表，"
        "并按 `users_total` 排序取前 N 个 → 偏向规模大、联通性好的实例。")
    add("2. **公开时间线发现偏差**：账号从 `notes/local-timeline` 发现，"
        "只覆盖「近期有公开本地帖」的账号；只看不发的账号、"
        "关闭本地时间线的实例的账号会被系统性漏掉。")
    add("3. **BFS 扩展偏差**：关注关系是分层扩展的，深度越大覆盖越稀疏；"
        "`max_bfs_depth` / 账号预算 / 请求预算会提前截断，"
        "得到的是**局部连通子图**，不是全网社交图。")
    add("4. **API 可访问性偏差**：`users/following`、`users/followers`、"
        "`notes/local-timeline` 的匿名可用性因实例而异（部分实例返回 400 FORBIDDEN），"
        "被拒绝的实例在关系数据上是结构性缺失，而不是「这些实例没有关系」。")
    add("5. **账号自我声明偏差**：`is_bot_declared` 只是平台字段，"
        "不能当作 bot/human 标签；真实标签依赖后续人工标注。")
    add("")
    add("**结论：本数据集不能宣称是整个 Misskey 网络的代表性样本。**"
        "所有跨实例结论都必须带上「来自哪些实例、通过何种发现路径、"
        "有多少关系数据缺失」的前提。")
    add("")
    return "\n".join(lines) + "\n"


def write_report(store, config, out_dir: str = None, logger=None) -> str:
    out = out_dir or config.root
    os.makedirs(out, exist_ok=True)
    path = os.path.join(out, "data_quality_report.md")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(build_report(store, config))
    if logger:
        logger.info("数据质量报告 -> %s", path)
    return path


__all__ = ["build_report", "write_report"]
