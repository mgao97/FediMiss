"""FediMiss 命令行入口（阶段 A/B/C 通用）。

子命令：
    discover   探测并校验候选实例（NodeInfo + robots.txt + /api/meta）
    collect    账号发现 → BFS 关注关系 → 资料 → 帖子（可中断、可续采）
    media      下载附件（默认关闭，仅保留元数据）
    export     导出 Parquet 快照与 collection_manifest.json
    report     生成 data_quality_report.md
    status     打印当前 store 现状

示例：
    python -m fedimiss --config config.json discover
    python -m fedimiss --config config.json collect --workers 4
    python -m fedimiss --config config.json export
    PYTHONPATH=/users/Min/FediMiss/.vendor/lib python3 -m fedimiss --config config.json export
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

from . import __version__
from .collect import InstanceCollector, mark_out_of_scope
from .config import load as load_config
from .discovery import Discovery
from .http import HttpClient
from .identity import IdentityResolver
from .robots import RobotsPolicy
from .store import Store


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fedimiss", description="Misskey 多实例公开数据采集工具")
    parser.add_argument("--config", default="config.json", help="配置文件路径（JSON）")
    parser.add_argument("--log-level", default="INFO", help="日志级别")
    parser.add_argument("--log-file", default=None, help="日志文件（默认 <storage.root>/fedimiss.log）")
    parser.add_argument("--version", action="version", version="FediMiss %s" % __version__)

    sub = parser.add_subparsers(dest="command")

    p_discover = sub.add_parser("discover", help="探测并校验候选实例")
    p_discover.add_argument("--hosts", default=None, help="逗号分隔的实例域名（覆盖配置种子）")

    p_collect = sub.add_parser("collect", help="采集：账号发现 → BFS → 资料 → 帖子")
    p_collect.add_argument("--instances", default=None, help="逗号分隔的实例域名（只采这些）")
    p_collect.add_argument("--workers", type=int, default=None, help="并行实例 worker 数")
    p_collect.add_argument("--seed-pages", type=int, default=3, help="每实例本地时间线翻页数（账号发现）")
    p_collect.add_argument("--no-discover", action="store_true", help="不重新校验实例，直接用已有清单")
    p_collect.add_argument("--refresh", action="store_true", help="重新领取并刷新资料、帖子和关系任务")

    sub.add_parser("media", help="下载附件（需 media.download=true）")

    p_export = sub.add_parser("export", help="导出 Parquet 与清单")
    p_export.add_argument("--out", default=None, help="输出目录（默认 storage.root）")

    p_report = sub.add_parser("report", help="生成数据质量报告")
    p_report.add_argument("--out", default=None, help="输出目录（默认 storage.root）")

    sub.add_parser("status", help="打印当前 store 现状")
    return parser


def setup_logging(level: str, log_file: str):
    logger = logging.getLogger("fedimiss")
    logger.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


def _config_version(path: str) -> str:
    if not path or not os.path.exists(path):
        return "unknown"
    with open(path, "rb") as handle:
        return hashlib.sha256(handle.read()).hexdigest()[:16]


def _split(value):
    if not value:
        return None
    return [item.strip().lower() for item in value.split(",") if item.strip()]


class Context:
    """一次性装配配置 / 日志 / robots / http / store / 身份解析。"""

    def __init__(self, args):
        self.args = args
        self.config = load_config(args.config)
        log_file = args.log_file or os.path.join(self.config.root, "fedimiss.log")
        self.logger = setup_logging(args.log_level, log_file)
        self.store = Store(self.config.db_path)
        self.store.set_meta("config_version", _config_version(args.config))
        self.robots = RobotsPolicy(
            user_agent=self.config.user_agent,
            timeout=self.config.rate("request_timeout", 30.0),
            logger=self.logger,
        )
        self.http = HttpClient(
            user_agent=self.config.user_agent,
            robots=self.robots,
            timeout=self.config.rate("request_timeout", 30.0),
            min_interval=self.config.rate("min_interval_seconds", 1.2),
            max_retries=int(self.config.rate("max_retries", 3)),
            logger=self.logger,
        )
        self.stop_event = threading.Event()
        self.identity = IdentityResolver(
            self.store, self.http, self.robots, self.config, self.logger
        )
        self.discovery = Discovery(
            self.store, self.http, self.robots, self.config, self.identity,
            self.logger, self.stop_event,
        )

    def install_signal_handlers(self):
        def handler(signum, _frame):
            self.logger.warning("收到信号 %s：停止领取新任务，等待当前批次收尾……", signum)
            self.stop_event.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def close(self):
        self.store.close()


# ---------------------------------------------------------------------- 子命令
def cmd_discover(ctx: Context) -> int:
    hosts = _split(ctx.args.hosts) or ctx.config.data["seed_instances"]
    if not hosts:
        ctx.logger.error("未配置 seed_instances，且未通过 --hosts 指定实例")
        return 2
    selected = ctx.discovery.validate_instances(hosts)
    ctx.logger.info("可采集实例 %d 个：%s", len(selected), ", ".join(selected) or "-")
    for row in ctx.store.instances():
        ctx.logger.info(
            "  %-28s %-10s %-12s users=%-8s status=%-10s %s",
            row["instance_domain"], row.get("software_name") or "-",
            (row.get("software_version") or "-")[:12], row.get("users_total") or 0,
            row.get("collection_status"), (row.get("failure_reason") or "")[:60],
        )
    return 0


def cmd_collect(ctx: Context) -> int:
    ctx.install_signal_handlers()
    args = ctx.args

    from .time_windows import longitudinal_windows
    try:
        longitudinal_windows(ctx.config)
    except ValueError as exc:
        ctx.logger.error("采集拒绝启动：%s", exc)
        return 2

    # 断点续采：把上次进程崩溃/被中断留下的 running 回滚为 pending
    recovered = ctx.store.reset_running(["bfs_status", "profile_status", "notes_status"])
    if recovered:
        ctx.logger.info("恢复上次未完成任务 %d 个（running → pending）", recovered)
    if getattr(args, "refresh", False):
        refreshed = ctx.store._exec(
            "UPDATE accounts SET bfs_status='pending' "
            "WHERE bfs_status IN ('completed','unavailable') AND home_instance_status!='out_of_scope'"
        ).rowcount or 0
        refreshed_profiles = ctx.store._exec(
            "UPDATE accounts SET profile_status='pending' "
            "WHERE profile_status IN ('completed','error') AND home_instance_status!='out_of_scope'"
        ).rowcount or 0
        refreshed_notes = ctx.store._exec(
            "UPDATE accounts SET notes_status='pending' "
            "WHERE notes_status IN ('completed','error') AND home_instance_status!='out_of_scope'"
        ).rowcount or 0
        ctx.logger.info(
            "强制刷新任务：BFS %d / 资料 %d / 帖子 %d（旧数据保留，新增媒体链接继续写入）",
            refreshed, refreshed_profiles, refreshed_notes,
        )

    instances = _split(args.instances)
    if not instances:
        if args.no_discover:
            instances = ctx.discovery.active_instances()
        else:
            instances = ctx.discovery.validate_instances()
        if not instances:
            ctx.logger.error("没有可采集实例，请先运行 discover 或检查 seed_instances")
            return 2
    else:
        known = {r["instance_domain"] for r in ctx.store.instances()}
        unknown = [h for h in instances if h not in known]
        if unknown:
            ctx.discovery.validate_instances(unknown)

    # 账号种子
    ctx.discovery.seed_from_config()
    per_instance_cap = ctx.config.limit("max_accounts_per_instance")
    for domain in instances:
        if ctx.stop_event.is_set():
            break
        remaining = ctx.config.limit("max_accounts_total") - ctx.store.count_accounts()
        if remaining <= 0:
            ctx.logger.info("账号总数已达预算 %d，停止账号发现", ctx.config.limit("max_accounts_total"))
            break
        ctx.discovery.seed_from_timeline(
            domain, max_pages=max(1, args.seed_pages), budget=min(per_instance_cap, remaining)
        )

    dropped = mark_out_of_scope(ctx.store, instances)
    if dropped:
        ctx.logger.info("本轮目标之外的账号标记 out_of_scope：%d 个任务", dropped)

    workers = args.workers or int(ctx.config.get("workers.instance_workers", 4))
    workers = max(1, min(workers, len(instances)))
    ctx.logger.info("开始采集 %d 个实例（并行度 %d）：%s", len(instances), workers, ", ".join(instances))

    totals = {"bfs": 0, "profiles": 0, "notes": 0}

    def work(domain: str):
        collector = InstanceCollector(
            ctx.store, ctx.http, ctx.robots, ctx.config, ctx.identity, domain,
            logger=ctx.logger, stop_event=ctx.stop_event,
        )
        return collector.run()

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for result in pool.map(work, instances):
                for key in totals:
                    totals[key] += result.get(key, 0)
    finally:
        if ctx.stop_event.is_set():
            again = ctx.store.reset_running(["bfs_status", "profile_status", "notes_status"])
            ctx.logger.warning("已中断：回滚未完成任务 %d 个，重跑 collect 即可续采", again)

    ctx.logger.info("采集结束：BFS %d / 资料 %d / 帖子 %d", totals["bfs"], totals["profiles"], totals["notes"])
    for key, value in ctx.store.counts().items():
        ctx.logger.info("   %-14s %d", key, value)
    return 0


def cmd_media(ctx: Context) -> int:
    from .media import MediaCollector

    ctx.install_signal_handlers()
    if not ctx.config.media("download", False):
        pending = ctx.store.count_media("pending")
        ctx.logger.info("media.download=false，跳过下载；附件元数据 %d 条已保留", pending)
        return 0
    collector = MediaCollector(
        ctx.store, ctx.http, ctx.robots, ctx.config, ctx.logger, ctx.stop_event
    )
    stats = collector.run()
    ctx.logger.info("媒体下载结束：%s", stats or "无待下载项")
    return 0


def cmd_export(ctx: Context) -> int:
    from .export import export_all

    manifest = export_all(ctx.store, ctx.config, out_dir=ctx.args.out, logger=ctx.logger)
    ctx.logger.info("导出完成，共 %d 个文件", len(manifest["outputs"]))
    return 0


def cmd_report(ctx: Context) -> int:
    from .quality import write_report

    path = write_report(ctx.store, ctx.config, out_dir=ctx.args.out, logger=ctx.logger)
    ctx.logger.info("数据质量报告 -> %s", path)
    return 0


def cmd_status(ctx: Context) -> int:
    counts = ctx.store.counts()
    ctx.logger.info("统计：%s", counts)
    for row in ctx.store.instances():
        ctx.logger.info(
            "  %-28s users=%-8s api=%-6s collect=%-10s following=%-10s followers=%-10s",
            row["instance_domain"], row.get("users_total") or 0, row.get("api_status"),
            row.get("collection_status"), row.get("following_api") or "-", row.get("followers_api") or "-",
        )
    pending = {col: ctx.store.accounts_pending(col) for col in
               ("bfs_status", "profile_status", "notes_status")}
    ctx.logger.info("待处理：%s", pending)
    return 0


COMMANDS = {
    "discover": cmd_discover,
    "collect": cmd_collect,
    "media": cmd_media,
    "export": cmd_export,
    "report": cmd_report,
    "status": cmd_status,
}


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 1

    try:
        ctx = Context(args)
    except FileNotFoundError:
        print("配置文件不存在: %s" % args.config, file=sys.stderr)
        return 2

    ctx.logger.info("FediMiss %s | 命令=%s | %s", __version__, args.command, ctx.config.summary())
    try:
        return COMMANDS[args.command](ctx)
    except KeyboardInterrupt:
        ctx.logger.warning("被用户中断")
        return 130
    finally:
        ctx.close()


if __name__ == "__main__":
    sys.exit(main())
