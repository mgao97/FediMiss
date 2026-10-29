"""FediMiss 命令行入口。

用法示例：
    python -m fedimiss --config config.json --out data
    python -m fedimiss --config config.json --preflight      # 仅做 robots/合规预检
    python -m fedimiss --config config.json --limit 50 --no-media
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys

from . import __version__
from .api import MisskeyApi
from .config import Config
from .crawler import Crawler
from .http import HttpClient, HttpError
from .robots import RobotsPolicy
from .storage import Storage, now_iso


def build_parser():
    p = argparse.ArgumentParser(prog="fedimiss", description="Misskey 多实例多模态数据抓取工具")
    p.add_argument("--config", default="config.json", help="配置文件路径（JSON）")
    p.add_argument("--out", default="data", help="数据输出目录")
    p.add_argument("--limit", type=int, default=None, help="每个账户抓取的帖子数上限（覆盖配置）")
    p.add_argument("--media", dest="media", action="store_true", default=None, help="下载图片")
    p.add_argument("--no-media", dest="media", action="store_false", help="不下载图片，仅保存元数据")
    p.add_argument("--purpose", default=None, help="数据用途：research | ai-training")
    p.add_argument("--min-interval", type=float, default=None, help="同一 host 的最小请求间隔（秒）")
    p.add_argument("--preflight", action="store_true", help="仅执行 robots/合规预检并输出报告")
    p.add_argument("--log-level", default="INFO", help="日志级别")
    p.add_argument("--log-file", default=None, help="日志文件路径（默认写入 <out>/fedimiss.log）")
    p.add_argument("--version", action="version", version="FediMiss %s" % __version__)
    return p


def setup_logging(level, log_file):
    logger = logging.getLogger("fedimiss")
    logger.setLevel(getattr(logging, str(level).upper(), logging.INFO))
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)
    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def run_preflight(config, storage, robots, logger):
    http = HttpClient(
        user_agent=config.user_agent,
        robots=robots,
        timeout=config.request_timeout,
        min_interval=config.min_interval_seconds,
        max_retries=config.max_retries,
        logger=logger,
    )
    report = []
    for inst in config.instances:
        host = inst["host"]
        api = MisskeyApi(host, http)
        rules = robots.rules_for(api.api_base)
        allowed = robots.can_fetch(api.api_base)
        ai_blocked = config.respect_content_signals and config.is_ai_training and rules.forbids_ai_training()
        info = {
            "fetched_at": now_iso(),
            "instance": host,
            "api_robots_status": rules.status,
            "api_allowed": allowed,
            "content_signals": rules.content_signals,
            "crawl_delay": rules.crawl_delay,
            "users": inst["users"],
        }
        if allowed and not ai_blocked:
            try:
                meta = api.meta()
                info["version"] = meta.get("version")
                info["name"] = meta.get("name")
            except HttpError as exc:
                info["error"] = str(exc)
        else:
            info["error"] = "robots/Content-Signal 禁止"
        storage.save_compliance(host, info)
        report.append(info)
        logger.info(
            "预检 %-20s api_allowed=%-5s signal=%-40s version=%s",
            host, allowed, rules.signal_summary, info.get("version", "-"),
        )
    return report


def main(argv=None):
    args = build_parser().parse_args(argv)

    try:
        config = Config.load(args.config)
    except FileNotFoundError:
        print("配置文件不存在: %s" % args.config, file=sys.stderr)
        return 2

    if args.limit is not None:
        config.notes_per_account = args.limit
    if args.media is not None:
        config.download_media = args.media
    if args.purpose:
        config.purpose = args.purpose.lower()
    if args.min_interval is not None:
        config.min_interval_seconds = args.min_interval

    os.makedirs(args.out, exist_ok=True)
    log_file = args.log_file or os.path.join(args.out, "fedimiss.log")
    logger = setup_logging(args.log_level, log_file)

    logger.info("FediMiss %s 启动 | %s", __version__, config.summary())
    if not config.instances:
        logger.error("未配置任何实例（config.instances 为空）")
        return 2

    storage = Storage(args.out)
    robots = RobotsPolicy(user_agent=config.user_agent, timeout=config.request_timeout, logger=logger)

    if args.preflight:
        run_preflight(config, storage, robots, logger)
        logger.info("预检完成，报告见各实例目录下的 compliance.json")
        return 0

    crawler = Crawler(config, storage, logger, robots=robots)

    def _handler(signum, _frame):
        logger.warning("收到信号 %s，将在当前请求后停止……", signum)
        crawler.stop_requested = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    stats = crawler.run()
    storage.append_run_log({"finished_at": now_iso(), "stats": stats})
    logger.info("抓取结束：%s", stats)
    logger.info("数据目录：%s", os.path.abspath(args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
