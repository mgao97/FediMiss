"""实例发现、校验与账号种子。

流程（对应需求 §4、§5.1）：
  1. 候选实例 = 配置种子 + 已知实例的 peer 列表（有硬上限，避免无界扩散）
  2. 逐个用 NodeInfo 校验（软件、版本、用户数、时间线开关）
  3. 合规判定：robots.txt 是否允许 ``/api``；Content-Signal 是否禁止当前用途
  4. 通过的实例按用户数排序，取前 ``limits.max_instances`` 个作为本轮采集目标
  5. 账号种子：
       a. 配置里的 ``seed_accounts``（形如 ``user@host``）
       b. 目标实例的 ``notes/local-timeline``（帖子内嵌 author 对象）

明确不做的事：不把本地时间线的账号当作该实例的完整用户列表。
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Set

from .api import ApiForbidden, ApiUnavailable, MisskeyAdapter
from .identity import IdentityResolver
from .nodeinfo import InstanceProbe


class Discovery:
    def __init__(
        self, store, http, robots, config, identity: IdentityResolver,
        logger=None, stop_event=None,
    ):
        self.store = store
        self.http = http
        self.robots = robots
        self.config = config
        self.identity = identity
        self.logger = logger
        self.stop_event = stop_event
        self.probe = InstanceProbe(http, robots, logger)

    def _log(self, level: str, message: str, *args) -> None:
        if self.logger:
            getattr(self.logger, level)(message, *args)

    def _stopped(self) -> bool:
        return self.stop_event is not None and self.stop_event.is_set()

    # ------------------------------------------------------------------ 候选实例
    def candidate_instances(self) -> List[str]:
        seeds: List[str] = list(self.config.data["seed_instances"])
        excluded: Set[str] = set(self.config.data["exclude_instances"])

        budget = self.config.limit("max_candidates")

        # 用已知实例的 peer 列表扩展候选（best-effort，失败不影响，且有硬上限）
        seen = set(seeds)
        added = 0
        for host in list(seeds)[:5]:
            if added >= budget or self._stopped():
                break
            for peer in self.probe.peers_of(host):
                if added >= budget:
                    break
                if peer in seen or peer in excluded:
                    continue
                seen.add(peer)
                seeds.append(peer)
                added += 1

        out = [h for h in seeds if h not in excluded]
        self._log(
            "info", "候选实例 %d 个（种子 %d + peer 扩展 %d，上限 %d）",
            len(out), len(self.config.data["seed_instances"]), added, budget,
        )
        return out

    # ------------------------------------------------------------------ 校验
    def validate_instances(self, hosts: List[str] = None) -> List[str]:
        """逐个校验并入库，返回「可采集」的实例域名（已排序）。"""
        targets = hosts if hosts is not None else self.candidate_instances()
        ok: List[Dict[str, Any]] = []
        for host in targets:
            if self._stopped():
                self._log("warning", "收到停止信号，中止实例校验")
                break
            if self.store.get_instance(host) and self.store.get_instance(host).get("api_status") == "ok":
                ok.append(self.store.get_instance(host))
                continue
            record = self.probe.probe(host)
            domain = record.pop("instance_domain")
            record["checked_at"] = record.get("checked_at") or _utc_now()
            self.store.upsert_instance(domain, **record)
            if record.get("api_status") == "ok":
                ok.append(self.store.get_instance(domain))
            else:
                self._log(
                    "info", "实例 %s 跳过：%s（%s）",
                    domain, record.get("collection_status"), record.get("failure_reason"),
                )
        ok.sort(key=lambda r: r.get("users_total") or 0, reverse=True)
        limit = self.config.limit("max_instances")
        selected = ok[:limit]
        for record in ok[limit:]:
            self.store.upsert_instance(
                record["instance_domain"],
                collection_status="deferred",
                failure_reason="超出本轮实例预算（max_instances=%d）" % limit,
            )
        return [r["instance_domain"] for r in selected]

    def active_instances(self) -> List[str]:
        return [
            r["instance_domain"]
            for r in self.store.instances()
            if r.get("api_status") == "ok" and r.get("collection_status") != "deferred"
        ]

    # ------------------------------------------------------------------ 账号种子
    def seed_from_config(self) -> int:
        """把 ``seed_accounts`` 中的账号解析到各自原属实例。"""
        count = 0
        for acct in self.config.data["seed_accounts"]:
            username, host = acct.split("@", 1)
            key = "%s@%s" % (username, host)
            inserted = self.store.insert_account_if_absent(
                key,
                username=username,
                host=None,
                instance_domain=host,
                discovery_instance=host,
                discovery_method="config_seed",
                discovered_at=_utc_now(),
                bfs_depth=0,
                home_instance_status="unknown",
            )
            self.store.upsert_mapping(
                key,
                raw_identifier=acct,
                normalized_identifier=key.lower(),
                source_instance="config",
                resolved_username=username,
                resolved_host=host,
                resolution_method="config_seed",
                resolution_evidence='{"from": "config.seed_accounts"}',
                confidence="declared",
                resolved_at=_utc_now(),
            )
            count += 1 if inserted else 0
        self._log("info", "配置种子账号入库 %d 个", count)
        return count

    def seed_from_timeline(self, domain: str, max_pages: int, budget: int) -> int:
        """从实例本地时间线发现账号（仅作为发现来源，不代表完整用户列表）。"""
        adapter = MisskeyAdapter(domain, self.http, self.store, self.logger)
        found = 0
        try:
            for note in adapter.local_timeline(limit=100, max_pages=max_pages):
                if found >= budget:
                    break
                user = note.get("user") if isinstance(note, dict) else None
                if not isinstance(user, dict):
                    continue
                key = self.identity.register(user, domain, "local_timeline", depth=0)
                if key:
                    found += 1
        except ApiForbidden:
            self.store.upsert_instance(domain, local_timeline_api="forbidden")
            self._log("info", "实例 %s 的 local-timeline 匿名不可用", domain)
        except ApiUnavailable:
            self.store.upsert_instance(domain, local_timeline_api="unavailable")
        except Exception as exc:  # noqa: BLE001
            self.store.log_error(
                stage="discovery", instance_domain=domain,
                error_type="timeline_failed", message=str(exc),
            )
        self._log("info", "实例 %s 由本地时间线发现账号 %d 个", domain, found)
        return found

    # ------------------------------------------------------------------ 搜索发现
    USERNAME_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"

    def seed_from_username_sweep(self, domain: str, budget: int) -> int:
        """按用户名前缀字典序枚举本地账号，突破时间线回溯深度的限制。

        前缀队列持久化在 meta 表（``username_sweep_queue:<domain>``），
        跨轮次接力：取队首前缀查询一次；若返回恰好 100 条（可能被截断），
        就把 36 个扩展前缀压回队列，后续轮次在用户密集的前缀上继续细分。
        ``budget`` 为本次允许发起的搜索请求数。
        """
        alphabet = list(self.USERNAME_ALPHABET)
        raw = self.store.get_meta("username_sweep_queue:%s" % domain)
        queue: List[str] = []
        if raw:
            try:
                queue = json.loads(raw) if isinstance(raw, str) else list(raw)
            except (ValueError, TypeError):
                queue = []
        if not queue:
            queue = list(alphabet)

        adapter = MisskeyAdapter(domain, self.http, self.store, self.logger)
        before = self.store.count_accounts("instance_domain=?", (domain,))
        checked = 0
        while queue and checked < budget and not self._stopped():
            prefix = queue.pop(0)
            try:
                users = adapter.search_users_by_username(prefix, limit=100)
            except (ApiForbidden, ApiUnavailable) as exc:
                queue.insert(0, prefix)
                self.store.set_meta("username_sweep_queue:%s" % domain, queue)
                self._log(
                    "info", "实例 %s 的用户名搜索不可用（%s），保存队列并终止本轮扫描",
                    domain, type(exc).__name__,
                )
                return 0
            except Exception as exc:  # noqa: BLE001
                self.store.log_error(
                    stage="discovery", instance_domain=domain,
                    error_type="username_sweep_failed", subject=prefix,
                    message=str(exc),
                )
                continue
            checked += 1
            for user in users:
                if isinstance(user, dict):
                    self.identity.register(user, domain, "username_sweep", depth=0)
            # 返回满页说明该前缀下的账号可能超过 100 个，细分到下一层
            if len(users) >= 100 and len(prefix) < 12:
                queue.extend(prefix + ch for ch in alphabet)
            if checked % 100 == 0:
                self._log(
                    "info", "实例 %s 用户名扫描进度：已查 %d 前缀，队列剩余 %d",
                    domain, checked, len(queue),
                )

        self.store.set_meta("username_sweep_queue:%s" % domain, queue)
        after = self.store.count_accounts("instance_domain=?", (domain,))
        self._log(
            "info", "实例 %s 用户名前缀扫描：查 %d 前缀，新登记 %d 个账号（实例现有 %d），队列剩余 %d",
            domain, checked, after - before, after, len(queue),
        )
        return checked

    def mine_hashtags(self, domain: str, top: int = 100) -> List[str]:
        """从已采集帖子的正文中统计高频话题标签，作为搜索发现的关键词。"""
        rows = self.store._rows(
            "SELECT text FROM notes WHERE source_instance=? AND text IS NOT NULL "
            "ORDER BY created_at DESC LIMIT 30000",
            (domain,),
        )
        counts: Dict[str, int] = {}
        for row in rows:
            for tag in re.findall(r"#([^\s#.,!?:;、。！？：；()（）\[\]【】「」『』<>…・'\"～~]+)", row["text"] or ""):
                tag = tag.strip()
                if 2 <= len(tag) <= 30 and not tag.isdigit():
                    counts[tag] = counts.get(tag, 0) + 1
        ranked = [t for t, _ in sorted(counts.items(), key=lambda x: -x[1])[:top]]
        self._log("info", "实例 %s 从最近帖子中挖掘出 %d 个高频标签", domain, len(ranked))
        return ranked

    def seed_from_hashtag_search(self, domain: str, tags: List[str], max_pages: int, budget: int) -> int:
        """用话题标签逐个 ``notes/search``，登记命中的帖子作者。"""
        adapter = MisskeyAdapter(domain, self.http, self.store, self.logger)
        before = self.store.count_accounts("instance_domain=?", (domain,))
        used = 0
        for tag in tags:
            if used >= budget or self._stopped():
                break
            try:
                for note in adapter.search_notes("#%s" % tag, limit=100, max_pages=max_pages):
                    user = note.get("user") if isinstance(note, dict) else None
                    if isinstance(user, dict):
                        self.identity.register(user, domain, "note_search", depth=0)
                used += max_pages
            except ApiForbidden:
                self._log("info", "实例 %s 的 notes/search 匿名不可用，跳过标签搜索", domain)
                break
            except Exception as exc:  # noqa: BLE001
                self.store.log_error(
                    stage="discovery", instance_domain=domain,
                    error_type="hashtag_search_failed", subject=tag,
                    message=str(exc),
                )
                used += 1
        after = self.store.count_accounts("instance_domain=?", (domain,))
        self._log(
            "info", "实例 %s 标签搜索：查 %d 个标签（约 %d 请求），新登记 %d 个账号",
            domain, len(tags), used, after - before,
        )
        return after - before


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
