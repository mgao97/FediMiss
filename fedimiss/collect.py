"""单实例采集 worker：BFS 关注关系 → 资料 → 帖子 → 媒体入队。

每个实例一个 worker（同 host 串行，天然满足按实例限速）；不同实例并行。
所有预算（账号数、请求数、帖子数、BFS 深度）都在这里被强制检查，
保证「BFS 有明确终止条件，不默认无限扩展到整个联邦网络」。

对应需求 §5.2、§5.3、§7、§8。
"""

from __future__ import annotations

import json
import random
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .api import (
    ApiForbidden,
    ApiUnavailable,
    MisskeyAdapter,
    map_media_files,
    map_note,
)
from .identity import IdentityResolver, account_key
from .time_windows import historical_window, longitudinal_windows, window_label


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Budgets:
    """预算守卫：所有数值都从 store 现算，进程重启后依然有效。"""

    def __init__(self, store, config):
        self.store = store
        self.config = config

    def total_accounts(self) -> int:
        return self.store.count_accounts()

    def accounts_of(self, domain: str) -> int:
        return self.store.count_accounts("instance_domain=?", (domain,))

    def can_accept_account(self, domain: str) -> bool:
        if self.total_accounts() >= self.config.limit("max_accounts_total"):
            return False
        if self.accounts_of(domain) >= self.config.limit("max_accounts_per_instance"):
            return False
        return True

    def instance_requests(self, domain: str) -> int:
        for row in self.store.instance_stats():
            if row["instance_domain"] == domain:
                return int(row["requests"] or 0)
        return 0

    def can_request(self, domain: str) -> bool:
        return self.instance_requests(domain) < self.config.limit("max_requests_per_instance")

    def can_store_note(self) -> bool:
        return self.store.count_notes() < self.config.limit("max_notes_total")


class InstanceCollector:
    def __init__(
        self,
        store,
        http,
        robots,
        config,
        identity: IdentityResolver,
        domain: str,
        logger=None,
        stop_event=None,
    ):
        self.store = store
        self.http = http
        self.robots = robots
        self.config = config
        self.identity = identity
        self.domain = domain
        self.logger = logger
        self.stop_event = stop_event
        self.account_batch_size = max(1, int(self.config.get("workers.account_batch_size", 50)))
        self.adapter = MisskeyAdapter(domain, http, store, logger)
        self.budgets = Budgets(store, config)
        try:
            self.historical_window = historical_window(config)
        except ValueError as exc:
            raise ValueError("时间窗口配置错误：%s" % exc)
        self.longitudinal_windows = longitudinal_windows(config)
        # 关系端点的「连续 FORBIDDEN」计数：键是**端点名**（users_following/users_followers），
        # 不是能力字段名（following_api/followers_api），用于区分账号级隐私与实例级限制
        self._relation_forbidden = {"users_following": 0, "users_followers": 0}

    def _log(self, level: str, message: str, *args) -> None:
        if self.logger:
            getattr(self.logger, level)(message, *args)

    def _stopped(self) -> bool:
        return self.stop_event is not None and self.stop_event.is_set()

    def _snapshot_edges(self, edges: List[Dict[str, Any]]) -> None:
        now = _now()
        window_id = (
            self.historical_window["window_id"] if self.historical_window
            else (self.longitudinal_windows[0]["window_id"] if self.longitudinal_windows else "unbounded")
        )
        for edge in edges:
            key = "%s#%s#%s" % (edge["edge_key"], window_id, now)
            self.store.insert_relationship_snapshot(key, edge, window_id, now)

    # ------------------------------------------------------------------ 工具
    def _set_instance_capability(self, field: str, value: str) -> None:
        self.store.upsert_instance(self.domain, **{field: value})

    def _ensure_local_id(self, account: Dict[str, Any]) -> Optional[str]:
        """确保拿到该账号在**本实例（即其原属实例）**上的本地 ID。"""
        local_id = account.get("local_user_id")
        depth = account.get("bfs_depth") or 0
        method = account.get("discovery_method") or "unknown"
        if local_id and account.get("home_instance_status") in ("local", "unknown"):
            # 本实例时间线直接拿到的 id 可信
            if method in ("local_timeline", "config_seed") or account.get("home_instance_status") == "local":
                return local_id
        try:
            user = self.adapter.users_show(username=account["username"], host=None)
        except ApiForbidden:
            return None
        except ApiUnavailable:
            return None
        except Exception as exc:  # noqa: BLE001
            self.store.log_error(
                stage="collect", instance_domain=self.domain, subject=account["account_key"],
                error_type="users_show_failed", message=str(exc),
            )
            return None
        if not isinstance(user, dict) or user.get("host") is not None:
            return None
        new_id = user.get("id")
        self.store.upsert_account(
            account["account_key"],
            local_user_id=new_id,
            home_instance_status="local",
            account_uri=user.get("uri"),
            url=user.get("url"),
        )
        self.store.upsert_mapping(
            account["account_key"],
            resolved_local_user_id=new_id,
            resolved_host=self.domain,
            resolution_method="collected_on_home",
            resolution_evidence='{"confirmed_host": null, "instance": "%s"}' % self.domain,
            confidence="verified",
            resolved_at=_now(),
        )
        if depth:
            pass
        return new_id

    # ------------------------------------------------------------------ BFS
    def run_bfs(self) -> int:
        """遍历本实例本地账号的关注/粉丝，写关系边并发现新账号。"""
        if self.config.get("sampling.strategy") == "mhrw":
            return self.run_mhrw()
        max_pages = max(1, self.config.limit("max_relations_per_account") // 100)
        bfs_cap = max(1, self.config.limit("max_bfs_accounts_per_instance"))
        processed = 0
        while True:
            if self._stopped():
                self._log("info", "[%s] 收到停止信号，中断 BFS", self.domain)
                break
            if processed >= bfs_cap:
                # 本轮到此为止：若继续排空 BFS，后面的资料/帖子阶段永远轮不到，
                # 每轮产出的快照会缺帖子。剩余账号保持 pending，下一轮续采。
                self._log(
                    "info", "[%s] 本轮 BFS 达账号上限 %d，剩余账号留待下次续采",
                    self.domain, bfs_cap,
                )
                break
            if not self.budgets.can_request(self.domain):
                self._log("info", "[%s] 请求预算用尽，停止 BFS", self.domain)
                break
            # 每次重新读取能力标记（不能在循环外快照，否则判定结果不会生效）
            capability = self.store.get_instance(self.domain) or {}
            if (capability.get("following_api") == "forbidden"
                    and capability.get("followers_api") == "forbidden"):
                self._log("info", "[%s] following/followers 均匿名不可用，跳过 BFS", self.domain)
                # 收敛为终态：否则该实例账号会永远停留在 pending，
                # 使质量报告里的「BFS 队列剩余」失真。关系数据无法采集属于实例级限制，
                # 用 unavailable 明确区分于 pending（未做）与 error（出错）。
                closed = self.store._exec(
                    "UPDATE accounts SET bfs_status='unavailable', "
                    "last_error='following/followers 匿名不可用（实例级限制）' "
                    "WHERE bfs_status='pending' AND instance_domain=?",
                    (self.domain,),
                ).rowcount or 0
                if closed:
                    self._log(
                        "info", "[%s] %d 个账号的关系采集标记为 unavailable（无关系数据）",
                        self.domain, closed,
                    )
                break
            batch = self.store.claim_accounts(
                "bfs_status", limit=self.account_batch_size, max_depth=self.config.limit("max_bfs_depth"),
                domain=self.domain,
            )
            if not batch:
                break
            for account in batch:
                self._process_bfs_account(account, max_pages)
                processed += 1
        self._log("info", "[%s] BFS 处理账号 %d 个", self.domain, processed)
        return processed

    def run_mhrw(self) -> int:
        """使用 MHRW 游走公开关注/粉丝关系，降低高度节点对关系样本的偏置。"""
        # MHRW only needs enough edges for neighbor sampling. Unavailable/private
        # accounts are passed immediately instead of paginating their full lists.
        max_pages = 1
        steps = max(1, int(self.config.get("sampling.mhrw_steps_per_instance", 3000)))
        seed = self.config.get("sampling.seed")
        rng = random.Random(seed)

        candidates = [
            a for a in self.store.iter_accounts(
                "instance_domain=? AND bfs_status IN ('pending','completed') "
                "AND (last_error IS NULL OR last_error NOT LIKE '关系列表不可见%%')",
                (self.domain,),
            )
            if (a.get("bfs_depth") or 0) <= self.config.limit("max_bfs_depth")
        ]
        if not candidates:
            return 0
        current = rng.choice(candidates)
        processed = 0
        dead_keys: set[str] = set()

        for _ in range(steps):
            if self._stopped() or not self.budgets.can_request(self.domain):
                break
            capability = self.store.get_instance(self.domain) or {}
            if (capability.get("following_api") == "forbidden"
                    and capability.get("followers_api") == "forbidden"):
                closed = self.store._exec(
                    "UPDATE accounts SET bfs_status='unavailable', "
                    "last_error='following/followers 匿名不可用（实例级限制）' "
                    "WHERE bfs_status='pending' AND instance_domain=?",
                    (self.domain,),
                ).rowcount or 0
                if closed:
                    self._log("info", "[%s] %d 个账号标记为 unavailable", self.domain, closed)
                break

            current_neighbors, current_degree = self._mhrw_neighbors(current, max_pages)
            if not current_neighbors:
                self.store.upsert_account(
                    current["account_key"], bfs_status="completed",
                    last_error="MHRW：公开邻居为空",
                )
                dead_keys.add(current["account_key"])
                usable = [
                    a for a in candidates
                    if a["account_key"] not in dead_keys
                ]
                if not usable:
                    break
                current = rng.choice(usable)
                continue

            local_neighbors = []
            for neighbor_key in current_neighbors:
                neighbor = self.store.get_account(neighbor_key)
                if neighbor and neighbor.get("instance_domain") == self.domain:
                    local_neighbors.append(neighbor_key)
            if not local_neighbors:
                dead_keys.add(current["account_key"])
                usable = [a for a in candidates if a["account_key"] not in dead_keys]
                if not usable:
                    break
                current = rng.choice(usable)
                continue

            candidate_key = rng.choice(local_neighbors)
            candidate = self.store.get_account(candidate_key)
            if not candidate:
                continue
            candidate_neighbors, candidate_degree = self._mhrw_neighbors(candidate, max_pages)

            if candidate_degree <= 0:
                dead_keys.add(candidate_key)
                continue
            acceptance = min(1.0, float(current_degree) / float(candidate_degree))
            if rng.random() <= acceptance:
                current = candidate
                self.store.upsert_account(current["account_key"], bfs_status="completed")

            self.store.upsert_account(current["account_key"], bfs_status="completed")
            processed += 1

        self._log("info", "[%s] MHRW 游走 %d 步", self.domain, processed)
        return processed

    def _mhrw_neighbors(
        self, account: Dict[str, Any], max_pages: int
    ) -> tuple[List[str], int]:
        """返回去重的公开邻居 account_key，并写入方向关系边。"""
        key = account["account_key"]
        user_id = self._ensure_local_id(account)
        if not user_id:
            self.store.upsert_account(key, bfs_status="error", last_error="无法确认本地 ID")
            return [], 0

        neighbor_keys: set[str] = set()
        edges: List[Dict[str, Any]] = []
        for endpoint, field, direction, embedded in (
            ("users_following", "following_api", "following", "followee"),
            ("users_followers", "followers_api", "follower", "follower"),
        ):
            state = (self.store.get_instance(self.domain) or {}).get(field)
            if state == "forbidden":
                continue
            try:
                items = list(getattr(self.adapter, endpoint)(
                    user_id, limit=100, max_pages=max_pages
                ))
                self._set_instance_capability(field, "ok")
            except ApiForbidden:
                self.store.upsert_account(
                    key,
                    bfs_status="unavailable",
                    last_error="关系列表不可见（快速 PASS）：%s" % direction,
                )
                return [], 0
            except ApiUnavailable:
                continue
            except Exception as exc:  # noqa: BLEULEH
                self.store.log_error(
                    stage="bfs", instance_domain=self.domain, subject=key,
                    error_type="mhrw_failed", message=str(exc),
                )
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue
                peer_id = item.get("followeeId" if direction == "following" else "followerId")
                peer = item.get(embedded)
                if not peer_id or peer_id == user_id:
                    continue
                peer_key = self._register_peer(peer, peer_id, account.get("bfs_depth") or 0)
                if not peer_key:
                    continue
                neighbor_keys.add(peer_key)
                if direction == "following":
                    source, target = key, peer_key
                else:
                    source, target = peer_key, key
                edges.append({
                    "edge_key": "%s->%s" % (source, target),
                    "source_account_key": source,
                    "target_account_key": target,
                    "source_instance_domain": source.split("@")[-1],
                    "target_instance_domain": target.split("@")[-1],
                    "direction": direction,
                    "collected_at": _now(),
                    "collection_method": endpoint,
                    "source_instance": self.domain,
                })

        if edges:
            self.store.insert_edges(edges)
            self._snapshot_edges(edges)
        self.store.upsert_account(key, bfs_status="completed")
        return sorted(neighbor_keys), len(neighbor_keys)

    def _process_bfs_account(self, account: Dict[str, Any], max_pages: int) -> None:
        key = account["account_key"]
        depth = account.get("bfs_depth") or 0
        if depth >= self.config.limit("max_bfs_depth"):
            self.store.upsert_account(key, bfs_status="completed")
            return
        user_id = self._ensure_local_id(account)
        if not user_id:
            self.store.upsert_account(key, bfs_status="error", last_error="无法确认本地 ID")
            return

        edges: List[Dict[str, Any]] = []
        hidden: List[str] = []
        for endpoint, field, direction, embedded in (
            ("users_following", "following_api", "following", "followee"),
            ("users_followers", "followers_api", "follower", "follower"),
        ):
            state = (self.store.get_instance(self.domain) or {}).get(field)
            if state == "forbidden":
                continue
            try:
                items = list(getattr(self.adapter, endpoint)(user_id, limit=100, max_pages=max_pages))
                self._set_instance_capability(field, "ok")
                self._relation_forbidden[endpoint] = 0
            except ApiForbidden:
                # 实测（2026-10，nijimiss.moe）：同一实例上约 40% 账号返回
                # 400 FORBIDDEN，其余正常 —— 说明该错误是**账号级隐私设置**
                # （following/followers 列表不可见），不是实例能力问题。
                # 只有连续 N 个不同账号都 FORBIDDEN 才判定为实例级并缓存。
                self._relation_forbidden[endpoint] += 1
                hidden.append(direction)
                threshold = max(1, self.config.limit("relation_forbidden_threshold"))
                if self._relation_forbidden[endpoint] >= threshold:
                    self._set_instance_capability(field, "forbidden")
                    self.store.log_error(
                        stage="bfs", instance_domain=self.domain, subject=key,
                        error_type="relation_forbidden_instance", http_status=400,
                        message="%s 连续 %d 个账号均 400 FORBIDDEN，判定为实例级限制"
                                % (endpoint, self._relation_forbidden[endpoint]),
                    )
                else:
                    self.store.log_error(
                        stage="bfs", instance_domain=self.domain, subject=key,
                        error_type="relation_visibility_private", http_status=400,
                        message="%s 返回 400 FORBIDDEN（疑似账号隐私设置）" % endpoint,
                    )
                continue
            except ApiUnavailable:
                self._set_instance_capability(field, "unavailable")
                continue
            except Exception as exc:  # noqa: BLE001
                self.store.log_error(
                    stage="bfs", instance_domain=self.domain, subject=key,
                    error_type="bfs_failed", message=str(exc),
                )
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue
                if direction == "following":
                    peer_id = item.get("followeeId")
                else:
                    peer_id = item.get("followerId")
                if not peer_id or peer_id == user_id:
                    continue
                peer = item.get(embedded)
                peer_key = self._register_peer(peer, peer_id, depth + 1)
                if not peer_key:
                    continue
                if direction == "following":
                    source, target = key, peer_key
                else:
                    source, target = peer_key, key
                edges.append(
                    {
                        "edge_key": "%s->%s" % (source, target),
                        "source_account_key": source,
                        "target_account_key": target,
                        "source_instance_domain": source.split("@")[-1],
                        "target_instance_domain": target.split("@")[-1],
                        "direction": direction,
                        "collected_at": _now(),
                        "collection_method": endpoint,
                        "source_instance": self.domain,
                    }
                )
        if hidden:
            self.store.upsert_account(
                key, last_error="关系列表不可见（隐私设置）: %s" % ",".join(sorted(set(hidden)))
            )
        if edges:
            self.store.insert_edges(edges)
            self._snapshot_edges(edges)
        self.store.upsert_account(key, bfs_status="completed")

    def _register_peer(self, peer: Any, peer_id: str, depth: int) -> Optional[str]:
        """把关系里的对端用户登记为账号（有内嵌对象就免费；没有则查一次）。"""
        if isinstance(peer, dict) and peer.get("username"):
            if not self.budgets.can_accept_account(self.domain):
                return None
            return self.identity.register(peer, self.domain, "bfs_relation", depth=depth)
        # 内嵌对象缺失：用本实例的 userId 查一次（LIMIT 由预算控制）
        if not self.budgets.can_request(self.domain):
            return None
        try:
            user = self.adapter.users_show(user_id=peer_id)
        except (ApiForbidden, ApiUnavailable):
            return None
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(user, dict) or not user.get("username"):
            return None
        if not self.budgets.can_accept_account(self.domain):
            return None
        return self.identity.register(user, self.domain, "bfs_relation_lookup", depth=depth)

    # ------------------------------------------------------------------ 资料
    def run_profiles(self) -> int:
        processed = 0
        while not self._stopped() and self.budgets.can_request(self.domain):
            batch = self.store.claim_accounts(
                "profile_status", limit=self.account_batch_size, domain=self.domain
            )
            if not batch:
                break
            for account in batch:
                self._refresh_profile(account)
                processed += 1
        self._log("info", "[%s] 刷新资料 %d 个账号", self.domain, processed)
        return processed

    def _refresh_profile(self, account: Dict[str, Any]) -> None:
        key = account["account_key"]
        try:
            user = self.adapter.users_show(user_id=account.get("local_user_id") or None,
                                           username=account["username"], host=None)
        except (ApiForbidden, ApiUnavailable):
            self.store.upsert_account(key, profile_status="error", last_error="users/show 不可用")
            return
        except Exception as exc:  # noqa: BLE001
            self.store.upsert_account(key, profile_status="error", last_error=str(exc)[:500])
            self.store.log_error(
                stage="profile", instance_domain=self.domain, subject=key,
                error_type="profile_failed", message=str(exc),
            )
            return
        if not isinstance(user, dict):
            self.store.upsert_account(key, profile_status="error", last_error="空响应")
            return
        patch = {
            "display_name": user.get("name"),
            "description": user.get("description"),
            "created_at": user.get("createdAt"),
            "followers_count": user.get("followersCount"),
            "following_count": user.get("followingCount"),
            "notes_count": user.get("notesCount"),
            "is_bot_declared": 1 if user.get("isBot") else 0,
            "is_locked": 1 if user.get("isLocked") else 0,
            "avatar_url": user.get("avatarUrl"),
            "banner_url": user.get("bannerUrl"),
            "avatar_blurhash": user.get("avatarBlurhash"),
            "banner_blurhash": user.get("bannerBlurhash"),
            "background_url": user.get("backgroundUrl"),
            "is_cat": 1 if user.get("isCat") else 0,
            "is_admin": 1 if user.get("isAdmin") else 0,
            "is_moderator": 1 if user.get("isModerator") else 0,
            "is_indexable": 1 if user.get("isIndexable") else 0,
            "is_suspended": 1 if user.get("isSuspended") else 0,
            "is_silenced": 1 if user.get("isSilenced") else 0,
            "hide_online_status": 1 if user.get("hideOnlineStatus") else 0,
            "no_crawling": 1 if user.get("noCrawling") else 0,
            "birthday": user.get("birthday"),
            "location": user.get("location"),
            "language": user.get("lang"),
            "fields_json": json.dumps(user.get("fields") or [], ensure_ascii=False),
            "emojis_json": json.dumps(user.get("emojis") or [], ensure_ascii=False),
            "raw_profile": json.dumps(user, ensure_ascii=False)[:40000],
            "profile_collected_at": _now(),
            "profile_status": "completed",
            "last_collected_at": _now(),
        }
        if user.get("host") is None and user.get("id"):
            patch["local_user_id"] = user["id"]
            patch["home_instance_status"] = "local"
        self.store.upsert_account(key, **patch)
        now = _now()
        windows = []
        if self.historical_window:
            windows.append(self.historical_window)
        windows.extend(self.longitudinal_windows)
        window_id = (windows[0]["window_id"] if windows else "unbounded")
        snapshot_key = "%s#profile#%s" % (key, now)
        self.store.insert_account_snapshot(snapshot_key, key, window_id, now, {
            **patch,
            "account_key": key,
        })

    # ------------------------------------------------------------------ 帖子
    def run_interleaved(self) -> Dict[str, int]:
        """逐账号流水线：同一账号的资料→帖子连续处理，尽早产出完整信息账号。

        与「先全部资料、后全部帖子」的分阶段模式相比，每处理完一个账号
        就立即形成一个可用的完整信息样本（若其有帖子与媒体记录）。
        """
        processed = {"profiles": 0, "notes": 0}
        page_size = min(100, max(1, self.config.limit("notes_page_size")))
        per_account = max(1, self.config.limit("notes_per_account"))
        fetch_limit = min(page_size, per_account)
        max_pages = max(1, (per_account + fetch_limit - 1) // fetch_limit)
        while True:
            if self._stopped():
                self._log("info", "[%s] 收到停止信号，中断流水线", self.domain)
                break
            if not self.budgets.can_request(self.domain) or not self.budgets.can_store_note():
                break
            batch = self.store.claim_accounts_any_pending(
                limit=self.account_batch_size, domain=self.domain
            )
            if not batch:
                break
            for account in batch:
                if self._stopped():
                    break
                if account.get("profile_status") == "running":
                    self._refresh_profile(account)
                    processed["profiles"] += 1
                    # 资料刷新会补齐 local_user_id，取最新行再采帖子
                    account = self.store.get_account(account["account_key"]) or account
                if account.get("notes_status") == "running":
                    self._collect_notes(account, fetch_limit, max_pages, per_account)
                    processed["notes"] += 1
        self._log(
            "info", "[%s] 逐账号流水线完成：资料 %d / 帖子 %d",
            self.domain, processed["profiles"], processed["notes"],
        )
        return processed

    def run_notes(self) -> int:
        processed = 0
        page_size = min(100, max(1, self.config.limit("notes_page_size")))
        per_account = max(1, self.config.limit("notes_per_account"))
        # 按单账号预算压小每页取数：否则 notes_per_account < notes_page_size 时
        # ceil(per/page)=1 页仍会取回整页（100 条），预算形同虚设。
        fetch_limit = min(page_size, per_account)
        max_pages = max(1, (per_account + fetch_limit - 1) // fetch_limit)
        while True:
            if self._stopped():
                self._log("info", "[%s] 收到停止信号，中断帖子采集", self.domain)
                break
            if not self.budgets.can_request(self.domain) or not self.budgets.can_store_note():
                break
            batch = self.store.claim_accounts(
                "notes_status", limit=self.account_batch_size, domain=self.domain
            )
            if not batch:
                break
            for account in batch:
                self._collect_notes(account, fetch_limit, max_pages, per_account)
                processed += 1
        self._log("info", "[%s] 采集帖子完成 %d 个账号", self.domain, processed)
        return processed

    def _collect_notes(
        self, account: Dict[str, Any], page_size: int, max_pages: int, per_account: int
    ) -> None:
        key = account["account_key"]
        user_id = self._ensure_local_id(account)
        if not user_id:
            self.store.upsert_account(key, notes_status="error", last_error="无法确认本地 ID")
            return
        note_rows: List[Dict[str, Any]] = []
        media_rows: List[Dict[str, Any]] = []
        try:
            for note in self.adapter.users_notes(
                user_id, limit=page_size, max_pages=max_pages, with_files=True
            ):
                if not isinstance(note, dict) or not note.get("id"):
                    continue
                row = map_note(note, self.domain, key)
                collected_at = _now()
                row["collection_time"] = collected_at
                row["collected_at"] = collected_at
                row["raw_note"] = json.dumps(note, ensure_ascii=False)[:40000]
                windows = []
                if self.historical_window:
                    windows.append(self.historical_window)
                windows.extend(self.longitudinal_windows)
                label = window_label(row.get("created_at"), windows) if windows else None
                if windows and not label:
                    # users/notes 按时间倒序返回；已早于配置窗口时无需继续翻页。
                    earliest = min((w["start"] for w in windows), key=str)
                    from .time_windows import parse_utc
                    try:
                        if parse_utc(row.get("created_at")) < parse_utc(earliest):
                            break
                    except Exception:  # noqa: BLE001
                        pass
                    continue
                row["window_id"] = label if label else "unbounded"
                row["window_type"] = (
                    "historical" if label and str(label).startswith("historical-")
                    else "longitudinal" if label else "unbounded"
                )
                author = note.get("user")
                if isinstance(author, dict) and author.get("username"):
                    self.identity.register(author, self.domain, "note_author", depth=account.get("bfs_depth") or 0)
                reply = note.get("reply")
                if isinstance(reply, dict):
                    row["reply_account_key"] = self._register_referenced(reply, account)
                renote = note.get("renote")
                if isinstance(renote, dict):
                    row["renote_account_key"] = self._register_referenced(renote, account)
                    row["renote_id"] = renote.get("id")
                note_rows.append(row)
                media_rows.extend(map_media_files(note, self.domain, key))
                if len(note_rows) >= per_account:
                    # 达到单账号帖子预算（多页时最后一页需要截断）
                    break
                if not self.budgets.can_store_note():
                    break
        except ApiForbidden:
            self.store.upsert_account(key, notes_status="error", last_error="users/notes 被拒绝")
            self.store.log_error(
                stage="notes", instance_domain=self.domain, subject=key,
                error_type="forbidden", http_status=403,
            )
            return
        except ApiUnavailable:
            self.store.upsert_account(key, notes_status="error", last_error="users/notes 不可用")
            return
        except Exception as exc:  # noqa: BLE001
            self.store.upsert_account(key, notes_status="error", last_error=str(exc)[:500])
            self.store.log_error(
                stage="notes", instance_domain=self.domain, subject=key,
                error_type="notes_failed", message=str(exc),
            )
            return
        if note_rows:
            self.store.insert_notes(note_rows)
        if media_rows:
            self.store.upsert_media(media_rows)
        self.store.upsert_account(key, notes_status="completed", last_collected_at=_now())

    def _register_referenced(self, referenced: Dict[str, Any], account: Dict[str, Any]) -> Optional[str]:
        user = referenced.get("user")
        if isinstance(user, dict) and user.get("username"):
            return self.identity.register(
                user, self.domain, "note_reference", depth=account.get("bfs_depth") or 0
            )
        return None

    # ------------------------------------------------------------------ 入口
    def run(self) -> Dict[str, int]:
        result = {"bfs": 0, "profiles": 0, "notes": 0}
        started_at = _now()
        run_status = "in_progress"
        window_id = (
            self.historical_window["window_id"] if self.historical_window
            else (self.longitudinal_windows[0]["window_id"] if self.longitudinal_windows else "unbounded")
        )
        try:
            phase = self.config.get("sampling.phase", "all")
            if phase in ("relations", "all"):
                result["bfs"] = self.run_bfs()
            if phase in ("profiles_media", "all"):
                if self.config.get("pipeline.interleave", False):
                    inter = self.run_interleaved()
                    result["profiles"] = inter["profiles"]
                    result["notes"] = inter["notes"]
                else:
                    result["profiles"] = self.run_profiles()
                    result["notes"] = self.run_notes()
        except Exception as exc:  # noqa: BLE001
            run_status = "failed"
            self.store.log_error(
                stage="collect", instance_domain=self.domain,
                error_type="worker_failed", message=str(exc),
            )
            self._log("error", "[%s] worker 异常：%s", self.domain, exc)
        if not self._stopped():
            if run_status == "in_progress":
                run_status = "completed"
            self.store.upsert_instance(self.domain, collection_status="completed")
            instance = self.store.get_instance(self.domain) or {}
            window_id = (
                self.historical_window["window_id"] if self.historical_window
                else (self.longitudinal_windows[0]["window_id"] if self.longitudinal_windows else "unbounded")
            )
            self.store.insert_instance_snapshot(
                "%s#instance#%s" % (self.domain, _now()),
                self.domain, window_id, _now(), instance,
            )
        stats = {}
        for row in self.store.instance_stats():
            if row["instance_domain"] == self.domain:
                stats = row
                break
        self.store.upsert_collection_run(
            "%s#%s" % (self.domain, started_at),
            window_id=window_id,
            window_type="historical" if self.historical_window else (
                "longitudinal" if self.longitudinal_windows else "unbounded"
            ),
            start_at=started_at,
            end_at=_now(),
            status="partial" if self._stopped() else run_status,
            requests=int(stats.get("requests") or 0),
            successes=int(stats.get("ok") or 0),
            failures=int(stats.get("errors") or 0),
            instance_domain=self.domain,
        )
        return result


def mark_out_of_scope(store, active_domains: List[str]) -> int:
    """把原属实例不在本轮目标内的账号标为 out_of_scope，不再消耗预算。"""
    if not active_domains:
        return 0
    marks = ",".join("?" * len(active_domains))
    changed = 0
    for column in ("bfs_status", "profile_status", "notes_status"):
        cur = store._exec(
            "UPDATE accounts SET %s='out_of_scope' WHERE %s='pending' AND instance_domain NOT IN (%s)"
            % (column, column, marks),
            tuple(active_domains),
        )
        changed += cur.rowcount or 0
    return changed


def domain_of_key(key: str) -> str:
    return key.split("@")[-1]


__all__ = ["InstanceCollector", "Budgets", "mark_out_of_scope", "account_key", "domain_of_key"]
