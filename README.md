# FediMiss

跨 **Misskey**（及同源衍生软件）多实例的**公开数据采集工具**，面向 bot/human
标注与跨实例泛化的社交机器人研究。

设计目标不是「抓得最多」，而是：

- **账号归属准确** —— 每个账号都尽量关联到它的**原属实例**，而不是发现它的实例；
- **实体关联正确** —— 实例 / 账号 / 帖子 / 关系 / 附件分层存储，主键与关联键明确；
- **过程可复现** —— 配置版本、时间戳、失败原因全部留痕；
- **失败可审计** —— 每次失败落 `errors` 表，可分类统计；
- **可断点续采** —— 状态机 + SQLite，进程中断后重跑同一命令即可继续。

采集设计参考了 FediData（`https://github.com/FDUDataNET/FediData`）的公开仓库与 README
（BFS 分层队列、发现实例≠归属实例、实体分层、失败审计、媒体独立模块），
但**没有复用其平台代码**：Misskey 与 Mastodon 在协议、分页、限流信号、ID 体系上
差异很大，相关差异与逐模块对照见 [docs/fedidata_review.md](file:///users/Min/FediMiss/docs/fedidata_review.md)
与 [docs/misskey_api_notes.md](file:///users/Min/FediMiss/docs/misskey_api_notes.md)。

---

## 1. 架构

```
config.json
   │
   ├─ discover   候选实例 → NodeInfo 校验 → robots.txt/Content-Signal → POST /api/meta
   │
   ├─ collect    账号发现（种子 + notes/local-timeline）
   │             → BFS 关注关系（users/following、users/followers，分层、有预算、有终止条件）
   │             → 资料刷新（users/show）
   │             → 帖子采集（users/notes，untilId 游标分页）+ 附件元数据
   │
   ├─ media      附件下载（独立模块，按媒体 host 单独校验 robots，默认关闭）
   │
   ├─ export     instances/accounts/notes/follow_edges/account_id_mapping(/media).parquet
   │             + collection_manifest.json
   └─ report     data_quality_report.md（8 项必报指标 + 选择偏差声明）
```

| 模块 | 职责 |
| --- | --- |
| [robots.py](file:///users/Min/FediMiss/fedimiss/robots.py) | RFC 9309 自实现的 robots.txt / Content-Signal 合规引擎 |
| [http.py](file:///users/Min/FediMiss/fedimiss/http.py) | 逐 host 限速、429/`Retry-After` 指数退避、robots 强校验 |
| [nodeinfo.py](file:///users/Min/FediMiss/fedimiss/nodeinfo.py) | NodeInfo 实例校验 + peer 列表扩展 |
| [api.py](file:///users/Min/FediMiss/fedimiss/api.py) | `MisskeyAdapter`：请求构造 / 游标分页 / 错误分类 / 字段映射 |
| [identity.py](file:///users/Min/FediMiss/fedimiss/identity.py) | **跨实例账号归属解析**与置信度分级 |
| [discovery.py](file:///users/Min/FediMiss/fedimiss/discovery.py) | 实例发现校验、账号种子 |
| [collect.py](file:///users/Min/FediMiss/fedimiss/collect.py) | 单实例 worker（BFS → 资料 → 帖子）与预算守卫 |
| [media.py](file:///users/Min/FediMiss/fedimiss/media.py) | 附件下载（限流、重试、断点续传、磁盘预算、sha256 去重） |
| [store.py](file:///users/Min/FediMiss/fedimiss/store.py) | SQLite（WAL）状态机与系统记录 |
| [export.py](file:///users/Min/FediMiss/fedimiss/export.py) / [quality.py](file:///users/Min/FediMiss/fedimiss/quality.py) | Parquet 快照 / 数据质量报告 |

**仅用 Python 标准库**（urllib / ssl / json / sqlite3 / threading）。
导出 Parquet 需要 pyarrow，本仓库用 vendored 版本（见 §4），不影响采集本身。

## 2. Misskey API 与合规要点

完整实测笔记见 [docs/misskey_api_notes.md](file:///users/Min/FediMiss/docs/misskey_api_notes.md)。要点：

- Misskey **不是 REST**：全部 `POST /api/<endpoint>` + JSON body。
- 分页是 `untilId` 游标（无 `Link` 响应头）；单页 `limit` 上限 100。
- 实测**不返回** `x-ratelimit-*` 头 → 限流只能靠「按 host 固定最小间隔 + 429/`Retry-After` 退避」。
- `users/notes` **必须传 `userId`**；`users/following` / `users/followers` 的匿名可用性**因实例而异**
  （实测 `misskey.io` 返回 400 FORBIDDEN，`nijimiss.moe` 正常），代码逐实例探测后记录到
  `instances.following_api` / `following_api`。
- 实例元数据以 **NodeInfo** 为准；候选实例用 `/api/v1/instance/peers` 扩展。
- 抓取前对**每个 host** 校验 robots.txt（含多分组合并、最长匹配优先）与 Content-Signal；
  媒体 CDN 与实例本体的 robots 策略**分开**判定。

只采集公开且允许访问的内容；不使用令牌访问非公开内容；不绕过封禁与访问限制。

## 3. 账号归属解析（本项目重点）

`host` / `url` / `uri` 三类证据 + 四级置信度，写入 `account_id_mapping`：

| confidence | 含义 |
| --- | --- |
| `verified` | 在**账号原属实例**上查到该账号且返回 `host == null` |
| `declared` | 原属实例来自远端缓存对象的 `host` 字段（归属已知、本地 ID 未知） |
| `inferred` | 仅由 `uri`（`https://<host>/users/<id>`）推断 |
| `unresolved` | 无法可靠判定 —— **留空，不猜测**（只写映射表，不进账号表） |

Misskey 的 `id` **只在返回它的实例上有意义**，因此绝不作为跨实例身份；
也绝不因用户名相同就合并账号。

## 4. 快速开始

```bash
# 1) 探测并校验实例（只读 NodeInfo / robots.txt / meta）
python3 -m fedimiss --config config.json discover

# 2) 采集（后台运行；Ctrl-C / SIGTERM 会安全停止并回滚未完成任务）
nohup python3 -m fedimiss --config config.json collect --workers 4 \
  > data/run.out 2>&1 &

# 3) 查看进度 / 续采（重跑同一命令即续采）
python3 -m fedimiss --config config.json status
python3 -m fedimiss --config config.json collect --workers 4

# 4) 导出 Parquet 与清单（需要 vendored pyarrow）
PYTHONPATH=/users/Min/FediMiss/.vendor/lib python3 -m fedimiss --config config.json export

# 5) 生成数据质量与偏差报告
python3 -m fedimiss --config config.json report
```

`collect` 的可选参数：`--instances a,b`（只采指定实例）、`--workers N`、
`--seed-pages N`（每实例本地时间线翻页数）、`--no-discover`（不重新校验实例）。

## 5. 分阶段扩容

同一套代码与状态库，只改 `config.json` 的 `limits` / `seed_instances` / `workers`：

| 阶段 | 实例数 | 目标账号 | 关键配置 |
| --- | --- | --- | --- |
| A | 3–5 | ~5,000 | `max_instances=5`、`max_accounts_total=5000` |
| B | 10–20 | ~30,000 | `max_instances=20`、`max_accounts_total=30000` |
| C | — | 100,000 | `max_instances=40`、`max_accounts_total=100000` |

每次 `collect` 都会把 `config.json` 的 SHA-256 前 16 位写入 `meta.config_version`，
并随 `collection_manifest.json` 一起导出，用于固定「配置版本 + 时间戳」的可复现快照。

## 6. 输出与交付物

全部产物在 `storage.root`（默认 `data/`）：

```
data/
├── fedimiss.sqlite3              # 状态库（断点续采 / 审计的事实来源）
├── fedimiss.log
├── instances.parquet             # 实例信息
├── accounts.parquet              # 账号信息（含归属状态）
├── notes.parquet                 # 公开帖子
├── follow_edges.parquet          # 关注关系边
├── account_id_mapping.parquet    # 跨实例归属解析映射与置信度
├── media.parquet                 # 附件元数据（多模态）
├── collection_manifest.json      # 配置版本 / 计数 / 文件校验和 / 实例清单
├── data_quality_report.md        # 8 项质量指标 + 选择偏差声明
└── media/<host>/<xx>/<sha256>.<ext>   # 仅在 media.download=true 且 robots 允许时
```

文档类交付物：[docs/fedidata_review.md](file:///users/Min/FediMiss/docs/fedidata_review.md)（FediData 逐模块审查 + Misskey 适配差异）、
[docs/misskey_api_notes.md](file:///users/Min/FediMiss/docs/misskey_api_notes.md)、
[docs/annotation_guidelines.md](file:///users/Min/FediMiss/docs/annotation_guidelines.md)（人工标注指南）。

## 7. 配置项

见 [config.example.json](file:///users/Min/FediMiss/config.example.json)。主要项：

| 分组 | 键 | 说明 |
| --- | --- | --- |
| 顶层 | `user_agent` / `contact` | **请改成带真实联系方式的标识** |
| 顶层 | `purpose` | `research` 或 `ai-training`；后者会触发 `ai-train=no` 跳过 |
| 顶层 | `respect_content_signals` | 是否遵循 Content-Signal |
| `limits` | `max_instances` / `max_accounts_total` / `max_accounts_per_instance` | 实例与账号预算 |
| `limits` | `max_bfs_depth` / `max_relations_per_account` | BFS 终止条件 |
| `limits` | `max_bfs_accounts_per_instance` | 单实例单轮 BFS 账号上限：BFS 不排空就不进入资料/帖子阶段，设上限保证每轮都能产出完整快照；剩余账号留给下一轮续采 |
| `limits` | `notes_per_account` / `max_notes_total` / `max_requests_per_instance` | 帖子与请求预算。`notes_per_account` 是**单账号硬上限**（小于一页时会压小请求 `limit`） |
| `limits` | `relation_forbidden_threshold` | `users/following|followers` 需连续 N 个账号返回 400 才判定为**实例级**限制（该错误多为账号级隐私，见下） |
| `limits` | `max_candidates` | 候选实例上限（种子 + peer 扩展），防止实例校验无界扩散 |
| `rate` | `min_interval_seconds` / `max_retries` | 逐 host 最小间隔与重试 |
| `media` | `download` / `types` / `max_total_bytes` | 附件下载开关与配额 |
| `workers` | `instance_workers` | 并行实例数（同实例始终串行） |

## 8. 已知限制与注意事项

- **账号级隐私 ≠ 实例级限制**：Misskey 的 `users/following` / `users/followers` 常对
  个别账号返回 `400 FORBIDDEN`，实测同一实例（nijimiss.moe）上约四成账号如此，其余正常 ——
  这是账号隐私设置。只有连续 `relation_forbidden_threshold` 个账号都失败才判定为实例级。
- **`bfs_status` 的取值**：`pending` / `running` / `completed` / `error` / `out_of_scope`（原属实例不在本轮目标内）
  / `unavailable`（该实例关系端点匿名不可用，关系数据无法采集，属实例级限制）。
  `unavailable` 是终态，避免账号永远停在 `pending` 使「剩余任务」失真。
- `is_bot_declared` 只是平台自我声明字段，**不是**最终 bot/human 标签；
  需按 [docs/annotation_guidelines.md](file:///users/Min/FediMiss/docs/annotation_guidelines.md) 人工标注。
- 若实例 robots.txt 禁止 `/api`，本工具会跳过该实例并记录原因 —— 这是**预期行为**，
  不提供任何绕过开关。
- `media.download` 默认 `false`：首轮只保留附件元数据。部分媒体 CDN 是 `Disallow: /`，
  即使打开开关也不会下载这些 host 的文件。
- 采集样本带有明确的选择偏差（实例选择、公开时间线发现、BFS 深度、API 可访问性），
  **不能**当作整个 Misskey 网络的代表性样本；详见数据质量报告。
- 抓取内容版权归原作者所有；用于 AI 训练前请遵守目标实例的 Content-Signal 与当地法规。
