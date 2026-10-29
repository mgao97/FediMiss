# FediMiss

跨 **Misskey** 多实例（instance）的**多模态**数据抓取工具：抓取账户 profile、帖子文本与图片（媒体元数据 +（合规时）实际图片文件）。

设计原则是「**合规优先**」：抓取任何 URL 之前，先解析该 host 的 `robots.txt` 与
`Content-Signal`（Cloudflare 的 AI 内容使用声明），被禁止就跳过并记录原因。
**仅使用 Python 标准库**，无需安装第三方依赖（Python >= 3.8）。

---

## 1. Misskey API 规则梳理

Misskey **不是 REST**：所有 HTTP API 都是 `POST /api/<endpoint>`，请求与响应均为 JSON。

| 项目 | 说明 |
| --- | --- |
| 请求方式 | `POST`，`Content-Type: application/json` |
| 鉴权 | 令牌放在请求体 JSON 的 `i` 字段；**公开数据（profile、公开帖子）匿名即可读取** |
| 官方文档 | `https://<instance>/api-doc`（每个实例自带 API 文档） |
| 限流 | 触发后返回 `HTTP 429` 并可能带 `Retry-After` 响应头 |
| 流式 API | 有 WebSocket 流式接口，本工具未使用 |

本工具用到的公开端点：

| 端点 | 用途 | 关键参数 |
| --- | --- | --- |
| `meta` | 实例元信息（名称、软件版本、URL） | — |
| `stats` | 实例统计（可选，失败不阻塞） | — |
| `users/show` | 账户 profile | `username`（或 `userId` / `username`+`host`） |
| `users/notes` | 账户帖子列表 | `userId`（必填）、`limit`(1–100)、`untilId`/`sinceId` 游标、`withFiles`、`withReplies` |

聚合抓取要点：

* `users/notes` **必须传 `userId`**（传 `username` 会 400），所以先 `users/show` 拿 `id`。
* 分页用 `untilId`（向更早翻）+ `limit`（单页上限 100），靠返回列表的最后一个 `id` 作为下一页游标。
* 传 `withFiles: true` 后，帖子对象里的 `files[]` 会带上媒体元数据：
  `id / url / thumbnailUrl / type / name / size / md5 / blurhash / isSensitive / properties(width,height)` —— 这就是**图片信息**。
* 部分实例关闭了本地时间线（`LTL_DISABLED`），且 `notes/featured`、`users/search` 在部分实例需要令牌；本工具只用上面 4 个匿名可用端点。

## 2. robots.txt 实测结果（2026-10）

不同实例差异极大，抓取前必须逐 host 解析：

| Host | `/api/` 是否允许 | 媒体 Host | 媒体是否允许 | Content-Signal |
| --- | --- | --- | --- | --- |
| `misskey.io` | 允许（`Allow: /`） | `media.misskeyusercontent.jp` | **禁止**（`Disallow: /`） | 无 |
| `misskey.cloud` | 允许 | `media.misskey.cloud` | 允许（无 robots.txt） | 无 |
| `nijimiss.moe` | 允许 | `media.nijimissusercontent.app` | 允许 | `ai-train=no` |
| `misskey.design` | **禁止**（`Disallow: /api`） | `file.misskey.design` | 允许 | `search=yes,ai-train=no,use=reference` |
| `misskey.art` | **禁止** | `files.misskey.art` | 允许 | — |
| `misskey.flowers` | **禁止** | — | — | `ai-train=no` |
| `misskey.niri.la` / `misskey.systems` / `misskey.stream` / `misskey.pm` / `misskey.social` / `sushi.ski` / `voskey.icalo.net` | **禁止** | — | — | — |
| `media.misskeyusercontent.com` | — | — | **禁止** | — |

两个必须注意的实测结论：

1. **很多实例用 Cloudflare「Managed robots.txt」，会额外下发一组 `User-agent: *` 且 `Disallow: /api`。**
   一个 robots.txt 里因此可能出现**多个 `User-agent: *` 分组**——标准库
   `urllib.robotparser` 只采用**第一个**匹配分组，会把 `/api` 的 `Disallow` 漏掉。
   本工具按 **RFC 9309** 自行实现匹配：合并所有匹配分组，路径取「最长匹配优先，同长度 Allow 优先」。
2. **媒体 CDN 的 robots.txt 与实例本体是分开的**，`media.misskeyusercontent.jp` 是 `Disallow: /`。
   即：可以读 API 元数据，但不应用爬虫去下载该 CDN 上的图片文件。

## 3. 合规策略（代码如何遵守）

对应实现见 [fedimiss/robots.py](file:///users/Min/FediMiss/fedimiss/robots.py) 与 [fedimiss/http.py](file:///users/Min/FediMiss/fedimiss/http.py)。

* **逐 host 解析 robots.txt**，按 `scheme://host` 缓存（默认 1 小时）。
  * `2xx` → 按内容解析；`4xx`（含 404）→ 视为未声明、允许；`5xx`/网络错误 → 保守地**禁止**。
* **抓取前强制校验**：`HttpClient` 每次请求前调用 `robots.can_fetch(url)`，不通过就抛
  `RobotsDisallowed`，绝不发起请求（获取 robots.txt 自身除外）。
* **实例级跳过**：若 `Disallow: /api`，整个实例跳过，并在 `compliance.json` 记录原因。
* **Content-Signal 处理**：解析 `search / ai-input / ai-train / use` 声明并记录到 `compliance.json`。
  当 `purpose = "ai-training"` 且某 host 声明 `ai-train=no` 时，跳过该 host 的内容；
  默认 `purpose = "research"`，不触发该限制（但仍如实记录声明）。
* **媒体独立校验**：图片 URL 属于哪个 host，就校验哪个 host 的 robots.txt；
  被禁止时**只保留元数据**（`url`、`type`、`size`、`blurhash`、`dimensions` 等），不下载文件，
  并把跳过原因写进记录的 `media[].skipped`（如 `robots` / `too_large` / `account_media_limit`）。
* **限速**：同一 host 串行请求，最小间隔取 `max(min_interval_seconds, robots 的 Crawl-delay)`；
  遇 `429`/`5xx` 指数退避 + 抖动，并优先使用 `Retry-After`。
* **可识别 UA**：请把 `config.json` 里的 `user_agent` 换成**带真实联系方式**的标识。
* **只抓公开数据**，不使用任何令牌去访问非公开内容。

## 4. 目录结构

```
FediMiss/
├── config.example.json      # 配置模板
├── config.json              # 实际运行配置
├── fedimiss/
│   ├── __main__.py          # CLI 入口（含 --preflight 合规预检）
│   ├── config.py            # 配置加载/校验
│   ├── robots.py            # robots.txt + Content-Signal 合规引擎（RFC 9309 匹配）
│   ├── http.py              # 限速 / 重试 / robots 拦截的 HTTP 客户端
│   ├── api.py               # Misskey API 封装
│   ├── crawler.py           # 多实例多模态抓取编排
│   └── storage.py           # JSONL / profile / 媒体文件 / 合规报告存储
└── data/                    # 抓取产物（已 gitignore）
```

## 5. 快速开始

```bash
# 1) 合规预检：只读 robots.txt / meta，确认哪些实例允许抓取
python3 -m fedimiss --config config.json --preflight

# 2) 正式抓取（后台运行并写日志）
nohup python3 -m fedimiss --config config.json --out data > data/run.out 2>&1 &

# 常用参数
python3 -m fedimiss --config config.json --limit 50 --no-media   # 每号最多 50 帖、不下载图片
python3 -m fedimiss --config config.json --purpose ai-training   # 触发 Content-Signal 限制
```

`Ctrl-C` / `SIGTERM` 会在当前请求结束后安全停止。重复运行可**断点续爬**（已存在的帖子 `id` 会跳过）。

## 6. 输出格式

```
data/
├── fedimiss.log                     # 运行日志
├── crawl_log.jsonl                  # 每次运行的汇总
└── <instance>/
    ├── instance_meta.json           # 实例 meta/stats（含软件版本）
    ├── compliance.json              # robots.txt / Content-Signal / 决策记录
    └── users/<account>/
        ├── profile.json             # 账户 profile（多模态之「账户信息」）
        ├── notes.jsonl              # 每行一条帖子
        └── media/<noteId>-<fileId>.<ext>   # 合规允许时下载的图片
```

`notes.jsonl` 每行结构：

```json
{
  "instance": "misskey.cloud",
  "account": "Exile_E",
  "fetched_at": "2026-10-29T03:28:57Z",
  "note": { "id": "...", "createdAt": "...", "text": "...", "cw": null, "files": [ ... ], "reactions": {...} },
  "media": [
    {
      "id": "...", "url": "https://media.misskey.cloud/files/....webp",
      "type": "image/jpeg", "name": "IMG_0032.jpeg", "size": 94024,
      "isSensitive": false, "blurhash": "eKHU^Gog...", 
      "local_path": "misskey.cloud/users/Exile_E/media/....jpeg",
      "skipped": null, "downloaded_bytes": 26398
    }
  ]
}
```

## 7. 配置项（config.json）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `user_agent` | 占位符 | **请改成带真实联系方式的 UA** |
| `purpose` | `research` | `research` 或 `ai-training`；后者会触发 `ai-train=no` 跳过 |
| `respect_content_signals` | `true` | 是否遵循 Content-Signal |
| `download_media` | `true` | 是否下载图片文件；`false` 时只存元数据 |
| `download_media_types` | `["image/"]` | 允许下载的 MIME 前缀 |
| `max_media_bytes` | 8 MiB | 单文件大小上限 |
| `max_media_per_account` | 200 | 每账户下载图片数量上限 |
| `notes_per_account` | 200 | 每账户抓取帖子数上限 |
| `notes_page_size` | 100 | 单页大小（API 上限 100） |
| `include_replies` | `false` | 是否包含回复 |
| `since` | `null` | 只抓该时间之后的帖子（ISO8601） |
| `min_interval_seconds` | 1.5 | 同 host 最小请求间隔 |
| `max_retries` | 4 | 429/5xx 重试次数 |
| `instances` | `[]` | `[{"host": "...", "users": ["a","b"]}]` |

## 8. 注意事项与已知限制

* **务必**把 `user_agent` 换成可联系的标识，并保持 `min_interval_seconds` 足够大（建议 ≥ 1s）。
* 若某实例 robots.txt 禁止 `/api`，本工具会跳过它——这是**预期行为**，不是故障；
  如确需抓取，请先获得该实例运营方的**明确授权**，再调整策略（本工具不提供绕过开关）。
* `misskey.io` 的图片托管在 `media.misskeyusercontent.jp`（`Disallow: /`），因此
  **该实例只会产出图片元数据，不会下载图片文件**；这是遵守 robots.txt 的结果。
* 抓取到的内容版权归原作者所有。用于 AI 训练前，请遵守目标实例的
  `Content-Signal`（`ai-train=no` 表示**不得**用于训练）以及当地法律法规。
* 部分字段（如 `followersCount`）对匿名请求可能返回 0 或被隐藏。
