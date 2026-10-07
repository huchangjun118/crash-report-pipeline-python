# crash-report-pipeline

客户端崩溃与错误上报收集链路：客户端本地落盘缓冲 + 异步上传，服务端去重、
聚合与查询。设计目标是在网络不稳定与流量突增下保持可靠。

## 架构

```
客户端                                  服务端
┌────────────────────┐   POST /ingest   ┌──────────────────────────────┐
│ PersistentEventBuffer│ ─────────────▶ │ QuotaManager (采样/配额)      │
│  (SQLite, 优先级淘汰) │                │ IngestPipeline (有界背压队列) │
│ Uploader (退避重试)  │ ◀───────────── │ EventStore (SQLite 去重/聚合) │
└────────────────────┘   acked event ids └──────────────────────────────┘
```

- `crash_pipeline/events.py` — 事件模型（`crash` / `error` / `telemetry`）。
- `crash_pipeline/client/buffer.py` — 客户端持久化队列。
- `crash_pipeline/client/uploader.py` — 异步上传器，可配置退避。
- `crash_pipeline/server/quota.py` — 采样与配额准入。
- `crash_pipeline/server/backpressure.py` — 有界队列与过载降级。
- `crash_pipeline/server/fingerprint.py` — 可配置指纹。
- `crash_pipeline/server/storage.py` — 去重入库、问题聚合、丢弃记录。
- `crash_pipeline/server/app.py` — FastAPI 服务（`/ingest`、`/issues`、`/drops`、`/stats`）。

运行服务：`uv run python main.py`（数据库路径由 `CRASH_PIPELINE_DB` 指定）。
运行测试：`uv run python -m pytest tests/`。

## 可靠性语义

### 客户端队列（至少一次投递）

- 事件先写入 SQLite 再上传，进程重启后未确认的事件继续上传。
- 只有收到服务端 `acked` 确认的事件才会从队列删除；已确认事件不会重发。
- 队列写满时按优先级淘汰：`telemetry`(0) < `error`(1) < `crash`(2)。
  崩溃事件永远不会先于普通遥测被丢弃；队列里全是崩溃时，新的遥测直接
  被拒绝（`enqueue` 返回 `False`）。

### 重试与退避

`BackoffPolicy` 配置项：

| 字段 | 默认值 | 含义 |
|---|---|---|
| `initial_delay` | 0.5s | 首次重试延迟 |
| `multiplier` | 2.0 | 指数退避倍率 |
| `max_delay` | 30.0s | 延迟上限 |
| `max_retries` | 0 | 最大重试次数，0 = 无限重试 |

上传失败只增加延迟，事件始终留在落盘队列里，网络恢复后自动续传。

### 去重语义（服务端幂等）

- 每个事件携带客户端生成的全局唯一 `event_id`。
- 服务端用 `INSERT OR IGNORE`（主键约束）原子写入：同一 `event_id` 无论
  重投多少次、多少并发请求同时处理，只会入库一次，返回 `duplicate`，
  问题计数不会虚高。
- 事件插入与问题聚合计数在同一个事务里提交，不会留下写了一半的记录；
  重启后从最近已提交状态继续，已确认事件不丢失、不重复入库。
- 客户端对 `stored` / `duplicate` / `rejected` 三种终态都视为已确认，
  可以安全删除本地副本。

### 指纹聚合

`FingerprintConfig` 配置项：

| 字段 | 默认值 | 含义 |
|---|---|---|
| `use_kind` | True | 事件类型参与指纹 |
| `use_message` | True | 归一化后的消息参与指纹 |
| `stack_frames` | 3 | 参与指纹的栈帧数，0 = 忽略调用栈 |
| `attribute_keys` | () | 参与指纹的属性键（按键名排序，保证确定性） |
| `normalize_numbers` | True | 消息中的数字归一化为 `?` |

指纹是 (事件, 配置) 的纯函数：同一批事件在相同配置下无论到达顺序如何，
都归并到同一个问题（`issue_id = tenant:fingerprint`），计数为去重后的
事件数。

### 采样与配额

`TenantPolicy` 配置项（按租户设置，`QuotaManager.set_policy`）：

| 字段 | 默认值 | 含义 |
|---|---|---|
| `sample_rate` | 1.0 | 遥测采样比例（仅 `telemetry`，对 `event_id` 哈希确定性采样） |
| `max_events_per_window` | 1000 | 每窗口每租户最大事件数 |
| `window_seconds` | 60 | 配额窗口长度 |

- 致命事件（`crash` 或 `severity=fatal`）跳过采样与配额，全量保留。
- 采样对 `event_id` 确定性哈希：同一事件重投得到相同判定，不会重复计数。
- 被拒绝的事件写入 `drops` 表，可通过 `GET /drops?tenant_id=...` 按原因
  （`sampled_out` / `quota_exceeded` / `overloaded`）查询，不会静默吞掉。

### 背压与过载降级

- `IngestPipeline(capacity=N)` 是有界队列，内存占用严格受 `capacity` 约束，
  不会无界增长。
- 队列满时：普通遥测被丢弃并计数；崩溃事件会挤掉队列中最旧的非致命事件
  腾位（被挤掉的事件同样计入丢弃并落盘可查）；只有队列里全是致命事件时
  才会丢弃新的致命事件。
- `drain` 优先吐出崩溃事件，保证致命错误通道优先入库。
- 丢弃量可通过 `GET /stats`（内存计数）与 `GET /drops`（持久化记录）查询。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/ingest` | 批量上报，`{"events": [...]}`，返回 `acked` 与逐事件 `outcomes` |
| GET | `/issues?tenant_id=` | 聚合问题列表（含事件计数、首末次出现时间） |
| GET | `/issues/{issue_id}` | 单个问题详情 |
| GET | `/drops?tenant_id=` | 按原因统计的丢弃量 |
| GET | `/stats` | 管道运行计数（接受/丢弃/处理/队列深度/已入库） |
