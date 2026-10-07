# crash-report-pipeline

客户端崩溃与错误上报收集链路：客户端本地落盘缓冲、异步上传；服务端去重、聚合、查询。
设计目标是在网络不稳定与流量突增下保持可靠。

## 架构

```
客户端                                  服务端
┌────────────────────┐   POST /v1/events   ┌──────────────────────────────┐
│ Event              │ ──────────────────▶ │ 1. 背压 BackpressureController│
│   ↓                │                     │ 2. 配额/采样 QuotaManager      │
│ PersistentEventQueue│ ◀── ack/reject ──  │ 3. 指纹 compute_fingerprint    │
│   (SQLite, 落盘)    │                     │ 4. 去重入库 EventStore (SQLite) │
│   ↓ peek           │                     │   ↓                          │
│ Uploader (退避重试) │                     │ issues / rejections / stats  │
└────────────────────┘                     └──────────────────────────────┘
```

## 去重语义（重要）

- **端到端去重键是 `event_id`**，由客户端在事件创建时生成（UUID）。
- 客户端队列：`event_id` 唯一，重复 `enqueue` 幂等；只有收到服务端确认
  （`accepted` / `duplicates` / `rejected`）才从队列删除，**未确认的事件重启后继续上传**。
- 服务端存储：`events.event_id` 主键 + `INSERT OR IGNORE`，同一事件无论重投多少次、
  多少并发请求同时处理，**只入库一次**；重复投递返回 `duplicates`，**不增加聚合计数**。
- 聚合（issues）与事件行在**同一个事务**中提交：要么都落盘要么都回滚，不留半写记录。
- 采样决策基于 `event_id` 哈希，**同一事件重投结论一致**，不会因重试时收时拒。

## 可靠性保证

| 场景 | 行为 |
|---|---|
| 进程重启 | 客户端队列与服务端存储均落盘（SQLite WAL），从最近已提交位置继续，不丢不重 |
| 网络中断 | Uploader 按 `BackoffConfig` 指数退避重试；事件留在队列，恢复后自动续传 |
| 队列写满 | 按优先级淘汰 info→warning→error；**fatal 永不先于普通遥测被淘汰**，队列全为 fatal 时拒绝新遥测入队 |
| 重复投递 | 服务端识别为 `duplicates`，计数不虚高 |
| 超配额 | 按策略 `reject` / `degrade`（只保 fatal）；拒绝原因落库，`GET /v1/rejections` 可查 |
| 系统过载 | 背压控制器限制在途事件数（内存有界）；普通遥测丢弃并计数（`/v1/stats`），fatal 走预留通道不丢 |

## 配置项

### 客户端 `BackoffConfig`
| 字段 | 默认 | 说明 |
|---|---|---|
| `initial_delay` | 0.5s | 首次重试等待 |
| `max_delay` | 30s | 退避上限 |
| `multiplier` | 2.0 | 指数倍率 |
| `max_attempts` | 8 | 单批最大尝试次数，超过后等下一轮 |

`PersistentEventQueue(db_path, max_events=10000)`：队列容量，写满按优先级淘汰。

### 服务端 `FingerprintConfig`
| 字段 | 默认 | 说明 |
|---|---|---|
| `fields` | `["exception_type", "stack_top"]` | 参与指纹的字段，可选 `exception_type` / `message` / `severity` / `tenant_id` / `stack_top` / `stack_full` / `tag:<name>` |
| `stack_depth` | 3 | `stack_top` 取前 N 帧 |

指纹是 (事件内容, 配置) 的纯函数：message 中数字折叠为 `#`、堆栈帧去行号，
同一批事件**无论到达顺序如何**都归并到同一问题。

### 服务端 `TenantPolicy`（按租户，`PUT /v1/policies/{tenant_id}`）
| 字段 | 默认 | 说明 |
|---|---|---|
| `sample_rate` | 1.0 | 普通遥测采样比例；**fatal 永远全量保留** |
| `max_events` | 100000 | 租户配额（已接受事件总数） |
| `over_quota` | `"reject"` | 超配策略：`reject` 或 `degrade`（只保 fatal） |

### 服务端 `BackpressureController`
`BackpressureController(max_pending=1000, fatal_reserved=None)`：
在途事件上限；`fatal_reserved` 为 fatal 预留容量（默认 10%），
普通遥测超过 `max_pending - fatal_reserved` 即丢弃并计数，fatal 永不因过载被丢。

## API

- `POST /v1/events` — 批量上报，返回 `{accepted, duplicates, rejected:[{event_id, reason}]}`
- `GET /v1/issues?tenant_id=` — 按指纹聚合的问题列表（count/first_seen/last_seen/样例）
- `GET /v1/rejections?tenant_id=` — 被拒绝事件及原因（不静默吞数据）
- `GET /v1/stats` — 背压丢弃量、在途数、已提交批次数
- `PUT /v1/policies/{tenant_id}` — 设置租户采样/配额策略

## 运行

```bash
uvicorn main:app --host 127.0.0.1 --port 8000   # 服务端，DB 路径用 CRASH_PIPELINE_DB 指定
python3 -m pytest tests/ -q                      # 测试
```

客户端用法：

```python
from crash_pipeline.client.queue import PersistentEventQueue
from crash_pipeline.client.uploader import Uploader, BackoffConfig
from crash_pipeline.client.http_sender import make_http_sender
from crash_pipeline.models import Event, Severity

queue = PersistentEventQueue("client_queue.db", max_events=10_000)
queue.enqueue(Event.new("tenant-1", Severity.FATAL, "segfault",
                        exception_type="SigSEGV", stack_frames=("a.c:10",)))
uploader = Uploader(queue, make_http_sender("http://127.0.0.1:8000"),
                    backoff=BackoffConfig(initial_delay=0.5, max_delay=30.0))
uploader.flush_all()   # 可在后台线程周期性调用
```

## 测试覆盖（tests/）

- `test_client_queue.py` — 持久化、确认语义、fatal 优先淘汰、重启恢复
- `test_uploader_retry.py` — 退避序列、网络恢复续传、乱序收敛
- `test_server_dedup.py` — 重复投递、乱序归并、8 线程并发去重、服务端重启
- `test_quota_sampling.py` — 配额拒绝可查、fatal 全量、采样确定性、拒绝不毒循环
- `test_backpressure.py` — 过载降级、丢弃计数、在途数有界
- `test_e2e_recovery.py` — 客户端崩溃中途恢复、抖动网络下端到端不丢不重
