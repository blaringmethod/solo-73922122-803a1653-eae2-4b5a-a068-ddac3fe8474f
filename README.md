# Traffic PITR Recovery API（交通数仓误删恢复）

路口分钟计数被值班员误删后，需要从**物理备份 + WAL 归档**把删除前的记录找回来补数；
恢复期间主库采集不中断。本服务在启动时对主库做一次 `pg_basebackup` 物理基准备份，
主库持续归档 WAL；用户在任意时刻创建**命名恢复点**，只有当该点所需的 WAL 全部归档
后才允许选择它恢复；每次恢复都从基准备份**重建一个全新的隔离只读副本**，重放 WAL 到
目标恢复点即停，供查询补数。

## 架构

```
┌──────────────┐   WAL 归档(archive_command)   ┌───────────────┐
│  db (主库)    │ ───────────────────────────▶ │  wal_archive  │
│  traffic 库   │                              │  (共享卷)      │
│  单表:        │ ◀──────── pg_basebackup ───┐ │               │
│  traffic_     │                            │ │               │
│  minute_      │        ┌───────────────────┴─┴─────────────┐ │
│  counts       │        │  api (FastAPI)                    │ │
└──────────────┘        │  · 启动时物理基准备份 (/backups)    │ │
       ▲                │  · 恢复点就绪检查 (WAL 连续性)       │ │
       │ 主库采集不中断   │  · 恢复执行器: 每次从基准备份重建    │ │
       └────────────────│    隔离副本, 重放 WAL 到恢复点暂停   │ │
                        │    (/recovery_work/rN, 只读热备)     │ │
                        └─────────────────────────────────────┘ │
```

- **db**：`postgres:16`，开启 `archive_mode=on`、`archive_timeout=10s`，WAL 归档到共享卷。
  源库只有一张业务表 `traffic_minute_counts(intersection_id, minute_utc, vehicle_count)`。
- **api**：`postgres:16` 镜像 + FastAPI。启动时执行 `pg_basebackup -X stream` 建立物理
  基准备份；恢复时把基准备份复制到全新目录，配置 `restore_command` +
  `recovery_target_name` + `recovery_target_action=pause`，以热备只读方式启动在
  独立端口（容器内 127.0.0.1:55433）。
- API 自身的台账（恢复点、恢复任务）存在独立 SQLite（api_state 卷），**不污染源库**，
  源库始终只有一张业务表。

## 快速开始

```bash
docker compose up --build
# 等待 api 就绪后:
# 1) 打开演示入口 http://localhost:8000/docs （Swagger UI，根路径 / 会重定向到这里）
# 2) 或跑一键演示脚本（需要 curl 和 jq）:
./scripts/demo.sh                  # PITR 恢复
./scripts/demo-dual-source.sh      # 双源按小时供数
```

演示脚本完整走一遍：模拟采集写入 → 创建恢复点 `before_delete` → 等待 WAL 归档齐全 →
模拟误删最近 10 分钟 → 主库继续写入新路口数据 → 发起 PITR 恢复 → 从只读副本查回被删
记录 → 断言副本不含恢复点之后的提交、主库数据未受影响。

两个脚本默认先调 `POST /demo/reset?confirm=true` 清空业务表与采集台账，因为 PITR 脚本
断言的是绝对行数；如要保留数据，设 `RESET=0`。

## 双源按小时供数（新增）

新接入路口同时用**主网关**和**备用网关**采集车流。分析员按小时取数，每小时只采用
一个来源。本服务新增下面这组接口（与 PITR 接口共用 `/docs`）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/ingest/reports` | 按 路口 + 来源(primary/backup) + UTC 整点小时 批量上报分钟结论 |
| GET | `/ingest/hours` | 按路口和小时查询：供数来源 + 该版本的明细；两源都未齐返回 409 |
| GET | `/ingest/progress` | 只看两源上报进度（不要求已就绪） |
| POST | `/demo/dual-source/generate` | 演示用：按分钟范围/缺报区间构造一批上报，走同一套逻辑 |
| POST | `/demo/reset?confirm=true` | 演示用：清空业务表与双源台账（恢复任务在途时拒绝） |

上报体：

```json
{
  "intersection_id": "IX-1",
  "source": "primary",
  "hour_utc": "2026-11-20T08:00:00Z",
  "minutes": [
    {"minute_utc": "2026-11-20T08:00:00Z", "vehicle_count": 42},
    {"minute_utc": "2026-11-20T08:01:00Z", "missing": true}
  ]
}
```

每条分钟结论要么是非负 `vehicle_count`，要么 `missing: true`（二者互斥，模型校验）。

### 语义（对照需求）

- **按路口/来源/整点小时分批**：`hour_utc` 必须是 UTC 整点，`minute_utc` 必须对齐到
  分钟且落在该小时内；一批最多 60 条。
- **先报齐者先供数**：某来源把 60 个分钟全部给出结论（计数或明确缺报）即“报齐”；
  备用网关先报齐就先用备用供数。
- **主源报齐后整小时换主源**：主网关报齐后该小时整体切换到主源。
- **主源缺报的分钟撤下备用读数**：切换/发布时整小时按供数来源重建——
  该来源标记缺报的分钟直接没有行，绝不留另一来源的读数。
- **同源同分钟幂等/整批拒绝**：相同内容重传不改变任何结果（版本不变）；任何一条
  与已记录结论不一致则整批 409，**同批里合法的新分钟也不落库**。
- **并发不倒退**：同一 `(路口, 小时)` 的写在事务级 advisory lock 下串行（提交即释放），
  已发布的主源不会被较慢的备用上报改回；数据库触发器 `hour_state_no_downgrade`
  在存储层再兜底一次 primary→backup 的倒退。
- **进度/来源/业务表同事务**：`gateway_ingest.*` 台账与 `traffic_minute_counts` 在
  **同一个 Postgres 库、同一个事务**里提交（这是台账放在源库而不是 SQLite 的原因），
  所以不存在“进度记了但业务行没写”或反之；API 重启后按已提交状态续报。
- **查询同一版**：`GET /ingest/hours` 在 REPEATABLE READ 快照里读供数来源、版本和明细，
  不会出现“来源是备用、明细却已是主源”的混版；两源都未报齐返回 409 且带进度。

## 恢复语义（对照需求）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 服务与主库连通性 |
| GET | `/backup` | 启动基准备份状态（running/ready/failed + 错误） |
| POST | `/demo/generate` | 模拟采集：生成 路口×分钟 计数（可指定前缀/数量） |
| POST | `/demo/delete` | 模拟误删：删除最近 N 分钟数据 |
| GET | `/primary/traffic` | 查主库当前数据（证明主库一直在采集） |
| GET | `/demo/overview` | 主库数据、备份、恢复点、恢复任务总览 |
| POST | `/restore-points?name=...` | 创建命名恢复点（`pg_create_restore_point` + 强制切 WAL） |
| GET | `/restore-points` | 恢复点列表：`pending`（WAL 未齐）/ `ready`（可选）/ `expired`（早于基准备份） |
| POST | `/recoveries` | 发起恢复 `{"restore_point": "name"}`，202 异步任务；同一时刻只允许一项在途 |
| GET | `/recoveries` / `/recoveries/{id}` | 恢复任务状态：running/success/failed/superseded + 错误详情 |
| GET | `/recoveries/current` | 当前对外服务的恢复（无则 404） |
| GET | `/replica/traffic` | 查询已恢复只读副本（未就绪/已失败返回 409） |

## 恢复语义（对照需求）

- **启动即备份**：api 启动时后台执行 `pg_basebackup -X stream`（失败自动重试 10 次，
  状态见 `/backup`）；主库 WAL 由 `archive_command` 持续归档。
- **恢复点门控**：创建恢复点时记录其 LSN 并 `pg_switch_wal()`；后台任务持续校验
  「基准备份结束 LSN → 恢复点 LSN」之间每一个 WAL 段都已在归档目录中，齐全才置
  `ready`；未就绪的点发起恢复返回 409。早于基准备份重做起点的点标记 `expired`。
- **物理恢复**：每次恢复都 `cp` 基准备份到全新目录 `recovery_work/rN`，写
  `recovery.signal` 与 PITR 配置后启动独立 postgres——不是逻辑留史/影子表。
- **到达目标才报成功**：轮询副本直到 `pg_last_wal_replay_lsn() >= 目标 LSN`，
  并对业务表执行一次真实查询确认可查询，才置 `success` 并开放查询。
- **失败即报、不留残缺**：缺 WAL（`restore_command` 失败 → postgres FATAL）、
  恢复进程退出（pg_ctl 探活 + 后台看门狗每 3s 巡检）都会把任务置 `failed` 并记录
  postgres 日志尾部；失败任务的目录被清理，**不开放残缺副本，也不回退到上一次副本**
  （新恢复启动时会先退役并删除旧副本）。重试 = 再 POST 一次。
- **单一在途**：存在 running 任务时再发起返回 409。
- **不混数据**：新恢复启动即退役（停止+删除）旧副本并清空服务指针；每次恢复都是
  基准备份的全新拷贝，先后两个恢复点互不干扰。
- **主库不阻塞**：恢复只读归档卷与基准备份，对主库零写入、零锁。

## 环境变量（api 服务）

| 变量 | 默认 | 说明 |
|---|---|---|
| `PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE` | db/5432/postgres/postgres/traffic | 主库连接（双源台账也在这个库的 `gateway_ingest` schema） |
| `REPLICA_PORT` | 55433 | 副本监听端口（容器内 127.0.0.1） |
| `RECOVERY_TIMEOUT_SEC` | 180 | 等待重放到目标点的超时 |
| `REPLICA_RUN_AS` | postgres | 副本进程运行用户（容器内 root 经 gosu 降权；本地裸跑可置空） |
| `BACKUP_ON_STARTUP` | 1 | 启动时是否做基准备份（预置备份的测试场景可置 0） |
| `WAL_ARCHIVE_DIR/BACKUP_DIR/RECOVERY_ROOT/STATE_DB` | /wal_archive /backups/base /recovery_work /state/api.db | 存储路径 |

## 已验证 / 未验证（诚实清单）

交付前在本沙箱用 PostgreSQL 16.16（PGDG 官方二进制）+ 本 API 真实运行验证。
**注意：本沙箱内核级禁止一切跨进程信号**（kill/pidfd 均 EPERM），导致两个环境性
限制：(1) `pg_basebackup` 的 checkpoint 信号被拒（`could not signal for checkpoint:
Permission denied`）；(2) 热备副本无法进入可连接状态（postmaster 收不到
PMSIGNAL）。这两点在正常 Linux / Docker 中不存在，属沙箱特有。

已验证（真实运行）：

- ✅ 主库建表、持续 WAL 归档（段 1..N 全部落归档卷）
- ✅ API 启动、/health、/docs、/openapi.json、/ 重定向
- ✅ 备份失败路径：`pg_basebackup` 失败被捕获并记录到 `/backup`（含重试逻辑）
- ✅ 写入/误删演示接口（删除行数确定性符合预期）
- ✅ 恢复点创建 → pending →（WAL 齐）ready；归档连续性校验与 `pg_walfile_name` 交叉一致
- ✅ 未就绪恢复点发起恢复 → 409；不存在 → 404；早于备份的点 → expired
- ✅ **物理恢复正确性**：基准备份拷贝 + 归档 WAL + `recovery_target_name` 恢复到
  `before_delete`，用单用户模式在暂停点直接查询：**60 行全在（含被删的 10 分钟）、
  恢复点后的提交（IXB 6 行）为 0**；主库当时为 36 行且持续可写
- ✅ 缺 WAL 恢复 → postgres `FATAL: recovery ended before configured recovery
  target was reached` → 任务 failed（错误含日志）、副本不可查（409/404）
- ✅ 恢复进程退出 → 看门狗 3 秒内置 failed 并清理
- ✅ 只允许一项在途 → 并发提交返回 409
- ✅ 失败可重试（生成新任务、不服务旧数据）
- ✅ API 重启清理：遗留 running/success 任务置 failed、目录删除、指针清空
- ✅ LSN/WAL 段名计算单元测试、compose YAML、demo.sh 语法、OpenAPI 生成

双源按小时供数（本轮新增，已真实运行验证）：

- ✅ `pytest api/tests/test_ingest.py`：**16 项全过**（真实 PostgreSQL 16.2）
  - 未报齐 → `/ingest/hours` 409 并带两源进度
  - 备用先报齐即由备用供数；主源报齐后整小时切主源，主源缺报分钟撤下备用读数
  - 主源已发布后，备用首次上报/迟到补报都**不会**退回备用
  - 同源同分钟同内容重传：`new_minutes=0`、`changed=false`、版本不变
  - 异内容整批 409，且同批合法的新分钟也**未落库**（事务整体回滚）
  - 同分钟不同来源互不干扰；批内重复分钟、越界分钟、非整点小时、无时区时间、
    负数/缺失计数或标记矛盾均按 400/422 拒绝
  - 同小时锁：持锁期间上报等待，释放后继续；超过等待上限返回 409 且未写入
  - 4 线程并发上报（两源交错分片 + 重复提交）：最终必为主源、60 行
  - 新进程（模拟重启）从已提交状态续报并完成发布
  - 数据库触发器阻止 `primary → backup` 的倒退 UPDATE
  - 事务中途失败（模拟崩溃）后结论/进度/业务行全部回滚，重试能正常发布
- ✅ 真实 uvicorn 进程 + `scripts/demo-dual-source.sh` 全流程通过（补传、切换、幂等、
  拒收、不倒退、未就绪 409）
- ✅ 并发压测：8 路并行上报 200 全成功；主/备整小时同时上送 5 轮，最终都为主源
- ✅ `/ingest/hours` 在 REPEATABLE READ 快照内读取：读取期间切换提交，返回的来源
  与明细仍属同一版本（已用注入切换的方式验证）
- ✅ `db/init/01-init.sql` 在空库上建表 + 建触发器成功，重复执行幂等
- ✅ OpenAPI 生成、`/docs` 200、`/demo/reset` 需 `confirm=true`
- ✅ 6 个进程同时冷启动建 schema（并发 DDL 竞争）全部成功

未能在本环境验证（沙箱限制所致）：

- ⚠️ `docker compose up` 整体构建与联调（沙箱无 docker）
- ⚠️ `pg_basebackup` 成功路径（沙箱禁止 checkpoint 信号；失败路径已验证）
- ⚠️ 热备副本接受连接后的 API 成功闭环（`status=success` 与 `/replica/traffic` 200）：
  沙箱中 postmaster 无法收到热备就绪信号。恢复本身已验证到达目标点（postgres 日志
  `recovery stopping at restore point` + `pausing at the end of recovery`），
  数据正确性已用单用户模式验证（见上）
- ⚠️ `pg_ctl stop` 退役旧副本（沙箱无法发信号；docker 中正常）

**关于本沙箱的一个新发现**：除信号限制外，本沙箱里任何被阻塞在锁上的 backend
即使等锁方释放也**不会被唤醒**（用纯 psycopg 复现：行锁、advisory 锁、
`lock_timeout` 均不触发）。因此双源并发串行化没有采用会无限期阻塞的
`pg_advisory_xact_lock` / `SELECT … FOR UPDATE`，改为 `pg_try_advisory_xact_lock`
有界轮询（`LOCK_WAIT_SEC`，默认 20s），保证在受限环境同样可用；正常环境两种写法
语义一致（同一 `(路口,小时)` 同一时刻仅一个写入者，持锁到提交）。

## 限制

- 单 api 进程、单副本端口：同一时刻一个恢复、一个副本（需求即如此）。
- 基准备份为启动时一份；api 重启会重取备份，此前创建的恢复点会标记 `expired`。
- 恢复点名称唯一（重复创建返回 409）；WAL 归档卷只增不减，演示规模无清理策略。
- 无认证鉴权，仅限内网演示。
- 双源：一个路口的某个小时一旦由主源发布就不会回退（即使主源之后数据有变，也只能
  追加分钟，不能改已确认分钟——结论按设计不可变）。上报是“全量批次”语义：一批里
  不含的分钟视为“还没报”，不会被当作缺报。
- 双源台账放在源库 `gateway_ingest` schema（PITR 库多了一个 schema，业务表仍只有
  `traffic_minute_counts` 一张）；恢复副本是源库的物理拷贝，因此镜像里也带这份台账，
  `/replica/traffic` 仍只查业务表。
