# Nginx 访问日志持续入仓（FastAPI + SQLite）

把 Nginx 的 JSON 行访问日志从共享只读目录**持续、精确一次（exactly-once）**
地采集进 SQLite，并提供按路径、状态码分页检索故障请求的 HTTP API。

采集按**有界、公平、分批**的方式轮询：每轮对每个文件最多读取
`READ_CHUNK_BYTES` 字节，只把完整行组成一个批次，与续采偏移在同一个
SQLite 事务里提交。大文件分多轮排空、单轮内存有硬上限，排在后面的短日志
每轮都能获得一次采集机会，不会再被大文件长期阻塞。

## 为什么不会漏行 / 重记 / 错用偏移 / 撑爆内存

| 风险 | 设计对策 |
| --- | --- |
| 大文件一次性读入，耗尽采集进程内存 | 每文件每轮只 `read` 最多 `READ_CHUNK_BYTES`（默认 1 MiB），大文件跨多轮排空；单轮最大驻留内存约为一个读取块 + 一个批次 + 一条跨轮残留行 |
| 大文件独占一轮，后面的短日志无法入仓 | 每轮稳定遍历目录内**每个**文件一次，各文件享有同样的有限字节预算；某文件未采完不影响其他文件当轮入仓 |
| 一行跨多个读取块被拆开 | 未遇 `\n` 的尾部作为跨轮 carry 保存在内存（不入库），已提交偏移始终停在该行首字节；多字节 UTF-8 按原始字节切分，绝不撕裂字符。进程退出后 carry 消失，重启从已提交偏移重读该行，不漏不重 |
| 轮转后把旧偏移用到新文件 | 文件身份用 `(st_dev, st_ino)`，**不是文件名**。rename 后旧 inode 与新 inode 各自独立追踪 |
| 旧文件轮转后仍被短暂写入 | 旧 inode 继续按已提交偏移采集到 EOF；归档永不删除、不截断 |
| 读到写了一半的行 | 只入仓以 `\n` 结尾的完整行；未换行尾部保持原偏移，下轮重读 |
| 单行超过批次可处理大小 | 超过 `MAX_LINE_BYTES`（默认 256 KiB，不含 `\n`）的行**不拆、不跳、不部分入仓**：偏移停在该行首字节，文件标记为 blocked，原因在 `/ingest/progress` 的 `blocked_reason` 说明；其余文件照常采集。行被修复到限制内后下一轮自动解除阻塞 |
| 进程在写入中途/前后退出 | 行数据与“下一字节偏移”在**同一个 SQLite 事务**提交，重启从最后提交点续采 |
| 查询读到半成品 | WAL + 事务隔离，查询只能看到已完整提交的事务 |
| 内容相同的两行 | 唯一性只约束物理位置 `(dev,inode,source_code,byte_offset)`，不含内容，重复内容各自成行 |
| JSON 坏行 | 隔离进 `malformed_lines` 表，不阻断后续行 |

## 目录结构

```
.
├── docker-compose.yml      # nginx + collector + traffic 一键启动
├── collector/              # FastAPI + SQLite 采集/查询服务
│   ├── app/storage.py      # 表结构、迁移、事务、查询
│   ├── app/ingest.py       # 有界读取、inode 识别、跨轮行重组、分批续采、超长行阻塞
│   └── app/main.py         # API（/docs 可用）
├── nginx/                  # JSON 行日志格式 + 可控轮转脚本
├── traffic/                # 持续请求生成器（含 4xx/5xx）
└── tests/                  # 80 项单元/集成断言 + 端到端模拟
```

## 快速开始

```bash
docker compose build
docker compose up -d
# 实时看日志
docker compose logs -f collector
```

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| API 文档 Swagger | http://localhost:8000/docs | 直接在页面上试所有接口 |
| 示例站点 | http://localhost:8080/ | Nginx，访问即产生 JSON 日志 |
| OpenAPI JSON | http://localhost:8000/openapi.json | |

### 可控轮转（手动）

```bash
# 随时手动轮转：rename 成唯一归档名 + nginx 重开同名新日志
docker compose exec nginx /usr/local/bin/rotate-nginx-log.sh

# 自定义归档后缀
docker compose exec nginx ROTATE_NAME=incident-1 \
    /usr/local/bin/rotate-nginx-log.sh
```

### 自动周期轮转（演示轮转与采集交错）

```bash
# 每 15 秒自动轮转一次
AUTO_ROTATE_INTERVAL=15 docker compose up -d
# 恢复纯手动
docker compose up -d --force-recreate nginx
```

轮转后共享目录里同时存在 `access.log` 与所有
`access.log.<时间戳>.log` 归档，采集器分别追踪、全部保留。

## 常用查询

```bash
# 采集进度（每个文件的 offset / size、是否采完、是否被超长行阻塞）
curl -s localhost:8000/ingest/progress | python3 -m json.tool

# 当某文件被超长行阻塞时，响应里会带 blocked 原因，例如：
#   "blocked": true,
#   "blocked_reason": "line at byte 1024 is 300000 bytes, exceeds
#                      max_line_bytes=262144; ingestion of this file is held
#                      at that offset until the line is shortened ...",
#   "blocked_at": "2026-11-14T10:00:00.000Z"
# 顶层 "blocked_count" 汇总当前阻塞文件数；last_poll.blocked_files 给出最近一轮明细。

# 所有 5xx 故障请求，分页
curl -s "localhost:8000/events?status_min=500&status_max=599&limit=50"

# 某路径的 404
curl -s "localhost:8000/events?path=/missing/page&status=404"

# 只看某个归档来源
curl -s "localhost:8000/events?source_code=access.log.20261113T100000Z.log"

# 翻页
curl -s "localhost:8000/events?limit=100&offset=200"

# 立即触发一次采集（默认每 0.5s 自动轮询）
curl -s -X POST localhost:8000/ingest/poll-now

# 无法解析的坏行
curl -s localhost:8000/malformed
```

## API 一览

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/health` | 存活、计数、采集线程状态、当前读取/行大小限制 |
| GET | `/ingest/progress` | 各来源文件 offset/size/是否完成/是否阻塞及 `blocked_reason` |
| POST | `/ingest/poll-now` | 立即执行一轮采集（每文件仍受字节预算约束） |
| GET | `/events` | 分页取数，支持 `path`、`status`、`status_min/max`、`source_code`、`limit`、`offset` |
| GET | `/events/{id}` | 单条事件（含原始 JSON 行 `raw_line`） |
| GET | `/malformed` | 坏行隔离区 |

## 数据模型

```
log_events(id, ts, path, status, method, source_code,
           dev, inode, byte_offset, raw_line, ingested_at)
ingest_state(dev, inode, source_code, path, offset, last_seen,
             blocked_reason, blocked_at)   -- 续采位置 + 超长行阻塞标记
malformed_lines(..., raw_line, error)                            -- 坏行隔离
```

- `source_code`：来源文件代号（当前文件名/归档名）。
- `byte_offset`：该行在其来源文件中的起始字节位置。
- 唯一索引 `(dev, inode, source_code, byte_offset)` 是防重的最后防线；
  内容不在键中，所以两行完全相同也分别保留。
- `blocked_reason` 非空表示该文件停在 `offset` 处的超长行上等待处理；
  旧版本数据库首次启动会自动补上这两列（无需手工迁移）。
- SQLite 运行在 `WAL + synchronous=FULL`：提交即落盘，读不阻塞写。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `LOG_DIR` | `/logs` | 共享日志目录（compose 中以 `:ro` 只读挂载） |
| `DB_PATH` | `/data/warehouse.db` | SQLite 数据库（独立可写卷） |
| `POLL_INTERVAL` | `0.5` | 轮询间隔秒 |
| `READ_CHUNK_BYTES` | `1048576`（1 MiB） | 每轮对**单个文件**最多读取的字节数；大文件分多轮入仓，单轮内存因此有硬上限 |
| `MAX_LINE_BYTES` | `262144`（256 KiB） | 单行最大字节数（不含 `\n`）；超限行不拆不跳，文件停在该行偏移并在进度接口报告阻塞。**不能大于 `READ_CHUNK_BYTES`**，否则启动直接报错 |
| `LOG_LEVEL` | `INFO` | 日志级别 |
| `AUTO_ROTATE_INTERVAL` | 空 | 仅 nginx 容器；非空时按秒自动轮转 |

> 两个限制都可通过环境变量（或项目根目录的 `.env`，见 `.env.example`）
> 在 `docker compose up` 时覆盖。配置不合法（如 `MAX_LINE_BYTES >=
> READ_CHUNK_BYTES`）会让采集器在启动阶段立即失败并打印原因，而不是静默卡死。

## 本地测试（无需 Docker）

```bash
python3 -m venv --without-pip .venv && . .venv/bin/activate
# 若 venv 内无 pip：python get-pip.py
pip install -r collector/requirements.txt httpx pyyaml
python -I tests/test_collector.py   # 80 项：有界分批/公平性/超长行阻塞/部分行/轮转/崩溃恢复/API
python -I tests/e2e_sim.py          # 完整服务启停 + 轮转 + 重启续采 E2E
```

覆盖的关键场景：每轮每文件字节预算与分批提交、跨多个读取块的长行/多字节
字符不被撕裂、大文件不饿死同目录短日志、超长行保留原偏移并在进度接口说明
阻塞原因且修复后自动解除、未换行尾部等待补齐、rename 轮转双 inode、旧 inode
延迟写入、分批提交流程中多轮随机“进程退出”后续采不漏不重、相同内容多行保留、
坏行隔离、分页与过滤。

## 故障排查

```bash
docker compose ps
docker compose logs --tail=100 collector
docker compose exec nginx ls -li /var/log/nginx/json   # 看 inode 与归档
docker compose exec collector sh -c 'ls -li /logs'      # 只读视角
# 入库数与文件总字节核对
curl -s localhost:8000/ingest/progress

# 某文件长期不前进时，先看是否被超长行阻塞：
curl -s localhost:8000/ingest/progress \
  | python3 -c 'import json,sys; d=json.load(sys.stdin);
[print(f["source_code"], f["blocked_reason"]) for f in d["files"] if f["blocked"]]'
```

### 处理被阻塞的文件

`/ingest/progress` 中 `blocked=true` 表示该文件在 `offset` 处有一行超过
`MAX_LINE_BYTES`。采集器不会拆分或跳过它，处理方式二选一：

1. 在日志生产端（或归档文件上）把该行缩短到限制内，下一轮采集会自动从原
   偏移续采并清除阻塞标记；
2. 若该行属预期的大报文，调大环境变量 `MAX_LINE_BYTES`（保持严格小于
   `READ_CHUNK_BYTES`，必要时一并调大后者）后重启 collector。
