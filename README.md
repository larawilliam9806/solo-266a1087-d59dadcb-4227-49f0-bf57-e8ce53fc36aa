# Click Lake Erasure API

面向广告数据分析的点击数据湖服务：按日追加上传点击 CSV、本地 Parquet 存储、按日下载，
支持 GDPR 风格的用户数据清除（erasure）——受理后立即拦截该用户的新数据，后台重写
受影响文件，完成后永久拦截；并提供**报表取数前的核对与凭证下载**能力——核对各页面
点击数与数据湖当日活动文件一致后才签发下载凭证。

技术栈：**FastAPI + PyArrow + SQLite**（SQLite 作为状态库，Parquet 作为数据湖格式）。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

服务监听 `http://localhost:8000`，交互式文档见 `http://localhost:8000/docs`。
数据与状态持久化在命名卷 `clicklake-data` 中。

### 本地开发

```bash
pip install -r requirements-dev.txt
export DATA_DIR=./data STATE_DIR=./state
uvicorn --factory app.main:create_app --reload
```

### 运行测试

```bash
python -m pytest tests/ -q
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/upload/{date}` | 上传当日点击 CSV（multipart 文件字段 `file`），返回接收/拦截/入库行数 |
| `GET` | `/dates` | 列出所有日期（文件数、行数、是否受限） |
| `GET` | `/dates/{date}/download?format=csv\|parquet` | 按日下载；日期受限时返回 `409` |
| `POST` | `/erasure` | 提交清除请求 `{"user_id": "..."}`（幂等，重复提交返回同一任务） |
| `GET` | `/erasure/{task_id}` | 查询清除进度 |
| `GET` | `/erasure?user_id=...` | 列出清除任务 |
| `POST` | `/reconcile` | 报表核对：提交 UTC 日期与各页面预期点击数，全部相符才签发下载凭证 |
| `GET` | `/report/download?voucher_id=...&format=csv\|parquet` | 凭凭证下载原始点击 |
| `GET` | `/vouchers/{voucher_id}` | 查询凭证状态（有效/失效原因/绑定文件集） |
| `GET` | `/health` | 健康检查 |

### 上传 CSV 格式

表头必须包含 `user_id,page,timestamp`（额外列被忽略）。时间戳支持 ISO-8601
（建议带 `Z` 或时区偏移）与 Unix 秒，统一归一化为 UTC 存储。

```bash
curl -X POST -F "file=@clicks.csv" http://localhost:8000/upload/2026-11-01
# {"date":"2026-11-01","received":4,"invalid":0,"blocked":1,"stored":3,"file_id":"..."}
```

- `received`：CSV 数据行总数；`invalid`：格式非法被跳过的行数
- `blocked`：因清除请求被拦截（未写入）的行数；`stored`：实际入库行数

### 清除流程

```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{"user_id":"alice"}' http://localhost:8000/erasure
# 201 Created（重复提交返回 200 与同一 task_id）

curl http://localhost:8000/erasure/<task_id>
# {"status":"processing","files_total":2,"files_done":1,"restricted_dates":["2026-11-01"],...}
```

任务状态机：`pending → processing → completed`，失败时为 `failed`（自动重试，重启后继续）。

## 清除语义（保证）

- **立即拦截**：请求受理的同一事务内，该用户即进入拦截集合；之后提交的上传直接丢弃其记录。
- **在途上传不写回**：上传的"过滤 → 写 Parquet → 提交状态"与"受理清除"在同一把提交锁下
  完成，两种操作的临界区不会交错，因此在途上传不可能把被清除用户的数据写回。
- **下载限制**：含该用户的日期在任务完成前暂停新下载（旧链接同样 `409`）；已开始的下载
  在日期锁保护下读取完数据后再响应，可正常结束；其他日期的导入与下载不受影响。
- **完成标准**：后台逐个重写受影响文件（tmp + fsync + 原子 rename），删除含目标数据的旧
  文件后才标记完成；其他用户记录完整保留。
- **交错安全**：单一后台 worker 顺序处理任务；文件被重写时，其他任务的待处理记录在
  同一事务内被重定向到新文件，多个清除与导入交错不会漏删、复活或丢失他人记录。
  完成前还有一次全量校验扫描作为兜底。
- **失败与重启**：写盘失败时任务进入 `failed` 并自动重试，期间拦截与日期限制保持有效；
  进程重启后恢复所有未完成任务，并清理临时文件、孤儿文件与未完成的删除。
- **幂等与永久拦截**：每个用户终生对应同一任务（`user_id` 唯一约束），重复提交返回同一
  任务；任务行永不删除，因此完成后该用户被永久拦截。

## 报表核对与凭证下载（保证）

报表取数前先核对"点击日分区"与上游页面计数清单是否一致，只有核对通过才能下载原始点击。

### 流程

```bash
# 1) 提交 UTC 日期 + 各页面预期点击数
curl -X POST -H "Content-Type: application/json" \
     -d '{"date":"2026-11-01","pages":{"/home":3,"/pricing":1}}' \
     http://localhost:8000/reconcile
# 201 Created
# {"voucher_id":"...","date":"2026-11-01","files":2,"file_ids":[...],
#  "expected_counts":{...},"actual_counts":{...}}

# 计数不符时返回 409，并回传实测值，不签发任何凭证
# {"detail":{"error":"count_mismatch","date":"...","expected":{...},"actual":{...}}}

# 2) 用凭证下载原始点击（CSV 或 Parquet）
curl "http://localhost:8000/report/download?voucher_id=...&format=csv"
```

### 语义保证

- **按当天活动文件核算**：核对在提交锁（与上传/清除受理同一把 `commit_lock`）与日期锁下
  读取该日**全部 `active` Parquet 文件**统计每页点击数，因此观察到的是一组完整、一致的
  活动文件，不可能与上传或清除重写交错。
- **全部相符 + 未受限才签发**：页面清单与实测计数必须逐项完全相等（多页、缺页、计数不同
  均为不匹配，返回 `409`）；当天处于清除限制中返回 `409`，当天无数据返回 `404`。
- **凭证绑定文件集快照**：凭证记录签发时刻的活动文件 ID 集合；下载只读取该文件集，不会
  取到之后新入湖或被重写的数据。
- **重复核对使旧凭证失效**：再次提交该日清单时，旧凭证先在状态事务内失效；即使新清单
  不匹配、没有新凭证产生，旧凭证也无法再使用（失效原因 `reconciled_again`）。
- **新点击入湖立即失效**：该日有新上传成功写入（产生新的活动文件）时，旧凭证在上传
  同一事务内失效（原因 `new_upload`）。全部被清除拦截、未写入任何文件的上传不改变文件集，
  不影响既有凭证。
- **清除受理立即失效、失败仍拒绝**：受理涉及该日的用户清除时，该日凭证立即失效
  （原因 `erasure_accepted`）；清除任务失败重试期间日期保持受限，凭证仍不可用、新核对
  也被拒绝；清除完成后须重新核对获取新凭证。其他日期的核对与下载不受影响。
- **在途下载可按其文件集完成**：凭证下载在日期锁保护下读完绑定的整组文件后再响应；
  此后即使清除重写开始，已开始的下载仍正常结束，但同一凭证的后续使用会被拒绝。
- **失败不产生可用凭证**：核对过程中读盘失败或凭证登记失败都会在凭证落库前抛出，数据库
  中不存在任何可用凭证行；已发生的旧凭证失效仍然保留。
- **持久化**：凭证与失效状态保存在 SQLite 中，进程重启后依然有效/失效，不随内存丢失。

## 数据布局

```
data/
  date=2026-11-01/
    part-<file_id>.parquet      # 每次上传追加一个 part 文件
state/
  erasure.db                    # SQLite：文件清单、任务、限制日期、用户-日期索引、核对凭证
```

Parquet schema：`user_id: string, page: string, ts: timestamp[us, tz=UTC]`。

凭证表 `reconciliation_vouchers` 保存凭证 ID、日期、绑定的活动文件 ID 集合（快照）、
提交与实测的页面计数、`valid/invalidated` 状态及失效原因（`reconciled_again`、
`new_upload`、`erasure_accepted`）。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `./data` | Parquet 数据目录 |
| `STATE_DIR` | `./state` | SQLite 状态目录 |
| `ERASURE_RETRY_DELAY` | `5` | 失败任务重试间隔（秒） |
| `ERASURE_WORKER_POLL` | `0.2` | 后台 worker 轮询间隔（秒） |
