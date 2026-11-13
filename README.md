# 档案 OCR 跨页检索服务（FastAPI + SQLite）

为档案数据工程师提供的检索 API：研究者用一段 OCR 文字就能定位扫描件中的
**原句**，即使该句子跨块、跨页切分，也能返回可直接用于高亮的块 ID、矩形
坐标与原文字符范围。

## 能力对照

| 需求 | 实现 |
| --- | --- |
| 按文档上传 UTF-8 NDJSON，行序即阅读顺序 | `PUT /documents/{doc_id}`，逐块流式接收，字段：`block_id`（文档内唯一）、`page`、`text`、`rect{x0,y0,x1,y1}` |
| 查询去空白后 1–80 字符，原文匹配也忽略空白 | 服务端对查询与存储文本统一做 Unicode 空白剥离后匹配 |
| 允许跨块、跨页，重叠命中分别保留 | 倒排表含跨块边界二元组；每个位置都是独立行，重叠命中天然各自保留 |
| 返回最早 100 处命中 | 按阅读顺序（全局紧凑字符位置）返回，`has_more` 表示还有第 101 处真命中 |
| 命中含块 ID、坐标、原文字符范围 | 每个 hit 的 `spans[]` 给出 `block_id/page/rect/char_start/char_end` |
| 文档可大于内存 | 接收、建索引、搜索均流式/分批，不载入整篇文本或全部位置映射（见下文设计） |
| 整篇重传更正，后台建索引期间旧版仍可查 | 新版本独立暂存表，旧版本持续可查；就绪后单事务原子切换 |
| 原文、位置映射、索引一起切换 | 切换只在三者全部落盘后发生，单事务更新目录指针 |
| 同文档同时只接受一次替换 | 进程内 asyncio 锁 + SQLite 目录状态双重保证，冲突返回 409 |
| 坏行 / 写盘失败 / 进程中断保留旧版 | 失败版本标记 `failed` 并清理暂存表；启动恢复把未完成版本统一标失败 |
| 撤档 API：提交文档 ID 与撤档号，撤档号幂等 | `POST /documents/{doc_id}/withdrawals`，重复撤档号直接返回首次回执（`replayed=true`） |
| 受理后文档立即下线，新检索 404，已选定版本的在途检索完整结束 | 撤档是单事务目录切换；在途读注册表保证读者用到的表等请求结束才删 |
| 上传未结束也可撤档，旧上传/旧后台发布不得复活档案 | 代次（epoch）守卫：旧版本属于旧代次，激活时检测代次不匹配即静默退出 |
| 同 ID 可重新上传，旧执行收尾与旧回执重试不得影响新档案 | 重传开启新代次；回收按显式版本号删表，永不按 doc_id 误伤新数据 |
| 后台回收撤档前各版原文与索引，失败/重启自动续清 | 回收范围逐版本持久化在 `withdrawal_items`，回执状态机 `accepted→reclaiming→completed/failed` |
| 旧执行退出且旧表清完才报完成，可按回执查状态与错误 | `GET /withdrawals/{withdrawal_id}` 返回状态、错误与每项回收进度 |
| 可启动的 Docker Compose + 交互式文档演示 | `docker compose up` 后访问 Swagger UI，含一键演示数据与撤档演示脚本 |

## 快速开始（Docker Compose）

```bash
# 1. 启动 API（自动构建镜像，SQLite 数据存放在命名卷 archive-data）
docker compose up --build -d

# 2. 灌入演示文档（gazette 公报、ledger 税册，幂等，可重复执行）
docker compose --profile demo run --rm seed

# 3. 打开交互式 API 文档
#    Swagger UI:  http://localhost:8000/docs
#    ReDoc:       http://localhost:8000/redoc
```

在 Swagger UI 中可直接试：

1. `PUT /documents/{doc_id}` —— 上传/替换文档，`doc_id` 填 `gazette`，
   Request body 选 `application/x-ndjson`，粘贴 `demo/gazette.ndjson` 内容；
2. `GET /documents/{doc_id}/search` —— `doc_id=gazette`，试这些查询：
   - `可用。第二條` —— **跨第 1、2 页**的命中，`spans` 含两页的块与坐标；
   - `轉型，保障` —— 同页跨块命中；
   - `檢索 系統應當 忽略排版空白` —— 查询自带任意空白仍命中；
   - `檔` —— 单字查询，多处重叠/重复命中按顺序返回；
3. 再次执行 `PUT` 粘贴修改后的 NDJSON，即完成整篇更正；
   `GET /documents/gazette/versions` 可看到旧 `ready` 版本与新版本。
4. 撤档演示：在 `POST /documents/{doc_id}/withdrawals` 中填 `doc_id=gazette`、
   请求体 `{"withdrawal_id": "WD-001"}`，执行后用
   `GET /withdrawals/{withdrawal_id}`（填 `WD-001`）观察 `accepted →
   reclaiming → completed`；再对同一 `doc_id` 执行 `PUT` 即完成同号重传。
   重复提交相同撤档号可验证幂等回执（HTTP 200、`replayed=true`）。

不用 Docker 时（需 Python 3.11 并能安装依赖）：

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
ARCHIVE_API_BASE=http://localhost:8000 python -I scripts/seed_demo.py
ARCHIVE_API_BASE=http://localhost:8000 python -I scripts/demo_requests.py
```

## NDJSON 输入格式

每行一个 JSON 对象，UTF-8 编码；空行忽略；行顺序即阅读顺序：

```json
{"block_id": "gazette-p1-b4", "page": 1, "text": "保障電子文檔的真實、完整與可用。", "rect": {"x0": 96.2, "y0": 199.8, "x1": 470.5, "y1": 232.0}}
```

- `block_id`：该文档内非空且唯一，重复即整次上传失败（400）；
- `page`：正整数页码（扫描页，1 起）；
- `text`：OCR 原文，空白原样保留（用于高亮回映），匹配时忽略所有空白；
- `rect`：扫描件上的矩形，`x1>=x0`、`y1>=y0`，允许零面积矩形。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 健康检查（含 Compose 健康检查） |
| `GET` | `/documents` | 文档清单及当前生效版本（已撤档且未重传的文档不出现） |
| `PUT` | `/documents/{doc_id}` | 整篇上传/替换（202，后台建索引）；坏行 400、写盘失败 507、并发替换 409 |
| `GET` | `/documents/{doc_id}/search?q=...` | 检索，去空白后 1–80 字符；未知/已撤档文档 404 |
| `GET` | `/documents/{doc_id}/versions` | 版本与状态（ready/failed/ingesting/indexing/withdrawn） |
| `POST` | `/documents/{doc_id}/withdrawals` | 撤档，请求体 `{"withdrawal_id": "撤档号"}`；首次 202，重复撤档号 200 返回原回执，未知文档 404、撤档号撞他档 409 |
| `GET` | `/withdrawals/{withdrawal_id}` | 按撤档号查回执：回收状态、错误信息、逐版本回收范围与进度 |
| `GET` | `/recovery` | 本次启动完成的崩溃恢复记录 |

命中结构（节选）：

```json
{
  "compact_start": 44, "compact_end": 50,
  "matched_text": "可用。第二條",
  "crosses_blocks": true, "crosses_pages": true,
  "spans": [
    {"block_id": "gazette-p1-b4", "page": 1,
     "rect": {"x0": 96.2, "y0": 199.8, "x1": 470.5, "y1": 232.0},
     "char_start": 13, "char_end": 16, "block_order": 3},
    {"block_id": "gazette-p2-b1", "page": 2,
     "rect": {"x0": 96.2, "y0": 110.0, "x1": 520.4, "y1": 142.2},
     "char_start": 0, "char_end": 3, "block_order": 4}
  ]
}
```

`char_start/char_end` 是该块**原始 `text`**（含空白）上的半开区间，前端可直接
切片高亮；矩形坐标可用于扫描件上画框。

## 设计要点：为什么大文档不会撑爆内存

- **接收**：`request.stream()` 分块进入有界队列（默认 32 块），工作线程逐行
  增量 UTF-8 解码与 JSON 解析，单行超过 4MB 直接判失败；
- **建索引**：每读一块就增量计算紧凑字符位置，按批（默认 500 块）写入该版本
  独立的三张表，绝不累积整篇文本：
  - `blocks_<vid>`：原文、矩形、每块紧凑起止位置（回表与字符回映）；
  - `unigrams_<vid>(ch, pos)`：单字倒排；
  - `bigrams_<vid>(bg, pos)`：二元组倒排，含**跨块边界二元组**（含跨页）；
- **搜索**：单字查询走 unigram；多字查询选取频次最低的二元组，按位置有序
  游标读取候选，逐个只回表读取命中跨度涉及的少数块做验证，单查询只读一致
  快照，不加载全量位置映射；
- **内存自测**：`tests` 中用 2 万块的文档断言 `tracemalloc` 峰值 < 4MB。

## 版本、原子切换与崩溃恢复

- 每次替换创建新版本号与独立暂存表，状态机
  `ingesting → indexing → ready`（异常 → `failed`）；
- 只有 `ready` 版本可被检索；激活在单事务内翻转 `documents.active_version_id`，
  旧版表在切换提交后的第二个事务中删除，读者始终看到完整的一版；
- 坏行、重复块 ID、写盘异常都在请求内把新版本标 `failed` 并清表，旧版不动；
- 进程被杀死后，重启时 `startup_recovery()` 把所有非终态版本标 `failed`、
  删除残留暂存表，并通过 `/recovery` 暴露处理记录。

## 撤档（撤下错误 OCR）与同档案号重传

档案扫描包归档错误时，先撤下 OCR，再用**同一档案号**接收正确内容。撤档把
"目录可见性切换"（同步、立即生效）与"物理存储回收"（异步、可续清）拆成两个阶段。

### 一次完整流程

```bash
# 1. 撤档：提交文档 ID 与客户端自定的撤档号（幂等键）
curl -X POST http://localhost:8000/documents/gazette/withdrawals \
  -H 'content-type: application/json' \
  -d '{"withdrawal_id": "WD-2026-0001"}'
# -> 202，回执含 state=accepted 与 items[]（撤档前各版本的回收范围）

# 2. 同一撤档号重复提交 -> 200，返回首次回执（replayed=true），不会二次撤档

# 3. 文档立即从 GET /documents 消失；新检索返回 404
curl http://localhost:8000/documents/gazette/search?q=...   # -> 404

# 4. 按撤档号查回执，直到 state=completed（后台回收各版 blocks/unigrams/bigrams）
curl http://localhost:8000/withdrawals/WD-2026-0001

# 5. 同一档案号重新上传正确内容，即开启全新代次，立即可检索
curl -X PUT --data-binary @demo/gazette.ndjson \
  http://localhost:8000/documents/gazette
```

也可直接运行 stdlib 演示脚本（容器内外均可）：

```bash
ARCHIVE_API_BASE=http://localhost:8000 python -I scripts/demo_withdrawal.py
```

### 并发与安全语义

- **上传未结束也能撤档**：撤档事务把在途版本立即标 `withdrawn` 并登记进回收
  范围，上传连接不会被强制中断；旧上传随后写完的数据只会落在旧代次的表里。
- **旧后台发布不得复活档案**：每个版本带 `epoch`（代次）。撤档时文档代次 +1，
  旧上传结束后的激活步骤检测到代次不匹配会抛 `StaleVersionError` 并静默退出，
  绝不修改目录指针。
- **同 ID 重传不受旧执行影响**：重传的新版本属于新代次；回收只按回执中显式的
  版本号删表，永不按 `doc_id` 匹配，因此不可能误删新档案。旧回执重试同样只
  作用于旧代次范围。
- **在途检索完整结束**：检索在选定版本后注册读守卫（`read_guard`），撤档可以
  立即隐藏文档，但该版本的表要等所有在途读结束才会被 DROP；回收项保持
  `pending`，由后台 worker 与条件变量唤醒重试。
- **旧执行退出且旧表清完才报完成**：在途入库/发布任务通过作业守卫
  （`job_guard`）登记，回执仅在旧作业退出**且**范围内所有表删除后才变为
  `completed`。
- **其他档案不受影响**：回收逐表进行且全程持有短事务，其他文档的上传与检索
  继续可用（SQLite WAL 允许读写并发）。
- **持久化与自动续清**：回执（`withdrawals`）与回收范围（`withdrawal_items`，
  逐版本一行）一并落盘。DROP 失败把回执置 `failed` 并记录 `error`，下一轮自动
  重试；进程重启后启动恢复会回填缺失的范围行并继续清理，已完成的回执幂等跳过。

回执状态机：

```
accepted ──► reclaiming ──► completed
                 │  ▲
                 ▼  │（下一轮/重启自动重试，error 记录上次失败原因）
               failed
```

回执示例（节选）：

```json
{
  "withdrawal_id": "WD-2026-0001",
  "doc_id": "gazette",
  "epoch": 0,
  "state": "completed",
  "replayed": false,
  "error": null,
  "items": [
    {"version_id": 1, "state": "done", "updated_at": 1794592247.49}
  ]
}
```

## 测试

```bash
# 标准环境（有 fastapi/pytest 时）
pytest -q

# 离线/无包索引环境：stdlib 垫片 + unittest（本仓库沙箱即此模式）
python -I scripts/run_tests.py     # 37 个单元/集成测试（含 16 个撤档工作流测试）
python -I scripts/smoke_test.py    # 真实 HTTP 端到端冒烟（守护线程内 WSGI，含撤档/同号重传）
```

`tests/_shim/` 仅是离线环境对 fastapi/pydantic 的最小垫片，不进入 Docker 镜像
（见 `.dockerignore`），生产代码只依赖 `requirements.txt` 中的真实组件。

## 目录结构

```
app/            FastAPI 应用、SQLite 目录、流式入库、检索引擎、撤档回收、响应模型
demo/           两份中文 NDJSON 示例文档
scripts/        演示播种、检索/撤档演示、测试运行器与离线冒烟
tests/          测试与离线垫片
Dockerfile, docker-compose.yml, requirements.txt
```
