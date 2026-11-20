# API集成与事件编排平台

面向第三方API接入、Webhook事件接收、消息队列消费与服务编排的全链路API数据集成平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| API端点注册 | `api_endpoint` | 端点配置 | 端点编号、端点名称、请求方法 |
| Webhook通道 | `webhook_channel` | 通道配置 | 通道编号、通道名称、接收路径 |
| 事件结构定义 | `event_schema` | 事件结构 | 结构编号、事件类型、字段名称 |
| 消息队列消费 | `mq_consumer` | 消费配置 | 配置编号、队列名称、消费组名 |
| 死信队列管理 | `dlq_handler` | 死信记录 | 记录编号、原始队列、失败原因 |
| 认证凭证管理 | `api_auth_store` | 认证凭证 | 凭证编号、关联端点、凭证类型 |
| 流量控制 | `rate_limit_config` | 限流规则 | 规则编号、关联端点、请求间隔 |
| 数据转换编排 | `data_transform` | 转换规则 | 规则编号、源Schema、目标Schema |
| 事件分发路由 | `dispatch_rule` | 分发规则 | 规则编号、事件类型、目标系统 |
| 事件去重 | `dedup_filter` | 去重规则 | 规则编号、事件类型、去重键列 |
| 批量接入调度 | `batch_ingest` | 接入任务 | 任务编号、关联端点、Cron表达式 |
| 数据落湖 | `data_lake_sink` | 落湖配置 | 配置编号、数据来源、目标路径 |
| 接入质量校验 | `data_quality_rule` | 质量规则 | 规则编号、数据来源、校验字段 |
| 调用链路追踪 | `call_trace_log` | 调用记录 | 记录编号、关联端点、请求参数 |
| 增量水位管理 | `incre_cursor` | 水位标记 | 标记编号、关联端点、增量字段 |
| 历史回补任务 | `backfill_job` | 回补任务 | 任务编号、关联端点、起始日期 |
| 事件归档清理 | `event_archive` | 归档策略 | 策略编号、事件类型、归档条件 |
| 集成监控看板 | `integration_monitor` | 监控指标 | 指标编号、监控对象、成功率 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA

## 归档格式升级与分区恢复

`event_archive` 模块在原有归档策略之外，新增了一条可重试的归档升级链路：

| 接口 | 说明 |
| --- | --- |
| `GET /api/archive/batches` | 列出归档批次、分区和文件格式 |
| `POST /api/archive/{batch}/migrate` | 旧版 `zlib` → 带分区摘要的 `gzip-v2`，可重复执行 |
| `POST /api/archive/{batch}/verify` | 校验 gzip v2 的字节摘要、分区摘要和事件身份 |
| `POST /api/archive/restore` | 恢复入口，按分区返回回执 |
| `GET /api/archive/{batch}/manifest` | 批次恢复清单（已恢复来源记录、实际文件） |
| `GET /api/archive/receipts/{id}` | 单条回执，含实际使用的压缩文件与被拒绝候选 |
| `GET /api/archive/receipts/{id}/legacy` | 旧版恢复环境的回执视图 |
| `GET /api/archive/source-row?partition=&event_id=` | 从已恢复的来源行反查原回执 |
| `GET /api/archive/lake?partition=` | 查询数据湖原分区，核对实际恢复的记录 |
| `GET /api/archive/{batch}/inspect?partition=` | 检查 gzip 头、日期、分区和事件身份 |

对应链路入口：`POST /api/pipeline/archive-migrate`、
`POST /api/pipeline/archive-restore-to-lake`、
`GET /api/pipeline/archive-lake-verify`。前端「归档恢复核对」页面复用同一组接口。

### 升级部署约定

- **迁移重试**：资产按 `批次/分区/格式/版本` 登记在 `<data_store>/archive/_state/assets.json`。
  已通过校验且分区摘要一致时，重试直接复用，不产生新版本；未校验版本不会被当成已迁移。
- **恢复选择**：`auto` 模式优先使用已通过校验的 gzip v2，否则回退到可读旧版 `zlib`；
  损坏的旧文件和未校验/被篡改的新版都会出现在回执的 `rejected_candidates` 里，绝不冒充可用数据。
  `gzip` 模式只接受已校验 gzip v2，`legacy` 模式模拟旧版恢复环境。
- **数据湖**：恢复数据写入 `lake/events/<分区>/<年>/<月>/<日>`，每行带
  `archive_partition`、`archive_date`、`restore_receipt_id` 和 `archive_source_file`；
  按事件身份去重，上线重试或回滚都不会多灌同一个源记录。
- **恢复清单**：每次恢复运行把结果回写到 `<data_store>/archive/_state/manifests/<批次>.json`，
  失败分区的 `last_failure` 与上一次成功的 `source_file`/`event_ids` 同时保留。
- **回滚可查**：回执、恢复清单和湖内索引都持久化，旧版恢复环境重启后仍能按回执 ID
  或来源行反查原始压缩文件。

归档、数据湖和 JSON 仓库默认落在 `backend/data_store`，可用 `APP_DATA_STORE_DIR`
指向独立目录（测试即使用该变量隔离）。
