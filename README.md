# qq_export 1.1.1

下载项目zip，导入LiteLoaderQQNT模块，重启后即可使用，有网页ui

以下内容为ai生成

---

LiteLoaderQQNT 聊天记录导出插件。默认仅监听 `127.0.0.1:18765`；端口被占用时会顺延到后续端口。

可通过环境变量设置：

- `QQ_EXPORT_PORT`：首选监听端口。
- `QQ_EXPORT_HOST`：监听地址，默认 `127.0.0.1`。
- `QQ_EXPORT_INCREMENTAL_OVERLAP_MS`：增量更新时间重叠区，默认 10 分钟。
- `QQ_EXPORT_INCREMENTAL_OVERLAP_SEQ`：增量更新消息序号重叠区，默认 2000。

## 增量导出

增量导出不再从“最后时间戳 + 1”开始。插件会同时按时间戳和消息序号向前重叠读取，再把新结果与最近一次相同会话的完整导出记录合并、去重并重新排序。这样可以覆盖同一时间戳的多条消息、延迟写入消息和分页边界消息。

增量结果是完整合并后的会话记录，而不是只包含新增消息的独立片段。

## HTTP API

主要接口：

- `GET /api/status`：状态、端口、当前账号信息。
- `GET /api/sessions?q=...`：查询会话。
- `POST /api/export`：创建导出任务。
- `POST /api/sync`：创建默认启用增量合并的 JSONL 同步任务。
- `GET /api/jobs/{jobId}`：查询任务状态。
- `POST /api/jobs/{jobId}/stop`：停止任务。
- `GET /api/jobs/{jobId}/manifest`：读取网页端原生的 `manifest.json` 和 chunk 文件清单。
- `GET /api/jobs/{jobId}/files/chunks/chunk_XXXX.jsonl`：逐个下载 manifest 声明的分块文件。
- `GET /api/jobs/{jobId}/messages`：兼容接口，将所有 chunk 临时串联为一个 NDJSON 流。
- `GET /api/history`：查询导出历史。
- `GET /api/history/latest?sessionType=group&sessionId=群号`：查询会话最新导出记录。
- `GET /api/history/{historyId}/manifest`：读取历史导出的 `manifest.json` 和 chunk 文件清单。
- `GET /api/history/{historyId}/files/chunks/chunk_XXXX.jsonl`：下载历史导出的单个 chunk。
- `GET /api/history/{historyId}/messages`：兼容接口，将所有 chunk 临时串联为一个 NDJSON 流。
- `POST /api/merge`：合并相同会话的多个导出目录或历史记录。
- `GET /api/logs`、`GET /api/tasks`：读取日志和任务列表。
- `POST /api/choose-folder`、`POST /api/open-folder`、`POST /api/open-web`：控制图形界面相关操作。

导出示例：

```bash
curl -sS -X POST http://127.0.0.1:18765/api/export \
  -H 'Content-Type: application/json' \
  -d '{
    "chatType":"group",
    "id":"123456789",
    "format":"jsonl",
    "incremental":true,
    "autoMergeIncremental":true,
    "incrementalOverlapMs":600000,
    "incrementalOverlapSeq":2000
  }'
```

合并示例：

```bash
curl -sS -X POST http://127.0.0.1:18765/api/merge \
  -H 'Content-Type: application/json' \
  -d '{
    "historyIds":["历史记录ID-1","历史记录ID-2"],
    "chatType":"group",
    "sessionId":"123456789",
    "sessionName":"示例群"
  }'
```

合并逻辑不要求来源属于同一个登录账号，只要求它们表示相同会话。去重优先使用消息 ID，其次使用消息序号、发送者和内容指纹；不会仅凭时间戳和文本删除不同序号的重复发言。

## 输出格式

JSONL 使用 QCE 分块布局。网页端和 API 的正式文件接口都保留该布局，不会把多个文件改造成单个 `history.jsonl`：

- `manifest.json`
- `chunks/chunk_0001.jsonl`
- `chunks/chunk_0002.jsonl`（记录较大时）

每次重写会清理旧分块，避免旧的多余 chunk 残留。


## 分块 API 示例

```bash
# 先获取 manifest 与文件列表
curl -sS http://127.0.0.1:18765/api/jobs/JOB_ID/manifest

# 再逐个下载文件
curl -o chunk_0001.jsonl \
  http://127.0.0.1:18765/api/jobs/JOB_ID/files/chunks/chunk_0001.jsonl
```

文件接口只允许访问 `manifest.json` 中声明的 chunk，且会拒绝目录穿越路径。
