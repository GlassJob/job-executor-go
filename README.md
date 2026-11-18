# job-executor-go

GlassJob Executor Go SDK（WebSocket 协议 **1.1**）。

## 能力

- HMAC `auth.request`（secret 不进帧）
- `service.register` / `heartbeat` / `unregister`
- `task.execute` → ack / started / completed|failed
- `JobContext`：`BindState` / `Checkpoint` / `SetNextState`（对接中心 `job_states.key=default`）
- `job.Log()`：独立上报 `task.log`（默认不打本机；缓冲 100 / 1s flush；可与自有 slog 双写封装）
- `task.cancel` → cancel_ack / cancelled（配合 Admin 取消）
- WS 断线续跑：任务不随断线取消；重连 `task.sync_running`；终态写入 `{JC_DATA_DIR}/pending_results` 直至 `result_ack OK`

示例 Job：`demo.hello` / `demo.fail` / `demo.sleep` / `demo.timeout` / `demo.counter`（状态累加）/ `demo.progress` / `demo.logs`

## 快速开始

在模块根目录复制 `.env.example` → `.env` 填入凭据后：

```powershell
go run ./examples/go-executor
```

示例会自动加载 `.env`（真实环境变量优先）。也可手动 `$env:JC_CREDENTIAL_ID=...`。

Admin 触发：`POST /v1/admin/services/:serviceId/jobs/:jobId/executions`  
取消：`POST .../executions/:executionId/cancel`（`demo.sleep` 便于联调）

## 配置

| 字段 | 说明 |
|------|------|
| `Endpoint` | `ws(s)://.../v1/executor/ws` |
| `ServiceCode` | 服务业务编码 `code` |
| `CredentialID` / `CredentialSecret` | 凭据（对应 `JC_CREDENTIAL_ID` / `JC_CREDENTIAL_SECRET`） |
| `Credential` | 可选 `jcs_<id>.<secret>` |
| `InstanceKey` | 稳定实例键（≤128） |
| `DataDir` / `JC_DATA_DIR` | 终态 pending 目录（默认 `./data`） |

协议类型与 center `pkg/protocol` 对齐，本模块自带副本，不依赖 `job-center` 整包。

### 断线 / Center 宕机

- 短断：本地任务继续，重连后 sync + 补报。
- 任务已跑完而 Center 未上线：终态先落盘，Center 恢复或执行器重启后重放，直到 ack。

## License

本项目采用 [Apache License 2.0](LICENSE)。第三方依赖仍保留原许可证，详见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。分发包含依赖的二进制或源码包时，请保留相应版权与许可声明。
