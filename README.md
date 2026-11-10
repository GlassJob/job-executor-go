# job-executor-go

GlassJob Executor Go SDK（WebSocket 协议 **1.1**）。

## 能力

- HMAC `auth.request`（secret 不进帧）
- `service.register` / `heartbeat` / `unregister`
- `task.execute` → ack / started / completed|failed
- `task.cancel` → cancel_ack / cancelled（配合 Admin 取消）

## 快速开始

PowerShell（凭据可用 `job-center/scripts/provision_sdk_demo.py` 生成）：

```powershell
$env:JC_WS_ENDPOINT='ws://127.0.0.1:8080/v1/executor/ws'
$env:JC_SERVICE_CODE='sdk.demo'
$env:JC_CREDENTIAL_ID='<uuid>'
$env:JC_CREDENTIAL_SECRET='<secret>'
# 或 $env:JC_CREDENTIAL='jcs_<id>.<secret>'

go run ./examples/go-executor
```

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

协议类型与 center `pkg/protocol` 对齐，本模块自带副本，不依赖 `job-center` 整包。
