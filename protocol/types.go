package protocol

import "encoding/json"

type ErrorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

type SDKInfo struct {
	Language string `json:"language"`
	Version  string `json:"version"`
}

type AuthRequest struct {
	Method           string   `json:"method,omitempty"`
	Code             string   `json:"code"`
	CredentialID     string   `json:"credential_id"`
	InstanceKey      string   `json:"instance_key"`
	Timestamp        string   `json:"timestamp"`
	Nonce            string   `json:"nonce"`
	Signature        string   `json:"signature"`
	ProtocolVersions []string `json:"protocol_versions"`
	SDK              SDKInfo  `json:"sdk"`
}

type AuthResponse struct {
	OK                       bool       `json:"ok"`
	SessionID                string     `json:"session_id,omitempty"`
	SessionEpoch             int64      `json:"session_epoch,omitempty"`
	InstanceID               string     `json:"instance_id,omitempty"`
	ProtocolVersion          string     `json:"protocol_version,omitempty"`
	ServerTime               string     `json:"server_time,omitempty"`
	HeartbeatIntervalSeconds int        `json:"heartbeat_interval_seconds,omitempty"`
	LeaseTTLSeconds          int        `json:"lease_ttl_seconds,omitempty"`
	MaxMessageBytes          int64      `json:"max_message_bytes,omitempty"`
	Error                    *ErrorBody `json:"error,omitempty"`
}

type HostInfo struct {
	Hostname string `json:"hostname"`
	IP       string `json:"ip,omitempty"`
	PID      int    `json:"pid,omitempty"`
}

type RuntimeInfo struct {
	Language string `json:"language"`
	Version  string `json:"version,omitempty"`
}

type DeploymentInfo struct {
	Version   string `json:"version,omitempty"`
	GitCommit string `json:"git_commit,omitempty"`
	BuildTime string `json:"build_time,omitempty"`
}

type CapacityInfo struct {
	MaxConcurrency int `json:"max_concurrency"`
}

type JobDecl struct {
	Code           string          `json:"job_code"`
	Name           string          `json:"job_name"`
	Description    string          `json:"description,omitempty"`
	Handler        string          `json:"handler,omitempty"`
	HandlerVersion string          `json:"handler_version"`
	DefaultCron    *string         `json:"default_cron,omitempty"`
	TimeoutSeconds int             `json:"timeout_seconds,omitempty"`
	MaxConcurrency int             `json:"max_concurrency,omitempty"`
	ParamsSchema   json.RawMessage `json:"params_schema,omitempty"`
}

type RegisterRequest struct {
	ServiceCode string         `json:"code"`
	InstanceKey string         `json:"instance_key"`
	Environment string         `json:"environment,omitempty"`
	Host        *HostInfo      `json:"host,omitempty"`
	Runtime     RuntimeInfo    `json:"runtime"`
	Deployment  DeploymentInfo `json:"deployment"`
	Capacity    CapacityInfo   `json:"capacity"`
	SDKVersion  string         `json:"sdk_version,omitempty"`
	StartedAt   string         `json:"started_at,omitempty"`
	Jobs        []JobDecl      `json:"jobs"`
}

type AcceptedJob struct {
	JobCode string `json:"job_code"`
	JobID   string `json:"job_id"`
}

type JobIssue struct {
	JobCode string `json:"job_code,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

type RegisterResponse struct {
	OK             bool          `json:"ok"`
	InstanceID     string        `json:"instance_id,omitempty"`
	InstanceKey    string        `json:"instance_key,omitempty"`
	SessionID      string        `json:"session_id,omitempty"`
	SessionEpoch   int64         `json:"session_epoch,omitempty"`
	InstanceStatus string        `json:"instance_status,omitempty"`
	AcceptedJobs   []AcceptedJob `json:"accepted_jobs,omitempty"`
	RejectedJobs   []JobIssue    `json:"rejected_jobs,omitempty"`
	Warnings       []JobIssue    `json:"warnings,omitempty"`
	Error          *ErrorBody    `json:"error,omitempty"`
}

type HeartbeatRequest struct {
	Status         string `json:"status"`
	Running        int    `json:"running"`
	MaxConcurrency *int   `json:"max_concurrency,omitempty"`
	InstanceKey    string `json:"instance_key,omitempty"`
}

type HeartbeatResponse struct {
	ServerTime     string `json:"server_time"`
	InstanceStatus string `json:"instance_status"`
}

type UnregisterRequest struct {
	Reason      string `json:"reason,omitempty"`
	InstanceKey string `json:"instance_key,omitempty"`
}

type TaskExecute struct {
	ExecutionID    string          `json:"execution_id"`
	AttemptID      string          `json:"attempt_id"`
	AttemptNumber  int             `json:"attempt_number"`
	LeaseToken     string          `json:"lease_token"`
	LeaseExpiresAt string          `json:"lease_expires_at"`
	JobCode        string          `json:"job_code"`
	JobName        string          `json:"job_name,omitempty"`
	Params         json.RawMessage `json:"params"`
	State          json.RawMessage `json:"state"`
	StateVersion   uint64          `json:"state_version"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	DeadlineAt     string          `json:"deadline_at"`
	TriggerType    string          `json:"trigger_type,omitempty"`
	HandlerVersion string          `json:"handler_version,omitempty"`
}

type TaskExecuteAck struct {
	AttemptID  string     `json:"attempt_id"`
	LeaseToken string     `json:"lease_token"`
	Accepted   bool       `json:"accepted"`
	Duplicate  bool       `json:"duplicate,omitempty"`
	Error      *ErrorBody `json:"error,omitempty"`
}

type TaskStarted struct {
	AttemptID  string `json:"attempt_id"`
	LeaseToken string `json:"lease_token"`
	StartedAt  string `json:"started_at"`
}

type TaskProgress struct {
	AttemptID  string   `json:"attempt_id"`
	LeaseToken string   `json:"lease_token"`
	Percent    *float64 `json:"percent,omitempty"`
	Message    string   `json:"message,omitempty"`
}

type TaskLogEntry struct {
	Seq     int64           `json:"seq"`
	Level   string          `json:"level"`
	Message string          `json:"message"`
	Stream  string          `json:"stream,omitempty"`
	Fields  json.RawMessage `json:"fields,omitempty"`
	Ts      string          `json:"ts"`
}

type TaskLog struct {
	AttemptID    string         `json:"attempt_id"`
	LeaseToken   string         `json:"lease_token"`
	Entries      []TaskLogEntry `json:"entries"`
	Dropped      int            `json:"dropped,omitempty"`
	DroppedInfo  int            `json:"dropped_info,omitempty"`
	DroppedWarn  int            `json:"dropped_warn,omitempty"`
	DroppedError int            `json:"dropped_error,omitempty"`
}

type TaskCheckpoint struct {
	AttemptID  string          `json:"attempt_id"`
	LeaseToken string          `json:"lease_token"`
	State      json.RawMessage `json:"state"`
}

type TaskCompleted struct {
	AttemptID  string          `json:"attempt_id"`
	LeaseToken string          `json:"lease_token"`
	FinishedAt string          `json:"finished_at"`
	Result     json.RawMessage `json:"result,omitempty"`
	NextState  json.RawMessage `json:"next_state,omitempty"`
}

type TaskFailedError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Detail    string `json:"detail,omitempty"`
	Retryable *bool  `json:"retryable,omitempty"`
}

type TaskFailed struct {
	AttemptID  string          `json:"attempt_id"`
	LeaseToken string          `json:"lease_token"`
	FinishedAt string          `json:"finished_at"`
	Reason     string          `json:"reason"`
	Error      TaskFailedError `json:"error"`
}

type TaskResultAck struct {
	Status          string `json:"status"`
	AttemptID       string `json:"attempt_id"`
	ErrorCode       string `json:"error_code,omitempty"`
	Message         string `json:"message,omitempty"`
	AckedThroughSeq *int64 `json:"acked_through_seq,omitempty"`
}

type TaskCancel struct {
	ExecutionID  string `json:"execution_id,omitempty"`
	AttemptID    string `json:"attempt_id"`
	LeaseToken   string `json:"lease_token,omitempty"`
	Reason       string `json:"reason,omitempty"`
	GraceSeconds int    `json:"grace_seconds,omitempty"`
}

type TaskCancelAck struct {
	AttemptID  string `json:"attempt_id"`
	LeaseToken string `json:"lease_token,omitempty"`
	State      string `json:"state"`
}

type TaskCancelled struct {
	AttemptID  string `json:"attempt_id"`
	LeaseToken string `json:"lease_token"`
	FinishedAt string `json:"finished_at"`
	StoppedBy  string `json:"stopped_by,omitempty"`
}

type TaskSyncRunning struct {
	Attempts []TaskSyncRunningItem `json:"attempts"`
}

type TaskSyncRunningItem struct {
	ExecutionID string `json:"execution_id"`
	AttemptID   string `json:"attempt_id"`
	LeaseToken  string `json:"lease_token"`
}

type TaskSyncRunningAck struct {
	Results []TaskSyncRunningResult `json:"results"`
}

type TaskSyncRunningResult struct {
	AttemptID      string `json:"attempt_id"`
	Status         string `json:"status"`
	LeaseExpiresAt string `json:"lease_expires_at,omitempty"`
	Message        string `json:"message,omitempty"`
}

type SystemError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}
