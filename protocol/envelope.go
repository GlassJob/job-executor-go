package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

const (
	Version     = "1.1"
	Subprotocol = "jobcenter.v1"
	TimeFormat  = "2006-01-02T15:04:05.000Z07:00"
)

const (
	TypeAuthRequest         = "auth.request"
	TypeAuthResponse        = "auth.response"
	TypeServiceRegister     = "service.register"
	TypeServiceRegisterAck  = "service.register_ack"
	TypeServiceHeartbeat    = "service.heartbeat"
	TypeServiceHeartbeatAck = "service.heartbeat_ack"
	TypeServiceUnregister   = "service.unregister"
	TypeTaskExecute         = "task.execute"
	TypeTaskExecuteAck      = "task.execute_ack"
	TypeTaskStarted         = "task.started"
	TypeTaskProgress        = "task.progress"
	TypeTaskLog             = "task.log"
	TypeTaskCheckpoint      = "task.checkpoint"
	TypeTaskCompleted       = "task.completed"
	TypeTaskFailed          = "task.failed"
	TypeTaskResultAck       = "task.result_ack"
	TypeTaskCancel          = "task.cancel"
	TypeTaskCancelAck       = "task.cancel_ack"
	TypeTaskCancelled       = "task.cancelled"
	TypeTaskSyncRunning     = "task.sync_running"
	TypeTaskSyncRunningAck  = "task.sync_running_ack"
	TypeSystemError         = "system.error"
	TypeSystemNotice        = "system.notice"
)

type Envelope struct {
	ProtocolVersion string          `json:"protocol_version"`
	MessageID       string          `json:"message_id"`
	Type            string          `json:"type"`
	RequestID       *string         `json:"request_id,omitempty"`
	Timestamp       string          `json:"timestamp"`
	TraceID         *string         `json:"trace_id,omitempty"`
	Data            json.RawMessage `json:"data"`
}

func FormatTime(t time.Time) string { return t.UTC().Format(TimeFormat) }

func NewMessageID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "m_" + hex.EncodeToString(b[:])
}

func NewEnvelope(msgType, messageID string, data any) (*Envelope, error) {
	if messageID == "" {
		messageID = NewMessageID()
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &Envelope{
		ProtocolVersion: Version,
		MessageID:       messageID,
		Type:            msgType,
		Timestamp:       FormatTime(time.Now()),
		Data:            raw,
	}, nil
}

func DecodeEnvelope(b []byte) (*Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	if env.ProtocolVersion == "" || env.MessageID == "" || env.Type == "" || env.Data == nil {
		return nil, fmt.Errorf("decode envelope: missing required fields")
	}
	return &env, nil
}

func (e *Envelope) Encode() ([]byte, error) { return json.Marshal(e) }

func (e *Envelope) DecodeData(v any) error { return json.Unmarshal(e.Data, v) }
