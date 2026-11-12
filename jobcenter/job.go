package jobcenter

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"
)

type JobHandler func(ctx context.Context, job *JobContext) error

type Job struct {
	Code           string
	Name           string
	Description    string
	HandlerVersion string
	DefaultCron    string
	TimeoutSeconds int
	MaxConcurrency int
	InputSchema    json.RawMessage
	Handler        JobHandler
}

type JobContext struct {
	ExecutionID   string
	AttemptID     string
	JobCode       string
	AttemptNumber int
	TriggerType   string
	Deadline      time.Time

	params       json.RawMessage
	state        json.RawMessage
	stateVersion uint64
	nextState    json.RawMessage
	local        *slog.Logger // 本机控制台
	log          *slog.Logger // job.Log：默认只上报中心
	onProgress   func(percent float64, msg string)
	onCheckpoint func(state json.RawMessage) error
	onResult     func(v any) error
	flushLogs    func()
}

func (j *JobContext) Bind(v any) error        { return json.Unmarshal(j.params, v) }
func (j *JobContext) Params() json.RawMessage { return j.params }

// State 返回派发时附带的 job_states（key=default）快照。
func (j *JobContext) State() json.RawMessage { return j.state }

// StateVersion 对应 job_states.version；尚无记录时为 0。
func (j *JobContext) StateVersion() uint64 { return j.stateVersion }

func (j *JobContext) BindState(v any) error {
	raw := j.state
	if len(raw) == 0 || string(raw) == "null" {
		raw = json.RawMessage(`{}`)
	}
	return json.Unmarshal(raw, v)
}

func (j *JobContext) Progress(percent float64, msg string) {
	if j.local != nil {
		j.local.Info("progress", "percent", percent, "message", msg)
	}
	if j.onProgress != nil {
		j.onProgress(percent, msg)
	}
}

// Checkpoint 中途落盘到中心 job_states.default（发 task.checkpoint）。
func (j *JobContext) Checkpoint(v any) error {
	if j.onCheckpoint == nil {
		return errors.New("checkpoint not available")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > 64*1024 {
		return errors.New("state exceeds 64KiB")
	}
	j.state = b
	if j.local != nil {
		j.local.Info("checkpoint", "bytes", len(b))
	}
	return j.onCheckpoint(b)
}

// SetNextState 在成功完成时写入 next_state（落 job_states.default）。
func (j *JobContext) SetNextState(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > 64*1024 {
		return errors.New("state exceeds 64KiB")
	}
	j.nextState = b
	j.state = b
	return nil
}

// Log 返回独立上报通道（默认不打本机控制台）。想双写请业务自行封装：local.Info + job.Log().Info。
func (j *JobContext) Log() *slog.Logger {
	if j.log != nil {
		return j.log
	}
	if j.local != nil {
		return j.local
	}
	return slog.Default()
}

func (j *JobContext) SetResult(v any) error {
	if j.onResult != nil {
		return j.onResult(v)
	}
	return nil
}
