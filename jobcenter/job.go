package jobcenter

import (
	"context"
	"encoding/json"
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

	params     json.RawMessage
	log        *slog.Logger
	onProgress func(percent float64, msg string)
	onResult   func(v any) error
}

func (j *JobContext) Bind(v any) error        { return json.Unmarshal(j.params, v) }
func (j *JobContext) Params() json.RawMessage { return j.params }
func (j *JobContext) Progress(percent float64, msg string) {
	if j.onProgress != nil {
		j.onProgress(percent, msg)
	}
}
func (j *JobContext) Log() *slog.Logger {
	if j.log != nil {
		return j.log
	}
	return slog.Default()
}
func (j *JobContext) SetResult(v any) error {
	if j.onResult != nil {
		return j.onResult(v)
	}
	return nil
}
