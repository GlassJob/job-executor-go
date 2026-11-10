package jobcenter

import (
	"context"
	"encoding/json"
	"errors"
	"runtime/debug"
	"time"

	"github.com/GlassJob/job-executor-go/protocol"
)

type runningJob struct {
	cancel     context.CancelFunc
	leaseToken string
}

func (c *Client) initWorkerState() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running == nil {
		c.running = make(map[string]*runningJob)
	}
	if c.seenAttempts == nil {
		c.seenAttempts = make(map[string]time.Time)
	}
	if c.cancelRequested == nil {
		c.cancelRequested = make(map[string]bool)
	}
	if c.globalSem == nil {
		c.globalSem = make(chan struct{}, c.cfg.MaxConcurrency)
	}
}

func (c *Client) handleExecute(parent context.Context, writeCh chan writeReq, ex *protocol.TaskExecute) {
	c.initWorkerState()
	lease := ex.LeaseToken
	jobCode := ex.JobCode

	c.mu.Lock()
	if _, ok := c.seenAttempts[ex.AttemptID]; ok {
		c.mu.Unlock()
		_ = c.enqueue(parent, writeCh, protocol.TypeTaskExecuteAck, protocol.TaskExecuteAck{
			AttemptID: ex.AttemptID, LeaseToken: lease, Accepted: true, Duplicate: true,
		})
		return
	}
	c.seenAttempts[ex.AttemptID] = time.Now()
	c.mu.Unlock()

	job, ok := c.lookupJob(jobCode)
	if !ok {
		_ = c.enqueue(parent, writeCh, protocol.TypeTaskExecuteAck, protocol.TaskExecuteAck{
			AttemptID: ex.AttemptID, LeaseToken: lease, Accepted: false,
			Error: &protocol.ErrorBody{Code: "UNKNOWN_JOB", Message: "job not registered"},
		})
		return
	}

	select {
	case c.globalSem <- struct{}{}:
	default:
		_ = c.enqueue(parent, writeCh, protocol.TypeTaskExecuteAck, protocol.TaskExecuteAck{
			AttemptID: ex.AttemptID, LeaseToken: lease, Accepted: false,
			Error: &protocol.ErrorBody{Code: "CAPACITY_FULL", Message: "max concurrency reached"},
		})
		return
	}

	deadline, _ := time.Parse(protocol.TimeFormat, ex.DeadlineAt)
	if deadline.IsZero() {
		deadline = time.Now().Add(time.Duration(ex.TimeoutSeconds) * time.Second)
	}
	jobCtx, cancel := context.WithDeadline(parent, deadline)

	c.mu.Lock()
	c.running[ex.AttemptID] = &runningJob{cancel: cancel, leaseToken: lease}
	c.mu.Unlock()
	c.inFlight.Add(1)

	_ = c.enqueue(parent, writeCh, protocol.TypeTaskExecuteAck, protocol.TaskExecuteAck{
		AttemptID: ex.AttemptID, LeaseToken: lease, Accepted: true,
	})
	_ = c.enqueue(parent, writeCh, protocol.TypeTaskStarted, protocol.TaskStarted{
		AttemptID: ex.AttemptID, LeaseToken: lease, StartedAt: protocol.FormatTime(time.Now()),
	})

	var result json.RawMessage
	jc := &JobContext{
		ExecutionID:   ex.ExecutionID,
		AttemptID:     ex.AttemptID,
		JobCode:       jobCode,
		AttemptNumber: ex.AttemptNumber,
		TriggerType:   ex.TriggerType,
		Deadline:      deadline,
		params:        ex.Params,
		log:           c.cfg.Logger.With("attempt_id", ex.AttemptID, "job_code", jobCode),
		onProgress: func(percent float64, msg string) {
			p := percent
			_ = c.enqueue(parent, writeCh, protocol.TypeTaskProgress, protocol.TaskProgress{
				AttemptID: ex.AttemptID, LeaseToken: lease, Percent: &p, Message: msg,
			})
		},
		onResult: func(v any) error {
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			if len(b) > 64*1024 {
				return errors.New("result exceeds 64KiB")
			}
			result = b
			return nil
		},
	}

	panicReported := false
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				panicReported = true
				_ = c.enqueue(parent, writeCh, protocol.TypeTaskFailed, protocol.TaskFailed{
					AttemptID: ex.AttemptID, LeaseToken: lease, FinishedAt: protocol.FormatTime(time.Now()),
					Reason: "PANIC",
					Error:  protocol.TaskFailedError{Code: "PANIC", Message: "handler panic", Detail: string(debug.Stack())},
				})
			}
		}()
		return job.Handler(jobCtx, jc)
	}()

	finished := protocol.FormatTime(time.Now())
	if !panicReported {
		switch {
		case c.wasCancelRequested(ex.AttemptID) && (errors.Is(jobCtx.Err(), context.Canceled) || (err != nil && errors.Is(err, context.Canceled))):
			_ = c.enqueue(parent, writeCh, protocol.TypeTaskCancelled, protocol.TaskCancelled{
				AttemptID: ex.AttemptID, LeaseToken: lease, FinishedAt: finished, StoppedBy: "COOPERATIVE",
			})
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(jobCtx.Err(), context.DeadlineExceeded):
			code := "TIMEOUT"
			msg := "deadline exceeded"
			if c.wasCancelRequested(ex.AttemptID) {
				code = "CANCEL_NOT_HONORED"
				msg = "handler ignored cancel"
			}
			_ = c.enqueue(parent, writeCh, protocol.TypeTaskFailed, protocol.TaskFailed{
				AttemptID: ex.AttemptID, LeaseToken: lease, FinishedAt: finished, Reason: "TIMEOUT",
				Error: protocol.TaskFailedError{Code: code, Message: msg},
			})
		case err != nil:
			code, retryable := IsRetryable(err)
			if code == "" {
				code = "HANDLER_ERROR"
			}
			rb := retryable
			_ = c.enqueue(parent, writeCh, protocol.TypeTaskFailed, protocol.TaskFailed{
				AttemptID: ex.AttemptID, LeaseToken: lease, FinishedAt: finished, Reason: "HANDLER_ERROR",
				Error: protocol.TaskFailedError{Code: code, Message: err.Error(), Retryable: &rb},
			})
		default:
			_ = c.enqueue(parent, writeCh, protocol.TypeTaskCompleted, protocol.TaskCompleted{
				AttemptID: ex.AttemptID, LeaseToken: lease, FinishedAt: finished, Result: result,
			})
		}
	}

	c.mu.Lock()
	delete(c.running, ex.AttemptID)
	c.mu.Unlock()
	cancel()
	<-c.globalSem
	c.inFlight.Add(-1)
}

func (c *Client) handleCancel(parent context.Context, writeCh chan writeReq, cancelMsg *protocol.TaskCancel) {
	c.initWorkerState()
	c.mu.Lock()
	if c.cancelRequested == nil {
		c.cancelRequested = make(map[string]bool)
	}
	c.cancelRequested[cancelMsg.AttemptID] = true
	rt, ok := c.running[cancelMsg.AttemptID]
	lease := cancelMsg.LeaseToken
	if ok && lease == "" {
		lease = rt.leaseToken
	}
	if ok && rt.cancel != nil {
		rt.cancel()
	}
	state := "CANCELLING"
	if !ok {
		state = "NOT_FOUND"
	}
	c.mu.Unlock()
	_ = c.enqueue(parent, writeCh, protocol.TypeTaskCancelAck, protocol.TaskCancelAck{
		AttemptID: cancelMsg.AttemptID, LeaseToken: lease, State: state,
	})
}

func (c *Client) wasCancelRequested(attemptID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancelRequested[attemptID]
}

func (c *Client) lookupJob(code string) (Job, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	j, ok := c.jobs[code]
	return j, ok
}
