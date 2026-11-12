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
	cancel      context.CancelFunc
	leaseToken  string
	executionID string
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

func (c *Client) handleExecute(ex *protocol.TaskExecute) {
	c.initWorkerState()
	lease := ex.LeaseToken
	jobCode := ex.JobCode

	c.mu.Lock()
	if _, ok := c.seenAttempts[ex.AttemptID]; ok {
		c.mu.Unlock()
		c.publishSoft(protocol.TypeTaskExecuteAck, protocol.TaskExecuteAck{
			AttemptID: ex.AttemptID, LeaseToken: lease, Accepted: true, Duplicate: true,
		})
		return
	}
	c.seenAttempts[ex.AttemptID] = time.Now()
	c.mu.Unlock()

	job, ok := c.lookupJob(jobCode)
	if !ok {
		c.publishSoft(protocol.TypeTaskExecuteAck, protocol.TaskExecuteAck{
			AttemptID: ex.AttemptID, LeaseToken: lease, Accepted: false,
			Error: &protocol.ErrorBody{Code: "UNKNOWN_JOB", Message: "job not registered"},
		})
		return
	}

	select {
	case c.globalSem <- struct{}{}:
	default:
		c.publishSoft(protocol.TypeTaskExecuteAck, protocol.TaskExecuteAck{
			AttemptID: ex.AttemptID, LeaseToken: lease, Accepted: false,
			Error: &protocol.ErrorBody{Code: "CAPACITY_FULL", Message: "max concurrency reached"},
		})
		return
	}

	deadline, _ := time.Parse(protocol.TimeFormat, ex.DeadlineAt)
	if deadline.IsZero() {
		deadline = time.Now().Add(time.Duration(ex.TimeoutSeconds) * time.Second)
	}
	// 任务 ctx 挂在 client 生命周期上，与 WS 会话解耦。
	jobCtx, cancel := context.WithDeadline(c.lifeContext(), deadline)

	c.mu.Lock()
	c.running[ex.AttemptID] = &runningJob{
		cancel: cancel, leaseToken: lease, executionID: ex.ExecutionID,
	}
	c.mu.Unlock()
	c.inFlight.Add(1)

	c.publishSoft(protocol.TypeTaskExecuteAck, protocol.TaskExecuteAck{
		AttemptID: ex.AttemptID, LeaseToken: lease, Accepted: true,
	})
	c.publishSoft(protocol.TypeTaskStarted, protocol.TaskStarted{
		AttemptID: ex.AttemptID, LeaseToken: lease, StartedAt: protocol.FormatTime(time.Now()),
	})
	c.cfg.Logger.Info("job started",
		"job_code", jobCode, "attempt_id", ex.AttemptID, "execution_id", ex.ExecutionID)

	state := ex.State
	if len(state) == 0 || string(state) == "null" {
		state = json.RawMessage(`{}`)
	}
	localLog := c.cfg.Logger.With("attempt_id", ex.AttemptID, "job_code", jobCode)
	logBuf := newAttemptLogBuf(ex.AttemptID, lease, func(msg protocol.TaskLog) {
		c.publishSoft(protocol.TypeTaskLog, msg)
	})
	c.registerLogBuf(ex.AttemptID, logBuf)

	var result json.RawMessage
	jc := &JobContext{
		ExecutionID:   ex.ExecutionID,
		AttemptID:     ex.AttemptID,
		JobCode:       jobCode,
		AttemptNumber: ex.AttemptNumber,
		TriggerType:   ex.TriggerType,
		Deadline:      deadline,
		params:        ex.Params,
		state:         state,
		stateVersion:  ex.StateVersion,
		local:         localLog,
		log:           logBuf.logger(),
		flushLogs:     logBuf.Flush,
		onProgress: func(percent float64, msg string) {
			p := percent
			c.publishSoft(protocol.TypeTaskProgress, protocol.TaskProgress{
				AttemptID: ex.AttemptID, LeaseToken: lease, Percent: &p, Message: msg,
			})
		},
		onCheckpoint: func(raw json.RawMessage) error {
			c.publishSoft(protocol.TypeTaskCheckpoint, protocol.TaskCheckpoint{
				AttemptID: ex.AttemptID, LeaseToken: lease, State: raw,
			})
			return nil
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
	var panicStack string
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				panicReported = true
				panicStack = string(debug.Stack())
			}
		}()
		return job.Handler(jobCtx, jc)
	}()

	// handler 返回（含 panic）后先 flush 日志，再发终态。
	if buf := c.unregisterLogBuf(ex.AttemptID); buf != nil {
		buf.CloseAndFlush()
	}

	finished := protocol.FormatTime(time.Now())
	switch {
	case panicReported:
		_ = c.publishTerminal(ex.AttemptID, protocol.TypeTaskFailed, protocol.TaskFailed{
			AttemptID: ex.AttemptID, LeaseToken: lease, FinishedAt: finished,
			Reason: "PANIC",
			Error:  protocol.TaskFailedError{Code: "PANIC", Message: "handler panic", Detail: panicStack},
		})
	case c.wasCancelRequested(ex.AttemptID) && (errors.Is(jobCtx.Err(), context.Canceled) || (err != nil && errors.Is(err, context.Canceled))):
		c.cfg.Logger.Info("job cancelled", "job_code", jobCode, "attempt_id", ex.AttemptID)
		if err := c.publishTerminal(ex.AttemptID, protocol.TypeTaskCancelled, protocol.TaskCancelled{
			AttemptID: ex.AttemptID, LeaseToken: lease, FinishedAt: finished, StoppedBy: "COOPERATIVE",
		}); err != nil {
			c.cfg.Logger.Error("persist cancelled pending", "err", err)
		}
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(jobCtx.Err(), context.DeadlineExceeded):
		code := "TIMEOUT"
		msg := "deadline exceeded"
		if c.wasCancelRequested(ex.AttemptID) {
			code = "CANCEL_NOT_HONORED"
			msg = "handler ignored cancel"
		}
		c.cfg.Logger.Warn("job timed out", "job_code", jobCode, "attempt_id", ex.AttemptID, "code", code)
		if err := c.publishTerminal(ex.AttemptID, protocol.TypeTaskFailed, protocol.TaskFailed{
			AttemptID: ex.AttemptID, LeaseToken: lease, FinishedAt: finished, Reason: "TIMEOUT",
			Error: protocol.TaskFailedError{Code: code, Message: msg},
		}); err != nil {
			c.cfg.Logger.Error("persist failed pending", "err", err)
		}
	case err != nil:
		code, retryable := IsRetryable(err)
		if code == "" {
			code = "HANDLER_ERROR"
		}
		rb := retryable
		c.cfg.Logger.Error("job failed", "job_code", jobCode, "attempt_id", ex.AttemptID, "err", err, "code", code)
		if err := c.publishTerminal(ex.AttemptID, protocol.TypeTaskFailed, protocol.TaskFailed{
			AttemptID: ex.AttemptID, LeaseToken: lease, FinishedAt: finished, Reason: "HANDLER_ERROR",
			Error: protocol.TaskFailedError{Code: code, Message: err.Error(), Retryable: &rb},
		}); err != nil {
			c.cfg.Logger.Error("persist failed pending", "err", err)
		}
	default:
		c.cfg.Logger.Info("job completed", "job_code", jobCode, "attempt_id", ex.AttemptID)
		if err := c.publishTerminal(ex.AttemptID, protocol.TypeTaskCompleted, protocol.TaskCompleted{
			AttemptID: ex.AttemptID, LeaseToken: lease, FinishedAt: finished,
			Result: result, NextState: jc.nextState,
		}); err != nil {
			c.cfg.Logger.Error("persist completed pending", "err", err)
		}
	}

	c.mu.Lock()
	delete(c.running, ex.AttemptID)
	c.mu.Unlock()
	cancel()
	<-c.globalSem
	c.inFlight.Add(-1)
}

func (c *Client) handleCancel(cancelMsg *protocol.TaskCancel) {
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
	c.publishSoft(protocol.TypeTaskCancelAck, protocol.TaskCancelAck{
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
