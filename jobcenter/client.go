package jobcenter

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GlassJob/job-executor-go/protocol"
)

type ClientStatus struct {
	Connected    bool
	InFlightJobs int
	SessionEpoch int64
	InstanceID   string
	InstanceKey  string
}

// Client is the Executor SDK entrypoint.
type Client struct {
	cfg    Config
	mu     sync.Mutex
	jobs   map[string]Job
	status atomic.Value // ClientStatus

	cancel  context.CancelFunc
	lifeCtx context.Context
	done    chan struct{}

	inFlight        atomic.Int32
	globalSem       chan struct{}
	running         map[string]*runningJob
	seenAttempts    map[string]time.Time
	cancelRequested map[string]bool

	serverInstanceID string
	sessionEpoch     int64

	pending     *pendingStore
	sendMu      sync.Mutex
	sessionSend *sessionSend
	softMu      sync.Mutex
	softBuf     []softMsg
	syncAckCh   chan *protocol.TaskSyncRunningAck

	logBufMu sync.Mutex
	logBufs  map[string]*attemptLogBuf
}

func NewClient(cfg Config) (*Client, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	pending, err := newPendingStore(filepath.Join(cfg.DataDir, "pending_results"))
	if err != nil {
		return nil, fmt.Errorf("pending store: %w", err)
	}
	c := &Client{
		cfg:             cfg,
		jobs:            make(map[string]Job),
		done:            make(chan struct{}),
		running:         make(map[string]*runningJob),
		seenAttempts:    make(map[string]time.Time),
		cancelRequested: make(map[string]bool),
		globalSem:       make(chan struct{}, cfg.MaxConcurrency),
		pending:         pending,
		syncAckCh:       make(chan *protocol.TaskSyncRunningAck, 1),
	}
	c.status.Store(ClientStatus{InstanceKey: cfg.InstanceKey})
	return c, nil
}

func (c *Client) RegisterJob(j Job) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if j.Code == "" || j.Handler == nil {
		return fmt.Errorf("job code and handler are required")
	}
	if _, exists := c.jobs[j.Code]; exists {
		return fmt.Errorf("duplicate job code: %s", j.Code)
	}
	if j.HandlerVersion == "" {
		j.HandlerVersion = "1.0.0"
	}
	if j.Name == "" {
		j.Name = j.Code
	}
	c.jobs[j.Code] = j
	return nil
}

func (c *Client) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		cancel()
		return fmt.Errorf("client already started")
	}
	c.cancel = cancel
	c.lifeCtx = ctx
	c.done = make(chan struct{})
	c.mu.Unlock()
	defer close(c.done)

	c.initWorkerState()
	go c.runLogFlusher(ctx)
	return c.runSession(ctx)
}

func (c *Client) registerLogBuf(attemptID string, buf *attemptLogBuf) {
	c.logBufMu.Lock()
	defer c.logBufMu.Unlock()
	if c.logBufs == nil {
		c.logBufs = make(map[string]*attemptLogBuf)
	}
	c.logBufs[attemptID] = buf
}

func (c *Client) unregisterLogBuf(attemptID string) *attemptLogBuf {
	c.logBufMu.Lock()
	defer c.logBufMu.Unlock()
	buf := c.logBufs[attemptID]
	delete(c.logBufs, attemptID)
	return buf
}

func (c *Client) runLogFlusher(ctx context.Context) {
	t := time.NewTicker(logFlushEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.logBufMu.Lock()
			bufs := make([]*attemptLogBuf, 0, len(c.logBufs))
			for _, b := range c.logBufs {
				bufs = append(bufs, b)
			}
			c.logBufMu.Unlock()
			for _, b := range bufs {
				b.Flush()
			}
		}
	}
}

func (c *Client) Stop(ctx context.Context) error {
	c.mu.Lock()
	cancel := c.cancel
	done := c.done
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (c *Client) Status() ClientStatus {
	if v := c.status.Load(); v != nil {
		st := v.(ClientStatus)
		st.InFlightJobs = int(c.inFlight.Load())
		return st
	}
	return ClientStatus{}
}

func (c *Client) snapshotJobs() []Job {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Job, 0, len(c.jobs))
	for _, j := range c.jobs {
		out = append(out, j)
	}
	return out
}

func (c *Client) lifeContext() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lifeCtx != nil {
		return c.lifeCtx
	}
	return context.Background()
}
