package jobcenter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
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

	cancel context.CancelFunc
	done   chan struct{}

	inFlight        atomic.Int32
	globalSem       chan struct{}
	running         map[string]*runningJob
	seenAttempts    map[string]time.Time
	cancelRequested map[string]bool

	serverInstanceID string
	sessionEpoch     int64
}

func NewClient(cfg Config) (*Client, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	c := &Client{
		cfg:             cfg,
		jobs:            make(map[string]Job),
		done:            make(chan struct{}),
		running:         make(map[string]*runningJob),
		seenAttempts:    make(map[string]time.Time),
		cancelRequested: make(map[string]bool),
		globalSem:       make(chan struct{}, cfg.MaxConcurrency),
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
	c.done = make(chan struct{})
	c.mu.Unlock()
	defer close(c.done)

	c.initWorkerState()
	return c.runSession(ctx)
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
