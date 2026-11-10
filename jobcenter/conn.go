package jobcenter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GlassJob/job-executor-go/protocol"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func (c *Client) runSession(ctx context.Context) error {
	var attempt int
	for {
		err := c.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			c.cfg.Logger.Warn("session ended", "err", err)
			if isFatalSessionError(err) {
				return err
			}
		}
		st := c.Status()
		st.Connected = false
		c.status.Store(st)

		delay := nextBackoff(attempt)
		attempt++
		c.cfg.Logger.Info("reconnecting", "attempt", attempt, "delay", delay.String())
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) runOnce(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	opts := &websocket.DialOptions{
		Subprotocols: []string{protocol.Subprotocol},
		HTTPClient:   &http.Client{Timeout: 15 * time.Second},
	}
	if c.cfg.TLS != nil {
		opts.HTTPClient.Transport = &http.Transport{TLSClientConfig: c.cfg.TLS}
	}
	conn, _, err := websocket.Dial(dialCtx, c.cfg.Endpoint, opts)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "bye")
	conn.SetReadLimit(1 << 20)

	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	auth, err := c.doAuth(sessCtx, conn)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.serverInstanceID = auth.InstanceID
	c.sessionEpoch = auth.SessionEpoch
	c.mu.Unlock()
	c.status.Store(ClientStatus{
		Connected: true, SessionEpoch: auth.SessionEpoch,
		InstanceID: auth.InstanceID, InstanceKey: c.cfg.InstanceKey,
	})

	if err := c.doRegister(sessCtx, conn); err != nil {
		return err
	}

	interval := time.Duration(auth.HeartbeatIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 10 * time.Second
	}

	writeCh := make(chan writeReq, 64)
	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.readLoop(sessCtx, conn, writeCh)
		sessCancel()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- c.writeLoop(sessCtx, conn, writeCh, interval)
		sessCancel()
	}()

	select {
	case <-sessCtx.Done():
	case err := <-errCh:
		sessCancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			wg.Wait()
			_ = c.doUnregister(context.Background(), conn)
			return err
		}
	}
	wg.Wait()
	_ = c.doUnregister(context.Background(), conn)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("session closed")
}

type writeReq struct {
	typ  string
	data any
	err  chan error
}

func (c *Client) enqueue(ctx context.Context, writeCh chan writeReq, typ string, data any) error {
	req := writeReq{typ: typ, data: data, err: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case writeCh <- req:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-req.err:
		return err
	}
}

func (c *Client) writeLoop(ctx context.Context, conn *websocket.Conn, writeCh <-chan writeReq, hbInterval time.Duration) error {
	ticker := time.NewTicker(hbInterval)
	defer ticker.Stop()
	if err := c.sendHeartbeat(ctx, conn); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.sendHeartbeat(ctx, conn); err != nil {
				return err
			}
		case req := <-writeCh:
			err := c.rawWrite(ctx, conn, req.typ, req.data)
			if req.err != nil {
				req.err <- err
			}
			if err != nil {
				return err
			}
		}
	}
}

func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn, writeCh chan writeReq) error {
	for {
		var env protocol.Envelope
		err := wsjson.Read(ctx, conn, &env)
		if err != nil {
			return err
		}
		switch env.Type {
		case protocol.TypeServiceHeartbeatAck:
			st := c.Status()
			st.Connected = true
			c.status.Store(st)
		case protocol.TypeTaskExecute:
			var ex protocol.TaskExecute
			if err := env.DecodeData(&ex); err != nil {
				c.cfg.Logger.Error("bad task.execute", "err", err)
				continue
			}
			go c.handleExecute(ctx, writeCh, &ex)
		case protocol.TypeTaskCancel:
			var cancelMsg protocol.TaskCancel
			if err := env.DecodeData(&cancelMsg); err != nil {
				continue
			}
			c.handleCancel(ctx, writeCh, &cancelMsg)
		case protocol.TypeTaskResultAck, protocol.TypeSystemNotice, protocol.TypeSystemError,
			protocol.TypeAuthResponse, protocol.TypeServiceRegisterAck:
		default:
			c.cfg.Logger.Debug("ignored message", "type", env.Type)
		}
	}
}

func (c *Client) sendHeartbeat(ctx context.Context, conn *websocket.Conn) error {
	return c.rawWrite(ctx, conn, protocol.TypeServiceHeartbeat, protocol.HeartbeatRequest{
		Status:      "ONLINE",
		Running:     int(c.inFlight.Load()),
		InstanceKey: c.cfg.InstanceKey,
	})
}

func (c *Client) rawWrite(ctx context.Context, conn *websocket.Conn, typ string, data any) error {
	env, err := protocol.NewEnvelope(typ, "", data)
	if err != nil {
		return err
	}
	b, err := env.Encode()
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, b)
}

func (c *Client) doAuth(ctx context.Context, conn *websocket.Conn) (*protocol.AuthResponse, error) {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce, err := randomNonce(16)
	if err != nil {
		return nil, err
	}
	canon := protocol.AuthCanonical(c.cfg.ServiceCode, c.cfg.CredentialID, c.cfg.InstanceKey, ts, nonce)
	sig := protocol.SignHMAC(c.cfg.CredentialSecret, canon)
	if err := c.rawWrite(ctx, conn, protocol.TypeAuthRequest, protocol.AuthRequest{
		Method:           "hmac",
		Code:             c.cfg.ServiceCode,
		CredentialID:     c.cfg.CredentialID,
		InstanceKey:      c.cfg.InstanceKey,
		Timestamp:        ts,
		Nonce:            nonce,
		Signature:        sig,
		ProtocolVersions: []string{protocol.Version},
		SDK:              protocol.SDKInfo{Language: "go", Version: c.cfg.SDKVersion},
	}); err != nil {
		return nil, err
	}
	var env protocol.Envelope
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := wsjson.Read(rctx, conn, &env); err != nil {
		return nil, fmt.Errorf("auth read: %w", err)
	}
	if env.Type != protocol.TypeAuthResponse {
		return nil, fmt.Errorf("auth: unexpected type %s", env.Type)
	}
	var resp protocol.AuthResponse
	if err := env.DecodeData(&resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		code := ""
		if resp.Error != nil {
			code = resp.Error.Code
		}
		return nil, fmt.Errorf("auth rejected: %s", code)
	}
	c.cfg.Logger.Info("authenticated", "session_epoch", resp.SessionEpoch, "instance_id", resp.InstanceID)
	return &resp, nil
}

func (c *Client) doRegister(ctx context.Context, conn *websocket.Conn) error {
	jobs := c.snapshotJobs()
	decls := make([]protocol.JobDecl, 0, len(jobs))
	for _, j := range jobs {
		d := protocol.JobDecl{
			Code: j.Code, Name: j.Name, Description: j.Description, Handler: j.Code,
			HandlerVersion: j.HandlerVersion, TimeoutSeconds: j.TimeoutSeconds,
			MaxConcurrency: j.MaxConcurrency, ParamsSchema: j.InputSchema,
		}
		if j.DefaultCron != "" {
			cron := j.DefaultCron
			d.DefaultCron = &cron
		}
		decls = append(decls, d)
	}
	host, _ := os.Hostname()
	buildTime := ""
	if !c.cfg.Build.BuildTime.IsZero() {
		buildTime = protocol.FormatTime(c.cfg.Build.BuildTime)
	}
	data := protocol.RegisterRequest{
		ServiceCode: c.cfg.ServiceCode,
		InstanceKey: c.cfg.InstanceKey,
		Environment: c.cfg.Environment,
		Host:        &protocol.HostInfo{Hostname: host, PID: os.Getpid()},
		Runtime:     protocol.RuntimeInfo{Language: "go", Version: runtime.Version()},
		Deployment: protocol.DeploymentInfo{
			Version: c.cfg.Build.Version, GitCommit: c.cfg.Build.GitCommit, BuildTime: buildTime,
		},
		Capacity:   protocol.CapacityInfo{MaxConcurrency: c.cfg.MaxConcurrency},
		SDKVersion: c.cfg.SDKVersion,
		StartedAt:  protocol.FormatTime(time.Now()),
		Jobs:       decls,
	}
	if err := c.rawWrite(ctx, conn, protocol.TypeServiceRegister, data); err != nil {
		return err
	}
	var env protocol.Envelope
	rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := wsjson.Read(rctx, conn, &env); err != nil {
		return fmt.Errorf("register read: %w", err)
	}
	if env.Type != protocol.TypeServiceRegisterAck {
		return fmt.Errorf("register: unexpected type %s data=%s", env.Type, string(env.Data))
	}
	var ack protocol.RegisterResponse
	if err := env.DecodeData(&ack); err != nil {
		return err
	}
	if !ack.OK {
		return fmt.Errorf("register rejected: %+v", ack.Error)
	}
	c.cfg.Logger.Info("registered", "accepted", len(ack.AcceptedJobs), "status", ack.InstanceStatus)
	return nil
}

func (c *Client) doUnregister(ctx context.Context, conn *websocket.Conn) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return c.rawWrite(ctx, conn, protocol.TypeServiceUnregister, protocol.UnregisterRequest{
		Reason:      "SHUTDOWN",
		InstanceKey: c.cfg.InstanceKey,
	})
}

func randomNonce(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func isFatalSessionError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "auth rejected") ||
		strings.Contains(s, "AUTH_INVALID_CREDENTIAL") ||
		strings.Contains(s, "StatusCode(4001)")
}
