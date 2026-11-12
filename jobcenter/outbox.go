package jobcenter

import (
	"context"
	"encoding/json"
	"time"

	"github.com/GlassJob/job-executor-go/protocol"
)

const softOutboxLimit = 256

type softMsg struct {
	typ  string
	data any
}

type sessionSend struct {
	ctx     context.Context
	writeCh chan writeReq
}

func (c *Client) setSessionSend(s *sessionSend) {
	c.sendMu.Lock()
	c.sessionSend = s
	c.sendMu.Unlock()
}

func (c *Client) clearSessionSend(s *sessionSend) {
	c.sendMu.Lock()
	if c.sessionSend == s {
		c.sessionSend = nil
	}
	c.sendMu.Unlock()
}

func (c *Client) currentSend() *sessionSend {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.sessionSend
}

func (c *Client) trySessionWrite(typ string, data any) bool {
	s := c.currentSend()
	if s == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	return c.enqueue(ctx, s.writeCh, typ, data) == nil
}

// publishSoft 非终态：能发就发，否则进内存缓冲（可丢 progress/log）。
func (c *Client) publishSoft(typ string, data any) {
	if c.trySessionWrite(typ, data) {
		return
	}
	c.softMu.Lock()
	defer c.softMu.Unlock()
	if len(c.softBuf) >= softOutboxLimit {
		switch typ {
		case protocol.TypeTaskProgress, protocol.TypeTaskLog:
			return
		default:
			kept := c.softBuf[:0]
			dropped := false
			for _, m := range c.softBuf {
				if !dropped && (m.typ == protocol.TypeTaskProgress || m.typ == protocol.TypeTaskLog) {
					dropped = true
					continue
				}
				kept = append(kept, m)
			}
			c.softBuf = kept
			if len(c.softBuf) >= softOutboxLimit {
				c.softBuf = c.softBuf[1:]
			}
		}
	}
	c.softBuf = append(c.softBuf, softMsg{typ: typ, data: data})
}

// publishTerminal 终态先落盘再尽力发送；ack 前不删。
func (c *Client) publishTerminal(attemptID, typ string, data any) error {
	if c.pending == nil {
		_ = c.trySessionWrite(typ, data)
		return nil
	}
	if err := c.pending.Save(attemptID, typ, data); err != nil {
		return err
	}
	_ = c.trySessionWrite(typ, data)
	return nil
}

func (c *Client) flushSoftOutbox() {
	c.softMu.Lock()
	buf := append([]softMsg(nil), c.softBuf...)
	c.softBuf = nil
	c.softMu.Unlock()
	for i, m := range buf {
		if !c.trySessionWrite(m.typ, m.data) {
			c.softMu.Lock()
			c.softBuf = append(append([]softMsg(nil), buf[i:]...), c.softBuf...)
			c.softMu.Unlock()
			return
		}
	}
}

func (c *Client) replayPending() {
	if c.pending == nil {
		return
	}
	list, err := c.pending.List()
	if err != nil {
		c.cfg.Logger.Warn("list pending results", "err", err)
		return
	}
	for _, rec := range list {
		var payload any
		if err := json.Unmarshal(rec.Data, &payload); err != nil {
			c.cfg.Logger.Warn("bad pending record", "attempt_id", rec.AttemptID, "err", err)
			continue
		}
		if !c.trySessionWrite(rec.Type, payload) {
			c.cfg.Logger.Warn("replay pending failed", "attempt_id", rec.AttemptID, "type", rec.Type)
			return
		}
		c.cfg.Logger.Info("replayed pending result", "attempt_id", rec.AttemptID, "type", rec.Type)
	}
}

func (c *Client) onResultAck(ack protocol.TaskResultAck) {
	if ack.AttemptID == "" {
		return
	}
	switch ack.Status {
	case "OK":
		if c.pending != nil {
			c.pending.Remove(ack.AttemptID)
		}
	case "REJECTED":
		if ack.ErrorCode == "ALREADY_FINISHED" {
			if c.pending != nil {
				c.pending.Remove(ack.AttemptID)
			}
			c.stopLocalAttempt(ack.AttemptID)
		}
	}
}

func (c *Client) stopLocalAttempt(attemptID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if rt, ok := c.running[attemptID]; ok && rt.cancel != nil {
		rt.cancel()
	}
}
