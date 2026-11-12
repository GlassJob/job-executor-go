package jobcenter

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/GlassJob/job-executor-go/protocol"
)

const (
	logBufCapacity = 100
	logFlushEvery  = time.Second
)

// attemptLogBuf 按 attempt 缓冲 job.Log 上报；满/ERROR/定时/结束时 flush 为 task.log。
type attemptLogBuf struct {
	mu         sync.Mutex
	attemptID  string
	leaseToken string
	nextSeq    int64
	entries    []protocol.TaskLogEntry
	dropInfo   int
	dropWarn   int
	dropError  int
	closed     bool
	publish    func(protocol.TaskLog)
}

func newAttemptLogBuf(attemptID, leaseToken string, publish func(protocol.TaskLog)) *attemptLogBuf {
	return &attemptLogBuf{
		attemptID:  attemptID,
		leaseToken: leaseToken,
		nextSeq:    1,
		entries:    make([]protocol.TaskLogEntry, 0, logBufCapacity),
		publish:    publish,
	}
}

func (b *attemptLogBuf) logger() *slog.Logger {
	return slog.New(&reportHandler{buf: b})
}

func (b *attemptLogBuf) append(level, message string, fields json.RawMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	level = normalizeReportLevel(level)
	if len(b.entries) >= logBufCapacity {
		b.flushLocked()
	}
	for len(b.entries) >= logBufCapacity {
		if !b.dropOneLocked() {
			break
		}
	}
	ent := protocol.TaskLogEntry{
		Seq:     b.nextSeq,
		Level:   level,
		Message: message,
		Fields:  fields,
		Ts:      protocol.FormatTime(time.Now()),
	}
	b.nextSeq++
	b.entries = append(b.entries, ent)
	if level == "ERROR" {
		b.flushLocked()
	}
}

func (b *attemptLogBuf) dropOneLocked() bool {
	if len(b.entries) == 0 {
		return false
	}
	best := -1
	bestPri := 99
	for i, e := range b.entries {
		p := dropPriority(e.Level)
		if p < bestPri {
			bestPri = p
			best = i
		}
	}
	if best < 0 {
		return false
	}
	dropped := b.entries[best]
	b.entries = append(b.entries[:best], b.entries[best+1:]...)
	switch normalizeReportLevel(dropped.Level) {
	case "WARN":
		b.dropWarn++
	case "ERROR":
		b.dropError++
	default:
		b.dropInfo++
	}
	return true
}

func dropPriority(level string) int {
	switch normalizeReportLevel(level) {
	case "DEBUG":
		return 0
	case "INFO":
		return 1
	case "WARN":
		return 2
	case "ERROR":
		return 3
	default:
		return 1
	}
}

func (b *attemptLogBuf) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

func (b *attemptLogBuf) CloseAndFlush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
	b.closed = true
}

func (b *attemptLogBuf) flushLocked() {
	di, dw, de := b.dropInfo, b.dropWarn, b.dropError
	if (dw > 0 || de > 0) && !b.closed {
		fields, _ := json.Marshal(map[string]int{
			"dropped_info":  di,
			"dropped_warn":  dw,
			"dropped_error": de,
		})
		b.entries = append(b.entries, protocol.TaskLogEntry{
			Seq:     b.nextSeq,
			Level:   "WARN",
			Message: "log buffer overflow: dropped entries before flush",
			Fields:  fields,
			Ts:      protocol.FormatTime(time.Now()),
		})
		b.nextSeq++
	}
	if len(b.entries) == 0 && di == 0 && dw == 0 && de == 0 {
		return
	}
	entries := b.entries
	b.entries = make([]protocol.TaskLogEntry, 0, logBufCapacity)
	b.dropInfo, b.dropWarn, b.dropError = 0, 0, 0
	if b.publish == nil {
		return
	}
	b.publish(protocol.TaskLog{
		AttemptID:    b.attemptID,
		LeaseToken:   b.leaseToken,
		Entries:      entries,
		Dropped:      di + dw + de,
		DroppedInfo:  di,
		DroppedWarn:  dw,
		DroppedError: de,
	})
}

func normalizeReportLevel(level string) string {
	switch level {
	case "DEBUG", "debug":
		return "DEBUG"
	case "WARN", "WARNING", "warn", "warning":
		return "WARN"
	case "ERROR", "FATAL", "PANIC", "error", "fatal", "panic":
		return "ERROR"
	case "INFO", "info":
		return "INFO"
	default:
		return "INFO"
	}
}

type reportHandler struct {
	buf    *attemptLogBuf
	attrs  []slog.Attr
	groups []string
}

func (h *reportHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *reportHandler) Handle(_ context.Context, r slog.Record) error {
	fields := map[string]any{}
	for _, a := range h.attrs {
		h.putAttr(fields, a)
	}
	r.Attrs(func(a slog.Attr) bool {
		h.putAttr(fields, a)
		return true
	})
	var raw json.RawMessage
	if len(fields) > 0 {
		raw, _ = json.Marshal(fields)
	}
	h.buf.append(slogLevelName(r.Level), r.Message, raw)
	return nil
}

func (h *reportHandler) putAttr(dst map[string]any, a slog.Attr) {
	a.Value = a.Value.Resolve()
	key := a.Key
	if len(h.groups) > 0 {
		prefix := ""
		for _, g := range h.groups {
			prefix += g + "."
		}
		key = prefix + key
	}
	if a.Value.Kind() == slog.KindGroup {
		for _, ga := range a.Value.Group() {
			h.putAttr(dst, slog.Attr{Key: key + "." + ga.Key, Value: ga.Value})
		}
		return
	}
	dst[key] = a.Value.Any()
}

func (h *reportHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cp := *h
	cp.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &cp
}

func (h *reportHandler) WithGroup(name string) slog.Handler {
	cp := *h
	cp.groups = append(append([]string{}, h.groups...), name)
	return &cp
}

func slogLevelName(l slog.Level) string {
	switch {
	case l < slog.LevelInfo:
		return "DEBUG"
	case l < slog.LevelWarn:
		return "INFO"
	case l < slog.LevelError:
		return "WARN"
	default:
		return "ERROR"
	}
}
