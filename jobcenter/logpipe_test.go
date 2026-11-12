package jobcenter

import (
	"testing"

	"github.com/GlassJob/job-executor-go/protocol"
)

func TestAttemptLogBufFlushOnFullThenError(t *testing.T) {
	var frames []protocol.TaskLog
	buf := newAttemptLogBuf("a1", "lease", func(msg protocol.TaskLog) {
		frames = append(frames, msg)
	})
	for i := 0; i < logBufCapacity; i++ {
		buf.append("INFO", "info", nil)
	}
	buf.append("ERROR", "boom", nil) // 满则先 flush INFO，再 append ERROR 并立即 flush
	if len(frames) != 2 {
		t.Fatalf("want 2 flushes (full + ERROR), got %d", len(frames))
	}
	if frames[0].Dropped != 0 {
		t.Fatalf("full flush should not drop, got %+v", frames[0])
	}
	hasError := false
	for _, e := range frames[1].Entries {
		if e.Level == "ERROR" {
			hasError = true
		}
	}
	if !hasError {
		t.Fatal("ERROR entry should be in second flush")
	}
}

func TestAttemptLogBufDropPrefersInfo(t *testing.T) {
	buf := newAttemptLogBuf("a1", "lease", nil) // publish nil：flush 清空但不发出
	buf.entries = []protocol.TaskLogEntry{
		{Seq: 1, Level: "INFO", Message: "i"},
		{Seq: 2, Level: "WARN", Message: "w"},
		{Seq: 3, Level: "ERROR", Message: "e"},
	}
	if !buf.dropOneLocked() {
		t.Fatal("dropOne failed")
	}
	if buf.dropInfo != 1 || buf.dropWarn != 0 || buf.dropError != 0 {
		t.Fatalf("want drop INFO first, got info=%d warn=%d error=%d", buf.dropInfo, buf.dropWarn, buf.dropError)
	}
	if !buf.dropOneLocked() {
		t.Fatal("dropOne failed")
	}
	if buf.dropWarn != 1 || buf.dropError != 0 {
		t.Fatalf("want drop WARN next, got warn=%d error=%d", buf.dropWarn, buf.dropError)
	}
	if !buf.dropOneLocked() {
		t.Fatal("dropOne failed")
	}
	if buf.dropError != 1 {
		t.Fatalf("want drop ERROR last, got error=%d", buf.dropError)
	}
}

func TestAttemptLogBufSyntheticWarn(t *testing.T) {
	var frames []protocol.TaskLog
	buf := newAttemptLogBuf("a1", "lease", func(msg protocol.TaskLog) {
		frames = append(frames, msg)
	})
	buf.entries = []protocol.TaskLogEntry{{Seq: 1, Level: "ERROR", Message: "e"}}
	buf.nextSeq = 2
	buf.dropError = 2
	buf.dropWarn = 1
	buf.flushLocked()
	if len(frames) != 1 {
		t.Fatalf("want 1 frame, got %d", len(frames))
	}
	f := frames[0]
	if f.DroppedError != 2 || f.DroppedWarn != 1 {
		t.Fatalf("dropped fields: %+v", f)
	}
	found := false
	for _, e := range f.Entries {
		if e.Level == "WARN" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want synthetic WARN, entries=%+v", f.Entries)
	}
}
