package jobcenter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type pendingRecord struct {
	AttemptID string          `json:"attempt_id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
}

type pendingStore struct {
	dir string
	mu  sync.Mutex
}

func newPendingStore(dir string) (*pendingStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &pendingStore{dir: dir}, nil
}

func (p *pendingStore) Save(attemptID, typ string, data any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	rec := pendingRecord{AttemptID: attemptID, Type: typ, Data: raw}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	path := p.pathFor(attemptID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (p *pendingStore) Remove(attemptID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = os.Remove(p.pathFor(attemptID))
}

func (p *pendingStore) List() ([]pendingRecord, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]pendingRecord, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(p.dir, e.Name()))
		if err != nil {
			continue
		}
		var rec pendingRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			continue
		}
		if rec.AttemptID == "" || rec.Type == "" {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

func (p *pendingStore) pathFor(attemptID string) string {
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, attemptID)
	return filepath.Join(p.dir, safe+".json")
}
