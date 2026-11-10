package jobcenter

import (
	"context"
	"testing"

	"github.com/GlassJob/job-executor-go/protocol"
)

func TestParseCredentialToken(t *testing.T) {
	id, secret, err := ParseCredentialToken("jcs_abc-123.s3cret")
	if err != nil {
		t.Fatal(err)
	}
	if id != "abc-123" || secret != "s3cret" {
		t.Fatalf("got %q %q", id, secret)
	}
	if _, _, err := ParseCredentialToken("bad"); err == nil {
		t.Fatal("expected error")
	}
}

func TestConfigCredentialCompat(t *testing.T) {
	cfg := Config{
		Endpoint:    "ws://127.0.0.1:8080/v1/executor/ws",
		ServiceCode: "demo",
		Credential:  "jcs_cred1.supersecret",
		InstanceKey: "inst-1",
	}
	if err := cfg.defaults(); err != nil {
		t.Fatal(err)
	}
	if cfg.CredentialID != "cred1" || cfg.CredentialSecret != "supersecret" {
		t.Fatalf("parsed %q %q", cfg.CredentialID, cfg.CredentialSecret)
	}
}

func TestAuthCanonical(t *testing.T) {
	canon := protocol.AuthCanonical("svc", "cred", "key", "1700000000", "nonce1")
	want := "AUTH\nsvc\ncred\nkey\n1700000000\nnonce1"
	if canon != want {
		t.Fatalf("canon=%q", canon)
	}
	sig := protocol.SignHMAC("secret", canon)
	if len(sig) != 64 {
		t.Fatalf("sig len %d", len(sig))
	}
}

func TestHandleCancelMarksRequested(t *testing.T) {
	c, err := NewClient(Config{
		Endpoint: "ws://127.0.0.1:9/v1/executor/ws", ServiceCode: "demo",
		CredentialID: "c1", CredentialSecret: "s1", InstanceKey: "i1",
	})
	if err != nil {
		t.Fatal(err)
	}
	writeCh := make(chan writeReq, 4)
	go func() {
		for req := range writeCh {
			if req.err != nil {
				req.err <- nil
			}
		}
	}()
	c.handleCancel(context.Background(), writeCh, &protocol.TaskCancel{AttemptID: "a1", LeaseToken: "L"})
	if !c.wasCancelRequested("a1") {
		t.Fatal("expected cancel requested")
	}
	close(writeCh)
}
