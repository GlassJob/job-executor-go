package jobcenter

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/GlassJob/job-executor-go/buildinfo"
)

type ShutdownPolicy int

const (
	WaitThenCancel ShutdownPolicy = iota
	CancelImmediately
)

type BuildInfo struct {
	Version   string
	GitCommit string
	BuildTime time.Time
}

type Config struct {
	Endpoint       string
	ServiceCode    string
	CredentialID     string
	CredentialSecret string
	Credential       string // optional jcs_<id>.<secret>
	Environment    string
	InstanceKey    string // empty = hostname-pid
	MaxConcurrency int
	Build          BuildInfo
	TLS            *tls.Config
	ShutdownTimeout time.Duration
	ShutdownPolicy ShutdownPolicy
	Logger         *slog.Logger
	Labels         map[string]string
	SDKVersion     string
	// DataDir 存放终态 pending（默认 ./data）；也可用环境变量 JC_DATA_DIR。
	DataDir string
}

func (c *Config) defaults() error {
	if c.Credential != "" && (c.CredentialID == "" || c.CredentialSecret == "") {
		id, secret, err := ParseCredentialToken(c.Credential)
		if err != nil {
			return err
		}
		if c.CredentialID == "" {
			c.CredentialID = id
		}
		if c.CredentialSecret == "" {
			c.CredentialSecret = secret
		}
	}
	if c.Endpoint == "" || c.ServiceCode == "" || c.CredentialID == "" || c.CredentialSecret == "" {
		return fmt.Errorf("endpoint, service_code, credential_id and credential_secret are required")
	}
	if c.Environment == "" {
		c.Environment = "dev"
	}
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = runtime.NumCPU()
		if c.MaxConcurrency < 1 {
			c.MaxConcurrency = 1
		}
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Build.Version == "" && c.Build.GitCommit == "" {
		bi := buildinfo.Current()
		c.Build = BuildInfo{Version: bi.Version, GitCommit: bi.GitCommit, BuildTime: bi.BuildTime}
	}
	if c.Build.Version == "" {
		c.Build.Version = "0.0.0-dev"
	}
	if c.SDKVersion == "" {
		c.SDKVersion = "0.2.0"
	}
	if c.InstanceKey == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "host"
		}
		c.InstanceKey = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	if len(c.InstanceKey) > 128 {
		c.InstanceKey = c.InstanceKey[:128]
	}
	if c.DataDir == "" {
		if v := strings.TrimSpace(os.Getenv("JC_DATA_DIR")); v != "" {
			c.DataDir = v
		} else {
			c.DataDir = "./data"
		}
	}
	return nil
}

// ParseCredentialToken parses jcs_<credential_id>.<secret>.
func ParseCredentialToken(token string) (credentialID, secret string, err error) {
	token = strings.TrimSpace(token)
	const prefix = "jcs_"
	if !strings.HasPrefix(token, prefix) {
		return "", "", fmt.Errorf("credential token must start with jcs_")
	}
	rest := strings.TrimPrefix(token, prefix)
	dot := strings.IndexByte(rest, '.')
	if dot <= 0 || dot >= len(rest)-1 {
		return "", "", fmt.Errorf("credential token must be jcs_<id>.<secret>")
	}
	return rest[:dot], rest[dot+1:], nil
}
