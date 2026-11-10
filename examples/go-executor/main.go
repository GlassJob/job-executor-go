// Example executor registering demo.hello / demo.sleep for Job Center 1.1.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GlassJob/job-executor-go/jobcenter"
)

func main() {
	cfg := jobcenter.Config{
		Endpoint:     env("JC_WS_ENDPOINT", "ws://127.0.0.1:8080/v1/executor/ws"),
		ServiceCode:  env("JC_SERVICE_CODE", "sdk.demo"),
		CredentialID:     os.Getenv("JC_CREDENTIAL_ID"),
		CredentialSecret: os.Getenv("JC_CREDENTIAL_SECRET"),
		Credential:       os.Getenv("JC_CREDENTIAL"), // optional jcs_<id>.<secret>
		Environment:      env("JC_ENVIRONMENT", "dev"),
		InstanceKey:      env("JC_INSTANCE_KEY", "go-executor-1"),
		Build:            jobcenter.BuildInfo{Version: "0.2.0", GitCommit: "example"},
	}
	if cfg.CredentialID == "" || cfg.CredentialSecret == "" {
		if cfg.Credential == "" {
			log.Fatal("缺少凭据：设置 JC_CREDENTIAL_ID + JC_CREDENTIAL_SECRET，或 JC_CREDENTIAL=jcs_<id>.<secret>\n" +
				"示例：\n" +
				"  $env:JC_CREDENTIAL_ID='...'\n" +
				"  $env:JC_CREDENTIAL_SECRET='...'\n" +
				"  $env:JC_SERVICE_CODE='sdk.demo'\n" +
				"  go run ./examples/go-executor")
		}
	}
	c, err := jobcenter.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	_ = c.RegisterJob(jobcenter.Job{Code: "demo.hello", Name: "Hello", HandlerVersion: "1.0.0",
		Handler: func(ctx context.Context, job *jobcenter.JobContext) error {
			return job.SetResult(map[string]any{"hello": "world"})
		}})
	_ = c.RegisterJob(jobcenter.Job{Code: "demo.sleep", Name: "Sleep", HandlerVersion: "1.0.0", TimeoutSeconds: 120,
		Handler: func(ctx context.Context, job *jobcenter.JobContext) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(60 * time.Second):
				return job.SetResult(map[string]any{"slept": true})
			}
		}})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Fatal(c.Start(ctx))
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
