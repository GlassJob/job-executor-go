// Example executor: demo.hello / demo.fail / demo.sleep / demo.timeout / demo.counter / demo.progress / demo.logs
//
// demo.counter 展示 job_states（中心 key=default）：
//
//	BindState 读入 → Checkpoint 中途落盘 → SetNextState 成功收尾。
//
// demo.progress 约 5 分钟跑完，周期性上报 Progress。
//
// demo.logs：job.Log 上报中心；灌满缓冲触发丢弃与合成 WARN。
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GlassJob/job-executor-go/internal/envload"
	"github.com/GlassJob/job-executor-go/jobcenter"
)

func main() {
	if err := envload.LoadFirst(envload.CandidatePaths()...); err != nil {
		log.Fatal("load .env: ", err)
	}
	cfg := jobcenter.Config{
		Endpoint:         env("JC_WS_ENDPOINT", "ws://127.0.0.1:8080/v1/executor/ws"),
		ServiceCode:      env("JC_SERVICE_CODE", "sdk.demo"),
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
			var in struct {
				Name string `json:"name"`
			}
			_ = job.Bind(&in)
			if in.Name == "" {
				in.Name = "world"
			}
			job.Log().Info("hello handler", "name", in.Name)
			job.Progress(100, "done")
			return job.SetResult(map[string]any{"hello": in.Name})
		}})

	_ = c.RegisterJob(jobcenter.Job{Code: "demo.fail", Name: "Fail", HandlerVersion: "1.0.0",
		Handler: func(ctx context.Context, job *jobcenter.JobContext) error {
			job.Log().Info("fail handler about to error")
			return jobcenter.NonRetryable(errors.New("boom"), "DEMO_FAIL")
		}})

	_ = c.RegisterJob(jobcenter.Job{Code: "demo.sleep", Name: "Sleep", HandlerVersion: "1.0.0", TimeoutSeconds: 120,
		Handler: func(ctx context.Context, job *jobcenter.JobContext) error {
			job.Log().Info("sleep start", "seconds", 60)
			select {
			case <-ctx.Done():
				job.Log().Info("sleep cancelled", "err", ctx.Err())
				return ctx.Err()
			case <-time.After(60 * time.Second):
				job.Log().Info("sleep finished")
				return job.SetResult(map[string]any{"slept": true})
			}
		}})

	_ = c.RegisterJob(jobcenter.Job{Code: "demo.timeout", Name: "Timeout", HandlerVersion: "1.0.0", TimeoutSeconds: 2,
		Handler: func(ctx context.Context, job *jobcenter.JobContext) error {
			job.Log().Info("timeout handler sleeping past deadline")
			time.Sleep(5 * time.Second)
			return context.DeadlineExceeded
		}})

	// 约 5 分钟：每 15 秒上报一次进度（0→100），可观察 Admin 执行详情进度条。
	_ = c.RegisterJob(jobcenter.Job{Code: "demo.progress", Name: "Progress", HandlerVersion: "1.0.0", TimeoutSeconds: 360,
		Handler: func(ctx context.Context, job *jobcenter.JobContext) error {
			const (
				total = 5 * time.Minute
				step  = 15 * time.Second
				steps = int(total / step) // 20
			)
			job.Log().Info("progress job start", "duration", total.String(), "step", step.String())
			job.Progress(0, "started")

			started := time.Now()
			for i := 1; i <= steps; i++ {
				select {
				case <-ctx.Done():
					job.Log().Info("progress cancelled", "err", ctx.Err(), "elapsed", time.Since(started).String())
					return ctx.Err()
				case <-time.After(step):
				}
				pct := float64(i) / float64(steps) * 100
				msg := "running"
				if i == steps {
					msg = "done"
				}
				job.Progress(pct, msg)
				job.Log().Info("progress tick", "percent", pct, "elapsed", time.Since(started).String())
			}

			return job.SetResult(map[string]any{
				"duration_ms": time.Since(started).Milliseconds(),
				"steps":       steps,
			})
		}})

	// job_states.default：累计计数。Admin 触发后可 GET .../states 查看 key=default。
	_ = c.RegisterJob(jobcenter.Job{Code: "demo.counter", Name: "Counter", HandlerVersion: "1.0.0", TimeoutSeconds: 30,
		Handler: func(ctx context.Context, job *jobcenter.JobContext) error {
			var st struct {
				Count int    `json:"count"`
				Last  string `json:"last,omitempty"`
			}
			_ = job.BindState(&st)
			job.Log().Info("loaded job state", "version", job.StateVersion(), "count", st.Count)

			var in struct {
				Step int `json:"step"`
			}
			_ = job.Bind(&in)
			if in.Step == 0 {
				in.Step = 1
			}

			st.Count += in.Step
			st.Last = "checkpoint"
			job.Progress(40, "checkpointing")
			if err := job.Checkpoint(st); err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}

			st.Last = "completed"
			job.Progress(100, "finishing")
			if err := job.SetNextState(st); err != nil {
				return err
			}
			return job.SetResult(map[string]any{
				"count":         st.Count,
				"step":          in.Step,
				"state_version": job.StateVersion(),
			})
		}})

	// job.Log → task.log 落库；超过缓冲容量时优先挤 INFO，丢异常会合成 WARN。
	_ = c.RegisterJob(jobcenter.Job{Code: "demo.logs", Name: "Logs", HandlerVersion: "1.0.0", TimeoutSeconds: 30,
		Handler: func(ctx context.Context, job *jobcenter.JobContext) error {
			job.Log().Info("demo.logs start")
			for i := 0; i < 120; i++ {
				job.Log().Info("flood info", "i", i)
			}
			job.Log().Warn("sample warn", "n", 1)
			job.Log().Error("sample error", "n", 1)
			return job.SetResult(map[string]any{"flooded": 120})
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
