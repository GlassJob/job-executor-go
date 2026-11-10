package buildinfo

import (
	"runtime/debug"
	"time"
)

var (
	Version   = ""
	GitCommit = ""
	BuildTime = ""
)

type Info struct {
	Version   string
	GitCommit string
	BuildTime time.Time
}

func Current() Info {
	info := Info{Version: Version, GitCommit: GitCommit}
	if BuildTime != "" {
		if t, err := time.Parse(time.RFC3339, BuildTime); err == nil {
			info.BuildTime = t
		}
	}
	if info.Version != "" || info.GitCommit != "" {
		return info
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			if info.GitCommit == "" {
				info.GitCommit = s.Value
				if len(info.GitCommit) > 12 {
					info.GitCommit = info.GitCommit[:12]
				}
			}
		case "vcs.time":
			if info.BuildTime.IsZero() {
				if t, err := time.Parse(time.RFC3339, s.Value); err == nil {
					info.BuildTime = t
				}
			}
		}
	}
	if info.Version == "" {
		info.Version = "dev"
	}
	return info
}
