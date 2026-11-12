// Package envload loads KEY=VALUE pairs from a .env file into the process
// environment. Existing real environment variables always take precedence.
package envload

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// LoadDotEnv reads path and sets unset variables. Missing file is a no-op.
func LoadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if len(v) >= 2 {
			if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
				v = v[1 : len(v)-1]
			}
		}
		if k == "" {
			continue
		}
		if _, exists := os.LookupEnv(k); exists {
			continue
		}
		_ = os.Setenv(k, v)
	}
	return sc.Err()
}

// LoadFirst tries paths in order; loads the first existing file.
func LoadFirst(paths ...string) error {
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, err := os.Stat(p); err != nil {
			continue
		}
		return LoadDotEnv(p)
	}
	return nil
}

// CandidatePaths returns common .env locations for examples run from module root or subdirs.
func CandidatePaths() []string {
	var out []string
	cwd, _ := os.Getwd()
	if cwd != "" {
		out = append(out, filepath.Join(cwd, ".env"))
		out = append(out, filepath.Join(cwd, "..", ".env"))
		out = append(out, filepath.Join(cwd, "..", "..", ".env"))
	}
	return out
}
