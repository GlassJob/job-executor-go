package jobcenter

import (
	"math/rand"
	"time"
)

func nextBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	base := time.Second << attempt
	if base > 30*time.Second {
		base = 30 * time.Second
	}
	jitter := 0.2
	f := 1 + (rand.Float64()*2-1)*jitter
	d := time.Duration(float64(base) * f)
	if d < time.Second {
		d = time.Second
	}
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}
