//go:build ios

package memory

import (
	"context"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

var (
	forceFreeMu     sync.Mutex
	forceFreeCancel context.CancelFunc
)

// parseGcIntervalSec читает xray.tunnel.gc.interval.sec (5–120), default 30. <0 — выкл.
func parseGcIntervalSec() time.Duration {
	s := os.Getenv("xray.tunnel.gc.interval.sec")
	if s == "" {
		return 30 * time.Second
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 30 * time.Second
	}
	if n < 0 {
		return 0
	}
	if n == 0 {
		return 0
	}
	if n < 5 {
		n = 5
	}
	if n > 120 {
		n = 120
	}
	return time.Duration(n) * time.Second
}

// InitForceFree запускает один фоновый тикер FreeOSMemory (не дублируется при повторном RunXray).
// GOMEMLIMIT / GOGC / SetMemoryLimitMB задаёт Swift до RunXray — здесь не перетираем.
func InitForceFree() {
	forceFreeMu.Lock()
	defer forceFreeMu.Unlock()
	if forceFreeCancel != nil {
		return
	}
	interval := parseGcIntervalSec()
	if interval <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	forceFreeCancel = cancel
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				debug.FreeOSMemory()
			}
		}
	}()
}

// StopForceFree останавливает тикер (вызывать из StopXray).
func StopForceFree() {
	forceFreeMu.Lock()
	defer forceFreeMu.Unlock()
	if forceFreeCancel != nil {
		forceFreeCancel()
		forceFreeCancel = nil
	}
}
