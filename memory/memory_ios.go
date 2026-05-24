//go:build ios

package memory

import (
	"context"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

const iosScavengeInterval = 1 * time.Second

var (
	forceFreeMu     sync.Mutex
	forceFreeCancel context.CancelFunc
)

// parseGoGcPercent reads GOGC from env (10–100), default 20.
func parseGoGcPercent() int {
	s := os.Getenv("GOGC")
	if s == "" {
		return 20
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 20
	}
	if n < 10 {
		return 10
	}
	if n > 100 {
		return 100
	}
	return n
}

// parseGcIntervalSec — Android / legacy: env ticker with GC+FreeOSMemory. iOS NE не использует.
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

// InitForceFree (iOS NE): GOGC из env, GOMAXPROCS(1), раз в 1с FreeOSMemory при HeapInuse > limit/2.
// GOMEMLIMIT / SetMemoryLimitMB задаёт Swift до RunXray — здесь не перетираем.
// Вызывается при каждом RunXray: перезапускает фоновую горутину.
func InitForceFree() {
	forceFreeMu.Lock()
	defer forceFreeMu.Unlock()
	if forceFreeCancel != nil {
		forceFreeCancel()
		forceFreeCancel = nil
	}
	debug.SetGCPercent(parseGoGcPercent())
	runtime.GOMAXPROCS(1)
	ctx, cancel := context.WithCancel(context.Background())
	forceFreeCancel = cancel
	go iosMemoryScavenger(ctx)
}

func iosMemoryScavenger(ctx context.Context) {
	ticker := time.NewTicker(iosScavengeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			limit := memoryLimitBytesValue()
			if limit <= 0 {
				continue
			}
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			if ms.HeapInuse > uint64(limit/2) {
				debug.FreeOSMemory()
			}
		}
	}
}

// InitForceFreeAndroid — env-интервал, каждый тик GC + FreeOSMemory (не iOS NE).
func InitForceFreeAndroid() {
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
				runtime.GC()
				debug.FreeOSMemory()
			}
		}
	}()
}

// StopForceFree останавливает фоновую горутину (StopXray).
func StopForceFree() {
	forceFreeMu.Lock()
	defer forceFreeMu.Unlock()
	if forceFreeCancel != nil {
		forceFreeCancel()
		forceFreeCancel = nil
	}
}
