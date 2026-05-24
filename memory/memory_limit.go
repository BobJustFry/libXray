package memory

import "sync/atomic"

// memoryLimitBytes — soft Go heap ceiling for iOS scavenger (half-limit check).
var memoryLimitBytes atomic.Int64

func init() {
	memoryLimitBytes.Store(30 * 1024 * 1024)
}

// SetMemoryLimitBytes updates the limit used by InitForceFree (iOS).
func SetMemoryLimitBytes(n int64) {
	if n <= 0 {
		memoryLimitBytes.Store(30 * 1024 * 1024)
		return
	}
	memoryLimitBytes.Store(n)
}

func memoryLimitBytesValue() int64 {
	return memoryLimitBytes.Load()
}
