// SPDX-License-Identifier: MIT
//
// tuning_vupen.go — Vupen API для LibXray (память + TUN-лимиты в BobJustFry/xray-core).
package libXray

import (
	"fmt"
	"runtime"
	"runtime/debug"

	"github.com/xtls/libxray/memory"
	"github.com/xtls/xray-core/proxy/tun"
	grpctransport "github.com/xtls/xray-core/transport/internet/grpc"
)

// SetMemoryLimitMB задаёт soft-limit Go-heap в МБ через `runtime/debug`.
func SetMemoryLimitMB(mb int64) {
	if mb <= 0 {
		debug.SetMemoryLimit(-1)
		memory.SetMemoryLimitBytes(30 * 1024 * 1024)
		return
	}
	n := mb * 1024 * 1024
	debug.SetMemoryLimit(n)
	memory.SetMemoryLimitBytes(n)
}

// ForceGC запускает сборку мусора Go и возвращает неиспользуемые страницы кучи ОС.
func ForceGC() {
	runtime.GC()
	debug.FreeOSMemory()
}

// GoMemoryDiag — heap_alloc / heap_sys / stack (МБ) для журнала NE.
func GoMemoryDiag() string {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	const mb = 1024.0 * 1024.0
	return fmt.Sprintf(
		"heap_alloc=%.1fMB heap_sys=%.1fMB stack=%.1fMB",
		float64(ms.HeapAlloc)/mb,
		float64(ms.HeapSys)/mb,
		float64(ms.StackInuse)/mb,
	)
}

// SetTCPBufMaxKB — лимит RX/TX буфера TCP (gVisor), KB.
func SetTCPBufMaxKB(kb int32) {
	tun.SetTCPBufMaxKB(int(kb))
}

// SetTCPMaxInFlight — лимит одновременных TCP в TUN (gVisor forwarder).
func SetTCPMaxInFlight(n int32) {
	tun.SetTCPMaxInFlight(int(n))
}

// SetMaxUDPConns — лимит одновременных UDP-сессий в TUN.
func SetMaxUDPConns(n int32) {
	tun.SetMaxUDPConns(int(n))
}

// ResetGrpcTransportPool — закрыть кэш gRPC upstream (recovery после смены net path).
func ResetGrpcTransportPool() int32 {
	return int32(grpctransport.ResetTransportPool())
}

// GrpcTransportPoolSize — число закэшированных gRPC ClientConn (диагностика NE).
func GrpcTransportPoolSize() int32 {
	return int32(grpctransport.TransportPoolSize())
}
