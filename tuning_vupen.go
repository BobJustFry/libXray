// SPDX-License-Identifier: MIT
//
// tuning_vupen.go — Vupen API для LibXray (память + TUN-лимиты в BobJustFry/xray-core).
package libXray

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"

	"github.com/xtls/libxray/memory"
	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/proxy/tun"
	grpctransport "github.com/xtls/xray-core/transport/internet/grpc"
	"golang.org/x/net/http2"
)

// vupenCoreBuild — BUILD ядра, как у приложения: +1 на каждый коммит в форке xray-core
// или в libXray, который меняет то, что уезжает в бинарник. Считается с момента, когда
// ядро впервые стало нашим: к 2026-09-07 указатели на ядро в приложении двигались
// 45 раз (22 коммита Vupen поверх v26.7.28 + 12 в libXray), эта сборка — 46-я.
// Правило — docs/VERSIONING.md, раздел «Ядро».
const vupenCoreBuild = 56

// VupenCoreInfo — строка для лога NE при старте туннеля: BUILD ядра и фактический
// потолок scratch-буфера отправки из НАШЕЙ копии x/net. Ссылка на
// http2.VupenRequestBodyScratchMax намеренная: со стоковым x/net этого символа нет,
// и сборка падает вместо того, чтобы молча уехать со старым ядром (так вышло с 580).
func VupenCoreInfo() string {
	return fmt.Sprintf("build=%d xnet.scratchMaxKB=%d", vupenCoreBuild, http2.VupenRequestBodyScratchMax>>10)
}

// VupenCoreBuild — только номер, для строки версии ядра в приложении: «26.7.28 (vupen 46)».
func VupenCoreBuild() string {
	return fmt.Sprintf("%d", vupenCoreBuild)
}

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

// GoMemoryDiag — память Go для журнала NE. `heap_alloc` первым: парсеры логов
// ищут его по префиксу. `go_limit_use` = Sys − HeapReleased — то, с чем сравнивает
// себя GOMEMLIMIT. `heap_sys` сам по себе не убывает (= inuse + idle), по нему нельзя
// судить, отдана ли память системе — для этого `heap_released`.
func GoMemoryDiag() string {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	const mb = 1024.0 * 1024.0
	return fmt.Sprintf(
		"heap_alloc=%.1fMB heap_sys=%.1fMB stack=%.1fMB heap_idle=%.1fMB heap_released=%.1fMB sys=%.1fMB go_limit_use=%.1fMB goroutines=%d",
		float64(ms.HeapAlloc)/mb,
		float64(ms.HeapSys)/mb,
		float64(ms.StackInuse)/mb,
		float64(ms.HeapIdle)/mb,
		float64(ms.HeapReleased)/mb,
		float64(ms.Sys)/mb,
		float64(ms.Sys-ms.HeapReleased)/mb,
		runtime.NumGoroutine(),
	)
}

// WriteHeapProfile — heap-профиль Go в текстовом виде (pprof debug=1: inuse/alloc по
// стекам с именами функций) в файл. Возвращает "" при успехе или текст ошибки.
// Текст читается без бинарника — это ответ на «кто держит память в NE», а не догадка.
func WriteHeapProfile(path string) string {
	f, err := os.Create(path)
	if err != nil {
		return err.Error()
	}
	defer f.Close()
	if err := pprof.Lookup("heap").WriteTo(f, 1); err != nil {
		return err.Error()
	}
	return ""
}

// TunStats — счётчики тракта TUN (proxy/tun/tuning_stats.go) для heartbeat NE:
// жив ли цикл чтения, пакеты в обе стороны, SYN, отброшенные gVisor по maxInFlight,
// UDP-сессии и сбросы по их лимиту.
func TunStats() string {
	// sniff[...] — исходы sniff в dispatcher (ядро 50): p1ok / p2ok / timeout / early.
	return tun.Stats() + " sniff[" + dispatcher.VupenSniffStats() + "]"
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
