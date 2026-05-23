// SPDX-License-Identifier: MIT
//
// tuning_vupen.go — Vupen-only API shims, отсутствующие в upstream XTLS/libXray.
// Эти функции добавлены, чтобы существующий Swift-код (`SwiftyXrayKit`) собирался
// против нашего форка без правок. После полной миграции на собственный
// `VupenXrayBridge` (сборка 175) часть из них уйдёт совсем.
//
// ВНИМАНИЕ — функции переехали сюда из стороннего форка `dima-u/libXray-apple`
// (v26.3.27-ios) практически вслепую. Их поведение **не валидировалось** на
// нашем стеке и не покрыто тестами. Перед использованием как-либо в продакшене:
//
//   1. Подтвердить, что `debug.SetMemoryLimit` действительно влияет на jetsam-порог
//      Network Extension и сборку мусора Go при нашем профиле нагрузки. Если для
//      нашей цели достаточно env `GOMEMLIMIT` (выставляется в Swift), эту функцию
//      можно выпилить.
//   2. TCP/UDP-функции ниже — **заглушки**: в upstream Xray-core нет публичных
//      рычагов для лимитов TCP-буферов / in-flight / UDP-сессий, которые
//      когда-то экспортировал форк `dima-u`. Скорее всего, в патче dima-u были
//      правки внутри `xray-core` (которых у нас нет). До их разбора эти три
//      функции просто принимают параметры и ничего не делают. См. логи Swift
//      `XrayTuningPreset.apply()` — он зовёт их **на каждом** старте VPN, без
//      реального эффекта на трафик.
//   3. Если позже окажется, что лимиты критичны (например, NE стабильно ловит
//      jetsam) — нужно либо повторить патч dima-u в нашем форке xray-core, либо
//      реализовать лимиты на уровне `XrayBridge` (`VupenXrayBridge` в 175).
//
// TL;DR: код собирается, `SetMemoryLimitMB` реально работает, остальные три
// функции — заглушки на время миграции.
package libXray

import (
	"fmt"
	"runtime"
	"runtime/debug"
)

// SetMemoryLimitMB задаёт soft-limit Go-heap в МБ через `runtime/debug`.
// Аналог переменной окружения `GOMEMLIMIT=NMiB`, выставляется до RunXray.
// 0 или отрицательное значение трактуем как «не ограничивать».
func SetMemoryLimitMB(mb int64) {
	if mb <= 0 {
		debug.SetMemoryLimit(-1)
		return
	}
	debug.SetMemoryLimit(mb * 1024 * 1024)
}

// ForceGC запускает сборку мусора Go и возвращает неиспользуемые страницы кучи ОС.
// Вызывать после смены GOMEMLIMIT на лету (аналог Android Libv2ray ForceGC).
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

// SetTCPBufMaxKB — ЗАГЛУШКА. Раньше (в форке dima-u/libXray-apple) задавала
// лимит RX/TX буфера на TCP-соединение в KB. В upstream Xray-core такой
// настройки нет; реальный эффект отсутствует. Параметр сохранён, чтобы Swift
// собирался без правок. См. шапку файла.
func SetTCPBufMaxKB(kb int32) {
	_ = kb
}

// SetTCPMaxInFlight — ЗАГЛУШКА. Раньше задавала лимит одновременных TCP
// in-flight в gVisor TUN. У нас не реализовано, эффекта нет. См. шапку файла.
func SetTCPMaxInFlight(n int32) {
	_ = n
}

// SetMaxUDPConns — ЗАГЛУШКА. Раньше задавала лимит одновременных UDP-сессий.
// У нас не реализовано, эффекта нет. См. шапку файла.
func SetMaxUDPConns(n int32) {
	_ = n
}
