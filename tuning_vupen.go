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
	"github.com/xtls/libxray/xray"
	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/observatory/burst"
	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/proxy/tun"
	grpctransport "github.com/xtls/xray-core/transport/internet/grpc"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"golang.org/x/net/http2"
)

// vupenCoreBuild — BUILD ядра, как у приложения: +1 на каждый коммит в форке xray-core
// или в libXray, который меняет то, что уезжает в бинарник. Считается с момента, когда
// ядро впервые стало нашим: к 2026-09-07 указатели на ядро в приложении двигались
// 45 раз (22 коммита Vupen поверх v26.7.28 + 12 в libXray), эта сборка — 46-я.
// Правило — docs/VERSIONING.md, раздел «Ядро».
const vupenCoreBuild = 84

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

// VupenObservatoryPause — пробы балансировщика на паузу: устройство уснуло или
// пропала сеть. Без этого каждый раунд во сне пишет каждому узлу провал, живущий
// `interval × sampling × 2`, и после сна балансировщик считает мёртвыми все узлы
// сразу (подробности — app/observatory/burst/tuning_vupen_sleep.go).
func VupenObservatoryPause(reason string) {
	burst.VupenObservatoryPause(reason)
}

// VupenObservatoryResume — проснулись или вернулась сеть: счётчики мёртвых узлов
// сбрасываются, ближайшие секунды провалы не записываются, пачка проб уходит сразу.
func VupenObservatoryResume(reason string) {
	burst.VupenObservatoryResume(reason)
}

// VupenObservatoryNetworkChanged — путь сменился (wifi↔cellular, пропала или
// вернулась сеть). Пробы, упавшие вместе со старым маршрутом, ближайшие секунды
// не записываются: они говорят о телефоне, а не об узле.
func VupenObservatoryNetworkChanged(reason string) {
	burst.VupenObservatoryNetworkChanged(reason)
}

// ── Ядро 78: АВТО 4.1 (ADR-0001 §13). Те же имена и форматы — в DLL для Windows
// (build/template/main.gotemplate, префикс CGo). JSON — строкой как есть (UTF-8),
// не base64. Ошибок через границу нет: ошибка — текст или verdict "unknown".

// VupenSetUplink — платформа сообщает тип сети и подпись пути при каждой их смене
// (и один раз при старте ядра). kind: "cellular" | "wifi" | "wired" | "other" |
// "none"; cellular — только сотовый интерфейс без Wi-Fi/Ethernet (раздача по
// Wi-Fi — "wifi"; лимитный Wi-Fi — не cellular); Windows "cellular" не передаёт.
// sig — подпись пути (набор интерфейсов + имя; Android — Network id), "" = kind.
// Смена kind или sig — новая эпоха сети: соединения прежней сети закрываются
// через 9 с стабильной новой, перелив в LTE сбрасывается, пробы обсерватории
// ближайшие секунды не пишутся узлам (CR-105). Возвращает "" или текст ошибки
// (неизвестный kind — ничего не меняется).
func VupenSetUplink(kind string, sig string) string {
	return burst.VupenSetUplink(kind, sig)
}

// VupenSetCheckURL — файл проверки 24 КБ: основной и запасной (запасной — когда
// основной ответил не 200; "" — без запасного). Только https. Зовёт платформа
// при старте ядра; по умолчанию — Cloudflare 24 КБ. Возвращает "" или текст ошибки
// (адреса тогда не меняются).
func VupenSetCheckURL(primary string, fallback string) string {
	return burst.VupenSetCheckURL(primary, fallback)
}

// VupenAutoState — состояние АВТО 4.1, JSON:
//
//	{"v":1,"active":true,"detail":"","balancer":"REG",
//	 "uplink":"cellular","sig":"cellular:pdp_ip0","epoch":3,"stableForMs":12000,
//	 "tier":"regular|lte","phase":"new|checking|holding|searching|no-working",
//	 "regularAlive":7,"lteAlive":3,                       // живых по последнему поиску; -1 — поиска не было
//	 "sticky":{"since":0,"startVerdict":"ok|failed|unknown|none","searching":false,"nextSearchAtMs":0,
//	           "lastFailSignal":{"atMs":0,"kind":"dial|freeze|manual","detail":""}},
//	 "current":{"balancer":"REG|LTE","tag":"proxy-4","host":"1.2.3.4","port":443},
//	 "spilled":false,"noWorking":false,"failedChecks":0,
//	 "network":"up|down|unknown|",                        // сеть устройства по последней проверке
//	 "notice":"|searching|no_working|no_network|lte",     // какое сообщение показать (D-27, D-12, D-14 п. 4)
//	 "lastFailure":{"atMs":0,"kind":"check|verify|check-route","detail":""},
//	 "pendingClose":{"atMs":0,"reason":"net-change|lte-return","conns":0},
//	 "lastClose":{"atMs":0,"reason":"net-change|lte-return|check-route","conns":0},
//	 "lastSwitch":{"atMs":0,"from":"proxy-4","to":"proxy-7","reason":"check|search|spill|return"},
//	 "lastCheckRoute":{"atMs":0,"mode":"check|heal","verdict":"","from":"","to":"","reason":""},
//	 "spills":0,"returns":0,"spillBlocked":"",
//	 "netClass":"normal|whitelist|noNetwork|unknown|",    // ядро 82: класс сети по прямой пробе DNS (только сотовая)
//	 "netClassAtMs":0,                                    // время последней пробы, мс Unix UTC; 0 — не было
//	 "netClassHosts":"1.1.1.1=ok 8.8.8.8=fail ...",       // итог по адресам, коротко
//	 "netClassWhitelist":false}                           // перелив в LTE сделан из-за белых списков
//
// Время — мс Unix UTC, 0 — не было. Ядро не запущено или без АВТО 4.1 —
// "active":false и причина в "detail".
func VupenAutoState() string {
	return xray.VupenAutoState()
}

// VupenSetAppleViaVpnMode — настройка «Apple через VPN при белых списках»
// (T-084, ядро 81): правило конфига с ruleTag "vupen-apple-direct" пропускается
// и Apple идёт через VPN:
//
//	0 — никогда (Apple напрямую);
//	1 — пока АВТО 4.1 перелит в мобильную группу (как VupenSetAppleViaVpnOnLte(true));
//	2 — всегда («Вручную» выбран узел мобильной группы).
//
// Другое значение не меняет ничего (предупреждение в журнале). На процесс
// (переживает перезапуск ядра), звать в любой момент; ядро не перезапускается,
// открытые соединения не трогаются. Состояние — VupenAutoState: appleViaVpnMode,
// appleViaVpnOnLte (режим не 0), appleViaVpnActive (правило пропускается сейчас).
func VupenSetAppleViaVpnMode(mode int) {
	router.VupenSetAppleViaVpnMode(mode)
}

// VupenSetAppleViaVpnOnLte — прежний вызов (T-079): true → режим 1, false → 0.
func VupenSetAppleViaVpnOnLte(on bool) {
	router.VupenSetAppleViaVpnOnLte(on)
}

// VupenCheckRoute — проверка маршрута на переднем плане (ADR-0001 §5.1), синхронно:
// check ≤ 8 с (ничего не меняет), heal ≤ 15 с (после failed), switch ≤ 15 с (ядро 79–80,
// кнопка «Сменить сервер»: текущий не проверяется и не исключается, перебор по кругу
// внутри той же группы, без перелива в LTE и без раунда обсерватории; пока ядро
// само ищет узел — сразу busy_searching; сам поиск — не дольше 12 с, вызов целиком
// с ожиданием очереди ядра — не дольше 15 с). Запрос:
//
//	{"v":1,"mode":"check|heal|switch","balancer":"REG","url":"https://…","fallbackUrl":"https://…",
//	 "bytes":24576,"minStallMs":1500,"maxFirstByteMs":5000,"timeoutMs":6000,"maxSwitches":3,
//	 "excludeMin":15,"closeConnections":true,"reason":"foreground|after-connect|manual"}
//
// Все поля, кроме mode, необязательны (0 / "" — значения ядра и VupenSetCheckURL;
// closeConnections по умолчанию true). Ответ:
//
//	{"v":1,"verdict":"ok|failed|switched|no_alternative|busy_searching|network_down|no_working|spilled_lte|unknown",
//	 "from":"proxy-4","to":"proxy-7","tried":2,"ms":2310,"kind":"stall|nostart|error|http",
//	 "detail":"stalled at 16 KB of 24 (1.5s silence)","balancer":"REG|LTE","closed":3}
//
// failed — в check (узел не прошёл проверку) и в switch (ядро 80: вышли 12 с поиска, а
// непроверенные кандидаты остались; остались на from, to = from, в detail — сколько);
// no_alternative — только в switch (других рабочих нет, все кандидаты проверены;
// остались на from, to = from); busy_searching — только в switch (ядро ещё выбирает
// узел, второй поиск не начат; итог — VupenAutoState); ошибка запроса или «ядро не запущено» — verdict
// "unknown" с причиной в detail. Туннель и ядро не перезапускаются.
func VupenCheckRoute(req string) string {
	return xray.VupenCheckRoute(req)
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
	// T-075: num_gc / gc_cpu (молотит ли сборщик: два замера с паузой дают
	// частоту) и размеры кэшей транспортов на весь процесс (gRPC и XHTTP): рост
	// при неизменном числе узлов — утечка клиентов. Новые поля — только в конце.
	xhttpPool, xhttpClients := splithttp.VupenDialerPoolSize()
	return fmt.Sprintf(
		"heap_alloc=%.1fMB heap_sys=%.1fMB stack=%.1fMB heap_idle=%.1fMB heap_released=%.1fMB sys=%.1fMB go_limit_use=%.1fMB goroutines=%d"+
			" num_gc=%d gc_cpu=%.4f grpc_pool=%d xhttp_pool=%d xhttp_clients=%d",
		float64(ms.HeapAlloc)/mb,
		float64(ms.HeapSys)/mb,
		float64(ms.StackInuse)/mb,
		float64(ms.HeapIdle)/mb,
		float64(ms.HeapReleased)/mb,
		float64(ms.Sys)/mb,
		float64(ms.Sys-ms.HeapReleased)/mb,
		runtime.NumGoroutine(),
		ms.NumGC,
		ms.GCCPUFraction,
		grpctransport.TransportPoolSize(),
		xhttpPool,
		xhttpClients,
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
