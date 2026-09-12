package xray

import (
	"strings"

	corestats "github.com/xtls/xray-core/features/stats"
)

// vupenTrafficOutboundCounted — считать ли байты outbound'а трафиком туннеля.
//
// Служебные outbound'ы (freedom/blackhole/dns, наши `block*` и пробы `probe_*`)
// не считаются. Всё остальное — узлы подписки под любыми именами: имена
// (`proxy`, `proxy-2`, …) даёт панель, а не мы, и чужая подписка назовёт их
// иначе. То же правило — в Dart (`traffic_outbound_tags.dart`, Windows) и в
// Kotlin (`VupenXrayController`, Android).
func vupenTrafficOutboundCounted(tag string) bool {
	t := strings.ToLower(strings.TrimSpace(tag))
	if t == "" {
		return false
	}
	switch t {
	case "freedom", "blackhole", "dns", "direct", "block", "selector", "urltest":
		return false
	}
	if strings.HasPrefix(t, "block") || strings.HasPrefix(t, "probe_") {
		return false
	}
	return true
}

// QueryTrafficTotals — накопленный uplink/downlink всех outbound'ов, кроме
// служебных, с момента старта этого экземпляра ядра. Счётчики НЕ обнуляются:
// приложение показывает число как есть и ничего не хранит, поэтому час в фоне
// или выгрузка приложения ничего не теряют. Балансировщик перекладывает трафик
// с узла на узел — сумма по всем outbound'ам этого не замечает.
func QueryTrafficTotals() (uplink int64, downlink int64) {
	s := coreServer
	if s == nil || !s.IsRunning() {
		return 0, 0
	}
	feat := s.GetFeature(corestats.ManagerType())
	if feat == nil {
		return 0, 0
	}
	sm, ok := feat.(corestats.Manager)
	if !ok || sm == nil {
		return 0, 0
	}
	sm.VisitCounters(func(name string, c corestats.Counter) bool {
		// outbound>>>TAG>>>traffic>>>uplink|downlink
		parts := strings.Split(name, ">>>")
		if len(parts) != 4 || parts[0] != "outbound" || parts[2] != "traffic" {
			return true
		}
		if !vupenTrafficOutboundCounted(parts[1]) {
			return true
		}
		switch parts[3] {
		case "uplink":
			uplink += c.Value()
		case "downlink":
			downlink += c.Value()
		}
		return true
	})
	return uplink, downlink
}
