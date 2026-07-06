package xray

import (
	"fmt"

	corestats "github.com/xtls/xray-core/features/stats"
)

// QueryOutboundTrafficDelta reads and resets uplink/downlink counters for [tag].
func QueryOutboundTrafficDelta(tag string) (uplink int64, downlink int64) {
	if coreServer == nil || !coreServer.IsRunning() {
		return 0, 0
	}
	feat := coreServer.GetFeature(corestats.ManagerType())
	if feat == nil {
		return 0, 0
	}
	sm, ok := feat.(corestats.Manager)
	if !ok || sm == nil {
		return 0, 0
	}
	if c := sm.GetCounter(fmt.Sprintf("outbound>>>%s>>>traffic>>>uplink", tag)); c != nil {
		uplink = c.Set(0)
	}
	if c := sm.GetCounter(fmt.Sprintf("outbound>>>%s>>>traffic>>>downlink", tag)); c != nil {
		downlink = c.Set(0)
	}
	return uplink, downlink
}
