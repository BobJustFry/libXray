package share

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xtls/xray-core/infra/conf"
)

// Vupen: since Xray-core v26.9.9 port hopping is a UDP mask ("udphop"), not
// QuicParams.UdpHop. Tests look it up by type instead of by index.
func findUDPMask(masks []conf.Mask, typ string) *conf.Mask {
	for i := range masks {
		if strings.EqualFold(masks[i].Type, typ) {
			return &masks[i]
		}
	}
	return nil
}

func requireUDPHopMask(t *testing.T, masks []conf.Mask) conf.UDPHop {
	t.Helper()
	m := findUDPMask(masks, "udphop")
	require.NotNil(t, m, "udphop mask")
	require.NotNil(t, m.Settings)
	var hop conf.UDPHop
	require.NoError(t, json.Unmarshal(*m.Settings, &hop))
	return hop
}
