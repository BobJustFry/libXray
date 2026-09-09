package share

import (
	"encoding/json"

	"github.com/xtls/xray-core/infra/conf"
)

// buildHy2FinalMask builds Hysteria2 QUIC hop / bandwidth / salamander mask (shared by URI and Clash).
//
// Vupen: since Xray-core v26.9.9 (#6327) port hopping is no longer
// QuicParams.UdpHop but a UDP mask of type "udphop" (conf.UDPHop). Upstream
// libXray answered that change by dropping Hysteria2 URI support altogether
// (#152); we keep it — a client may come with a hy2 subscription.
func buildHy2FinalMask(up, down, ports string, hopInterval *int32, obfsType, obfsPassword string) (*conf.FinalMask, error) {
	var quicParams *conf.QuicParamsConfig
	if up != "" || down != "" {
		quicParams = &conf.QuicParamsConfig{Congestion: "brutal"}
		if up != "" {
			quicParams.BrutalUp = conf.Bandwidth(up)
		}
		if down != "" {
			quicParams.BrutalDown = conf.Bandwidth(down)
		}
	}

	var udpMasks []conf.Mask
	if obfsType == "salamander" && obfsPassword != "" {
		obfs := conf.Mask{Type: "salamander"}
		salamander := &conf.Salamander{Password: obfsPassword}
		salamanderRawMessage, err := convertJsonToRawMessage(salamander)
		if err != nil {
			return nil, err
		}
		obfs.Settings = &salamanderRawMessage
		udpMasks = append(udpMasks, obfs)
	}
	if ports != "" {
		// Hysteria2 "ports"/"hop-interval" = hop to a random remote port every N
		// seconds: mode intervalRemote. Listed after salamander so it is the
		// outermost mask — the obfuscated packet gets its destination port last.
		hop := conf.UDPHop{Mode: "intervalRemote"}
		portListJSON, err := json.Marshal(ports)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(portListJSON, &hop.RemotePorts); err != nil {
			return nil, err
		}
		if hopInterval != nil {
			i := *hopInterval
			hop.Interval = conf.Int32Range{Left: i, Right: i, From: i, To: i}
		}
		hopRawMessage, err := convertJsonToRawMessage(&hop)
		if err != nil {
			return nil, err
		}
		udpMasks = append(udpMasks, conf.Mask{Type: "udphop", Settings: &hopRawMessage})
	}

	if quicParams == nil && len(udpMasks) == 0 {
		return nil, nil
	}
	return &conf.FinalMask{QuicParams: quicParams, Udp: udpMasks}, nil
}
