package xray

import "testing"

func TestVupenTrafficOutboundCounted(t *testing.T) {
	counted := []string{"proxy", "proxy-27", "vless-de", "hy2-nl", " Proxy-3 "}
	for _, tag := range counted {
		if !vupenTrafficOutboundCounted(tag) {
			t.Errorf("%q must be counted", tag)
		}
	}
	service := []string{"", "direct", "block", "block_rst", "BLOCK-udp", "dns", "freedom", "blackhole", "probe_1", "probe_bal_x", "selector", "urltest"}
	for _, tag := range service {
		if vupenTrafficOutboundCounted(tag) {
			t.Errorf("%q must not be counted", tag)
		}
	}
}
