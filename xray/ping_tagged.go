package xray

import (
	"context"
	"errors"
	stdnet "net"
	"net/http"
	"time"

	"github.com/xtls/libxray/nodep"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
)

// PingTaggedOutbound measures HTTP HEAD latency via a running Xray instance,
// forcing traffic through the outbound tag (observatory / tagged.Dialer pattern).
// Requires GetXrayState()==true; config must not enable metrics for ping tests.
func PingTaggedOutbound(timeout int, url string, outboundTag string) (int64, error) {
	if coreServer == nil || !coreServer.IsRunning() {
		return nodep.PingDelayError, errors.New("xray not running")
	}
	tag := outboundTag
	if tag == "" {
		return nodep.PingDelayError, errors.New("empty outbound tag")
	}
	if timeout < 1 {
		timeout = 5
	}
	if url == "" {
		url = "http://connectivitycheck.gstatic.com/generate_204"
	}

	httpTimeout := time.Second * time.Duration(timeout)
	tr := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(_ context.Context, network, addr string) (stdnet.Conn, error) {
			dest, err := xnet.ParseDestination(network + ":" + addr)
			if err != nil {
				return nil, err
			}
			ctx := session.SetForcedOutboundTagToContext(context.Background(), tag)
			return core.Dial(ctx, coreServer, dest)
		},
	}
	client := &http.Client{
		Transport: tr,
		Timeout:   httpTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return nodep.PingHTTPRequest(client, url, timeout)
}
