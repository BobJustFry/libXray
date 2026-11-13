package xray

// T-258: speedtest engine tests. Not copied into the aar build (build.ps1 copies the
// non-test files by name); run in a scratch copy of build_tmp with the fork replaced
// (offline recipe: docs/knowledge, agent memory fork-go-tooling-windows).

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/xtls/xray-core/core"
	coreserial "github.com/xtls/xray-core/infra/conf/serial"

	"github.com/gorilla/websocket"
)

// stServer is a local __down/__up/trace source with switchable misbehaviour.
type stServer struct {
	*httptest.Server
	traceStatus atomic.Int32 // 0 = 200
	pingDelay   atomic.Int64 // ns
	pingFail    atomic.Bool
	downMode    atomic.Int32 // 0 normal, 1 headers then hang, 2 some bytes then hang
}

func newStServer(t *testing.T) *stServer {
	s := &stServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/cdn-cgi/trace", func(w http.ResponseWriter, r *http.Request) {
		if c := s.traceStatus.Load(); c != 0 {
			w.WriteHeader(int(c))
			return
		}
		io.WriteString(w, "fl=1\nip=203.0.113.7\nloc=NL\n")
	})
	mux.HandleFunc("/__down", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.ParseInt(r.URL.Query().Get("bytes"), 10, 64)
		if n == 0 {
			if s.pingFail.Load() {
				select {
				case <-r.Context().Done():
				case <-time.After(5 * time.Second):
				}
				return
			}
			time.Sleep(time.Duration(s.pingDelay.Load()))
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		mode := s.downMode.Load()
		if mode == 1 {
			<-r.Context().Done()
			return
		}
		buf := make([]byte, 16<<10)
		var sent int64
		for sent < n {
			if mode == 2 && sent >= 64<<10 {
				<-r.Context().Done()
				return
			}
			k := int64(len(buf))
			if n-sent < k {
				k = n - sent
			}
			if _, err := w.Write(buf[:k]); err != nil {
				return
			}
			sent += k
			time.Sleep(time.Millisecond) // keep the rate finite and the counters moving
		}
	})
	mux.HandleFunc("/__up", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	})
	s.Server = httptest.NewTLSServer(mux)
	t.Cleanup(s.Close)
	return s
}

// useSource swaps the built-in allowlist for the local server for one test.
func (s *stServer) useSource(t *testing.T) {
	old := speedtestSources
	speedtestSources = []speedtestSource{{
		Name: "cloudflare", Down: s.URL + "/__down", Up: s.URL + "/__up", Trace: s.URL + "/cdn-cgi/trace",
	}}
	t.Cleanup(func() { speedtestSources = old })
}

func stTestTimings() speedtestTimings {
	return speedtestTimings{
		reach: time.Second, connect: 600 * time.Millisecond, stall: 600 * time.Millisecond,
		phaseCap: 3 * time.Second, runCap: 15 * time.Second, ping: 300 * time.Millisecond,
		lease: 10 * time.Second, leaseCheck: 50 * time.Millisecond, tick: 100 * time.Millisecond,
		downChunk: 4 << 20, upChunk: 1 << 20,
	}
}

// engineFor dials the local server directly (the core path is covered by the
// forced-tag test below).
func (s *stServer) engineFor(t speedtestTimings, dialErr bool) func(string) (*stEngine, string) {
	pool := s.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	addr := s.Listener.Addr().String()
	return func(string) (*stEngine, string) {
		return &stEngine{
			t:   t,
			tls: &tls.Config{RootCAs: pool, ServerName: "example.com"},
			dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				if dialErr {
					return nil, io.ErrUnexpectedEOF
				}
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		}, ""
	}
}

func stPollStatus(t *testing.T) stStatus {
	t.Helper()
	var st stStatus
	if err := json.Unmarshal([]byte(speedtestPoll()), &st); err != nil {
		t.Fatalf("poll json: %v", err)
	}
	return st
}

// stWait polls (renewing the lease) until the run finished or the limit passed.
func stWait(t *testing.T, limit time.Duration) stStatus {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if r := stCur.Load(); r != nil && r.isFinished() {
			// Poll only after the run is finished: a snapshot taken before could predate
			// the final state (T-264 CR-004).
			return stPollStatus(t)
		}
		stPollStatus(t) // renews the lease while running
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("run did not finish within %v: %+v", limit, stPollStatus(t))
	return stStatus{}
}

func stStart(t *testing.T, req string, mk func(string) (*stEngine, string)) {
	t.Helper()
	if out := speedtestStart(req, mk); out != `{"ok":true}` {
		t.Fatalf("start: %s", out)
	}
}

// stLeftovers lists goroutines of the engine and client-side HTTP connections.
func stLeftovers() []string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	var out []string
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(g, "libv2ray.(*stEngine)") || strings.Contains(g, "net/http.(*persistConn)") {
			out = append(out, g)
		}
	}
	return out
}

func stNoLeaks(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		left := stLeftovers()
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutine(s) left:\n%s", len(left), strings.Join(left, "\n\n"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

const stReq = `{"tag":"proxy","expect":"198.51.100.1:443","durationMs":1000,"pingCount":5}`

func TestSpeedtestFullRun(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	s.pingDelay.Store(int64(25 * time.Millisecond))
	stStart(t, stReq, s.engineFor(stTestTimings(), false))
	st := stWait(t, 10*time.Second)
	if st.State != stStateDone || st.Error != "" || st.Up.Unavailable || st.Down.Unavailable {
		t.Fatalf("state %q error %q unavailable %v/%v", st.State, st.Error, st.Down.Unavailable, st.Up.Unavailable)
	}
	if st.ExitIP != "203.0.113.7" || st.ExitLoc != "NL" || st.Tag != "proxy" || st.Expect != "198.51.100.1:443" {
		t.Fatalf("identity fields: %+v", st)
	}
	if st.Ping.Sent != 5 || st.Ping.Answered != 5 || st.Ping.Unavailable || st.Ping.MedianMs < 20 {
		t.Fatalf("ping: %+v", st.Ping)
	}
	for name, p := range map[string]stPhaseResult{"down": st.Down, "up": st.Up} {
		if len(p.Samples) != 10 || p.Bytes <= 0 || p.Mbps <= 0 || p.PeakMbps < p.Mbps {
			t.Fatalf("%s: samples=%d bytes=%d mbps=%v peak=%v", name, len(p.Samples), p.Bytes, p.Mbps, p.PeakMbps)
		}
		if want := stWindowMbps(p.Samples); p.Mbps != want {
			t.Fatalf("%s: mbps %v, window math %v", name, p.Mbps, want)
		}
	}
	if st.Down.Streams != 6 || st.Up.Streams != 4 {
		t.Fatalf("streams: down %d up %d", st.Down.Streams, st.Up.Streams)
	}
	stNoLeaks(t)
}

func TestSpeedtestWindowMath(t *testing.T) {
	// 1 s ramp-up of 1 MB is dropped; 9 x 1.25 MB per 1000 ms = 10 Mbit/s.
	s := []stSample{{Bytes: 1_000_000, Ms: 1000}}
	for i := 0; i < 9; i++ {
		s = append(s, stSample{Bytes: 1_250_000, Ms: 1000})
	}
	if got := stWindowMbps(s); got != 10 {
		t.Fatalf("window mbps %v, want 10", got)
	}
	if got := stWindowMbps(s[:1]); got != 8 {
		t.Fatalf("single sample %v, want 8", got)
	}
	if got := stMbps(125_000, 100*time.Millisecond); got != 10 {
		t.Fatalf("stMbps %v", got)
	}
}

func TestSpeedtestPingMedian(t *testing.T) {
	cases := []struct {
		in    []int64
		want  int64
		unavl bool
	}{
		{[]int64{50, 10, 30}, 30, false},
		{[]int64{40, 10, 30, 20}, 25, false},
		{[]int64{90, 10, 30, 20, 1000}, 30, false},
		{[]int64{10, 20}, -1, true},
		{nil, -1, true},
	}
	for _, c := range cases {
		got, u := stPingMedian(c.in)
		if got != c.want || u != c.unavl {
			t.Fatalf("median(%v) = %d,%v want %d,%v", c.in, got, u, c.want, c.unavl)
		}
	}
}

func TestSpeedtestPingUnavailableDoesNotFailRun(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	s.pingFail.Store(true)
	stStart(t, stReq, s.engineFor(stTestTimings(), false))
	st := stWait(t, 10*time.Second)
	if st.State != stStateDone || !st.Ping.Unavailable || st.Ping.MedianMs != -1 || st.Ping.Answered != 0 {
		t.Fatalf("state %q ping %+v", st.State, st.Ping)
	}
	stNoLeaks(t)
}

func TestSpeedtestBadURLAndRequest(t *testing.T) {
	mk := func(string) (*stEngine, string) { t.Fatal("engine must not be built"); return nil, "" }
	for _, req := range []string{
		`{"tag":"proxy","downUrl":"http://speed.cloudflare.com/__down"}`,
		`{"tag":"proxy","downUrl":"https://evil.example/__down"}`,
		`{"tag":"proxy","upUrl":"https://speed.cloudflare.com/__up?x=1"}`,
		`{"tag":"proxy","traceUrl":"https://user@speed.cloudflare.com/cdn-cgi/trace"}`,
		`{"tag":"proxy","source":"speedtest.example"}`,
		`{"tag":"proxy","source":"yandex","downUrl":"https://speed.cloudflare.com/__down"}`,
	} {
		if out := speedtestStart(req, mk); out != `{"error":"bad_url"}` {
			t.Fatalf("%s -> %s", req, out)
		}
	}
	if out := speedtestStart(`not json`, mk); out != `{"error":"bad_request"}` {
		t.Fatalf("bad json -> %s", out)
	}
	if out := speedtestStart(`{"tag":" "}`, mk); out != `{"error":"unknown_tag"}` {
		t.Fatalf("empty tag -> %s", out)
	}
	// The allowlisted URLs themselves are accepted (checked before any engine work).
	var src speedtestSource
	var ok bool
	if src, ok = stResolveSource(&speedtestRequest{DownURL: "https://speed.cloudflare.com/__down",
		UpURL: "https://speed.cloudflare.com/__up", TraceURL: "https://speed.cloudflare.com/cdn-cgi/trace"}); !ok {
		t.Fatalf("cloudflare URLs rejected: %+v", src)
	}
}

func TestSpeedtestBusy(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	stStart(t, stReq, s.engineFor(stTestTimings(), false))
	if out := speedtestStart(stReq, s.engineFor(stTestTimings(), false)); out != `{"error":"busy"}` {
		t.Fatalf("second start -> %s", out)
	}
	speedtestCancel(stErrCancelled)
	stWait(t, time.Second)
	stNoLeaks(t)
}

func TestSpeedtestSourceUnreachable(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	s.traceStatus.Store(http.StatusServiceUnavailable)
	stStart(t, stReq, s.engineFor(stTestTimings(), false))
	if st := stWait(t, 5*time.Second); st.State != stStateFailed || st.Error != stErrSourceUnreachable || st.Phase != stPhaseReach {
		t.Fatalf("got %q %q phase %q", st.State, st.Error, st.Phase)
	}
	stNoLeaks(t)
}

func TestSpeedtestDialFailureIsSourceUnreachable(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	stStart(t, stReq, s.engineFor(stTestTimings(), true))
	if st := stWait(t, 5*time.Second); st.Error != stErrSourceUnreachable {
		t.Fatalf("got %q %q", st.State, st.Error)
	}
}

func TestSpeedtestSlowStart(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	s.downMode.Store(1) // headers arrive, payload never does
	stStart(t, stReq, s.engineFor(stTestTimings(), false))
	if st := stWait(t, 5*time.Second); st.State != stStateFailed || st.Error != stErrSlowStart || st.Phase != stPhaseDown {
		t.Fatalf("got %q %q phase %q", st.State, st.Error, st.Phase)
	}
	stNoLeaks(t)
}

func TestSpeedtestStallIsNoConnection(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	s.downMode.Store(2) // 64 KB then silence
	stStart(t, stReq, s.engineFor(stTestTimings(), false))
	if st := stWait(t, 5*time.Second); st.State != stStateFailed || st.Error != stErrNoConnection || st.Phase != stPhaseDown {
		t.Fatalf("got %q %q phase %q", st.State, st.Error, st.Phase)
	}
	stNoLeaks(t)
}

func TestSpeedtestNothingConnectsIsNoConnection(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	tm := stTestTimings()
	mk := s.engineFor(tm, false)
	var calls atomic.Int32
	stStart(t, stReq, func(tag string) (*stEngine, string) {
		e, _ := mk(tag)
		inner := e.dial
		e.dial = func(ctx context.Context, n, a string) (net.Conn, error) {
			if calls.Add(1) > 2 { // reach + ping work, transfer dials fail
				return nil, io.ErrUnexpectedEOF
			}
			return inner(ctx, n, a)
		}
		return e, ""
	})
	if st := stWait(t, 5*time.Second); st.State != stStateFailed || st.Error != stErrNoConnection || st.Phase != stPhaseDown {
		t.Fatalf("got %q %q phase %q", st.State, st.Error, st.Phase)
	}
	stNoLeaks(t)
}

// The phase cap and the run cap have their own codes (T-264 CR-006), not no_connection.
func TestSpeedtestPhaseCapCode(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	tm := stTestTimings()
	tm.phaseCap = 300 * time.Millisecond // window is 1 s, data flows: the cap fires first
	stStart(t, stReq, s.engineFor(tm, false))
	if st := stWait(t, 5*time.Second); st.State != stStateFailed || st.Error != stErrPhaseCap || st.Phase != stPhaseDown {
		t.Fatalf("got %q %q phase %q", st.State, st.Error, st.Phase)
	}
	stNoLeaks(t)
}

func TestSpeedtestRunCapCode(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	tm := stTestTimings()
	tm.runCap = 400 * time.Millisecond // reach + ping fit, the 1 s download window does not
	stStart(t, stReq, s.engineFor(tm, false))
	if st := stWait(t, 5*time.Second); st.State != stStateFailed || st.Error != stErrRunCap {
		t.Fatalf("got %q %q phase %q", st.State, st.Error, st.Phase)
	}
	stNoLeaks(t)
}

func TestSpeedtestCancelUnderOneSecond(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	tm := stTestTimings()
	tm.tick = time.Second // production pacing, long phase
	stStart(t, `{"tag":"proxy","durationMs":10000}`, s.engineFor(tm, false))
	for stPollStatus(t).Phase != stPhaseDown {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond) // streams are transferring
	t0 := time.Now()
	speedtestCancel(stErrCancelled)
	if st := stPollStatus(t); st.State != stStateCancelled || st.Error != stErrCancelled {
		t.Fatalf("poll right after cancel: %q %q", st.State, st.Error)
	}
	<-stCur.Load().finished
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("cancel took %v", d)
	}
	stNoLeaks(t)
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("cancel + goroutine exit took %v", d)
	}
	// After the drain a new run may start.
	stStart(t, stReq, s.engineFor(stTestTimings(), false))
	speedtestCancel(stErrCancelled)
	stWait(t, time.Second)
	stNoLeaks(t)
}

func TestSpeedtestLeaseExpires(t *testing.T) {
	s := newStServer(t)
	s.useSource(t)
	tm := stTestTimings()
	tm.tick = time.Second
	tm.lease = 300 * time.Millisecond
	stStart(t, `{"tag":"proxy","durationMs":10000}`, s.engineFor(tm, false))
	t0 := time.Now()
	r := stCur.Load()
	select {
	case <-r.finished:
	case <-time.After(2 * time.Second):
		t.Fatal("run kept going without polls")
	}
	if d := time.Since(t0); d < 300*time.Millisecond {
		t.Fatalf("lease fired too early: %v", d)
	}
	if st := stPollStatus(t); st.State != stStateCancelled || st.Error != stErrLeaseExpired {
		t.Fatalf("got %q %q", st.State, st.Error)
	}
	stNoLeaks(t)
}

// --- the core wiring: forced outbound tag through a real in-process instance ---

// Default (first) outbound is blackhole; only the forced "proxy" (freedom) reaches the
// local server. A route rule sends everything to blackhole as well.
const stCoreCfg = `{
  "outbounds": [
    {"protocol": "blackhole", "tag": "block"},
    {"protocol": "freedom", "tag": "proxy", "settings": {"finalRules": [{"action": "allow", "ip": ["127.0.0.0/8"]}]}}
  ],
  "routing": {"rules": [{"type": "field", "network": "tcp,udp", "outboundTag": "block"}]}
}`

func stCoreInstance(t *testing.T) *core.Instance {
	cfg, err := coreserial.LoadJSONConfig(strings.NewReader(stCoreCfg))
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	inst, err := core.New(cfg)
	if err != nil {
		t.Fatalf("core.New: %v", err)
	}
	if err := inst.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { inst.Close() })
	return inst
}

func TestSpeedtestForcedTagThroughCore(t *testing.T) {
	inst := stCoreInstance(t)
	s := newStServer(t)
	addr := s.Listener.Addr().String()
	pool := s.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	get := func(tag string) error {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "example.com"},
			DialContext: func(ctx context.Context, n, _ string) (net.Conn, error) {
				return stForcedDialer(inst, tag)(ctx, n, addr)
			},
		}
		defer tr.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, s.URL+"/cdn-cgi/trace", nil)
		resp, err := (&http.Client{Transport: tr}).Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(b), "ip=203.0.113.7") {
			t.Fatalf("body %q", b)
		}
		return nil
	}
	if err := get("proxy"); err != nil {
		t.Fatalf("forced proxy tag: %v", err)
	}
	if err := get("block"); err == nil {
		t.Fatal("forced block tag reached the server: forcing is not applied")
	}

	stNoLeaks(t)
}

// --- source "yandex" (T-271): a local get-probes / echo / ping / 50mb / upload-ws ---

// yxServer fakes the Internetometer. Every URL of its probe list is dialed to this one
// server (engineFor ignores the address), so the hosts in the list only matter for the
// allowlist; the Host header shows which ones the engine actually requested.
type yxServer struct {
	*httptest.Server
	probes      atomic.Value // string: get-probes body; empty = default list
	echoStatus  atomic.Int32 // 0 = 200
	wsReject    atomic.Bool  // upload-ws answers 403
	wsDrop      atomic.Bool  // upload-ws drops the TCP connection during the handshake
	wsCloseMs   atomic.Int64 // >0: the server ends each session after this many ms
	wsAckForMs  atomic.Int64 // >0: acknowledgements stop after this many ms (session stays open)
	wsSessions  atomic.Int32
	hosts       sync.Map // Host header -> true
	badDuration atomic.Bool
}

const yxDefaultProbes = `{"mid":"m1","lid":["l1"],
 "latency":{"probes":[{"url":"http://a.cdn.yandex.net/pfx/ping?mid=m1"},{"url":"https://a.cdn.yandex.net/pfx/ping?mid=m1&lid=l1"}]},
 "download":{"probes":[
  {"url":"https://a.cdn.yandex.net/pfx/probes/50mb?lid=l1&mid=m1"},
  {"url":"https://a.cdn.yandex.net/pfx/probes/100kb?lid=l1&mid=m1"},
  {"url":"https://evil.example/pfx/probes/50mb?lid=l1&mid=m1"},
  {"url":"https://b.cdn.yandex.net:8443/pfx/probes/50mb?lid=l1&mid=m1"},
  {"url":"https://b.cdn.yandex.net/pfx/probes/50mb?lid=l1&mid=m1"}]},
 "upload":{"probes":[]}}`

func newYxServer(t *testing.T) *yxServer {
	s := &yxServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("/internet/api/v0/get-probes", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("from") != "internet" || r.URL.Query().Get("nocache") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, _ := s.probes.Load().(string)
		if body == "" {
			body = yxDefaultProbes
		}
		io.WriteString(w, body)
	})
	mux.HandleFunc("/api/v0/ip", func(w http.ResponseWriter, r *http.Request) {
		if c := s.echoStatus.Load(); c != 0 {
			w.WriteHeader(int(c))
			return
		}
		io.WriteString(w, `"203.0.113.9"`)
	})
	mux.HandleFunc("/pfx/ping", func(w http.ResponseWriter, r *http.Request) {
		s.hosts.Store(r.Host, true)
		time.Sleep(25 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/pfx/probes/50mb", func(w http.ResponseWriter, r *http.Request) {
		s.hosts.Store(r.Host, true)
		const n = 4 << 20
		w.Header().Set("Content-Length", strconv.Itoa(n))
		buf := make([]byte, 16<<10)
		for sent := 0; sent < n; sent += len(buf) {
			if _, err := w.Write(buf); err != nil {
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	mux.HandleFunc("/pfx/upload-ws", func(w http.ResponseWriter, r *http.Request) {
		s.hosts.Store(r.Host, true)
		q := r.URL.Query()
		if q.Get("mid") != "m1" || q.Get("type") != "upload" || q.Get("duration") != "3" {
			s.badDuration.Store(true)
		}
		if s.wsReject.Load() {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if s.wsDrop.Load() {
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					_ = conn.Close()
				}
			}
			return
		}
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		s.wsSessions.Add(1)
		var got atomic.Int64
		readerDone := make(chan struct{})
		go func() {
			defer close(readerDone)
			for {
				_, p, err := c.ReadMessage()
				if err != nil {
					return
				}
				got.Add(int64(len(p)))
			}
		}()
		end := time.Duration(1<<62 - 1)
		if ms := s.wsCloseMs.Load(); ms > 0 {
			end = time.Duration(ms) * time.Millisecond
		}
		stop := time.NewTimer(end)
		defer stop.Stop()
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		t0 := time.Now()
		for {
			select {
			case <-readerDone:
				return
			case <-stop.C:
				_ = c.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "disconnect timeout"), time.Now().Add(time.Second))
				return
			case <-tick.C:
				if ms := s.wsAckForMs.Load(); ms > 0 && time.Since(t0) > time.Duration(ms)*time.Millisecond {
					continue // the link "died" mid-upload: no more acknowledgements
				}
				msg := `{"k":"u","b":` + strconv.FormatInt(got.Swap(0), 10) + `,"i":50}`
				if c.WriteMessage(websocket.TextMessage, []byte(msg)) != nil {
					return
				}
			}
		}
	})
	s.Server = httptest.NewTLSServer(mux)
	t.Cleanup(s.Close)
	old := speedtestSources
	speedtestSources = []speedtestSource{old[0], {
		Name:      "yandex",
		Probes:    s.URL + "/internet/api/v0/get-probes",
		Echo:      s.URL + "/api/v0/ip",
		ProbeHost: ".cdn.yandex.net",
	}}
	t.Cleanup(func() { speedtestSources = old })
	return s
}

func (s *yxServer) engineFor(t speedtestTimings) func(string) (*stEngine, string) {
	return (&stServer{Server: s.Server}).engineFor(t, false)
}

// 1 s window -> the engine asks the server for a 3 s session (window + 2 s slack).
const yxReq = `{"tag":"proxy","expect":"198.51.100.1:443","source":"yandex","durationMs":1000,"pingCount":5}`

func TestSpeedtestYandexFullRun(t *testing.T) {
	s := newYxServer(t)
	stStart(t, yxReq, s.engineFor(stTestTimings()))
	st := stWait(t, 10*time.Second)
	if st.State != stStateDone || st.Error != "" || st.Source != "yandex" || st.Up.Unavailable {
		t.Fatalf("state %q error %q source %q up.unavailable %v", st.State, st.Error, st.Source, st.Up.Unavailable)
	}
	if st.ExitIP != "203.0.113.9" || st.ExitLoc != "" {
		t.Fatalf("exit %q %q", st.ExitIP, st.ExitLoc)
	}
	if st.Ping.Sent != 5 || st.Ping.Answered != 5 || st.Ping.Unavailable || st.Ping.MedianMs < 20 {
		t.Fatalf("ping: %+v", st.Ping)
	}
	for name, p := range map[string]stPhaseResult{"down": st.Down, "up": st.Up} {
		if len(p.Samples) != 10 || p.Bytes <= 0 || p.Mbps <= 0 || p.PeakMbps < p.Mbps {
			t.Fatalf("%s: samples=%d bytes=%d mbps=%v peak=%v", name, len(p.Samples), p.Bytes, p.Mbps, p.PeakMbps)
		}
	}
	if s.badDuration.Load() {
		t.Fatal("upload-ws query: want mid=m1, type=upload, duration=3")
	}
	var hosts []string
	s.hosts.Range(func(k, _ any) bool { hosts = append(hosts, k.(string)); return true })
	sort.Strings(hosts)
	if strings.Join(hosts, ",") != "a.cdn.yandex.net,b.cdn.yandex.net" {
		t.Fatalf("hosts requested: %v (only allowlisted :443 *.cdn.yandex.net expected)", hosts)
	}
	stNoLeaks(t)
}

// The server ends sessions inside the window: streams reconnect, the phase completes.
func TestSpeedtestYandexUploadReconnects(t *testing.T) {
	s := newYxServer(t)
	s.wsCloseMs.Store(300)
	stStart(t, yxReq, s.engineFor(stTestTimings()))
	st := stWait(t, 10*time.Second)
	if st.State != stStateDone || len(st.Up.Samples) != 10 || st.Up.Mbps <= 0 {
		t.Fatalf("state %q error %q up %+v", st.State, st.Error, st.Up)
	}
	if n := s.wsSessions.Load(); n <= 4 {
		t.Fatalf("sessions %d, want reconnects beyond the 4 streams", n)
	}
	stNoLeaks(t)
}

func TestSpeedtestYandexNoUsableProbesIsSourceUnreachable(t *testing.T) {
	s := newYxServer(t)
	s.probes.Store(`{"mid":"m1","latency":{"probes":[{"url":"https://evil.example/pfx/ping"}]},
 "download":{"probes":[{"url":"https://evil.example/pfx/probes/50mb"}]}}`)
	stStart(t, yxReq, s.engineFor(stTestTimings()))
	if st := stWait(t, 5*time.Second); st.State != stStateFailed || st.Error != stErrSourceUnreachable || st.Phase != stPhaseReach {
		t.Fatalf("got %q %q phase %q", st.State, st.Error, st.Phase)
	}
	stNoLeaks(t)
}

func TestSpeedtestYandexEchoFailureIsNotFatal(t *testing.T) {
	s := newYxServer(t)
	s.echoStatus.Store(http.StatusServiceUnavailable)
	stStart(t, yxReq, s.engineFor(stTestTimings()))
	if st := stWait(t, 10*time.Second); st.State != stStateDone || st.ExitIP != "" {
		t.Fatalf("got %q %q exit %q", st.State, st.Error, st.ExitIP)
	}
	stNoLeaks(t)
}

// stCheckUploadUnavailable: done + upload_unavailable, the download window kept, upload empty.
func stCheckUploadUnavailable(t *testing.T, st stStatus) {
	t.Helper()
	if st.State != stStateDone || st.Error != stErrUploadUnavailable || st.Phase != stPhaseUp || !st.Up.Unavailable {
		t.Fatalf("got %q %q phase %q up.unavailable %v", st.State, st.Error, st.Phase, st.Up.Unavailable)
	}
	if len(st.Down.Samples) != 10 || st.Down.Mbps <= 0 || st.Down.Unavailable {
		t.Fatalf("download lost: %+v", st.Down)
	}
	if len(st.Up.Samples) != 0 || st.Up.Bytes != 0 || st.Up.Mbps != 0 {
		t.Fatalf("upload not empty: %+v", st.Up)
	}
	if st.Ping.Answered != 5 {
		t.Fatalf("ping lost: %+v", st.Ping)
	}
	var raw map[string]any
	if json.Unmarshal([]byte(speedtestPoll()), &raw) != nil || raw["error"] != "upload_unavailable" ||
		raw["state"] != "done" || raw["up"].(map[string]any)["unavailable"] != true {
		t.Fatalf("wire shape: %v", raw)
	}
}

// T-283 CR-004: every upload-ws handshake refused (403) -> the run is done, the download
// stands, the upload is flagged upload_unavailable (was failed/no_connection).
func TestSpeedtestYandexUploadRefusedIsUploadUnavailable(t *testing.T) {
	s := newYxServer(t)
	s.wsReject.Store(true)
	stStart(t, yxReq, s.engineFor(stTestTimings()))
	stCheckUploadUnavailable(t, stWait(t, 10*time.Second))
	stNoLeaks(t)
}

// Every session ended by the server before any acknowledgement = upload_unavailable too.
func TestSpeedtestYandexUploadClosedBeforeDataIsUploadUnavailable(t *testing.T) {
	s := newYxServer(t)
	s.wsCloseMs.Store(1)
	stStart(t, yxReq, s.engineFor(stTestTimings()))
	stCheckUploadUnavailable(t, stWait(t, 10*time.Second))
	stNoLeaks(t)
}

// No mid in the probe list -> no upload URL -> no upload phase traffic, upload_unavailable.
func TestSpeedtestYandexNoMidIsUploadUnavailable(t *testing.T) {
	s := newYxServer(t)
	s.probes.Store(strings.Replace(yxDefaultProbes, `"mid":"m1",`, ``, 1))
	stStart(t, yxReq, s.engineFor(stTestTimings()))
	stCheckUploadUnavailable(t, stWait(t, 10*time.Second))
	if n := s.wsSessions.Load(); n != 0 {
		t.Fatalf("upload-ws sessions %d without mid", n)
	}
	stNoLeaks(t)
}

// A network failure during the handshake (connection dropped, not a refusal) stays an error.
func TestSpeedtestYandexUploadDroppedIsNoConnection(t *testing.T) {
	s := newYxServer(t)
	s.wsDrop.Store(true)
	stStart(t, yxReq, s.engineFor(stTestTimings()))
	if st := stWait(t, 10*time.Second); st.State != stStateFailed || st.Error != stErrNoConnection || st.Up.Unavailable {
		t.Fatalf("got %q %q up.unavailable %v", st.State, st.Error, st.Up.Unavailable)
	}
	stNoLeaks(t)
}

// Data flowed, then stopped mid-upload: still no_connection (unchanged by T-283).
func TestSpeedtestYandexUploadStallAfterDataIsNoConnection(t *testing.T) {
	s := newYxServer(t)
	s.wsAckForMs.Store(300)
	// 2 s window: the stall (600 ms after the last ack) fires well before the window ends.
	stStart(t, strings.Replace(yxReq, `"durationMs":1000`, `"durationMs":2000`, 1), s.engineFor(stTestTimings()))
	st := stWait(t, 10*time.Second)
	if st.State != stStateFailed || st.Error != stErrNoConnection || st.Up.Unavailable || st.Up.Bytes <= 0 {
		t.Fatalf("got %q %q up %+v", st.State, st.Error, st.Up)
	}
	stNoLeaks(t)
}

func TestSpeedtestYandexCancelUnderOneSecond(t *testing.T) {
	s := newYxServer(t)
	tm := stTestTimings()
	tm.tick = time.Second
	stStart(t, `{"tag":"proxy","source":"yandex","durationMs":10000}`, s.engineFor(tm))
	deadline := time.Now().Add(5 * time.Second)
	for stPollStatus(t).Phase != stPhaseDown {
		if time.Now().After(deadline) {
			t.Fatal("never reached down")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	t0 := time.Now()
	speedtestCancel(stErrCancelled)
	<-stCur.Load().finished
	stNoLeaks(t)
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("cancel + goroutine exit took %v", d)
	}
}

func TestSpeedtestYandexParseProbes(t *testing.T) {
	ping, down, ws, ok := stParseYandexProbes([]byte(yxDefaultProbes), ".cdn.yandex.net", 12)
	if !ok || ping != "https://a.cdn.yandex.net/pfx/ping?mid=m1&lid=l1" {
		t.Fatalf("ok %v ping %q", ok, ping)
	}
	if len(down) != 2 || !strings.HasPrefix(down[0], "https://a.cdn.yandex.net/") || !strings.HasPrefix(down[1], "https://b.cdn.yandex.net/") {
		t.Fatalf("down %v", down)
	}
	if len(ws) != 2 || ws[0] != "wss://a.cdn.yandex.net/pfx/upload-ws?duration=12&mid=m1&type=upload" {
		t.Fatalf("ws %v", ws)
	}
	// No mid -> usable without upload (T-283: upload_unavailable, not source_unreachable);
	// suffix must not match a bare "cdn.yandex.net" or a look-alike.
	if _, d, w, ok := stParseYandexProbes([]byte(`{"latency":{"probes":[{"url":"https://a.cdn.yandex.net/p/ping"}]},
 "download":{"probes":[{"url":"https://a.cdn.yandex.net/p/probes/50mb"}]}}`), ".cdn.yandex.net", 12); !ok || len(d) != 1 || len(w) != 0 {
		t.Fatalf("no mid: ok %v down %v ws %v", ok, d, w)
	}
	for _, u := range []string{"https://cdn.yandex.net/x", "https://a.cdn.yandex.net.evil.example/x",
		"https://acdn.yandex.net/x", "https://u@a.cdn.yandex.net/x", "wss://a.cdn.yandex.net/x"} {
		if _, ok := stProbeURL(u, ".cdn.yandex.net"); ok {
			t.Fatalf("%s accepted", u)
		}
	}
	if stParseEchoIP([]byte(`"203.0.113.9"`)) != "203.0.113.9" || stParseEchoIP([]byte(`{"ip":"2001:db8::1"}`)) != "2001:db8::1" ||
		stParseEchoIP([]byte(`"nope"`)) != "" || stParseEchoIP([]byte(`<html>`)) != "" {
		t.Fatal("echo parse")
	}
}

// T-276 CR-003: a server that ends every session right after the handshake does not
// cause a reconnect storm (each stream pauses 200 ms before the next dial).
func TestSpeedtestYandexUploadNoHandshakeStorm(t *testing.T) {
	s := newYxServer(t)
	s.wsCloseMs.Store(1)
	stStart(t, yxReq, s.engineFor(stTestTimings()))
	stWait(t, 10*time.Second)
	// 4 streams over an upload phase of at most phaseCap (3 s) at >= 200 ms per session.
	if n := s.wsSessions.Load(); n > 4*(3000/200+1) {
		t.Fatalf("sessions %d: reconnects without a pause", n)
	}
	stNoLeaks(t)
}

// T-276 CR-004: on a Yandex run redirects are followed only to https *.cdn.yandex.net.
func TestSpeedtestYandexRedirectAllowlist(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/off", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://evil.example.com/ok", http.StatusFound)
	})
	mux.HandleFunc("/plain", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://a.cdn.yandex.net/ok", http.StatusFound)
	})
	mux.HandleFunc("/on", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://b.cdn.yandex.net/ok", http.StatusFound)
	})
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") })
	srv := &stServer{Server: httptest.NewTLSServer(mux)}
	defer srv.Close()
	e, _ := srv.engineFor(stTestTimings(), false)("proxy")
	e.redirectSuffix = ".cdn.yandex.net"
	ctx := context.Background()
	if _, err := e.getSmall(ctx, "https://a.cdn.yandex.net/on", 64); err != nil {
		t.Fatalf("allowlisted redirect refused: %v", err)
	}
	for _, p := range []string{"/off", "/plain"} {
		if _, err := e.getSmall(ctx, "https://a.cdn.yandex.net"+p, 64); err == nil {
			t.Fatalf("%s: redirect off the allowlist followed", p)
		}
	}
	// Cloudflare runs keep the default policy.
	e.redirectSuffix = ""
	if _, err := e.getSmall(ctx, "https://a.cdn.yandex.net/off", 64); err != nil {
		t.Fatalf("default policy: %v", err)
	}
}
