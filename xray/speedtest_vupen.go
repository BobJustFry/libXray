package xray

// Speedtest through the live core (T-258, option A of
// docs/project/plans/android-speedtest-design.md).
//
// The app's own sockets bypass the tunnel on Android (the package is excluded from the
// VPN), so the test runs here: a Go net/http client whose connections are core.Dial'ed
// into the running core with the outbound FORCED to the current node's tag
// (session.SetForcedOutboundTagToContext). The dispatcher takes the handler directly
// (isPickRoute = 1): routing rules, sniffing and the route memory are bypassed, the TUN
// stack is not part of the path. Result = node + channel, an upper bound for TUN apps.
//
// One run per process: reach (source check, 3 s) -> ping (warm-up + N small requests,
// 3 s each, median of the answered, < 3 answered = unavailable) -> download (N parallel
// GET __down) -> upload (N parallel POST __up). Each transfer phase: window of
// durationMs from the first byte, per-second samples, average Mbit/s over seconds
// 2..N (the first second is ramp-up), hard cap 15 s per phase, 60 s per run.
// Failure codes: no bytes within 5 s while connected = slow_start; nothing connected
// within 5 s or no bytes for 5 s after data flowed = no_connection; the 60 s run cap =
// run_cap; the 15 s cap of one transfer phase = phase_cap (both mean "the node was too
// slow / the phase never completed its window", not "no connection").
// Poll "error" is one of: no_connection, source_unreachable, slow_start, unknown_tag,
// bad_url, bad_request, busy, cancelled, lease_expired, run_cap, phase_cap (unknown future
// codes must be shown as a generic failure by the client).
// Not a failure (Lib v55, T-283): state "done" + error "upload_unavailable" + up.unavailable
// = the source offers no usable upload (Yandex: no mid in the probe list, or every
// upload-ws handshake refused / every session closed before any acknowledgement); the
// download (and ping) result stands, the upload is empty. Data that flowed and then
// stopped is still no_connection.
//
// No listening ports. Only https URLs of the built-in source list (not an open proxy
// for other code in the process). Poll/Cancel never take coreMutex (they must not wait
// behind a 15 s VupenCheckRoute); the run's own mutex guards in-memory status only.
// Lease: no Poll for 2 s -> the run cancels itself (Dart isolate gone, service alive).
// Memory: per stream one 32 KB read buffer (download) or the transport's 32 KB write
// buffer (upload) plus bufio 4 KB; bodies go to the counter, nothing is accumulated.
// Upload payload is one shared read-only 32 KB slab.
//
// Source "yandex" (T-271, Yandex Internetometer; protocol in
// docs/knowledge/speedtest-yandex-sources.md): reach = GET get-probes (3 s) -> probe URLs,
// accepted only as https on *.cdn.yandex.net (still not an open proxy); exit IP = the
// IPv4 echo (not fatal: an IPv6-only exit leaves it empty). Ping = GET of the first
// latency probe; download = N streams of GET probes/50mb spread over the CDN hosts;
// upload = N WebSocket streams (upload-ws, 64 KB binary frames, one shared random slab);
// the counted bytes are the server's acknowledgements {"k":"u","b":N} (bytes received in
// its last interval), so the measure is what reached the server. A stream reconnects
// when the server ends its session. Same states, codes, caps and lease as Cloudflare.

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corenet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	core "github.com/xtls/xray-core/core"

	"github.com/gorilla/websocket"
)

// Run states and phases reported by SpeedtestPoll.
const (
	stStateIdle      = "idle"
	stStateRunning   = "running"
	stStateDone      = "done"
	stStateFailed    = "failed"
	stStateCancelled = "cancelled"

	stPhaseReach = "reach"
	stPhasePing  = "ping"
	stPhaseDown  = "down"
	stPhaseUp    = "up"
)

// Error codes (SpeedtestStart "error" and SpeedtestPoll "error").
const (
	stErrNoConnection      = "no_connection"
	stErrSourceUnreachable = "source_unreachable"
	stErrSlowStart         = "slow_start"
	stErrUnknownTag        = "unknown_tag"
	stErrBadURL            = "bad_url"
	stErrBadRequest        = "bad_request"
	stErrBusy              = "busy"
	stErrCancelled         = "cancelled"
	stErrLeaseExpired      = "lease_expired"
	stErrRunCap            = "run_cap"   // whole run exceeded 60 s
	stErrPhaseCap          = "phase_cap" // one transfer phase exceeded 15 s
	// stErrUploadUnavailable comes with state "done" (not "failed"): download kept.
	stErrUploadUnavailable = "upload_unavailable"
)

// stCode is an error code used as a context cancel cause.
type stCode string

func (c stCode) Error() string { return string(c) }

// speedtestSource is one built-in source; requests may only name these URLs.
// Cloudflare: Down/Up/Trace. Yandex (Probes set): Probes/Echo; the measurement URLs come
// from the probe list and must be https on a host ending with ProbeHost.
type speedtestSource struct {
	Name  string
	Down  string // GET <Down>?bytes=N; bytes=0 is the ping request
	Up    string // POST <Up> with a body of N bytes
	Trace string // GET, small text "ip=...\nloc=..." (reachability + exit IP)

	Probes    string // GET <Probes>?nocache=<ms>&from=internet, JSON probe list (Yandex)
	Echo      string // GET, JSON "1.2.3.4" or {"ip":...}: the exit IP (Yandex, IPv4 only)
	ProbeHost string // allowed host suffix of the probe URLs, e.g. ".cdn.yandex.net"
}

var speedtestSources = []speedtestSource{{
	Name:  "cloudflare",
	Down:  "https://speed.cloudflare.com/__down",
	Up:    "https://speed.cloudflare.com/__up",
	Trace: "https://speed.cloudflare.com/cdn-cgi/trace",
}, {
	Name:      "yandex",
	Probes:    "https://yandex.ru/internet/api/v0/get-probes",
	Echo:      "https://ipv4-internet.yandex.net/api/v0/ip",
	ProbeHost: ".cdn.yandex.net",
}}

// speedtestTimings are the T-255 timeouts; tests shorten them.
type speedtestTimings struct {
	reach      time.Duration // source reachability (trace GET)
	connect    time.Duration // first byte of a transfer phase / TLS / response headers
	stall      time.Duration // no bytes after data flowed
	phaseCap   time.Duration // hard cap of one transfer phase
	runCap     time.Duration // hard cap of the whole run
	ping       time.Duration // one ping request
	lease      time.Duration // no Poll for this long -> cancel
	leaseCheck time.Duration
	tick       time.Duration // sample interval
	downChunk  int64         // bytes per GET __down (repeated until the window ends)
	upChunk    int64         // bytes per POST __up
}

var stDefaultTimings = speedtestTimings{
	reach:      3 * time.Second,
	connect:    5 * time.Second,
	stall:      5 * time.Second,
	phaseCap:   15 * time.Second,
	runCap:     60 * time.Second,
	ping:       3 * time.Second,
	lease:      2 * time.Second,
	leaseCheck: 250 * time.Millisecond,
	tick:       time.Second,
	downChunk:  25_000_000,
	upChunk:    10_000_000,
}

const stBufSize = 32 << 10

// stWsFrame is the Yandex upload frame (the size the Internetometer page sends).
const stWsFrame = 64 << 10

// stWsSlab is the shared read-only WebSocket payload, made on first use. The client
// masks a copy in the connection's write buffer; the slab itself is never written.
var stWsSlab = sync.OnceValue(func() []byte {
	b := make([]byte, stWsFrame)
	_, _ = rand.Read(b)
	return b
})

// stSlab is the shared read-only upload payload (random, so no layer can compress it).
var stSlab = func() []byte {
	b := make([]byte, stBufSize)
	_, _ = rand.Read(b)
	return b
}()

// speedtestRequest is the SpeedtestStart JSON.
type speedtestRequest struct {
	Tag         string `json:"tag"`
	Expect      string `json:"expect"`
	Source      string `json:"source"`
	DownURL     string `json:"downUrl"`
	UpURL       string `json:"upUrl"`
	TraceURL    string `json:"traceUrl"`
	DownStreams int    `json:"downStreams"`
	UpStreams   int    `json:"upStreams"`
	DurationMs  int    `json:"durationMs"`
	PingCount   int    `json:"pingCount"`
}

type stSample struct {
	Bytes int64   `json:"bytes"`
	Ms    int64   `json:"ms"`
	Mbps  float64 `json:"mbps"`
}

type stPhaseResult struct {
	Streams  int        `json:"streams"`
	Bytes    int64      `json:"bytes"`
	Samples  []stSample `json:"samples"`
	Mbps     float64    `json:"mbps"`
	PeakMbps float64    `json:"peakMbps"`
	WindowMs int64      `json:"windowMs"`
	// Unavailable: the source has no usable upload (up only, with error upload_unavailable).
	Unavailable bool `json:"unavailable"`
}

type stPingResult struct {
	Sent        int     `json:"sent"`
	Answered    int     `json:"answered"`
	SamplesMs   []int64 `json:"samplesMs"`
	MedianMs    int64   `json:"medianMs"`
	Unavailable bool    `json:"unavailable"`
}

type stStatus struct {
	State     string        `json:"state"`
	Phase     string        `json:"phase"`
	Error     string        `json:"error"`
	Tag       string        `json:"tag"`
	Expect    string        `json:"expect"`
	Source    string        `json:"source"`
	ElapsedMs int64         `json:"elapsedMs"`
	ExitIP    string        `json:"exitIp"`
	ExitLoc   string        `json:"exitLoc"`
	Ping      stPingResult  `json:"ping"`
	Down      stPhaseResult `json:"down"`
	Up        stPhaseResult `json:"up"`
}

type stRun struct {
	mu       sync.Mutex
	st       stStatus
	started  time.Time
	ended    time.Time
	lastPoll atomic.Int64 // unix nanos
	cancel   context.CancelCauseFunc
	finished chan struct{}
}

func (r *stRun) update(f func(s *stStatus)) {
	r.mu.Lock()
	f(&r.st)
	r.mu.Unlock()
}

func (r *stRun) snapshot() stStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.st
	s.Ping.SamplesMs = append([]int64(nil), s.Ping.SamplesMs...)
	s.Down.Samples = append([]stSample(nil), s.Down.Samples...)
	s.Up.Samples = append([]stSample(nil), s.Up.Samples...)
	end := r.ended
	if end.IsZero() {
		end = time.Now()
	}
	s.ElapsedMs = end.Sub(r.started).Milliseconds()
	return s
}

func (r *stRun) isFinished() bool {
	select {
	case <-r.finished:
		return true
	default:
		return false
	}
}

// stEngine does the network work; dial is core.Dial with the forced tag in production.
type stEngine struct {
	dial func(ctx context.Context, network, addr string) (net.Conn, error)
	tls  *tls.Config // nil = system roots
	t    speedtestTimings
	// redirectSuffix: when set (Yandex run), HTTP redirects are followed only to https
	// hosts under this suffix (stProbeURL), at most stMaxRedirects hops; others are refused.
	redirectSuffix string
}

const stMaxRedirects = 3

var (
	stStartMu sync.Mutex
	stCur     atomic.Pointer[stRun]
)

// stForcedDialer dials through the live core with the outbound forced to tag, like
// transport/internet/tagged/taggedimpl. The connection lives with lifeCtx (the phase),
// not with the single request that happened to dial it (keep-alive reuse).
func stForcedDialer(inst *core.Instance, tag string) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dest, err := corenet.ParseDestination(network + ":" + addr)
		if err != nil {
			return nil, err
		}
		ctx = session.ContextWithContent(ctx, &session.Content{SkipDNSResolve: true})
		ctx = session.SetForcedOutboundTagToContext(ctx, tag)
		return core.Dial(ctx, inst, dest)
	}
}

func stJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `{"error":"` + stErrBadRequest + `"}`
	}
	return string(b)
}

func stErrJSON(code string) string { return stJSON(map[string]string{"error": code}) }

func clampInt(v, def, lo, hi int) int {
	if v == 0 {
		v = def
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// stCheckURL: https, no query/fragment/userinfo, equal to the allowlisted URL.
func stCheckURL(given, allowed string) (string, bool) {
	if given == "" {
		return allowed, true
	}
	u, err := url.Parse(given)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Host == "" {
		return "", false
	}
	return allowed, given == allowed
}

func stResolveSource(req *speedtestRequest) (speedtestSource, bool) {
	name := req.Source
	if name == "" {
		name = "cloudflare"
	}
	for _, s := range speedtestSources {
		if s.Name != name {
			continue
		}
		var ok1, ok2, ok3 bool
		s.Down, ok1 = stCheckURL(req.DownURL, s.Down)
		s.Up, ok2 = stCheckURL(req.UpURL, s.Up)
		s.Trace, ok3 = stCheckURL(req.TraceURL, s.Trace)
		return s, ok1 && ok2 && ok3
	}
	return speedtestSource{}, false
}

func speedtestStart(reqJSON string, mk func(tag string) (*stEngine, string)) string {
	var req speedtestRequest
	if err := json.Unmarshal([]byte(reqJSON), &req); err != nil {
		return stErrJSON(stErrBadRequest)
	}
	src, ok := stResolveSource(&req)
	if !ok {
		return stErrJSON(stErrBadURL)
	}
	if strings.TrimSpace(req.Tag) == "" {
		return stErrJSON(stErrUnknownTag)
	}
	down := clampInt(req.DownStreams, 6, 1, 8)
	up := clampInt(req.UpStreams, 4, 1, 8)
	durMs := clampInt(req.DurationMs, 10000, 1000, 10000)
	pings := clampInt(req.PingCount, 5, 1, 10)

	stStartMu.Lock()
	defer stStartMu.Unlock()
	if r := stCur.Load(); r != nil && !r.isFinished() {
		return stErrJSON(stErrBusy)
	}
	eng, code := mk(req.Tag)
	if code != "" {
		return stErrJSON(code)
	}
	base, cancel := context.WithCancelCause(context.Background())
	r := &stRun{started: time.Now(), cancel: cancel, finished: make(chan struct{})}
	r.st = stStatus{
		State: stStateRunning, Phase: stPhaseReach, Tag: req.Tag, Expect: req.Expect, Source: src.Name,
		Ping: stPingResult{MedianMs: -1}, Down: stPhaseResult{Streams: down}, Up: stPhaseResult{Streams: up},
	}
	r.lastPoll.Store(time.Now().UnixNano())
	stCur.Store(r)
	go eng.run(base, r, src, down, up, time.Duration(durMs)*time.Millisecond, pings)
	return `{"ok":true}`
}

func speedtestPoll() string {
	r := stCur.Load()
	if r == nil {
		return stJSON(stStatus{State: stStateIdle, Ping: stPingResult{MedianMs: -1}})
	}
	r.lastPoll.Store(time.Now().UnixNano())
	return stJSON(r.snapshot())
}

// speedtestCancel marks a running run cancelled at once (Poll shows it immediately);
// Start answers busy until its goroutines are gone (< 1 s).
func speedtestCancel(code string) {
	r := stCur.Load()
	if r == nil || r.isFinished() {
		return
	}
	r.update(func(s *stStatus) {
		if s.State == stStateRunning {
			s.State, s.Error = stStateCancelled, code
		}
	})
	r.cancel(stCode(code))
}

func (e *stEngine) run(base context.Context, r *stRun, src speedtestSource, down, up int, dur time.Duration, pings int) {
	ctx, stopCap := context.WithTimeoutCause(base, e.t.runCap, stCode(stErrRunCap))
	defer stopCap()
	leaseDone := make(chan struct{})
	go e.leaseWatch(r, leaseDone)

	code := e.phases(ctx, r, src, down, up, dur, pings)

	close(leaseDone)
	r.update(func(s *stStatus) {
		if ctx.Err() != nil {
			code = stErrNoConnection
			var c stCode
			if errors.As(context.Cause(ctx), &c) {
				code = string(c)
			}
		}
		switch {
		case s.State != stStateRunning: // already cancelled by speedtestCancel
		case code == stErrCancelled || code == stErrLeaseExpired:
			s.State, s.Error = stStateCancelled, code
		case code == stErrUploadUnavailable:
			s.State, s.Error, s.Up.Unavailable = stStateDone, code, true
		case code != "":
			s.State, s.Error = stStateFailed, code
		default:
			s.State = stStateDone
		}
	})
	r.mu.Lock()
	r.ended = time.Now()
	r.mu.Unlock()
	r.cancel(nil)
	close(r.finished)
}

func (e *stEngine) leaseWatch(r *stRun, done <-chan struct{}) {
	t := time.NewTicker(e.t.leaseCheck)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			if time.Since(time.Unix(0, r.lastPoll.Load())) > e.t.lease {
				r.update(func(s *stStatus) {
					if s.State == stStateRunning {
						s.State, s.Error = stStateCancelled, stErrLeaseExpired
					}
				})
				r.cancel(stCode(stErrLeaseExpired))
				return
			}
		}
	}
}

// stWorker is one stream of a transfer phase; i is the stream number (0..streams-1).
type stWorker func(c context.Context, cl *http.Client, k *stConnTracker, ctr *stCounter, i int)

// stPlan is what a source measures with once it is reached.
type stPlan struct {
	ping string // full ping URL
	down stWorker
	up   stWorker
}

func (e *stEngine) phases(ctx context.Context, r *stRun, src speedtestSource, down, up int, dur time.Duration, pings int) string {
	var plan stPlan
	var ip, loc string
	var err error
	if src.Probes != "" {
		plan, ip, err = e.reachYandex(ctx, src, dur)
	} else {
		ip, loc, err = e.reach(ctx, src.Trace)
		downURL := src.Down + "?bytes=" + strconv.FormatInt(e.t.downChunk, 10)
		plan = stPlan{
			ping: src.Down + "?bytes=0",
			down: func(c context.Context, cl *http.Client, _ *stConnTracker, ctr *stCounter, _ int) {
				e.downStream(c, cl, downURL, ctr)
			},
			up: func(c context.Context, cl *http.Client, _ *stConnTracker, ctr *stCounter, _ int) {
				e.upStream(c, cl, src.Up, ctr)
			},
		}
	}
	if ctx.Err() != nil {
		return ""
	}
	if err != nil {
		return stErrSourceUnreachable
	}
	r.update(func(s *stStatus) { s.ExitIP, s.ExitLoc, s.Phase = ip, loc, stPhasePing })

	e.ping(ctx, r, plan.ping, pings)
	if ctx.Err() != nil {
		return ""
	}

	r.update(func(s *stStatus) { s.Phase = stPhaseDown })
	if code := e.measure(ctx, r, false, down, dur, plan.down); code != "" || ctx.Err() != nil {
		return code
	}

	return e.upPhase(ctx, r, up, dur, plan)
}

// upPhase: the upload phase. A plan without an upload worker (Yandex probe list without
// mid) or a phase whose every upload-ws handshake was refused / session closed before
// any acknowledgement = upload_unavailable (the run is done, the download stands).
// noinline: its name is the Lib v55 freshness marker of build_android_apk.ps1.
//
//go:noinline
func (e *stEngine) upPhase(ctx context.Context, r *stRun, up int, dur time.Duration, plan stPlan) string {
	r.update(func(s *stStatus) { s.Phase = stPhaseUp })
	if plan.up == nil {
		return stErrUploadUnavailable
	}
	return e.measure(ctx, r, true, up, dur, plan.up)
}

// stConnTracker closes every connection of a phase at its end, whatever state the
// transport left them in (a guarantee for the < 1 s cancel).
type stConnTracker struct {
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

func (k *stConnTracker) add(c net.Conn) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return false
	}
	if k.conns == nil {
		k.conns = map[net.Conn]struct{}{}
	}
	k.conns[c] = struct{}{}
	return true
}

func (k *stConnTracker) closeAll() {
	k.mu.Lock()
	k.closed = true
	conns := k.conns
	k.conns = nil
	k.mu.Unlock()
	for c := range conns {
		_ = c.Close()
	}
}

func (e *stEngine) client(life context.Context, k *stConnTracker, upload bool, conns int) (*http.Client, *http.Transport) {
	tr := &http.Transport{
		DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
			c, err := e.dial(life, network, addr)
			if err != nil {
				return nil, err
			}
			if !k.add(c) {
				_ = c.Close()
				return nil, context.Canceled
			}
			return c, nil
		},
		TLSHandshakeTimeout:   e.t.connect,
		ResponseHeaderTimeout: e.t.connect,
		DisableCompression:    true,
		MaxIdleConnsPerHost:   conns,
		IdleConnTimeout:       e.t.phaseCap,
	}
	if e.tls != nil {
		tr.TLSClientConfig = e.tls.Clone()
	}
	if upload {
		tr.WriteBufferSize = stBufSize
	}
	cl := &http.Client{Transport: tr}
	if sfx := e.redirectSuffix; sfx != "" {
		cl.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= stMaxRedirects {
				return errors.New("speedtest: too many redirects")
			}
			if _, ok := stProbeURL(req.URL.String(), sfx); !ok {
				return fmt.Errorf("speedtest: redirect to %q refused", req.URL.Host)
			}
			return nil
		}
	}
	return cl, tr
}

// reach: one GET of the trace URL within 3 s; returns the exit ip/loc it reports.
func (e *stEngine) reach(ctx context.Context, traceURL string) (string, string, error) {
	cctx, cancel := context.WithTimeout(ctx, e.t.reach)
	defer cancel()
	k := &stConnTracker{}
	cl, tr := e.client(cctx, k, false, 1)
	defer func() { tr.CloseIdleConnections(); k.closeAll() }()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, traceURL, nil)
	if err != nil {
		return "", "", err
	}
	resp, err := cl.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("trace status %d", resp.StatusCode)
	}
	var ip, loc string
	for _, line := range strings.Split(string(body), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "ip="); ok {
			ip = v
		} else if v, ok := strings.CutPrefix(strings.TrimSpace(line), "loc="); ok {
			loc = v
		}
	}
	return ip, loc, nil
}

// ping: warm-up + n small requests (Cloudflare __down?bytes=0, Yandex ping probe) on one
// keep-alive connection, time to the response headers, 3 s each; median of the answered;
// < 3 answered = unavailable.
func (e *stEngine) ping(ctx context.Context, r *stRun, pingURL string, n int) {
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	k := &stConnTracker{}
	cl, tr := e.client(pctx, k, false, 1)
	defer func() { tr.CloseIdleConnections(); k.closeAll() }()
	one := func() (time.Duration, bool) {
		rctx, rc := context.WithTimeout(pctx, e.t.ping)
		defer rc()
		req, err := http.NewRequestWithContext(rctx, http.MethodGet, pingURL, nil)
		if err != nil {
			return 0, false
		}
		t0 := time.Now()
		resp, err := cl.Do(req)
		if err != nil {
			return 0, false
		}
		d := time.Since(t0)
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
		return d, resp.StatusCode == http.StatusOK
	}
	one() // warm-up: TCP + TLS through the node, not counted
	var got []int64
	for i := 0; i < n && ctx.Err() == nil; i++ {
		d, ok := one()
		if ok {
			got = append(got, d.Milliseconds())
		}
		sent := i + 1
		r.update(func(s *stStatus) {
			s.Ping.Sent, s.Ping.Answered = sent, len(got)
			s.Ping.SamplesMs = append([]int64(nil), got...)
		})
	}
	med, unavailable := stPingMedian(got)
	r.update(func(s *stStatus) { s.Ping.MedianMs, s.Ping.Unavailable = med, unavailable })
}

// stPingMedian: median of the answered samples; fewer than 3 = unavailable (-1).
func stPingMedian(ms []int64) (int64, bool) {
	if len(ms) < 3 {
		return -1, true
	}
	s := append([]int64(nil), ms...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	m := len(s) / 2
	if len(s)%2 == 1 {
		return s[m], false
	}
	return (s[m-1] + s[m] + 1) / 2, false
}

type stCounter struct {
	bytes     atomic.Int64
	connected atomic.Bool
	// refused (upload-ws only): the server answered a handshake with a non-101 status or
	// ended a session before any acknowledgement. With no bytes at all by the connect
	// timeout the phase is upload_unavailable instead of no_connection / slow_start.
	refused atomic.Bool
}

// downStream repeats GET u (one chunk per request) until the phase ends.
func (e *stEngine) downStream(ctx context.Context, cl *http.Client, u string, ctr *stCounter) {
	buf := make([]byte, stBufSize)
	for ctx.Err() == nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return
		}
		resp, err := cl.Do(req)
		if err != nil {
			stPause(ctx, 200*time.Millisecond)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			stPause(ctx, 200*time.Millisecond)
			continue
		}
		ctr.connected.Store(true)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				ctr.bytes.Add(int64(n))
			}
			if err != nil {
				break
			}
		}
		resp.Body.Close()
	}
}

// stUpBody yields chunk bytes from the shared slab and counts them as they are handed
// to the transport (the standard upload measure; the stall check catches a full pipe).
type stUpBody struct {
	left int64
	ctr  *stCounter
}

func (b *stUpBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, io.EOF
	}
	b.ctr.connected.Store(true)
	n := len(p)
	if n > len(stSlab) {
		n = len(stSlab)
	}
	if int64(n) > b.left {
		n = int(b.left)
	}
	copy(p, stSlab[:n])
	b.left -= int64(n)
	b.ctr.bytes.Add(int64(n))
	return n, nil
}

func (e *stEngine) upStream(ctx context.Context, cl *http.Client, upURL string, ctr *stCounter) {
	for ctx.Err() == nil {
		body := &stUpBody{left: e.t.upChunk, ctr: ctr}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, upURL, body)
		if err != nil {
			return
		}
		req.ContentLength = e.t.upChunk
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := cl.Do(req)
		if err != nil {
			stPause(ctx, 200*time.Millisecond)
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
	}
}

func stPause(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func stMbps(bytes int64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return math.Round(float64(bytes)*8/d.Seconds()/1e6*100) / 100
}

// stWindowMbps: average over samples 2..N (the first one is ramp-up); one sample = itself.
func stWindowMbps(samples []stSample) float64 {
	if len(samples) == 0 {
		return 0
	}
	from := 1
	if len(samples) == 1 {
		from = 0
	}
	var b, ms int64
	for _, s := range samples[from:] {
		b += s.Bytes
		ms += s.Ms
	}
	return stMbps(b, time.Duration(ms)*time.Millisecond)
}

// measure runs one transfer phase: streams workers, per-tick samples, window of dur
// from the first byte. Returns "" when the window completed or the run was cancelled.
func (e *stEngine) measure(ctx context.Context, r *stRun, upload bool, streams int, dur time.Duration,
	worker stWorker) string {
	pctx, cancel := context.WithTimeoutCause(ctx, e.t.phaseCap, stCode(stErrPhaseCap))
	k := &stConnTracker{}
	cl, tr := e.client(pctx, k, upload, streams)
	ctr := &stCounter{}
	var wg sync.WaitGroup
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(pctx, cl, k, ctr, i)
		}()
	}
	defer func() {
		cancel()
		k.closeAll()
		tr.CloseIdleConnections()
		wg.Wait()
		tr.CloseIdleConnections()
	}()

	set := func(f func(p *stPhaseResult)) {
		r.update(func(s *stStatus) {
			if upload {
				f(&s.Up)
			} else {
				f(&s.Down)
			}
		})
	}
	want := int(dur / e.t.tick)
	if want < 1 {
		want = 1
	}
	start := time.Now()
	ticker := time.NewTicker(e.t.tick)
	defer ticker.Stop()
	var last int64
	var lastTick, lastChange time.Time
	started := false
	for {
		select {
		case <-pctx.Done():
			if ctx.Err() != nil {
				return ""
			}
			return stErrPhaseCap
		case now := <-ticker.C:
			total := ctr.bytes.Load()
			if !started {
				if total == 0 {
					if now.Sub(start) >= e.t.connect {
						if upload && ctr.refused.Load() {
							return stErrUploadUnavailable
						}
						if ctr.connected.Load() {
							return stErrSlowStart
						}
						return stErrNoConnection
					}
					lastTick = now
					continue
				}
				started, lastChange = true, now
				if lastTick.IsZero() {
					lastTick = start
				}
			}
			delta := total - last
			last = total
			if delta > 0 {
				lastChange = now
			} else if now.Sub(lastChange) >= e.t.stall {
				return stErrNoConnection
			}
			d := now.Sub(lastTick)
			lastTick = now
			smp := stSample{Bytes: delta, Ms: d.Milliseconds(), Mbps: stMbps(delta, d)}
			done := false
			set(func(p *stPhaseResult) {
				p.Samples = append(p.Samples, smp)
				p.Bytes = total
				p.WindowMs += smp.Ms
				p.Mbps = stWindowMbps(p.Samples)
				if smp.Mbps > p.PeakMbps {
					p.PeakMbps = smp.Mbps
				}
				done = len(p.Samples) >= want
			})
			if done {
				return ""
			}
		}
	}
}

// --- source "yandex" (T-271) ---

// stYandexMaxBody caps the get-probes answer (about 7.5 KB observed 2026-11-11).
const stYandexMaxBody = 256 << 10

// getSmall: one GET within the reach timeout, status 200, body up to limit bytes.
func (e *stEngine) getSmall(ctx context.Context, u string, limit int64) ([]byte, error) {
	cctx, cancel := context.WithTimeout(ctx, e.t.reach)
	defer cancel()
	k := &stConnTracker{}
	cl, tr := e.client(cctx, k, false, 1)
	defer func() { tr.CloseIdleConnections(); k.closeAll() }()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := cl.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return body, nil
}

// reachYandex: the probe list (3 s, failure = source_unreachable), then the exit IP from
// the echo (3 s, failure = empty IP, not an error: the echo is IPv4 only).
func (e *stEngine) reachYandex(ctx context.Context, src speedtestSource, dur time.Duration) (stPlan, string, error) {
	e.redirectSuffix = src.ProbeHost // before any client of this run is built
	body, err := e.getSmall(ctx, src.Probes+"?nocache="+strconv.FormatInt(time.Now().UnixMilli(), 10)+"&from=internet", stYandexMaxBody)
	if err != nil {
		return stPlan{}, "", err
	}
	secs := int(dur/time.Second) + 2 // the server ends a session at duration; slack past the window
	ping, down, ws, ok := stParseYandexProbes(body, src.ProbeHost, secs)
	if !ok {
		return stPlan{}, "", errors.New("yandex probes: no usable ping/download probe")
	}
	var ip string
	if b, err := e.getSmall(ctx, src.Echo, 4<<10); err == nil {
		ip = stParseEchoIP(b)
	}
	plan := stPlan{
		ping: ping,
		down: func(c context.Context, cl *http.Client, _ *stConnTracker, ctr *stCounter, i int) {
			e.downStream(c, cl, down[i%len(down)], ctr)
		},
	}
	if len(ws) > 0 { // no mid = no upload URL: upPhase answers upload_unavailable
		plan.up = func(c context.Context, _ *http.Client, k *stConnTracker, ctr *stCounter, i int) {
			e.wsUpStream(c, k, ws[i%len(ws)], ctr)
		}
	}
	return plan, ip, nil
}

// stProbeURL: an absolute https URL on a host ending with suffix (port empty or 443).
func stProbeURL(raw, suffix string) (*url.URL, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" || suffix == "" {
		return nil, false
	}
	h := strings.ToLower(u.Hostname())
	if len(h) <= len(suffix) || !strings.HasSuffix(h, suffix) {
		return nil, false
	}
	if p := u.Port(); p != "" && p != "443" {
		return nil, false
	}
	return u, true
}

// stParseYandexProbes: ping = first allowed latency probe; download = allowed probes
// ending with /50mb (one per CDN host); upload = wss://<host>/<prefix>/upload-ws?mid=..
// &type=upload&duration=<secs> on the download hosts. ok = ping and download are non-empty;
// ws may be empty (no mid): the run then measures no upload (upload_unavailable).
func stParseYandexProbes(body []byte, suffix string, secs int) (ping string, down, ws []string, ok bool) {
	type probes struct {
		Probes []struct {
			URL string `json:"url"`
		} `json:"probes"`
	}
	var j struct {
		Mid      json.RawMessage `json:"mid"`
		Latency  probes          `json:"latency"`
		Download probes          `json:"download"`
	}
	if json.Unmarshal(body, &j) != nil {
		return "", nil, nil, false
	}
	for _, p := range j.Latency.Probes {
		if _, good := stProbeURL(p.URL, suffix); good {
			ping = p.URL
			break
		}
	}
	var mid string
	_ = json.Unmarshal(j.Mid, &mid)
	for _, p := range j.Download.Probes {
		u, good := stProbeURL(p.URL, suffix)
		if !good || !strings.HasSuffix(u.Path, "/50mb") {
			continue
		}
		down = append(down, p.URL)
		i := strings.Index(u.Path, "/probes/")
		if mid == "" || i < 0 {
			continue
		}
		ws = append(ws, (&url.URL{
			Scheme: "wss",
			Host:   u.Host,
			Path:   u.Path[:i] + "/upload-ws",
			RawQuery: url.Values{
				"mid": {mid}, "type": {"upload"}, "duration": {strconv.Itoa(secs)},
			}.Encode(),
		}).String())
	}
	return ping, down, ws, ping != "" && len(down) > 0
}

// stParseEchoIP: JSON string "1.2.3.4" or {"ip":"1.2.3.4"}; anything else = "".
func stParseEchoIP(body []byte) string {
	var s string
	if json.Unmarshal(body, &s) != nil {
		var o struct {
			IP string `json:"ip"`
		}
		if json.Unmarshal(body, &o) != nil {
			return ""
		}
		s = o.IP
	}
	ip := net.ParseIP(strings.TrimSpace(s))
	if ip == nil {
		return ""
	}
	return ip.String()
}

// wsUpStream: WebSocket upload sessions to wsURL until the phase ends; a session the
// server closes (at its duration) is reopened. Dials go through e.dial like the HTTP
// streams and are registered in the phase tracker (closed at the phase end).
func (e *stEngine) wsUpStream(ctx context.Context, k *stConnTracker, wsURL string, ctr *stCounter) {
	d := &websocket.Dialer{
		NetDialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
			c, err := e.dial(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if !k.add(c) {
				_ = c.Close()
				return nil, context.Canceled
			}
			return c, nil
		},
		HandshakeTimeout: e.t.connect,
		ReadBufferSize:   4 << 10,
		WriteBufferSize:  stWsFrame, // one frame per message (the header is added on top)
	}
	if e.tls != nil {
		d.TLSClientConfig = e.tls.Clone()
	}
	for ctx.Err() == nil {
		c, resp, err := d.DialContext(ctx, wsURL, nil)
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		if err != nil {
			// A non-101 answer (gorilla: ErrBadHandshake) is the server refusing the upload;
			// a dial / TLS / network error is not (it stays no_connection).
			if errors.Is(err, websocket.ErrBadHandshake) && ctx.Err() == nil {
				ctr.refused.Store(true)
			}
			stPause(ctx, 200*time.Millisecond)
			continue
		}
		ctr.connected.Store(true)
		if !e.wsSession(ctx, c, ctr) && ctx.Err() == nil {
			ctr.refused.Store(true) // the server ended the session before any acknowledgement
		}
		// The server may end a session right after the handshake: no TLS handshake storm.
		stPause(ctx, 200*time.Millisecond)
	}
}

// wsSession: the writer sends the slab as binary frames; the reader adds the server's
// acknowledged bytes {"k":"u","b":N} to the counter. Either side ending closes the conn.
// Returns whether the server acknowledged any bytes in this session.
func (e *stEngine) wsSession(ctx context.Context, c *websocket.Conn, ctr *stCounter) bool {
	c.SetReadLimit(64 << 10)
	var acked atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer c.Close()
		for {
			mt, p, err := c.ReadMessage()
			if err != nil {
				return
			}
			if mt != websocket.TextMessage {
				continue
			}
			var m struct {
				K string `json:"k"`
				B int64  `json:"b"`
			}
			if json.Unmarshal(p, &m) == nil && m.K == "u" && m.B > 0 {
				ctr.bytes.Add(m.B)
				acked.Store(true)
			}
		}
	}()
	slab := stWsSlab()
	for ctx.Err() == nil {
		if err := c.WriteMessage(websocket.BinaryMessage, slab); err != nil {
			break
		}
	}
	c.Close()
	<-done
	return acked.Load()
}
