package xray

// Tests of the libXray adapter (speedtest_vupen_ctl.go) and of the drift guard. The engine
// tests are speedtest_vupen_test.go (generated copy of the Android ones).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// withRunningCore makes inst the "running" core as RunXray does, restoring after the test.
func withRunningCore(t *testing.T) {
	t.Helper()
	inst := stCoreInstance(t)
	coreServer = inst
	coreServerNow.Store(inst)
	t.Cleanup(func() {
		speedtestAbort(stErrNoConnection, time.Second)
		coreServerNow.Store(nil)
		coreServer = nil
	})
}

func TestSpeedtestAPINoCoreIsNoConnection(t *testing.T) {
	if out := SpeedtestStart(`{"tag":"proxy"}`); out != `{"error":"no_connection"}` {
		t.Fatalf("no core -> %s", out)
	}
	SpeedtestCancel() // idle: no-op
	SpeedtestAbortMemory()
	if st := stPollStatus(t); st.State != stStateIdle {
		t.Fatalf("idle poll: %+v", st)
	}
}

func TestSpeedtestAPIUnknownTagAndBadRequest(t *testing.T) {
	withRunningCore(t)
	if out := SpeedtestStart(`{"tag":"node-missing"}`); out != `{"error":"unknown_tag"}` {
		t.Fatalf("unknown tag -> %s", out)
	}
	if out := SpeedtestStart(`not json`); out != `{"error":"bad_request"}` {
		t.Fatalf("bad json -> %s", out)
	}
}

// A run forced to the blackhole outbound hangs in reach without touching the network, so it
// is still running when we stop it: that checks cancel / memory abort / StopXray.
func startHangingRun(t *testing.T) {
	t.Helper()
	if out := SpeedtestStart(`{"tag":"block"}`); out != `{"ok":true}` {
		t.Fatalf("start -> %s", out)
	}
	if out := SpeedtestStart(`{"tag":"block"}`); out != `{"error":"busy"}` {
		t.Fatalf("second start -> %s", out)
	}
}

func TestSpeedtestAPICancel(t *testing.T) {
	withRunningCore(t)
	startHangingRun(t)
	t0 := time.Now()
	SpeedtestCancel()
	if st := stPollStatus(t); st.State != stStateCancelled || st.Error != stErrCancelled {
		t.Fatalf("after cancel: %+v", st)
	}
	<-stCur.Load().finished
	if d := time.Since(t0); d > time.Second {
		t.Fatalf("cancel took %v", d)
	}
	stNoLeaks(t)
}

func TestSpeedtestAPIAbortMemory(t *testing.T) {
	withRunningCore(t)
	startHangingRun(t)
	SpeedtestAbortMemory()
	<-stCur.Load().finished
	if st := stPollStatus(t); st.State != stStateFailed || st.Error != "memory" {
		t.Fatalf("after memory abort: %+v", st)
	}
	stNoLeaks(t)
}

// StopXray ends a running test as failed/no_connection before closing the instance.
func TestSpeedtestStopXrayEndsRun(t *testing.T) {
	withRunningCore(t)
	startHangingRun(t)
	t0 := time.Now()
	if err := StopXray(); err != nil {
		t.Fatalf("StopXray: %v", err)
	}
	if r := stCur.Load(); !r.isFinished() {
		t.Fatal("run still active after StopXray")
	}
	if d := time.Since(t0); d > 2*time.Second {
		t.Fatalf("StopXray took %v", d)
	}
	if st := stPollStatus(t); st.State != stStateFailed || st.Error != stErrNoConnection {
		t.Fatalf("after StopXray: %+v", st)
	}
	if out := SpeedtestStart(`{"tag":"block"}`); out != `{"error":"no_connection"}` {
		t.Fatalf("start after stop -> %s", out)
	}
	stNoLeaks(t)
}

// CR-001: a SpeedtestStart queued on stStartMu while StopXray runs must not start a run on
// the instance StopXray is closing. Interleaving: the test holds stStartMu, Start blocks on
// it, StopXray unpublishes the core and then blocks in speedtestCoreStopping; releasing the
// lock lets either go first, and both orders must end with no_connection and no live run
// (Start first: it reads the already-unpublished core inside the lock; Stop first: nothing
// to abort, Start then reads nil). If Start read the core before taking the lock, it would
// get the old instance and return ok with a run nobody aborts.
func TestSpeedtestStartRacingStopXrayGetsNoConnection(t *testing.T) {
	withRunningCore(t)
	stStartMu.Lock()
	locked := true
	defer func() {
		if locked {
			stStartMu.Unlock()
		}
	}()

	startRes := make(chan string, 1)
	go func() { startRes <- SpeedtestStart(`{"tag":"block"}`) }()
	time.Sleep(100 * time.Millisecond) // Start is parked on stStartMu (a late goroutine only weakens the test)

	stopRes := make(chan error, 1)
	go func() { stopRes <- StopXray() }()
	deadline := time.Now().Add(5 * time.Second)
	for runningCore() != nil { // StopXray's first step; it then waits for stStartMu
		if time.Now().After(deadline) {
			t.Fatal("StopXray did not unpublish the core")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-stopRes:
		t.Fatalf("StopXray finished while stStartMu was held (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
	}

	stStartMu.Unlock()
	locked = false
	if out := <-startRes; out != `{"error":"no_connection"}` {
		t.Fatalf("Start racing StopXray -> %s", out)
	}
	if err := <-stopRes; err != nil {
		t.Fatalf("StopXray: %v", err)
	}
	if r := stCur.Load(); r != nil && !r.isFinished() {
		t.Fatal("a run is still active after StopXray")
	}
	stNoLeaks(t)
}

// Drift guard: speedtest_vupen*.go must equal the Android sources except for the package
// clause (scripts/sync_speedtest_engine.ps1). Skipped when the fork is built standalone.
func TestSpeedtestEngineMatchesAndroid(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var androidDir string
	for d := dir; ; d = filepath.Dir(d) {
		c := filepath.Join(d, "android", "libv2ray-patch")
		if _, err := os.Stat(filepath.Join(c, "libv2ray_speedtest.go")); err == nil {
			androidDir = c
			break
		}
		if filepath.Dir(d) == d {
			t.Skip("android/libv2ray-patch not found (fork built standalone)")
		}
	}
	norm := func(p string) string {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		s := strings.ReplaceAll(string(b), "\r", "")
		i := strings.Index(s, "\n")
		if !strings.HasPrefix(s, "package ") || i < 0 {
			t.Fatalf("%s: no package clause", p)
		}
		return s[i:]
	}
	for _, c := range [][2]string{
		{"libv2ray_speedtest.go", "speedtest_vupen.go"},
		{"libv2ray_speedtest_test.go", "speedtest_vupen_test.go"},
	} {
		if norm(filepath.Join(androidDir, c[0])) != norm(c[1]) {
			t.Errorf("%s drifted from android/libv2ray-patch/%s: run scripts/sync_speedtest_engine.ps1", c[1], c[0])
		}
	}
}

// RunXrayFromJSON replaces the core instance: a run dialing through the old one ends as
// failed/no_connection before that (T-302). The old instance is closed by the test (RunXray*
// itself never closes it).
func TestSpeedtestRunXrayFromJSONEndsRun(t *testing.T) {
	dir := t.TempDir()
	if err := RunXrayFromJSON(dir, "", stCoreCfg); err != nil {
		t.Fatalf("RunXrayFromJSON: %v", err)
	}
	first := coreServer
	t.Cleanup(func() {
		_ = StopXray()
		if first != nil {
			_ = first.Close()
		}
	})
	startHangingRun(t)
	if err := RunXrayFromJSON(dir, "", stCoreCfg); err != nil {
		t.Fatalf("second RunXrayFromJSON: %v", err)
	}
	if r := stCur.Load(); !r.isFinished() {
		t.Fatal("run still active after RunXrayFromJSON")
	}
	if st := stPollStatus(t); st.State != stStateFailed || st.Error != stErrNoConnection {
		t.Fatalf("after RunXrayFromJSON: %+v", st)
	}
	// the new instance is usable straight away
	startHangingRun(t)
	SpeedtestCancel()
	<-stCur.Load().finished
	stNoLeaks(t)
}
