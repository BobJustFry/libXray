package libXray

// The root (gomobile / CGo) face of the in-core speedtest: Start / Poll / Cancel / AbortMemory
// through a real in-process core (T-302). The engine itself is tested in ./xray.

import (
	"encoding/json"
	"runtime"
	"testing"
	"time"

	"github.com/xtls/libxray/xray"
)

// blackhole is the first outbound, so a run forced to it hangs in reach without any network.
const stRootCfg = `{
  "outbounds": [
    {"protocol": "blackhole", "tag": "block"},
    {"protocol": "freedom", "tag": "proxy"}
  ]
}`

type stRootStatus struct {
	State string `json:"state"`
	Error string `json:"error"`
}

func stRootPoll(t *testing.T) stRootStatus {
	t.Helper()
	var st stRootStatus
	if err := json.Unmarshal([]byte(SpeedtestPoll()), &st); err != nil {
		t.Fatalf("poll json: %v", err)
	}
	return st
}

func stRootWait(t *testing.T, want string) stRootStatus {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		st := stRootPoll(t)
		if st.State == want || time.Now().After(deadline) {
			return st
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// stRootStart starts a run; a just-cancelled run keeps the slot ("busy") until its
// goroutines are gone (<1 s), exactly as on Android, so retry briefly.
func stRootStart(t *testing.T, req string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		out := SpeedtestStart(req)
		if out == `{"ok":true}` {
			return
		}
		if out != `{"error":"busy"}` || time.Now().After(deadline) {
			t.Fatalf("start -> %s", out)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSpeedtestRootNoCore(t *testing.T) {
	if out := SpeedtestStart(`{"tag":"proxy"}`); out != `{"error":"no_connection"}` {
		t.Fatalf("no core -> %s", out)
	}
	SpeedtestCancel()
	SpeedtestAbortMemory()
	if st := stRootPoll(t); st.State != "idle" {
		t.Fatalf("idle poll: %+v", st)
	}
}

func TestSpeedtestRootStartPollCancelNoLeak(t *testing.T) {
	if err := xray.RunXrayFromJSON(t.TempDir(), "", stRootCfg); err != nil {
		t.Fatalf("RunXrayFromJSON: %v", err)
	}
	t.Cleanup(func() { _ = xray.StopXray() })

	if out := SpeedtestStart(`{"tag":"nope"}`); out != `{"error":"unknown_tag"}` {
		t.Fatalf("unknown tag -> %s", out)
	}
	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		stRootStart(t, `{"tag":"block","downStreams":2,"upStreams":1}`)
		if out := SpeedtestStart(`{"tag":"block"}`); out != `{"error":"busy"}` {
			t.Fatalf("second start -> %s", out)
		}
		if st := stRootPoll(t); st.State != "running" {
			t.Fatalf("poll after start: %+v", st)
		}
		SpeedtestCancel()
		if st := stRootWait(t, "cancelled"); st.State != "cancelled" || st.Error != "cancelled" {
			t.Fatalf("after cancel: %+v", st)
		}
	}
	// memory guard hook
	stRootStart(t, `{"tag":"block"}`)
	SpeedtestAbortMemory()
	if st := stRootWait(t, "failed"); st.State != "failed" || st.Error != "memory" {
		t.Fatalf("after memory abort: %+v", st)
	}
	// StopXray ends the run too
	stRootStart(t, `{"tag":"block"}`)
	if err := xray.StopXray(); err != nil {
		t.Fatalf("StopXray: %v", err)
	}
	if st := stRootPoll(t); st.State != "failed" || st.Error != "no_connection" {
		t.Fatalf("after StopXray: %+v", st)
	}
	// no goroutine leak: the count returns near the start value (HTTP/engine goroutines gone)
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+3 {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines %d -> %d after 7 runs:\n%s", before, n, buf[:runtime.Stack(buf, true)])
	}
}
