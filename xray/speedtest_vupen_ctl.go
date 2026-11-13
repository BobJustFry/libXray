package xray

// libXray adapter of the in-core speedtest engine (T-299/T-302). The engine itself is
// speedtest_vupen.go: a byte copy of android/libv2ray-patch/libv2ray_speedtest.go with only
// the package clause changed (scripts/sync_speedtest_engine.ps1 keeps them equal; do not edit
// the copy). This file is the libXray-only part: where the live core comes from and how the
// run is torn down.
//
// Wire contract = the Android one (CoreController.SpeedtestStart/Poll/Cancel, Lib v55):
// Start request JSON {tag, expect, source, downStreams, upStreams, durationMs, pingCount, ...}
// (streams are clamped 1..8, default down 6 / up 4, so a caller can run fewer streams without
// a rebuild), answer {"ok":true} or {"error":"<code>"}; Poll answer = the status JSON
// (state idle|running|done|failed|cancelled, phase, error, ping, down, up, ...).
// Extra codes of this wrapper (Android never produces them): "memory" (SpeedtestAbortMemory,
// the host ran short of memory) and "no_connection" with state "failed" when the core was
// stopped under a running test.

import (
	"time"

	"github.com/xtls/xray-core/features/outbound"
)

// stCodeMemory is the failure code of a run stopped by the host's memory guard. The engine's
// code list (libv2ray_speedtest.go) does not know it; clients show unknown codes as a
// generic failure.
const stCodeMemory = "memory"

// SpeedtestStart starts a run through the live core, outbound forced to the request's tag.
// Returns {"ok":true} or {"error":"<code>"} (busy, bad_request, bad_url, unknown_tag,
// no_connection when the core is not running). Never blocks on network I/O.
func SpeedtestStart(reqJSON string) string {
	return speedtestStart(reqJSON, func(tag string) (*stEngine, string) {
		// Read the core here, under stStartMu (speedtestStart calls mk with it held): see
		// speedtestCoreStopping for why reading it earlier is a race (CR-001).
		inst := runningCore()
		if inst == nil {
			return nil, stErrNoConnection
		}
		om, _ := inst.GetFeature(outbound.ManagerType()).(outbound.Manager)
		if om == nil || om.GetHandler(tag) == nil {
			return nil, stErrUnknownTag
		}
		return &stEngine{dial: stForcedDialer(inst, tag), t: stDefaultTimings}, ""
	})
}

// SpeedtestPoll returns the status JSON of the current (or last) run and renews the lease
// (no Poll for 2 s -> the run cancels itself).
func SpeedtestPoll() string {
	return speedtestPoll()
}

// SpeedtestCancel cancels the current run without waiting (connections close < 1 s);
// Poll shows state "cancelled", error "cancelled".
func SpeedtestCancel() {
	speedtestCancel(stErrCancelled)
}

// SpeedtestAbortMemory is the hook for the host's memory guard: it stops the run at once
// (does not wait) and Poll shows state "failed", error "memory". No-op when nothing runs.
func SpeedtestAbortMemory() {
	speedtestAbort(stCodeMemory, 0)
}

// speedtestAbort ends a running run as FAILED with code (Poll: state "failed", error code),
// unlike speedtestCancel, whose state is "cancelled". With wait > 0 it also waits (at most
// wait) for the run's goroutines to be gone. No-op when no run is active.
func speedtestAbort(code string, wait time.Duration) {
	speedtestWaitRun(speedtestAbortRun(code), wait)
}

// speedtestAbortRun marks the active run failed with code and cancels it, without waiting.
// Returns that run, or nil when none is active.
func speedtestAbortRun(code string) *stRun {
	r := stCur.Load()
	if r == nil || r.isFinished() {
		return nil
	}
	r.update(func(s *stStatus) {
		if s.State == stStateRunning {
			s.State, s.Error = stStateFailed, code
		}
	})
	r.cancel(stCode(code))
	return r
}

// speedtestWaitRun waits at most wait for r's goroutines to be gone (r == nil: nothing).
func speedtestWaitRun(r *stRun, wait time.Duration) {
	if r == nil || wait <= 0 {
		return
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-r.finished:
	case <-t.C:
	}
}

// speedtestCoreStopping is called by RunXray*/StopXray AFTER they have unpublished the old
// instance (coreServerNow.Store(nil)) and BEFORE they close or replace it: a run dialing
// through it can only fail, so end it now ("no_connection") and give its goroutines up to a
// second to release their connections.
//
// The abort runs under stStartMu, the lock SpeedtestStart holds from its busy check to
// stCur.Store, and SpeedtestStart reads the core inside that lock. So a Start either took the
// lock first (its run is already in stCur and is aborted here) or takes it after this abort
// and reads the already-unpublished core (-> no_connection). Without the lock a Start that
// had read the old instance could register its run after the abort found nothing to stop and
// keep dialing through a closed instance (CR-001). The wait is outside the lock (a Start
// must not stall behind it).
func speedtestCoreStopping() {
	stStartMu.Lock()
	r := speedtestAbortRun(stErrNoConnection)
	stStartMu.Unlock()
	speedtestWaitRun(r, time.Second)
}
