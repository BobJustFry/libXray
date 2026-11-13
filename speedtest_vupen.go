// SPDX-License-Identifier: MIT
//
// speedtest_vupen.go — speedtest through the live core (T-299/T-302, core build 90).
//
// Same engine and the same JSON contract as Android's CoreController.SpeedtestStart/Poll/
// Cancel (Lib v55); see xray/speedtest_vupen_ctl.go. Strings are plain UTF-8 JSON (not
// base64), like VupenCheckRoute. All calls are non-blocking: the core is read atomically,
// the run's own mutex only guards in-memory status.
//
// Start request: {"tag":"proxy","expect":"","source":"cloudflare"|"yandex",
// "downStreams":6,"upStreams":4,"durationMs":10000,"pingCount":5} — streams are clamped
// 1..8, so the host may lower them (e.g. iPhone NE memory) without rebuilding the core.
//
// gomobile: LibXraySpeedtestStart / LibXraySpeedtestPoll / LibXraySpeedtestCancel /
// LibXraySpeedtestAbortMemory. Windows: CGoSpeedtest* (build/template/main.gotemplate).
package libXray

import "github.com/xtls/libxray/xray"

// SpeedtestStart starts a run; returns {"ok":true} or {"error":"<code>"}.
func SpeedtestStart(reqJSON string) string {
	return xray.SpeedtestStart(reqJSON)
}

// SpeedtestPoll returns the status JSON of the current (or last) run and renews the
// 2 s lease: the host must poll (every ~500 ms), otherwise the run cancels itself.
func SpeedtestPoll() string {
	return xray.SpeedtestPoll()
}

// SpeedtestCancel cancels the running test (state "cancelled").
func SpeedtestCancel() {
	xray.SpeedtestCancel()
}

// SpeedtestAbortMemory stops the running test as failed with code "memory": the hook for
// the host's memory guard (iOS NE) — call it before restarting the tunnel. No-op when idle.
func SpeedtestAbortMemory() {
	xray.SpeedtestAbortMemory()
}
