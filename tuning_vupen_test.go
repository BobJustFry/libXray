package libXray

import (
	"encoding/json"
	"strings"
	"testing"
)

// Ядро 78: экспорты АВТО 4.1 без запущенного ядра — понятный ответ, без паники.
func TestVupenExportsWithoutCore(t *testing.T) {
	if VupenCoreBuild() != "82" {
		t.Fatalf("core build %s", VupenCoreBuild())
	}
	var st map[string]any
	if err := json.Unmarshal([]byte(VupenAutoState()), &st); err != nil {
		t.Fatal(err)
	}
	if st["v"] != float64(1) || st["active"] != false || st["detail"] != "the core is not running" {
		t.Fatalf("state: %v", st)
	}
	for req, want := range map[string]string{
		`{"v":1,"mode":"check"}`:   "the core is not running",
		`{"v":1,"mode":"heal"}`:    "the core is not running",
		`{"v":1,"mode":"switch"}`:  "the core is not running",
		`not json`:                 "bad request",
		`{"v":1,"mode":"restart"}`: "bad request",
	} {
		var resp struct {
			V       int    `json:"v"`
			Verdict string `json:"verdict"`
			Detail  string `json:"detail"`
		}
		if err := json.Unmarshal([]byte(VupenCheckRoute(req)), &resp); err != nil {
			t.Fatal(err)
		}
		if resp.V != 1 || resp.Verdict != "unknown" || !strings.HasPrefix(resp.Detail, want) {
			t.Fatalf("%s → %+v", req, resp)
		}
	}
}

func TestVupenSetUplinkExport(t *testing.T) {
	if e := VupenSetUplink("wifi", "libxray-test"); e != "" {
		t.Fatal(e)
	}
	if e := VupenSetUplink("ethernet", ""); e == "" {
		t.Fatal("unknown kind must be rejected")
	}
	var st struct {
		Uplink string `json:"uplink"`
		Sig    string `json:"sig"`
	}
	_ = json.Unmarshal([]byte(VupenAutoState()), &st)
	if st.Uplink != "wifi" || st.Sig != "libxray-test" {
		t.Fatalf("uplink in the state: %+v", st)
	}
}

func TestVupenSetCheckURLExport(t *testing.T) {
	const def = "https://speed.cloudflare.com/__down?bytes=24576"
	t.Cleanup(func() { VupenSetCheckURL(def, "") })
	if e := VupenSetCheckURL("http://vupen.fun/24k.bin", ""); e == "" {
		t.Fatal("http must be rejected")
	}
	if e := VupenSetCheckURL("https://vupen.fun/app/probe/24k.bin", def); e != "" {
		t.Fatal(e)
	}
}

// T-075: в диагностике памяти — сборщик и кэши транспортов (новые поля в конце,
// heap_alloc остаётся первым: его разбирают Swift и Dart по префиксу).
func TestGoMemoryDiagFields(t *testing.T) {
	d := GoMemoryDiag()
	if !strings.HasPrefix(d, "heap_alloc=") {
		t.Fatalf("heap_alloc must stay first: %s", d)
	}
	for _, k := range []string{" goroutines=", " num_gc=", " gc_cpu=", " grpc_pool=", " xhttp_pool=", " xhttp_clients="} {
		if !strings.Contains(d, k) {
			t.Fatalf("%q missing: %s", k, d)
		}
	}
}

// T-079: настройка «Apple через VPN при белых списках» видна в состоянии и без ядра.
func TestVupenSetAppleViaVpnOnLte(t *testing.T) {
	t.Cleanup(func() { VupenSetAppleViaVpnOnLte(false) })
	VupenSetAppleViaVpnOnLte(true)
	var st map[string]any
	if err := json.Unmarshal([]byte(VupenAutoState()), &st); err != nil {
		t.Fatal(err)
	}
	if st["appleViaVpnOnLte"] != true || st["appleViaVpnActive"] != false || st["appleViaVpnMode"] != float64(1) {
		t.Fatalf("state: %v", st)
	}
}

// T-084: режим 2 действует и без АВТО; неверный режим ничего не меняет.
func TestVupenSetAppleViaVpnMode(t *testing.T) {
	t.Cleanup(func() { VupenSetAppleViaVpnMode(0) })
	state := func() map[string]any {
		var st map[string]any
		if err := json.Unmarshal([]byte(VupenAutoState()), &st); err != nil {
			t.Fatal(err)
		}
		return st
	}
	VupenSetAppleViaVpnMode(2)
	if st := state(); st["appleViaVpnMode"] != float64(2) || st["appleViaVpnOnLte"] != true || st["appleViaVpnActive"] != true {
		t.Fatalf("mode 2: %v", st)
	}
	VupenSetAppleViaVpnMode(7)
	if st := state(); st["appleViaVpnMode"] != float64(2) {
		t.Fatalf("invalid mode changed the setting: %v", st)
	}
	VupenSetAppleViaVpnMode(0)
	if st := state(); st["appleViaVpnMode"] != float64(0) || st["appleViaVpnOnLte"] != false || st["appleViaVpnActive"] != false {
		t.Fatalf("mode 0: %v", st)
	}
}
