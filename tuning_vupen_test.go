package libXray

import (
	"encoding/json"
	"strings"
	"testing"
)

// Ядро 78: экспорты АВТО 4.1 без запущенного ядра — понятный ответ, без паники.
func TestVupenExportsWithoutCore(t *testing.T) {
	if VupenCoreBuild() != "79" {
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
