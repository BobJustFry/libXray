package xray

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xtls/libxray/nodep"
)

const tspuTestFileBytes = 64 << 10

// Узел в тесте — ядро с SOCKS-входом и выходом freedom: путь запроса тот же,
// что у настоящей проверки, только «сервер» свой и ведёт себя как задано.
func tspuTestCore(t *testing.T) (configPath, proxy string) {
	t.Helper()
	ports, err := nodep.GetFreePorts(1)
	if err != nil {
		t.Fatal(err)
	}
	cfg := fmt.Sprintf(`{"log":{"loglevel":"none"},`+
		`"inbounds":[{"listen":"127.0.0.1","port":%d,"protocol":"socks","settings":{"udp":false}}],`+
		`"outbounds":[{"protocol":"freedom","tag":"direct"}]}`, ports[0])
	configPath = filepath.Join(t.TempDir(), "probe.json")
	if err := os.WriteFile(configPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return configPath, fmt.Sprintf("socks5://127.0.0.1:%d", ports[0])
}

type tspuServer struct {
	sendBytes   int           // сколько тела отдать; меньше файла — дальше тишина
	headerDelay time.Duration // молчание до заголовков
	pingDelay   time.Duration // задержка ответа на HEAD (медленный узел)
}

// HEAD → 204 (пинг), GET → тело размером tspuTestFileBytes. Недоотданное тело —
// тишина без закрытия соединения, как у соединения под заморозкой ТСПУ.
func (c tspuServer) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wait := func(d time.Duration) bool {
			select {
			case <-time.After(d):
				return true
			case <-r.Context().Done():
				return false
			}
		}
		if r.Method == http.MethodHead {
			if c.pingDelay > 0 && !wait(c.pingDelay) {
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if c.headerDelay > 0 && !wait(c.headerDelay) {
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(tspuTestFileBytes))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(strings.Repeat("x", c.sendBytes)))
		w.(http.Flusher).Flush()
		if c.sendBytes < tspuTestFileBytes {
			<-r.Context().Done()
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// stallSec = 1 — чтобы тесты шли секунды; порог всё равно не ниже 3×пинг.
func runTspuProxyProbe(t *testing.T, srv *httptest.Server, downloadTimeoutSec int) TspuProxyResult {
	t.Helper()
	configPath, proxy := tspuTestCore(t)
	return TspuProxyProbe(t.TempDir(), configPath, 5, srv.URL+"/ping", srv.URL+"/file",
		tspuTestFileBytes, downloadTimeoutSec, 1, proxy)
}

func TestTspuProxyProbeFullDownload(t *testing.T) {
	r := runTspuProxyProbe(t, tspuServer{sendBytes: tspuTestFileBytes}.start(t), 10)
	if r.PingMs < 0 || r.Status != http.StatusOK || r.Bytes != tspuTestFileBytes ||
		r.Stalled || r.Err != "" || r.FirstByteMs < 0 {
		t.Fatalf("full download: %+v", r)
	}
}

func TestTspuProxyProbeFreezeAfter16K(t *testing.T) {
	r := runTspuProxyProbe(t, tspuServer{sendBytes: 16 << 10}.start(t), 10)
	if r.PingMs < 0 || r.Status != http.StatusOK || r.Bytes != 16<<10 || !r.Stalled {
		t.Fatalf("freeze at 16 KiB must be reported as stalled with 16384 bytes: %+v", r)
	}
	if r.StallMs != 1000 || r.DownloadMs > 5000 {
		t.Fatalf("stall watchdog must stop the download after ~1 s: %+v", r)
	}
}

// Тишина до первого байта — не заморозка: через узел к этому моменту прошли
// только рукопожатия, это ниже порога ТСПУ. Загрузка ждёт до общего таймаута.
func TestTspuProxyProbeSilenceBeforeBodyIsNotAStall(t *testing.T) {
	r := runTspuProxyProbe(t, tspuServer{sendBytes: tspuTestFileBytes, headerDelay: time.Minute}.start(t), 3)
	if r.PingMs < 0 || r.Status != 0 || r.Bytes != 0 || r.Stalled || r.Err == "" ||
		r.FirstByteMs != -1 {
		t.Fatalf("silence before the body must end as a timeout, not a stall: %+v", r)
	}
}

// Медленный первый байт при целом файле — это ОК, а не заморозка.
func TestTspuProxyProbeSlowFirstByteStillCompletes(t *testing.T) {
	r := runTspuProxyProbe(t, tspuServer{sendBytes: tspuTestFileBytes, headerDelay: 2 * time.Second}.start(t), 10)
	if r.Stalled || r.Bytes != tspuTestFileBytes || r.FirstByteMs < 2000 {
		t.Fatalf("a slow start with a whole file is not a freeze: %+v", r)
	}
}

// Порог тишины растёт с пингом: медленный узел не сходит за замороженный.
func TestTspuProxyProbeStallThresholdFollowsPing(t *testing.T) {
	r := runTspuProxyProbe(t, tspuServer{sendBytes: 16 << 10, pingDelay: 600 * time.Millisecond}.start(t), 15)
	if !r.Stalled || r.PingMs < 600 || r.StallMs < 3*r.PingMs {
		t.Fatalf("stall threshold must be at least 3×ping: %+v", r)
	}
}

// Вариант для Apple: без SOCKS — ядро из JSON без входов, запросы через core.Dial.
func runConfigProbe(t *testing.T, srv *httptest.Server) TspuProxyResult {
	t.Helper()
	cfg := `{"log":{"loglevel":"none"},"outbounds":[{"protocol":"freedom","tag":"proxy"}]}`
	return TspuConfigProbe(t.TempDir(), cfg, 5, srv.URL+"/ping", srv.URL+"/file",
		tspuTestFileBytes, 10, 1)
}

func TestTspuConfigProbeFullDownload(t *testing.T) {
	r := runConfigProbe(t, tspuServer{sendBytes: tspuTestFileBytes}.start(t))
	if r.PingMs < 0 || r.Status != http.StatusOK || r.Bytes != tspuTestFileBytes ||
		r.Stalled || r.Err != "" {
		t.Fatalf("full download through core.Dial: %+v", r)
	}
}

func TestTspuConfigProbeStall(t *testing.T) {
	r := runConfigProbe(t, tspuServer{sendBytes: 16 << 10}.start(t))
	if r.PingMs < 0 || r.Bytes != 16<<10 || !r.Stalled {
		t.Fatalf("a stall through core.Dial must be reported: %+v", r)
	}
}

func TestTspuConfigProbeBadConfigIsAPingFailure(t *testing.T) {
	r := TspuConfigProbe(t.TempDir(), "{not json", 5, "http://127.0.0.1:1/", "http://127.0.0.1:1/", 1, 3, 1)
	if r.PingMs != -1 || r.PingErr == "" {
		t.Fatalf("a broken config must end before the ping: %+v", r)
	}
}
