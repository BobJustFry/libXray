package xray

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tspuFreezer — TCP-ретранслятор перед сервером, который ведёт себя как ТСПУ:
// после limit байт (в обе стороны вместе) перестаёт пересылать данные, но
// соединение не закрывает. limit == 0 — пропускает всё.
type tspuFreezer struct {
	ln    net.Listener
	limit int64
}

func startTspuFreezer(t *testing.T, backend string, limit int64) *tspuFreezer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &tspuFreezer{ln: ln, limit: limit}
	var wg sync.WaitGroup
	t.Cleanup(func() {
		ln.Close()
		wg.Wait()
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				f.relay(c, backend)
			}()
		}
	}()
	return f
}

func (f *tspuFreezer) relay(client net.Conn, backend string) {
	defer client.Close()
	server, err := net.Dial("tcp", backend)
	if err != nil {
		return
	}
	defer server.Close()
	var total atomic.Int64
	frozen := make(chan struct{})
	var once sync.Once
	pipe := func(dst, src net.Conn) {
		buf := make([]byte, 1024)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				if f.limit > 0 && total.Add(int64(n)) > f.limit {
					once.Do(func() { close(frozen) })
					return // дальше тишина: не пишем и не закрываем
				}
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}
	done := make(chan struct{}, 2)
	go func() { pipe(server, client); done <- struct{}{} }()
	go func() { pipe(client, server); done <- struct{}{} }()
	select {
	case <-done:
	case <-frozen:
		// Держим оба соединения открытыми, пока клиент сам не сдастся.
		buf := make([]byte, 1024)
		for {
			if _, err := client.Read(buf); err != nil {
				return
			}
		}
	}
}

func (f *tspuFreezer) port() int { return f.ln.Addr().(*net.TCPAddr).Port }

func tspuTLSBackend(t *testing.T, closeEach bool) string {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if closeEach {
			w.Header().Set("Connection", "close")
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

func runRawProbe(t *testing.T, port int) TspuRawResult {
	t.Helper()
	// Шаг ждёт не меньше 1 с — чтобы тесты шли секунды, а не минуты.
	return TspuRawProbe("127.0.0.1", port, "example.com", "example.com", "chrome",
		10, 4000, 3, 1000, 1500)
}

func TestTspuRawProbePathIsClean(t *testing.T) {
	f := startTspuFreezer(t, tspuTLSBackend(t, false), 0)
	r := runRawProbe(t, f.port())
	if r.Stage != "done" || r.Steps != 10 || r.Silent || r.Err != "" || r.RttMs < 0 {
		t.Fatalf("clean path must pass every step: %+v", r)
	}
	if r.SentBytes < 40_000 {
		t.Fatalf("the probe must push tens of kilobytes over the wire, sent %d", r.SentBytes)
	}
}

func TestTspuRawProbeFreezeMidPush(t *testing.T) {
	f := startTspuFreezer(t, tspuTLSBackend(t, false), 16<<10)
	start := time.Now()
	r := runRawProbe(t, f.port())
	if r.Stage != "push" || !r.Silent || r.Steps >= 10 {
		t.Fatalf("a freeze after 16 KiB must end the push in silence: %+v", r)
	}
	if total := r.SentBytes + r.RecvBytes; total < 12<<10 {
		t.Fatalf("the freeze must be seen after >=12 KiB on the wire, got %d", total)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("a freeze must be caught by the step timeout, took %s", time.Since(start))
	}
}

func TestTspuRawProbeFreezeInsideHandshake(t *testing.T) {
	f := startTspuFreezer(t, tspuTLSBackend(t, false), 3<<10)
	r := runRawProbe(t, f.port())
	if r.Stage != "tls" || !r.Silent || r.ConnectMs != -1 {
		t.Fatalf("silence inside the handshake must be a silent tls stage: %+v", r)
	}
	if total := r.SentBytes + r.RecvBytes; total >= 12<<10 {
		t.Fatalf("this freeze happened below 12 KiB, got %d", total)
	}
}

func TestTspuRawProbeServerClosesIsAnErrorNotSilence(t *testing.T) {
	f := startTspuFreezer(t, tspuTLSBackend(t, true), 0)
	r := runRawProbe(t, f.port())
	if r.Stage != "push" || r.Silent || r.Err == "" || r.Steps != 0 {
		t.Fatalf("a server that closes after each reply is an error, not a freeze: %+v", r)
	}
}

func TestTspuRawProbeClosedPort(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	r := runRawProbe(t, port)
	if r.Stage != "tcp" || r.Silent || r.Err == "" {
		t.Fatalf("a refused connection is an instant tcp error: %+v", r)
	}
}
