package xray

import (
	"strings"
	"sync"
	"testing"
)

// CR-706: состояние читается из потоков платформы параллельно с Run/Stop —
// без гонки (go test -race) и без паники.
func TestVupenAutoStateConcurrentWithRunStop(t *testing.T) {
	const cfg = `{"log":{"loglevel":"none"},"outbounds":[{"protocol":"freedom","tag":"direct"}]}`
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if s := VupenAutoState(); !strings.Contains(s, `"v":1`) {
				t.Errorf("state: %s", s)
				return
			}
			_ = GetXrayState()
			_, _ = QueryTrafficTotals()
		}
	}()
	for i := 0; i < 5; i++ {
		if err := RunXrayFromJSON(t.TempDir(), "", cfg); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(VupenAutoState(), "no AUTO 4.1 balancer") {
			t.Fatalf("running core without AUTO: %s", VupenAutoState())
		}
		if err := StopXray(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}
