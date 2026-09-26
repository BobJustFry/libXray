package xray

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/xtls/libxray/nodep"
)

// TspuProxyResult — итог проверки узла загрузкой через его собственный протокол.
//
// У UDP-узлов (Hysteria) нет TCP-соединения до ноды, которое можно нагрузить
// напрямую: заморозка «после 16 КБ» — механизм TCP, а что ТСПУ делает с UDP к
// зарубежным адресам (резка, сброс, белые списки), по трафику видно только
// одним способом — прогнать данные через узел и посмотреть, дойдут ли.
type TspuProxyResult struct {
	// PingMs — задержка HEAD, как у обычного пинга; -1 — пинг не прошёл, и
	// загрузку тогда не пробуем.
	PingMs  int64  `json:"pingMs"`
	PingErr string `json:"pingErr,omitempty"`
	// Status — HTTP-код ответа на загрузку; 0 — заголовки не пришли.
	Status int `json:"status"`
	// Bytes — сколько байт тела пришло; Expected — сколько должно было.
	Bytes    int64 `json:"bytes"`
	Expected int64 `json:"expected"`
	// DownloadMs — от запроса до конца загрузки или до остановки.
	DownloadMs int64 `json:"downloadMs"`
	// FirstByteMs — от запроса до первого байта тела; -1 — тело не началось.
	FirstByteMs int64 `json:"firstByteMs"`
	// Stalled — файл начал качаться, а потом данные перестали приходить на
	// StallMs; загрузку оборвали.
	Stalled bool  `json:"stalled"`
	StallMs int64 `json:"stallMs"`
	Err     string `json:"err,omitempty"`
}

// TspuProxyProbe поднимает отдельное ядро по configPath (как Ping), делает HEAD
// на pingURL через узел и, если он прошёл, качает downloadURL через тот же узел,
// считая байты тела. Нужна для UDP-узлов (Hysteria): прямой TCP-замер
// (TspuRawProbe) к ним неприменим, и проверить путь можно только через протокол
// самого узла. expected — сколько байт просили (Content-Length уточняет).
// downloadTimeoutSec ограничивает всю загрузку; тишину сторож считает только
// после первого байта тела, порог — max(stallSec, 3×пинг), чтобы медленный узел
// не сошёл за замороженный.
func TspuProxyProbe(datDir, configPath string, pingTimeoutSec int, pingURL, downloadURL string,
	expected int64, downloadTimeoutSec, stallSec int, proxy string) TspuProxyResult {
	r := TspuProxyResult{PingMs: -1, Expected: expected, FirstByteMs: -1}

	InitEnv(datDir, "")
	server, err := StartXray(configPath)
	if err != nil {
		r.PingErr = err.Error()
		return r
	}
	if err := server.Start(); err != nil {
		r.PingErr = err.Error()
		return r
	}
	defer server.Close()

	delay, err := nodep.MeasureDelay(pingTimeoutSec, pingURL, proxy)
	if err != nil {
		r.PingErr = err.Error()
		return r
	}
	r.PingMs = delay

	downloadTimeout := time.Duration(downloadTimeoutSec) * time.Second
	stall := time.Duration(stallSec) * time.Second
	if byPing := 3 * time.Duration(delay) * time.Millisecond; byPing > stall {
		stall = byPing
	}
	r.StallMs = stall.Milliseconds()
	client, err := nodep.CoreHTTPClient(0, proxy)
	if err != nil {
		r.Err = err.Error()
		return r
	}
	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		r.Err = err.Error()
		return r
	}
	req.Header.Set("Cache-Control", "no-cache")
	// Без сжатия: по сети должны пройти все expected байт, иначе порог ТСПУ
	// можно и не пересечь (Go по умолчанию просит gzip).
	req.Header.Set("Accept-Encoding", "identity")

	start := time.Now()
	var got atomic.Int64
	// lastProgress == 0 — тело ещё не началось, сторож молчит.
	var lastProgress atomic.Int64
	var stalled atomic.Bool
	done := make(chan struct{})
	defer close(done)
	// Сторож тишины: после первого байта тела байты должны приходить хотя бы раз
	// в stall. Иначе загрузку обрываем и отмечаем, на скольких байтах она встала.
	go func() {
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				last := lastProgress.Load()
				if last != 0 && time.Since(time.Unix(0, last)) > stall {
					stalled.Store(true)
					cancel()
					return
				}
			}
		}
	}()

	resp, err := client.Do(req)
	if err != nil {
		r.DownloadMs = time.Since(start).Milliseconds()
		r.Stalled = stalled.Load()
		r.Err = err.Error()
		return r
	}
	defer resp.Body.Close()
	r.Status = resp.StatusCode
	if resp.ContentLength > 0 {
		r.Expected = resp.ContentLength
	}
	buf := make([]byte, 8<<10)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if got.Add(int64(n)) == int64(n) {
				r.FirstByteMs = time.Since(start).Milliseconds()
			}
			lastProgress.Store(time.Now().UnixNano())
		}
		if rerr != nil {
			if rerr != io.EOF {
				r.Err = rerr.Error()
			}
			break
		}
	}
	r.DownloadMs = time.Since(start).Milliseconds()
	r.Bytes = got.Load()
	r.Stalled = stalled.Load() && r.Bytes < r.Expected
	return r
}
