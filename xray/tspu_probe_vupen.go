package xray

import (
	"context"
	gotls "crypto/tls"
	"errors"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	xtls "github.com/xtls/xray-core/transport/internet/tls"
)

// TspuRawResult — итог прямой проверки пути до узла на заморозку ТСПУ.
//
// ТСПУ не рвёт соединение с зарубежным адресом хостинга, а после ~15–20 КБ в
// одном TCP-соединении (в обе стороны, ~25 пакетов) перестаёт пропускать
// данные: RST нет, клиент ждёт до таймаута (net4people/bbs#490). Проверка
// повторяет метод dpi-detector: одно соединение к IP узла с его SNI и тем же
// отпечатком TLS, что у туннеля, первый HEAD — жив ли узел, затем HEAD с
// набивкой в заголовке, пока через соединение не пройдут десятки килобайт.
// Прокси, пинг и сторонние сайты не участвуют: меряется ровно путь до ноды.
type TspuRawResult struct {
	// ConnectMs — TCP+TLS; -1 — не соединились.
	ConnectMs int64 `json:"connectMs"`
	// RttMs — первый HEAD без набивки; -1 — узел не ответил.
	RttMs int64 `json:"rttMs"`
	// StepTimeoutMs — сколько ждали ответа на каждый шаг с набивкой.
	StepTimeoutMs int64 `json:"stepTimeoutMs"`
	// Steps — сколько шагов с набивкой прошло из StepsTotal.
	Steps      int `json:"steps"`
	StepsTotal int `json:"stepsTotal"`
	// SentBytes/RecvBytes — байты на самом TCP-соединении (TLS целиком): то,
	// что видит ТСПУ.
	SentBytes int64 `json:"sentBytes"`
	RecvBytes int64 `json:"recvBytes"`
	// Stage — где остановились: tcp, tls, alive, push; done — всё прошло.
	Stage string `json:"stage"`
	// Silent — остановились тишиной (таймаут), а не ошибкой. Заморозка ТСПУ —
	// это тишина; RST, закрытие и прочие мгновенные ошибки — не её почерк.
	Silent bool   `json:"silent"`
	Err    string `json:"err,omitempty"`
}

var errTspuServerClosed = errors.New("server closed the keep-alive connection")

type tspuCountingConn struct {
	net.Conn
	sent, recv *atomic.Int64
}

func (c *tspuCountingConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.recv.Add(int64(n))
	return n, err
}

func (c *tspuCountingConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.sent.Add(int64(n))
	return n, err
}

func tspuIsTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errTspuHandshakeTimeout) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

var errTspuHandshakeTimeout = errors.New("tls handshake timeout")

const tspuPadAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func tspuPad(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = tspuPadAlphabet[rand.IntN(len(tspuPadAlphabet))]
	}
	return string(b)
}

const tspuUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
	"(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"

// TspuRawProbe соединяется с ip:port напрямую (без прокси), делает TLS с sni и
// отпечатком fingerprint (как у туннеля, ALPN http/1.1), шлёт HEAD на host и
// затем steps запросов HEAD с padBytes набивки по тому же соединению. На каждый
// шаг ждёт max(minStepMs, 3×RTT), но не больше maxStepMs; на TCP, TLS и первый
// HEAD — connectTimeoutSec. Второе соединение не открывает: если сервер закрыл
// первое, это ошибка шага, а не повод начать счёт байт заново.
func TspuRawProbe(ip string, port int, sni, host, fingerprint string,
	steps, padBytes, connectTimeoutSec, minStepMs, maxStepMs int) (r TspuRawResult) {
	r = TspuRawResult{ConnectMs: -1, RttMs: -1, StepsTotal: steps, Stage: "tcp"}
	var sent, recv atomic.Int64
	defer func() {
		r.SentBytes = sent.Load()
		r.RecvBytes = recv.Load()
	}()
	fail := func(err error) {
		r.Err = err.Error()
		r.Silent = tspuIsTimeout(err)
	}

	connectTimeout := time.Duration(connectTimeoutSec) * time.Second
	start := time.Now()
	dialer := net.Dialer{Timeout: connectTimeout}
	raw, err := dialer.Dial("tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
	if err != nil {
		fail(err)
		return
	}
	counted := &tspuCountingConn{Conn: raw, sent: &sent, recv: &recv}

	r.Stage = "tls"
	fp := xtls.GetFingerprint(fingerprint)
	if fp == nil {
		fp = xtls.GetFingerprint("")
	}
	uconn := xtls.UClient(counted, &gotls.Config{
		ServerName:         sni,
		InsecureSkipVerify: true,
		NextProtos:         []string{"http/1.1"},
	}, fp).(*xtls.UConn)
	defer uconn.Close()
	// Дедлайн на сокете, а не только контекст: тишина посреди рукопожатия
	// должна закончиться таймаутом, который отличим от ошибки.
	_ = raw.SetDeadline(time.Now().Add(connectTimeout))
	hctx, hcancel := context.WithTimeout(context.Background(), connectTimeout)
	err = uconn.WebsocketHandshakeContext(hctx)
	hcancel()
	if err != nil {
		if hctx.Err() != nil {
			err = errors.Join(errTspuHandshakeTimeout, err)
		}
		fail(err)
		return
	}
	_ = raw.SetDeadline(time.Time{})
	r.ConnectMs = time.Since(start).Milliseconds()

	var dials atomic.Int32
	tr := &http.Transport{
		DialTLSContext: func(context.Context, string, string) (net.Conn, error) {
			if dials.Add(1) > 1 {
				return nil, errTspuServerClosed
			}
			return uconn, nil
		},
		MaxConnsPerHost:     1,
		MaxIdleConnsPerHost: 1,
		DisableCompression:  true,
		// Только HTTP/1.1: ALPN h2 мы не предлагали.
		TLSNextProto: map[string]func(string, *gotls.Conn) http.RoundTripper{},
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{
		Transport: tr,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	if host == "" {
		host = sni
	}
	if host == "" {
		host = ip
	}
	url := "https://" + host + "/"
	head := func(pad string, timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", tspuUserAgent)
		req.Header.Set("Accept", "*/*")
		if pad != "" {
			req.Header.Set("X-Pad", pad)
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		return resp.Body.Close()
	}

	r.Stage = "alive"
	t0 := time.Now()
	if err := head("", connectTimeout); err != nil {
		fail(err)
		return
	}
	rtt := time.Since(t0)
	r.RttMs = rtt.Milliseconds()

	stepTimeout := 3 * rtt
	if lo := time.Duration(minStepMs) * time.Millisecond; stepTimeout < lo {
		stepTimeout = lo
	}
	if hi := time.Duration(maxStepMs) * time.Millisecond; stepTimeout > hi {
		stepTimeout = hi
	}
	r.StepTimeoutMs = stepTimeout.Milliseconds()

	r.Stage = "push"
	for i := 0; i < steps; i++ {
		if err := head(tspuPad(padBytes), stepTimeout); err != nil {
			fail(err)
			return
		}
		r.Steps++
	}
	r.Stage = "done"
	return
}
