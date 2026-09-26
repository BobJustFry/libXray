package libXray

import (
	"encoding/base64"
	"encoding/json"

	"github.com/xtls/libxray/nodep"
	"github.com/xtls/libxray/xray"
)

type tspuRawProbeRequest struct {
	Ip                string `json:"ip,omitempty"`
	Port              int    `json:"port,omitempty"`
	Sni               string `json:"sni,omitempty"`
	Host              string `json:"host,omitempty"`
	Fingerprint       string `json:"fingerprint,omitempty"`
	Steps             int    `json:"steps,omitempty"`
	PadBytes          int    `json:"padBytes,omitempty"`
	ConnectTimeoutSec int    `json:"connectTimeoutSec,omitempty"`
	MinStepMs         int    `json:"minStepMs,omitempty"`
	MaxStepMs         int    `json:"maxStepMs,omitempty"`
}

// TspuRawProbe — экспериментальная проверка пути до узла на заморозку ТСПУ:
// прямое TLS-соединение к IP узла и десятки килобайт по нему (см.
// xray.TspuRawProbe). Прокси не нужен: меряется ровно путь до ноды.
func TspuRawProbe(base64Text string) string {
	var response nodep.CallResponse[*xray.TspuRawResult]
	req, err := base64.StdEncoding.DecodeString(base64Text)
	if err != nil {
		return response.EncodeToBase64(nil, err)
	}
	var q tspuRawProbeRequest
	if err := json.Unmarshal(req, &q); err != nil {
		return response.EncodeToBase64(nil, err)
	}
	if q.Steps < 1 {
		q.Steps = 10
	}
	if q.PadBytes < 1 {
		q.PadBytes = 4000
	}
	if q.ConnectTimeoutSec < 1 {
		q.ConnectTimeoutSec = 5
	}
	if q.MinStepMs < 1 {
		q.MinStepMs = 1500
	}
	if q.MaxStepMs < q.MinStepMs {
		q.MaxStepMs = 5000
	}
	r := xray.TspuRawProbe(q.Ip, q.Port, q.Sni, q.Host, q.Fingerprint,
		q.Steps, q.PadBytes, q.ConnectTimeoutSec, q.MinStepMs, q.MaxStepMs)
	return response.EncodeToBase64(&r, nil)
}

type tspuProxyProbeRequest struct {
	DatDir             string `json:"datDir,omitempty"`
	ConfigPath         string `json:"configPath,omitempty"`
	PingTimeout        int    `json:"pingTimeout,omitempty"`
	PingUrl            string `json:"pingUrl,omitempty"`
	DownloadUrl        string `json:"downloadUrl,omitempty"`
	Expected           int64  `json:"expected,omitempty"`
	DownloadTimeoutSec int    `json:"downloadTimeoutSec,omitempty"`
	StallSec           int    `json:"stallSec,omitempty"`
	Proxy              string `json:"proxy,omitempty"`
}

// TspuProxyProbe — проверка UDP-узла (Hysteria) загрузкой через его протокол:
// отдельное ядро по configPath, HEAD через узел и файл известного размера
// (см. xray.TspuProxyProbe).
func TspuProxyProbe(base64Text string) string {
	var response nodep.CallResponse[*xray.TspuProxyResult]
	req, err := base64.StdEncoding.DecodeString(base64Text)
	if err != nil {
		return response.EncodeToBase64(nil, err)
	}
	var q tspuProxyProbeRequest
	if err := json.Unmarshal(req, &q); err != nil {
		return response.EncodeToBase64(nil, err)
	}
	if q.PingTimeout < 1 {
		q.PingTimeout = 5
	}
	if q.DownloadTimeoutSec < 1 {
		q.DownloadTimeoutSec = 15
	}
	if q.StallSec < 1 {
		q.StallSec = 5
	}
	r := xray.TspuProxyProbe(q.DatDir, q.ConfigPath, q.PingTimeout, q.PingUrl,
		q.DownloadUrl, q.Expected, q.DownloadTimeoutSec, q.StallSec, q.Proxy)
	return response.EncodeToBase64(&r, nil)
}

type tspuConfigProbeRequest struct {
	DatDir             string `json:"datDir,omitempty"`
	ConfigJSON         string `json:"configJSON,omitempty"`
	PingTimeout        int    `json:"pingTimeout,omitempty"`
	PingUrl            string `json:"pingUrl,omitempty"`
	DownloadUrl        string `json:"downloadUrl,omitempty"`
	Expected           int64  `json:"expected,omitempty"`
	DownloadTimeoutSec int    `json:"downloadTimeoutSec,omitempty"`
	StallSec           int    `json:"stallSec,omitempty"`
}

// TspuConfigProbe — проверка UDP-узла (Hysteria) загрузкой через его протокол,
// без SOCKS и файла: ядро из configJSON, запросы через core.Dial (см.
// xray.TspuConfigProbe). Для iOS и macOS — в процессе приложения, VPN выключен.
func TspuConfigProbe(base64Text string) string {
	var response nodep.CallResponse[*xray.TspuProxyResult]
	req, err := base64.StdEncoding.DecodeString(base64Text)
	if err != nil {
		return response.EncodeToBase64(nil, err)
	}
	var q tspuConfigProbeRequest
	if err := json.Unmarshal(req, &q); err != nil {
		return response.EncodeToBase64(nil, err)
	}
	if q.PingTimeout < 1 {
		q.PingTimeout = 5
	}
	if q.DownloadTimeoutSec < 1 {
		q.DownloadTimeoutSec = 15
	}
	if q.StallSec < 1 {
		q.StallSec = 5
	}
	r := xray.TspuConfigProbe(q.DatDir, q.ConfigJSON, q.PingTimeout, q.PingUrl,
		q.DownloadUrl, q.Expected, q.DownloadTimeoutSec, q.StallSec)
	return response.EncodeToBase64(&r, nil)
}
