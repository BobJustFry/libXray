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
