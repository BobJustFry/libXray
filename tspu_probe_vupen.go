package libXray

import (
	"encoding/base64"
	"encoding/json"

	"github.com/xtls/libxray/nodep"
	"github.com/xtls/libxray/xray"
)

type tspuProbeRequest struct {
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

// TspuProbe — экспериментальная проверка узла на заморозку ТСПУ: пинг, затем
// загрузка файла известного размера через тот же узел (см. xray.TspuProbe).
func TspuProbe(base64Text string) string {
	var response nodep.CallResponse[*xray.TspuProbeResult]
	req, err := base64.StdEncoding.DecodeString(base64Text)
	if err != nil {
		return response.EncodeToBase64(nil, err)
	}
	var request tspuProbeRequest
	if err := json.Unmarshal(req, &request); err != nil {
		return response.EncodeToBase64(nil, err)
	}
	if request.PingTimeout < 1 {
		request.PingTimeout = 5
	}
	if request.DownloadTimeoutSec < 1 {
		request.DownloadTimeoutSec = 15
	}
	if request.StallSec < 1 {
		request.StallSec = 5
	}
	r := xray.TspuProbe(request.DatDir, request.ConfigPath, request.PingTimeout,
		request.PingUrl, request.DownloadUrl, request.Expected,
		request.DownloadTimeoutSec, request.StallSec, request.Proxy)
	return response.EncodeToBase64(&r, nil)
}
