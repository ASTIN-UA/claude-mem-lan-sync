package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// workerInfo is what claude-mem's own worker says about itself over HTTP.
type workerInfo struct {
	Version string
	Sync    *workerSync // nil when /api/sync/status did not answer
}

// workerSync is the part of GET /api/sync/status that doctor reports.
type workerSync struct {
	LastError        string
	ContentKinds     []string
	PendingWorkState int
	PendingTotal     int
	Quarantined      int
	PullQuarantined  int
}

// workerPort finds the worker's HTTP port: worker.pid records it (claude-mem
// 13.x and 15.x both write {"pid":…, "port":…}), and the setting is the
// fallback. Zero means there is nothing to probe — no default is assumed, so a
// test directory never reaches a worker that happens to run on this machine.
func workerPort(memDir, setting string) int {
	if raw, err := os.ReadFile(filepath.Join(memDir, "worker.pid")); err == nil {
		var doc struct {
			Port int `json:"port"`
		}
		if json.Unmarshal(raw, &doc) == nil && doc.Port > 0 && doc.Port < 65536 {
			return doc.Port
		}
	}
	if p, err := strconv.Atoi(setting); err == nil && p > 0 && p < 65536 {
		return p
	}
	return 0
}

// probeWorker asks a running worker for its version and sync state.
//
// This is how doctor knows the worker is up on claude-mem 15.x, where the
// worker is a standalone process and the pid-file check alone was unreliable on
// Windows. Only loopback is contacted, with short timeouts.
func probeWorker(port int) (workerInfo, bool) {
	if port == 0 {
		return workerInfo{}, false
	}
	client := &http.Client{Timeout: 3 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	var health struct {
		Status  string `json:"status"`
		Version string `json:"version"`
	}
	if !getJSON(client, base+"/api/health", &health) || health.Status != "ok" {
		return workerInfo{}, false
	}
	info := workerInfo{Version: health.Version}

	var st struct {
		LastError *string        `json:"lastError"`
		Pending   map[string]int `json:"pending"`
		Hub       struct {
			ContentKinds []string `json:"contentKinds"`
		} `json:"hub"`
		Quarantine struct {
			Count int `json:"count"`
		} `json:"quarantine"`
		PullQuarantine struct {
			Count int `json:"count"`
		} `json:"pullQuarantine"`
	}
	if getJSON(client, base+"/api/sync/status", &st) {
		s := &workerSync{
			ContentKinds:     st.Hub.ContentKinds,
			PendingWorkState: st.Pending["workState"],
			Quarantined:      st.Quarantine.Count,
			PullQuarantined:  st.PullQuarantine.Count,
		}
		if st.LastError != nil {
			s.LastError = *st.LastError
		}
		for _, n := range st.Pending {
			s.PendingTotal += n
		}
		info.Sync = s
	}
	return info, true
}

func getJSON(client *http.Client, url string, out any) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return json.NewDecoder(resp.Body).Decode(out) == nil
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}
