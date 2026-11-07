package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Issue #8: --install-service on Windows registers a Scheduled Task.
func TestWindowsTaskScriptCarriesPathsAndSupervision(t *testing.T) {
	script := windowsTaskScript(`D:\Dev tools\cmemlan.exe`, `E:\DEVELOP\cmemlan data`,
		"192.168.111.25:8787", "192.168.111.0/24")
	for _, want := range []string{
		`$exe  = 'D:\Dev tools\cmemlan.exe'`,
		// The data directory is written in, quoted because it has a space.
		`--data-dir "E:\DEVELOP\cmemlan data"`,
		`--bind 192.168.111.25:8787`,
		`--allow-cidr 192.168.111.0/24`,
		`-LogonType S4U`,
		`-RestartCount 5`,
		`-ExecutionTimeLimit ([TimeSpan]::Zero)`,
		`$t.Delay = 'PT1M'`,
		`-TaskName 'cmemlan-hub'`,
		`IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("task script missing %q:\n%s", want, script)
		}
	}
}

func TestWindowsTaskScriptQuotesSingleQuotes(t *testing.T) {
	script := windowsTaskScript(`C:\O'Brien\cmemlan.exe`, `C:\d`, "127.0.0.1:8787", "")
	if !strings.Contains(script, `'C:\O''Brien\cmemlan.exe'`) {
		t.Errorf("single quote not doubled:\n%s", script)
	}
	if strings.Contains(script, "--allow-cidr") {
		t.Error("empty allow-cidr should be left out")
	}
}

func TestWindowsTaskRemoveNeverTouchesData(t *testing.T) {
	script := windowsTaskRemoveScript()
	if !strings.Contains(script, "Unregister-ScheduledTask -TaskName 'cmemlan-hub'") {
		t.Errorf("remove script: %s", script)
	}
	for _, bad := range []string{"Remove-Item", "hub.db", "rm "} {
		if strings.Contains(script, bad) {
			t.Errorf("remove script touches data (%q)", bad)
		}
	}
}

func TestWorkerPortPrefersPidFileAndNeverGuesses(t *testing.T) {
	dir := t.TempDir()
	if p := workerPort(dir, ""); p != 0 {
		t.Errorf("no pid file, no setting: port %d, want 0", p)
	}
	if p := workerPort(dir, "37800"); p != 37800 {
		t.Errorf("setting fallback: port %d", p)
	}
	raw, _ := json.Marshal(map[string]any{"pid": 1, "port": 37777, "startToken": "x"})
	if err := os.WriteFile(filepath.Join(dir, "worker.pid"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if p := workerPort(dir, "37800"); p != 37777 {
		t.Errorf("pid file port: %d", p)
	}
}

// The 15.x worker answers /api/health and /api/sync/status; doctor reads the
// version and the sync state from there.
func TestProbeWorkerReadsHealthAndSync(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/health":
			_, _ = w.Write([]byte(`{"status":"ok","version":"15.11.1","pid":1}`))
		case "/api/sync/status":
			_, _ = w.Write([]byte(`{"pending":{"observations":1,"workState":32},"lastError":null,` +
				`"quarantine":{"count":0},"pullQuarantine":{"count":2},` +
				`"hub":{"contentKinds":["observation","summary","prompt"]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	u, _ := url.Parse(ts.URL)
	port, _ := strconv.Atoi(u.Port())

	info, ok := probeWorker(port)
	if !ok || info.Version != "15.11.1" || info.Sync == nil {
		t.Fatalf("probe = %+v, %v", info, ok)
	}
	if info.Sync.PendingWorkState != 32 || info.Sync.PendingTotal != 33 || info.Sync.PullQuarantined != 2 {
		t.Errorf("sync = %+v", info.Sync)
	}
	lines := workerSyncChecks(info.Sync)
	var text strings.Builder
	for _, c := range lines {
		c.write(&text)
	}
	for _, want := range []string{"work_state waiting", "32 entries", "sync quarantine"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("checks missing %q:\n%s", want, text.String())
		}
	}
}

func TestServeRejectsBadWorkStateFlag(t *testing.T) {
	code, _, stderr := runCmd(t, Env{}, "serve", "--work-state", "maybe")
	if code != 2 || !strings.Contains(stderr, "--work-state") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
}
