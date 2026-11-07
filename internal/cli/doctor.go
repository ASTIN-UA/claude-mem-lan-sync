package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/andrhamm/claude-mem-lan-sync/internal/buildinfo"
	"github.com/andrhamm/claude-mem-lan-sync/internal/clientdb"
	"github.com/andrhamm/claude-mem-lan-sync/internal/paths"
	"github.com/andrhamm/claude-mem-lan-sync/internal/settings"
)

type check struct {
	ok     bool
	warn   bool
	label  string
	detail string
	remedy string
}

func (c check) write(w io.Writer) {
	mark := "✗"
	switch {
	case c.ok:
		mark = "✓"
	case c.warn:
		mark = "!"
	}
	fmt.Fprintf(w, "%s %s", mark, c.label)
	if c.detail != "" {
		fmt.Fprintf(w, " — %s", c.detail)
	}
	fmt.Fprintln(w)
	if c.remedy != "" && !c.ok {
		fmt.Fprintf(w, "    → %s\n", c.remedy)
	}
}

func runDoctor(args []string, env Env) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	if _, err := parseFlags(fs, args); err != nil {
		return 2
	}

	var checks []check
	failed := false

	fmt.Fprintf(env.Stdout, "%s\n\n", buildinfo.String())

	// 1. Is claude-mem present at all?
	memDir, err := paths.ClaudeMemDir()
	if err != nil {
		fmt.Fprintf(env.Stderr, "cmemlan: %v\n", err)
		return 1
	}
	dbPath := filepath.Join(memDir, "claude-mem.db")
	if _, err := os.Stat(dbPath); err != nil {
		checks = append(checks, check{
			label:  "claude-mem installed",
			detail: dbPath + " not found",
			remedy: "install claude-mem, or set CLAUDE_MEM_DATA_DIR if it lives elsewhere",
		})
		for _, c := range checks {
			c.write(env.Stdout)
		}
		return 1
	}
	checks = append(checks, check{ok: true, label: "claude-mem database found", detail: dbPath})

	// 2. Sync configuration, and anything shadowing it.
	settingsPath, _ := paths.ClaudeMemSettings()
	current, err := settings.Read(settingsPath)
	if err != nil {
		checks = append(checks, check{label: "settings readable", detail: err.Error()})
		failed = true
		current = map[string]json.RawMessage{}
	}

	hubURL := settings.Value(current, keyHubURL)
	token := settings.Value(current, keyToken)
	userID := settings.Value(current, keyUserID)

	switch {
	case hubURL == "" || token == "" || userID == "":
		checks = append(checks, check{
			label:  "sync configured",
			detail: "hub url, token, or hub id is empty — claude-mem only enables sync when all three are set",
			remedy: "run: cmemlan connect <hub-url> --code <code>",
		})
		failed = true
	default:
		checks = append(checks, check{ok: true, label: "sync configured", detail: hubURL})
	}

	if settings.Value(current, settings.DeviceIDKey) == "" {
		checks = append(checks, check{
			warn:   true,
			label:  "device id",
			detail: "not yet minted; claude-mem creates one on its next start",
		})
	} else {
		checks = append(checks, check{ok: true, label: "device id present"})
	}

	// 3. A stale value in Claude Code's settings shadows the good one, because
	// claude-mem applies environment overrides on top of its own file.
	if codePath, err := paths.ClaudeCodeSettings(); err == nil {
		if shadow := shadowingKeys(codePath); len(shadow) > 0 {
			checks = append(checks, check{
				label:  "no conflicting configuration",
				detail: fmt.Sprintf("%s also sets %s", codePath, strings.Join(shadow, ", ")),
				remedy: "remove those keys: environment values override claude-mem's own settings file",
			})
			failed = true
		} else {
			checks = append(checks, check{ok: true, label: "no conflicting configuration"})
		}
	}

	// 4. Worker state and the local queue.
	db, err := clientdb.Open(dbPath)
	if err != nil {
		checks = append(checks, check{label: "database readable", detail: err.Error()})
		failed = true
	} else {
		defer func() { _ = db.Close() }()

		worker, answered := probeWorker(workerPort(memDir, settings.Value(current, keyWorkerPort)))
		switch {
		case answered:
			detail := "answering on loopback"
			if worker.Version != "" {
				detail = "claude-mem " + worker.Version
			}
			checks = append(checks, check{ok: true, label: "claude-mem worker running", detail: detail})
			if worker.Version != "" && worker.Version != buildinfo.TestedClaudeMem {
				checks = append(checks, check{
					warn:  true,
					label: "claude-mem version",
					detail: fmt.Sprintf("%s installed, this cmemlan was verified against %s",
						worker.Version, buildinfo.TestedClaudeMem),
				})
			}
		case db.WorkerRunning():
			checks = append(checks, check{ok: true, label: "claude-mem worker running"})
		default:
			checks = append(checks, check{
				warn:   true,
				label:  "claude-mem worker not running",
				detail: "nothing will sync until it starts",
				remedy: "open Claude Code, or run: npx claude-mem start",
			})
		}

		if counts, err := db.Pending(); err == nil {
			detail := fmt.Sprintf("%d waiting to upload", counts.Total())
			if counts.WorkState > 0 {
				detail += fmt.Sprintf(" (%d of them work_state)", counts.WorkState)
			}
			checks = append(checks, check{ok: true, label: "local queue", detail: detail})
		}

		if worker.Sync != nil {
			checks = append(checks, workerSyncChecks(worker.Sync)...)
		}
	}

	// 5. Can we actually reach the hub, and is the key right?
	if hubURL != "" && token != "" && userID != "" {
		ctx, cancel := env.ctx()
		defer cancel()

		res, err := env.hub().Status(ctx, hubURL, userID, token, settings.Value(current, settings.DeviceIDKey))
		switch {
		case err != nil:
			checks = append(checks, check{
				label:  "hub reachable",
				detail: err.Error(),
				remedy: "check the hub is running (" + hubServiceHint() + ") and reachable from here",
			})
			failed = true
		default:
			checks = append(checks, check{
				ok:     true,
				label:  "hub reachable",
				detail: fmt.Sprintf("epoch %s, head %s", res.Epoch, res.HeadSeq),
			})
			if res.SyncMode == "poll" {
				checks = append(checks, check{ok: true, label: "13.x websocket disabled by the hub (expected; 15.x uses live updates)"})
			}
			if containsString(res.ContentKinds, "work_state") {
				checks = append(checks, check{ok: true, label: "hub accepts work_state"})
			} else {
				checks = append(checks, check{
					warn:   true,
					label:  "hub does not list work_state yet",
					detail: "My Work and to-do lists stay on this machine until it does",
					remedy: "update every paired machine to claude-mem 13.29.0 or later, then see `cmemlan devices`",
				})
			}
		}
	}

	for _, c := range checks {
		c.write(env.Stdout)
	}
	if failed {
		fmt.Fprintln(env.Stdout, "\nSome checks failed. Fix the items marked ✗ and run doctor again.")
		return 1
	}
	fmt.Fprintln(env.Stdout, "\nEverything looks healthy.")
	return 0
}

// workerSyncChecks turns the worker's own sync status into doctor lines.
func workerSyncChecks(s *workerSync) []check {
	var out []check
	if s.LastError != "" {
		out = append(out, check{
			label:  "worker sync",
			detail: "last error: " + s.LastError,
			remedy: "run doctor again in a minute; if it persists, check the hub log",
		})
	} else {
		out = append(out, check{ok: true, label: "worker sync", detail: "no error reported"})
	}
	if q := s.Quarantined + s.PullQuarantined; q > 0 {
		out = append(out, check{
			warn:   true,
			label:  "sync quarantine",
			detail: fmt.Sprintf("%d push and %d pull op(s) set aside by claude-mem", s.Quarantined, s.PullQuarantined),
		})
	}
	if s.PendingWorkState > 0 && !containsString(s.ContentKinds, "work_state") {
		out = append(out, check{
			warn:   true,
			label:  "work_state waiting",
			detail: fmt.Sprintf("%d entries held until the hub lists work_state", s.PendingWorkState),
		})
	}
	return out
}

// hubServiceHint names how the hub runs on this platform, for remedies.
func hubServiceHint() string {
	switch runtime.GOOS {
	case "windows":
		return "Get-ScheduledTask " + windowsTaskName
	case "darwin":
		return "launchctl print gui/$(id -u)/dev.andrhamm.cmemlan"
	default:
		return "systemctl --user status cmemlan"
	}
}

// shadowingKeys reports sync keys set in Claude Code's settings environment
// block, which override claude-mem's own settings file.
func shadowingKeys(path string) []string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var parsed struct {
		Env map[string]json.RawMessage `json:"env"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil
	}
	var out []string
	for _, k := range []string{keyHubURL, keyToken, keyUserID, keyWS, settings.DeviceIDKey} {
		if _, ok := parsed.Env[k]; ok {
			out = append(out, k)
		}
	}
	return out
}
