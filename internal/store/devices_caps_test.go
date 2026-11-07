package store

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// A hub database written by cmemlan 0.1.0 (schema v1) upgrades in place, keeps
// its devices, and starts with no capabilities recorded.
func TestMigratesV1DatabaseInPlace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub.db")
	db, err := sql.Open("sqlite", dsn(path, "immediate"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schemaV1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO devices (user_id, device_id, device_name, first_seen, last_seen)
		VALUES ('u', 'mac', 'MacBook', 1, 2)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	s, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Open on a v1 database: %v", err)
	}
	defer func() { _ = s.Close() }()

	var version int
	if err := s.w.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 2 {
		t.Fatalf("user_version = %d, %v", version, err)
	}
	var caps sql.NullString
	if err := s.w.QueryRow(`SELECT capabilities FROM devices WHERE device_id = 'mac'`).Scan(&caps); err != nil {
		t.Fatal(err)
	}
	if caps.Valid {
		t.Errorf("migrated device has capabilities %q, want NULL", caps.String)
	}
}

func TestSeenDeviceRecordsAndClearsCapabilities(t *testing.T) {
	s, _ := tempStore(t)
	ctx := t.Context()
	now := time.UnixMilli(1_800_000_000_000)
	s.SetClock(func() time.Time { return now })

	if err := s.SeenDevice(ctx, "win", "PC", "quarantine-unknown-kinds"); err != nil {
		t.Fatal(err)
	}
	ok, laggards, err := s.AllActiveDevicesHave(ctx, CapabilityQuarantineUnknownKinds, now.Add(-time.Hour))
	if err != nil || !ok || len(laggards) != 0 {
		t.Fatalf("one capable device: ok=%v laggards=%v err=%v", ok, laggards, err)
	}

	if err := s.SeenDevice(ctx, "mac", "Mac", ""); err != nil {
		t.Fatal(err)
	}
	ok, laggards, _ = s.AllActiveDevicesHave(ctx, CapabilityQuarantineUnknownKinds, now.Add(-time.Hour))
	if ok || len(laggards) != 1 || laggards[0].ID != "mac" {
		t.Fatalf("old mac: ok=%v laggards=%+v", ok, laggards)
	}

	// The capability list is comma-separated; other entries are fine.
	if err := s.SeenDevice(ctx, "mac", "Mac", "something-else, quarantine-unknown-kinds"); err != nil {
		t.Fatal(err)
	}
	if ok, _, _ := s.AllActiveDevicesHave(ctx, CapabilityQuarantineUnknownKinds, now.Add(-time.Hour)); !ok {
		t.Fatal("capability inside a list was not recognised")
	}
}

func TestEmptyHubKeepsGateShut(t *testing.T) {
	s, _ := tempStore(t)
	if ok, _, _ := s.AllActiveDevicesHave(t.Context(), CapabilityQuarantineUnknownKinds, time.Time{}); ok {
		t.Fatal("no devices must not count as everyone capable")
	}
}
