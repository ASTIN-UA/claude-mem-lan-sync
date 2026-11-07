package clientdb

import "testing"

// work_state_entries as claude-mem 15.11.1 creates it (read from a live
// database; the captured 13.15.0 schema in testdata predates the table).
const workStateTable = `CREATE TABLE work_state_entries (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      project TEXT NOT NULL,
      list_name TEXT NOT NULL,
      fields TEXT NOT NULL,
      created_at TEXT NOT NULL,
      created_at_epoch INTEGER NOT NULL
    , synced_at INTEGER, origin_device_id TEXT, origin_local_id TEXT, sync_rev TEXT NOT NULL DEFAULT '1')`

func TestPendingWithoutWorkStateTable(t *testing.T) {
	d := newClientDB(t)
	c, err := d.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if c.WorkState != 0 {
		t.Errorf("WorkState = %d on a 13.x schema", c.WorkState)
	}
}

func TestPendingCountsLocalUnsyncedWorkState(t *testing.T) {
	d := newClientDB(t)
	if _, err := d.db.Exec(workStateTable); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		// local, unsynced: counts
		`INSERT INTO work_state_entries (project, list_name, fields, created_at, created_at_epoch)
		 VALUES ('p', 'l', '{}', 't', 1)`,
		`INSERT INTO work_state_entries (project, list_name, fields, created_at, created_at_epoch)
		 VALUES ('p', 'l', '{}', 't', 2)`,
		// already synced: does not count
		`INSERT INTO work_state_entries (project, list_name, fields, created_at, created_at_epoch, synced_at)
		 VALUES ('p', 'l', '{}', 't', 3, 99)`,
		// pulled from another device: does not count
		`INSERT INTO work_state_entries (project, list_name, fields, created_at, created_at_epoch, origin_device_id)
		 VALUES ('p', 'l', '{}', 't', 4, 'mac')`,
	} {
		if _, err := d.db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	c, err := d.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if c.WorkState != 2 {
		t.Errorf("WorkState = %d, want 2", c.WorkState)
	}
	if c.Total() != c.Observations+c.Summaries+c.Prompts+2 {
		t.Errorf("Total %d does not include work_state", c.Total())
	}
}
