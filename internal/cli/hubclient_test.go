package cli

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/andrhamm/claude-mem-lan-sync/internal/hub"
	"github.com/andrhamm/claude-mem-lan-sync/internal/store"
)

func realHub(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "hub.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := hub.New(st, hub.Options{
		UserID: st.UserID(),
		Auth:   hub.StaticToken{UserID: st.UserID(), Token: "k"},
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, st
}

// Issue #7: the probe used to omit X-Device-Id, and the hub refuses that with a
// 400 before it checks the key, so doctor failed against a healthy hub.
func TestStatusProbeReachesARealHub(t *testing.T) {
	ts, st := realHub(t)
	for _, device := range []string{"", "cde83933-a887-496c-9dbe-710da8d2de07"} {
		res, err := newHubClient().Status(context.Background(), ts.URL, st.UserID(), "k", device)
		if err != nil {
			t.Fatalf("device %q: %v", device, err)
		}
		if res.Epoch != st.Epoch().String() {
			t.Errorf("epoch = %q", res.Epoch)
		}
		if res.ContentKinds == nil {
			t.Errorf("content_kinds missing from the status response")
		}
	}
}

// A diagnostic probe must not register a device: an unknown device without the
// quarantine capability would close the work_state gate for everyone.
func TestStatusProbeIsNotRecordedAsADevice(t *testing.T) {
	ts, st := realHub(t)
	if _, err := newHubClient().Status(context.Background(), ts.URL, st.UserID(), "k", ""); err != nil {
		t.Fatal(err)
	}
	devices, err := st.ListDevices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 0 {
		t.Errorf("probe recorded %d device(s): %+v", len(devices), devices)
	}
}

func TestStatusProbeNamesBadCredentials(t *testing.T) {
	ts, st := realHub(t)
	_, err := newHubClient().Status(context.Background(), ts.URL, st.UserID(), "wrong", "")
	if err == nil {
		t.Fatal("want an error for a wrong key")
	}
	_ = http.StatusUnauthorized
}
