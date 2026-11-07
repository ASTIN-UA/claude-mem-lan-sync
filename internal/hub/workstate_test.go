package hub

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/andrhamm/claude-mem-lan-sync/internal/proto"
)

const quarantineCap = "quarantine-unknown-kinds"

// makeWorkStateWrapper builds a work_state op shaped like claude-mem 15.11.1's
// CanonicalContent.ts: the twelve envelope keys, a payload of exactly
// created_at, created_at_epoch, fields, list_name, project.
func makeWorkStateWrapper(t *testing.T, device, localID string) json.RawMessage {
	t.Helper()
	payload := `{"created_at":"2026-11-07T15:00:00.000Z","created_at_epoch":"1794063600000",` +
		`"fields":{"status":"todo","task":"ship <cmemlan> & co"},"list_name":"release","project":"cmemlan"}`
	body := fmt.Sprintf(
		`{"body_schema_version":1,"deleted":false,"deleted_at":null,"entity_rev":"1",`+
			`"id":"%s","kind":"work_state","mutation":null,"origin_device_id":"%s",`+
			`"origin_local_id":"%s","payload":%s,"payload_schema_version":2,"payload_sha256":"%s"}`,
		workStateID(device, localID), device, localID, payload, proto.Digest([]byte(payload)))
	// Encoded without HTML escaping, the way the client's JSON.stringify sends
	// it, so the round-trip check below sees a raw < > & in the literal.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(map[string]string{
		"body":             body,
		"operation_sha256": proto.Digest([]byte(body)),
	}); err != nil {
		t.Fatal(err)
	}
	return json.RawMessage(bytes.TrimSpace(buf.Bytes()))
}

// workStateID is CanonicalContent.ts stableDocumentId for kind work_state.
func workStateID(device, localID string) string {
	input, _ := json.Marshal([]string{"cmem-doc-id-v1", "device", "work_state", device, localID})
	return "work_state:" + proto.Digest(input)
}

func withDevice(id, caps string) func(*http.Request) {
	return func(r *http.Request) {
		r.Header.Set("X-Device-Id", id)
		r.Header.Set("X-Device-Name", id)
		if caps == "" {
			r.Header.Del("X-Sync-Capabilities")
		} else {
			r.Header.Set("X-Sync-Capabilities", caps)
		}
	}
}

func contentKinds(t *testing.T, srv *Server, mutate ...func(*http.Request)) []string {
	t.Helper()
	w := request(t, srv, http.MethodGet, "/v1/sync/status", nil, mutate...)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body)
	}
	var got struct {
		ContentKinds *[]string `json:"content_kinds"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ContentKinds == nil {
		t.Fatalf("status has no content_kinds: %s", w.Body)
	}
	return *got.ContentKinds
}

func has(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// The client reads content_kinds as an array of strings and pushes work_state
// only when it is listed (CloudSync.runHubStatusProbe, hubAcceptsKind).
func TestStatusListsLaunchKinds(t *testing.T) {
	srv, _ := newTestServer(t)
	kinds := contentKinds(t, srv, withDevice("old-mac", ""))
	for _, k := range []string{"observation", "summary", "prompt"} {
		if !has(kinds, k) {
			t.Errorf("content_kinds %v lacks %s", kinds, k)
		}
	}
	if has(kinds, "work_state") {
		t.Errorf("work_state listed for a device without %s: %v", quarantineCap, kinds)
	}
}

func TestWorkStateGateOpensWhenEveryActiveDeviceCanQuarantine(t *testing.T) {
	srv, _ := newTestServer(t)

	if !has(contentKinds(t, srv, withDevice("win", quarantineCap)), "work_state") {
		t.Fatal("a lone capable device should see work_state")
	}

	// An old build appears: claude-mem 13.17.2 sends no capabilities and would
	// retry a page with an unknown kind forever.
	contentKinds(t, srv, withDevice("mac", ""))
	if has(contentKinds(t, srv, withDevice("win", quarantineCap)), "work_state") {
		t.Fatal("work_state must be held back while an old device is active")
	}

	// The Mac updates; its next request carries the capability.
	contentKinds(t, srv, withDevice("mac", "quarantine-unknown-kinds"))
	if !has(contentKinds(t, srv, withDevice("win", quarantineCap)), "work_state") {
		t.Fatal("work_state should open once every device declared the capability")
	}

	// Downgrading closes it again.
	contentKinds(t, srv, withDevice("mac", ""))
	if has(contentKinds(t, srv, withDevice("win", quarantineCap)), "work_state") {
		t.Fatal("a downgraded device must close the gate")
	}
}

func TestWorkStateGateIgnoresRevokedAndStaleDevices(t *testing.T) {
	srv, st := newTestServer(t)
	start := time.UnixMilli(1755300000000)
	st.SetClock(func() time.Time { return start })

	contentKinds(t, srv, withDevice("old", ""))
	if err := st.RevokeDevice(t.Context(), "old"); err != nil {
		t.Fatal(err)
	}
	if !has(contentKinds(t, srv, withDevice("win", quarantineCap)), "work_state") {
		t.Fatal("a revoked device must not hold the gate")
	}

	// A machine on an old build, last seen 31 days ago, no longer holds the
	// gate under the default 30-day window; one seen yesterday still does.
	contentKinds(t, srv, withDevice("stale", ""))
	later := start.Add(31 * 24 * time.Hour)
	st.SetClock(func() time.Time { return later })
	srv.opts.Now = func() time.Time { return later }
	if !has(contentKinds(t, srv, withDevice("win", quarantineCap)), "work_state") {
		t.Fatal("a device silent for longer than the window must not hold the gate")
	}
	contentKinds(t, srv, withDevice("stale", ""))
	if has(contentKinds(t, srv, withDevice("win", quarantineCap)), "work_state") {
		t.Fatal("the stale device came back on an old build; the gate must close")
	}
}

func TestWorkStateModesOverrideTheGate(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.opts.WorkState = WorkStateOn
	if !has(contentKinds(t, srv, withDevice("mac", "")), "work_state") {
		t.Error("--work-state on must always list work_state")
	}
	srv.opts.WorkState = WorkStateOff
	if has(contentKinds(t, srv, withDevice("win", quarantineCap)), "work_state") {
		t.Error("--work-state off must never list work_state")
	}
}

// A diagnostic probe must neither register a device nor change one's
// capabilities: doctor running on the Mac would otherwise close the gate.
func TestProbeDoesNotRecordTheDevice(t *testing.T) {
	srv, st := newTestServer(t)
	contentKinds(t, srv, withDevice("win", quarantineCap))
	probe := func(r *http.Request) {
		withDevice("win", "")(r)
		r.Header.Set(ProbeHeader, "1")
	}
	if !has(contentKinds(t, srv, probe), "work_state") {
		t.Error("a probe should see the gate as the real client left it")
	}
	devices, err := st.ListDevices(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 1 || devices[0].Capabilities != quarantineCap {
		t.Errorf("devices after probe = %+v", devices)
	}
}

// work_state is accepted, acked, stored, and served back byte for byte, like
// the launch kinds.
func TestWorkStateRoundTrip(t *testing.T) {
	srv, _ := newTestServer(t)
	op := makeWorkStateWrapper(t, "device-A", "17")

	// Built by hand: json.Marshal would HTML-escape the RawMessage.
	raw := []byte(`{"protocol_version":2,"ops":[` + string(op) + `]}`)
	w := request(t, srv, http.MethodPost, "/v1/sync/ops", raw,
		withDevice("device-A", quarantineCap))
	if w.Code != http.StatusOK {
		t.Fatalf("push = %d, body %s", w.Code, w.Body)
	}
	var push struct {
		Acked []struct {
			ID            string  `json:"id"`
			Kind          string  `json:"kind"`
			OriginLocalID *string `json:"origin_local_id"`
		} `json:"acked"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &push); err != nil {
		t.Fatal(err)
	}
	if len(push.Acked) != 1 || push.Acked[0].Kind != "work_state" ||
		push.Acked[0].ID != workStateID("device-A", "17") ||
		push.Acked[0].OriginLocalID == nil || *push.Acked[0].OriginLocalID != "17" {
		t.Fatalf("ack = %s", w.Body)
	}

	w = request(t, srv, http.MethodGet, "/v1/sync/changes?since=0&limit=500", nil,
		withDevice("device-B", quarantineCap))
	if w.Code != http.StatusOK {
		t.Fatalf("changes = %d", w.Code)
	}
	var page struct {
		Ops []struct {
			Body            string `json:"body"`
			OperationSHA256 string `json:"operation_sha256"`
		} `json:"ops"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	var sent map[string]string
	if err := json.Unmarshal(op, &sent); err != nil {
		t.Fatal(err)
	}
	if len(page.Ops) != 1 || page.Ops[0].Body != sent["body"] ||
		page.Ops[0].OperationSHA256 != sent["operation_sha256"] {
		t.Fatalf("changes did not return the op verbatim: %s", w.Body)
	}
	// The body literal must keep <, > and & as sent: the client re-canonicalises
	// and compares the raw string.
	if !strings.Contains(w.Body.String(), `ship <cmemlan> & co`) {
		t.Errorf("body was re-encoded: %s", w.Body)
	}
}

// Even with the gate shut, a work_state push must land: a 400 would park the op
// at the head of the sender's outbox forever.
func TestWorkStateAcceptedWhileGateClosed(t *testing.T) {
	srv, _ := newTestServer(t)
	srv.opts.WorkState = WorkStateOff
	w := request(t, srv, http.MethodPost, "/v1/sync/ops",
		pushBody(t, makeWorkStateWrapper(t, "device-A", "1")))
	if w.Code != http.StatusOK {
		t.Fatalf("push = %d, body %s", w.Code, w.Body)
	}
}
