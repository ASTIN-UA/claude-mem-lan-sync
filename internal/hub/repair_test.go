package hub

import (
	"encoding/json"
	"net/http"
	"testing"
)

// repairRequest mirrors SyncConflictRecovery.request: POST, JSON, the auth
// headers and X-Device-Id, and nothing else (no capabilities, no name).
func repairRequest(t *testing.T, srv *Server, path, device string, body any) (int, map[string]json.RawMessage) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	w := request(t, srv, http.MethodPost, path, raw, func(r *http.Request) {
		r.Header.Set("X-Device-Id", device)
		r.Header.Del("X-Device-Name")
		r.Header.Del("X-Sync-Capabilities")
	})
	var out map[string]json.RawMessage
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

type headDoc struct {
	EntityRev       string `json:"entity_rev"`
	OperationSHA256 string `json:"operation_sha256"`
	Seq             string `json:"seq"`
}

func readHead(t *testing.T, srv *Server, entity string) (headDoc, map[string]json.RawMessage) {
	t.Helper()
	code, out := repairRequest(t, srv, "/v1/sync/repair/head", "device-A", map[string]string{"entity_id": entity})
	if code != http.StatusOK {
		t.Fatalf("repair/head = %d %v", code, out)
	}
	var h headDoc
	if string(out["head"]) != "null" {
		if err := json.Unmarshal(out["head"], &h); err != nil {
			t.Fatal(err)
		}
	}
	return h, out
}

func wrapperOf(t *testing.T, raw json.RawMessage) map[string]string {
	t.Helper()
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// SyncConflictRecovery.hub(): protocol_version 1, the capability name, decimal
// strings, head and op both null or both present.
func TestRepairHeadOfUnknownEntity(t *testing.T) {
	srv, st := newTestServer(t)
	_, out := readHead(t, srv, "observation:none")
	if string(out["protocol_version"]) != "1" || string(out["capability"]) != `"origin-repair-v1"` {
		t.Fatalf("head = %v", out)
	}
	if string(out["epoch"]) != `"`+st.Epoch().String()+`"` || string(out["entity_id"]) != `"observation:none"` {
		t.Fatalf("head = %v", out)
	}
	if string(out["head"]) != "null" || string(out["op"]) != "null" {
		t.Fatalf("head/op = %s %s", out["head"], out["op"])
	}
}

func TestRepairHeadReturnsNewestRevisionVerbatim(t *testing.T) {
	srv, _ := newTestServer(t)
	for _, rev := range []string{"2", "10", "9"} { // 10 is newest despite arriving before 9
		if w := request(t, srv, http.MethodPost, "/v1/sync/ops", pushBody(t, makeWrapper(t, "5", rev))); w.Code != 200 {
			t.Fatalf("push = %d", w.Code)
		}
	}
	h, out := readHead(t, srv, "observation:5")
	if h.EntityRev != "10" || h.Seq != "2" {
		t.Fatalf("head = %+v", h)
	}
	op := wrapperOf(t, out["op"])
	sent := wrapperOf(t, makeWrapper(t, "5", "10"))
	if op["body"] != sent["body"] || op["operation_sha256"] != sent["operation_sha256"] || h.OperationSHA256 != sent["operation_sha256"] {
		t.Fatal("op is not the stored wrapper")
	}
}

func TestRepairAppendsAndRetriesExactly(t *testing.T) {
	srv, st := newTestServer(t)
	request(t, srv, http.MethodPost, "/v1/sync/ops", pushBody(t, makeWrapper(t, "5", "3")))
	h, _ := readHead(t, srv, "observation:5")

	body := map[string]any{
		"repair_id": "abc123", "expected_epoch": st.Epoch().String(),
		"expected_head": h, "op": makeWrapper(t, "5", "4"),
	}
	code, out := repairRequest(t, srv, "/v1/sync/repair", "device-A", body)
	if code != http.StatusOK {
		t.Fatalf("repair = %d %v", code, out)
	}
	if string(out["protocol_version"]) != "1" || string(out["capability"]) != `"origin-repair-v1"` ||
		string(out["epoch"]) != `"`+st.Epoch().String()+`"` || string(out["head_seq"]) != `"2"` {
		t.Fatalf("repair response = %v", out)
	}
	var acked []struct {
		ID              string  `json:"id"`
		Kind            string  `json:"kind"`
		OriginLocalID   *string `json:"origin_local_id"`
		EntityRev       string  `json:"entity_rev"`
		OperationSHA256 string  `json:"operation_sha256"`
		Seq             string  `json:"seq"`
	}
	if err := json.Unmarshal(out["acked"], &acked); err != nil || len(acked) != 1 {
		t.Fatalf("acked = %s", out["acked"])
	}
	a := acked[0]
	sent := wrapperOf(t, makeWrapper(t, "5", "4"))
	if a.ID != "observation:5" || a.Kind != "observation" || a.OriginLocalID == nil || *a.OriginLocalID != "5" ||
		a.EntityRev != "4" || a.OperationSHA256 != sent["operation_sha256"] || a.Seq != "2" {
		t.Fatalf("ack = %+v", a)
	}

	// The response was lost; the client retries the identical request after its
	// own repair already became the head. Same ack, nothing appended.
	code, out = repairRequest(t, srv, "/v1/sync/repair", "device-A", body)
	if code != http.StatusOK || string(out["head_seq"]) != `"2"` {
		t.Fatalf("retry = %d %v", code, out)
	}
	if head, _ := st.HeadSeq(t.Context()); head != 2 {
		t.Fatalf("retry appended: head %d", head)
	}

	// Reusing the id for a different op is a conflict, not a second write.
	body["op"] = makeWrapper(t, "5", "6")
	if code, _ := repairRequest(t, srv, "/v1/sync/repair", "device-A", body); code != http.StatusConflict {
		t.Fatalf("reused repair_id = %d, want 409", code)
	}

	// The repair reaches pullers like any op.
	w := request(t, srv, http.MethodGet, "/v1/sync/changes?since=1&limit=500", nil)
	if w.Code != 200 || !json.Valid(w.Body.Bytes()) {
		t.Fatalf("changes = %d", w.Code)
	}
}

func TestRepairPreconditions(t *testing.T) {
	srv, st := newTestServer(t)
	request(t, srv, http.MethodPost, "/v1/sync/ops", pushBody(t, makeWrapper(t, "5", "3")))
	h, _ := readHead(t, srv, "observation:5")
	epoch := st.Epoch().String()

	cases := []struct {
		name   string
		device string
		body   map[string]any
		want   int
	}{
		{"stale epoch", "device-A", map[string]any{"repair_id": "r1", "expected_epoch": "1",
			"expected_head": h, "op": makeWrapper(t, "5", "4")}, http.StatusConflict},
		{"head moved", "device-A", map[string]any{"repair_id": "r2", "expected_epoch": epoch,
			"expected_head": headDoc{EntityRev: "2", OperationSHA256: h.OperationSHA256, Seq: h.Seq},
			"op":            makeWrapper(t, "5", "4")}, http.StatusConflict},
		{"expected none but one exists", "device-A", map[string]any{"repair_id": "r3", "expected_epoch": epoch,
			"expected_head": nil, "op": makeWrapper(t, "5", "4")}, http.StatusConflict},
		{"revision not higher", "device-A", map[string]any{"repair_id": "r4", "expected_epoch": epoch,
			"expected_head": h, "op": makeWrapper(t, "5", "3")}, http.StatusConflict},
		{"someone else's entity", "device-B", map[string]any{"repair_id": "r5", "expected_epoch": epoch,
			"expected_head": h, "op": makeWrapper(t, "5", "4")}, http.StatusForbidden},
		{"bad repair id", "device-A", map[string]any{"repair_id": "../x", "expected_epoch": epoch,
			"expected_head": h, "op": makeWrapper(t, "5", "4")}, http.StatusBadRequest},
	}
	for _, c := range cases {
		code, out := repairRequest(t, srv, "/v1/sync/repair", c.device, c.body)
		if code != c.want {
			t.Errorf("%s: %d %v, want %d", c.name, code, out, c.want)
		}
	}
	if head, _ := st.HeadSeq(t.Context()); head != 1 {
		t.Fatalf("a refused repair appended: head %d", head)
	}

	// No head yet, none expected: allowed.
	code, out := repairRequest(t, srv, "/v1/sync/repair", "device-A", map[string]any{
		"repair_id": "r6", "expected_epoch": epoch, "expected_head": nil, "op": makeWrapper(t, "7", "1")})
	if code != http.StatusOK {
		t.Fatalf("first-revision repair = %d %v", code, out)
	}
}

// Repair requests carry no X-Sync-Capabilities; recording them would close the
// work_state gate every time the user repairs a conflict.
func TestRepairDoesNotTouchDeviceCapabilities(t *testing.T) {
	srv, st := newTestServer(t)
	contentKinds(t, srv, withDevice("device-A", quarantineCap))
	readHead(t, srv, "observation:1")
	devices, err := st.ListDevices(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 1 || devices[0].Capabilities != quarantineCap {
		t.Fatalf("devices = %+v", devices)
	}
	if !has(contentKinds(t, srv, withDevice("device-A", quarantineCap)), "work_state") {
		t.Fatal("gate closed by a repair request")
	}
}

func TestRepairAnnouncesToLiveSockets(t *testing.T) {
	srv, st, ts := newLiveServer(t)
	mac := mintGrant(t, ts, st.UserID(), "mac")
	ws := dial(t, mac)
	if r := join(t, ws, mac); status(t, r) != "ok" {
		t.Fatal("join refused")
	}
	code, out := repairRequest(t, srv, "/v1/sync/repair", "device-A", map[string]any{
		"repair_id": "live1", "expected_epoch": st.Epoch().String(), "expected_head": nil,
		"op": makeWrapper(t, "9", "1")})
	if code != http.StatusOK {
		t.Fatalf("repair = %d %v", code, out)
	}
	if r := recv(t, ws); r.Event != "broadcast" {
		t.Fatalf("frame = %+v", r)
	}
}

