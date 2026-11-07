package hub

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"

	"github.com/andrhamm/claude-mem-lan-sync/internal/store"
)

func newLiveServer(t *testing.T) (*Server, *store.Store, *httptest.Server) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "hub.db"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(st, Options{
		UserID:      st.UserID(),
		Auth:        StaticToken{UserID: st.UserID(), Token: testToken},
		RealtimeKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(func() { srv.CloseRealtime(); ts.Close() })
	return srv, st, ts
}

type grant struct {
	AccessToken string  `json:"access_token"`
	ExpiresAt   float64 `json:"expires_at"`
	RealtimeURL string  `json:"realtime_url"`
	APIKey      string  `json:"apikey"`
	Topic       string  `json:"topic"`
}

func mintGrant(t *testing.T, ts *httptest.Server, userID, device string) grant {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/sync/realtime-token", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("X-User-Id", userID)
	req.Header.Set("X-Device-Id", device)
	req.Header.Set("X-Sync-Capabilities", quarantineCap)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("realtime-token = %d", resp.StatusCode)
	}
	var g grant
	if err := json.NewDecoder(resp.Body).Decode(&g); err != nil {
		t.Fatal(err)
	}
	return g
}

type phx struct {
	Topic   string          `json:"topic"`
	Event   string          `json:"event"`
	Payload json.RawMessage `json:"payload"`
	Ref     *string         `json:"ref"`
	JoinRef *string         `json:"join_ref"`
}

// dial opens the socket the way SyncClient does: realtime_url plus apikey and
// vsn=1.0.0 in the query, no auth headers.
func dial(t *testing.T, g grant) *websocket.Conn {
	t.Helper()
	u := g.RealtimeURL + "?apikey=" + g.APIKey + "&vsn=1.0.0"
	ws, err := websocket.Dial(u, "", "http://localhost/")
	if err != nil {
		t.Fatalf("dial %s: %v", u, err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return ws
}

func send(t *testing.T, ws *websocket.Conn, v map[string]any) {
	t.Helper()
	b, _ := json.Marshal(v)
	if err := websocket.Message.Send(ws, string(b)); err != nil {
		t.Fatal(err)
	}
}

func recv(t *testing.T, ws *websocket.Conn) phx {
	t.Helper()
	_ = ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	var msg string
	if err := websocket.Message.Receive(ws, &msg); err != nil {
		t.Fatalf("receive: %v", err)
	}
	var f phx
	if err := json.Unmarshal([]byte(msg), &f); err != nil {
		t.Fatalf("frame %q: %v", msg, err)
	}
	return f
}

func status(t *testing.T, f phx) string {
	t.Helper()
	var p struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(f.Payload, &p)
	return p.Status
}

func join(t *testing.T, ws *websocket.Conn, g grant) phx {
	t.Helper()
	send(t, ws, map[string]any{
		"topic": "realtime:" + g.Topic, "event": "phx_join", "ref": "1", "join_ref": "1",
		"payload": map[string]any{
			"config": map[string]any{
				"broadcast": map[string]any{"self": false, "ack": false},
				"presence":  map[string]any{"enabled": false},
				"private":   true,
			},
			"access_token": g.AccessToken,
		},
	})
	return recv(t, ws)
}

// SyncClient.fetchRealtimeGrant: every field present with the right type, and
// expires_at a number of unix seconds.
func TestRealtimeTokenShape(t *testing.T) {
	_, st, ts := newLiveServer(t)
	g := mintGrant(t, ts, st.UserID(), "win")
	if g.AccessToken == "" || g.APIKey == "" {
		t.Fatalf("grant = %+v", g)
	}
	if !strings.HasPrefix(g.RealtimeURL, "ws://") || !strings.HasSuffix(g.RealtimeURL, RealtimePath) {
		t.Errorf("realtime_url = %q", g.RealtimeURL)
	}
	if g.Topic != "user:"+st.UserID() {
		t.Errorf("topic = %q", g.Topic)
	}
	if left := g.ExpiresAt - float64(time.Now().Unix()); left < 3000 || left > 3700 {
		t.Errorf("expires_at %v is %vs away, want about an hour", g.ExpiresAt, left)
	}
}

// Without a key the hub has no Realtime, and a 404 is what keeps the client
// polling quietly (it re-probes hourly).
func TestRealtimeTokenIs404WhenDisabled(t *testing.T) {
	srv, _ := newTestServer(t)
	w := request(t, srv, http.MethodPost, "/v1/sync/realtime-token", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestRealtimeJoinHeartbeatAndAdvance(t *testing.T) {
	srv, st, ts := newLiveServer(t)
	mac := mintGrant(t, ts, st.UserID(), "mac")
	ws := dial(t, mac)

	if r := join(t, ws, mac); r.Event != "phx_reply" || status(t, r) != "ok" || r.Ref == nil || *r.Ref != "1" {
		t.Fatalf("join reply = %+v %s", r, r.Payload)
	}

	send(t, ws, map[string]any{"topic": "phoenix", "event": "heartbeat", "payload": map[string]any{}, "ref": "2"})
	if r := recv(t, ws); r.Topic != "phoenix" || r.Event != "phx_reply" || *r.Ref != "2" || status(t, r) != "ok" {
		t.Fatalf("heartbeat reply = %+v", r)
	}

	// Another device pushes: the joined Mac hears an advance.
	w := request(t, srv, http.MethodPost, "/v1/sync/ops", pushBody(t, makeWrapper(t, "1", "1")),
		withDevice("win", quarantineCap))
	if w.Code != http.StatusOK {
		t.Fatalf("push = %d", w.Code)
	}
	r := recv(t, ws)
	if r.Event != "broadcast" || r.Topic != "realtime:"+mac.Topic {
		t.Fatalf("frame = %+v", r)
	}
	var b struct {
		Event   string `json:"event"`
		Payload struct {
			Epoch   string `json:"epoch"`
			HeadSeq string `json:"head_seq"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(r.Payload, &b); err != nil {
		t.Fatal(err)
	}
	if b.Event != "advance" || b.Payload.Epoch != st.Epoch().String() || b.Payload.HeadSeq != "1" {
		t.Fatalf("advance = %s", r.Payload)
	}
}

// The pusher learns its head from the push response; it gets no echo.
func TestRealtimeDoesNotEchoToThePusher(t *testing.T) {
	srv, st, ts := newLiveServer(t)
	win := mintGrant(t, ts, st.UserID(), "win")
	ws := dial(t, win)
	if r := join(t, ws, win); status(t, r) != "ok" {
		t.Fatal("join refused")
	}
	request(t, srv, http.MethodPost, "/v1/sync/ops", pushBody(t, makeWrapper(t, "1", "1")),
		withDevice("win", quarantineCap))

	send(t, ws, map[string]any{"topic": "phoenix", "event": "heartbeat", "payload": map[string]any{}, "ref": "9"})
	if r := recv(t, ws); r.Event != "phx_reply" || *r.Ref != "9" {
		t.Fatalf("expected only the heartbeat reply, got %+v %s", r, r.Payload)
	}
}

func TestRealtimeJoinRefusesBadTokens(t *testing.T) {
	srv, st, ts := newLiveServer(t)
	g := mintGrant(t, ts, st.UserID(), "mac")

	for name, token := range map[string]string{
		"empty":    "",
		"garbage":  "abc.def",
		"tampered": g.AccessToken[:len(g.AccessToken)-2] + "xx",
	} {
		ws := dial(t, g)
		bad := g
		bad.AccessToken = token
		if r := join(t, ws, bad); status(t, r) != "error" {
			t.Errorf("%s token joined: %s", name, r.Payload)
		}
	}

	// A revoked device cannot join with a token minted before revocation.
	if err := st.RevokeDevice(t.Context(), "mac"); err != nil {
		t.Fatal(err)
	}
	ws := dial(t, g)
	if r := join(t, ws, g); status(t, r) != "error" {
		t.Errorf("revoked device joined")
	}
	_ = srv
}

func TestRealtimeTokenRefreshAndExpiry(t *testing.T) {
	srv, st, ts := newLiveServer(t)
	g := mintGrant(t, ts, st.UserID(), "mac")
	ws := dial(t, g)
	if r := join(t, ws, g); status(t, r) != "ok" {
		t.Fatal("join refused")
	}

	fresh := mintGrant(t, ts, st.UserID(), "mac")
	send(t, ws, map[string]any{"topic": "realtime:" + g.Topic, "event": "access_token",
		"payload": map[string]any{"access_token": fresh.AccessToken}, "ref": "3", "join_ref": "1"})
	if r := recv(t, ws); r.Event != "phx_reply" || status(t, r) != "ok" {
		t.Fatalf("refresh reply = %+v %s", r, r.Payload)
	}

	// Two hours on with no refresh: the next heartbeat ends the channel with a
	// system error, which the client heals from by reconnecting.
	srv.opts.Now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	send(t, ws, map[string]any{"topic": "phoenix", "event": "heartbeat", "payload": map[string]any{}, "ref": "4"})
	if r := recv(t, ws); r.Event != "system" || status(t, r) != "error" {
		t.Fatalf("expected a system error, got %+v %s", r, r.Payload)
	}
}

func TestRealtimeSocketDoesNotUseRequestSlots(t *testing.T) {
	srv, st, ts := newLiveServer(t)
	srv.inflight = make(chan struct{}, 1)
	g := mintGrant(t, ts, st.UserID(), "mac")
	ws := dial(t, g)
	if r := join(t, ws, g); status(t, r) != "ok" {
		t.Fatal("join refused")
	}
	// The open socket holds no slot, so an ordinary request still gets in.
	if w := request(t, srv, http.MethodGet, "/v1/sync/status", nil); w.Code != http.StatusOK {
		t.Fatalf("status with a socket open = %d", w.Code)
	}
	if open, joined := srv.RealtimeConns(); open != 1 || joined != 1 {
		t.Errorf("conns = %d open, %d joined", open, joined)
	}
}
