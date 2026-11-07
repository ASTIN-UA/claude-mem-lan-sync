package hub

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/websocket"

	"github.com/andrhamm/claude-mem-lan-sync/internal/proto"
)

// Live updates, the way claude-mem 15.x expects them from the hosted hub.
//
// The client (SyncClient.ts) mints short-lived credentials with
// POST /v1/sync/realtime-token, opens a WebSocket to the returned
// realtime_url speaking the Phoenix protocol (vsn 1.0.0, the one Supabase
// Realtime uses), joins the private broadcast channel `realtime:user:<id>`
// and from then on waits for {event:"advance", payload:{epoch, head_seq}}.
// An advance only triggers its ordinary HTTP pull: nothing durable rides the
// socket, so a lost frame costs at most one poll interval and the cursor is
// never touched here. That is why this file can afford to be small.
//
// The WebSocket request carries no auth headers (it is a plain
// `new WebSocket(url)`), so the channel join is what authenticates: its
// access_token is an HMAC over (user, device, expiry) keyed from the
// pre-shared key, verified on join, on every token refresh and on heartbeats.

// RealtimePath is where the socket is served. It mirrors Supabase's
// /realtime/v1/websocket so a log line reads the same against either hub.
const RealtimePath = "/realtime/v1/websocket"

// realtimeAPIKey is returned because the client requires a non-empty apikey
// and appends it to the URL. It is not a secret and the hub ignores it.
const realtimeAPIKey = "cmemlan"

const (
	// realtimeTokenTTL: the client re-mints at ~80% of the lifetime and hands
	// the new token to the open channel, so an hour means one refresh call
	// every 48 minutes per device.
	realtimeTokenTTL = time.Hour
	// realtimeReadTimeout: the client sends a heartbeat at most every 25s and
	// drops the socket itself after one unanswered beat.
	realtimeReadTimeout  = 75 * time.Second
	realtimeWriteTimeout = 10 * time.Second
	realtimeMaxFrame     = 64 << 10
	realtimeSendBuffer   = 32
	// DefaultMaxRealtimeConns bounds open sockets; each paired machine holds
	// one while a session is active.
	DefaultMaxRealtimeConns = 64
)

// phxFrame is a Phoenix vsn 1.0.0 frame in object form.
type phxFrame struct {
	Topic   string          `json:"topic"`
	Event   string          `json:"event"`
	Payload json.RawMessage `json:"payload"`
	Ref     *string         `json:"ref"`
	JoinRef *string         `json:"join_ref,omitempty"`
}

type rtClaims struct {
	User   string `json:"u"`
	Device string `json:"d"`
	Exp    int64  `json:"e"` // unix seconds
}

// realtime is the set of open sockets.
type realtime struct {
	mu    sync.Mutex
	conns map[*rtConn]struct{}
}

type rtConn struct {
	ws     *websocket.Conn
	send   chan []byte
	done   chan struct{}
	once   sync.Once
	joined atomic.Bool
	device atomic.Value // string, set on join
	exp    atomic.Int64 // unix seconds of the current token
}

func (c *rtConn) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.ws.Close()
	})
}

// enqueue never blocks the caller: a socket too slow to take an advance is
// dropped, and its client heals by reconnecting and pulling over HTTP.
func (c *rtConn) enqueue(b []byte) {
	select {
	case c.send <- b:
	case <-c.done:
	default:
		c.close()
	}
}

// sendFinal writes a last frame synchronously, before the socket is closed;
// queued, it would race the close. websocket.Conn serialises its writers.
func (c *rtConn) sendFinal(b []byte) {
	_ = c.ws.SetWriteDeadline(time.Now().Add(realtimeWriteTimeout))
	_ = websocket.Message.Send(c.ws, string(b))
}

func (s *Server) realtimeEnabled() bool { return len(s.opts.RealtimeKey) > 0 }

func (s *Server) realtimeTopic() string { return "realtime:user:" + s.opts.UserID }

// mintRealtimeToken signs (user, device, expiry).
func (s *Server) mintRealtimeToken(device string, exp time.Time) string {
	claims, _ := json.Marshal(rtClaims{User: s.opts.UserID, Device: device, Exp: exp.Unix()})
	body := base64.RawURLEncoding.EncodeToString(claims)
	mac := hmac.New(sha256.New, s.opts.RealtimeKey)
	mac.Write([]byte(body))
	return body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

var errRealtimeToken = errors.New("invalid or expired access token")

// verifyRealtimeToken checks signature, user, expiry and revocation.
func (s *Server) verifyRealtimeToken(ctx context.Context, token string) (rtClaims, error) {
	body, sig, ok := strings.Cut(token, ".")
	if !ok || !s.realtimeEnabled() {
		return rtClaims{}, errRealtimeToken
	}
	want := hmac.New(sha256.New, s.opts.RealtimeKey)
	want.Write([]byte(body))
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil || !hmac.Equal(got, want.Sum(nil)) {
		return rtClaims{}, errRealtimeToken
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return rtClaims{}, errRealtimeToken
	}
	var c rtClaims
	if err := json.Unmarshal(raw, &c); err != nil || c.User != s.opts.UserID || c.Device == "" {
		return rtClaims{}, errRealtimeToken
	}
	if s.opts.Now().Unix() >= c.Exp {
		return rtClaims{}, errRealtimeToken
	}
	revoked, err := s.st.DeviceRevoked(ctx, c.Device)
	if err != nil || revoked {
		return rtClaims{}, errRealtimeToken
	}
	return c, nil
}

// handleRealtimeToken answers POST /v1/sync/realtime-token.
//
// Field set and types are SyncClient.fetchRealtimeGrant's: access_token and
// apikey non-empty strings, expires_at a finite number of unix seconds,
// realtime_url ws:// or wss://, topic without the "realtime:" prefix.
func (s *Server) handleRealtimeToken(w http.ResponseWriter, r *http.Request) {
	if !s.realtimeEnabled() {
		// A 404 is the client's "this server has no Realtime": it stays on
		// polling and re-probes hourly.
		s.writeError(w, http.StatusNotFound, proto.ReasonBadRequest)
		return
	}
	device := r.Header.Get("X-Device-Id")
	exp := s.opts.Now().Add(realtimeTokenTTL)

	var buf bytes.Buffer
	buf.WriteString(`{"access_token":`)
	writeJSONString(&buf, s.mintRealtimeToken(device, exp))
	buf.WriteString(`,"expires_at":`)
	buf.WriteString(strconv.FormatInt(exp.Unix(), 10))
	buf.WriteString(`,"realtime_url":`)
	writeJSONString(&buf, "ws://"+r.Host+RealtimePath)
	buf.WriteString(`,"apikey":`)
	writeJSONString(&buf, realtimeAPIKey)
	buf.WriteString(`,"topic":`)
	writeJSONString(&buf, "user:"+s.opts.UserID)
	buf.WriteString(`}`)
	s.writeJSON(w, &buf)
}

func writeJSONString(buf *bytes.Buffer, v string) {
	b, _ := json.Marshal(v)
	buf.Write(b)
}

// handleRealtimeSocket upgrades GET /realtime/v1/websocket.
func (s *Server) handleRealtimeSocket(w http.ResponseWriter, r *http.Request) {
	if !s.realtimeEnabled() {
		s.writeError(w, http.StatusNotFound, proto.ReasonBadRequest)
		return
	}
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, proto.ReasonBadRequest)
		return
	}
	s.rt.mu.Lock()
	full := len(s.rt.conns) >= s.opts.MaxRealtimeConns
	s.rt.mu.Unlock()
	if full {
		s.writeError(w, http.StatusServiceUnavailable, proto.ReasonOverloaded)
		return
	}
	srv := websocket.Server{
		// The default handshake insists on an Origin header; the client may
		// send none. Authentication is the channel join, not the upgrade.
		Handshake: func(*websocket.Config, *http.Request) error { return nil },
		Handler:   s.serveRealtimeConn,
	}
	srv.ServeHTTP(w, r)
}

func (s *Server) serveRealtimeConn(ws *websocket.Conn) {
	ws.MaxPayloadBytes = realtimeMaxFrame
	// The HTTP server's read/write timeouts still sit on the hijacked
	// connection; replace them with per-frame deadlines.
	_ = ws.SetDeadline(time.Time{})

	c := &rtConn{ws: ws, send: make(chan []byte, realtimeSendBuffer), done: make(chan struct{})}
	s.rt.mu.Lock()
	if s.rt.conns == nil {
		s.rt.conns = make(map[*rtConn]struct{})
	}
	s.rt.conns[c] = struct{}{}
	s.rt.mu.Unlock()
	defer func() {
		s.rt.mu.Lock()
		delete(s.rt.conns, c)
		s.rt.mu.Unlock()
		c.close()
	}()

	go s.realtimeWriter(c)

	for {
		_ = ws.SetReadDeadline(time.Now().Add(realtimeReadTimeout))
		var msg string
		if err := websocket.Message.Receive(ws, &msg); err != nil {
			return
		}
		var f phxFrame
		if err := json.Unmarshal([]byte(msg), &f); err != nil {
			return // a client never sends garbage; nothing to heal
		}
		if !s.realtimeFrame(c, f) {
			return
		}
	}
}

func (s *Server) realtimeWriter(c *rtConn) {
	for {
		select {
		case <-c.done:
			return
		case b := <-c.send:
			_ = c.ws.SetWriteDeadline(time.Now().Add(realtimeWriteTimeout))
			if err := websocket.Message.Send(c.ws, string(b)); err != nil {
				c.close()
				return
			}
		}
	}
}

func reply(topic string, ref, joinRef *string, status string, response any) []byte {
	if response == nil {
		response = struct{}{}
	}
	payload, _ := json.Marshal(map[string]any{"status": status, "response": response})
	b, _ := json.Marshal(phxFrame{Topic: topic, Event: "phx_reply", Payload: payload, Ref: ref, JoinRef: joinRef})
	return b
}

// realtimeFrame handles one client frame; false closes the socket.
func (s *Server) realtimeFrame(c *rtConn, f phxFrame) bool {
	ctx := context.Background()
	topic := s.realtimeTopic()

	if f.Topic == "phoenix" {
		if f.Event != "heartbeat" {
			return true
		}
		// A token that expired without a refresh ends the channel the way
		// Supabase does, with a system error the client heals from.
		if c.joined.Load() && s.opts.Now().Unix() >= c.exp.Load() {
			c.sendFinal(systemError(topic, "Token has expired"))
			return false
		}
		c.enqueue(reply("phoenix", f.Ref, nil, "ok", nil))
		return true
	}

	if f.Topic != topic {
		if f.Event == "phx_join" {
			c.enqueue(reply(f.Topic, f.Ref, f.JoinRef, "error", map[string]string{"reason": "unknown topic"}))
		}
		return true
	}

	var p struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.Unmarshal(f.Payload, &p)

	switch f.Event {
	case "phx_join":
		claims, err := s.verifyRealtimeToken(ctx, p.AccessToken)
		if err != nil {
			c.enqueue(reply(topic, f.Ref, f.JoinRef, "error", map[string]string{"reason": err.Error()}))
			return true
		}
		c.device.Store(claims.Device)
		c.exp.Store(claims.Exp)
		c.joined.Store(true)
		c.enqueue(reply(topic, f.Ref, f.JoinRef, "ok", nil))

	case "access_token":
		claims, err := s.verifyRealtimeToken(ctx, p.AccessToken)
		if err != nil || !c.joined.Load() || claims.Device != c.device.Load() {
			c.sendFinal(systemError(topic, "invalid access token"))
			return false
		}
		c.exp.Store(claims.Exp)
		c.enqueue(reply(topic, f.Ref, f.JoinRef, "ok", nil))

	case "phx_leave":
		c.joined.Store(false)
		c.enqueue(reply(topic, f.Ref, f.JoinRef, "ok", nil))
	}
	return true
}

func systemError(topic, message string) []byte {
	payload, _ := json.Marshal(map[string]string{"status": "error", "message": message, "extension": "system"})
	b, _ := json.Marshal(phxFrame{Topic: topic, Event: "system", Payload: payload})
	return b
}

// announceAdvance tells every joined socket except the pusher's that the log
// grew. The pusher learns its head from the push response.
func (s *Server) announceAdvance(head proto.Dec, fromDevice string) {
	if !s.realtimeEnabled() {
		return
	}
	inner, _ := json.Marshal(map[string]string{"epoch": s.st.Epoch().String(), "head_seq": head.String()})
	payload, _ := json.Marshal(map[string]any{"type": "broadcast", "event": "advance", "payload": json.RawMessage(inner)})
	frame, _ := json.Marshal(phxFrame{Topic: s.realtimeTopic(), Event: "broadcast", Payload: payload})

	s.rt.mu.Lock()
	targets := make([]*rtConn, 0, len(s.rt.conns))
	for c := range s.rt.conns {
		if c.joined.Load() && c.device.Load() != fromDevice {
			targets = append(targets, c)
		}
	}
	s.rt.mu.Unlock()
	for _, c := range targets {
		c.enqueue(frame)
	}
}

// RealtimeConns reports how many sockets are open and how many are joined.
func (s *Server) RealtimeConns() (open, joined int) {
	s.rt.mu.Lock()
	defer s.rt.mu.Unlock()
	for c := range s.rt.conns {
		open++
		if c.joined.Load() {
			joined++
		}
	}
	return open, joined
}

// CloseRealtime drops every socket. http.Server.Shutdown does not track
// hijacked connections, so serve calls this on the way out.
func (s *Server) CloseRealtime() {
	s.rt.mu.Lock()
	conns := make([]*rtConn, 0, len(s.rt.conns))
	for c := range s.rt.conns {
		conns = append(conns, c)
	}
	s.rt.mu.Unlock()
	for _, c := range conns {
		c.close()
	}
}
