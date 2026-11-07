package hub

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"

	"github.com/andrhamm/claude-mem-lan-sync/internal/proto"
	"github.com/andrhamm/claude-mem-lan-sync/internal/store"
)

// Conflict repair: capability origin-repair-v1, as SyncConflictRecovery.ts in
// claude-mem 15.x calls it. Both routes are POST with a JSON body and answer
// protocol_version 1. A 404 tells the client the capability is missing; any
// other failure aborts that one repair and the user previews again, so these
// routes may refuse freely — unlike /v1/sync/ops, nothing is wedged by a 409.

const repairCapability = "origin-repair-v1"

var repairIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

const maxEntityIDLen = 512

// isRepairPath: these requests carry no X-Sync-Capabilities and no device
// name, so they must not overwrite what the device's sync client declared.
func isRepairPath(p string) bool {
	return p == "/v1/sync/repair" || p == "/v1/sync/repair/head"
}

func (s *Server) readJSONBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, bool) {
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		s.writeError(w, http.StatusBadRequest, proto.ReasonBadRequest)
		return nil, false
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, proto.ReasonTooLarge)
		return nil, false
	}
	return raw, true
}

func (s *Server) writeRepairPreamble(buf *bytes.Buffer, head proto.Dec) {
	buf.WriteString(`{"protocol_version":1,"capability":"` + repairCapability + `","epoch":"`)
	buf.WriteString(s.st.Epoch().String())
	buf.WriteString(`","head_seq":"`)
	buf.WriteString(head.String())
	buf.WriteString(`","projected_seq":"`)
	buf.WriteString(head.String())
	buf.WriteString(`"`)
}

// handleRepairHead answers POST /v1/sync/repair/head {entity_id}: the newest
// revision the log holds for that entity, with its wrapper exactly as stored.
func (s *Server) handleRepairHead(w http.ResponseWriter, r *http.Request) {
	raw, ok := s.readJSONBody(w, r, 4096)
	if !ok {
		return
	}
	var req struct {
		EntityID string `json:"entity_id"`
	}
	if err := json.Unmarshal(raw, &req); err != nil || req.EntityID == "" || len(req.EntityID) > maxEntityIDLen {
		s.writeError(w, http.StatusBadRequest, proto.ReasonBadRequest)
		return
	}
	headSeq, err := s.st.HeadSeq(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, proto.ReasonInternal)
		return
	}
	h, err := s.st.EntityHead(r.Context(), req.EntityID)
	if err != nil {
		s.opts.Logger.Error("reading entity head", "error", err)
		s.writeError(w, http.StatusInternalServerError, proto.ReasonInternal)
		return
	}

	var buf bytes.Buffer
	s.writeRepairPreamble(&buf, headSeq)
	buf.WriteString(`,"entity_id":`)
	writeJSONString(&buf, req.EntityID)
	if h == nil {
		buf.WriteString(`,"head":null,"op":null}`)
	} else {
		buf.WriteString(`,"head":{"entity_rev":"` + h.EntityRev.String() + `","operation_sha256":`)
		writeJSONString(&buf, h.Digest)
		buf.WriteString(`,"seq":"` + h.Seq.String() + `"},"op":{"body":`)
		// The stored literal, byte for byte: the client re-canonicalises it and
		// compares, exactly as on /v1/sync/changes.
		buf.Write(h.RawLiteral)
		buf.WriteString(`,"operation_sha256":`)
		writeJSONString(&buf, h.Digest)
		buf.WriteString(`}}`)
	}
	s.writeJSON(w, &buf)
}

// handleRepair answers POST /v1/sync/repair
// {repair_id, expected_epoch, expected_head, op}.
func (s *Server) handleRepair(w http.ResponseWriter, r *http.Request) {
	if err := s.checkFreeSpace(); err != nil {
		s.writeError(w, http.StatusInsufficientStorage, proto.ReasonStorageFull)
		return
	}
	raw, ok := s.readJSONBody(w, r, int64(proto.MaxBodyLiteral+8192))
	if !ok {
		return
	}
	var req struct {
		RepairID      string          `json:"repair_id"`
		ExpectedEpoch string          `json:"expected_epoch"`
		ExpectedHead  json.RawMessage `json:"expected_head"`
		Op            json.RawMessage `json:"op"`
	}
	if err := json.Unmarshal(raw, &req); err != nil || !repairIDPattern.MatchString(req.RepairID) || len(req.Op) == 0 {
		s.writeError(w, http.StatusBadRequest, proto.ReasonBadRequest)
		return
	}
	if _, err := proto.ParseDecPositive(req.ExpectedEpoch); err != nil {
		s.writeError(w, http.StatusBadRequest, proto.ReasonBadRequest)
		return
	}
	var expected *store.RepairHead
	if t := bytes.TrimSpace(req.ExpectedHead); len(t) > 0 && !bytes.Equal(t, []byte("null")) {
		var h struct {
			EntityRev       string `json:"entity_rev"`
			OperationSHA256 string `json:"operation_sha256"`
			Seq             string `json:"seq"`
		}
		if err := json.Unmarshal(t, &h); err != nil {
			s.writeError(w, http.StatusBadRequest, proto.ReasonBadRequest)
			return
		}
		rev, err1 := proto.ParseDecPositive(h.EntityRev)
		seq, err2 := proto.ParseDecPositive(h.Seq)
		if err1 != nil || err2 != nil || !proto.ValidDigest(h.OperationSHA256) {
			s.writeError(w, http.StatusBadRequest, proto.ReasonBadRequest)
			return
		}
		expected = &store.RepairHead{EntityRev: rev, Digest: h.OperationSHA256, Seq: seq}
	}

	op, err := proto.ParseOp(req.Op)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, proto.ReasonOf(err))
		return
	}
	if op.Kind == "mutation" {
		// Repair is defined for the content kinds only.
		s.writeError(w, http.StatusBadRequest, proto.ReasonUnknownKind)
		return
	}

	deviceID := r.Header.Get("X-Device-Id")
	res, err := s.st.Repair(r.Context(), req.RepairID, req.ExpectedEpoch, expected, op, deviceID)
	switch {
	case errors.Is(err, store.ErrRepairAuthority):
		s.writeError(w, http.StatusForbidden, proto.ReasonUnauthorized)
		return
	case errors.Is(err, store.ErrRepairConflict):
		s.writeError(w, http.StatusConflict, proto.ReasonConflict)
		return
	case err != nil:
		s.opts.Logger.Error("repair failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, proto.ReasonInternal)
		return
	}

	var buf bytes.Buffer
	s.writeRepairPreamble(&buf, res.HeadSeq)
	buf.WriteString(`,"acked":[`)
	if err := proto.EmitAck(&buf, res.Ack); err != nil {
		s.writeError(w, http.StatusInternalServerError, proto.ReasonInternal)
		return
	}
	buf.WriteString(`]}`)
	s.writeJSON(w, &buf)

	if res.Appended {
		s.announceAdvance(res.HeadSeq, deviceID)
	}
}
