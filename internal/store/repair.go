package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/andrhamm/claude-mem-lan-sync/internal/proto"
)

// Conflict repair, capability origin-repair-v1 (claude-mem 15.x,
// SyncConflictRecovery.ts). A device that owns an entity reads the hub's head
// for it, then appends a strictly newer revision of its own native row,
// conditioned on that head and on the epoch being unchanged. Nothing is ever
// rewritten or removed: the repair is one more op in the log, and the hub keeps
// a receipt so a retry after a lost response gets the same acknowledgement.

// EntityHead is the newest revision the log holds for one entity.
type EntityHead struct {
	EntityRev  proto.Dec
	Digest     string
	Seq        proto.Dec
	RawLiteral []byte // the body as received, quotes included
}

// RepairHead is what the client read earlier and expects to still be current.
type RepairHead struct {
	EntityRev proto.Dec
	Digest    string
	Seq       proto.Dec
}

// RepairResult acknowledges an accepted repair.
type RepairResult struct {
	Ack     proto.Ack
	HeadSeq proto.Dec
	// Appended is false for an exact retry answered from the receipt.
	Appended bool
}

var (
	// ErrRepairConflict: the epoch, the head or the revision order no longer
	// matches what the client reviewed; it must preview again (HTTP 409).
	ErrRepairConflict = errors.New("store: repair precondition failed")
	// ErrRepairAuthority: the op's origin is not the requesting device (403).
	ErrRepairAuthority = errors.New("store: repair origin is not the requesting device")
)

// headOf reads the newest revision of an entity. Revisions are canonical
// decimals stored as text, so length then text orders them numerically.
func headOf(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, userID, entityID string) (*EntityHead, error) {
	var rev, digest, body string
	var seq int64
	err := q.QueryRowContext(ctx, `
		SELECT entity_rev, operation_sha256, seq, body FROM ops
		WHERE user_id = ? AND entity_id = ?
		ORDER BY length(entity_rev) DESC, entity_rev DESC LIMIT 1`,
		userID, entityID).Scan(&rev, &digest, &seq, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: reading entity head: %w", err)
	}
	h := &EntityHead{Digest: digest, RawLiteral: []byte(body)}
	if h.EntityRev, err = proto.ParseDec(rev); err != nil {
		return nil, fmt.Errorf("store: stored entity_rev %q: %w", rev, err)
	}
	if h.Seq, err = proto.DecFromInt64(seq); err != nil {
		return nil, err
	}
	return h, nil
}

// EntityHead returns the newest revision of an entity, or nil when the log has
// none.
func (s *Store) EntityHead(ctx context.Context, entityID string) (*EntityHead, error) {
	return headOf(ctx, s.r, s.userID, entityID)
}

// Repair appends op if every precondition the client reviewed still holds.
func (s *Store) Repair(ctx context.Context, repairID, expectedEpoch string, expected *RepairHead,
	op proto.Op, deviceID string) (RepairResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx = context.WithoutCancel(ctx)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return RepairResult{}, fmt.Errorf("store: beginning repair transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var head int64
	if err := tx.QueryRowContext(ctx,
		`SELECT head_seq FROM meta WHERE user_id = ?`, s.userID).Scan(&head); err != nil {
		return RepairResult{}, fmt.Errorf("store: reading head_seq: %w", err)
	}
	headDec, err := proto.DecFromInt64(head)
	if err != nil {
		return RepairResult{}, err
	}
	ack := proto.Ack{
		ID: op.ID, Kind: op.Kind, EntityRev: op.EntityRev, Digest: op.Digest, OriginLocalID: op.OriginLocalID,
	}

	// An exact retry is answered from its receipt even though the head it
	// expected has since become this very op.
	var rEntity, rRev, rDigest string
	var rSeq int64
	err = tx.QueryRowContext(ctx, `
		SELECT entity_id, entity_rev, operation_sha256, seq FROM repairs
		WHERE user_id = ? AND repair_id = ?`, s.userID, repairID).Scan(&rEntity, &rRev, &rDigest, &rSeq)
	switch {
	case err == nil:
		if rEntity != op.ID || rRev != op.EntityRev.String() || rDigest != op.Digest {
			return RepairResult{}, ErrRepairConflict
		}
		if ack.Seq, err = proto.DecFromInt64(rSeq); err != nil {
			return RepairResult{}, err
		}
		return RepairResult{Ack: ack, HeadSeq: headDec}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return RepairResult{}, fmt.Errorf("store: reading repair receipt: %w", err)
	}

	if op.OriginDeviceID != deviceID {
		return RepairResult{}, ErrRepairAuthority
	}
	if expectedEpoch != s.epoch.String() {
		return RepairResult{}, ErrRepairConflict
	}
	current, err := headOf(ctx, tx, s.userID, op.ID)
	if err != nil {
		return RepairResult{}, err
	}
	switch {
	case current == nil && expected == nil:
	case current == nil || expected == nil:
		return RepairResult{}, ErrRepairConflict
	case current.EntityRev != expected.EntityRev || current.Digest != expected.Digest || current.Seq != expected.Seq:
		return RepairResult{}, ErrRepairConflict
	}
	if current != nil && op.EntityRev <= current.EntityRev {
		return RepairResult{}, ErrRepairConflict
	}

	head++
	now := s.now().UnixMilli()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ops (user_id, seq, entity_id, entity_rev, kind,
		                 origin_device_id, operation_sha256, body, server_ts)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.userID, head, op.ID, op.EntityRev.String(), op.Kind,
		op.OriginDeviceID, op.Digest, string(op.RawLiteral), now); err != nil {
		return RepairResult{}, fmt.Errorf("store: appending repair: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE meta SET head_seq = ? WHERE user_id = ?`, head, s.userID); err != nil {
		return RepairResult{}, fmt.Errorf("store: updating head_seq: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO repairs (user_id, repair_id, entity_id, entity_rev, operation_sha256, seq, device_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		s.userID, repairID, op.ID, op.EntityRev.String(), op.Digest, head, deviceID, now); err != nil {
		return RepairResult{}, fmt.Errorf("store: recording repair receipt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return RepairResult{}, fmt.Errorf("store: committing repair: %w", err)
	}

	if ack.Seq, err = proto.DecFromInt64(head); err != nil {
		return RepairResult{}, err
	}
	headDec, _ = proto.DecFromInt64(head)
	s.log.Info("conflict repair appended", "entity_id", op.ID, "entity_rev", op.EntityRev.String(), "seq", head)
	return RepairResult{Ack: ack, HeadSeq: headDec, Appended: true}, nil
}
