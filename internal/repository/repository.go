package repository

import (
	"context"

	"github.com/TimKotowski/pg-kafka-outbox/hash"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type OutboxDB interface {
	// GetPendingMessages find any pendng messages that need to be processed.
	GetPendingMessages(ctx context.Context) ([]Message, error)

	// GetPendingMessagesFIFO retrieves pending messages in groups by kafka key, that arn't curently being processed.
	// Used only if FifoKafkaKeyProcessing if enabled. To ensure FIFO processing of messages per kafka key.
	//
	// 1. Only one batch of messages per kafka key can be processed at any given time.
	// 2. Messages must finish per key, before more are processed. Or when TTL is reached, in which messages will be re-processed.
	// 3. Messages are processed in the order they were received.
	GetPendingMessagesFIFO(ctx context.Context) ([]Message, error)

	// GetEligibleKeys finds unique keys that have no processing messages already.
	GetEligibleKeysByGroupIds(ctx context.Context) ([]string, error)

	// ExistsByFingerprint finds any messages with same fingerpring that are pending or being processed.
	// This ensures messages with the same fingerprint cant enter the outbox.
	ExistsByFingerprint(ctx context.Context, fingerprint []byte) (bool, error)

	// DeleteCompletedMessages deletes messages that are passed TTL time. To clean up space.
	DeleteCompletedMessages(ctx context.Context, jobIds []string) (int, error)

	// UpdateMessageStatus updats given message with correct status.
	UpdateMessageStatus(ctx context.Context, message Message) error

	// RequeueOrphanedMessages will updates status of message to be able to be reprocessed.
	// in case of hanging/stalled messages.
	RequeueOrphanedMessages(ctx context.Context) (int, error)
}

type repository struct {
	db *bun.DB
}

func NewRepository(db *bun.DB) OutboxDB {
	return &repository{
		db: db,
	}
}

func (r *repository) GetPendingMessages(ctx context.Context) ([]Message, error) {
	return nil, nil
}

func (r *repository) DeleteCompletedMessages(ctx context.Context, ids []string) (int, error) {
	return 0, nil
}

func (r *repository) RequeueOrphanedMessages(ctx context.Context) (int, error) {
	return 0, nil
}

func (r *repository) UpdateMessageStatus(ctx context.Context, message Message) error {
	return nil
}

func (r *repository) GetPendingMessagesFIFO(ctx context.Context) ([]Message, error) {
	messages, err := RunInTxWithReturnType(ctx, r.db, func(tx bun.Tx) ([]Message, error) {
		// Get unique keys that are not already being processed.
		elibleKeys, err := r.GetEligibleKeysByGroupIds(ctx)
		if err != nil {
			return nil, err
		}

		// Hash keys to allow right advisory lock type.
		var advisorytLockHashedKeys []uint64
		for _, key := range elibleKeys {
			xactHash, err := hash.GenerateAdvisoryLockHash(key)
			if err != nil {
				continue
			}
			advisorytLockHashedKeys = append(advisorytLockHashedKeys, xactHash)
		}

		// Try acquire advisory lock
		acquiredGroupIDLocks, err := r.tryAdvisoryXactLock(advisorytLockHashedKeys, tx)
		if err != nil {
			return nil, err
		}

		var messages []Message

		// Query to get messages by key that were locked.
		subQuery := r.db.NewSelect().
			TableExpr("outbox as o").
			ColumnExpr("o.*").
			ColumnExpr("ROW_NUMBER() OVER (PARTITION BY o.group_id ORDER BY o.created_at) AS rn").
			Where("group_id IN (?)", bun.In(acquiredGroupIDLocks)).
			Where("o.status IN (?)", bun.In([]string{PENDING, PENDING_RETRY}))

		err = r.db.NewSelect().
			TableExpr("(?) as sub", subQuery).
			Where("rn <= 10=").
			Scan(ctx, &messages)
		if err != nil {
			return nil, err
		}

		return messages, nil
	})
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (r *repository) ExistsByFingerprint(ctx context.Context, fingerprint []byte) (bool, error) {
	return false, nil
}

func (r *repository) GetEligibleKeysByGroupIds(ctx context.Context) ([]string, error) {
	var groupId []string

	if err := r.db.NewSelect().
		Table("outbox").
		ColumnExpr("DISTINCT group_id").
		Where("status IN (?)", bun.In([]string{PENDING, PENDING_RETRY})).
		Limit(10).
		Scan(ctx, &groupId); err != nil {
		return nil, err
	}

	return groupId, nil
}

// tryAdvisoryXactLock finds unique keys that obtained an xact advisoy lock key ordering semantics.
func (r *repository) tryAdvisoryXactLock(keys []uint64, tx bun.Tx) ([]string, error) {
	ctx := context.Background()
	var xacts []AdvisoryXactLock
	var acquiredGroupIDLocks []string

	if err := tx.NewSelect().
		Column("tx.id").
		ColumnExpr("pg_try_advisory_xact_lock(tx.id) as locked").
		TableExpr("unnest(?) as tx(id)", pgdialect.Array(keys)).
		Scan(ctx, &xacts); err != nil {
		return nil, err
	}

	for _, key := range xacts {
		if key.Locked {
			acquiredGroupIDLocks = append(acquiredGroupIDLocks, key.Key)
		}
	}

	return acquiredGroupIDLocks, nil
}
