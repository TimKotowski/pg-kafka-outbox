package repository

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type OutboxDB interface {
	// GetPendingMessages find any pendng messages that need to be processed.
	GetPendingMessages(ctx context.Context) ([]Message, error)

	// GetPendingMessagesByUniqueKey finds at most one pending message per unique key, that arn't already being processed.
	// This ensures at-most-once semantics for processing messages, where process ordering is needed.
	GetPendingMessagesByUniqueKey(ctx context.Context) ([]Message, error)

	// GetUniqueKeys finds unique keys that have no processing messages already.
	GetUniqueKeys(ctx context.Context) ([]string, error)

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

func (r *repository) GetPendingMessagesByUniqueKey(ctx context.Context) ([]Message, error) {
	messages, err := RunInTxWithReturnType(ctx, r.db, func(tx bun.Tx) ([]Message, error) {
		var messages []Message
		// Get unique keys that are not already being processed.

		// Hash keys to allow right advisory lock type.

		// Try acquire advisory lock
		acquiredLocks, err := r.tryAdvisoryXactLock([]uint64{}, tx)
		if err != nil {
			return nil, err
		}

		fmt.Println(acquiredLocks)
		// Query to get messages by keyc that were locked

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

func (r *repository) GetUniqueKeys(ctx context.Context) ([]string, error) {
	return nil, nil
}

// tryAdvisoryXactLock finds unique keys that obtained an xact advisoy lock key ordering semantics.
func (r *repository) tryAdvisoryXactLock(keys []uint64, tx bun.Tx) ([]string, error) {
	ctx := context.Background()
	var xacts []AdvisoryXactLock
	acquiredLocks := []string{}

	if err := tx.NewSelect().
		Column("tx.id").
		ColumnExpr("pg_try_advisory_xact_lock(tx.id) as locked").
		TableExpr("unnest(?) as tx(id)", pgdialect.Array(keys)).
		Scan(ctx, &xacts); err != nil {
		return nil, err
	}

	for _, key := range xacts {
		if key.Locked {
			acquiredLocks = append(acquiredLocks, key.Key)
		}
	}

	return acquiredLocks, nil
}
