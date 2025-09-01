package outboxdb

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

const (
	FifoLimit = 10
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

	// ExistsByFingerprint finds any messages with same fingerpring that are pending or being processed.
	// This ensures messages with the same fingerprint cant enter the outbox.
	ExistsByFingerprint(ctx context.Context, fingerprint []byte) (bool, error)

	// DeleteCompletedMessages deletes messages that are passed TTL time. To clean up space.
	DeleteCompletedMessages(ctx context.Context, jobIds []string) (int, error)

	// UpdateMessageStatus updats given message with correct status.
	UpdateMessageStatus(ctx context.Context, jobID string, status Status) error

	// RequeueOrphanedMessages will updates status of message to be able to be reprocessed.
	// in case of hanging/stalled messages.
	RequeueOrphanedMessages(ctx context.Context) (int, error)
}

type outboxDB struct {
	db *bun.DB
}

func NewOutboxDB(db *bun.DB) OutboxDB {
	return &outboxDB{
		db: db,
	}
}

func (r *outboxDB) GetPendingMessages(ctx context.Context) ([]Message, error) {
	return nil, nil
}

func (r *outboxDB) GetPendingMessagesFIFO(ctx context.Context) ([]Message, error) {
	messages, err := RunInTxWithReturnType(ctx, r.db, func(tx bun.Tx) ([]Message, error) {
		// Keep trying until keys can be locked and processed or there is none to process.
		// Could be due to wother works getting locks on keys causing other workers to no get any messagess.
		for {
			// Get unique keys that are not already being processed.
			elibleKeys, err := r.getEligibleGroupIds(ctx)
			if err != nil {
				return nil, err
			}
			if len(elibleKeys) == 0 {
				return nil, nil
			}

			var advisorytLockHashedKeys []uint64
			for _, key := range elibleKeys {
				xactHash, err := getAdvisoryLockForGroupID(key)
				if err != nil {
					continue
				}
				advisorytLockHashedKeys = append(advisorytLockHashedKeys, xactHash)
			}

			// Try acquire advisory lock
			acquiredGroupIDLocks, err := r.withDistributedLocks(advisorytLockHashedKeys, tx)
			if err != nil {
				return nil, err
			}

			if len(acquiredGroupIDLocks) == 0 {
				fmt.Println("unable to obtain any locks, trying again")
				continue
			}

			fmt.Printf("acquired %d locks", len(acquiredGroupIDLocks))
			// Query to get messages by key that were locked.
			messages, err := r.getPendingMessagesFIFO(ctx, acquiredGroupIDLocks)
			if err != nil {
				return nil, err
			}

			return messages, nil
		}
	})
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (r *outboxDB) DeleteCompletedMessages(ctx context.Context, ids []string) (int, error) {
	return 0, nil
}

func (r *outboxDB) RequeueOrphanedMessages(ctx context.Context) (int, error) {
	return 0, nil
}

func (r *outboxDB) UpdateMessageStatus(ctx context.Context, jobID string, status Status) error {
	return nil
}

func (r *outboxDB) ExistsByFingerprint(ctx context.Context, fingerprint []byte) (bool, error) {
	return false, nil
}

func (r *outboxDB) getPendingMessagesFIFO(ctx context.Context, acquiredGroupIDLocks []string) ([]Message, error) {
	var messages []Message
	subQuery := r.db.NewSelect().
		TableExpr("outbox as o").
		ColumnExpr("o.*").
		ColumnExpr("ROW_NUMBER() OVER (PARTITION BY o.group_id ORDER BY o.created_at) AS rn").
		Where("group_id IN (?)", bun.In(acquiredGroupIDLocks)).
		Where("o.status IN (?)", bun.In([]string{PENDING, PENDING_RETRY}))

	err := r.db.NewSelect().
		TableExpr("(?) as sub", subQuery).
		Where("rn <= (?)", FifoLimit).
		Scan(ctx, &messages)
	if err != nil {
		return nil, err
	}
	return messages, nil
}

// getEligibleKeys finds unique keys that have no processing messages already.
func (r *outboxDB) getEligibleGroupIds(ctx context.Context) ([]string, error) {
	var groupID []string

	if err := r.db.NewSelect().
		Table("outbox").
		ColumnExpr("DISTINCT group_id").
		Where("status IN (?)", bun.In([]string{PENDING, PENDING_RETRY})).
		Order("created_at ASC").
		Limit(10).
		Scan(ctx, &groupID); err != nil {
		return nil, err
	}

	return groupID, nil
}

// withDistrubutedLocks finds unique keys that obtained an xact advisoy lock.
func (r *outboxDB) withDistributedLocks(keys []uint64, tx bun.Tx) ([]string, error) {
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

// getAdvisoryLockForGroupID returns first 8 bytes as uint64 from the groupId hash from the key.
// Allthough possible to have a collison, very unlikeyly but possble.
// The impact wouldn't get big, since it would just not process theese messsages for the key.
// Worst case, messsages for group id dereived from the key will wait to be processed eventually.
func getAdvisoryLockForGroupID(key string) (uint64, error) {
	decodedHash, err := hex.DecodeString(key)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(decodedHash[:8]), nil
}
