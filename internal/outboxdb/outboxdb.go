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
		// Get unique keys that are not already being processed.
		elibleKeys, err := r.getEligibleGroupIdsTx(ctx, tx)
		if err != nil {
			return nil, err
		}
		if len(elibleKeys) == 0 {
			return nil, nil
		}

		distributedLockMap := make(map[uint64]string, 0)
		var advisorytLockHashedKeys []uint64
		for _, key := range elibleKeys {
			xactHash, err := getAdvisoryLockForGroupID(key)
			if err != nil {
				continue
			}
			distributedLockMap[xactHash] = key
			advisorytLockHashedKeys = append(advisorytLockHashedKeys, xactHash)
		}

		// Try acquire advisory lock
		acquiredGroupIDLocks, err := r.withDistributedLocksTx(ctx, advisorytLockHashedKeys, tx)
		if err != nil {
			return nil, err
		}

		// TODO: Re-check eligibility for acquired group IDs under the advisory lock to prevent processing stale groups that may already be running.
		// In the initial eligibility query (getEligibleGroupIdsTx), a worker could fetch groups with PENDING/PENDING_RETRY messages that dont have any running already.
		// However, another worker that has a lock, could commit its transaction, update messages with groupids to RUNNING, and release the lock.
		// But if another worker then acquires the now-free lock, it might of already fetched same eligibile groupids right before the previous
		// worker holding a advisory lock on same groupids updated its messages to be running and committed the transaction for its groupids.
		// Due to concurrency timing issues, leading to new batch of messages but with the same groupids to also be updated to RUNNING when it shouldnt.
		// This violates the invariant of only one batch per group processing at a time, until the full batch completed/failed/requeued.
		// After lock acquisition, just re-query locked groupids check again, for RUNNING count per group. skip if >0.
		// Due to FIFO behavior on group ids, and many rows can have same group ids since its a hash on  (kafka key, topic), FOR UPDATE
		// really can be applicatable here. Due to wanting correct ordering semanctics per kafka key/topic and making sure that batch
		// finishes nefore processing more.
		if len(acquiredGroupIDLocks) == 0 {
			fmt.Println("unable to obtain any locks, trying again")
		}

		fmt.Printf("acquired %d locks", len(acquiredGroupIDLocks))

		var lookUpGID []string
		for _, lock := range acquiredGroupIDLocks {
			if val, ok := distributedLockMap[lock]; ok {
				lookUpGID = append(lookUpGID, val)
			}
		}

		// Query to get messages by key that were locked.
		messages, err := r.getPendingMessagesFIFOTx(ctx, lookUpGID, tx)
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

func (r *outboxDB) getPendingMessagesFIFOTx(ctx context.Context, acquiredGroupIDLocks []string, tx bun.IDB) ([]Message, error) {
	var messages []Message
	subQuery := tx.NewSelect().
		TableExpr("outbox as o").
		ColumnExpr("o.*").
		ColumnExpr("ROW_NUMBER() OVER (PARTITION BY o.group_id ORDER BY o.created_at) AS rn").
		Where("group_id IN (?)", bun.In(acquiredGroupIDLocks)).
		Where("o.status IN (?)", bun.In([]string{PENDING, PENDING_RETRY}))

	err := tx.NewSelect().
		Model(&messages).
		TableExpr("(?) as sub", subQuery).
		Where("rn <= (?)", FifoLimit).
		Scan(ctx)
	if err != nil {
		return nil, err
	}
	return messages, nil
}

// getEligibleKeys finds unique keys that have no processing messages already.
func (r *outboxDB) getEligibleGroupIdsTx(ctx context.Context, tx bun.IDB) ([]string, error) {
	var groupID []string

	subQuery := tx.NewSelect().
		TableExpr("SELECT 1 FROM outbox").
		Where("group_id = outbox.group_id").
		Where("and status = (?)", RUNNING)

	sub := tx.NewSelect().
		TableExpr("outbox").
		Column("group_id").
		Where("status IN (?)", bun.In([]string{PENDING, PENDING_RETRY})).
		Where("NOT EXISTS (?)", subQuery).
		Group("group_id").
		OrderExpr("MIN(created_at) ASC").
		Limit(100)

	if err := tx.NewSelect().
		Column("sub.group_id").
		TableExpr("(?) as sub", sub).
		Scan(ctx, &groupID); err != nil {
		return nil, err
	}

	return groupID, nil
}

// withDistrubutedLocks finds unique keys that obtained an xact advisoy lock.
func (r *outboxDB) withDistributedLocksTx(ctx context.Context, keys []uint64, tx bun.Tx) ([]uint64, error) {
	var xacts []AdvisoryXactLock
	var acquiredGroupIDLocks []uint64

	if err := tx.NewSelect().
		Column("tx.group_id").
		ColumnExpr("pg_try_advisory_xact_lock(tx.group_id) as locked").
		TableExpr("unnest(?::bigint[]) as tx(group_id)", pgdialect.Array(keys)).
		Scan(ctx, &xacts); err != nil {
		return nil, err
	}

	for _, key := range xacts {
		if key.Locked {
			acquiredGroupIDLocks = append(acquiredGroupIDLocks, key.GroupID)
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
