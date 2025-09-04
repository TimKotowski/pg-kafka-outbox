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

	// GetPendingMessagesFIFO retrieves pending messages in groups by kafka key, that aren't currently being processed.
	// Used only if FifoKafkaKeyProcessing is enabled. To ensure FIFO processing of messages per kafka key.
	//
	// 1. Only one batch of messages per kafka key can be processed at any given time.
	// 2. Messages must finish per key, before more are processed. Or when TTL is reached, in which messages will be re-processed.
	// 3. Messages are processed in the order they were received.
	GetPendingMessagesFIFO(ctx context.Context) ([]Message, error)

	// ExistsByFingerprint finds any messages with same fingerprint that are pending or being processed.
	// This ensures messages with the same fingerprint cant enter the outbox.
	ExistsByFingerprint(ctx context.Context, fingerprint []byte) (bool, error)

	// DeleteCompletedMessages deletes messages that are passed TTL time. To clean up space.
	DeleteCompletedMessages(ctx context.Context, jobIds []string) (int, error)

	// UpdateMessageStatus updates given message with correct status.
	UpdateMessageStatus(ctx context.Context, jobID string, status Status) error

	// RequeueOrphanedMessages will update status of message to be able to be reprocessed.
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
		for {
			eligibleGroupIds, err := r.getEligibleGroupIdsTx(ctx, tx)
			if err != nil {
				return nil, err
			}

			if len(eligibleGroupIds) == 0 {
				return nil, nil
			}

			lockIdToGroupIdMap := make(map[uint64]string)
			var advisoryLockIds []uint64

			for _, groupId := range eligibleGroupIds {
				lockId, err := r.getAdvisoryLockIdentifier(groupId)
				if err != nil {
					continue
				}
				lockIdToGroupIdMap[lockId] = groupId
				advisoryLockIds = append(advisoryLockIds, lockId)
			}

			acquiredLockIds, err := r.withDistributedLocksTx(ctx, advisoryLockIds, tx)
			if err != nil {
				return nil, err
			}

			if len(acquiredLockIds) == 0 {
				fmt.Println("unable to obtain any locks, trying again")
				continue
			} else {
				fmt.Printf("acquired %d locks", len(acquiredLockIds))
			}

			var lockedGroupIDs []string
			for _, lockId := range acquiredLockIds {
				if groupId, ok := lockIdToGroupIdMap[lockId]; ok {
					lockedGroupIDs = append(lockedGroupIDs, groupId)
				}
			}

			validGroupIds, err := r.reCheckEligibleGroupIdsTx(ctx, lockedGroupIDs, tx)
			if err != nil {
				return nil, err
			}
			messages, err := r.getPendingMessagesFIFOTx(ctx, validGroupIds, tx)
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
		Table("outbox").
		ColumnExpr("1").
		Where("group_id = outbox.group_id").
		Where("status = (?)", RUNNING)

	err := tx.NewSelect().
		Table("outbox").
		Column("group_id").
		Where("status IN (?)", bun.In([]string{PENDING, PENDING_RETRY})).
		Where("NOT EXISTS (?)", subQuery).
		Group("group_id").
		OrderExpr("MIN(created_at) ASC").
		Limit(10).
		Scan(ctx, &groupID)
	if err != nil {
		return nil, err
	}

	return groupID, nil
}

// reCheckEligibleGroupIdsTx will recheck eligibility for acquired group IDs under the advisory lock to prevent processing stale groups that may already be running.
// In the initial eligibility query (getEligibleGroupIdsTx), a worker could fetch groups with PENDING/PENDING_RETRY messages that don't have any running already.
// However, another worker that has a lock, could commit its transaction, update messages with groupIds to RUNNING, and release the lock.
// But if another worker then acquires the now-free lock, it might of already fetched same eligible groupIds right before the previous
// worker holding a advisory lock on same groupIds updated its messages to be running and committed the transaction for its groupIds.
// Due to concurrency timing issues, leading to new batch of messages but with the same groupIds to also be updated to RUNNING when it shouldn't.
// This violates the invariant of only one batch per group processing at a time, until the full batch completed/failed/re-queued.
// After lock acquisition, just re-query locked groupIds check again, for RUNNING count per group. skip if >0.
// Due to FIFO behavior on group ids, and many rows can have same group ids since it's a hash on  (kafka key, topic).
// FOR UPDATE really cant be applicable here unless creating separate tables for lock tracking and another table to keep track of jobs running for groups.
func (r *outboxDB) reCheckEligibleGroupIdsTx(ctx context.Context, lockedGroupIDs []string, tx bun.Tx) ([]string, error) {
	var alreadyRunningGroupIds []string
	err := tx.NewSelect().
		TableExpr("outbox").
		Column("group_id").
		Where("group_id IN (?)", bun.In(lockedGroupIDs)).
		Where("status = (?)", RUNNING).
		Distinct().
		Scan(ctx, &alreadyRunningGroupIds)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	validGroupIds := make([]string, len(lockedGroupIDs))
	copy(validGroupIds, lockedGroupIDs)

	for _, runningGroup := range alreadyRunningGroupIds {
		seen[runningGroup] = true
	}

	for i, lockedGroup := range lockedGroupIDs {
		if _, ok := seen[lockedGroup]; ok {
			validGroupIds = append(validGroupIds[:i], validGroupIds[i+1:]...)
		}
	}

	return validGroupIds, nil
}

// withDistributedLocks finds unique keys that obtained a xact advisory lock.
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

// getAdvisoryLockIdentifier returns first 8 bytes as uint64 from the groupId hash from the key.
// Although possible to have a collison, very unlikely but possible.
// The impact wouldn't get big, since it would just not process these messages for the key.
// Worst case, messages for group id derived from the key will wait to be processed eventually.
func (r *outboxDB) getAdvisoryLockIdentifier(key string) (uint64, error) {
	decodedHash, err := hex.DecodeString(key)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(decodedHash[:8]), nil
}
