package outboxdb

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

const (
	FifoLimit      = 10
	NoRowsAffected = 0
)

type OutboxDB interface {
	// ClaimPendingMessagesForProcessing claims any pending messages that need to be processed.
	ClaimPendingMessagesForProcessing(ctx context.Context) ([]Message, error)

	// ClaimPendingMessagesForProcessingFIFO claims pending messages in groups by kafka key, that aren't currently being processed.
	// Used only if FifoKafkaKeyProcessing is enabled. To ensure FIFO processing of messages per kafka key.
	//
	// 1. Only one batch of messages per kafka key can be processed at any given time.
	// 2. Messages must be finished (none running) per key, before more are processed. Or when TTL is reached, in which messages will be re-processed.
	// 3. Messages are processed in the order they were received.
	ClaimPendingMessagesForProcessingFIFO(ctx context.Context) ([]Message, error)

	// ExistsByFingerprint finds any messages with same fingerprint that are pending or being processed.
	// This ensures messages with the same fingerprint cant enter the outbox.
	ExistsByFingerprint(ctx context.Context, fingerprint []byte) (bool, error)

	// UpdateMessageStatus updates given message with correct status.
	UpdateMessageStatus(ctx context.Context, jobID string, status Status) error

	UpdateMessagesStatusInTx(ctx context.Context, jobID []string, tx bun.IDB, status Status) ([]Message, error)
}

type outboxDB struct {
	db    *bun.DB
	limit int
}

func NewOutboxDB(db *bun.DB, limit int) OutboxDB {
	return &outboxDB{
		db:    db,
		limit: limit,
	}
}

func (r *outboxDB) ClaimPendingMessagesForProcessing(ctx context.Context) ([]Message, error) {
	var messages []Message
	sub := r.db.NewSelect().
		Table("outbox").
		Column("id").
		Where("status IN (?)", bun.In([]string{Pending, PendingRetry})).
		Order("created_at").
		Limit(r.limit).
		For("UPDATE SKIP LOCKED")

	err := r.db.NewUpdate().
		TableExpr("outbox as o").
		TableExpr("(?) as sub", sub).
		Set("retries = o.retries + (?)", 1).
		Set("started_at = (?)", time.Now().UTC()).
		Set("updated_at = (?)", time.Now().UTC()).
		Set("status = (?)", Running).
		Where("sub.id = o.id").
		Returning("o.*").
		Scan(ctx, &messages)

	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (r *outboxDB) ClaimPendingMessagesForProcessingFIFO(ctx context.Context) ([]Message, error) {
	messages, err := RunInTxWithReturnType(ctx, r.db, func(tx bun.Tx) ([]Message, error) {
		for {
			eligibleGroupIds, err := r.getEligibleGroupIdsTx(ctx, tx)
			if err != nil {
				return nil, err
			}

			if len(eligibleGroupIds) == 0 {
				return nil, nil
			}

			lockIdToGroupIdMap := make(map[int64]string)
			var advisoryLockIds []int64
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
				log.Println("unable to obtain any locks, trying again")
				continue
			} else {
				log.Printf("acquired %d locks %v", len(acquiredLockIds), acquiredLockIds)
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

			jobIds, err := r.getPendingMessagesFIFOTx(ctx, validGroupIds, tx)
			if err != nil {
				return nil, err
			}

			if len(jobIds) == 0 {
				log.Printf("obtained locks for %v but no messages to update to Running, concurrency timing issue", jobIds)
				return nil, nil
			}

			messages, err := r.UpdateMessagesStatusInTx(ctx, jobIds, tx, Running)
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

func (r *outboxDB) UpdateMessageStatus(ctx context.Context, jobId string, status Status) error {
	now := time.Now().UTC()
	baseQuery := r.db.NewUpdate().
		Table("outbox").
		Set("retries = outbox.retries + (?)", 1).
		Set("updated_at = (?)", now).
		Set("status = (?)", status)

	// Most likely an orphaned job update, allow a clean slate. For re-queueing.
	if status == PendingRetry || status == Pending {
		baseQuery.Set("started_at = (?)", nil)
	}
	if status == Running {
		baseQuery.Set("started_at = (?)", now)
	}
	if status == Completed {
		baseQuery.Set("completed_at = (?)", now)
	}

	res, err := baseQuery.Where("id = (?)", jobId).Exec(ctx)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == NoRowsAffected {
		return errors.New(fmt.Sprintf("updating message failed for id %s status %s", jobId, status))
	}

	return nil
}

func (r *outboxDB) ExistsByFingerprint(ctx context.Context, fingerprint []byte) (bool, error) {
	return false, nil
}

func (r *outboxDB) UpdateMessagesStatusInTx(ctx context.Context, jobIds []string, tx bun.IDB, status Status) ([]Message, error) {
	var messages []Message
	now := time.Now().UTC()
	baseQuery := tx.NewUpdate().
		Table("outbox").
		Set("retries = outbox.retries + (?)", 1).
		Set("updated_at = (?)", now).
		Set("status = (?)", status)

	// Most likely an orphaned job update, allow a clean slate. For re-queueing.
	if status == PendingRetry || status == Pending {
		baseQuery.Set("started_at = (?)", nil)
	}
	if status == Running {
		baseQuery.Set("started_at = (?)", now)
	}
	if status == Completed {
		baseQuery.Set("completed_at = (?)", now)
	}

	err := baseQuery.
		Where("id IN (?)", bun.In(jobIds)).
		Returning("*").
		Scan(ctx, &messages)

	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (r *outboxDB) getPendingMessagesFIFOTx(ctx context.Context, acquiredGroupIDLocks []string, tx bun.IDB) ([]string, error) {
	var jobIds []string
	subQuery := tx.NewSelect().
		TableExpr("outbox as o").
		ColumnExpr("o.*").
		ColumnExpr("ROW_NUMBER() OVER (PARTITION BY o.group_id ORDER BY o.created_at) AS rn").
		Where("o.group_id IN (?)", bun.In(acquiredGroupIDLocks)).
		Where("o.status IN (?)", bun.In([]string{Pending, PendingRetry}))

	err := tx.NewSelect().
		Column("sub.id").
		TableExpr("(?) as sub", subQuery).
		Where("sub.rn <= (?)", FifoLimit).
		Scan(ctx, &jobIds)
	if err != nil {
		return nil, err
	}
	return jobIds, nil
}

// getEligibleKeys finds unique keys that have no processing messages already.
func (r *outboxDB) getEligibleGroupIdsTx(ctx context.Context, tx bun.IDB) ([]string, error) {
	var groupID []string

	subQuery := tx.NewSelect().
		TableExpr("outbox as o2").
		ColumnExpr("1").
		Where("o2.group_id = o1.group_id").
		Where("o2.status = (?)", Running)

	err := tx.NewSelect().
		TableExpr("outbox as o1").
		Column("o1.group_id").
		Where("o1.status IN (?)", bun.In([]string{Pending, PendingRetry})).
		Where("NOT EXISTS (?)", subQuery).
		Group("o1.group_id").
		OrderExpr("MIN(o1.created_at) ASC").
		Limit(10).
		Scan(ctx, &groupID)
	if err != nil {
		return nil, err
	}

	return groupID, nil
}

// reCheckEligibleGroupIdsTx will recheck eligibility for acquired group IDs under the advisory lock to prevent processing stale groups that may already be running.
// In the initial eligibility query (getEligibleGroupIdsTx), a worker could fetch groups with Pending/PENDING_RETRY messages that don't have any running already.
// However, another worker that has a lock, could commit its transaction, update messages with groupIds to Running, and release the lock.
// But if another worker then acquires the now-free lock, it might of already fetched the same eligible groupIds right before the previous
// worker which held an advisory lock on same groupIds updated its messages to be running and committed the transaction for same groupIds.
// Due to concurrency timing issues, this can lead to new batch of messages but with the same groupIds to also be updated to Running when it shouldn't.
// This violates the invariant of only one batch per group processing at a time, until the full batch completed/failed/re-queued.
// After lock acquisition, just re-query locked groupIds check again, for Running count per group. skip if >0.
// Due to FIFO behavior on group ids, and many rows can have same group ids since it's a hash on  (kafka key, topic).
// FOR UPDATE really cant be applicable here unless creating separate tables for lock group locking and tracking
// and another table to keep track of jobs running for per group.
func (r *outboxDB) reCheckEligibleGroupIdsTx(ctx context.Context, lockedGroupIds []string, tx bun.Tx) ([]string, error) {
	var alreadyRunningGroupIds []string
	err := tx.NewSelect().
		TableExpr("outbox").
		Column("group_id").
		Where("group_id IN (?)", bun.In(lockedGroupIds)).
		Where("status = (?)", Running).
		Distinct().
		Scan(ctx, &alreadyRunningGroupIds)
	if err != nil {
		return nil, err
	}

	if alreadyRunningGroupIds == nil {
		return lockedGroupIds, nil
	}

	seen := make(map[string]bool)
	validGroupIds := make([]string, 0)
	for _, groupId := range alreadyRunningGroupIds {
		seen[groupId] = true
	}

	for _, groupId := range lockedGroupIds {
		if _, ok := seen[groupId]; !ok {
			validGroupIds = append(validGroupIds, groupId)
		}
	}

	return validGroupIds, nil
}

// withDistributedLocks finds unique keys that obtained a xact advisory lock.
func (r *outboxDB) withDistributedLocksTx(ctx context.Context, keys []int64, tx bun.Tx) ([]int64, error) {
	var xacts []AdvisoryXactLock
	var acquiredGroupIDLocks []int64

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

// getAdvisoryLockIdentifier ensures that the resulting hash fits within the positive range of an int64 (0 to 2^63 - 1).
// Want to avoid out-of-range conversions.
func (r *outboxDB) getAdvisoryLockIdentifier(key string) (int64, error) {
	decodedHash, err := hex.DecodeString(key)
	if err != nil {
		return 0, err
	}

	return int64(binary.BigEndian.Uint64(decodedHash[:8]) & 0x7FFFFFFFFFFFFFFF), nil
}
