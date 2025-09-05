package outboxdb

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"log"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

const (
	FifoLimit = 10
)

type OutboxDB interface {
	// GetPendingMessages find any pending messages that need to be processed.
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
				return nil, errors.New("no more eligible groups founds to process")
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
				return nil, nil
			}

			var messages []Message
			err = tx.NewUpdate().
				Table("outbox").
				Set("retries = outbox.retries + (?)", 1).
				Set("started_at = (?)", time.Now().UTC()).
				Set("updated_at = (?)", time.Now().UTC()).
				Set("status = (?)", RUNNING).
				Where("job_id IN (?)", bun.In(jobIds)).
				Returning("*").
				Scan(ctx, &messages)

			if err != nil {
				return nil, err
			}
			log.Println("updated", messages)

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

func (r *outboxDB) getPendingMessagesFIFOTx(ctx context.Context, acquiredGroupIDLocks []string, tx bun.IDB) ([]string, error) {
	var jobIds []string
	subQuery := tx.NewSelect().
		TableExpr("outbox as o").
		ColumnExpr("o.*").
		ColumnExpr("ROW_NUMBER() OVER (PARTITION BY o.group_id ORDER BY o.created_at) AS rn").
		Where("o.group_id IN (?)", bun.In(acquiredGroupIDLocks)).
		Where("o.status IN (?)", bun.In([]string{PENDING, PendingRetry}))

	err := tx.NewSelect().
		Column("sub.job_id").
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
		Table("outbox").
		ColumnExpr("1").
		Where("group_id = outbox.group_id").
		Where("status = (?)", RUNNING)

	err := tx.NewSelect().
		Table("outbox").
		Column("group_id").
		Where("status IN (?)", bun.In([]string{PENDING, PendingRetry})).
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
// But if another worker then acquires the now-free lock, it might of already fetched the same eligible groupIds right before the previous
// worker which held an advisory lock on same groupIds updated its messages to be running and committed the transaction for same groupIds.
// Due to concurrency timing issues, this can lead to new batch of messages but with the same groupIds to also be updated to RUNNING when it shouldn't.
// This violates the invariant of only one batch per group processing at a time, until the full batch completed/failed/re-queued.
// After lock acquisition, just re-query locked groupIds check again, for RUNNING count per group. skip if >0.
// Due to FIFO behavior on group ids, and many rows can have same group ids since it's a hash on  (kafka key, topic).
// FOR UPDATE really cant be applicable here unless creating separate tables for lock group locking and tracking
// and another table to keep track of jobs running for per group.
func (r *outboxDB) reCheckEligibleGroupIdsTx(ctx context.Context, lockedGroupIds []string, tx bun.Tx) ([]string, error) {
	var alreadyRunningGroupIds []string
	err := tx.NewSelect().
		TableExpr("outbox").
		Column("group_id").
		Where("group_id IN (?)", bun.In(lockedGroupIds)).
		Where("status = (?)", RUNNING).
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

// getAdvisoryLockIdentifier returns a mask of first 8 bytes as int64 from the groupId hash from the key.
// Although possible to have a collison, very unlikely but possible.
// The impact wouldn't get big, since it would just not process these messages for the key.
// Worst case, messages for group id derived from the key will wait to be processed eventually.
func (r *outboxDB) getAdvisoryLockIdentifier(key string) (int64, error) {
	decodedHash, err := hex.DecodeString(key)
	if err != nil {
		return 0, err
	}

	return int64(binary.BigEndian.Uint64(decodedHash[:8]) & 0x7FFFFFFFFFFFFFFF), nil
}
