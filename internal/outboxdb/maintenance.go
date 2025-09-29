package outboxdb

import (
	"context"
)

type OutboxMaintenanceDB interface {
	// ReIndex will rebuild certain indexes in outbox table.
	// Outbox can have very high churn. Causing bloat on the B-Tree index.
	// Usually re-index isn't needed but outbox table is prone to a lot of empty or partial empty pages.
	// Leading to excessive wasted space (empty or nearly empty pages) without compacting.
	ReIndex() error

	// RequeueOrphanedMessages will update status of message to be able to be reprocessed.
	// in case of hanging/stalled messages.
	RequeueOrphanedMessages(ctx context.Context) (int, error)

	// DeleteCompletedMessages deletes messages that are passed TTL time. To clean up space.
	DeleteCompletedMessages(ctx context.Context, jobIds []string) (int, error)
}
