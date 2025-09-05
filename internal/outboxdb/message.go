package outboxdb

import (
	"context"
	"time"

	"github.com/uptrace/bun"
)

type Status = string

const (
	PENDING      Status = "PENDING"
	PendingRetry Status = "PENDING_RETRY"
	COMPLETED    Status = "COMPLETED"
	FAILED       Status = "FAILED"
	RUNNING      Status = "RUNNING"
)

type Message struct {
	bun.BaseModel `bun:"table:outbox"`

	JobID       string     `bun:"job_id,pk"`
	Topic       string     `bun:"topic,notnull"`
	Key         []byte     `bun:"key"`
	Payload     []byte     `bun:"payload"`
	Partition   *int       `bun:"partition"`
	Headers     []byte     `bun:"headers"`
	Status      Status     `bun:"status,notnull"`
	Retries     int        `bun:"retries,notnull"`
	MaxRetries  int        `bun:"max_retries,notnull"`
	GroupID     string     `bun:"group_id,notnull"`
	Fingerprint string     `bun:"fingerprint,notnull"`
	CreatedAt   time.Time  `bun:"created_at,notnull"`
	UpdatedAt   time.Time  `bun:"updated_at,notnull"`
	CompletedAt *time.Time `bun:"completed_at,notnull"`
	StartedAt   *time.Time `bun:"started_at"`
}

func (m *Message) BeforeAppendModel(ctx context.Context, query bun.Query) error {
	switch query.(type) {
	case *bun.InsertQuery:
		now := time.Now().UTC()
		if m.CreatedAt.IsZero() {
			m.CreatedAt = now
		}
		if m.UpdatedAt.IsZero() {
			m.UpdatedAt = now
		}
	}
	return nil
}

type AdvisoryXactLock struct {
	GroupID int64 `bun:"group_id"`
	Locked  bool  `bun:"locked"`
}
