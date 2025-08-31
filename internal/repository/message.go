package repository

import (
	"time"

	"github.com/uptrace/bun"
)

type Status = string

const (
	PENDING       Status = "PENDING"
	SUCCESSFUL    Status = "SUCCESSFUL"
	FAILED        Status = "FAILED"
	RUNNING       Status = "RUNNING"
	PENDING_RETRY Status = "PENDING_RETRY"
)

type Message struct {
	bun.BaseModel `bun:"table:outbox"`

	JobID      string    `bun:"job_id,pk"`
	Topic      string    `bun:"topic, notnull"`
	Key        []byte    `bun:"key"`
	Payload    []byte    `bun:"payload"`
	Partition  *int      `bun:"partition"`
	Headers    []byte    `bun:"headers"`
	GroupID    string    `bun:"group_id, notnull"`
	Status     Status    `bun:"status, notnull"`
	Retries    int       `bun:"retries, notnull"`
	MaxRetries int       `bun:"max_retries, notnull"`
	Priority   time.Time `bun:"priority, notnull"`
	CreatedAt  time.Time `bun:"created_at, notnull"`
	StartedBy  string    `bun:"started_by, notnull"`
	StartedAt  string    `bun:"started_at, notnull"`
}

type AdvisoryXactLock struct {
	Key    string `bun:"key"`
	Locked bool   `bun:"locked"`
}
