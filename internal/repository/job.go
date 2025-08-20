package repository

import (
	"time"

	"github.com/uptrace/bun"
)

type Status = string

const (
	SCHEDULED Status = "SCHEDULED"
	COMPLETED Status = "COMPLETED"
	FAILED    Status = "FAILED"
)

type Job struct {
	bun.BaseModel `bun:"table:jobs"`

	JobID      string    `bun:"job_id,pk"`
	Topic      string    `bun:"topic, notnull"`
	Key        []byte    `bun:"key"`
	Payload    []byte    `bun:"payload"`
	Partition  *int      `bun:"partition"`
	Headers    []byte    `bun:"headers"`
	Status     Status    `bun:"status, notnull"`
	Retries    int       `bun:"retries, notnull"`
	MaxRetries int       `bun:"max_retries, notnull"`
	Priority   time.Time `bun:"priority, notnull"`
	CreatedAt  time.Time `bun:"created_at, notnull"`
	StartedBy  string    `bun:"started_by, notnull"`
	StartedAt  string    `bun:"started_at, notnull"`
}
