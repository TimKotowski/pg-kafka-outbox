package outbox

import (
	"context"

	"github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"
)

var (
	_ JobHandler = &cleanUpJobHandler{}
	_ JobHandler = &orphanedJobHandler{}
)

type HandleFunc = func(ctx context.Context) error

type JobRegister interface {
	Register(handle JobHandler)
}

type JobHandler interface {
	JobMeta
	Handle(ctx context.Context) error
}

type JobMeta interface {
	PeriodicSchedule() string
	Name() string
}

type baseJobHandler struct {
	db   outboxdb.OutboxMaintenanceDB
	conf *Config
}

type cleanUpJobHandler struct {
	baseJobHandler
}

func newCleanUpJob(conf *Config, db outboxdb.OutboxMaintenanceDB) *cleanUpJobHandler {
	return &cleanUpJobHandler{
		baseJobHandler: baseJobHandler{
			db:   db,
			conf: conf,
		},
	}
}

func (c *cleanUpJobHandler) Handle(ctx context.Context) error {
	return nil
}

// PeriodicSchedule Start little past beginning 7 hour mark to prevent scheduling oddities.
func (c *cleanUpJobHandler) PeriodicSchedule() string {
	return "5 */7 * * *"
}

func (c *cleanUpJobHandler) Name() string {
	return "Clean Up Job"
}

type orphanedJobHandler struct {
	baseJobHandler
}

func newOrphanedJob(conf *Config, db outboxdb.OutboxMaintenanceDB) *orphanedJobHandler {
	return &orphanedJobHandler{
		baseJobHandler: baseJobHandler{
			db:   db,
			conf: conf,
		},
	}
}

func (o *orphanedJobHandler) Handle(ctx context.Context) error {
	return nil
}

// PeriodicSchedule Start little past beginning of every hour to prevent scheduling oddities.
func (o *orphanedJobHandler) PeriodicSchedule() string {
	return "5 * * * *"
}

func (o *orphanedJobHandler) Name() string {
	return "Orphaned Job"
}

type reindexJobHandler struct {
	baseJobHandler
}

func newReindexJobHandler(conf *Config, db outboxdb.OutboxMaintenanceDB) *reindexJobHandler {
	return &reindexJobHandler{
		baseJobHandler: baseJobHandler{
			db:   db,
			conf: conf,
		},
	}
}

func (r *reindexJobHandler) Handle(ctx context.Context) error {
	return nil
}

// PeriodicSchedule Start little past beginning of 12am to prevent scheduling oddities.
func (r *reindexJobHandler) PeriodicSchedule() string {
	return "5 0 * * *"
}

func (r *reindexJobHandler) Name() string {
	return "Orphaned Job"
}
