package outbox

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"
	"github.com/TimKotowski/pg-kafka-outbox/migrations"

	"github.com/uptrace/bun"
)

const (
	uninitialized = iota
	running
)

type Outbox struct {
	ctx           context.Context
	conf          *Config
	outboxDB      outboxdb.OutboxDB
	maintenanceDB outboxdb.OutboxMaintenanceDB
	jobScheduler  JobScheduler
	db            *bun.DB
	state         atomic.Uint32
}

func NewFromConfig(ctx context.Context, conf *Config) (*Outbox, error) {
	db, err := initializeDB(conf)
	if err != nil {
		return nil, err
	}
	outboxDB := outboxdb.NewOutboxDB(db, conf.FetchLimit)
	maintenanceDB := outboxdb.NewOutboxMaintaner(db, conf.FetchLimit)
	jobScheduler := NewBackgroundJobProcessor(conf, nil, NewClock())

	return &Outbox{
		ctx:           ctx,
		conf:          conf,
		outboxDB:      outboxDB,
		maintenanceDB: maintenanceDB,
		jobScheduler:  jobScheduler,
		db:            db,
		state:         atomic.Uint32{},
	}, nil
}

func (o *Outbox) Init() error {
	if !o.state.CompareAndSwap(uninitialized, running) {
		return errors.New("initializing outbox already occurred, and outbox is actively running")
	}
	if err := migrations.Migrate(o.ctx, o.db); err != nil {
		return err
	}
	o.jobScheduler.SetUp()
	o.jobScheduler.Start()

	return nil
}

// TODO: add shutdown chan for consumers.
func (o *Outbox) Shutdown() {
	o.jobScheduler.Close()
}

func (o *Outbox) EnqueueBatchMessages(messages []Message) error {
	return nil
}

func (o *Outbox) EnqueueMessage(message Message) error {
	return nil
}
