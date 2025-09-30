package outbox

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/jonboulle/clockwork"

	"github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"
	"github.com/TimKotowski/pg-kafka-outbox/migrations"

	"github.com/uptrace/bun"
)

const (
	uninitialized = iota
	running
)

type Outbox struct {
	ctx        context.Context
	conf       *Config
	repository outboxdb.OutboxDB
	db         *bun.DB
	state      atomic.Uint32
}

func NewFromConfig(ctx context.Context, conf *Config) (*Outbox, error) {
	db, err := initializeDB(conf)
	if err != nil {
		return nil, err
	}
	repository := outboxdb.NewOutboxDB(db, conf.FetchLimit)

	return &Outbox{
		ctx:        ctx,
		conf:       conf,
		repository: repository,
		db:         db,
		state:      atomic.Uint32{},
	}, nil
}

func (o *Outbox) Init() error {
	if !o.state.CompareAndSwap(uninitialized, running) {
		return errors.New("initializing outbox already occurred, and outbox is actively running")
	}

	if err := migrations.Migrate(o.ctx, o.db); err != nil {
		return err
	}

	processor := NewBackgroundJobProcessor(o.conf, nil, clockwork.NewRealClock())
	processor.Start()

	return nil
}

func (o *Outbox) EnqueueBatchMessages(messages []Message) error {
	return nil
}

func (o *Outbox) EnqueueMessage(message Message) error {
	return nil
}
