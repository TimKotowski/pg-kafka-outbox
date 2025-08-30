package outbox

import (
	"context"
	"errors"

	"github.com/TimKotowski/pg-kafka-outbox/internal/repository"
	"github.com/TimKotowski/pg-kafka-outbox/migrations"
	"github.com/uptrace/bun"
)

type Outbox struct {
	ctx        context.Context
	conf       *Config
	repository repository.OutboxDB
	db         *bun.DB
	worker     *worker
}

func NewFromConfig(ctx context.Context, conf *Config) (*Outbox, error) {
	db, err := initalizeDB(conf)
	if err != nil {
		return nil, err
	}

	repository := repository.NewRepository(db)
	worker := newWorker(ctx, conf, repository)

	return &Outbox{
		ctx:        ctx,
		conf:       conf,
		repository: repository,
		db:         db,
		worker:     worker,
	}, nil
}

func (o *Outbox) Init() error {
	if !o.worker.state.CompareAndSwap(unintialized, running) {
		return errors.New("initalizing outbox already occured, and outbox is activley running")
	}

	if err := migrations.Migrate(o.ctx, o.db); err != nil {
		return err
	}

	for range o.worker.availableWorkers {
		go o.worker.start()
	}

	return nil
}

func (o *Outbox) EnqueueBatchMessages(messages []Message) error {
	return nil
}

func (o *Outbox) EnqueueMessage(message Message) error {
	return nil
}
