package outbox

import (
	"context"
	"time"

	"github.com/TimKotowski/pg-kafka-outbox/migrations"
)

type Outbox struct {
	ctx    context.Context
	config *Config
	pg     *pg
}

func NewFromConfig(ctx context.Context, config *Config) (*Outbox, error) {
	db, err := initalizeDB(config)
	if err != nil {
		return nil, err
	}

	return &Outbox{
		ctx:    ctx,
		config: config,
		pg:     db,
	}, nil
}

func (o *Outbox) Init() error {
	if err := migrations.Migrate(o.ctx, o.pg.db); err != nil {
		return err
	}
	go o.requeueOrphanedJobs(o.ctx)

	return nil
}

func (o *Outbox) Enqueue() error {
	return nil
}

// Crashed pods, rolling deployments can lead to needing adoption/reprocessing of jobs.
// reQueueOrphanedJobs makes sure jobs are re-added to the queue in a way that they can be re-processed.
func (o *Outbox) requeueOrphanedJobs(ctx context.Context) {
	ticker := time.NewTicker(o.config.JobStalledInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
