package outbox

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/TimKotowski/pg-kafka-outbox/internal/repository"
)

var availableWorkers = 2

const (
	unintialized = iota
	running
)

type worker struct {
	availableWorkers     int
	state                atomic.Uint32
	ctx                  context.Context
	config               *Config
	repository           repository.OutboxDB
	acknowledgedMessages chan Message
}

func newWorker(ctx context.Context, config *Config, repo repository.OutboxDB) *worker {
	return &worker{
		availableWorkers: availableWorkers,
		ctx:              ctx,
		config:           config,
		repository:       repo,
		state:            atomic.Uint32{},
	}
}

func (w *worker) start() {
	ticker := time.NewTicker(w.config.StalledInterval)

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Crashed pods, rolling deployments can lead to needing adoption/reprocessing of jobs.
// reQueueOrphanedJobs makes sure jobs are re-added to the queue in a way that they can be re-processed.
func (w *worker) requeueOrphanedJobs() error {
	return nil
}

func (w *worker) moveToDeadLetter() error {
	return nil
}
