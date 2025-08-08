package outbox

import (
	"context"
	"time"
)

type Consumer interface {
	Claim(ctx context.Context)
}

type ConsumerClaims interface {
	Messages() <-chan Job
}

type consumer struct {
	messages chan Job
	ctx      context.Context
	shutdown chan struct{}
	config   *Config
	// TODO BackendService
}

func newConsumer(ctx context.Context, config *Config) *consumer {
	return &consumer{
		messages: make(chan Job, config.MaxJobSize),
		ctx:      ctx,
		config:   config,
	}
}

func Consume(ctx context.Context, config Config, consumer Consumer) (ConsumerClaims, error) {
	c := newConsumer(ctx, &config)

	go consumer.Claim(ctx)
	go c.consume()

	return c, nil
}

func (c *consumer) consume() {
	ticker := time.NewTicker(c.config.JobPollInterval)
	for {
		select {
		// Allows context passing for graceful closure of channels.
		// Channels are independent of global in-memory storage.
		// Ensures less error-prone behavior by delegating message closure to separate threads,
		// enabling multiple consumer instances based on client requirements.
		case <-c.ctx.Done():
			close(c.messages)
			return
		case <-ticker.C:
		}

		c.messages <- Job{}
	}
}

// Crashed pods, rolling deployments can lead to needing adoption/reprocessing of jobs.
func (c *consumer) consumeOrphanedJobs() {
	ticker := time.NewTicker(c.config.JobStalledInterval)

	for {
		select {
		// Allows context passing for graceful closure of channels.
		// Channels are independent of global in-memory storage.
		// Ensures less error-prone behavior by delegating message closure to separate threads,
		// enabling multiple consumer instances based on client requirements.
		case <-c.ctx.Done():
			close(c.messages)
			return
		case <-ticker.C:

			c.messages <- Job{}
		}
	}
}

// Noop for now.
func (c *consumer) Close() {}

func (c *consumer) Messages() <-chan Job {
	return c.messages
}
