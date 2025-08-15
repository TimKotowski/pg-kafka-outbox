package outbox

import (
	"context"
	"time"
)

type Consumer interface {
	Consume(ctx context.Context, claim ConsumerClaim)
}

type ConsumerHandler interface {
	StartConsumer(ctx context.Context, consumer Consumer) error
}

type ConsumerClaim interface {
	Messages() <-chan Job
}

type consumer struct {
	config *Config
	// TODO BackendService
}

func NewConsumer(ctx context.Context, config *Config) ConsumerHandler {
	return &consumer{
		config: config,
	}
}

// StartConsumer starts up an independent consumer.
// Consumers are thread safe, allowing many Consumers to be instantiated.
// Each with there own seperate processing of outbox messages.
func (c *consumer) StartConsumer(ctx context.Context, consumer Consumer) error {
	receiver := newConsumerClaim(c.config)

	go consumer.Consume(ctx, receiver)

	go c.consume(ctx, receiver)

	return nil
}

func (c *consumer) consume(ctx context.Context, recv *consumerClaim) {
	ticker := time.NewTicker(c.config.JobPollInterval)
	for {
		select {
		// Allows context passing for graceful closure of channels.
		// Channels are independent of global in-memory storage.
		// Ensures less error-prone behavior by delegating message closure to separate threads,
		// enabling multiple consumer instances based on client requirements.
		case <-ctx.Done():
			close(recv.messages)
			return
		case <-ticker.C:
		}
	}
}

// Crashed pods, rolling deployments can lead to needing adoption/reprocessing of jobs.
// reQueueOrphanedJobs makes sure jobs are re-added to the queue in a way that they can be re-processed.
func (c *consumer) reQueueOrphanedJobs(ctx context.Context) {
	ticker := time.NewTicker(c.config.JobStalledInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type consumerClaim struct {
	messages chan Job
}

func newConsumerClaim(config *Config) *consumerClaim {
	return &consumerClaim{
		messages: make(chan Job, config.MaxJobSize),
	}
}

func (c *consumerClaim) Messages() <-chan Job {
	return c.messages
}
