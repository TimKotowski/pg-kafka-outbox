package outbox

import (
	"context"
	"time"
)

type Consumer interface {
	Consume(claim ConsumerClaim)
}

type ConsumerHandler interface {
	StartConsumer(consumer Consumer) error
}

type ConsumerClaim interface {
	Messages() <-chan Job
}

type consumer struct {
	ctx    context.Context
	config *Config
	outbox *Outbox
}

func NewConsumer(outbox *Outbox) ConsumerHandler {
	return &consumer{
		ctx:    outbox.ctx,
		config: outbox.config,
		outbox: outbox,
	}
}

// StartConsumer starts up an independent consumer.
// Consumers are thread safe, allowing many Consumers to be instantiated.
// Each with there own seperate processing of outbox messages.
func (c *consumer) StartConsumer(consumer Consumer) error {
	receiver := newConsumerClaim(c.config)

	go consumer.Consume(receiver)

	go c.consume(receiver)

	return nil
}

func (c *consumer) consume(recv *consumerClaim) {
	ticker := time.NewTicker(c.config.JobPollInterval)
	for {
		select {
		// Allows context passing for graceful closure of channels.
		// Channels are independent of global in-memory storage.
		// Ensures less error-prone behavior by delegating message closure to separate threads,
		// enabling multiple consumer instances based on client requirements.
		case <-c.ctx.Done():
			close(recv.messages)
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
