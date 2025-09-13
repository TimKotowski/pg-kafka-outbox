package outbox

import (
	"context"
	"time"

	"github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"
)

type Consumer interface {
	Consume(ctx context.Context, ack Acknowledger, claim ConsumerClaim)
}

type ConsumerHandler interface {
	StartConsumer(consumer Consumer) error
}

type ConsumerClaim interface {
	Messages() <-chan Message
}

type consumer struct {
	ctx        context.Context
	conf       *Config
	outbox     *Outbox
	repository outboxdb.OutboxDB
}

func NewConsumer(outbox *Outbox) ConsumerHandler {
	return &consumer{
		ctx:    outbox.ctx,
		conf:   outbox.conf,
		outbox: outbox,
	}
}

// StartConsumer starts up an independent consumer.
// Consumers are thread safe, allowing many Consumers to be instantiated.
// Each with their own separate processing of outbox messages.
func (c *consumer) StartConsumer(consumer Consumer) error {
	receiver := newConsumerClaim(c.conf)
	ack := newAcknowledgement(c.repository)

	go consumer.Consume(c.ctx, ack, receiver)

	go c.consume(receiver)

	return nil
}

func (c *consumer) consume(recv *consumerClaim) {
	ticker := time.NewTicker(c.conf.QueueDelay)
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
	messages chan Message
}

func newConsumerClaim(config *Config) *consumerClaim {
	return &consumerClaim{
		messages: make(chan Message),
	}
}

func (c *consumerClaim) Messages() <-chan Message {
	return c.messages
}
