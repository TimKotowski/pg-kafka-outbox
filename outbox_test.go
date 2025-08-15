package outbox_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	outbox "github.com/TimKotowski/pg-kafka-outbox"
)

var _ outbox.Consumer = &TestConsumer{}

type TestConsumer struct {
	ops atomic.Uint64
}

func TestConfig(t *testing.T) {
	c := outbox.NewConfig(
		outbox.WithJobPollInterval(time.Duration(5) * time.Second),
	)
	assert.Equal(t, time.Duration(5)*time.Second, c.JobPollInterval)
}

func TestStartConsumerDryRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	availableThreads := 10
	consumer := TestConsumer{
		ops: atomic.Uint64{},
	}

	config := outbox.NewConfig(outbox.WithJobPollInterval(time.Duration(50) * time.Millisecond))

	o := outbox.NewOutbox(config)
	err := o.Init()
	assert.NoError(t, err)

	c := outbox.NewConsumer(ctx, config)
	for i := 0; i < availableThreads; i++ {
		err = c.StartConsumer(ctx, &consumer)
		assert.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		return consumer.ops.Load() == uint64(availableThreads)
	},
		time.Duration(10)*time.Second, time.Duration(15)*time.Millisecond,
	)
}

func (t *TestConsumer) Consume(ctx context.Context, claim outbox.ConsumerClaim) {
	t.ops.Add(1)
}
