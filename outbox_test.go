package outbox_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest"
	"github.com/stretchr/testify/assert"

	outbox "github.com/TimKotowski/pg-kafka-outbox"
	"github.com/TimKotowski/pg-kafka-outbox/testHelper/postgres"
)

var _ outbox.Consumer = &TestConsumer{}

type TestConsumer struct {
	ops atomic.Uint64
}

func TestConfig(t *testing.T) {
	pool, err := dockertest.NewPool("")
	assert.NoError(t, err)
	resource := postgres.SetUp(pool, t)

	t.Run("config options allow correct setting", func(t *testing.T) {
		c := outbox.NewConfig(
			outbox.WithJobPollInterval(time.Duration(5) * time.Second),
		)
		assert.Equal(t, time.Duration(5)*time.Second, c.JobPollInterval)
	})

	t.Run("consumer to start succesfully", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		availableThreads := 10
		consumer := TestConsumer{
			ops: atomic.Uint64{},
		}

		config := outbox.NewConfig(
			outbox.WithJobPollInterval(time.Duration(50)*time.Millisecond),
			outbox.WithDSN(resource.Dsn),
		)

		o, err := outbox.NewFromConfig(ctx, config)
		assert.NoError(t, err)
		err = o.Init()
		assert.NoError(t, err)

		c := outbox.NewConsumer(o)
		for i := 0; i < availableThreads; i++ {
			err = c.StartConsumer(&consumer)
			assert.NoError(t, err)
		}

		assert.Eventually(t, func() bool {
			return consumer.ops.Load() == uint64(availableThreads)
		},
			time.Duration(10)*time.Second, time.Duration(15)*time.Millisecond,
		)
	})
}

func (t *TestConsumer) Consume(claim outbox.ConsumerClaim) {
	t.ops.Add(1)
}
