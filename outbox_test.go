package outbox_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/ory/dockertest"
	"github.com/stretchr/testify/assert"

	outbox "github.com/TimKotowski/pg-kafka-outbox"
	"github.com/TimKotowski/pg-kafka-outbox/testHelper/postgres"
)

var _ outbox.Consumer = &TestConsumer{}

type TestConsumer struct {
	ops atomic.Uint64
}

func TestOutbox(t *testing.T) {
	pool, err := dockertest.NewPool("")
	assert.NoError(t, err)
	resource := postgres.SetUp(pool, t)

	// t.Run("allow to start outbox succesfully with listening consumers", func(t *testing.T) {
	// 	ctx, cancel := context.WithCancel(context.Background())
	// 	defer cancel()
	//
	// 	availableThreads := 10
	// 	consumer := TestConsumer{
	// 		ops: atomic.Uint64{},
	// 	}
	//
	// 	config := outbox.NewConfig(
	// 		outbox.WithJobPollInterval(time.Duration(50)*time.Millisecond),
	// 		outbox.WithDSN(resource.Dsn),
	// 	)
	//
	// 	o, err := outbox.NewFromConfig(ctx, config)
	// 	assert.NoError(t, err)
	// 	err = o.Init()
	// 	assert.NoError(t, err)
	//
	// 	c := outbox.NewConsumer(o)
	// 	for i := 0; i < availableThreads; i++ {
	// 		err = c.StartConsumer(&consumer)
	// 		assert.NoError(t, err)
	// 	}
	//
	// 	assert.Eventually(t, func() bool {
	// 		return consumer.ops.Load() == uint64(availableThreads)
	// 	},
	// 		time.Duration(10)*time.Second, time.Duration(15)*time.Millisecond,
	// 	)
	// })
	//
	// t.Run("outbox initialzing should happen exactly once", func(t *testing.T) {
	// 	ctx, cancel := context.WithCancel(context.Background())
	// 	defer cancel()
	//
	// 	config := outbox.NewConfig(
	// 		outbox.WithJobPollInterval(time.Duration(50)*time.Millisecond),
	// 		outbox.WithDSN(resource.Dsn),
	// 	)
	//
	// 	o, err := outbox.NewFromConfig(ctx, config)
	// 	assert.NoError(t, err)
	// 	err = o.Init()
	// 	assert.NoError(t, err)
	//
	// 	err = o.Init()
	// 	assert.EqualError(t, err, errors.New("initalizing outbox already occured, and outbox is activley running").Error())
	// })
}

func (t *TestConsumer) Consume(ctx context.Context, ack outbox.Acknowledger, claim outbox.ConsumerClaim) {
	t.ops.Add(1)
	for {
		select {
		case _, ok := <-claim.Messages():
			if !ok {
				return
			}

		case <-ctx.Done():
			return
		}
	}
}
