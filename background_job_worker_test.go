package outbox_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	outbox "github.com/TimKotowski/pg-kafka-outbox"
)

func TestSimpleCronJobRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := outbox.NewConfig()

	o, err := outbox.NewFromConfig(ctx, config)
	assert.NoError(t, err)
	err = o.Init()
	assert.NoError(t, err)
	a := outbox.NewBackgroundJobProcessor()

}
