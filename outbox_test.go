package outbox_test

import (
	"testing"
	"time"

	outbox "github.com/TimKotowski/pg-kafka-outbox"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	c := outbox.NewConfig(
		outbox.WithConnectionString("postgres_connection_string"),
		outbox.WithPollInterval(time.Duration(5)*time.Second),
	)
	assert.Equal(t, "postgres_connection_string", c.ConnectionString)
	assert.Equal(t, time.Duration(5)*time.Second, c.JobPollInterval)
}
