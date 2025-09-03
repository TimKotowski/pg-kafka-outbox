package outboxdb_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/ory/dockertest"
	"github.com/stretchr/testify/assert"
	"github.com/uptrace/bun/extra/bundebug"

	"github.com/TimKotowski/pg-kafka-outbox/hash"
	"github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"
	"github.com/TimKotowski/pg-kafka-outbox/testHelper/postgres"
)

func TestAdvisoryXactLock(t *testing.T) {
	pool, err := dockertest.NewPool("")
	assert.NoError(t, err)
	resource := postgres.SetUp(pool, t)

	t.Run("simple", func(t *testing.T) {
		f := hash.NewHash(sha256.New())
		f.Write([]byte("user-42"))

		resource.DB.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(true)))

		id := ulid.Make().String()
		message := &outboxdb.Message{
			JobID:       id,
			Topic:       "user.signup",
			Key:         []byte("user-42"),
			Payload:     []byte(`{"name":"Alice","email":"alice@example.com"}`),
			Headers:     []byte(`{"header1":"value1"}`),
			Status:      outboxdb.PENDING,
			Retries:     0,
			MaxRetries:  5,
			GroupID:     f.Key(),
			Fingerprint: "fp-abc123",
		}

		ctx := context.Background()
		res, err := resource.DB.NewInsert().Model(message).Exec(ctx)
		assert.NoError(t, err)
		fmt.Println(res.RowsAffected())

		rr := outboxdb.NewOutboxDB(resource.DB)

		rss, err := rr.GetPendingMessagesFIFO(ctx)
		assert.NoError(t, err)
		fmt.Println(rss)
		// var m outboxdb.Message
		// err = resource.DB.NewSelect().Model(&m).Where("job_id = ?", id).Scan(ctx)
		// assert.NoError(t, err)
		// fmt.Println(m)
	})
}
