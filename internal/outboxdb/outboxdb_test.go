package outboxdb_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

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

		resource.DB.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(true)))
		var messages []outboxdb.Message

		for i := range 20 {
			f := hash.NewHash(sha256.New())
			k := fmt.Sprintf("user-%d", i)
			f.Write([]byte(k))
			message := outboxdb.Message{
				JobID:       ulid.Make().String(),
				Topic:       "public.topic.v1",
				Key:         []byte("user-42"),
				Payload:     []byte(`{"name":"Alice","email":"alice@example.com"}`),
				Headers:     []byte(`{"header1":"value1"}`),
				Status:      outboxdb.PENDING,
				Retries:     1,
				MaxRetries:  5,
				GroupID:     f.Key(),
				Fingerprint: "fp-abc123",
				CreatedAt:   time.Now().Add(time.Hour * time.Duration(i)),
			}

			messages = append(messages, message)
		}

		ctx := context.Background()
		_, err := resource.DB.NewInsert().Model(&messages).Exec(ctx)
		assert.NoError(t, err)

		rr := outboxdb.NewOutboxDB(resource.DB)
		wg := sync.WaitGroup{}
		wg.Add(10)
		for range 10 {
			go func() {
				defer wg.Done()
				_, err = rr.GetPendingMessagesFIFO(ctx)
				fmt.Println(err)
			}()
		}
		wg.Wait()
		var updatedMessages []outboxdb.Message
		err = resource.DB.NewSelect().
			Model(&updatedMessages).
			Scan(ctx)

		// Test first 10 messages were updated only ASC order. Should be monotonically increasing order.
		sort.Slice(updatedMessages, func(i, j int) bool {
			return updatedMessages[i].CreatedAt.Before(updatedMessages[j].CreatedAt)
		})

		fmt.Println(updatedMessages)
	})
}
