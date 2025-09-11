package outboxdb_test

import (
	"context"
	"crypto/sha256"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/ory/dockertest"
	"github.com/stretchr/testify/assert"

	"github.com/TimKotowski/pg-kafka-outbox/hash"
	"github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"
	"github.com/TimKotowski/pg-kafka-outbox/testHelper"
	"github.com/TimKotowski/pg-kafka-outbox/testHelper/postgres"
)

type TestFifoTuple struct {
	topic string
	key   []byte
}

func TestFifoMessageProcessing(t *testing.T) {
	pool, err := dockertest.NewPool("")
	assert.NoError(t, err)
	resource := postgres.SetUp(pool, t)

	t.Run("will allow order processing semantics,  with at most only 10 messages running", func(t *testing.T) {
		defer resource.CleanUpJobs(t.Context(), t)

		topic := "public.topic.v1"
		key := []byte("user-42")
		f := hash.NewHash(sha256.New())
		f.Write([]byte(topic), key)

		var messages []outboxdb.Message
		for i := range 20 {
			message := outboxdb.Message{
				JobID:       ulid.Make().String(),
				Topic:       topic,
				Key:         key,
				Payload:     []byte(`{"name":"Alice","email":"alice@example.com"}`),
				Headers:     []byte(`{"header1":"value1"}`),
				Status:      outboxdb.PENDING,
				Retries:     1,
				MaxRetries:  5,
				GroupID:     f.Key(),
				Fingerprint: "fp-abc123",
				CreatedAt:   time.Now().Add(time.Second * time.Duration(i)),
			}

			messages = append(messages, message)
		}

		ctx := context.Background()
		_, err := resource.DB.NewInsert().Model(&messages).Exec(ctx)
		assert.NoError(t, err)

		r := outboxdb.NewOutboxDB(resource.DB)

		_, err = r.GetPendingMessagesFIFO(ctx)
		assert.NoError(t, err)

		var updatedMessages []outboxdb.Message
		err = resource.DB.NewSelect().Model(&updatedMessages).Scan(ctx)
		assert.NoError(t, err)

		// Correct precessing ordering. Should be monotonically increasing order by created.
		sort.Slice(updatedMessages, func(i, j int) bool {
			return updatedMessages[i].CreatedAt.Before(updatedMessages[j].CreatedAt)
		})

		runningMessages := updatedMessages[:10]
		pendingMessages := updatedMessages[10:]

		for i := 0; i < len(runningMessages)-1; i++ {
			currentMessage := runningMessages[i]
			nextMessage := runningMessages[i+1]

			assert.NotNil(t, currentMessage.StartedAt)
			assert.NotNil(t, nextMessage.StartedAt)
			assert.True(t, currentMessage.CreatedAt.Before(nextMessage.CreatedAt), "transactional ordering incorrect")
			assert.Equal(t, outboxdb.RUNNING, currentMessage.Status)
		}

		for _, pendingMessage := range pendingMessages {
			assert.Equal(t, outboxdb.PENDING, pendingMessage.Status)
			assert.Nil(t, pendingMessage.StartedAt)
		}
	})

	t.Run("will not allow processing more messages for a group, if there are already processing messages for group", func(t *testing.T) {
		defer resource.CleanUpJobs(t.Context(), t)

		topic := "public.topic.v1"
		key := []byte("user-42")
		f := hash.NewHash(sha256.New())
		f.Write([]byte(topic), key)

		var messages []outboxdb.Message
		for i := range 20 {
			if i < 10 {
				now := time.Now()
				ptr := &now
				message := outboxdb.Message{
					JobID:       ulid.Make().String(),
					Topic:       topic,
					Key:         key,
					Payload:     []byte(`{"name":"Alice","email":"alice@example.com"}`),
					Headers:     []byte(`{"header1":"value1"}`),
					Status:      outboxdb.RUNNING,
					Retries:     1,
					MaxRetries:  5,
					GroupID:     f.Key(),
					Fingerprint: "fp-abc123",
					CreatedAt:   time.Now().Add(time.Second * time.Duration(i)),
					StartedAt:   ptr,
				}

				messages = append(messages, message)
			} else {
				message := outboxdb.Message{
					JobID:       ulid.Make().String(),
					Topic:       topic,
					Key:         key,
					Payload:     []byte(`{"name":"Alice","email":"alice@example.com"}`),
					Headers:     []byte(`{"header1":"value1"}`),
					Status:      outboxdb.PENDING,
					Retries:     1,
					MaxRetries:  5,
					GroupID:     f.Key(),
					Fingerprint: "fp-abc123",
					CreatedAt:   time.Now().Add(time.Second * time.Duration(i)),
				}
				messages = append(messages, message)
			}
		}

		ctx := context.Background()
		_, err := resource.DB.NewInsert().Model(&messages).Exec(ctx)
		assert.NoError(t, err)

		r := outboxdb.NewOutboxDB(resource.DB)

		results, err := r.GetPendingMessagesFIFO(ctx)
		assert.Nil(t, results)

		var updatedMessages []outboxdb.Message
		err = resource.DB.NewSelect().Model(&updatedMessages).Scan(ctx)
		assert.NoError(t, err)
		// Correct precessing ordering. Should be monotonically increasing order by created.
		sort.Slice(updatedMessages, func(i, j int) bool {
			return updatedMessages[i].CreatedAt.Before(updatedMessages[j].CreatedAt)
		})

		runningMessages := updatedMessages[:10]
		pendingMessages := updatedMessages[10:]
		assert.Len(t, runningMessages, 10)
		assert.Len(t, pendingMessages, 10)

		for _, message := range runningMessages {
			assert.Equal(t, message.Status, outboxdb.RUNNING)
			assert.NotNil(t, message.StartedAt)
		}

		for _, message := range pendingMessages {
			assert.Equal(t, message.Status, outboxdb.PENDING)
			assert.Nil(t, message.StartedAt)
		}
	})

	t.Run("will allow transactional ordering processing, for many different kafka keys, with at most only 10 messages running", func(t *testing.T) {
		defer resource.CleanUpJobs(t.Context(), t)

		var testMessages []outboxdb.Message
		tuples := []TestFifoTuple{
			{
				topic: "public.user.v1",
				key:   []byte("user: 82912190892o1kow1_9121112121, name: Bobo"),
			},
			{
				topic: "public.orders.v1",
				key:   []byte("order_id: 1fb9cec5-b44f-4984-9f88-622a4f6e0618, event_type: PURCHASE"),
			},
		}

		for i := range 20 {
			for _, t := range tuples {
				f := hash.NewHash(sha256.New())
				f.Write([]byte(t.topic), t.key)
				message := outboxdb.Message{
					JobID:       ulid.Make().String(),
					Topic:       t.topic,
					Key:         t.key,
					Payload:     []byte(`{"name":"Alice","email":"alice@example.com"}`),
					Headers:     []byte(`{"header1":"value1"}`),
					Status:      outboxdb.PENDING,
					Retries:     1,
					MaxRetries:  5,
					GroupID:     f.Key(),
					Fingerprint: "fp-abc123",
					CreatedAt:   time.Now().Add(time.Minute * time.Duration(i)),
				}

				testMessages = append(testMessages, message)
			}
		}

		assert.Equal(t, 40, len(testMessages))

		ctx := context.Background()
		_, err := resource.DB.NewInsert().Model(&testMessages).Exec(ctx)
		assert.NoError(t, err)

		signal := make(chan struct{}, 1)
		r := outboxdb.NewOutboxDB(resource.DB)
		wg := sync.WaitGroup{}
		wg.Add(8)

		for range 8 {
			go func(signal chan struct{}) {
				<-signal

				defer wg.Done()
				_, err = r.GetPendingMessagesFIFO(ctx)
				assert.NoError(t, err)
			}(signal)
		}

		close(signal)
		wg.Wait()

		var updatedMessages []outboxdb.Message
		err = resource.DB.NewSelect().Model(&updatedMessages).Scan(ctx)
		assert.NoError(t, err)

		// Correct precessing ordering. Should be monotonically increasing order by created.
		sort.Slice(updatedMessages, func(i, j int) bool {
			return updatedMessages[i].CreatedAt.Before(updatedMessages[j].CreatedAt)
		})

		userMessages, orderMessages := testHelper.Partition(updatedMessages, func(message outboxdb.Message) bool {
			userTopic := tuples[0].topic
			return message.Topic == userTopic
		})

		runningUserMessages := userMessages[:10]
		pendingUserMessages := userMessages[10:]
		for i := 0; i < len(runningUserMessages)-1; i++ {
			currentMessage := runningUserMessages[i]
			nextMessage := runningUserMessages[i+1]

			assert.NotNil(t, currentMessage.StartedAt)
			assert.NotNil(t, nextMessage.StartedAt)
			assert.True(t, currentMessage.CreatedAt.Before(nextMessage.CreatedAt), "transactional ordering incorrect")
			assert.Equal(t, outboxdb.RUNNING, currentMessage.Status)
		}

		for _, pendingMessage := range pendingUserMessages {
			assert.Equal(t, outboxdb.PENDING, pendingMessage.Status)
			assert.Nil(t, pendingMessage.StartedAt)
		}

		runningOrderMessages := orderMessages[:10]
		pendingOrderMessages := orderMessages[10:]
		for i := 0; i < len(runningOrderMessages)-1; i++ {
			currentMessage := runningOrderMessages[i]
			nextMessage := runningOrderMessages[i+1]

			assert.NotNil(t, currentMessage.StartedAt)
			assert.NotNil(t, nextMessage.StartedAt)
			assert.True(t, currentMessage.CreatedAt.Before(nextMessage.CreatedAt), "transactional ordering incorrect")
			assert.Equal(t, outboxdb.RUNNING, currentMessage.Status)
		}

		for _, pendingMessage := range pendingOrderMessages {
			assert.Equal(t, outboxdb.PENDING, pendingMessage.Status)
			assert.Nil(t, pendingMessage.StartedAt)
		}
	})

	t.Run("multiple groups with mixed statuses", func(t *testing.T) {
		// Setup: Group1: 5 PENDING; Group2: 5 PENDING; Group3: 5 PENDING + 1 RUNNING (ineligible).
	})
	t.Run("groups with pending retries can still be processed", func(t *testing.T) {

	})
}
