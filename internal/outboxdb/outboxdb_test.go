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

	t.Run("will allow transactional ordering processing, for kafka key, with at most only 10 messages running", func(t *testing.T) {
		var messages []outboxdb.Message

		topic := "public.topic.v1"
		key := []byte("user-42")
		f := hash.NewHash(sha256.New())
		f.Write([]byte(topic), key)

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

		signal := make(chan struct{}, 1)
		r := outboxdb.NewOutboxDB(resource.DB)
		wg := sync.WaitGroup{}
		wg.Add(5)

		for range 5 {
			go func(signal chan struct{}) {
				defer wg.Done()
				<-signal
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

	t.Run("will allow transactional ordering processing, for many different kafka key, with at most only 10 messages running", func(t *testing.T) {
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
		wg.Add(5)

		for range 5 {
			go func(signal chan struct{}) {
				defer wg.Done()
				<-signal
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

		fmt.Println(userMessages, orderMessages)
	})
}
