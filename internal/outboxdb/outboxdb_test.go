package outboxdb_test

import (
	"context"
	"crypto/sha256"
	"slices"
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

func TestFifoMessageProcessing(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	assert.NoError(t, err)
	resource := postgres.SetUp(pool, t)

	t.Run("will allow order processing semantics,  with at most only 10 messages running", func(t *testing.T) {
		defer resource.CleanUpJobs(t.Context(), t)

		topic := "public.topic.v1"
		key := []byte("user-42")
		f := hash.NewHash(sha256.New())
		f.Write([]byte(topic), key)

		messages := generateOutBoxMessages(key, nil, topic, 20, outboxdb.PENDING)

		ctx := context.Background()
		_, err := resource.DB.NewInsert().Model(&messages).Exec(ctx)
		assert.NoError(t, err)

		r := outboxdb.NewOutboxDB(resource.DB, 0)

		_, err = r.GetPendingMessagesFIFO(ctx)
		assert.NoError(t, err)

		var updatedMessages []outboxdb.Message
		err = resource.DB.NewSelect().Model(&updatedMessages).Scan(ctx)
		assert.NoError(t, err)

		// Correct precessing ordering. Should be monotonically increasing order by created based on status
		sortMessagesByStatusAndCreatedAt(updatedMessages)
		runningMessages := updatedMessages[:10]
		pendingMessages := updatedMessages[10:]
		assert.Len(t, runningMessages, 10)
		assert.Len(t, pendingMessages, 10)

		// Ensure messages with the same timestamp maintain correct order.
		// Verify that the last running message's timestamp is less than or equal to the pending message's timestamp.
		assert.LessOrEqual(t, runningMessages[len(runningMessages)-1].CreatedAt, pendingMessages[0].CreatedAt)

		assertRunningInRepository(t, runningMessages[:10])
		assertPendingInRepository(t, pendingMessages[10:])
	})

	t.Run("will not allow processing more messages for a group, if there are already processing messages for group", func(t *testing.T) {
		defer resource.CleanUpJobs(t.Context(), t)

		topic := "public.topic.v1"
		key := []byte("user-42")
		f := hash.NewHash(sha256.New())
		f.Write([]byte(topic), key)

		messagesRunning := generateOutBoxMessages(key, nil, topic, 10, outboxdb.RUNNING)
		_, err := resource.DB.NewInsert().Model(&messagesRunning).Exec(t.Context())
		assert.NoError(t, err)
		messagesPending := generateOutBoxMessages(key, nil, topic, 10, outboxdb.PENDING)

		_, err = resource.DB.NewInsert().Model(&messagesPending).Exec(t.Context())
		assert.NoError(t, err)

		r := outboxdb.NewOutboxDB(resource.DB, 0)

		results, err := r.GetPendingMessagesFIFO(t.Context())
		assert.Nil(t, results)

		var updatedMessages []outboxdb.Message
		err = resource.DB.NewSelect().Model(&updatedMessages).Scan(t.Context())
		assert.NoError(t, err)
		sortMessagesByStatusAndCreatedAt(updatedMessages)

		runningMessages := updatedMessages[:10]
		pendingMessages := updatedMessages[10:]

		assert.Len(t, runningMessages, 10)
		assert.Len(t, pendingMessages, 10)

		// Ensure messages with the same timestamp maintain correct order.
		// Verify that the last running message's timestamp is less than or equal to the pending message's timestamp.
		assert.LessOrEqual(t, runningMessages[len(runningMessages)-1].CreatedAt, pendingMessages[0].CreatedAt)

		assertRunningInRepository(t, runningMessages[:10])
		assertPendingInRepository(t, pendingMessages[10:])
	})

	t.Run("will process messages in high availability environments or concurrent worker processes", func(t *testing.T) {
		defer resource.CleanUpJobs(t.Context(), t)

		userTopic := "public.user.v1"
		userKafkaKey1 := []byte("user: 82912190892o1kow1_9121112121, name: Bob")
		userKafkaKey2 := []byte("user: 9292932923_299232332, name: Tim")

		orderTopic := "public.orders.v1"
		orderKafkaKey := []byte("order_id: 1fb9cec5-b44f-4984-9f88-622a4f6e0618, event_type: PURCHASE")

		messagesPendingUserGroup := generateOutBoxMessages(userKafkaKey1, nil, userTopic, 20, outboxdb.PENDING)

		_, err = resource.DB.NewInsert().Model(&messagesPendingUserGroup).Exec(t.Context())
		assert.NoError(t, err)
		messagesPendingUserGroup2 := generateOutBoxMessages(userKafkaKey2, nil, userTopic, 20, outboxdb.PENDING)

		_, err = resource.DB.NewInsert().Model(&messagesPendingUserGroup2).Exec(t.Context())
		assert.NoError(t, err)
		messagesPendingOrderGroup := generateOutBoxMessages(orderKafkaKey, nil, orderTopic, 20, outboxdb.PENDING)

		_, err = resource.DB.NewInsert().Model(&messagesPendingOrderGroup).Exec(t.Context())
		assert.NoError(t, err)
		ctx := context.Background()

		signal := make(chan struct{}, 1)
		errChan := make(chan error, 8)
		wg := sync.WaitGroup{}
		wg.Add(8)

		for range 8 {
			go func(signal chan struct{}) {
				<-signal
				defer wg.Done()

				r := outboxdb.NewOutboxDB(resource.DB, 0)
				_, err := r.GetPendingMessagesFIFO(ctx)
				errChan <- err
			}(signal)
		}

		close(signal)
		wg.Wait()
		close(errChan)

		for err := range errChan {
			assert.NoError(t, err)
		}

		var updatedMessages []outboxdb.Message
		err = resource.DB.NewSelect().Model(&updatedMessages).Scan(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 60, len(updatedMessages))

		userMessages, orderMessages := testHelper.Partition(updatedMessages, func(message outboxdb.Message) bool {
			return message.Topic == userTopic
		})

		userKeyMap := testHelper.GroupBy(userMessages, func(m outboxdb.Message) string {
			return m.GroupID
		})

		group1Hash := hash.NewHash(sha256.New())
		group1Hash.Write([]byte(userTopic), userKafkaKey1)
		userGroup1, group1Found := userKeyMap[group1Hash.Key()]
		assert.True(t, group1Found)
		assert.Len(t, userGroup1, 20)

		sortMessagesByStatusAndCreatedAt(userGroup1)
		runningGroup1Messages := userGroup1[:10]
		pendingGroup1Messages := userGroup1[10:]
		// Ensure messages with the same timestamp maintain correct order.
		// Verify that the last running message's timestamp is less than or equal to the pending message's timestamp.
		assert.LessOrEqual(t, runningGroup1Messages[len(runningGroup1Messages)-1].CreatedAt, pendingGroup1Messages[0].CreatedAt)

		assertRunningInRepository(t, runningGroup1Messages)
		assertPendingInRepository(t, pendingGroup1Messages)

		group2Hash := hash.NewHash(sha256.New())
		group2Hash.Write([]byte(userTopic), userKafkaKey2)
		userGroup2, group2Found := userKeyMap[group2Hash.Key()]
		assert.True(t, group2Found)
		sortMessagesByStatusAndCreatedAt(userGroup2)

		runningGroup2Messages := userGroup2[:10]
		pendingGroup2Messages := userGroup2[10:]
		// Ensure messages with the same timestamp maintain correct order.
		// Verify that the last running message's timestamp is less than or equal to the pending message's timestamp.
		assert.LessOrEqual(t, runningGroup2Messages[len(runningGroup2Messages)-1].CreatedAt, pendingGroup2Messages[0].CreatedAt)
		assertRunningInRepository(t, userGroup2[:10])
		assertPendingInRepository(t, userGroup2[10:])

		sortMessagesByStatusAndCreatedAt(orderMessages)
		runningGroup3Messages := orderMessages[:10]
		pendingGroup3Messages := orderMessages[10:]
		// Ensure messages with the same timestamp maintain correct order.
		// Verify that the last running message's timestamp is less than or equal to the pending message's timestamp.
		assert.LessOrEqual(t, runningGroup3Messages[len(runningGroup3Messages)-1].CreatedAt, pendingGroup3Messages[0].CreatedAt)
		assertRunningInRepository(t, orderMessages[:10])
		assertPendingInRepository(t, orderMessages[10:])
	})

	t.Run("multiple groups with mixed statuses", func(t *testing.T) {
		defer resource.CleanUpJobs(t.Context(), t)
		// Setup: Group1: 5 PENDING; Group2: 5 PENDING; Group3: 5 PENDING + 1 RUNNING (ineligible).
		var messages []outboxdb.Message
		userTopic := "public.user.v1"
		userKafkaKey1 := []byte("key_00001")
		group1Pending := generateOutBoxMessages(userKafkaKey1, nil, userTopic, 5, outboxdb.PENDING)

		userKafkaKey2 := []byte("key_0000002")
		group2Pending := generateOutBoxMessages(userKafkaKey2, nil, userTopic, 5, outboxdb.PENDING)

		userKafkaKey3 := []byte("key_0000003")
		group3Running := generateOutBoxMessages(userKafkaKey3, nil, userTopic, 1, outboxdb.RUNNING)
		group3Pending := generateOutBoxMessages(userKafkaKey3, nil, userTopic, 5, outboxdb.PENDING)

		messages = append(messages, group1Pending...)
		messages = append(messages, group2Pending...)
		messages = append(messages, group3Pending...)
		messages = append(messages, group3Running...)

		ctx := context.Background()
		_, err := resource.DB.NewInsert().Model(&messages).Exec(ctx)
		assert.NoError(t, err)

		r := outboxdb.NewOutboxDB(resource.DB, 0)

		_, err = r.GetPendingMessagesFIFO(ctx)
		assert.NoError(t, err)

		var updatedMessages []outboxdb.Message
		err = resource.DB.NewSelect().Model(&updatedMessages).Scan(ctx)
		assert.NoError(t, err)

		groupMap := testHelper.GroupBy(updatedMessages, func(m outboxdb.Message) string {
			return m.GroupID
		})

		group1Hash := hash.NewHash(sha256.New())
		group1Hash.Write([]byte(userTopic), userKafkaKey1)
		group1, group1Found := groupMap[group1Hash.Key()]
		assert.True(t, group1Found)
		assert.Len(t, group1, 5)
		sortMessagesByStatusAndCreatedAt(group1)
		assertRunningInRepository(t, group1)

		group2Hash := hash.NewHash(sha256.New())
		group2Hash.Write([]byte(userTopic), userKafkaKey2)
		group2, group2Found := groupMap[group2Hash.Key()]
		assert.True(t, group2Found)
		assert.Len(t, group2, 5)
		sortMessagesByStatusAndCreatedAt(group2)
		assertRunningInRepository(t, group2)

		group3Hash := hash.NewHash(sha256.New())
		group3Hash.Write([]byte(userTopic), userKafkaKey3)
		group3, group3Found := groupMap[group3Hash.Key()]
		assert.True(t, group3Found)
		assert.Len(t, group3, 6)

		sortMessagesByStatusAndCreatedAt(group3)
		runningMessage := group3[0]
		pendingMessages := group3[1:]
		assert.NotNil(t, runningMessage.StartedAt)
		assert.Equal(t, outboxdb.RUNNING, runningMessage.Status)
		assertPendingInRepository(t, pendingMessages)
	})

	t.Run("groups with pending retries can still be processed", func(t *testing.T) {
		defer resource.CleanUpJobs(t.Context(), t)

		topic := "public.topic.v1"
		key := []byte("user-42")
		f := hash.NewHash(sha256.New())
		f.Write([]byte(topic), key)

		messages := generateOutBoxMessages(key, nil, topic, 5, outboxdb.PendingRetry)

		ctx := context.Background()
		_, err := resource.DB.NewInsert().Model(&messages).Exec(ctx)
		assert.NoError(t, err)

		r := outboxdb.NewOutboxDB(resource.DB, 0)

		_, err = r.GetPendingMessagesFIFO(ctx)
		assert.NoError(t, err)

		var updatedMessages []outboxdb.Message
		err = resource.DB.NewSelect().Model(&updatedMessages).Scan(ctx)
		assert.NoError(t, err)

		// Correct precessing ordering. Should be monotonically increasing order by created based on status
		sortMessagesByStatusAndCreatedAt(updatedMessages)
		assertRunningInRepository(t, updatedMessages)
	})
}

func TestStandardMessageProcessing(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	assert.NoError(t, err)
	resource := postgres.SetUp(pool, t)

	t.Run("will process messages in high availability environments or concurrent worker processes", func(t *testing.T) {
		defer resource.CleanUpJobs(t.Context(), t)

		topic := "public.topic.v1"
		key := []byte("user-42")
		f := hash.NewHash(sha256.New())
		f.Write([]byte(topic), key)

		messages := generateOutBoxMessages(key, nil, topic, 5, outboxdb.PENDING)

		ctx := context.Background()
		_, err := resource.DB.NewInsert().Model(&messages).Exec(ctx)
		assert.NoError(t, err)

		signal := make(chan struct{}, 1)
		errChan := make(chan error, 4)
		wg := sync.WaitGroup{}
		wg.Add(4)

		for range 4 {
			go func(signal chan struct{}) {
				<-signal
				defer wg.Done()

				r := outboxdb.NewOutboxDB(resource.DB, 1)
				_, err := r.GetPendingMessages(ctx)
				errChan <- err
			}(signal)
		}

		close(signal)
		wg.Wait()
		close(errChan)

		for err := range errChan {
			assert.NoError(t, err)
		}

		var updatedMessages []outboxdb.Message
		err = resource.DB.NewSelect().Model(&updatedMessages).Scan(ctx)
		assert.NoError(t, err)
	})
}

func generateOutBoxMessages(key, payload []byte, topic string, amount int, status outboxdb.Status) []outboxdb.Message {
	var messages []outboxdb.Message
	f := hash.NewHash(sha256.New())
	f.Write([]byte(topic), key)

	for i := range amount {
		fingerPrintHash := hash.NewHash(sha256.New())
		fingerPrintHash.Write([]byte(topic), key, payload)

		var startedAt *time.Time
		if status != outboxdb.PENDING {
			ptr := time.Now().Add(time.Minute * time.Duration(i))
			startedAt = &ptr
		} else {
			startedAt = nil
		}
		message := outboxdb.Message{
			JobID:       ulid.Make().String(),
			Topic:       topic,
			Key:         key,
			Payload:     []byte(`{"name":"Alice","email":"alice@example.com"}`),
			Headers:     []byte(`{"header1":"value1"}`),
			Status:      status,
			Retries:     1,
			MaxRetries:  5,
			GroupID:     f.Key(),
			Fingerprint: fingerPrintHash.Key(),
			StartedAt:   startedAt,
		}
		messages = append(messages, message)
	}

	return messages
}

func assertRunningInRepository(t *testing.T, messages []outboxdb.Message) {
	for i := 0; i < len(messages)-1; i++ {
		currentMessage := messages[i]
		nextMessage := messages[i+1]

		assert.NotNil(t, currentMessage.StartedAt)
		assert.NotNil(t, nextMessage.StartedAt)
		// Batch messaging could result in messages being inserted at same time, but make sure
		// both are equal at least or current most be less.
		assert.LessOrEqual(t, currentMessage.CreatedAt, nextMessage.CreatedAt, "transactional ordering incorrect")
		assert.Equal(t, outboxdb.RUNNING, currentMessage.Status)
	}
}

func assertPendingInRepository(t *testing.T, messages []outboxdb.Message) {
	for _, pendingMessage := range messages {
		assert.Equal(t, outboxdb.PENDING, pendingMessage.Status)
		assert.Nil(t, pendingMessage.StartedAt)
	}
}

func sortMessagesByStatusAndCreatedAt(messages []outboxdb.Message) {
	slices.SortFunc(messages, func(a, b outboxdb.Message) int {
		if a.Status == outboxdb.RUNNING && b.Status == outboxdb.PENDING {
			return -1
		}

		if a.Status == outboxdb.PENDING && b.Status == outboxdb.RUNNING {
			return 1
		}

		if a.CreatedAt.Before(b.CreatedAt) {
			return -1
		}
		if a.CreatedAt.After(b.CreatedAt) {
			return 1
		}
		return 0
	})
}
