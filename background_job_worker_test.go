package outbox_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	outbox "github.com/TimKotowski/pg-kafka-outbox"
	"github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"
	mock_outbox "github.com/TimKotowski/pg-kafka-outbox/mocks"
	"github.com/TimKotowski/pg-kafka-outbox/testHelper/postgres"
)

var (
	_ outbox.JobHandler = &MockTestHandler0{}
	_ outbox.JobHandler = &MockTestHandler1{}
)

type MockTestHandler0 struct {
	db     outboxdb.OutboxMaintenanceDB
	conf   *outbox.Config
	atomic atomic.Int64
}

func newMockTestJobHandler0(conf *outbox.Config, db outboxdb.OutboxMaintenanceDB) *MockTestHandler0 {
	return &MockTestHandler0{
		db:     db,
		conf:   conf,
		atomic: atomic.Int64{},
	}
}

func (t *MockTestHandler0) PeriodicSchedule() string {
	return "* * * * * *"
}

func (t *MockTestHandler0) Name() string {
	return "Cron Job_0"

}

func (t *MockTestHandler0) Handle(ctx context.Context) error {
	t.atomic.Add(1)
	return nil
}

type MockTestHandler1 struct {
	db     outboxdb.OutboxMaintenanceDB
	conf   *outbox.Config
	atomic atomic.Int64
}

func newMockTestJobHandler1(conf *outbox.Config, db outboxdb.OutboxMaintenanceDB) *MockTestHandler1 {
	return &MockTestHandler1{
		db:     db,
		conf:   conf,
		atomic: atomic.Int64{},
	}
}

func (t *MockTestHandler1) PeriodicSchedule() string {
	return "* * * * * *"
}

func (t *MockTestHandler1) Name() string {
	return "Cron Job_1"

}

func (t *MockTestHandler1) Handle(ctx context.Context) error {
	t.atomic.Add(1)
	return nil
}

func TestSimpleCronJobRun(t *testing.T) {
	pool, err := dockertest.NewPool("")
	assert.NoError(t, err)
	resource := postgres.SetUp(pool, t)

	t.Run("simple", func(t *testing.T) {
		clock := outbox.NewClock()
		db := outboxdb.NewOutboxMaintaner(resource.DB, 1)

		config := outbox.NewConfig()
		testHandler := newMockTestJobHandler0(config, db)
		bg := outbox.NewBackgroundJobProcessor(config, db, clock)

		bg.Register(testHandler)
		bg.Start()

		assert.Eventually(t, func() bool {
			return assert.Greater(t, testHandler.atomic.Load(), int64(0))
		}, time.Second*2, time.Second*1)
	})

	t.Run("multiple crons firing", func(t *testing.T) {
		clock := outbox.NewClock()
		db := outboxdb.NewOutboxMaintaner(resource.DB, 1)

		config := outbox.NewConfig()
		testHandlerOne := newMockTestJobHandler0(config, db)
		testHandlerTwo := newMockTestJobHandler1(config, db)
		bg := outbox.NewBackgroundJobProcessor(config, db, clock)

		bg.Register(testHandlerOne)
		bg.Register(testHandlerTwo)
		bg.Start()

		assert.Eventually(t, func() bool {
			return assert.Greater(t, testHandlerOne.atomic.Load(), int64(0))
		}, time.Second*2, time.Second*1)

		assert.Eventually(t, func() bool {
			return assert.Greater(t, testHandlerTwo.atomic.Load(), int64(0))
		}, time.Second*2, time.Second*1)

	})

	t.Run("ensure negative wait time gracefully handling running crons still", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		m := mock_outbox.NewMockClock(ctrl)
		db := outboxdb.NewOutboxMaintaner(resource.DB, 1)

		config := outbox.NewConfig()
		testHandlerOne := newMockTestJobHandler0(config, db)
		bg := outbox.NewBackgroundJobProcessor(config, db, m)

		m.EXPECT().Now().Return(time.Now())
		m.EXPECT().Now().Return(time.Now().Add(time.Second * 10))
		// After second mock the cron should fire, just make sure last mock is called till assert is complete.
		m.EXPECT().Now().Return(time.Now()).AnyTimes()
		bg.Register(testHandlerOne)
		bg.Start()

		assert.Eventually(t, func() bool {
			return assert.Greater(t, testHandlerOne.atomic.Load(), int64(0))
		}, time.Minute*60, time.Second*1)
	})

	t.Run("idempotence", func(t *testing.T) {

	})
}
