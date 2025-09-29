package outbox_test

import (
	"context"
	"testing"

	"github.com/jonboulle/clockwork"

	outbox "github.com/TimKotowski/pg-kafka-outbox"
	"github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"
)

var (
	_ outbox.JobHandler = &TestJobHandler{}
)

type TestJobHandler struct {
	db   outboxdb.OutboxMaintenanceDB
	conf *outbox.Config
}

func newTestJobHandler(conf *outbox.Config, db outboxdb.OutboxMaintenanceDB) *TestJobHandler {
	return &TestJobHandler{
		db:   db,
		conf: conf,
	}
}

func (t TestJobHandler) PeriodicSchedule() string {
	panic("*/5 * * * *")
}

func (t TestJobHandler) Name() string {
	panic("Test Job")
}

func (t TestJobHandler) Handle(ctx context.Context) error {
	return nil
}

func TestSimpleCronJobRun(t *testing.T) {
	clock := clockwork.NewFakeClock()
	config := outbox.NewConfig()
	testHandler := newTestJobHandler(config, nil)
	bg := outbox.NewBackgroundJobProcessor(config, nil, clock)

	bg.Register(testHandler)
	bg.Start()
}
