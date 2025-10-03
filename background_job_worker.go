package outbox

import (
	"context"
	"log"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"
)

type JobScheduler interface {
	SetUp()
	Start()
	Close()
}

var (
	_ JobRegister  = &backgroundJobProcessor{}
	_ JobScheduler = &backgroundJobProcessor{}
)

var (
	cronParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
)

type backgroundJobProcessor struct {
	baseJobHandler
	registeredJobs    map[string]HandleFunc
	jobMetas          []JobMeta
	jobsChan          chan string
	clock             Clock
	shutdown          chan struct{}
	jobSchedulerQueue JobSchedulerQueue[cronJobScheduler]
}

type cronJobScheduler struct {
	meta      JobMeta
	schedule  cron.Schedule
	nextRunAt time.Time
}

func NewBackgroundJobProcessor(conf *Config, db outboxdb.OutboxMaintenanceDB, clock Clock) *backgroundJobProcessor {
	b := baseJobHandler{conf: conf, db: db}

	bgJobProcessor := &backgroundJobProcessor{
		baseJobHandler:    b,
		registeredJobs:    make(map[string]HandleFunc),
		clock:             clock,
		jobMetas:          make([]JobMeta, 0),
		jobsChan:          make(chan string),
		shutdown:          make(chan struct{}),
		jobSchedulerQueue: NewJobSchedulerQueue(),
	}

	return bgJobProcessor
}

func (b *backgroundJobProcessor) SetUp() {
	handlers := []JobHandler{
		newCleanUpJob(b.conf, b.db),
		newOrphanedJob(b.conf, b.db),
		newReindexJobHandler(b.conf, b.db),
	}

	for _, j := range handlers {
		b.Register(j)
	}
}

func (b *backgroundJobProcessor) Register(handle JobHandler) {
	handleFunc := func(ctx context.Context) error {
		return handle.Handle(ctx)
	}
	b.registeredJobs[handle.Name()] = handleFunc
	b.jobMetas = append(b.jobMetas, handle)
}

func (b *backgroundJobProcessor) Start() {
	go b.cronJobOrchestrator()

	for range 3 {
		go b.cronJobExecutor()
	}
}

func (b *backgroundJobProcessor) Close() {
	b.shutdown <- struct{}{}
	// TODO: Make sure everything gracefully shuts down.
	close(b.jobsChan)
}

func (b *backgroundJobProcessor) cronJobOrchestrator() {
	for _, j := range b.jobMetas {
		schedule, err := cronParser.Parse(j.PeriodicSchedule())
		if err != nil {
			log.Println("unable to parse crontab schedule", j.Name(), j.PeriodicSchedule())
			continue
		}
		cronJob := &cronJobScheduler{
			meta:      j,
			schedule:  schedule,
			nextRunAt: schedule.Next(b.clock.Now()),
		}
		b.jobSchedulerQueue.Push(cronJob)
	}

	for {
		cronJob := b.jobSchedulerQueue.Pop()
		dur := cronJob.nextRunAt.Sub(b.clock.Now())
		// In case of negative make sure ticker to just set ticker to really low, so it can fire right away.
		// I don't know how practical this edge case could be, but seems fine to at least have this safeguard,
		// so NewTicker won't cause a potential panic, due to weird timing of negative.
		if dur < 0 {
			dur = time.Millisecond * 50
		}
		wait := time.NewTicker(dur)
		select {
		case <-b.shutdown:
			return
		case <-wait.C:
		}

		now := b.clock.Now()
		cronJob.nextRunAt = cronJob.schedule.Next(now)
		b.jobSchedulerQueue.Push(cronJob)

		// TODO: At some point, another step is needed before running crons.
		// Need to ensure a stateful process of storing cron runs, before executing, to ensure they should indeed
		// be idempotent. Due to HA environments could have many of the same cron triggered at same time.
		b.jobsChan <- cronJob.meta.Name()
	}
}

func (b *backgroundJobProcessor) cronJobExecutor() {
	for {
		select {
		case <-b.shutdown:
			return
		case cronName := <-b.jobsChan:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
			handler := b.registeredJobs[cronName]
			err := handler(ctx)
			if err != nil {
				log.Println("failed to execute")
			}
			cancel()
		}
	}
}
