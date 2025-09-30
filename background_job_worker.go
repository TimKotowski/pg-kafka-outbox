package outbox

import (
	"context"
	"log"
	"slices"
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
	_ JobRegister  = &BackgroundJobProcessor{}
	_ JobScheduler = &BackgroundJobProcessor{}
)
var (
	cronParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
)

type BackgroundJobProcessor struct {
	baseJobHandler
	registeredJobs map[string]HandleFunc
	jobMetas       []JobMeta
	jobsChan       chan string
	clock          Clock
	shutdown       chan struct{}
}

type cronJobScheduler struct {
	meta             JobMeta
	schedule         cron.Schedule
	nextScheduleTime time.Time
}

func NewBackgroundJobProcessor(conf *Config, db outboxdb.OutboxMaintenanceDB, clock Clock) *BackgroundJobProcessor {
	b := baseJobHandler{conf: conf, db: db}
	bgJobProcessor := &BackgroundJobProcessor{
		baseJobHandler: b,
		registeredJobs: make(map[string]HandleFunc),
		clock:          clock,
		jobMetas:       make([]JobMeta, 0),
		jobsChan:       make(chan string),
		shutdown:       make(chan struct{}),
	}

	return bgJobProcessor
}

func (b *BackgroundJobProcessor) SetUp() {
	handlers := []JobHandler{
		newCleanUpJob(b.conf, b.db),
		newOrphanedJob(b.conf, b.db),
		newReindexJobHandler(b.conf, b.db),
	}

	for _, j := range handlers {
		b.Register(j)
	}
}

func (b *BackgroundJobProcessor) Register(handle JobHandler) {
	handleFunc := func(ctx context.Context) error {
		return handle.Handle(ctx)
	}
	b.registeredJobs[handle.Name()] = handleFunc
	b.jobMetas = append(b.jobMetas, handle)
}

func (b *BackgroundJobProcessor) Start() {
	go b.cronJobOrchestrator()

	for range 3 {
		go b.cronJobExecutor()
	}
}

func (b *BackgroundJobProcessor) Close() {
	b.shutdown <- struct{}{}
	// TODO: Make sure everything gracefully shuts down.
	close(b.jobsChan)
}

func (b *BackgroundJobProcessor) cronJobOrchestrator() {
	cronJobs := make([]cronJobScheduler, len(b.jobMetas))
	for i, j := range b.jobMetas {
		schedule, err := cronParser.Parse(j.PeriodicSchedule())
		if err != nil {
			log.Println("unable to parse crontab schedule", j.Name(), j.PeriodicSchedule())
			continue
		}
		cronJobs[i] = cronJobScheduler{
			meta:             j,
			schedule:         schedule,
			nextScheduleTime: schedule.Next(b.clock.Now()),
		}
	}

	for {
		slices.SortFunc(cronJobs, func(a, b cronJobScheduler) int {
			if a.nextScheduleTime.Before(b.nextScheduleTime) {
				return -1
			}
			if a.nextScheduleTime.After(b.nextScheduleTime) {
				return 1
			}
			return 0
		})

		cronJob := cronJobs[0]
		dur := cronJob.nextScheduleTime.Sub(b.clock.Now())
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

		// Clock work is not actualy
		now := b.clock.Now()
		cronJob.nextScheduleTime = cronJob.schedule.Next(now)
		// in case more than one cron is overdue/ready. This can happen due to more frequent running jobs that
		// eventually overlap with other longer waiting jobs that are ready.
		cronJobsToConsume := []cronJobScheduler{cronJob}
		for i := 1; i < len(cronJobs); i++ {
			if cronJobs[i].nextScheduleTime.Before(now) || cronJobs[i].nextScheduleTime.Equal(now) {
				cronJobs[i].nextScheduleTime = cronJobs[i].schedule.Next(now)
				cronJobsToConsume = append(cronJobsToConsume, cronJobs[i])
			}
		}

		// TODO: At some point, another step is needed before running crons.
		// Need to ensure a stateful process of storing cron runs, before executing, to ensure they should indeed
		// be idempotent. Due to HA environments could have many of the same cron triggered at same time.
		for _, readyJob := range cronJobsToConsume {
			b.jobsChan <- readyJob.meta.Name()
		}
	}
}

func (b *BackgroundJobProcessor) cronJobExecutor() {
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
