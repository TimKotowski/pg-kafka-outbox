package outbox

import "time"

type Clock interface {
	Sleep(d time.Duration)
	Now() time.Time
}

// NewClock returns a Clock which calls to the actual time.
// This is used to ensure support of real time for cron scheduling, and mock support.
func NewClock() Clock {
	return &clock{}
}

type clock struct{}

func (c *clock) Sleep(d time.Duration) {
	time.Sleep(d)
}

func (c *clock) Now() time.Time {
	return time.Now()
}
