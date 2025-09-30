package outbox

import "time"

type Clock interface {
	Sleep(d time.Duration)
	Now() time.Time
}

// NewClock returns a Clock which  delegates calls to the actual time.
// Safe use for mocking.
func NewClock() Clock {
	return &clock{}
}

type clock struct{}

func (rc *clock) Sleep(d time.Duration) {
	time.Sleep(d)
}

func (rc *clock) Now() time.Time {
	return time.Now()
}
