package outbox

import "time"

const (
	MaxJobSize = 500
)

type Config struct {
	JobPollInterval    time.Duration
	JobStalledInterval time.Duration
	MaxJobSize         int
}

type ConfigFunc func(c *Config)

func NewConfig(opts ...ConfigFunc) *Config {
	c := &Config{
		JobPollInterval:    time.Duration(15) * time.Second,
		JobStalledInterval: time.Duration(2) * time.Minute,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func WithJobPollInterval(interval time.Duration) ConfigFunc {
	return func(c *Config) {
		c.JobPollInterval = interval
	}
}

func WithJobStallPollInterval(interval time.Duration) ConfigFunc {
	return func(c *Config) {
		c.JobStalledInterval = interval
	}
}

func WithMaxJobSize(size int) ConfigFunc {
	return func(c *Config) {
		c.MaxJobSize = size
	}
}
