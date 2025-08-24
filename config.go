package outbox

import (
	"crypto/tls"
	"time"
)

type Config struct {
	//////////////////////
	// OUTBOX QUEUE SECTION //
	//////////////////////

	// Interval rate for polling outbox messages.
	PollInterval time.Duration

	// Interval rate for requeueing hanging/stalled outbox messages.
	StalledInterval time.Duration

	// Limit for fetching outbox messages
	FetchLimit int

	// Allows multiple messages with the same Kafka key to exist in the outbox queue.
	// However, it ensures that only one message per Kafka key is processed at any given time, enforcing at-most-once processing per kafka key.
	// Enable this option if the order of Kafka messages by key is important and only one record per key should be processed at any given time.
	KafkaSingleKeyProcessing bool

	// Enables deduplication of Kafka records based on (key, payload, and topic).
	// When enabled, this setting ensures that exactly once processing of Kafka record
	// with the same key, payload, and topic is allowed in the queue at any given time.
	// Enable this option if deduplication of Kafka records is important for your processing logic.
	// Pairing with EnableSingleKeyProcessing achieves stronger exactly-once semantics by
	// combining deduplication with ordered execution, reducing the risk of reprocessing the same record.
	KafkaRecordDeduplication bool

	/////////////////////
	// GENERAL SECTION //
	/////////////////////

	DSN string

	TLSConfig *tls.Config
}

type ConfigFunc func(c *Config)

func NewConfig(opts ...ConfigFunc) *Config {
	c := &Config{
		PollInterval:             time.Duration(15) * time.Second,
		StalledInterval:          time.Duration(2) * time.Minute,
		KafkaSingleKeyProcessing: false,
		KafkaRecordDeduplication: false,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func WithJobPollInterval(interval time.Duration) ConfigFunc {
	return func(c *Config) {
		c.PollInterval = interval
	}
}

func WithJobStallPollInterval(interval time.Duration) ConfigFunc {
	return func(c *Config) {
		c.StalledInterval = interval
	}
}

func WithMaxJobSize(size int) ConfigFunc {
	return func(c *Config) {
		c.FetchLimit = size
	}
}

func WithDSN(dsn string) ConfigFunc {
	return func(c *Config) {
		c.DSN = dsn
	}
}

func WithTLSConfig(tlsConfig *tls.Config) ConfigFunc {
	return func(c *Config) {
		c.TLSConfig = tlsConfig
	}
}

func WithKafkaSingleKeyProcessing(exactlyOnceKeyProcessing bool) ConfigFunc {
	return func(c *Config) {
		c.KafkaSingleKeyProcessing = exactlyOnceKeyProcessing
	}
}

func WithKafkaRecordDeduplication(exactlyOnceRecordProcessing bool) ConfigFunc {
	return func(c *Config) {
		c.KafkaRecordDeduplication = exactlyOnceRecordProcessing
	}
}
