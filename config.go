package outbox

import (
	"crypto/tls"
	"time"
)

type Config struct {
	//////////////////////////
	// OUTBOX QUEUE SECTION //
	/////////////////////////

	// Interval rate for polling outbox messages.
	QueueDelay time.Duration

	// Interval period for requeueing hanging/stalled outbox messages.
	RequeueDelay time.Duration

	// Interval period for removing completed outbox messages.
	CompletedRetentionPeriod time.Duration

	// Interval period for removing failed outbox messages to dead letter queue.
	FailedRetentionPeriod time.Duration

	// Limit for fetching outbox messages
	FetchLimit int

	// FifoKafkaKeyProcessing is designed to enhance message processing for kafka records,
	// particularly when dealing with message ordering of kafka keys.
	//
	// Ensures FIFO processing of messages per kafka key.
	// Only one batch of messages per kafka key is processed at any given time,
	// and messages are processed in the order they were received.
	// Messages also must finish before more messages with same kafka key can be retrieved.
	// Or if messages all being processed for the kafka key reach TTL.
	//
	// Enable to prevent the outbox process from processing messages with the same kafka key concurrently.
	//
	// At most 10 keys per 10 messages can be processed. Making FetchLimit void in this case.
	FifoKafkaKeyProcessing bool

	// Enables deduplication of Kafka records based on (key, payload, and topic) using SHA-256 hashing.
	// When enabled, this setting ensures exactly once processing of messages where prcoessing duplicates
	// isnt accepatble.
	//
	// Enable this option if deduplication of Kafka records is important for processing logic.
	//
	// Pairing with FifoKafkaKeyProcessing achieves FIFO processing with exactly-once semantics.
	// By combining deduplication with ordered execution per kafka key.
	MessageDeduplication bool

	// The number of outbox messages to buffer.
	// Defaults to 256.
	ChannelBufferSize int

	/////////////////////
	// GENERAL SECTION //
	/////////////////////

	DSN string

	TLSConfig *tls.Config
}

type ConfigFunc func(c *Config)

func NewConfig(opts ...ConfigFunc) *Config {
	c := &Config{
		QueueDelay:             time.Duration(5) * time.Second,
		RequeueDelay:           time.Duration(2) * time.Minute,
		FifoKafkaKeyProcessing: false,
		MessageDeduplication:   false,
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func WithJobPollInterval(interval time.Duration) ConfigFunc {
	return func(c *Config) {
		c.QueueDelay = interval
	}
}

func WithJobStallPollInterval(interval time.Duration) ConfigFunc {
	return func(c *Config) {
		c.RequeueDelay = interval
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
		c.FifoKafkaKeyProcessing = exactlyOnceKeyProcessing
	}
}

func WithKafkaRecordDeduplication(exactlyOnceRecordProcessing bool) ConfigFunc {
	return func(c *Config) {
		c.MessageDeduplication = exactlyOnceRecordProcessing
	}
}
