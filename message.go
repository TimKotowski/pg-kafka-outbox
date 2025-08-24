package outbox

import (
	"errors"
)

type Partition struct {
	// Partition number for kafka topic.
	PartitionID int
}

type Message struct {
	Topic      string
	Key        []byte
	Payload    []byte
	Headers    []byte
	Partition  *Partition
	MaxRetries int
}

type Header struct {
	Key   string
	Value []byte
}

func (m Message) isValidMessage() error {
	if len(m.Key) == 0 {
		return errors.New("kafka key cant be empty")
	}

	if len(m.Topic) == 0 {
		return errors.New("kafka topic cant be empty")
	}

	return nil
}
