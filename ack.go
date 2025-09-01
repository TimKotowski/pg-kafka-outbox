package outbox

import "github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"

type Acknowledgement struct {
	Status string
}

var (
	Unknown = Acknowledgement{""}
	Success = Acknowledgement{"success"}
	Failure = Acknowledgement{"failure"}
)

func (a Acknowledgement) String() string {
	return a.Status
}

type Acknowledger interface {
	Acknowledge(message Message, ackAcknowledgement Acknowledgement) error
}

type ackAcknowledgement struct {
	repository outboxdb.OutboxDB
}

func newAcknowledgement(repository outboxdb.OutboxDB) Acknowledger {
	return &ackAcknowledgement{
		repository: repository,
	}
}

func (a *ackAcknowledgement) Acknowledge(message Message, acackAcknowledgement Acknowledgement) error {
	return nil
}
