package outbox

import "github.com/TimKotowski/pg-kafka-outbox/internal/outboxdb"

type Acknowledgement struct {
	Status AckStatus
}

type AckStatus = string

var (
	success AckStatus = "success"
	retry   AckStatus = "retry"
	failed  AckStatus = "failed"
)

var (
	Success = Acknowledgement{success}
	Retry   = Acknowledgement{retry}
	Failure = Acknowledgement{failed}
)

func (a Acknowledgement) String() string {
	return a.Status
}

type Acknowledger interface {
	Acknowledge(message Message, ackAcknowledgement Acknowledgement)
}

type ackAcknowledgement struct {
	repository outboxdb.OutboxDB
}

func newAcknowledgement(repository outboxdb.OutboxDB) Acknowledger {
	return &ackAcknowledgement{
		repository: repository,
	}
}

func (a *ackAcknowledgement) Acknowledge(message Message, ackAcknowledgement Acknowledgement) {

}
