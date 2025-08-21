package outbox

import (
	"github.com/TimKotowski/pg-kafka-outbox/internal/repository"
)

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
	Acknowledge(message Job, ackAcknowledgement Acknowledgement) error
}

type ackAcknowledgement struct {
	repository repository.Repository
}

func newAcknowledgement(repository repository.Repository) Acknowledger {
	return &ackAcknowledgement{
		repository: repository,
	}
}

func (a *ackAcknowledgement) Acknowledge(message Job, acackAcknowledgement Acknowledgement) error {
	return nil
}
