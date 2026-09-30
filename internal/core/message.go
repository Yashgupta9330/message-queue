package core

import "time"

type MessageID string

type DeliveryStatus uint8

const (
	StatusPending  DeliveryStatus = iota
	StatusInFlight DeliveryStatus = iota
	StatusAcked    DeliveryStatus = iota
	StatusNacked   DeliveryStatus = iota
	StatusDead     DeliveryStatus = iota
)

type AckOutcome uint8

const (
	OutcomeAck  AckOutcome = iota
	OutcomeNack AckOutcome = iota
)

type Header struct {
	Key   string
	Value string
}

type Message struct {
	ID          MessageID
	Queue       string
	Payload     []byte
	Headers     []Header
	RetryCount   int32
	EnqueueTime  time.Time
	DispatchedAt time.Time
	Status       DeliveryStatus
}

func (m Message) WithRetry() Message {
	m.RetryCount++
	m.Status = StatusPending
	return m
}
