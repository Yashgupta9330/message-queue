package connmgr

import (
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/yashgupta/message-queue/internal/core"
)

// Consumer represents a connected subscriber on a single queue.
type Consumer struct {
	ID            string
	Queue         string
	Send          chan core.Message // dispatcher writes here; Subscribe goroutine reads from this channel
	PrefetchLimit int32
	ConnectedAt   time.Time
	inFlight      atomic.Int32
}

// NewConsumer creates a Consumer with a generated ID and a Send channel
// buffered to the prefetch limit.
func NewConsumer(queue string, prefetchLimit int32) (*Consumer, error) {
	id, err := newConsumerID()
	if err != nil {
		return nil, err
	}

	bufferSize := prefetchLimit
	if bufferSize < 1 {
		bufferSize = 1
	}

	return &Consumer{
		ID:            id,
		Queue:         queue,
		Send:          make(chan core.Message, bufferSize),
		PrefetchLimit: prefetchLimit,
		ConnectedAt:   time.Now(),
	}, nil
}

func (c *Consumer) IsEligible() bool {
	return c.inFlight.Load() < c.PrefetchLimit
}

// returns the current number of unacknowledged messages.
func (c *Consumer) InFlight() int32 {
	return c.inFlight.Load()
}

// called by the dispatcher before sending a message.
func (c *Consumer) IncrementInFlight() {
	c.inFlight.Add(1)
}

// called by the Ack Manager when a message is acknowledged.
func (c *Consumer) DecrementInFlight() {
	c.inFlight.Add(-1)
}

func newConsumerID() (string, error) {
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(randomBytes), nil
}
