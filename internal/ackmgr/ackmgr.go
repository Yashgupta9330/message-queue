package ackmgr

import (
	"log/slog"
	"sync"
	"time"

	"github.com/yashgupta/message-queue/internal/connmgr"
	"github.com/yashgupta/message-queue/internal/core"
	"github.com/yashgupta/message-queue/internal/queue"
)

type inFlightEntry struct {
	message    core.Message
	consumerID string
}

// AckManager tracks in-flight messages and handles ACK/NACK outcomes.
type AckManager struct {
	mu         sync.Mutex
	inFlight   map[core.MessageID]inFlightEntry
	manager    *queue.Manager
	registry   *connmgr.Registry
	maxRetries int32
	log        *slog.Logger
}

func NewAckManager(manager *queue.Manager, registry *connmgr.Registry, maxRetries int32, logger *slog.Logger) *AckManager {
	return &AckManager{
		inFlight:   make(map[core.MessageID]inFlightEntry),
		manager:    manager,
		registry:   registry,
		maxRetries: maxRetries,
		log:        logger.With("component", "ackmgr"),
	}
}

// Register records a dispatched message as in-flight.
// Called by the Dispatcher immediately before pushing the message to consumer.Send.
func (a *AckManager) Register(msg core.Message, consumerID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inFlight[msg.ID] = inFlightEntry{message: msg, consumerID: consumerID}
}

// Unregister removes a message from the in-flight map and decrements the
// consumer's in-flight counter. Called by the Dispatcher when ctx is cancelled
// after Register but before the consumer.Send channel write completes.
func (a *AckManager) Unregister(id core.MessageID) {
	a.mu.Lock()
	entry, ok := a.inFlight[id]
	if !ok {
		a.mu.Unlock()
		return
	}
	delete(a.inFlight, id)
	a.mu.Unlock()
	a.registry.DecrementInFlight(entry.consumerID)
}

// Ack removes the message from the in-flight map, decrements the consumer's in-flight
// counter, and writes an ACK tombstone to the WAL so the message is not redelivered on restart.
func (a *AckManager) Ack(id core.MessageID) error {
	a.mu.Lock()
	entry, ok := a.inFlight[id]
	if !ok {
		a.mu.Unlock()
		return nil
	}
	delete(a.inFlight, id)
	a.mu.Unlock()

	a.log.Info("ack processed", "msg_id", id, "queue", entry.message.Queue, "consumer_id", entry.consumerID)
	a.registry.DecrementInFlight(entry.consumerID)
	return a.manager.WriteTombstone(id, entry.message.Queue)
}

// Nack removes the message from the in-flight map, decrements the consumer's in-flight
// counter, and either re-enqueues the message (retries remaining) or moves it to the DLQ
// (retries exhausted).
func (a *AckManager) Nack(id core.MessageID) error {
	a.mu.Lock()
	entry, ok := a.inFlight[id]
	if !ok {
		a.mu.Unlock()
		return nil
	}
	delete(a.inFlight, id)
	a.mu.Unlock()

	a.log.Warn("nack received", "msg_id", id, "queue", entry.message.Queue, "consumer_id", entry.consumerID, "retry_count", entry.message.RetryCount)
	return a.nackEntry(entry)
}

// ScanAndTimeout iterates the in-flight map, removes entries whose DispatchedAt is older
// than dispatchTimeout, and treats each as an implicit NACK (requeue or DLQ).
// Called periodically by the retry.Scanner.
func (a *AckManager) ScanAndTimeout(dispatchTimeout time.Duration) error {
	threshold := time.Now().Add(-dispatchTimeout)

	a.mu.Lock()
	var timedOut []inFlightEntry
	for id, entry := range a.inFlight {
		if entry.message.DispatchedAt.Before(threshold) {
			timedOut = append(timedOut, entry)
			delete(a.inFlight, id)
		}
	}
	a.mu.Unlock()

	if len(timedOut) > 0 {
		a.log.Warn("messages timed out", "count", len(timedOut))
	}

	var firstErr error
	for _, entry := range timedOut {
		a.log.Warn("message timed out", "msg_id", entry.message.ID, "queue", entry.message.Queue, "consumer_id", entry.consumerID, "retry_count", entry.message.RetryCount, "dispatched_at", entry.message.DispatchedAt)
		if err := a.nackEntry(entry); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ExpireDLQInFlight tombstones in-flight DLQ messages older than cutoff without requeue.
// Only messages on queues ending with .dlq are affected.
func (a *AckManager) ExpireDLQInFlight(cutoff time.Time) (int, error) {
	a.mu.Lock()
	var expired []inFlightEntry
	for id, entry := range a.inFlight {
		if core.IsDLQQueue(entry.message.Queue) && entry.message.EnqueueTime.Before(cutoff) {
			expired = append(expired, entry)
			delete(a.inFlight, id)
		}
	}
	a.mu.Unlock()

	var firstErr error
	for _, entry := range expired {
		a.log.Info("dlq in-flight message expired", "msg_id", entry.message.ID, "queue", entry.message.Queue, "enqueue_time", entry.message.EnqueueTime)
		a.registry.DecrementInFlight(entry.consumerID)
		if err := a.manager.WriteTombstone(entry.message.ID, entry.message.Queue); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return len(expired), firstErr
}

// nackEntry decrements the consumer's in-flight counter, then either re-enqueues
// the message with an incremented retry count or sends it to the DLQ if retries
// are exhausted.
func (a *AckManager) nackEntry(entry inFlightEntry) error {
	a.registry.DecrementInFlight(entry.consumerID)

	if entry.message.RetryCount >= a.maxRetries {
		dlqMsg := entry.message
		dlqMsg.Queue = entry.message.Queue + core.DLQSuffix
		dlqMsg.EnqueueTime = time.Now()
		dlqMsg.Status = core.StatusDead
		a.log.Error("message dead-lettered", "msg_id", entry.message.ID, "src_queue", entry.message.Queue, "dlq", dlqMsg.Queue, "retry_count", entry.message.RetryCount)
		return a.manager.Enqueue(dlqMsg)
	}
	a.log.Info("message requeued for retry", "msg_id", entry.message.ID, "queue", entry.message.Queue, "retry_count", entry.message.RetryCount+1, "max_retries", a.maxRetries)
	return a.manager.Enqueue(entry.message.WithRetry())
}

// WaitDrained blocks until the in-flight map is empty or the timeout elapses.
// Returns true if the map drained within the timeout.
func (a *AckManager) WaitDrained(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		n := len(a.inFlight)
		a.mu.Unlock()
		if n == 0 {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	a.mu.Lock()
	n := len(a.inFlight)
	a.mu.Unlock()
	return n == 0
}
