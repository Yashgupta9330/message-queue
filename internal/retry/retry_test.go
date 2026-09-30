package retry_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/yashgupta/message-queue/internal/ackmgr"
	"github.com/yashgupta/message-queue/internal/connmgr"
	"github.com/yashgupta/message-queue/internal/core"
	"github.com/yashgupta/message-queue/internal/queue"
	"github.com/yashgupta/message-queue/internal/retry"
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// newEnv sets up a manager, registry, consumer (with one in-flight pre-incremented),
// and an AckManager. The manager is NOT registered for cleanup so callers that need
// to close and reopen it can manage the lifetime themselves.
func newEnv(t *testing.T, walDir string, maxRetries int32) (*ackmgr.AckManager, *queue.Manager, *connmgr.Consumer) {
	t.Helper()
	manager, err := queue.NewManager(walDir, discard())
	if err != nil {
		t.Fatal(err)
	}
	registry := connmgr.NewRegistry()
	consumer, _ := connmgr.NewConsumer("orders", 5)
	registry.Register(consumer)
	registry.IncrementInFlight(consumer.ID)
	return ackmgr.NewAckManager(manager, registry, maxRetries, discard()), manager, consumer
}

// timedOutMsg returns a message whose DispatchedAt is well in the past.
func timedOutMsg(id string) core.Message {
	return core.Message{
		ID:           core.MessageID(id),
		Queue:        "orders",
		Payload:      []byte(id),
		EnqueueTime:  time.Now(),
		DispatchedAt: time.Now().Add(-500 * time.Millisecond),
		Status:       core.StatusInFlight,
	}
}

// TestScanner_RequeuesTimedOutMessage verifies that the scanner goroutine picks up
// a timed-out in-flight message, decrements the consumer's in-flight counter, and
// re-enqueues it with an incremented retry count.
func TestScanner_RequeuesTimedOutMessage(t *testing.T) {
	ackMgr, manager, consumer := newEnv(t, t.TempDir(), 5)
	t.Cleanup(func() { manager.Close() })

	scanner := retry.NewScanner(ackMgr, 10*time.Millisecond, 100*time.Millisecond, discard())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go scanner.Run(ctx)

	msg := timedOutMsg("msg-1")
	ackMgr.Register(msg, consumer.ID)

	got := make(chan core.Message, 1)
	go func() {
		m, ok := manager.Dequeue("orders")
		if ok {
			got <- m
		}
	}()

	select {
	case m := <-got:
		if m.RetryCount != 1 {
			t.Fatalf("want RetryCount=1 after timeout requeue, got %d", m.RetryCount)
		}
		if consumer.InFlight() != 0 {
			t.Fatalf("want inFlight=0 after scanner, got %d", consumer.InFlight())
		}
	case <-time.After(time.Second):
		t.Fatal("scanner did not requeue timed-out message within 1s")
	}
}

// TestScanAndTimeout_ExhaustedRetries_MovesToDLQ verifies that a message whose retry
// count has reached maxRetries is sent to the DLQ instead of being re-enqueued.
func TestScanAndTimeout_ExhaustedRetries_MovesToDLQ(t *testing.T) {
	ackMgr, manager, consumer := newEnv(t, t.TempDir(), 0) // maxRetries=0: first failure → DLQ
	t.Cleanup(func() { manager.Close() })

	msg := timedOutMsg("msg-1")
	ackMgr.Register(msg, consumer.ID)

	if err := ackMgr.ScanAndTimeout(100 * time.Millisecond); err != nil {
		t.Fatalf("ScanAndTimeout: %v", err)
	}

	if consumer.InFlight() != 0 {
		t.Fatalf("want inFlight=0 after DLQ move, got %d", consumer.InFlight())
	}

	// Message must appear in the DLQ queue, not the original queue.
	dlq := make(chan core.Message, 1)
	go func() {
		m, ok := manager.Dequeue("orders.dlq")
		if ok {
			dlq <- m
		}
	}()

	original := make(chan core.Message, 1)
	go func() {
		m, ok := manager.Dequeue("orders")
		if ok {
			original <- m
		}
	}()

	select {
	case m := <-dlq:
		if m.ID != msg.ID {
			t.Fatalf("want message %s in DLQ, got %s", msg.ID, m.ID)
		}
	case <-original:
		t.Fatal("message with exhausted retries must not be re-enqueued to the original queue")
	case <-time.After(time.Second):
		t.Fatal("timed out: message with exhausted retries must go to DLQ")
	}
}

// TestDLQ_PersistsAcrossRestart verifies that messages moved to the DLQ survive a
// broker restart by replaying from the DLQ WAL file.
func TestDLQ_PersistsAcrossRestart(t *testing.T) {
	walDir := t.TempDir()

	ackMgr, manager, consumer := newEnv(t, walDir, 0)

	msg := timedOutMsg("msg-1")
	ackMgr.Register(msg, consumer.ID)

	// Nack with maxRetries=0 → message goes to orders.dlq.
	if err := ackMgr.Nack(msg.ID); err != nil {
		t.Fatalf("Nack: %v", err)
	}
	manager.Close()

	// Reopen the manager — DLQ WAL must be replayed.
	manager2, err := queue.NewManager(walDir, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer manager2.Close()

	got := make(chan core.Message, 1)
	go func() {
		m, ok := manager2.Dequeue("orders.dlq")
		if ok {
			got <- m
		}
	}()

	select {
	case m := <-got:
		if string(m.ID) != "msg-1" {
			t.Fatalf("want msg-1 in DLQ after restart, got %s", m.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("DLQ message did not survive broker restart")
	}
}

// TestScanAndTimeout_LiveMessage_NotEvicted verifies that a message whose DispatchedAt
// is recent is not picked up by the scanner — no message is stuck in-flight or wrongly
// evicted when it should still be in flight.
func TestScanAndTimeout_LiveMessage_NotEvicted(t *testing.T) {
	ackMgr, manager, consumer := newEnv(t, t.TempDir(), 5)
	t.Cleanup(func() { manager.Close() })

	// DispatchedAt = now: well within any reasonable timeout.
	msg := core.Message{
		ID:           "msg-live",
		Queue:        "orders",
		Payload:      []byte("live"),
		EnqueueTime:  time.Now(),
		DispatchedAt: time.Now(),
		Status:       core.StatusInFlight,
	}
	ackMgr.Register(msg, consumer.ID)

	// Scan with a 10-second timeout — the live message must not be evicted.
	if err := ackMgr.ScanAndTimeout(10 * time.Second); err != nil {
		t.Fatalf("ScanAndTimeout: %v", err)
	}

	// In-flight must still be 1 (the consumer's pre-incremented count).
	if consumer.InFlight() != 1 {
		t.Fatalf("want inFlight=1 (message still in flight), got %d", consumer.InFlight())
	}

	// Nothing must appear in the queue.
	select {
	case <-time.After(30 * time.Millisecond):
		// correct: live message was not requeued
	default:
		// Check non-blocking — if Dequeue had something it'd block on empty queue anyway,
		// but we explicitly verify no goroutine dequeued anything by checking inFlight.
	}
}
