package dispatcher_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/yashgupta/message-queue/internal/ackmgr"
	"github.com/yashgupta/message-queue/internal/connmgr"
	"github.com/yashgupta/message-queue/internal/core"
	"github.com/yashgupta/message-queue/internal/dispatcher"
	"github.com/yashgupta/message-queue/internal/queue"
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func newMsg(id string) core.Message {
	return core.Message{ID: core.MessageID(id), Queue: "orders", Payload: []byte(id), EnqueueTime: time.Now()}
}

func newTestDispatcher(t *testing.T) (*dispatcher.Dispatcher, *queue.Manager, *connmgr.Registry) {
	t.Helper()
	manager, err := queue.NewManager(t.TempDir(), discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.Close() })
	registry := connmgr.NewRegistry()
	ackManager := ackmgr.NewAckManager(manager, registry, 3, discard())
	return dispatcher.NewDispatcher(manager, registry, ackManager, discard()), manager, registry
}

func TestDispatcher_LoadDistributedByInFlight(t *testing.T) {
	d, manager, registry := newTestDispatcher(t)

	consumer1, _ := connmgr.NewConsumer("orders", 10)
	consumer2, _ := connmgr.NewConsumer("orders", 10)
	registry.Register(consumer1)
	registry.Register(consumer2)

	// consumer1 already has 1 in-flight; consumer2 has 0
	registry.IncrementInFlight(consumer1.ID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx, "orders")

	manager.Enqueue(newMsg("1"))

	// Message must go to consumer2 (lower in-flight)
	select {
	case msg := <-consumer2.Send:
		if msg.ID != "1" {
			t.Fatalf("want id=1, got %s", msg.ID)
		}
	case <-consumer1.Send:
		t.Fatal("expected consumer2 (less in-flight) to receive the message")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message delivery")
	}
}

func TestDispatcher_PrefetchLimitEnforced(t *testing.T) {
	d, manager, registry := newTestDispatcher(t)

	consumer, _ := connmgr.NewConsumer("orders", 1) // prefetch limit = 1
	registry.Register(consumer)
	registry.IncrementInFlight(consumer.ID) // already at the limit

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx, "orders")

	manager.Enqueue(newMsg("1"))

	// Dispatcher holds the message but must not deliver to a consumer at its limit.
	select {
	case <-consumer.Send:
		t.Fatal("consumer at prefetch limit must not receive messages")
	case <-time.After(50 * time.Millisecond):
		// expected: dispatcher is waiting for the consumer to become eligible
	}
}

func TestDispatcher_WaitsWithoutBlockingQueueWhenNoConsumer(t *testing.T) {
	d, manager, registry := newTestDispatcher(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx, "orders")

	// Enqueue a message with no registered consumers.
	manager.Enqueue(newMsg("1"))

	// Give the dispatcher time to dequeue the message and enter the retry loop.
	time.Sleep(20 * time.Millisecond)

	// Now register a consumer — dispatcher must deliver the held message.
	consumer, _ := connmgr.NewConsumer("orders", 5)
	registry.Register(consumer)

	select {
	case msg := <-consumer.Send:
		if msg.ID != "1" {
			t.Fatalf("want id=1, got %s", msg.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not deliver message after consumer became available")
	}
}

func TestDispatcher_DispatchedAtIsSet(t *testing.T) {
	d, manager, registry := newTestDispatcher(t)

	consumer, _ := connmgr.NewConsumer("orders", 5)
	registry.Register(consumer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx, "orders")

	before := time.Now()
	manager.Enqueue(newMsg("1"))

	select {
	case msg := <-consumer.Send:
		if msg.DispatchedAt.IsZero() {
			t.Fatal("DispatchedAt must be set on dispatch")
		}
		if msg.DispatchedAt.Before(before) {
			t.Fatalf("DispatchedAt %v is before dispatch started %v", msg.DispatchedAt, before)
		}
		if msg.Status != core.StatusInFlight {
			t.Fatalf("want status InFlight, got %v", msg.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
}
