package queue

import (
	"log/slog"
	"testing"
	"time"

	"github.com/yashgupta/message-queue/internal/core"
	"github.com/yashgupta/message-queue/internal/wal"
)

func TestManager_ExpireDLQOlderThan(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(dir, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	old := core.Message{
		ID:          "old",
		Queue:       "orders.dlq",
		Payload:     []byte("old"),
		EnqueueTime: time.Now().Add(-31 * 24 * time.Hour),
	}
	fresh := core.Message{
		ID:          "fresh",
		Queue:       "orders.dlq",
		Payload:     []byte("fresh"),
		EnqueueTime: time.Now(),
	}
	if err := m.Enqueue(old); err != nil {
		t.Fatal(err)
	}
	if err := m.Enqueue(fresh); err != nil {
		t.Fatal(err)
	}

	n, err := m.ExpireDLQOlderThan(30 * 24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("want 1 expired, got %d", n)
	}

	got, ok := m.Dequeue("orders.dlq")
	if !ok {
		t.Fatal("expected fresh message")
	}
	if string(got.ID) != "fresh" {
		t.Fatalf("want fresh, got %s", got.ID)
	}

	msgs, err := wal.NewReader(dir, "orders.dlq").Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || string(msgs[0].ID) != "fresh" {
		t.Fatalf("replay want [fresh], got %v", msgs)
	}
}

func TestManager_ExpireDLQOlderThan_SkipsMainQueue(t *testing.T) {
	m, err := NewManager(t.TempDir(), discard())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	msg := core.Message{
		ID:          "1",
		Queue:       "orders",
		Payload:     []byte("x"),
		EnqueueTime: time.Now().Add(-31 * 24 * time.Hour),
	}
	if err := m.Enqueue(msg); err != nil {
		t.Fatal(err)
	}

	n, err := m.ExpireDLQOlderThan(30 * 24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("want 0 expired on main queue, got %d", n)
	}

	got, ok := m.Dequeue("orders")
	if !ok || string(got.ID) != "1" {
		t.Fatalf("main queue message should remain, got %v ok=%v", got, ok)
	}
}

func TestManager_ExpireDLQOlderThan_Disabled(t *testing.T) {
	m, err := NewManager(t.TempDir(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	msg := core.Message{
		ID:          "1",
		Queue:       "orders.dlq",
		Payload:     []byte("x"),
		EnqueueTime: time.Now().Add(-31 * 24 * time.Hour),
	}
	if err := m.Enqueue(msg); err != nil {
		t.Fatal(err)
	}

	n, err := m.ExpireDLQOlderThan(0)
	if err != nil || n != 0 {
		t.Fatalf("disabled ttl: n=%d err=%v", n, err)
	}
}
