package queue

import (
	"log/slog"
	"testing"
	"time"

	"github.com/yashgupta/message-queue/internal/core"
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func newMsg(id, queue string) core.Message {
	return core.Message{
		ID:          core.MessageID(id),
		Queue:       queue,
		Payload:     []byte(id),
		EnqueueTime: time.Now(),
	}
}

func TestManager_EnqueueDequeue(t *testing.T) {
	m, err := NewManager(t.TempDir(), discard())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if err := m.Enqueue(newMsg("1", "orders")); err != nil {
		t.Fatal(err)
	}

	got, ok := m.Dequeue("orders")
	if !ok {
		t.Fatal("unexpected close")
	}
	if string(got.ID) != "1" {
		t.Fatalf("want id=1, got %s", got.ID)
	}
}

func TestManager_RestoreOnRestart(t *testing.T) {
	dir := t.TempDir()

	// first run: enqueue two messages, ack one
	func() {
		m, err := NewManager(dir, discard())
		if err != nil {
			t.Fatal(err)
		}
		m.Enqueue(newMsg("a", "orders"))
		m.Enqueue(newMsg("b", "orders"))
		m.WriteTombstone("b", "orders")
		m.Close()
	}()

	// second run: only "a" should survive
	m2, err := NewManager(dir, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	got, ok := m2.Dequeue("orders")
	if !ok || string(got.ID) != "a" {
		t.Fatalf("want id=a, got %v (ok=%v)", got.ID, ok)
	}
	if m2.topics["orders"].Len() != 0 {
		t.Fatal("expected only one message to survive replay")
	}
}

func TestManager_WALFailureDoesNotEnqueue(t *testing.T) {
	// Use a non-existent directory that can't be created (file in the way)
	// to trigger a WAL writer init failure.
	// Easiest: init manager with a good dir, then corrupt the writer by
	// replacing the WAL dir with a file.
	dir := t.TempDir()
	m, err := NewManager(dir, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	// prime the queue so initQueue has been called
	m.Enqueue(newMsg("1", "q"))

	// now point walDir at a file so any future writer open for a new queue fails
	m.walDir = dir + "/not-a-dir.wal" // initQueue for a new queue will fail MkdirAll

	err = m.Enqueue(newMsg("x", "newqueue"))
	if err == nil {
		t.Fatal("expected error when WAL dir is invalid")
	}
	// "newqueue" topic must not exist
	m.mu.Lock()
	_, exists := m.topics["newqueue"]
	m.mu.Unlock()
	if exists {
		t.Fatal("topic must not be created when WAL init fails")
	}
}

func TestManager_EnqueueTimestampPreserved(t *testing.T) {
	dir := t.TempDir()
	ts := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	func() {
		m, _ := NewManager(dir, discard())
		msg := newMsg("1", "q")
		msg.EnqueueTime = ts
		m.Enqueue(msg)
		m.Close()
	}()

	m2, _ := NewManager(dir, discard())
	defer m2.Close()
	got, _ := m2.Dequeue("q")

	if !got.EnqueueTime.Equal(ts) {
		t.Fatalf("want %v, got %v", ts, got.EnqueueTime)
	}
}

// TestGracefulShutdown_InFlightMessagesRequeued verifies that a message dequeued
// (in-flight) but never ACKed before shutdown is redelivered on the next start,
// because the WAL record is never tombstoned.
func TestGracefulShutdown_InFlightMessagesRequeued(t *testing.T) {
	walDir := t.TempDir()

	func() {
		m, err := NewManager(walDir, discard())
		if err != nil {
			t.Fatal(err)
		}
		m.Enqueue(newMsg("msg-1", "orders"))

		// Simulate dispatch: dequeue the message (marks it in-flight) without ACKing.
		dequeued := make(chan struct{}, 1)
		go func() {
			m.Dequeue("orders")
			dequeued <- struct{}{}
		}()
		<-dequeued

		m.BeginShutdown()
		m.Close()
	}()

	// On the next start, WAL replay restores the un-tombstoned message.
	m2, err := NewManager(walDir, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	got := make(chan core.Message, 1)
	go func() {
		msg, ok := m2.Dequeue("orders")
		if ok {
			got <- msg
		}
	}()

	select {
	case msg := <-got:
		if string(msg.ID) != "msg-1" {
			t.Fatalf("want msg-1 redelivered, got %s", msg.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("in-flight message at shutdown must be redelivered on next start")
	}
}

// TestBeginShutdown_RejectsNewEnqueues verifies that Enqueue returns an error
// after BeginShutdown is called.
func TestBeginShutdown_RejectsNewEnqueues(t *testing.T) {
	m, err := NewManager(t.TempDir(), discard())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	m.BeginShutdown()

	if err := m.Enqueue(newMsg("1", "orders")); err == nil {
		t.Fatal("Enqueue after BeginShutdown must return an error")
	}
}

// TestCompact_PreservesMessages verifies that compaction produces a WAL that
// replays identically (only un-tombstoned messages survive).
func TestCompact_PreservesMessages(t *testing.T) {
	walDir := t.TempDir()
	m, err := NewManager(walDir, discard())
	if err != nil {
		t.Fatal(err)
	}

	m.Enqueue(newMsg("1", "orders"))
	m.Enqueue(newMsg("2", "orders"))
	m.Enqueue(newMsg("3", "orders"))
	m.WriteTombstone("2", "orders")

	if err := m.Compact("orders"); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Enqueue after compaction must still work.
	if err := m.Enqueue(newMsg("4", "orders")); err != nil {
		t.Fatalf("Enqueue after compact: %v", err)
	}

	m.Close()

	// Reopen and verify: messages 1, 3, 4 must survive; 2 was tombstoned.
	m2, err := NewManager(walDir, discard())
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()

	var ids []string
	for i := 0; i < 3; i++ {
		got := make(chan core.Message, 1)
		go func() {
			msg, ok := m2.Dequeue("orders")
			if ok {
				got <- msg
			}
		}()
		select {
		case msg := <-got:
			ids = append(ids, string(msg.ID))
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for message %d", i+1)
		}
	}

	want := []string{"1", "3", "4"}
	for i, id := range ids {
		if id != want[i] {
			t.Fatalf("position %d: want %s, got %s", i, want[i], id)
		}
	}
}

