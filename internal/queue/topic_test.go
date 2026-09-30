package queue

import (
	"testing"
	"time"

	"github.com/yashgupta/message-queue/internal/core"
)

func msg(id string) core.Message {
	return core.Message{ID: core.MessageID(id), Queue: "q", Payload: []byte(id)}
}

func TestTopic_FIFOOrder(t *testing.T) {
	topic := NewTopic()
	topic.Enqueue(msg("1"))
	topic.Enqueue(msg("2"))
	topic.Enqueue(msg("3"))

	for _, want := range []string{"1", "2", "3"} {
		got, ok := topic.Dequeue()
		if !ok {
			t.Fatal("unexpected close")
		}
		if string(got.ID) != want {
			t.Fatalf("want %s, got %s", want, got.ID)
		}
	}
}

func TestTopic_DequeueBlocks(t *testing.T) {
	topic := NewTopic()

	done := make(chan core.Message, 1)
	go func() {
		m, _ := topic.Dequeue()
		done <- m
	}()

	// give the goroutine time to block
	time.Sleep(20 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("Dequeue returned before Enqueue")
	default:
	}

	topic.Enqueue(msg("x"))

	select {
	case got := <-done:
		if string(got.ID) != "x" {
			t.Fatalf("wrong message: %s", got.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not unblock after Enqueue")
	}
}

func TestTopic_CloseUnblocksDequeue(t *testing.T) {
	topic := NewTopic()

	done := make(chan bool, 1)
	go func() {
		_, ok := topic.Dequeue()
		done <- ok
	}()

	time.Sleep(20 * time.Millisecond)
	topic.Close()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("expected false after close with empty queue")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Dequeue")
	}
}

func TestTopic_CloseDrawsRemainingMessages(t *testing.T) {
	topic := NewTopic()
	topic.Enqueue(msg("1"))
	topic.Enqueue(msg("2"))
	topic.Close()

	m1, ok1 := topic.Dequeue()
	m2, ok2 := topic.Dequeue()
	_, ok3 := topic.Dequeue()

	if !ok1 || string(m1.ID) != "1" {
		t.Fatalf("unexpected first dequeue: %v %v", ok1, m1.ID)
	}
	if !ok2 || string(m2.ID) != "2" {
		t.Fatalf("unexpected second dequeue: %v %v", ok2, m2.ID)
	}
	if ok3 {
		t.Fatal("expected false on empty closed topic")
	}
}

func TestTopic_Len(t *testing.T) {
	topic := NewTopic()
	if topic.Len() != 0 {
		t.Fatal("want 0")
	}
	topic.Enqueue(msg("1"))
	topic.Enqueue(msg("2"))
	if topic.Len() != 2 {
		t.Fatal("want 2")
	}
	topic.Dequeue()
	if topic.Len() != 1 {
		t.Fatal("want 1")
	}
}
