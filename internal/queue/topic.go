package queue

import (
	"sync"

	"github.com/yashgupta/message-queue/internal/core"
)

// Topic is a thread-safe FIFO queue for a single named queue.
// Dequeue blocks until a message is available or the topic is closed.
type Topic struct {
	mu     sync.Mutex
	cond   *sync.Cond
	msgs   []core.Message
	closed bool
}

func NewTopic() *Topic {
	t := &Topic{}
	t.cond = sync.NewCond(&t.mu)
	return t
}

// Enqueue adds a message to the back of the queue.
func (t *Topic) Enqueue(msg core.Message) {
	t.mu.Lock()
	t.msgs = append(t.msgs, msg)
	t.mu.Unlock()
	t.cond.Signal()
}

// Dequeue removes and returns the front message.
// Blocks until a message is available.
// Returns the zero Message and false if the topic is closed and empty.
func (t *Topic) Dequeue() (core.Message, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for len(t.msgs) == 0 && !t.closed {
		t.cond.Wait()
	}

	if len(t.msgs) == 0 {
		return core.Message{}, false
	}

	msg := t.msgs[0]
	t.msgs = t.msgs[1:]
	return msg, true
}

// RemoveIf removes messages matching pred from the topic and returns them.
func (t *Topic) RemoveIf(pred func(core.Message) bool) []core.Message {
	t.mu.Lock()
	defer t.mu.Unlock()

	var removed []core.Message
	kept := t.msgs[:0]
	for _, msg := range t.msgs {
		if pred(msg) {
			removed = append(removed, msg)
		} else {
			kept = append(kept, msg)
		}
	}
	t.msgs = kept
	return removed
}

// Len returns the current number of queued messages.
func (t *Topic) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.msgs)
}

// Close unblocks any goroutine waiting in Dequeue.
// After Close, Dequeue drains remaining messages then returns false.
func (t *Topic) Close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.cond.Broadcast()
}
