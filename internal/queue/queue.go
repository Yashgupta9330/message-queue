package queue

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
	"sync/atomic"

	"github.com/yashgupta/message-queue/internal/core"
	"github.com/yashgupta/message-queue/internal/wal"
)

// Manager owns all topics and their WAL writers.
// One WAL file and one Topic per queue name.
type Manager struct {
	walDir  string
	mu      sync.Mutex
	topics  map[string]*Topic
	writers map[string]*wal.Writer
	closing atomic.Bool
	log     *slog.Logger
}

// NewManager creates a Manager and replays any existing WAL files in walDir.
// Messages surviving replay (not tombstoned) are pre-loaded into their topics.
func NewManager(walDir string, logger *slog.Logger) (*Manager, error) {
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}

	m := &Manager{
		walDir:  walDir,
		topics:  make(map[string]*Topic),
		writers: make(map[string]*wal.Writer),
		log:     logger.With("component", "queue"),
	}

	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		queue := strings.TrimSuffix(e.Name(), ".wal")
		if err := m.initQueue(queue); err != nil {
			return nil, fmt.Errorf("replay %s: %w", queue, err)
		}
	}

	return m, nil
}

// initQueue replays the WAL for a queue and sets up its topic and writer.
// Must be called with m.mu held or during single-threaded init.
func (m *Manager) initQueue(queue string) error {
	if _, ok := m.topics[queue]; ok {
		return nil
	}

	msgs, err := wal.NewReader(m.walDir, queue).Replay()
	if err != nil {
		return err
	}

	w, err := wal.NewWriter(m.walDir, queue)
	if err != nil {
		return err
	}

	t := NewTopic()
	for _, msg := range msgs {
		t.Enqueue(msg)
	}

	m.topics[queue] = t
	m.writers[queue] = w
	m.log.Info("queue initialised", "queue", queue, "replayed_messages", len(msgs))
	return nil
}

// Enqueue writes the message to the WAL then adds it to the in-memory topic.
// The in-memory enqueue only happens if the WAL write succeeds.
// Returns an error if the broker is shutting down.
func (m *Manager) Enqueue(msg core.Message) error {
	if m.closing.Load() {
		return errors.New("broker is shutting down")
	}

	m.mu.Lock()
	if err := m.initQueue(msg.Queue); err != nil {
		m.mu.Unlock()
		return err
	}
	w := m.writers[msg.Queue]
	t := m.topics[msg.Queue]
	m.mu.Unlock()

	// WAL write outside the lock — Sync is the slow path
	if err := w.WriteMessage(msg); err != nil {
		return err
	}

	t.Enqueue(msg)
	return nil
}

// Dequeue blocks until a message is available for the given queue.
// Returns false only when the topic is closed and empty (shutdown).
func (m *Manager) Dequeue(queue string) (core.Message, bool) {
	m.mu.Lock()
	if err := m.initQueue(queue); err != nil {
		m.mu.Unlock()
		return core.Message{}, false
	}
	t := m.topics[queue]
	m.mu.Unlock()

	return t.Dequeue() // blocks outside the lock
}

// WriteTombstone writes an ACK tombstone to the queue's WAL.
// Called by the Ack Manager (Phase 6) when a message is acknowledged.
func (m *Manager) WriteTombstone(id core.MessageID, queue string) error {
	m.mu.Lock()
	w := m.writers[queue]
	m.mu.Unlock()

	if w == nil {
		return fmt.Errorf("queue %q not found", queue)
	}
	return w.WriteTombstone(id)
}

func (m *Manager) ExpireDLQOlderThan(retention time.Duration) (int, error) {
	if retention <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Add(-retention)

	m.mu.Lock()
	dlqQueues := make([]string, 0)
	for q := range m.writers {
		if core.IsDLQQueue(q) {
			dlqQueues = append(dlqQueues, q)
		}
	}
	m.mu.Unlock()

	var total int
	var firstErr error
	for _, queue := range dlqQueues {
		n, err := m.expireDLQQueue(queue, cutoff)
		total += n
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return total, firstErr
}

func (m *Manager) expireDLQQueue(queue string, cutoff time.Time) (int, error) {
	m.mu.Lock()
	if err := m.initQueue(queue); err != nil {
		m.mu.Unlock()
		return 0, err
	}
	t := m.topics[queue]
	w := m.writers[queue]
	m.mu.Unlock()

	removed := t.RemoveIf(func(msg core.Message) bool {
		return msg.EnqueueTime.Before(cutoff)
	})
	if len(removed) == 0 {
		return 0, nil
	}

	var firstErr error
	for _, msg := range removed {
		m.log.Info("dlq message expired", "queue", queue, "msg_id", msg.ID, "enqueue_time", msg.EnqueueTime)
		if err := w.WriteTombstone(msg.ID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return len(removed), firstErr
}

// Compact rewrites the WAL for a single queue, dropping all tombstoned records.
// Safe to call concurrently with ongoing reads and writes.
func (m *Manager) Compact(queue string) error {
	m.mu.Lock()
	w := m.writers[queue]
	m.mu.Unlock()

	if w == nil {
		return nil
	}
	return w.Compact()
}

// CompactAll compacts the WAL for every known queue.
func (m *Manager) CompactAll() error {
	m.mu.Lock()
	queues := make([]string, 0, len(m.writers))
	for q := range m.writers {
		queues = append(queues, q)
	}
	m.mu.Unlock()

	var firstErr error
	for _, q := range queues {
		m.log.Info("compacting WAL", "queue", q)
		if err := m.Compact(q); err != nil && firstErr == nil {
			m.log.Error("compaction failed", "queue", q, "err", err)
			firstErr = err
		} else {
			m.log.Info("compaction complete", "queue", q)
		}
	}
	return firstErr
}

// BeginShutdown marks the manager as closing so new Enqueue calls return an error.
// Call this before GracefulStop to stop accepting new messages. Close must still
// be called separately to flush and close the WAL writers.
func (m *Manager) BeginShutdown() {
	m.closing.Store(true)
}

// Close shuts down all topics (unblocking any waiting dispatchers) and
// flushes and closes all WAL writers.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, t := range m.topics {
		t.Close()
	}

	var firstErr error
	for _, w := range m.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
