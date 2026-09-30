package wal

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/yashgupta/message-queue/internal/core"
)

// Writer handles append-only WAL writes for a single queue
type Writer struct {
	mu     sync.Mutex
	file   *os.File
	path   string
	walDir string
	queue  string
}

// NewWriter creates/open WAL file for a queue
func NewWriter(dir, queue string) (*Writer, error) {
	path := filepath.Join(dir, queue+".wal")

	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &Writer{
		file:   f,
		path:   path,
		walDir: dir,
		queue:  queue,
	}, nil
}

// WriteMessage appends a message record to the WAL.
func (w *Writer) WriteMessage(msg core.Message) error {
	headers := make([]recordHeader, len(msg.Headers))
	for i, h := range msg.Headers {
		headers[i] = recordHeader{Key: h.Key, Value: h.Value}
	}
	rec := record{
		Type: RecordMessage,
		ID:   string(msg.ID),
		Msg: &recordMsg{
			Queue:       msg.Queue,
			Payload:     msg.Payload,
			Headers:     headers,
			RetryCount:  msg.RetryCount,
			EnqueueTime: msg.EnqueueTime.UnixNano(),
		},
	}
	return w.append(rec)
}

// WriteTombstone appends an ACK tombstone for the given message ID.
func (w *Writer) WriteTombstone(id core.MessageID) error {
	return w.append(record{Type: RecordTombstone, ID: string(id)})
}

// append writes a single record to WAL (durable).
func (w *Writer) append(rec record) error {
	data, err := encode(rec)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	return w.file.Sync()
}

// Close closes WAL file
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}