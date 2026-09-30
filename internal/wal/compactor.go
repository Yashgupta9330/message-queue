package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/yashgupta/message-queue/internal/core"
)

// Compact rewrites the WAL file for this queue, dropping all tombstoned records.
// The operation is atomic: a temp file is written and verified before replacing
// the original, so a crash during compaction cannot corrupt the WAL.
func (w *Writer) Compact() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("compact: close WAL: %w", err)
	}
	w.file = nil

	// Always reopen the original file before returning, even on error.
	reopenOriginal := func() {
		f, err := os.OpenFile(w.path, os.O_APPEND|os.O_WRONLY, 0644)
		if err == nil {
			w.file = f
		}
	}

	msgs, err := NewReader(w.walDir, w.queue).Replay()
	if err != nil {
		reopenOriginal()
		return fmt.Errorf("compact: replay: %w", err)
	}

	tmpPath := w.path + ".tmp"
	if err := writeCleanWAL(tmpPath, msgs); err != nil {
		os.Remove(tmpPath)
		reopenOriginal()
		return fmt.Errorf("compact: write temp: %w", err)
	}

	// Verify: the temp file must replay to the same number of messages.
	verified, err := replayFilePath(tmpPath)
	if err != nil || len(verified) != len(msgs) {
		os.Remove(tmpPath)
		reopenOriginal()
		return fmt.Errorf("compact: verify failed (wrote %d, verified %d): %w", len(msgs), len(verified), err)
	}

	if err := os.Rename(tmpPath, w.path); err != nil {
		os.Remove(tmpPath)
		reopenOriginal()
		return fmt.Errorf("compact: rename: %w", err)
	}

	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("compact: reopen after rename: %w", err)
	}
	w.file = f
	return nil
}

// writeCleanWAL writes messages to a new WAL file at path, truncating if it exists.
func writeCleanWAL(path string, msgs []core.Message) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, msg := range msgs {
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
		data, err := encode(rec)
		if err != nil {
			return err
		}
		if n, err := f.Write(data); err != nil {
			return err
		} else if n != len(data) {
			return io.ErrShortWrite
		}
	}
	return f.Sync()
}

// replayFilePath replays a WAL at an absolute path and returns non-tombstoned messages.
// Used to verify the compacted temp file before replacing the original.
func replayFilePath(path string) ([]core.Message, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var msgs []core.Message
	tombstones := make(map[string]struct{})
	offset := 0
	for offset < len(data) {
		rec, n, ok := decode(data[offset:])
		if !ok {
			break
		}
		offset += n
		switch rec.Type {
		case RecordMessage:
			if rec.Msg == nil {
				continue
			}
			msg := core.Message{
				ID:          core.MessageID(rec.ID),
				Queue:       rec.Msg.Queue,
				Payload:     rec.Msg.Payload,
				RetryCount:  rec.Msg.RetryCount,
				EnqueueTime: time.Unix(0, rec.Msg.EnqueueTime),
				Status:      core.StatusPending,
			}
			for _, h := range rec.Msg.Headers {
				msg.Headers = append(msg.Headers, core.Header{Key: h.Key, Value: h.Value})
			}
			msgs = append(msgs, msg)
		case RecordTombstone:
			tombstones[rec.ID] = struct{}{}
		}
	}

	result := msgs[:0]
	for _, msg := range msgs {
		if _, dead := tombstones[string(msg.ID)]; !dead {
			result = append(result, msg)
		}
	}
	return result, nil
}
