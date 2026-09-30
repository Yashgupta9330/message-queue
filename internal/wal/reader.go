package wal

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/yashgupta/message-queue/internal/core"
)

// Reader replays a WAL file for a single queue.
type Reader struct {
	path string
}

// NewReader returns a Reader for the given queue's WAL file.
func NewReader(dir, queue string) *Reader {
	return &Reader{path: filepath.Join(dir, queue+".wal")}
}

// Replay reads the WAL from the beginning and returns all messages that have
// not been tombstoned. It stops at the first CRC mismatch (torn write) and
// treats everything before that point as valid.
//
// Returns nil, nil if the WAL file does not exist yet.
func (r *Reader) Replay() ([]core.Message, error) {
	data, err := os.ReadFile(r.path)
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
			break // torn write boundary — everything after is invalid
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
