package wal

import (
	"os"
	"testing"
	"time"

	"github.com/yashgupta/message-queue/internal/core"
)

func TestEncodeDecode_RoundTrip(t *testing.T) {
	orig := record{
		Type: RecordMessage,
		ID:   "msg-1",
		Msg: &recordMsg{
			Queue:       "orders",
			Payload:     []byte("hello"),
			Headers:     []recordHeader{{Key: "k", Value: "v"}},
			RetryCount:  2,
			EnqueueTime: time.Now().UnixNano(),
		},
	}

	buf, err := encode(orig)
	if err != nil {
		t.Fatal(err)
	}

	got, n, ok := decode(buf)
	if !ok {
		t.Fatal("decode returned false")
	}
	if n != len(buf) {
		t.Fatalf("consumed %d bytes, want %d", n, len(buf))
	}
	if got.ID != orig.ID || got.Type != orig.Type {
		t.Fatalf("record mismatch: got %+v", got)
	}
	if got.Msg.Queue != orig.Msg.Queue || string(got.Msg.Payload) != string(orig.Msg.Payload) {
		t.Fatalf("recordMsg mismatch: got %+v", got.Msg)
	}
}

func TestDecode_ShortBuffer(t *testing.T) {
	_, _, ok := decode([]byte{0x01, 0x02})
	if ok {
		t.Fatal("expected decode to fail on short buffer")
	}
}

func TestDecode_BadMagic(t *testing.T) {
	buf, _ := encode(record{Type: RecordMessage, ID: "x", Msg: &recordMsg{Queue: "q", Payload: []byte("p")}})
	buf[0] = 0xFF // corrupt magic
	_, _, ok := decode(buf)
	if ok {
		t.Fatal("expected decode to fail on bad magic")
	}
}

func TestDecode_TornWrite(t *testing.T) {
	buf, _ := encode(record{Type: RecordMessage, ID: "x", Msg: &recordMsg{Queue: "q", Payload: []byte("p")}})
	buf[len(buf)-1] ^= 0xFF // flip last CRC byte
	_, _, ok := decode(buf)
	if ok {
		t.Fatal("expected decode to fail on CRC mismatch")
	}
}

func TestReplay_RestoresMessages(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, "q")
	if err != nil {
		t.Fatal(err)
	}

	msgs := []core.Message{
		{ID: "1", Queue: "q", Payload: []byte("a"), EnqueueTime: time.Now()},
		{ID: "2", Queue: "q", Payload: []byte("b"), EnqueueTime: time.Now()},
	}
	for _, m := range msgs {
		if err := w.WriteMessage(m); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	got, err := NewReader(dir, "q").Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 messages, got %d", len(got))
	}
	if string(got[0].ID) != "1" || string(got[1].ID) != "2" {
		t.Fatalf("wrong IDs: %v %v", got[0].ID, got[1].ID)
	}
}

func TestReplay_TombstonedMessagesExcluded(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, "q")
	if err != nil {
		t.Fatal(err)
	}

	w.WriteMessage(core.Message{ID: "1", Queue: "q", Payload: []byte("keep"), EnqueueTime: time.Now()})
	w.WriteMessage(core.Message{ID: "2", Queue: "q", Payload: []byte("ack"), EnqueueTime: time.Now()})
	w.WriteTombstone("2")
	w.Close()

	got, err := NewReader(dir, "q").Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 message, got %d", len(got))
	}
	if string(got[0].ID) != "1" {
		t.Fatalf("wrong message survived: %v", got[0].ID)
	}
}

func TestReplay_StopsAtTornWrite(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, "q")
	if err != nil {
		t.Fatal(err)
	}
	w.WriteMessage(core.Message{ID: "1", Queue: "q", Payload: []byte("ok"), EnqueueTime: time.Now()})
	w.Close()

	// append garbage bytes to simulate a torn write
	f, _ := os.OpenFile(dir+"/q.wal", os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0xFF})
	f.Close()

	got, err := NewReader(dir, "q").Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 message before torn write, got %d", len(got))
	}
}

func TestReplay_NoFile(t *testing.T) {
	got, err := NewReader(t.TempDir(), "nonexistent").Replay()
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("want nil, got %v", got)
	}
}

func TestReplay_PreservesRetryCount(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, "q")
	w.WriteMessage(core.Message{ID: "1", Queue: "q", Payload: []byte("x"), RetryCount: 3, EnqueueTime: time.Now()})
	w.Close()

	got, _ := NewReader(dir, "q").Replay()
	if got[0].RetryCount != 3 {
		t.Fatalf("want RetryCount=3, got %d", got[0].RetryCount)
	}
}

func TestCompact_DropsTombstonedRecords(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, "q")
	if err != nil {
		t.Fatal(err)
	}

	w.WriteMessage(core.Message{ID: "1", Queue: "q", Payload: []byte("keep"), EnqueueTime: time.Now()})
	w.WriteMessage(core.Message{ID: "2", Queue: "q", Payload: []byte("ack"), EnqueueTime: time.Now()})
	w.WriteMessage(core.Message{ID: "3", Queue: "q", Payload: []byte("keep"), EnqueueTime: time.Now()})
	w.WriteTombstone("2")

	statBefore, _ := os.Stat(dir + "/q.wal")

	if err := w.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// WAL must be smaller — tombstoned record and its tombstone are gone.
	statAfter, _ := os.Stat(dir + "/q.wal")
	if statAfter.Size() >= statBefore.Size() {
		t.Fatalf("WAL should shrink after compact: before=%d after=%d", statBefore.Size(), statAfter.Size())
	}

	// Replay must return exactly the two un-tombstoned messages in order.
	got, err := NewReader(dir, "q").Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 messages after compact, got %d", len(got))
	}
	if string(got[0].ID) != "1" || string(got[1].ID) != "3" {
		t.Fatalf("wrong IDs after compact: %v %v", got[0].ID, got[1].ID)
	}
}

func TestCompact_WriterUsableAfterCompact(t *testing.T) {
	dir := t.TempDir()
	w, _ := NewWriter(dir, "q")
	w.WriteMessage(core.Message{ID: "1", Queue: "q", Payload: []byte("a"), EnqueueTime: time.Now()})
	w.WriteTombstone("1")

	if err := w.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Writer must still accept new records after compaction.
	if err := w.WriteMessage(core.Message{ID: "2", Queue: "q", Payload: []byte("b"), EnqueueTime: time.Now()}); err != nil {
		t.Fatalf("write after compact: %v", err)
	}
	w.Close()

	got, _ := NewReader(dir, "q").Replay()
	if len(got) != 1 || string(got[0].ID) != "2" {
		t.Fatalf("want only msg-2 after compact+write, got %v", got)
	}
}

