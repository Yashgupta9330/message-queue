package wal

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
)

// Wire format per record:
//   [magic: 4B] [type: 1B] [length: 4B] [payload: NB] [crc32: 4B]
//
// CRC32 covers type + length + payload (not magic).
// A CRC mismatch means a torn write — the reader stops here.

const (
	magicVal     uint32 = 0xC0FFEE01
	headerSize          = 4 + 1 + 4 // magic + type + length
	checksumSize        = 4
	recordOverhead      = headerSize + checksumSize
)

// RecordType identifies what kind of entry this is.
type RecordType uint8

const (
	RecordMessage   RecordType = 0x01 // a new message being enqueued
	RecordTombstone RecordType = 0x02 // an ACK — message is done
)

// record is the envelope serialised into the payload field.
type record struct {
	Type RecordType `json:"t"`
	ID   string     `json:"id"`           // always present
	Msg  *recordMsg `json:"m,omitempty"`  // only on RecordMessage
}

// recordMsg holds the message fields for a RecordMessage entry.
type recordMsg struct {
	Queue       string        `json:"q"`
	Payload     []byte        `json:"p"`
	Headers     []recordHeader `json:"h,omitempty"`
	RetryCount  int32         `json:"r"`
	EnqueueTime int64         `json:"t"` // unix nanoseconds
}

type recordHeader struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

// encode serialises a record into the binary wire format.
func encode(rec record) ([]byte, error) {
	payload, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, recordOverhead+len(payload))

	// magic
	binary.BigEndian.PutUint32(buf[0:4], magicVal)
	// type
	buf[4] = byte(rec.Type)
	// length
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(payload)))
	// payload
	copy(buf[9:], payload)
	// crc32 over [type + length + payload]
	crc := crc32.ChecksumIEEE(buf[4 : 9+len(payload)])
	binary.BigEndian.PutUint32(buf[9+len(payload):], crc)

	return buf, nil
}

// decode reads one record from buf.
// Returns the record, the number of bytes consumed, and true on success.
// Returns false if the magic is wrong, the buffer is too short,
// or the CRC does not match — all of which indicate a torn write.
func decode(buf []byte) (record, int, bool) {
	if len(buf) < recordOverhead {
		return record{}, 0, false
	}

	if binary.BigEndian.Uint32(buf[0:4]) != magicVal {
		return record{}, 0, false
	}

	payloadLen := binary.BigEndian.Uint32(buf[5:9])
	total := int(recordOverhead) + int(payloadLen)
	if len(buf) < total {
		return record{}, 0, false
	}

	// verify CRC before touching the payload
	wantCRC := crc32.ChecksumIEEE(buf[4 : 9+payloadLen])
	gotCRC := binary.BigEndian.Uint32(buf[9+payloadLen : 13+payloadLen])
	if wantCRC != gotCRC {
		return record{}, 0, false
	}

	var rec record
	if err := json.Unmarshal(buf[9:9+payloadLen], &rec); err != nil {
		return record{}, 0, false
	}

	return rec, total, true
}