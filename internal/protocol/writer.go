package protocol

import (
	"encoding/binary"
)

// Writer provides methods for writing Kafka protocol data types
type Writer struct {
	buf []byte
}

// NewWriter creates a new protocol writer with optional initial capacity
func NewWriter(capacity int) *Writer {
	return &Writer{buf: make([]byte, 0, capacity)}
}

// Bytes returns the accumulated bytes
func (w *Writer) Bytes() []byte {
	return w.buf
}

// Len returns the current length
func (w *Writer) Len() int {
	return len(w.buf)
}

// Reset clears the buffer
func (w *Writer) Reset() {
	w.buf = w.buf[:0]
}

// WriteInt8 writes a signed 8-bit integer
func (w *Writer) WriteInt8(val int8) {
	w.buf = append(w.buf, byte(val))
}

// WriteInt16 writes a signed 16-bit integer (big-endian)
func (w *Writer) WriteInt16(val int16) {
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(val))
}

// WriteInt32 writes a signed 32-bit integer (big-endian)
func (w *Writer) WriteInt32(val int32) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(val))
}

// WriteInt64 writes a signed 64-bit integer (big-endian)
func (w *Writer) WriteInt64(val int64) {
	w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(val))
}

// WriteUint16 writes an unsigned 16-bit integer (big-endian)
func (w *Writer) WriteUint16(val uint16) {
	w.buf = binary.BigEndian.AppendUint16(w.buf, val)
}

// WriteUint32 writes an unsigned 32-bit integer (big-endian)
func (w *Writer) WriteUint32(val uint32) {
	w.buf = binary.BigEndian.AppendUint32(w.buf, val)
}

// WriteUint64 writes an unsigned 64-bit integer (big-endian)
func (w *Writer) WriteUint64(val uint64) {
	w.buf = binary.BigEndian.AppendUint64(w.buf, val)
}

// WriteVarint writes a variable-length signed integer
func (w *Writer) WriteVarint(val int32) {
	// Zigzag encode
	uval := uint32((val << 1) ^ (val >> 31))
	w.WriteUvarint(uval)
}

// WriteVarlong writes a variable-length signed 64-bit integer
func (w *Writer) WriteVarlong(val int64) {
	// Zigzag encode
	uval := uint64((val << 1) ^ (val >> 63))
	w.WriteUvarlong(uval)
}

// WriteUvarint writes an unsigned variable-length integer
func (w *Writer) WriteUvarint(val uint32) {
	for val >= 0x80 {
		w.buf = append(w.buf, byte(val)|0x80)
		val >>= 7
	}
	w.buf = append(w.buf, byte(val))
}

// WriteUvarlong writes an unsigned variable-length 64-bit integer
func (w *Writer) WriteUvarlong(val uint64) {
	for val >= 0x80 {
		w.buf = append(w.buf, byte(val)|0x80)
		val >>= 7
	}
	w.buf = append(w.buf, byte(val))
}

// WriteString writes a length-prefixed string (INT16 length)
func (w *Writer) WriteString(s string) {
	w.WriteInt16(int16(len(s)))
	w.buf = append(w.buf, s...)
}

// WriteNullableString writes a nullable length-prefixed string
func (w *Writer) WriteNullableString(s *string) {
	if s == nil {
		w.WriteInt16(-1)
		return
	}
	w.WriteString(*s)
}

// WriteCompactString writes a compact string (UNSIGNED_VARINT length)
func (w *Writer) WriteCompactString(s string) {
	w.WriteUvarint(uint32(len(s) + 1))
	w.buf = append(w.buf, s...)
}

// WriteCompactNullableString writes a compact nullable string
func (w *Writer) WriteCompactNullableString(s *string) {
	if s == nil {
		w.WriteUvarint(0)
		return
	}
	w.WriteCompactString(*s)
}

// WriteBytes writes length-prefixed bytes (INT32 length)
func (w *Writer) WriteBytes(data []byte) {
	if data == nil {
		w.WriteInt32(-1)
		return
	}
	w.WriteInt32(int32(len(data)))
	w.buf = append(w.buf, data...)
}

// WriteNullableBytes writes nullable length-prefixed bytes
func (w *Writer) WriteNullableBytes(data []byte) {
	w.WriteBytes(data)
}

// WriteCompactBytes writes compact bytes (UNSIGNED_VARINT length)
func (w *Writer) WriteCompactBytes(data []byte) {
	if data == nil {
		w.WriteUvarint(0)
		return
	}
	w.WriteUvarint(uint32(len(data) + 1))
	w.buf = append(w.buf, data...)
}

// WriteRawBytes writes raw bytes without length prefix
func (w *Writer) WriteRawBytes(data []byte) {
	w.buf = append(w.buf, data...)
}

// WriteBool writes a boolean value
func (w *Writer) WriteBool(val bool) {
	if val {
		w.WriteInt8(1)
	} else {
		w.WriteInt8(0)
	}
}

// WriteArrayLen writes array length (INT32)
func (w *Writer) WriteArrayLen(length int32) {
	w.WriteInt32(length)
}

// WriteCompactArrayLen writes compact array length (UNSIGNED_VARINT)
func (w *Writer) WriteCompactArrayLen(length int) {
	if length < 0 {
		w.WriteUvarint(0) // null array
	} else {
		w.WriteUvarint(uint32(length + 1))
	}
}

// WriteTaggedFields writes empty tagged fields
func (w *Writer) WriteTaggedFields() {
	w.WriteUvarint(0) // no tagged fields
}

// WritePlaceholder writes a placeholder and returns its position
func (w *Writer) WritePlaceholder(size int) int {
	pos := len(w.buf)
	for i := 0; i < size; i++ {
		w.buf = append(w.buf, 0)
	}
	return pos
}

// FillInt32 fills a placeholder with an int32 value
func (w *Writer) FillInt32(pos int, val int32) {
	binary.BigEndian.PutUint32(w.buf[pos:], uint32(val))
}

// FillInt16 fills a placeholder with an int16 value
func (w *Writer) FillInt16(pos int, val int16) {
	binary.BigEndian.PutUint16(w.buf[pos:], uint16(val))
}
