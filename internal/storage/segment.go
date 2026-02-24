package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	// DefaultSegmentMaxBytes is the default maximum size of a segment (1GB)
	DefaultSegmentMaxBytes = 1024 * 1024 * 1024

	// DefaultIndexIntervalBytes is the default interval for index entries
	DefaultIndexIntervalBytes = 4096

	// LogFileSuffix is the suffix for log files
	LogFileSuffix = ".log"

	// IndexFileSuffix is the suffix for index files
	IndexFileSuffix = ".index"

	// TimeIndexFileSuffix is the suffix for time index files
	TimeIndexFileSuffix = ".timeindex"
)

// IndexEntry represents an entry in the offset index
type IndexEntry struct {
	Offset   int64 // Relative offset from base offset
	Position int64 // Physical position in log file
}

// TimeIndexEntry represents an entry in the time index
type TimeIndexEntry struct {
	Timestamp int64 // Timestamp
	Offset    int64 // Relative offset
}

// Segment represents a log segment file
type Segment struct {
	mu sync.RWMutex

	// Base offset of this segment
	baseOffset int64

	// Directory containing segment files
	dir string

	// Log file
	logFile *os.File

	// Index file
	indexFile *os.File

	// Time index file
	timeIndexFile *os.File

	// Current size of the log file
	size int64

	// Next offset to be assigned
	nextOffset int64

	// Index entries (kept in memory for fast lookup)
	index []IndexEntry

	// Time index entries
	timeIndex []TimeIndexEntry

	// Maximum segment size in bytes
	maxBytes int64

	// Bytes since last index entry
	bytesSinceLastIndex int64

	// Index interval in bytes
	indexIntervalBytes int64

	// Whether segment is closed
	closed bool
}

// SegmentConfig holds configuration for a segment
type SegmentConfig struct {
	MaxBytes           int64
	IndexIntervalBytes int64
}

// DefaultSegmentConfig returns default segment configuration
func DefaultSegmentConfig() SegmentConfig {
	return SegmentConfig{
		MaxBytes:           DefaultSegmentMaxBytes,
		IndexIntervalBytes: DefaultIndexIntervalBytes,
	}
}

// NewSegment creates or opens a segment
func NewSegment(dir string, baseOffset int64, config SegmentConfig) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create segment directory: %w", err)
	}

	s := &Segment{
		baseOffset:         baseOffset,
		dir:                dir,
		maxBytes:           config.MaxBytes,
		indexIntervalBytes: config.IndexIntervalBytes,
		nextOffset:         baseOffset,
	}

	// Open log file
	logPath := s.logPath()
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	s.logFile = logFile

	// Get log file size
	stat, err := logFile.Stat()
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to stat log file: %w", err)
	}
	s.size = stat.Size()

	// Open index file
	indexPath := s.indexPath()
	indexFile, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}
	s.indexFile = indexFile

	// Open time index file
	timeIndexPath := s.timeIndexPath()
	timeIndexFile, err := os.OpenFile(timeIndexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		logFile.Close()
		indexFile.Close()
		return nil, fmt.Errorf("failed to open time index file: %w", err)
	}
	s.timeIndexFile = timeIndexFile

	// Load index entries
	if err := s.loadIndex(); err != nil {
		s.Close()
		return nil, fmt.Errorf("failed to load index: %w", err)
	}

	// Recover next offset from log file
	if err := s.recover(); err != nil {
		s.Close()
		return nil, fmt.Errorf("failed to recover segment: %w", err)
	}

	return s, nil
}

// logPath returns the path to the log file
func (s *Segment) logPath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, LogFileSuffix))
}

// indexPath returns the path to the index file
func (s *Segment) indexPath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, IndexFileSuffix))
}

// timeIndexPath returns the path to the time index file
func (s *Segment) timeIndexPath() string {
	return filepath.Join(s.dir, fmt.Sprintf("%020d%s", s.baseOffset, TimeIndexFileSuffix))
}

// BaseOffset returns the base offset of this segment
func (s *Segment) BaseOffset() int64 {
	return s.baseOffset
}

// NextOffset returns the next offset to be assigned
func (s *Segment) NextOffset() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextOffset
}

// Size returns the current size of the log file
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// IsFull returns true if the segment has reached its maximum size
func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size >= s.maxBytes
}

// Append writes a record batch to the segment
func (s *Segment) Append(batch *RecordBatch) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrStorageClosed
	}

	if s.size >= s.maxBytes {
		return 0, ErrSegmentFull
	}

	// Set base offset for the batch
	batch.BaseOffset = s.nextOffset

	// Update record offsets
	for i := range batch.Records {
		batch.Records[i].OffsetDelta = int32(i)
	}
	if len(batch.Records) > 0 {
		batch.LastOffsetDelta = int32(len(batch.Records) - 1)
	}

	// Encode batch
	data := batch.Encode()

	// Current position
	position := s.size

	// Write to log file
	n, err := s.logFile.Write(data)
	if err != nil {
		return 0, fmt.Errorf("failed to write to log: %w", err)
	}

	// Update size
	s.size += int64(n)
	s.bytesSinceLastIndex += int64(n)

	// Add index entry if interval reached
	if s.bytesSinceLastIndex >= s.indexIntervalBytes {
		if err := s.addIndexEntry(s.nextOffset, position); err != nil {
			return 0, fmt.Errorf("failed to write index: %w", err)
		}
		s.bytesSinceLastIndex = 0
	}

	// Add time index entry
	if err := s.addTimeIndexEntry(batch.MaxTimestamp, s.nextOffset); err != nil {
		return 0, fmt.Errorf("failed to write time index: %w", err)
	}

	// Calculate next offset
	baseOffset := s.nextOffset
	s.nextOffset += int64(len(batch.Records))

	return baseOffset, nil
}

// addIndexEntry adds an entry to the offset index
func (s *Segment) addIndexEntry(offset, position int64) error {
	relativeOffset := offset - s.baseOffset
	entry := IndexEntry{
		Offset:   relativeOffset,
		Position: position,
	}
	s.index = append(s.index, entry)

	// Write to index file (relative offset as int32, position as int32)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], uint32(relativeOffset))
	binary.BigEndian.PutUint32(buf[4:8], uint32(position))

	_, err := s.indexFile.Write(buf)
	return err
}

// addTimeIndexEntry adds an entry to the time index
func (s *Segment) addTimeIndexEntry(timestamp, offset int64) error {
	relativeOffset := offset - s.baseOffset
	entry := TimeIndexEntry{
		Timestamp: timestamp,
		Offset:    relativeOffset,
	}
	s.timeIndex = append(s.timeIndex, entry)

	// Write to time index file
	buf := make([]byte, 12)
	binary.BigEndian.PutUint64(buf[0:8], uint64(timestamp))
	binary.BigEndian.PutUint32(buf[8:12], uint32(relativeOffset))

	_, err := s.timeIndexFile.Write(buf)
	return err
}

// Read reads record batches starting from the given offset
func (s *Segment) Read(offset int64, maxBytes int64) ([]*RecordBatch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrStorageClosed
	}

	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, ErrOffsetOutOfRange
	}

	// Find position using index
	position := s.findPosition(offset)

	// Read from log file
	data := make([]byte, maxBytes)
	n, err := s.logFile.ReadAt(data, position)
	if err != nil && n == 0 {
		return nil, fmt.Errorf("failed to read log: %w", err)
	}
	data = data[:n]

	// Decode record batches
	var batches []*RecordBatch
	pos := 0
	for pos < len(data) {
		// Need at least 12 bytes for base offset and batch length
		if len(data)-pos < 12 {
			break
		}

		batchLen := int32(binary.BigEndian.Uint32(data[pos+8:]))
		totalLen := 12 + int(batchLen)

		if len(data)-pos < totalLen {
			break
		}

		batch, err := DecodeRecordBatch(data[pos : pos+totalLen])
		if err != nil {
			break // Stop at first error
		}

		// Only include batches at or after requested offset
		if batch.BaseOffset+int64(batch.LastOffsetDelta) >= offset {
			batches = append(batches, batch)
		}

		pos += totalLen
	}

	return batches, nil
}

// findPosition finds the physical position for an offset using binary search
func (s *Segment) findPosition(offset int64) int64 {
	relativeOffset := offset - s.baseOffset

	// Binary search in index
	left, right := 0, len(s.index)-1
	var position int64 = 0

	for left <= right {
		mid := (left + right) / 2
		if s.index[mid].Offset <= relativeOffset {
			position = s.index[mid].Position
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	return position
}

// loadIndex loads index entries from the index file
func (s *Segment) loadIndex() error {
	stat, err := s.indexFile.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		return nil
	}

	// Read all index entries
	data := make([]byte, stat.Size())
	_, err = s.indexFile.ReadAt(data, 0)
	if err != nil {
		return err
	}

	// Parse entries (8 bytes each: 4 for offset, 4 for position)
	for i := 0; i+8 <= len(data); i += 8 {
		entry := IndexEntry{
			Offset:   int64(binary.BigEndian.Uint32(data[i : i+4])),
			Position: int64(binary.BigEndian.Uint32(data[i+4 : i+8])),
		}
		s.index = append(s.index, entry)
	}

	return nil
}

// recover recovers the segment state by scanning the log file
func (s *Segment) recover() error {
	if s.size == 0 {
		return nil
	}

	// Read entire log file (for simplicity, could optimize with chunked reading)
	data := make([]byte, s.size)
	_, err := s.logFile.ReadAt(data, 0)
	if err != nil {
		return err
	}

	// Scan batches to find last offset
	pos := 0
	var lastOffset int64 = s.baseOffset

	for pos < len(data) {
		if len(data)-pos < 12 {
			break
		}

		_ = int64(binary.BigEndian.Uint64(data[pos:])) // baseOffset from header
		batchLen := int32(binary.BigEndian.Uint32(data[pos+8:]))
		totalLen := 12 + int(batchLen)

		if len(data)-pos < totalLen {
			break
		}

		batch, err := DecodeRecordBatch(data[pos : pos+totalLen])
		if err != nil {
			// Truncate at corruption
			s.size = int64(pos)
			break
		}

		lastOffset = batch.BaseOffset + int64(len(batch.Records))
		pos += totalLen
	}

	s.nextOffset = lastOffset
	return nil
}

// Sync flushes data to disk
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrStorageClosed
	}

	if err := s.logFile.Sync(); err != nil {
		return err
	}
	if err := s.indexFile.Sync(); err != nil {
		return err
	}
	return s.timeIndexFile.Sync()
}

// Close closes the segment
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	var errs []error
	if s.logFile != nil {
		if err := s.logFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.indexFile != nil {
		if err := s.indexFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.timeIndexFile != nil {
		if err := s.timeIndexFile.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// Delete removes segment files from disk
func (s *Segment) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.logPath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(s.indexPath()); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(s.timeIndexPath()); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}
