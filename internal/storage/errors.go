package storage

import "errors"

var (
	// ErrCorruptedData indicates data corruption was detected
	ErrCorruptedData = errors.New("corrupted data")

	// ErrCRCMismatch indicates a CRC checksum mismatch
	ErrCRCMismatch = errors.New("CRC mismatch")

	// ErrSegmentNotFound indicates the requested segment doesn't exist
	ErrSegmentNotFound = errors.New("segment not found")

	// ErrOffsetOutOfRange indicates the requested offset is not available
	ErrOffsetOutOfRange = errors.New("offset out of range")

	// ErrPartitionNotFound indicates the partition doesn't exist
	ErrPartitionNotFound = errors.New("partition not found")

	// ErrTopicNotFound indicates the topic doesn't exist
	ErrTopicNotFound = errors.New("topic not found")

	// ErrTopicExists indicates the topic already exists
	ErrTopicExists = errors.New("topic already exists")

	// ErrSegmentFull indicates the segment has reached its maximum size
	ErrSegmentFull = errors.New("segment full")

	// ErrStorageClosed indicates the storage has been closed
	ErrStorageClosed = errors.New("storage closed")

	// ErrInvalidOffset indicates an invalid offset value
	ErrInvalidOffset = errors.New("invalid offset")
)
