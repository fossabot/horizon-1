package storage

import (
	"os"
	"testing"
)

func TestRecordBatchEncodeDecode(t *testing.T) {
	// Create records
	records := []Record{
		{
			OffsetDelta:    0,
			TimestampDelta: 0,
			Key:            []byte("key1"),
			Value:          []byte("value1"),
			Headers: []RecordHeader{
				{Key: "header1", Value: []byte("hvalue1")},
			},
		},
		{
			OffsetDelta:    1,
			TimestampDelta: 100,
			Key:            []byte("key2"),
			Value:          []byte("value2"),
		},
	}

	// Create batch
	batch := NewRecordBatch(0, records)

	// Encode
	encoded := batch.Encode()
	if len(encoded) == 0 {
		t.Fatal("encoded batch is empty")
	}

	// Decode
	decoded, err := DecodeRecordBatch(encoded)
	if err != nil {
		t.Fatalf("failed to decode batch: %v", err)
	}

	// Verify
	if decoded.BaseOffset != batch.BaseOffset {
		t.Errorf("base offset mismatch: got %d, want %d", decoded.BaseOffset, batch.BaseOffset)
	}

	if len(decoded.Records) != len(records) {
		t.Fatalf("record count mismatch: got %d, want %d", len(decoded.Records), len(records))
	}

	for i, r := range decoded.Records {
		if string(r.Key) != string(records[i].Key) {
			t.Errorf("record %d key mismatch: got %s, want %s", i, r.Key, records[i].Key)
		}
		if string(r.Value) != string(records[i].Value) {
			t.Errorf("record %d value mismatch: got %s, want %s", i, r.Value, records[i].Value)
		}
	}
}

func TestSegmentAppendRead(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "horizon-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create segment
	config := DefaultSegmentConfig()
	segment, err := NewSegment(tmpDir, 0, config)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer segment.Close()

	// Create and append batch
	records := []Record{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}
	batch := NewRecordBatch(0, records)

	baseOffset, err := segment.Append(batch)
	if err != nil {
		t.Fatalf("failed to append batch: %v", err)
	}

	if baseOffset != 0 {
		t.Errorf("unexpected base offset: got %d, want 0", baseOffset)
	}

	// Read back
	batches, err := segment.Read(0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to read segment: %v", err)
	}

	if len(batches) != 1 {
		t.Fatalf("unexpected batch count: got %d, want 1", len(batches))
	}

	if len(batches[0].Records) != len(records) {
		t.Errorf("record count mismatch: got %d, want %d", len(batches[0].Records), len(records))
	}
}

func TestPartitionAppendFetch(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "horizon-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create partition
	config := DefaultPartitionConfig()
	partition, err := NewPartition(tmpDir, "test-topic", 0, config)
	if err != nil {
		t.Fatalf("failed to create partition: %v", err)
	}
	defer partition.Close()

	// Append records
	records := []Record{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}

	baseOffset, err := partition.Append(records)
	if err != nil {
		t.Fatalf("failed to append records: %v", err)
	}

	if baseOffset != 0 {
		t.Errorf("unexpected base offset: got %d, want 0", baseOffset)
	}

	// Verify offsets
	if partition.LogStartOffset() != 0 {
		t.Errorf("unexpected log start offset: got %d, want 0", partition.LogStartOffset())
	}

	if partition.LogEndOffset() != 3 {
		t.Errorf("unexpected log end offset: got %d, want 3", partition.LogEndOffset())
	}

	// Fetch records
	batches, err := partition.Fetch(0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to fetch records: %v", err)
	}

	if len(batches) == 0 {
		t.Fatal("no batches returned")
	}

	totalRecords := 0
	for _, b := range batches {
		totalRecords += len(b.Records)
	}

	if totalRecords != len(records) {
		t.Errorf("record count mismatch: got %d, want %d", totalRecords, len(records))
	}
}

func TestLogCreateTopicAppend(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "horizon-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create log
	config := DefaultLogConfig(tmpDir)
	log, err := NewLog(config)
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}
	defer log.Close()

	// Create topic
	err = log.CreateTopic("test-topic", 3)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Verify topic exists
	topics := log.ListTopics()
	if len(topics) != 1 || topics[0] != "test-topic" {
		t.Errorf("unexpected topics: %v", topics)
	}

	// Get partitions
	partitions, err := log.GetTopicPartitions("test-topic")
	if err != nil {
		t.Fatalf("failed to get partitions: %v", err)
	}

	if len(partitions) != 3 {
		t.Errorf("unexpected partition count: got %d, want 3", len(partitions))
	}

	// Append to partition 0
	records := []Record{
		{Key: []byte("key"), Value: []byte("value")},
	}

	offset, err := log.Append("test-topic", 0, records)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	if offset != 0 {
		t.Errorf("unexpected offset: got %d, want 0", offset)
	}

	// Fetch
	batches, err := log.Fetch("test-topic", 0, 0, 1024*1024)
	if err != nil {
		t.Fatalf("failed to fetch: %v", err)
	}

	if len(batches) == 0 {
		t.Fatal("no batches returned")
	}
}
