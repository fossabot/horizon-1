// Package broker implements the Horizon message broker.
// It handles Kafka protocol requests and coordinates storage.
package broker

import (
	"fmt"
	"sync"
	"time"

	"horizon/internal/storage"
)

// BrokerConfig holds configuration for the broker
type BrokerConfig struct {
	// NodeID is the unique identifier for this broker
	NodeID int32

	// Host is the bind host
	Host string

	// AdvertisedHost is the host advertised to clients (defaults to Host)
	AdvertisedHost string

	// Port is the advertised port
	Port int32

	// DataDir is the directory for data storage
	DataDir string

	// LogRetentionBytes is the max size of log before cleanup
	LogRetentionBytes int64

	// LogRetentionMs is the max age of log before cleanup
	LogRetentionMs int64

	// SegmentBytes is the max size of a segment
	SegmentBytes int64

	// DefaultNumPartitions for auto-created topics
	DefaultNumPartitions int32

	// DefaultReplicationFactor for auto-created topics
	DefaultReplicationFactor int16

	// AutoCreateTopics enables auto topic creation
	AutoCreateTopics bool
}

// DefaultBrokerConfig returns default broker configuration
func DefaultBrokerConfig() BrokerConfig {
	return BrokerConfig{
		NodeID:                   0,
		Host:                     "localhost",
		Port:                     9092,
		DataDir:                  "./data",
		LogRetentionBytes:        -1, // unlimited
		LogRetentionMs:           7 * 24 * 60 * 60 * 1000, // 7 days
		SegmentBytes:             1024 * 1024 * 1024, // 1GB
		DefaultNumPartitions:     1,
		DefaultReplicationFactor: 1,
		AutoCreateTopics:         true,
	}
}

// Broker is the main message broker
type Broker struct {
	mu sync.RWMutex

	config BrokerConfig

	// Storage layer
	log *storage.Log

	// Topic configurations
	topicConfigs map[string]*TopicConfig

	// Consumer group manager
	groupManager *GroupManager

	// Cluster metadata version
	metadataVersion int32

	// Whether broker is running
	running bool

	// Shutdown channel
	shutdownCh chan struct{}
}

// TopicConfig holds configuration for a topic
type TopicConfig struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	RetentionMs       int64
	RetentionBytes    int64
	CleanupPolicy     string // "delete" or "compact"
}

// New creates a new broker instance
func New(config BrokerConfig) (*Broker, error) {
	// Create storage
	logConfig := storage.LogConfig{
		Dir: config.DataDir,
		PartitionConfig: storage.PartitionConfig{
			SegmentConfig: storage.SegmentConfig{
				MaxBytes:           config.SegmentBytes,
				IndexIntervalBytes: 4096,
			},
		},
	}

	log, err := storage.NewLog(logConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create log: %w", err)
	}

	b := &Broker{
		config:       config,
		log:          log,
		topicConfigs: make(map[string]*TopicConfig),
		groupManager: NewGroupManager(),
		shutdownCh:   make(chan struct{}),
	}

	return b, nil
}

// Start starts the broker
func (b *Broker) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.running {
		return nil
	}

	b.running = true

	// Start background tasks
	go b.backgroundTasks()

	return nil
}

// Stop stops the broker
func (b *Broker) Stop() error {
	b.mu.Lock()
	if !b.running {
		b.mu.Unlock()
		return nil
	}
	b.running = false
	close(b.shutdownCh)
	b.mu.Unlock()

	// Close storage
	if err := b.log.Close(); err != nil {
		return fmt.Errorf("failed to close log: %w", err)
	}

	return nil
}

// backgroundTasks runs periodic background tasks
func (b *Broker) backgroundTasks() {
	syncTicker := time.NewTicker(5 * time.Second)
	defer syncTicker.Stop()

	for {
		select {
		case <-b.shutdownCh:
			return
		case <-syncTicker.C:
			b.log.Sync()
		}
	}
}

// Produce writes records to a topic partition
func (b *Broker) Produce(topic string, partition int32, records []storage.Record) (int64, error) {
	b.mu.RLock()
	if !b.running {
		b.mu.RUnlock()
		return 0, fmt.Errorf("broker not running")
	}
	autoCreate := b.config.AutoCreateTopics
	b.mu.RUnlock()

	// Auto-create topic if needed
	if autoCreate {
		if err := b.ensureTopic(topic); err != nil {
			return 0, err
		}
	}

	return b.log.Append(topic, partition, records)
}

// Fetch reads records from a topic partition
func (b *Broker) Fetch(topic string, partition int32, offset int64, maxBytes int64) ([]*storage.RecordBatch, error) {
	b.mu.RLock()
	if !b.running {
		b.mu.RUnlock()
		return nil, fmt.Errorf("broker not running")
	}
	b.mu.RUnlock()

	return b.log.Fetch(topic, partition, offset, maxBytes)
}

// CreateTopic creates a new topic
func (b *Broker) CreateTopic(name string, numPartitions int32, replicationFactor int16) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.running {
		return fmt.Errorf("broker not running")
	}

	if _, exists := b.topicConfigs[name]; exists {
		return storage.ErrTopicExists
	}

	// Create in storage
	if err := b.log.CreateTopic(name, numPartitions); err != nil {
		return err
	}

	// Save config
	b.topicConfigs[name] = &TopicConfig{
		Name:              name,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		RetentionMs:       b.config.LogRetentionMs,
		RetentionBytes:    b.config.LogRetentionBytes,
		CleanupPolicy:     "delete",
	}

	b.metadataVersion++

	return nil
}

// DeleteTopic deletes a topic
func (b *Broker) DeleteTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.running {
		return fmt.Errorf("broker not running")
	}

	if err := b.log.DeleteTopic(name); err != nil {
		return err
	}

	delete(b.topicConfigs, name)
	b.metadataVersion++

	return nil
}

// ensureTopic creates topic if it doesn't exist
func (b *Broker) ensureTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topicConfigs[topic]; exists {
		return nil
	}

	// Create with defaults
	if err := b.log.CreateTopic(topic, b.config.DefaultNumPartitions); err != nil {
		if err == storage.ErrTopicExists {
			return nil
		}
		return err
	}

	b.topicConfigs[topic] = &TopicConfig{
		Name:              topic,
		NumPartitions:     b.config.DefaultNumPartitions,
		ReplicationFactor: b.config.DefaultReplicationFactor,
		RetentionMs:       b.config.LogRetentionMs,
		RetentionBytes:    b.config.LogRetentionBytes,
		CleanupPolicy:     "delete",
	}

	b.metadataVersion++

	return nil
}

// GetMetadata returns cluster metadata
func (b *Broker) GetMetadata(topics []string) (*ClusterMetadata, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.running {
		return nil, fmt.Errorf("broker not running")
	}

	// Use advertised host if set, otherwise fallback to localhost for 0.0.0.0
	advertisedHost := b.config.AdvertisedHost
	if advertisedHost == "" {
		if b.config.Host == "0.0.0.0" || b.config.Host == "" {
			advertisedHost = "localhost"
		} else {
			advertisedHost = b.config.Host
		}
	}

	meta := &ClusterMetadata{
		Brokers: []BrokerInfo{{
			NodeID: b.config.NodeID,
			Host:   advertisedHost,
			Port:   b.config.Port,
		}},
		ControllerID: b.config.NodeID,
	}

	// If no topics specified, return all
	if len(topics) == 0 {
		topics = b.log.ListTopics()
	}

	for _, topic := range topics {
		topicMeta, err := b.log.GetTopicMetadata(topic)
		if err != nil {
			continue
		}

		tm := TopicMetadata{
			Topic: topic,
		}

		for _, pm := range topicMeta.Partitions {
			tm.Partitions = append(tm.Partitions, PartitionMetadata{
				Partition:     pm.Partition,
				Leader:        b.config.NodeID,
				Replicas:      []int32{b.config.NodeID},
				ISR:           []int32{b.config.NodeID},
				HighWatermark: pm.HighWatermark,
			})
		}

		meta.Topics = append(meta.Topics, tm)
	}

	return meta, nil
}

// ListOffsets returns offsets for a partition
func (b *Broker) ListOffsets(topic string, partition int32, timestamp int64) (int64, error) {
	b.mu.RLock()
	if !b.running {
		b.mu.RUnlock()
		return 0, fmt.Errorf("broker not running")
	}
	b.mu.RUnlock()

	p, err := b.log.GetPartition(topic, partition)
	if err != nil {
		return 0, err
	}

	return p.GetOffsetByTime(timestamp)
}

// GetGroupManager returns the consumer group manager
func (b *Broker) GetGroupManager() *GroupManager {
	return b.groupManager
}

// ClusterMetadata contains cluster metadata
type ClusterMetadata struct {
	Brokers      []BrokerInfo
	ControllerID int32
	Topics       []TopicMetadata
}

// BrokerInfo contains broker information
type BrokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
}

// TopicMetadata contains topic metadata
type TopicMetadata struct {
	Topic      string
	Partitions []PartitionMetadata
}

// PartitionMetadata contains partition metadata
type PartitionMetadata struct {
	Partition     int32
	Leader        int32
	Replicas      []int32
	ISR           []int32
	HighWatermark int64
}
