package broker

import (
	"sync"
	"time"
)

// ConsumerGroupState represents the state of a consumer group
type ConsumerGroupState int

const (
	GroupStateEmpty ConsumerGroupState = iota
	GroupStatePreparingRebalance
	GroupStateCompletingRebalance
	GroupStateStable
	GroupStateDead
)

func (s ConsumerGroupState) String() string {
	switch s {
	case GroupStateEmpty:
		return "Empty"
	case GroupStatePreparingRebalance:
		return "PreparingRebalance"
	case GroupStateCompletingRebalance:
		return "CompletingRebalance"
	case GroupStateStable:
		return "Stable"
	case GroupStateDead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// GroupMember represents a member of a consumer group
type GroupMember struct {
	// MemberID is the unique identifier for the member
	MemberID string

	// ClientID is the client identifier
	ClientID string

	// ClientHost is the host of the client
	ClientHost string

	// SessionTimeoutMs is the session timeout
	SessionTimeoutMs int32

	// RebalanceTimeoutMs is the rebalance timeout
	RebalanceTimeoutMs int32

	// ProtocolType is the protocol type (e.g., "consumer")
	ProtocolType string

	// Protocols are the supported assignment protocols
	Protocols []GroupProtocol

	// Assignment is the current partition assignment
	Assignment []byte

	// LastHeartbeat is the time of last heartbeat
	LastHeartbeat time.Time
}

// GroupProtocol represents a group protocol
type GroupProtocol struct {
	Name     string
	Metadata []byte
}

// ConsumerGroup represents a consumer group
type ConsumerGroup struct {
	mu sync.RWMutex

	// GroupID is the group identifier
	GroupID string

	// State is the current group state
	State ConsumerGroupState

	// Generation is the current generation ID
	Generation int32

	// ProtocolType is the protocol type
	ProtocolType string

	// Protocol is the selected protocol
	Protocol string

	// LeaderID is the ID of the group leader
	LeaderID string

	// Members in the group
	Members map[string]*GroupMember

	// Committed offsets: topic -> partition -> offset
	Offsets map[string]map[int32]*OffsetAndMetadata
}

// OffsetAndMetadata holds committed offset and metadata
type OffsetAndMetadata struct {
	Offset          int64
	Metadata        string
	CommitTimestamp int64
}

// GroupManager manages consumer groups
type GroupManager struct {
	mu sync.RWMutex

	// Groups keyed by group ID
	groups map[string]*ConsumerGroup
}

// NewGroupManager creates a new group manager
func NewGroupManager() *GroupManager {
	return &GroupManager{
		groups: make(map[string]*ConsumerGroup),
	}
}

// GetOrCreateGroup gets or creates a consumer group
func (gm *GroupManager) GetOrCreateGroup(groupID string) *ConsumerGroup {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if group, exists := gm.groups[groupID]; exists {
		return group
	}

	group := &ConsumerGroup{
		GroupID:    groupID,
		State:      GroupStateEmpty,
		Generation: 0,
		Members:    make(map[string]*GroupMember),
		Offsets:    make(map[string]map[int32]*OffsetAndMetadata),
	}
	gm.groups[groupID] = group

	return group
}

// GetGroup gets a consumer group
func (gm *GroupManager) GetGroup(groupID string) *ConsumerGroup {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	return gm.groups[groupID]
}

// DeleteGroup deletes a consumer group
func (gm *GroupManager) DeleteGroup(groupID string) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	delete(gm.groups, groupID)
}

// ListGroups returns all group IDs
func (gm *GroupManager) ListGroups() []string {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	groups := make([]string, 0, len(gm.groups))
	for id := range gm.groups {
		groups = append(groups, id)
	}
	return groups
}

// JoinGroup handles a member joining a group
func (g *ConsumerGroup) JoinGroup(memberID, clientID, clientHost, protocolType string,
	sessionTimeoutMs, rebalanceTimeoutMs int32, protocols []GroupProtocol) (string, int32, string, string, []GroupMember, error) {

	g.mu.Lock()
	defer g.mu.Unlock()

	// Generate member ID if not provided
	if memberID == "" {
		memberID = generateMemberID(clientID)
	}

	// Check protocol type
	if g.ProtocolType != "" && g.ProtocolType != protocolType {
		return "", 0, "", "", nil, ErrInconsistentProtocol
	}

	// Add or update member
	member := &GroupMember{
		MemberID:           memberID,
		ClientID:           clientID,
		ClientHost:         clientHost,
		SessionTimeoutMs:   sessionTimeoutMs,
		RebalanceTimeoutMs: rebalanceTimeoutMs,
		ProtocolType:       protocolType,
		Protocols:          protocols,
		LastHeartbeat:      time.Now(),
	}
	g.Members[memberID] = member

	// Update group state
	if g.State == GroupStateEmpty || g.State == GroupStateStable {
		g.State = GroupStatePreparingRebalance
	}

	// Set protocol type
	if g.ProtocolType == "" {
		g.ProtocolType = protocolType
	}

	// Select protocol (first common protocol)
	if g.Protocol == "" && len(protocols) > 0 {
		g.Protocol = protocols[0].Name
	}

	// If only member, it becomes leader
	if len(g.Members) == 1 {
		g.LeaderID = memberID
	}

	// Increment generation
	g.Generation++
	g.State = GroupStateCompletingRebalance

	// Return members to leader only
	var members []GroupMember
	if memberID == g.LeaderID {
		for _, m := range g.Members {
			members = append(members, *m)
		}
	}

	return memberID, g.Generation, g.Protocol, g.LeaderID, members, nil
}

// SyncGroup handles sync group request
func (g *ConsumerGroup) SyncGroup(memberID string, generation int32, assignments map[string][]byte) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Verify member exists
	member, exists := g.Members[memberID]
	if !exists {
		return nil, ErrUnknownMember
	}

	// Verify generation
	if generation != g.Generation {
		return nil, ErrIllegalGeneration
	}

	// Leader provides assignments
	if memberID == g.LeaderID && len(assignments) > 0 {
		for mid, assignment := range assignments {
			if m, exists := g.Members[mid]; exists {
				m.Assignment = assignment
			}
		}
		g.State = GroupStateStable
	}

	// Update heartbeat
	member.LastHeartbeat = time.Now()

	return member.Assignment, nil
}

// Heartbeat handles heartbeat request
func (g *ConsumerGroup) Heartbeat(memberID string, generation int32) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	member, exists := g.Members[memberID]
	if !exists {
		return ErrUnknownMember
	}

	if generation != g.Generation {
		return ErrIllegalGeneration
	}

	member.LastHeartbeat = time.Now()

	if g.State == GroupStatePreparingRebalance {
		return ErrRebalanceInProgress
	}

	return nil
}

// LeaveGroup handles a member leaving the group
func (g *ConsumerGroup) LeaveGroup(memberID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.Members[memberID]; !exists {
		return ErrUnknownMember
	}

	delete(g.Members, memberID)

	// If leader left, trigger rebalance
	if memberID == g.LeaderID {
		g.LeaderID = ""
		if len(g.Members) > 0 {
			// Pick new leader
			for id := range g.Members {
				g.LeaderID = id
				break
			}
			g.State = GroupStatePreparingRebalance
		} else {
			g.State = GroupStateEmpty
		}
	} else if len(g.Members) > 0 {
		g.State = GroupStatePreparingRebalance
	} else {
		g.State = GroupStateEmpty
	}

	return nil
}

// CommitOffset commits an offset for a topic-partition
func (g *ConsumerGroup) CommitOffset(topic string, partition int32, offset int64, metadata string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.Offsets[topic] == nil {
		g.Offsets[topic] = make(map[int32]*OffsetAndMetadata)
	}

	g.Offsets[topic][partition] = &OffsetAndMetadata{
		Offset:          offset,
		Metadata:        metadata,
		CommitTimestamp: time.Now().UnixMilli(),
	}
}

// FetchOffset fetches committed offset for a topic-partition
func (g *ConsumerGroup) FetchOffset(topic string, partition int32) (int64, string, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.Offsets[topic] == nil {
		return -1, "", false
	}

	om, exists := g.Offsets[topic][partition]
	if !exists {
		return -1, "", false
	}

	return om.Offset, om.Metadata, true
}

// generateMemberID generates a unique member ID
func generateMemberID(clientID string) string {
	return clientID + "-" + time.Now().Format("20060102150405.000")
}

// GroupInfo contains group information
type GroupInfo struct {
	GroupID      string
	State        ConsumerGroupState
	ProtocolType string
	Protocol     string
}

// Describe returns group information
func (g *ConsumerGroup) Describe() *GroupInfo {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return &GroupInfo{
		GroupID:      g.GroupID,
		State:        g.State,
		ProtocolType: g.ProtocolType,
		Protocol:     g.Protocol,
	}
}
