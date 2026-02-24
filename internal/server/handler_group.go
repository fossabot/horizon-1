package server

import (
	"horizon/internal/broker"
	"horizon/internal/protocol"
)

// handleFindCoordinator handles FindCoordinator request
func (h *RequestHandler) handleFindCoordinator(req *Request) *Response {
	resp := NewResponse(req.CorrelationID)
	w := resp.Writer

	// Read key (group ID or transaction ID)
	_, _ = req.Reader.ReadString()

	// Throttle time (v1+)
	if req.ApiVersion >= 1 {
		w.WriteInt32(0)
	}

	// Error code
	w.WriteInt16(int16(protocol.ErrNone))

	// Error message (v1+)
	if req.ApiVersion >= 1 {
		w.WriteNullableString(nil)
	}

	// Coordinator info (this broker)
	meta, _ := h.broker.GetMetadata(nil)
	if len(meta.Brokers) > 0 {
		b := meta.Brokers[0]
		w.WriteInt32(b.NodeID)
		w.WriteString(b.Host)
		w.WriteInt32(b.Port)
	} else {
		w.WriteInt32(0)
		w.WriteString("localhost")
		w.WriteInt32(9092)
	}

	return resp
}

// handleJoinGroup handles JoinGroup request
func (h *RequestHandler) handleJoinGroup(req *Request) *Response {
	resp := NewResponse(req.CorrelationID)
	w := resp.Writer

	// Read request
	groupID, _ := req.Reader.ReadString()
	sessionTimeoutMs, _ := req.Reader.ReadInt32()
	var rebalanceTimeoutMs int32 = sessionTimeoutMs
	if req.ApiVersion >= 1 {
		rebalanceTimeoutMs, _ = req.Reader.ReadInt32()
	}
	memberID, _ := req.Reader.ReadString()
	if req.ApiVersion >= 5 {
		_, _ = req.Reader.ReadNullableString() // group_instance_id
	}
	protocolType, _ := req.Reader.ReadString()

	// Read protocols
	protocolCount, _ := req.Reader.ReadArrayLen()
	var protocols []broker.GroupProtocol
	for i := int32(0); i < protocolCount; i++ {
		name, _ := req.Reader.ReadString()
		metadata, _ := req.Reader.ReadBytes()
		protocols = append(protocols, broker.GroupProtocol{
			Name:     name,
			Metadata: metadata,
		})
	}

	// Join group
	group := h.broker.GetGroupManager().GetOrCreateGroup(groupID)
	newMemberID, generation, selectedProtocol, leaderID, members, err := group.JoinGroup(
		memberID, req.ClientID, "", protocolType,
		sessionTimeoutMs, rebalanceTimeoutMs, protocols,
	)

	// Throttle time (v2+)
	if req.ApiVersion >= 2 {
		w.WriteInt32(0)
	}

	// Error code
	if err != nil {
		w.WriteInt16(int16(protocol.ErrUnknown))
		w.WriteInt32(0)               // generation
		if req.ApiVersion >= 7 {
			w.WriteNullableString(nil)    // protocol_type
		}
		w.WriteString("")             // protocol
		w.WriteString("")             // leader
		w.WriteString(newMemberID)    // member_id
		w.WriteArrayLen(0)            // members
		return resp
	}

	w.WriteInt16(int16(protocol.ErrNone))
	w.WriteInt32(generation)
	// Protocol type (v7+)
	if req.ApiVersion >= 7 {
		w.WriteNullableString(&protocolType)
	}
	w.WriteString(selectedProtocol)
	w.WriteString(leaderID)
	w.WriteString(newMemberID)

	// Members (only for leader)
	w.WriteArrayLen(int32(len(members)))
	for _, m := range members {
		w.WriteString(m.MemberID)
		if req.ApiVersion >= 5 {
			w.WriteNullableString(nil) // group_instance_id
		}
		w.WriteBytes(m.Protocols[0].Metadata)
	}

	return resp
}

// handleSyncGroup handles SyncGroup request
func (h *RequestHandler) handleSyncGroup(req *Request) *Response {
	resp := NewResponse(req.CorrelationID)
	w := resp.Writer

	// Read request
	groupID, _ := req.Reader.ReadString()
	generation, _ := req.Reader.ReadInt32()
	memberID, _ := req.Reader.ReadString()
	if req.ApiVersion >= 3 {
		_, _ = req.Reader.ReadNullableString() // group_instance_id
	}
	if req.ApiVersion >= 5 {
		_, _ = req.Reader.ReadNullableString() // protocol_type
		_, _ = req.Reader.ReadNullableString() // protocol_name
	}

	// Read assignments
	assignmentCount, _ := req.Reader.ReadArrayLen()
	assignments := make(map[string][]byte)
	for i := int32(0); i < assignmentCount; i++ {
		mid, _ := req.Reader.ReadString()
		assignment, _ := req.Reader.ReadBytes()
		assignments[mid] = assignment
	}

	// Sync group
	group := h.broker.GetGroupManager().GetGroup(groupID)
	var assignment []byte
	var err error

	if group == nil {
		err = broker.ErrGroupNotFound
	} else {
		assignment, err = group.SyncGroup(memberID, generation, assignments)
	}

	// Throttle time (v1+)
	if req.ApiVersion >= 1 {
		w.WriteInt32(0)
	}

	// Error code
	if err != nil {
		w.WriteInt16(int16(protocol.ErrUnknown))
		if req.ApiVersion >= 5 {
			w.WriteNullableString(nil) // protocol_type
			w.WriteNullableString(nil) // protocol_name
		}
		w.WriteBytes(nil)
		return resp
	}

	w.WriteInt16(int16(protocol.ErrNone))
	if req.ApiVersion >= 5 {
		w.WriteNullableString(nil) // protocol_type
		w.WriteNullableString(nil) // protocol_name
	}
	w.WriteBytes(assignment)

	return resp
}

// handleHeartbeat handles Heartbeat request
func (h *RequestHandler) handleHeartbeat(req *Request) *Response {
	resp := NewResponse(req.CorrelationID)
	w := resp.Writer

	// Read request
	groupID, _ := req.Reader.ReadString()
	generation, _ := req.Reader.ReadInt32()
	memberID, _ := req.Reader.ReadString()

	// Process heartbeat
	group := h.broker.GetGroupManager().GetGroup(groupID)
	var err error

	if group == nil {
		err = broker.ErrGroupNotFound
	} else {
		err = group.Heartbeat(memberID, generation)
	}

	// Throttle time (v1+)
	if req.ApiVersion >= 1 {
		w.WriteInt32(0)
	}

	// Error code
	if err != nil {
		if err == broker.ErrRebalanceInProgress {
			w.WriteInt16(int16(protocol.ErrRebalanceInProgress))
		} else if err == broker.ErrUnknownMember {
			w.WriteInt16(int16(protocol.ErrUnknownMemberId))
		} else if err == broker.ErrIllegalGeneration {
			w.WriteInt16(int16(protocol.ErrIllegalGeneration))
		} else {
			w.WriteInt16(int16(protocol.ErrUnknown))
		}
	} else {
		w.WriteInt16(int16(protocol.ErrNone))
	}

	return resp
}

// handleLeaveGroup handles LeaveGroup request
func (h *RequestHandler) handleLeaveGroup(req *Request) *Response {
	resp := NewResponse(req.CorrelationID)
	w := resp.Writer

	// Read request
	groupID, _ := req.Reader.ReadString()

	group := h.broker.GetGroupManager().GetGroup(groupID)
	var groupErr error

	type memberResult struct {
		memberID  string
		errorCode protocol.ErrorCode
	}
	var memberResults []memberResult

	if req.ApiVersion >= 3 {
		// v3+: batch leave with members array
		memberCount, _ := req.Reader.ReadArrayLen()
		for i := int32(0); i < memberCount; i++ {
			memberID, _ := req.Reader.ReadString()
			if req.ApiVersion >= 4 {
				_, _ = req.Reader.ReadNullableString() // group_instance_id
			}

			mr := memberResult{memberID: memberID}
			if group == nil {
				mr.errorCode = protocol.ErrUnknown
			} else if err := group.LeaveGroup(memberID); err != nil {
				mr.errorCode = protocol.ErrUnknown
			} else {
				mr.errorCode = protocol.ErrNone
			}
			memberResults = append(memberResults, mr)
		}
	} else {
		// v0-v2: single member leave
		memberID, _ := req.Reader.ReadString()
		if group == nil {
			groupErr = broker.ErrGroupNotFound
		} else {
			groupErr = group.LeaveGroup(memberID)
		}
	}

	// Throttle time (v1+)
	if req.ApiVersion >= 1 {
		w.WriteInt32(0)
	}

	// Error code
	if req.ApiVersion >= 3 {
		w.WriteInt16(int16(protocol.ErrNone))
		// Member responses (v3+)
		w.WriteArrayLen(int32(len(memberResults)))
		for _, mr := range memberResults {
			w.WriteString(mr.memberID)
			if req.ApiVersion >= 4 {
				w.WriteNullableString(nil) // group_instance_id
			}
			w.WriteInt16(int16(mr.errorCode))
		}
	} else {
		if groupErr != nil {
			w.WriteInt16(int16(protocol.ErrUnknown))
		} else {
			w.WriteInt16(int16(protocol.ErrNone))
		}
	}

	return resp
}
