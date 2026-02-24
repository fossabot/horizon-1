package server

import (
	"horizon/internal/protocol"
)

// handleOffsetCommit handles OffsetCommit request
func (h *RequestHandler) handleOffsetCommit(req *Request) *Response {
	resp := NewResponse(req.CorrelationID)
	w := resp.Writer

	// Read request
	groupID, _ := req.Reader.ReadString()
	if req.ApiVersion >= 1 {
		_, _ = req.Reader.ReadInt32()   // generation
		_, _ = req.Reader.ReadString()  // member_id
	}
	if req.ApiVersion >= 2 && req.ApiVersion <= 4 {
		_, _ = req.Reader.ReadInt64() // retention_time
	}
	if req.ApiVersion >= 7 {
		_, _ = req.Reader.ReadNullableString() // group_instance_id
	}

	topicCount, _ := req.Reader.ReadArrayLen()

	type commitPartition struct {
		partition int32
		offset    int64
		metadata  string
		errorCode protocol.ErrorCode
	}
	type commitTopic struct {
		topic      string
		partitions []commitPartition
	}

	var results []commitTopic
	group := h.broker.GetGroupManager().GetOrCreateGroup(groupID)

	for i := int32(0); i < topicCount; i++ {
		topic, _ := req.Reader.ReadString()
		partCount, _ := req.Reader.ReadArrayLen()

		ct := commitTopic{topic: topic}

		for j := int32(0); j < partCount; j++ {
			partition, _ := req.Reader.ReadInt32()
			offset, _ := req.Reader.ReadInt64()
			if req.ApiVersion >= 6 {
				_, _ = req.Reader.ReadInt32() // leader_epoch
			}
			metadata, _ := req.Reader.ReadNullableString()

			metaStr := ""
			if metadata != nil {
				metaStr = *metadata
			}

			group.CommitOffset(topic, partition, offset, metaStr)

			ct.partitions = append(ct.partitions, commitPartition{
				partition: partition,
				offset:    offset,
				metadata:  metaStr,
				errorCode: protocol.ErrNone,
			})
		}

		results = append(results, ct)
	}

	// Throttle time (v3+)
	if req.ApiVersion >= 3 {
		w.WriteInt32(0)
	}

	// Write response
	w.WriteArrayLen(int32(len(results)))
	for _, ct := range results {
		w.WriteString(ct.topic)
		w.WriteArrayLen(int32(len(ct.partitions)))
		for _, cp := range ct.partitions {
			w.WriteInt32(cp.partition)
			w.WriteInt16(int16(cp.errorCode))
		}
	}

	return resp
}

// handleOffsetFetch handles OffsetFetch request
func (h *RequestHandler) handleOffsetFetch(req *Request) *Response {
	resp := NewResponse(req.CorrelationID)
	w := resp.Writer

	// Read request
	groupID, _ := req.Reader.ReadString()
	topicCount, _ := req.Reader.ReadArrayLen()

	type fetchPartition struct {
		partition int32
		offset    int64
		metadata  string
		errorCode protocol.ErrorCode
	}
	type fetchTopic struct {
		topic      string
		partitions []fetchPartition
	}

	var results []fetchTopic
	group := h.broker.GetGroupManager().GetGroup(groupID)

	for i := int32(0); i < topicCount; i++ {
		topic, _ := req.Reader.ReadString()
		partCount, _ := req.Reader.ReadArrayLen()

		ft := fetchTopic{topic: topic}

		for j := int32(0); j < partCount; j++ {
			partition, _ := req.Reader.ReadInt32()

			fp := fetchPartition{partition: partition}

			if group != nil {
				offset, metadata, found := group.FetchOffset(topic, partition)
				if found {
					fp.offset = offset
					fp.metadata = metadata
					fp.errorCode = protocol.ErrNone
				} else {
					fp.offset = -1
					fp.errorCode = protocol.ErrNone
				}
			} else {
				fp.offset = -1
				fp.errorCode = protocol.ErrNone
			}

			ft.partitions = append(ft.partitions, fp)
		}

		results = append(results, ft)
	}

	// Throttle time (v3+)
	if req.ApiVersion >= 3 {
		w.WriteInt32(0)
	}

	// Write response
	w.WriteArrayLen(int32(len(results)))
	for _, ft := range results {
		w.WriteString(ft.topic)
		w.WriteArrayLen(int32(len(ft.partitions)))
		for _, fp := range ft.partitions {
			w.WriteInt32(fp.partition)
			w.WriteInt64(fp.offset)
			if req.ApiVersion >= 5 {
				w.WriteInt32(-1) // leader_epoch
			}
			w.WriteNullableString(&fp.metadata)
			w.WriteInt16(int16(fp.errorCode))
		}
	}

	// Error code (v2+)
	if req.ApiVersion >= 2 {
		w.WriteInt16(int16(protocol.ErrNone))
	}

	return resp
}
