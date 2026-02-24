package server

import (
	"horizon/internal/protocol"
	"horizon/internal/storage"
)

// handleProduce handles Produce request
func (h *RequestHandler) handleProduce(req *Request) *Response {
	resp := NewResponse(req.CorrelationID)
	w := resp.Writer

	// Read request
	// v3+ includes transactional_id before acks
	if req.ApiVersion >= 3 {
		_, _ = req.Reader.ReadNullableString() // transactional_id
	}
	_, _ = req.Reader.ReadInt16() // acks
	_, _ = req.Reader.ReadInt32() // timeout

	topicCount, _ := req.Reader.ReadArrayLen()

	type partitionResult struct {
		partition  int32
		errorCode  protocol.ErrorCode
		baseOffset int64
	}
	type topicResult struct {
		topic      string
		partitions []partitionResult
	}

	var results []topicResult

	for i := int32(0); i < topicCount; i++ {
		topic, _ := req.Reader.ReadString()
		partCount, _ := req.Reader.ReadArrayLen()

		tr := topicResult{topic: topic}

		for j := int32(0); j < partCount; j++ {
			partition, _ := req.Reader.ReadInt32()
			recordData, _ := req.Reader.ReadBytes()

			pr := partitionResult{partition: partition}

			// Parse and append records
			if recordData != nil {
				batch, err := storage.DecodeRecordBatch(recordData)
				if err != nil {
					pr.errorCode = protocol.ErrCorruptMessage
				} else {
					baseOffset, err := h.broker.Produce(topic, partition, batch.Records)
					if err != nil {
						pr.errorCode = protocol.ErrUnknown
					} else {
						pr.baseOffset = baseOffset
						pr.errorCode = protocol.ErrNone
					}
				}
			}

			tr.partitions = append(tr.partitions, pr)
		}

		results = append(results, tr)
	}

	// Write response
	w.WriteArrayLen(int32(len(results)))
	for _, tr := range results {
		w.WriteString(tr.topic)
		w.WriteArrayLen(int32(len(tr.partitions)))
		for _, pr := range tr.partitions {
			w.WriteInt32(pr.partition)
			w.WriteInt16(int16(pr.errorCode))
			w.WriteInt64(pr.baseOffset)
			// Log append time (v2+)
			if req.ApiVersion >= 2 {
				w.WriteInt64(-1)
			}
			// Log start offset (v5+)
			if req.ApiVersion >= 5 {
				w.WriteInt64(0)
			}
			// Record errors (v8+) - empty array
			if req.ApiVersion >= 8 {
				w.WriteArrayLen(0)
			}
			// Error message (v8+) - null
			if req.ApiVersion >= 8 {
				w.WriteNullableString(nil)
			}
		}
	}

	// Throttle time (v1+)
	if req.ApiVersion >= 1 {
		w.WriteInt32(0)
	}

	return resp
}
