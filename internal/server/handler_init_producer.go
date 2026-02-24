package server

import (
	"horizon/internal/protocol"
	"sync/atomic"
)

// producerIdCounter is a global atomic counter for generating producer IDs
var producerIdCounter atomic.Int64

func init() {
	producerIdCounter.Store(1000)
}

// handleInitProducerId handles InitProducerId request (API key 22)
func (h *RequestHandler) handleInitProducerId(req *Request) *Response {
	resp := NewResponse(req.CorrelationID)
	w := resp.Writer

	// Read request (fields are not used, but must be consumed)
	// v0+: transactional_id (nullable_string), transaction_timeout_ms (int32)
	// We just skip the remaining bytes since we don't do transactions

	// Throttle time (v0+)
	w.WriteInt32(0)

	// Error code
	w.WriteInt16(int16(protocol.ErrNone))

	// Producer ID (monotonically increasing)
	w.WriteInt64(producerIdCounter.Add(1))

	// Producer epoch
	w.WriteInt16(0)

	return resp
}
