package server

import (
	"horizon/internal/protocol"
)

// Request represents a Kafka protocol request
type Request struct {
	ApiKey        protocol.ApiKey
	ApiVersion    int16
	CorrelationID int32
	ClientID      string
	Reader        *protocol.Reader
}

// Response represents a Kafka protocol response
type Response struct {
	CorrelationID int32
	Writer        *protocol.Writer
}

// NewResponse creates a new response
func NewResponse(correlationID int32) *Response {
	w := protocol.NewWriter(1024)
	w.WriteInt32(correlationID)
	return &Response{
		CorrelationID: correlationID,
		Writer:        w,
	}
}
