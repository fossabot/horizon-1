package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"horizon/internal/protocol"
)

// Connection represents a client connection
type Connection struct {
	id      int64
	conn    net.Conn
	handler *RequestHandler
	closed  bool
	mu      sync.Mutex
}

// NewConnection creates a new connection
func NewConnection(id int64, conn net.Conn, handler *RequestHandler) *Connection {
	return &Connection{
		id:      id,
		conn:    conn,
		handler: handler,
	}
}

// Handle handles requests on the connection
func (c *Connection) Handle() {
	defer c.conn.Close()

	for {
		// Read request
		req, err := c.readRequest()
		if err != nil {
			if err != io.EOF {
				// Log error
			}
			return
		}

		// Handle request
		resp := c.handler.HandleRequest(req)

		// Write response
		if err := c.writeResponse(resp); err != nil {
			return
		}
	}
}

// readRequest reads a request from the connection
func (c *Connection) readRequest() (*Request, error) {
	// Read size (4 bytes)
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, sizeBuf); err != nil {
		return nil, err
	}
	size := int32(binary.BigEndian.Uint32(sizeBuf))

	// Read request data
	data := make([]byte, size)
	if _, err := io.ReadFull(c.conn, data); err != nil {
		return nil, err
	}

	reader := protocol.NewReader(data)

	// Parse header
	apiKey, _ := reader.ReadInt16()
	apiVersion, _ := reader.ReadInt16()
	correlationID, _ := reader.ReadInt32()
	clientID, _ := reader.ReadNullableString()

	clientIDStr := ""
	if clientID != nil {
		clientIDStr = *clientID
	}

	return &Request{
		ApiKey:        protocol.ApiKey(apiKey),
		ApiVersion:    apiVersion,
		CorrelationID: correlationID,
		ClientID:      clientIDStr,
		Reader:        reader,
	}, nil
}

// writeResponse writes a response to the connection
func (c *Connection) writeResponse(resp *Response) error {
	// Prepend size
	data := resp.Writer.Bytes()
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(data)))

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("connection closed")
	}

	if _, err := c.conn.Write(sizeBuf); err != nil {
		return err
	}
	if _, err := c.conn.Write(data); err != nil {
		return err
	}

	return nil
}

// Close closes the connection
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}
