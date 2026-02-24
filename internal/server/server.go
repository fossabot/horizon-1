// Package server implements the TCP server for Horizon.
// It handles Kafka protocol connections and request dispatch.
package server

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"horizon/internal/broker"
)

// Server is the TCP server
type Server struct {
	mu sync.RWMutex

	// Broker reference
	broker *broker.Broker

	// Listener
	listener net.Listener

	// Address to listen on
	addr string

	// Active connections
	connections map[int64]*Connection
	nextConnID  int64

	// Request handler
	handler *RequestHandler

	// Whether server is running
	running bool

	// Wait group for graceful shutdown
	wg sync.WaitGroup

	// Shutdown channel
	shutdownCh chan struct{}
}

// ServerConfig holds server configuration
type ServerConfig struct {
	// Address to listen on
	Addr string

	// Maximum request size
	MaxRequestSize int32
}

// DefaultServerConfig returns default server configuration
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Addr:           ":9092",
		MaxRequestSize: 100 * 1024 * 1024, // 100MB
	}
}

// NewServer creates a new server
func NewServer(b *broker.Broker, config ServerConfig) *Server {
	s := &Server{
		broker:      b,
		addr:        config.Addr,
		connections: make(map[int64]*Connection),
		shutdownCh:  make(chan struct{}),
	}
	s.handler = NewRequestHandler(b)
	return s
}

// Start starts the server
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	s.listener = listener
	s.running = true

	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// Stop stops the server
func (s *Server) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	close(s.shutdownCh)
	s.listener.Close()

	// Close all connections
	for _, conn := range s.connections {
		conn.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	return nil
}

// Addr returns the listen address
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// acceptLoop accepts incoming connections
func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdownCh:
				return
			default:
				continue
			}
		}

		s.mu.Lock()
		connID := atomic.AddInt64(&s.nextConnID, 1)
		c := NewConnection(connID, conn, s.handler)
		s.connections[connID] = c
		s.mu.Unlock()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			c.Handle()

			s.mu.Lock()
			delete(s.connections, connID)
			s.mu.Unlock()
		}()
	}
}
