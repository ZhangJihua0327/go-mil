package tso

import (
	"context"
	pb "go-mil/proto/tso"
	"sync/atomic"
)

// Server TSO (Timestamp Oracle) provides monotonically increasing timestamps
// for the Arbitration Relation (AR)
type Server struct {
	pb.UnimplementedTSOServer
	current int64
}

// NewTsoServer NewTSO creates a new Timestamp Oracle
// Initializes counter to 0
func NewTsoServer() *Server {
	return &Server{
		current: 0,
	}
}

func (s *Server) Tick(ctx context.Context, _ *pb.TickRequest) (*pb.TickResponse, error) {
	val := atomic.LoadInt64(&s.current)
	return &pb.TickResponse{Timestamp: val}, nil
}

func (s *Server) Tock(ctx context.Context, _ *pb.TockRequest) (*pb.TockResponse, error) {
	val := atomic.AddInt64(&s.current, 1)
	return &pb.TockResponse{Timestamp: val}, nil
}
