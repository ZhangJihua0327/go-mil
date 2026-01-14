package tso

import (
	"context"
	pb "go-mil/proto/tso"
	"sync"
	"sync/atomic"
	"time"
)

type lockEntry struct {
	ownerID   string
	expiresAt time.Time
}

// Server TSO (Timestamp Oracle) provides monotonically increasing timestamps
// for the Arbitration Relation (AR)
type Server struct {
	pb.UnimplementedTSOServer
	current int64

	mu    sync.Mutex
	locks map[string]lockEntry
}

// NewTsoServer NewTSO creates a new Timestamp Oracle
// Initializes counter to 0
func NewTsoServer() *Server {
	return &Server{
		current: 0,
		locks:   make(map[string]lockEntry),
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

func (s *Server) BatchLock(_ context.Context, req *pb.BatchLockRequest) (*pb.BatchLockResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var failedKeys []string

	// Check if any key is already locked by another owner
	for _, key := range req.Keys {
		if entry, exists := s.locks[key]; exists {
			// If lock exists
			if now.Before(entry.expiresAt) {
				// And not expired
				if entry.ownerID != req.OwnerId {
					// And held by someone else
					failedKeys = append(failedKeys, key)
				}
			}
		}
	}

	// Atomic batch acquisition: if any fail, none are acquired
	if len(failedKeys) > 0 {
		return &pb.BatchLockResponse{
			Success:    false,
			FailedKeys: failedKeys,
		}, nil
	}

	// Acquire all
	ttl := time.Duration(req.TtlMs) * time.Millisecond
	if req.TtlMs <= 0 {
		ttl = 10 * time.Second // Default TTL
	}
	expiry := now.Add(ttl)

	for _, key := range req.Keys {
		s.locks[key] = lockEntry{
			ownerID:   req.OwnerId,
			expiresAt: expiry,
		}
	}

	return &pb.BatchLockResponse{Success: true}, nil
}

func (s *Server) BatchUnlock(_ context.Context, req *pb.BatchUnlockRequest) (*pb.BatchUnlockResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range req.Keys {
		if entry, exists := s.locks[key]; exists {
			// Only unlock if we own it
			// Note: Strict ownership check prevents deleting others' locks if ours expired and someone else took it.
			if entry.ownerID == req.OwnerId {
				delete(s.locks, key)
			}
		}
	}

	return &pb.BatchUnlockResponse{Success: true}, nil
}
