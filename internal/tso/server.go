package tso

import (
	"context"
	pb "go-mil/proto/tso"
	"log"
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
	log.Printf("[TSO] Tick: current=%d", val)
	return &pb.TickResponse{Timestamp: val}, nil
}

func (s *Server) Tock(ctx context.Context, _ *pb.TockRequest) (*pb.TockResponse, error) {
	val := atomic.AddInt64(&s.current, 1)
	log.Printf("[TSO] Tock: new=%d", val)
	return &pb.TockResponse{Timestamp: val}, nil
}

func (s *Server) AcquireLock(_ context.Context, req *pb.AcquireLockRequest) (*pb.AcquireLockResponse, error) {
	log.Printf("[TSO] AcquireLock Request: key=%s owner=%s ttl=%dms", req.Key, req.OwnerId, req.TtlMs)
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	if entry, exists := s.locks[req.Key]; exists {
		if now.Before(entry.expiresAt) && entry.ownerID != req.OwnerId {
			log.Printf("[TSO] AcquireLock FAILED: key=%s owned by %s", req.Key, entry.ownerID)
			return &pb.AcquireLockResponse{Success: false}, nil
		}
	}

	ttl := time.Duration(req.TtlMs) * time.Millisecond
	if req.TtlMs <= 0 {
		ttl = 10 * time.Second // Default TTL
	}
	s.locks[req.Key] = lockEntry{
		ownerID:   req.OwnerId,
		expiresAt: now.Add(ttl),
	}

	log.Printf("[TSO] AcquireLock SUCCESS: key=%s owner=%s", req.Key, req.OwnerId)
	return &pb.AcquireLockResponse{Success: true}, nil
}

func (s *Server) ReleaseLock(_ context.Context, req *pb.ReleaseLockRequest) (*pb.ReleaseLockResponse, error) {
	log.Printf("[TSO] ReleaseLock Request: key=%s owner=%s", req.Key, req.OwnerId)
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, exists := s.locks[req.Key]; exists {
		if entry.ownerID == req.OwnerId {
			delete(s.locks, req.Key)
		}
	}
	log.Printf("[TSO] ReleaseLock Response: key=%s success=true", req.Key)
	return &pb.ReleaseLockResponse{Success: true}, nil
}

func (s *Server) ReleaseAllLocks(_ context.Context, req *pb.ReleaseAllLocksRequest) (*pb.ReleaseAllLocksResponse, error) {
	log.Printf("[TSO] ReleaseAllLocks Request: owner=%s", req.OwnerId)
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for key, entry := range s.locks {
		if entry.ownerID == req.OwnerId {
			delete(s.locks, key)
			count++
		}
	}
	log.Printf("[TSO] ReleaseAllLocks Response: owner=%s released=%d", req.OwnerId, count)
	return &pb.ReleaseAllLocksResponse{Success: true}, nil
}
