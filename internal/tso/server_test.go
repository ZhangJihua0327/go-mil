package tso

import (
	"context"
	"go-mil/internal/config"
	pb "go-mil/proto/tso"
	"testing"
)

func TestTickTock(t *testing.T) {
	cfg := &config.TSOConfig{Central: true}
	s := NewTsoServer(cfg)
	ctx := context.Background()

	// Initial tick should be 0
	resp, err := s.Tick(ctx, &pb.TickRequest{})
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}
	if resp.Timestamp != 0 {
		t.Errorf("Expected initial timestamp 0, got %d", resp.Timestamp)
	}

	// Tock should increment to 1
	resp2, err := s.Tock(ctx, &pb.TockRequest{})
	if err != nil {
		t.Fatalf("Tock failed: %v", err)
	}
	if resp2.Timestamp != 1 {
		t.Errorf("Expected timestamp 1 after Tock, got %d", resp2.Timestamp)
	}

	// Another tick should still be 1
	resp3, err := s.Tick(ctx, &pb.TickRequest{})
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}
	if resp3.Timestamp != 1 {
		t.Errorf("Expected timestamp 1 after Tock, got %d", resp3.Timestamp)
	}

	// Multiple Tocks
	for i := 2; i <= 10; i++ {
		resp, err := s.Tock(ctx, &pb.TockRequest{})
		if err != nil {
			t.Fatalf("Tock failed at %d: %v", i, err)
		}
		if resp.Timestamp != int64(i) {
			t.Errorf("Expected timestamp %d, got %d", i, resp.Timestamp)
		}
	}
}

func TestConcurrentTock(t *testing.T) {
	cfg := &config.TSOConfig{Central: true}
	s := NewTsoServer(cfg)
	ctx := context.Background()
	n := 1000
	done := make(chan bool)

	for i := 0; i < n; i++ {
		go func() {
			_, err := s.Tock(ctx, &pb.TockRequest{})
			if err != nil {
				t.Errorf("Concurrent Tock failed: %v", err)
			}
			done <- true
		}()
	}

	for i := 0; i < n; i++ {
		<-done
	}

	resp, err := s.Tick(ctx, &pb.TickRequest{})
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}
	if resp.Timestamp != int64(n) {
		t.Errorf("Expected timestamp %d after %d concurrent Tocks, got %d", n, n, resp.Timestamp)
	}
}

func TestReleaseAllLocks(t *testing.T) {
	cfg := &config.TSOConfig{Central: true}
	s := NewTsoServer(cfg)
	ctx := context.Background()

	owner1 := "tx1_replica1"
	owner2 := "tx2_replica2"

	// 1. owner1 acquires multiple locks
	s.AcquireLock(ctx, &pb.AcquireLockRequest{Key: "key1", OwnerId: owner1, TtlMs: 1000})
	s.AcquireLock(ctx, &pb.AcquireLockRequest{Key: "key2", OwnerId: owner1, TtlMs: 1000})

	// 2. owner2 acquires a lock
	s.AcquireLock(ctx, &pb.AcquireLockRequest{Key: "key3", OwnerId: owner2, TtlMs: 1000})

	// 3. Release all locks for owner1
	resp, err := s.ReleaseAllLocks(ctx, &pb.ReleaseAllLocksRequest{OwnerId: owner1})
	if err != nil {
		t.Fatalf("ReleaseAllLocks failed: %v", err)
	}
	if !resp.Success {
		t.Errorf("Expected success, got false")
	}

	// 4. Verify owner1's locks are free (owner2 can take them)
	respL, _ := s.AcquireLock(ctx, &pb.AcquireLockRequest{Key: "key1", OwnerId: owner2, TtlMs: 100})
	if !respL.Success {
		t.Errorf("key1 should be free for owner2")
	}

	// 5. Verify owner2's lock is still held
	respL, _ = s.AcquireLock(ctx, &pb.AcquireLockRequest{Key: "key3", OwnerId: owner1, TtlMs: 100})
	if respL.Success {
		t.Errorf("key3 should still be held by owner2")
	}
}
