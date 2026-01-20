package tso

import (
	"context"
	pb "go-mil/proto/tso"
	"testing"
	"time"
)

func TestTickTock(t *testing.T) {
	s := NewTsoServer()
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

func TestBatchLock(t *testing.T) {
	s := NewTsoServer()
	ctx := context.Background()

	keys := []string{"key1", "key2"}
	owner1 := "tx1_replica1"
	owner2 := "tx2_replica2"

	// 1. Successful lock
	resp, err := s.BatchLock(ctx, &pb.BatchLockRequest{
		Keys:    keys,
		OwnerId: owner1,
		TtlMs:   100,
	})
	if err != nil {
		t.Fatalf("BatchLock failed: %v", err)
	}
	if !resp.Success {
		t.Errorf("Expected lock success, got failed keys: %v", resp.FailedKeys)
	}

	// 2. Conflict lock from another owner
	resp, err = s.BatchLock(ctx, &pb.BatchLockRequest{
		Keys:    []string{"key2", "key3"},
		OwnerId: owner2,
		TtlMs:   100,
	})
	if err != nil {
		t.Fatalf("BatchLock failed: %v", err)
	}
	if resp.Success {
		t.Errorf("Expected lock failure due to conflict on key2")
	}
	if len(resp.FailedKeys) != 1 || resp.FailedKeys[0] != "key2" {
		t.Errorf("Expected failed key 'key2', got %v", resp.FailedKeys)
	}

	// 3. Same owner re-acquiring/extending should succeed
	resp, err = s.BatchLock(ctx, &pb.BatchLockRequest{
		Keys:    keys,
		OwnerId: owner1,
		TtlMs:   200,
	})
	if err != nil {
		t.Fatalf("BatchLock failed: %v", err)
	}
	if !resp.Success {
		t.Errorf("Expected lock extension success for same owner")
	}

	// 4. Wait for TTL to expire
	time.Sleep(250 * time.Millisecond)

	// 5. Another owner should now succeed
	resp, err = s.BatchLock(ctx, &pb.BatchLockRequest{
		Keys:    keys,
		OwnerId: owner2,
		TtlMs:   100,
	})
	if err != nil {
		t.Fatalf("BatchLock failed: %v", err)
	}
	if !resp.Success {
		t.Errorf("Expected lock success after TTL expiration")
	}
}

func TestBatchUnlock(t *testing.T) {
	s := NewTsoServer()
	ctx := context.Background()

	keys := []string{"key1", "key2"}
	owner1 := "tx1_replica1"
	owner2 := "tx2_replica2"

	// Lock keys
	_, _ = s.BatchLock(ctx, &pb.BatchLockRequest{
		Keys:    keys,
		OwnerId: owner1,
		TtlMs:   1000,
	})

	// 1. Wrong owner tries to unlock - should NOT error but also NOT unlock
	resp, err := s.BatchUnlock(ctx, &pb.BatchUnlockRequest{
		Keys:    []string{"key1"},
		OwnerId: owner2,
	})
	if err != nil {
		t.Fatalf("BatchUnlock failed: %v", err)
	}
	if !resp.Success {
		t.Errorf("BatchUnlock response should be success even if no keys were unlocked by this owner")
	}

	// Verify key1 is still locked by owner1 (try to lock by owner2)
	respL, _ := s.BatchLock(ctx, &pb.BatchLockRequest{
		Keys:    []string{"key1"},
		OwnerId: owner2,
		TtlMs:   100,
	})
	if respL.Success {
		t.Errorf("Key1 should still be locked by owner1")
	}

	// 2. Correct owner unlocks
	resp, err = s.BatchUnlock(ctx, &pb.BatchUnlockRequest{
		Keys:    []string{"key1"},
		OwnerId: owner1,
	})
	if err != nil {
		t.Fatalf("BatchUnlock failed: %v", err)
	}
	if !resp.Success {
		t.Errorf("Expected unlock success")
	}

	// Verify key1 is now free
	respL, _ = s.BatchLock(ctx, &pb.BatchLockRequest{
		Keys:    []string{"key1"},
		OwnerId: owner2,
		TtlMs:   100,
	})
	if !respL.Success {
		t.Errorf("Key1 should be free for owner2 after owner1 unlocked it")
	}
}
