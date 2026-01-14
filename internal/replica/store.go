package replica

import "sync"

// ValNode represents a version in the MVCC chain
type ValNode struct {
	Value int      // The value of this version
	Cts   uint64   // Commit timestamp
	Next  *ValNode // Pointer to the next (older) version
}

// headLock wraps the head of the chain and a mutex for fine-grained locking
type headLock struct {
	mu   sync.Mutex
	head *ValNode
}

// Store represents the Key-Value map with MVCC support
type Store struct {
	mu   sync.RWMutex
	data map[string]*headLock
}

// NewStore creates a new Store instance
func NewStore() *Store {
	return &Store{
		data: make(map[string]*headLock),
	}
}

// getOrCreateLock returns the lock for a specific key, creating it if necessary
func (s *Store) getOrCreateLock(key string) *headLock {
	s.mu.RLock()
	hl, exists := s.data[key]
	s.mu.RUnlock()
	if exists {
		return hl
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Double check
	if hl, exists = s.data[key]; exists {
		return hl
	}
	hl = &headLock{}
	s.data[key] = hl
	return hl
}

// Put inserts a new version ensuring the list is sorted by Cts descending
func (s *Store) Put(key string, val int, cts uint64) {
	hl := s.getOrCreateLock(key)

	hl.mu.Lock()
	defer hl.mu.Unlock()

	newNode := &ValNode{
		Value: val,
		Cts:   cts,
	}

	// If list is empty or new node has the largest timestamp, insert at head
	if hl.head == nil || cts > hl.head.Cts {
		newNode.Next = hl.head
		hl.head = newNode
		return
	}

	// Traverse to find insertion point
	current := hl.head
	for current.Next != nil && current.Next.Cts > cts {
		current = current.Next
	}

	// Insert newNode after current
	newNode.Next = current.Next
	current.Next = newNode
}

// BatchPut inserts multiple key-value pairs with the same commit timestamp
func (s *Store) BatchPut(kvs map[string]int, cts uint64) {
	for k, v := range kvs {
		s.Put(k, v, cts)
	}
}

// Get returns the version visible at sts for a key
func (s *Store) Get(key string, sts uint64) *ValNode {
	s.mu.RLock()
	hl, exists := s.data[key]
	s.mu.RUnlock()

	if !exists {
		return nil
	}

	hl.mu.Lock()
	defer hl.mu.Unlock()

	current := hl.head
	for current != nil {
		if current.Cts <= sts {
			return current
		}
		current = current.Next
	}
	return nil
}
