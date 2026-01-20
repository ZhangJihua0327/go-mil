package replica

import (
	"cmp"
	"go-mil/internal/model"
	"slices"
	"sync"
)

// ValNode represents a version in the MVCC chain
type ValNode struct {
	Value int64    // The value of this version
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

	historyMu sync.RWMutex
	history   []*model.Transaction

	deps *model.Deps

	// pendingTxs is a cache for transactions received from other nodes but not yet applied
	pendingMu  sync.Mutex
	pendingTxs map[uint64]*model.Transaction // key: cts
}

// NewStore creates a new Store instance
func NewStore() *Store {
	return &Store{
		data:       make(map[string]*headLock),
		pendingTxs: make(map[uint64]*model.Transaction),
		deps:       model.NewDeps(),
		history:    make([]*model.Transaction, 0),
	}
}

// AddPendingTx adds a transaction to the pending cache
func (s *Store) AddPendingTx(tx *model.Transaction) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	s.pendingTxs[tx.Cts] = tx
}

// GetPendingTx retrieves a transaction from the pending cache by cts
func (s *Store) GetPendingTx(cts uint64) *model.Transaction {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	return s.pendingTxs[cts]
}

// RemovePendingTx removes a transaction from the pending cache
func (s *Store) RemovePendingTx(cts uint64) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	delete(s.pendingTxs, cts)
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
func (s *Store) Put(key string, val int64, cts uint64) {
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
func (s *Store) BatchPut(kvs map[string]int64, cts uint64) {
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

// GC deletes all value nodes whose Cts <= ts, except head nodes.
func (s *Store) GC(ts uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hl := range s.data {
		hl.mu.Lock()
		if hl.head != nil {
			curr := hl.head
			for curr.Next != nil {
				if curr.Next.Cts <= ts {
					curr.Next = nil
					break
				}
				curr = curr.Next
			}
		}
		hl.mu.Unlock()
	}

	// History GC
	s.historyMu.Lock()
	if len(s.history) > 0 {
		// Find the index of the first transaction that should be kept.
		// We keep all transactions with Cts > ts.
		// If we want to keep at least the latest one (even if <= ts) to mimic old behavior:
		idx, _ := slices.BinarySearchFunc(s.history, ts, func(t *model.Transaction, target uint64) int {
			return cmp.Compare(t.Cts, target)
		})

		// idx is where Cts would be inserted or is found.
		// Transactions from 0 to idx-1 have Cts <= ts.
		// However, old logic kept the latest one. In ascending slice, latest is at the end.
		// If all transactions are <= ts, idx will be len(s.history).
		// To keep at least one if it exists:
		if idx > 0 && idx == len(s.history) {
			idx = len(s.history) - 1
		}

		if idx > 0 {
			s.history = s.history[idx:]
		}
	}
	s.historyMu.Unlock()
}

// AppendTx adds a transaction to the history and updates deps.
func (s *Store) AppendTx(tx *model.Transaction) {
	s.historyMu.Lock()
	// Insert into history while maintaining ascending order by Cts
	idx, found := slices.BinarySearchFunc(s.history, tx.Cts, func(t *model.Transaction, target uint64) int {
		return cmp.Compare(t.Cts, target)
	})
	if !found {
		s.history = slices.Insert(s.history, idx, tx)
	}
	s.historyMu.Unlock()

	// Update deps
	s.mu.Lock()
	if s.deps == nil {
		s.deps = model.NewDeps()
	}
	s.deps.Add(tx.Cts)
	s.mu.Unlock()

	cts := tx.Cts
	for _, op := range tx.Operations {
		// if op is write-op
		// Type assertion on interface for Java-like instanceof/casting
		if writeOp, ok := op.(*model.WriteOperation); ok {
			s.Put(writeOp.Key, writeOp.Value, cts)
		}
	}
}

// GetTx returns the transaction with the given cts
func (s *Store) GetTx(cts uint64) *model.Transaction {
	s.mu.RLock()
	received := s.deps.IsReceived(cts)
	s.mu.RUnlock()

	if !received {
		return nil
	}

	s.historyMu.RLock()
	defer s.historyMu.RUnlock()

	idx, found := slices.BinarySearchFunc(s.history, cts, func(t *model.Transaction, target uint64) int {
		return cmp.Compare(t.Cts, target)
	})

	if found {
		return s.history[idx]
	}
	return nil
}
