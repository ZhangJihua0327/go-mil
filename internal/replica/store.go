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

// DataStore manages key-value data with MVCC support
type DataStore struct {
	mu   sync.RWMutex
	data map[string]*headLock
}

// HistoryStore manages received and applied transactions
type HistoryStore struct {
	mu      sync.RWMutex
	history []*model.Transaction
	deps    *model.Deps
}

// PendingStore manages transactions that have been received but not yet applied
type PendingStore struct {
	mu         sync.Mutex
	pendingTxs map[uint64]*model.Transaction // key: cts
}

// Store represents the Key-Value map with MVCC support
// It combines DataStore, HistoryStore, and PendingStore
type Store struct {
	data    *DataStore
	history *HistoryStore
	pending *PendingStore
}

// NewDataStore creates a new DataStore instance
func NewDataStore() *DataStore {
	return &DataStore{
		data: make(map[string]*headLock),
	}
}

// NewHistoryStore creates a new HistoryStore instance
func NewHistoryStore() *HistoryStore {
	return &HistoryStore{
		history: make([]*model.Transaction, 0),
		deps:    model.NewDeps(),
	}
}

// NewPendingStore creates a new PendingStore instance
func NewPendingStore() *PendingStore {
	return &PendingStore{
		pendingTxs: make(map[uint64]*model.Transaction),
	}
}

// NewStore creates a new Store instance
func NewStore() *Store {
	return &Store{
		data:    NewDataStore(),
		history: NewHistoryStore(),
		pending: NewPendingStore(),
	}
}

// Add adds a transaction to the pending cache
func (ps *PendingStore) Add(tx *model.Transaction) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.pendingTxs[tx.Cts] = tx
}

// Get retrieves a transaction from the pending cache by cts
func (ps *PendingStore) Get(cts uint64) *model.Transaction {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.pendingTxs[cts]
}

// Remove removes a transaction from the pending cache
func (ps *PendingStore) Remove(cts uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.pendingTxs, cts)
}

// AddPendingTx adds a transaction to the pending cache
func (s *Store) AddPendingTx(tx *model.Transaction) {
	s.pending.Add(tx)
}

// GetPendingTx retrieves a transaction from the pending cache by cts
func (s *Store) GetPendingTx(cts uint64) *model.Transaction {
	return s.pending.Get(cts)
}

// RemovePendingTx removes a transaction from the pending cache
func (s *Store) RemovePendingTx(cts uint64) {
	s.pending.Remove(cts)
}

// getOrCreateLock returns the lock for a specific key, creating it if necessary
func (ds *DataStore) getOrCreateLock(key string) *headLock {
	ds.mu.RLock()
	hl, exists := ds.data[key]
	ds.mu.RUnlock()
	if exists {
		return hl
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()
	// Double check
	if hl, exists = ds.data[key]; exists {
		return hl
	}
	hl = &headLock{}
	ds.data[key] = hl
	return hl
}

// Put inserts a new version ensuring the list is sorted by Cts descending
func (ds *DataStore) Put(key string, val int64, cts uint64) {
	hl := ds.getOrCreateLock(key)

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
func (ds *DataStore) BatchPut(kvs map[string]int64, cts uint64) {
	for k, v := range kvs {
		ds.Put(k, v, cts)
	}
}

// Get returns the version visible at sts for a key
func (ds *DataStore) Get(key string, sts uint64) *ValNode {
	ds.mu.RLock()
	hl, exists := ds.data[key]
	ds.mu.RUnlock()

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
func (ds *DataStore) GC(ts uint64) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	for _, hl := range ds.data {
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
}

// GC removes transactions from history whose Cts <= ts, keeping at least the latest transaction
func (hs *HistoryStore) GC(ts uint64) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	if len(hs.history) > 0 {
		// Find the index of the first transaction that has Cts > ts.
		idx, found := slices.BinarySearchFunc(hs.history, ts, func(t *model.Transaction, target uint64) int {
			return cmp.Compare(t.Cts, target)
		})

		splitIdx := idx
		if found {
			splitIdx++
		}

		// To mimic the original behavior of keeping at least the latest transaction:
		if splitIdx == len(hs.history) {
			splitIdx = len(hs.history) - 1
		}

		if splitIdx > 0 {
			hs.history = hs.history[splitIdx:]
		}
	}
}

// AppendTx adds a transaction to the history and updates deps.
func (hs *HistoryStore) AppendTx(tx *model.Transaction) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	// Insert into history while maintaining ascending order by Cts
	idx, _ := slices.BinarySearchFunc(hs.history, tx.Cts, func(t *model.Transaction, target uint64) int {
		return cmp.Compare(t.Cts, target)
	})
	hs.history = slices.Insert(hs.history, idx, tx)

	// Update deps
	if hs.deps == nil {
		hs.deps = model.NewDeps()
	}
	hs.deps.Add(tx.Cts)
}

// GetTx returns the transaction with the given cts
func (hs *HistoryStore) GetTx(cts uint64) *model.Transaction {
	hs.mu.RLock()
	defer hs.mu.RUnlock()

	received := hs.deps.IsReceived(cts)
	if !received {
		return nil
	}

	idx, found := slices.BinarySearchFunc(hs.history, cts, func(t *model.Transaction, target uint64) int {
		return cmp.Compare(t.Cts, target)
	})

	if found {
		return hs.history[idx]
	}
	return nil
}

// Put inserts a new version ensuring the list is sorted by Cts descending
func (s *Store) Put(key string, val int64, cts uint64) {
	s.data.Put(key, val, cts)
}

// BatchPut inserts multiple key-value pairs with the same commit timestamp
func (s *Store) BatchPut(kvs map[string]int64, cts uint64) {
	s.data.BatchPut(kvs, cts)
}

// Get returns the version visible at sts for a key
func (s *Store) Get(key string, sts uint64) *ValNode {
	return s.data.Get(key, sts)
}

// GC deletes all value nodes whose Cts <= ts, except head nodes.
func (s *Store) GC(ts uint64) {
	s.data.GC(ts)
	s.history.GC(ts)
}

// AppendTx adds a transaction to the history and updates deps.
func (s *Store) AppendTx(tx *model.Transaction) {
	s.history.AppendTx(tx)

	cts := tx.Cts
	for _, op := range tx.Operations {
		// if op is write-op
		// Type assertion on interface for Java-like instanceof/casting
		if writeOp, ok := op.(*model.WriteOperation); ok {
			s.data.Put(writeOp.Key, writeOp.Value, cts)
		}
	}
}

// GetTx returns the transaction with the given cts
func (s *Store) GetTx(cts uint64) *model.Transaction {
	return s.history.GetTx(cts)
}
