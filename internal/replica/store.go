package replica

import (
	"go-mil/internal/model"
	"sync"
)

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

// TxNode represents a node in the transaction history
type TxNode struct {
	Tx   *model.Transaction
	Next *TxNode
}

// Store represents the Key-Value map with MVCC support
type Store struct {
	mu   sync.RWMutex
	data map[string]*headLock

	historyMu sync.Mutex
	history   *TxNode
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

// GC deletes all value nodes whose Cts <= ts, except head nodes.
func (s *Store) GC(ts uint64) {
	s.mu.RLock()
	locks := make([]*headLock, 0, len(s.data))
	for _, hl := range s.data {
		locks = append(locks, hl)
	}
	s.mu.RUnlock()

	for _, hl := range locks {
		hl.mu.Lock()
		if hl.head != nil {
			curr := hl.head
			// Iterate to find the cut-off point
			// Since list is sorted descending by Cts, once we find a node <= ts,
			// all following nodes are also <= ts.
			// We never delete head, so we start checking from head.Next.
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
	defer s.historyMu.Unlock()

	if s.history != nil {
		curr := s.history
		// Apply same GC logic as value nodes: preserve head (or first valid), cut off rest
		for curr.Next != nil {
			if curr.Next.Tx.Cts <= ts {
				curr.Next = nil
				break
			}
			curr = curr.Next
		}
	}
}

// AddTx adds a transaction to the history, maintaining order by Cts descending
func (s *Store) AddTx(tx *model.Transaction) {
	s.historyMu.Lock()
	defer s.historyMu.Unlock()

	newNode := &TxNode{
		Tx: tx,
	}

	if s.history == nil || tx.Cts > s.history.Tx.Cts {
		newNode.Next = s.history
		s.history = newNode
		return
	}

	curr := s.history
	for curr.Next != nil && curr.Next.Tx.Cts > tx.Cts {
		curr = curr.Next
	}
	newNode.Next = curr.Next
	curr.Next = newNode
}

// GetTx returns the transaction with the given cts
func (s *Store) GetTx(cts uint64) *model.Transaction {
	s.historyMu.Lock()
	defer s.historyMu.Unlock()

	curr := s.history
	for curr != nil {
		if curr.Tx.Cts == cts {
			return curr.Tx
		}
		if curr.Tx.Cts < cts {
			return nil
		}
		curr = curr.Next
	}
	return nil
}
