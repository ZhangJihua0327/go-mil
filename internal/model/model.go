package model

import "strings"

// ============================================================================
// Isolation Level - Transaction isolation levels
// ============================================================================

// IsolationLevel represents the isolation level of a transaction
// Lattice structure (Strength order):
//
//	  SER
//	   |
//	   SI
//	  /  \
//	PC    PSI
//	  \  /
//	   CC
//	   |
//	   RA
type IsolationLevel int

const (
	// RA - Read Atomic: Ensures atomic visibility of reads
	RA IsolationLevel = iota
	// CC - Causal Consistency: Maintains causal ordering of operations
	CC
	// PC - Prefix Consistency: Ensures prefix of operations are consistent
	PC
	// PSI - Parallel Snapshot Isolation: Allows parallel snapshots with conflict detection
	PSI
	// SI - Snapshot Isolation: Provides consistent snapshot view (LUB of PC and PSI)
	SI
	// SER - Serializability: Strongest isolation, equivalent to serial execution
	SER
)

// Satisfies returns true if the receiver isolation level is stronger than or equal to the required level.
func (il IsolationLevel) Satisfies(required IsolationLevel) bool {
	if il == required {
		return true
	}
	switch il {
	case SER:
		return true
	case SI:
		return required != SER
	case PC:
		return required == CC || required == RA
	case PSI:
		return required == CC || required == RA
	case CC:
		return required == RA
	case RA:
		return false
	default:
		return false
	}
}

// LeastUpperBound returns the weakest isolation level that satisfies both il and other.
func (il IsolationLevel) LeastUpperBound(other IsolationLevel) IsolationLevel {
	if il.Satisfies(other) {
		return il
	}
	if other.Satisfies(il) {
		return other
	}
	// If neither satisfies the other, they must be PC and PSI (incomparable).
	// Their LUB is SI.
	return SI
}

func (il IsolationLevel) String() string {
	switch il {
	case RA:
		return "RA"
	case CC:
		return "CC"
	case PC:
		return "PC"
	case PSI:
		return "PSI"
	case SI:
		return "SI"
	case SER:
		return "SER"
	default:
		return "UNKNOWN"
	}
}

// ParseIsolationLevel parses a string to IsolationLevel
func ParseIsolationLevel(s string) IsolationLevel {
	switch strings.ToUpper(s) {
	case "RA":
		return RA
	case "CC":
		return CC
	case "PC":
		return PC
	case "PSI":
		return PSI
	case "SI":
		return SI
	case "SER":
		return SER
	default:
		return SI // Default to Snapshot Isolation
	}
}

// ============================================================================
// Operation - Transaction operations
// ============================================================================

// OperationType represents the type of an operation
type OperationType int

const (
	// OpStart represents a start operation
	OpStart OperationType = iota
	// OpRead represents a read operation
	OpRead
	// OpWrite represents a write operation
	OpWrite
	// OpPrepare represents a prepare operation
	OpPrepare
	// OpCommit represents a commit operation
	OpCommit
	// OpAbort represents an abort operation
	OpAbort
)

func (ot OperationType) String() string {
	switch ot {
	case OpStart:
		return "START"
	case OpRead:
		return "READ"
	case OpWrite:
		return "WRITE"
	case OpPrepare:
		return "PREPARE"
	case OpCommit:
		return "COMMIT"
	case OpAbort:
		return "ABORT"
	default:
		return "UNKNOWN"
	}
}

// Operation defines the interface for all transaction operations.
// This allows for polymorphism and easy type-checking (similar to Java's instanceof).
type Operation interface {
	OpType() OperationType
	GetBase() *BaseOperation
}

// BaseOperation represents common fields for all operations.
type BaseOperation struct {
}

// GetBase returns the base operation, implementing part of the Operation interface.
func (b *BaseOperation) GetBase() *BaseOperation {
	return b
}

// StartOperation represents a start operation
type StartOperation struct {
	BaseOperation
}

// OpType returns OpStart
func (o *StartOperation) OpType() OperationType { return OpStart }

// ReadOperation represents a read operation
type ReadOperation struct {
	BaseOperation
	Key        string
	ReadResult int
}

// OpType returns OpRead
func (o *ReadOperation) OpType() OperationType { return OpRead }

// WriteOperation represents a write operation
type WriteOperation struct {
	BaseOperation
	Key   string
	Value int
}

// OpType returns OpWrite
func (o *WriteOperation) OpType() OperationType { return OpWrite }

// PrepareOperation represents a prepare operation
type PrepareOperation struct {
	BaseOperation
}

// OpType returns OpPrepare
func (o *PrepareOperation) OpType() OperationType { return OpPrepare }

// CommitOperation represents a commit operation
type CommitOperation struct {
	BaseOperation
}

// OpType returns OpCommit
func (o *CommitOperation) OpType() OperationType { return OpCommit }

// AbortOperation represents an abort operation
type AbortOperation struct {
	BaseOperation
}

// OpType returns OpAbort
func (o *AbortOperation) OpType() OperationType { return OpAbort }

// Transaction represents a transaction in the system.
type Transaction struct {
	TxId           string
	Sts            uint64
	Cts            uint64
	IsolationLevel IsolationLevel
	Deps           *Deps
	Operations     []Operation
}

// AddOperation adds an operation to the transaction.
func (tx *Transaction) AddOperation(op Operation) {
	tx.Operations = append(tx.Operations, op)
}

func (tx *Transaction) WRSet() (map[string]int, map[string]int) {
	wSet := make(map[string]int)
	rSet := make(map[string]int)
	for _, op := range tx.Operations {
		switch o := op.(type) {
		case *WriteOperation:
			wSet[o.Key] = o.Value
			break
		case *ReadOperation:
			// if key in wset, skip
			if _, exists := wSet[o.Key]; exists {
				continue
			}
			rSet[o.Key] = o.ReadResult
		}
	}

	return wSet, rSet
}

// Deps represents the set of transactions visible to the current node.
type Deps struct {
	MinDep uint64              // All transactions with timestamp <= MinDep are received
	DepSet map[uint64]struct{} // Set of timestamps > MinDep that are received
}

// NewDeps returns a new initialized Deps.
func NewDeps() *Deps {
	return &Deps{
		MinDep: 0,
		DepSet: make(map[uint64]struct{}),
	}
}

func (d *Deps) MaxDeps() uint64 {
	if d.DepSet == nil || len(d.DepSet) == 0 {
		return d.MinDep
	}
	// the maxVal in depset
	var maxVal uint64 = d.MinDep
	for ts := range d.DepSet {
		if ts > maxVal {
			maxVal = ts
		}
	}
	return maxVal
}

// Add inserts a timestamp into the dependencies.
// It advances MinDep if the new timestamp fills a gap in the sequence.
func (d *Deps) Add(ts uint64) {
	if ts <= d.MinDep {
		return
	}

	if d.DepSet == nil {
		d.DepSet = make(map[uint64]struct{})
	}

	// Add to set first
	d.DepSet[ts] = struct{}{}

	// Try to advance MinDep closing contiguous gaps
	for {
		next := d.MinDep + 1
		if _, exists := d.DepSet[next]; exists {
			delete(d.DepSet, next)
			d.MinDep = next
		} else {
			break
		}
	}
}

func (d *Deps) IsReceived(ts uint64) bool {
	if ts <= d.MinDep {
		return true
	}
	if d.DepSet == nil {
		return false
	}
	_, exists := d.DepSet[ts]
	return exists
}

// Merge combines another Deps into this one.
// It assumes that if a timestamp is in 'other', it is now visible to 'd'.
func (d *Deps) Merge(other *Deps) {
	if other == nil {
		return
	}

	// 1. Adopt the higher MinDep
	if other.MinDep > d.MinDep {
		d.MinDep = other.MinDep
		// Clean up redundant entries in DepSet that are now covered by MinDep
		if d.DepSet != nil {
			for ts := range d.DepSet {
				if ts <= d.MinDep {
					delete(d.DepSet, ts)
				}
			}
		}
	}

	// 2. Add individual timestamps from other
	for ts := range other.DepSet {
		d.Add(ts)
	}
}

func (d *Deps) Clone() *Deps {
	if d == nil {
		return nil
	}
	newDepSet := make(map[uint64]struct{}, len(d.DepSet))
	for k, v := range d.DepSet {
		newDepSet[k] = v
	}
	return &Deps{
		MinDep: d.MinDep,
		DepSet: newDepSet,
	}
}
