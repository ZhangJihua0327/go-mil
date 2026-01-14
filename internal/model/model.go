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

// BaseOperation Operation represents an operation in the system.
// Details will be defined later.
type BaseOperation struct {
	TxID     string   // Transaction ID (string format)
	Sts      uint64   // Start timestamp from parent transaction
	Response chan any // Channel for async response
}

// StartOperation represents a start operation
type StartOperation struct {
	BaseOperation
}

// ReadOperation represents a read operation
type ReadOperation struct {
	BaseOperation
}

// WriteOperation represents a write operation
type WriteOperation struct {
	BaseOperation
}

// PrepareOperation represents a prepare operation
type PrepareOperation struct {
	BaseOperation
}

// CommitOperation represents a commit operation
type CommitOperation struct {
	BaseOperation
}

// AbortOperation represents an abort operation
type AbortOperation struct {
	BaseOperation
}

// Transaction represents a transaction in the system.
// Details will be defined later.
type Transaction struct {
	TxId       string
	Sts        uint64
	Cts        uint64
	Deps       *Deps
	Operations []BaseOperation
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
