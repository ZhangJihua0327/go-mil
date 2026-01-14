package model

import "strings"

// ============================================================================
// Isolation Level - Transaction isolation levels
// ============================================================================

// IsolationLevel represents the isolation level of a transaction
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
	// SI - Snapshot Isolation: Provides consistent snapshot view
	SI
	// SER - Serviceability: Strongest isolation, equivalent to serial execution
	SER
)

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
	TxId string
	Cts  uint64
}

// Deps represents the set of transactions visible to the current node.
type Deps struct {
	MinDep uint64              // All transactions with timestamp <= MinDep are received
	DepSet map[uint64]struct{} // Set of timestamps > MinDep that are received
}
