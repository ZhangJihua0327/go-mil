package replica

import (
	"context"
	"fmt"
	"go-mil/internal/model"
	"sync"
)

// TxnClient defines the transaction operations exposed to local callers (non-RPC).
type TxnClient interface {
	Start(ctx context.Context, isolationLevel string) (string, uint64, error)
	Read(ctx context.Context, key string) (int64, bool, error)
	Write(ctx context.Context, key string, value int64) error
	Commit(ctx context.Context) (uint64, error)
	Abort(ctx context.Context) error
}

// LocalClient is a prototype, in-process implementation that forwards to a Replica.
// NOTE: Transactional logic is not implemented yet; methods return placeholder behavior.

// CentralizedClient is a placeholder for a central-coordinator TxnClient implementation.
type CentralizedClient struct {
	replica *Replica
	txId    string
	mu      sync.Mutex
}

func NewCentralizedClient(r *Replica) *CentralizedClient {
	return &CentralizedClient{
		replica: r,
	}
}

func (c *CentralizedClient) Start(ctx context.Context, isolationLevel string) (string, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.txId != "" {
		if info := c.replica.Store.GetRunTime(c.txId); info != nil && info.Active {
			return "", 0, fmt.Errorf("transaction already active")
		}
	}

	isoLevel := model.ParseIsolationLevel(isolationLevel)
	var sts uint64
	if isoLevel == model.CC || isoLevel == model.SER {
		sts, _ = c.replica.Tick(ctx)
	} else {
		sts = c.replica.Store.history.deps.MaxDeps()
	}

	tx := &model.Transaction{
		TxId:           fmt.Sprintf("tx-%d-%s", sts, c.replica.ID),
		Sts:            sts,
		Deps:           c.replica.Store.history.deps.Clone(),
		IsolationLevel: isoLevel,
	}
	c.txId = tx.TxId

	// Create runtime info in store
	c.replica.Store.CreateRunTime(c.txId, tx)

	if isoLevel == model.CC {
		err := c.replica.EnsureCausal(sts)
		if err != nil {
			c.replica.Store.DeleteRunTime(c.txId)
			return "", 0, err
		}
	} else if isoLevel == model.PC || isoLevel == model.SI || isoLevel == model.SER {
		err := c.replica.EnsureTotal(sts)
		if err != nil {
			c.replica.Store.DeleteRunTime(c.txId)
			return "", 0, err
		}
	}

	// Add start operation
	c.replica.Store.UpdateRunTime(c.txId, func(info *RunTimeStore) {
		info.Tx.AddOperation(&model.StartOperation{})
	})

	return c.txId, sts, nil
}

func (c *CentralizedClient) Read(ctx context.Context, key string) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := c.replica.Store.GetRunTime(c.txId)
	if info == nil || !info.Active {
		return 0, false, fmt.Errorf("no active transaction")
	}

	var val int64
	var succeed bool
	if info.Tx.IsolationLevel == model.SER {
		err := c.replica.AcquireLock(ctx, key, c.txId)
		if err != nil {
			return 0, false, err
		}
	}

	// 1. Read from buffer first
	if value, exists := info.Buffer[key]; exists {
		val = value
		succeed = true
	} else {
		// 2. Read from snapshot (Store)
		node := c.replica.Store.Get(key, info.Tx.Sts)

		if node != nil {
			val = node.Value
			succeed = true
		} else {
			val = 0
			succeed = false
		}
	}

	// Add operation
	c.replica.Store.UpdateRunTime(c.txId, func(info *RunTimeStore) {
		info.Tx.AddOperation(&model.ReadOperation{
			Key:        key,
			ReadResult: val,
		})
	})

	return val, succeed, nil
}

func (c *CentralizedClient) Write(ctx context.Context, key string, value int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := c.replica.Store.GetRunTime(c.txId)
	if info == nil || !info.Active {
		return fmt.Errorf("no active transaction")
	}

	isoLevel := info.Tx.IsolationLevel
	if isoLevel == model.SER || isoLevel == model.CC {
		err := c.replica.AcquireLock(ctx, key, c.txId)
		if err != nil {
			return err
		}
	}

	// Update runtime info
	c.replica.Store.UpdateRunTime(c.txId, func(info *RunTimeStore) {
		info.Buffer[key] = value
		info.Tx.AddOperation(&model.WriteOperation{
			Key:   key,
			Value: value,
		})
	})

	return nil
}

func (c *CentralizedClient) Commit(ctx context.Context) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := c.replica.Store.GetRunTime(c.txId)
	if info == nil || !info.Active {
		return 0, fmt.Errorf("no active transaction")
	}

	// 1. Get commit timestamp
	cts, err := c.replica.Tock(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get commit timestamp: %v", err)
	}
	info.Tx.Cts = cts
	isoLevel := info.Tx.IsolationLevel

	// 2. Atomic write to Store
	c.replica.Store.BatchPut(info.Buffer, cts)
	// 3. add to history
	c.replica.Store.AppendTx(info.Tx)
	c.replica.SendTxToPeers(info.Tx)

	if isoLevel == model.SER || isoLevel == model.CC {
		err := c.replica.ReleaseLocksByOwner(ctx, c.txId)
		if err != nil {
			return 0, err
		}
	}

	// Clean up runtime info
	c.replica.Store.DeleteRunTime(c.txId)
	c.txId = ""

	return cts, nil
}

func (c *CentralizedClient) Abort(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := c.replica.Store.GetRunTime(c.txId)
	if info == nil || !info.Active {
		return fmt.Errorf("no active transaction")
	}

	// Clean up runtime info
	c.replica.Store.DeleteRunTime(c.txId)
	c.txId = ""

	return nil
}

// DecentralizedClient is a peer-coordination TxnClient implementation following the Logic Specification Manifesto.
type DecentralizedClient struct {
	replica *Replica
	txId    string
	mu      sync.Mutex
}

func NewDecentralizedClient(r *Replica) *DecentralizedClient {
	return &DecentralizedClient{
		replica: r,
	}
}

func (c *DecentralizedClient) Start(ctx context.Context, isolationLevel string) (string, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.txId != "" {
		if info := c.replica.Store.GetRunTime(c.txId); info != nil && info.Active {
			return "", 0, fmt.Errorf("transaction already active")
		}
	}

	isoLevel := model.ParseIsolationLevel(isolationLevel)
	var sts uint64
	var err error

	// Phase 2.1: The START Logic (Decentralized using HLC)
	// If level is RA, CC, or PSI: Set sts = clientDep (causal visibility optimization)
	// If level is PC, SI, or SER: Use local HLC Now() to ensure prefix/total delivery without TSO
	if isoLevel == model.RA || isoLevel == model.CC || isoLevel == model.PSI {
		// Use dep from previous transaction if exists
		sts = 0
		if c.txId != "" {
			if prevInfo := c.replica.Store.GetRunTime(c.txId); prevInfo != nil {
				sts = prevInfo.Dep
			}
		}
	} else {
		sts = c.replica.hlc.Now()
	}

	tx := &model.Transaction{
		TxId:           fmt.Sprintf("tx-dec-%d-%s", sts, c.replica.ID),
		Sts:            sts,
		Deps:           c.replica.Store.history.deps.Clone(),
		IsolationLevel: isoLevel,
	}
	c.txId = tx.TxId

	// Create runtime info in store
	info := c.replica.Store.CreateRunTime(c.txId, tx)
	info.Dep = sts

	// Visibility Logic
	if isoLevel == model.CC || isoLevel == model.PSI {
		err = c.replica.EnsureCausal(sts)
	} else if isoLevel == model.PC || isoLevel == model.SI || isoLevel == model.SER {
		err = c.replica.EnsureTotal(sts)
	}
	if err != nil {
		c.replica.Store.DeleteRunTime(c.txId)
		return "", 0, err
	}

	// Add start operation
	c.replica.Store.UpdateRunTime(c.txId, func(info *RunTimeStore) {
		info.Tx.AddOperation(&model.StartOperation{})
	})

	return c.txId, sts, nil
}

func (c *DecentralizedClient) Read(ctx context.Context, key string) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := c.replica.Store.GetRunTime(c.txId)
	if info == nil || !info.Active {
		return 0, false, fmt.Errorf("no active transaction")
	}

	// Phase 2.2: SER lock acquisition
	if info.Tx.IsolationLevel == model.SER {
		err := c.replica.AcquireLock(ctx, key, c.txId)
		if err != nil {
			return 0, false, err
		}
	}

	// 1. Read from local buffer
	if val, exists := info.Buffer[key]; exists {
		return val, true, nil
	}

	// 2. Read from Shard (MVCC version selection)
	node := c.replica.Store.Get(key, info.Tx.Sts)
	var val int64
	var found bool
	if node != nil {
		val = node.Value
		found = true
		// Phase 3: Update local HLC and client dependency (Axiom Int)
		newDep := c.replica.hlc.Update(node.Cts)
		c.replica.Store.UpdateRunTime(c.txId, func(info *RunTimeStore) {
			info.Dep = newDep
			info.ReadSet[key] = node.Cts
		})
	}

	// Add operation
	c.replica.Store.UpdateRunTime(c.txId, func(info *RunTimeStore) {
		info.Tx.AddOperation(&model.ReadOperation{
			Key:        key,
			ReadResult: val,
		})
	})

	return val, found, nil
}

func (c *DecentralizedClient) Write(ctx context.Context, key string, value int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := c.replica.Store.GetRunTime(c.txId)
	if info == nil || !info.Active {
		return fmt.Errorf("no active transaction")
	}

	// Phase 2.3: SER/CC lock acquisition (Wait-for-Commit)
	if info.Tx.IsolationLevel == model.SER || info.Tx.IsolationLevel == model.CC {
		err := c.replica.AcquireLock(ctx, key, c.txId)
		if err != nil {
			return err
		}
	}

	// Update runtime info
	c.replica.Store.UpdateRunTime(c.txId, func(info *RunTimeStore) {
		info.Buffer[key] = value
		info.Tx.AddOperation(&model.WriteOperation{
			Key:   key,
			Value: value,
		})
	})

	return nil
}

func (c *DecentralizedClient) Commit(ctx context.Context) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := c.replica.Store.GetRunTime(c.txId)
	if info == nil || !info.Active {
		return 0, fmt.Errorf("no active transaction")
	}

	// Phase 2.5: The COMMIT Finalization (Using local HLC)
	cts := c.replica.hlc.Now()

	info.Tx.Cts = cts
	// Phase 3: Update client dependency (Axiom TransVis)
	info.Dep = cts

	// Atomic write to Store
	c.replica.Store.BatchPut(info.Buffer, cts)
	c.replica.Store.AppendTx(info.Tx)
	c.replica.SendTxToPeers(info.Tx)

	// Release locks
	if info.Tx.IsolationLevel == model.SER || info.Tx.IsolationLevel == model.CC {
		_ = c.replica.ReleaseLocksByOwner(ctx, c.txId)
	}

	// Clean up runtime info
	c.replica.Store.DeleteRunTime(c.txId)
	c.txId = ""

	return cts, nil
}

func (c *DecentralizedClient) Abort(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	info := c.replica.Store.GetRunTime(c.txId)
	if info == nil || !info.Active {
		return fmt.Errorf("no active transaction")
	}

	// Release locks if held
	if info.Tx.IsolationLevel == model.SER || info.Tx.IsolationLevel == model.CC {
		_ = c.replica.ReleaseLocksByOwner(ctx, c.txId)
	}

	// Clean up runtime info
	c.replica.Store.DeleteRunTime(c.txId)
	c.txId = ""

	return nil
}
