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
	tx      model.Transaction
	buffer  map[string]int64
	active  bool
	mu      sync.Mutex
}

func NewCentralizedClient(r *Replica) *CentralizedClient {
	return &CentralizedClient{
		replica: r,
		buffer:  make(map[string]int64),
	}
}

func (c *CentralizedClient) Start(ctx context.Context, isolationLevel string) (string, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.active {
		return "", 0, fmt.Errorf("transaction already active")
	}
	isoLevel := model.ParseIsolationLevel(isolationLevel)
	var sts uint64
	if isoLevel == model.CC || isoLevel == model.SER {
		sts, _ = c.replica.Tick(ctx)
	} else {
		sts = c.replica.Store.deps.MaxDeps()
	}
	c.tx = model.Transaction{
		TxId:           fmt.Sprintf("tx-%d-%s", sts, c.replica.ID),
		Sts:            sts,
		Deps:           c.replica.Store.deps.Clone(),
		IsolationLevel: isoLevel,
	}
	c.buffer = make(map[string]int64)
	if isoLevel == model.CC {
		err := c.replica.EnsureCausal(sts)
		if err != nil {
			return "", 0, err
		}
	} else if isoLevel == model.PC || isoLevel == model.SI || isoLevel == model.SER {
		err := c.replica.EnsureTotal(sts)
		if err != nil {
			return "", 0, err
		}
	}
	c.active = true
	c.tx.AddOperation(&model.StartOperation{})
	return c.txId, sts, nil
}

func (c *CentralizedClient) Read(ctx context.Context, key string) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.active {
		return 0, false, fmt.Errorf("no active transaction")
	}

	var val int64
	var succeed bool
	if c.tx.IsolationLevel == model.SER {
		err := c.replica.AcquireLock(ctx, key, c.tx.TxId)
		if err != nil {
			return 0, false, err
		}
	}
	// 1. Read from buffer first
	if value, exists := c.buffer[key]; exists {
		val = value
		succeed = true
	} else {
		// 2. Read from snapshot (Store)
		node := c.replica.Store.Get(key, c.tx.Sts)

		if node != nil {
			val = node.Value
			succeed = true
		} else {
			val = 0
			succeed = false
		}
	}
	c.tx.AddOperation(&model.ReadOperation{
		Key:        key,
		ReadResult: val,
	})

	return val, succeed, nil
}

func (c *CentralizedClient) Write(ctx context.Context, key string, value int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.active {
		return fmt.Errorf("no active transaction")
	}
	isoLevel := c.tx.IsolationLevel
	if isoLevel == model.SER || isoLevel == model.CC {
		err := c.replica.AcquireLock(ctx, key, c.tx.TxId)
		if err != nil {
			return err
		}
	}
	// Write to buffer
	c.buffer[key] = value
	c.tx.AddOperation(&model.WriteOperation{
		Key:   key,
		Value: value,
	})

	return nil
}

func (c *CentralizedClient) Commit(ctx context.Context) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.active {
		return 0, fmt.Errorf("no active transaction")
	}

	// 1. Get commit timestamp
	cts, err := c.replica.Tock(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get commit timestamp: %v", err)
	}
	c.tx.Cts = cts
	isoLevel := c.tx.IsolationLevel

	// 2. Atomic write to Store
	c.replica.Store.BatchPut(c.buffer, cts)
	// 3. add to history
	c.replica.Store.AppendTx(&c.tx)
	c.replica.SendTxToPeers(&c.tx)
	if isoLevel == model.SER || isoLevel == model.CC {
		err := c.replica.ReleaseLocksByOwner(ctx, c.tx.TxId)
		if err != nil {
			return 0, err
		}
	}
	c.active = false
	c.tx = model.Transaction{}
	c.buffer = nil
	return cts, nil
}

func (c *CentralizedClient) Abort(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.active {
		return fmt.Errorf("no active transaction")
	}
	c.active = false
	c.tx = model.Transaction{}
	c.buffer = nil
	return nil
}

// DecentralizedClient is a placeholder for a peer-coordination TxnClient implementation.
type DecentralizedClient struct {
}

func NewDecentralizedClient(r *Replica) *DecentralizedClient { return &DecentralizedClient{} }

func (c *DecentralizedClient) Start(ctx context.Context, isolationLevel string) (string, uint64, error) {
	return "", 0, fmt.Errorf("decentralized start not implemented (iso=%s)", isolationLevel)
}
func (c *DecentralizedClient) Read(ctx context.Context, key string) (int64, bool, error) {
	return 0, false, fmt.Errorf("decentralized read not implemented for key %s", key)
}
func (c *DecentralizedClient) Write(ctx context.Context, key string, value int64) error {
	return fmt.Errorf("decentralized write not implemented for key %s", key)
}
func (c *DecentralizedClient) Commit(ctx context.Context) (uint64, error) {
	return 0, fmt.Errorf("decentralized commit not implemented")
}
func (c *DecentralizedClient) Abort(ctx context.Context) error {
	return fmt.Errorf("decentralized abort not implemented")
}
