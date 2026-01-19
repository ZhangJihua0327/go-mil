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
	replica  *Replica
	txId     string
	sts      uint64
	cts      uint64
	buffer   map[string]int
	isoLevel model.IsolationLevel
	active   bool
	mu       sync.Mutex
}

func NewCentralizedClient(r *Replica) *CentralizedClient {
	return &CentralizedClient{
		replica: r,
		buffer:  make(map[string]int),
	}
}

func (c *CentralizedClient) Start(ctx context.Context, isolationLevel string) (string, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.active {
		return "", 0, fmt.Errorf("transaction already active")
	}
	c.isoLevel = model.ParseIsolationLevel(isolationLevel)
	sts, err := c.replica.Tick(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("failed to get start timestamp: %v", err)
	}
	c.sts = sts
	c.txId = fmt.Sprintf("tx-%d-%s", sts, c.replica.ID)
	c.buffer = make(map[string]int)
	c.active = true

	return c.txId, c.sts, nil
}

func (c *CentralizedClient) Read(ctx context.Context, key string) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.active {
		return 0, false, fmt.Errorf("no active transaction")
	}

	// 1. Read from buffer first
	if val, exists := c.buffer[key]; exists {
		return int64(val), true, nil
	}

	// 2. Read from snapshot (Store)
	node := c.replica.Store.Get(key, c.sts)
	if node != nil {
		return int64(node.Value), true, nil
	}

	return 0, false, nil
}

func (c *CentralizedClient) Write(ctx context.Context, key string, value int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.active {
		return fmt.Errorf("no active transaction")
	}

	// Write to buffer
	c.buffer[key] = int(value)
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
	c.cts = cts

	// 2. Atomic write to Store
	c.replica.Store.BatchPut(c.buffer, c.cts)

	// 3. Add to history
	c.replica.Store.AddTx(&model.Transaction{
		TxId: c.txId,
		Sts:  c.sts,
		Cts:  c.cts,
	})

	c.active = false
	return c.cts, nil
}

func (c *CentralizedClient) Abort(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.active {
		return fmt.Errorf("no active transaction")
	}

	c.buffer = nil
	c.active = false
	return nil
}

// DecentralizedClient is a placeholder for a peer-coordination TxnClient implementation.
type DecentralizedClient struct {
}

func NewDecentralizedClient() *DecentralizedClient { return &DecentralizedClient{} }

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
