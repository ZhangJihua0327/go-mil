package replica

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

// TxnClient defines the transaction operations exposed to local callers (non-RPC).
type TxnClient interface {
	Start(ctx context.Context, isolationLevel string) (string, uint64, error)
	Read(ctx context.Context, txID, key string) (int64, bool, error)
	Write(ctx context.Context, txID, key string, value int64) error
	Commit(ctx context.Context, txID string) (uint64, error)
	Abort(ctx context.Context, txID string) error
}

// LocalClient is a prototype, in-process implementation that forwards to a Replica.
// NOTE: Transactional logic is not implemented yet; methods return placeholder behavior.
type LocalClient struct {
	replica   *Replica
	txCounter uint64
}

// NewLocalClient creates a new LocalClient bound to a Replica.
func NewLocalClient(r *Replica) *LocalClient {
	return &LocalClient{replica: r}
}

func (c *LocalClient) nextTxID() string {
	id := atomic.AddUint64(&c.txCounter, 1)
	return "tx-" + strconv.FormatUint(id, 10)
}

func (c *LocalClient) Start(_ context.Context, isolationLevel string) (string, uint64, error) {
	_ = isolationLevel // placeholder until isolation levels are implemented
	txID := c.nextTxID()
	// Prototype: use wall clock as sts placeholder.
	sts := uint64(time.Now().UnixNano())
	return txID, sts, nil
}

func (c *LocalClient) Read(_ context.Context, _, _ string) (int64, bool, error) {
	// TODO: hook into replica.Store once read path is implemented.
	return 0, false, fmt.Errorf("read not implemented")
}

func (c *LocalClient) Write(_ context.Context, _, _ string, _ int64) error {
	// TODO: buffer writes and apply to replica.Store.
	return fmt.Errorf("write not implemented")
}

func (c *LocalClient) Commit(_ context.Context, _ string) (uint64, error) {
	// TODO: commit logic and replication.
	return 0, fmt.Errorf("commit not implemented")
}

func (c *LocalClient) Abort(_ context.Context, _ string) error {
	// TODO: cleanup logic.
	return fmt.Errorf("abort not implemented")
}

// CentralizedClient is a placeholder for a central-coordinator TxnClient implementation.
type CentralizedClient struct{}

func NewCentralizedClient() *CentralizedClient { return &CentralizedClient{} }

func (c *CentralizedClient) Start(ctx context.Context, isolationLevel string) (string, uint64, error) {
	return "", 0, fmt.Errorf("centralized start not implemented (iso=%s)", isolationLevel)
}
func (c *CentralizedClient) Read(ctx context.Context, txID, key string) (int64, bool, error) {
	return 0, false, fmt.Errorf("centralized read not implemented for tx %s key %s", txID, key)
}
func (c *CentralizedClient) Write(ctx context.Context, txID, key string, value int64) error {
	return fmt.Errorf("centralized write not implemented for tx %s key %s", txID, key)
}
func (c *CentralizedClient) Commit(ctx context.Context, txID string) (uint64, error) {
	return 0, fmt.Errorf("centralized commit not implemented for tx %s", txID)
}
func (c *CentralizedClient) Abort(ctx context.Context, txID string) error {
	return fmt.Errorf("centralized abort not implemented for tx %s", txID)
}

// DecentralizedClient is a placeholder for a peer-coordination TxnClient implementation.
type DecentralizedClient struct{}

func NewDecentralizedClient() *DecentralizedClient { return &DecentralizedClient{} }

func (c *DecentralizedClient) Start(ctx context.Context, isolationLevel string) (string, uint64, error) {
	return "", 0, fmt.Errorf("decentralized start not implemented (iso=%s)", isolationLevel)
}
func (c *DecentralizedClient) Read(ctx context.Context, txID, key string) (int64, bool, error) {
	return 0, false, fmt.Errorf("decentralized read not implemented for tx %s key %s", txID, key)
}
func (c *DecentralizedClient) Write(ctx context.Context, txID, key string, value int64) error {
	return fmt.Errorf("decentralized write not implemented for tx %s key %s", txID, key)
}
func (c *DecentralizedClient) Commit(ctx context.Context, txID string) (uint64, error) {
	return 0, fmt.Errorf("decentralized commit not implemented for tx %s", txID)
}
func (c *DecentralizedClient) Abort(ctx context.Context, txID string) error {
	return fmt.Errorf("decentralized abort not implemented for tx %s", txID)
}
