package replica

import (
	"context"
	"fmt"
	"go-mil/internal/model"
	pb "go-mil/proto/replica"
	tso "go-mil/proto/tso"
	"sync"
	"time"
)

// Replica represents a unit of storage (shard/partition) and the tso
type Replica struct {
	mu    sync.RWMutex
	ID    string
	Store *Store

	conns *ConnManager
}

// NewReplica creates a new replica instance
func NewReplica(id string) *Replica {
	return &Replica{
		ID:    id,
		Store: NewStore(),
		conns: NewConnManager(),
	}
}

// AddPeer adds a peer address and establishes a connection
func (r *Replica) AddPeer(addr string) error {
	_, err := r.conns.AddPeer(addr)
	return err
}

// ConnectTSO establishes a connection to the TSO server
func (r *Replica) ConnectTSO(addr string) error {
	_, err := r.conns.ConnectTSO(addr)
	return err
}

// Close closes all persistent connections
func (r *Replica) Close() {
	r.conns.Close()
}

// GetTimestamp calls the TSO to get a new timestamp.
// This is an example of using the established tsoClient.
func (r *Replica) GetTimestamp(ctx context.Context) (uint64, error) {
	client := r.conns.TSOClient()
	if client == nil {
		return 0, fmt.Errorf("TSO not connected")
	}

	// Use the client to make the RPC call over the existing connection
	resp, err := client.Tick(ctx, &tso.TickRequest{})
	if err != nil {
		return 0, fmt.Errorf("failed to get timestamp from TSO: %v", err)
	}

	return uint64(resp.Timestamp), nil
}

func (r *Replica) applyToStore(req *pb.BatchPutRequest) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	kvs := make(map[string]int)
	for k, v := range req.Kvs {
		kvs[k] = int(v)
	}

	r.Store.BatchPut(kvs, req.Cts)
}

func (r *Replica) satisfyTotalDelivery(ts uint64) {
	if r.Store.deps == nil || r.Store.deps.MinDep >= ts {
		return
	}
	// from r.Store.deps.MinDep to ts, fetch missing transactions
	for t := r.Store.deps.MinDep + 1; t <= ts; t++ {
		// fetch transaction with commit timestamp t from other replicas
		var tx *model.Transaction

		if existingTx := r.Store.GetTx(t); existingTx != nil {
			tx = existingTx
		} else {
			tx = r.fetchTxFromPeers(t)
		}

		// In a real implementation, we would apply 'tx' or update dependencies here.
		// For now, we just ensure we attempted to fetch it.
		_ = tx
	}
}

func (r *Replica) fetchTxFromPeers(cts uint64) *model.Transaction {
	clients := r.conns.GetPeerClients()
	for _, client := range clients {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.GetTransaction(ctx, &pb.GetTransactionRequest{Cts: cts})
		cancel()

		if err == nil && resp.Found {
			return &model.Transaction{
				TxId: resp.TxId,
				Cts:  resp.Cts,
			}
		}
	}
	return nil
}
