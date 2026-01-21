package replica

import (
	"context"
	"fmt"
	"go-mil/internal/config"
	"go-mil/internal/model"
	pb "go-mil/proto/replica"
	tso "go-mil/proto/tso"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Replica represents a unit of storage (shard/partition) and the tso
type Replica struct {
	mu                   sync.RWMutex
	ID                   string
	Store                *Store
	currTxID             string
	currTxIsolationLevel model.IsolationLevel
	buffer               map[string]int64

	// gRPC clients and connections
	tsoClient   tso.TSOClient
	peerClients map[string]pb.ReplicaServiceClient
	conns       []*grpc.ClientConn
}

// NewReplica creates a new replica instance based on the provided configuration.
func NewReplica(cfg *config.ReplicaConfig) (*Replica, error) {
	r := &Replica{
		ID:          cfg.ReplicaID,
		Store:       NewStore(),
		peerClients: make(map[string]pb.ReplicaServiceClient),
	}

	// Connect to TSO
	if cfg.TSOAddr != "" {
		conn, err := grpc.NewClient(cfg.TSOAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to connect to TSO at %s: %v", cfg.TSOAddr, err)
		}
		r.conns = append(r.conns, conn)
		r.tsoClient = tso.NewTSOClient(conn)
	}

	// Connect to peers
	for _, peer := range cfg.Peers {
		conn, err := grpc.NewClient(peer.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			r.Close() // Clean up already opened connections
			return nil, fmt.Errorf("failed to connect to peer %s at %s: %v", peer.ID, peer.Addr, err)
		}
		r.conns = append(r.conns, conn)
		r.peerClients[peer.ID] = pb.NewReplicaServiceClient(conn)
	}

	return r, nil
}

// Tick gets a timestamp from TSO. Use isTick=true for STS, isTick=false for CTS.
func (r *Replica) Tick(ctx context.Context) (uint64, error) {
	if r.tsoClient == nil {
		return 0, fmt.Errorf("TSO client not initialized")
	}

	resp, err := r.tsoClient.Tick(ctx, &tso.TickRequest{})
	if err != nil {
		return 0, err
	}
	return uint64(resp.Timestamp), nil

}

func (r *Replica) Tock(ctx context.Context) (uint64, error) {
	if r.tsoClient == nil {
		return 0, fmt.Errorf("TSO client not initialized")
	}

	resp, err := r.tsoClient.Tock(ctx, &tso.TockRequest{})
	if err != nil {
		return 0, err
	}
	return uint64(resp.Timestamp), nil
}

func (r *Replica) AcquireLock(ctx context.Context, key string, owner string) error {
	if r.tsoClient == nil {
		return fmt.Errorf("TSO client not initialized")
	}
	resp, err := r.tsoClient.AcquireLock(ctx, &tso.AcquireLockRequest{
		Key:     key,
		OwnerId: owner,
		TtlMs:   3,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("failed to acquire lock for key %s", key)
	}
	return nil
}

func (r *Replica) ReleaseLock(ctx context.Context, key string, owner string) error {
	if r.tsoClient == nil {
		return fmt.Errorf("TSO client not initialized")
	}
	resp, err := r.tsoClient.ReleaseLock(ctx, &tso.ReleaseLockRequest{
		Key:     key,
		OwnerId: owner,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("failed to release lock for key %s", key)
	}
	return nil
}

// Close closes all persistent connections
func (r *Replica) Close() {
	for _, conn := range r.conns {
		_ = conn.Close()
	}
}

// EnsureCausal ensures that the transaction with the given cts and all its dependencies are received.
func (r *Replica) EnsureCausal(ts uint64) error {
	tx := r.Store.pendingTxs[ts]
	if tx == nil {
		// Try to fetch from peers
		tx = r.fetchTxFromPeers(ts)
		if tx == nil {
			return fmt.Errorf("missing transaction with cts %d", ts)
		}
	} else {
		r.Store.RemovePendingTx(ts)
	}
	// Ensure dependencies
	if tx.Deps.MinDep > r.Store.deps.MinDep {
		err := r.EnsureTotal(tx.Deps.MinDep)
		if err != nil {
			return err
		}
	}
	for depTs := range tx.Deps.DepSet {
		err := r.EnsureCausal(depTs)
		if err != nil {
			return err
		}
	}
	r.Store.AppendTx(tx)
	return nil
}

// EnsureTotal ensures all transactions with cts <= ts are received.
func (r *Replica) EnsureTotal(ts uint64) error {
	for cts := uint64(r.Store.deps.MinDep); cts <= ts; cts++ {
		if r.Store.deps.IsReceived(cts) {
			continue
		}
		tx := r.Store.pendingTxs[cts]
		if tx == nil {
			// Try to fetch from peers
			tx = r.fetchTxFromPeers(cts)
			if tx == nil {
				return fmt.Errorf("missing transaction with cts %d", cts)
			}
		} else {
			r.Store.RemovePendingTx(cts)
		}
		r.Store.AppendTx(tx)
	}
	return nil
}

func (r *Replica) SendTxToPeers(tx *model.Transaction) {
	pbTx := modelToProto(tx)
	req := &pb.DeliverTransactionRequest{Tx: pbTx}

	go func() {
		var wg sync.WaitGroup
		for id, client := range r.peerClients {
			wg.Add(1)
			go func(peerID string, c pb.ReplicaServiceClient) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				log.Printf("[Replica] Sending transaction to peer %s", peerID)
				_, err := c.DeliverTransaction(ctx, req)

				if err != nil {
					fmt.Printf("Error sending tx %d to peer %s: %v\n", pbTx.Cts, peerID, err)
				}
			}(id, client)
		}
		wg.Wait()
	}()
}

func modelToProto(m *model.Transaction) *pb.Transaction {
	p := &pb.Transaction{
		TxId: m.TxId,
		Sts:  m.Sts,
		Cts:  m.Cts,
		Deps: &pb.Deps{
			MinDep: m.Deps.MinDep,
			DepSet: make([]uint64, 0, len(m.Deps.DepSet)),
		},
		Operations: make([]*pb.Operation, 0, len(m.Operations)),
	}

	for d := range m.Deps.DepSet {
		p.Deps.DepSet = append(p.Deps.DepSet, d)
	}

	for _, op := range m.Operations {
		pbOp := &pb.Operation{}
		switch op.OpType() {
		case model.OpStart:
			pbOp.Type = pb.Operation_START
		case model.OpRead:
			pbOp.Type = pb.Operation_READ
			if rop, ok := op.(*model.ReadOperation); ok {
				pbOp.Key = rop.Key
			}
		case model.OpWrite:
			pbOp.Type = pb.Operation_WRITE
			if wop, ok := op.(*model.WriteOperation); ok {
				pbOp.Key = wop.Key
				pbOp.Value = int64(wop.Value)
			}
		case model.OpPrepare:
			pbOp.Type = pb.Operation_PREPARE
		case model.OpCommit:
			pbOp.Type = pb.Operation_COMMIT
		case model.OpAbort:
			pbOp.Type = pb.Operation_ABORT
		}
		p.Operations = append(p.Operations, pbOp)
	}

	return p
}

func (r *Replica) fetchTxFromPeers(cts uint64) *model.Transaction {
	for _, client := range r.peerClients {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.GetTransaction(ctx, &pb.GetTransactionRequest{Cts: cts})
		cancel()

		if err == nil && resp.Found {
			if resp.Tx != nil {
				return protoToModel(resp.Tx)
			}
			return &model.Transaction{
				TxId: resp.TxId,
				Cts:  resp.Cts,
			}
		}
	}
	return nil
}
