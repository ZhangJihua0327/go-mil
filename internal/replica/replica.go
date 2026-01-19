package replica

import (
	"context"
	"fmt"
	"go-mil/internal/config"
	"go-mil/internal/model"
	pb "go-mil/proto/replica"
	tso "go-mil/proto/tso"
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

// Close closes all persistent connections
func (r *Replica) Close() {
	for _, conn := range r.conns {
		_ = conn.Close()
	}
}

func (r *Replica) checkCausalDelivery(ts uint64) error {
	return nil
}

func (r *Replica) checkTotalDelivery(ts uint64) error {
	return nil
}

func (r *Replica) fetchTxFromPeers(cts uint64) *model.Transaction {
	for _, client := range r.peerClients {
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
