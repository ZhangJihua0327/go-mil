package replica

import (
	"context"
	"fmt"
	pb "go-mil/proto/replica"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// PeerClient abstracts the communication with other replicas
type PeerClient interface {
	Gossip(ctx context.Context, addr string, req *pb.BatchPutRequest) (*pb.GossipResponse, error)
	GetTransaction(ctx context.Context, addr string, cts uint64) (*pb.GetTransactionResponse, error)
}

// Replica represents a unit of storage (shard/partition) and the tso
type Replica struct {
	mu         sync.RWMutex
	ID         string
	Store      *Store
	peers      []string // Addresses of peer replicas
	peerClient PeerClient
}

// NewReplica creates a new replica instance
func NewReplica(id string, peerClient PeerClient) *Replica {
	return &Replica{
		ID:         id,
		Store:      NewStore(),
		peers:      make([]string, 0),
		peerClient: peerClient,
	}
}

// AddPeer adds a peer address to the gossip list
func (r *Replica) AddPeer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Simple dedup
	for _, p := range r.peers {
		if p == addr {
			return
		}
	}
	r.peers = append(r.peers, addr)
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

func (r *Replica) gossip(req *pb.BatchPutRequest) {
	r.mu.RLock()
	peers := make([]string, len(r.peers))
	copy(peers, r.peers)
	r.mu.RUnlock()

	if len(peers) == 0 {
		return
	}

	// Pick random k peers (e.g., k=3)
	k := 3
	if len(peers) < k {
		k = len(peers)
	}

	perm := rand.Perm(len(peers))
	for i := 0; i < k; i++ {
		peer := peers[perm[i]]
		// Use PeerClient instead of sendGossip
		go func(addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = r.peerClient.Gossip(ctx, addr, req)
		}(peer)
	}
}

// ============================================================================
// Client Interface
// ============================================================================

// Client defines the interface for users to interact with the replica system.
// It abstracts the underlying coordination mechanism (centralized vs decentralized).
type Client interface {
	Start(ctx context.Context) (string, uint64, error)
	Read(ctx context.Context, txID, key string) (int64, bool, error)
	Write(ctx context.Context, txID, key string, value int64) error
	Commit(ctx context.Context, txID string) (uint64, error)
	Abort(ctx context.Context, txID string) error
}

// ============================================================================
// Centralized Client Implementation (Remote/gRPC)
// ============================================================================

// CentralizedClient connects to a specific replica via gRPC.
// That replica acts as the transaction coordinator.
type CentralizedClient struct {
	client pb.ReplicaServiceClient
}

// NewCentralizedClient creates a new CentralizedClient.
func NewCentralizedClient(conn grpc.ClientConnInterface) *CentralizedClient {
	return &CentralizedClient{
		client: pb.NewReplicaServiceClient(conn),
	}
}

func (c *CentralizedClient) Start(ctx context.Context) (string, uint64, error) {
	resp, err := c.client.Start(ctx, &pb.StartRequest{})
	if err != nil {
		return "", 0, err
	}
	return resp.TxId, resp.Sts, nil
}

func (c *CentralizedClient) Read(ctx context.Context, txID, key string) (int64, bool, error) {
	resp, err := c.client.Read(ctx, &pb.ReadRequest{
		TxId: txID,
		Key:  key,
	})
	if err != nil {
		return 0, false, err
	}
	return resp.Value, resp.Found, nil
}

func (c *CentralizedClient) Write(ctx context.Context, txID, key string, value int64) error {
	resp, err := c.client.Write(ctx, &pb.WriteRequest{
		TxId:  txID,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("write failed")
	}
	return nil
}

func (c *CentralizedClient) Commit(ctx context.Context, txID string) (uint64, error) {
	resp, err := c.client.Commit(ctx, &pb.CommitRequest{
		TxId: txID,
	})
	if err != nil {
		return 0, err
	}
	if !resp.Success {
		return resp.Cts, fmt.Errorf("commit failed")
	}
	return resp.Cts, nil
}

func (c *CentralizedClient) Abort(ctx context.Context, txID string) error {
	resp, err := c.client.Abort(ctx, &pb.AbortRequest{
		TxId: txID,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("abort failed")
	}
	return nil
}

// ============================================================================
// Decentralized Client Implementation (Decentralized)
// ============================================================================

// DecentralizedClient acts as the transaction coordinator itself.
// It communicates with TSO and multiple replicas directly.
type DecentralizedClient struct {
	// TODO: Add connections to TSO and Replicas
}

// NewDecentralizedClient creates a new DecentralizedClient.
func NewDecentralizedClient() *DecentralizedClient {
	return &DecentralizedClient{}
}

func (c *DecentralizedClient) Start(_ context.Context) (string, uint64, error) {
	// TODO: Implement client-side start logic (fetch TSO)
	return "", 0, nil
}

func (c *DecentralizedClient) Read(_ context.Context, _, _ string) (int64, bool, error) {
	// TODO: Implement client-side read logic (route to correct replica)
	return 0, false, nil
}

func (c *DecentralizedClient) Write(_ context.Context, _, _ string, _ int64) error {
	// TODO: Implement client-side write logic (buffer locally)
	return nil
}

func (c *DecentralizedClient) Commit(_ context.Context, _ string) (uint64, error) {
	// TODO: Implement client-side commit logic (2PC or similar)
	return 0, nil
}

func (c *DecentralizedClient) Abort(_ context.Context, _ string) error {
	// TODO: Implement client-side abort logic
	return nil
}
