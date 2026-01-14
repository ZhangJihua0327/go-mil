package replica

import (
	"context"
	"fmt"
	pb "go-mil/proto/replica"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Replica represents a unit of storage (shard/partition)
type Replica struct {
	ID    uint64
	Store *Store
}

// NewReplica creates a new replica instance
func NewReplica(id uint64) *Replica {
	return &Replica{
		ID:    id,
		Store: NewStore(),
	}
}

// Server implements the ReplicaService gRPC server
type Server struct {
	pb.UnimplementedReplicaServiceServer
	mu       sync.RWMutex
	replicas map[uint64]*Replica
	peers    []string // Addresses of peer replicas
}

// NewServer creates a new Replica gRPC server
func NewServer() *Server {
	return &Server{
		replicas: make(map[uint64]*Replica),
		peers:    make([]string, 0),
	}
}

// AddReplica adds a new replica to the server
func (s *Server) AddReplica(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.replicas[id]; !exists {
		s.replicas[id] = NewReplica(id)
	}
}

// AddPeer adds a peer address to the gossip list
func (s *Server) AddPeer(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Simple dedup
	for _, p := range s.peers {
		if p == addr {
			return
		}
	}
	s.peers = append(s.peers, addr)
}

// Get handles the Get RPC request
func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// TODO: Implement proper routing logic to find the correct replica for the key.
	// For now, scan all replicas.
	for _, r := range s.replicas {
		if node := r.Store.Get(req.Key, req.Sts); node != nil {
			return &pb.GetResponse{
				Value: int64(node.Value),
				Found: true,
			}, nil
		}
	}

	return &pb.GetResponse{Found: false}, nil
}

// BatchPut handles the BatchPut RPC request
func (s *Server) BatchPut(_ context.Context, req *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	// Apply locally
	s.applyToStore(req)

	// Gossip asynchronously
	go s.gossip(req)

	return &pb.BatchPutResponse{Success: true}, nil
}

// Gossip handles incoming gossip messages
func (s *Server) Gossip(_ context.Context, req *pb.GossipRequest) (*pb.GossipResponse, error) {
	// TODO: Add relay logic if we want multi-hop

	var putReq pb.BatchPutRequest
	if err := proto.Unmarshal(req.Payload, &putReq); err != nil {
		return &pb.GossipResponse{Success: false}, fmt.Errorf("failed to unmarshal payload: %v", err)
	}

	s.applyToStore(&putReq)
	return &pb.GossipResponse{Success: true}, nil
}

// GetTransaction handles request to retrieve a transaction by its commit timestamp
func (s *Server) GetTransaction(_ context.Context, req *pb.GetTransactionRequest) (*pb.GetTransactionResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// TODO: Proper routing again.
	// For now, scan all replicas. The first one that finds it returns it.
	for _, r := range s.replicas {
		if tx := r.Store.GetTx(req.Cts); tx != nil {
			return &pb.GetTransactionResponse{
				TxId:  tx.TxId,
				Cts:   tx.Cts,
				Found: true,
			}, nil
		}
	}

	return &pb.GetTransactionResponse{Found: false}, nil
}

// ============================================================================
// User Interface - Transaction operations
// ============================================================================

// Start begins a new transaction.
// The user process interacts with a single replica for the duration of the transaction.
func (s *Server) Start(_ context.Context, _ *pb.StartRequest) (*pb.StartResponse, error) {
	// TODO: Implement Start logic
	// 1. Assign new TxID
	// 2. Determine start timestamp (Sts)
	// 3. Initialize transaction state on this replica
	return &pb.StartResponse{}, nil
}

// Read reads a value associated with a key, visible at the transaction's start timestamp.
func (s *Server) Read(_ context.Context, _ *pb.ReadRequest) (*pb.ReadResponse, error) {
	// TODO: Implement Read logic
	// 1. Check if key is in local write buffer of the transaction
	// 2. If not, read from storage using Sts
	return &pb.ReadResponse{}, nil
}

// Write writes a value to the transaction's local buffer.
func (s *Server) Write(_ context.Context, _ *pb.WriteRequest) (*pb.WriteResponse, error) {
	// TODO: Implement Write logic
	// 1. Buffer the write locally for the transaction
	return &pb.WriteResponse{}, nil
}

// Commit attempts to commit the transaction.
func (s *Server) Commit(_ context.Context, _ *pb.CommitRequest) (*pb.CommitResponse, error) {
	// TODO: Implement Commit logic
	// 1. Perform conflict detection if necessary
	// 2. Assign commit timestamp (Cts) via TSO
	// 3. Apply writes to storage
	// 4. Replicate via Gossip (BatchPut)
	return &pb.CommitResponse{}, nil
}

// Abort checks and aborts the transaction.
func (s *Server) Abort(_ context.Context, _ *pb.AbortRequest) (*pb.AbortResponse, error) {
	// TODO: Implement Abort logic
	// 1. Clear local transaction state/buffer
	return &pb.AbortResponse{}, nil
}

func (s *Server) applyToStore(req *pb.BatchPutRequest) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	kvs := make(map[string]int)
	for k, v := range req.Kvs {
		kvs[k] = int(v)
	}

	for _, r := range s.replicas {
		r.Store.BatchPut(kvs, req.Cts)
	}
}

func (s *Server) gossip(req *pb.BatchPutRequest) {
	payload, err := proto.Marshal(req)
	if err != nil {
		fmt.Printf("Failed to marshal gossip payload: %v\n", err)
		return
	}

	s.mu.RLock()
	peers := make([]string, len(s.peers))
	copy(peers, s.peers)
	s.mu.RUnlock()

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
		go s.sendGossip(peer, payload)
	}
}

func (s *Server) sendGossip(address string, payload []byte) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("Failed to connect to peer %s: %v\n", address, err)
		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			fmt.Printf("Failed to close connection to %s: %v\n", address, err)
		}
	}()

	client := pb.NewReplicaServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = client.Gossip(ctx, &pb.GossipRequest{
		From:    "myself", // TODO: Should be real address
		Payload: payload,
	})
	if err != nil {
		// Log error but don't fail hard
		// fmt.Printf("Failed to gossip to %s: %v\n", address, err)
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
