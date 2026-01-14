package replica

import (
	"context"
	"fmt"
	pb "go-mil/proto/replica"

	"google.golang.org/grpc"
)

// Server implements the ReplicaService gRPC server
type Server struct {
	pb.UnimplementedReplicaServiceServer
	replica *Replica
	client  Client // Strategy for transaction coordination
}

// NewServer creates a new Replica gRPC server
func NewServer(r *Replica, client Client) *Server {
	return &Server{
		replica: r,
		client:  client,
	}
}

// GetTransaction handles request to retrieve a transaction by its commit timestamp
func (s *Server) GetTransaction(_ context.Context, req *pb.GetTransactionRequest) (*pb.GetTransactionResponse, error) {
	s.replica.mu.RLock()
	defer s.replica.mu.RUnlock()

	if tx := s.replica.Store.GetTx(req.Cts); tx != nil {
		return &pb.GetTransactionResponse{
			TxId:  tx.TxId,
			Cts:   tx.Cts,
			Found: true,
		}, nil
	}

	return &pb.GetTransactionResponse{Found: false}, nil
}

// Get handles the Get RPC request
func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.replica.mu.RLock()
	defer s.replica.mu.RUnlock()

	// Check local store
	if node := s.replica.Store.Get(req.Key, req.Sts); node != nil {
		return &pb.GetResponse{
			Value: int64(node.Value),
			Found: true,
		}, nil
	}

	return &pb.GetResponse{Found: false}, nil
}

// BatchPut handles the BatchPut RPC request
func (s *Server) BatchPut(_ context.Context, req *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	// Apply locally
	s.replica.applyToStore(req)

	return &pb.BatchPutResponse{Success: true}, nil
}

// ============================================================================
// User Interface - Transaction operations
// ============================================================================

// Start begins a new transaction.
// The user process interacts with a single replica for the duration of the transaction.
func (s *Server) Start(ctx context.Context, _ *pb.StartRequest) (*pb.StartResponse, error) {
	if s.client == nil {
		return nil, fmt.Errorf("transaction client not configured")
	}
	txID, sts, err := s.client.Start(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.StartResponse{
		TxId: txID,
		Sts:  sts,
	}, nil
}

// Read reads a value associated with a key, visible at the transaction's start timestamp.
func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	if s.client == nil {
		return nil, fmt.Errorf("transaction client not configured")
	}
	val, found, err := s.client.Read(ctx, req.TxId, req.Key)
	if err != nil {
		return nil, err
	}
	return &pb.ReadResponse{
		Value: val,
		Found: found,
	}, nil
}

// Write writes a value to the transaction's local buffer.
func (s *Server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	if s.client == nil {
		return nil, fmt.Errorf("transaction client not configured")
	}
	if err := s.client.Write(ctx, req.TxId, req.Key, req.Value); err != nil {
		return &pb.WriteResponse{Success: false}, err
	}
	return &pb.WriteResponse{Success: true}, nil
}

// Commit attempts to commit the transaction.
func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	if s.client == nil {
		return nil, fmt.Errorf("transaction client not configured")
	}
	cts, err := s.client.Commit(ctx, req.TxId)
	if err != nil {
		return &pb.CommitResponse{Success: false}, err
	}
	return &pb.CommitResponse{Success: true, Cts: cts}, nil
}

// Abort checks and aborts the transaction.
func (s *Server) Abort(ctx context.Context, req *pb.AbortRequest) (*pb.AbortResponse, error) {
	if s.client == nil {
		return nil, fmt.Errorf("transaction client not configured")
	}
	if err := s.client.Abort(ctx, req.TxId); err != nil {
		return &pb.AbortResponse{Success: false}, err
	}
	return &pb.AbortResponse{Success: true}, nil
}

// ============================================================================
// Client Interface & Implementations
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
