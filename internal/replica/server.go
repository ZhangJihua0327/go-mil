package replica

import (
	"context"
	"fmt"
	pb "go-mil/proto/replica"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// GrpcPeerClient implements PeerClient using gRPC
type GrpcPeerClient struct{}

// NewGrpcPeerClient creates a new GrpcPeerClient
func NewGrpcPeerClient() *GrpcPeerClient {
	return &GrpcPeerClient{}
}

func (c *GrpcPeerClient) Gossip(ctx context.Context, addr string, req *pb.BatchPutRequest) (*pb.GossipResponse, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s: %v", addr, err)
	}
	defer func() { _ = conn.Close() }()

	client := pb.NewReplicaServiceClient(conn)
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %v", err)
	}

	return client.Gossip(ctx, &pb.GossipRequest{
		From:    "myself", // TODO: Should be real address
		Payload: payload,
	})
}

func (c *GrpcPeerClient) GetTransaction(ctx context.Context, addr string, cts uint64) (*pb.GetTransactionResponse, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s: %v", addr, err)
	}
	defer func() { _ = conn.Close() }()

	client := pb.NewReplicaServiceClient(conn)
	return client.GetTransaction(ctx, &pb.GetTransactionRequest{Cts: cts})
}

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

// Gossip handles incoming gossip messages
func (s *Server) Gossip(_ context.Context, req *pb.GossipRequest) (*pb.GossipResponse, error) {
	// TODO: Add relay logic if we want multi-hop

	var putReq pb.BatchPutRequest
	if err := proto.Unmarshal(req.Payload, &putReq); err != nil {
		return &pb.GossipResponse{Success: false}, fmt.Errorf("failed to unmarshal payload: %v", err)
	}

	s.replica.applyToStore(&putReq)
	return &pb.GossipResponse{Success: true}, nil
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

	// Gossip asynchronously
	go s.replica.gossip(req)

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
