package replica

import (
	"context"
	pb "go-mil/proto/replica"
)

// Server implements the ReplicaService gRPC server
type Server struct {
	pb.UnimplementedReplicaServiceServer
	replica *Replica
}

// NewServer creates a new Replica gRPC server
func NewServer(r *Replica) *Server {
	return &Server{
		replica: r,
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
