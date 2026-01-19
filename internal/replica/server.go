package replica

import (
	"context"
	"go-mil/internal/model"
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
			Tx:    modelToProto(tx),
		}, nil
	}

	return &pb.GetTransactionResponse{Found: false}, nil
}

// DeliverTransaction handles the delivery of a transaction from another replica
func (s *Server) DeliverTransaction(_ context.Context, req *pb.DeliverTransactionRequest) (*pb.DeliverTransactionResponse, error) {
	if req.Tx == nil {
		return &pb.DeliverTransactionResponse{Success: false}, nil
	}

	tx := protoToModel(req.Tx)
	s.replica.Store.AddPendingTx(tx)

	return &pb.DeliverTransactionResponse{Success: true}, nil
}

func protoToModel(p *pb.Transaction) *model.Transaction {
	m := &model.Transaction{
		TxId: p.TxId,
		Sts:  p.Sts,
		Cts:  p.Cts,
		Deps: &model.Deps{
			MinDep: p.Deps.MinDep,
			DepSet: make(map[uint64]struct{}),
		},
		Operations: make([]model.Operation, 0, len(p.Operations)),
	}

	for _, d := range p.Deps.DepSet {
		m.Deps.DepSet[d] = struct{}{}
	}

	for _, op := range p.Operations {
		var modelOp model.Operation
		base := model.BaseOperation{
			TxID: p.TxId,
			Sts:  p.Sts,
		}

		switch op.Type {
		case pb.Operation_WRITE:
			modelOp = &model.WriteOperation{
				BaseOperation: base,
				Key:           op.Key,
				Value:         int(op.Value),
			}
		case pb.Operation_READ:
			modelOp = &model.ReadOperation{
				BaseOperation: base,
				Key:           op.Key,
			}
		case pb.Operation_START:
			modelOp = &model.StartOperation{
				BaseOperation: base,
			}
		case pb.Operation_PREPARE:
			modelOp = &model.PrepareOperation{
				BaseOperation: base,
			}
		case pb.Operation_COMMIT:
			modelOp = &model.CommitOperation{
				BaseOperation: base,
			}
		case pb.Operation_ABORT:
			modelOp = &model.AbortOperation{
				BaseOperation: base,
			}
		}

		if modelOp != nil {
			m.Operations = append(m.Operations, modelOp)
		}
	}

	return m
}
