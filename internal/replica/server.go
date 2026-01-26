package replica

import (
	"context"
	"go-mil/internal/model"
	pb "go-mil/proto/replica"
	"log"
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
	log.Printf("[ReplicaServer] GetTransaction: cts=%d", req.Cts)
	s.replica.mu.RLock()
	defer s.replica.mu.RUnlock()

	if tx := s.replica.Store.GetTx(req.Cts); tx != nil {
		log.Printf("[ReplicaServer] GetTransaction SUCCESS: txid=%s cts=%d", tx.TxId, tx.Cts)
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
		log.Printf("[ReplicaServer] DeliverTransaction FAILED: nil tx")
		return &pb.DeliverTransactionResponse{Success: false}, nil
	}

	tx := protoToModel(req.Tx)
	log.Printf("[ReplicaServer] DeliverTransaction: txid=%s sts=%d cts=%d", tx.TxId, tx.Sts, tx.Cts)

	// Update local HLC with transaction's commit timestamp to maintain causality
	s.replica.hlc.Update(tx.Cts)

	s.replica.Store.AddPendingTx(tx)

	return &pb.DeliverTransactionResponse{Success: true}, nil
}

// HlcGet returns the current HLC timestamp
func (s *Server) HlcGet(_ context.Context, _ *pb.GetHLCTimeRequest) (*pb.HlcResponse, error) {
	ts := s.replica.hlc.Now()
	return &pb.HlcResponse{
		Ts: ts,
	}, nil
}

// HlcUpdate updates the local HLC with a remote timestamp
func (s *Server) HlcUpdate(_ context.Context, req *pb.HlcResponse) (*pb.HlcResponse, error) {
	ts := s.replica.hlc.Update(req.Ts)
	return &pb.HlcResponse{
		Ts: ts,
	}, nil
}

// GetRunTime returns runtime information for a specific transaction
func (s *Server) GetRunTime(_ context.Context, req *pb.GetRunTimeRequest) (*pb.GetRunTimeResponse, error) {
	info := s.replica.Store.GetRunTime(req.TxId)
	if info == nil {
		return &pb.GetRunTimeResponse{Found: false}, nil
	}

	return &pb.GetRunTimeResponse{
		Found:   true,
		Runtime: runtimeToProto(info),
	}, nil
}

// GetAllRunTime returns runtime information for all active transactions
func (s *Server) GetAllRunTime(_ context.Context, _ *pb.GetAllRunTimeRequest) (*pb.GetAllRunTimeResponse, error) {
	allRuntime := s.replica.Store.GetAllRunTime()
	runtimes := make([]*pb.TxnRunTimeInfo, 0, len(allRuntime))

	for _, info := range allRuntime {
		runtimes = append(runtimes, runtimeToProto(info))
	}

	return &pb.GetAllRunTimeResponse{
		Runtimes: runtimes,
	}, nil
}

// runtimeToProto converts internal RunTimeStore to protobuf format
func runtimeToProto(info *RunTimeStore) *pb.TxnRunTimeInfo {
	return &pb.TxnRunTimeInfo{
		TxId:    info.TxId,
		Tx:      modelToProto(info.Tx),
		Buffer:  info.Buffer,
		ReadSet: info.ReadSet,
		Dep:     info.Dep,
		Active:  info.Active,
	}
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
		base := model.BaseOperation{}

		switch op.Type {
		case pb.Operation_WRITE:
			modelOp = &model.WriteOperation{
				BaseOperation: base,
				Key:           op.Key,
				Value:         op.Value,
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
