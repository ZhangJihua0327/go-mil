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
