package replica

import (
	"fmt"
	pb "go-mil/proto/replica"
	tso "go-mil/proto/tso"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ConnManager maintains long-lived gRPC connections to peers and TSO.
type ConnManager struct {
	peerConns   map[string]*grpc.ClientConn
	peerClients map[string]pb.ReplicaServiceClient

	tsoConn   *grpc.ClientConn
	tsoClient tso.TSOClient
}

// NewConnManager creates a new connection manager.
func NewConnManager() *ConnManager {
	return &ConnManager{
		peerConns:   make(map[string]*grpc.ClientConn),
		peerClients: make(map[string]pb.ReplicaServiceClient),
	}
}

// AddPeer establishes (or reuses) a connection to a peer and returns its client.
func (m *ConnManager) AddPeer(addr string) (pb.ReplicaServiceClient, error) {
	if client, ok := m.peerClients[addr]; ok {
		return client, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s: %v", addr, err)
	}

	m.peerConns[addr] = conn
	client := pb.NewReplicaServiceClient(conn)
	m.peerClients[addr] = client
	return client, nil
}

// GetPeerClients returns all peer clients for read-only use.
func (m *ConnManager) GetPeerClients() []pb.ReplicaServiceClient {
	clients := make([]pb.ReplicaServiceClient, 0, len(m.peerClients))
	for _, c := range m.peerClients {
		clients = append(clients, c)
	}
	return clients
}

// ConnectTSO establishes (or reuses) the connection to TSO and returns the client.
func (m *ConnManager) ConnectTSO(addr string) (tso.TSOClient, error) {
	if m.tsoClient != nil {
		return m.tsoClient, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to tso %s: %v", addr, err)
	}

	m.tsoConn = conn
	m.tsoClient = tso.NewTSOClient(conn)
	return m.tsoClient, nil
}

// TSOClient returns the current TSO client if connected.
func (m *ConnManager) TSOClient() tso.TSOClient {
	return m.tsoClient
}

// Close closes all managed connections.
func (m *ConnManager) Close() {
	if m.tsoConn != nil {
		_ = m.tsoConn.Close()
	}
	for _, conn := range m.peerConns {
		_ = conn.Close()
	}
}
