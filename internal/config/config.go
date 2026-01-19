package config

import (
	"fmt"
	"os"
	"strings"
)

// NodeType represents the type of node.
type NodeType string

const (
	NodeTypeTSO     NodeType = "tso"
	NodeTypeReplica NodeType = "replica"
)

// Peer represents a peer node configuration.
type Peer struct {
	ID   string
	Addr string
}

// TSOConfig holds the configuration for TSO nodes.
type TSOConfig struct {
	Port string
}

// ReplicaConfig holds the configuration for Replica nodes.
type ReplicaConfig struct {
	ReplicaID string
	Port      string
	TSOAddr   string
	Peers     []Peer
}

// GetNodeType returns the node type from NODE_TYPE environment variable.
func GetNodeType() NodeType {
	nodeType := strings.ToLower(strings.TrimSpace(os.Getenv("NODE_TYPE")))
	if nodeType == string(NodeTypeReplica) {
		return NodeTypeReplica
	}
	return NodeTypeTSO // default to TSO
}

// LoadTSOConfig loads TSO configuration from environment variables.
func LoadTSOConfig() *TSOConfig {
	return &TSOConfig{
		Port: getEnv("PORT", "50051"),
	}
}

// LoadReplicaConfig loads Replica configuration from environment variables.
func LoadReplicaConfig() *ReplicaConfig {
	config := &ReplicaConfig{
		ReplicaID: os.Getenv("REPLICA_ID"),
		Port:      getEnv("PORT", "50051"),
		TSOAddr:   os.Getenv("TSO_ADDR"),
		Peers:     loadPeers(),
	}
	return config
}

// loadPeers loads all peer configurations from environment variables.
// It looks for PEER1_ID, PEER1_ADDR, PEER2_ID, PEER2_ADDR, etc.
func loadPeers() []Peer {
	peers := make([]Peer, 0)
	for i := 1; ; i++ {
		peerIDKey := fmt.Sprintf("PEER%d_ID", i)
		peerAddrKey := fmt.Sprintf("PEER%d_ADDR", i)

		peerID := os.Getenv(peerIDKey)
		peerAddr := os.Getenv(peerAddrKey)

		// Stop if both ID and Addr are empty
		if peerID == "" && peerAddr == "" {
			break
		}

		// Only add peer if both ID and Addr are present
		if peerID != "" && peerAddr != "" {
			peers = append(peers, Peer{
				ID:   peerID,
				Addr: peerAddr,
			})
		}
	}
	return peers
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
