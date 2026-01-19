package config

import (
	"os"
	"testing"
)

func TestGetNodeType(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected NodeType
	}{
		{"TSO node", "tso", NodeTypeTSO},
		{"TSO node uppercase", "TSO", NodeTypeTSO},
		{"Replica node", "replica", NodeTypeReplica},
		{"Replica node uppercase", "REPLICA", NodeTypeReplica},
		{"Replica node mixed case", "Replica", NodeTypeReplica},
		{"Empty defaults to TSO", "", NodeTypeTSO},
		{"Invalid defaults to TSO", "invalid", NodeTypeTSO},
		{"With spaces", "  replica  ", NodeTypeReplica},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("NODE_TYPE", tt.envValue)
			defer os.Unsetenv("NODE_TYPE")

			result := GetNodeType()
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestLoadTSOConfig(t *testing.T) {
	t.Run("with custom port", func(t *testing.T) {
		os.Setenv("PORT", "60051")
		defer os.Unsetenv("PORT")

		cfg := LoadTSOConfig()

		if cfg.Port != "60051" {
			t.Errorf("Expected Port 60051, got %s", cfg.Port)
		}
	})

	t.Run("with default port", func(t *testing.T) {
		os.Unsetenv("PORT")

		cfg := LoadTSOConfig()

		if cfg.Port != "50051" {
			t.Errorf("Expected default Port 50051, got %s", cfg.Port)
		}
	})
}

func TestLoadReplicaConfig(t *testing.T) {
	t.Run("complete config with multiple peers", func(t *testing.T) {
		// Set environment variables
		os.Setenv("PORT", "60061")
		os.Setenv("REPLICA_ID", "node-1")
		os.Setenv("TSO_ADDR", "127.0.0.1:50051")
		os.Setenv("PEER1_ID", "node-2")
		os.Setenv("PEER1_ADDR", "127.0.0.1:60062")
		os.Setenv("PEER2_ID", "node-3")
		os.Setenv("PEER2_ADDR", "127.0.0.1:60063")
		os.Setenv("PEER3_ID", "node-4")
		os.Setenv("PEER3_ADDR", "127.0.0.1:60064")
		defer func() {
			os.Unsetenv("PORT")
			os.Unsetenv("REPLICA_ID")
			os.Unsetenv("TSO_ADDR")
			os.Unsetenv("PEER1_ID")
			os.Unsetenv("PEER1_ADDR")
			os.Unsetenv("PEER2_ID")
			os.Unsetenv("PEER2_ADDR")
			os.Unsetenv("PEER3_ID")
			os.Unsetenv("PEER3_ADDR")
		}()

		cfg := LoadReplicaConfig()

		if cfg.Port != "60061" {
			t.Errorf("Expected Port 60061, got %s", cfg.Port)
		}
		if cfg.ReplicaID != "node-1" {
			t.Errorf("Expected ReplicaID node-1, got %s", cfg.ReplicaID)
		}
		if cfg.TSOAddr != "127.0.0.1:50051" {
			t.Errorf("Expected TSOAddr 127.0.0.1:50051, got %s", cfg.TSOAddr)
		}

		if len(cfg.Peers) != 3 {
			t.Errorf("Expected 3 peers, got %d", len(cfg.Peers))
		}

		// Verify first peer
		if cfg.Peers[0].ID != "node-2" {
			t.Errorf("Expected Peer1 ID node-2, got %s", cfg.Peers[0].ID)
		}
		if cfg.Peers[0].Addr != "127.0.0.1:60062" {
			t.Errorf("Expected Peer1 Addr 127.0.0.1:60062, got %s", cfg.Peers[0].Addr)
		}

		// Verify second peer
		if cfg.Peers[1].ID != "node-3" {
			t.Errorf("Expected Peer2 ID node-3, got %s", cfg.Peers[1].ID)
		}
		if cfg.Peers[1].Addr != "127.0.0.1:60063" {
			t.Errorf("Expected Peer2 Addr 127.0.0.1:60063, got %s", cfg.Peers[1].Addr)
		}

		// Verify third peer
		if cfg.Peers[2].ID != "node-4" {
			t.Errorf("Expected Peer3 ID node-4, got %s", cfg.Peers[2].ID)
		}
		if cfg.Peers[2].Addr != "127.0.0.1:60064" {
			t.Errorf("Expected Peer3 Addr 127.0.0.1:60064, got %s", cfg.Peers[2].Addr)
		}
	})

	t.Run("config with no peers", func(t *testing.T) {
		os.Setenv("PORT", "60061")
		os.Setenv("REPLICA_ID", "node-1")
		os.Setenv("TSO_ADDR", "127.0.0.1:50051")
		defer func() {
			os.Unsetenv("PORT")
			os.Unsetenv("REPLICA_ID")
			os.Unsetenv("TSO_ADDR")
		}()

		cfg := LoadReplicaConfig()

		if len(cfg.Peers) != 0 {
			t.Errorf("Expected 0 peers, got %d", len(cfg.Peers))
		}
	})

	t.Run("config with default port", func(t *testing.T) {
		os.Unsetenv("PORT")
		os.Setenv("REPLICA_ID", "node-1")
		os.Setenv("TSO_ADDR", "127.0.0.1:50051")
		defer func() {
			os.Unsetenv("REPLICA_ID")
			os.Unsetenv("TSO_ADDR")
		}()

		cfg := LoadReplicaConfig()

		if cfg.Port != "50051" {
			t.Errorf("Expected default Port 50051, got %s", cfg.Port)
		}
	})
}

func TestLoadPeers(t *testing.T) {
	t.Run("skip incomplete peer configs", func(t *testing.T) {
		// Only ID without Addr should be skipped
		os.Setenv("PEER1_ID", "node-2")
		os.Setenv("PEER1_ADDR", "127.0.0.1:60062")
		os.Setenv("PEER2_ID", "node-3")
		// PEER2_ADDR is missing
		os.Setenv("PEER3_ID", "node-4")
		os.Setenv("PEER3_ADDR", "127.0.0.1:60064")
		defer func() {
			os.Unsetenv("PEER1_ID")
			os.Unsetenv("PEER1_ADDR")
			os.Unsetenv("PEER2_ID")
			os.Unsetenv("PEER3_ID")
			os.Unsetenv("PEER3_ADDR")
		}()

		peers := loadPeers()

		// Should only load PEER1 and PEER3, skip PEER2
		if len(peers) != 2 {
			t.Errorf("Expected 2 peers, got %d", len(peers))
		}

		if peers[0].ID != "node-2" {
			t.Errorf("Expected first peer ID node-2, got %s", peers[0].ID)
		}
		if peers[1].ID != "node-4" {
			t.Errorf("Expected second peer ID node-4, got %s", peers[1].ID)
		}
	})

	t.Run("empty peer list", func(t *testing.T) {
		// Clean all peer env vars
		for i := 1; i <= 5; i++ {
			os.Unsetenv("PEER" + string(rune(i+'0')) + "_ID")
			os.Unsetenv("PEER" + string(rune(i+'0')) + "_ADDR")
		}

		peers := loadPeers()

		if len(peers) != 0 {
			t.Errorf("Expected 0 peers, got %d", len(peers))
		}
	})
}
