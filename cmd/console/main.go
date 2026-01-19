package main

import (
	"flag"
	"go-mil/internal/config"
	"go-mil/internal/replica"
	"log"
	"time"
)

// Console client uses an in-process transaction client (no gRPC).
// Commands: start, read <key>, write <key> <value>, commit, abort, exit
func main() {
	_ = flag.String("addr", "unused", "deprecated: no rpc in console")
	flag.Parse()

	cfg := config.LoadReplicaConfig()
	r, err := replica.NewReplica(cfg)
	if err != nil {
		log.Fatalf("failed to create replica: %v", err)
	}

	var client replica.TxnClient
	if cfg.CentralMode {
		client = replica.NewCentralizedClient(r)
	} else {
		client = replica.NewDecentralizedClient(r)
	}

	runConsole(client)
}

func runConsole(txn replica.TxnClient) {

}

// Optional: set a reasonable default timeout for any future context usage.
func defaultTimeout() time.Duration { return 3 * time.Second }
