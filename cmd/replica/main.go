package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"go-mil/internal/config"
	"go-mil/internal/replica"
	pb "go-mil/proto/replica"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Console client uses an in-process transaction client (no gRPC).
// Commands: start, read <key>, write <key> <value>, commit, abort, exit
func main() {
	_ = flag.String("addr", "unused", "deprecated: no rpc in console")
	noServer := flag.Bool("no-server", false, "if true, don't start the gRPC server")
	flag.Parse()

	cfg := config.LoadReplicaConfig()
	r, err := replica.NewReplica(cfg)
	if err != nil {
		log.Fatalf("failed to create replica: %v", err)
	}

	// Start gRPC server in background
	if !*noServer {
		lis, err := net.Listen("tcp", ":"+cfg.Port)
		if err != nil {
			log.Fatalf("failed to listen on port %s: %v", cfg.Port, err)
		}
		grpcServer := grpc.NewServer()
		pb.RegisterReplicaServiceServer(grpcServer, replica.NewServer(r))

		go func() {
			fmt.Printf("Replica Server %s starting on :%s...\n", cfg.ReplicaID, cfg.Port)
			if err := grpcServer.Serve(lis); err != nil {
				log.Fatalf("failed to serve gRPC: %v", err)
			}
		}()
	}

	var client replica.TxnClient
	if cfg.CentralMode {
		client = replica.NewCentralizedClient(r)
	} else {
		client = replica.NewDecentralizedClient(r)
	}

	runConsole(client)
	// If console exits (e.g. in non-interactive Docker), keep the gRPC server running
	select {}
}

func runConsole(txn replica.TxnClient) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Transaction Console Started. Commands: start [iso], read <key>, write <key> <val>, commit, abort, exit")

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout())

		cmd := parts[0]
		switch cmd {
		case "start":
			iso := "RA"
			if len(parts) > 1 {
				iso = parts[1]
			}
			id, sts, err := txn.Start(ctx, iso)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Transaction started. ID: %s, STS: %d\n", id, sts)
			}
		case "read":
			if len(parts) < 2 {
				fmt.Println("Usage: read <key>")
			} else {
				val, found, err := txn.Read(ctx, parts[1])
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else if !found {
					fmt.Printf("Key %s not found\n", parts[1])
				} else {
					fmt.Printf("%s = %d\n", parts[1], val)
				}
			}
		case "write":
			if len(parts) < 3 {
				fmt.Println("Usage: write <key> <value>")
			} else {
				val, err := strconv.ParseInt(parts[2], 10, 64)
				if err != nil {
					fmt.Printf("Invalid value: %v\n", err)
				} else {
					err = txn.Write(ctx, parts[1], val)
					if err != nil {
						fmt.Printf("Error: %v\n", err)
					} else {
						fmt.Println("OK")
					}
				}
			}
		case "commit":
			cts, err := txn.Commit(ctx)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Committed. CTS: %d\n", cts)
			}
		case "abort":
			err := txn.Abort(ctx)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Aborted")
			}
		case "exit":
			cancel()
			return
		default:
			fmt.Printf("Unknown command: %s\n", cmd)
		}
		cancel()
	}
}

// Optional: set a reasonable default timeout for any future context usage.
func defaultTimeout() time.Duration { return 3 * time.Second }
