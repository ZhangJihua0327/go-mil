package main

import (
	"fmt"
	"go-mil/internal/config"
	"go-mil/internal/tso"
	pb "go-mil/proto/tso"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	cfg := config.LoadTSOConfig()
	port := cfg.Port

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterTSOServer(s, tso.NewTsoServer(cfg))

	fmt.Printf("TSO Server starting on :%s...\n", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
