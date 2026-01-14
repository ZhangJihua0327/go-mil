package replica

import (
	"context"
)

// Client defines the interface for interacting with the replica system from a client perspective.
// A client session is bound to a single transaction on a single replica.
type Client interface {
	// Start begins a new transaction.
	Start(ctx context.Context) error

	// Read reads a value for a start timestamp.
	Read(ctx context.Context, key string) (int, error)

	// Write writes a value to the local transaction buffer.
	Write(ctx context.Context, key string, value int) error

	// Commit attempts to commit the transaction.
	Commit(ctx context.Context) error

	// Abort aborts the transaction.
	Abort(ctx context.Context) error
}

// CentralClient implements the Client interface.
type CentralClient struct {
	// Connection to the replica server
	// TODO: Add grpc client connection
}

// NewReplicaClient creates a new CentralClient.
func NewReplicaClient(address string) *CentralClient {
	// TODO: Initialize connection
	return &CentralClient{}
}

func (c *CentralClient) Start(ctx context.Context) error {
	// TODO: Implement Start
	return nil
}

func (c *CentralClient) Read(ctx context.Context, key string) (int, error) {
	// TODO: Implement Read
	return 0, nil
}

func (c *CentralClient) Write(ctx context.Context, key string, value int) error {
	// TODO: Implement Write
	return nil
}

func (c *CentralClient) Commit(ctx context.Context) error {
	// TODO: Implement Commit
	return nil
}

func (c *CentralClient) Abort(ctx context.Context) error {
	// TODO: Implement Abort
	return nil
}
