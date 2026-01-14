package replica

import (
	"context"
)

// ReplicaClient implements the Client interface.
type Client struct {
	// Connection to the replica server
	// TODO: Add grpc client connection
}

// NewReplicaClient creates a new ReplicaClient.
func NewReplicaClient(address string) *Client {
	// TODO: Initialize connection
	return &Client{}
}

func (c *Client) Start(ctx context.Context) error {
	// TODO: Implement Start
	return nil
}

func (c *Client) Read(ctx context.Context, key string) (int, error) {
	// TODO: Implement Read
	return 0, nil
}

func (c *Client) Write(ctx context.Context, key string, value int) error {
	// TODO: Implement Write
	return nil
}

func (c *Client) Commit(ctx context.Context) error {
	// TODO: Implement Commit
	return nil
}

func (c *Client) Abort(ctx context.Context) error {
	// TODO: Implement Abort
	return nil
}
