// Package testutil provides shared test utilities for integration tests.
package testutil

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
)

const (
	maxRetries   = 10
	initialDelay = 100 * time.Millisecond
	maxDelay     = 2 * time.Second
)

// ContainerHost returns the host for a container, retrying on transient Docker failures.
func ContainerHost(ctx context.Context, c testcontainers.Container) (string, error) {
	return retryContainerOp(ctx, "Host", func() (string, error) {
		return c.Host(ctx)
	})
}

// ContainerPort returns the mapped port for a container, retrying on transient Docker failures.
func ContainerPort(ctx context.Context, c testcontainers.Container, port string) (int, error) {
	var result int
	_, err := retryContainerOp(ctx, "MappedPort", func() (string, error) {
		p, err := c.MappedPort(ctx, nat.Port(port))
		if err != nil {
			return "", err
		}
		result = p.Int()
		return "", nil
	})
	return result, err
}

// ContainerEndpoint returns the endpoint for a container, retrying on transient Docker failures.
func ContainerEndpoint(ctx context.Context, c testcontainers.Container, proto string) (string, error) {
	return retryContainerOp(ctx, "Endpoint", func() (string, error) {
		return c.Endpoint(ctx, proto)
	})
}

// ContainerConnectionString returns the connection string for a MySQL container,
// retrying on transient Docker failures.
func ContainerConnectionString(ctx context.Context, c interface {
	ConnectionString(context.Context, ...string) (string, error)
}, args ...string) (string, error) {
	return retryContainerOp(ctx, "ConnectionString", func() (string, error) {
		return c.ConnectionString(ctx, args...)
	})
}

func retryContainerOp(ctx context.Context, opName string, op func() (string, error)) (string, error) {
	var lastErr error
	delay := initialDelay
	for attempt := range maxRetries {
		result, err := op()
		if err == nil {
			return result, nil
		}
		lastErr = err
		if attempt < maxRetries-1 {
			select {
			case <-ctx.Done():
				return "", fmt.Errorf("%s: context cancelled after %d attempts: %w", opName, attempt+1, lastErr)
			case <-time.After(delay):
			}
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
	return "", fmt.Errorf("%s: failed after %d attempts: %w", opName, maxRetries, lastErr)
}
