package testutil

import (
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// MiniStackImage is the AWS emulator the test layers run against. Pinned so
	// an emulator release cannot change what a green suite means, and kept in
	// step with the tag in e2e/k8s/etre-stack.yaml so the container and
	// in-cluster layers emulate AWS identically.
	MiniStackImage = "ministackorg/ministack:1.3.64"

	miniStackPort           = "4566"
	miniStackStartupTimeout = 60 * time.Second
)

// MiniStackContainerRequest returns a request for a MiniStack container, which
// serves every emulated AWS service on one port.
//
// Readiness waits on the log line MiniStack prints once its services are
// registered, not just on the port: the HTTP server binds before then, so a
// listening port alone lets an API call race startup and fail with an error
// that reads like a genuine AWS rejection.
func MiniStackContainerRequest() testcontainers.ContainerRequest {
	return testcontainers.ContainerRequest{
		Image:        MiniStackImage,
		ExposedPorts: []string{miniStackPort + "/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForLog("services available on port "+miniStackPort),
			wait.ForListeningPort(miniStackPort+"/tcp"),
		).WithDeadline(miniStackStartupTimeout),
	}
}
