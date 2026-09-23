package localdemo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSampleRequiresLocalDocker(t *testing.T) {
	for _, endpoint := range []string{"unix:///var/run/docker.sock", "npipe:////./pipe/docker_engine", "tcp://127.0.0.1:2376", "tcp://[::1]:2376"} {
		require.True(t, localDockerEndpoint(endpoint), endpoint)
	}
	for _, endpoint := range []string{"ssh://production.example.com", "tcp://production.example.com:2376", "tcp://127.0.0.1.attacker.example:2376", ""} {
		require.False(t, localDockerEndpoint(endpoint), endpoint)
	}
}
