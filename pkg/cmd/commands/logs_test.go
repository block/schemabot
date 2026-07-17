package commands

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogsCommandDeploymentValidation(t *testing.T) {
	err := (&LogsCmd{Deployment: "data-plane", Database: "orders", Environment: "production"}).Run(&Globals{})
	require.EqualError(t, err, "--deployment requires an explicit apply_id")

	err = (&LogsCmd{Deployment: "data-plane", ApplyID: "apply-a", Follow: true}).Run(&Globals{})
	require.EqualError(t, err, "--deployment is incompatible with --follow")
}
