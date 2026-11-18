package api

import (
	"errors"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// SchemaBot's own stop of an apply whose deferred cutover can no longer run
// is recognized by its caller. A client that sends a stop under that caller
// is refused before anything is recorded, so it cannot leave a stop pending
// that the drive tail skips and nothing settles.
func TestExecuteStopForApply_RefusesTheCallerSchemaBotReserves(t *testing.T) {
	svc := &Service{logger: slog.New(slog.DiscardHandler)}
	apply := &storage.Apply{ApplyIdentifier: "apply-x", Database: "orders", Environment: "staging", Engine: storage.EngineSpirit, State: state.Apply.Running}

	resp, _, err := svc.executeStopForApply(t.Context(), nil, apply, apply.ApplyIdentifier, apply.Environment, storage.EndedOperationStopCaller)

	require.Error(t, err)
	assert.Nil(t, resp)
	var httpErr *controlOperationHTTPError
	require.True(t, errors.As(err, &httpErr))
	assert.Equal(t, http.StatusBadRequest, httpErr.status)
	assert.Contains(t, err.Error(), `the caller "schemabot" is reserved for SchemaBot's own stop`)
}
