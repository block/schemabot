//go:build integration

package postgres

import (
	"database/sql"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine/postgres/indexphase"
	"github.com/block/schemabot/pkg/testutil"
)

// TestConcurrentIndexPhasesMatchServer proves the phase table names exactly
// the phases PostgreSQL can publish for an index build — the fixed phases of
// pg_stat_progress_create_index and the btree sub-phases the view appends to
// the build phase — and nothing else. A phase the server adds fails here
// instead of rendering as nothing, and a phase the table invents fails here
// instead of being trusted.
func TestConcurrentIndexPhasesMatchServer(t *testing.T) {
	_, db := testutil.StartPostgres(t, "index_phase_test")

	server := make(map[string]bool)
	for _, phase := range viewPhases(t, db) {
		server[phase] = true
	}
	for _, subPhase := range btreeBuildSubPhases(t, db) {
		server[indexphase.BuildingIndex+": "+subPhase] = true
	}

	table := make(map[string]bool, len(indexphase.Phases))
	for _, phase := range indexphase.Phases {
		table[phase.Name] = true
	}

	for phase := range server {
		assert.True(t, table[phase], "server phase %q is missing from indexphase.Phases", phase)
	}
	for phase := range table {
		assert.True(t, server[phase], "indexphase.Phases lists %q, which the server never publishes", phase)
	}
}

// viewPhases reads the phase names out of the view's definition: the string
// literals of the CASE expression that produces its phase column. The bare
// build phase is the leading literal of a concatenation there, so it is
// collected the same way as the fixed ones.
func viewPhases(t *testing.T, db *sql.DB) []string {
	t.Helper()
	var definition string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT pg_get_viewdef('pg_catalog.pg_stat_progress_create_index')`).Scan(&definition))
	_, afterCommand, found := strings.Cut(definition, "END AS command")
	require.True(t, found, "view definition has no command column:\n%s", definition)
	phaseCase, _, found := strings.Cut(afterCommand, "END AS phase")
	require.True(t, found, "view definition has no phase column:\n%s", definition)

	var phases []string
	for _, match := range regexp.MustCompile(`'([^']*)'::text`).FindAllStringSubmatch(phaseCase, -1) {
		if literal := match[1]; literal != "" && literal != ": " {
			phases = append(phases, literal)
		}
	}
	require.NotEmpty(t, phases, "no phase literals found in:\n%s", phaseCase)
	return phases
}

// btreeBuildSubPhases asks the btree access method for every build sub-phase
// name it reports, in sub-phase number order, stopping at the first number it
// does not name.
func btreeBuildSubPhases(t *testing.T, db *sql.DB) []string {
	t.Helper()
	var subPhases []string
	for n := 1; ; n++ {
		var name sql.NullString
		require.NoError(t, db.QueryRowContext(t.Context(),
			`SELECT pg_indexam_progress_phasename((SELECT oid FROM pg_am WHERE amname = 'btree'), $1)`, n).Scan(&name))
		if !name.Valid {
			break
		}
		subPhases = append(subPhases, name.String)
	}
	require.NotEmpty(t, subPhases, "btree reports no build sub-phases")
	return subPhases
}
