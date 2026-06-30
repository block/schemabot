package webhook

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTenantStateBlockRoundTrip(t *testing.T) {
	state := TenantState{
		Tenant:      "tenant-b",
		Environment: "staging",
		SHA:         "abc1234",
		Rollup:      "pending",
		Databases: []TenantDatabaseState{
			{Database: "orders", Op: "ALTER TABLE", State: "running", Detail: "add column email"},
			{Database: "ledger", Op: "CREATE TABLE", State: "applied"},
		},
	}

	block, err := renderTenantStateBlock(state)
	require.NoError(t, err)

	got, found, err := parseTenantStateBlock(block)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, state, got, "round-trip must preserve nested database rows")
}

// The block round-trips even when embedded in an otherwise human-facing comment
// (the collapsed summary the writer adds around it), and nested JSON in the
// databases array is captured up to the closing marker rather than the first
// brace.
func TestParseTenantStateBlockEmbeddedInComment(t *testing.T) {
	state := TenantState{
		Tenant:      "tenant-b",
		Environment: "staging",
		SHA:         "abc1234",
		Rollup:      "pending",
		Databases:   []TenantDatabaseState{{Database: "orders", State: "running"}},
	}
	block, err := renderTenantStateBlock(state)
	require.NoError(t, err)

	comment := "<details><summary>SchemaBot · tenant-b — 1 change pending</summary>\n\n" +
		block + "\n\n| Database | State |\n|---|---|\n| orders | running |\n</details>"

	got, found, err := parseTenantStateBlock(comment)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "tenant-b", got.Tenant)
	assert.Equal(t, "staging", got.Environment)
	assert.Equal(t, "abc1234", got.SHA)
	require.Len(t, got.Databases, 1)
	assert.Equal(t, "orders", got.Databases[0].Database)
}

func TestParseTenantStateBlockNotFound(t *testing.T) {
	got, found, err := parseTenantStateBlock("just a normal PR comment, nothing to see here")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, TenantState{}, got)
}

// A present-but-broken block must surface an error (found=true) so the leader
// fails closed and treats the tenant as not-reported rather than trusting it.
func TestParseTenantStateBlockMalformed(t *testing.T) {
	comment := "<!-- " + tenantStateMarker + " v=1\n{not valid json}\n-->"
	_, found, err := parseTenantStateBlock(comment)
	assert.True(t, found, "the marker was present")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse tenant-state block")
}

func TestParseTenantStateBlockUnsupportedVersion(t *testing.T) {
	comment := "<!-- " + tenantStateMarker + " v=2\n{\"tenant\":\"tenant-b\"}\n-->"
	_, found, err := parseTenantStateBlock(comment)
	assert.True(t, found)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported tenant-state block version")
}

// An out-of-range version is reported as a malformed version (a distinct,
// actionable cause), not conflated with an unsupported one.
func TestParseTenantStateBlockMalformedVersion(t *testing.T) {
	comment := "<!-- " + tenantStateMarker + " v=99999999999999999999\n{\"tenant\":\"tenant-b\"}\n-->"
	_, found, err := parseTenantStateBlock(comment)
	assert.True(t, found)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "malformed tenant-state block version")
}

// A block whose JSON parses but is missing fields a reader needs to trust it
// must be rejected (found=true, descriptive error) so the reader fails closed
// instead of treating a semantically-empty block as a real report.
func TestParseTenantStateBlockMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name    string
		json    string
		wantErr string
	}{
		{"missing tenant", `{"environment":"staging","sha":"abc","rollup":"pending"}`, "missing tenant"},
		{"missing environment", `{"tenant":"tenant-b","sha":"abc","rollup":"pending"}`, "missing environment"},
		{"missing sha", `{"tenant":"tenant-b","environment":"staging","rollup":"pending"}`, "missing sha"},
		{"missing rollup", `{"tenant":"tenant-b","environment":"staging","sha":"abc"}`, "missing rollup"},
		{"database missing db", `{"tenant":"tenant-b","environment":"staging","sha":"abc","rollup":"pending","databases":[{"state":"running"}]}`, "missing db"},
		{"database missing state", `{"tenant":"tenant-b","environment":"staging","sha":"abc","rollup":"pending","databases":[{"db":"orders"}]}`, "missing state"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			comment := "<!-- " + tenantStateMarker + " v=1\n" + tc.json + "\n-->"
			_, found, err := parseTenantStateBlock(comment)
			assert.True(t, found, "the marker was present")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid tenant-state block")
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// A no-changes report carries an empty databases list and still round-trips —
// participants must publish this so the leader can distinguish "done, nothing to
// do" from "not yet reported".
func TestTenantStateBlockNoChanges(t *testing.T) {
	state := TenantState{Tenant: "tenant-c", Environment: "production", SHA: "abc1234", Rollup: "no_changes"}
	block, err := renderTenantStateBlock(state)
	require.NoError(t, err)
	assert.True(t, strings.Contains(block, "no_changes"))

	got, found, err := parseTenantStateBlock(block)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, state, got)
	assert.Empty(t, got.Databases)
}
