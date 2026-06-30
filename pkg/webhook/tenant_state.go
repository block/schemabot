package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// tenantStateVersion is the schema version of the machine-readable tenant-state
// block embedded in a per-tenant comment. The leader rejects blocks it cannot
// parse (including unknown versions) and treats the tenant as not-reported,
// which is fail-closed.
const tenantStateVersion = 1

// tenantStateMarker identifies the hidden HTML-comment block that carries a
// tenant's machine-readable state inside an otherwise human-facing comment.
const tenantStateMarker = "schemabot:state"

// TenantDatabaseState is one (database) row of a tenant's reported state for a
// pull request head SHA.
type TenantDatabaseState struct {
	Database string `json:"db"`
	// Op is the DDL operation (for example "ALTER TABLE" or "CREATE TABLE").
	// Optional; carried through for display.
	Op string `json:"op,omitempty"`
	// State is this database's state for the head SHA (for example running,
	// applied, or failed). It is carried verbatim; the leader's fold maps it to
	// an aggregate check conclusion.
	State string `json:"state"`
	// Detail is a short human-facing description (for example a DDL summary).
	// Optional.
	Detail string `json:"detail,omitempty"`
}

// TenantState is a single tenant's published, machine-readable state for a pull
// request head SHA — the cross-tenant interface the leader folds into the one
// aggregate check. It is transport-agnostic: a TenantStateSource yields these
// regardless of how the tenant published them (PR comments today; an issue or an
// authenticated RPC channel later).
type TenantState struct {
	Tenant string `json:"tenant"`
	// Environment scopes this block to one environment. A deployment may manage
	// several environments (AllowedEnvironments), and the aggregate check is
	// per-environment, so state is keyed by (tenant, environment): a tenant
	// publishes one block per environment it participates in, and each
	// environment's leader folds only the blocks for its own environment.
	Environment string `json:"environment"`
	SHA         string `json:"sha"`
	// Rollup is the tenant's overall state for this environment at the head SHA,
	// folded from its databases. The leader maps it to the check conclusion.
	Rollup    string                `json:"rollup"`
	Databases []TenantDatabaseState `json:"databases,omitempty"`
}

// TenantStateSource yields every participating tenant's published state for a
// pull request at a head SHA, independent of the underlying transport. The
// leader fold reads through this seam so the transport (PR comments, a dedicated
// issue, or a future RPC channel) can change without touching the fold, the
// render, or the check.
type TenantStateSource interface {
	StatesForPR(ctx context.Context, repo string, pr int, headSHA string) ([]TenantState, error)
}

// tenantStateBlockRE matches the hidden tenant-state block. The payload is
// captured up to the closing "-->" (not up to the first "}"), so nested JSON in
// the databases array round-trips correctly.
var tenantStateBlockRE = regexp.MustCompile(`(?s)<!--\s*` + regexp.QuoteMeta(tenantStateMarker) + `\s+v=(\d+)\s+(.*?)-->`)

// renderTenantStateBlock serializes a tenant's state into the hidden
// HTML-comment block embedded in its comment. The block is the machine-readable
// source of truth; the human-facing summary is rendered separately.
func renderTenantStateBlock(state TenantState) (string, error) {
	payload, err := json.Marshal(state)
	if err != nil {
		return "", fmt.Errorf("marshal tenant state for %q: %w", state.Tenant, err)
	}
	return fmt.Sprintf("<!-- %s v=%d\n%s\n-->", tenantStateMarker, tenantStateVersion, payload), nil
}

// parseTenantStateBlock extracts and parses the tenant-state block from a
// comment body. found is false when the comment carries no block (not an
// error). A present-but-malformed or unsupported-version block returns an error
// so the leader treats the tenant as not-reported (fail-closed) rather than
// silently trusting a bad block.
func parseTenantStateBlock(commentBody string) (state TenantState, found bool, err error) {
	match := tenantStateBlockRE.FindStringSubmatch(commentBody)
	if match == nil {
		return TenantState{}, false, nil
	}
	version, convErr := strconv.Atoi(match[1])
	if convErr != nil {
		return TenantState{}, true, fmt.Errorf("malformed tenant-state block version %q: %w", match[1], convErr)
	}
	if version != tenantStateVersion {
		return TenantState{}, true, fmt.Errorf("unsupported tenant-state block version %d (want %d)", version, tenantStateVersion)
	}
	if err := json.Unmarshal([]byte(match[2]), &state); err != nil {
		return TenantState{}, true, fmt.Errorf("parse tenant-state block: %w", err)
	}
	if err := state.validate(); err != nil {
		return TenantState{}, true, fmt.Errorf("invalid tenant-state block: %w", err)
	}
	return state, true, nil
}

// validate reports whether a parsed TenantState carries the fields a reader
// requires to trust it. A syntactically valid but semantically empty block
// (missing tenant, sha, rollup, or a database row missing its name or state)
// must be rejected so a reader fails closed rather than treating it as a real
// report.
func (s TenantState) validate() error {
	if strings.TrimSpace(s.Tenant) == "" {
		return fmt.Errorf("missing tenant")
	}
	if strings.TrimSpace(s.Environment) == "" {
		return fmt.Errorf("missing environment")
	}
	if strings.TrimSpace(s.SHA) == "" {
		return fmt.Errorf("missing sha")
	}
	if strings.TrimSpace(s.Rollup) == "" {
		return fmt.Errorf("missing rollup")
	}
	for i, db := range s.Databases {
		if strings.TrimSpace(db.Database) == "" {
			return fmt.Errorf("databases[%d] missing db", i)
		}
		if strings.TrimSpace(db.State) == "" {
			return fmt.Errorf("databases[%d] (%s) missing state", i, db.Database)
		}
	}
	return nil
}
