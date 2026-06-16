package auth

import (
	"net/http"
	"strings"

	"github.com/block/schemabot/pkg/metrics"
)

// Tier is the access level an API endpoint requires.
type Tier int

const (
	// TierRead is read-only / visibility access: status, progress, logs, locks
	// list, history, and reading live schema (pull). Any valid token may use it.
	TierRead Tier = iota
	// TierWrite covers everything that stages or makes a change: plan, apply,
	// cutover, control operations, locks, and settings. It requires membership
	// in a configured admin group — the CLI/direct-API write path is for a small
	// privileged set; general users go through the PR-comment workflow instead.
	// Planning is included deliberately — running a plan stages a change against
	// a specific database and reads its live schema, so it belongs to the change
	// workflow, not open read access. Viewing existing plan results and reading a
	// schema (pull) stay in TierRead.
	TierWrite
)

func (t Tier) String() string {
	switch t {
	case TierRead:
		return "read"
	case TierWrite:
		return "write"
	default:
		return "unknown"
	}
}

// readPaths are non-GET endpoints that are nonetheless read-only — they expose
// information without staging a change. `pull` exports a database's live schema
// for visibility; it is not a step toward modifying anything.
var readPaths = map[string]bool{
	"/api/pull": true,
}

// tierForRequest classifies an API request into the access tier it requires.
// GET/HEAD requests and the explicit read-only endpoints are read; everything
// else is write, so a newly added mutating-looking endpoint fails closed
// (requires authorization) until it is classified here.
func tierForRequest(method, path string) Tier {
	if readPaths[path] {
		return TierRead
	}
	switch method {
	case http.MethodGet, http.MethodHead:
		return TierRead
	default:
		return TierWrite
	}
}

// teamSlug returns the last path segment of a group/team name. Some identity
// providers emit a bare group name (e.g. "schema-admins") while configuration
// names the org-qualified team (e.g. "org/schema-admins"); comparing slugs lets
// those line up. See groupMatches for how the bridge is bounded.
func teamSlug(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}

// groupMatches compares one caller group against one configured group. They
// match on an exact string, or by slug only when at least one side is a bare
// slug (no "/"). The bare-slug condition is important: it bridges a provider
// that emits "schema-admins" to a configured "org/schema-admins", without
// letting "org-a/admins" match "org-b/admins" across organization boundaries.
func groupMatches(callerGroup, configured string) bool {
	if callerGroup == configured {
		return true
	}
	if !strings.Contains(callerGroup, "/") || !strings.Contains(configured, "/") {
		return teamSlug(callerGroup) == teamSlug(configured)
	}
	return false
}

// matchesAnyGroup reports whether any of the caller's groups matches any of the
// configured groups (see groupMatches).
func matchesAnyGroup(callerGroups, configured []string) bool {
	for _, cg := range callerGroups {
		for _, want := range configured {
			if groupMatches(cg, want) {
				return true
			}
		}
	}
	return false
}

// authDecision records an API auth decision metric for the request.
func authDecision(r *http.Request, tier Tier, decision, reason string) {
	metrics.RecordAuthDecision(r.Context(), tier.String(), decision, reason)
}
