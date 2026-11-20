package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/auth"
)

// FreezeConfig configures environment freezes.
type FreezeConfig struct {
	// Groups are the caller groups, besides the deployment write groups,
	// that may set and lift freezes. A member of a freeze group gains no
	// other write: every other mutating route still requires a write group
	// or a database operator grant. Empty leaves freezes to the write groups.
	// Requires auth type forward_auth, the authenticator that forwards caller
	// groups.
	Groups []string `yaml:"groups,omitempty"`
}

// DirectWriteReasonNotFreezeAdmin denies a freeze write to a caller in
// neither a write group nor a freeze group.
const DirectWriteReasonNotFreezeAdmin = "not_freeze_admin"

// DirectWriteReasonFreezeAllow allows a freeze write through a freeze group.
const DirectWriteReasonFreezeAllow = "freeze_allow"

// validateFreeze rejects freeze groups that could never take effect.
func (c *ServerConfig) validateFreeze() error {
	groups := trimmedNonEmpty(c.Freeze.Groups)
	if len(groups) == 0 {
		return nil
	}
	if c.Auth.Type != "forward_auth" {
		return fmt.Errorf("freeze.groups requires auth type forward_auth (got %q): no other authenticator forwards the caller groups the grant matches against", c.Auth.Type)
	}
	return validateUniqueNames("freeze.groups", groups)
}

// ServesEnvironment reports whether this instance stores and acts on freezes
// of env: the environment is allowed here and at least one configured
// database routes it. A peer instance's environment is not served, even when
// the fleet-wide promotion order names it (AZ-2).
func (c *ServerConfig) ServesEnvironment(env string) bool {
	return c.IsEnvironmentAllowed(env) && c.environmentConfiguredOnAnyDatabase(env)
}

// ScopedWriteGroupUnion returns the sorted, deduplicated union of the database
// operator groups and the freeze groups. The forward-auth middleware widens
// write-tier admission to this union; each handler then decides what the
// caller's group grants.
func (c *ServerConfig) ScopedWriteGroupUnion() []string {
	seen := make(map[string]struct{})
	var union []string
	for _, group := range append(c.OperatorGroupUnion(), trimmedNonEmpty(c.Freeze.Groups)...) {
		if _, ok := seen[group]; ok {
			continue
		}
		seen[group] = struct{}{}
		union = append(union, group)
	}
	sort.Strings(union)
	return union
}

// AuthorizeFreezeWrite decides whether the caller may set or lift a freeze.
// Write-group members may, as may freeze-group members; database operator
// grants do not cover freezes, which reach past any one database.
func (c *ServerConfig) AuthorizeFreezeWrite(user *auth.User) DirectWriteAuthorizationResult {
	result, decided := c.directWriteCommonDecision(user)
	if decided {
		return result
	}
	if matched, ok := auth.MatchedGroup(user.Groups, trimmedNonEmpty(c.Freeze.Groups)); ok {
		return DirectWriteAuthorizationResult{Allowed: true, Reason: DirectWriteReasonFreezeAllow, MatchedPrincipal: matched}
	}
	return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonNotFreezeAdmin}
}

// authorizeFreezeWrite enforces AuthorizeFreezeWrite for an HTTP handler and
// reports whether the request may proceed.
func (s *Service) authorizeFreezeWrite(w http.ResponseWriter, r *http.Request, operation string) bool {
	result := s.config.AuthorizeFreezeWrite(auth.UserFromContext(r.Context()))
	return s.finishDirectWriteDecision(w, r, operation, "", "", result)
}

// freezeDenialMessage names the groups that may set and lift freezes.
func (s *Service) freezeDenialMessage(operation string) string {
	groups := append(trimmedNonEmpty(s.config.Auth.ForwardAuth.WriteGroups), trimmedNonEmpty(s.config.Freeze.Groups)...)
	return fmt.Sprintf("%s requires membership in a deployment write group or a freeze group (%s)",
		operation, strings.Join(groups, ", "))
}
