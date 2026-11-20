package api

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
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

	// GitHubTeams are the GitHub teams, as org/team-slug, whose members may
	// approve a freeze override from the pull request it was asked for on.
	// They govern only that approval and need no forward_auth. Empty means
	// nobody approves from a pull request.
	GitHubTeams []string `yaml:"github_teams,omitempty"`
}

// DirectWriteReasonNotFreezeAdmin denies a freeze write to a caller in
// neither a write group nor a freeze group.
const DirectWriteReasonNotFreezeAdmin = "not_freeze_admin"

// DirectWriteReasonFreezeAllow allows a freeze write through a freeze group.
const DirectWriteReasonFreezeAllow = "freeze_allow"

// DirectWriteReasonNotFreezeApprover denies approving a freeze override to a
// caller who is not in a freeze group, write-group members included.
const DirectWriteReasonNotFreezeApprover = "not_freeze_approver"

// DirectWriteReasonPullRequestIdentity denies approving through the API to a
// caller whose subject is in the pull request identity domain.
const DirectWriteReasonPullRequestIdentity = "pull_request_identity"

// validateFreeze rejects freeze groups that could never take effect, and
// malformed or repeated approver teams.
func (c *ServerConfig) validateFreeze() error {
	if err := validateGitHubTeamPrincipals("freeze.github_teams", c.Freeze.GitHubTeams); err != nil {
		return err
	}
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

// AuthorizeFreezeApprove decides whether the caller may approve a freeze
// override request. Only a freeze-group member may: a write group grants
// freeze writes but not approval, so nobody overrides alone, admins included.
func (c *ServerConfig) AuthorizeFreezeApprove(user *auth.User) DirectWriteAuthorizationResult {
	// No lane grants approval by default: with no freeze groups configured,
	// nobody approves, write-group members included.
	if user == nil {
		return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonMissingIdentity}
	}
	// A request from a pull request is approved from a pull request, where the
	// identity is a GitHub login. An API subject spelled like one would pass
	// for that login, so none approves here (AZ-10).
	if storage.IsGitHubIdentity(user.Subject) {
		return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonPullRequestIdentity}
	}
	if matched, ok := auth.MatchedGroup(user.Groups, trimmedNonEmpty(c.Freeze.Groups)); ok {
		return DirectWriteAuthorizationResult{Allowed: true, Reason: DirectWriteReasonFreezeAllow, MatchedPrincipal: matched}
	}
	return DirectWriteAuthorizationResult{Allowed: false, Reason: DirectWriteReasonNotFreezeApprover}
}

// authorizeFreezeApprove enforces AuthorizeFreezeApprove for an HTTP handler
// and reports whether the request may proceed. It writes the denial itself,
// naming the freeze groups.
func (s *Service) authorizeFreezeApprove(w http.ResponseWriter, r *http.Request) bool {
	result := s.config.AuthorizeFreezeApprove(auth.UserFromContext(r.Context()))
	metrics.RecordDirectWriteAuthorization(r.Context(), "freeze_override_approve", "", "",
		directWriteMetricStatus(result), result.Reason)
	if result.Allowed {
		return true
	}
	user := auth.UserFromContext(r.Context())
	subject := ""
	if user != nil {
		subject = user.Subject
	}
	s.logger.Warn("freeze override approval denied", "subject", subject, "reason", result.Reason)
	if result.Reason == DirectWriteReasonPullRequestIdentity {
		s.writeError(w, http.StatusForbidden, "an identity that starts with \""+storage.GitHubIdentityPrefix+"\" cannot approve an override through the API")
		return false
	}
	groups := trimmedNonEmpty(s.config.Freeze.Groups)
	message := "approving an override requires membership in a freeze group (" + strings.Join(groups, ", ") + ")"
	if len(groups) == 0 {
		message = "approving an override requires membership in a freeze group, and none are configured"
	}
	s.writeError(w, http.StatusForbidden, message)
	return false
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
