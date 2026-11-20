package api

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/block/schemabot/pkg/storage"
)

// PullRequestApproval is a freeze approver's comment on a pull request
// approving an override request.
type PullRequestApproval struct {
	// Login is the commenter's GitHub login.
	Login string
	// Repository and PullRequest name the pull request the comment is on.
	Repository  string
	PullRequest int
	// ID is the override request the comment names.
	ID string
	// IsApprover reports whether Login may approve from a pull request. The
	// webhook supplies it, as it owns the GitHub client that reads team
	// membership. Nil, or false, refuses; an error refuses too, never failing
	// open.
	IsApprover func(ctx context.Context) (bool, error)
}

// ErrInvalidOverrideRequestID is the cause of a refusal because the ID does
// not have the shape of an override request ID.
var ErrInvalidOverrideRequestID = errors.New("not an override request ID")

// ErrNotPullRequestApprover is the cause of a refusal because the commenter
// may not approve overrides from a pull request.
var ErrNotPullRequestApprover = errors.New("the commenter is not a freeze approver")

// ErrPullRequestApproverUnchecked is the cause of a failure because the
// commenter's standing could not be read. Nothing was approved.
var ErrPullRequestApproverUnchecked = errors.New("could not check whether the commenter is a freeze approver")

// PullRequestApprovalRefusedError is a refusal whose Message is fit to show
// the commenter. The other errors from ApprovePullRequestOverride are
// internal failures with nothing to show.
type PullRequestApprovalRefusedError struct {
	Message string
	// Cause is the sentinel the refusal stands for, when it has one.
	Cause error
}

func (e *PullRequestApprovalRefusedError) Error() string { return e.Message }
func (e *PullRequestApprovalRefusedError) Unwrap() error { return e.Cause }

func refusedApproval(cause error, format string, args ...any) error {
	return &PullRequestApprovalRefusedError{Message: fmt.Sprintf(format, args...), Cause: cause}
}

// overrideIDPattern is the shape engine.NewFreezeOverrideID mints. It is
// checked before any storage call so that a stray word never reaches a query.
var overrideIDPattern = regexp.MustCompile(`^ovr-[0-9a-z]{1,64}$`)

// ApprovePullRequestOverride approves the override request a pull request
// comment names, as "github:<login>", and returns the approved request.
//
// Every rule is decided here, so none depends on the caller (AZ-10):
//   - the ID has the shape of an override request ID;
//   - the commenter is a freeze approver (in.IsApprover), checked once the
//     request exists so a stranger learns nothing about it;
//   - the request was asked for on a pull request, the reverse of the HTTP
//     approval route, which takes only API and CLI requests;
//   - that pull request is this one, in this repository;
//   - storage refuses the requester and the pull request's author, a request
//     already approved and one that expired.
func (s *Service) ApprovePullRequestOverride(ctx context.Context, in PullRequestApproval) (*storage.FreezeOverrideRequest, error) {
	login := strings.TrimSpace(in.Login)
	if login == "" || in.Login != login {
		return nil, errors.New("approve override from a pull request: the commenter is unknown")
	}
	if !overrideIDPattern.MatchString(in.ID) {
		// The word is the commenter's, so the reply does not repeat it.
		return nil, refusedApproval(ErrInvalidOverrideRequestID, "That is not an override request ID; they look like `ovr-1a2b3c4d5e6f7a8b`.")
	}
	if s.storage == nil || s.storage.FreezeOverrides() == nil {
		return nil, errors.New("approve override from a pull request: freeze override storage is not available")
	}
	overrides := s.storage.FreezeOverrides()

	request, err := overrides.Get(ctx, in.ID)
	switch {
	case errors.Is(err, storage.ErrFreezeOverrideNotFound):
		return nil, refusedApproval(storage.ErrFreezeOverrideNotFound, "Override request `%s` was not found.", in.ID)
	case err != nil:
		return nil, fmt.Errorf("read override request %s: %w", in.ID, err)
	}
	if in.IsApprover == nil {
		return nil, refusedApproval(ErrNotPullRequestApprover, "@%s may not approve an override from a pull request.", login)
	}
	approver, err := in.IsApprover(ctx)
	switch {
	case err != nil:
		return nil, fmt.Errorf("%w: %w", ErrPullRequestApproverUnchecked, err)
	case !approver:
		return nil, refusedApproval(ErrNotPullRequestApprover, "@%s may not approve an override from a pull request.", login)
	}
	switch {
	case !request.FromPullRequest():
		return nil, refusedApproval(nil, "Override request `%s` was asked for through the API or CLI, so it is not approved here. Approve it with `schemabot freeze approve %s` in the CLI.", in.ID, in.ID)
	case !strings.EqualFold(request.Repository, in.Repository) || request.PullRequest != in.PullRequest:
		return nil, refusedApproval(nil, "Override request `%s` belongs to %s#%d. Approve it there.", in.ID, request.Repository, request.PullRequest)
	}

	identity := storage.GitHubIdentity(login)
	approved, err := overrides.Approve(ctx, in.ID, identity)
	switch {
	case errors.Is(err, storage.ErrFreezeOverrideNotFound):
		return nil, refusedApproval(storage.ErrFreezeOverrideNotFound, "Override request `%s` was not found.", in.ID)
	case errors.Is(err, storage.ErrFreezeOverrideSelfApproval):
		return nil, refusedApproval(nil, "An override needs an approver other than the person who asked for it and the pull request's author.")
	case errors.Is(err, storage.ErrFreezeOverrideAlreadyApproved):
		return nil, refusedApproval(nil, "Override request `%s` is already approved.", in.ID)
	case errors.Is(err, storage.ErrFreezeOverrideExpired):
		return nil, refusedApproval(nil, "Override request `%s` expired. The requester can ask again.", in.ID)
	case err != nil:
		return nil, fmt.Errorf("approve override request %s as %s: %w", in.ID, identity, err)
	}
	s.logger.Info("freeze override request approved from a pull request",
		"override_request", approved.Identifier, "approver", identity, "requested_by", approved.RequestedBy,
		"repository", approved.Repository, "pull_request", approved.PullRequest,
		"environment", approved.Environment, "action", approved.Action, "target_id", approved.TargetID,
		"blockers", approved.Blockers, "reason", approved.Reason)
	return approved, nil
}
