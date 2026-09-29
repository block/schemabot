package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/storage"
)

// LockAcquireRequest is the request body for POST /api/locks/acquire.
type LockAcquireRequest struct {
	Database     string `json:"database"`
	DatabaseType string `json:"database_type"`
	Owner        string `json:"owner"`
	Repository   string `json:"repository,omitempty"`
	PullRequest  int    `json:"pull_request,omitempty"`
}

// LockReleaseRequest is the request body for DELETE /api/locks.
type LockReleaseRequest struct {
	Database     string `json:"database"`
	DatabaseType string `json:"database_type"`
	Owner        string `json:"owner,omitempty"`
	Force        bool   `json:"force,omitempty"`
}

// LockResponse is the response for lock operations.
type LockResponse struct {
	Lock *LockInfo `json:"lock,omitempty"`
}

// LockInfo represents lock information in API responses.
type LockInfo struct {
	Database     string `json:"database"`
	DatabaseType string `json:"database_type"`
	Owner        string `json:"owner"`
	Repository   string `json:"repository,omitempty"`
	PullRequest  int    `json:"pull_request,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
	UpdatedAt    string `json:"updated_at,omitempty"`
}

// LockListResponse is the response for GET /api/locks.
type LockListResponse struct {
	Locks []*LockInfo `json:"locks"`
}

// LockConflictResponse is returned when a lock is already held.
type LockConflictResponse struct {
	Error       string    `json:"error"`
	CurrentLock *LockInfo `json:"current_lock"`
}

// handleLockAcquire handles POST /api/locks/acquire.
func (s *Service) handleLockAcquire(w http.ResponseWriter, r *http.Request) {
	var req LockAcquireRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	req.Database = storage.CanonicalKey(req.Database)
	req.DatabaseType = storage.CanonicalKey(req.DatabaseType)
	req.Repository = storage.CanonicalKey(req.Repository)
	// The owner is a match token: release and conditional-release compare it
	// byte-exact against the stored value, so both sides must fold or a
	// mixed-case spelling (a CLI owner embedding a mixed-case hostname)
	// could acquire a lock it can never release.
	req.Owner = storage.CanonicalKey(req.Owner)

	if req.Database == "" || req.DatabaseType == "" || req.Owner == "" {
		s.writeError(w, http.StatusBadRequest, "database, database_type, and owner are required")
		return
	}

	// A named lock is an operator control on the database itself (holding
	// applies off), in the same family as stop/cancel, so a database's
	// operator grant covers it. Locks have no environment dimension.
	authorization, allowed := s.authorizeDirectDatabaseWrite(w, r, "lock_acquire", req.Database)
	if !allowed {
		return
	}

	ctx := r.Context()
	acquirer, noAcquirerReason := s.config.lockAcquirer(ctx, req.Database)
	if acquirer == nil {
		s.logger.Debug("lock acquire records no acquirer",
			"database", req.Database, "database_type", req.DatabaseType, "owner", req.Owner,
			"reason", noAcquirerReason)
	}

	// Acquiring a lock the same owner already holds succeeds, so the owner
	// string alone would tell anyone who listed locks that they hold one. A
	// scoped operator is held to the lock's recorded acquirer instead (see
	// scopedLockAcquireHeld), which needs the row as it stood before this
	// acquire to tell a lock this request created from one it found.
	scoped := !holdsLockByOwnerAlone(authorization.Reason)
	var before *storage.Lock
	if scoped {
		var err error
		before, err = s.storage.Locks().Get(ctx, req.Database, req.DatabaseType)
		if err != nil {
			metrics.RecordLockOperation(ctx, "acquire", req.Database, "error")
			s.logger.Error("scoped lock acquire could not read the lock; nothing was acquired",
				"repository", req.Repository, "database", req.Database, "database_type", req.DatabaseType,
				"owner", req.Owner, "error", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}

	lock := &storage.Lock{
		DatabaseName: req.Database,
		DatabaseType: req.DatabaseType,
		Owner:        req.Owner,
		Repository:   req.Repository,
		PullRequest:  req.PullRequest,
		Acquirer:     acquirer,
	}

	err := s.storage.Locks().Acquire(ctx, lock)
	if errors.Is(err, storage.ErrLockHeld) {
		metrics.RecordLockOperation(ctx, "acquire", req.Database, "conflict")
		// Lock is held by someone else - return the current lock info
		existing, getErr := s.storage.Locks().Get(ctx, req.Database, req.DatabaseType)
		if getErr != nil || existing == nil {
			s.writeError(w, http.StatusConflict, "lock is already held")
			return
		}

		s.writeJSON(w, http.StatusConflict, LockConflictResponse{
			Error:       "lock is already held by another owner",
			CurrentLock: lockToInfo(existing),
		})
		return
	}
	if err != nil {
		metrics.RecordLockOperation(ctx, "acquire", req.Database, "error")
		s.logger.Error("acquire lock failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Refetch to get created_at
	acquired, err := s.storage.Locks().Get(ctx, req.Database, req.DatabaseType)
	if err != nil || acquired == nil {
		// Shouldn't happen, but handle gracefully
		metrics.RecordLockOperation(ctx, "acquire", req.Database, "success")
		s.writeJSON(w, http.StatusOK, LockResponse{Lock: &LockInfo{
			Database:     req.Database,
			DatabaseType: req.DatabaseType,
			Owner:        req.Owner,
		}})
		return
	}
	if scoped && !s.scopedLockAcquireHeld(w, r, req, before, acquired, acquirer) {
		return
	}
	metrics.RecordLockOperation(ctx, "acquire", req.Database, "success")

	s.writeJSON(w, http.StatusOK, LockResponse{Lock: lockToInfo(acquired)})
}

// scopedLockAcquireHeld decides whether a scoped operator whose acquire
// succeeded may be told they hold acquired, the lock row as read after it, and
// on refusal writes the 403 and reports false. The caller holds a row this
// request created, recognized by carrying the acquirer the request recorded on
// a row that was not there before it; any other row is one they found, and they
// hold it only under the rule that decides who may release it (see
// scopedLockRefusal). Deciding after the acquire leaves a refused caller's
// target lock as it was: this endpoint carries no pending plan, so a same-owner
// acquire writes nothing, and deciding on the row as read afterwards also covers
// a lock another group took under the same owner while this acquire ran.
func (s *Service) scopedLockAcquireHeld(w http.ResponseWriter, r *http.Request, req LockAcquireRequest, before, acquired *storage.Lock, acquirer *storage.LockAcquirer) bool {
	if lockCreatedByAcquire(before, acquired, acquirer) {
		return true
	}
	ctx := r.Context()
	user := auth.UserFromContext(ctx)
	callerGroups := s.config.callerOperatorGroups(user, req.Database)
	refusal := scopedLockRefusal(acquired.Acquirer, callerGroups)
	if refusal == "" {
		return true
	}

	subject := ""
	if user != nil {
		subject = user.Subject
	}
	metrics.RecordLockOperation(ctx, "acquire", req.Database, "not_owned")
	attrs := []any{
		"repository", acquired.Repository, "database", acquired.DatabaseName, "database_type", acquired.DatabaseType,
		"owner", acquired.Owner, "subject", subject, "reason", refusal, "caller_operator_groups", callerGroups,
	}
	if acquired.Acquirer != nil {
		attrs = append(attrs, "acquirer_subject", acquired.Acquirer.Subject,
			"acquirer_operator_groups", acquired.Acquirer.OperatorGroups)
	}
	s.logger.Warn("scoped lock acquire refused: the lock is already held under this owner and does not belong to any of the caller's operator groups; the lock is unchanged", attrs...)
	s.writeError(w, http.StatusForbidden, s.scopedLockDenialMessage("lock_acquire", "holding", req.Database, acquired.Acquirer, refusal))
	return false
}

// lockCreatedByAcquire reports whether acquired, the lock row read after an
// acquire, is the row that acquire created: a row that was not there before it
// (before is the row read first, or nil), carrying the acquirer it recorded.
func lockCreatedByAcquire(before, acquired *storage.Lock, acquirer *storage.LockAcquirer) bool {
	if before != nil && before.ID == acquired.ID {
		return false
	}
	return sameLockAcquirer(acquired.Acquirer, acquirer)
}

// sameLockAcquirer reports whether two recorded acquirers are the same record:
// both unrecorded, or the same subject with the same operator groups.
func sameLockAcquirer(a, b *storage.LockAcquirer) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Subject == b.Subject && slices.Equal(a.OperatorGroups, b.OperatorGroups)
}

// handleLockRelease handles DELETE /api/locks.
func (s *Service) handleLockRelease(w http.ResponseWriter, r *http.Request) {
	var req LockReleaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	req.Database = storage.CanonicalKey(req.Database)
	req.DatabaseType = storage.CanonicalKey(req.DatabaseType)
	// Folded to match the folded spelling stored at acquire; see
	// handleLockAcquire.
	req.Owner = storage.CanonicalKey(req.Owner)

	if req.Database == "" || req.DatabaseType == "" {
		s.writeError(w, http.StatusBadRequest, "database and database_type are required")
		return
	}

	// Force release bypasses the lock ownership check, so it is an
	// administrative override rather than an operator control on the caller's
	// own database: a database operator grant does not cover it. Normal
	// release keeps the ownership check, so the operator grant applies, and a
	// scoped operator's release is further held to the lock's recorded
	// acquirer (see releaseScopedLock).
	var authorization DirectWriteAuthorizationResult
	if req.Force {
		if !s.authorizeDirectForceLockRelease(w, r, req.Database) {
			return
		}
	} else {
		authorization = s.config.AuthorizeDirectDatabaseWrite(auth.UserFromContext(r.Context()), req.Database)
		if !s.finishDirectWriteDecision(w, r, "lock_release", req.Database, "", authorization) {
			return
		}
	}

	ctx := r.Context()

	if req.Force {
		// Force release - no ownership check
		err := s.storage.Locks().ForceRelease(ctx, req.Database, req.DatabaseType)
		if errors.Is(err, storage.ErrLockNotFound) {
			metrics.RecordLockOperation(ctx, "release", req.Database, "not_found")
			s.writeError(w, http.StatusNotFound, "lock not found")
			return
		}
		if err != nil {
			metrics.RecordLockOperation(ctx, "release", req.Database, "error")
			s.logger.Error("force release lock failed", "error", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		metrics.RecordLockOperation(ctx, "release", req.Database, "success")

		s.writeJSON(w, http.StatusOK, map[string]string{"status": "released"})
		return
	}

	// Normal release - requires ownership
	if req.Owner == "" {
		s.writeError(w, http.StatusBadRequest, "owner is required (or use force: true)")
		return
	}

	// The owner string is readable by anyone who can list locks, so it proves
	// nothing about who took the lock. Only a deployment write-group member,
	// or a caller on a deployment without scoped grants, releases by owner
	// alone; every other allowed caller is held to the lock's recorded
	// acquirer, so a grant added later is scoped until it is decided otherwise.
	if !holdsLockByOwnerAlone(authorization.Reason) {
		s.releaseScopedLock(w, r, req)
		return
	}

	err := s.storage.Locks().Release(ctx, req.Database, req.DatabaseType, req.Owner)
	if errors.Is(err, storage.ErrLockNotFound) {
		metrics.RecordLockOperation(ctx, "release", req.Database, "not_found")
		s.writeError(w, http.StatusNotFound, "lock not found")
		return
	}
	if errors.Is(err, storage.ErrLockNotOwned) {
		metrics.RecordLockOperation(ctx, "release", req.Database, "not_owned")
		s.writeErrorCode(w, http.StatusForbidden, apitypes.ErrCodeLockNotOwned, "lock is not owned by you")
		return
	}
	if err != nil {
		metrics.RecordLockOperation(ctx, "release", req.Database, "error")
		s.logger.Error("release lock failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	metrics.RecordLockOperation(ctx, "release", req.Database, "success")

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "released"})
}

// Reasons a scoped operator is refused a lock release, or a re-acquire of a
// lock already held, although the owner matches, for the refusal log. The lock
// operations counter records every one of them as not_owned: ownership is per
// operator group, and the caller's group does not own the lock.
const (
	lockRefusalAcquirerUnrecorded         = "acquirer_unrecorded"
	lockRefusalAcquirerNoOperatorGroup    = "acquirer_no_operator_group"
	lockRefusalAcquirerOtherOperatorGroup = "acquirer_other_operator_group"
)

// holdsLockByOwnerAlone reports whether a caller allowed under reason holds a
// lock, to re-acquire or release it, on the strength of its owner string. Only
// the deployment write groups and a deployment that grants no operator groups
// hold a lock that way; any other allowed decision, including one this
// function has not been taught about, is held to the lock's recorded acquirer.
func holdsLockByOwnerAlone(reason string) bool {
	return reason == DirectWriteReasonAdminAllow || reason == DirectWriteReasonScopedLaneDisabled
}

// releaseScopedLock releases a lock for a scoped (non-admin) operator. A
// scoped operator may release a lock only when the lock's recorded acquirer
// shared at least one of the database's operator groups with them: ownership
// is per operator group, so a teammate can release a lock another member of
// the same group took, and nobody outside the group can, whatever owner string
// they send.
//
// The check reads the lock, decides against the acquirer recorded on that
// row, and then deletes only that row (ReleaseByID). The acquirer never
// changes under a row ID, so the decision cannot go stale between the read and
// the delete; a lock released and acquired again in between is reported, and
// the new lock is left held.
func (s *Service) releaseScopedLock(w http.ResponseWriter, r *http.Request, req LockReleaseRequest) {
	ctx := r.Context()
	user := auth.UserFromContext(ctx)
	subject := ""
	if user != nil {
		subject = user.Subject
	}

	lock, err := s.storage.Locks().Get(ctx, req.Database, req.DatabaseType)
	if err != nil {
		metrics.RecordLockOperation(ctx, "release", req.Database, "error")
		s.logger.Error("scoped lock release could not read the lock; nothing was released",
			"database", req.Database, "database_type", req.DatabaseType, "owner", req.Owner,
			"subject", subject, "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if lock == nil {
		metrics.RecordLockOperation(ctx, "release", req.Database, "not_found")
		s.logger.Info("scoped lock release found no lock",
			"database", req.Database, "database_type", req.DatabaseType, "owner", req.Owner, "subject", subject)
		s.writeError(w, http.StatusNotFound, "lock not found")
		return
	}

	logAttrs := []any{
		"repository", lock.Repository, "database", lock.DatabaseName, "database_type", lock.DatabaseType,
		"owner", lock.Owner, "subject", subject,
	}
	if lock.Owner != req.Owner {
		metrics.RecordLockOperation(ctx, "release", req.Database, "not_owned")
		s.logger.Warn("scoped lock release refused: the lock is held under another owner",
			append(logAttrs, "requested_owner", req.Owner)...)
		s.writeErrorCode(w, http.StatusForbidden, apitypes.ErrCodeLockNotOwned, "lock is not owned by you")
		return
	}

	callerGroups := s.config.callerOperatorGroups(user, req.Database)
	if refusal := scopedLockRefusal(lock.Acquirer, callerGroups); refusal != "" {
		metrics.RecordLockOperation(ctx, "release", req.Database, "not_owned")
		attrs := slices.Concat(logAttrs, []any{"reason", refusal, "caller_operator_groups", callerGroups})
		if lock.Acquirer != nil {
			attrs = append(attrs, "acquirer_subject", lock.Acquirer.Subject,
				"acquirer_operator_groups", lock.Acquirer.OperatorGroups)
		}
		s.logger.Warn("scoped lock release refused: the lock does not belong to any of the caller's operator groups; the lock stays held", attrs...)
		s.writeError(w, http.StatusForbidden, s.scopedLockDenialMessage("lock_release", "releasing", req.Database, lock.Acquirer, refusal))
		return
	}

	err = s.storage.Locks().ReleaseByID(ctx, lock.ID, req.Database, req.DatabaseType, req.Owner)
	if errors.Is(err, storage.ErrLockNotFound) {
		metrics.RecordLockOperation(ctx, "release", req.Database, "not_found")
		s.logger.Info("scoped lock release found the lock already released", logAttrs...)
		s.writeError(w, http.StatusNotFound, "lock not found")
		return
	}
	if errors.Is(err, storage.ErrLockReplaced) {
		metrics.RecordLockOperation(ctx, "release", req.Database, "conflict")
		s.logger.Warn("scoped lock release refused: the lock was released and acquired again after it was checked; the new lock stays held",
			logAttrs...)
		s.writeError(w, http.StatusConflict,
			fmt.Sprintf("lock on database %q was released and acquired again while this release ran; nothing was released, check the lock and retry", req.Database))
		return
	}
	if errors.Is(err, storage.ErrLockNotOwned) {
		metrics.RecordLockOperation(ctx, "release", req.Database, "not_owned")
		s.logger.Warn("scoped lock release refused: the checked lock row is held under another owner", logAttrs...)
		s.writeErrorCode(w, http.StatusForbidden, apitypes.ErrCodeLockNotOwned, "lock is not owned by you")
		return
	}
	if err != nil {
		metrics.RecordLockOperation(ctx, "release", req.Database, "error")
		s.logger.Error("scoped lock release failed; the lock may still be held", append(logAttrs, "error", err)...)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	metrics.RecordLockOperation(ctx, "release", req.Database, "success")
	s.logger.Info("scoped lock release released the lock", logAttrs...)
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "released"})
}

// scopedLockRefusal reports why a scoped operator holding callerGroups (their
// operator groups on the lock's database) may not release, or be told they
// hold, a lock taken by acquirer, or "" when they may. A lock with no recorded
// acquirer shares a group with nobody, so it is refused rather than read as
// anyone's.
func scopedLockRefusal(acquirer *storage.LockAcquirer, callerGroups []string) string {
	if acquirer == nil {
		return lockRefusalAcquirerUnrecorded
	}
	if len(acquirer.OperatorGroups) == 0 {
		return lockRefusalAcquirerNoOperatorGroup
	}
	if !sharesOperatorGroup(acquirer.OperatorGroups, callerGroups) {
		return lockRefusalAcquirerOtherOperatorGroup
	}
	return ""
}

// sharesOperatorGroup reports whether the two operator-group lists, both by
// configured name, have a group in common.
func sharesOperatorGroup(acquirerGroups, callerGroups []string) bool {
	return slices.ContainsFunc(acquirerGroups, func(g string) bool { return slices.Contains(callerGroups, g) })
}

// scopedLockDenialMessage explains a scoped operation refused on a lock
// (operation, and action as the gerund naming what the grant does not cover)
// and names who may release the lock instead, so the caller knows who to ask.
// Group names are visible only to callers already authenticated behind the
// trusted proxy.
func (s *Service) scopedLockDenialMessage(operation, action, database string, acquirer *storage.LockAcquirer, refusal string) string {
	writeGroups := strings.Join(s.config.Auth.ForwardAuth.WriteGroups, ", ")
	switch refusal {
	case lockRefusalAcquirerUnrecorded:
		return fmt.Sprintf("%s on database %q: the lock records no verified acquirer, so no operator grant covers %s it; a deployment write group (%s) may release it",
			operation, database, action, writeGroups)
	case lockRefusalAcquirerNoOperatorGroup:
		return fmt.Sprintf("%s on database %q: the lock was acquired by a caller in none of the database's operator groups, so no operator grant covers %s it; a deployment write group (%s) may release it",
			operation, database, action, writeGroups)
	case lockRefusalAcquirerOtherOperatorGroup:
		return fmt.Sprintf("%s on database %q: the lock was acquired under operator groups (%s), and you are a member of none of them; a member of one of those groups or of a deployment write group (%s) may release it",
			operation, database, strings.Join(acquirer.OperatorGroups, ", "), writeGroups)
	default:
		return fmt.Sprintf("%s on database %q: your operator grant does not cover %s this lock; a deployment write group (%s) may release it",
			operation, database, action, writeGroups)
	}
}

// callerOperatorGroups returns every configured operator group of database
// the caller is a member of, by configured name, sorted. It is empty when the
// caller holds none, and when there is no caller or no configuration for the
// database, since neither grants any group.
func (c *ServerConfig) callerOperatorGroups(user *auth.User, database string) []string {
	if user == nil {
		return []string{}
	}
	dbConfig, ok := c.DatabaseConfigs()[database]
	if !ok {
		return []string{}
	}
	return auth.MatchedGroups(user.Groups, trimmedNonEmpty(dbConfig.OperatorGroups))
}

// handleLockGet handles GET /api/locks/{database}/{dbtype}.
func (s *Service) handleLockGet(w http.ResponseWriter, r *http.Request) {
	database := storage.CanonicalKey(r.PathValue("database"))
	dbType := storage.CanonicalKey(r.PathValue("dbtype"))

	if database == "" || dbType == "" {
		s.writeError(w, http.StatusBadRequest, "database and dbtype path parameters are required")
		return
	}

	lock, err := s.storage.Locks().Get(r.Context(), database, dbType)
	if err != nil {
		s.logger.Error("get lock failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	if lock == nil {
		s.writeError(w, http.StatusNotFound, "lock not found")
		return
	}

	s.writeJSON(w, http.StatusOK, LockResponse{Lock: lockToInfo(lock)})
}

// handleLockList handles GET /api/locks.
func (s *Service) handleLockList(w http.ResponseWriter, r *http.Request) {
	locks, err := s.storage.Locks().List(r.Context())
	if err != nil {
		s.logger.Error("list locks failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	infos := make([]*LockInfo, len(locks))
	for i, lock := range locks {
		infos[i] = lockToInfo(lock)
	}

	s.writeJSON(w, http.StatusOK, LockListResponse{Locks: infos})
}

// lockAcquirer returns the verified caller to record on a lock being acquired
// on database: the caller's subject and every operator group of that database
// they are a member of, by configured name. Whether a scoped operator shares a
// grant with a lock's acquirer is decided against this record, so it holds all
// of the caller's operator groups for the database, not only the one that
// authorized the acquire, and an empty list when they hold none (a deployment
// admin acting through a write group alone shares a grant with no operator).
//
// It returns nil, with the reason, when there is no verified caller to record:
// on a deployment with no scoped operator grants configured there is no grant
// to share, and a request without an authenticated identity names nobody.
func (c *ServerConfig) lockAcquirer(ctx context.Context, database string) (*storage.LockAcquirer, string) {
	if !c.scopedWriteEnabled() {
		return nil, DirectWriteReasonScopedLaneDisabled
	}
	subject, ok := auth.VerifiedSubject(ctx)
	if !ok {
		if _, authenticated := auth.AuthenticatedSubject(ctx); authenticated {
			return nil, DirectWriteReasonUnverifiedIdentity
		}
		return nil, DirectWriteReasonMissingIdentity
	}
	groups := c.callerOperatorGroups(auth.UserFromContext(ctx), database)
	return &storage.LockAcquirer{Subject: subject, OperatorGroups: groups}, ""
}

// lockToInfo converts a storage.Lock to an API LockInfo.
func lockToInfo(lock *storage.Lock) *LockInfo {
	info := &LockInfo{
		Database:     lock.DatabaseName,
		DatabaseType: lock.DatabaseType,
		Owner:        lock.Owner,
		Repository:   lock.Repository,
		PullRequest:  lock.PullRequest,
	}
	if !lock.CreatedAt.IsZero() {
		info.CreatedAt = lock.CreatedAt.Format("2006-01-02T15:04:05Z07:00")
	}
	if !lock.UpdatedAt.IsZero() {
		info.UpdatedAt = lock.UpdatedAt.Format("2006-01-02T15:04:05Z07:00")
	}
	return info
}
