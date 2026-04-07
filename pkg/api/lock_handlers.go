package api

import (
	"encoding/json"
	"errors"
	"net/http"

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
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Database == "" || req.DatabaseType == "" || req.Owner == "" {
		s.writeError(w, http.StatusBadRequest, "database, database_type, and owner are required")
		return
	}

	lock := &storage.Lock{
		DatabaseName: req.Database,
		DatabaseType: req.DatabaseType,
		Owner:        req.Owner,
		Repository:   req.Repository,
		PullRequest:  req.PullRequest,
	}

	ctx := r.Context()
	err := s.storage.Locks().Acquire(ctx, lock)
	if errors.Is(err, storage.ErrLockHeld) {
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
		s.logger.Error("acquire lock failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	// Refetch to get created_at
	acquired, err := s.storage.Locks().Get(ctx, req.Database, req.DatabaseType)
	if err != nil || acquired == nil {
		// Shouldn't happen, but handle gracefully
		s.writeJSON(w, http.StatusOK, LockResponse{Lock: &LockInfo{
			Database:     req.Database,
			DatabaseType: req.DatabaseType,
			Owner:        req.Owner,
		}})
		return
	}

	s.writeJSON(w, http.StatusOK, LockResponse{Lock: lockToInfo(acquired)})
}

// handleLockRelease handles DELETE /api/locks.
func (s *Service) handleLockRelease(w http.ResponseWriter, r *http.Request) {
	var req LockReleaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.Database == "" || req.DatabaseType == "" {
		s.writeError(w, http.StatusBadRequest, "database and database_type are required")
		return
	}

	ctx := r.Context()

	if req.Force {
		// Force release - no ownership check
		err := s.storage.Locks().ForceRelease(ctx, req.Database, req.DatabaseType)
		if errors.Is(err, storage.ErrLockNotFound) {
			s.writeError(w, http.StatusNotFound, "lock not found")
			return
		}
		if err != nil {
			s.logger.Error("force release lock failed", "error", err)
			s.writeError(w, http.StatusInternalServerError, "internal error")
			return
		}

		s.writeJSON(w, http.StatusOK, map[string]string{"status": "released"})
		return
	}

	// Normal release - requires ownership
	if req.Owner == "" {
		s.writeError(w, http.StatusBadRequest, "owner is required (or use force: true)")
		return
	}

	err := s.storage.Locks().Release(ctx, req.Database, req.DatabaseType, req.Owner)
	if errors.Is(err, storage.ErrLockNotFound) {
		s.writeError(w, http.StatusNotFound, "lock not found")
		return
	}
	if errors.Is(err, storage.ErrLockNotOwned) {
		s.writeError(w, http.StatusForbidden, "lock is not owned by you")
		return
	}
	if err != nil {
		s.logger.Error("release lock failed", "error", err)
		s.writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "released"})
}

// handleLockGet handles GET /api/locks/{database}/{dbtype}.
func (s *Service) handleLockGet(w http.ResponseWriter, r *http.Request) {
	database := r.PathValue("database")
	dbType := r.PathValue("dbtype")

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
