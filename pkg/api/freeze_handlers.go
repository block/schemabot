package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

const (
	// freezeRecentDefault and freezeRecentMax bound the recent list of
	// GET /api/freezes.
	freezeRecentDefault = 20
	freezeRecentMax     = 200
)

// handleFreezeCreate sets an environment freeze. While it is in effect,
// SchemaBot refuses changes in its scope.
func (s *Service) handleFreezeCreate(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeFreezeWrite(w, r, "freeze_create") {
		return
	}
	if !s.freezeStorageReady(w) {
		return
	}
	var req apitypes.CreateFreezeRequest
	if err := decodeStrictJSONBody(r, &req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	now, ok := s.freezeNow(r.Context(), w)
	if !ok {
		return
	}
	freeze, err := s.freezeFromRequest(r.Context(), &req, now)
	if err != nil {
		s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, err.Error())
		return
	}
	created, err := s.storage.Freezes().Create(r.Context(), freeze)
	if err != nil {
		if errors.Is(err, storage.ErrFreezeExists) {
			s.writeError(w, http.StatusConflict, "freeze already exists")
			return
		}
		s.logger.Error("create freeze failed", "freeze_id", freeze.Identifier, "environment", freeze.Environment, "error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "create freeze failed")
		return
	}
	s.logger.Info("environment freeze set", freezeLogArgs(created, created.CreatedBy)...)
	s.writeJSON(w, http.StatusCreated, freezeResponse(created, now))
}

// freezeFromRequest validates a create request and fills its defaults.
func (s *Service) freezeFromRequest(ctx context.Context, req *apitypes.CreateFreezeRequest, now time.Time) (*storage.EnvironmentFreeze, error) {
	config := s.config.withDatabaseSnapshot()
	environment := storage.CanonicalKey(strings.TrimSpace(req.Environment))
	if environment == "" {
		return nil, errors.New("environment is required")
	}
	if !config.ServesEnvironment(environment) {
		return nil, fmt.Errorf("environment %q is not served by this instance", environment)
	}
	databases := canonicalFreezeScope(req.Databases)
	for _, database := range databases {
		if database == "" {
			continue // Validate refuses the blank entry
		}
		dbConfig, ok := config.DatabaseConfigs()[database]
		if !ok {
			return nil, fmt.Errorf("database %q is not configured", database)
		}
		if _, ok := dbConfig.Environments[environment]; !ok {
			return nil, fmt.Errorf("database %q has no environment %q", database, environment)
		}
	}

	kind := cmp.Or(req.Kind, storage.FreezeKindOnDemand)
	startsAt := now
	if req.StartsAt != nil {
		startsAt = req.StartsAt.UTC()
	}
	if req.EndsAt != nil && !req.EndsAt.After(now) {
		return nil, errors.New("freeze end time must be in the future")
	}

	freeze := &storage.EnvironmentFreeze{
		Identifier:  engine.NewFreezeID(),
		Environment: environment,
		Deployments: canonicalFreezeScope(req.Deployments),
		Databases:   databases,
		Kind:        kind,
		Running:     cmp.Or(req.Running, storage.FreezeRunningContinue),
		StartsAt:    startsAt,
		EndsAt:      req.EndsAt,
		Reason:      strings.TrimSpace(req.Reason),
		CreatedBy:   freezeCaller(ctx),
		Source:      storage.FreezeSourceManual,
	}
	if err := freeze.Validate(); err != nil {
		return nil, err
	}
	return freeze, nil
}

// handleFreezeList returns the freezes in effect, the scheduled ones, and the
// most recent ones of any state. ?environment= narrows the list; ?recent=
// sets how many recent freezes to return.
func (s *Service) handleFreezeList(w http.ResponseWriter, r *http.Request) {
	if !s.freezeStorageReady(w) {
		return
	}
	environment := storage.CanonicalKey(strings.TrimSpace(r.URL.Query().Get("environment")))
	limit := freezeRecentDefault
	if raw := r.URL.Query().Get("recent"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > freezeRecentMax {
			s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest,
				fmt.Sprintf("recent must be a number from 1 to %d", freezeRecentMax))
			return
		}
		limit = parsed
	}
	// One snapshot and one clock label every freeze in the response, so a
	// freeze that ends or is lifted mid-request cannot appear twice with
	// different statuses.
	listing, err := s.storage.Freezes().List(r.Context(), environment, limit)
	if err != nil {
		s.logger.Error("list freezes failed", "environment", environment, "error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "list freezes failed")
		return
	}
	now := listing.Now
	response := apitypes.FreezeListResponse{
		InEffect:  []apitypes.Freeze{},
		Scheduled: []apitypes.Freeze{},
		Recent:    make([]apitypes.Freeze, 0, len(listing.Recent)),
	}
	for _, freeze := range listing.Current {
		if freeze.InEffectAt(now) {
			response.InEffect = append(response.InEffect, freezeResponse(freeze, now))
		} else {
			response.Scheduled = append(response.Scheduled, freezeResponse(freeze, now))
		}
	}
	for _, freeze := range listing.Recent {
		response.Recent = append(response.Recent, freezeResponse(freeze, now))
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleFreezeGet returns one freeze.
func (s *Service) handleFreezeGet(w http.ResponseWriter, r *http.Request) {
	if !s.freezeStorageReady(w) {
		return
	}
	id := r.PathValue("id")
	freeze, err := s.storage.Freezes().Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, storage.ErrFreezeNotFound) {
			s.writeErrorCode(w, http.StatusNotFound, apitypes.ErrCodeNotFound, fmt.Sprintf("freeze %q not found", id))
			return
		}
		s.logger.Error("get freeze failed", "freeze_id", id, "error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "get freeze failed")
		return
	}
	now, ok := s.freezeNow(r.Context(), w)
	if !ok {
		return
	}
	s.writeJSON(w, http.StatusOK, freezeResponse(freeze, now))
}

// handleFreezeLift ends a freeze early. The row stays as the audit trail.
func (s *Service) handleFreezeLift(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeFreezeWrite(w, r, "freeze_lift") {
		return
	}
	if !s.freezeStorageReady(w) {
		return
	}
	var req apitypes.LiftFreezeRequest
	if err := decodeStrictJSONBody(r, &req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}
	id := r.PathValue("id")
	lifted, err := s.storage.Freezes().Lift(r.Context(), id, freezeCaller(r.Context()), strings.TrimSpace(req.Reason))
	switch {
	case errors.Is(err, storage.ErrFreezeNotFound):
		s.writeErrorCode(w, http.StatusNotFound, apitypes.ErrCodeNotFound, fmt.Sprintf("freeze %q not found", id))
		return
	case errors.Is(err, storage.ErrFreezeAlreadyLifted):
		s.writeError(w, http.StatusConflict, fmt.Sprintf("freeze %q is already lifted", id))
		return
	case errors.Is(err, storage.ErrFreezeEnded):
		s.writeError(w, http.StatusConflict, fmt.Sprintf("freeze %q already ended", id))
		return
	case err != nil:
		s.logger.Error("lift freeze failed", "freeze_id", id, "error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "lift freeze failed")
		return
	}
	s.logger.Info("environment freeze lifted", append(freezeLogArgs(lifted, lifted.LiftedBy),
		"lift_reason", lifted.LiftReason)...)
	// A lifted freeze is never in effect, so the response needs no clock read.
	s.writeJSON(w, http.StatusOK, freezeResponse(lifted, time.Time{}))
}

// freezeNow reads the database clock, the one the freeze store compares
// against, so a response labels a freeze in effect exactly when the change
// gate would refuse under it. It writes a 503 when the clock cannot be read.
func (s *Service) freezeNow(ctx context.Context, w http.ResponseWriter) (time.Time, bool) {
	now, err := s.storage.Freezes().Now(ctx)
	if err != nil {
		s.logger.Error("read database clock for freezes failed", "error", err)
		s.writeErrorCode(w, http.StatusServiceUnavailable, apitypes.ErrCodeStorageError, "freeze storage is not available")
		return time.Time{}, false
	}
	return now, true
}

// freezeStorageReady writes a 503 when the service has no freeze store, as a
// server started without storage does.
func (s *Service) freezeStorageReady(w http.ResponseWriter) bool {
	if s.storage == nil || s.storage.Freezes() == nil {
		s.writeErrorCode(w, http.StatusServiceUnavailable, apitypes.ErrCodeStorageError, "freeze storage is not available")
		return false
	}
	return true
}

// freezeCaller names the caller who sets or lifts a freeze. Only a subject
// from a verified lane is recorded (AZ-3): a direct-to-pod request admitted
// with caller-supplied identity headers is recorded as "unverified", and a
// request with no identity (auth type none) as "anonymous".
func freezeCaller(ctx context.Context) string {
	if subject, ok := auth.VerifiedSubject(ctx); ok {
		return subject
	}
	if _, ok := auth.AuthenticatedSubject(ctx); ok {
		return unverifiedFreezeCaller
	}
	return "anonymous"
}

// unverifiedFreezeCaller records a freeze action whose caller is admitted but
// not verified.
const unverifiedFreezeCaller = "unverified"

// canonicalFreezeScope folds scope entries. A blank entry stays, so
// Validate refuses it instead of the freeze silently widening.
func canonicalFreezeScope(values []string) []string {
	var scope []string
	for _, value := range values {
		scope = append(scope, storage.CanonicalKey(strings.TrimSpace(value)))
	}
	return scope
}

// freezeLogArgs are the audit fields of a freeze, with caller the person who
// acted: the creator on a create, the lifter on a lift. The numeric row id
// stays out of logs; the identifier is the handle.
func freezeLogArgs(freeze *storage.EnvironmentFreeze, caller string) []any {
	args := []any{
		"freeze_id", freeze.Identifier,
		"environment", freeze.Environment,
		"deployments", freeze.Deployments,
		"databases", freeze.Databases,
		"kind", freeze.Kind,
		"running", freeze.Running,
		"starts_at", freeze.StartsAt,
		"reason", freeze.Reason,
		"created_by", freeze.CreatedBy,
		"caller", caller,
	}
	if freeze.EndsAt != nil {
		args = append(args, "ends_at", *freeze.EndsAt)
	}
	return args
}

func freezeResponse(freeze *storage.EnvironmentFreeze, now time.Time) apitypes.Freeze {
	return apitypes.Freeze{
		ID:          freeze.Identifier,
		Environment: freeze.Environment,
		Deployments: freeze.Deployments,
		Databases:   freeze.Databases,
		Kind:        freeze.Kind,
		Running:     freeze.Running,
		StartsAt:    freeze.StartsAt,
		EndsAt:      freeze.EndsAt,
		Reason:      freeze.Reason,
		CreatedBy:   freeze.CreatedBy,
		Source:      freeze.Source,
		ExternalID:  freeze.ExternalID,
		LiftedBy:    freeze.LiftedBy,
		LiftReason:  freeze.LiftReason,
		LiftedAt:    freeze.LiftedAt,
		CreatedAt:   freeze.CreatedAt,
		InEffect:    freeze.InEffectAt(now),
	}
}
