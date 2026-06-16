// Package etre is a thin client over the Etre entity registry, a generic
// entity database.
//
// It adds two things over the raw Etre client: construction from config, and
// fail-closed "exactly one match" lookup semantics for resolving a single
// entity from a selector. It is deliberately generic — no entity type, label,
// or field name is baked in. Mapping a data-plane target to a selector, and a
// resolved entity to a connection, is the caller's concern.
package etre

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/square/etre"
)

// ErrNotFound is wrapped by QueryOne when no entity matches the selector. A
// resolver that looks a target up across more than one entity type (for
// example one type for each database engine) can use errors.Is to fall back to
// the next type on not-found, while still treating query errors and ambiguous
// (more than one) matches as hard failures.
var ErrNotFound = errors.New("no matching etre entity")

// Config configures a Client. A Client is bound to a single entity type, which
// mirrors the underlying Etre entity client.
type Config struct {
	// Addr is the Etre server address (e.g. https://etre.example:8443).
	Addr string
	// EntityType is the Etre entity type this client queries.
	EntityType string
	// HTTPClient is used for Etre requests. The deployment owns any required
	// routing/TLS. When nil, http.DefaultClient is used.
	HTTPClient *http.Client
	// Retry is the per-request retry count on network or API error.
	Retry uint
	// Logger receives debug logs for lookups. Defaults to slog.Default().
	Logger *slog.Logger
}

// Client looks up entities in an Etre entity registry.
type Client struct {
	entities   etre.EntityClient
	entityType string
	logger     *slog.Logger
}

// New builds a Client backed by a real Etre entity client.
func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, fmt.Errorf("etre address is required")
	}
	if strings.TrimSpace(cfg.EntityType) == "" {
		return nil, fmt.Errorf("etre entity type is required")
	}
	// The Etre client does not default a nil HTTP client and would panic on the
	// first request, so default it here.
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	entities := etre.NewEntityClientWithConfig(etre.EntityClientConfig{
		EntityType: cfg.EntityType,
		Addr:       cfg.Addr,
		HTTPClient: httpClient,
		Retry:      cfg.Retry,
	})
	return newClient(entities, cfg.EntityType, cfg.Logger), nil
}

// newClient constructs a Client over a given entity client. It exists so tests
// can inject a fake etre.EntityClient.
func newClient(entities etre.EntityClient, entityType string, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		entities:   entities,
		entityType: entityType,
		logger:     logger,
	}
}

// QueryOne returns the single entity matching selector.
//
// It fails closed: an empty selector, zero matches, or more than one match are
// all errors rather than a best-effort pick. Callers resolving a connection
// target must get exactly one entity or a clear error — never an arbitrary one.
//
// Selector predicates are equality matches joined into an Etre query; keys are
// sorted so the query string is deterministic.
func (c *Client) QueryOne(ctx context.Context, selector map[string]string) (etre.Entity, error) {
	if len(selector) == 0 {
		return nil, fmt.Errorf("selector is required to query etre %q entities", c.entityType)
	}

	query := buildQuery(selector)
	c.logger.Debug("etre: querying for one entity", "entity_type", c.entityType, "query", query)

	matches, err := c.entities.Query(ctx, query, etre.QueryFilter{})
	if err != nil {
		return nil, fmt.Errorf("query etre %q entities matching %q: %w", c.entityType, query, err)
	}
	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("no etre %q entity matched %q: %w", c.entityType, query, ErrNotFound)
	case 1:
		c.logger.Debug("etre: resolved one entity", "entity_type", c.entityType, "query", query)
		return matches[0], nil
	default:
		return nil, fmt.Errorf("%d etre %q entities matched %q (expected exactly one); narrow the selector to disambiguate", len(matches), c.entityType, query)
	}
}

// buildQuery turns equality predicates into a deterministic Etre query string
// of the form "k1=v1,k2=v2", sorted by key.
func buildQuery(selector map[string]string) string {
	keys := make([]string, 0, len(selector))
	for k := range selector {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+selector[k])
	}
	return strings.Join(parts, ",")
}

// StringField reads a string-valued entity field, returning "" when the field
// is absent or not a string. It is a convenience for callers mapping entities
// to their own connection records.
func StringField(e etre.Entity, key string) string {
	v, _ := e[key].(string)
	return v
}
