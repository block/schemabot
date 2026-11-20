package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/tern"
)

// namespaceResolutionTimeout bounds one namespace resolution, so a data plane
// that stops answering costs the shared schema check a bounded wait and the
// check fails rather than hanging the plan.
const namespaceResolutionTimeout = 30 * time.Second

var _ NamespaceResolver = (*Service)(nil)

// ResolveNamespaces asks the data plane behind target which schema each of a
// database's namespaces addresses there. The resolution reads the target's
// configuration and runs nothing on the database. A data plane that predates
// the RPC refuses it, and the error says to upgrade that data plane, since the
// shared schema check cannot pass until it can answer. A data plane whose
// engine has no answer refuses it too, and upgrading does not change that.
func (s *Service) ResolveNamespaces(ctx context.Context, database, environment string, target routing.ExecutionTarget, namespaces []string) (NamespacePlacement, error) {
	client, err := s.TernClient(target.Deployment, environment)
	if err != nil {
		return NamespacePlacement{}, fmt.Errorf("tern client for deployment %q environment %q: %w", target.Deployment, environment, err)
	}
	ctx, cancel := context.WithTimeout(ctx, namespaceResolutionTimeout)
	defer cancel()
	resp, err := tern.ResolveNamespacesWith(ctx, client, &ternv1.ResolveNamespacesRequest{
		Database:    database,
		Type:        target.DatabaseType,
		Environment: environment,
		Target:      target.Target,
		Namespaces:  namespaces,
	})
	if errors.Is(err, tern.ErrResolveNamespacesUnimplemented) {
		s.logger.Warn("data plane predates namespace resolution, so the shared schema check will refuse the plan",
			"database", database, "environment", environment, "deployment", target.Deployment, "target", target.Target, "error", err)
		return NamespacePlacement{}, fmt.Errorf("the data plane of deployment %q cannot report which schemas the namespaces of database %q address on target %q. Upgrade that data plane: %w",
			target.Deployment, database, target.Target, err)
	}
	if errors.Is(err, tern.ErrResolveNamespacesUnsupported) {
		s.logger.Warn("data plane cannot resolve namespaces for this database type, so the shared schema check will refuse the plan",
			"database", database, "database_type", target.DatabaseType, "environment", environment, "deployment", target.Deployment, "target", target.Target, "error", err)
		return NamespacePlacement{}, fmt.Errorf("the data plane of deployment %q cannot resolve the namespaces of database %q of type %q on target %q: %w",
			target.Deployment, database, target.DatabaseType, target.Target, err)
	}
	if err != nil {
		return NamespacePlacement{}, fmt.Errorf("resolve namespaces of database %q on target %q of deployment %q in environment %q: %w",
			database, target.Target, target.Deployment, environment, err)
	}
	return NamespacePlacement{Scope: resp.GetScope(), Schemas: resp.GetSchemas()}, nil
}
