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
// shared schema check cannot pass until it can answer.
func (s *Service) ResolveNamespaces(ctx context.Context, database, environment string, target routing.ExecutionTarget, namespaces []string) (map[string]string, error) {
	client, err := s.TernClient(target.Deployment, environment)
	if err != nil {
		return nil, fmt.Errorf("tern client for deployment %q environment %q: %w", target.Deployment, environment, err)
	}
	ctx, cancel := context.WithTimeout(ctx, namespaceResolutionTimeout)
	defer cancel()
	resp, err := client.ResolveNamespaces(ctx, &ternv1.ResolveNamespacesRequest{
		Database:    database,
		Type:        target.DatabaseType,
		Environment: environment,
		Target:      target.Target,
		Namespaces:  namespaces,
	})
	if errors.Is(err, tern.ErrResolveNamespacesUnsupported) {
		s.logger.Warn("data plane cannot resolve namespaces, so the shared schema check will refuse the plan",
			"database", database, "environment", environment, "deployment", target.Deployment, "target", target.Target, "error", err)
		return nil, fmt.Errorf("the data plane of deployment %q cannot report which schemas the namespaces of database %q address on target %q. Upgrade that data plane: %w",
			target.Deployment, database, target.Target, err)
	}
	if err != nil {
		return nil, fmt.Errorf("resolve namespaces of database %q on target %q of deployment %q in environment %q: %w",
			database, target.Target, target.Deployment, environment, err)
	}
	return resp.GetSchemas(), nil
}
