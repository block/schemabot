package api

import (
	"context"
	"errors"
	"fmt"
	"sort"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/tern"
)

// pullTargetSchema fetches one execution target's live schema, one call per
// namespace, and merges the results into a single response.
//
// It is the single place a target is pulled from, so the primary target whose
// schema the caller materializes and a non-primary rollout member pulled only to
// compare against it are fetched identically and fail the same way.
func (s *Service) pullTargetSchema(
	ctx context.Context,
	req apitypes.PullSchemaRequest,
	target routing.ExecutionTarget,
	namespaces []string,
	catalogDetail ternv1.PullCatalogDetail,
) (*ternv1.PullSchemaResponse, error) {
	client, err := s.TernClient(target.Deployment, req.Environment)
	if err != nil {
		return nil, fmt.Errorf("database %q (%s): %w", req.Database, req.Environment, err)
	}

	isRemoteTarget := client.IsRemote()
	s.logger.Info("ExecutePullSchema: calling PullSchema",
		"database", req.Database,
		"type", target.DatabaseType,
		"deployment", target.Deployment,
		"target", target.Target,
		"environment", req.Environment,
		"pull_call_count", len(namespaces),
		"explicit_namespace_count", len(req.Namespaces),
		"is_remote", isRemoteTarget,
	)

	merged := &ternv1.PullSchemaResponse{
		Database:    req.Database,
		Type:        target.DatabaseType,
		Environment: req.Environment,
		Namespaces:  make(map[string]*ternv1.PulledNamespace),
	}
	for _, namespace := range namespaces {
		resp, pullErr := client.PullSchema(ctx, &ternv1.PullSchemaRequest{
			Database:      req.Database,
			Type:          target.DatabaseType,
			Target:        target.Target,
			Environment:   req.Environment,
			Namespace:     namespace,
			CatalogDetail: catalogDetail,
		})
		if pullErr != nil {
			s.logger.Error("ExecutePullSchema: routing client PullSchema failed",
				"database", req.Database,
				"type", target.DatabaseType,
				"deployment", target.Deployment,
				"target", target.Target,
				"environment", req.Environment,
				"namespace", namespace,
				"endpoint", client.Endpoint(),
				"is_remote", isRemoteTarget,
				"error", pullErr,
			)
			if isRemoteTarget && grpcstatus.Code(pullErr) == grpccodes.Unavailable {
				return nil, &RemoteDeploymentUnavailableError{
					Deployment: target.Deployment,
					Target:     target.Target,
					Err:        pullErr,
				}
			}
			// Whether pull is supported is the data plane's answer — it depends
			// on which engine backs the deployment — so the 501 is derived from
			// the pull attempt instead of gating on database type. One sentinel
			// check covers both routes: the local client returns
			// ErrPullSchemaUnsupportedType directly, and the gRPC client
			// re-derives the same sentinel from the remote data plane's own
			// unsupported verdict (infrastructure Unimplemented errors
			// deliberately fall through as ordinary failures).
			if errors.Is(pullErr, tern.ErrPullSchemaUnsupportedType) {
				return nil, &unsupportedPullSchemaError{DatabaseType: target.DatabaseType}
			}
			return nil, pullErr
		}
		if err := mergePullSchemaResponse(merged, resp, namespace); err != nil {
			return nil, err
		}
	}
	return merged, nil
}

// pullMemberDivergence pulls every non-primary rollout member of an environment
// whose members hold their own schemas and reports how each one's live schema
// differs from the primary's.
//
// It returns nil for an environment whose members are expected to hold the same
// schema: pulling them would cost one round trip per member to learn what the
// deployments contract already asserts, and any difference there is drift for
// the review-time rollup to block on, not divergence for a pull to describe.
//
// Divergence is what a multi-target environment is for, so it is reported rather
// than treated as an error — but a member that cannot be pulled, or whose DDL
// cannot be compared, fails the request. Reporting "no divergence" for a member
// that was never successfully compared would describe the environment as
// converged on the strength of a comparison that did not happen.
func (s *Service) pullMemberDivergence(
	ctx context.Context,
	req apitypes.PullSchemaRequest,
	primary routing.ExecutionTarget,
	primarySchema *ternv1.PullSchemaResponse,
	namespaces []string,
	catalogDetail ternv1.PullCatalogDetail,
) ([]*apitypes.TargetDivergence, error) {
	planning, err := s.config.MemberPlanningFor(req.Database, req.Environment)
	if err != nil {
		return nil, fmt.Errorf("resolve member planning for %s/%s: %w", req.Database, req.Environment, err)
	}
	if planning != PlanIndependent {
		s.logger.Debug("pull reports no per-target divergence; this environment's members are expected to hold the same schema",
			"database", req.Database,
			"environment", req.Environment)
		return nil, nil
	}

	targets, err := s.config.ResolveDatabaseTargets(req.Database, req.Environment)
	if err != nil {
		return nil, fmt.Errorf("resolve targets for %s/%s: %w", req.Database, req.Environment, err)
	}

	dialect := schema.DialectForDatabaseType(primary.DatabaseType)
	parser, err := ddl.ParserForDialect(dialect)
	if err != nil {
		return nil, fmt.Errorf("compare targets of %s/%s: %w", req.Database, req.Environment, err)
	}
	primaryTables, err := canonicalTablesByNamespace(parser, primarySchema)
	if err != nil {
		return nil, fmt.Errorf("canonicalize schema of rollout member %s: %w", primary.MemberID(), err)
	}

	divergences := make([]*apitypes.TargetDivergence, 0, len(targets)-1)
	for _, target := range targets {
		if target.Deployment == primary.Deployment && target.Target == primary.Target {
			continue
		}
		memberSchema, err := s.pullTargetSchema(ctx, req, target, namespaces, catalogDetail)
		if err != nil {
			return nil, fmt.Errorf("pull rollout member %s: %w", target.MemberID(), err)
		}
		memberTables, err := canonicalTablesByNamespace(parser, memberSchema)
		if err != nil {
			return nil, fmt.Errorf("canonicalize schema of rollout member %s: %w", target.MemberID(), err)
		}
		divergences = append(divergences, &apitypes.TargetDivergence{
			Deployment:     target.Deployment,
			Target:         target.Target,
			TableCount:     memberSchema.TableCount,
			DivergedTables: divergedTables(primaryTables, memberTables),
		})
	}
	return divergences, nil
}

// namespaceTable identifies one pulled table within its namespace.
type namespaceTable struct {
	namespace string
	table     string
}

// canonicalTablesByNamespace reduces a pulled schema to the canonical form of
// each table's DDL, which is what two targets are compared by: a difference in
// whitespace, keyword case, or clause order is the same schema, and must not be
// reported as divergence.
//
// A table whose DDL the dialect's parser cannot canonicalize fails the request.
// Comparing raw text for it would report every formatting difference as a
// schema difference, and skipping it would drop a table out of the comparison
// without saying so.
func canonicalTablesByNamespace(parser ddl.StatementParser, pulled *ternv1.PullSchemaResponse) (map[namespaceTable]string, error) {
	canonical := make(map[namespaceTable]string)
	for namespace, ns := range pulled.Namespaces {
		if ns == nil {
			return nil, fmt.Errorf("namespace %q has no pulled content", namespace)
		}
		for table, tableDDL := range ns.Tables {
			stmtType, _, err := parser.Classify(tableDDL)
			if err != nil {
				return nil, fmt.Errorf("namespace %q table %q: DDL rejected by the statement parser: %w", namespace, table, err)
			}
			if stmtType != ddl.StatementCreateTable {
				return nil, fmt.Errorf("namespace %q table %q: expected a CREATE TABLE statement, got %s", namespace, table, stmtType)
			}
			canonical[namespaceTable{namespace: namespace, table: table}] = parser.Canonicalize(tableDDL)
		}
	}
	return canonical, nil
}

// divergedTables reports every table the two targets do not agree on, sorted for
// a stable response body. A table only one target holds is as much a divergence
// as a table both hold with different DDL, so all three cases are reported
// together and distinguished by Difference.
func divergedTables(primary, member map[namespaceTable]string) []apitypes.DivergedTable {
	diverged := make([]apitypes.DivergedTable, 0)
	for key, primaryDDL := range primary {
		memberDDL, ok := member[key]
		if !ok {
			diverged = append(diverged, divergedTable(key, apitypes.DivergenceOnlyOnPrimary))
			continue
		}
		if memberDDL != primaryDDL {
			diverged = append(diverged, divergedTable(key, apitypes.DivergenceDiffers))
		}
	}
	for key := range member {
		if _, ok := primary[key]; !ok {
			diverged = append(diverged, divergedTable(key, apitypes.DivergenceOnlyOnTarget))
		}
	}
	sort.Slice(diverged, func(i, j int) bool {
		if diverged[i].Namespace != diverged[j].Namespace {
			return diverged[i].Namespace < diverged[j].Namespace
		}
		return diverged[i].Table < diverged[j].Table
	})
	if len(diverged) == 0 {
		return nil
	}
	return diverged
}

func divergedTable(key namespaceTable, difference string) apitypes.DivergedTable {
	return apitypes.DivergedTable{Namespace: key.namespace, Table: key.table, Difference: difference}
}
