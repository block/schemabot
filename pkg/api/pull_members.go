package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"golang.org/x/sync/errgroup"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/tern"
)

// pullMemberConcurrency bounds how many rollout members are pulled at once. Each
// member costs one live-schema read per namespace against an often remote
// target, so a cap keeps a wide environment's pull from opening an unbounded
// number of concurrent connections across regions.
const pullMemberConcurrency = 4

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

// pullMemberDivergence reports every rollout member of an environment whose
// members hold their own schemas: the primary, marked as such and carrying no
// comparison of its own, and each other member with how its live schema differs
// from the primary's.
//
// The primary is listed rather than left implicit so the response names the
// whole member set. A caller reconciling an environment against its own shard
// inventory can then read the members straight off the payload, instead of
// having to know that the schema in Namespaces belongs to a member the list
// omits — which would leave a four-target environment describing three.
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
	// The caller resolved the primary from its own read of the config; this is a
	// second read, and a config reloaded in between could have re-mapped the
	// environment. If the primary is no longer among the members, the loop below
	// would pull it again as an ordinary member and return a member set with
	// nothing marked primary — a response describing an environment that does not
	// exist in either config. Fail instead of reporting it.
	if !containsMember(targets, primary) {
		return nil, fmt.Errorf("pull for %s/%s resolves rollout members %s, which do not include the primary %s the schema was pulled from; the environment's routing changed during the pull, so re-run it",
			req.Database, req.Environment, memberIDs(targets), primary.MemberID())
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

	// Members are pulled concurrently under a small cap. Each one is a live-schema
	// read per namespace against an often remote target, so a serial fan-out costs
	// a wide environment the sum of its members' read times on every pull. Results
	// are written into fixed positions, so the response keeps configuration order
	// whatever order the reads finish in.
	members := make([]*apitypes.TargetDivergence, len(targets))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(pullMemberConcurrency)

	for i, target := range targets {
		// The primary is already pulled — its schema is what every other member
		// is compared against — so it is recorded from the schema in hand rather
		// than fetched a second time, and carries no comparison against itself.
		if target.MemberID() == primary.MemberID() {
			members[i] = &apitypes.TargetDivergence{
				Deployment: target.Deployment,
				Target:     target.Target,
				TableCount: primarySchema.TableCount,
				Primary:    true,
			}
			continue
		}
		g.Go(func() error {
			memberSchema, err := s.pullTargetSchema(gctx, req, target, namespaces, catalogDetail)
			if err != nil {
				return fmt.Errorf("pull rollout member %s: %w", target.MemberID(), err)
			}
			memberTables, err := canonicalTablesByNamespace(parser, memberSchema)
			if err != nil {
				return fmt.Errorf("canonicalize schema of rollout member %s: %w", target.MemberID(), err)
			}
			members[i] = &apitypes.TargetDivergence{
				Deployment:     target.Deployment,
				Target:         target.Target,
				TableCount:     memberSchema.TableCount,
				DivergedTables: divergedTables(primaryTables, memberTables),
			}
			return nil
		})
	}
	// One member's failure fails the pull. Reporting the members that did succeed
	// would describe the environment as compared when part of it was not, which is
	// the reading this whole comparison exists to prevent.
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return members, nil
}

// containsMember reports whether a member set contains one specific member.
func containsMember(targets []routing.ExecutionTarget, want routing.ExecutionTarget) bool {
	return slices.ContainsFunc(targets, func(target routing.ExecutionTarget) bool {
		return target.MemberID() == want.MemberID()
	})
}

// memberIDs names a member set for an error message.
func memberIDs(targets []routing.ExecutionTarget) string {
	ids := make([]string, 0, len(targets))
	for _, target := range targets {
		ids = append(ids, target.MemberID())
	}
	return strings.Join(ids, ", ")
}

// namespaceTable identifies one pulled table within its namespace.
type namespaceTable struct {
	namespace string
	table     string
}

// canonicalTablesByNamespace reduces a pulled schema to the canonical form of
// each table's DDL, which is what two targets are compared by: a difference in
// whitespace or keyword case is the same schema, and must not be reported as
// divergence.
//
// Canonicalization re-renders the parsed tree in the order it was written, so it
// does not reorder clauses. Two targets holding the same columns and indexes in
// a different order therefore compare as different and are reported as diverged.
// That over-reports rather than under-reports — the comparison never calls two
// different schemas equal — so an operator is sent to look at a table that turns
// out to agree, which costs attention rather than correctness. Making the
// comparison order-insensitive belongs in the dialect's canonical form, where
// every consumer of it would benefit, not in a set comparison built here.
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
