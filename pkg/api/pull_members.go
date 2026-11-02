package api

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	// Each member gets its own span rather than recording on the caller's. A
	// rollout pulls its members concurrently, so one span per member is what
	// attributes a slow or failing read to the member that owns it; statuses from
	// several members on one shared span would describe whichever finished last.
	ctx, span := otel.Tracer("schemabot").Start(ctx, "pullTargetSchema",
		trace.WithAttributes(
			attribute.String("database", req.Database),
			attribute.String("environment", req.Environment),
			attribute.String("deployment", target.Deployment),
			attribute.String("target", target.Target),
		))
	defer span.End()

	client, err := s.TernClient(target.Deployment, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "resolve tern client failed")
		return nil, fmt.Errorf("database %q (%s): %w", req.Database, req.Environment, err)
	}

	namespaces = s.memberPullNamespaces(req, target, namespaces)

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
			span.RecordError(pullErr, trace.WithAttributes(attribute.String("namespace", namespace)))
			span.SetStatus(otelcodes.Error, "pull schema failed")
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
			span.RecordError(err, trace.WithAttributes(attribute.String("namespace", namespace)))
			span.SetStatus(otelcodes.Error, "merge pull schema response")
			return nil, err
		}
	}

	span.SetAttributes(attribute.Int("table_count", int(merged.TableCount)))
	return merged, nil
}

// memberPullNamespaces narrows the namespaces a pull asks one target for to the
// namespaces its targets entry selects. A target that selects none is pulled
// exactly as requested.
//
// A pull of every namespace becomes one call per selected namespace, so the
// data plane is asked for names rather than left to discover them: the
// selection outranks a live scan, and a target DSN bound to a different
// database refuses a named namespace instead of answering with its own. An
// explicitly requested namespace the target does not select is left out, since
// the configuration does not place it there.
func (s *Service) memberPullNamespaces(req apitypes.PullSchemaRequest, target routing.ExecutionTarget, namespaces []string) []string {
	if len(target.Namespaces) == 0 {
		return namespaces
	}
	if len(namespaces) == 1 && namespaces[0] == "" {
		return slices.Clone(target.Namespaces)
	}
	selected := make([]string, 0, len(namespaces))
	for _, namespace := range namespaces {
		if !slices.Contains(target.Namespaces, namespace) {
			s.logger.Info("pull skips a requested namespace the target's entry does not select",
				"database", req.Database,
				"environment", req.Environment,
				"deployment", target.Deployment,
				"target", target.Target,
				"namespace", namespace,
				"selected_namespaces", target.Namespaces)
			continue
		}
		selected = append(selected, namespace)
	}
	return selected
}

// requireSelectablePullNamespaces refuses an explicitly requested namespace that
// no targets entry of the environment selects. When every entry selects
// namespaces, their union is every namespace the environment places, so a name
// outside it is one no target would be asked for: answering with an empty
// schema would read as a namespace that exists and holds no tables. An entry
// selecting nothing holds every namespace, so an environment with one places
// any name, and a pull of every namespace names none to check.
func requireSelectablePullNamespaces(req apitypes.PullSchemaRequest, targets []routing.ExecutionTarget, namespaces []string) error {
	if len(namespaces) == 1 && namespaces[0] == "" {
		return nil
	}
	selectable := map[string]bool{}
	for _, target := range targets {
		if len(target.Namespaces) == 0 {
			return nil
		}
		for _, namespace := range target.Namespaces {
			selectable[namespace] = true
		}
	}
	var unselected []string
	for _, namespace := range namespaces {
		if !selectable[namespace] {
			unselected = append(unselected, namespace)
		}
	}
	if len(unselected) == 0 {
		return nil
	}
	return &UnselectedPullNamespaceError{
		Database:    req.Database,
		Environment: req.Environment,
		Namespaces:  unselected,
		Selectable:  slices.Sorted(maps.Keys(selectable)),
	}
}

// UnselectedPullNamespaceError reports requested pull namespaces that no targets
// entry of the environment selects.
type UnselectedPullNamespaceError struct {
	Database    string
	Environment string
	// Namespaces are the requested namespaces no entry selects, in request order.
	Namespaces []string
	// Selectable are the namespaces the environment's entries select, sorted.
	Selectable []string
}

func (e *UnselectedPullNamespaceError) Error() string {
	return fmt.Sprintf("database %q environment %q has no targets entry selecting namespaces [%s]; the selectable namespaces are [%s]",
		e.Database, e.Environment, strings.Join(e.Namespaces, ", "), strings.Join(e.Selectable, ", "))
}

// pullRolloutMembers pulls every rollout member of an environment whose members
// hold their own schemas, and returns the schema a caller materializes together
// with every member and how it differs.
//
// No single member's schema is the environment's when members hold different
// namespaces, so the schema is assembled namespace by namespace: each comes from
// its baseline member, the first in rollout order holding it (see
// apitypes.BaselineTarget). Where every member holds every namespace, that is
// the primary for all of them. Each later member holding a namespace is compared
// with the namespace's baseline, so a member is never reported as missing a
// namespace its entry does not place on it.
//
// The primary is listed rather than left implicit so the response names the
// whole member set. A caller reconciling an environment against its own shard
// inventory can then read the members straight off the payload, instead of
// having to know which member the schema came from.
//
// Divergence is what a multi-target environment is for, so it is reported rather
// than treated as an error — but a member that cannot be pulled, or whose DDL
// cannot be compared, fails the request. Reporting "no divergence" for a member
// that was never successfully compared would describe the environment as
// converged on the strength of a comparison that did not happen.
func (s *Service) pullRolloutMembers(
	ctx context.Context,
	req apitypes.PullSchemaRequest,
	targets []routing.ExecutionTarget,
	namespaces []string,
	catalogDetail ternv1.PullCatalogDetail,
) (*ternv1.PullSchemaResponse, []*apitypes.TargetDivergence, error) {
	dialect := schema.DialectForDatabaseType(targets[0].DatabaseType)
	parser, err := ddl.ParserForDialect(dialect)
	if err != nil {
		return nil, nil, fmt.Errorf("compare targets of %s/%s: %w", req.Database, req.Environment, err)
	}

	// Members are pulled concurrently under a small cap. Each one is a live-schema
	// read per namespace against an often remote target, so a serial fan-out costs
	// a wide environment the sum of its members' read times on every pull. Results
	// are written into fixed positions, so the response keeps configuration order
	// whatever order the reads finish in.
	pulled := make([]*ternv1.PullSchemaResponse, len(targets))
	canonical := make([]map[namespaceTable]string, len(targets))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(pullMemberConcurrency)
	for i, target := range targets {
		g.Go(func() error {
			memberSchema, err := s.pullTargetSchema(gctx, req, target, namespaces, catalogDetail)
			if err != nil {
				return fmt.Errorf("pull rollout member %s: %w", target.MemberID(), err)
			}
			memberTables, err := canonicalTablesByNamespace(parser, memberSchema)
			if err != nil {
				return fmt.Errorf("canonicalize schema of rollout member %s: %w", target.MemberID(), err)
			}
			pulled[i] = memberSchema
			canonical[i] = memberTables
			return nil
		})
	}
	// One member's failure fails the pull. Reporting the members that did succeed
	// would describe the environment as compared when part of it was not, which is
	// the reading this whole comparison exists to prevent.
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	members := make([]*apitypes.TargetDivergence, len(targets))
	for i, target := range targets {
		members[i] = &apitypes.TargetDivergence{
			Deployment: target.Deployment,
			Target:     target.Target,
			TableCount: pulled[i].TableCount,
			Primary:    i == 0,
			Namespaces: slices.Clone(target.Namespaces),
		}
	}

	merged := &ternv1.PullSchemaResponse{
		Database:    req.Database,
		Type:        targets[0].DatabaseType,
		Environment: req.Environment,
		Namespaces:  make(map[string]*ternv1.PulledNamespace),
	}
	for i, memberSchema := range pulled {
		for namespace, content := range memberSchema.Namespaces {
			if apitypes.BaselineTarget(members, namespace) != i {
				continue
			}
			merged.Namespaces[namespace] = content
			merged.TableCount += int32(len(content.GetTables()))
		}
	}

	// Each namespace's baseline is resolved once, so comparing every member
	// costs one pass over each member's tables rather than a pass over every
	// other member's.
	owners := make(map[string]int)
	baselineTables := make(map[string]map[namespaceTable]string)
	for i, memberTables := range canonical {
		for key, tableDDL := range memberTables {
			owner, ok := owners[key.namespace]
			if !ok {
				owner = apitypes.BaselineTarget(members, key.namespace)
				owners[key.namespace] = owner
			}
			if owner != i {
				continue
			}
			if baselineTables[key.namespace] == nil {
				baselineTables[key.namespace] = make(map[namespaceTable]string)
			}
			baselineTables[key.namespace][key] = tableDDL
		}
	}

	for i, member := range members {
		baseline := make(map[namespaceTable]string)
		for namespace, tables := range baselineTables {
			if owners[namespace] != i && member.Holds(namespace) {
				maps.Copy(baseline, tables)
			}
		}
		compared := make(map[namespaceTable]string)
		for key, tableDDL := range canonical[i] {
			if owners[key.namespace] != i {
				compared[key] = tableDDL
			}
		}
		member.DivergedTables = divergedTables(baseline, compared)
	}
	return merged, members, nil
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
