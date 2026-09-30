package api

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// planDeploymentDiffConcurrency bounds how many deployments are diffed at once.
// Each deployment diff runs one live-schema read against a (often remote)
// deployment; a small cap rides out multi-deployment fan-out without opening an
// unbounded number of concurrent connections across regions.
const planDeploymentDiffConcurrency = 4

// DeploymentPlanDiff is one deployment's desired-vs-live diff computed at review
// time, or the error that prevented computing it. It is the per-deployment
// producer output the control-plane rollup compares across a database's
// deployments to surface drift before approval. The rollup must treat any Err —
// or a deployment missing from the results — as blocking, never as agreement.
type DeploymentPlanDiff struct {
	DatabaseType string
	Deployment   string
	Target       string

	// SchemaFiles is the desired state this member was planned against: the
	// request's schema files narrowed to the namespaces its targets entry
	// selects. The member's stored plan records exactly these.
	SchemaFiles map[string]*ternv1.SchemaFiles

	Engine         ternv1.Engine
	Changes        []*ternv1.SchemaChange
	Shards         []*ternv1.ShardPlan
	LintViolations []*ternv1.LintViolation

	// ExistingCopies are the unfinished copies on this member that applying its
	// diff would continue or destroy. ExistingCopiesReported says the data plane
	// looked: one that predates the disclosure leaves both unset, which is not
	// the same as a clean target. The primary's entry leaves them unset too; its
	// copies are disclosed on the reviewed plan itself.
	ExistingCopies         []*ternv1.ExistingCopy
	ExistingCopiesReported bool

	// DirectExecution is the policy this member's diff was judged under. The
	// member's stored plan records it, so the apply that dispatches this
	// member runs its statements under the policy its verdicts were computed
	// against. The primary's entry leaves it unset: its plan is the reviewed
	// plan, already stored by the path that planned it.
	DirectExecution *storage.DirectExecutionPolicy

	Err error
}

// PlanDeploymentDiffs computes every configured deployment's desired-vs-live
// diff for a database/environment at review time. Only the primary deployment
// plans locally today; the non-primary deployments never do, so drift on them is
// invisible until apply. This is the producer that closes that gap: each
// non-primary deployment is diffed with the non-persisting PlanDiff RPC, and the
// primary reuses the already-persisted reviewed plan (primaryPlan) so the rollup
// compares against exactly what the user reviewed rather than re-reading the
// primary's live schema — which could differ from the reviewed plan and trip a
// spurious primary-vs-primary mismatch.
//
// Per-deployment failures are captured in each result's Err so one unreachable
// deployment neither hides the others nor aborts the rollup. Results are
// returned in rollout order, primary first.
//
// targets is the resolved deployment set in rollout order (primary first),
// supplied by the caller so a database/environment is resolved once per rollup
// rather than re-resolved here. It must be non-empty.
//
// primaryMember is the rollout member the reviewed primaryPlan was created
// against (rollout index 0 at plan time), identified by deployment and target
// together because one deployment can address several targets. When a
// primaryPlan is reused, it is checked against targets[0] here so a rollout
// order change between plan and rollup — which would map the reviewed baseline
// onto a different member — fails closed rather than being compared against the
// wrong live schema.
func (s *Service) PlanDeploymentDiffs(ctx context.Context, req PlanRequest, primaryPlan *ternv1.PlanResponse, primaryMember routing.ExecutionTarget, targets []routing.ExecutionTarget) ([]DeploymentPlanDiff, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("no deployment targets for %s/%s", req.Database, req.Environment)
	}

	// Reusing primaryPlan for the primary member assumes it was created against
	// the member now at rollout index 0. Verify that against the plan's recorded
	// origin member (captured at plan time) and fail closed on a mismatch — e.g.
	// deployment_order changed between plan and rollup — rather than comparing
	// members against a baseline built for a different one.
	if primaryPlan != nil {
		if primaryMember.Deployment == "" {
			return nil, fmt.Errorf("plan diff for %s/%s: reviewed plan has no origin deployment to verify the primary against", req.Database, req.Environment)
		}
		if targets[0].Deployment != primaryMember.Deployment || targets[0].Target != primaryMember.Target {
			return nil, fmt.Errorf("primary invariant violated for %s/%s: rollout index 0 is %q but the reviewed plan was created against %q", req.Database, req.Environment, targets[0].MemberID(), primaryMember.MemberID())
		}
		// The reviewed plan covers only the namespaces the primary selected when
		// it was planned, as the caller reports them. A primary member whose
		// selection differs from the placement resolved here would pair that plan
		// with members planned under another placement, leaving a namespace in
		// neither or in both, so it fails closed too.
		if !slices.Equal(targets[0].Namespaces, primaryMember.Namespaces) {
			return nil, fmt.Errorf("primary invariant violated for %s/%s: rollout member %s now selects namespaces [%s] but the reviewed plan was created for [%s]; the environment's placement changed since the plan, so re-run it",
				req.Database, req.Environment, primaryMember.MemberID(), strings.Join(targets[0].Namespaces, ", "), strings.Join(primaryMember.Namespaces, ", "))
		}
	}

	results := make([]DeploymentPlanDiff, len(targets))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(planDeploymentDiffConcurrency)

	for i, target := range targets {
		results[i] = DeploymentPlanDiff{
			DatabaseType: target.DatabaseType,
			Deployment:   target.Deployment,
			Target:       target.Target,
		}

		// Rollout index 0 is the primary (ResolveDatabaseTargets returns targets
		// primary-first; the guard above enforces it). When its reviewed plan is
		// provided, reuse it so the rollup's primary member is exactly what was
		// reviewed and no redundant live-schema read runs.
		if i == 0 && primaryPlan != nil {
			results[i].Engine = primaryPlan.Engine
			results[i].Changes = primaryPlan.Changes
			results[i].Shards = primaryPlan.Shards
			results[i].LintViolations = primaryPlan.LintViolations
			// A reviewed plan that reported planning errors is not a trustworthy
			// baseline; record the error so the rollup fails closed rather than
			// comparing deployments against a broken primary.
			if len(primaryPlan.Errors) > 0 {
				results[i].Err = fmt.Errorf("reviewed primary plan reported errors: %v", primaryPlan.Errors)
			}
			continue
		}

		schemaFiles, err := memberSchemaFiles(req, target)
		if err != nil {
			s.logger.Warn("rollout member's namespace selection cannot be planned; deployment will block the review rollup",
				"database", req.Database,
				"environment", req.Environment,
				"deployment", target.Deployment,
				"target", target.Target,
				"error", err)
			metrics.RecordDeploymentDiff(ctx, req.Database, target.Deployment, req.Environment, "errored")
			results[i].Err = err
			continue
		}
		results[i].SchemaFiles = schemaFiles

		g.Go(func() error {
			directExecution, err := s.config.DirectExecutionPolicyFor(req.Database, req.Environment, target.DatabaseType)
			if err != nil {
				policyErr := fmt.Errorf("resolve direct_execution policy for database %q environment %q: %w", req.Database, req.Environment, err)
				s.logger.Warn("could not resolve the direct_execution policy for a rollout member; deployment will block the review rollup",
					"database", req.Database,
					"environment", req.Environment,
					"deployment", target.Deployment,
					"target", target.Target,
					"error", policyErr)
				metrics.RecordDeploymentDiff(gctx, req.Database, target.Deployment, req.Environment, "errored")
				results[i].Err = policyErr
				return nil
			}
			results[i].DirectExecution = directExecution

			resp, err := s.planDeploymentDiff(gctx, req, target, schemaFiles, directExecution)
			if err != nil {
				s.logger.Warn("plan deployment diff failed; deployment will block the review rollup",
					"database", req.Database,
					"environment", req.Environment,
					"deployment", target.Deployment,
					"target", target.Target,
					"error", err)
				metrics.RecordDeploymentDiff(gctx, req.Database, target.Deployment, req.Environment, "errored")
				results[i].Err = err
				return nil
			}
			// The execution-mode vocabulary is enforced here, where the diff
			// crosses into SchemaBot, rather than only where the member's plan is
			// written. The rollup classifies this diff and publishes its blocked
			// count from it, so a verdict normalized later would be disclosed at
			// review under the value the planner sent and stored under the one
			// SchemaBot settled on. The primary's baseline arrives already
			// normalized by the path that planned it.
			s.normalizePlanExecutionVerdicts(resp.Changes, resp.Shards, req.Database, target.Deployment)
			results[i].Engine = resp.Engine
			results[i].Changes = resp.Changes
			results[i].Shards = resp.Shards
			results[i].LintViolations = resp.LintViolations
			results[i].ExistingCopies = resp.ExistingCopies
			results[i].ExistingCopiesReported = resp.ExistingCopiesReported
			// A diff that succeeded at the RPC layer but reported planning errors
			// is not a trustworthy comparison input; block on it so the rollup
			// never mistakes an incomplete diff for agreement.
			if len(resp.Errors) > 0 {
				diffErr := fmt.Errorf("plan diff on deployment %q target %q reported errors: %v", target.Deployment, target.Target, resp.Errors)
				s.logger.Warn("plan deployment diff reported errors; deployment will block the review rollup",
					"database", req.Database,
					"environment", req.Environment,
					"deployment", target.Deployment,
					"target", target.Target,
					"error", diffErr)
				metrics.RecordDeploymentDiff(gctx, req.Database, target.Deployment, req.Environment, "errored")
				results[i].Err = diffErr
				return nil
			}
			metrics.RecordDeploymentDiff(gctx, req.Database, target.Deployment, req.Environment, "ok")
			return nil
		})
	}

	// Deployment failures are captured per result, so Wait only returns non-nil
	// if the parent context was cancelled.
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("plan deployment diffs for %s/%s: %w", req.Database, req.Environment, err)
	}
	return results, nil
}

// planDeploymentDiff runs the non-persisting PlanDiff RPC against a single
// deployment, building the per-target request the same way ExecutePlan builds
// the primary's plan request.
func (s *Service) planDeploymentDiff(ctx context.Context, req PlanRequest, target routing.ExecutionTarget, schemaFiles map[string]*ternv1.SchemaFiles, directExecution *storage.DirectExecutionPolicy) (*ternv1.PlanDiffResponse, error) {
	client, err := s.TernClient(target.Deployment, req.Environment)
	if err != nil {
		return nil, fmt.Errorf("tern client for deployment %q environment %q: %w", target.Deployment, req.Environment, err)
	}

	trustedSchemaPath := ""
	if req.SourceTrusted {
		trustedSchemaPath = req.SchemaPath
	}
	ternReq := &ternv1.PlanRequest{
		Database:    req.Database,
		Type:        target.DatabaseType,
		SchemaFiles: schemaFiles,
		Repository:  req.Repository,
		Environment: req.Environment,
		Target:      target.Target,
		SchemaPath:  trustedSchemaPath,
		// The desired state is partial in exactly the way the primary's is: a
		// namespace the caller withheld is excluded, not absent. Without this
		// the data plane reads the omission as intent to remove and plans DROPs
		// for namespaces the configuration excluded on purpose.
		IgnoredNamespaces: req.IgnoredNamespaces,
		// A namespace the member's entry does not select is withheld the same
		// way: it is on another target, not removed from this one.
		UnselectedNamespaces: unselectedNamespaces(req.SchemaFiles, target),
		// The exclusions travel with every member's diff. An ignored namespace
		// is already absent from SchemaFiles, but an ignored table lives on the
		// target, so a member asked without the list would diff tables the
		// primary withheld and report them as drift the primary does not have.
		IgnoreTables: req.IgnoreTables,
		// Always stated, never left absent: absence tells the data plane the
		// caller predates the grouping choice, and this caller has made one.
		GroupedExecution: new(req.GroupedExecution),
		// A member's diff is judged under the same policy as the primary's,
		// so a refused statement reads as the same verdict on every member
		// rather than as drift between them.
		DirectExecution: tern.DirectExecutionPolicyProto(directExecution),
	}
	if req.PullRequest != nil {
		ternReq.PullRequest = *req.PullRequest
	}
	if req.HeadSHA != nil {
		ternReq.HeadSha = *req.HeadSHA
	}

	resp, err := client.PlanDiff(ctx, ternReq)
	if err != nil {
		return nil, fmt.Errorf("plan diff on deployment %q target %q: %w", target.Deployment, target.Target, err)
	}
	return resp, nil
}

// memberSchemaFiles returns the desired state one rollout member is planned
// against: the request's schema files narrowed to the namespaces the member's
// targets entry selects, or all of them when it selects none.
//
// The schema files declare the namespace set, and a selection can only narrow
// it. A selected namespace the files do not carry is an error naming the member
// rather than a plan without it: planning nothing for the namespace would read
// as a target already converged on a schema no file describes.
func memberSchemaFiles(req PlanRequest, member routing.ExecutionTarget) (map[string]*ternv1.SchemaFiles, error) {
	if len(member.Namespaces) == 0 {
		return req.SchemaFiles, nil
	}
	selected := make(map[string]*ternv1.SchemaFiles, len(member.Namespaces))
	for _, namespace := range member.Namespaces {
		files, ok := req.SchemaFiles[namespace]
		if ok {
			selected[namespace] = files
			continue
		}
		selectionErr := &NamespaceSelectionError{Database: req.Database, Environment: req.Environment, Target: member.Target, Namespace: namespace}
		if slices.Contains(req.IgnoredNamespaces, namespace) {
			selectionErr.reason = "ignore_namespaces withholds from this plan; remove it from one of the two"
			return nil, selectionErr
		}
		selectionErr.reason = fmt.Sprintf("the schema files do not declare (declared: %s); a targets entry can only select namespaces the schema directory declares",
			strings.Join(slices.Sorted(maps.Keys(req.SchemaFiles)), ", "))
		return nil, selectionErr
	}
	return selected, nil
}

// NamespaceSelectionError reports a targets entry selecting a namespace the
// plan does not carry: one the schema files do not declare, or one
// ignore_namespaces withholds.
type NamespaceSelectionError struct {
	Database    string
	Environment string
	Target      string
	Namespace   string
	// reason completes "selects namespace %q, which ..." with why the plan
	// lacks it and what to change.
	reason string
}

func (e *NamespaceSelectionError) Error() string {
	return fmt.Sprintf("database %q environment %q target %q selects namespace %q, which %s", e.Database, e.Environment, e.Target, e.Namespace, e.reason)
}

// NamespacePlacementRefused reports whether a plan was refused because the
// environment's targets entries and the schema files disagree about where
// namespaces live: a declared namespace no entry selects, or an entry selecting
// one the plan does not carry. Either is a defect in the config or the schema
// files, not a server failure: every plan reproduces it until one of them
// changes. A caller that gates a merge on the plan must fail that environment's
// check closed on it, since the refused environment has no plan to fold.
func NamespacePlacementRefused(err error) bool {
	var coverageErr *NamespaceCoverageError
	var selectionErr *NamespaceSelectionError
	return errors.As(err, &coverageErr) || errors.As(err, &selectionErr)
}

// unselectedNamespaces returns the declared namespaces a rollout member's
// targets entry leaves out, in sorted order, or nil when it selects none and so
// holds every declared namespace. They travel to the data plane with the
// narrowed schema files, so an engine that diffs the whole target as one unit
// can refuse rather than read the omission as intent to drop.
func unselectedNamespaces(schemaFiles map[string]*ternv1.SchemaFiles, member routing.ExecutionTarget) []string {
	if len(member.Namespaces) == 0 {
		return nil
	}
	var unselected []string
	for _, namespace := range slices.Sorted(maps.Keys(schemaFiles)) {
		if !slices.Contains(member.Namespaces, namespace) {
			unselected = append(unselected, namespace)
		}
	}
	return unselected
}

// requireNamespaceCoverage refuses a plan when a declared namespace is held by
// no rollout member. One that no targets entry selects has no plan anywhere, so
// the rollout would read converged while that namespace's schema change never
// runs. Every plan of the environment runs it, not only the pull request
// review: a plan of a lone target whose entry selects a subset would otherwise
// report success for a schema change it silently leaves out.
func requireNamespaceCoverage(req PlanRequest, targets []routing.ExecutionTarget) error {
	uncovered := uncoveredNamespaces(req, targets)
	if len(uncovered) == 0 {
		return nil
	}
	return &NamespaceCoverageError{Database: req.Database, Environment: req.Environment, Uncovered: uncovered}
}

// NamespaceCoverageError reports declared namespaces that no targets entry of
// the environment selects, so no target would plan or apply them.
type NamespaceCoverageError struct {
	Database    string
	Environment string
	// Uncovered is the declared namespaces no entry selects, in sorted order.
	Uncovered []string
}

func (e *NamespaceCoverageError) Error() string {
	return fmt.Sprintf("database %q environment %q declares namespaces [%s] that no targets entry selects; select each on the target that holds it, or list it in ignore_namespaces to keep it out of the rollout",
		e.Database, e.Environment, strings.Join(e.Uncovered, ", "))
}

// uncoveredNamespaces returns the declared namespaces no rollout member holds,
// in sorted order. A member selecting nothing holds every declared namespace,
// so any such member covers the whole set.
//
// The selections only narrow what each member plans, so a namespace every
// entry leaves out would be planned and applied nowhere while each member's
// plan read clean. A namespace deliberately kept out of the rollout belongs in
// ignore_namespaces, which removes it from the declared set before this runs.
func uncoveredNamespaces(req PlanRequest, targets []routing.ExecutionTarget) []string {
	covered := make(map[string]bool)
	for _, target := range targets {
		if len(target.Namespaces) == 0 {
			return nil
		}
		for _, namespace := range target.Namespaces {
			covered[namespace] = true
		}
	}
	var uncovered []string
	for _, namespace := range slices.Sorted(maps.Keys(req.SchemaFiles)) {
		if !covered[namespace] {
			uncovered = append(uncovered, namespace)
		}
	}
	return uncovered
}
