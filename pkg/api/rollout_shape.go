package api

import (
	"fmt"

	"github.com/block/schemabot/pkg/routing"
)

// RolloutShapeRefusal is what an apply asked of a multi-target rollout that the
// rollout does not run.
type RolloutShapeRefusal int

const (
	// RolloutDeferCutoverRefused is --defer-cutover on an apply to more than
	// one target of a deployment. Each target cuts over as its table finishes,
	// so there is no cutover across targets to hold for an operator.
	RolloutDeferCutoverRefused RolloutShapeRefusal = iota + 1
	// RolloutMultiTargetDeploymentsRefused is an apply to more than one
	// deployment where some deployment addresses more than one target.
	RolloutMultiTargetDeploymentsRefused
)

// RolloutShapeRefusedError is apply creation refusing a rollout shape it does
// not run. It is the caller's request to change, not a server failure, and the
// message names only the database, environment and counts, so a caller may
// show it as is.
type RolloutShapeRefusedError struct {
	Database    string
	Environment string
	Refusal     RolloutShapeRefusal
	// Targets is how many targets the apply would run on.
	Targets int
	// Deployments is how many deployments those targets route through.
	Deployments int
	// FirstTarget is the --target selector of the first target, in rollout
	// order, of a deployment with several targets: the one to apply first
	// when applying one target at a time.
	FirstTarget string
}

func (e *RolloutShapeRefusedError) Error() string {
	switch e.Refusal {
	case RolloutDeferCutoverRefused:
		return fmt.Sprintf("%s/%s rolls out to %d targets, and --defer-cutover is not supported on an apply to more than one target; apply again without --defer-cutover, or apply one target with --target",
			e.Database, e.Environment, e.Targets)
	case RolloutMultiTargetDeploymentsRefused:
		return fmt.Sprintf("%s/%s rolls out to %d targets across %d deployments, and an apply to more than one deployment is not supported yet when a deployment has several targets; apply one target at a time, starting with --target %s",
			e.Database, e.Environment, e.Targets, e.Deployments, e.FirstTarget)
	}
	return fmt.Sprintf("%s/%s: this rollout shape is not supported", e.Database, e.Environment)
}

// IsMultiTargetRollout reports whether some deployment among targets addresses
// more than one target. That is the rollout a multi-target apply runs; a
// deployment that addresses one target, or several deployments that each
// address one, is not.
func IsMultiTargetRollout(targets []routing.ExecutionTarget) bool {
	return len(routing.MultiTargetDeployments(targets)) > 0
}

// rolloutShapeRefusal is the refusal apply creation returns for an apply of
// targets whatever options it carries, or "" when their shape admits one. A
// plan carries it so the CLI refuses before it prompts or takes the lock.
func rolloutShapeRefusal(database, environment string, targets []routing.ExecutionTarget) string {
	if err := RefuseUnsupportedRolloutShape(database, environment, targets, false); err != nil {
		return err.Error()
	}
	return ""
}

// RefuseUnsupportedRolloutShape refuses an apply to targets that a multi-target
// rollout does not run: --defer-cutover, or more than one deployment. An apply
// that is not multi-target is admitted unchanged, whatever it asks for. Apply
// creation runs it on every path, and the webhook runs it before it takes the
// apply lock, so a refused command holds nothing.
func RefuseUnsupportedRolloutShape(database, environment string, targets []routing.ExecutionTarget, deferCutover bool) error {
	multiTarget := routing.MultiTargetDeployments(targets)
	if len(multiTarget) == 0 {
		return nil
	}
	deployments := make(map[string]struct{}, len(targets))
	members := make(map[string]struct{}, len(targets))
	selectors := rolloutMemberSelectors(targets)
	firstTarget := ""
	for i, t := range targets {
		deployments[t.Deployment] = struct{}{}
		members[t.MemberID()] = struct{}{}
		if firstTarget == "" && multiTarget[t.Deployment] {
			firstTarget = selectors[i]
		}
	}
	refused := &RolloutShapeRefusedError{
		Database:    database,
		Environment: environment,
		Targets:     len(members),
		Deployments: len(deployments),
		FirstTarget: firstTarget,
	}
	// The deployment shape is refused first: it refuses the apply whatever
	// options it carries, so dropping --defer-cutover would not admit it.
	if len(deployments) > 1 {
		refused.Refusal = RolloutMultiTargetDeploymentsRefused
		return refused
	}
	if deferCutover {
		refused.Refusal = RolloutDeferCutoverRefused
		return refused
	}
	return nil
}
