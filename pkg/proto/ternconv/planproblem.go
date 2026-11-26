package ternconv

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// PlanRefusalStatus is the FAILED_PRECONDITION status a data plane answers a
// refused plan with: the refusal's text as the message and, when the refusal
// names statements in the schema files, those statements as a PlanRefusal
// detail. A caller that predates the detail reads the message alone, which
// still describes every problem.
func PlanRefusalStatus(message string, problems []engine.PlanProblem) (*status.Status, error) {
	st := status.New(codes.FailedPrecondition, message)
	if len(problems) == 0 {
		return st, nil
	}
	detailed, err := st.WithDetails(&ternv1.PlanRefusal{Problems: planProblemsToProto(problems)})
	if err != nil {
		return st, fmt.Errorf("attach %d plan problems to the refusal status: %w", len(problems), err)
	}
	return detailed, nil
}

// PlanProblemsFromStatus returns the statements a refused plan's status names,
// or nil when the status carries no PlanRefusal detail, as a refusal about the
// target or one from a data plane that predates the detail does.
func PlanProblemsFromStatus(st *status.Status) []engine.PlanProblem {
	if st == nil || st.Code() != codes.FailedPrecondition {
		return nil
	}
	var problems []engine.PlanProblem
	for _, detail := range st.Details() {
		refusal, ok := detail.(*ternv1.PlanRefusal)
		if !ok {
			continue
		}
		for _, p := range refusal.GetProblems() {
			problems = append(problems, engine.PlanProblem{
				File:      p.GetFile(),
				Line:      int(p.GetLine()),
				Message:   p.GetMessage(),
				Statement: p.GetStatement(),
			})
		}
	}
	return problems
}

func planProblemsToProto(problems []engine.PlanProblem) []*ternv1.PlanProblem {
	out := make([]*ternv1.PlanProblem, len(problems))
	for i, p := range problems {
		out[i] = &ternv1.PlanProblem{
			File:      p.File,
			Line:      clampLine(p.Line),
			Message:   p.Message,
			Statement: p.Statement,
		}
	}
	return out
}

// clampLine narrows a line number to the wire's int32. A line past its range,
// or below 1, is sent as unknown.
func clampLine(line int) int32 {
	if line < 1 || line > 1<<31-1 {
		return 0
	}
	return int32(line)
}
