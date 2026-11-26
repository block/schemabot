package ternconv

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// The PlanRefusal detail travels base64-encoded in a response trailer, which
// proxies bound, so it lists at most maxPlanRefusalProblems problems, as many
// as a PR comment renders, and cuts each field to what a caller shows. The
// field bounds are bytes, cut on a rune boundary, so the whole detail stays
// far below a proxy's header limit whatever the files contain.
const (
	maxPlanRefusalProblems       = 25
	maxPlanProblemFileBytes      = 256
	maxPlanProblemMessageBytes   = 768
	maxPlanProblemStatementBytes = 256
)

// PlanRefusalStatus is the FAILED_PRECONDITION status a data plane answers a
// refused plan with: the refusal's text as the message and, when the refusal
// names statements in the schema files, those statements as a PlanRefusal
// detail, bounded as the constants above say, with the ones left out counted.
// A caller that predates the detail reads the message alone, which still
// describes every problem.
func PlanRefusalStatus(message string, problems []engine.PlanProblem) (*status.Status, error) {
	st := status.New(codes.FailedPrecondition, message)
	if len(problems) == 0 {
		return st, nil
	}
	listed := problems[:min(len(problems), maxPlanRefusalProblems)]
	detailed, err := st.WithDetails(&ternv1.PlanRefusal{
		Problems: planProblemsToProto(listed),
		Omitted:  clampCount(len(problems) - len(listed)),
	})
	if err != nil {
		return st, fmt.Errorf("attach %d plan problems to the refusal status: %w", len(problems), err)
	}
	return detailed, nil
}

// PlanProblemsFromStatus returns the statements a refused plan's status lists
// and how many more it refused without listing them. It returns nil and 0 when
// the status carries no PlanRefusal detail, as a refusal about the target or
// one from a data plane that predates the detail does.
func PlanProblemsFromStatus(st *status.Status) ([]engine.PlanProblem, int) {
	if st == nil || st.Code() != codes.FailedPrecondition {
		return nil, 0
	}
	var problems []engine.PlanProblem
	omitted := 0
	for _, detail := range st.Details() {
		refusal, ok := detail.(*ternv1.PlanRefusal)
		if !ok {
			continue
		}
		omitted += max(int(refusal.GetOmitted()), 0)
		for _, p := range refusal.GetProblems() {
			problems = append(problems, engine.PlanProblem{
				File:      p.GetFile(),
				Line:      int(p.GetLine()),
				Message:   p.GetMessage(),
				Statement: p.GetStatement(),
			})
		}
	}
	return problems, omitted
}

func planProblemsToProto(problems []engine.PlanProblem) []*ternv1.PlanProblem {
	out := make([]*ternv1.PlanProblem, len(problems))
	for i, p := range problems {
		out[i] = &ternv1.PlanProblem{
			File:      cutToBytes(p.File, maxPlanProblemFileBytes),
			Line:      clampLine(p.Line),
			Message:   cutToBytes(p.Message, maxPlanProblemMessageBytes),
			Statement: cutToBytes(p.Statement, maxPlanProblemStatementBytes),
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

// clampCount narrows a count to the wire's int32.
func clampCount(n int) int32 {
	return int32(min(n, 1<<31-1))
}

// cutToBytes returns s whole when it fits limit bytes, and otherwise its
// longest prefix that ends on a rune boundary and leaves room for " …", which
// marks that more followed.
func cutToBytes(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	const more = " …"
	cut := limit - len(more)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return strings.TrimRight(s[:cut], " \t\r\n") + more
}
