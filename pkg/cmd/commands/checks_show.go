package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/caller"
	"github.com/block/schemabot/pkg/checkstate"
	cmdclient "github.com/block/schemabot/pkg/cmd/client"
)

// ChecksShowCmd answers "why is this pull request's SchemaBot check saying
// that" without a trip through server logs or the database.
//
// A blocked merge gate has two records behind it: the Check Run GitHub shows,
// and the stored check state SchemaBot decides it from. The interesting cases
// are the ones where those disagree, because a row recorded for a commit the
// pull request has since moved past holds the gate open whatever it concluded,
// and from the outside that is indistinguishable from an apply that hung. This
// prints both, with the reading of each row: what it says, whether it is
// holding the gate, and whether SchemaBot will resolve it on its own or is
// waiting on a person.
//
// The pull request is named however the operator already has it. Reaching for
// this command means having a pull request open, and what is on the clipboard
// then is its address, not a repository and a number to be typed out
// separately.
type ChecksShowCmd struct {
	PullRequest string `arg:"" name:"pull-request" help:"Pull request: its URL, owner/name#number, or owner/name with the number as the next argument"`
	Number      int    `arg:"" optional:"" name:"number" help:"Pull request number, when the first argument does not carry one"`
	Environment string `short:"e" help:"Only show rows for this environment"`
	JSON        bool   `help:"Output as JSON"`
}

func (cmd *ChecksShowCmd) Run(ctx context.Context, g *Globals) error {
	repo, pr, err := cmd.target()
	if err != nil {
		return err
	}
	endpoint, err := g.Resolve()
	if err != nil {
		return err
	}

	response, err := cmdclient.ChecksInspect(ctx, endpoint, apitypes.ChecksInspectRequest{
		Repo:        repo,
		PullRequest: pr,
		Environment: cmd.Environment,
	})
	if err != nil {
		return fmt.Errorf("inspect check state for %s#%d: %w", repo, pr, err)
	}

	if cmd.JSON {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(response)
	}
	return renderChecksInspection(os.Stdout, response)
}

// target resolves the pull request to inspect from the two positionals. The
// number may ride along in the first argument or stand alone as the second,
// and giving both a reference that carries a number and a different number is
// refused: resolving it by precedence would silently inspect a pull request
// the operator did not name.
func (cmd *ChecksShowCmd) target() (string, int, error) {
	repo, pr, err := caller.ParsePullRequestReference(cmd.PullRequest)
	if err != nil {
		return "", 0, err
	}
	switch {
	case pr == 0 && cmd.Number <= 0:
		return "", 0, fmt.Errorf("%s names a repository but no pull request; add the number, or pass the pull request URL", repo)
	case pr == 0:
		return repo, cmd.Number, nil
	case cmd.Number > 0 && cmd.Number != pr:
		return "", 0, fmt.Errorf("%q names pull request %d but the next argument is %d; pass one of the two", cmd.PullRequest, pr, cmd.Number)
	default:
		return repo, pr, nil
	}
}

func renderChecksInspection(w io.Writer, response *apitypes.ChecksInspectResponse) error {
	if _, err := fmt.Fprintf(w, "%s#%d is %s at %s.\n", response.Repo, response.PullRequest, response.PRState, shortSHA(response.HeadSHA)); err != nil {
		return err
	}
	if err := writeCheckRunLines(w, response); err != nil {
		return err
	}
	if len(response.Rows) == 0 {
		_, err := fmt.Fprintln(w, "\nSchemaBot holds no check state for this pull request.")
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "\nENVIRONMENT\tDATABASE\tCOMMIT\tSTATUS\tCONCLUSION\tDISPOSITION"); err != nil {
		return err
	}
	for _, row := range response.Rows {
		commit := shortSHA(row.RecordedSHA)
		if !row.CoversHead {
			commit += " (older)"
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			row.Environment, row.Database, commit, row.Status, orDash(row.Conclusion), row.Reason); err != nil {
			return err
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	return writeChecksBlockingSection(w, response)
}

// writeCheckRunLines reports the Check Runs branch protection actually gates
// on. An absent one is worth stating: a pull request with stored rows and no
// Check Run on its head is a different problem from one whose Check Run is
// sitting in progress, and `checks backfill` is what addresses the first.
//
// Every expected name is reported, present or not. A deployment publishing one
// check per environment has one required check per name, so a present staging
// run says nothing about an absent production run, and folding them into a
// single line would hide the one an operator has to act on.
func writeCheckRunLines(w io.Writer, response *apitypes.ChecksInspectResponse) error {
	for _, run := range response.CheckRunsOnHead {
		line := fmt.Sprintf("Check Run %q on %s: %s", run.Name, shortSHA(response.HeadSHA), run.Status)
		if run.Conclusion != "" {
			line += " / " + run.Conclusion
		}
		if run.StartedAt != "" {
			line += fmt.Sprintf(" (started %s)", run.StartedAt)
		}
		if _, err := fmt.Fprintln(w, line+"."); err != nil {
			return err
		}
	}
	if !response.ChecksEnabled {
		return writeChecksDisabledLine(w, response)
	}
	if err := writeMissingCheckRunLine(w, response); err != nil {
		return err
	}
	if err := writeUntrustedConflictLine(w, response); err != nil {
		return err
	}
	return writeUnreadableCheckRunLine(w, response)
}

// writeUntrustedConflictLine names the expected Check Runs another app is also
// answering under. Branch protection reads whichever run it picked, and no
// backfill touches the other app's, so an operator told only that a run is
// missing runs the backfill and watches the gate stay closed. The conflict is
// resolved on GitHub — by removing or renaming the other app's run, or by
// trusting that app — and this line is the only place the text output says so.
func writeUntrustedConflictLine(w io.Writer, response *apitypes.ChecksInspectResponse) error {
	if len(response.UntrustedConflictNames) == 0 {
		return nil
	}
	_, err := fmt.Fprintf(w, "Held by another app on %s: %s. A backfill recreates SchemaBot's run but cannot touch the other app's, so remove or rename it, or add its app to the trusted apps, before expecting the gate to move.\n",
		shortSHA(response.HeadSHA), strings.Join(response.UntrustedConflictNames, ", "))
	return err
}

// writeChecksDisabledLine states that the deployment publishes no Check Runs
// for the repository, so no absence here is a gap to close. Whatever sits on
// the head under an expected name came from somewhere else, and pointing at
// the backfill would report a configuration choice as an incident.
func writeChecksDisabledLine(w io.Writer, response *apitypes.ChecksInspectResponse) error {
	if len(response.CheckRunsOnHead) == 0 {
		_, err := fmt.Fprintf(w, "This deployment does not publish Check Runs for %s (enable_checks: false), so there is none on %s.\n",
			response.Repo, shortSHA(response.HeadSHA))
		return err
	}
	subject := "the run above is not one"
	if len(response.CheckRunsOnHead) > 1 {
		subject = "the runs above are not ones"
	}
	_, err := fmt.Fprintf(w, "This deployment does not publish Check Runs for %s (enable_checks: false), so %s it maintains.\n",
		response.Repo, subject)
	return err
}

// writeMissingCheckRunLine names the expected Check Runs GitHub reported no run
// for, and what recreates them.
//
// The backfill refuses a pull request whose head still carries an uncompleted
// Check Run, since recreating one beside an apply in flight would publish a
// verdict over a result that has not landed. Recommending it there would send
// an operator after a command that reports the pull request as held and
// recreates nothing, so the line says what the backfill will actually do.
func writeMissingCheckRunLine(w io.Writer, response *apitypes.ChecksInspectResponse) error {
	if len(response.MissingCheckRunNames) == 0 {
		return nil
	}
	missing := strings.Join(response.MissingCheckRunNames, ", ")
	if unconcluded := unconcludedCheckRunNames(response); len(unconcluded) > 0 {
		_, err := fmt.Fprintf(w, "Missing on %s: %s. `sq schemabot checks backfill %s` will not recreate %s while %s has not concluded: the backfill holds a pull request whose head still carries an uncompleted Check Run.\n",
			shortSHA(response.HeadSHA), missing, response.Repo, recreatesPronoun(response.MissingCheckRunNames), strings.Join(unconcluded, ", "))
		return err
	}
	_, err := fmt.Fprintf(w, "Missing on %s: %s. `sq schemabot checks backfill %s` recreates %s.\n",
		shortSHA(response.HeadSHA), missing, response.Repo, recreatesPronoun(response.MissingCheckRunNames))
	return err
}

// writeUnreadableCheckRunLine names the expected Check Runs GitHub could not be
// read for. Such a name is neither present nor missing, and the difference is
// the operator's next step: a missing run is recreated, an unreadable one is
// read again once GitHub answers.
func writeUnreadableCheckRunLine(w io.Writer, response *apitypes.ChecksInspectResponse) error {
	if len(response.UnreadableCheckRunNames) == 0 {
		return nil
	}
	if len(response.CheckRunsOnHead) == 0 && len(response.MissingCheckRunNames) == 0 {
		_, err := fmt.Fprintf(w, "No Check Run on %s could be read; see server logs. Whether one is missing is not known, so the backfill is not the next step yet.\n",
			shortSHA(response.HeadSHA))
		return err
	}
	_, err := fmt.Fprintf(w, "Could not read %s on %s; see server logs. Whether %s on the head is not known.\n",
		strings.Join(response.UnreadableCheckRunNames, ", "), shortSHA(response.HeadSHA), presencePronoun(response.UnreadableCheckRunNames))
	return err
}

func recreatesPronoun(names []string) string {
	if len(names) == 1 {
		return "it"
	}
	return "them"
}

func presencePronoun(names []string) string {
	if len(names) == 1 {
		return "it is"
	}
	return "they are"
}

// writeChecksBlockingSection explains every row holding the gate open, and
// separates the two that matter operationally: rows SchemaBot resolves on its
// own, where the answer is to wait or to nudge a plan, and rows waiting on a
// person, where no amount of waiting or re-planning changes anything.
func writeChecksBlockingSection(w io.Writer, response *apitypes.ChecksInspectResponse) error {
	var waiting, owed []apitypes.InspectedCheck
	for _, row := range response.Rows {
		switch {
		case !row.Blocking:
			continue
		case row.SelfConverging:
			waiting = append(waiting, row)
		default:
			owed = append(owed, row)
		}
	}
	if len(waiting) == 0 && len(owed) == 0 {
		return writeChecksNothingBlockingLine(w, response)
	}
	if err := writeChecksRowDetail(w, "\nWaiting on SchemaBot:", waiting); err != nil {
		return err
	}
	return writeChecksRowDetail(w, "\nWaiting on an operator:", owed)
}

// writeChecksNothingBlockingLine states what is left once no stored row is
// holding the gate. Settled rows are not the same as a clear gate, so the three
// things that outlive them are reported before the passing line: a required
// Check Run that is absent, one that exists without a passing result, and one
// GitHub could not be read for. Each keeps branch protection closed on its own,
// and the last leaves the question open rather than answered.
//
// A name another app also answers under is reported before any of them: the
// gate may be reading that run, so no claim this command can make about
// SchemaBot's own state settles whether the gate is clear.
//
// The passing line also names the environment when the response was narrowed
// to one. Rows outside that environment were never read, so an unqualified
// claim about the whole pull request would be one this response cannot make.
func writeChecksNothingBlockingLine(w io.Writer, response *apitypes.ChecksInspectResponse) error {
	if len(response.MissingCheckRunNames) > 0 && response.ChecksEnabled {
		_, err := fmt.Fprintf(w, "\nNo stored row is holding the merge gate open, but a missing Check Run is: branch protection cannot pass without %s.\n",
			strings.Join(response.MissingCheckRunNames, ", "))
		return err
	}
	if len(response.UntrustedConflictNames) > 0 {
		_, err := fmt.Fprintf(w, "\nNo stored row is holding the merge gate open, but another app answers under %s on %s, so which run branch protection reads is not SchemaBot's to say.\n",
			strings.Join(response.UntrustedConflictNames, ", "), shortSHA(response.HeadSHA))
		return err
	}
	if holding := checkRunNamesHoldingGate(response); len(holding) > 0 {
		_, err := fmt.Fprintf(w, "\nNo stored row is holding the merge gate open, but branch protection is, on %s: %s.\n",
			shortSHA(response.HeadSHA), strings.Join(holding, ", "))
		return err
	}
	if len(response.UnreadableCheckRunNames) > 0 {
		_, err := fmt.Fprintf(w, "\nNo stored row is holding the merge gate open, but %s could not be read on GitHub, so whether the gate is clear is not known; see server logs.\n",
			strings.Join(response.UnreadableCheckRunNames, ", "))
		return err
	}
	if response.Environment != "" {
		_, err := fmt.Fprintf(w, "\nNothing in %s is holding the merge gate open. Other environments were not read.\n", response.Environment)
		return err
	}
	_, err := fmt.Fprintln(w, "\nNothing is holding the merge gate open.")
	return err
}

// checkRunNamesHoldingGate reports the Check Runs on the head that branch
// protection is still waiting on: one without a conclusion, and one whose
// conclusion is not a passing result. A run in either state holds the gate
// whatever the stored rows say, and the disagreement is itself the finding.
func checkRunNamesHoldingGate(response *apitypes.ChecksInspectResponse) []string {
	var names []string
	for _, run := range response.CheckRunsOnHead {
		switch {
		case run.Status != checkstate.StatusCompleted:
			names = append(names, fmt.Sprintf("%q is %s", run.Name, run.Status))
		case !checkstate.ConclusionClearsGate(run.Conclusion):
			names = append(names, fmt.Sprintf("%q concluded %s", run.Name, orDash(run.Conclusion)))
		}
	}
	return names
}

// unconcludedCheckRunNames reports the Check Runs on the head GitHub has not
// settled. This is the state the backfill refuses to write over, so it decides
// what the missing-run line can recommend.
func unconcludedCheckRunNames(response *apitypes.ChecksInspectResponse) []string {
	var names []string
	for _, run := range response.CheckRunsOnHead {
		if run.Status != checkstate.StatusCompleted {
			names = append(names, fmt.Sprintf("%q (%s)", run.Name, run.Status))
		}
	}
	return names
}

func writeChecksRowDetail(w io.Writer, heading string, rows []apitypes.InspectedCheck) error {
	if len(rows) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(w, heading); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(w, "  %s/%s: %s\n", row.Environment, row.Database, row.Summary); err != nil {
			return err
		}
		if row.ApplyIdentifier != "" {
			if _, err := fmt.Fprintf(w, "    apply: %s (%s)\n", row.ApplyIdentifier, orDash(row.ApplyState)); err != nil {
				return err
			}
		}
		if row.BlockingReason != "" {
			if _, err := fmt.Fprintf(w, "    blocking reason: %s\n", row.BlockingReason); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "    do: %s\n", row.Remedy); err != nil {
			return err
		}
	}
	return nil
}

// shortSHA abbreviates a commit for display. Anything shorter than the
// abbreviation is printed whole rather than padded, so a test fixture or an
// empty head reads as itself.
func shortSHA(sha string) string {
	const abbrev = 8
	if len(sha) <= abbrev {
		if sha == "" {
			return "an unknown commit"
		}
		return sha
	}
	return sha[:abbrev]
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}
