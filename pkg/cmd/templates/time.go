package templates

import (
	"fmt"
	"time"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// WriteTimeInfo writes time information based on state.
// State should be in canonical form (e.g. "failed", "stopped").
func WriteTimeInfo(startedAt, completedAt, s string) {
	if startedAt == "" {
		return
	}

	started, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return
	}

	if completedAt != "" {
		// Finished - show duration with state-appropriate wording
		completed, err := time.Parse(time.RFC3339, completedAt)
		if err == nil {
			duration := completed.Sub(started)
			switch s {
			case state.Apply.Failed:
				fmt.Printf("  %sFailed after %s%s\n",
					ANSIDim, ui.FormatHumanDuration(duration), ANSIReset)
			case state.Apply.Stopped:
				fmt.Printf("  %sStopped after %s%s\n",
					ANSIDim, ui.FormatHumanDuration(duration), ANSIReset)
			default:
				fmt.Printf("  %sCompleted in %s%s\n",
					ANSIDim, ui.FormatHumanDuration(duration), ANSIReset)
			}
			return
		}
	}

	// Show elapsed time with state-appropriate wording
	elapsed := nowFunc().Sub(started)
	switch s {
	case state.Apply.Stopped:
		fmt.Printf("  %sStopped after %s%s\n",
			ANSIDim, ui.FormatHumanDuration(elapsed), ANSIReset)
	case state.Apply.Failed:
		fmt.Printf("  %sFailed after %s%s\n",
			ANSIDim, ui.FormatHumanDuration(elapsed), ANSIReset)
	default:
		fmt.Printf("  %sRunning for %s%s\n",
			ANSIDim, ui.FormatHumanDuration(elapsed), ANSIReset)
	}
}
