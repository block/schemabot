package apitypes

import (
	"fmt"
	"strconv"
)

// Progress display-metadata keys under which an engine reports the position
// of a running apply inside its statement sequence.
const (
	ProgressStepMetadataKey       = "step"
	ProgressStepsTotalMetadataKey = "steps_total"
	ProgressStatementMetadataKey  = "statement"
)

// ProgressStep is the position of a running apply inside its statement
// sequence: the one-based step being executed, the sequence length, and the
// statement text the engine reports for that step (empty when the engine
// does not publish it).
type ProgressStep struct {
	Step       int
	StepsTotal int
	Statement  string
}

// ParseProgressStep decodes the statement position carried in progress
// display metadata. It returns the zero value with a nil error when the
// engine publishes no position, and an error when a published value is not a
// positive integer, so a renderer can log the malformed value and show no
// position rather than a wrong one.
func ParseProgressStep(metadata map[string]string) (ProgressStep, error) {
	rawStep, rawTotal := metadata[ProgressStepMetadataKey], metadata[ProgressStepsTotalMetadataKey]
	if rawStep == "" && rawTotal == "" {
		return ProgressStep{}, nil
	}
	step, err := parsePositiveInt(ProgressStepMetadataKey, rawStep)
	if err != nil {
		return ProgressStep{}, err
	}
	total, err := parsePositiveInt(ProgressStepsTotalMetadataKey, rawTotal)
	if err != nil {
		return ProgressStep{}, err
	}
	if step > total {
		return ProgressStep{}, fmt.Errorf("progress metadata %s %d exceeds %s %d", ProgressStepMetadataKey, step, ProgressStepsTotalMetadataKey, total)
	}
	return ProgressStep{Step: step, StepsTotal: total, Statement: metadata[ProgressStatementMetadataKey]}, nil
}

func parsePositiveInt(key, raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("progress metadata %s %q is not an integer: %w", key, raw, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("progress metadata %s %d is not positive", key, value)
	}
	return value, nil
}
