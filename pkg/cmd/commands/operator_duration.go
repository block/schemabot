package commands

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// operatorNow is the clock the operator commands measure --last windows and
// rate limit pauses against.
func operatorNow() time.Time { return time.Now().UTC() }

// parseOperatorDuration parses the human-friendly duration format shared by
// the operator flags (--last, --stuck-after). Errors name
// no flag; the caller wraps with the flag it was parsing.
func parseOperatorDuration(value string) (time.Duration, error) {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return 0, fmt.Errorf("duration must not be empty")
	}
	if duration, err := time.ParseDuration(trimmed); err == nil {
		if duration <= 0 {
			return 0, fmt.Errorf("duration must be positive")
		}
		return duration, nil
	}

	amountText, unit, ok := strings.Cut(trimmed, " ")
	if !ok {
		amountText, unit = splitOperatorDurationAmountAndUnit(trimmed)
	}
	amount, err := strconv.Atoi(amountText)
	if err != nil {
		return 0, fmt.Errorf("parse duration %q: use a positive duration like 1h, 6h, 2d, or '2 days'", value)
	}
	if amount <= 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	switch strings.TrimSpace(unit) {
	case "d", "day", "days":
		return time.Duration(amount) * 24 * time.Hour, nil
	case "h", "hour", "hours":
		return time.Duration(amount) * time.Hour, nil
	case "m", "min", "mins", "minute", "minutes":
		return time.Duration(amount) * time.Minute, nil
	}
	return 0, fmt.Errorf("parse duration %q: use a positive duration like 1h, 6h, 2d, or '2 days'", value)
}

func splitOperatorDurationAmountAndUnit(value string) (string, string) {
	for i, r := range value {
		if r < '0' || r > '9' {
			return value[:i], value[i:]
		}
	}
	return value, ""
}
