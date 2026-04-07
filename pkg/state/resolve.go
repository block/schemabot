package state

import "strings"

// NoActiveChange is the canonical state for "no active schema change".
// Returned when the progress API finds no active tasks for the database.
const NoActiveChange = "no_active_change"

// NormalizeState converts a state string from any format (proto "STATE_RUNNING",
// uppercase "RUNNING", or lowercase "running") to the canonical lowercase form ("running").
// Empty string is normalized to NoActiveChange ("no_active_change").
func NormalizeState(s string) string {
	s = strings.TrimPrefix(s, "STATE_")
	s = strings.ToLower(s)
	if s == "" {
		return NoActiveChange
	}
	return s
}
