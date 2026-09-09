package apitypes

import (
	"fmt"
	"strconv"
)

const (
	buildOperationMetadataKey   = "executor_operation"
	buildServerPhaseMetadataKey = "server_phase"
)

// BuildWork describes server-reported work for the operation currently being
// executed. Counter fields are zero when the server does not report that kind
// of work.
type BuildWork struct {
	Operation    string
	ServerPhase  string
	Attempt      int64
	BlocksDone   int64
	BlocksTotal  int64
	TuplesDone   int64
	TuplesTotal  int64
	LockersDone  int64
	LockersTotal int64
}

// IsConcurrentIndexBuild reports whether the work belongs to a concurrent
// index build. The operation name is pg-sprite's progress.OperationConcurrentIndex
// value, carried through the engine's progress metadata as a plain string so
// this package does not depend on the executor.
func (w BuildWork) IsConcurrentIndexBuild() bool {
	return w.Operation == "concurrent-index-build"
}

// WaitingOnLockers reports whether the server is in a concurrent index phase
// that waits for sessions which could conflict with the build.
func (w BuildWork) WaitingOnLockers() bool {
	switch w.ServerPhase {
	case "waiting for writers before build",
		"waiting for writers before validation",
		"waiting for old snapshots",
		"waiting for readers before marking dead":
		return true
	default:
		return false
	}
}

// ParseBuildWork decodes the current operation's work from progress display
// metadata. It returns the zero value with a nil error when the engine
// publishes no operation, and an error when a published counter is not a
// non-negative integer, so a renderer can log the malformed value and omit
// the work rather than show a wrong value. A counter is published when its
// key is present, whatever the value: an absent key means the engine has no
// reading, while a present empty value is malformed.
func ParseBuildWork(metadata map[string]string) (BuildWork, error) {
	operation := metadata[buildOperationMetadataKey]
	if operation == "" {
		return BuildWork{}, nil
	}
	work := BuildWork{Operation: operation, ServerPhase: metadata[buildServerPhaseMetadataKey]}
	fields := []struct {
		key   string
		value *int64
	}{
		{"attempt", &work.Attempt},
		{"blocks_done", &work.BlocksDone},
		{"blocks_total", &work.BlocksTotal},
		{"tuples_done", &work.TuplesDone},
		{"tuples_total", &work.TuplesTotal},
		{"lockers_done", &work.LockersDone},
		{"lockers_total", &work.LockersTotal},
	}
	for _, field := range fields {
		raw, present := metadata[field.key]
		if !present {
			continue
		}
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return BuildWork{}, fmt.Errorf("progress metadata %s %q is not an integer: %w", field.key, raw, err)
		}
		if value < 0 {
			return BuildWork{}, fmt.Errorf("progress metadata %s %d is negative", field.key, value)
		}
		*field.value = value
	}
	return work, nil
}
