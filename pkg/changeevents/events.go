// Package changeevents delivers best-effort schema change notifications.
package changeevents

import "time"

// Type identifies the lifecycle transition an event describes.
type Type string

const (
	OperationStarted  Type = "operation.started"
	OperationFinished Type = "operation.finished"
	ApplyFinished     Type = "apply.finished"
)

// Event is a snapshot taken by the owner of a lifecycle transition.
type Event struct {
	Type            Type
	App             string
	Environment     string
	OccurredAtNanos int64
	Description     string
}

// New records the time at which the producer observed the transition.
func New(kind Type, app, environment, description string) Event {
	return Event{
		Type: kind, App: app, Environment: environment,
		OccurredAtNanos: time.Now().UnixNano(), Description: description,
	}
}
