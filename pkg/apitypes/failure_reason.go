package apitypes

// StatusFailureReasonWidth is the width, in bytes, at which the status
// listing clamps an apply's failure reason: a longer reason is cut from the
// tail and ends in StatusFailureReasonEllipsis, so it keeps
// StatusFailureReasonKeptWidth bytes of its own text. The listing is the
// narrowest operator surface a failure reason is rendered on, and an engine
// that composes a refusal with its remedy last keeps the remedy's lead inside
// the kept width so that surface still says what to do next.
const (
	StatusFailureReasonWidth     = 240
	StatusFailureReasonEllipsis  = "..."
	StatusFailureReasonKeptWidth = StatusFailureReasonWidth - len(StatusFailureReasonEllipsis)
)
