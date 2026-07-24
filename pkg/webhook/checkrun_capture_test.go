package webhook

import (
	ghclient "github.com/block/schemabot/pkg/github"
)

// checkRunCapture captures the Check Run payload a test's fake GitHub server
// receives, so assertions can inspect exactly what was published. It is shared
// by the unit tests and (being untagged) the integration-tagged tests; tests
// decode only the fields they assert on.
type checkRunCapture struct {
	Name       string                   `json:"name"`
	HeadSHA    string                   `json:"head_sha"`
	Status     string                   `json:"status"`
	Conclusion string                   `json:"conclusion"`
	Output     *ghclient.CheckRunOutput `json:"output"`
}
