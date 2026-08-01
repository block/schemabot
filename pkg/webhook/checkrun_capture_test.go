package webhook

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

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

// serveTrustedCheckRuns registers the commit check-runs lookup used to resolve
// the newest run for a check name, reporting each given run as created by the
// trusted "schemabot" GitHub App. Like the real API, it honors the check_name
// filter so lookups for other names see only their own runs.
func serveTrustedCheckRuns(t *testing.T, mux *http.ServeMux, repo, sha string, runs ...ghclient.CheckRunResult) {
	t.Helper()
	mux.HandleFunc("GET /repos/"+repo+"/commits/"+sha+"/check-runs", func(w http.ResponseWriter, r *http.Request) {
		nameFilter := r.URL.Query().Get("check_name")
		out := make([]map[string]any, 0, len(runs))
		for _, run := range runs {
			if nameFilter != "" && run.Name != nameFilter {
				continue
			}
			out = append(out, map[string]any{
				"id":         run.ID,
				"name":       run.Name,
				"status":     run.Status,
				"conclusion": run.Conclusion,
				"app":        map[string]any{"slug": "schemabot"},
			})
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"total_count": len(out),
			"check_runs":  out,
		}))
	})
}
