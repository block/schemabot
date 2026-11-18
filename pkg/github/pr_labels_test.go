package github

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchPRLabelStateReadsOnlyTheTerminalPage(t *testing.T) {
	const label = `{"id":13,"event":"labeled","label":{"name":"ack"}}`
	for _, tc := range []struct {
		name, tail      string
		moving          bool
		event, revision int64
		present         bool
	}{
		{"acknowledged", "[" + label + "]", false, 13, 0, true},
		{"event outside window", "[]", false, 0, 0, false},
		{"removed", "[" + label + `,{"id":14,"event":"unlabeled","label":{"name":"ack"}}]`, false, 14, 0, false},
		{"reapplied", "[" + label + `,{"id":15,"event":"labeled","label":{"name":"ack"}}]`, false, 15, 0, true},
		{"force pushed", "[" + label + `,{"id":16,"event":"head_ref_force_pushed"}]`, false, 13, 16, true},
		{"tail moved", "[" + label + "]", true, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, mux := setupRateLimitedTestGitHubServer(t)
			ic := NewInstallationClient(client, slog.Default())
			route := "/repos/org/repo/issues/1/timeline"
			var pages []string
			mux.HandleFunc(route, func(w http.ResponseWriter, r *http.Request) {
				page := r.URL.Query().Get("page")
				pages = append(pages, page)
				switch page {
				case "":
					w.Header().Set("Link", fmt.Sprintf(`<%s?page=2>; rel="next", <%s?page=99>; rel="last"`, route, route))
					_, _ = io.WriteString(w, "["+label+"]")
				case "99":
					if tc.moving {
						w.Header().Set("Link", fmt.Sprintf(`<%s?page=100>; rel="next"`, route))
					}
					_, _ = io.WriteString(w, tc.tail)
				default:
					t.Errorf("unexpected timeline page %s", page)
					w.WriteHeader(http.StatusBadRequest)
				}
			})
			state, err := ic.FetchPRLabelState(t.Context(), "org/repo", 1, "ack")
			require.NoError(t, err)
			assert.Equal(t, []string{"", "99"}, pages)
			assert.Equal(t, tc.event, state.EventID)
			assert.Equal(t, tc.revision, state.RevisionID)
			assert.Equal(t, tc.present, state.Present)
		})
	}
}
