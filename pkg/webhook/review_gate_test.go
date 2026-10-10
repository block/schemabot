package webhook

import (
	"cmp"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

func TestReviewGateErrorDetailTeamMembership(t *testing.T) {
	err := fmt.Errorf("expand team @octocat/schema-admins: %w", ghclient.ErrTeamMembershipUnreadable)

	detail := reviewGateErrorDetail(err)

	assert.Contains(t, detail, "Review gate check failed; see server logs for details")
	assert.Contains(t, detail, "GitHub App can read organization members")
	assert.NotContains(t, detail, "expand team @octocat/schema-admins",
		"raw error text must never render in PR markdown")
}

func TestReviewGateErrorDetailGeneric(t *testing.T) {
	detail := reviewGateErrorDetail(assert.AnError)

	assert.Contains(t, detail, "Review gate check failed; see server logs for details")
	assert.NotContains(t, detail, assert.AnError.Error(),
		"raw error text must never render in PR markdown")
	assert.NotContains(t, detail, "GitHub App can read organization members")
}

func setupReviewGateHandler(t *testing.T, config *api.ServerConfig) (*Handler, *http.ServeMux) {
	t.Helper()
	if config == nil {
		config = &api.ServerConfig{}
	}

	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	svc := api.New(&emptyStorage{}, config, nil, logger)

	installClient := ghclient.NewInstallationClient(client, logger)
	factory := &fakeClientFactory{client: installClient}

	h := NewHandler(svc, factory, nil, logger)
	return h, mux
}

func registerPREndpoint(mux *http.ServeMux, prAuthor string) {
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
		pr := &gh.PullRequest{
			Head: &gh.PullRequestBranch{
				Ref: new("feature-branch"),
				SHA: new(reviewGateTestHeadSHA),
			},
			Base: &gh.PullRequestBranch{
				Ref: new("main"),
				SHA: new("def456"),
			},
			User: &gh.User{Login: new(prAuthor)},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(pr)
	})
}

func registerCodeownersEndpoint(mux *http.ServeMux, content string, found bool) {
	mux.HandleFunc("GET /repos/octocat/hello-world/contents/.github/CODEOWNERS", func(w http.ResponseWriter, _ *http.Request) {
		if !found {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(&gh.ErrorResponse{
				Response: &http.Response{StatusCode: http.StatusNotFound},
				Message:  "Not Found",
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.RepositoryContent{
			Type:     new("file"),
			Encoding: new("base64"),
			Content:  new(base64.StdEncoding.EncodeToString([]byte(content))),
		})
	})
	// Fallback for other CODEOWNERS locations
	if !found {
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/CODEOWNERS", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(&gh.ErrorResponse{
				Response: &http.Response{StatusCode: http.StatusNotFound},
				Message:  "Not Found",
			})
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/contents/docs/CODEOWNERS", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(&gh.ErrorResponse{
				Response: &http.Response{StatusCode: http.StatusNotFound},
				Message:  "Not Found",
			})
		})
	}
}

// reviewGateTestHeadSHA is the PR head commit registerPREndpoint serves.
const reviewGateTestHeadSHA = "abc123"

// registerReviewsEndpoint serves reviews for PR 1. A review without a commit
// is served as submitted on the PR head.
func registerReviewsEndpoint(mux *http.ServeMux, reviews []*gh.PullRequestReview) {
	for _, r := range reviews {
		if r.CommitID == nil {
			r.CommitID = new(reviewGateTestHeadSHA)
		}
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/reviews", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reviews)
	})
}

func TestCheckReviewGate_Disabled(t *testing.T) {
	h, _ := setupReviewGateHandler(t, nil)

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	assert.Nil(t, result, "gate should return nil when disabled")
}

func TestCheckReviewGate_NoReviewsBlocks(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, []string{"bob"}, result.OperatorReviewers)
	assert.Empty(t, result.OtherReviewers)
}

func TestCheckReviewGate_OperatorUserApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
}

// Commits the review gate tests build: the default branch's tip, the older
// default branch commit an approved commit was built on, and the approved
// commit itself.
const (
	reviewGateBaseTip  = "main222"
	reviewGateOldBase  = "main111"
	reviewGateApproved = "aaa111"
)

// The approval-coverage tables expect one of three verdicts per approval, or
// an evaluation error.
const (
	wantCovers     = "covers"
	wantChanged    = "changed"
	wantUncompared = "uncompared"
)

// An operator approves the PR at an earlier commit and the author then
// rebases it onto a newer default branch, merges the default branch in, or
// pushes more commits. The approval keeps counting while the PR's own change
// to the database's schema inputs (schema directory, followed symlinks,
// config, environment link) is the same at the head, however much the default
// branch moved underneath it. When the PR's change differs at the head, the PR
// is blocked until someone approves the head and the comment says why. When
// the approved commit cannot be compared with the head, the approval does not
// count and the PR is blocked the same way. When GitHub is unavailable during
// the comparison, the gate cannot decide and returns a retryable evaluation
// error, never a verdict.
func TestCheckReviewGate_ApprovalCoverage(t *testing.T) {
	ordersTable := "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	baseFiles := map[string]string{
		"README.md":                         "readme",
		"app/orders.go":                     "package app",
		"schema/testdb/schemabot.yaml":      "database: orders\n",
		"schema/testdb/README.md":           "orders schema",
		"schema/testdb/orders/orders.sql":   ordersTable,
		"schema/testdb/orders/vschema.json": `{"sharded": true}`,
		"schema/testdb/legacy/old.sql":      "CREATE TABLE `old` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
		"schema/testdb/legacy_alias":        reviewGateSymlink + "legacy",
		"schema/testdb/shared":              reviewGateSymlink + "../shared/orders",
		"schema/shared/orders/audit.sql":    "CREATE TABLE `audit` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
		"schema/payments/ledger.sql":        "CREATE TABLE `ledger` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
	}
	envFiles := map[string]string{
		"schema/testdb/schemabot.yaml":             "database: orders\n",
		"schema/testdb/production/orders.sql":      ordersTable,
		"schema/testdb/staging/orders_staging.sql": ordersTable,
	}
	addVotes := func(files map[string]string) {
		files["schema/testdb/orders/votes.sql"] = "CREATE TABLE `votes` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	}
	addVotesAnd := func(more func(map[string]string)) func(map[string]string) {
		return func(files map[string]string) {
			addVotes(files)
			more(files)
		}
	}

	tests := []struct {
		name string
		// base is the default branch the approved commit was built on;
		// baseChange turns it into the default branch's tip.
		base       map[string]string
		baseChange func(map[string]string)
		// prChange turns the old default branch into the approved commit.
		// headChange builds the head from the tip, or from the old default
		// branch when headOnOldBase is set; nil replays prChange.
		// approvedOnNewBase builds the approved commit from the tip instead,
		// so with headOnOldBase the head is on older base content.
		prChange          func(map[string]string)
		headChange        func(map[string]string)
		headOnOldBase     bool
		approvedOnNewBase bool
		schemaPath        string
		schemaLinkPath    string
		configPath        string
		// approvedUnknown leaves the approved commit unknown to GitHub.
		approvedUnknown  bool
		treesUnavailable bool
		want             string
		wantErr          error
	}{
		{
			name:     "a rebase onto a default branch that added tables beside the PR's counts",
			prChange: addVotes,
			baseChange: func(files map[string]string) {
				files["schema/testdb/orders/feedback.sql"] = "CREATE TABLE `feedback` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
				files["schema/testdb/orders/receipts.sql"] = "CREATE TABLE `receipts` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
				files["README.md"] = "readme, updated on the default branch"
			},
			want: wantCovers,
		},
		{
			name: "a rebase onto a default branch that changed the config and removed a namespace counts",
			prChange: func(files map[string]string) {
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
			},
			baseChange: func(files map[string]string) {
				delete(files, "schema/testdb/legacy/old.sql")
				delete(files, "schema/testdb/legacy_alias")
				files["schema/testdb/schemabot.yaml"] = "database: orders\nignore_namespaces:\n  - legacy\n"
				files["schema/testdb/README.md"] = "orders schema, without legacy"
			},
			want: wantCovers,
		},
		{
			name:     "a rebase onto a default branch that changed only another database counts",
			prChange: addVotes,
			baseChange: func(files map[string]string) {
				files["schema/payments/ledger.sql"] = "CREATE TABLE `ledger` (`id` bigint NOT NULL, `amount` bigint, PRIMARY KEY (`id`))"
			},
			want: wantCovers,
		},
		{
			name:     "a rebase onto a default branch that changed a symlinked namespace's target counts",
			prChange: addVotes,
			baseChange: func(files map[string]string) {
				files["schema/shared/orders/audit.sql"] = "CREATE TABLE `audit` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
			},
			want: wantCovers,
		},
		{
			name: "a rebase onto a default branch that changed a symlinked config's target counts",
			base: func() map[string]string {
				files := maps.Clone(baseFiles)
				files["schema/testdb/schemabot.yaml"] = reviewGateSymlink + "../shared/orders.yaml"
				files["schema/shared/orders.yaml"] = "database: orders\n"
				return files
			}(),
			prChange: addVotes,
			baseChange: func(files map[string]string) {
				files["schema/shared/orders.yaml"] = "database: orders\nignore_tables:\n  - legacy_old\n"
			},
			want: wantCovers,
		},
		{
			name: "a head rebuilt on an older default branch that restores a config outside the schema directory does not count",
			base: func() map[string]string {
				files := maps.Clone(baseFiles)
				files["schemabot.yaml"] = "database: orders\n"
				return files
			}(),
			baseChange: func(files map[string]string) {
				files["schemabot.yaml"] = "database: orders\nignore_tables:\n  - legacy_old\n"
			},
			prChange:          addVotes,
			approvedOnNewBase: true,
			headOnOldBase:     true,
			configPath:        "schemabot.yaml",
			want:              wantChanged,
		},
		{
			name:              "a head rebuilt on an older default branch that left the schema inputs alone counts",
			baseChange:        func(files map[string]string) { files["README.md"] = "readme, updated on the default branch" },
			prChange:          addVotes,
			approvedOnNewBase: true,
			headOnOldBase:     true,
			want:              wantCovers,
		},
		{
			name:          "a push on the same base that changed only files outside the schema inputs counts",
			prChange:      addVotes,
			headChange:    addVotesAnd(func(files map[string]string) { files["app/orders.go"] = "package app // votes" }),
			headOnOldBase: true,
			want:          wantCovers,
		},
		{
			name:     "the PR editing its own schema file after the approval does not count",
			prChange: addVotes,
			headChange: func(files map[string]string) {
				files["schema/testdb/orders/votes.sql"] = "CREATE TABLE `votes` (`id` bigint NOT NULL, `score` int, PRIMARY KEY (`id`))"
			},
			want: wantChanged,
		},
		{
			// The PR was stacked on another, so its approved diff carried the
			// lower PR's table. The lower PR merged with a revised table, and
			// the rebase left this PR changing only its own files.
			name: "a rebase after the stacked PR below it merged a revised table counts",
			prChange: addVotesAnd(func(files map[string]string) {
				files["schema/testdb/orders/captures.sql"] = "CREATE TABLE `captures` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
			}),
			baseChange: func(files map[string]string) {
				files["schema/testdb/orders/captures.sql"] = "CREATE TABLE `captures` (`id` bigint NOT NULL, `task_id` bigint, PRIMARY KEY (`id`))"
			},
			headChange: addVotes,
			want:       wantCovers,
		},
		{
			name: "a rebase after the stacked PR below it merged unchanged counts",
			prChange: addVotesAnd(func(files map[string]string) {
				files["schema/testdb/orders/captures.sql"] = "CREATE TABLE `captures` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
			}),
			baseChange: func(files map[string]string) {
				files["schema/testdb/orders/captures.sql"] = "CREATE TABLE `captures` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
			},
			headChange: addVotes,
			want:       wantCovers,
		},
		{
			name: "a table the PR dropped beside one the default branch took over does not count",
			prChange: func(files map[string]string) {
				files["schema/testdb/captures/captures.sql"] = "CREATE TABLE `captures` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
				files["schema/testdb/captures/capture_events.sql"] = "CREATE TABLE `capture_events` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
			},
			baseChange: func(files map[string]string) {
				files["schema/testdb/captures/captures.sql"] = "CREATE TABLE `captures` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
			},
			headChange: func(map[string]string) {},
			want:       wantChanged,
		},
		{
			name: "a head rebuilt on an older default branch that no longer carries the PR's edit does not count",
			baseChange: func(files map[string]string) {
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `total` bigint, PRIMARY KEY (`id`))"
			},
			prChange: func(files map[string]string) {
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `total` bigint, `note` text, PRIMARY KEY (`id`))"
			},
			headChange:        func(map[string]string) {},
			approvedOnNewBase: true,
			headOnOldBase:     true,
			want:              wantChanged,
		},
		{
			name:       "the PR dropping a table it had added after the approval does not count",
			prChange:   addVotes,
			headChange: func(map[string]string) {},
			want:       wantChanged,
		},
		{
			name:     "a push on the same base that changed another schema file does not count",
			prChange: addVotes,
			headChange: addVotesAnd(func(files map[string]string) {
				files["schema/testdb/legacy/old.sql"] = "CREATE TABLE `old` (`id` bigint NOT NULL, `x` int, PRIMARY KEY (`id`))"
			}),
			headOnOldBase: true,
			want:          wantChanged,
		},
		{
			name: "a rebase that combined the PR's change with the default branch's in one file does not count",
			prChange: func(files map[string]string) {
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
			},
			baseChange: func(files map[string]string) {
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `total` bigint, PRIMARY KEY (`id`))"
			},
			headChange: func(files map[string]string) {
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `total` bigint, `note` text, PRIMARY KEY (`id`))"
			},
			want: wantChanged,
		},
		{
			name:     "a head that restores a schema file the default branch changed does not count",
			prChange: addVotes,
			baseChange: func(files map[string]string) {
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `total` bigint, PRIMARY KEY (`id`))"
			},
			headChange: addVotesAnd(func(files map[string]string) { files["schema/testdb/orders/orders.sql"] = ordersTable }),
			want:       wantChanged,
		},
		{
			name: "a rebase that kept the PR's version of a file over the default branch's does not count",
			prChange: func(files map[string]string) {
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
			},
			baseChange: func(files map[string]string) {
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `total` bigint, PRIMARY KEY (`id`))"
			},
			headChange: func(files map[string]string) {
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
			},
			want: wantChanged,
		},
		{
			name:     "the PR changing the config after the approval does not count",
			prChange: addVotes,
			headChange: addVotesAnd(func(files map[string]string) {
				files["schema/testdb/schemabot.yaml"] = "database: orders\nignore_tables:\n  - orders\n"
			}),
			want: wantChanged,
		},
		{
			name: "the PR editing a config outside the schema directory after the approval does not count",
			base: func() map[string]string {
				files := maps.Clone(baseFiles)
				files["schemabot.yaml"] = "database: orders\n"
				return files
			}(),
			prChange: addVotes,
			headChange: addVotesAnd(func(files map[string]string) {
				files["schemabot.yaml"] = "database: orders\nignore_tables:\n  - orders\n"
			}),
			configPath: "schemabot.yaml",
			want:       wantChanged,
		},
		{
			name:     "schema files and a config the PR adds outside the database's inputs after the approval do not affect it",
			prChange: addVotes,
			headChange: addVotesAnd(func(files map[string]string) {
				files["schema/payments/refunds.sql"] = "CREATE TABLE `refunds` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
				files["schema/payments/schemabot.yaml"] = "database: payments\n"
				files["db/stray.sql"] = "DROP TABLE `orders`"
			}),
			want: wantCovers,
		},
		{
			name:     "the PR changing a symlinked namespace's target after the approval does not count",
			prChange: addVotes,
			headChange: addVotesAnd(func(files map[string]string) {
				files["schema/shared/orders/audit.sql"] = "CREATE TABLE `audit` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
			}),
			want: wantChanged,
		},
		{
			name: "the PR retargeting the environment link after the approval does not count",
			base: func() map[string]string {
				files := maps.Clone(baseFiles)
				files["schema/production"] = reviewGateSymlink + "testdb"
				return files
			}(),
			prChange:       addVotes,
			headChange:     addVotesAnd(func(files map[string]string) { files["schema/production"] = reviewGateSymlink + "payments" }),
			schemaLinkPath: "schema/production",
			want:           wantChanged,
		},
		{
			name: "another environment's schema changing in the PR does not affect the approval",
			base: envFiles,
			prChange: func(files map[string]string) {
				files["schema/testdb/production/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
			},
			headChange: func(files map[string]string) {
				files["schema/testdb/production/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
				files["schema/testdb/staging/orders_staging.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `x` int, PRIMARY KEY (`id`))"
			},
			schemaPath: "schema/testdb/production",
			want:       wantCovers,
		},
		{
			name:       "a schema path the PR added after the approval does not count",
			base:       envFiles,
			headChange: func(files map[string]string) { files["schema/testdb/canary/orders.sql"] = ordersTable },
			schemaPath: "schema/testdb/canary",
			want:       wantChanged,
		},
		{
			name: "a symlink pointing outside the repository cannot be compared",
			base: func() map[string]string {
				files := maps.Clone(baseFiles)
				files["schema/testdb/escape"] = reviewGateSymlink + "../../../outside"
				return files
			}(),
			prChange:   addVotes,
			headChange: addVotesAnd(func(files map[string]string) { files["app/orders.go"] = "package app // votes" }),
			want:       wantUncompared,
		},
		{
			name:       "a schema path reached through a symlink cannot be compared",
			base:       map[string]string{"linked": reviewGateSymlink + "schema", "schema/testdb/orders/orders.sql": ordersTable},
			schemaPath: "linked/testdb",
			configPath: "linked/testdb/schemabot.yaml",
			want:       wantUncompared,
		},
		{
			name:            "an approved commit GitHub cannot find cannot be compared",
			prChange:        addVotes,
			approvedUnknown: true,
			want:            wantUncompared,
		},
		{
			name:             "a tree lookup GitHub cannot answer is a retryable evaluation failure",
			prChange:         addVotes,
			treesUnavailable: true,
			wantErr:          ghclient.ErrGitHubUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
				db := cfg.Databases["orders"]
				db.OperatorUsers = []string{"bob"}
				cfg.Databases["orders"] = db
			}))
			registerPREndpoint(mux, "alice")
			registerReviewsEndpoint(mux, []*gh.PullRequestReview{
				{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new(reviewGateApproved)},
			})

			derive := func(from map[string]string, change func(map[string]string)) map[string]string {
				files := maps.Clone(from)
				if change != nil {
					change(files)
				}
				return files
			}
			oldBase := tt.base
			if oldBase == nil {
				oldBase = baseFiles
			}
			tip := derive(oldBase, tt.baseChange)
			headChange := tt.headChange
			if headChange == nil {
				headChange = tt.prChange
			}
			headBase, headMergeBase := tip, reviewGateBaseTip
			if tt.headOnOldBase {
				headBase, headMergeBase = oldBase, reviewGateOldBase
			}
			commits := map[string]map[string]string{
				reviewGateOldBase:     oldBase,
				reviewGateBaseTip:     tip,
				reviewGateTestHeadSHA: derive(headBase, headChange),
			}
			mergeBases := map[string]string{reviewGateTestHeadSHA: headMergeBase}
			if !tt.approvedUnknown {
				approvedBase, approvedMergeBase := oldBase, reviewGateOldBase
				if tt.approvedOnNewBase {
					approvedBase, approvedMergeBase = tip, reviewGateBaseTip
				}
				commits[reviewGateApproved] = derive(approvedBase, tt.prChange)
				mergeBases[reviewGateApproved] = approvedMergeBase
			}
			registerReviewGateBaseBranch(t, mux, mergeBases)
			if tt.treesUnavailable {
				mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{sha}", func(w http.ResponseWriter, _ *http.Request) {
					http.Error(w, "service unavailable", http.StatusServiceUnavailable)
				})
			} else {
				registerReviewGateGitObjects(t, mux, commits)
			}

			client, err := h.clientForRepo("octocat/hello-world", 12345)
			require.NoError(t, err)

			schema := reviewGateSchema(cmp.Or(tt.schemaPath, "schema/testdb"), reviewGateTestHeadSHA)
			schema.SchemaLinkPath = tt.schemaLinkPath
			schema.ConfigPath = cmp.Or(tt.configPath, "schema/testdb/schemabot.yaml")
			result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, schema)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, result)
				return
			}
			require.NoError(t, err, "an approval that does not count blocks on the merits, not as an evaluation failure")
			require.NotNil(t, result)
			assertApprovalVerdict(t, result, tt.want, "bob")
		})
	}
}

// assertApprovalVerdict checks the gate result for a single authorized
// reviewer's approval on an earlier commit.
func assertApprovalVerdict(t *testing.T, result *ReviewGateResult, want, reviewer string) {
	t.Helper()
	switch want {
	case wantCovers:
		assert.True(t, result.Approved)
		assert.Empty(t, result.ChangedApprovers)
		assert.Empty(t, result.UncomparedApprovers)
	case wantChanged:
		assert.False(t, result.Approved)
		assert.Equal(t, []string{reviewer}, result.ChangedApprovers)
		assert.Empty(t, result.UncomparedApprovers)
	case wantUncompared:
		assert.False(t, result.Approved)
		assert.Empty(t, result.ChangedApprovers)
		assert.Equal(t, []string{reviewer}, result.UncomparedApprovers)
	default:
		require.Failf(t, "unknown verdict", "%q", want)
	}
}

// An approval GitHub recorded without a commit cannot be compared with the
// head, so it does not count and the gate blocks, reading nothing to find
// that out.
func TestCheckReviewGate_ApprovalWithoutCommit(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new("")},
	})
	reads := registerReviewGateBaseBranch(t, mux, nil)
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assertApprovalVerdict(t, result, wantUncompared, "bob")
	assert.Zero(t, reads.refs.Load(), "an approval without a commit needs no comparison")
}

// reviewGateSchema is the schema request the review gate tests evaluate: the
// orders database read from schemaPath at headSHA.
func reviewGateSchema(schemaPath, headSHA string) *ghclient.SchemaRequestResult {
	return &ghclient.SchemaRequestResult{Database: "orders", SchemaPath: schemaPath, HeadSHA: headSHA}
}

// reviewGateBaseReads counts the base branch reads registerReviewGateBaseBranch
// served.
type reviewGateBaseReads struct {
	refs atomic.Int64
	mu   sync.Mutex
	// compared lists the commits whose merge base was read, in order.
	compared []string
}

func (r *reviewGateBaseReads) comparedCommits() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.compared)
}

// registerReviewGateBaseBranch serves the default branch at reviewGateBaseTip
// and, for each commit in mergeBases, its merge base with that tip. Any other
// commit is unknown to GitHub.
func registerReviewGateBaseBranch(t *testing.T, mux *http.ServeMux, mergeBases map[string]string) *reviewGateBaseReads {
	t.Helper()
	reads := &reviewGateBaseReads{}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		reads.refs.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.Reference{Ref: new("refs/heads/main"), Object: &gh.GitObject{SHA: new(reviewGateBaseTip), Type: new("commit")}})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, r *http.Request) {
		base, commit, _ := strings.Cut(r.PathValue("range"), "...")
		reads.mu.Lock()
		reads.compared = append(reads.compared, commit)
		reads.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		// The default branch's history is the old base followed by its tip,
		// so of two default-branch commits the old base is their merge base.
		onDefaultBranch := func(sha string) bool { return sha == reviewGateOldBase || sha == reviewGateBaseTip }
		if onDefaultBranch(base) && onDefaultBranch(commit) {
			mergeBase := reviewGateOldBase
			if base == commit {
				mergeBase = base
			}
			_ = json.NewEncoder(w).Encode(&gh.CommitsComparison{Status: new("diverged"), MergeBaseCommit: &gh.RepositoryCommit{SHA: &mergeBase}})
			return
		}
		mergeBase, ok := mergeBases[commit]
		if base != reviewGateBaseTip || !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "No commit found for SHA: " + commit})
			return
		}
		_ = json.NewEncoder(w).Encode(&gh.CommitsComparison{Status: new("diverged"), MergeBaseCommit: &gh.RepositoryCommit{SHA: &mergeBase}})
	})
	return reads
}

// reviewGateSymlink prefixes a file's content in registerReviewGateGitObjects
// to make the file a symlink whose target is the rest of the content.
const reviewGateSymlink = "symlink:"

// registerReviewGateGitObjects serves GitHub's Git Trees and Blobs APIs for
// commits, each given as repo-relative file paths mapped to content. Object
// SHAs are content-addressed like Git's, so a directory has the same tree SHA
// at two commits exactly when its content is identical. Unknown commits and
// objects are 404s.
// It returns the number of blob reads served.
func registerReviewGateGitObjects(t *testing.T, mux *http.ServeMux, commits map[string]map[string]string) *atomic.Int64 {
	t.Helper()
	blobReads := new(atomic.Int64)
	type object struct {
		name, mode, kind, sha string
	}
	trees := make(map[string][]object)
	blobs := make(map[string]string)
	hash := func(kind, content string) string {
		sum := sha1.Sum([]byte(kind + "\x00" + content))
		return hex.EncodeToString(sum[:])
	}
	var buildTree func(files map[string]string) string
	buildTree = func(files map[string]string) string {
		subdirs := make(map[string]map[string]string)
		var entries []object
		for name, content := range files {
			if dir, rest, nested := strings.Cut(name, "/"); nested {
				if subdirs[dir] == nil {
					subdirs[dir] = make(map[string]string)
				}
				subdirs[dir][rest] = content
				continue
			}
			mode := "100644"
			if target, isLink := strings.CutPrefix(content, reviewGateSymlink); isLink {
				mode, content = "120000", target
			}
			sha := hash("blob", content)
			blobs[sha] = content
			entries = append(entries, object{name: name, mode: mode, kind: "blob", sha: sha})
		}
		for dir, subFiles := range subdirs {
			entries = append(entries, object{name: dir, mode: "040000", kind: "tree", sha: buildTree(subFiles)})
		}
		slices.SortFunc(entries, func(a, b object) int { return strings.Compare(a.name, b.name) })
		var serialized strings.Builder
		for _, e := range entries {
			fmt.Fprintf(&serialized, "%s %s %s\n", e.mode, e.name, e.sha)
		}
		sha := hash("tree", serialized.String())
		trees[sha] = entries
		return sha
	}
	roots := make(map[string]string, len(commits))
	for commit, files := range commits {
		roots[commit] = buildTree(files)
	}

	notFound := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{"message": "Not Found"})
	}
	mux.HandleFunc("GET /repos/octocat/hello-world/git/trees/{sha}", func(w http.ResponseWriter, r *http.Request) {
		treeSHA := r.PathValue("sha")
		if root, isCommit := roots[treeSHA]; isCommit {
			treeSHA = root
		}
		if _, ok := trees[treeSHA]; !ok {
			notFound(w)
			return
		}
		var listed []*gh.TreeEntry
		var list func(sha, prefix string)
		list = func(sha, prefix string) {
			for _, e := range trees[sha] {
				listed = append(listed, &gh.TreeEntry{Path: new(prefix + e.name), Mode: new(e.mode), Type: new(e.kind), SHA: new(e.sha)})
				if e.kind == "tree" && r.URL.Query().Get("recursive") != "" {
					list(e.sha, prefix+e.name+"/")
				}
			}
		}
		list(treeSHA, "")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.Tree{SHA: new(treeSHA), Entries: listed, Truncated: new(false)})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/blobs/{sha}", func(w http.ResponseWriter, r *http.Request) {
		blobReads.Add(1)
		content, ok := blobs[r.PathValue("sha")]
		if !ok {
			notFound(w)
			return
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(content))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.Blob{Content: &encoded, Encoding: new("base64")})
	})
	return blobReads
}

// A schema directory holds more distinct symlinks than a comparison reads.
// The comparison cannot be completed, so the approval does not count, and the
// gate gives up before reading any symlink: a directory full of links costs
// one listing, not a GitHub read per link.
func TestCheckReviewGate_SymlinkBudgetCheckedBeforeReads(t *testing.T) {
	files := map[string]string{
		"schema/testdb/schemabot.yaml":    "database: orders\n",
		"schema/testdb/orders/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
	}
	for i := range 200 {
		files[fmt.Sprintf("schema/testdb/alias_%03d", i)] = reviewGateSymlink + fmt.Sprintf("orders/../orders_%03d", i)
	}
	result, blobReads, err := checkReviewGateOnUnchangedFiles(t, files)
	require.NoError(t, err)
	require.NotNil(t, result)
	assertApprovalVerdict(t, result, wantUncompared, "bob")
	assert.Zero(t, blobReads, "the symlink budget is checked before any symlink is read")
}

// A schema directory holds many aliases with the same target text. They share
// one read, so the comparison completes and the approval counts.
func TestCheckReviewGate_SymlinksWithTheSameTargetShareOneRead(t *testing.T) {
	files := map[string]string{
		"schema/testdb/schemabot.yaml":    "database: orders\n",
		"schema/testdb/orders/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
	}
	for i := range 200 {
		files[fmt.Sprintf("schema/testdb/alias_%03d", i)] = reviewGateSymlink + "orders"
	}
	result, blobReads, err := checkReviewGateOnUnchangedFiles(t, files)
	require.NoError(t, err)
	require.NotNil(t, result)
	assertApprovalVerdict(t, result, wantCovers, "bob")
	assert.Equal(t, int64(1), blobReads, "aliases with the same target text share one read")
}

// checkReviewGateOnUnchangedFiles evaluates bob's approval on an earlier
// commit when the default branch, the approved commit, and the head all hold
// files, and returns the result, the number of blob reads it took, and the
// evaluation error.
func checkReviewGateOnUnchangedFiles(t *testing.T, files map[string]string) (*ReviewGateResult, int64, error) {
	t.Helper()
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new(reviewGateApproved)},
	})
	registerReviewGateBaseBranch(t, mux, map[string]string{reviewGateApproved: reviewGateBaseTip, reviewGateTestHeadSHA: reviewGateBaseTip})
	blobReads := registerReviewGateGitObjects(t, mux, map[string]map[string]string{reviewGateBaseTip: files, reviewGateApproved: files, reviewGateTestHeadSHA: files})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	schema := reviewGateSchema("schema/testdb", reviewGateTestHeadSHA)
	schema.ConfigPath = "schema/testdb/schemabot.yaml"
	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, schema)
	return result, blobReads.Load(), err
}

// Two operators approved different earlier commits. One cannot be compared
// with the head; the other proves the PR's schema change is the same, so the
// gate is decided by that one and the apply proceeds. When the comparable
// approval is of a different change instead, neither approval counts and the
// gate blocks, naming each reviewer under the reason their approval missed.
func TestCheckReviewGate_ApprovalNotComparableDefersToOtherApprovals(t *testing.T) {
	const otherApproved = "bbb222"
	ordersTable := "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
	base := map[string]string{"schema/testdb/orders.sql": ordersTable}
	withNote := map[string]string{"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"}
	withMemo := map[string]string{"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, `memo` text, PRIMARY KEY (`id`))"}
	for _, tt := range []struct {
		name         string
		otherCommit  map[string]string
		wantApproved bool
	}{
		{name: "another approval covers the head", otherCommit: withNote, wantApproved: true},
		{name: "no other approval covers the head", otherCommit: withMemo},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
				db := cfg.Databases["orders"]
				db.OperatorUsers = []string{"bob", "carol"}
				cfg.Databases["orders"] = db
			}))
			registerPREndpoint(mux, "alice")
			now := time.Now()
			// bob's commit is unknown to GitHub, so it cannot be compared.
			registerReviewsEndpoint(mux, []*gh.PullRequestReview{
				{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new(reviewGateApproved)},
				{User: &gh.User{Login: new("carol")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new(otherApproved)},
			})
			registerReviewGateBaseBranch(t, mux, map[string]string{otherApproved: reviewGateOldBase, reviewGateTestHeadSHA: reviewGateOldBase})
			registerReviewGateGitObjects(t, mux, map[string]map[string]string{
				reviewGateOldBase:     base,
				otherApproved:         tt.otherCommit,
				reviewGateTestHeadSHA: withNote,
			})
			client, err := h.clientForRepo("octocat/hello-world", 12345)
			require.NoError(t, err)

			result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
			require.NoError(t, err)
			require.NotNil(t, result)
			if !tt.wantApproved {
				assert.False(t, result.Approved)
				assert.Equal(t, []string{"bob"}, result.UncomparedApprovers)
				assert.Equal(t, []string{"carol"}, result.ChangedApprovers)
				return
			}
			assert.True(t, result.Approved)
		})
	}
}

// Two operators approved the same earlier commit and a third approved the
// head: the head approval satisfies the gate on its own, so the earlier
// commit is never compared.
func TestCheckReviewGate_HeadApprovalOutranksEarlierApprovals(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob", "carol", "dave"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	now := time.Now()
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new(reviewGateApproved)},
		{User: &gh.User{Login: new("carol")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new(reviewGateApproved)},
		{User: &gh.User{Login: new("dave")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}},
	})
	reads := registerReviewGateBaseBranch(t, mux, map[string]string{reviewGateApproved: reviewGateOldBase, reviewGateTestHeadSHA: reviewGateBaseTip})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
	assert.Zero(t, reads.refs.Load(), "a head approval is checked before approvals that need a comparison")
	assert.Empty(t, reads.comparedCommits())
}

// Two operators approved the same earlier commit and nobody approved the head.
// The PR changed its schema file since that commit, so neither approval
// counts, the commit is compared exactly once, and both are named.
func TestCheckReviewGate_SharedEarlierApprovalBlocksAndComparesOnce(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob", "carol"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	now := time.Now()
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new(reviewGateApproved)},
		{User: &gh.User{Login: new("carol")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new(reviewGateApproved)},
	})
	base := map[string]string{"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}
	reads := registerReviewGateBaseBranch(t, mux, map[string]string{reviewGateApproved: reviewGateOldBase, reviewGateTestHeadSHA: reviewGateOldBase})
	registerReviewGateGitObjects(t, mux, map[string]map[string]string{
		reviewGateOldBase:     base,
		reviewGateApproved:    {"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"},
		reviewGateTestHeadSHA: {"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, `memo` text, PRIMARY KEY (`id`))"},
	})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved, "neither earlier approval covers the head")
	assert.ElementsMatch(t, []string{"bob", "carol"}, result.ChangedApprovers)
	assert.Equal(t, int64(1), reads.refs.Load(), "reviewers who approved the same commit share one comparison")
	assert.Equal(t, []string{reviewGateApproved, reviewGateTestHeadSHA}, reads.comparedCommits())
}

// The PR retargets a namespace symlink under the database's schema path after
// the approval: a non-.sql entry is part of the PR's schema change, so the
// approval no longer covers the head.
func TestCheckReviewGate_RetargetedNamespaceSymlinkInvalidatesApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new(reviewGateApproved)},
	})
	base := map[string]string{
		"schema/testdb/orders/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
		"schema/testdb/legacy/old.sql":    "CREATE TABLE `old` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
		"schema/testdb/billing":           reviewGateSymlink + "orders",
	}
	head := maps.Clone(base)
	head["schema/testdb/billing"] = reviewGateSymlink + "legacy"
	registerReviewGateBaseBranch(t, mux, map[string]string{reviewGateApproved: reviewGateOldBase, reviewGateTestHeadSHA: reviewGateOldBase})
	registerReviewGateGitObjects(t, mux, map[string]map[string]string{reviewGateOldBase: base, reviewGateApproved: base, reviewGateTestHeadSHA: head})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assertApprovalVerdict(t, result, wantChanged, "bob")
}

// GitHub is unavailable while finding the merge bases: the approval state
// could not be determined, so the gate returns an evaluation error (retryable)
// rather than a "Review Required" merit block, matching TestEnforceReviewGate's
// contract.
func TestCheckReviewGate_CompareUnavailableIsEvaluationFailure(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new(reviewGateApproved)},
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.Reference{Ref: new("refs/heads/main"), Object: &gh.GitObject{SHA: new(reviewGateBaseTip), Type: new("commit")}})
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.ErrorIs(t, err, ghclient.ErrGitHubUnavailable)
	assert.Nil(t, result)
}

// GitHub is unavailable for comparisons, but another operator approved the
// head: that approval satisfies the gate without any comparison, so the outage
// does not turn a valid approval into an evaluation failure.
func TestCheckReviewGate_HeadApprovalNeedsNoComparison(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob", "carol"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	now := time.Now()
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new(reviewGateApproved)},
		{User: &gh.User{Login: new("carol")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}},
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/git/ref/heads/main", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	// Approvals arrive in no fixed order; repeating catches an evaluation that
	// would compare the earlier commit before reaching the head approval.
	for range 5 {
		result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, result.Approved)
	}
}

// The gate measures approvals against the commit the schema files were read
// from, not against whatever head a later PR read reports: an approval on the
// PR-info head still has to prove it covers the schema's commit.
func TestCheckReviewGate_CoverageUsesSchemaCommit(t *testing.T) {
	const schemaSHA = "fff999"
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new(reviewGateTestHeadSHA)},
	})
	base := map[string]string{"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"}
	reads := registerReviewGateBaseBranch(t, mux, map[string]string{reviewGateTestHeadSHA: reviewGateOldBase, schemaSHA: reviewGateOldBase})
	registerReviewGateGitObjects(t, mux, map[string]map[string]string{
		reviewGateOldBase:     base,
		reviewGateTestHeadSHA: {"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"},
		schemaSHA:             {"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, `memo` text, PRIMARY KEY (`id`))"},
	})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", schemaSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assertApprovalVerdict(t, result, wantChanged, "bob")
	assert.Equal(t, []string{reviewGateTestHeadSHA, schemaSHA}, reads.comparedCommits())
}

// Without the commit the schema was read from, no approval can be measured
// against it: the gate fails closed with an evaluation error.
func TestCheckReviewGate_UnknownSchemaCommitIsEvaluationFailure(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}},
	})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", ""))
	require.ErrorContains(t, err, "the commit the schema was read from is unknown")
	assert.Nil(t, result)
}

func TestCheckReviewGate_AdminUserApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.AdminUsers = []string{"mona"}
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("mona")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
}

// A repo admin's approval satisfies the review gate for any database managed
// through that repository, without granting approval authority on databases
// managed through other repositories.
func TestCheckReviewGate_RepoAdminUserApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.Repos = map[string]api.RepoConfig{
			"octocat/hello-world": {AdminUsers: []string{"kara"}},
		}
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("kara")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
	assert.Contains(t, result.OtherReviewers, "kara")
}

// A repo admin team's member approval satisfies the review gate for any
// database managed through that repository, resolved via GitHub team
// membership, and the configured team appears among the required reviewers.
func TestCheckReviewGate_RepoAdminTeamApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.Repos = map[string]api.RepoConfig{
			"octocat/hello-world": {AdminTeams: []string{"octocat/repo-admins"}},
		}
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})
	mux.HandleFunc("GET /orgs/octocat/teams/repo-admins/members", teamMembersHandler(t, http.StatusOK, "bob", "carol"))

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
	assert.Contains(t, result.OtherReviewers, "octocat/repo-admins")
}

// An approval from someone outside the repo admin team does not satisfy the
// review gate: team membership is resolved via GitHub, not assumed.
func TestCheckReviewGate_RepoAdminTeamNonMemberBlocked(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.Repos = map[string]api.RepoConfig{
			"octocat/hello-world": {AdminTeams: []string{"octocat/repo-admins"}},
		}
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("dave")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})
	mux.HandleFunc("GET /orgs/octocat/teams/repo-admins/members", teamMembersHandler(t, http.StatusOK, "bob", "carol"))

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
}

// An approval from a user who is only a repo admin of a different repository
// does not satisfy the review gate for this repository's PRs.
func TestCheckReviewGate_RepoAdminOfOtherRepoBlocked(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.AdminUsers = []string{"mona"}
		cfg.Repos = map[string]api.RepoConfig{
			"octocat/other-repo": {AdminUsers: []string{"kara"}},
		}
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("kara")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.NotContains(t, result.OperatorReviewers, "kara")
	assert.NotContains(t, result.OtherReviewers, "kara")
}

func TestCheckReviewGate_NotApproved(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob", "carol"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, "alice", result.PRAuthor)
	assert.Contains(t, result.OperatorReviewers, "bob")
	assert.Contains(t, result.OperatorReviewers, "carol")
}

func TestCheckReviewGate_SelfApprovalBlocked(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"alice", "bob"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("alice")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved, "self-approval should be blocked")
}

func TestCheckReviewGate_DisabledByDefault(t *testing.T) {
	h, _ := setupReviewGateHandler(t, actorAuthTestConfig(false))

	enabled := h.isReviewGateEnabled("octocat/hello-world")
	assert.False(t, enabled)
}

func TestCheckReviewGate_OperatorTeamApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorTeams = []string{"octocat/db-admins"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})
	mux.HandleFunc("GET /orgs/octocat/teams/db-admins/members", teamMembersHandler(t, http.StatusOK, "bob", "carol"))

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
}

func TestCheckReviewGate_OperatorTeamNotApproved(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorTeams = []string{"octocat/db-admins"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("dave")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})
	mux.HandleFunc("GET /orgs/octocat/teams/db-admins/members", teamMembersHandler(t, http.StatusOK, "bob", "carol"))

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
}

// The review-required comment splits reviewers by how close each principal is
// to the change: the database's own operators get their own leading section —
// they are the reviewers the author should ping — while the broader fallback
// principals (global admins, then repo admins) form the "other authorized
// reviewers" section.
func TestCheckReviewGate_OperatorsSplitFromOtherReviewers(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.AdminTeams = []string{"octocat/global-admins"}
		cfg.ReviewPolicy.AdminUsers = []string{"kara"}
		cfg.Repos = map[string]api.RepoConfig{
			"octocat/hello-world": {
				AdminTeams: []string{"octocat/repo-admins"},
				AdminUsers: []string{"dave"},
			},
		}
		db := cfg.Databases["orders"]
		db.OperatorTeams = []string{"octocat/db-admins"}
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, nil)

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, []string{"octocat/db-admins", "bob"}, result.OperatorReviewers)
	assert.Equal(t, []string{
		"octocat/global-admins", "kara",
		"octocat/repo-admins", "dave",
	}, result.OtherReviewers)
}

func TestCheckReviewGate_CodeownersIgnoredByDefault(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "* @dave\n", true)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("dave")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, []string{"bob"}, result.OperatorReviewers)
	assert.Empty(t, result.OtherReviewers)
}

func TestCheckReviewGate_CodeownersOptIn(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.IncludeCodeowners = true
		cfg.ReviewPolicy.IncludeDatabaseOperators = new(false)
	}))

	registerPREndpoint(mux, "alice")
	registerCodeownersEndpoint(mux, "schema/payments/ @bob\nschema/orders/ @carol\n", true)
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{
			User:        &gh.User{Login: new("bob")},
			State:       new(ghclient.ReviewApproved),
			SubmittedAt: &gh.Timestamp{Time: time.Now()},
		},
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/payments", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
	assert.Contains(t, result.OtherReviewers, "bob")

	result, err = h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/orders", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Contains(t, result.OtherReviewers, "carol")
}

func TestCheckReviewGate_NoConfiguredReviewersErrors(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		cfg.ReviewPolicy.IncludeDatabaseOperators = new(false)
	}))

	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, nil)

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "no configured reviewers")
}

func reviewGateTestConfig(opts ...func(*api.ServerConfig)) *api.ServerConfig {
	cfg := actorAuthTestConfig(false)
	cfg.ReviewPolicy = api.ReviewPolicyConfig{Enabled: true}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// TestEnforceReviewGate pins the review gate's three-way disposition: an
// internal evaluation failure (a GitHub reviews read here) is not a merit
// block — the gate returns the error so the command stays retryable — while
// missing approval blocks on the merits without error. The evaluation-failure
// comment must stay sanitized: no raw GitHub error text in PR markdown.
func TestEnforceReviewGate(t *testing.T) {
	schemaResult := &ghclient.SchemaRequestResult{Database: "orders", SchemaPath: "schema/testdb", HeadSHA: reviewGateTestHeadSHA}

	registerCommentsCapture := func(mux *http.ServeMux) chan string {
		comments := make(chan string, 10)
		mux.HandleFunc("POST /repos/octocat/hello-world/issues/1/comments", func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Body string `json:"body"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			comments <- body.Body
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
		})
		return comments
	}

	t.Run("evaluation failure returns error without blocking", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		comments := registerCommentsCapture(mux)
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/reviews", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "Resource not accessible by integration"})
		})

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply", false)
		require.Error(t, err)
		assert.False(t, blocked, "evaluation failure must not report a merit block")

		select {
		case body := <-comments:
			assert.Contains(t, body, "Review gate check failed; see server logs")
			assert.NotContains(t, body, "Resource not accessible", "raw GitHub error text must never render in PR markdown")
		case <-time.After(2 * time.Second):
			require.Fail(t, "timed out waiting for evaluation-failure comment")
		}
	})

	t.Run("an approval that cannot be compared blocks with a gate error naming the remedy", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		registerReviewsEndpoint(mux, []*gh.PullRequestReview{
			{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new(reviewGateApproved)},
		})
		// The approved commit is unknown to GitHub, so it has no merge base.
		registerReviewGateBaseBranch(t, mux, map[string]string{reviewGateTestHeadSHA: reviewGateBaseTip})
		comments := registerCommentsCapture(mux)

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		// A durable attempt still comments: asking GitHub again returns the
		// same answer, so the block is the command's answer and is not retried.
		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply", true)
		require.NoError(t, err)
		assert.True(t, blocked)

		select {
		case body := <-comments:
			assert.Contains(t, body, "Review Gate Error")
			assert.Contains(t, body, "**This apply needs an approval of the latest commit.** @bob approved an earlier commit.")
			assert.Contains(t, body, "1. Ask anyone listed above to approve the latest commit\n2. Once approved, run `schemabot apply -e staging` again\n")
			assert.Contains(t, body, "- @bob\n")
			assert.NotContains(t, body, "Review Required")
			assert.NotContains(t, body, reviewGateApproved, "raw comparison errors must never render in PR markdown")
		case <-time.After(2 * time.Second):
			require.Fail(t, "timed out waiting for gate-error comment")
		}
	})

	t.Run("durable attempt suppresses the evaluation-failure comment", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		comments := registerCommentsCapture(mux)
		mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1/reviews", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "Resource not accessible by integration"})
		})

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply", true)
		require.Error(t, err)
		assert.False(t, blocked, "evaluation failure must not report a merit block")
		assert.Empty(t, comments, "a durable attempt must not post per-retry evaluation-failure comments")
	})

	t.Run("missing approval blocks on the merits without error", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		registerReviewsEndpoint(mux, []*gh.PullRequestReview{})
		comments := registerCommentsCapture(mux)

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply", false)
		require.NoError(t, err)
		assert.True(t, blocked)

		select {
		case body := <-comments:
			assert.Contains(t, body, "Review Required")
			assert.Contains(t, body, "@bob")
		case <-time.After(2 * time.Second):
			require.Fail(t, "timed out waiting for review-required comment")
		}
	})

	t.Run("approval on an earlier commit is named in the review-required comment", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		registerReviewsEndpoint(mux, []*gh.PullRequestReview{
			{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new(reviewGateApproved)},
		})
		registerReviewGateBaseBranch(t, mux, map[string]string{reviewGateApproved: reviewGateOldBase, reviewGateTestHeadSHA: reviewGateOldBase})
		registerReviewGateGitObjects(t, mux, map[string]map[string]string{
			reviewGateOldBase:     {"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"},
			reviewGateApproved:    {"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"},
			reviewGateTestHeadSHA: {"schema/testdb/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, `memo` text, PRIMARY KEY (`id`))"},
		})
		comments := registerCommentsCapture(mux)

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply", false)
		require.NoError(t, err)
		assert.True(t, blocked)

		select {
		case body := <-comments:
			assert.Contains(t, body, "Review Required")
			assert.Contains(t, body, "Approvals on an earlier commit no longer count because this PR's schema change is different now: @bob.")
		case <-time.After(2 * time.Second):
			require.Fail(t, "timed out waiting for review-required comment")
		}
	})

	t.Run("approved review passes without comment", func(t *testing.T) {
		h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
			db := cfg.Databases["orders"]
			db.OperatorUsers = []string{"bob"}
			cfg.Databases["orders"] = db
		}))
		registerPREndpoint(mux, "alice")
		registerReviewsEndpoint(mux, []*gh.PullRequestReview{
			{
				User:        &gh.User{Login: new("bob")},
				State:       new(ghclient.ReviewApproved),
				SubmittedAt: &gh.Timestamp{Time: time.Now()},
			},
		})
		comments := registerCommentsCapture(mux)

		client, err := h.clientForRepo("octocat/hello-world", 12345)
		require.NoError(t, err)

		blocked, err := h.enforceReviewGate(t.Context(), client, "octocat/hello-world", 1, 12345, schemaResult, "staging", "alice", "apply", false)
		require.NoError(t, err)
		assert.False(t, blocked)
		assert.Empty(t, comments, "an approved review must not draw a gate comment")
	})
}
