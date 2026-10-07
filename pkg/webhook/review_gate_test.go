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

// An operator approves an earlier commit, then the author pushes more commits
// on top of it. When the head descends from the approved commit, the history
// compare decides on its own: a change under the database's schema inputs
// leaves the PR blocked until someone approves the head, and a push that
// touches no schema or config file keeps the approval.
func TestCheckReviewGate_ApprovalCommit(t *testing.T) {
	const approvedSHA = "aaa111"
	compareRange := approvedSHA + "..." + reviewGateTestHeadSHA

	tests := []struct {
		name           string
		schemaLinkPath string
		reviewCommit   string
		compareStatus  string
		compareFiles   []string
		wantApproved   bool
		wantCompare    bool
	}{
		{
			name:         "approval on the head counts without a comparison",
			reviewCommit: reviewGateTestHeadSHA,
			wantApproved: true,
		},
		{
			name:          "approval on an earlier commit counts when only non-schema files changed",
			reviewCommit:  approvedSHA,
			compareStatus: "ahead",
			compareFiles:  []string{"README.md", "app/orders.go"},
			wantApproved:  true,
			wantCompare:   true,
		},
		{
			name:          "approval on an earlier commit does not count when a schema file changed",
			reviewCommit:  approvedSHA,
			compareStatus: "ahead",
			compareFiles:  []string{"README.md", "schema/testdb/legacy_orders.sql"},
			wantCompare:   true,
		},
		{
			name:           "approval on an earlier commit does not count when the schema link changed",
			schemaLinkPath: "schema/production",
			reviewCommit:   approvedSHA,
			compareStatus:  "ahead",
			compareFiles:   []string{"schema/production"},
			wantCompare:    true,
		},
		{
			name:          "approval on an earlier commit does not count when its config changed",
			reviewCommit:  approvedSHA,
			compareStatus: "ahead",
			compareFiles:  []string{"schema/testdb/schemabot.yaml"},
			wantCompare:   true,
		},
		{
			name:         "approval without a recorded commit does not count",
			reviewCommit: "",
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
				{
					User:        &gh.User{Login: new("bob")},
					State:       new(ghclient.ReviewApproved),
					SubmittedAt: &gh.Timestamp{Time: time.Now()},
					CommitID:    new(tt.reviewCommit),
				},
			})
			compared := make(chan string, 10)
			mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, r *http.Request) {
				compared <- r.PathValue("range")
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(reviewGateComparison(tt.compareStatus, tt.compareFiles, 0))
			})

			client, err := h.clientForRepo("octocat/hello-world", 12345)
			require.NoError(t, err)

			schema := reviewGateSchema("schema/testdb", reviewGateTestHeadSHA)
			schema.SchemaLinkPath = tt.schemaLinkPath
			schema.ConfigPath = "schema/testdb/schemabot.yaml"
			result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, schema)
			require.NoError(t, err, "an approval that does not count blocks on the merits, not as an evaluation failure")
			require.NotNil(t, result)
			assert.Equal(t, tt.wantApproved, result.Approved)
			assert.Equal(t, []string{"bob"}, result.OperatorReviewers)
			if tt.wantCompare {
				require.Len(t, compared, 1)
				assert.Equal(t, compareRange, <-compared)
			} else {
				assert.Empty(t, compared, "no comparison is needed")
			}
		})
	}
}

// An operator approves an earlier commit, then the author rebases the PR onto
// a newer default branch or pushes past the compare's file cap, so GitHub's
// history compare cannot prove what changed. The gate compares the content of
// the database's schema inputs — schema directory, followed symlinks, config —
// at the approved commit and the head instead: identical inputs keep the
// approval, and any difference, or any comparison that cannot be completed,
// leaves the PR blocked until someone approves the head. A GitHub outage
// during the comparison is a retryable evaluation failure, never a verdict.
func TestCheckReviewGate_ApprovalContentComparison(t *testing.T) {
	const approvedSHA = "aaa111"
	approvedFiles := map[string]string{
		"README.md":                         "readme",
		"app/orders.go":                     "package app",
		"schema/testdb/schemabot.yaml":      "database: orders\n",
		"schema/testdb/README.md":           "orders schema",
		"schema/testdb/orders/orders.sql":   "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
		"schema/testdb/orders/vschema.json": `{"sharded": true}`,
		"schema/testdb/legacy/old.sql":      "CREATE TABLE `old` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
		"schema/testdb/legacy_alias":        reviewGateSymlink + "legacy",
		"schema/testdb/shared":              reviewGateSymlink + "../shared/orders",
		"schema/shared/orders/audit.sql":    "CREATE TABLE `audit` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
		"schema/payments/ledger.sql":        "CREATE TABLE `ledger` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
	}
	envFiles := map[string]string{
		"schema/testdb/schemabot.yaml":             "database: orders\n",
		"schema/testdb/production/orders.sql":      "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
		"schema/testdb/staging/orders_staging.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))",
	}
	unrelatedChanges := func(files map[string]string) {
		files["README.md"] = "readme, updated on the default branch"
		files["app/orders.go"] = "package app // updated"
		files["schema/payments/ledger.sql"] = "CREATE TABLE `ledger` (`id` bigint NOT NULL, `amount` bigint, PRIMARY KEY (`id`))"
	}

	tests := []struct {
		name string
		// approved is the repository at the approved commit; nil leaves the
		// commit unknown to GitHub. head is derived from approved by
		// changeHead.
		approved         map[string]string
		changeHead       func(map[string]string)
		schemaPath       string
		configPath       string
		compareStatus    string
		compareFiles     []string
		compareFileCount int
		compareNotFound  bool
		treesUnavailable bool
		wantApproved     bool
		wantErr          error
	}{
		{
			name:          "rebased history with identical schema inputs counts",
			approved:      approvedFiles,
			changeHead:    unrelatedChanges,
			compareStatus: "diverged",
			compareFiles:  []string{"README.md", "app/orders.go", "schema/payments/ledger.sql"},
			wantApproved:  true,
		},
		{
			name:     "rebased history with a changed schema file does not count",
			approved: approvedFiles,
			changeHead: func(files map[string]string) {
				unrelatedChanges(files)
				files["schema/testdb/orders/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
			},
			compareStatus: "diverged",
		},
		{
			name:     "rebased history with a namespace removed and the config changed does not count",
			approved: approvedFiles,
			changeHead: func(files map[string]string) {
				delete(files, "schema/testdb/legacy/old.sql")
				delete(files, "schema/testdb/legacy_alias")
				files["schema/testdb/schemabot.yaml"] = "database: orders\nignore_namespaces:\n  - legacy\n"
			},
			compareStatus: "diverged",
		},
		{
			name:     "a schema path missing at the approved commit does not count",
			approved: envFiles,
			changeHead: func(files map[string]string) {
				files["schema/testdb/canary/orders.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"
			},
			schemaPath:    "schema/testdb/canary",
			configPath:    "schema/testdb/schemabot.yaml",
			compareStatus: "diverged",
		},
		{
			name:     "a config outside the environment schema root that changed does not count",
			approved: envFiles,
			changeHead: func(files map[string]string) {
				files["schema/testdb/schemabot.yaml"] = "database: orders\nignore_tables:\n  - orders\n"
			},
			schemaPath:    "schema/testdb/production",
			configPath:    "schema/testdb/schemabot.yaml",
			compareStatus: "diverged",
		},
		{
			name:     "another environment's schema changing does not affect the approval",
			approved: envFiles,
			changeHead: func(files map[string]string) {
				files["schema/testdb/staging/orders_staging.sql"] = "CREATE TABLE `orders` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
			},
			schemaPath:    "schema/testdb/production",
			configPath:    "schema/testdb/schemabot.yaml",
			compareStatus: "diverged",
			wantApproved:  true,
		},
		{
			name:     "a symlinked namespace whose target changed does not count",
			approved: approvedFiles,
			changeHead: func(files map[string]string) {
				files["schema/shared/orders/audit.sql"] = "CREATE TABLE `audit` (`id` bigint NOT NULL, `note` text, PRIMARY KEY (`id`))"
			},
			compareStatus: "diverged",
		},
		{
			name: "a symlink pointing outside the repository does not count",
			approved: func() map[string]string {
				files := maps.Clone(approvedFiles)
				files["schema/testdb/escape"] = reviewGateSymlink + "../../../outside"
				return files
			}(),
			changeHead:    func(map[string]string) {},
			compareStatus: "diverged",
		},
		{
			name:          "a schema path reached through a symlink cannot be compared and does not count",
			approved:      map[string]string{"linked": reviewGateSymlink + "schema", "schema/testdb/orders/orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`))"},
			changeHead:    func(map[string]string) {},
			schemaPath:    "linked/testdb",
			configPath:    "linked/testdb/schemabot.yaml",
			compareStatus: "diverged",
		},
		{
			name:             "a truncated compare with identical schema inputs counts",
			approved:         approvedFiles,
			changeHead:       unrelatedChanges,
			compareStatus:    "ahead",
			compareFileCount: 300,
			wantApproved:     true,
		},
		{
			name:          "linear history where only another database's schema changed counts",
			approved:      approvedFiles,
			changeHead:    unrelatedChanges,
			compareStatus: "ahead",
			compareFiles:  []string{"schema/payments/ledger.sql"},
			wantApproved:  true,
		},
		{
			name:            "an approved commit GitHub cannot find does not count",
			changeHead:      func(map[string]string) {},
			compareNotFound: true,
		},
		{
			name:             "a tree lookup GitHub cannot answer is a retryable evaluation failure",
			approved:         approvedFiles,
			changeHead:       unrelatedChanges,
			compareStatus:    "diverged",
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
				{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new(approvedSHA)},
			})
			mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if tt.compareNotFound {
					w.WriteHeader(http.StatusNotFound)
					_ = json.NewEncoder(w).Encode(map[string]any{"message": "No commit found for SHA: " + approvedSHA})
					return
				}
				_ = json.NewEncoder(w).Encode(reviewGateComparison(tt.compareStatus, tt.compareFiles, tt.compareFileCount))
			})

			head := maps.Clone(tt.approved)
			if head == nil {
				head = maps.Clone(approvedFiles)
			}
			tt.changeHead(head)
			commits := map[string]map[string]string{reviewGateTestHeadSHA: head}
			if tt.approved != nil {
				commits[approvedSHA] = tt.approved
			}
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
			schema.ConfigPath = cmp.Or(tt.configPath, "schema/testdb/schemabot.yaml")
			result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, schema)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, result)
				return
			}
			require.NoError(t, err, "an approval that does not count blocks on the merits, not as an evaluation failure")
			require.NotNil(t, result)
			assert.Equal(t, tt.wantApproved, result.Approved)
			if tt.wantApproved {
				assert.Empty(t, result.StaleApprovers)
			} else {
				assert.Equal(t, []string{"bob"}, result.StaleApprovers)
			}
		})
	}
}

// reviewGateSchema is the schema request the review gate tests evaluate: the
// orders database read from schemaPath at headSHA.
func reviewGateSchema(schemaPath, headSHA string) *ghclient.SchemaRequestResult {
	return &ghclient.SchemaRequestResult{Database: "orders", SchemaPath: schemaPath, HeadSHA: headSHA}
}

// reviewGateComparison builds a compare response listing files, padded with
// fileCount unrelated files so a test can reach GitHub's compare file cap.
func reviewGateComparison(status string, files []string, fileCount int) *gh.CommitsComparison {
	commitFiles := make([]*gh.CommitFile, 0, len(files)+fileCount)
	for _, f := range files {
		commitFiles = append(commitFiles, &gh.CommitFile{Filename: new(f), Status: new("modified")})
	}
	for i := range fileCount {
		commitFiles = append(commitFiles, &gh.CommitFile{Filename: new(fmt.Sprintf("app/file-%d.go", i)), Status: new("modified")})
	}
	return &gh.CommitsComparison{Status: new(status), Files: commitFiles}
}

// reviewGateSymlink prefixes a file's content in registerReviewGateGitObjects
// to make the file a symlink whose target is the rest of the content.
const reviewGateSymlink = "symlink:"

// registerReviewGateGitObjects serves GitHub's Git Trees and Blobs APIs for
// commits, each given as repo-relative file paths mapped to content. Object
// SHAs are content-addressed like Git's, so a directory has the same tree SHA
// at two commits exactly when its content is identical. Unknown commits and
// objects are 404s.
func registerReviewGateGitObjects(t *testing.T, mux *http.ServeMux, commits map[string]map[string]string) {
	t.Helper()
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
		content, ok := blobs[r.PathValue("sha")]
		if !ok {
			notFound(w)
			return
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(content))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.Blob{Content: &encoded, Encoding: new("base64")})
	})
}

// Two operators approved the same earlier commit and a third approved the
// head: the head approval satisfies the gate on its own, so the earlier
// commit is never compared.
func TestCheckReviewGate_HeadApprovalOutranksStaleApprovals(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob", "carol", "dave"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	now := time.Now()
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new("aaa111")},
		{User: &gh.User{Login: new("carol")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new("aaa111")},
		{User: &gh.User{Login: new("dave")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}},
	})
	compares := make(chan struct{}, 10)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, _ *http.Request) {
		compares <- struct{}{}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.CommitsComparison{
			Status: new("ahead"),
			Files:  []*gh.CommitFile{{Filename: new("schema/testdb/orders.sql"), Status: new("modified")}},
		})
	})

	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.True(t, result.Approved)
	assert.Empty(t, compares, "a head approval is checked before approvals that need a comparison")
}

// Two operators approved the same earlier commit and nobody approved the head.
// Schema inputs changed since that commit, so neither approval counts, the
// commit is compared exactly once, and both are named as stale approvers.
func TestCheckReviewGate_SharedStaleApprovalBlocksAndComparesOnce(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob", "carol"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	now := time.Now()
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new("aaa111")},
		{User: &gh.User{Login: new("carol")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new("aaa111")},
	})
	compares := make(chan struct{}, 10)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, _ *http.Request) {
		compares <- struct{}{}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.CommitsComparison{
			Status: new("ahead"),
			Files:  []*gh.CommitFile{{Filename: new("schema/testdb/orders.sql"), Status: new("modified")}},
		})
	})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved, "neither stale approval covers the head")
	assert.ElementsMatch(t, []string{"bob", "carol"}, result.StaleApprovers)
	assert.Len(t, compares, 1, "reviewers who approved the same commit share one comparison")
}

// A non-.sql entry under the database's own schema path (a namespace symlink
// retargeted, for example) changed after the approval: the approval no longer
// covers the head.
func TestCheckReviewGate_NonSQLFileUnderSchemaPathInvalidatesApproval(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new("aaa111")},
	})
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.CommitsComparison{
			Status: new("ahead"),
			Files:  []*gh.CommitFile{{Filename: new("schema/testdb/billing"), Status: new("modified")}},
		})
	})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", reviewGateTestHeadSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	assert.Equal(t, []string{"bob"}, result.StaleApprovers)
}

// GitHub is unavailable for the comparison: the approval state could not be
// determined, so the gate returns an evaluation error (retryable) rather than
// a "Review Required" merit block, matching TestEnforceReviewGate's contract.
func TestCheckReviewGate_CompareUnavailableIsEvaluationFailure(t *testing.T) {
	h, mux := setupReviewGateHandler(t, reviewGateTestConfig(func(cfg *api.ServerConfig) {
		db := cfg.Databases["orders"]
		db.OperatorUsers = []string{"bob"}
		cfg.Databases["orders"] = db
	}))
	registerPREndpoint(mux, "alice")
	registerReviewsEndpoint(mux, []*gh.PullRequestReview{
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new("aaa111")},
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
		{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}, CommitID: new("aaa111")},
		{User: &gh.User{Login: new("carol")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: now}},
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
	compared := make(chan string, 10)
	mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, r *http.Request) {
		compared <- r.PathValue("range")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(&gh.CommitsComparison{
			Status: new("ahead"),
			Files:  []*gh.CommitFile{{Filename: new("schema/testdb/orders.sql"), Status: new("modified")}},
		})
	})
	client, err := h.clientForRepo("octocat/hello-world", 12345)
	require.NoError(t, err)

	result, err := h.checkReviewGate(t.Context(), client, "octocat/hello-world", 1, reviewGateSchema("schema/testdb", schemaSHA))
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.False(t, result.Approved)
	require.Len(t, compared, 1)
	assert.Equal(t, reviewGateTestHeadSHA+"..."+schemaSHA, <-compared)
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
			t.Fatal("timed out waiting for evaluation-failure comment")
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
			t.Fatal("timed out waiting for review-required comment")
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
			{User: &gh.User{Login: new("bob")}, State: new(ghclient.ReviewApproved), SubmittedAt: &gh.Timestamp{Time: time.Now()}, CommitID: new("aaa111")},
		})
		mux.HandleFunc("GET /repos/octocat/hello-world/compare/{range}", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(&gh.CommitsComparison{
				Status: new("ahead"),
				Files:  []*gh.CommitFile{{Filename: new("schema/testdb/orders.sql"), Status: new("modified")}},
			})
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
			assert.Contains(t, body, "Approvals on an earlier commit no longer count")
			assert.Contains(t, body, "could not confirm they did not: @bob.")
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for review-required comment")
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
