package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/schema"
)

// A diff has two sides, and only one of them is fixed.
//
// The live side is always the catalog of the database the command addresses.
// The desired side is whatever schema the operator is asking about, and during
// a deploy that is usually not the schema of the binary that answers: the
// question is whether the storage is ready for the release about to roll, and
// the release about to roll is by definition not the one running.
//
// So the desired side is named, always, one of three ways, and the report
// repeats which one was used:
//
//	--embedded            the schema built into the binary that answers — what
//	                      its own next boot would converge to
//	--schema-dir <path>   the .sql files in a directory — a checkout of the
//	                      release, or an unreleased commit, and offline
//	--release <tag>       the .sql files of a published tag, fetched from the
//	                      repository
//
// There is deliberately no default. A diff read without knowing which schema it
// compared against is not a weaker answer, it is an unusable one: the same
// database is converged against the release that is running and three
// statements short of the release about to roll, and an operator who assumed
// the wrong side of that either rolls into a failing bootstrap or converges
// storage they did not mean to touch.
//
// Only --embedded is available on `storage apply`, and that is the safety
// property rather than an omission: a convergence runs the schema of the binary
// running it, so "apply is what a boot does" holds by construction (AV-9). To
// converge a release's schema, run that release's binary.

// storageSchemaSourceFlags names the desired side of the diff. Exactly one
// selector is required: each names a complete schema, so the parser refuses
// two of them and validateSource refuses none.
type storageSchemaSourceFlags struct {
	Embedded  bool   `help:"Diff against the schema built into the binary that answers — through the API that is the release currently running, which is what its next boot would converge to" xor:"desired-schema"`
	SchemaDir string `help:"Diff against the .sql files in this directory instead — a checkout of the release you are about to deploy (e.g. ./pkg/schema/mysql)" name:"schema-dir" type:"path" xor:"desired-schema"`
	Release   string `help:"Diff against the schema files of this published tag, fetched from the SchemaBot repository (e.g. v1.4.0)" xor:"desired-schema"`
	Repo      string `help:"Repository to fetch --release schema files from" name:"release-repo" default:"block/schemabot"`
}

// suppliesFiles reports whether the desired schema is files the CLI carries to
// the target, rather than the schema the answering binary already has.
func (f *storageSchemaSourceFlags) suppliesFiles() bool {
	return strings.TrimSpace(f.SchemaDir) != "" || strings.TrimSpace(f.Release) != ""
}

// validateSource requires a desired schema and refuses --release-repo without
// the flag it modifies. Two selectors at once is the parser's own refusal, and
// restated here for callers that build the command in Go.
func (f *storageSchemaSourceFlags) validateSource() error {
	named := make([]string, 0, 3)
	if f.Embedded {
		named = append(named, "--embedded")
	}
	if strings.TrimSpace(f.SchemaDir) != "" {
		named = append(named, "--schema-dir")
	}
	if strings.TrimSpace(f.Release) != "" {
		named = append(named, "--release")
	}
	switch {
	case len(named) == 0:
		return fmt.Errorf("missing flags: --embedded or --schema-dir=STRING or --release=STRING")
	case len(named) > 1:
		return fmt.Errorf("%s can't be used together", strings.Join(named, " and "))
	}
	repo := strings.TrimSpace(f.Repo)
	if strings.TrimSpace(f.Release) == "" && repo != "" && repo != defaultStorageSchemaRepo {
		return fmt.Errorf("--release-repo only applies with --release: it says which repository to fetch a published tag's schema files from")
	}
	return nil
}

// storageSchemaSourceRefusal refuses the diff's file selectors on a
// convergence, and names the two ways to converge a release's schema instead.
// --embedded is not refused: it is the schema a convergence runs, so naming it
// is the operator stating what they are about to do.
//
// The refusal is the invariant, stated where an operator meets it. A
// convergence runs the schema embedded in the binary running it, which is what
// makes an operator apply identical to the next boot's — so it can be used to
// converge storage ahead of a deploy without the fleet's own boots then
// disagreeing with it (AV-9). A convergence to files named on the command line
// would be a second implementation of the one path that must not have two.
func storageSchemaSourceRefusal(schemaDir, release string) error {
	selector := ""
	switch {
	case strings.TrimSpace(release) != "":
		selector = "--release"
	case strings.TrimSpace(schemaDir) != "":
		selector = "--schema-dir"
	default:
		return nil
	}
	return fmt.Errorf("%s cannot be used with a convergence: an apply runs the schema embedded in the binary running it, so that it converges exactly what that binary's next boot would. To converge a release's schema, run that release's binary — its container image is that release — or let the release's own first boot converge it. To see what it would do, use the same flag on `storage diff`", selector)
}

// resolve reads the desired schema the flags named, or returns nil for
// --embedded: the answering binary already has those files, so carrying a copy
// of them to it would only create a way for the two to disagree.
//
// dialect is resolved lazily, by calling it, because only one selector needs
// it: a release's schema files live in a per-dialect directory of the
// repository, while a directory on disk is read as given. On the API path
// resolving the dialect costs a round trip, so the release path pays for it and
// the directory path does not.
func (f *storageSchemaSourceFlags) resolve(ctx context.Context, dialect func() (schema.Dialect, error)) (*api.StorageSchemaSource, error) {
	if dir := strings.TrimSpace(f.SchemaDir); dir != "" {
		return storageSchemaFromDirectory(dir)
	}
	release := strings.TrimSpace(f.Release)
	if release == "" {
		return nil, nil
	}
	d, err := dialect()
	if err != nil {
		return nil, fmt.Errorf("resolve the storage dialect, which says which of release %s's schema files to fetch: %w", release, err)
	}
	repo := strings.TrimSpace(f.Repo)
	if repo == "" {
		repo = defaultStorageSchemaRepo
	}
	return storageSchemaFromRelease(ctx, repo, release, d)
}

// storageSchemaFromDirectory reads a release's schema files from a checkout.
//
// The directory is the dialect's own schema directory, read as given: a
// checkout of the release is the exact schema that release embeds, which is
// what makes this the offline answer and the one that also works for a commit
// that was never tagged.
func storageSchemaFromDirectory(dir string) (*api.StorageSchemaSource, error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve --schema-dir %s: %w", dir, err)
	}
	entries, err := os.ReadDir(absolute)
	if err != nil {
		return nil, fmt.Errorf("read the schema files in --schema-dir %s: %w", absolute, err)
	}
	files := make(map[string]string)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(absolute, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read schema file %s in --schema-dir %s: %w", entry.Name(), absolute, err)
		}
		files[entry.Name()] = string(content)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no .sql files in --schema-dir %s%s", absolute, storageSchemaDirectoryHint(absolute, entries))
	}
	return api.StorageSchemaFromFiles(fmt.Sprintf("the schema files in %s", absolute), files)
}

// storageSchemaDirectoryHint names the per-dialect subdirectories when the
// operator pointed one level too high, which is the likeliest way to land on a
// directory with no .sql files in it.
func storageSchemaDirectoryHint(dir string, entries []os.DirEntry) string {
	subdirectories := make([]string, 0, 2)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if name := entry.Name(); name == string(schema.DialectMySQL) || name == string(schema.DialectPostgres) {
			subdirectories = append(subdirectories, filepath.Join(dir, name))
		}
	}
	if len(subdirectories) == 0 {
		return "; point it at a release's schema directory, which holds one .sql file per storage table"
	}
	return fmt.Sprintf("; it holds a directory per dialect, so point --schema-dir at the one your storage runs: %s", strings.Join(subdirectories, " or "))
}

// defaultStorageSchemaRepo is where a released tag's schema files are
// published. A private mirror or a fork is reachable with --release-repo.
const defaultStorageSchemaRepo = "block/schemabot"

// storageSchemaReleaseAPIBase is the GitHub API a release's files are fetched
// from. The environment variable is the one GitHub tooling already sets, so a
// GitHub Enterprise host needs no flag of its own.
func storageSchemaReleaseAPIBase() string {
	if base := strings.TrimSpace(os.Getenv("GITHUB_API_URL")); base != "" {
		return strings.TrimRight(base, "/")
	}
	return "https://api.github.com"
}

// storageSchemaReleaseToken authorizes the fetch when one is available.
// Unauthenticated requests work against a public repository and are rate
// limited; a private mirror needs a token.
func storageSchemaReleaseToken() string {
	for _, key := range []string{"GITHUB_TOKEN", "GH_TOKEN"} {
		if token := strings.TrimSpace(os.Getenv(key)); token != "" {
			return token
		}
	}
	return ""
}

const (
	// storageSchemaFetchTimeout bounds the whole fetch. A diff is run at a
	// terminal during a deploy, so a repository that does not answer has to
	// fail rather than hang: the operator still has --schema-dir, which needs
	// no network at all.
	storageSchemaFetchTimeout = 30 * time.Second
	// storageSchemaMaxFileBytes and storageSchemaMaxFiles bound what a fetch
	// will read. The storage schema is a few dozen small files; anything past
	// these bounds means the path being fetched is not a storage schema
	// directory, and saying so beats reading a repository into memory.
	storageSchemaMaxFileBytes = 1 << 20
	storageSchemaMaxFiles     = 256
)

// storageSchemaFromRelease fetches one release's storage schema files for the
// dialect the live storage runs.
//
// It reads the files at the tag rather than a release artifact, because the
// files are what the release embeds: the same directory the binary's //go:embed
// captured, at the same commit. A tag that does not exist, or a repository the
// caller cannot read, is an error naming the tag — never an empty schema, which
// would diff as "every storage table is surplus".
func storageSchemaFromRelease(ctx context.Context, repo, tag string, dialect schema.Dialect) (*api.StorageSchemaSource, error) {
	directory, err := storageSchemaReleaseDirectory(dialect)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: storageSchemaFetchTimeout}
	listing, err := listReleaseSchemaFiles(ctx, client, repo, tag, directory)
	if err != nil {
		return nil, err
	}
	files := make(map[string]string, len(listing))
	for _, entry := range listing {
		content, err := fetchReleaseSchemaFile(ctx, client, repo, tag, entry.Path)
		if err != nil {
			return nil, err
		}
		files[entry.Name] = content
	}
	description := fmt.Sprintf("the schema files of release %s", tag)
	if repo != defaultStorageSchemaRepo {
		description = fmt.Sprintf("the schema files of release %s in %s", tag, repo)
	}
	return api.StorageSchemaFromFiles(description, files)
}

// storageSchemaReleaseDirectory is the path in the repository holding one
// dialect's schema files. It fails closed for a dialect with no directory
// rather than fetching another family's DDL, which would parse as a schema and
// diff as nonsense.
func storageSchemaReleaseDirectory(dialect schema.Dialect) (string, error) {
	switch dialect {
	case schema.DialectMySQL, schema.DialectPostgres:
		return "pkg/schema/" + string(dialect), nil
	default:
		return "", fmt.Errorf("no published schema directory for storage dialect %q (%q and %q have one)", dialect, schema.DialectMySQL, schema.DialectPostgres)
	}
}

// releaseSchemaEntry is the one part of a repository listing this needs.
type releaseSchemaEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Type string `json:"type"`
}

// listReleaseSchemaFiles lists the .sql files in one directory of the
// repository at a tag.
func listReleaseSchemaFiles(ctx context.Context, client *http.Client, repo, tag, directory string) ([]releaseSchemaEntry, error) {
	body, err := getReleaseContents(ctx, client, repo, tag, directory, "application/vnd.github+json")
	if err != nil {
		return nil, err
	}
	var entries []releaseSchemaEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("read the listing of %s at %s in %s: %w", directory, tag, repo, err)
	}
	files := make([]releaseSchemaEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Type == "file" && strings.HasSuffix(entry.Name, ".sql") {
			files = append(files, entry)
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("release %s in %s has no .sql files in %s: check that the tag exists and names a SchemaBot release", tag, repo, directory)
	}
	if len(files) > storageSchemaMaxFiles {
		return nil, fmt.Errorf("release %s in %s has %d .sql files in %s, past the %d a storage schema can have; check that the tag names a SchemaBot release", tag, repo, len(files), directory, storageSchemaMaxFiles)
	}
	return files, nil
}

// fetchReleaseSchemaFile reads one schema file's contents at a tag.
func fetchReleaseSchemaFile(ctx context.Context, client *http.Client, repo, tag, path string) (string, error) {
	body, err := getReleaseContents(ctx, client, repo, tag, path, "application/vnd.github.raw")
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// getReleaseContents reads one path of the repository at a tag.
//
// The contents API is used for both the listing and the files, rather than the
// raw download URLs a listing also carries, so one request shape and one
// authorization header cover a public repository and a private mirror alike.
func getReleaseContents(ctx context.Context, client *http.Client, repo, tag, path, accept string) ([]byte, error) {
	endpoint := fmt.Sprintf("%s/repos/%s/contents/%s?ref=%s",
		storageSchemaReleaseAPIBase(), repo, path, url.QueryEscape(tag))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build the request for %s at %s in %s: %w", path, tag, repo, err)
	}
	request.Header.Set("Accept", accept)
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if token := storageSchemaReleaseToken(); token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch %s at %s from %s: %w; --schema-dir reads the same files from a checkout without the network", path, tag, repo, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, releaseContentsError(response, repo, tag, path)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, storageSchemaMaxFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s at %s from %s: %w", path, tag, repo, err)
	}
	if len(body) > storageSchemaMaxFileBytes {
		return nil, fmt.Errorf("%s at %s in %s is larger than the %d MiB a storage schema file can be; check that the tag names a SchemaBot release", path, tag, repo, storageSchemaMaxFileBytes>>20)
	}
	return body, nil
}

// releaseContentsError turns a fetch failure into the remediation for it. The
// three that happen are a tag that does not exist, a repository the caller
// cannot read, and a rate limit — each with a different fix, and all three
// solved for good by --schema-dir.
func releaseContentsError(response *http.Response, repo, tag, path string) error {
	switch response.StatusCode {
	case http.StatusNotFound:
		return fmt.Errorf("release %s in %s has no %s: check the tag spelling, or pass --release-repo if the release is published elsewhere", tag, repo, path)
	case http.StatusUnauthorized, http.StatusForbidden:
		return fmt.Errorf("not allowed to read %s at %s in %s (HTTP %d): set GITHUB_TOKEN to a token that can read the repository, or use --schema-dir to read the files from a checkout instead", path, tag, repo, response.StatusCode)
	default:
		return fmt.Errorf("fetching %s at %s from %s failed with HTTP %d; --schema-dir reads the same files from a checkout without the network", path, tag, repo, response.StatusCode)
	}
}
