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
	cmdclient "github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/schema"
)

// A plan has two sides, and only one of them is fixed.
//
// The live side is always the catalog of the database the command addresses.
// The desired side is whatever schema the operator is asking about, and during
// a deploy that is usually not the schema of the binary that answers: the
// question is whether the storage is ready for the release about to roll, and
// the release about to roll is by definition not the one running.
//
// So the desired side is a release, named one of two ways, and the report
// repeats which one was used:
//
//	--release <tag>       the .sql files of a published tag, fetched from the
//	                      repository
//	--schema-dir <path>   the .sql files in a directory — a checkout of that
//	                      release, or an unreleased commit, and offline
//
// Both name a schema the operator can point at and read for themselves. The
// schema compiled into whichever binary answered the request is deliberately
// not offered: which release that is depends on which pod took the call, so an
// operator mid-roll would be asking about a release they had not chosen and
// could not predict, and would learn which one only from the report they were
// about to act on.
//
// Neither selector is available on `storage apply`, and that is the safety
// property rather than an omission: a convergence runs the schema of the binary
// running it, so "apply is what a boot does" holds by construction (AV-9). To
// converge a release's schema, run that release's binary.

// storageSchemaSourceFlags names the desired side of the diff. Exactly one
// selector is required: each names a complete schema, so the parser refuses
// both of them and validateSource refuses neither.
type storageSchemaSourceFlags struct {
	Release   string `help:"Diff against the schema files of this published tag, fetched from the SchemaBot repository (e.g. v1.4.0)" xor:"desired-schema"`
	SchemaDir string `help:"Diff against the .sql files in this directory instead — a checkout of the release you are about to deploy (e.g. ./pkg/schema/mysql)" name:"schema-dir" type:"path" xor:"desired-schema"`
	// Repo carries no Kong default, so an unset flag stays empty and
	// validateSource can tell "omitted" from "typed the default value". The
	// default is applied in resolve instead, where nothing has to distinguish
	// the two any more.
	Repo string `help:"Repository to fetch --release schema files from (default block/schemabot)" name:"release-repo"`
}

// validateSource requires a desired schema and refuses --release-repo without
// the flag it modifies. Both selectors at once is the parser's own refusal, and
// restated here for callers that build the command in Go.
func (f *storageSchemaSourceFlags) validateSource() error {
	named := make([]string, 0, 2)
	if strings.TrimSpace(f.Release) != "" {
		named = append(named, "--release")
	}
	if strings.TrimSpace(f.SchemaDir) != "" {
		named = append(named, "--schema-dir")
	}
	switch {
	case len(named) == 0:
		return fmt.Errorf("missing flags: --release=STRING or --schema-dir=STRING")
	case len(named) > 1:
		return fmt.Errorf("%s can't be used together", strings.Join(named, " and "))
	}
	repo := strings.TrimSpace(f.Repo)
	if strings.TrimSpace(f.Release) == "" && repo != "" {
		return fmt.Errorf("--release-repo only applies with --release: it says which repository to fetch a published tag's schema files from")
	}
	return nil
}

// storageSchemaSourceRefusal refuses the diff's release selectors on a
// convergence, and names the two ways to converge a release's schema instead.
//
// The refusal is the invariant, stated where an operator meets it. A
// convergence runs the schema embedded in the binary running it, which is what
// makes an operator apply identical to the next boot's — so it can be used to
// converge storage ahead of a deploy without the fleet's own boots then
// disagreeing with it (AV-9). A convergence to files named on the command line
// would be a second implementation of the one path that must not have two.
func storageSchemaSourceRefusal(schemaDir, release, repo string) error {
	selector := ""
	switch {
	case strings.TrimSpace(release) != "":
		selector = "--release"
	case strings.TrimSpace(schemaDir) != "":
		selector = "--schema-dir"
	case strings.TrimSpace(repo) != "":
		selector = "--release-repo"
	default:
		return nil
	}
	return fmt.Errorf("%s cannot be used with a convergence: an apply runs the schema embedded in the binary running it, so that it converges exactly what that binary's next boot would. To converge a release's schema, run that release's binary — its container image is that release — or let the release's own first boot converge it. To see what it would do, use the same flag on `storage plan`", selector)
}

// resolve reads the desired schema the flags named.
//
// No selector yields nil, which asks the target about its own embedded schema.
// A diff never gets there, since one selector is required; an apply's preview
// is the caller that does, because the schema a convergence runs is the
// answering binary's own and asking it beats carrying a copy of those files to
// the binary that already has them.
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
	// storageSchemaFetchTimeout bounds the whole fetch — the listing and every
	// file it names, together. A plan is run at a terminal during a deploy, so
	// a repository that does not answer has to fail rather than hang.
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
	// One budget over the whole fetch, not one per request: a listing plus a
	// request per file is dozens of round trips, and a per-request timeout
	// bounds none of them together. An operator waiting on this during a deploy
	// has --schema-dir, which needs no network at all.
	ctx, cancel := context.WithTimeout(ctx, storageSchemaFetchTimeout)
	defer cancel()

	warnIfReleaseHostIsPlaintext()

	client := &http.Client{CheckRedirect: refuseInsecureRedirect}
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

// refuseInsecureRedirect holds every hop of a fetch to the rule the first hop
// was checked against.
//
// Guarding only the URL the request started at is not enough. Go carries the
// Authorization header across a redirect that stays on the same host, and it
// compares hosts without comparing schemes — so a same-host redirect from
// https to http forwards the token in plaintext, which is the exact outcome
// the first check exists to prevent. The guard belongs on every destination,
// not on the one the operator typed.
// The check is on the header rather than on the environment, because the header
// is what actually travels: net/http copies it onto the redirect request — and
// drops it when the destination is a different host — before consulting this
// policy, so its presence here is exactly the question of whether this hop
// carries the token. A fetch with no token is left alone; an unauthenticated
// public release has nothing to protect on the wire.
func refuseInsecureRedirect(request *http.Request, via []*http.Request) error {
	if len(via) >= maxStorageSchemaRedirects {
		return fmt.Errorf("stopped after %d redirects fetching release schema files", maxStorageSchemaRedirects)
	}
	if request.Header.Get("Authorization") == "" {
		// Nothing to leak on this hop, so it is followed. The schema still
		// arrives over whatever channel the redirect chose, though, and the
		// warning on the configured base URL cannot speak for a host the
		// operator never named — so the downgrade says so here instead of
		// passing silently.
		warnPlaintextSchemaSource(request.URL)
		return nil
	}
	if err := cmdclient.GuardInsecureToken(request.URL); err != nil {
		return fmt.Errorf("refusing a redirect to %s://%s: %w; the redirect stays on the same host, so the token would follow it in plaintext", request.URL.Scheme, request.URL.Host, err)
	}
	return nil
}

// maxStorageSchemaRedirects matches the ceiling net/http applies when a client
// sets no policy of its own, so installing one costs no redirects.
const maxStorageSchemaRedirects = 10

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

// warnIfReleaseHostIsPlaintext says so when the desired side of the diff is
// about to be read over a channel anyone on the path can rewrite.
//
// It warns rather than refuses. With no token there is nothing to leak, and a
// plaintext mirror is a legitimate thing for an operator to point
// $GITHUB_API_URL at; refusing would take away a working configuration to
// protect against a tampered report. What the warning buys is that the report
// is not read as authoritative: a rewritten desired schema makes a plan
// describe work the release does not need, and nothing downstream can tell,
// because the files parsed and diffed exactly as a real schema would.
//
// A convergence is a different matter and needs no warning here: apply refuses
// --release and --schema-dir by construction (AV-9), so a binary only ever
// converges the schema it embeds. Nothing fetched over this path can be
// applied to a database.
//
// It asks GuardInsecureToken what counts as insecure rather than testing the
// scheme itself, so "plaintext host" has one definition in the CLI — including
// its loopback carve-out, which is right here for the same reason: a mirror on
// the loopback interface has no network path for anything to sit on.
func warnIfReleaseHostIsPlaintext() {
	if storageSchemaReleaseToken() != "" {
		// A token makes this a refusal instead, at the request that would
		// carry it — see getReleaseContents.
		return
	}
	parsed, err := url.Parse(storageSchemaReleaseAPIBase())
	if err != nil {
		// An unparseable base is the fetch's problem to report, with the
		// reason; a warning about a URL nothing could read would only add
		// noise to the error that follows.
		return
	}
	warnPlaintextSchemaSource(parsed)
}

// warnPlaintextSchemaSource says once, for one URL, that the desired side of
// the diff is arriving over a channel anyone on the path can rewrite. It is
// shared by the configured base URL and by every hop a redirect adds, because
// the property being warned about belongs to the channel the files actually
// travel over rather than to the address an operator typed.
func warnPlaintextSchemaSource(u *url.URL) {
	if cmdclient.GuardInsecureToken(u) == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "%s The schema being compared against is read from %s over plaintext, so anything on the network path can change it before it arrives: treat this report as unverified.\n\n",
		glyph.Attention, u.Scheme+"://"+u.Host)
}

// escapePathSegments escapes a value for use inside a URL path while leaving
// its separators alone.
//
// Escaping the whole value is wrong here: both values this is called with are
// legitimately multi-segment — an owner/name repository and a directory path
// within it — and PathEscape would turn their separators into %2F, so every
// fetch would 404 against a path that does exist. Escaping per segment keeps
// the structure and still encodes a segment carrying a character the path
// grammar reserves.
func escapePathSegments(value string) string {
	segments := strings.Split(value, "/")
	for i, segment := range segments {
		segments[i] = url.PathEscape(segment)
	}
	return strings.Join(segments, "/")
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
		storageSchemaReleaseAPIBase(), escapePathSegments(repo), escapePathSegments(path), url.QueryEscape(tag))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build the request for %s at %s in %s: %w", path, tag, repo, err)
	}
	request.Header.Set("Accept", accept)
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if token := storageSchemaReleaseToken(); token != "" {
		// $GITHUB_API_URL is whatever the operator's environment set, and a
		// token sent to a plaintext host is readable by anyone on the path.
		// Refuse rather than leak it: the same rule the API transport applies
		// to SchemaBot's own token.
		if err := cmdclient.GuardInsecureToken(request.URL); err != nil {
			return nil, fmt.Errorf("%w; GITHUB_API_URL is %s — point it at an https:// host, or unset GITHUB_TOKEN and GH_TOKEN to fetch a public release without one", err, storageSchemaReleaseAPIBase())
		}
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
	switch {
	case response.StatusCode == http.StatusNotFound:
		// 404 is also what GitHub answers for a repository the caller is not
		// allowed to see, so it cannot be reported as a spelling mistake
		// alone. An operator whose only problem is a missing token would
		// otherwise re-check a tag that was never wrong, mid-deploy, while
		// the advice that would have worked sits in a branch this status
		// never reaches.
		return fmt.Errorf("release %s in %s has no %s: check the tag spelling, pass --release-repo if the release is published elsewhere, or — if the repository is private — set GITHUB_TOKEN to a token that can read it, since GitHub reports a repository it will not show you as missing", tag, repo, path)
	case releaseFetchRateLimited(response):
		return fmt.Errorf("GitHub rate-limited the fetch of %s at %s from %s (HTTP %d): wait for the limit to reset, or set GITHUB_TOKEN for the larger authenticated budget — or use --schema-dir to read the files from a checkout and not spend any budget at all", path, tag, repo, response.StatusCode)
	case response.StatusCode == http.StatusUnauthorized, response.StatusCode == http.StatusForbidden:
		return fmt.Errorf("not allowed to read %s at %s in %s (HTTP %d): set GITHUB_TOKEN to a token that can read the repository, or use --schema-dir to read the files from a checkout instead", path, tag, repo, response.StatusCode)
	default:
		return fmt.Errorf("fetching %s at %s from %s failed with HTTP %d; --schema-dir reads the same files from a checkout without the network", path, tag, repo, response.StatusCode)
	}
}

// releaseFetchRateLimited reports whether GitHub refused the request for budget
// rather than for permission. The two arrive as the same status, so only the
// headers separate them: a spent primary budget answers with its remaining
// count at zero, and a secondary limit answers with when to come back.
//
// Telling them apart is the whole point of asking. A rate limit is fixed by
// waiting or by authenticating for a larger budget; the permission message
// sends an operator mid-deploy to re-issue a token that was never the problem,
// and it will not work when they retry with it.
func releaseFetchRateLimited(response *http.Response) bool {
	if response.StatusCode != http.StatusForbidden && response.StatusCode != http.StatusTooManyRequests {
		return false
	}
	return response.Header.Get("X-RateLimit-Remaining") == "0" || response.Header.Get("Retry-After") != ""
}
