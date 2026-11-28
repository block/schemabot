package localdemo

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/distribution/reference"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/api/types/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// daemonPull is a pull stream as Docker Engine sends it for a seven-layer
// image: every layer is announced before any downloads, small layers finish
// without ever reporting bytes, extraction overlaps downloading, and the
// image config and manifest are reported as finished without being announced.
const daemonPull = `{"status":"Pulling from library/app","id":"1.0"}
{"status":"Pulling fs layer","progressDetail":{},"id":"16cda2556898"}
{"status":"Pulling fs layer","progressDetail":{},"id":"8fe75981dbac"}
{"status":"Pulling fs layer","progressDetail":{},"id":"fb602ba0b128"}
{"status":"Pulling fs layer","progressDetail":{},"id":"e8978eba2bb2"}
{"status":"Pulling fs layer","progressDetail":{},"id":"fea5b91588ca"}
{"status":"Pulling fs layer","progressDetail":{},"id":"54abef31ddd9"}
{"status":"Pulling fs layer","progressDetail":{},"id":"4f4fb700ef54"}
{"status":"Download complete","progressDetail":{"hidecounts":true},"id":"16cda2556898"}
{"status":"Download complete","progressDetail":{"hidecounts":true},"id":"8fe75981dbac"}
{"status":"Downloading","progressDetail":{"current":2097152,"total":4127385},"id":"fea5b91588ca"}
{"status":"Download complete","progressDetail":{"hidecounts":true},"id":"e8978eba2bb2"}
{"status":"Download complete","progressDetail":{"hidecounts":true},"id":"fea5b91588ca"}
{"status":"Extracting","progressDetail":{"current":1,"units":"s"},"id":"fea5b91588ca"}
{"status":"Download complete","progressDetail":{"hidecounts":true},"id":"fb602ba0b128"}
{"status":"Download complete","progressDetail":{"hidecounts":true},"id":"4f4fb700ef54"}
{"status":"Downloading","progressDetail":{"current":5242880,"total":12578115},"id":"54abef31ddd9"}
{"status":"Pull complete","progressDetail":{"hidecounts":true},"id":"fea5b91588ca"}
{"status":"Download complete","progressDetail":{"hidecounts":true},"id":"54abef31ddd9"}
{"status":"Pull complete","progressDetail":{"hidecounts":true},"id":"fb602ba0b128"}
{"status":"Pull complete","progressDetail":{"hidecounts":true},"id":"8fe75981dbac"}
{"status":"Pull complete","progressDetail":{"hidecounts":true},"id":"16cda2556898"}
{"status":"Pull complete","progressDetail":{"hidecounts":true},"id":"e8978eba2bb2"}
{"status":"Pull complete","progressDetail":{"hidecounts":true},"id":"4f4fb700ef54"}
{"status":"Pull complete","progressDetail":{"hidecounts":true},"id":"54abef31ddd9"}
{"status":"Download complete","progressDetail":{"hidecounts":true},"id":"a99ab7f50347"}
{"status":"Download complete","progressDetail":{"hidecounts":true},"id":"4704a97c8705"}
{"status":"Digest: sha256:7f7d30b7fdd09467bac58198a1f166dd69789d348d4fe859cccb20e0cf287fbe"}
{"status":"Status: Downloaded newer image for app:1.0"}`

// daemonPullSizes is what the registry lists for daemonPull's layers.
var daemonPullSizes = map[string]int64{
	"16cda2556898": 1_000,
	"8fe75981dbac": 2_000,
	"fb602ba0b128": 3_000,
	"e8978eba2bb2": 4_000,
	"fea5b91588ca": 4_127_385,
	"54abef31ddd9": 12_578_115,
	"4f4fb700ef54": 32,
}

// replay feeds a pull stream to a tracker line by line, recording the
// progress after each line that changed it, keyed by that line's position.
func replay(t *testing.T, tracker *pullTracker, stream string) map[int]PullProgress {
	t.Helper()
	seen := map[int]PullProgress{}
	for i, line := range strings.Split(stream, "\n") {
		var message jsonstream.Message
		require.NoError(t, json.Unmarshal([]byte(line), &message), line)
		if tracker.observe(message) {
			seen[i] = tracker.progress()
		}
	}
	return seen
}

// A download sized by the registry reports bytes against the image's full
// size from its first line, counts small layers that finish without reporting
// bytes at their listed size, and reaches unpacking only once every layer has
// downloaded.
func TestPullTrackerMeasuresADownloadAgainstTheRegistrySize(t *testing.T) {
	tracker := newPullTracker()
	tracker.sizes = daemonPullSizes
	seen := replay(t, tracker, daemonPull)
	const total = int64(1_000 + 2_000 + 3_000 + 4_000 + 4_127_385 + 12_578_115 + 32)

	assert.Equal(t, PullProgress{Total: total, Layers: 7}, seen[1], "the full size shows from the first layer announced")
	assert.Equal(t, PullProgress{Total: total, Layers: 7}, seen[7], "every layer announced, none started")
	assert.Equal(t, PullProgress{Downloaded: 1_000, Total: total, Layers: 7, Fetched: 1}, seen[8])
	assert.Equal(t, PullProgress{Downloaded: 3_000 + 2_097_152, Total: total, Layers: 7, Fetched: 2}, seen[10],
		"a layer partway through counts the bytes it has moved")
	assert.False(t, seen[16].Unpacking(), "one layer is still downloading")
	assert.Equal(t, PullProgress{Downloaded: total - 12_578_115 + 5_242_880, Total: total, Layers: 7, Fetched: 6, Unpacked: 1}, seen[17])
	assert.True(t, seen[18].Unpacking(), "every layer has downloaded")
	assert.Equal(t, PullProgress{Downloaded: total, Total: total, Layers: 7, Fetched: 7, Unpacked: 7}, tracker.progress())
	_, configChanged := seen[25]
	assert.False(t, configChanged, "the image config is not a layer")
}

// Without a size from the registry, a download reports only the bytes the
// daemon has counted, and still counts layers so unpacking is shown.
func TestPullTrackerReportsBytesWhenTheSizeIsUnknown(t *testing.T) {
	tracker := newPullTracker()
	seen := replay(t, tracker, daemonPull)

	assert.Equal(t, PullProgress{Layers: 7}, seen[7])
	assert.Equal(t, PullProgress{Downloaded: 2_097_152, Layers: 7, Fetched: 2}, seen[10])
	assert.Equal(t, PullProgress{Downloaded: 4_127_385 + 12_578_115, Layers: 7, Fetched: 7, Unpacked: 7}, tracker.progress(),
		"a finished layer counts the size the daemon gave while downloading it")
}

// A layer the daemon already holds is not downloaded, so it counts toward
// neither the bytes moved nor the size.
func TestPullTrackerLeavesOutLayersAlreadyHeld(t *testing.T) {
	tracker := newPullTracker()
	tracker.sizes = map[string]int64{"aaaaaaaaaaaa": 500, "bbbbbbbbbbbb": 700}
	replay(t, tracker, `{"status":"Pulling fs layer","id":"aaaaaaaaaaaa"}
{"status":"Already exists","id":"bbbbbbbbbbbb"}
{"status":"Downloading","progressDetail":{"current":100,"total":500},"id":"aaaaaaaaaaaa"}`)
	assert.Equal(t, PullProgress{Downloaded: 100, Total: 500, Layers: 1}, tracker.progress())
}

// A download that runs behind other steps holds its progress until its own
// step shows, then reports where it has reached and every report after.
func TestGatedPullReportsOnlyOnceItsStepShows(t *testing.T) {
	var reported []PullProgress
	gate := &gatedPull{report: func(p PullProgress) { reported = append(reported, p) }}
	gate.observe(PullProgress{Downloaded: 1, Total: 10})
	gate.observe(PullProgress{Downloaded: 4, Total: 10})
	assert.Empty(t, reported)
	gate.show()
	gate.observe(PullProgress{Downloaded: 6, Total: 10})
	assert.Equal(t, []PullProgress{{Downloaded: 4, Total: 10}, {Downloaded: 6, Total: 10}}, reported)
}

func TestEncodeRegistryAuthMatchesTheDockerCLI(t *testing.T) {
	decode := func(encoded string) registry.AuthConfig {
		raw, err := base64.URLEncoding.DecodeString(encoded)
		require.NoError(t, err)
		var auth registry.AuthConfig
		require.NoError(t, json.Unmarshal(raw, &auth))
		return auth
	}
	anonymous, err := encodeRegistryAuth(credentials{server: "ghcr.io"})
	require.NoError(t, err)
	assert.Empty(t, anonymous)

	password, err := encodeRegistryAuth(credentials{server: "ghcr.io", username: "octo", password: "secret"})
	require.NoError(t, err)
	assert.Equal(t, registry.AuthConfig{ServerAddress: "ghcr.io", Username: "octo", Password: "secret"}, decode(password))

	token, err := encodeRegistryAuth(credentials{server: "https://index.docker.io/v1/", password: "identity"})
	require.NoError(t, err)
	assert.Equal(t, registry.AuthConfig{ServerAddress: "https://index.docker.io/v1/", IdentityToken: "identity"}, decode(token))
}

// The size comes from the registry the way the daemon's own pull reads it:
// answer the token challenge with the CLI's credentials, pick the daemon's
// platform from the index, and read that platform's layers.
func TestImageLayerSizesReadsTheDaemonPlatformFromTheRegistry(t *testing.T) {
	const token = "pull-token"
	const armDigest = "sha256:" + "a1" + "000000000000000000000000000000000000000000000000000000000000"
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/token":
			user, pass, ok := r.BasicAuth()
			if !assert.True(t, ok) || !assert.Equal(t, "octo", user) || !assert.Equal(t, "secret", pass) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			assert.Equal(t, "registry.test", r.URL.Query().Get("service"))
			assert.Equal(t, "repository:acme/app:pull", r.URL.Query().Get("scope"))
			_, _ = w.Write([]byte(`{"token":"` + token + `"}`))
		case r.Header.Get("Authorization") != "Bearer "+token:
			w.Header().Set("WWW-Authenticate", `Bearer realm="`+server.URL+`/token",service="registry.test",scope="repository:acme/app:pull"`)
			w.WriteHeader(http.StatusUnauthorized)
		case r.URL.Path == "/v2/acme/app/manifests/1.0":
			_, _ = w.Write([]byte(`{"manifests":[
				{"digest":"sha256:amd","platform":{"os":"linux","architecture":"amd64"}},
				{"digest":"sha256:armv7","platform":{"os":"linux","architecture":"arm","variant":"v7"}},
				{"digest":"` + armDigest + `","platform":{"os":"linux","architecture":"arm64","variant":"v8"}}]}`))
		case r.URL.Path == "/v2/acme/app/manifests/"+armDigest:
			_, _ = w.Write([]byte(`{"layers":[
				{"digest":"sha256:16cda2556898aaaa","size":1000},
				{"digest":"sha256:54abef31ddd9bbbb","size":12578115}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	named, err := reference.ParseNormalizedNamed(server.Listener.Addr().String() + "/acme/app:1.0")
	require.NoError(t, err)
	sizes, err := imageLayerSizes(t.Context(), server.Client(), named, "linux", "arm64",
		credentials{server: server.Listener.Addr().String(), username: "octo", password: "secret"})
	require.NoError(t, err)
	assert.Equal(t, map[string]int64{"16cda2556898": 1000, "54abef31ddd9": 12_578_115}, sizes)

	_, err = imageLayerSizes(t.Context(), server.Client(), named, "linux", "s390x",
		credentials{server: server.Listener.Addr().String(), username: "octo", password: "secret"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no manifest for linux/s390x")
}

func TestPlatformManifestMatchesHowTheDaemonPicksItsPlatform(t *testing.T) {
	var index manifest
	require.NoError(t, json.Unmarshal([]byte(`{"manifests":[
		{"digest":"sha256:attestation"},
		{"digest":"sha256:armv7","platform":{"os":"linux","architecture":"arm","variant":"v7"}},
		{"digest":"sha256:arm64","platform":{"os":"linux","architecture":"arm64"}},
		{"digest":"sha256:amd64","platform":{"os":"linux","architecture":"amd64"}}]}`), &index))
	for arch, want := range map[string]string{"amd64": "sha256:amd64", "arm64": "sha256:arm64"} {
		digest, err := platformManifest(index, "linux", arch)
		require.NoError(t, err)
		assert.Equal(t, want, digest, arch)
	}
	_, err := platformManifest(index, "windows", "amd64")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "windows/amd64")
}

func TestParseChallengeKeepsCommasInsideQuotedValues(t *testing.T) {
	assert.Equal(t, map[string]string{
		"realm":   "https://auth.docker.io/token",
		"service": "registry.docker.io",
		"scope":   "repository:library/mysql:pull,push",
	}, parseChallenge(`realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/mysql:pull,push"`))
	assert.Equal(t, map[string]string{"realm": "https://ghcr.io/token", "service": "ghcr.io"},
		parseChallenge(`realm="https://ghcr.io/token", service=ghcr.io`))
}
