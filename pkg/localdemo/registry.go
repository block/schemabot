package localdemo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/distribution/reference"
)

// manifestAccept lists the manifest forms an image can be published in: a
// multi-platform index in either the OCI or Docker form, or a single
// platform's manifest in either form.
var manifestAccept = strings.Join([]string{
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.v2+json",
}, ", ")

// maxManifestBytes bounds a manifest read. A real manifest is a few kilobytes.
const maxManifestBytes = 4 << 20

// manifest holds the fields of an index or a single-platform manifest that
// size a download: an index lists one manifest per platform, and a manifest
// lists its layers.
type manifest struct {
	Manifests []struct {
		Digest   string `json:"digest"`
		Platform *struct {
			OS           string `json:"os"`
			Architecture string `json:"architecture"`
			Variant      string `json:"variant"`
		} `json:"platform"`
	} `json:"manifests"`
	Layers []struct {
		Digest string `json:"digest"`
		Size   int64  `json:"size"`
	} `json:"layers"`
}

// imageLayerSizes reads from the image's registry the compressed size of each
// layer the daemon would download for its own platform, keyed by the short ID
// the daemon's pull messages name layers by.
func imageLayerSizes(ctx context.Context, httpClient *http.Client, named reference.Named, platformOS, platformArch string, creds credentials) (map[string]int64, error) {
	ref := "latest"
	if digested, ok := named.(reference.Digested); ok {
		ref = digested.Digest().String()
	} else if tagged, ok := named.(reference.Tagged); ok {
		ref = tagged.Tag()
	}
	host := reference.Domain(named)
	if host == "docker.io" {
		host = "registry-1.docker.io"
	}
	r := &registryReader{
		http:       httpClient,
		repository: "https://" + host + "/v2/" + reference.Path(named),
		creds:      creds,
	}
	top, err := r.manifest(ctx, ref)
	if err != nil {
		return nil, err
	}
	if len(top.Manifests) == 0 {
		return layerSizes(top)
	}
	digest, err := platformManifest(top, platformOS, platformArch)
	if err != nil {
		return nil, err
	}
	platform, err := r.manifest(ctx, digest)
	if err != nil {
		return nil, err
	}
	return layerSizes(platform)
}

// platformManifest picks the index entry the daemon pulls for its platform.
// A variant is matched only where an architecture has more than one in use:
// arm64 images are listed as v8 or with no variant, and both are what an arm64
// daemon pulls.
func platformManifest(index manifest, platformOS, platformArch string) (string, error) {
	for _, entry := range index.Manifests {
		p := entry.Platform
		if p == nil || p.OS != platformOS || p.Architecture != platformArch {
			continue
		}
		if platformArch == "arm64" && p.Variant != "" && p.Variant != "v8" {
			continue
		}
		return entry.Digest, nil
	}
	return "", fmt.Errorf("image lists no manifest for %s/%s", platformOS, platformArch)
}

func layerSizes(m manifest) (map[string]int64, error) {
	if len(m.Layers) == 0 {
		return nil, errors.New("manifest lists no layers")
	}
	sizes := make(map[string]int64, len(m.Layers))
	for _, layer := range m.Layers {
		_, hex, ok := strings.Cut(layer.Digest, ":")
		if !ok || len(hex) < 12 {
			return nil, fmt.Errorf("manifest lists layer digest %q", layer.Digest)
		}
		sizes[hex[:12]] = layer.Size
	}
	return sizes, nil
}

// registryReader reads manifests from one repository, answering the
// registry's token challenge the way the docker CLI does.
type registryReader struct {
	http       *http.Client
	repository string
	creds      credentials
	token      string
}

func (r *registryReader) manifest(ctx context.Context, ref string) (manifest, error) {
	resp, err := r.get(ctx, r.repository+"/manifests/"+ref)
	if err != nil {
		return manifest{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized && r.token == "" {
		challenge := resp.Header.Get("WWW-Authenticate")
		if err := r.authorize(ctx, challenge); err != nil {
			return manifest{}, err
		}
		retry, err := r.get(ctx, r.repository+"/manifests/"+ref)
		if err != nil {
			return manifest{}, err
		}
		defer retry.Body.Close()
		resp = retry
	}
	if resp.StatusCode != http.StatusOK {
		return manifest{}, fmt.Errorf("read manifest %s: registry answered %s", ref, resp.Status)
	}
	var m manifest
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxManifestBytes)).Decode(&m); err != nil {
		return manifest{}, fmt.Errorf("decode manifest %s: %w", ref, err)
	}
	return m, nil
}

func (r *registryReader) get(ctx context.Context, target string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("build registry request %s: %w", target, err)
	}
	req.Header.Set("Accept", manifestAccept)
	if r.token != "" {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", target, err)
	}
	return resp, nil
}

// authorize exchanges the registry's bearer challenge for a pull token. A
// username and password are sent to the token service when the docker CLI
// holds them; otherwise the token is anonymous, which public images allow.
func (r *registryReader) authorize(ctx context.Context, challenge string) error {
	scheme, params, ok := strings.Cut(challenge, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return fmt.Errorf("registry asks for unsupported authorization %q", scheme)
	}
	fields := parseChallenge(params)
	realm, err := url.Parse(fields["realm"])
	if err != nil {
		return fmt.Errorf("registry token service %q: %w", fields["realm"], err)
	}
	if realm.Scheme != "https" {
		return fmt.Errorf("registry token service %q is not https", fields["realm"])
	}
	query := realm.Query()
	for _, key := range []string{"service", "scope"} {
		if fields[key] != "" {
			query.Set(key, fields[key])
		}
	}
	realm.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, realm.String(), nil)
	if err != nil {
		return fmt.Errorf("build registry token request: %w", err)
	}
	if r.creds.username != "" && r.creds.password != "" {
		req.SetBasicAuth(r.creds.username, r.creds.password)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return fmt.Errorf("request registry token from %s: %w", realm.Host, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registry token service %s answered %s", realm.Host, resp.Status)
	}
	var body struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxManifestBytes)).Decode(&body); err != nil {
		return fmt.Errorf("decode registry token from %s: %w", realm.Host, err)
	}
	r.token = body.Token
	if r.token == "" {
		r.token = body.AccessToken
	}
	if r.token == "" {
		return fmt.Errorf("registry token service %s returned no token", realm.Host)
	}
	return nil
}

// parseChallenge reads the comma-separated key="value" pairs of a
// WWW-Authenticate challenge. Values are quoted and may themselves hold
// commas, as a scope listing several actions does.
func parseChallenge(params string) map[string]string {
	fields := map[string]string{}
	for params != "" {
		key, rest, ok := strings.Cut(params, "=")
		if !ok {
			break
		}
		key = strings.TrimSpace(key)
		var value string
		if strings.HasPrefix(rest, `"`) {
			end := strings.Index(rest[1:], `"`)
			if end < 0 {
				break
			}
			value, rest = rest[1:end+1], rest[end+2:]
		} else {
			value, rest, _ = strings.Cut(rest, ",")
			rest = "," + rest
		}
		fields[key] = value
		params = strings.TrimPrefix(strings.TrimSpace(rest), ",")
	}
	return fields
}
