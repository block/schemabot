package localdemo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/cpuguy83/dockercfg"
	"github.com/distribution/reference"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/api/types/registry"
	"github.com/moby/moby/client"
)

// Progress receives sample setup as it happens. Step is called as each step
// starts. Pull, when set, is called as an image download advances, for the
// step that is downloading it.
type Progress struct {
	Step func(string)
	Pull func(PullProgress)
}

func (p Progress) step(stage string) {
	if p.Step != nil {
		p.Step(stage)
	}
}

func (p Progress) pull(progress PullProgress) {
	if p.Pull != nil {
		p.Pull(progress)
	}
}

// PullProgress is how far an image download has come. Downloaded and Total
// count compressed bytes over the layers this download fetches. Total is the
// size the registry lists for those layers, and zero when the registry could
// not be asked. Layers is how many layers the download fetches, Fetched how
// many of those have finished downloading, and Unpacked how many are unpacked.
type PullProgress struct {
	Downloaded int64
	Total      int64
	Layers     int
	Fetched    int
	Unpacked   int
}

// Unpacking reports whether every layer has downloaded and the daemon is now
// unpacking them, the part of a download whose bytes no longer move.
func (p PullProgress) Unpacking() bool {
	return p.Layers > 0 && p.Fetched == p.Layers
}

// pullReportInterval spaces progress reports. The daemon reports each layer
// many times a second, far more often than anyone can read a changing number.
const pullReportInterval = 100 * time.Millisecond

// registrySizeTimeout bounds the registry lookup that sizes a download, so a
// registry that does not answer delays the download itself only briefly.
const registrySizeTimeout = 10 * time.Second

// pullImage downloads image through the daemon at endpoint, reporting progress
// as it goes. It uses the credentials the docker CLI would use for the image's
// registry, so a download counts against the same account as a docker pull.
func pullImage(ctx context.Context, endpoint, image string, report func(PullProgress)) error {
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil {
		return fmt.Errorf("parse sample image %s: %w", image, err)
	}
	creds, err := registryCredentials(reference.Domain(named))
	if err != nil {
		return fmt.Errorf("read Docker credentials for %s: %w", reference.Domain(named), err)
	}
	encodedAuth, err := encodeRegistryAuth(creds)
	if err != nil {
		return fmt.Errorf("encode Docker credentials for %s: %w", reference.Domain(named), err)
	}
	docker, err := client.New(client.FromEnv, client.WithHost(endpoint))
	if err != nil {
		return fmt.Errorf("connect to Docker at %s: %w", endpoint, err)
	}
	defer utils.CloseAndLog(docker)
	version, err := docker.ServerVersion(ctx, client.ServerVersionOptions{})
	if err != nil {
		return fmt.Errorf("read Docker version: %w", err)
	}

	tracker := newPullTracker()
	// The size is only for display, so a registry that cannot be asked, or is
	// slow to answer, leaves the download running and reports bytes without a
	// total.
	sizeCtx, cancelSize := context.WithTimeout(ctx, registrySizeTimeout)
	sizes, err := imageLayerSizes(sizeCtx, http.DefaultClient, named, version.Os, version.Arch, creds)
	cancelSize()
	if err != nil {
		slog.Debug("sample image size unavailable; download progress will show bytes without a total",
			"image", image, "error", err)
	} else {
		tracker.sizes = sizes
	}

	stream, err := docker.ImagePull(ctx, image, client.ImagePullOptions{RegistryAuth: encodedAuth})
	if err != nil {
		return fmt.Errorf("pull sample image %s: %w", image, err)
	}
	var lastReport time.Time
	for message, err := range stream.JSONMessages(ctx) {
		if err != nil {
			return fmt.Errorf("pull sample image %s: %w", image, err)
		}
		if message.Error != nil {
			return fmt.Errorf("pull sample image %s: %s", image, message.Error.Message)
		}
		if !tracker.observe(message) {
			continue
		}
		// Once every layer has downloaded, every change is reported, so the
		// unpacking count never lags behind the daemon.
		progress := tracker.progress()
		if time.Since(lastReport) >= pullReportInterval || progress.Unpacking() {
			lastReport = time.Now()
			report(progress)
		}
	}
	return nil
}

// layerState is where one layer is in a download, in the order a layer moves
// through them.
type layerState int

const (
	layerWaiting layerState = iota
	layerDownloading
	layerFetched
	layerUnpacked
)

// pullTracker folds the daemon's per-layer messages into one progress figure.
// The daemon names each layer by the first twelve hex digits of its digest.
type pullTracker struct {
	// sizes is each layer's compressed size from the registry, by short ID.
	sizes map[string]int64
	// layers is where each layer this download fetches has reached.
	layers map[string]layerState
	// held is the layers the daemon already holds, which it does not download.
	held       map[string]bool
	downloaded map[string]int64
	// reported is a layer's size as the daemon gave it while downloading, used
	// when the registry did not list it.
	reported map[string]int64
}

func newPullTracker() *pullTracker {
	return &pullTracker{
		layers:     map[string]layerState{},
		held:       map[string]bool{},
		downloaded: map[string]int64{},
		reported:   map[string]int64{},
	}
}

// observe records one message and reports whether it changed the download's
// progress. Messages about the image as a whole, and about blobs the daemon
// fetches besides layers, change nothing.
func (t *pullTracker) observe(message jsonstream.Message) bool {
	id := message.ID
	if !isShortLayerID(id) {
		return false
	}
	if t.sizes != nil {
		if _, listed := t.sizes[id]; !listed {
			return false
		}
	}
	switch message.Status {
	case "Already exists":
		delete(t.layers, id)
		t.held[id] = true
		return true
	case "Pulling fs layer", "Waiting":
		if _, seen := t.layers[id]; !seen {
			t.layers[id] = layerWaiting
			return true
		}
		return false
	case "Downloading":
		if t.layers[id] > layerDownloading {
			return false
		}
		t.advance(id, layerDownloading)
		if message.Progress != nil {
			t.downloaded[id] = message.Progress.Current
			if message.Progress.Total > 0 {
				t.reported[id] = message.Progress.Total
			}
		}
		return true
	case "Verifying Checksum", "Download complete", "Extracting":
		return t.advance(id, layerFetched)
	case "Pull complete":
		return t.advance(id, layerUnpacked)
	}
	return false
}

// advance moves a layer forward. A layer never moves back: the daemon repeats
// statuses and reports extraction of a layer that already finished
// downloading. Only a layer the download announced moves at all; the daemon
// also reports finishing blobs that are not layers, such as the image config.
func (t *pullTracker) advance(id string, state layerState) bool {
	current, seen := t.layers[id]
	if !seen && state != layerDownloading {
		return false
	}
	if seen && state <= current {
		return false
	}
	t.layers[id] = state
	return true
}

// progress sums the layers this download fetches. With sizes from the
// registry that is every listed layer the daemon does not already hold, from
// the first message on; without them it is the layers the daemon has
// announced so far.
func (t *pullTracker) progress() PullProgress {
	var p PullProgress
	if t.sizes == nil {
		for id, state := range t.layers {
			countLayer(&p, state, t.reported[id], t.downloaded[id])
		}
		return p
	}
	for id, size := range t.sizes {
		if t.held[id] {
			continue
		}
		p.Total += size
		countLayer(&p, t.layers[id], size, t.downloaded[id])
	}
	p.Downloaded = min(p.Downloaded, p.Total)
	return p
}

// countLayer adds one layer to p. A layer small enough to finish between the
// daemon's reports never says how many bytes it moved; its size is what it
// took.
func countLayer(p *PullProgress, state layerState, size, downloaded int64) {
	p.Layers++
	switch state {
	case layerWaiting:
	case layerDownloading:
		p.Downloaded += downloaded
	case layerFetched:
		p.Fetched++
		p.Downloaded += max(size, downloaded)
	case layerUnpacked:
		p.Fetched++
		p.Unpacked++
		p.Downloaded += max(size, downloaded)
	}
}

func isShortLayerID(id string) bool {
	if len(id) != 12 {
		return false
	}
	for _, r := range id {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

// gatedPull holds an image download's progress until the step showing that
// download starts. A download that runs in the background behind other steps
// would otherwise report against whichever step happens to be showing.
type gatedPull struct {
	mu     sync.Mutex
	open   bool
	latest *PullProgress
	report func(PullProgress)
}

func (g *gatedPull) observe(progress PullProgress) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.latest = &progress
	if g.open {
		g.report(progress)
	}
}

// show starts reporting, beginning with how far the download has already come.
func (g *gatedPull) show() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.open = true
	if g.latest != nil {
		g.report(*g.latest)
	}
}

// credentials is what the docker CLI holds for one registry. An empty username
// with a password is an identity token.
type credentials struct {
	server   string
	username string
	password string
}

func registryCredentials(domain string) (credentials, error) {
	server := dockercfg.ResolveRegistryHost(domain)
	username, password, err := dockercfg.GetRegistryCredentials(server)
	if err != nil {
		return credentials{}, err
	}
	return credentials{server: server, username: username, password: password}, nil
}

// encodeRegistryAuth renders credentials in the form the daemon's pull
// endpoint takes. No credentials is an anonymous pull.
func encodeRegistryAuth(creds credentials) (string, error) {
	if creds.password == "" {
		return "", nil
	}
	auth := registry.AuthConfig{ServerAddress: creds.server, Username: creds.username, Password: creds.password}
	if creds.username == "" {
		auth = registry.AuthConfig{ServerAddress: creds.server, IdentityToken: creds.password}
	}
	encoded, err := json.Marshal(auth)
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(encoded), nil
}
