package planetscale

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/psclient"
)

// Volume adjusts schema change speed by setting the Vitess throttle ratio.
// Volume 1 = max throttle (ratio 0.95), Volume 11 = full speed (ratio 0.0).
// NOTE: Volume/Throttle requires the PlanetScale client to be initialized with a
// base URL (via Credentials.DSN). This is wired in the tern layer.
func (e *Engine) Volume(ctx context.Context, req *engine.VolumeRequest) (*engine.VolumeResult, error) {
	if req.ResumeState == nil || req.ResumeState.Metadata == "" {
		return nil, fmt.Errorf("no active schema change")
	}
	meta, err := decodePSMetadata(req.ResumeState.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode resume state: %w", err)
	}
	if meta.DeployRequestID == 0 {
		return nil, fmt.Errorf("no active schema change")
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	if req.Volume < 1 || req.Volume > 11 {
		e.logger.Warn("volume out of range, clamping to [1, 11]", "requested", req.Volume)
	}
	ratio := volumeToThrottleRatio(req.Volume)

	err = client.ThrottleDeployRequest(ctx, &psclient.ThrottleDeployRequestRequest{
		Organization:  credOrg(req.Credentials),
		Database:      req.Database,
		Number:        meta.DeployRequestID,
		ThrottleRatio: ratio,
	})
	if err != nil {
		return nil, fmt.Errorf("throttle deploy request: %w", err)
	}

	return &engine.VolumeResult{
		Accepted:       true,
		PreviousVolume: 0, // Unknown — PlanetScale has no query API for current ratio
		NewVolume:      req.Volume,
		Message:        fmt.Sprintf("Throttle ratio set to %.0f%%", ratio*100),
	}, nil
}

// DefaultVolume is the default throttle volume for new deploys.
// Maps to a throttle ratio of 0.85 — aggressive enough to limit impact on
// production traffic while still making progress.
const DefaultVolume int32 = 2

// volumeToThrottleRatio converts volume (1-11) to a PlanetScale throttle ratio.
// Lower volume = more throttling. DefaultVolume (2) maps to 0.85.
// See engine.VolumeRequest for how volume semantics differ between engines.
var volumeThrottleMap = [12]float64{
	0:  0.95, // unused (volume is 1-indexed)
	1:  0.95, // max throttle
	2:  0.85, // default
	3:  0.75,
	4:  0.65,
	5:  0.55,
	6:  0.45,
	7:  0.35,
	8:  0.25,
	9:  0.15,
	10: 0.05,
	11: 0.0, // no throttle
}

func volumeToThrottleRatio(volume int32) float64 {
	if volume <= 1 {
		return volumeThrottleMap[1]
	}
	if volume >= 11 {
		return volumeThrottleMap[11]
	}
	return volumeThrottleMap[volume]
}
