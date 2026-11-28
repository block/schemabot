package commands

import (
	"bytes"
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/spinner"
	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/localdemo"
)

func TestFormatSamplePullShowsTheDownloadAgainstItsSize(t *testing.T) {
	const gib = 1 << 30
	assert.Equal(t, "1.1 GiB of 2.4 GiB · 45%",
		formatSamplePull(localdemo.PullProgress{Downloaded: 1_160_000_000, Total: 2_576_980_378, Layers: 12, Fetched: 4}))
	assert.Equal(t, "0 B of 2.0 GiB · 0%", formatSamplePull(localdemo.PullProgress{Total: 2 * gib, Layers: 12}))
	assert.Equal(t, "300.0 MiB downloaded", formatSamplePull(localdemo.PullProgress{Downloaded: 300 << 20, Layers: 3}))
	assert.Equal(t, "Unpacking · 7 of 12 layers",
		formatSamplePull(localdemo.PullProgress{Downloaded: 2 * gib, Total: 2 * gib, Layers: 12, Fetched: 12, Unpacked: 7}))
}

// Output that cannot be redrawn, such as a CI log, gets the image's size as
// soon as the download starts, a line per tenth of the download, and one line
// when unpacking starts, rather than a line per daemon message.
func TestSamplePullMilestonesPrintEveryTenthOfTheDownload(t *testing.T) {
	var out bytes.Buffer
	milestones := &samplePullMilestones{out: &out}
	const total = 1000 << 20
	for downloaded := int64(0); downloaded <= total; downloaded += 25 << 20 {
		milestones.observe(localdemo.PullProgress{Downloaded: downloaded, Total: total, Layers: 4, Fetched: 1})
	}
	milestones.observe(localdemo.PullProgress{Downloaded: total, Total: total, Layers: 4, Fetched: 4, Unpacked: 1})
	milestones.observe(localdemo.PullProgress{Downloaded: total, Total: total, Layers: 4, Fetched: 4, Unpacked: 4})

	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	assert.Len(t, lines, 12, out.String())
	assert.Equal(t, "    0 B of 1000.0 MiB · 0%", lines[0])
	assert.Equal(t, "    100.0 MiB of 1000.0 MiB · 10%", lines[1])
	assert.Equal(t, "    1000.0 MiB of 1000.0 MiB · 100%", lines[10])
	assert.Equal(t, "    Unpacking 4 layers", lines[11])
}

func TestSamplePullMilestonesStepByBytesWhenTheSizeIsUnknown(t *testing.T) {
	var out bytes.Buffer
	milestones := &samplePullMilestones{out: &out}
	for downloaded := int64(0); downloaded < 600<<20; downloaded += 8 << 20 {
		milestones.observe(localdemo.PullProgress{Downloaded: downloaded, Layers: 2})
	}
	assert.Equal(t, "    0 B downloaded\n    256.0 MiB downloaded\n    512.0 MiB downloaded\n", out.String())
}

// The line under a step shows how far its download has come, and goes away
// when the next step starts.
func TestInitProgressShowsADetailUnderTheCurrentStepOnly(t *testing.T) {
	m := &initProgress{heading: "sample database", spinner: spinner.New(), width: 72, cancel: func() {}}
	m.Update(initStageMsg("Downloading ghcr.io/acme/app:1.0"))
	m.Update(initDetailMsg("1.1 GiB of 2.4 GiB · 45%"))
	view := trimLineEnds(stripANSI(m.View()))
	assert.Contains(t, view, "Downloading ghcr.io/acme/app:1.0\n    1.1 GiB of 2.4 GiB · 45%")

	m.Update(initStageMsg("Starting your sample cluster"))
	view = trimLineEnds(stripANSI(m.View()))
	assert.NotContains(t, view, "45%")
	assert.Contains(t, view, "✓ Downloading ghcr.io/acme/app:1.0")
}

func trimLineEnds(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " ")
	}
	return strings.Join(lines, "\n")
}
