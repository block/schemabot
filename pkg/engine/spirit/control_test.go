package spirit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVolumeToSpiritSettings(t *testing.T) {
	// Volumes 1-5 use fixed thread counts regardless of CPU hint.

	t.Run("volume 1 - minimal", func(t *testing.T) {
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(1, 20)
		assert.Equal(t, 1, threads)
		assert.Equal(t, 100*time.Millisecond, chunkTime)
		assert.Equal(t, 10*time.Second, lockTimeout)
	})

	t.Run("volume 2 - conservative", func(t *testing.T) {
		// CPUs not factored, always 2
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(2, 20)
		assert.Equal(t, 2, threads)
		assert.Equal(t, 500*time.Millisecond, chunkTime)
		assert.Equal(t, 15*time.Second, lockTimeout)

		threads, chunkTime, lockTimeout = volumeToSpiritSettings(2, 48)
		assert.Equal(t, 2, threads)
		assert.Equal(t, 500*time.Millisecond, chunkTime)
		assert.Equal(t, 15*time.Second, lockTimeout)
	})

	t.Run("volume 3 - default", func(t *testing.T) {
		// CPUs not factored, always 2
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(3, 20)
		assert.Equal(t, 2, threads)
		assert.Equal(t, 2*time.Second, chunkTime)
		assert.Equal(t, 30*time.Second, lockTimeout)

		threads, chunkTime, lockTimeout = volumeToSpiritSettings(3, 48)
		assert.Equal(t, 2, threads)
		assert.Equal(t, 2*time.Second, chunkTime)
		assert.Equal(t, 30*time.Second, lockTimeout)
	})

	t.Run("volume 4", func(t *testing.T) {
		// CPUs not factored, always 4
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(4, 20)
		assert.Equal(t, 4, threads)
		assert.Equal(t, 2*time.Second, chunkTime)
		assert.Equal(t, 60*time.Second, lockTimeout)

		threads, chunkTime, lockTimeout = volumeToSpiritSettings(4, 48)
		assert.Equal(t, 4, threads)
		assert.Equal(t, 2*time.Second, chunkTime)
		assert.Equal(t, 60*time.Second, lockTimeout)
	})

	t.Run("volume 5", func(t *testing.T) {
		// CPUs not factored, always 8
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(5, 20)
		assert.Equal(t, 8, threads)
		assert.Equal(t, 2*time.Second, chunkTime)
		assert.Equal(t, 60*time.Second, lockTimeout)

		threads, chunkTime, lockTimeout = volumeToSpiritSettings(5, 48)
		assert.Equal(t, 8, threads)
		assert.Equal(t, 2*time.Second, chunkTime)
		assert.Equal(t, 60*time.Second, lockTimeout)
	})

	// Volumes 6-11 use CPU-scaled thread counts, capped at maxThreads.
	// Thread counts are capped at maxThreads (16).

	t.Run("volume 6 - ceil(cpus/16)", func(t *testing.T) {
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(6, 20)
		assert.Equal(t, 2, threads) // ceil(20/16) = 2
		assert.Equal(t, 5*time.Second, chunkTime)
		assert.Equal(t, 60*time.Second, lockTimeout)

		threads, _, _ = volumeToSpiritSettings(6, 48)
		assert.Equal(t, 3, threads) // ceil(48/16) = 3

		threads, _, _ = volumeToSpiritSettings(6, 128)
		assert.Equal(t, 8, threads) // ceil(128/16) = 8
	})

	t.Run("volume 7 - ceil(cpus/12)", func(t *testing.T) {
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(7, 20)
		assert.Equal(t, 2, threads) // ceil(20/12) = 2
		assert.Equal(t, 5*time.Second, chunkTime)
		assert.Equal(t, 60*time.Second, lockTimeout)

		threads, _, _ = volumeToSpiritSettings(7, 48)
		assert.Equal(t, 4, threads) // ceil(48/12) = 4

		threads, _, _ = volumeToSpiritSettings(7, 128)
		assert.Equal(t, 11, threads) // ceil(128/12) = 11
	})

	t.Run("volume 8 - ceil(cpus/8)", func(t *testing.T) {
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(8, 20)
		assert.Equal(t, 3, threads) // ceil(20/8) = 3
		assert.Equal(t, 5*time.Second, chunkTime)
		assert.Equal(t, 60*time.Second, lockTimeout)

		threads, _, _ = volumeToSpiritSettings(8, 48)
		assert.Equal(t, 6, threads) // ceil(48/8) = 6

		threads, _, _ = volumeToSpiritSettings(8, 128)
		assert.Equal(t, maxThreads, threads) // ceil(128/8) = 16
	})

	t.Run("volume 9 - ceil(cpus/6)", func(t *testing.T) {
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(9, 20)
		assert.Equal(t, 4, threads) // ceil(20/6) = 4
		assert.Equal(t, 5*time.Second, chunkTime)
		assert.Equal(t, 60*time.Second, lockTimeout)

		threads, _, _ = volumeToSpiritSettings(9, 48)
		assert.Equal(t, 8, threads) // ceil(48/6) = 8

		// ceil(128/6) = 22, capped to maxThreads.
		threads, _, _ = volumeToSpiritSettings(9, 128)
		assert.Equal(t, maxThreads, threads) // ceil(128/6) = 22, capped
	})

	t.Run("volume 10 - ceil(cpus/4)", func(t *testing.T) {
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(10, 20)
		assert.Equal(t, 5, threads) // ceil(20/4) = 5
		assert.Equal(t, 5*time.Second, chunkTime)
		assert.Equal(t, 600*time.Second, lockTimeout)

		threads, _, _ = volumeToSpiritSettings(10, 48)
		assert.Equal(t, 12, threads) // ceil(48/4) = 12

		// ceil(128/4) = 32, capped to maxThreads.
		threads, _, _ = volumeToSpiritSettings(10, 128)
		assert.Equal(t, maxThreads, threads) // ceil(128/4) = 32, capped
	})

	t.Run("volume 11 - ceil(cpus/2)", func(t *testing.T) {
		threads, chunkTime, lockTimeout := volumeToSpiritSettings(11, 20)
		assert.Equal(t, 10, threads) // ceil(20/2) = 10
		assert.Equal(t, 5*time.Second, chunkTime)
		assert.Equal(t, 600*time.Second, lockTimeout)

		// ceil(48/2) = 24, capped to maxThreads.
		threads, _, _ = volumeToSpiritSettings(11, 48)
		assert.Equal(t, maxThreads, threads) // ceil(48/2) = 24, capped

		// ceil(128/2) = 64, capped to maxThreads.
		threads, _, _ = volumeToSpiritSettings(11, 128)
		assert.Equal(t, maxThreads, threads) // ceil(128/2) = 64, capped
	})
}

func TestVolumeToSpiritSettings_NoCPUHint(t *testing.T) {
	// When cpuHint is 0, volumes 6-11 fall back to fixed thread counts.
	t.Run("fallback thread counts", func(t *testing.T) {
		threads, _, _ := volumeToSpiritSettings(6, 0)
		assert.Equal(t, 8, threads)

		threads, _, _ = volumeToSpiritSettings(7, 0)
		assert.Equal(t, 8, threads)

		threads, _, _ = volumeToSpiritSettings(8, 0)
		assert.Equal(t, 12, threads)

		threads, _, _ = volumeToSpiritSettings(9, 0)
		assert.Equal(t, 12, threads)

		threads, _, _ = volumeToSpiritSettings(10, 0)
		assert.Equal(t, maxThreads, threads)

		threads, _, _ = volumeToSpiritSettings(11, 0)
		assert.Equal(t, maxThreads, threads)
	})
}

func TestVolumeToSpiritSettings_ThreadCap(t *testing.T) {
	// Even with very high CPU hints, threads never exceed maxThreads.
	for vol := int32(6); vol <= 11; vol++ {
		threads, _, _ := volumeToSpiritSettings(vol, 256)
		require.LessOrEqual(t, threads, maxThreads, "volume %d with 256 CPUs exceeded max threads", vol)
		require.GreaterOrEqual(t, threads, 2, "volume %d returned fewer than 2 threads", vol)
	}
}

func TestCPUScaledThreads(t *testing.T) {
	t.Run("uses fallback when no CPU hint", func(t *testing.T) {
		assert.Equal(t, 8, cpuScaledThreads(0, 16, 8))
		assert.Equal(t, 12, cpuScaledThreads(0, 8, 12))
	})

	t.Run("scales with CPU hint", func(t *testing.T) {
		assert.Equal(t, 2, cpuScaledThreads(20, 16, 8))                   // ceil(20/16) = 2
		assert.Equal(t, 3, cpuScaledThreads(48, 16, 8))                   // ceil(48/16) = 3
		assert.Equal(t, 10, cpuScaledThreads(20, 2, maxThreads))          // ceil(20/2) = 10
		assert.Equal(t, maxThreads, cpuScaledThreads(128, 2, maxThreads)) // ceil(128/2) = 64, capped
	})

	t.Run("minimum 2 threads", func(t *testing.T) {
		// Ensure floor of 2 so CPU-scaled volumes don't regress below volume 2
		assert.Equal(t, 2, cpuScaledThreads(1, 100, 0))
		assert.Equal(t, 2, cpuScaledThreads(1, 100, 1))
	})

	t.Run("capped at maxThreads", func(t *testing.T) {
		assert.Equal(t, maxThreads, cpuScaledThreads(1000, 2, maxThreads))
		assert.Equal(t, maxThreads, cpuScaledThreads(256, 4, maxThreads))
	})
}

func TestSettingsToVolume(t *testing.T) {
	assert.Equal(t, int32(1), settingsToVolume(1, 100*time.Millisecond))
	assert.Equal(t, int32(2), settingsToVolume(2, 500*time.Millisecond))
	assert.Equal(t, int32(3), settingsToVolume(2, 2*time.Second))
	assert.Equal(t, int32(4), settingsToVolume(4, 2*time.Second))
	assert.Equal(t, int32(5), settingsToVolume(8, 2*time.Second))
	assert.Equal(t, int32(6), settingsToVolume(8, 5*time.Second))
	assert.Equal(t, int32(8), settingsToVolume(12, 5*time.Second))
	assert.Equal(t, int32(10), settingsToVolume(maxThreads, 5*time.Second))
}
