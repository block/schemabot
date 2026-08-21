package spirit

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

func TestVolumeToSpiritSettings(t *testing.T) {
	// Volumes 1-5 use fixed thread counts regardless of CPU hint.

	t.Run("volume 1 - minimal", func(t *testing.T) {
		assert.Equal(t, 1, volumeToSpiritSettings(1, 20))
	})

	t.Run("volume 2 - conservative", func(t *testing.T) {
		// CPUs not factored, always 2
		assert.Equal(t, 2, volumeToSpiritSettings(2, 20))
		assert.Equal(t, 2, volumeToSpiritSettings(2, 48))
	})

	t.Run("volume 3 - default", func(t *testing.T) {
		// CPUs not factored, always 2
		assert.Equal(t, 2, volumeToSpiritSettings(3, 20))
		assert.Equal(t, 2, volumeToSpiritSettings(3, 48))
	})

	t.Run("volume 4", func(t *testing.T) {
		// CPUs not factored, always 4
		assert.Equal(t, 4, volumeToSpiritSettings(4, 20))
		assert.Equal(t, 4, volumeToSpiritSettings(4, 48))
	})

	t.Run("volume 5", func(t *testing.T) {
		// CPUs not factored, always 8
		assert.Equal(t, 8, volumeToSpiritSettings(5, 20))
		assert.Equal(t, 8, volumeToSpiritSettings(5, 48))
	})

	// Volumes 6-11 use CPU-scaled thread counts, capped at maxThreads.
	// Thread counts are capped at maxThreads (16).

	t.Run("volume 6 - ceil(cpus/16)", func(t *testing.T) {
		assert.Equal(t, 2, volumeToSpiritSettings(6, 20))  // ceil(20/16) = 2
		assert.Equal(t, 3, volumeToSpiritSettings(6, 48))  // ceil(48/16) = 3
		assert.Equal(t, 8, volumeToSpiritSettings(6, 128)) // ceil(128/16) = 8
	})

	t.Run("volume 7 - ceil(cpus/12)", func(t *testing.T) {
		assert.Equal(t, 2, volumeToSpiritSettings(7, 20))   // ceil(20/12) = 2
		assert.Equal(t, 4, volumeToSpiritSettings(7, 48))   // ceil(48/12) = 4
		assert.Equal(t, 11, volumeToSpiritSettings(7, 128)) // ceil(128/12) = 11
	})

	t.Run("volume 8 - ceil(cpus/8)", func(t *testing.T) {
		assert.Equal(t, 3, volumeToSpiritSettings(8, 20))           // ceil(20/8) = 3
		assert.Equal(t, 6, volumeToSpiritSettings(8, 48))           // ceil(48/8) = 6
		assert.Equal(t, maxThreads, volumeToSpiritSettings(8, 128)) // ceil(128/8) = 16
	})

	t.Run("volume 9 - ceil(cpus/6)", func(t *testing.T) {
		assert.Equal(t, 4, volumeToSpiritSettings(9, 20)) // ceil(20/6) = 4
		assert.Equal(t, 8, volumeToSpiritSettings(9, 48)) // ceil(48/6) = 8
		// ceil(128/6) = 22, capped to maxThreads.
		assert.Equal(t, maxThreads, volumeToSpiritSettings(9, 128))
	})

	t.Run("volume 10 - ceil(cpus/4)", func(t *testing.T) {
		assert.Equal(t, 5, volumeToSpiritSettings(10, 20))  // ceil(20/4) = 5
		assert.Equal(t, 12, volumeToSpiritSettings(10, 48)) // ceil(48/4) = 12
		// ceil(128/4) = 32, capped to maxThreads.
		assert.Equal(t, maxThreads, volumeToSpiritSettings(10, 128))
	})

	t.Run("volume 11 - ceil(cpus/2)", func(t *testing.T) {
		assert.Equal(t, 10, volumeToSpiritSettings(11, 20)) // ceil(20/2) = 10
		// ceil(48/2) = 24, capped to maxThreads.
		assert.Equal(t, maxThreads, volumeToSpiritSettings(11, 48))
		// ceil(128/2) = 64, capped to maxThreads.
		assert.Equal(t, maxThreads, volumeToSpiritSettings(11, 128))
	})
}

func TestVolumeToSpiritSettings_NoCPUHint(t *testing.T) {
	// When cpuHint is 0, volumes 6-11 fall back to fixed thread counts.
	t.Run("fallback thread counts", func(t *testing.T) {
		assert.Equal(t, 8, volumeToSpiritSettings(6, 0))
		assert.Equal(t, 8, volumeToSpiritSettings(7, 0))
		assert.Equal(t, 12, volumeToSpiritSettings(8, 0))
		assert.Equal(t, 12, volumeToSpiritSettings(9, 0))
		assert.Equal(t, maxThreads, volumeToSpiritSettings(10, 0))
		assert.Equal(t, maxThreads, volumeToSpiritSettings(11, 0))
	})
}

func TestVolumeToSpiritSettings_ThreadCap(t *testing.T) {
	// Even with very high CPU hints, threads never exceed maxThreads.
	for vol := int32(6); vol <= 11; vol++ {
		threads := volumeToSpiritSettings(vol, 256)
		require.LessOrEqual(t, threads, maxThreads, "volume %d with 256 CPUs exceeded max threads", vol)
		require.GreaterOrEqual(t, threads, 2, "volume %d returned fewer than 2 threads", vol)
	}
}

func TestCancelMarksRunningSchemaChangeCancelled(t *testing.T) {
	eng := New(Config{})
	cancelCalled := false
	eng.runningSchemaChange = &runningSchemaChange{
		database: "testdb",
		tables:   []string{"users"},
		state:    engine.StateRunning,
		cancelFunc: func() {
			cancelCalled = true
		},
	}

	_, err := eng.Cancel(t.Context(), &engine.ControlRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cleanup missing connection details")
	assert.True(t, cancelCalled)
	assert.Equal(t, engine.StateCancelled, eng.runningSchemaChange.state)
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
	assert.Equal(t, int32(1), settingsToVolume(1))
	assert.Equal(t, int32(3), settingsToVolume(2)) // the documented default; vol 2 shares this thread count
	assert.Equal(t, int32(4), settingsToVolume(4))
	assert.Equal(t, int32(5), settingsToVolume(8))
	assert.Equal(t, int32(8), settingsToVolume(12))
	assert.Equal(t, int32(10), settingsToVolume(maxThreads))
}

// Volume adjustments store a stopped state so Spirit can resume from checkpoint
// with new settings. Progress should still report running during that window so
// the operator keeps polling the active schema change.
func TestVolumeReportsRunningWhileStoredStoppedStateRestarts(t *testing.T) {
	eng := New(Config{})
	rm := &runningSchemaChange{
		database:       "testdb",
		tableNamespace: map[string]string{},
		state:          engine.StateRunning,
		host:           "127.0.0.1:1",
		username:       "root",
	}
	rm.wg.Add(1)
	eng.mu.Lock()
	eng.runningSchemaChange = rm
	eng.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		_, err := eng.Volume(t.Context(), &engine.VolumeRequest{
			Database: "testdb",
			Volume:   4,
			Credentials: &engine.Credentials{
				DSN: "root@tcp(127.0.0.1:1)/testdb",
			},
		})
		errCh <- err
	}()

	require.Eventually(t, func() bool {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		return rm.state == engine.StateStopped && rm.volumeRestartInProgress
	}, time.Second, 10*time.Millisecond)

	progress, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
	require.NoError(t, err)
	assert.Equal(t, engine.StateRunning, progress.State)

	rm.wg.Done()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for volume change")
	}
	eng.Drain()
}

// registerRunningSchemaChange installs a simulated running schema change on the
// engine, carrying the engine's configured default copy settings the same way
// Apply initializes a change. The caller drives the change's lifecycle through
// rm.wg.
func registerRunningSchemaChange(eng *Engine) *runningSchemaChange {
	rm := &runningSchemaChange{
		database:       "testdb",
		tableNamespace: map[string]string{},
		state:          engine.StateRunning,
		host:           "127.0.0.1:1",
		username:       "root",
		threads:        eng.threads,
	}
	eng.mu.Lock()
	eng.runningSchemaChange = rm
	eng.mu.Unlock()
	return rm
}

// adjustVolume drives a Volume call through its stop/retune/restart sequence
// against a schema change whose driver goroutine is simulated via rm.wg, and
// returns the volume result.
func adjustVolume(t *testing.T, eng *Engine, rm *runningSchemaChange, volume int32) *engine.VolumeResult {
	t.Helper()
	rm.wg.Add(1)

	type volumeOutcome struct {
		result *engine.VolumeResult
		err    error
	}
	outCh := make(chan volumeOutcome, 1)
	go func() {
		result, err := eng.Volume(t.Context(), &engine.VolumeRequest{
			Database:    rm.database,
			Volume:      volume,
			Credentials: &engine.Credentials{DSN: "root@tcp(127.0.0.1:1)/testdb"},
		})
		outCh <- volumeOutcome{result: result, err: err}
	}()

	// Volume stops the change and waits for its driver goroutine before
	// restarting; release the simulated goroutine once the stop is observed.
	require.Eventually(t, func() bool {
		eng.mu.Lock()
		defer eng.mu.Unlock()
		return rm.state == engine.StateStopped && rm.volumeRestartInProgress
	}, 5*time.Second, 10*time.Millisecond, "volume change did not stop the schema change")
	rm.wg.Done()

	select {
	case out := <-outCh:
		require.NoError(t, out.err)
		return out.result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for volume change to complete")
		return nil
	}
}

// A volume set while a schema change is running is scoped to that change: the
// engine's configured defaults are untouched, and the next schema change starts
// from the defaults again rather than inheriting the earlier retune.
func TestVolumeScopedToRunningSchemaChange(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)

	result := adjustVolume(t, eng, rm, 11)
	assert.Equal(t, int32(11), result.NewVolume)

	wantThreads := volumeToSpiritSettings(11, 0)
	eng.mu.Lock()
	assert.Equal(t, wantThreads, rm.threads)
	assert.Equal(t, int32(11), rm.volume)
	eng.mu.Unlock()

	// The engine's configured defaults are not modified by the adjustment.
	assert.Equal(t, DefaultThreads, eng.threads)
	assert.Equal(t, DefaultLockWaitTimeout, eng.lockWaitTimeout)

	eng.Drain()

	// A new schema change starts from the configured defaults. The apply
	// targets an unreachable host, so execution fails immediately — the copy
	// settings are snapshotted when the change is registered, which is what
	// this scenario verifies.
	_, err := eng.Apply(t.Context(), &engine.ApplyRequest{
		Database:    "testdb",
		Credentials: &engine.Credentials{DSN: "root@tcp(127.0.0.1:1)/testdb"},
		Changes: []engine.SchemaChange{{
			Namespace: "testdb",
			TableChanges: []engine.TableChange{{
				Table: "users",
				DDL:   "CREATE TABLE `users` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			}},
		}},
	})
	require.NoError(t, err)
	defer eng.Drain()

	threads, lockTimeout := eng.copySettings()
	assert.Equal(t, DefaultThreads, threads)
	assert.Equal(t, DefaultLockWaitTimeout, lockTimeout)
	eng.mu.Lock()
	assert.Equal(t, int32(0), eng.runningSchemaChange.volume)
	eng.mu.Unlock()
}

// Volume reporting reflects the explicit volume set for the running schema
// change, including neighboring levels that derive the same Spirit settings.
func TestVolumeReportingUsesExplicitPerChangeValue(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)

	// The change starts on the configured defaults (2 threads), the documented
	// default of volume 3 — even though volume 2 now derives the same thread
	// count since lock wait timeout no longer varies by volume.
	result := adjustVolume(t, eng, rm, 7)
	assert.Equal(t, int32(3), result.PreviousVolume)
	assert.Equal(t, int32(7), result.NewVolume)

	// Without a CPU hint, volumes 6 and 7 derive the same Spirit settings, so
	// no restart is needed — and the reported previous volume is the explicit
	// value set for this change.
	result, err := eng.Volume(t.Context(), &engine.VolumeRequest{
		Database:    "testdb",
		Volume:      6,
		Credentials: &engine.Credentials{DSN: "root@tcp(127.0.0.1:1)/testdb"},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(7), result.PreviousVolume)
	assert.Equal(t, int32(6), result.NewVolume)
	assert.Contains(t, result.Message, "no restart")

	eng.mu.Lock()
	assert.Equal(t, int32(6), rm.volume)
	eng.mu.Unlock()

	eng.Drain()
}

// setSchemaChangeVolume must not apply settings, and Volume must not report
// success, when the schema change it targets is no longer the tracked one —
// e.g. it completed and a new change started while a volume adjustment was
// in flight.
func TestSetSchemaChangeVolumeNotAppliedWhenUntracked(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)

	eng.mu.Lock()
	eng.runningSchemaChange = &runningSchemaChange{database: "otherdb", threads: eng.threads}
	eng.mu.Unlock()

	applied := eng.setSchemaChangeVolume(rm, 7, 8)
	assert.False(t, applied, "expected no-op when rm is no longer the tracked schema change")
	assert.Equal(t, int32(0), rm.volume, "stale rm must not be mutated")
}

// Copy settings are shared between volume adjustments, progress pollers, and
// the schema change execution path; adjusting volume while the settings are
// read concurrently must be safe.
func TestVolumeConcurrentWithSettingsReads(t *testing.T) {
	eng := New(Config{})
	rm := registerRunningSchemaChange(eng)

	stopReaders := make(chan struct{})
	var readers sync.WaitGroup
	readers.Go(func() {
		for {
			select {
			case <-stopReaders:
				return
			default:
			}
			threads, lockTimeout := eng.copySettings()
			assert.Positive(t, threads)
			assert.Positive(t, lockTimeout)
			_, err := eng.Progress(t.Context(), &engine.ProgressRequest{})
			assert.NoError(t, err)
		}
	})

	result := adjustVolume(t, eng, rm, 11)
	assert.Equal(t, int32(11), result.NewVolume)

	close(stopReaders)
	readers.Wait()
	eng.Drain()
}

// Stateless control operations (cutover, deferred cutover sentinel lookup)
// must address the schema the DSN connects to: under per-deployment schema
// overrides the DSN carries the physical schema name while the request carries
// the logical (canonical) database name. The request database is only a
// fallback for DSNs without a schema.
func TestStatelessControlDatabase(t *testing.T) {
	t.Run("DSN database wins over request database", func(t *testing.T) {
		got, err := statelessControlDatabase("root@tcp(localhost:3306)/bikeshare_eu_qa", "bikeshare")
		require.NoError(t, err)
		assert.Equal(t, "bikeshare_eu_qa", got)
	})

	t.Run("request database is the fallback for a namespace-free DSN", func(t *testing.T) {
		got, err := statelessControlDatabase("root@tcp(localhost:3306)/", "bikeshare")
		require.NoError(t, err)
		assert.Equal(t, "bikeshare", got)
	})

	t.Run("empty when neither names a schema", func(t *testing.T) {
		got, err := statelessControlDatabase("root@tcp(localhost:3306)/", "")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("invalid DSN is an error", func(t *testing.T) {
		_, err := statelessControlDatabase("not a dsn", "bikeshare")
		require.Error(t, err)
	})
}
