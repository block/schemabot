package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyOptionsFromMapRoundTrip verifies that user-facing apply options can
// be decoded from API-style strings and encoded back without losing fields.
func TestApplyOptionsFromMapRoundTrip(t *testing.T) {
	options := ApplyOptionsFromMap(map[string]string{
		"allow_unsafe":  "true",
		"branch":        "schema-change-branch",
		"defer_cutover": "true",
		"defer_deploy":  "true",
		"skip_revert":   "true",
		"volume":        "7",
	})

	assert.Equal(t, ApplyOptions{
		AllowUnsafe:  true,
		Branch:       "schema-change-branch",
		DeferCutover: true,
		DeferDeploy:  true,
		SkipRevert:   true,
		Volume:       7,
	}, options)

	assert.Equal(t, map[string]string{
		"allow_unsafe":  "true",
		"branch":        "schema-change-branch",
		"defer_cutover": "true",
		"defer_deploy":  "true",
		"skip_revert":   "true",
		"volume":        "7",
	}, options.Map())
}

func TestPlanUnsafeDDLChanges(t *testing.T) {
	plan := &Plan{Namespaces: map[string]*NamespacePlanData{
		"testdb": {Tables: []TableChange{
			{Namespace: "testdb", Table: "users", Operation: "alter", IsUnsafe: true, UnsafeReason: "DROP COLUMN removes data"},
			{Namespace: "testdb", Table: "orders", Operation: "drop"},
			{Namespace: "testdb", Table: "products", Operation: "alter"},
		}},
	}, Shards: []ShardPlan{{
		Namespace: "testdb",
		Shard:     "-80",
		Changes:   []TableChange{{Table: "accounts", Operation: "alter", IsUnsafe: true, UnsafeReason: "DROP PRIMARY KEY rebuilds the table"}},
	}}}

	changes := plan.UnsafeDDLChanges()

	require.Len(t, changes, 3)
	assert.Equal(t, "users", changes[0].Table)
	assert.Equal(t, "DROP COLUMN removes data", changes[0].UnsafeOptInReason())
	assert.Equal(t, "orders", changes[1].Table)
	assert.Equal(t, "DROP TABLE removes all data", changes[1].UnsafeOptInReason())
	assert.Equal(t, "accounts", changes[2].Table)
	assert.Equal(t, "testdb", changes[2].Namespace)
	assert.Equal(t, "DROP PRIMARY KEY rebuilds the table", changes[2].UnsafeOptInReason())
}

// TestReleasesPausedRollout verifies the one-way release latch semantics: a
// pending or completed release request releases a paused rollout, while a
// failed release, any non-release operation, and a nil request do not (the
// rollout stays paused — fail-closed).
func TestReleasesPausedRollout(t *testing.T) {
	tests := []struct {
		name string
		req  *ApplyControlRequest
		want bool
	}{
		{name: "nil request", req: nil, want: false},
		{
			name: "pending release latches",
			req:  &ApplyControlRequest{Operation: ControlOperationRelease, Status: ControlRequestPending},
			want: true,
		},
		{
			name: "completed release latches",
			req:  &ApplyControlRequest{Operation: ControlOperationRelease, Status: ControlRequestCompleted},
			want: true,
		},
		{
			name: "failed release does not latch",
			req:  &ApplyControlRequest{Operation: ControlOperationRelease, Status: ControlRequestFailed},
			want: false,
		},
		{
			name: "pending start is not a release",
			req:  &ApplyControlRequest{Operation: ControlOperationStart, Status: ControlRequestPending},
			want: false,
		},
		{
			name: "completed stop is not a release",
			req:  &ApplyControlRequest{Operation: ControlOperationStop, Status: ControlRequestCompleted},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.req.ReleasesPausedRollout())
		})
	}
}

// TestApplyOptionsFromMapIgnoresInvalidVolume verifies that malformed numeric
// options and out-of-range volumes are ignored instead of being persisted back
// into apply metadata.
func TestApplyOptionsFromMapIgnoresInvalidVolume(t *testing.T) {
	for _, volume := range []string{"fast", "0", "-1", "12"} {
		options := ApplyOptionsFromMap(map[string]string{"volume": volume})

		assert.Zero(t, options.Volume)
		assert.Empty(t, options.Map())
	}
}

// TestVolumeControlRequestMetadataRoundTrip verifies the desired level a
// volume control request carries survives the storage encode/decode round
// trip for every level on the shared volume scale.
func TestVolumeControlRequestMetadataRoundTrip(t *testing.T) {
	for volume := MinVolume; volume <= MaxVolume; volume++ {
		metadata, err := EncodeVolumeControlRequestMetadata(volume)
		require.NoError(t, err)
		assert.JSONEq(t, fmt.Sprintf(`{"volume":%d}`, volume), string(metadata))

		decoded, err := DecodeVolumeControlRequestMetadata(metadata)
		require.NoError(t, err)
		assert.Equal(t, volume, decoded)
	}
}

// TestEncodeVolumeControlRequestMetadataRejectsOutOfRange verifies an
// out-of-range level is refused at write time so the driver never reads an
// unactionable volume request.
func TestEncodeVolumeControlRequestMetadataRejectsOutOfRange(t *testing.T) {
	for _, volume := range []int32{0, -1, 12} {
		_, err := EncodeVolumeControlRequestMetadata(volume)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of range")
	}
}

// TestDecodeVolumeControlRequestMetadataRejectsInvalidPayloads verifies the
// driver refuses empty, malformed, and out-of-range payloads with a decode
// error instead of retuning the engine to an unintended level.
func TestDecodeVolumeControlRequestMetadataRejectsInvalidPayloads(t *testing.T) {
	tests := []struct {
		name     string
		metadata []byte
	}{
		{name: "empty", metadata: nil},
		{name: "malformed json", metadata: []byte(`{"volume":`)},
		{name: "missing volume", metadata: []byte(`{}`)},
		{name: "below range", metadata: []byte(`{"volume":0}`)},
		{name: "above range", metadata: []byte(`{"volume":12}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeVolumeControlRequestMetadata(tt.metadata)
			require.Error(t, err)
		})
	}
}

// EngineForType must route each database type to its engine, with Postgres
// mapping to the Postgres engine and unknown types defaulting to Spirit.
func TestEngineForType(t *testing.T) {
	tests := []struct {
		name   string
		dbType string
		want   string
	}{
		{name: "mysql", dbType: DatabaseTypeMySQL, want: EngineSpirit},
		{name: "vitess", dbType: DatabaseTypeVitess, want: EnginePlanetScale},
		{name: "strata", dbType: DatabaseTypeStrata, want: EngineStrata},
		{name: "postgres", dbType: DatabaseTypePostgres, want: EnginePostgres},
		{name: "empty defaults to spirit", dbType: "", want: EngineSpirit},
		{name: "unknown defaults to spirit", dbType: "unknown", want: EngineSpirit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, EngineForType(tc.dbType))
		})
	}
}
