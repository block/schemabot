package namedlock

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The name→key derivation is a cross-process, cross-version contract: pods
// running different binary versions coordinate through these advisory-lock
// keys during a rolling deploy, so the values are pinned. If this test fails,
// the hash changed and mixed-version pods would no longer exclude each other.
// The pinned names are the fixed production lock names plus one
// representative of each dynamic family (apply-target and pending-drops
// locks, whose suffixes hash the target identity).
func TestAdvisoryLockKeyIsStable(t *testing.T) {
	assert.Equal(t, int64(-6406129118649469951), advisoryLockKey("schemabot_ensure_schema"))
	assert.Equal(t, int64(7473791374071279902), advisoryLockKey("schemabot_stranded_reaper"))
	assert.Equal(t, int64(463405793398757012), advisoryLockKey("schemabot_apply_00112233445566778899aabbccddeeff"))
	assert.Equal(t, int64(-3035819114795400762), advisoryLockKey("schemabot_pending_drops_0011223344556677"))
}

// pg_locks reports a single-argument advisory key split across two unsigned
// OID columns, so the split has to survive the sign bit: a key whose high half
// looks negative as an int32 must still round-trip, or the affinity probe
// would find no row for a lock it is holding and refuse a healthy connection.
func TestAdvisoryLockCatalogKeyRoundTrips(t *testing.T) {
	for _, key := range []int64{
		0,
		1,
		-1,
		advisoryLockKey("schemabot_ensure_schema"),
		advisoryLockKey("schemabot_stranded_reaper"),
	} {
		classID, objID := advisoryLockCatalogKey(key)
		assert.GreaterOrEqual(t, classID, int64(0), "classid is an unsigned OID")
		assert.GreaterOrEqual(t, objID, int64(0), "objid is an unsigned OID")
		assert.Equal(t, key, int64(uint64(classID)<<32|uint64(objID)), "key %d did not round-trip", key)
	}
}

// Distinct lock names derive distinct keys, so unrelated coordination points
// do not contend on the same advisory lock.
func TestAdvisoryLockKeyDistinguishesNames(t *testing.T) {
	assert.NotEqual(t, advisoryLockKey("schemabot_ensure_schema"), advisoryLockKey("schemabot_stranded_reaper"))
	assert.NotEqual(t, advisoryLockKey("lock_a"), advisoryLockKey("lock_b"))
}
