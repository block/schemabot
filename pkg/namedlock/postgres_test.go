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

// Distinct lock names derive distinct keys, so unrelated coordination points
// do not contend on the same advisory lock.
func TestAdvisoryLockKeyDistinguishesNames(t *testing.T) {
	assert.NotEqual(t, advisoryLockKey("schemabot_ensure_schema"), advisoryLockKey("schemabot_stranded_reaper"))
	assert.NotEqual(t, advisoryLockKey("lock_a"), advisoryLockKey("lock_b"))
}
