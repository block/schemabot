package github

import (
	"sync"
	"time"
)

const (
	branchPolicyTTL   = time.Minute
	maxBranchPolicies = 1024
)

// branchPolicyCache stores complete policy results per installation client.
// Inactive keys expire on read or when the cache reaches capacity.
type branchPolicyCache struct {
	mu      sync.Mutex
	entries map[string]branchPolicyEntry
	now     func() time.Time // Test clock. Set before using the cache.
}

type branchPolicyEntry struct {
	policy    branchPolicy
	expiresAt time.Time
}

func (c *branchPolicyCache) currentTime() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *branchPolicyCache) get(key string) (branchPolicy, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return branchPolicy{}, false
	}
	if !c.currentTime().Before(entry.expiresAt) {
		delete(c.entries, key)
		return branchPolicy{}, false
	}
	return entry.policy.clone(), true
}

func (c *branchPolicyCache) put(key string, policy branchPolicy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.currentTime()
	if c.entries == nil {
		c.entries = make(map[string]branchPolicyEntry)
	}
	if _, exists := c.entries[key]; !exists && len(c.entries) >= maxBranchPolicies {
		for key, entry := range c.entries {
			if !now.Before(entry.expiresAt) {
				delete(c.entries, key)
			}
		}
		// A full cache only costs fresh reads. Bound retention even when
		// many distinct branches are checked within one minute.
		if len(c.entries) >= maxBranchPolicies {
			clear(c.entries)
		}
	}
	c.entries[key] = branchPolicyEntry{policy: policy.clone(), expiresAt: now.Add(branchPolicyTTL)}
}
