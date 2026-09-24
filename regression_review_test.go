// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lru

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegression_F01_UpdateSizeSingleEntryExceedsMaxSizePreservesOtherEntries(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			// Arrange
			cache := b.fn(100, WithInvariantChecking(true))
			_, err := cache.Insert("k1", NewSizedValue("v1", 20))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 20))
			require.NoError(t, err)
			_, err = cache.Insert("k3", NewSizedValue("v3", 30))
			require.NoError(t, err)

			// Act: grow k3 from 30 to 110 (> maxSize 100). Only k3 should be evicted.
			err = cache.UpdateSize("k3", 80)

			// Assert
			require.NoError(t, err)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("k3"), "k3 exceeds maxSize and must be evicted")
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k1"), "k1 must not be collateral-evicted")
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k2"), "k2 must not be collateral-evicted")
		})
	}
}

func TestRegression_F01_ArenaRadixUpdateSizeAtTailUnderCriticalPressureShedsOlderEntries(t *testing.T) {
	// Arrange: k_tail is at LRU tail, k_mru is at MRU head, critical pressure = 0.95 (targetSize = 50).
	pressure := 0.0
	cache := NewArenaRadixCache(
		100,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 { return pressure }),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
	).(*arenaRadix)

	_, err := cache.Insert("k_tail", NewSizedValue("v_tail", 20))
	require.NoError(t, err)
	_, err = cache.Insert("k_mid", NewSizedValue("v_mid", 40))
	require.NoError(t, err)
	_, err = cache.Insert("k_mru", NewSizedValue("v_mru", 20))
	require.NoError(t, err)

	// Act: enable critical pressure and grow k_tail (which sits at c.tail) by +10 (20 -> 30).
	// Total size becomes 90 > targetSize (50). k_tail is protectedNodeID at c.tail, so shedding
	// must skip k_tail and evict k_mid (c.nodes[c.tail].prev) to reach currentSize == 50.
	pressure = 0.95
	err = cache.UpdateSize("k_tail", 10)

	// Assert
	require.NoError(t, err)
	assert.NotNil(t, cache.LookUpWithoutChangingOrder("k_tail"))
	assert.Nil(t, cache.LookUpWithoutChangingOrder("k_mid"))
	assert.NotNil(t, cache.LookUpWithoutChangingOrder("k_mru"))
	assert.Equal(t, uint64(50), cache.currentSize)
}

func TestRegression_F02_InsertUint64OverflowNearMaxUint64EvictsCleanly(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			// Arrange
			const maxCap = uint64(math.MaxUint64)
			const halfPlus = uint64(math.MaxUint64/2) + 100
			cache := b.fn(maxCap, WithInvariantChecking(true))

			_, err := cache.Insert("k1", NewSizedValue("v1", halfPlus))
			require.NoError(t, err)

			// Act: inserting k2 of size halfPlus would overflow uint64 if added before evicting k1.
			evicted, err := cache.Insert("k2", NewSizedValue("v2", halfPlus))

			// Assert
			require.NoError(t, err)
			require.Len(t, evicted, 1)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("k1"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k2"))

			// Subsequent Erase must not underflow currentSize or panic.
			erased := cache.Erase("k2")
			require.NotNil(t, erased)
		})
	}
}

func TestRegression_F03_CriticalPressureSheddingPreservesZeroSizeEntriesAfterByteTargetMet(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			// Arrange
			pressure := 0.0
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return pressure }),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			// Insert 1 positive-size entry (60B) at LRU tail, followed by 50 zero-size entries toward MRU.
			_, err := cache.Insert("lru_60", NewSizedValue("payload", 60))
			require.NoError(t, err)
			for i := range 50 {
				_, err = cache.Insert("zero_"+string(rune('A'+i)), NewStringValue(""))
				require.NoError(t, err)
			}

			// Act: trigger critical-pressure shedding (targetSize = 50).
			pressure = 0.95
			evicted := cache.EvaluateMemoryPressure()

			// Assert: only "lru_60" should be shed to drop currentSize from 60 -> 0 (<= 50).
			// All 50 zero-size entries must remain in the cache.
			require.Len(t, evicted, 1)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("lru_60"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("zero_A"))
		})
	}
}

func TestRegression_F05_ReentrantPressureFuncDoesNotStackOverflow(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			// Arrange
			var pac PressureAwareCache
			reentered := false
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if !reentered && pac != nil {
						reentered = true
						_ = pac.EvaluateMemoryPressure()
					}
					return 0.95
				}),
			)
			pac = cache.(PressureAwareCache)
			_, err := cache.Insert("k1", NewSizedValue("v1", 80))
			require.NoError(t, err)

			// Act & Assert: EvaluateMemoryPressure must not infinitely recurse.
			evicted := pac.EvaluateMemoryPressure()
			assert.True(t, reentered)
			assert.Len(t, evicted, 1)
		})
	}
}

func TestRegression_F07_EraseEmptyPrefixInvalidatesStaleCachedPressure(t *testing.T) {
	// Arrange
	c := NewArenaRadixCache(100, WithInvariantChecking(true)).(*arenaRadix)
	_, err := c.Insert("pre_1", NewSizedValue("v1", 40))
	require.NoError(t, err)

	// Simulate high cached runtime pressure prior to full-cache reset.
	c.cachedPressureBits.Store(math.Float64bits(2.5))
	c.pressureNeedsRefresh.Store(false)
	epochBefore := c.reclaimEpoch.Load()

	// Act
	c.EraseEntriesWithGivenPrefix("")

	// Assert: pressureNeedsRefresh must be true and reclaimEpoch incremented so next Insert refreshes pressure.
	assert.True(t, c.pressureNeedsRefresh.Load())
	assert.Greater(t, c.reclaimEpoch.Load(), epochBefore)

	_, err = c.Insert("post_1", NewSizedValue("v1", 40))
	require.NoError(t, err)
	_, err = c.Insert("post_2", NewSizedValue("v2", 40))
	require.NoError(t, err)
	assert.NotNil(t, c.LookUpWithoutChangingOrder("post_1"))
	assert.NotNil(t, c.LookUpWithoutChangingOrder("post_2"))
}

func TestRegression_F08_FNV1aHashCollisionDoesNotThrashCompaction(t *testing.T) {
	// Arrange: insert two distinct keys and compact initial pre-allocated slice slack.
	k1 := "collision_key_1"
	k2 := "collision_key_2"
	c := NewArenaRadixCache(1000, WithInvariantChecking(true)).(*arenaRadix)
	_, err := c.Insert(k1, NewStringValue("val1"))
	require.NoError(t, err)
	_, err = c.Insert(k2, NewStringValue("val2"))
	require.NoError(t, err)
	c.compactLocked()
	require.Equal(t, len(c.nodes), cap(c.nodes))

	// Simulate post-compaction state where two live keys share one 64-bit FNV-1a slot
	// (len(nodeMap) == 1 < c.len == 2, expectedNodeMapLen == 1, nodeMapDirty == false).
	delete(c.nodeMap, hashString(k1))
	c.expectedNodeMapLen = 1
	c.nodeMapDirty = false

	// Act & Assert: shouldAutoCompactLocked(nilNode) must be false so EvaluateMemoryPressure does not thrash.
	assert.False(t, c.shouldAutoCompactLocked(nilNode))

	// LookUp on k1 must find k1 via trie fallback and heal c.nodeMap[hashString(k1)].
	val := c.LookUp(k1)
	require.NotNil(t, val)
	assert.Equal(t, StringValue("val1"), val)
	_, healed := c.nodeMap[hashString(k1)]
	assert.True(t, healed)
}

func TestRegression_F09_BytesValueClonesInputAndOutputSlices(t *testing.T) {
	// Arrange
	raw := []byte("immutable-payload")
	bv := NewBytesValue(raw)

	// Act: mutate caller input slice and returned Bytes() slice.
	raw[0] = 'X'
	out := bv.Bytes()
	out[1] = 'Y'

	// Assert: cached BytesValue remains unmodified.
	assert.Equal(t, []byte("immutable-payload"), bv.Bytes())
}

func TestRegression_F15_DeepRadixTreeOver64LevelsAndFreeSubtree(t *testing.T) {
	// Arrange: insert a 70-level hierarchy into ArenaRadixCache to exceed the 64-entry stack buffer.
	c := NewArenaRadixCache(100000, WithInvariantChecking(true)).(*arenaRadix)
	var b strings.Builder
	for i := range 70 {
		b.WriteString("d")
		b.WriteByte(byte('a' + (i % 26)))
		b.WriteByte('/')
		key := b.String()
		_, err := c.Insert(key, NewStringValue("val"))
		require.NoError(t, err)
	}

	// Act: look up deepest key and erase the entire 70-level prefix subtree.
	deepKey := b.String()
	got := c.LookUp(deepKey)
	require.NotNil(t, got)
	c.EraseEntriesWithGivenPrefix("da/")

	// Assert
	assert.Equal(t, 0, c.len)
	assert.Equal(t, uint64(0), c.currentSize)
}
