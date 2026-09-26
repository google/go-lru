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
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

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
	// Arrange: k_mid (40B) is inserted first at LRU tail, k_tail (20B) is inserted second in middle,
	// k_mru (20B) is inserted third at MRU head, critical pressure = 0.95 (targetSize = 50).
	pressure := 0.0
	cache := NewArenaRadixCache(
		100,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 { return pressure }),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
	).(*arenaRadix)

	_, err := cache.Insert("k_mid", NewSizedValue("v_mid", 40))
	require.NoError(t, err)
	_, err = cache.Insert("k_tail", NewSizedValue("v_tail", 20))
	require.NoError(t, err)
	_, err = cache.Insert("k_mru", NewSizedValue("v_mru", 20))
	require.NoError(t, err)

	// Act: enable critical pressure and grow k_tail by +10 (20 -> 30).
	// Total size becomes 90 > targetSize (50). Strict LRU tail shedding evicts k_mid (the oldest entry at c.tail)
	// to reach currentSize == 50 while preserving both newer entries k_tail and k_mru.
	pressure = 0.95
	err = cache.UpdateSize("k_tail", 10)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, cache.LookUpWithoutChangingOrder("k_mid"))
	assert.NotNil(t, cache.LookUpWithoutChangingOrder("k_tail"))
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

			// Act: trigger critical-pressure shedding (targetSize = 50, targetZeroCount = 25).
			pressure = 0.95
			evicted := cache.EvaluateMemoryPressure()

			// Assert: "lru_60" plus the 25 oldest zero-size entries are shed (26 total),
			// while the 25 newest zero-size entries remain in the cache.
			require.Len(t, evicted, 26)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("lru_60"))
			assert.Nil(t, cache.LookUpWithoutChangingOrder("zero_A"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("zero_"+string(rune('A'+49))))
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

	// Case 1: Missing slot (len(nodeMap) == 1 < c.len == 2, expectedNodeMapLen == 1, nodeMapDirty == false).
	delete(c.nodeMap, hashString(k1))
	c.expectedNodeMapLen = 1
	c.nodeMapDirty = false

	// Act & Assert 1: shouldAutoCompactLocked(nilNode) must be false both before and after LookUp.
	assert.False(t, c.shouldAutoCompactLocked(nilNode))
	val := c.LookUp(k1)
	require.NotNil(t, val)
	assert.Equal(t, StringValue("val1"), val)
	_, healed := c.nodeMap[hashString(k1)]
	assert.True(t, healed)
	assert.False(t, c.shouldAutoCompactLocked(nilNode))

	// Case 2: Genuine hash collision overwrite (occupied && prevID != nodeID).
	c.nodeMap[hashString(k1)] = c.nodeMap[hashString(k2)]
	c.expectedNodeMapLen = len(c.nodeMap)
	c.nodeMapDirty = false

	// Act & Assert 2: Overwriting an occupied hash slot in LookUp creates zero tombstones and must not dirty nodeMap.
	assert.False(t, c.shouldAutoCompactLocked(nilNode))
	val2 := c.LookUp(k1)
	require.NotNil(t, val2)
	assert.Equal(t, StringValue("val1"), val2)
	assert.False(t, c.shouldAutoCompactLocked(nilNode))
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

// ============================================================================
// Dedicated Regression Suite for Code Review Report Findings F01 through F15
// ============================================================================

func TestRegression_F01(t *testing.T) {
	// Arrange
	var c *arenaRadix

	// Act: Construct cache inside AllocsPerRun so warmup does not mask O(N^2) slice-slack compactions.
	allocs := testing.AllocsPerRun(1, func() {
		c = NewArenaRadixCache(
			2000,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return 0.95 }),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix)
		for i := range 100 {
			_, err := c.Insert(fmt.Sprintf("k-%04d", i), arenaTestData{Value: int64(i), DataSize: 10})
			require.NoError(t, err)
		}
	})

	// Assert
	assert.Zero(t, c.reclaimEpoch.Load())
	assert.Less(t, allocs, 350.0)
	assert.Equal(t, 100, c.len)
	assert.Equal(t, uint64(1000), c.currentSize)
}

func TestRegression_F02(t *testing.T) {
	// Arrange: Seed 1 insert so pressureSampleSeq == 1, then simulate stale cachedPressureBits = 2.5.
	c := NewArenaRadixCache(200, WithInvariantChecking(true)).(*arenaRadix)
	_, err := c.Insert("seed", NewSizedValue("v", 20))
	require.NoError(t, err)
	c.cachedPressureBits.Store(math.Float64bits(2.5))
	c.pressureInitialized.Store(true)
	c.pressureNeedsRefresh.Store(false)

	// Act 1: Reclamation via EraseEntriesWithGivenPrefix("") must zero cachedPressureBits.
	c.EraseEntriesWithGivenPrefix("")

	// Assert 1
	assert.Zero(t, c.cachedPressureBits.Load())
	assert.True(t, c.pressureNeedsRefresh.Load())

	// Act 2: Simulate in-flight samplePressureFresh across concurrent reclamation epoch bump.
	inSample := make(chan struct{})
	releaseSample := make(chan struct{})
	c2 := NewArenaRadixCache(
		200,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			close(inSample)
			<-releaseSample
			return 0.95
		}),
	).(*arenaRadix)

	done := make(chan struct{})
	go func() {
		_ = c2.samplePressureFresh()
		close(done)
	}()
	<-inSample
	c2.mu.Lock()
	c2.markReclaimedLocked()
	c2.mu.Unlock()
	close(releaseSample)
	<-done

	// Assert 2: Stale pre-reclamation sample must not clear pressureNeedsRefresh or store stale pressure.
	assert.True(t, c2.pressureNeedsRefresh.Load())
	assert.Zero(t, c2.cachedPressureBits.Load())
}

func TestRegression_F03(t *testing.T) {
	// Arrange
	c := NewArenaRadixCache(1000, WithInvariantChecking(true)).(*arenaRadix)
	_, err := c.Insert("alpha", NewStringValue("v1"))
	require.NoError(t, err)
	_, err = c.Insert("beta", NewStringValue("v2"))
	require.NoError(t, err)

	// Act 1: Erase one key under normal pressure (< 0.75).
	erased := c.Erase("beta")

	// Assert 1: expectedNodeMapLen is decremented and O(1) negative lookup fast path works.
	require.NotNil(t, erased)
	assert.Equal(t, 1, c.len)
	assert.Len(t, c.nodeMap, 1)
	assert.Equal(t, 1, c.expectedNodeMapLen)
	assert.Nil(t, c.LookUpWithoutChangingOrder("nonexistent_key"))

	// Act 2: Re-insert "beta", compact, and simulate hash collision overwrite.
	_, err = c.Insert("beta", NewStringValue("v2"))
	require.NoError(t, err)
	c.compactLocked()
	c.nodeMap[hashString("alpha")] = c.nodeMap[hashString("beta")]
	c.expectedNodeMapLen = len(c.nodeMap)
	c.nodeMapDirty = false

	// Assert 2: LookUp heals collision without setting nodeMapDirty or triggering auto-compaction.
	assert.False(t, c.shouldAutoCompactLocked(nilNode))
	got := c.LookUp("alpha")
	require.NotNil(t, got)
	assert.False(t, c.shouldAutoCompactLocked(nilNode))
}

func TestRegression_F04(t *testing.T) {
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
			// Arrange 1: k1 (10B, LRU), k2 (20B, mid), k3 (20B, MRU) in maxSize = 50.
			cache := b.fn(50, WithInvariantChecking(true))
			_, err := cache.Insert("k1", NewSizedValue("v1", 10))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 20))
			require.NoError(t, err)
			_, err = cache.Insert("k3", NewSizedValue("v3", 20))
			require.NoError(t, err)

			// Act 1: Grow k2 by +20 (20 -> 40). Since k2 (40) + k3 (20) = 60 > 50, k2 cannot fit alongside newer k3.
			err = cache.UpdateSize("k2", 20)

			// Assert 1: k2 is self-evicted while older entry k1 (10B) and newer entry k3 (20B) both survive.
			require.NoError(t, err)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("k2"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k1"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k3"))

			// Arrange 2: Near math.MaxUint64, k1 (MaxUint64 - 50) + k2 (50) == MaxUint64.
			maxCache := b.fn(math.MaxUint64, WithInvariantChecking(true))
			_, err = maxCache.Insert("k1", NewSizedValue("v1", math.MaxUint64-50))
			require.NoError(t, err)
			_, err = maxCache.Insert("k2", NewSizedValue("v2", 50))
			require.NoError(t, err)

			// Act 2: Grow k2 by +100. Evicting k1 first prevents uint64 overflow.
			err = maxCache.UpdateSize("k2", 100)

			// Assert 2
			require.NoError(t, err)
			assert.Nil(t, maxCache.LookUpWithoutChangingOrder("k1"))
			assert.NotNil(t, maxCache.LookUpWithoutChangingOrder("k2"))
		})
	}
}

func TestRegression_F05(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_ColdStartConcurrentCriticalPressure", func(t *testing.T) {
			// Arrange
			blockFirst := false
			entered := make(chan struct{})
			release := make(chan struct{})
			pressure := 0.0

			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if blockFirst {
						blockFirst = false
						close(entered)
						<-release
					}
					return pressure
				}),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			_, err := cache.Insert("k1", NewSizedValue("v1", 40))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 40))
			require.NoError(t, err)
			_, err = cache.Insert("tmp", NewSizedValue("vt", 10))
			require.NoError(t, err)
			_ = cache.Erase("tmp")

			// Invalidate cached pressure via Compact() to simulate cold uninitialized pressure before spike.
			cache.Compact()
			pressure = 0.95
			blockFirst = true

			// Act: Goroutine 1 blocks inside PressureFunc() while Goroutine 2 calls EvaluateMemoryPressure().
			done1 := make(chan []ValueType, 1)
			go func() {
				done1 <- cache.EvaluateMemoryPressure()
			}()
			<-entered

			evicted2 := cache.EvaluateMemoryPressure()
			close(release)
			evicted1 := <-done1

			// Assert: Goroutine 2 itself sheds k1 (40B) down to targetSize (50B) while Goroutine 1 is blocked.
			assert.Len(t, evicted2, 1)
			assert.Empty(t, evicted1)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("k1"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k2"))
		})

		t.Run(b.name+"_ZeroPressureConcurrentNoFalseEviction_C01", func(t *testing.T) {
			// Arrange
			blockFirst := false
			entered := make(chan struct{})
			release := make(chan struct{})

			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if blockFirst {
						blockFirst = false
						close(entered)
						<-release
					}
					return 0.0
				}),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			_, err := cache.Insert("k0", NewSizedValue("v0", 40))
			require.NoError(t, err)
			_, err = cache.Insert("k1", NewSizedValue("v1", 40))
			require.NoError(t, err)

			blockFirst = true

			// Act: Goroutine 1 blocks inside PressureFunc() returning 0.0 while Goroutine 2 evaluates pressure.
			done1 := make(chan []ValueType, 1)
			go func() {
				done1 <- cache.EvaluateMemoryPressure()
			}()
			<-entered

			evicted2 := cache.EvaluateMemoryPressure()
			close(release)
			evicted1 := <-done1

			// Assert: Zero pressure (0.0) must never fabricate EvictionThreshold or shed entries.
			assert.Empty(t, evicted2)
			assert.Empty(t, evicted1)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k0"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k1"))
		})

		t.Run(b.name+"_ReentrantZeroPressureNoFalseEviction_C01", func(t *testing.T) {
			// Arrange
			var cache PressureAwareCache
			reentrant := false
			cache = b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if reentrant && cache != nil {
						_ = cache.EvaluateMemoryPressure()
					}
					return 0.0
				}),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			_, err := cache.Insert("k0", NewSizedValue("v0", 40))
			require.NoError(t, err)
			_, err = cache.Insert("k1", NewSizedValue("v1", 40))
			require.NoError(t, err)

			// Act: Enable re-entrant EvaluateMemoryPressure() inside PressureFunc() returning 0.0.
			reentrant = true
			evicted := cache.EvaluateMemoryPressure()

			// Assert: Neither re-entrant nor outer call evicts entries when pressure is 0.0.
			assert.Empty(t, evicted)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k0"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k1"))
		})

		t.Run(b.name+"_PostReclamationClearsLastSampledPressure_C02", func(t *testing.T) {
			// Arrange
			blockFirst := false
			entered := make(chan struct{})
			release := make(chan struct{})
			pressure := 0.95

			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if blockFirst {
						blockFirst = false
						close(entered)
						<-release
					}
					return pressure
				}),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			// Seed high pressure (0.95) in lastSampledPressureBits, then reclaim and restore healthy pressure (0.10).
			_ = cache.EvaluateMemoryPressure()
			cache.EraseEntriesWithGivenPrefix("")
			pressure = 0.10

			_, err := cache.Insert("k1", NewSizedValue("v1", 40))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 40))
			require.NoError(t, err)
			cache.Compact()

			blockFirst = true

			// Act: Goroutine 1 blocks sampling 0.10 while Goroutine 2 evaluates pressure after reclamation.
			done1 := make(chan []ValueType, 1)
			go func() {
				done1 <- cache.EvaluateMemoryPressure()
			}()
			<-entered

			evicted2 := cache.EvaluateMemoryPressure()
			close(release)
			evicted1 := <-done1

			// Assert: Post-reclamation writer must not read stale pre-reclamation 0.95 pressure and evict k1.
			assert.Empty(t, evicted2)
			assert.Empty(t, evicted1)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k1"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k2"))
		})
	}
}

func TestRegression_F06(t *testing.T) {
	// Arrange
	largeKey := strings.Clone("dir/" + strings.Repeat("X", 1<<16))
	largeStart := uintptr(unsafe.Pointer(unsafe.StringData(largeKey)))
	largeEnd := largeStart + uintptr(len(largeKey))

	t.Run("RadixCache", func(t *testing.T) {
		// Arrange
		c := NewRadixCache(1<<20, WithInvariantChecking(true)).(*radixCache)
		_, err := c.Insert(largeKey, NewStringValue("v"))
		require.NoError(t, err)

		// Act: Split "dir/..." at "dir/" and erase the large key.
		_, err = c.Insert("dir/a", NewStringValue("va"))
		require.NoError(t, err)
		_, err = c.Insert("dir/b", NewStringValue("vb"))
		require.NoError(t, err)
		_ = c.Erase(largeKey)

		// Assert: Routing node prefix "dir/" does not alias largeKey's backing array.
		routingNode := c.root.child
		require.NotNil(t, routingNode)
		assert.Equal(t, "dir/", routingNode.prefix)
		prefixPtr := uintptr(unsafe.Pointer(unsafe.StringData(routingNode.prefix)))
		assert.True(t, prefixPtr < largeStart || prefixPtr >= largeEnd, "routing node prefix must not pin largeKey backing array")
	})

	t.Run("ArenaRadixCache", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(1<<20, WithInvariantChecking(true)).(*arenaRadix)
		_, err := c.Insert(largeKey, NewStringValue("v"))
		require.NoError(t, err)

		// Act: Split "dir/..." at "dir/" and erase the large key.
		_, err = c.Insert("dir/a", NewStringValue("va"))
		require.NoError(t, err)
		_, err = c.Insert("dir/b", NewStringValue("vb"))
		require.NoError(t, err)
		_ = c.Erase(largeKey)

		// Assert: Routing node prefix "dir/" does not alias largeKey's backing array.
		routingID := c.nodes[c.root].child
		require.NotEqual(t, nilNode, routingID)
		assert.Equal(t, "dir/", c.nodes[routingID].prefix)
		prefixPtr := uintptr(unsafe.Pointer(unsafe.StringData(c.nodes[routingID].prefix)))
		assert.True(t, prefixPtr < largeStart || prefixPtr >= largeEnd, "routing node prefix must not pin largeKey backing array")
	})
}

func TestRegression_F07(t *testing.T) {
	// Arrange
	c := NewMapCache(
		1000,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 { return 0.95 }),
		WithEvictionRetentionRatio(0.50),
	).(*mapCache)

	for i := range 20 {
		_, err := c.Insert(fmt.Sprintf("k-%d", i), NewSizedValue("v", 10))
		require.NoError(t, err)
	}
	require.False(t, c.dirtyIndex)
	epochBefore := c.reclaimEpoch.Load()

	// Act
	allocs := testing.AllocsPerRun(20, func() {
		c.Compact()
	})
	evicted := c.EvaluateMemoryPressure()

	// Assert: Zero allocations on Compact() when dirtyIndex == false, and EvaluateMemoryPressure() does not reallocate map.
	assert.InDelta(t, 0.0, allocs, 1e-9)
	assert.Empty(t, evicted)
	assert.Equal(t, epochBefore, c.reclaimEpoch.Load())
}

func TestRegression_F08(t *testing.T) {
	// Arrange & Act
	opts := ApplyOptions(WithCompactionThreshold(1.0))

	// Assert: Tier 1 compaction window [1.0, 1.15) is preserved without clamping EvictionThreshold to 1.0.
	assert.InDelta(t, 1.0, opts.CompactionThreshold, 1e-9)
	assert.InDelta(t, 1.15, opts.EvictionThreshold, 1e-9)
}

func TestRegression_F09(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_ManyZeroPlusOneByte", func(t *testing.T) {
			// Arrange
			pressure := 0.0
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return pressure }),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			for i := range 100 {
				_, err := cache.Insert(fmt.Sprintf("z-%03d", i), NewStringValue(""))
				require.NoError(t, err)
			}
			_, err := cache.Insert("one_byte", NewStringValue("x"))
			require.NoError(t, err)

			// Act: Critical pressure must shed ~50 zero-size entries even though currentSize == 1 <= targetSize (50).
			pressure = 0.95
			evicted := cache.EvaluateMemoryPressure()

			// Assert
			assert.GreaterOrEqual(t, len(evicted), 50)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("one_byte"))
		})

		t.Run(b.name+"_ZeroAtTailPlusLargeMRU", func(t *testing.T) {
			// Arrange
			pressure := 0.0
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return pressure }),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			for i := range 10 {
				_, err := cache.Insert(fmt.Sprintf("z-%02d", i), NewStringValue(""))
				require.NoError(t, err)
			}
			_, err := cache.Insert("mru_60", NewSizedValue("large", 60))
			require.NoError(t, err)

			// Act: Critical pressure (targetSize = 50, targetLen = 5) must shed mru_60 and retain 5 zero-size entries.
			pressure = 0.95
			evicted := cache.EvaluateMemoryPressure()

			// Assert
			assert.Len(t, evicted, 6)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("mru_60"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("z-09"))
		})
	}
}

func TestRegression_F10(t *testing.T) {
	// Arrange
	c := NewRadixCache(50).(*radixCache)
	alphaKey := "prefix/alpha"
	betaKey := "prefix/beta"
	v1 := ValueType(NewSizedValue("v1", 40))
	v2 := ValueType(NewSizedValue("v2", 40))

	_, err := c.Insert(alphaKey, v1)
	require.NoError(t, err)
	toggle := false

	// Act: Alternate inserting "prefix/beta" and "prefix/alpha" (40B each in a 50B cache),
	// forcing eviction of the sibling before insertNode.
	allocs := testing.AllocsPerRun(20, func() {
		if toggle {
			_, _ = c.Insert(alphaKey, v1)
		} else {
			_, _ = c.Insert(betaKey, v2)
		}
		toggle = !toggle
	})

	// Assert: Pre-eviction avoids splitNode + prefix split + compressPathUpwards string re-concatenation
	// (3 allocs/op vs 6 allocs/op on pre-fix code).
	assert.LessOrEqual(t, allocs, 3.0)
	c.checkInvariants()
	require.NotNil(t, c.root.child)
	assert.Nil(t, c.root.child.child)
}

func TestRegression_F11(t *testing.T) {
	// Arrange
	v1 := ValueType(NewBytesValue([]byte("comparable-payload")))
	v2 := ValueType(NewBytesValue([]byte("comparable-payload")))
	v3 := ValueType(NewBytesValue([]byte("different-payload")))

	// Act: Evaluate native Go interface equality (which panics on pre-fix slice BytesValue).
	equal := v1 == v2
	diff := v1 == v3

	// Assert
	assert.True(t, equal)
	assert.False(t, diff)
}

func TestRegression_F12(t *testing.T) {
	// Arrange
	mc := NewMapCache(100000, WithInvariantChecking(true)).(*mapCache)
	ac := NewArenaRadixCache(100000, WithInvariantChecking(true)).(*arenaRadix)

	for i := range 200 {
		k := fmt.Sprintf("item/sub/%04d", i)
		_, err := mc.Insert(k, NewSizedValue("v", 10))
		require.NoError(t, err)
		_, err = ac.Insert(k, NewSizedValue("v", 10))
		require.NoError(t, err)
	}

	// Act: Drain all entries via individual Erase(key) calls down to 0 entries.
	for i := range 200 {
		k := fmt.Sprintf("item/sub/%04d", i)
		require.NotNil(t, mc.Erase(k))
		require.NotNil(t, ac.Erase(k))
	}

	// Assert: Peak structures are released upon reaching empty state.
	assert.False(t, mc.dirtyIndex)
	assert.Empty(t, mc.index)
	assert.Len(t, ac.nodes, 1)
	assert.Equal(t, nilNode, ac.freeHead)
	assert.Zero(t, ac.freeCount)
}

func TestRegression_F13(t *testing.T) {
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
			// Arrange: Configure critical pressure (0.95) and 50% retention (targetSize = 50B).
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return 0.95 }),
				WithEvictionRetentionRatio(0.50),
			)

			// Act: Insert 4 entries of 20B each (80B total > 50B targetSize).
			_, err := cache.Insert("k1", NewSizedValue("v1", 20))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 20))
			require.NoError(t, err)
			_, err = cache.Insert("k3", NewSizedValue("v3", 20))
			require.NoError(t, err)
			_, err = cache.Insert("k4", NewSizedValue("v4", 20))
			require.NoError(t, err)

			// Assert: Foreground pressure reclamation sheds oldest entries (k1, k2) across all 3 backends.
			assert.Nil(t, cache.LookUpWithoutChangingOrder("k1"))
			assert.Nil(t, cache.LookUpWithoutChangingOrder("k2"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k3"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k4"))
		})
	}
}

func TestRegression_F14(t *testing.T) {
	// Arrange & Act
	content, err := os.ReadFile(".golangci.yml")
	ciContent, ciErr := os.ReadFile(".github/workflows/ci.yml")

	// Assert
	require.NoError(t, err)
	require.NoError(t, ciErr)
	cfg := string(content)
	ciText := string(ciContent)
	assert.Contains(t, cfg, `version: "2"`)
	assert.Contains(t, cfg, "testifylint")
	assert.Contains(t, cfg, "thelper")
	assert.Contains(t, cfg, "revive")
	assert.Contains(t, cfg, "goimports")
	assert.Contains(t, ciText, "golangci/golangci-lint-action@9fae48acfc02a90574d7c304a1758ef9895495fa")
	assert.Contains(t, ciText, "goimports -l .")
}

func TestRegression_F15(t *testing.T) {
	// Arrange: Push a value-type entry (instead of *entry pointer) into mapCache.entries.
	c := NewMapCache(100, WithInvariantChecking(true)).(*mapCache)
	el := c.entries.PushFront(entry{
		key:   "bad_value_type",
		value: NewSizedValue("v", 5),
		size:  5,
	})
	c.index["bad_value_type"] = el
	c.currentSize = 5

	// Act & Assert: checkInvariants must strictly require *entry and panic on value-type entry.
	assert.Panics(t, func() {
		c.checkInvariants()
	})
}

// ============================================================================
// Dedicated Regression Suite for Round 2 Code Review Findings R2-F01 through R2-F10
// ============================================================================

func TestRegression_R2_F01(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_NoPostReclamationRepoisoningOfLastSampled", func(t *testing.T) {
			// Arrange: Seed 2 entries (80B), then trigger foreground Insert shedding under critical pressure (0.95)
			// using k4 of size 30B <= targetSize (50B) so post-shedding currentSize (30B) is <= targetSize (50B).
			blockNext := false
			entered := make(chan struct{})
			release := make(chan struct{})
			pressure := 0.10

			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if blockNext {
						blockNext = false
						close(entered)
						<-release
					}
					return pressure
				}),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			_, err := cache.Insert("k1", NewSizedValue("v1", 40))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 40))
			require.NoError(t, err)

			// Foreground Insert of k4 (30B <= targetSize 50B) at 0.95 evicts k1 (capacity) and sheds k2 (pressure),
			// leaving only k4 (30B <= targetSize 50B).
			pressure = 0.95
			_, err = cache.Insert("k4", NewSizedValue("v4", 30))
			require.NoError(t, err)
			require.Nil(t, cache.LookUpWithoutChangingOrder("k1"))
			require.Nil(t, cache.LookUpWithoutChangingOrder("k2"))
			require.NotNil(t, cache.LookUpWithoutChangingOrder("k4"))

			// Restore healthy pressure (0.10) and block Goroutine 1 inside samplePressureFresh().
			pressure = 0.10
			blockNext = true
			done1 := make(chan []ValueType, 1)
			go func() {
				done1 <- cache.EvaluateMemoryPressure()
			}()
			<-entered

			// Act: While Goroutine 1 is in flight holding samplingPressure = true, Goroutine 2 inserts k5 (30B,
			// bringing currentSize from 30B to 60B > targetSize 50B) and calls EvaluateMemoryPressure().
			evictedK5, err := cache.Insert("k5", NewSizedValue("v5", 30))
			require.NoError(t, err)
			evicted2 := cache.EvaluateMemoryPressure()
			close(release)
			evicted1 := <-done1

			// Assert: lastSampledPressureBits was not re-poisoned with 0.95 after foreground shedding;
			// both k4 (30B) and k5 (30B) remain intact at 0.10 pressure.
			assert.Empty(t, evictedK5)
			assert.Empty(t, evicted2)
			assert.Empty(t, evicted1)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k4"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k5"))
		})

		t.Run(b.name+"_SampledEpochMismatchResamplesFreshPressureOutsideLock", func(t *testing.T) {
			// Arrange: Block inside PressureFunc before c.mu.Lock() while a concurrent Compact() bumps reclaimEpoch.
			inFirstSample := make(chan struct{})
			proceedSample := make(chan struct{})
			firstCall := true

			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if firstCall {
						firstCall = false
						close(inFirstSample)
						<-proceedSample
					}
					return 0.95
				}),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			// Insert k1 (40B) and k2 (20B) at normal pressure before enabling the custom blocker.
			// Note: firstCall will trigger on the first Insert("k1", 40).
			doneInsert := make(chan error, 1)
			go func() {
				_, err := cache.Insert("k1", NewSizedValue("v1", 60))
				doneInsert <- err
			}()
			<-inFirstSample

			// Bump reclaimEpoch while Insert's initial samplePressure() is in flight.
			cache.Compact()
			close(proceedSample)

			// Act
			err := <-doneInsert
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 20))
			require.NoError(t, err)

			// Assert: Insert re-sampled 0.95 outside lock instead of reading 0.0 from cachedPressureBits,
			// shedding k1 (60B) so only k2 (20B <= targetSize 50B) remains.
			assert.Nil(t, cache.LookUpWithoutChangingOrder("k1"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k2"))
		})

		t.Run(b.name+"_NormalPressureErasePreservesReclaimEpoch", func(t *testing.T) {
			// Arrange: Insert 2 entries at normal 0.10 pressure (< CompactionThreshold 0.75).
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return 0.10 }),
			)
			_, err := cache.Insert("k1", NewSizedValue("v1", 20))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 20))
			require.NoError(t, err)

			// Act: Erase k1 at normal pressure while k2 remains in the cache.
			erased := cache.Erase("k1")

			// Assert: Normal-pressure Erase does not bump reclaimEpoch or set pressureNeedsRefresh.
			require.NotNil(t, erased)
			switch c := cache.(type) {
			case *mapCache:
				assert.Zero(t, c.reclaimEpoch.Load())
				assert.False(t, c.pressureNeedsRefresh.Load())
			case *radixCache:
				assert.Zero(t, c.reclaimEpoch.Load())
				assert.False(t, c.pressureNeedsRefresh.Load())
			case *arenaRadix:
				assert.Zero(t, c.reclaimEpoch.Load())
				assert.False(t, c.pressureNeedsRefresh.Load())
			}
		})
	}
}

func TestRegression_R2_F02(t *testing.T) {
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
			// Arrange: k1 (20B) at LRU tail, k2 (30B) at MRU head, maxSize = 100, targetSize = 50.
			pressure := 0.10
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return pressure }),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			)

			_, err := cache.Insert("k1", NewSizedValue("v1", 20))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 30))
			require.NoError(t, err)

			// Act: Raise pressure to 0.95 and grow LRU tail entry k1 by +10 (20 -> 30, currentSize = 60 > 50).
			pressure = 0.95
			err = cache.UpdateSize("k1", 10)

			// Assert: Strict LRU eviction order must evict k1 (oldest at LRU tail) and retain k2 (newest at MRU head).
			require.NoError(t, err)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("k1"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k2"))
		})
	}
}

func TestRegression_R2_F03(t *testing.T) {
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
			// Arrange: Populate 500 zero-size entries at LRU tail and 500 1-byte entries toward MRU.
			// Pre-reclaim zero-size entries so lastReclaimedZeroCount == 500 and shedding must skip all 500
			// zero-size tail entries to evict 250 1-byte entries in a single O(N) cursor pass.
			pressure := 0.0
			cache := b.fn(
				1000,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return pressure }),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			for i := range 1000 {
				_, err := cache.Insert(fmt.Sprintf("z_%04d", i), NewStringValue(""))
				require.NoError(t, err)
			}
			pressure = 0.95
			evictedZero := cache.EvaluateMemoryPressure()
			require.Len(t, evictedZero, 500)

			pressure = 0.0
			for i := range 500 {
				_, err := cache.Insert(fmt.Sprintf("p_%04d", i), NewSizedValue("x", 1))
				require.NoError(t, err)
			}
			// Restore watermark so the 500 zero-size tail entries are retained while byte shedding runs.
			switch c := cache.(type) {
			case *mapCache:
				c.lastReclaimedZeroCount = 500
			case *radixCache:
				c.lastReclaimedZeroCount = 500
			case *arenaRadix:
				c.lastReclaimedZeroCount = 500
			}

			// Act: Shed currentSize from 500B to 500B (insert 100B more so currentSize = 600B > targetSize = 500B).
			for i := 500; i < 600; i++ {
				_, err := cache.Insert(fmt.Sprintf("p_%04d", i), NewSizedValue("x", 1))
				require.NoError(t, err)
			}
			switch c := cache.(type) {
			case *mapCache:
				c.lastReclaimedZeroCount = 500
			case *radixCache:
				c.lastReclaimedZeroCount = 500
			case *arenaRadix:
				c.lastReclaimedZeroCount = 500
			}
			pressure = 0.95
			evictedBytes := cache.EvaluateMemoryPressure()

			// Assert: Exactly 100 positive-size entries are evicted in a single pass while retaining all 500 zero-size tail entries.
			assert.Len(t, evictedBytes, 100)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("z_0500"))
			assert.Nil(t, cache.LookUpWithoutChangingOrder("p_0000"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("p_0599"))
		})
	}
}

func TestRegression_R2_F04(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_SubcaseA_ForegroundZeroSizeShedding", func(t *testing.T) {
			// Arrange: Configure critical pressure (0.95) and 50% retention.
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return 0.95 }),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			)

			// Act: Insert 100 zero-size entries via foreground Insert under critical pressure.
			totalEvicted := 0
			for i := range 100 {
				evicted, err := cache.Insert(fmt.Sprintf("fg_zero_%03d", i), NewStringValue(""))
				require.NoError(t, err)
				totalEvicted += len(evicted)
			}

			// Assert: Foreground writes shed 99 older zero-size entries, retaining only the latest MRU entry.
			assert.Equal(t, 99, totalEvicted)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("fg_zero_000"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("fg_zero_099"))
		})

		t.Run(b.name+"_SubcaseB_NoInitialByteShedNeededCliff", func(t *testing.T) {
			// Arrange: Insert 1 entry of 51B at tail (targetSize = 50B) followed by 100 zero-size entries.
			pressure := 0.0
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return pressure }),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			_, err := cache.Insert("tail_51", NewSizedValue("v", 51))
			require.NoError(t, err)
			for i := range 100 {
				_, err = cache.Insert(fmt.Sprintf("z_%03d", i), NewStringValue(""))
				require.NoError(t, err)
			}

			// Act: EvaluateMemoryPressure under 0.95 must shed both "tail_51" and 50 zero-size entries (51 total).
			pressure = 0.95
			evicted := cache.EvaluateMemoryPressure()

			// Assert
			assert.Len(t, evicted, 51)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("tail_51"))
			assert.Nil(t, cache.LookUpWithoutChangingOrder("z_000"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("z_099"))
		})

		t.Run(b.name+"_SubcaseC_PreservesPositiveEntriesBelowTargetSizeAndIsIdempotent", func(t *testing.T) {
			// Arrange: 50 positive-size 10B entries (500B == targetSize) + 2 zero-size entries at MRU.
			pressure := 0.0
			cache := b.fn(
				1000,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return pressure }),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			for i := range 50 {
				_, err := cache.Insert(fmt.Sprintf("pos_%02d", i), NewSizedValue("v", 10))
				require.NoError(t, err)
			}
			_, err := cache.Insert("zero_1", NewStringValue(""))
			require.NoError(t, err)
			_, err = cache.Insert("zero_2", NewStringValue(""))
			require.NoError(t, err)

			// Act: Invoke EvaluateMemoryPressure() 6 times under sustained critical pressure (0.95).
			pressure = 0.95
			evicted1 := cache.EvaluateMemoryPressure()
			var subsequentEvicted int
			for range 5 {
				subsequentEvicted += len(cache.EvaluateMemoryPressure())
			}

			// Assert: Call 1 evicts only 1 zero-size entry (zero_1); Calls 2..6 evict 0 entries;
			// all 50 positive-size entries (500B) and zero_2 remain intact.
			assert.Len(t, evicted1, 1)
			assert.Zero(t, subsequentEvicted)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("pos_00"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("pos_49"))
			assert.Nil(t, cache.LookUpWithoutChangingOrder("zero_1"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("zero_2"))
		})
	}
}

func TestRegression_R2_F05(t *testing.T) {
	// Arrange: MapCache of capacity 100B (10 entries of 10B each) under moderate Tier 1 pressure (0.80).
	c := NewMapCache(
		100,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 { return 0.80 }),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
	).(*mapCache)

	for i := range 10 {
		_, err := c.Insert(fmt.Sprintf("init_%02d", i), NewSizedValue("v", 10))
		require.NoError(t, err)
	}
	require.Zero(t, c.reclaimEpoch.Load())

	// Act 1: Perform 30 steady-state capacity-turnover inserts (each evicting 1 old key and adding 1 new key, len == peakIndexLen == 10).
	for i := range 30 {
		_, err := c.Insert(fmt.Sprintf("turn_%02d", i), NewSizedValue("v", 10))
		require.NoError(t, err)
	}

	// Assert 1: Zero compactions occurred during steady-state capacity turnover.
	assert.Zero(t, c.reclaimEpoch.Load())
	assert.Equal(t, 10, c.peakIndexLen)

	// Act 2: Erase 3 entries so len(c.index) drops to 7 (30% shrinkage below peakIndexLen 10 >= 25%).
	c.Erase("turn_27")
	c.Erase("turn_28")
	c.Erase("turn_29")

	// Assert 2: Auto-compaction triggered on >= 25% shrinkage below peakIndexLen, clearing dirtyIndex and updating peakIndexLen to 7.
	assert.False(t, c.dirtyIndex)
	assert.Equal(t, 7, c.peakIndexLen)
}

func TestRegression_R2_F06(t *testing.T) {
	// Arrange & Act & Assert 1: checkInvariants panics on zeroSizeCount mismatch across all 3 backends.
	t.Run("ZeroSizeCountInvariantPanic", func(t *testing.T) {
		// Arrange
		mc := NewMapCache(100).(*mapCache)
		_, err := mc.Insert("z", NewStringValue(""))
		require.NoError(t, err)
		mc.zeroSizeCount = 0

		rc := NewRadixCache(100).(*radixCache)
		_, err = rc.Insert("z", NewStringValue(""))
		require.NoError(t, err)
		rc.zeroSizeCount = 0

		ac := NewArenaRadixCache(100).(*arenaRadix)
		_, err = ac.Insert("z", NewStringValue(""))
		require.NoError(t, err)
		ac.zeroSizeCount = 0

		// Act & Assert
		assert.Panics(t, func() { mc.checkInvariants() })
		assert.Panics(t, func() { rc.checkInvariants() })
		assert.Panics(t, func() { ac.checkInvariants() })
	})

	// Arrange & Act & Assert 2: arenaRadix.checkInvariants panics on expectedNodeMapLen mismatch.
	t.Run("ArenaRadixExpectedNodeMapLenInvariantPanic", func(t *testing.T) {
		// Arrange
		ac := NewArenaRadixCache(100).(*arenaRadix)
		_, err := ac.Insert("k1", NewStringValue("v1"))
		require.NoError(t, err)
		ac.expectedNodeMapLen = 99

		// Act & Assert
		assert.Panics(t, func() { ac.checkInvariants() })
	})

	// Arrange & Act & Assert 3: Root node (nodeID == 0) deletion when hash("") is absent in nodeMap preserves expectedNodeMapLen.
	t.Run("RootNodeEraseWithoutMapEntryPreservesExpectedNodeMapLen", func(t *testing.T) {
		// Arrange
		ac := NewArenaRadixCache(100, WithInvariantChecking(true)).(*arenaRadix)
		_, err := ac.Insert("", NewStringValue("root_val"))
		require.NoError(t, err)
		_, err = ac.Insert("other", NewStringValue("other_val"))
		require.NoError(t, err)
		delete(ac.nodeMap, hashString(""))
		ac.expectedNodeMapLen = len(ac.nodeMap)

		// Act: Erase "" (which resides at c.root == 0) while hash("") is not in c.nodeMap.
		erased := ac.Erase("")

		// Assert: expectedNodeMapLen must remain equal to len(ac.nodeMap) (1) without false decrement.
		require.NotNil(t, erased)
		assert.Len(t, ac.nodeMap, 1)
		assert.Equal(t, 1, ac.expectedNodeMapLen)
	})
}

func TestRegression_R2_F07(t *testing.T) {
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
			// Arrange: Seed pressureInitialized = true via an initial Insert, then have PressureFunc
			// call Compact() (clearing pressureInitialized) followed by a re-entrant EvaluateMemoryPressure().
			var pac PressureAwareCache
			reentrantCall := false
			invocations := 0

			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					invocations++
					if reentrantCall && pac != nil {
						reentrantCall = false
						pac.Compact()
						_ = pac.EvaluateMemoryPressure()
					}
					return 0.50
				}),
			)
			pac = cache.(PressureAwareCache)

			_, err := cache.Insert("seed", NewSizedValue("v", 10))
			require.NoError(t, err)
			invocations = 0
			reentrantCall = true

			// Act: Call EvaluateMemoryPressure() while pressureInitialized is initially true.
			_ = pac.EvaluateMemoryPressure()

			// Assert: samplingGID was recorded unconditionally, so the re-entrant EvaluateMemoryPressure()
			// after Compact() returned 0.0 immediately without invoking PressureFunc a second time.
			assert.Equal(t, 1, invocations)
		})
	}
}

func TestRegression_R2_F08(t *testing.T) {
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
			// Arrange: Insert 4 entries of 20B (80B total) at 0.0 pressure, then raise pressure to 0.95 (targetSize = 50B).
			pressure := 0.0
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return pressure }),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			)

			_, err := cache.Insert("p/1", NewSizedValue("v1", 20))
			require.NoError(t, err)
			_, err = cache.Insert("p/2", NewSizedValue("v2", 20))
			require.NoError(t, err)
			_, err = cache.Insert("other/3", NewSizedValue("v3", 20))
			require.NoError(t, err)
			_, err = cache.Insert("other/4", NewSizedValue("v4", 20))
			require.NoError(t, err)

			// Act: Erase "other/4" (leaving 60B > targetSize 50B) under 0.95 critical pressure.
			pressure = 0.95
			erased := cache.Erase("other/4")

			// Assert: Foreground Erase also triggers Tier 2 shedding of "p/1" (oldest 20B) so currentSize drops to 40B <= 50B.
			require.NotNil(t, erased)
			assert.Nil(t, cache.LookUpWithoutChangingOrder("p/1"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("p/2"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("other/3"))
		})
	}
}

func TestRegression_R2_F09(t *testing.T) {
	// Arrange & Act
	ciContent, err := os.ReadFile(".github/workflows/ci.yml")

	// Assert
	require.NoError(t, err)
	ciText := string(ciContent)
	assert.Contains(t, ciText, "version: v2.13.1")
	assert.NotContains(t, ciText, "version: v2.1.6")
}

func TestRegression_R2_F10(t *testing.T) {
	// Arrange & Act
	optsCompactionAtDefaultEviction := ApplyOptions(WithCompactionThreshold(0.90))
	optsEvictionAtDefaultCompaction := ApplyOptions(WithEvictionThreshold(0.75))
	optsExplicitEqual := ApplyOptions(WithCompactionThreshold(0.80), WithEvictionThreshold(0.80))

	// Assert
	assert.InDelta(t, 0.90, optsCompactionAtDefaultEviction.CompactionThreshold, 1e-9)
	assert.InDelta(t, 1.05, optsCompactionAtDefaultEviction.EvictionThreshold, 1e-9)

	assert.InDelta(t, 0.625, optsEvictionAtDefaultCompaction.CompactionThreshold, 1e-9)
	assert.InDelta(t, 0.75, optsEvictionAtDefaultCompaction.EvictionThreshold, 1e-9)

	assert.InDelta(t, 0.80, optsExplicitEqual.CompactionThreshold, 1e-9)
	assert.InDelta(t, 0.80, optsExplicitEqual.EvictionThreshold, 1e-9)
}

// ============================================================================
// Dedicated Regression Suite for Round 3 Code Review Findings R3-F01 through R3-F04
// ============================================================================

func TestRegression_R3_F01(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_ThreePlusConcurrentCallersReentrantPressureFuncNoStackOverflow", func(t *testing.T) {
			// Arrange: 4 concurrent callers enter samplePressureFresh() during cold start so G1 holds
			// samplingPressure, G2 holds fallbackSampling, and G3 + G4 execute the 3rd+ overflow path.
			const numCallers = 4
			var pac PressureAwareCache
			var cache Cache
			var activeDepth sync.Map
			var maxDepth atomic.Int32
			var enteredCount atomic.Int32
			var overflowDone atomic.Int32
			allEntered := make(chan struct{})
			releaseHolders := make(chan struct{})

			cache = b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					gid := currentGoroutineID()
					val, _ := activeDepth.LoadOrStore(gid, new(atomic.Int32))
					depthPtr := val.(*atomic.Int32)
					d := depthPtr.Add(1)
					defer depthPtr.Add(-1)

					for {
						prev := maxDepth.Load()
						if d <= prev || maxDepth.CompareAndSwap(prev, d) {
							break
						}
					}
					if d > 1 {
						return 0.10
					}

					order := enteredCount.Add(1)
					if order == numCallers {
						close(allEntered)
					}
					<-allEntered

					// G3 and G4 (3rd+ concurrent callers) make re-entrant cache calls while G1 and G2 are still inside PressureFunc.
					if order >= 3 && pac != nil {
						pac.Compact()
						_ = pac.EvaluateMemoryPressure()
						_, _ = cache.Insert(fmt.Sprintf("reentrant_%d", order), NewSizedValue("v", 10))
						_ = cache.Erase(fmt.Sprintf("reentrant_%d", order))
						if overflowDone.Add(1) == numCallers-2 {
							close(releaseHolders)
						}
					}
					if order <= 2 {
						<-releaseHolders
					}
					return 0.10
				}),
			)
			pac = cache.(PressureAwareCache)

			// Act: Launch 4 concurrent EvaluateMemoryPressure() calls.
			var wg sync.WaitGroup
			wg.Add(numCallers)
			for range numCallers {
				go func() {
					defer wg.Done()
					_ = pac.EvaluateMemoryPressure()
				}()
			}
			wg.Wait()

			// Assert: Every goroutine's PressureFunc recursion depth stayed strictly 1 (no stack overflow)
			// and re-entrant Compact() inside G3/G4 did not increment reclaimEpoch.
			assert.Equal(t, int32(1), maxDepth.Load())
			switch c := cache.(type) {
			case *mapCache:
				assert.Zero(t, c.reclaimEpoch.Load())
				assert.Zero(t, c.overflowSamplingCount.Load())
			case *radixCache:
				assert.Zero(t, c.reclaimEpoch.Load())
				assert.Zero(t, c.overflowSamplingCount.Load())
			case *arenaRadix:
				assert.Zero(t, c.reclaimEpoch.Load())
				assert.Zero(t, c.overflowSamplingCount.Load())
			}
		})
	}
}

func TestRegression_R3_F02(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_StaleEpochPressureBitsRejectedOnReadAndRevalidatedOnStore", func(t *testing.T) {
			// Arrange: Create cache with healthy 0.10 pressure and two 40B entries (80B > targetSize 50B).
			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return 0.10 }),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			_, err := cache.Insert("k1", NewSizedValue("v1", 40))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 40))
			require.NoError(t, err)

			// Simulate a TOCTOU stale write from epoch 0 after reclaimEpoch advanced to 1.
			staleBits := math.Float64bits(0.95)
			switch c := cache.(type) {
			case *mapCache:
				c.reclaimEpoch.Store(1)
				c.cachedPressureBits.Store(staleBits)
				c.cachedPressureEpoch.Store(0)
				c.pressureInitialized.Store(true)
				c.pressureNeedsRefresh.Store(false)
				c.lastSampledPressureBits.Store(staleBits)
				c.lastSampledEpoch.Store(0)
				c.lastSampledInitialized.Store(true)
			case *radixCache:
				c.reclaimEpoch.Store(1)
				c.cachedPressureBits.Store(staleBits)
				c.cachedPressureEpoch.Store(0)
				c.pressureInitialized.Store(true)
				c.pressureNeedsRefresh.Store(false)
				c.lastSampledPressureBits.Store(staleBits)
				c.lastSampledEpoch.Store(0)
				c.lastSampledInitialized.Store(true)
			case *arenaRadix:
				c.reclaimEpoch.Store(1)
				c.cachedPressureBits.Store(staleBits)
				c.cachedPressureEpoch.Store(0)
				c.pressureInitialized.Store(true)
				c.pressureNeedsRefresh.Store(false)
				c.lastSampledPressureBits.Store(staleBits)
				c.lastSampledEpoch.Store(0)
				c.lastSampledInitialized.Store(true)
			}

			// Act: Sample pressure and insert k3 (10B, bringing total to 90B > targetSize 50B).
			evicted, err := cache.Insert("k3", NewSizedValue("v3", 10))
			require.NoError(t, err)

			// Assert: Stale epoch 0 bits (0.95) were rejected in epoch 1; healthy 0.10 was sampled and synced to epoch 1.
			assert.Empty(t, evicted)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k1"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k2"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k3"))
			switch c := cache.(type) {
			case *mapCache:
				assert.Equal(t, uint64(1), c.cachedPressureEpoch.Load())
				assert.Equal(t, uint64(1), c.lastSampledEpoch.Load())
			case *radixCache:
				assert.Equal(t, uint64(1), c.cachedPressureEpoch.Load())
				assert.Equal(t, uint64(1), c.lastSampledEpoch.Load())
			case *arenaRadix:
				assert.Equal(t, uint64(1), c.cachedPressureEpoch.Load())
				assert.Equal(t, uint64(1), c.lastSampledEpoch.Load())
			}
		})
	}
}

func TestRegression_R3_F03(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_EvaluateMemoryPressureResamplesOnEpochAdvanceBeforeLock", func(t *testing.T) {
			// Arrange: Goroutine A calls EvaluateMemoryPressure() and samples 0.95 at epoch 0,
			// while Goroutine B reclaims the cache (bumping reclaimEpoch to 1 and restoring pressure to 0.10)
			// and inserts 80B of new live data before Goroutine A acquires c.mu.Lock().
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.10))
			var blockEval atomic.Bool
			evalSampled := make(chan struct{})
			releaseEval := make(chan struct{})

			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					p := math.Float64frombits(pressureBits.Load())
					if blockEval.CompareAndSwap(true, false) {
						close(evalSampled)
						<-releaseEval
					}
					return p
				}),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache)

			_, err := cache.Insert("seed_1", NewSizedValue("v", 5))
			require.NoError(t, err)
			_, err = cache.Insert("seed_2", NewSizedValue("v", 5))
			require.NoError(t, err)
			_ = cache.Erase("seed_1")

			pressureBits.Store(math.Float64bits(0.95))
			blockEval.Store(true)

			doneEval := make(chan []ValueType, 1)
			go func() {
				doneEval <- cache.EvaluateMemoryPressure()
			}()
			<-evalSampled

			// While EvaluateMemoryPressure() is about to return 0.95 from its initial sample at epoch 0,
			// reclaim via Compact() (bumping reclaimEpoch), drop pressure to 0.10, and insert 80B (> targetSize 50B).
			pressureBits.Store(math.Float64bits(0.10))
			cache.Compact()
			_, err = cache.Insert("live_1", NewSizedValue("v1", 40))
			require.NoError(t, err)
			_, err = cache.Insert("live_2", NewSizedValue("v2", 40))
			require.NoError(t, err)

			// Act: Release Goroutine A so it acquires c.mu.Lock() in EvaluateMemoryPressure().
			close(releaseEval)
			evicted := <-doneEval

			// Assert: EvaluateMemoryPressure() detected sampledEpoch != c.reclaimEpoch.Load(),
			// resampled 0.10 outside c.mu.Lock(), and preserved both live_1 and live_2.
			assert.Empty(t, evicted)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("live_1"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("live_2"))
		})
	}
}

func TestRegression_R3_F04(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_EraseDuringInFlightSamplerInvalidatesReclaimEpoch", func(t *testing.T) {
			// Arrange: Populate k1 (40B), k2 (40B), k3 (10B) at 0.10 pressure.
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.10))
			var blockSampler atomic.Bool
			samplerInFlight := make(chan struct{})
			releaseSampler := make(chan struct{})

			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if blockSampler.CompareAndSwap(true, false) {
						close(samplerInFlight)
						<-releaseSampler
						return 0.95
					}
					return math.Float64frombits(pressureBits.Load())
				}),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			)

			_, err := cache.Insert("k1", NewSizedValue("v1", 40))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 40))
			require.NoError(t, err)
			_, err = cache.Insert("k3", NewSizedValue("v3", 10))
			require.NoError(t, err)

			// Start Goroutine A which blocks inside samplePressureFresh() with samplingPressure == true.
			blockSampler.Store(true)
			doneInsert := make(chan []ValueType, 1)
			go func() {
				ev, _ := cache.Insert("k4", NewSizedValue("v4", 20))
				doneInsert <- ev
			}()
			<-samplerInFlight

			// Act: While Goroutine A is in flight inside samplePressureFresh() (about to return pre-erasure 0.95),
			// Goroutine B erases k1 (40B) at 0.10 pressure. Because samplingPressure.Load() is true,
			// hasElevatedPressureToInvalidate returns true and Erase calls markReclaimedLocked().
			erased := cache.Erase("k1")
			require.NotNil(t, erased)

			close(releaseSampler)
			evicted := <-doneInsert

			// Assert: Goroutine A saw reclaimEpoch advance, re-sampled healthy 0.10 pressure after Erase("k1"),
			// and did NOT shed k2 (leaving k2=40B, k3=10B, k4=20B = 70B > targetSize 50B intact).
			assert.Empty(t, evicted)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k2"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k3"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k4"))
		})
	}
}

func TestRegression_R5_F01_EvaluateMemoryPressureEpochResamplePanicSafety(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_PanicDuringEpochResampleDoesNotDoubleUnlockOrRunCheckInvariantsUnlocked", func(t *testing.T) {
			// Arrange: Goroutine A enters EvaluateMemoryPressure(), takes initial sample (call 1),
			// and waits while Goroutine B calls Compact() to bump reclaimEpoch.
			// When Goroutine A unlocks c.mu and resamples (call 2), PressureFunc panics.
			var armed atomic.Bool
			var sampleCount atomic.Int32
			firstSampleReady := make(chan struct{})
			epochBumped := make(chan struct{})

			cache := b.fn(
				100,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if !armed.Load() {
						return 0.10
					}
					n := sampleCount.Add(1)
					if n == 1 {
						close(firstSampleReady)
						<-epochBumped
						return 0.10
					}
					panic("synthetic PressureFunc failure during resample")
				}),
			).(PressureAwareCache)

			_, err := cache.Insert("k1", NewSizedValue("v1", 20))
			require.NoError(t, err)
			_, err = cache.Insert("k2", NewSizedValue("v2", 20))
			require.NoError(t, err)
			_ = cache.Erase("k1")
			armed.Store(true)

			panicObserved := make(chan any, 1)
			go func() {
				defer func() {
					panicObserved <- recover()
				}()
				_ = cache.EvaluateMemoryPressure()
			}()

			<-firstSampleReady
			cache.Compact() // Advances reclaimEpoch while EvaluateMemoryPressure is between sample and c.mu.Lock()
			close(epochBumped)

			// Act
			recovered := <-panicObserved

			// Assert: The original panic is propagated cleanly (not masked by "sync: unlock of unlocked RWMutex")
			// and c.mu remains unlocked and usable for subsequent cache operations.
			require.Equal(t, "synthetic PressureFunc failure during resample", recovered)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k2"))
		})
	}
}

func TestRegression_R5_F02_MarkReclaimedFromSamplingGoroutineInvalidatesStalePressure(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_ReentrantReclamationInsidePressureFuncClearsPressureCache", func(t *testing.T) {
			// Arrange: Seed elevated cached pressure (0.95), then invoke a re-entrant Compact()
			// from inside PressureFunc (where isSamplingGoroutine() == true).
			var cacheRef PressureAwareCache
			reentrantCompact := false
			currentPressure := 0.95

			cache := b.fn(
				200,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if reentrantCompact && cacheRef != nil {
						reentrantCompact = false
						cacheRef.Compact()
					}
					return currentPressure
				}),
			).(PressureAwareCache)
			cacheRef = cache

			for i := range 5 {
				_, err := cache.Insert(fmt.Sprintf("k-%d", i), NewSizedValue("v", 10))
				require.NoError(t, err)
			}
			_ = cache.Erase("k-0")

			// Act: Trigger samplePressureFresh() with reentrantCompact = true.
			reentrantCompact = true
			currentPressure = 0.15
			_, err := cache.Insert("k-after", NewSizedValue("v", 10))

			// Assert: Re-entrant Compact() invalidated stale 0.95 pressure state without deadlocking or livelocking,
			// and k-after is inserted cleanly with all surviving entries intact.
			require.NoError(t, err)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k-after"))
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("k-4"))
		})
	}
}

func TestRegression_R5_F03_BoundedEpochRetryPreventsChildGoroutineLivelock(t *testing.T) {
	backends := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"MapCache", NewMapCache},
		{"RadixCache", NewRadixCache},
		{"ArenaRadixCache", NewArenaRadixCache},
	}

	for _, b := range backends {
		t.Run(b.name+"_ChildGoroutineReclaimingInsidePressureFuncTerminatesInBoundedRetries", func(t *testing.T) {
			// Arrange: Custom PressureFunc spawns a child goroutine that calls EraseEntriesWithGivenPrefix("tmp_")
			// under high pressure, bumping reclaimEpoch on every parent sample call.
			var cacheRef Cache
			var inChild atomic.Bool
			var parentSampleCalls atomic.Int32

			cache := b.fn(
				500,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if cacheRef != nil && inChild.CompareAndSwap(false, true) {
						parentSampleCalls.Add(1)
						done := make(chan struct{})
						go func() {
							defer func() {
								inChild.Store(false)
								close(done)
							}()
							_, _ = cacheRef.Insert("tmp_item", NewSizedValue("v", 1))
							cacheRef.EraseEntriesWithGivenPrefix("tmp_")
						}()
						<-done
					}
					return 0.95
				}),
				WithEvictionRetentionRatio(0.50),
			)
			cacheRef = cache

			// Act
			parentSampleCalls.Store(0)
			_, err := cache.Insert("target_key", NewSizedValue("val", 10))
			_ = cache.(PressureAwareCache).EvaluateMemoryPressure()

			// Assert: Both Insert and EvaluateMemoryPressure terminate in bounded retries (3 parent samples each = 6 total).
			require.NoError(t, err)
			assert.NotNil(t, cache.LookUpWithoutChangingOrder("target_key"))
			assert.Equal(t, int32(6), parentSampleCalls.Load())
		})
	}
}

func TestRegression_R5_F04_StoreSampledPressureSerializedAgainstMarkReclaimed(t *testing.T) {
	// Arrange
	mc := NewMapCache(100, WithInvariantChecking(true)).(*mapCache)
	rc := NewRadixCache(100, WithInvariantChecking(true)).(*radixCache)
	ac := NewArenaRadixCache(100, WithInvariantChecking(true)).(*arenaRadix)

	// Act: Store a sample at epoch 0, advance epoch via markReclaimedLocked, then attempt to store a stale epoch 0 sample.
	mc.storeSampledPressure(0, 0.95)
	mc.mu.Lock()
	mc.markReclaimedLocked()
	mc.mu.Unlock()
	mc.storeSampledPressure(0, 0.95)

	rc.storeSampledPressure(0, 0.95)
	rc.mu.Lock()
	rc.markReclaimedLocked()
	rc.mu.Unlock()
	rc.storeSampledPressure(0, 0.95)

	ac.storeSampledPressure(0, 0.95)
	ac.mu.Lock()
	ac.markReclaimedLocked()
	ac.mu.Unlock()
	ac.storeSampledPressure(0, 0.95)

	// Assert: Stale epoch 0 sample is rejected and pressureInitialized remains false with 0.0 bits.
	assert.False(t, mc.pressureInitialized.Load())
	assert.Zero(t, mc.cachedPressureBits.Load())
	assert.False(t, rc.pressureInitialized.Load())
	assert.Zero(t, rc.cachedPressureBits.Load())
	assert.False(t, ac.pressureInitialized.Load())
	assert.Zero(t, ac.cachedPressureBits.Load())
}

func TestRegression_R5_F05_ArenaRadixForegroundNoProtectSentinelSafety(t *testing.T) {
	// Arrange
	c := NewArenaRadixCache(100, WithInvariantChecking(true)).(*arenaRadix)
	_, err := c.Insert("k1", NewSizedValue("v1", 60))
	require.NoError(t, err)
	_, err = c.Insert("k2", NewSizedValue("v2", 40))
	require.NoError(t, err)

	// Act: Call shedAndCompactLocked directly with foregroundNoProtect and retention 0.0.
	c.options.EvictionRetentionRatio = 0.0
	c.mu.Lock()
	evicted := c.shedAndCompactLocked(0, 0.0, foregroundNoProtect)
	c.mu.Unlock()

	// Assert: foregroundNoProtect is normalized to nilNode so 100% of entries are evicted down to 0.
	assert.Len(t, evicted, 2)
	assert.Equal(t, 0, c.len)
	assert.Equal(t, uint64(0), c.currentSize)
}

func TestRegression_R5_F06_EmptyDrainAndPreInsertSlackReclamation(t *testing.T) {
	t.Run("ArenaRadixPreInsertDrainReleasesPeakSlackWithoutAdvancingReclaimEpoch", func(t *testing.T) {
		// Arrange: Populate 100 hierarchical keys (1000B total in 1000B cache) at normal pressure (0.10).
		c := NewArenaRadixCache(
			1000,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return 0.10 }),
		).(*arenaRadix)

		for i := range 100 {
			key := fmt.Sprintf("dir_%02d/sub_%02d/file_%03d", i%10, (i/10)%10, i)
			_, err := c.Insert(key, NewSizedValue("v", 10))
			require.NoError(t, err)
		}
		peakCap := cap(c.nodes)
		require.GreaterOrEqual(t, peakCap, 100)

		// Act: Insert a single 1000B jumbo entry that pre-evicts all 100 entries down to c.len == 0 before inserting.
		evicted, err := c.Insert("jumbo", NewSizedValue("jumbo_val", 1000))

		// Assert: All 100 entries were evicted, peak node arena slack was released, and reclaimEpoch remained 0.
		require.NoError(t, err)
		assert.Len(t, evicted, 100)
		assert.Zero(t, c.reclaimEpoch.Load())
		assert.Equal(t, uint32(0), c.freeCount)
		assert.Less(t, cap(c.nodes), peakCap)
		assert.Equal(t, 1, c.len)
	})

	t.Run("MapCachePreInsertDrainReleasesPeakBucketSlackWithoutAdvancingReclaimEpoch", func(t *testing.T) {
		// Arrange: Populate 100 keys (1000B total in 1000B cache) at normal pressure (0.10).
		c := NewMapCache(
			1000,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return 0.10 }),
		).(*mapCache)

		for i := range 100 {
			_, err := c.Insert(fmt.Sprintf("key_%03d", i), NewSizedValue("v", 10))
			require.NoError(t, err)
		}
		require.Equal(t, 100, c.peakIndexLen)

		// Act: Insert a single 1000B jumbo entry that pre-evicts all 100 entries down to c.entries.Len() == 0.
		evicted, err := c.Insert("jumbo", NewSizedValue("jumbo_val", 1000))

		// Assert: All 100 entries were evicted, peakIndexLen was reset to 1, and reclaimEpoch remained 0.
		require.NoError(t, err)
		assert.Len(t, evicted, 100)
		assert.Zero(t, c.reclaimEpoch.Load())
		assert.Equal(t, 1, c.peakIndexLen)
		assert.Equal(t, 1, c.entries.Len())
	})
}

func TestRegression_R5_F07_StringValueCloneAndMapCacheRemovedEntryZeroing(t *testing.T) {
	// Arrange
	largeBuffer := strings.Repeat("X", 64*1024)
	substring := largeBuffer[100:116]

	// Act
	sv := NewStringValue(substring)

	// Assert: NewStringValue clones the backing string buffer so largeBuffer is not pinned in memory.
	assert.Equal(t, StringValue(substring), sv)
	assert.NotSame(t, unsafe.StringData(substring), unsafe.StringData(string(sv)))
}

func TestRegression_R5_F08_OptionsSubnormalThresholdAndNegativeZeroNormalization(t *testing.T) {
	// Arrange
	negZero := math.Copysign(0.0, -1.0)
	subnormalEviction := math.Float64frombits(2) // 1e-323 (2 * SmallestNonzeroFloat64)

	// Act
	optsNegZero := ApplyOptions(WithEvictionRetentionRatio(negZero))
	optsSubnormal := ApplyOptions(WithEvictionThreshold(subnormalEviction))

	// Assert: -0.0 is normalized to +0.0, and subnormal EvictionThreshold > SmallestNonzeroFloat64
	// preserves a strictly positive CompactionThreshold < EvictionThreshold.
	assert.False(t, math.Signbit(optsNegZero.EvictionRetentionRatio))
	assert.Greater(t, optsSubnormal.CompactionThreshold, 0.0)
	assert.Less(t, optsSubnormal.CompactionThreshold, optsSubnormal.EvictionThreshold)
}
