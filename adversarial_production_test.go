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
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
	"weak"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realFNV1aCollisionKeyA and realFNV1aCollisionKeyB are two distinct 12-byte keys
// that produce the exact same 64-bit FNV-1a hash (12191099477593943617 / 0xa92d176f78f8c4c1).
// Because 64-bit FNV-1a is an iterative byte-by-byte state machine, appending any
// identical suffix S to both keys also produces a 64-bit FNV-1a collision:
// hashString(realFNV1aCollisionKeyA + S) == hashString(realFNV1aCollisionKeyB + S).
const (
	realFNV1aCollisionKeyA = "!!!!!!!!!!!!"
	realFNV1aCollisionKeyB = "&+!o9)1!=\x1c\xd2\x10"
)

// ============================================================================
// Suite 1.1: Cross-Backend Differential & Edge-Case Verification
// ============================================================================

func runBinaryKeysDifferentialTest(t *testing.T) {
	t.Helper()
	type evictEvent struct {
		key    string
		val    int
		reason EvictionReason
	}

	var mapEvents, radixEvents, arenaEvents []evictEvent
	var pressureBits atomic.Uint64
	setPressure := func(p float64) {
		pressureBits.Store(math.Float64bits(p))
	}
	setPressure(0.10)

	weighFn := func(_ string, v int) uint64 {
		if v < 0 {
			return 0
		}
		return uint64(v)
	}

	makeOpts := func(backend Backend, dest *[]evictEvent) []Option {
		return []Option{
			WithBackend(backend),
			WithInvariantChecking(true),
			WithWeigher(weighFn),
			WithPressureFunc(func() float64 {
				return math.Float64frombits(pressureBits.Load())
			}),
			WithCompactionThreshold(0.75),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithOnEvictEntry(func(k string, v int, r EvictionReason) {
				*dest = append(*dest, evictEvent{key: k, val: v, reason: r})
			}),
		}
	}

	const maxCap = uint64(600)
	mapC := New[int](maxCap, makeOpts(BackendMap, &mapEvents)...).(PressureAwareCache[int])
	radixC := New[int](maxCap, makeOpts(BackendRadix, &radixEvents)...).(PressureAwareCache[int])
	arenaC := New[int](maxCap, makeOpts(BackendArenaRadix, &arenaEvents)...).(PressureAwareCache[int])

	assertParity := func(step string) {
		t.Helper()
		var mapKV, radixKV, arenaKV []string
		for k, v := range mapC.All() {
			mapKV = append(mapKV, fmt.Sprintf("%x=%d", k, v))
		}
		for k, v := range radixC.All() {
			radixKV = append(radixKV, fmt.Sprintf("%x=%d", k, v))
		}
		for k, v := range arenaC.All() {
			arenaKV = append(arenaKV, fmt.Sprintf("%x=%d", k, v))
		}
		require.Equal(t, mapKV, radixKV, "Map vs Radix All() mismatch at %s", step)
		require.Equal(t, mapKV, arenaKV, "Map vs Arena All() mismatch at %s", step)

		ms := mapC.Stats()
		rs := radixC.Stats()
		as := arenaC.Stats()
		require.Equal(t, ms.Len, rs.Len, "Len mismatch at %s", step)
		require.Equal(t, ms.Len, as.Len, "Len mismatch at %s", step)
		require.Equal(t, ms.CurrentSize, rs.CurrentSize, "CurrentSize mismatch at %s", step)
		require.Equal(t, ms.CurrentSize, as.CurrentSize, "CurrentSize mismatch at %s", step)
		require.Equal(t, ms.ZeroSizeCount, rs.ZeroSizeCount, "ZeroSizeCount mismatch at %s", step)
		require.Equal(t, ms.ZeroSizeCount, as.ZeroSizeCount, "ZeroSizeCount mismatch at %s", step)
		require.Equal(t, ms.GetHits, rs.GetHits)
		require.Equal(t, ms.GetHits, as.GetHits)
		require.Equal(t, ms.GetMisses, rs.GetMisses)
		require.Equal(t, ms.GetMisses, as.GetMisses)
		require.Equal(t, ms.PeekHits, rs.PeekHits)
		require.Equal(t, ms.PeekHits, as.PeekHits)
		require.Equal(t, ms.PeekMisses, rs.PeekMisses)
		require.Equal(t, ms.PeekMisses, as.PeekMisses)
		require.Equal(t, ms.EvictionsCapacity, rs.EvictionsCapacity)
		require.Equal(t, ms.EvictionsCapacity, as.EvictionsCapacity)
		require.Equal(t, ms.EvictionsPressure, rs.EvictionsPressure)
		require.Equal(t, ms.EvictionsPressure, as.EvictionsPressure)
		require.Equal(t, ms.EvictionsDeleted, rs.EvictionsDeleted)
		require.Equal(t, ms.EvictionsDeleted, as.EvictionsDeleted)
		require.Equal(t, ms.EvictionsReplaced, rs.EvictionsReplaced)
		require.Equal(t, ms.EvictionsReplaced, as.EvictionsReplaced)
		require.ElementsMatch(t, mapEvents, radixEvents, "Map vs Radix eviction multiset mismatch at %s", step)
		require.ElementsMatch(t, mapEvents, arenaEvents, "Map vs Arena eviction multiset mismatch at %s", step)
	}

	putAll := func(k string, v int) {
		evM, errM := mapC.Put(k, v)
		evR, errR := radixC.Put(k, v)
		evA, errA := arenaC.Put(k, v)
		require.Equal(t, errM, errR)
		require.Equal(t, errM, errA)
		require.Equal(t, evM, evR)
		require.Equal(t, evM, evA)
	}

	// 1. Insert empty key, interior null bytes, invalid UTF-8, and all 256 single-byte keys.
	specialKeys := []string{
		"",
		"\x00",
		"\x00\x00",
		"a\x00b\xffc",
		"\xfe\xff\x80\x81",
		"\xff\x00\xff",
	}
	for i, k := range specialKeys {
		putAll(k, (i%5)+1)
	}
	for b := range 256 {
		putAll(string([]byte{byte(b)}), (b%4)+1)
	}
	assertParity("after-256-single-byte-keys")

	// 2. Insert an 80-level deep prefix chain (exceeding the [64] stack buffer in reconstructKey/hashNodeKey/freeSubtree).
	for depth := 1; depth <= 80; depth++ {
		k := strings.Repeat("x", depth)
		val := (depth % 7) + 1
		if depth%10 == 0 {
			val = -1 // weight 0
		}
		putAll(k, val)
	}
	assertParity("after-depth-80-chain")

	// 3. Interleaved Get, Peek, Replace (grow, shrink, zero-weight, !canFit self-evict, >maxSize self-evict).
	probeKeys := []string{"", "\x00", "a\x00b\xffc", strings.Repeat("x", 65), strings.Repeat("x", 80), "missing_key"}
	for _, k := range probeKeys {
		vM, okM := mapC.Get(k)
		vR, okR := radixC.Get(k)
		vA, okA := arenaC.Get(k)
		require.Equal(t, okM, okR)
		require.Equal(t, okM, okA)
		require.Equal(t, vM, vR)
		require.Equal(t, vM, vA)

		pM, pokM := mapC.Peek(k)
		pR, pokR := radixC.Peek(k)
		pA, pokA := arenaC.Peek(k)
		require.Equal(t, pokM, pokR)
		require.Equal(t, pokM, pokA)
		require.Equal(t, pM, pR)
		require.Equal(t, pM, pA)
	}
	assertParity("after-get-peek-probes")

	k65 := strings.Repeat("x", 65)
	putAll(k65, 5)
	// Replace grow, shrink, zero-weight, and >maxSize self-eviction.
	for _, newVal := range []int{12, 3, -1, int(maxCap) + 50} {
		errM := mapC.Replace(k65, newVal)
		errR := radixC.Replace(k65, newVal)
		errA := arenaC.Replace(k65, newVal)
		require.Equal(t, errM, errR)
		require.Equal(t, errM, errA)
		assertParity(fmt.Sprintf("after-replace-%d", newVal))
	}

	// 4. DeletePrefix at depth 65, mid-edge split prefix, and non-matching prefix.
	for _, pfx := range []string{strings.Repeat("x", 65), "a\x00", "nonexistent_prefix_"} {
		mapC.DeletePrefix(pfx)
		radixC.DeletePrefix(pfx)
		arenaC.DeletePrefix(pfx)
		assertParity("after-delete-prefix-" + fmt.Sprintf("%x", pfx))
	}

	// 5. Tier 1 Compaction and Tier 2 Pressure Shedding.
	setPressure(0.80)
	require.Empty(t, mapC.EvaluateMemoryPressure())
	require.Empty(t, radixC.EvaluateMemoryPressure())
	require.Empty(t, arenaC.EvaluateMemoryPressure())
	assertParity("after-tier1-pressure")

	setPressure(0.95)
	evM := mapC.EvaluateMemoryPressure()
	evR := radixC.EvaluateMemoryPressure()
	evA := arenaC.EvaluateMemoryPressure()
	require.Equal(t, evM, evR)
	require.Equal(t, evM, evA)
	assertParity("after-tier2-pressure")

	// 6. Explicit Compact and full clear via DeletePrefix("").
	mapC.Compact()
	radixC.Compact()
	arenaC.Compact()
	assertParity("after-explicit-compact")

	mapC.DeletePrefix("")
	radixC.DeletePrefix("")
	arenaC.DeletePrefix("")
	assertParity("after-delete-prefix-empty")
}

func TestAdversarial_CrossBackendDifferentialAndEdgeCases(t *testing.T) {
	t.Run("BinaryKeys_HighBit_NullBytes_UTF8_And_SingleByteChain_Depth80", runBinaryKeysDifferentialTest)

	t.Run("ZeroWeightStorm_MixedWithWeightedEntries_And_PressureShedding", func(t *testing.T) {
		// Arrange
		var pressureBits atomic.Uint64
		setPressure := func(p float64) {
			pressureBits.Store(math.Float64bits(p))
		}
		setPressure(0.10)

		weighFn := func(_ string, v int) uint64 {
			if v <= 0 {
				return 0
			}
			return uint64(v)
		}

		makeCache := func(b Backend) PressureAwareCache[int] {
			return New[int](500,
				WithBackend(b),
				WithInvariantChecking(true),
				WithWeigher(weighFn),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
				WithCompactionThreshold(0.75),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			).(PressureAwareCache[int])
		}

		mapC := makeCache(BackendMap)
		radixC := makeCache(BackendRadix)
		arenaC := makeCache(BackendArenaRadix)

		// Act: Insert 200 zero-weight entries interleaved with 50 weighted entries (weight=10, total=500).
		for i := range 250 {
			var k string
			var v int
			if i%5 == 0 {
				k = fmt.Sprintf("weighted/g%02d/k%03d", i%10, i)
				v = 10
			} else {
				k = fmt.Sprintf("zero/g%02d/k%03d", i%10, i)
				v = 0
			}
			_, errM := mapC.Put(k, v)
			_, errR := radixC.Put(k, v)
			_, errA := arenaC.Put(k, v)
			require.NoError(t, errM)
			require.NoError(t, errR)
			require.NoError(t, errA)
		}

		require.Equal(t, 250, mapC.Stats().Len)
		require.Equal(t, 200, mapC.Stats().ZeroSizeCount)
		require.Equal(t, uint64(500), mapC.Stats().CurrentSize)
		require.Equal(t, mapC.Stats().ZeroSizeCount, radixC.Stats().ZeroSizeCount)
		require.Equal(t, mapC.Stats().ZeroSizeCount, arenaC.Stats().ZeroSizeCount)

		// Transition weights via Replace: 0 -> 10, 10 -> 0, and >maxSize self-eviction.
		for _, c := range []PressureAwareCache[int]{mapC, radixC, arenaC} {
			require.NoError(t, c.Replace("weighted/g00/k000", 0))
			require.NoError(t, c.Replace("zero/g01/k001", 10))
			require.NoError(t, c.Replace("weighted/g05/k005", 600)) // self-evicts
		}
		require.Equal(t, mapC.Stats().ZeroSizeCount, radixC.Stats().ZeroSizeCount)
		require.Equal(t, mapC.Stats().ZeroSizeCount, arenaC.Stats().ZeroSizeCount)
		require.Equal(t, mapC.Stats().CurrentSize, radixC.Stats().CurrentSize)
		require.Equal(t, mapC.Stats().CurrentSize, arenaC.Stats().CurrentSize)

		// Trigger Tier 2 pressure shedding (50% retention).
		setPressure(0.95)
		evM := mapC.EvaluateMemoryPressure()
		evR := radixC.EvaluateMemoryPressure()
		evA := arenaC.EvaluateMemoryPressure()

		// Assert
		require.Equal(t, evM, evR)
		require.Equal(t, evM, evA)
		require.LessOrEqual(t, mapC.Stats().CurrentSize, uint64(250))
		require.LessOrEqual(t, mapC.Stats().ZeroSizeCount, 100)
		require.Equal(t, mapC.Stats().ZeroSizeCount, arenaC.Stats().ZeroSizeCount)
	})

	t.Run("DeletePrefixEmpty_vs_NonEmpty_PressureSamplingAndCallbackOrderCharacterization", func(t *testing.T) {
		// Arrange
		recordOrder := func(b Backend, prefix string) ([]string, Stats) {
			var evictedKeys []string
			pressure := 0.10
			c := New[int](100,
				WithBackend(b),
				WithPressureFunc(func() float64 { return pressure }),
				WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
					if r == EvictionReasonDeleted {
						evictedKeys = append(evictedKeys, k)
					}
				}),
			)
			// Insert in order p/c, p/a, p/b so MRU-to-LRU is [p/b, p/a, p/c]
			// while lexicographical DFS order is [p/a, p/b, p/c].
			_, _ = c.Put("p/c", 1)
			_, _ = c.Put("p/a", 2)
			_, _ = c.Put("p/b", 3)

			pressure = 0.85
			c.DeletePrefix(prefix)
			return evictedKeys, c.Stats()
		}

		// Act: When prefix == "", all 3 backends iterate head -> tail (MRU-to-LRU: p/b, p/a, p/c).
		mapEmptyKeys, mapEmptyStats := recordOrder(BackendMap, "")
		radixEmptyKeys, radixEmptyStats := recordOrder(BackendRadix, "")
		arenaEmptyKeys, arenaEmptyStats := recordOrder(BackendArenaRadix, "")

		// Assert
		assert.Equal(t, []string{"p/b", "p/a", "p/c"}, mapEmptyKeys)
		assert.Equal(t, []string{"p/b", "p/a", "p/c"}, arenaEmptyKeys)
		assert.Equal(t, []string{"p/b", "p/a", "p/c"}, radixEmptyKeys)

		// DeletePrefix("") samples PressureFunc via lockWithPressure, updating MemoryPressure to 0.85.
		assert.InDelta(t, 0.85, mapEmptyStats.MemoryPressure, 1e-9)
		assert.InDelta(t, 0.85, radixEmptyStats.MemoryPressure, 1e-9)
		assert.InDelta(t, 0.85, arenaEmptyStats.MemoryPressure, 1e-9)

		// When prefix == "p/":
		// RadixCache and ArenaRadixCache both traverse in lexicographical DFS order (p/a, p/b, p/c),
		// and all 3 backends sample PressureFunc (updating MemoryPressure to 0.85).
		_, mapPfxStats := recordOrder(BackendMap, "p/")
		radixPfxKeys, radixPfxStats := recordOrder(BackendRadix, "p/")
		arenaPfxKeys, arenaPfxStats := recordOrder(BackendArenaRadix, "p/")

		assert.Equal(t, []string{"p/a", "p/b", "p/c"}, radixPfxKeys)
		assert.Equal(t, []string{"p/a", "p/b", "p/c"}, arenaPfxKeys)
		assert.InDelta(t, 0.85, mapPfxStats.MemoryPressure, 1e-9)
		assert.InDelta(t, 0.85, radixPfxStats.MemoryPressure, 1e-9)
		assert.InDelta(t, 0.85, arenaPfxStats.MemoryPressure, 1e-9)
	})

	t.Run("ArenaNodeSlack_vs_EntrySlack_CompactionDivergenceCharacterization", func(t *testing.T) {
		// Arrange: Construct a workload where intermediate routing nodes are split and freed.
		// Under Tier 1 pressure (0.80), ArenaRadixCache.shouldAutoCompactLocked checks
		// node-level free-list slack (freeCount*4 >= len(nodes)), whereas MapCache and
		// RadixCache check entry-level shrinkage ((peakEntryLen-len)*4 >= peakEntryLen).
		pressure := 0.10
		makeOpts := func(b Backend) []Option {
			return []Option{
				WithBackend(b),
				WithPressureFunc(func() float64 { return pressure }),
				WithCompactionThreshold(0.75),
				WithEvictionThreshold(0.90),
			}
		}
		mapC := New[int](1000, makeOpts(BackendMap)...)
		radixC := New[int](1000, makeOpts(BackendRadix)...)
		arenaC := New[int](1000, makeOpts(BackendArenaRadix)...)

		// Populate 32 entries: 16 pairs of sibling keys under distinct prefixes
		// (32 leaf nodes + 16 routing nodes + 1 root = 49 arena nodes).
		for i := range 16 {
			k1 := fmt.Sprintf("branch_%03d/leaf_a", i)
			k2 := fmt.Sprintf("branch_%03d/leaf_b", i)
			for _, c := range []Cache[int]{mapC, radixC, arenaC} {
				_, _ = c.Put(k1, i)
				_, _ = c.Put(k2, i)
			}
		}

		// Act: Raise pressure to Tier 1 (0.80) and delete 1 leaf from 7 branches.
		// Entry shrinkage is only 7/32 = 21.875% (< 25%), so MapCache and RadixCache do NOT compact.
		// However, each deleted leaf in ArenaRadixCache also collapses its single-child routing node
		// via compressPathUpwards, freeing 2 arena nodes per delete (14 freed nodes / 49 total >= 25%)!
		pressure = 0.80
		for i := range 7 {
			k1 := fmt.Sprintf("branch_%03d/leaf_a", i)
			for _, c := range []Cache[int]{mapC, radixC, arenaC} {
				_, _ = c.Delete(k1)
			}
		}

		// Assert
		assert.Equal(t, uint64(0), mapC.Stats().CompactionsPressureTier1)
		assert.Equal(t, uint64(0), radixC.Stats().CompactionsPressureTier1)
		assert.Equal(t, uint64(1), arenaC.Stats().CompactionsPressureTier1)

		// Despite compaction counter divergence, key-value and LRU ordering remain 100% identical.
		require.Equal(t, slices.Collect(mapC.Keys()), slices.Collect(radixC.Keys()))
		require.Equal(t, slices.Collect(mapC.Keys()), slices.Collect(arenaC.Keys()))
	})
}

// ============================================================================
// Suite 1.2: ArenaRadix Deep Tree (>64) + Real 64-Bit FNV-1a Collisions + Compaction
// ============================================================================

func TestAdversarial_ArenaRadix_DeepTreeHashCollisionsAndCompaction(t *testing.T) {
	// Arrange: Verify that our two 12-byte keys genuinely collide in 64-bit FNV-1a.
	require.NotEqual(t, realFNV1aCollisionKeyA, realFNV1aCollisionKeyB)
	require.Equal(t, hashString(realFNV1aCollisionKeyA), hashString(realFNV1aCollisionKeyB),
		"realFNV1aCollisionKeyA and realFNV1aCollisionKeyB must have identical 64-bit FNV-1a hashes")

	var pressureBits atomic.Uint64
	pressureBits.Store(math.Float64bits(0.10))

	var evictedKeys []string
	c := NewArenaRadixCache[int](500,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			return math.Float64frombits(pressureBits.Load())
		}),
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			evictedKeys = append(evictedKeys, k)
		}),
	).(*arenaRadix[int])

	// Build a 75-level deep tree under both colliding root prefixes so that:
	// 1. Tree depth exceeds the 64-entry stack buffer in reconstructKey, hashNodeKey, and freeSubtree.
	// 2. Every pair (keyA_depth, keyB_depth) at depth d shares the EXACT SAME 64-bit FNV-1a hash!
	var keysA, keysB []string
	var sbA, sbB strings.Builder
	sbA.WriteString(realFNV1aCollisionKeyA)
	sbB.WriteString(realFNV1aCollisionKeyB)

	// Act
	for depth := 1; depth <= 75; depth++ {
		seg := fmt.Sprintf("/%02d", depth)
		sbA.WriteString(seg)
		sbB.WriteString(seg)
		kA := sbA.String()
		kB := sbB.String()
		require.Equal(t, hashString(kA), hashString(kB), "64-bit FNV-1a collision must hold at depth %d", depth)

		keysA = append(keysA, kA)
		keysB = append(keysB, kB)

		_, err := c.Put(kA, depth*10+1)
		require.NoError(t, err)
		_, err = c.Put(kB, depth*10+2)
		require.NoError(t, err)
	}

	require.Equal(t, 150, c.Stats().Len)
	// Because every (kA, kB) pair collides on 64-bit FNV-1a, nodeMap has only 75 entries while c.len == 150!
	require.Len(t, c.nodeMap, 75)

	// Exercise Peek and Get across all 150 colliding deep keys (forcing verifyKey upward walks and trie fallbacks).
	for i := range 75 {
		vA, okA := c.Peek(keysA[i])
		require.True(t, okA)
		require.Equal(t, (i+1)*10+1, vA)

		vB, okB := c.Get(keysB[i])
		require.True(t, okB)
		require.Equal(t, (i+1)*10+2, vB)

		// Replace keysA[i] (which was displaced in nodeMap by keysB[i]).
		require.NoError(t, c.Replace(keysA[i], (i+1)*100+1))
	}
	require.Positive(t, c.Stats().ArenaHashFallbacks)

	// Delete a subtree at depth 66 (exercising freeSubtree stack spill > 64 with colliding hashes).
	c.DeletePrefix(keysA[65])
	require.Equal(t, 140, c.Stats().Len)
	require.NotEmpty(t, evictedKeys)

	// Trigger Tier 1 and Tier 2 EvaluateMemoryPressure and explicit Compact() (exercising hashNodeKey at depth > 64).
	pressureBits.Store(math.Float64bits(0.80))
	_ = c.EvaluateMemoryPressure()

	c.Compact()

	// Assert
	st := c.Stats()
	assert.Zero(t, st.ArenaFreeNodes, "ArenaFreeNodes must be 0 immediately after Compact()")
	assert.Zero(t, st.ArenaUnallocatedCap, "ArenaUnallocatedCap must be 0 immediately after Compact()")

	// Verify all remaining 65 keysA and 75 keysB are retrievable and match All() order.
	for i := range 65 {
		vA, okA := c.Get(keysA[i])
		require.True(t, okA)
		require.Equal(t, (i+1)*100+1, vA)
	}
	for i := range 75 {
		vB, okB := c.Peek(keysB[i])
		require.True(t, okB)
		require.Equal(t, (i+1)*10+2, vB)
	}
}

// ============================================================================
// Suite 1.3: High-Contention Concurrency Torture & Conservation Laws
// ============================================================================

func startHighContentionReadWriteWorkers(wg *sync.WaitGroup, c PressureAwareCache[int], keys []string, iters int) {
	// 8 Zipfian hot-key writers (Put + Replace)
	for w := range 8 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(100+w), 1))
			zipf := rand.NewZipf(r, 1.15, 1.0, uint64(len(keys)-1))
			for i := range iters {
				k := keys[zipf.Uint64()]
				weight := i % 5 // includes 0-weight when i%5 == 0
				if i%3 == 0 {
					_ = c.Replace(k, weight)
				} else {
					_, _ = c.Put(k, weight)
				}
			}
		})
	}

	// 8 Get readers (write-lock path)
	for rID := range 8 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(200+rID), 2))
			for range iters {
				_, _ = c.Get(keys[r.IntN(len(keys))])
			}
		})
	}

	// 8 Peek + Stats + early-break iterator readers (read-lock path)
	for pID := range 8 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(300+pID), 3))
			for i := range iters {
				_, _ = c.Peek(keys[r.IntN(len(keys))])
				if i%15 == 0 {
					_ = c.Stats()
				}
				if i%25 == 0 {
					sampleFirstThreeEntries(c)
				}
				if i%35 == 0 {
					sampleFirstThreeValues(c)
				}
			}
		})
	}
}

func sampleFirstThreeEntries(c Cache[int]) {
	count := 0
	for range c.All() {
		count++
		if count >= 3 {
			break
		}
	}
}

func sampleFirstThreeValues(c Cache[int]) {
	count := 0
	for range c.Values() {
		count++
		if count >= 3 {
			break
		}
	}
}

func runHighContentionTortureWorkers(c PressureAwareCache[int], keys []string, pressureBits *atomic.Uint64) {
	const iters = 300
	var wg sync.WaitGroup

	startHighContentionReadWriteWorkers(&wg, c, keys, iters)

	// 4 Delete + DeletePrefix purgers
	for dID := range 4 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(400+dID), 4))
			for i := range iters {
				switch {
				case i%50 == 49:
					c.DeletePrefix("tenantA/svc1/cold_")
				case i%120 == 119:
					c.DeletePrefix("")
				default:
					_, _ = c.Delete(keys[r.IntN(len(keys))])
				}
			}
		})
	}

	// 4 Background pressure & compaction controllers
	for bgID := range 4 {
		wg.Go(func() {
			levels := []float64{0.20, 0.80, 0.95}
			for i := range iters / 2 {
				p := levels[(bgID+i)%len(levels)]
				pressureBits.Store(math.Float64bits(p))
				if i%5 == 0 {
					_ = c.EvaluateMemoryPressure()
				}
				if i%7 == 0 {
					c.Compact()
				}
			}
		})
	}

	wg.Wait()
}

func recordEvictionReasonCounter(r EvictionReason, capCnt, pressCnt, delCnt, repCnt *atomic.Uint64) {
	switch r {
	case EvictionReasonCapacity:
		capCnt.Add(1)
	case EvictionReasonPressure:
		pressCnt.Add(1)
	case EvictionReasonDeleted:
		delCnt.Add(1)
	case EvictionReasonReplaced:
		repCnt.Add(1)
	}
}

func TestAdversarial_HighContentionConcurrencyTorture(t *testing.T) {
	backends := []struct {
		name    string
		backend Backend
	}{
		{name: "MapCache", backend: BackendMap},
		{name: "RadixCache", backend: BackendRadix},
		{name: "ArenaRadixCache", backend: BackendArenaRadix},
	}

	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			// Arrange
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.20))

			var cbValCap, cbValPress, cbValDel, cbValRep atomic.Uint64
			var cbEntCap, cbEntPress, cbEntDel, cbEntRep atomic.Uint64

			c := New[int](120,
				WithBackend(b.backend),
				WithInvariantChecking(false), // keep lock hold times fast under 32-goroutine race stress; verified post-quiescence
				WithWeigher(func(_ string, v int) uint64 {
					if v <= 0 {
						return 0
					}
					return uint64(v)
				}),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
				WithCompactionThreshold(0.75),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithOnEvictValue(func(_ int, r EvictionReason) {
					recordEvictionReasonCounter(r, &cbValCap, &cbValPress, &cbValDel, &cbValRep)
				}),
				WithOnEvictEntry(func(_ string, _ int, r EvictionReason) {
					recordEvictionReasonCounter(r, &cbEntCap, &cbEntPress, &cbEntDel, &cbEntRep)
				}),
			).(PressureAwareCache[int])

			// Build key pool: 16 hot keys + 128 cold keys under hierarchical prefixes.
			keys := make([]string, 144)
			for i := range 16 {
				keys[i] = fmt.Sprintf("tenantA/svc1/hot_%02d", i)
			}
			for i := 16; i < 80; i++ {
				keys[i] = fmt.Sprintf("tenantA/svc1/cold_%03d", i)
			}
			for i := 80; i < 144; i++ {
				keys[i] = fmt.Sprintf("tenantB/svc2/cold_%03d", i)
			}

			// Act
			runHighContentionTortureWorkers(c, keys, &pressureBits)

			// Assert: Post-quiescence Conservation Law Assertions
			st := c.Stats()
			totalRemoved := st.EvictionsCapacity + st.EvictionsPressure + st.EvictionsDeleted
			assert.Equal(t, st.PutInserted, uint64(st.Len)+totalRemoved,
				"Conservation law violated: PutInserted (%d) != Len (%d) + Evictions[Cap+Press+Del] (%d)",
				st.PutInserted, st.Len, totalRemoved)

			assert.Equal(t, st.EvictionsCapacity, cbValCap.Load())
			assert.Equal(t, st.EvictionsPressure, cbValPress.Load())
			assert.Equal(t, st.EvictionsDeleted, cbValDel.Load())
			assert.Equal(t, st.EvictionsReplaced, cbValRep.Load())

			assert.Equal(t, st.EvictionsCapacity, cbEntCap.Load())
			assert.Equal(t, st.EvictionsPressure, cbEntPress.Load())
			assert.Equal(t, st.EvictionsDeleted, cbEntDel.Load())
			assert.Equal(t, st.EvictionsReplaced, cbEntRep.Load())

			var liveWeightSum uint64
			var liveCount, liveZeroCount int
			for _, v := range c.All() {
				liveCount++
				if v <= 0 {
					liveZeroCount++
				} else {
					liveWeightSum += uint64(v)
				}
			}
			assert.Equal(t, st.Len, liveCount)
			assert.Equal(t, st.ZeroSizeCount, liveZeroCount)
			assert.Equal(t, st.CurrentSize, liveWeightSum)
		})
	}
}

// ============================================================================
// Suite 1.4: Cache-Line Layout, Struct Alignment & GC Scan Type Verification
// ============================================================================

func TestAdversarial_CacheLineLayoutAndStructAlignment(t *testing.T) {
	// Arrange
	const cacheLineSize = uintptr(64)

	mc := NewMapCache[uint64](100).(*mapCache[uint64])
	rc := NewRadixCache[uint64](100).(*radixCache[uint64])
	ac := NewArenaRadixCache[uint64](100).(*arenaRadix[uint64])
	require.NotNil(t, mc)
	require.NotNil(t, rc)
	require.NotNil(t, ac)

	// Act & Assert 1: Verify ArenaRadixCache separates c.nodes from c.mu across 64-byte cache lines.
	acNodesEnd := unsafe.Offsetof(ac.nodes) + unsafe.Sizeof(ac.nodes)
	acMuOffset := unsafe.Offsetof(ac.mu)
	assert.GreaterOrEqual(t, acMuOffset-acNodesEnd, cacheLineSize,
		"arenaRadix.mu must be separated from arenaRadix.nodes by at least 64 bytes")

	// Act & Assert 2: Characterize pressureState atomic counter packing:
	// peekHits, peekMisses, and putRejectedOversized are packed contiguously (8 bytes apart)
	// in the same 64-byte cache line, and arenaHashFallbacks shares a 64-byte cache line with pressureWriteMu.
	peekHitsOff := unsafe.Offsetof(mc.peekHits)
	peekMissesOff := unsafe.Offsetof(mc.peekMisses)
	rejectedOff := unsafe.Offsetof(mc.putRejectedOversized)
	fallbacksOff := unsafe.Offsetof(mc.arenaHashFallbacks)
	pressureMuOff := unsafe.Offsetof(mc.pressureWriteMu)
	assert.Equal(t, uintptr(8), peekMissesOff-peekHitsOff,
		"peekHits and peekMisses are adjacent 8-byte fields sharing a cache line")
	assert.Equal(t, peekHitsOff/cacheLineSize, peekMissesOff/cacheLineSize,
		"peekHits and peekMisses sit in the same 64-byte cache line")
	assert.Equal(t, peekHitsOff/cacheLineSize, rejectedOff/cacheLineSize,
		"peekHits and putRejectedOversized sit in the same 64-byte cache line")
	assert.Equal(t, fallbacksOff/cacheLineSize, pressureMuOff/cacheLineSize,
		"arenaHashFallbacks and pressureWriteMu sit in the same 64-byte cache line")

	t.Logf("Layout offsets (bytes): mapCache{index=%d, mu=%d, peekHits=%d, peekMisses=%d, pressureWriteMu=%d} | radixCache{root=%d, mu=%d} | arenaRadix{nodes=%d, mu=%d}",
		unsafe.Offsetof(mc.index), unsafe.Offsetof(mc.mu), peekHitsOff, peekMissesOff, pressureMuOff,
		unsafe.Offsetof(rc.root), unsafe.Offsetof(rc.mu),
		unsafe.Offsetof(ac.nodes), acMuOffset)

	// Act & Assert 3: Verify H-04 (HIGH-1): arenaRadixNode[uint64] embeds `prefix string` at offset 0,
	// meaning even for pointer-free scalar V = uint64, the node struct contains a Go pointer
	// (`string` header {*byte, int}) and cannot be placed in a `noscan` span by the Go GC.
	var node arenaRadixNode[uint64]
	assert.Equal(t, uintptr(0), unsafe.Offsetof(node.prefix))
	assert.Equal(t, uintptr(16), unsafe.Sizeof(node.prefix))
	assert.Equal(t, uintptr(56), unsafe.Sizeof(node))
	nodeType := reflect.TypeOf(node)
	assert.Equal(t, reflect.String, nodeType.Field(0).Type.Kind())
}

// ============================================================================
// Suite 1.5: Allocation Characterization (Hot Paths, Path Compression, Deep Keys)
// ============================================================================

func TestAdversarial_AllocationCharacterization_PathCompressionAndDeepKeys(t *testing.T) {
	t.Run("ZeroAllocSteadyStateHotPaths", func(t *testing.T) {
		for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			// Arrange
			c := New[uint64](100, WithBackend(b))
			_, _ = c.Put("dir/sub/k1", 1)
			_, _ = c.Put("dir/sub/k2", 2)
			valSeq := c.Values()

			// Act & Assert
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				_, _ = c.Get("dir/sub/k1")
			}), "backend %v Get must be 0 allocs/op", b)
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				_, _ = c.Peek("dir/sub/k2")
			}), "backend %v Peek must be 0 allocs/op", b)
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				_, _ = c.Put("dir/sub/k1", 3)
			}), "backend %v in-place Put must be 0 allocs/op", b)
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				_ = c.Replace("dir/sub/k2", 4)
			}), "backend %v Replace must be 0 allocs/op", b)
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				_ = c.Stats()
			}), "backend %v Stats must be 0 allocs/op", b)
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				valSeq(func(v uint64) bool { return v > 0 })
			}), "backend %v Values must be 0 allocs/op", b)
		}
	})

	t.Run("RadixSplitAndCompressStringAllocationChurn", func(t *testing.T) {
		// Arrange: In RadixCache and ArenaRadixCache, inserting a sibling key that splits an edge
		// into multi-byte segments calls clonePrefix 3 times (3 heap allocs), and deleting
		// that key calls compressPathUpwards which executes `curr.prefix + onlyChild.prefix` (1 heap alloc).
		arenaC := NewArenaRadixCache[int](100)
		_, _ = arenaC.Put("service/users/profile/1001_alpha", 1)
		k2 := "service/users/profile/1002_beta"

		// Act
		allocs := testing.AllocsPerRun(100, func() {
			_, _ = arenaC.Put(k2, 2)
			_, _ = arenaC.Delete(k2)
		})

		// Assert
		assert.GreaterOrEqual(t, allocs, 4.0,
			"ArenaRadixCache split+compress cycle allocates >= 4 heap strings per Put+Delete")
	})

	t.Run("DeepTreeStackBufferSpillAboveDepth64", func(t *testing.T) {
		// Arrange
		arenaShallow := NewArenaRadixCache[int](200).(*arenaRadix[int])
		arenaDeep := NewArenaRadixCache[int](200).(*arenaRadix[int])

		_, _ = arenaShallow.Put("a/b/c/d", 1)
		var deepSB strings.Builder
		for i := 1; i <= 75; i++ {
			fmt.Fprintf(&deepSB, "/%02d", i)
			_, _ = arenaDeep.Put(deepSB.String(), i)
		}
		deepLeafKey := deepSB.String()
		deepLeafID := arenaDeep.nodeMap[hashString(deepLeafKey)]

		// Act
		shallowHashAllocs := testing.AllocsPerRun(100, func() {
			_ = arenaShallow.hashNodeKey(arenaShallow.head)
		})
		deepHashAllocs := testing.AllocsPerRun(100, func() {
			_ = arenaDeep.hashNodeKey(deepLeafID)
		})

		// Assert
		assert.Zero(t, shallowHashAllocs, "depth <= 64 hashNodeKey uses stackBuf [64]uint32 (0 allocs)")
		assert.GreaterOrEqual(t, deepHashAllocs, 1.0, "depth > 64 hashNodeKey spills stackBuf [64]uint32 to heap")
	})
}

// ============================================================================
// Suite 1.6: Empirical Defect Verification (C-01..C-03, H-01..H-06, M-01..M-04, L-04)
// ============================================================================

// TestDefect_C01_ZeroWeightUnboundedGrowth verifies C-01 / F-01 / HIGH-5 remediation:
// When Weigher returns 0, inserting new zero-weight entries into a cache at capacity or when
// zeroSizeCount >= maxSize evicts at most 1 LRU tail entry per insert so entry count remains
// bounded by maxSize without wiping out positive-weight tail entries.
func TestDefect_C01_ZeroWeightUnboundedGrowth(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange
			c := New[[]byte](10,
				WithBackend(b),
				WithInvariantChecking(true),
				WithWeigher(func(_ string, v []byte) uint64 { return uint64(len(v)) }),
				WithPressureFunc(func() float64 { return 0.0 }),
			)

			// Fill cache to 100% byte capacity (10 / 10 bytes).
			_, err := c.Put("full_capacity_key", make([]byte, 10))
			require.NoError(t, err)
			require.Equal(t, uint64(10), c.Stats().CurrentSize)

			// Act: Insert 5,000 zero-weight entries into a full cache (maxSize = 10).
			totalEvicted := 0
			for i := range 5000 {
				evicted, putErr := c.Put(fmt.Sprintf("zero_weight_key_%d", i), nil)
				require.NoError(t, putErr)
				totalEvicted += len(evicted)
			}

			// Assert
			st := c.Stats()
			assert.Equal(t, 10, st.Len)
			assert.Equal(t, 10, st.ZeroSizeCount)
			assert.Equal(t, 4991, totalEvicted)
			assert.Equal(t, uint64(4991), st.EvictionsCapacity)
		})

		t.Run(b.String()+"_SingleZeroWeightInsertDoesNotWipeWeightedTailEntries", func(t *testing.T) {
			// Arrange: Populate 3 weighted entries (size 1 each) at the LRU tail followed by
			// 5 zero-weight entries (zeroSizeCount == maxSize == 5).
			c := New[int](5,
				WithBackend(b),
				WithInvariantChecking(true),
				WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
				WithPressureFunc(func() float64 { return 0.0 }),
			)
			for i := range 3 {
				_, err := c.Put(fmt.Sprintf("weighted_%d", i), 1)
				require.NoError(t, err)
			}
			for i := range 5 {
				_, err := c.Put(fmt.Sprintf("zero_%d", i), 0)
				require.NoError(t, err)
			}
			require.Equal(t, 8, c.Stats().Len)
			require.Equal(t, 5, c.Stats().ZeroSizeCount)

			// Act: Insert 1 more zero-weight entry while weighted entries sit at the LRU tail.
			evicted, err := c.Put("zero_extra", 0)

			// Assert: At most 1 entry is evicted on this single insert, preserving the remaining weighted entries.
			require.NoError(t, err)
			assert.Len(t, evicted, 1)
			assert.Equal(t, 8, c.Stats().Len)
			assert.Equal(t, uint64(2), c.Stats().CurrentSize)
		})

		t.Run(b.String()+"_PositiveToZeroUpdateAndPumpingBounded", func(t *testing.T) {
			// Arrange
			c := New[int](10,
				WithBackend(b),
				WithInvariantChecking(true),
				WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
				WithPressureFunc(func() float64 { return 0.0 }),
			)

			// Act: Repeatedly insert a positive-weight entry and downgrade it to zero-weight via Put and Replace,
			// and alternate maxSize weighted inserts with zero-weight inserts.
			for i := range 500 {
				key := fmt.Sprintf("pump_%04d", i)
				_, err := c.Put(key, 1)
				require.NoError(t, err)
				if i%2 == 0 {
					_, err = c.Put(key, 0)
					require.NoError(t, err)
				} else {
					err = c.Replace(key, 0)
					require.NoError(t, err)
				}
			}
			for i := range 200 {
				_, err := c.Put(fmt.Sprintf("alt_pos_%04d", i), 10)
				require.NoError(t, err)
				for j := range 10 {
					_, err = c.Put(fmt.Sprintf("alt_zero_%04d_%02d", i, j), 0)
					require.NoError(t, err)
				}
			}

			// Assert: Entry count and zero-size count remain strictly bounded by 2*maxSize (and <= maxSize+1 here).
			st := c.Stats()
			assert.LessOrEqual(t, st.Len, 11)
			assert.LessOrEqual(t, st.ZeroSizeCount, 10)
		})
	}
}

// TestDefect_C02_OnEvictPanicStateCorruptionAndLockLeak verifies C-02 / F-02 / CRITICAL-1 remediation:
//  1. User OnEvict* callbacks are invoked after internal state transitions and c.mu.Unlock() complete,
//     so a recovered panic in OnEvict* never leaves the cache in a corrupted state.
//  2. WithInvariantChecking(true) runs checkInvariants() before OnEvict* and with defer c.mu.Unlock(),
//     never masking user callback panics or leaking c.mu.
func TestDefect_C02_OnEvictPanicStateCorruptionAndLockLeak(t *testing.T) {
	t.Run("MapCache_DeletePrefixEmpty_PreservesStateOnCallbackPanic", func(t *testing.T) {
		// Arrange
		panicNow := false
		c := NewMapCache[string](100,
			WithInvariantChecking(true),
			WithOnEvictValue(func(_ string, _ EvictionReason) {
				if panicNow {
					panic("simulated user callback panic")
				}
			}),
		)
		_, _ = c.Put("k1", "v1")
		_, _ = c.Put("k2", "v2")

		// Act
		panicNow = true
		require.PanicsWithValue(t, "simulated user callback panic", func() {
			c.DeletePrefix("")
		})
		panicNow = false

		// Assert
		st := c.Stats()
		keys := slices.Collect(c.Keys())
		assert.Equal(t, 0, st.Len)
		assert.Equal(t, uint64(0), st.CurrentSize)
		assert.Empty(t, keys)
	})

	t.Run("ArenaRadixCache_DeletePrefix_PreservesTreeAndSubsequentEvictionAfterCallbackPanic", func(t *testing.T) {
		// Arrange
		panicNow := false
		c := NewArenaRadixCache[int](20,
			WithInvariantChecking(true),
			WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
			WithOnEvictEntry(func(_ string, _ int, _ EvictionReason) {
				if panicNow {
					panic("simulated arena callback panic")
				}
			}),
		)
		_, _ = c.Put("app/db/1", 10)
		_, _ = c.Put("app/db/2", 10)

		// Act
		panicNow = true
		require.PanicsWithValue(t, "simulated arena callback panic", func() {
			c.DeletePrefix("app/db/")
		})
		panicNow = false

		// Assert
		require.NotPanics(t, func() {
			_, err := c.Put("new_entry", 15)
			require.NoError(t, err)
		})
		st := c.Stats()
		assert.Equal(t, 1, st.Len)
		assert.Equal(t, uint64(15), st.CurrentSize)
	})

	t.Run("AllBackends_PutOverwrite_PreservesCurrentSizeOnCallbackPanic", func(t *testing.T) {
		for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			// Arrange
			panicOnCap := false
			c := New[int](100,
				WithBackend(b),
				WithInvariantChecking(true),
				WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
				WithOnEvictValue(func(_ int, r EvictionReason) {
					if panicOnCap && r == EvictionReasonCapacity {
						panic("simulated capacity eviction panic")
					}
				}),
			)
			_, _ = c.Put("lru_victim", 50)
			_, _ = c.Put("updated_key", 40) // total = 90

			// Act
			panicOnCap = true
			require.PanicsWithValue(t, "simulated capacity eviction panic", func() {
				_, _ = c.Put("updated_key", 80)
			})
			panicOnCap = false

			// Assert
			st := c.Stats()
			assert.Equal(t, 1, st.Len)
			assert.Equal(t, uint64(80), st.CurrentSize)
			val, ok := c.Get("updated_key")
			assert.True(t, ok)
			assert.Equal(t, 80, val)
		}
	})

	t.Run("InvariantChecking_PreservesOriginalPanicAndReleasesMutexLock", func(t *testing.T) {
		// Arrange
		panicNow := false
		c := NewArenaRadixCache[string](100,
			WithInvariantChecking(true),
			WithOnEvictValue(func(_ string, _ EvictionReason) {
				if panicNow {
					panic("original user callback panic")
				}
			}),
		).(*arenaRadix[string])
		_, _ = c.Put("dir/a", "v1")
		_, _ = c.Put("dir/b", "v2")

		// Act
		panicNow = true
		var recovered any
		func() {
			defer func() {
				recovered = recover()
			}()
			c.DeletePrefix("dir/")
		}()
		panicNow = false

		// Assert
		require.Equal(t, "original user callback panic", recovered)

		done := make(chan struct{})
		go func() {
			c.mu.RLock()
			remaining := c.len
			c.mu.RUnlock()
			assert.Zero(t, remaining)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			require.Fail(t, "c.mu remained locked after callback panic")
		}
	})

	t.Run("EvictCallbackQueue_ZeroesBufAndOverflowOnCompletionAndMidBatchPanic", func(t *testing.T) {
		// Arrange
		var p pressureState
		var q evictCallbackQueue[*int]
		vals := [4]int{10, 20, 30, 40}
		for i := range vals {
			q.enqueue(fmt.Sprintf("key_%d", i), &vals[i], EvictionReasonDeleted)
		}
		require.Equal(t, 4, q.n)
		require.Len(t, q.overflow, 2)
		overflowBacking := q.overflow[:len(q.overflow)]

		// Act: Normal completion across inline buf and overflow slice.
		callIdx := 0
		q.invoke(&p, func(v *int, _ EvictionReason) {
			require.NotNil(t, v)
			if callIdx < len(q.buf) {
				assert.Equal(t, evictEvent[*int]{}, q.buf[callIdx], "inline buf slot must be zeroed before callback runs")
			} else {
				assert.Equal(t, evictEvent[*int]{}, overflowBacking[callIdx-len(q.buf)], "overflow slot must be zeroed before callback runs")
			}
			callIdx++
		}, nil)

		// Assert: Normal completion zeroes all inline and overflow slots.
		assert.Equal(t, 4, callIdx)
		assert.Zero(t, q.n)
		assert.Nil(t, q.overflow)
		assert.Equal(t, [2]evictEvent[*int]{}, q.buf)
		assert.Equal(t, []evictEvent[*int]{{}, {}}, overflowBacking)

		// Arrange: Re-populate queue and panic mid-batch on the first event.
		for i := range vals {
			q.enqueue(fmt.Sprintf("panic_key_%d", i), &vals[i], EvictionReasonDeleted)
		}
		overflowBackingPanic := q.overflow[:len(q.overflow)]

		// Act: Panic on the first callback while inline buf[1] and overflow[0..1] remain unconsumed.
		require.PanicsWithValue(t, "mid-batch callback panic", func() {
			q.invoke(&p, nil, func(_ string, _ *int, _ EvictionReason) {
				panic("mid-batch callback panic")
			})
		})

		// Assert: Deferred reset clears all remaining inline buf and overflow slots.
		assert.Zero(t, q.n)
		assert.Nil(t, q.overflow)
		assert.Equal(t, [2]evictEvent[*int]{}, q.buf)
		assert.Equal(t, []evictEvent[*int]{{}, {}}, overflowBackingPanic)
	})
}

// TestDefect_C03_ReentrancyHazards verifies C-03 / F-03 / CRITICAL-2 remediation:
//  1. OnEvict* callbacks execute after c.mu.Unlock(), allowing re-entrant read and write calls
//     on the same cache instance without deadlocking.
//  2. PressureFunc re-entrancy detection inspects the parent creator GID and creator frame of child goroutines,
//     preventing recursive PressureFunc execution when a child goroutine touches the cache while allowing
//     pre-existing sibling worker goroutines spawned before PressureFunc to sample normally.
func TestDefect_C03_ReentrancyHazards(t *testing.T) {
	t.Run("OnEvictCallbackCallingCacheMethodsSucceedsWithoutDeadlock", func(t *testing.T) {
		for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			// Arrange
			var cache Cache[int]
			callbackDone := make(chan struct{})

			cache = New[int](1,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictValue(func(_ int, r EvictionReason) {
					if r == EvictionReasonCapacity {
						st := cache.Stats()
						assert.Equal(t, 1, st.Len)
						val, ok := cache.Peek("k2")
						assert.True(t, ok)
						assert.Equal(t, 2, val)
						close(callbackDone)
					}
				}),
			)

			_, err := cache.Put("k1", 1)
			require.NoError(t, err)

			// Act
			putDone := make(chan struct{})
			go func() {
				_, putErr := cache.Put("k2", 2)
				assert.NoError(t, putErr)
				close(putDone)
			}()

			// Assert
			select {
			case <-putDone:
			case <-time.After(2 * time.Second):
				require.Failf(t, "deadlocked", "backend=%v OnEvictValue calling cache.Stats()/Peek() deadlocked", b)
			}
			<-callbackDone
		}
	})

	t.Run("PressureFuncChildGoroutineDetectedByParentGIDGuard", func(t *testing.T) {
		// Arrange
		var cache Cache[int]
		var depth atomic.Int32

		cache = NewArenaRadixCache[int](10, WithPressureFunc(func() float64 {
			if depth.Add(1) > 5 {
				return 0.0
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = cache.Delete("missing")
			}()
			<-done
			return 0.10
		}))

		// Act
		_, err := cache.Put("k1", 1)

		// Assert
		require.NoError(t, err)
		observedDepth := depth.Load()
		assert.Equal(t, int32(1), observedDepth)
	})

	t.Run("PreExistingWorkerGoroutineNotMisclassifiedAsChildOfSampler", func(t *testing.T) {
		// Arrange: Spawn a worker goroutine BEFORE entering cache.Put so its parentGID is the
		// current test goroutine, but its creator frame lies below invokeAndStorePressure.
		var cache Cache[int]
		var calls atomic.Int32
		inPrimary := make(chan struct{})
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})

		go func() {
			<-workerTrigger
			_, err := cache.Put("worker_key", 2)
			assert.NoError(t, err)
			close(workerDone)
		}()

		var primaryOnce sync.Once
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			c := calls.Add(1)
			if c == 1 {
				primaryOnce.Do(func() {
					close(inPrimary)
					close(workerTrigger)
				})
				for calls.Load() < 2 {
					runtime.Gosched()
				}
			}
			return 0.10
		}))

		// Act
		_, err := cache.Put("parent_key", 1)
		<-inPrimary
		<-workerDone

		// Assert: Both the parent goroutine and the pre-existing worker goroutine sampled PressureFunc.
		require.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})
}

// TestDefect_H01_CustomPressureFuncPerWriteRuntimeStackOverhead verifies H-01 / F-04 / HIGH-3 remediation:
// Custom WithPressureFunc is evaluated on every foreground write without invoking runtime.Stack on the
// uncontended primary sampler slot (leaving samplingGID == 0 while samplingPressure == true and achieving 0 allocs/op).
func TestDefect_H01_CustomPressureFuncPerWriteRuntimeStackOverhead(t *testing.T) {
	// Arrange
	var calls atomic.Uint64
	var mc *mapCache[int]
	c := NewMapCache[int](1000, WithPressureFunc(func() float64 {
		calls.Add(1)
		assert.True(t, mc.samplingPressure.Load(), "primary sampler slot must be active during PressureFunc")
		assert.Zero(t, mc.samplingGID.Load(), "uncontended primary sampler must leave samplingGID == 0 without calling runtime.Stack")
		return 0.10
	}))
	mc = c.(*mapCache[int])

	// Act
	const numWrites = 1000
	for i := range numWrites {
		_, err := c.Put(fmt.Sprintf("k_%d", i%10), i)
		require.NoError(t, err)
	}

	// Assert
	actualCalls := calls.Load()
	assert.Equal(t, uint64(numWrites), actualCalls)

	allocs := testing.AllocsPerRun(50, func() {
		_, _ = c.Put("k_0", 42)
	})
	assert.Zero(t, allocs)
}

// TestDefect_H03_ReplaceEvictsNonMRUUnderTier2Pressure verifies H-03 / F-06 remediation across all 3 backends:
// Under Tier 2 pressure (>= EvictionThreshold), Replace() protects the replaced entry even when it is not at
// the MRU head, shedding other unprotected entries instead of evicting the entry it just updated.
func TestDefect_H03_ReplaceEvictsNonMRUUnderTier2Pressure(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange
			pressure := 0.10
			c := New[string](100,
				WithBackend(b),
				WithInvariantChecking(true),
				WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
				WithPressureFunc(func() float64 { return pressure }),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			)

			_, _ = c.Put("lru_key", strings.Repeat("a", 20)) // 20B at LRU tail
			_, _ = c.Put("mru_key", strings.Repeat("b", 40)) // 40B at MRU head (total = 60B > target 50B)

			// Act
			pressure = 0.95 // Tier 2 critical pressure
			err := c.Replace("lru_key", strings.Repeat("x", 20))
			require.NoError(t, err)

			// Assert
			val, exists := c.Peek("lru_key")
			_, mruExists := c.Peek("mru_key")
			st := c.Stats()
			assert.True(t, exists, "replaced lru_key must be protected during its own Replace() under Tier 2 pressure")
			assert.Equal(t, strings.Repeat("x", 20), val)
			assert.False(t, mruExists, "unprotected mru_key must be shed to meet Tier 2 retention target")
			assert.Equal(t, uint64(1), st.ReplaceUpdated)
			assert.Equal(t, uint64(0), st.ReplaceSelfEvicted)
			assert.Equal(t, uint64(1), st.EvictionsPressure)
		})
	}
}

// TestDefect_H05_FNV1aCollisionDisablesO1MissGlobally verifies H-05 / HIGH-2 remediation:
// ArenaRadixCache tracks active hash collisions via collisionCount and collisionPeers, using the trie fallback only while
// an active collision exists (collisionCount > 0) and immediately restoring the O(1) cache-miss fast-path
// as soon as the colliding key is deleted or evicted.
func TestDefect_H05_FNV1aCollisionDisablesO1MissGlobally(t *testing.T) {
	// Arrange: Construct two pairs of distinct keys that produce genuine 64-bit FNV-1a collisions.
	k1a := realFNV1aCollisionKeyA + "_bucket1"
	k1b := realFNV1aCollisionKeyB + "_bucket1"
	k2a := realFNV1aCollisionKeyA + "_bucket2"
	k2b := realFNV1aCollisionKeyB + "_bucket2"
	require.Equal(t, hashString(k1a), hashString(k1b))
	require.Equal(t, hashString(k2a), hashString(k2b))

	c := NewArenaRadixCache[int](100,
		WithInvariantChecking(true),
		WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
	).(*arenaRadix[int])

	// Act: Insert all 4 colliding keys (2 independent collision buckets), exercise Get/Replace/Put-with-eviction
	// promotion across peers, then remove the remaining colliding peer.
	_, err := c.Put(k2b, 20)
	require.NoError(t, err)
	_, err = c.Put(k2a, 20)
	require.NoError(t, err)
	_, err = c.Put(k1a, 10)
	require.NoError(t, err)
	_, err = c.Put(k1b, 20)
	require.NoError(t, err)
	require.Equal(t, 2, c.collisionCount)
	require.Len(t, c.nodeMap, 2)

	// Promote k1a via Get and k1b via Replace, then overwrite k1a with a weight that evicts LRU tail k2b.
	v1, ok := c.Get(k1a)
	require.True(t, ok)
	require.Equal(t, 10, v1)
	require.NoError(t, c.Replace(k1b, 25))
	evicted, err := c.Put(k1a, 55) // total 55 + 25 + 20 + 20 = 120 > 100 -> evicts LRU tail k2b (20)
	require.NoError(t, err)
	require.Equal(t, []int{20}, evicted)
	require.Equal(t, 1, c.collisionCount)

	// Delete the remaining colliding peer k1b; collisionCount immediately drops to 0 and len(nodeMap) == c.len == 2.
	_, deleted := c.Delete(k1b)
	require.True(t, deleted)

	// Assert
	require.Equal(t, 0, c.collisionCount)
	require.Empty(t, c.collisionPeers)
	require.Equal(t, 2, c.len)
	require.Len(t, c.nodeMap, 2)

	beforeFallbacks := c.Stats().ArenaHashFallbacks
	const missProbes = 1000
	for i := range missProbes {
		_, ok := c.Peek(fmt.Sprintf("completely_unrelated_missing_key_%d", i))
		require.False(t, ok)
	}
	afterFallbacks := c.Stats().ArenaHashFallbacks
	delta := afterFallbacks - beforeFallbacks
	assert.Equal(t, uint64(0), delta, "O(1) cache-miss fast-path must be restored immediately after colliding key removal")
}

// TestDefect_H06_MapCompactReallocatesWithoutSlack verifies H-06 / F-07 remediation:
// In MapCache, a single capacity eviction leaves len(c.index) == peakEntryLen (zero bucket slack).
// Neither EvaluateMemoryPressure() at Tier 1 nor explicit Compact() reallocates the map when hasSlackLocked() is false.
func TestDefect_H06_MapCompactReallocatesWithoutSlack(t *testing.T) {
	// Arrange
	pressure := 0.10
	c := NewMapCache[int](100, WithPressureFunc(func() float64 { return pressure })).(*mapCache[int])

	for i := range 101 {
		_, err := c.Put(fmt.Sprintf("key_%03d", i), i)
		require.NoError(t, err)
	}
	require.Equal(t, 100, c.Stats().Len)
	require.Equal(t, 100, c.Stats().PeakEntryLen)
	require.True(t, c.dirtyIndex)
	require.False(t, c.hasSlackLocked())
	indexPtrBefore := reflect.ValueOf(c.index).Pointer()

	// Act
	pressure = 0.80
	_ = c.EvaluateMemoryPressure()
	c.Compact()
	indexPtrAfter := reflect.ValueOf(c.index).Pointer()
	compactAllocs := testing.AllocsPerRun(50, func() {
		c.Compact()
	})

	// Assert
	st := c.Stats()
	assert.Equal(t, indexPtrBefore, indexPtrAfter, "c.index map must not be reallocated when hasSlackLocked() is false")
	assert.Zero(t, compactAllocs, "Compact() without slack must perform 0 allocations")
	assert.Equal(t, uint64(0), st.CompactionsPressureTier1)
	assert.Equal(t, uint64(0), st.CompactionsExplicit)
}

// TestDefect_M01_to_M06_MediumDefectsAndDivergences verifies M-01..M-04 remediation:
// - M-01 / F-08 / MEDIUM-3: DeletePrefix("") samples pressure via lockWithPressure and attributes compaction telemetry accurately.
// - M-02 / MEDIUM-1: ArenaRadixCache.Compact() on a pure-insert cache is a no-op when !isDirtyLocked().
// - M-03 / F-09: RadixCache.Compact() resets watermarks without incrementing compaction counters or ReclaimEpoch.
// - M-04 / F-10 / MEDIUM-4: reconcileThresholdWindow clamps derived EvictionThreshold to <= 1.0 and preserves valid explicit pairs.
func TestDefect_M01_to_M06_MediumDefectsAndDivergences(t *testing.T) {
	t.Run("M01_DeletePrefixEmptySamplesPressureAndAttributesTelemetry", func(t *testing.T) {
		for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			// Arrange
			pressure := 0.10
			c := New[string](100,
				WithBackend(b),
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 { return pressure }),
				WithCompactionThreshold(0.75),
				WithEvictionThreshold(0.90),
			)
			for i := range 10 {
				_, _ = c.Put(fmt.Sprintf("p/%d", i), "val")
			}

			// Act
			pressure = 0.95 // Tier 2 critical pressure
			c.DeletePrefix("")
			st := c.Stats()

			// Assert
			assert.InDelta(t, 0.95, st.MemoryPressure, 1e-9)
			assert.Equal(t, uint64(0), st.CompactionsAutoSlack)
			assert.Equal(t, uint64(1), st.CompactionsPressureTier2)
			assert.Equal(t, uint64(1), st.ReclaimEpoch)
		}
	})

	t.Run("M02_PureInsertCompactParityAcrossBackends", func(t *testing.T) {
		// Arrange
		mapC := NewMapCache[int](100).(PressureAwareCache[int])
		radixC := NewRadixCache[int](100).(PressureAwareCache[int])
		arenaC := NewArenaRadixCache[int](100).(PressureAwareCache[int])

		for i := range 10 {
			k := fmt.Sprintf("key_%d", i)
			_, _ = mapC.Put(k, i)
			_, _ = radixC.Put(k, i)
			_, _ = arenaC.Put(k, i)
		}

		// Act
		mapC.Compact()
		radixC.Compact()
		arenaC.Compact()

		// Assert
		assert.Equal(t, uint64(0), mapC.Stats().CompactionsExplicit)
		assert.Equal(t, uint64(0), radixC.Stats().CompactionsExplicit)
		assert.Equal(t, uint64(0), arenaC.Stats().CompactionsExplicit)
		assert.Equal(t, uint64(0), arenaC.Stats().ReclaimEpoch)
	})

	t.Run("M03_RadixCacheNoOpCompactionDoesNotIncrementTelemetry", func(t *testing.T) {
		// Arrange
		c := NewRadixCache[int](100).(PressureAwareCache[int])
		_, _ = c.Put("k1", 1)
		_, _ = c.Put("k2", 2)
		_, _ = c.Delete("k1")

		// Act
		c.Compact()
		st := c.Stats()

		// Assert
		assert.Equal(t, uint64(0), st.CompactionsExplicit)
		assert.Equal(t, uint64(0), st.ReclaimEpoch)
		assert.Equal(t, 0, st.DeletedSinceCompact)
	})

	t.Run("M04_ThresholdReconciliationClampsEvictionThresholdToOne", func(t *testing.T) {
		// Arrange & Act
		opts1 := ApplyOptions(WithCompactionThreshold(0.95))
		opts2 := ApplyOptions(WithCompactionThreshold(0.95), WithEvictionThreshold(0.90))
		opts3 := ApplyOptions(WithCompactionThreshold(0.60), WithEvictionThreshold(0.85))

		// Assert
		assert.InDelta(t, 0.95, opts1.CompactionThreshold, 1e-9)
		assert.InDelta(t, 1.0, opts1.EvictionThreshold, 1e-9)

		assert.InDelta(t, 0.75, opts2.CompactionThreshold, 1e-9)
		assert.InDelta(t, 0.90, opts2.EvictionThreshold, 1e-9)

		assert.InDelta(t, 0.60, opts3.CompactionThreshold, 1e-9)
		assert.InDelta(t, 0.85, opts3.EvictionThreshold, 1e-9)
	})
}

// ============================================================================
// Suite 1.7: Remediation Verification (R1..R5: Contiguous Slack, Multi-Cache
// Isolation, Zero-Alloc Reclaim, and Child-Goroutine Concurrency Invariants)
// ============================================================================

// TestDefect_R3_ArenaRadixContiguousCompactRequiresTrueSlack verifies R3:
// When ArenaRadixCache is at capacity and a Put triggers 1 LRU eviction that is immediately
// reused by allocNode() (leaving freeHead == nilNode, len(nodes) == cap(nodes), and
// len(nodeMap) == peakEntryLen), Compact() and Tier 1 EvaluateMemoryPressure() must be
// zero-allocation no-ops unless true contiguous slack exists.
func TestDefect_R3_ArenaRadixContiguousCompactRequiresTrueSlack(t *testing.T) {
	t.Run("SteadyStateTurnoverWithZeroSlackPerformsZeroAllocCompactAndTier1NoOp", func(t *testing.T) {
		// Arrange: Fill ArenaRadixCache(100) with 100 entries and trigger 1 capacity eviction
		// (matching TestDefect_H06_MapCompactReallocatesWithoutSlack). The evicted node is immediately
		// reused by allocNode(), leaving freeHead == nilNode, len == peakEntryLen == 100, and deletedSinceCompact == 1.
		var pressureBits atomic.Uint64
		pressureBits.Store(math.Float64bits(0.10))
		c := NewArenaRadixCache[int](100,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				return math.Float64frombits(pressureBits.Load())
			}),
			WithCompactionThreshold(0.75),
			WithEvictionThreshold(0.90),
		).(*arenaRadix[int])

		for i := range 100 {
			_, err := c.Put(fmt.Sprintf("key_%03d", i), i)
			require.NoError(t, err)
		}
		evicted, err := c.Put("key_100", 100)
		require.NoError(t, err)
		require.Len(t, evicted, 1)

		require.Equal(t, nilNode, c.freeHead)
		require.Zero(t, c.freeCount)
		require.Equal(t, 100, c.len)
		require.Equal(t, 100, c.peakEntryLen)
		require.False(t, c.hasContiguousSlackLocked())
		require.False(t, c.isDirtyLocked())
		require.False(t, c.shouldAutoCompactLocked(nilNode))

		nodesPtrBefore := unsafe.SliceData(c.nodes)
		mapPtrBefore := reflect.ValueOf(c.nodeMap).Pointer()

		// Act: Evaluate Tier 1 pressure (0.80) and call explicit Compact().
		pressureBits.Store(math.Float64bits(0.80))
		_ = c.EvaluateMemoryPressure()
		c.Compact()
		compactAllocs := testing.AllocsPerRun(50, func() {
			c.Compact()
		})

		// Assert
		st := c.Stats()
		assert.Same(t, nodesPtrBefore, unsafe.SliceData(c.nodes), "c.nodes must not be reallocated when contiguous slack is absent")
		assert.Equal(t, mapPtrBefore, reflect.ValueOf(c.nodeMap).Pointer(), "c.nodeMap must not be reallocated when map slack is absent")
		assert.Zero(t, compactAllocs, "Compact() without contiguous slack must perform 0 allocations")
		assert.Zero(t, st.CompactionsPressureTier1)
		assert.Zero(t, st.CompactionsExplicit)
		assert.Zero(t, st.ReclaimEpoch)
	})

	t.Run("ContiguousUnallocatedSliceSlackAndMapChurnTriggersCompaction", func(t *testing.T) {
		// Arrange: Insert 9 single-byte keys ("a".."i") into capacity 9 so c.nodes has len 10 in a 16-cap slice,
		// then perform minChurnCompactDeletes (64) single-byte capacity evictions so freeHead == nilNode
		// while deletedSinceCompact >= minChurnCompactDeletes and cap(c.nodes) (16) > len(c.nodes) (10).
		var pressureBits atomic.Uint64
		pressureBits.Store(math.Float64bits(0.10))
		c := NewArenaRadixCache[int](9,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				return math.Float64frombits(pressureBits.Load())
			}),
		).(*arenaRadix[int])

		for i := range 9 {
			_, err := c.Put(string([]byte{byte('a' + i)}), i)
			require.NoError(t, err)
		}
		for i := range minChurnCompactDeletes {
			_, err := c.Put(string([]byte{byte(128 + i)}), i)
			require.NoError(t, err)
		}

		require.Equal(t, nilNode, c.freeHead)
		require.Greater(t, cap(c.nodes), len(c.nodes))
		require.True(t, c.hasContiguousSlackLocked())
		require.True(t, c.isDirtyLocked())

		// Act: Explicit Compact() trims the unallocated tail capacity of c.nodes down to len(c.nodes) and rebuilds nodeMap.
		c.Compact()

		// Assert
		st := c.Stats()
		assert.Equal(t, len(c.nodes), cap(c.nodes), "Compact() must trim unallocated c.nodes capacity")
		assert.Zero(t, st.ArenaUnallocatedCap)
		assert.Equal(t, uint64(1), st.CompactionsExplicit)
		assert.Equal(t, uint64(1), st.ReclaimEpoch)
	})
}

// TestDefect_R1_R2_ZeroGlobalStackDumpContentionAndZeroAllocReclaim verifies R1 and R2:
//  1. Independent cache instances executing OnEvict* or PressureFunc concurrently do not
//     contend on global singleton slots or allocate 64 KiB runtime.Stack(_, true) buffers.
//  2. Contended writers on the same cache instance do not repeatedly invoke captureAllGoroutineStacks
//     once the active primary sampler has been resolved.
//  3. Steady-state Put under Tier 2 pressure allocates <= 2 objects/op (restoring the PutUnderPressure target).
func TestDefect_R1_R2_ZeroGlobalStackDumpContentionAndZeroAllocReclaim(t *testing.T) {
	t.Run("IndependentCachesConcurrentOnEvictAndPressureFuncAllocateZeroStackBuffers", testMultiCacheConcurrentHooksZeroAllocs)

	t.Run("SameCacheContendedWritersResolvePrimarySamplerOnceWithZeroAllocs", func(t *testing.T) {
		// Arrange: Block primary sampler inside PressureFunc on cacheA, then run a contended writer
		// from the same parent goroutine (spawned before PressureFunc) in a loop and verify 0 allocs/op.
		inPrimary := make(chan struct{})
		releasePrimary := make(chan struct{})
		var armPrimary atomic.Bool

		c := NewMapCache[int](100, WithPressureFunc(func() float64 {
			if armPrimary.CompareAndSwap(true, false) {
				close(inPrimary)
				<-releasePrimary
			}
			return 0.10
		}))
		_, err := c.Put("seed", 1)
		require.NoError(t, err)

		armPrimary.Store(true)
		var wg sync.WaitGroup
		wg.Go(func() {
			_, _ = c.Put("primary_write", 2)
		})
		<-inPrimary

		// Act: Repeated contended writes on the same cache resolve the primary sampler once and allocate 0 objects/op.
		allocs := testing.AllocsPerRun(50, func() {
			_ = c.Replace("seed", 3)
		})

		close(releasePrimary)
		wg.Wait()

		// Assert
		assert.Zero(t, allocs, "contended writer on same cache must not allocate after primary sampler resolution")
	})

	t.Run("ArenaRadixPutUnderPressureMeetsTwoAllocsPerOpTarget", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[int](512,
			WithPressureFunc(func() float64 { return 0.95 }),
			WithCompactionThreshold(0.75),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
		)
		keys := make([]string, 1024)
		for i := range keys {
			keys[i] = fmt.Sprintf("service/instance/item/%06d", i)
		}
		for i := range 512 {
			_, err := c.Put(keys[i], i)
			require.NoError(t, err)
		}

		// Act: Measure allocations per Put under continuous Tier 2 pressure (0.95).
		idx := 512
		allocs := testing.AllocsPerRun(200, func() {
			_, _ = c.Put(keys[idx&1023], idx)
			idx++
		})

		// Assert: Must be <= 2.0 allocs/op (1 evicted slice + at most 1 prefix string clone, 0 sync.Map Clear allocs).
		assert.LessOrEqual(t, allocs, 2.0, "ArenaRadixCache PutUnderPressure must allocate <= 2 objects/op")
	})
}

func testMultiCacheConcurrentHooksZeroAllocs(t *testing.T) {
	t.Helper()
	// Arrange: Hold cachePressureA inside PressureFunc and cacheEvictA inside OnEvictValue concurrently,
	// while cacheB (with distinct hookTag) executes Replace with PressureFunc and OnEvictValue.
	inPressureA := make(chan struct{})
	releasePressureA := make(chan struct{})
	inEvictA := make(chan struct{})
	releaseEvictA := make(chan struct{})

	var armPressureA, armEvictA atomic.Bool
	cachePressureA := NewMapCache[int](2, WithPressureFunc(func() float64 {
		if armPressureA.CompareAndSwap(true, false) {
			close(inPressureA)
			<-releasePressureA
		}
		return 0.10
	}))
	cacheEvictA := NewMapCache[int](1, WithOnEvictValue(func(_ int, _ EvictionReason) {
		if armEvictA.CompareAndSwap(true, false) {
			close(inEvictA)
			<-releaseEvictA
		}
	}))
	_, err := cacheEvictA.Put("e1", 1)
	require.NoError(t, err)

	var sinkB int
	cacheB := NewMapCache[int](2,
		WithPressureFunc(func() float64 { return 0.10 }),
		WithOnEvictValue(func(v int, _ EvictionReason) { sinkB = v }),
	)
	_, err = cacheB.Put("b1", 1)
	require.NoError(t, err)

	armPressureA.Store(true)
	armEvictA.Store(true)
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = cachePressureA.Put("p1", 1)
	})
	wg.Go(func() {
		_, _ = cacheEvictA.Put("e2", 2)
	})
	<-inPressureA
	<-inEvictA

	// Act: Measure allocations on cacheB while both cachePressureA and cacheEvictA are blocked in their hooks.
	allocsB := testing.AllocsPerRun(100, func() {
		_ = cacheB.Replace("b1", 2)
	})

	close(releasePressureA)
	close(releaseEvictA)
	wg.Wait()

	// Assert
	assert.Zero(t, allocsB, "independent cacheB must allocate 0 objects while other caches are in PressureFunc and OnEvictValue")
	assert.Equal(t, 2, sinkB)
}

// TestDefect_R4_WaitGroupGoReentrancyReclaimSurvivalAndEvictSerialization verifies R4 (CI-01..CI-04):
//   - CI-01: Child goroutines spawned via sync.WaitGroup.Go or returning helper functions inside
//     OnEvict* and PressureFunc are recognized as descendants and never deadlock or re-sample.
//   - CI-02: Multi-hop child -> grandchild chains where the intermediate child goroutine exits
//     before the grandchild accesses the cache are still recognized via the parent's creatorFunc.
//   - CI-03: Reclamations performed by a child goroutine inside PressureFunc are attributed to the
//     active root sampler so the parent sampler does not false-positive retry 3 times and over-evict.
//   - CI-04: Re-entrant OnEvict* callbacks triggered by child goroutines are strictly serialized
//     (maxConcurrent == 1) and delivered in FIFO order without deadlocking wg.Wait().
func TestDefect_R4_WaitGroupGoReentrancyReclaimSurvivalAndEvictSerialization(t *testing.T) {
	t.Run("CI01_WaitGroupGoAndHelperSpawnedChildInOnEvictAndPressureFunc", testCI01WaitGroupGoAndHelperChild)
	t.Run("CI02_ExitedIntermediateGoroutineGrandchildDetection", testCI02ExitedIntermediateGrandchild)
	t.Run("CI03_ChildGoroutineReclaimInsidePressureFuncPreservesSurvivors", testCI03ChildReclaimPreservesSurvivors)
	t.Run("CI04_ReentrantChildGoroutineEvictCallbacksAreStrictlySerialized", testCI04ReentrantChildEvictSerialized)
}

func spawnViaReturningHelper(fn func()) {
	go fn()
}

func testCI01WaitGroupGoAndHelperChild(t *testing.T) {
	t.Helper()
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		// Arrange
		var cache Cache[int]
		var pressureSamples, evictCount atomic.Int32
		var firstPressure, firstEvict atomic.Bool
		firstPressure.Store(true)
		firstEvict.Store(true)

		cache = New[int](1,
			WithBackend(b),
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				pressureSamples.Add(1)
				if firstPressure.CompareAndSwap(true, false) {
					var wg sync.WaitGroup
					wg.Go(func() {
						_, _ = cache.Delete("nonexistent_from_wg_go")
					})
					wg.Wait()
				}
				return 0.10
			}),
			WithOnEvictEntry(func(_ string, _ int, _ EvictionReason) {
				evictCount.Add(1)
				if firstEvict.CompareAndSwap(true, false) {
					var wg sync.WaitGroup
					wg.Go(func() {
						_, putErr := cache.Put("k3_from_wg_go", 3)
						assert.NoError(t, putErr)
					})
					doneHelper := make(chan struct{})
					spawnViaReturningHelper(func() {
						defer close(doneHelper)
						_, _ = cache.Delete("missing_from_helper")
					})
					wg.Wait()
					<-doneHelper
				}
			}),
		)

		// Act
		_, err := cache.Put("k1", 1)
		require.NoError(t, err)
		require.Equal(t, int32(1), pressureSamples.Load(), "wg.Go child inside PressureFunc must not re-sample")

		done := make(chan struct{})
		go func() {
			_, putErr := cache.Put("k2", 2)
			assert.NoError(t, putErr)
			close(done)
		}()

		// Assert
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			require.Failf(t, "deadlock", "backend %v deadlocked on wg.Go child inside OnEvictEntry", b)
		}
		assert.Equal(t, int32(2), evictCount.Load())
		val, ok := cache.Peek("k3_from_wg_go")
		require.True(t, ok)
		assert.Equal(t, 3, val)
	}
}

func testCI02ExitedIntermediateGrandchild(t *testing.T) {
	t.Helper()
	// Arrange: G1 (in OnEvictValue and PressureFunc) spawns intermediate child G2 via wg.Go;
	// G2 spawns grandchild G3 and exits immediately (`childExited` closes before G3 touches cache).
	var cache Cache[int]
	var pressureCalls, evictCalls atomic.Int32

	cache = NewArenaRadixCache[int](1,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			if pressureCalls.Add(1) == 1 {
				grandchildDone := make(chan struct{})
				childExited := make(chan struct{})
				var wg sync.WaitGroup
				wg.Go(func() {
					go func() {
						defer close(grandchildDone)
						<-childExited
						runtime.Gosched()
						_, _ = cache.Delete("from_orphan_grandchild")
					}()
				})
				wg.Wait()
				close(childExited)
				<-grandchildDone
			}
			return 0.10
		}),
		WithOnEvictValue(func(_ int, _ EvictionReason) {
			if evictCalls.Add(1) == 1 {
				grandchildDone := make(chan struct{})
				childExited := make(chan struct{})
				var wg sync.WaitGroup
				wg.Go(func() {
					go func() {
						defer close(grandchildDone)
						<-childExited
						runtime.Gosched()
						_, putErr := cache.Put("k3_from_orphan_grandchild", 30)
						assert.NoError(t, putErr)
					}()
				})
				wg.Wait()
				close(childExited)
				<-grandchildDone
			}
		}),
	)

	// Act
	_, err := cache.Put("k1", 1)
	require.NoError(t, err)
	assert.Equal(t, int32(1), pressureCalls.Load(), "orphan grandchild inside PressureFunc must not re-sample")

	_, err = cache.Put("k2", 2)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, int32(2), evictCalls.Load())
	val, ok := cache.Peek("k3_from_orphan_grandchild")
	require.True(t, ok)
	assert.Equal(t, 30, val)
}

func testCI03ChildReclaimPreservesSurvivors(t *testing.T) {
	t.Helper()
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		// Arrange: Populate 4 entries of 25B each at 0.10 pressure (100B total, retention = 0.50 -> targetSize = 50B).
		// When EvaluateMemoryPressure() runs at 0.95, its PressureFunc spawns a child goroutine via wg.Go
		// that reclaims 1 entry ("k1", 25B) via DeletePrefix, triggering markReclaimedLocked on the child goroutine.
		var pac PressureAwareCache[int]
		var pressureBits atomic.Uint64
		pressureBits.Store(math.Float64bits(0.10))
		var triggerChildDelete atomic.Bool
		var sampleCount atomic.Int32

		c := New[int](100,
			WithBackend(b),
			WithInvariantChecking(true),
			WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				sampleCount.Add(1)
				if triggerChildDelete.CompareAndSwap(true, false) {
					var wg sync.WaitGroup
					wg.Go(func() {
						pac.DeletePrefix("k1")
					})
					wg.Wait()
				}
				return math.Float64frombits(pressureBits.Load())
			}),
		)
		pac = c.(PressureAwareCache[int])

		for _, k := range []string{"k1", "k2", "k3", "k4"} {
			_, err := c.Put(k, 25)
			require.NoError(t, err)
		}

		// Act
		pressureBits.Store(math.Float64bits(0.95))
		sampleCount.Store(0)
		triggerChildDelete.Store(true)
		evicted := pac.EvaluateMemoryPressure()

		// Assert: Parent sampler sampled only once (did not retry 3 times) and shed only "k2" (25B)
		// to reach 50B (k3 + k4), rather than over-evicting k3 on a post-retry pass.
		assert.Equal(t, int32(1), sampleCount.Load(), "backend %v parent sampler must not retry when child goroutine reclaims", b)
		assert.Len(t, evicted, 1, "backend %v must shed only 1 entry (k2) to reach 50%% retention", b)
		assert.Equal(t, 2, c.Stats().Len, "backend %v must preserve 2 survivors (k3, k4)", b)
	}
}

func testCI04ReentrantChildEvictSerialized(t *testing.T) {
	t.Helper()
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		// Arrange
		var cache Cache[int]
		var inFlight, maxConcurrent atomic.Int32
		var mu sync.Mutex
		var evictedOrder []string

		cache = New[int](10,
			WithBackend(b),
			WithInvariantChecking(true),
			WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
				cur := inFlight.Add(1)
				for {
					prev := maxConcurrent.Load()
					if cur <= prev || maxConcurrent.CompareAndSwap(prev, cur) {
						break
					}
				}
				mu.Lock()
				evictedOrder = append(evictedOrder, key)
				mu.Unlock()

				if key == "k0" {
					var wg sync.WaitGroup
					wg.Go(func() {
						_, _ = cache.Delete("k1")
					})
					wg.Go(func() {
						_, _ = cache.Delete("k2")
					})
					wg.Wait()
				}
				time.Sleep(2 * time.Millisecond)
				inFlight.Add(-1)
			}),
		)

		_, err := cache.Put("k0", 0)
		require.NoError(t, err)
		_, err = cache.Put("k1", 1)
		require.NoError(t, err)
		_, err = cache.Put("k2", 2)
		require.NoError(t, err)

		// Act
		_, ok := cache.Delete("k0")
		require.True(t, ok)

		// Assert: All 3 callbacks ran with strict mutual exclusion (maxConcurrent == 1), with "k0" first.
		assert.Equal(t, int32(1), maxConcurrent.Load(), "backend %v OnEvictEntry must never execute concurrently (maxConcurrent == 1)", b)
		mu.Lock()
		assert.Len(t, evictedOrder, 3)
		assert.Equal(t, "k0", evictedOrder[0])
		assert.ElementsMatch(t, []string{"k0", "k1", "k2"}, evictedOrder)
		mu.Unlock()
	}
}

// TestAdv_Bug1_FastEvictTagOwnerGlobalMemoryLeak verifies that invoking OnEvict* and
// PressureFunc on a cache instance never retains a strong pointer to the cache or its
// cached entries in package-global tag state after the caller drops the cache.
func TestAdv_Bug1_FastEvictTagOwnerGlobalMemoryLeak(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange: Create a cache inside a non-inlined helper, trigger both PressureFunc
			// and OnEvictValue on the fast path, and keep a weak.Pointer to a surviving value.
			wp := createAndDropCacheForGCLeakCheck(t, b)

			// Act: Force garbage collection cycles after the cache reference has been dropped.
			for range 6 {
				runtime.GC()
				runtime.Gosched()
			}

			// Assert: Neither the cache struct nor its surviving cached entries remain reachable.
			assert.Nil(t, wp.Value(), "dropped %v instance and its cached entries must be garbage-collected", b)
		})
	}
}

//go:noinline
func createAndDropCacheForGCLeakCheck(t *testing.T, b Backend) weak.Pointer[byte] {
	t.Helper()
	c := New[*byte](2,
		WithBackend(b),
		WithPressureFunc(func() float64 { return 0.10 }),
		WithOnEvictValue(func(_ *byte, _ EvictionReason) {}),
	)
	v1 := new(byte)
	v2 := new(byte)
	v3 := new(byte)
	*v2 = 42
	_, err := c.Put("k1", v1)
	require.NoError(t, err)
	_, err = c.Put("k2", v2)
	require.NoError(t, err)
	_, err = c.Put("k3", v3) // Evicts k1 on the fast path while k2 and k3 remain in c.
	require.NoError(t, err)
	return weak.Make(v2)
}

// TestAdv_Bug2_PendingChildCallbackPanicLeaksEvictCallbackMuAndDeadlocks verifies that
// if a child goroutine spawned inside OnEvict* triggers a nested eviction whose queued
// callback panics inside drainPendingEvictCallbacks, evictCallbackMu is unlocked, all
// queued events/closures are zeroed, and subsequent evictions do not deadlock.
func TestAdv_Bug2_PendingChildCallbackPanicLeaksEvictCallbackMuAndDeadlocks(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange
			var cache Cache[*int]
			var panicOnBatch atomic.Bool
			var evictedKeys []string
			var mu sync.Mutex

			cache = New[*int](10,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictEntry(func(key string, _ *int, _ EvictionReason) {
					mu.Lock()
					evictedKeys = append(evictedKeys, key)
					mu.Unlock()

					if key == "k1" {
						var wg sync.WaitGroup
						wg.Go(func() {
							pac := cache.(PressureAwareCache[*int])
							pac.DeletePrefix("batch/")
						})
						wg.Wait()
					}
					if panicOnBatch.Load() && strings.HasPrefix(key, "batch/") {
						panic("simulated pending child callback panic")
					}
				}),
			)

			wpBatch2 := populateBug2Cache(t, cache)

			// Act: Delete k1 so its OnEvictEntry spawns a child that deletes batch/1 and batch/2,
			// and batch/1 panics when drained inside unlockEvictCallbackSlow.
			panicOnBatch.Store(true)
			require.PanicsWithValue(t, "simulated pending child callback panic", func() {
				_, _ = cache.Delete("k1")
			})
			panicOnBatch.Store(false)

			for range 6 {
				runtime.GC()
				runtime.Gosched()
			}

			done := make(chan struct{})
			go func() {
				_, delOk := cache.Delete("k3")
				assert.True(t, delOk)
				close(done)
			}()

			// Assert: Subsequent eviction completes without deadlocking on evictCallbackMu,
			// and unconsumed events in the panicking child batch were zeroed for GC.
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				require.Failf(t, "deadlock", "backend %v permanently deadlocked on evictCallbackMu after pending child callback panic", b)
			}
			assert.Nil(t, wpBatch2.Value(), "unconsumed event value in panicking pending callback batch must be zeroed")
			mu.Lock()
			assert.Contains(t, evictedKeys, "k3")
			mu.Unlock()
		})
	}
}

//go:noinline
func populateBug2Cache(t *testing.T, cache Cache[*int]) weak.Pointer[int] {
	t.Helper()
	v1, vBatch1, vBatch2, v3 := new(int), new(int), new(int), new(int)
	wpBatch2 := weak.Make(vBatch2)
	_, err := cache.Put("k1", v1)
	require.NoError(t, err)
	_, err = cache.Put("batch/1", vBatch1)
	require.NoError(t, err)
	_, err = cache.Put("batch/2", vBatch2)
	require.NoError(t, err)
	_, err = cache.Put("k3", v3)
	require.NoError(t, err)
	return wpBatch2
}

// TestAdv_Bug3_UnrelatedGoroutineWithExitedParentMisclassifiedAsChild verifies that:
//  1. An unrelated worker goroutine whose parent goroutine has already exited is not
//     misclassified as a re-entrant child of an active OnEvict* or PressureFunc holder.
//  2. A pre-existing worker goroutine spawned via sync.WaitGroup.Go before cache.Put
//     is not misclassified as a re-entrant child of PressureFunc.
func TestAdv_Bug3_UnrelatedGoroutineWithExitedParentMisclassifiedAsChild(t *testing.T) {
	t.Run("UnrelatedWorkerWithExitedParentReturnsFromDeleteOnlyAfterOnEvictRuns", func(t *testing.T) {
		// Arrange: Spawn a worker from a parent goroutine that immediately exits.
		parentExited := make(chan struct{})
		startWorker := make(chan struct{})
		workerDone := make(chan struct{})

		var cache Cache[int]
		inK1Evict := make(chan struct{})
		releaseK1Evict := make(chan struct{})
		var k2EvictCompleted atomic.Bool
		var k2CompletedBeforeDeleteReturn atomic.Bool

		go func() {
			go func() {
				<-startWorker
				_, ok := cache.Delete("k2")
				assert.True(t, ok)
				k2CompletedBeforeDeleteReturn.Store(k2EvictCompleted.Load())
				close(workerDone)
			}()
			close(parentExited)
		}()
		<-parentExited
		time.Sleep(5 * time.Millisecond)

		var k1Once sync.Once
		cache = NewMapCache[int](10, WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
			switch key {
			case "k1":
				k1Once.Do(func() {
					close(inK1Evict)
					<-releaseK1Evict
				})
			case "k2":
				time.Sleep(15 * time.Millisecond)
				k2EvictCompleted.Store(true)
			}
		}))
		_, err := cache.Put("k1", 1)
		require.NoError(t, err)
		_, err = cache.Put("k2", 2)
		require.NoError(t, err)

		// Act: Hold G1 inside OnEvict("k1"), trigger the orphan-parent worker to call Delete("k2"),
		// then release OnEvict("k1") once the worker is contending on evictCallbackMu.
		g1Done := make(chan struct{})
		go func() {
			_, _ = cache.Delete("k1")
			close(g1Done)
		}()
		<-inK1Evict
		close(startWorker)
		time.Sleep(10 * time.Millisecond)
		close(releaseK1Evict)
		<-g1Done
		<-workerDone

		// Assert: Unrelated worker waited for its own OnEvict("k2") to finish before Delete("k2") returned.
		assert.True(t, k2CompletedBeforeDeleteReturn.Load(), "unrelated worker's Delete(k2) returned before OnEvict(k2) executed")
	})

	t.Run("UnrelatedWorkerSpawnedViaWaitGroupGoBeforePutNotMisclassifiedAsChildOfSampler", func(t *testing.T) {
		// Arrange: Spawn a background worker via wg.Go BEFORE calling cache.Put on the same parent goroutine.
		var cache Cache[int]
		var calls atomic.Int32
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})
		var workerErr error
		var wg sync.WaitGroup

		wg.Go(func() {
			<-workerTrigger
			_, workerErr = cache.Put("worker_key", 2)
			close(workerDone)
		})

		var primaryOnce sync.Once
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			c := calls.Add(1)
			if c == 1 {
				primaryOnce.Do(func() {
					close(workerTrigger)
				})
				deadline := time.Now().Add(200 * time.Millisecond)
				for calls.Load() < 2 && time.Now().Before(deadline) {
					runtime.Gosched()
				}
			}
			return 0.10
		}))

		// Act
		_, err := cache.Put("parent_key", 1)
		require.NoError(t, err)
		<-workerDone
		wg.Wait()
		require.NoError(t, workerErr)

		// Assert: Both the parent goroutine and the pre-existing wg.Go worker evaluated PressureFunc.
		assert.Equal(t, int32(2), calls.Load(), "pre-existing worker spawned via wg.Go before cache.Put must not be misclassified as a child of PressureFunc")
	})

	t.Run("UnrelatedWorkerWithExitedParentSamplesPressureAndIncrementsExternalReclaimEpoch", func(t *testing.T) {
		// Arrange: Spawn an unrelated worker from a parent goroutine that immediately exits.
		parentExited := make(chan struct{})
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})
		var cache *mapCache[int]
		var calls atomic.Int32
		var workerErr error

		go func() {
			go func() {
				<-workerTrigger
				_, _ = cache.Delete("k_reclaim")
				_, workerErr = cache.Put("worker_key", 2)
				close(workerDone)
			}()
			close(parentExited)
		}()
		<-parentExited
		time.Sleep(5 * time.Millisecond)

		var armOnce sync.Once
		var armed atomic.Bool
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.95
			}
			c := calls.Add(1)
			if c == 1 {
				armOnce.Do(func() {
					close(workerTrigger)
				})
				<-workerDone
			}
			return 0.95
		})).(*mapCache[int])

		_, err := cache.Put("k_reclaim", 1)
		require.NoError(t, err)
		epochBefore := cache.externalReclaimEpoch.Load()
		armed.Store(true)

		// Act
		_, err = cache.Put("parent_key", 1)
		require.NoError(t, err)
		require.NoError(t, workerErr)

		// Assert: Unrelated worker with exited parent evaluated PressureFunc and incremented externalReclaimEpoch.
		assert.GreaterOrEqual(t, calls.Load(), int32(2), "unrelated worker with exited parent must evaluate PressureFunc")
		assert.Greater(t, cache.externalReclaimEpoch.Load(), epochBefore, "unrelated worker Delete must increment externalReclaimEpoch")
	})
}

// TestAdv_Bug4_SiblingHelperClosureInSameOuterFunctionDeadlocksOnEvictAndBypassesPressureFunc
// verifies that when an outer function defines both a helper closure (`spawn := func(fn func()) { go fn() }`)
// and PressureFunc / OnEvict* closures, child goroutines spawned via `spawn` inside PressureFunc or OnEvict*
// are recognized as descendants rather than rejected because the outer function frame lies below the hook.
func TestAdv_Bug4_SiblingHelperClosureInSameOuterFunctionDeadlocksOnEvictAndBypassesPressureFunc(t *testing.T) {
	// Arrange
	spawnHelper := func(fn func()) {
		go fn()
	}

	var cache Cache[int]
	var pressureCalls, evictCalls atomic.Int32
	var firstPressure, firstEvict atomic.Bool
	firstPressure.Store(true)
	firstEvict.Store(true)

	cache = NewMapCache[int](1,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			pressureCalls.Add(1)
			if firstPressure.CompareAndSwap(true, false) {
				done := make(chan struct{})
				spawnHelper(func() {
					defer close(done)
					_, _ = cache.Delete("missing_from_sibling_helper")
				})
				<-done
			}
			return 0.10
		}),
		WithOnEvictValue(func(_ int, _ EvictionReason) {
			evictCalls.Add(1)
			if firstEvict.CompareAndSwap(true, false) {
				done := make(chan struct{})
				spawnHelper(func() {
					defer close(done)
					_, err := cache.Put("k3_from_sibling_helper", 3)
					assert.NoError(t, err)
				})
				<-done
			}
		}),
	)

	// Act: First Put triggers PressureFunc (where child spawned via spawnHelper must not re-sample);
	// second Put evicts k1 and triggers OnEvictValue (where child spawned via spawnHelper must not deadlock).
	_, err := cache.Put("k1", 1)
	require.NoError(t, err)
	callsAfterFirstPut := pressureCalls.Load()

	putDone := make(chan struct{})
	go func() {
		_, putErr := cache.Put("k2", 2)
		assert.NoError(t, putErr)
		close(putDone)
	}()

	// Assert
	select {
	case <-putDone:
	case <-time.After(2 * time.Second):
		require.Fail(t, "OnEvictValue deadlocked on evictCallbackMu when child was spawned via sibling helper closure")
	}
	assert.Equal(t, int32(1), callsAfterFirstPut, "child spawned via sibling helper closure inside PressureFunc must not re-sample")
	assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and k2 eviction callbacks must complete")
}

// TestAdv_Bug5_DetachedChildDrainPendingCallbackNestedEvictionDeadlock verifies that when
// pending callbacks are drained (either via unlockEvictCallbackSlow or via the post-unlock
// TryLock path in enqueuePendingCallback), the draining goroutine is registered as the active
// callback holder with "deliverCallbacks" on the stack so nested synchronous and child-goroutine
// evictions inside the drained callback never self-deadlock on evictCallbackMu.
func TestAdv_Bug5_DetachedChildDrainPendingCallbackNestedEvictionDeadlock(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange
			var cache Cache[int]
			var evicted []string
			var mu sync.Mutex
			allDone := make(chan struct{})

			onEntry := func(key string, _ int, _ EvictionReason) {
				mu.Lock()
				evicted = append(evicted, key)
				n := len(evicted)
				mu.Unlock()

				switch key {
				case "k1":
					go func() {
						_, _ = cache.Delete("k2")
					}()
				case "k2":
					// Trigger both a synchronous nested eviction and a wg.Go child nested eviction
					// while draining pending callbacks.
					_, _ = cache.Delete("k3")
					var wg sync.WaitGroup
					wg.Go(func() {
						_, _ = cache.Delete("k4")
					})
					wg.Wait()
				}
				if n == 4 {
					close(allDone)
				}
			}

			cache = New[int](10,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictEntry(onEntry),
			)
			for _, k := range []string{"k1", "k2", "k3", "k4"} {
				_, err := cache.Put(k, 1)
				require.NoError(t, err)
			}

			// Act: First verify the end-to-end detached child chain (k1 -> detached k2 -> k3 & k4).
			_, ok := cache.Delete("k1")
			require.True(t, ok)

			select {
			case <-allDone:
			case <-time.After(2 * time.Second):
				require.Failf(t, "deadlock", "backend %v deadlocked during detached child pending callback nested eviction", b)
			}

			// Also deterministically exercise enqueuePendingCallback's direct TryLock() drain path
			// when evictCallbackMu is currently unlocked.
			_, err := cache.Put("k2", 2)
			require.NoError(t, err)
			_, err = cache.Put("k3", 3)
			require.NoError(t, err)
			_, err = cache.Put("k4", 4)
			require.NoError(t, err)

			var p *pressureState
			switch impl := cache.(type) {
			case *mapCache[int]:
				p = &impl.pressureState
			case *radixCache[int]:
				p = &impl.pressureState
			case *arenaRadix[int]:
				p = &impl.pressureState
			}

			directDone := make(chan struct{})
			go func() {
				var q evictCallbackQueue[int]
				q.enqueue("k2", 2, EvictionReasonDeleted)
				q.enqueuePendingCallback(p, nil, onEntry)
				close(directDone)
			}()

			// Assert
			select {
			case <-directDone:
			case <-time.After(2 * time.Second):
				require.Failf(t, "deadlock", "backend %v enqueuePendingCallback TryLock drain self-deadlocked on nested eviction", b)
			}
		})
	}
}

// TestAdv_Bug6_TagCollisionForcesIndependentCacheIntoStackDumpAllocations verifies that when
// more than 8 caches exist (e.g. caches[0] and caches[8]), holding caches[0] inside OnEvict*
// and PressureFunc does not force an uncontended caches[8] onto the slow stack-capture path
// or mark caches[8] as contended.
func TestAdv_Bug6_TagCollisionForcesIndependentCacheIntoStackDumpAllocations(t *testing.T) {
	// Arrange: Create 17 caches so caches[0], caches[8], and caches[16] would collide under modulo-8 indexing.
	const numCaches = 17
	inEvict0 := make(chan struct{})
	releaseEvict0 := make(chan struct{})
	inPressure0 := make(chan struct{})
	releasePressure0 := make(chan struct{})
	var armEvict0, armPressure0 atomic.Bool

	caches := make([]*mapCache[int], numCaches)
	for i := range numCaches {
		idx := i
		c := NewMapCache[int](2,
			WithPressureFunc(func() float64 {
				if idx == 0 && armPressure0.CompareAndSwap(true, false) {
					close(inPressure0)
					<-releasePressure0
				}
				return 0.10
			}),
			WithOnEvictValue(func(_ int, _ EvictionReason) {
				if idx == 0 && armEvict0.CompareAndSwap(true, false) {
					close(inEvict0)
					<-releaseEvict0
				}
			}),
		).(*mapCache[int])
		_, err := c.Put("k1", 1)
		require.NoError(t, err)
		_, err = c.Put("k2", 2)
		require.NoError(t, err)
		caches[i] = c
	}

	armEvict0.Store(true)
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = caches[0].Put("k3", 3) // Evicts k1 on caches[0] and blocks inside OnEvictValue.
	})
	<-inEvict0

	// Act: Perform uncontended evictions and replacements on caches[8] and caches[16] while caches[0] is blocked.
	_, err := caches[8].Put("k3", 3)
	require.NoError(t, err)
	_, err = caches[16].Put("k3", 3)
	require.NoError(t, err)

	allocs8 := testing.AllocsPerRun(50, func() {
		_ = caches[8].Replace("k2", 20)
	})

	close(releaseEvict0)
	wg.Wait()

	armPressure0.Store(true)
	wg.Go(func() {
		_ = caches[0].Replace("k2", 30) // Blocks caches[0] inside PressureFunc.
	})
	<-inPressure0

	_ = caches[8].Replace("k2", 40)
	_ = caches[16].Replace("k2", 40)

	close(releasePressure0)
	wg.Wait()

	// Assert: Neither caches[8] nor caches[16] was forced onto the slow stack-capture path or marked contended.
	assert.Zero(t, allocs8)
	assert.False(t, caches[8].evictCallbackContended.Load(), "uncontended caches[8] must not be marked evictCallbackContended")
	assert.Zero(t, caches[8].evictCallbackGID.Load(), "uncontended caches[8] must not capture evictCallbackGID")
	assert.False(t, caches[8].samplingContended.Load(), "uncontended caches[8] must not be marked samplingContended")
	assert.Zero(t, caches[8].samplingGID.Load(), "uncontended caches[8] must not capture samplingGID")
	assert.False(t, caches[16].evictCallbackContended.Load(), "uncontended caches[16] must not be marked evictCallbackContended")
	assert.False(t, caches[16].samplingContended.Load(), "uncontended caches[16] must not be marked samplingContended")
}

//go:noinline
func runWithWaitGroupWrapper(wg *sync.WaitGroup, fn func()) {
	wg.Go(func() {
		fn()
	})
}

// TestAdv_Iter2_WaitGroupGoWrapperHelperDropsCallerFrame verifies Bug Iter2-1:
// when sync.WaitGroup.Go is wrapped inside a helper function (pushing the caller's
// closure to the third bottom stack frame fn2 above "created by"), child and
// grandchild goroutines spawned inside OnEvict* or PressureFunc are still recognized
// as descendants (even when an intermediate goroutine has exited), while pre-existing
// workers spawned via the same wrapper before cache.Put are not misclassified as children.
func TestAdv_Iter2_WaitGroupGoWrapperHelperDropsCallerFrame(t *testing.T) {
	t.Run("ExitedIntermediateGoroutineSpawningViaWaitGroupGoWrapperDeadlocksOnEvictAndBypassesPressureFunc", func(t *testing.T) {
		// Arrange
		var cache Cache[int]
		var pressureCalls, evictCalls atomic.Int32
		var firstPressure, firstEvict atomic.Bool
		firstPressure.Store(true)
		firstEvict.Store(true)

		cache = NewMapCache[int](1,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				pressureCalls.Add(1)
				if firstPressure.CompareAndSwap(true, false) {
					g2Exited := make(chan struct{})
					g3Done := make(chan struct{})
					var wg sync.WaitGroup
					go func() {
						runWithWaitGroupWrapper(&wg, func() {
							defer close(g3Done)
							<-g2Exited
							time.Sleep(5 * time.Millisecond)
							_, _ = cache.Delete("missing_from_wg_wrapper")
						})
						close(g2Exited)
					}()
					<-g3Done
					wg.Wait()
				}
				return 0.10
			}),
			WithOnEvictValue(func(_ int, _ EvictionReason) {
				evictCalls.Add(1)
				if firstEvict.CompareAndSwap(true, false) {
					g2Exited := make(chan struct{})
					g3Done := make(chan struct{})
					var wg sync.WaitGroup
					go func() {
						runWithWaitGroupWrapper(&wg, func() {
							defer close(g3Done)
							<-g2Exited
							time.Sleep(5 * time.Millisecond)
							_, putErr := cache.Put("k3_from_wg_wrapper", 3)
							assert.NoError(t, putErr)
						})
						close(g2Exited)
					}()
					<-g3Done
					wg.Wait()
				}
			}),
		)

		// Act
		_, err := cache.Put("k1", 1)
		require.NoError(t, err)
		callsAfterFirstPut := pressureCalls.Load()

		putDone := make(chan struct{})
		go func() {
			_, putErr := cache.Put("k2", 2)
			assert.NoError(t, putErr)
			close(putDone)
		}()

		// Assert
		select {
		case <-putDone:
		case <-time.After(2 * time.Second):
			require.Fail(t, "deadlock: OnEvictValue deadlocked on evictCallbackMu when exited child spawned grandchild via wg.Go wrapper")
		}
		assert.Equal(t, int32(1), callsAfterFirstPut, "grandchild spawned via wg.Go wrapper after intermediate child exited must not bypass PressureFunc guard")
		assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and k2 eviction callbacks must complete")
	})

	t.Run("PreExistingWorkerViaWaitGroupGoWrapperMisclassifiedWhenCallbackUsesWaitGroup", func(t *testing.T) {
		// Arrange: Spawn a pre-existing worker via runWithWaitGroupWrapper before cache.Put,
		// and also use a local sync.WaitGroup inside PressureFunc so "\nsync.(*WaitGroup)."
		// is present in aboveHook while the pre-existing worker accesses the cache.
		var cache Cache[int]
		var calls atomic.Int32
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})
		var workerErr error
		var preWG sync.WaitGroup

		runWithWaitGroupWrapper(&preWG, func() {
			<-workerTrigger
			_, workerErr = cache.Put("worker_key", 2)
			close(workerDone)
		})

		var primaryOnce sync.Once
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			c := calls.Add(1)
			if c == 1 {
				var innerWG sync.WaitGroup
				innerWG.Go(func() {
					primaryOnce.Do(func() {
						close(workerTrigger)
					})
					deadline := time.Now().Add(200 * time.Millisecond)
					for calls.Load() < 2 && time.Now().Before(deadline) {
						runtime.Gosched()
					}
				})
				innerWG.Wait()
			}
			return 0.10
		}))

		// Act
		_, err := cache.Put("parent_key", 1)
		require.NoError(t, err)
		<-workerDone
		preWG.Wait()
		require.NoError(t, workerErr)

		// Assert
		assert.Equal(t, int32(2), calls.Load(), "pre-existing worker spawned via wg.Go wrapper before cache.Put was misclassified as child of PressureFunc")
	})
}

//go:noinline
func startPreExistingWorkerHelper(trigger <-chan struct{}, fn func()) {
	go func() {
		<-trigger
		fn()
	}()
}

// TestAdv_Iter2_PreExistingWorkerSpawnedViaHelperOrSetupClosureMisclassifiedAsChild
// verifies Bug Iter2-2: pre-existing background goroutines spawned by the same parent
// goroutine before cache.Delete or cache.Put via a helper function or local setup closure
// are not misclassified as re-entrant children of OnEvict* or PressureFunc.
func TestAdv_Iter2_PreExistingWorkerSpawnedViaHelperOrSetupClosureMisclassifiedAsChild(t *testing.T) {
	t.Run("PreExistingWorkerStartedViaHelperBeforeDeleteReturnsBeforeOnEvictRuns", func(t *testing.T) {
		// Arrange
		startWorker := make(chan struct{})
		workerDone := make(chan struct{})

		var cache Cache[int]
		var k2EvictCompleted atomic.Bool
		var k2CompletedBeforeDeleteReturn atomic.Bool

		startPreExistingWorkerHelper(startWorker, func() {
			_, ok := cache.Delete("k2")
			assert.True(t, ok)
			k2CompletedBeforeDeleteReturn.Store(k2EvictCompleted.Load())
			close(workerDone)
		})

		var k1Once sync.Once
		cache = NewMapCache[int](10, WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
			switch key {
			case "k1":
				k1Once.Do(func() {
					close(startWorker)
					time.Sleep(10 * time.Millisecond)
				})
			case "k2":
				time.Sleep(15 * time.Millisecond)
				k2EvictCompleted.Store(true)
			}
		}))
		_, err := cache.Put("k1", 1)
		require.NoError(t, err)
		_, err = cache.Put("k2", 2)
		require.NoError(t, err)

		// Act: Call Delete("k1") directly on the same parent goroutine that started the helper worker.
		_, ok := cache.Delete("k1")
		require.True(t, ok)
		<-workerDone

		// Assert
		assert.True(t, k2CompletedBeforeDeleteReturn.Load(), "pre-existing worker's Delete(k2) returned BEFORE OnEvict(k2) executed (misclassified as child of OnEvict(k1))")
	})

	t.Run("PreExistingWorkerStartedViaHelperBeforePutMisclassifiedAsChildOfSampler", func(t *testing.T) {
		// Arrange
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})
		var cache *mapCache[int]
		var calls atomic.Int32
		var workerErr error

		startPreExistingWorkerHelper(workerTrigger, func() {
			_, _ = cache.Delete("k_reclaim")
			_, workerErr = cache.Put("worker_key", 2)
			close(workerDone)
		})

		var armOnce sync.Once
		var armed atomic.Bool
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.95
			}
			c := calls.Add(1)
			if c == 1 {
				armOnce.Do(func() {
					close(workerTrigger)
				})
				<-workerDone
			}
			return 0.95
		})).(*mapCache[int])

		_, err := cache.Put("k_reclaim", 1)
		require.NoError(t, err)
		epochBefore := cache.externalReclaimEpoch.Load()
		armed.Store(true)

		// Act
		_, err = cache.Put("parent_key", 1)
		require.NoError(t, err)
		require.NoError(t, workerErr)

		// Assert
		assert.GreaterOrEqual(t, calls.Load(), int32(2), "pre-existing worker spawned via helper before cache.Put was misclassified as a child of PressureFunc")
		assert.Greater(t, cache.externalReclaimEpoch.Load(), epochBefore, "pre-existing worker Delete was misclassified as child sampler reclaim and failed to increment externalReclaimEpoch")
	})

	t.Run("PreExistingWorkerStartedViaSetupClosureBeforePutMisclassifiedAsChildOfSampler", func(t *testing.T) {
		// Arrange
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})
		var cache Cache[int]
		var calls atomic.Int32
		var workerErr error

		startSetup := func() {
			go func() {
				<-workerTrigger
				_, workerErr = cache.Put("worker_key", 2)
				close(workerDone)
			}()
		}
		startSetup()

		var primaryOnce sync.Once
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			c := calls.Add(1)
			if c == 1 {
				primaryOnce.Do(func() {
					close(workerTrigger)
				})
				deadline := time.Now().Add(200 * time.Millisecond)
				for calls.Load() < 2 && time.Now().Before(deadline) {
					runtime.Gosched()
				}
			}
			return 0.10
		}))

		// Act
		_, err := cache.Put("parent_key", 1)
		require.NoError(t, err)
		<-workerDone
		require.NoError(t, workerErr)

		// Assert
		assert.Equal(t, int32(2), calls.Load(), "pre-existing worker spawned via setup closure before cache.Put was misclassified as a child of PressureFunc")
	})
}

//go:noinline
func runGenericWaitGroupWorkerScenario[T any](t *testing.T, val T) {
	t.Helper()

	// Arrange: Both the pre-existing wg.Go worker (.func1) and PressureFunc (.func2)
	// are closures inside the same generic function runGenericWaitGroupWorkerScenario[...].
	var cache Cache[T]
	var calls atomic.Int32
	workerTrigger := make(chan struct{})
	workerDone := make(chan struct{})
	var workerErr error
	var wg sync.WaitGroup

	wg.Go(func() {
		<-workerTrigger
		_, workerErr = cache.Put("worker_key", val)
		close(workerDone)
	})

	var primaryOnce sync.Once
	cache = NewMapCache[T](10, WithPressureFunc(func() float64 {
		c := calls.Add(1)
		if c == 1 {
			var childWG sync.WaitGroup
			childWG.Go(func() {
				_, _ = cache.Delete("missing_generic_child")
			})
			childWG.Wait()

			primaryOnce.Do(func() {
				close(workerTrigger)
			})
			deadline := time.Now().Add(200 * time.Millisecond)
			for calls.Load() < 2 && time.Now().Before(deadline) {
				runtime.Gosched()
			}
		}
		return 0.10
	}))

	// Act
	_, err := cache.Put("parent_key", val)
	require.NoError(t, err)
	<-workerDone
	wg.Wait()
	require.NoError(t, workerErr)

	// Assert
	assert.Equal(t, int32(2), calls.Load(), "pre-existing wg.Go worker in generic function must not be misclassified as child of PressureFunc due to '[' truncation in trimFrameFuncName")
}

// TestAdv_Iter2_GenericFunctionClosuresTruncatedByTrimFrameFuncName verifies Bug Iter2-3:
// trimFrameFuncName skips generic type parameter brackets "[...]" rather than truncating at '[',
// so a pre-existing wg.Go worker closure (fn[...].func1) is not confused with a PressureFunc
// closure (fn[...].func2) defined in the same generic function.
func TestAdv_Iter2_GenericFunctionClosuresTruncatedByTrimFrameFuncName(t *testing.T) {
	runGenericWaitGroupWorkerScenario(t, 42)
}

// TestAdv_Iter2_PanickingPendingCallbackLeavesNestedChildQueueLeaked verifies Bug Iter2-4:
// when a drained pending child callback spawns another nested child goroutine that enqueues
// a second-generation callback into pendingEvictCallbacks and the drained callback then panics,
// unlockEvictCallbackSlow clears pendingEvictCallbacks and resets evictPendingEnqueues to 0.
func TestAdv_Iter2_PanickingPendingCallbackLeavesNestedChildQueueLeaked(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange
			var cache Cache[*int]
			cache = New[*int](10,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictEntry(func(key string, _ *int, _ EvictionReason) {
					switch key {
					case "k1":
						var wg sync.WaitGroup
						wg.Go(func() {
							_, _ = cache.Delete("k2")
						})
						wg.Wait()
					case "k2":
						var wg sync.WaitGroup
						wg.Go(func() {
							_, _ = cache.Delete("k3")
						})
						wg.Wait()
						panic("simulated panic after nested child enqueue")
					}
				}),
			)

			wpV3 := populateIter2Bug4Cache(t, cache)
			p := extractPressureStateForIter2Test(cache)

			// Act
			require.PanicsWithValue(t, "simulated panic after nested child enqueue", func() {
				_, _ = cache.Delete("k1")
			})

			for range 6 {
				runtime.GC()
				runtime.Gosched()
			}

			done := make(chan struct{})
			go func() {
				_, ok := cache.Delete("k4")
				assert.True(t, ok)
				close(done)
			}()

			// Assert
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				require.Failf(t, "deadlock", "backend %v deadlocked on subsequent Delete after panicking pending callback", b)
			}
			assert.Zero(t, p.evictPendingEnqueues.Load(), "evictPendingEnqueues must be cleared when drainPendingEvictCallbacks unwinds on panic")
			assert.Nil(t, wpV3.Value(), "nested pending callback closure and its captured value v3 must be cleared on panic")
		})
	}
}

//go:noinline
func populateIter2Bug4Cache(t *testing.T, cache Cache[*int]) weak.Pointer[int] {
	t.Helper()
	v1, v2, v3, v4 := new(int), new(int), new(int), new(int)
	wpV3 := weak.Make(v3)
	_, err := cache.Put("k1", v1)
	require.NoError(t, err)
	_, err = cache.Put("k2", v2)
	require.NoError(t, err)
	_, err = cache.Put("k3", v3)
	require.NoError(t, err)
	_, err = cache.Put("k4", v4)
	require.NoError(t, err)
	return wpV3
}

func extractPressureStateForIter2Test[V any](cache Cache[V]) *pressureState {
	switch impl := cache.(type) {
	case *mapCache[V]:
		return &impl.pressureState
	case *radixCache[V]:
		return &impl.pressureState
	case *arenaRadix[V]:
		return &impl.pressureState
	default:
		return nil
	}
}

type methodChildWorker struct {
	cache   Cache[int]
	putDone chan struct{}
	putErr  error
}

//go:noinline
func (w *methodChildWorker) deleteFromChild() {
	_, _ = w.cache.Delete("missing_from_named_method")
}

//go:noinline
func (w *methodChildWorker) putFromChild() {
	defer close(w.putDone)
	_, w.putErr = w.cache.Put("k3_from_named_method", 3)
}

// TestAdv_Iter3_NamedMethodAndOuterWrapperChildGoroutines verifies Iteration 3 Findings 1 & 2:
// (1) spawning child goroutines inside OnEvict* and PressureFunc via named methods or method
// values (go w.putFromChild() and wg.Go(w.deleteFromChild)) does not match shared go-lru
// entry frames in belowHook, and (2) spawning child goroutines via a returning outer helper
// closure that wraps go func() { fn() }() does not abort at Frame 0 before matching the
// callback's closure owner at Frame 1 in aboveHook.
func TestAdv_Iter3_NamedMethodAndOuterWrapperChildGoroutines(t *testing.T) {
	t.Run("MethodValueInWaitGroupGoAndGoStatementInsideOnEvictAndPressureFunc", func(t *testing.T) {
		// Arrange
		var pressureCalls, evictCalls atomic.Int32
		var firstPressure, firstEvict, evictDeadlocked atomic.Bool
		firstPressure.Store(true)
		firstEvict.Store(true)

		w := &methodChildWorker{
			putDone: make(chan struct{}),
		}

		w.cache = NewMapCache[int](1,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				pressureCalls.Add(1)
				if firstPressure.CompareAndSwap(true, false) {
					var wg sync.WaitGroup
					wg.Go(w.deleteFromChild)
					wg.Wait()
				}
				return 0.10
			}),
			WithOnEvictValue(func(_ int, _ EvictionReason) {
				evictCalls.Add(1)
				if firstEvict.CompareAndSwap(true, false) {
					go w.putFromChild()
					select {
					case <-w.putDone:
					case <-time.After(500 * time.Millisecond):
						evictDeadlocked.Store(true)
					}
					var wg sync.WaitGroup
					wg.Go(w.deleteFromChild)
					wg.Wait()
				}
			}),
		)

		// Act
		_, err := w.cache.Put("k1", 1)
		require.NoError(t, err)
		callsAfterFirstPut := pressureCalls.Load()

		_, err = w.cache.Put("k2", 2)
		require.NoError(t, err)
		<-w.putDone

		// Assert
		require.False(t, evictDeadlocked.Load(), "deadlock: OnEvictValue deadlocked when spawning child via go w.putFromChild()")
		require.NoError(t, w.putErr)
		assert.Equal(t, int32(1), callsAfterFirstPut, "child spawned via wg.Go(w.deleteFromChild) must not bypass PressureFunc re-entrancy guard")
		assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and k2 eviction callbacks must complete")
	})

	t.Run("ReturningOuterClosureSpawningInnerGoFuncWrapper", func(t *testing.T) {
		// Arrange
		spawnReturningInnerGoFunc := func(fn func()) <-chan struct{} {
			done := make(chan struct{})
			go func() {
				defer close(done)
				fn()
			}()
			return done
		}

		var cache Cache[int]
		var pressureCalls, evictCalls atomic.Int32
		var firstPressure, firstEvict, evictDeadlocked atomic.Bool
		var childDone <-chan struct{}
		var childPutErr error
		firstPressure.Store(true)
		firstEvict.Store(true)

		cache = NewMapCache[int](1,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				pressureCalls.Add(1)
				if firstPressure.CompareAndSwap(true, false) {
					<-spawnReturningInnerGoFunc(func() {
						_, _ = cache.Delete("missing_from_outer_closure_wrapper")
					})
				}
				return 0.10
			}),
			WithOnEvictValue(func(_ int, _ EvictionReason) {
				evictCalls.Add(1)
				if firstEvict.CompareAndSwap(true, false) {
					childDone = spawnReturningInnerGoFunc(func() {
						_, childPutErr = cache.Put("k3_from_outer_closure_wrapper", 3)
					})
					select {
					case <-childDone:
					case <-time.After(500 * time.Millisecond):
						evictDeadlocked.Store(true)
					}
				}
			}),
		)

		// Act
		_, err := cache.Put("k1", 1)
		require.NoError(t, err)
		callsAfterFirstPut := pressureCalls.Load()

		_, err = cache.Put("k2", 2)
		require.NoError(t, err)
		<-childDone

		// Assert
		require.False(t, evictDeadlocked.Load(), "deadlock: OnEvictValue deadlocked when spawning child via returning outer helper closure")
		require.NoError(t, childPutErr)
		assert.Equal(t, int32(1), callsAfterFirstPut, "returning outer helper closure using go func() { fn() }() must not bypass PressureFunc re-entrancy guard")
		assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and k2 eviction callbacks must complete")
	})
}
