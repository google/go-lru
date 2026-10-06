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
