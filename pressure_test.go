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
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hasKey[V any](c Cache[V], key string) bool {
	_, ok := c.Peek(key)
	return ok
}

func TestPressure_CriticalPressureSheddingAndZeroSizeRetention(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("PreservesZeroSizeEntriesAfterByteTargetMet", func(t *testing.T) {
				// Arrange
				pressure := 0.0
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				// Put 1 positive-size entry (60B) at LRU tail, followed by 50 zero-size entries toward MRU.
				_, err := cache.Put("lru_60", testData{value: 1, dataSize: 60})
				require.NoError(t, err)
				for i := range 50 {
					_, err = cache.Put("zero_"+string(rune('A'+i)), testData{value: 0, dataSize: 0})
					require.NoError(t, err)
				}

				// Act: trigger critical-pressure shedding (targetSize = 50, targetZeroCount = 25).
				pressure = 0.95
				evicted := cache.EvaluateMemoryPressure()

				// Assert: "lru_60" plus the 25 oldest zero-size entries are shed (26 total),
				// while the 25 newest zero-size entries remain in the cache.
				require.Len(t, evicted, 26)
				assert.False(t, hasKey(cache, "lru_60"))
				assert.False(t, hasKey(cache, "zero_A"))
				assert.True(t, hasKey(cache, "zero_"+string(rune('A'+49))))
			})

			t.Run("ManyZeroPlusOneByteShedsZeroSizeEntries", func(t *testing.T) {
				// Arrange
				pressure := 0.0
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				for i := range 100 {
					_, err := cache.Put(fmt.Sprintf("z-%03d", i), testData{value: 0, dataSize: 0})
					require.NoError(t, err)
				}
				_, err := cache.Put("one_byte", testData{value: 1, dataSize: 1})
				require.NoError(t, err)

				// Act: Critical pressure must shed ~50 zero-size entries even though currentSize == 1 <= targetSize (50).
				pressure = 0.95
				evicted := cache.EvaluateMemoryPressure()

				// Assert
				assert.GreaterOrEqual(t, len(evicted), 50)
				assert.True(t, hasKey(cache, "one_byte"))
			})

			t.Run("ZeroAtTailPlusLargeMRU", func(t *testing.T) {
				// Arrange
				pressure := 0.0
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				for i := range 10 {
					_, err := cache.Put(fmt.Sprintf("z-%02d", i), testData{value: 0, dataSize: 0})
					require.NoError(t, err)
				}
				_, err := cache.Put("mru_60", testData{value: 1, dataSize: 60})
				require.NoError(t, err)

				// Act: Critical pressure (targetSize = 50, targetLen = 5) must shed mru_60 and retain 5 zero-size entries.
				pressure = 0.95
				evicted := cache.EvaluateMemoryPressure()

				// Assert
				assert.Len(t, evicted, 6)
				assert.False(t, hasKey(cache, "mru_60"))
				assert.True(t, hasKey(cache, "z-09"))
			})

			t.Run("ForegroundZeroSizeShedding", func(t *testing.T) {
				// Arrange: Configure critical pressure (0.95) and 50% retention.
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return 0.95 }),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
				)

				// Act: Put 100 zero-size entries via foreground Put under critical pressure.
				totalEvicted := 0
				for i := range 100 {
					evicted, err := cache.Put(fmt.Sprintf("fg_zero_%03d", i), testData{value: 0, dataSize: 0})
					require.NoError(t, err)
					totalEvicted += len(evicted)
				}

				// Assert: Foreground writes shed 99 older zero-size entries, retaining only the latest MRU entry.
				assert.Equal(t, 99, totalEvicted)
				assert.False(t, hasKey(cache, "fg_zero_000"))
				assert.True(t, hasKey(cache, "fg_zero_099"))
			})

			t.Run("PositiveTailPlusZeroSizeEntriesShedTogether", func(t *testing.T) {
				// Arrange: Put 1 entry of 51B at tail (targetSize = 50B) followed by 100 zero-size entries.
				pressure := 0.0
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				_, err := cache.Put("tail_51", testData{value: 1, dataSize: 51})
				require.NoError(t, err)
				for i := range 100 {
					_, err = cache.Put(fmt.Sprintf("z_%03d", i), testData{value: 0, dataSize: 0})
					require.NoError(t, err)
				}

				// Act: EvaluateMemoryPressure under 0.95 must shed both "tail_51" and 50 zero-size entries (51 total).
				pressure = 0.95
				evicted := cache.EvaluateMemoryPressure()

				// Assert
				assert.Len(t, evicted, 51)
				assert.False(t, hasKey(cache, "tail_51"))
				assert.False(t, hasKey(cache, "z_000"))
				assert.True(t, hasKey(cache, "z_099"))
			})

			t.Run("PreservesPositiveEntriesBelowTargetSizeAndIsIdempotent", func(t *testing.T) {
				// Arrange: 50 positive-size 10B entries (500B == targetSize) + 2 zero-size entries at MRU.
				pressure := 0.0
				cache := b.fn(
					1000,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				for i := range 50 {
					_, err := cache.Put(fmt.Sprintf("pos_%02d", i), testData{value: 1, dataSize: 10})
					require.NoError(t, err)
				}
				_, err := cache.Put("zero_1", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				_, err = cache.Put("zero_2", testData{value: 0, dataSize: 0})
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
				assert.True(t, hasKey(cache, "pos_00"))
				assert.True(t, hasKey(cache, "pos_49"))
				assert.False(t, hasKey(cache, "zero_1"))
				assert.True(t, hasKey(cache, "zero_2"))
			})

			t.Run("SinglePassCursorSkipsRetainedZeroSizeTailEntries", func(t *testing.T) {
				// Arrange: Populate 1,000 zero-size entries at the LRU tail followed by 1x1B entry ("p_seed")
				// and 500x1B entries ("p_0000".."p_0499") toward MRU (501B total > targetSize 500B).
				pressure := 0.0
				cache := b.fn(
					1000,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				for i := range 1000 {
					_, err := cache.Put(fmt.Sprintf("z_%04d", i), testData{value: 0, dataSize: 0})
					require.NoError(t, err)
				}
				_, err := cache.Put("p_seed", testData{value: 1, dataSize: 1})
				require.NoError(t, err)
				for i := range 500 {
					_, err = cache.Put(fmt.Sprintf("p_%04d", i), testData{value: 1, dataSize: 1})
					require.NoError(t, err)
				}

				// First critical-pressure evaluation sheds "p_seed" (1B) + 500 oldest zero-size entries ("z_0000".."z_0499"),
				// naturally establishing the retained zero-size watermark at 500 while keeping "z_0500".."z_0999" at the LRU tail.
				pressure = 0.95
				evictedInitial := cache.EvaluateMemoryPressure()
				require.Len(t, evictedInitial, 501)

				// Grow the MRU entry by +100B under critical pressure (bringing currentSize to 600B > targetSize 500B),
				// which sheds 100x1B entries ("p_0000".."p_0099") past the 500 retained zero-size tail entries in a single pass.
				err = cache.Replace("p_0499", testData{value: 1, dataSize: 101})
				require.NoError(t, err)

				// Assert: All 500 retained zero-size tail entries survive while the 100 oldest 1B entries were evicted.
				assert.True(t, hasKey(cache, "z_0500"))
				assert.True(t, hasKey(cache, "z_0999"))
				assert.False(t, hasKey(cache, "p_0000"))
				assert.False(t, hasKey(cache, "p_0099"))
				assert.True(t, hasKey(cache, "p_0100"))
				assert.True(t, hasKey(cache, "p_0499"))
			})
		})
	}
}

func TestPressure_ForegroundMutations(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("ForegroundPutShedsOldestEntriesAcrossAllBackends", func(t *testing.T) {
				// Arrange: Configure critical pressure (0.95) and 50% retention (targetSize = 50B).
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return 0.95 }),
					WithEvictionRetentionRatio(0.50),
				)

				// Act: Put 4 entries of 20B each (80B total > 50B targetSize).
				_, err := cache.Put("k1", testData{value: 1, dataSize: 20})
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 20})
				require.NoError(t, err)
				_, err = cache.Put("k3", testData{value: 3, dataSize: 20})
				require.NoError(t, err)
				_, err = cache.Put("k4", testData{value: 4, dataSize: 20})
				require.NoError(t, err)

				// Assert: Foreground pressure reclamation sheds oldest entries (k1, k2) across all 3 backends.
				assert.False(t, hasKey(cache, "k1"))
				assert.False(t, hasKey(cache, "k2"))
				assert.True(t, hasKey(cache, "k3"))
				assert.True(t, hasKey(cache, "k4"))
			})

			t.Run("ForegroundDeleteTriggersTier2SheddingOfRemainingExcess", func(t *testing.T) {
				// Arrange: Put 4 entries of 20B (80B total) at 0.0 pressure, then raise pressure to 0.95 (targetSize = 50B).
				pressure := 0.0
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
				)

				_, err := cache.Put("p/1", testData{value: 1, dataSize: 20})
				require.NoError(t, err)
				_, err = cache.Put("p/2", testData{value: 2, dataSize: 20})
				require.NoError(t, err)
				_, err = cache.Put("other/3", testData{value: 3, dataSize: 20})
				require.NoError(t, err)
				_, err = cache.Put("other/4", testData{value: 4, dataSize: 20})
				require.NoError(t, err)

				// Act: Delete "other/4" (leaving 60B > targetSize 50B) under 0.95 critical pressure.
				pressure = 0.95
				_, ok := cache.Delete("other/4")

				// Assert: Foreground Delete also triggers Tier 2 shedding of "p/1" (oldest 20B) so currentSize drops to 40B <= 50B.
				require.True(t, ok)
				assert.False(t, hasKey(cache, "p/1"))
				assert.True(t, hasKey(cache, "p/2"))
				assert.True(t, hasKey(cache, "other/3"))
			})

			t.Run("ReplaceStrictLRUEvictsOldestTailEntry", func(t *testing.T) {
				// Arrange: k1 (20B) at LRU tail, k2 (30B) at MRU head, maxSize = 100, targetSize = 50.
				pressure := 0.10
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
				)

				_, err := cache.Put("k1", testData{value: 1, dataSize: 20})
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 30})
				require.NoError(t, err)

				// Act: Raise pressure to 0.95 and grow LRU tail entry k1 by +10 (20 -> 30, currentSize = 60 > 50).
				pressure = 0.95
				err = cache.Replace("k1", testData{value: 1, dataSize: 30})

				// Assert: Strict LRU eviction order must evict k1 (oldest at LRU tail) and retain k2 (newest at MRU head).
				require.NoError(t, err)
				assert.False(t, hasKey(cache, "k1"))
				assert.True(t, hasKey(cache, "k2"))
			})

			t.Run("ProtectedMRUEntryAboveTargetSizeDoesNotSpuriouslyAdvanceEpoch", func(t *testing.T) {
				// Arrange: Empty cache with maxSize=1000, retention=0.50 (targetSize=500), and critical pressure 0.95.
				probe := newPressureProbe(0.95)
				cache := b.fn(
					1000,
					WithInvariantChecking(true),
					WithEvictionRetentionRatio(0.50),
					probe.Option(),
				).(PressureAwareCache[testData])

				// Act: Put a single 800B entry (> targetSize 500B). Because it is the protected MRU head
				// and there are no older entries or dirty index buckets, 0 evictions and 0 compactions occur.
				var evicted []testData
				var err error
				advanced := observeEpochAdvance(t, probe, cache, func() {
					evicted, err = cache.Put("protected_jumbo", testData{value: 1, dataSize: 800})
				})

				// Assert: The reclamation epoch must not advance because no memory was evicted or compacted.
				require.NoError(t, err)
				assert.Empty(t, evicted)
				assert.False(t, advanced)
			})

			t.Run("ReplaceWithZeroDeltaProtectsMRUHeadUnderCriticalPressure", func(t *testing.T) {
				// Arrange: Put "head" (800B > targetSize 500B) under critical pressure (0.95).
				cache := b.fn(
					1000,
					WithInvariantChecking(true),
					WithEvictionRetentionRatio(0.50),
					WithPressureFunc(func() float64 { return 0.95 }),
				)
				_, err := cache.Put("head", testData{value: 1, dataSize: 800})
				require.NoError(t, err)
				require.True(t, hasKey(cache, "head"))

				// Act: Call Replace("head", 800B) on the MRU head entry.
				err = cache.Replace("head", testData{value: 2, dataSize: 800})

				// Assert: Updating the MRU head with sizeDelta == 0 must protect the MRU head just like sizeDelta > 0.
				require.NoError(t, err)
				assert.True(t, hasKey(cache, "head"))
			})

			t.Run("ZeroByteErasureAndSelfEvictionDoNotSpuriouslyAdvanceReclaimEpoch", func(t *testing.T) {
				// Arrange: Populate 100B cache with 3 zero-size tail entries and 1 100B MRU entry under Tier 1 pressure (0.80).
				probe := newPressureProbe(0.80)
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					probe.Option(),
				).(PressureAwareCache[testData])
				_, err := cache.Put("z_erase", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				_, err = cache.Put("z_prefix/1", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				_, err = cache.Put("z_self_evict", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				_, err = cache.Put("mru_full", testData{value: 1, dataSize: 100})
				require.NoError(t, err)

				// Act: Delete a 0B entry, prefix-erase a 0B entry, and self-evict a 0B entry via Replace (+10B when avail == 0).
				advanced := observeEpochAdvance(t, probe, cache, func() {
					_, _ = cache.Delete("z_erase")
					cache.DeletePrefix("z_prefix/")
					err = cache.Replace("z_self_evict", testData{value: 1, dataSize: 10})
				})
				require.NoError(t, err)

				// Assert: Because currentSize remained 100B (0 bytes freed) and no compaction ran, the reclamation epoch must not advance.
				assert.False(t, advanced)
				assert.True(t, hasKey(cache, "mru_full"))
			})

			t.Run("ReplaceNetByteGrowthUnderTier1PressureDoesNotAdvanceReclaimEpoch", func(t *testing.T) {
				// Arrange: Put "small" (10B) and "target" (20B) in a 100B cache under Tier 1 pressure (0.80).
				probe := newPressureProbe(0.80)
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					probe.Option(),
				).(PressureAwareCache[testData])

				_, err := cache.Put("small", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				_, err = cache.Put("target", testData{value: 1, dataSize: 20})
				require.NoError(t, err)

				// Act: Grow "target" by +80B (to 100B). This evicts "small" (10B), so net currentSize increases from 30B to 100B.
				advanced := observeEpochAdvance(t, probe, cache, func() {
					err = cache.Replace("target", testData{value: 1, dataSize: 100})
				})

				// Assert: Because net cache memory grew from 30B to 100B and no compaction occurred, the reclamation epoch must not advance.
				require.NoError(t, err)
				assert.False(t, advanced)
				assert.False(t, hasKey(cache, "small"))
				require.True(t, hasKey(cache, "target"))
				assert.NoError(t, cache.Replace("target", testData{value: 2, dataSize: 100}))
			})
		})
	}
}

func TestPressure_KeyAndPrefixMissesDoNotEvictLiveEntriesUnderCriticalPressure(t *testing.T) {
	missOps := []struct {
		name string
		act  func(t *testing.T, c Cache[testData])
	}{
		{
			name: "DeleteMiss",
			act: func(t *testing.T, c Cache[testData]) {
				t.Helper()
				_, ok := c.Delete("absent_key")
				assert.False(t, ok)
			},
		},
		{
			name: "ReplaceMiss",
			act: func(t *testing.T, c Cache[testData]) {
				t.Helper()
				err := c.Replace("absent_key", testData{value: 1, dataSize: 10})
				assert.ErrorIs(t, err, ErrEntryNotExist)
			},
		},
		{
			name: "DeletePrefixMiss",
			act: func(t *testing.T, c Cache[testData]) {
				t.Helper()
				c.DeletePrefix("absent_prefix/")
			},
		},
	}

	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			for _, op := range missOps {
				t.Run(op.name, func(t *testing.T) {
					// Arrange: Populate cache with "victim" (300B) and "protected_head" (500B) at low pressure (0.10)
					// so currentSize is 800B > targetSize (500B), then raise pressure to 0.95 before the miss operation.
					pressure := 0.10
					cache := b.fn(
						1000,
						WithInvariantChecking(true),
						WithEvictionRetentionRatio(0.50),
						WithPressureFunc(func() float64 { return pressure }),
					)

					_, err := cache.Put("victim", testData{value: 1, dataSize: 300})
					require.NoError(t, err)

					_, err = cache.Put("protected_head", testData{value: 2, dataSize: 500})
					require.NoError(t, err)
					require.True(t, hasKey(cache, "victim"))
					require.True(t, hasKey(cache, "protected_head"))

					// Act: Perform a key/prefix miss under critical pressure.
					pressure = 0.95
					op.act(t, cache)

					// Assert: A key or prefix miss must be a state no-op and must not evict live entries.
					assert.True(t, hasKey(cache, "victim"))
					assert.True(t, hasKey(cache, "protected_head"))
				})
			}
		})
	}
}

func TestPressure_ShedAndCompactZeroSizeWatermarkAccuracyWhenTargetLenStopsShedding(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("ProtectedMRUHeadPutThenEvaluate", func(t *testing.T) {
				// Arrange: Populate 100B cache at 0.10 pressure with 5x8B positive-size entries (40B total)
				// followed by 4 zero-size entries.
				pressure := 0.10
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
					WithPressureFunc(func() float64 { return pressure }),
				).(PressureAwareCache[testData])

				for i := range 5 {
					_, err := cache.Put(fmt.Sprintf("p8_%d", i), testData{value: 1, dataSize: 8})
					require.NoError(t, err)
				}
				for i := range 4 {
					_, err := cache.Put(fmt.Sprintf("z_%d", i), testData{value: 0, dataSize: 0})
					require.NoError(t, err)
				}

				// Act: Under critical pressure (0.95), insert "mru_60" (60B), then call EvaluateMemoryPressure().
				pressure = 0.95
				evicted1, err := cache.Put("mru_60", testData{value: 1, dataSize: 60})
				require.NoError(t, err)
				require.Len(t, evicted1, 5)

				evicted2 := cache.EvaluateMemoryPressure()

				// Assert: EvaluateMemoryPressure must evict "mru_60" plus 2 zero-size entries (3 total),
				// leaving 2 zero-size entries in the cache.
				assert.Len(t, evicted2, 3)
				assert.False(t, hasKey(cache, "z_0"))
				assert.False(t, hasKey(cache, "z_1"))
				assert.True(t, hasKey(cache, "z_2"))
				assert.True(t, hasKey(cache, "z_3"))
			})
		})
	}
}

func TestPressure_DeleteEmptyPrefixInvalidationAndEmptyNoOp(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("NonEmptyCacheDeleteEmptyPrefixInvalidatesStaleCachedPressure", func(t *testing.T) {
				// Arrange: Populate cache and seed critical pressure (0.95) before resetting via DeletePrefix("").
				probe := newPressureProbe(0.10)
				c := b.fn(
					200,
					WithInvariantChecking(true),
					WithEvictionRetentionRatio(0.50),
					probe.Option(),
				).(PressureAwareCache[testData])

				_, err := c.Put("pre_1", testData{value: 1, dataSize: 40})
				require.NoError(t, err)

				probe.Set(0.95)
				_ = c.EvaluateMemoryPressure()
				probe.Set(0.10)

				// Act: DeletePrefix("") on a non-empty cache must advance the reclamation epoch and invalidate cached pressure.
				advanced := observeEpochAdvance(t, probe, c, func() {
					c.DeletePrefix("")
				})

				// Assert
				assert.True(t, advanced)
				_, err = c.Put("post_1", testData{value: 1, dataSize: 60})
				require.NoError(t, err)
				_, err = c.Put("post_2", testData{value: 2, dataSize: 60})
				require.NoError(t, err)
				assert.True(t, hasKey(c, "post_1"))
				assert.True(t, hasKey(c, "post_2"))
			})

			t.Run("InFlightSampleAcrossReclamationDoesNotClearPressureNeedsRefresh", func(t *testing.T) {
				// Arrange: Goroutine 1 starts sampling 0.95 before DeletePrefix("") reclaims the cache
				// and pressure returns to 0.10.
				var blockSample atomic.Bool
				var currentPressure atomic.Uint64
				currentPressure.Store(math.Float64bits(0.10))
				inSample := make(chan struct{})
				releaseSample := make(chan struct{})

				c := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionRetentionRatio(0.50),
					WithPressureFunc(func() float64 {
						if blockSample.CompareAndSwap(true, false) {
							close(inSample)
							<-releaseSample
							return 0.95
						}
						return math.Float64frombits(currentPressure.Load())
					}),
				).(PressureAwareCache[testData])

				_, err := c.Put("seed", testData{value: 1, dataSize: 20})
				require.NoError(t, err)

				blockSample.Store(true)
				done := make(chan struct{})
				go func() {
					defer close(done)
					_ = c.EvaluateMemoryPressure()
				}()
				<-inSample

				// Act: Reclaim all entries while the stale 0.95 sample is in-flight, then release the sampler.
				c.DeletePrefix("")
				close(releaseSample)
				<-done

				// Assert: Stale pre-reclamation 0.95 sample was rejected; subsequent inserts above targetSize (50B) at 0.10 survive.
				_, err = c.Put("k1", testData{value: 1, dataSize: 40})
				require.NoError(t, err)
				_, err = c.Put("k2", testData{value: 2, dataSize: 40})
				require.NoError(t, err)
				assert.True(t, hasKey(c, "k1"))
				assert.True(t, hasKey(c, "k2"))
			})
		})
	}
}

func TestPressure_EmptyPrefixOnAlreadyEmptyCacheDoesNotInvalidate(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("ZeroAllocsOnEmptyCache", func(t *testing.T) {
				// Arrange: Create a fresh empty cache.
				cache := b.fn(1000, WithInvariantChecking(true))

				// Act & Assert: Calling DeletePrefix("") on an already-empty cache must allocate 0 objects.
				allocs := testing.AllocsPerRun(20, func() {
					cache.DeletePrefix("")
				})
				assert.Zero(t, allocs,
					"%s.DeletePrefix(\"\") on an already-empty cache allocated memory on every call", b.name)
			})

			t.Run("DoesNotForceConcurrentWriterToResamplePressureOnEmptyCache", func(t *testing.T) {
				// Arrange: Empty cache where Goroutine A samples healthy 0.10 pressure for Put,
				// and while Goroutine A is about to return from PressureFunc, Goroutine B calls
				// DeletePrefix("") on the already-empty cache.
				var cache Cache[testData]
				var writerSamples atomic.Int32
				var interceptWriter atomic.Bool
				writerInSample := make(chan struct{})
				releaseWriter := make(chan struct{})

				cache = b.fn(
					1000,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						if interceptWriter.CompareAndSwap(true, false) {
							writerSamples.Add(1)
							close(writerInSample)
							<-releaseWriter
							return 0.10
						}
						if writerSamples.Load() > 0 {
							writerSamples.Add(1)
						}
						return 0.10
					}),
				)

				interceptWriter.Store(true)
				insertDone := make(chan error, 1)
				go func() {
					_, err := cache.Put("k1", testData{value: 1, dataSize: 10})
					insertDone <- err
				}()
				<-writerInSample

				// Act: Call DeletePrefix("") on the already-empty cache while Goroutine A is in flight.
				cache.DeletePrefix("")
				close(releaseWriter)
				require.NoError(t, <-insertDone)

				// Assert: Because the cache was already empty and clean, DeletePrefix("") must not
				// advance the reclamation epoch or force Goroutine A to re-run PressureFunc.
				assert.Equal(t, int32(1), writerSamples.Load(),
					"%s.DeletePrefix(\"\") on an already-empty cache advanced the reclamation epoch and forced concurrent Put to re-sample PressureFunc", b.name)
			})
		})
	}
}

func TestPressure_ConcurrentColdStartZeroPressureAndPostReclamationSampling(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("ColdStartConcurrentCriticalPressure", func(t *testing.T) {
				// Arrange
				var blockFirst atomic.Bool
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.0))
				entered := make(chan struct{})
				release := make(chan struct{})

				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						if blockFirst.CompareAndSwap(true, false) {
							close(entered)
							<-release
						}
						return math.Float64frombits(pressureBits.Load())
					}),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				_, err := cache.Put("k1", testData{value: 1, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("tmp", testData{value: 3, dataSize: 10})
				require.NoError(t, err)
				_, _ = cache.Delete("tmp")

				// Invalidate cached pressure via Compact() to simulate cold uninitialized pressure before spike.
				cache.Compact()
				pressureBits.Store(math.Float64bits(0.95))
				blockFirst.Store(true)

				// Act: Goroutine 1 blocks inside PressureFunc() while Goroutine 2 calls EvaluateMemoryPressure().
				done1 := make(chan []testData, 1)
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
				assert.False(t, hasKey(cache, "k1"))
				assert.True(t, hasKey(cache, "k2"))
			})

			t.Run("ZeroPressureConcurrentNoFalseEviction", func(t *testing.T) {
				// Arrange
				var blockFirst atomic.Bool
				entered := make(chan struct{})
				release := make(chan struct{})

				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						if blockFirst.CompareAndSwap(true, false) {
							close(entered)
							<-release
						}
						return 0.0
					}),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				_, err := cache.Put("k0", testData{value: 0, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("k1", testData{value: 1, dataSize: 40})
				require.NoError(t, err)

				blockFirst.Store(true)

				// Act: Goroutine 1 blocks inside PressureFunc() returning 0.0 while Goroutine 2 evaluates pressure.
				done1 := make(chan []testData, 1)
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
				assert.True(t, hasKey(cache, "k0"))
				assert.True(t, hasKey(cache, "k1"))
			})

			t.Run("ReentrantZeroPressureNoFalseEviction", func(t *testing.T) {
				// Arrange
				var cache PressureAwareCache[testData]
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
				).(PressureAwareCache[testData])

				_, err := cache.Put("k0", testData{value: 0, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("k1", testData{value: 1, dataSize: 40})
				require.NoError(t, err)

				// Act: Enable re-entrant EvaluateMemoryPressure() inside PressureFunc() returning 0.0.
				reentrant = true
				evicted := cache.EvaluateMemoryPressure()

				// Assert: Neither re-entrant nor outer call evicts entries when pressure is 0.0.
				assert.Empty(t, evicted)
				assert.True(t, hasKey(cache, "k0"))
				assert.True(t, hasKey(cache, "k1"))
			})

			t.Run("PostReclamationClearsLastSampledPressure", func(t *testing.T) {
				// Arrange
				var blockFirst atomic.Bool
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.95))
				entered := make(chan struct{})
				release := make(chan struct{})

				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						if blockFirst.CompareAndSwap(true, false) {
							close(entered)
							<-release
						}
						return math.Float64frombits(pressureBits.Load())
					}),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				// Seed high pressure (0.95) in cachedPressureBits, then reclaim and restore healthy pressure (0.10).
				_ = cache.EvaluateMemoryPressure()
				cache.DeletePrefix("")
				pressureBits.Store(math.Float64bits(0.10))

				_, err := cache.Put("k1", testData{value: 1, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("tmp", testData{value: 3, dataSize: 10})
				require.NoError(t, err)
				_, _ = cache.Delete("tmp")
				cache.Compact()

				blockFirst.Store(true)

				// Act: Goroutine 1 blocks sampling 0.10 while Goroutine 2 evaluates pressure after reclamation.
				done1 := make(chan []testData, 1)
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
				assert.True(t, hasKey(cache, "k1"))
				assert.True(t, hasKey(cache, "k2"))
			})
		})
	}
}

func TestPressure_EpochInvalidationAndResamplingSynchronization(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("NoPostReclamationRepoisoningOfLastSampled", func(t *testing.T) {
				// Arrange
				var blockNext atomic.Bool
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.10))
				entered := make(chan struct{})
				release := make(chan struct{})

				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						if blockNext.CompareAndSwap(true, false) {
							close(entered)
							<-release
						}
						return math.Float64frombits(pressureBits.Load())
					}),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				_, err := cache.Put("k1", testData{value: 1, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 40})
				require.NoError(t, err)

				pressureBits.Store(math.Float64bits(0.95))
				_, err = cache.Put("k4", testData{value: 4, dataSize: 30})
				require.NoError(t, err)
				require.False(t, hasKey(cache, "k1"))
				require.False(t, hasKey(cache, "k2"))
				require.True(t, hasKey(cache, "k4"))

				// Restore healthy pressure (0.10) and block Goroutine 1 inside fresh pressure sampling.
				pressureBits.Store(math.Float64bits(0.10))
				blockNext.Store(true)
				done1 := make(chan []testData, 1)
				go func() {
					done1 <- cache.EvaluateMemoryPressure()
				}()
				<-entered

				// Act: While Goroutine 1 is in flight, Goroutine 2 inserts k5 and calls EvaluateMemoryPressure().
				evictedK5, err := cache.Put("k5", testData{value: 5, dataSize: 30})
				require.NoError(t, err)
				evicted2 := cache.EvaluateMemoryPressure()
				close(release)
				evicted1 := <-done1

				// Assert
				assert.Empty(t, evictedK5)
				assert.Empty(t, evicted2)
				assert.Empty(t, evicted1)
				assert.True(t, hasKey(cache, "k4"))
				assert.True(t, hasKey(cache, "k5"))
			})

			t.Run("SampledEpochMismatchResamplesFreshPressureOutsideLock", func(t *testing.T) {
				// Arrange: Create real reclaimable slack so a concurrent Compact() bumps the reclamation epoch while Put samples.
				inFirstSample := make(chan struct{})
				proceedSample := make(chan struct{})
				var armed atomic.Bool
				var trapFirst atomic.Bool

				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						if !armed.Load() {
							return 0.10
						}
						if trapFirst.CompareAndSwap(true, false) {
							close(inFirstSample)
							<-proceedSample
						}
						return 0.95
					}),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				_, err := cache.Put("keep_zero", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				_, err = cache.Put("scratch", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				_, ok := cache.Delete("scratch")
				require.True(t, ok)
				trapFirst.Store(true)
				armed.Store(true)

				donePut := make(chan error, 1)
				go func() {
					_, err := cache.Put("k1", testData{value: 1, dataSize: 60})
					donePut <- err
				}()
				<-inFirstSample

				// Bump the reclamation epoch via real compaction while Put's initial pressure sample is in flight.
				cache.Compact()
				close(proceedSample)

				// Act
				err = <-donePut
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 20})
				require.NoError(t, err)

				// Assert: Put re-sampled 0.95 outside lock instead of reading 0.0 from the invalidated pressure cache.
				assert.False(t, hasKey(cache, "k1"))
				assert.True(t, hasKey(cache, "k2"))
			})

			t.Run("NormalPressureDeletePreservesReclaimEpoch", func(t *testing.T) {
				// Arrange: Put 3 entries at normal 0.10 pressure (< CompactionThreshold 0.75).
				var samples atomic.Int32
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionRetentionRatio(0.50),
					WithPressureFunc(func() float64 {
						samples.Add(1)
						return 0.10
					}),
				)
				_, err := cache.Put("k1", testData{value: 1, dataSize: 20})
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("k3", testData{value: 3, dataSize: 40})
				require.NoError(t, err)
				samples.Store(0)

				// Act: Delete k1 at normal pressure while k2 and k3 (80B > targetSize 50B) remain in the cache.
				_, ok := cache.Delete("k1")

				// Assert: Normal-pressure Delete samples once, does not retry or shed remaining entries.
				require.True(t, ok)
				assert.Equal(t, int32(1), samples.Load())
				assert.False(t, hasKey(cache, "k1"))
				assert.True(t, hasKey(cache, "k2"))
				assert.True(t, hasKey(cache, "k3"))
			})

			t.Run("EvaluateMemoryPressureResamplesOnEpochAdvanceBeforeLock", func(t *testing.T) {
				// Arrange
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
				).(PressureAwareCache[testData])

				_, err := cache.Put("seed_1", testData{value: 1, dataSize: 5})
				require.NoError(t, err)
				_, err = cache.Put("seed_2", testData{value: 2, dataSize: 5})
				require.NoError(t, err)
				_, _ = cache.Delete("seed_1")

				pressureBits.Store(math.Float64bits(0.95))
				blockEval.Store(true)

				doneEval := make(chan []testData, 1)
				go func() {
					doneEval <- cache.EvaluateMemoryPressure()
				}()
				<-evalSampled

				pressureBits.Store(math.Float64bits(0.10))
				cache.Compact()
				_, err = cache.Put("live_1", testData{value: 1, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("live_2", testData{value: 2, dataSize: 40})
				require.NoError(t, err)

				// Act: Release Goroutine A so it acquires c.mu.Lock() in EvaluateMemoryPressure().
				close(releaseEval)
				evicted := <-doneEval

				// Assert
				assert.Empty(t, evicted)
				assert.True(t, hasKey(cache, "live_1"))
				assert.True(t, hasKey(cache, "live_2"))
			})

			t.Run("DeleteDuringInFlightSamplerInvalidatesReclaimEpoch", func(t *testing.T) {
				// Arrange
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

				_, err := cache.Put("k1", testData{value: 1, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("k3", testData{value: 3, dataSize: 10})
				require.NoError(t, err)

				blockSampler.Store(true)
				donePut := make(chan []testData, 1)
				go func() {
					ev, _ := cache.Put("k4", testData{value: 4, dataSize: 20})
					donePut <- ev
				}()
				<-samplerInFlight

				// Act
				_, ok := cache.Delete("k1")
				require.True(t, ok)

				close(releaseSampler)
				evicted := <-donePut

				// Assert
				assert.Empty(t, evicted)
				assert.True(t, hasKey(cache, "k2"))
				assert.True(t, hasKey(cache, "k3"))
				assert.True(t, hasKey(cache, "k4"))
			})

			t.Run("PanicDuringEpochResampleDoesNotDoubleUnlockOrRunCheckInvariantsUnlocked", func(t *testing.T) {
				// Arrange
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
				).(PressureAwareCache[testData])

				_, err := cache.Put("k1", testData{value: 1, dataSize: 20})
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 20})
				require.NoError(t, err)
				_, _ = cache.Delete("k1")
				armed.Store(true)

				panicObserved := make(chan any, 1)
				go func() {
					defer func() {
						panicObserved <- recover()
					}()
					_ = cache.EvaluateMemoryPressure()
				}()

				<-firstSampleReady
				cache.Compact()
				close(epochBumped)

				// Act
				recovered := <-panicObserved

				// Assert
				require.Equal(t, "synthetic PressureFunc failure during resample", recovered)
				assert.True(t, hasKey(cache, "k2"))
			})

			t.Run("ChildGoroutineReclaimingInsidePressureFuncTerminatesInBoundedRetries", func(t *testing.T) {
				testChildGoroutineReclaimingInsidePressureFunc(t, b)
			})

			t.Run("ExhaustedEpochRetriesUseLatestEpochPressure", func(t *testing.T) {
				testExhaustedEpochRetriesUseLatestEpochPressure(t, b)
			})
		})
	}
}

func testChildGoroutineReclaimingInsidePressureFunc(t *testing.T, b backendDef) {
	t.Helper()
	// Arrange
	var cacheRef Cache[testData]
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
					_, _ = cacheRef.Put("tmp_item", testData{value: 1, dataSize: 1})
					cacheRef.DeletePrefix("tmp_")
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
	_, err := cache.Put("target_key", testData{value: 1, dataSize: 10})
	_ = cache.(PressureAwareCache[testData]).EvaluateMemoryPressure()

	// Assert: Both Put and EvaluateMemoryPressure terminate in bounded retries (3 parent samples each = 6 total).
	require.NoError(t, err)
	assert.True(t, hasKey(cache, "target_key"))
	assert.Equal(t, int32(6), parentSampleCalls.Load())
}

func testExhaustedEpochRetriesUseLatestEpochPressure(t *testing.T, b backendDef) {
	t.Helper()
	// Arrange
	var cache PressureAwareCache[testData]
	var currentPressure atomic.Uint64
	currentPressure.Store(math.Float64bits(0.10))

	var interceptCaller atomic.Bool
	sampleEntered := make(chan struct{}, 4)
	sampleProceed := make(chan struct{}, 4)

	cache = b.fn(
		1000,
		WithInvariantChecking(true),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			if interceptCaller.Load() {
				sampleEntered <- struct{}{}
				<-sampleProceed
				return 0.95
			}
			return math.Float64frombits(currentPressure.Load())
		}),
	).(PressureAwareCache[testData])

	_, err := cache.Put("survivor", testData{value: 1, dataSize: 600})
	require.NoError(t, err)

	interceptCaller.Store(true)
	done := make(chan []testData, 1)
	go func() {
		done <- cache.EvaluateMemoryPressure()
	}()

	for attempt := range 3 {
		<-sampleEntered
		interceptCaller.Store(false)
		if attempt == 2 {
			currentPressure.Store(math.Float64bits(0.10))
		}
		_, _ = cache.Put("tmp", testData{value: 1, dataSize: 10})
		_, _ = cache.Delete("tmp")
		cache.Compact()
		if attempt == 2 {
			_ = cache.EvaluateMemoryPressure()
		}
		if attempt < 2 {
			interceptCaller.Store(true)
		}
		sampleProceed <- struct{}{}
	}

	// Act
	evicted := <-done

	// Assert
	assert.Empty(t, evicted)
	assert.True(t, hasKey(cache, "survivor"))
}

func TestPressure_ReentrancyAndOverflowSamplerCoordination(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("ReentrantPressureFuncDoesNotStackOverflow", func(t *testing.T) {
				// Arrange
				var pac PressureAwareCache[testData]
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
				pac = cache.(PressureAwareCache[testData])
				_, err := cache.Put("k1", testData{value: 1, dataSize: 80})
				require.NoError(t, err)

				// Act & Assert: EvaluateMemoryPressure must not infinitely recurse.
				evicted := pac.EvaluateMemoryPressure()
				assert.True(t, reentered)
				assert.Len(t, evicted, 1)
			})

			t.Run("ReentrantCompactFollowedByEvaluateDoesNotReinvokePressureFunc", func(t *testing.T) {
				// Arrange
				var pac PressureAwareCache[testData]
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
				pac = cache.(PressureAwareCache[testData])

				_, err := cache.Put("seed", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				invocations = 0
				reentrantCall = true

				// Act
				_ = pac.EvaluateMemoryPressure()

				// Assert
				assert.Equal(t, 1, invocations)
			})

			t.Run("ThreePlusConcurrentCallersReentrantPressureFuncNoStackOverflow", func(t *testing.T) {
				testThreePlusConcurrentCallersReentrant(t, b)
			})

			testOverflowAndWatermarkReentrancy(t, b)
		})
	}
}

func testThreePlusConcurrentCallersReentrant(t *testing.T, b backendDef) {
	t.Helper()
	// Arrange: 4 concurrent callers enter fresh pressure sampling during cold start in deterministic order
	// so G1 holds the primary sampler slot (order == 1), G2 holds the fallback sampler slot (order == 2), and G3 + G4
	// execute the 3rd+ overflow path (order == 3, 4).
	const numCallers = 4
	var pac PressureAwareCache[testData]
	var cache Cache[testData]
	var activeDepth sync.Map
	var maxDepth atomic.Int32
	var enteredCount atomic.Int32
	var overflowDone atomic.Int32
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	allEntered := make(chan struct{})
	releaseHolders := make(chan struct{})

	cache = b.fn(
		100,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			gid := testGoroutineID()
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
			switch order {
			case 1:
				close(firstEntered)
			case 2:
				close(secondEntered)
			case numCallers:
				close(allEntered)
			}
			<-allEntered

			// G3 and G4 (3rd+ overflow callers) make re-entrant cache calls while G1 and G2 are still inside PressureFunc.
			if (order == 3 || order == 4) && pac != nil {
				pac.Compact()
				_ = pac.EvaluateMemoryPressure()
				_, _ = cache.Put(fmt.Sprintf("reentrant_%d", order), testData{value: 1, dataSize: 10})
				_, _ = cache.Delete(fmt.Sprintf("reentrant_%d", order))
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
	pac = cache.(PressureAwareCache[testData])

	// Act: Launch G1 first (primary), then G2 (fallback), then G3 and G4 (overflow).
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = pac.EvaluateMemoryPressure()
	})
	<-firstEntered

	wg.Go(func() {
		_ = pac.EvaluateMemoryPressure()
	})
	<-secondEntered

	for range numCallers - 2 {
		wg.Go(func() {
			_ = pac.EvaluateMemoryPressure()
		})
	}
	wg.Wait()

	// Assert: Every goroutine's PressureFunc recursion depth stayed strictly 1 (no stack overflow)
	// and none of the re-entrant calls inside G3/G4 bumped the reclamation epoch (enteredCount == 4 with zero retries).
	assert.Equal(t, int32(1), maxDepth.Load())
	assert.Equal(t, int32(numCallers), enteredCount.Load())
	assert.False(t, hasKey(cache, "reentrant_3"))
	assert.False(t, hasKey(cache, "reentrant_4"))
}

func testOverflowAndWatermarkReentrancy(t *testing.T, b backendDef) {
	t.Helper()
	t.Run("ReentrantReclamationInsidePressureFuncNotOverwrittenByStoreSampledPressure", func(t *testing.T) {
		// Arrange: PressureFunc performs a re-entrant Compact() (which marks the cache reclaimed)
		// and returns the pre-reclamation reading 0.95 on that call while subsequent calls return 0.10.
		var pac PressureAwareCache[testData]
		var reentrantCompact atomic.Bool
		var currentPressure atomic.Uint64
		currentPressure.Store(math.Float64bits(0.10))

		cache := b.fn(
			200,
			WithInvariantChecking(true),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				if reentrantCompact.CompareAndSwap(true, false) && pac != nil {
					pac.Compact()
					return 0.95
				}
				return math.Float64frombits(currentPressure.Load())
			}),
		)
		pac = cache.(PressureAwareCache[testData])

		for i := range 5 {
			_, err := cache.Put(fmt.Sprintf("k-%d", i), testData{value: int64(i), dataSize: 10})
			require.NoError(t, err)
		}
		_, _ = cache.Delete("k-0")

		// Act: Trigger fresh pressure sampling with reentrantCompact enabled, then insert 80B at 0.10 pressure
		// (bringing total occupancy to 130B > targetSize 100B).
		reentrantCompact.Store(true)
		_, err := cache.Put("k-after", testData{value: 10, dataSize: 10})
		require.NoError(t, err)
		evicted, err := cache.Put("k-large", testData{value: 11, dataSize: 80})
		require.NoError(t, err)

		// Assert: Because Compact() reclaimed memory during PressureFunc, the pre-reclamation 0.95 reading
		// did not overwrite the post-reclamation pressure state, so k-large and all surviving keys remain intact.
		assert.Empty(t, evicted)
		assert.True(t, hasKey(cache, "k-1"))
		assert.True(t, hasKey(cache, "k-after"))
		assert.True(t, hasKey(cache, "k-large"))
	})

	t.Run("SamplePressureFreshRetainsPostReclamationDirectReturnValue", func(t *testing.T) {
		// Arrange: Populate cache with 2 entries of 40B (80B > targetSize 50B) at healthy pressure (0.10)
		// and create slack via a temporary entry so Compact() reclaims memory across all backends.
		var pac PressureAwareCache[testData]
		reentrantCompact := false
		pressure := 0.10

		cache := b.fn(
			100,
			WithInvariantChecking(true),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				if reentrantCompact && pac != nil {
					reentrantCompact = false
					pac.Compact()
				}
				return pressure
			}),
		)
		pac = cache.(PressureAwareCache[testData])

		_, err := cache.Put("k1", testData{value: 1, dataSize: 40})
		require.NoError(t, err)
		_, err = cache.Put("k2", testData{value: 2, dataSize: 40})
		require.NoError(t, err)
		_, err = cache.Put("tmp", testData{value: 3, dataSize: 10})
		require.NoError(t, err)
		_, ok := cache.Delete("tmp")
		require.True(t, ok)

		// Act: Evaluate memory pressure when PressureFunc performs re-entrant Compact() before returning critical pressure (0.95).
		pressure = 0.95
		reentrantCompact = true
		evicted := pac.EvaluateMemoryPressure()

		// Assert: Because lossless Compact() does not reduce live byte occupancy (80B > 50B targetSize),
		// the 0.95 return value from PressureFunc must not be discarded as 0.0 and must shed LRU tail "k1".
		assert.Len(t, evicted, 1)
		assert.False(t, hasKey(cache, "k1"))
		assert.True(t, hasKey(cache, "k2"))
	})

	t.Run("ForegroundReclaimDuringOverflowSamplingAdvancesReclaimEpoch", func(t *testing.T) {
		// Arrange: Prepare a cache with two 40B live entries and dirty/fragmented state so Compact() reclaims.
		var pressureBits atomic.Uint64
		pressureBits.Store(math.Float64bits(0.10))
		var trapActive atomic.Bool
		enteredCh := make(chan struct{}, 3)
		releaseCh := make(chan struct{})

		cache := b.fn(100,
			WithInvariantChecking(true),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				p := math.Float64frombits(pressureBits.Load())
				if trapActive.Load() {
					enteredCh <- struct{}{}
					<-releaseCh
				}
				return p
			}),
		)
		pac, ok := cache.(PressureAwareCache[testData])
		require.True(t, ok)

		_, err := cache.Put("k1", testData{value: 1, dataSize: 40})
		require.NoError(t, err)
		_, err = cache.Put("k2", testData{value: 2, dataSize: 40})
		require.NoError(t, err)
		_, err = cache.Put("scratch", testData{value: 3, dataSize: 1})
		require.NoError(t, err)
		_, ok = cache.Delete("scratch")
		require.True(t, ok)

		pressureBits.Store(math.Float64bits(0.95))
		trapActive.Store(true)

		// Act: 3 concurrent samplers enter PressureFunc (primary + fallback + 1 overflow sampler).
		var wg sync.WaitGroup
		for range 3 {
			wg.Go(func() {
				pac.EvaluateMemoryPressure()
			})
		}
		for range 3 {
			<-enteredCh
		}
		trapActive.Store(false)
		pressureBits.Store(math.Float64bits(0.10))
		pac.Compact() // Foreground reclamation while an overflow sampler is in flight
		close(releaseCh)
		wg.Wait()

		// Assert: Stale 0.95 pre-Compact samples must be invalidated by Compact(), preserving both k1 and k2.
		assert.True(t, hasKey(cache, "k1"))
		assert.True(t, hasKey(cache, "k2"))
	})

	t.Run("ReentrantPutPreservesLastSampledPressureAndZeroWatermark", func(t *testing.T) {
		// Arrange: Populate 4 zero-size entries at 0.0 pressure, then shed 2 of them at 0.95 pressure
		// so 2 zero-size entries ("z-2", "z-3") remain with a zero-size watermark of 2.
		var cache Cache[testData]
		var reentrantPut atomic.Bool
		var pressure atomic.Uint64
		pressure.Store(math.Float64bits(0.0))

		cache = b.fn(
			1000,
			WithInvariantChecking(true),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				if reentrantPut.CompareAndSwap(true, false) && cache != nil {
					_, _ = cache.Put("reentrant_key", testData{value: 1, dataSize: 10})
				}
				return math.Float64frombits(pressure.Load())
			}),
		)
		pac := cache.(PressureAwareCache[testData])

		for i := range 4 {
			_, err := cache.Put(fmt.Sprintf("z-%d", i), testData{value: int64(i), dataSize: 0})
			require.NoError(t, err)
		}
		pressure.Store(math.Float64bits(0.95))
		evicted := pac.EvaluateMemoryPressure()
		require.Len(t, evicted, 2)
		require.True(t, hasKey(cache, "z-2"))
		require.True(t, hasKey(cache, "z-3"))

		// Act: Put "trigger" under 0.95 while PressureFunc performs a re-entrant 10B Put.
		reentrantPut.Store(true)
		_, err := cache.Put("trigger", testData{value: 99, dataSize: 10})
		require.NoError(t, err)

		// Assert: Re-entrant Put at synthetic 0.0 pressure did not reset the zero-size watermark to 0,
		// so the retained zero-size entries ("z-2", "z-3") were not re-shed.
		assert.True(t, hasKey(cache, "z-2"))
		assert.True(t, hasKey(cache, "z-3"))
		assert.True(t, hasKey(cache, "reentrant_key"))
		assert.True(t, hasKey(cache, "trigger"))
	})
}

func TestPressure_ConcurrentReentrantSamplersAndOverflowEpochInvalidation(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("PrimaryAndFallbackReentrantCompactDoNotForceResample", func(t *testing.T) {
				// Arrange: Primary sampler G1 and fallback sampler G2 both call Compact() re-entrantly
				// inside PressureFunc. Each slot's self-reclamation must be isolated so neither G1 nor G2
				// is forced to retry PressureFunc in lockWithPressure.
				var pac PressureAwareCache[testData]
				var cache Cache[testData]
				var armed atomic.Bool
				var calls atomic.Int32
				primaryEntered := make(chan struct{})
				fallbackEntered := make(chan struct{})
				releasePrimary := make(chan struct{})
				releaseFallback := make(chan struct{})
				primaryCompacted := make(chan struct{})

				cache = b.fn(
					1000,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						if !armed.Load() {
							return 0.10
						}
						n := calls.Add(1)
						switch n {
						case 1:
							// Primary sampler G1 compacts initial dirty slack from "k1".
							close(primaryEntered)
							<-releasePrimary
							pac.Compact()
							close(primaryCompacted)
							<-releaseFallback
							return 0.10
						case 2:
							// Fallback sampler G2 creates dirty slack and compacts it re-entrantly.
							close(fallbackEntered)
							<-primaryCompacted
							_, _ = cache.Put("k2", testData{value: 2, dataSize: 10})
							_, _ = cache.Delete("k2")
							pac.Compact()
							close(releaseFallback)
							return 0.10
						default:
							return 0.10
						}
					}),
				)
				pac = cache.(PressureAwareCache[testData])

				_, err := cache.Put("keep", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				_, err = cache.Put("k1", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				_, ok := cache.Delete("k1") // Leaves non-empty dirty slack so G1's Compact() reclaims
				require.True(t, ok)
				armed.Store(true)

				var wg sync.WaitGroup
				wg.Go(func() {
					_ = pac.EvaluateMemoryPressure()
				})
				<-primaryEntered

				wg.Go(func() {
					_ = pac.EvaluateMemoryPressure()
				})
				<-fallbackEntered

				// Act: Release G1 to run re-entrant Compact(), then G2 runs re-entrant Compact(), and both return.
				close(releasePrimary)
				wg.Wait()

				// Assert: Neither G1 nor G2 was forced to re-sample in lockWithPressure (exactly 2 PressureFunc calls).
				assert.Equal(t, int32(2), calls.Load())
			})

			t.Run("PrimaryReentrantCompactWhileOverflowActiveInvalidatesStaleSamples", func(t *testing.T) {
				// Arrange: Primary sampler G1, fallback sampler G2, and overflow sampler G3 are all in-flight.
				// When G1 performs a re-entrant Compact() on real dirty slack while G3 is in overflow,
				// the reclamation epoch must advance so G2 and G3's stale 0.95 readings from before G1's Compact() are discarded.
				var pac PressureAwareCache[testData]
				var cache Cache[testData]
				var armed atomic.Bool
				var orderCounter atomic.Int32
				g1Ready := make(chan struct{})
				g2Ready := make(chan struct{})
				g3Ready := make(chan struct{})
				releaseG1 := make(chan struct{})
				releaseG2AndG3 := make(chan struct{})
				g1Compacted := make(chan struct{})

				cache = b.fn(
					200,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
					WithPressureFunc(func() float64 {
						if !armed.Load() {
							return 0.10
						}
						order := orderCounter.Add(1)
						switch order {
						case 1:
							// Primary sampler G1
							close(g1Ready)
							<-releaseG1
							pac.Compact()
							close(g1Compacted)
							return 0.10
						case 2:
							// Fallback sampler G2
							close(g2Ready)
							<-releaseG2AndG3
							return 0.95
						case 3:
							// Overflow sampler G3
							close(g3Ready)
							<-releaseG2AndG3
							return 0.95
						default:
							return 0.10
						}
					}),
				)
				pac = cache.(PressureAwareCache[testData])

				// Populate two 60B live entries (120B > targetSize 100B) plus a deleted "tmp" entry at 0.10 pressure
				// so G1's re-entrant Compact() performs a real compaction (compactDataStructuresLocked() == true).
				_, err := cache.Put("k1", testData{value: 1, dataSize: 60})
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 60})
				require.NoError(t, err)
				_, err = cache.Put("tmp", testData{value: 3, dataSize: 10})
				require.NoError(t, err)
				_, ok := cache.Delete("tmp")
				require.True(t, ok)
				armed.Store(true)

				// Act: Launch G1 first (primary), then G2 (fallback), then G3 (overflow) deterministically.
				var wg sync.WaitGroup
				wg.Go(func() {
					_ = pac.EvaluateMemoryPressure()
				})
				<-g1Ready

				wg.Go(func() {
					_ = pac.EvaluateMemoryPressure()
				})
				<-g2Ready

				wg.Go(func() {
					_ = pac.EvaluateMemoryPressure()
				})
				<-g3Ready

				// Release G1 first so G1 calls pac.Compact() re-entrantly while G3 is still in overflow,
				// then release G2 and G3 which return stale 0.95 from the pre-Compact epoch.
				close(releaseG1)
				<-g1Compacted
				close(releaseG2AndG3)
				wg.Wait()

				// Assert: Because G1's re-entrant Compact() advanced the reclamation epoch even while G3 was in overflow,
				// G2 and G3's stale 0.95 readings were invalidated and neither k1 nor k2 was evicted.
				assert.True(t, hasKey(cache, "k1"))
				assert.True(t, hasKey(cache, "k2"))
			})
			t.Run("ExternalReclamationBeforeReentrantCompactInvalidatesEpoch", func(t *testing.T) {
				// Arrange: Sampler G1 is in flight. An external goroutine G2 reclaims.
				// Then G1 performs a re-entrant Compact() and returns a stale 0.95 reading.
				// The sampler must detect the external reclamation, retry PressureFunc,
				// get 0.10, and NOT evict entries.
				var pac PressureAwareCache[testData]
				var cache Cache[testData]
				var calls atomic.Int32
				var armed atomic.Bool
				g1Entered := make(chan struct{})
				releaseG1Compact := make(chan struct{})
				releaseG1Finish := make(chan struct{})

				cache = b.fn(
					200,
					WithInvariantChecking(true),
					WithCompactionThreshold(0.90),
					WithEvictionThreshold(0.90),
					WithPressureFunc(func() float64 {
						if !armed.Load() {
							return 0.10
						}
						n := calls.Add(1)
						if n == 1 {
							// G1 in-flight
							close(g1Entered)
							// Wait for G2 external reclamation
							<-releaseG1Compact
							// Perform re-entrant Compact
							_, _ = cache.Put("k_scratch", testData{value: 1, dataSize: 10})
							_, _ = cache.Delete("k_scratch")
							pac.Compact()
							<-releaseG1Finish
							return 0.95 // Stale pre-reclamation reading
						}
						// Retry should occur, returning 0.10
						return 0.10
					}),
				)
				pac = cache.(PressureAwareCache[testData])

				_, err := cache.Put("k1", testData{value: 1, dataSize: 100})
				require.NoError(t, err)

				armed.Store(true)
				var wg sync.WaitGroup
				wg.Go(func() {
					_ = pac.EvaluateMemoryPressure()
				})

				<-g1Entered

				// External reclamation G2
				_, _ = cache.Put("k_ext", testData{value: 1, dataSize: 10})
				_, _ = cache.Delete("k_ext")
				pac.Compact() // This increments externalReclaimEpoch

				close(releaseG1Compact)
				close(releaseG1Finish)
				wg.Wait()

				// Assert: The sampler detected the external reclamation across its own reentrant compaction,
				// retried, got 0.10, and did not shed k1.
				assert.GreaterOrEqual(t, calls.Load(), int32(2), "Should retry PressureFunc due to intervening external reclamation")
				assert.True(t, hasKey(cache, "k1"), "k1 should not be shed because pressure dropped to 0.10")
			})
		})
	}
}

func TestPressure_PreSampleEpochLoadAndConcurrentSamplerOrdering(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("PreSampleEpochLoadUsesActualSampledEpoch", func(t *testing.T) {
				// Arrange: Populate two 40B entries (80B > targetSize 50B) plus a deleted scratch entry
				// so Compact() can advance the reclamation epoch while G1 holds the primary sampler slot.
				var armed atomic.Bool
				var blockG1 atomic.Bool
				var g2SampleCalls atomic.Int32
				g1InSample := make(chan struct{})
				releaseG1 := make(chan struct{})

				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
					WithPressureFunc(func() float64 {
						if !armed.Load() {
							return 0.10
						}
						if blockG1.CompareAndSwap(true, false) {
							close(g1InSample)
							<-releaseG1
							return 0.10
						}
						g2SampleCalls.Add(1)
						return 0.95
					}),
				)
				pac := cache.(PressureAwareCache[testData])

				_, err := cache.Put("k1", testData{value: 1, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("k2", testData{value: 2, dataSize: 40})
				require.NoError(t, err)
				_, err = cache.Put("scratch", testData{value: 3, dataSize: 10})
				require.NoError(t, err)
				_, ok := cache.Delete("scratch")
				require.True(t, ok)

				// G1 enters PressureFunc at epoch 0 and holds the primary sampler slot.
				armed.Store(true)
				blockG1.Store(true)
				g1Done := make(chan struct{})
				go func() {
					defer close(g1Done)
					_ = pac.EvaluateMemoryPressure()
				}()
				<-g1InSample

				// Advance the reclamation epoch to 1 via Compact() while G1 still holds the primary sampler slot from epoch 0.
				pac.Compact()

				// Act: G2 inserts k3 (10B), spins against G1's primary sampler slot, and samples 0.95 via the fallback slot in epoch 1.
				evicted, err := cache.Put("k3", testData{value: 3, dataSize: 10})
				g2Calls := g2SampleCalls.Load()
				close(releaseG1)
				<-g1Done

				// Assert: G2 used the actual epoch (1) at which its fallback sample was taken (1 sample call, no false retry)
				// and shed k1 under 0.95 pressure.
				require.NoError(t, err)
				assert.Equal(t, int32(1), g2Calls)
				assert.Len(t, evicted, 1)
				assert.False(t, hasKey(cache, "k1"))
				assert.True(t, hasKey(cache, "k2"))
				assert.True(t, hasKey(cache, "k3"))
			})

			t.Run("OlderInFlightSamplerDoesNotOverwriteNewerCompletedSample", func(t *testing.T) {
				testOlderInFlightSamplerDoesNotOverwrite(t, b)
			})

			t.Run("ReentrantReclamationInsidePressureFuncInvalidatesConcurrentCallerEpoch", func(t *testing.T) {
				testReentrantReclamationInvalidatesConcurrentCaller(t, b)
			})

			testOverflowAndExternalReclamationResampling(t, b)
		})
	}
}

func testOlderInFlightSamplerDoesNotOverwrite(t *testing.T, b backendDef) {
	t.Helper()
	// Arrange
	var mode atomic.Int32
	g1Entered := make(chan struct{})
	releaseG1 := make(chan struct{})

	cache := b.fn(1000,
		WithInvariantChecking(true),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			m := mode.Load()
			if m == 0 {
				return 0.10
			}
			if m == 1 && mode.CompareAndSwap(1, 2) {
				close(g1Entered)
				<-releaseG1
				return 0.10
			}
			return 0.95
		}),
	)

	for i := range 80 {
		_, err := cache.Put(fmt.Sprintf("k/%02d", i), testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}

	// Act
	mode.Store(1)

	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = cache.Delete("nonexistent-1")
	})
	<-g1Entered

	_, _ = cache.Delete("nonexistent-2")
	close(releaseG1)
	wg.Wait()

	_, err := cache.Put("post-spike", testData{value: 99, dataSize: 10})
	require.NoError(t, err)

	// Assert: Compute surviving byte total solely via the public Peek API.
	var survivingBytes uint64
	for i := range 80 {
		if v, ok := cache.Peek(fmt.Sprintf("k/%02d", i)); ok {
			survivingBytes += v.dataSize
		}
	}
	if v, ok := cache.Peek("post-spike"); ok {
		survivingBytes += v.dataSize
	}
	assert.LessOrEqual(t, survivingBytes, uint64(500))
	assert.False(t, hasKey(cache, "k/00"))
	assert.True(t, hasKey(cache, "post-spike"))
}

func testReentrantReclamationInvalidatesConcurrentCaller(t *testing.T, b backendDef) {
	t.Helper()
	// Arrange
	var phase atomic.Int32
	g2Sampled := make(chan struct{})
	g1Reclaimed := make(chan struct{})

	var pac PressureAwareCache[testData]
	cache := b.fn(1000,
		WithInvariantChecking(true),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			switch phase.Load() {
			case 1:
				if phase.CompareAndSwap(1, 2) {
					close(g2Sampled)
					<-g1Reclaimed
					return 0.95
				}
				phase.Store(3)
				pac.Compact()
				return 0.10
			case 2:
				phase.Store(3)
				pac.Compact()
				return 0.10
			default:
				return 0.10
			}
		}),
	)
	pac = cache.(PressureAwareCache[testData])

	for i := range 85 {
		_, err := cache.Put(fmt.Sprintf("k/%02d", i), testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}
	_, err := cache.Put("scratch", testData{value: 1, dataSize: 10})
	require.NoError(t, err)
	_, ok := cache.Delete("scratch")
	require.True(t, ok)

	// Act
	phase.Store(1)

	var wg sync.WaitGroup
	wg.Go(func() {
		_, err := cache.Put("g2-key", testData{value: 1, dataSize: 10})
		assert.NoError(t, err) //nolint:testifylint // wg.Go runs in a child goroutine where require.* (t.FailNow) is invalid
	})

	<-g2Sampled
	pac.EvaluateMemoryPressure()
	close(g1Reclaimed)
	wg.Wait()

	// Assert: Verify all 85 original entries plus "g2-key" (860B total) survived via public API.
	var survivingBytes uint64
	for i := range 85 {
		v, ok := cache.Peek(fmt.Sprintf("k/%02d", i))
		if assert.True(t, ok) {
			survivingBytes += v.dataSize
		}
	}
	vG2, ok := cache.Peek("g2-key")
	if assert.True(t, ok) {
		survivingBytes += vG2.dataSize
	}
	assert.Equal(t, uint64(860), survivingBytes)
}

func testOverflowAndExternalReclamationResampling(t *testing.T, b backendDef) {
	t.Helper()
	t.Run("OverflowSamplerReentrantCompactAdvancesEpochAndInvalidatesPrimaryAndFallback", func(t *testing.T) {
		// Arrange: Primary sampler G1, fallback sampler G2, and overflow sampler G3 all enter PressureFunc.
		// When the 3rd (overflow) sampler G3 calls pac.Compact() re-entrantly on dirty slack,
		// markReclaimedLocked() must unconditionally advance reclaimEpoch so G1 and G2's stale 0.95
		// readings are invalidated and resampled as 0.10.
		var pac PressureAwareCache[testData]
		var cache Cache[testData]
		var armed atomic.Bool
		var orderCounter atomic.Int32
		g1Ready := make(chan struct{})
		g2Ready := make(chan struct{})
		g3Ready := make(chan struct{})
		releaseG3 := make(chan struct{})
		g3Compacted := make(chan struct{})
		releaseG1AndG2 := make(chan struct{})

		cache = b.fn(
			200,
			WithInvariantChecking(true),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				if !armed.Load() {
					return 0.10
				}
				switch orderCounter.Add(1) {
				case 1:
					// Primary sampler G1
					close(g1Ready)
					<-releaseG1AndG2
					return 0.95
				case 2:
					// Fallback sampler G2
					close(g2Ready)
					<-releaseG1AndG2
					return 0.95
				case 3:
					// Overflow sampler G3 performs re-entrant Compact()
					close(g3Ready)
					<-releaseG3
					pac.Compact()
					close(g3Compacted)
					return 0.10
				default:
					return 0.10
				}
			}),
		)
		pac = cache.(PressureAwareCache[testData])

		_, err := cache.Put("k1", testData{value: 1, dataSize: 60})
		require.NoError(t, err)
		_, err = cache.Put("k2", testData{value: 2, dataSize: 60})
		require.NoError(t, err)
		_, err = cache.Put("tmp", testData{value: 3, dataSize: 10})
		require.NoError(t, err)
		_, ok := cache.Delete("tmp")
		require.True(t, ok)
		armed.Store(true)

		var wg sync.WaitGroup
		wg.Go(func() {
			_ = pac.EvaluateMemoryPressure()
		})
		<-g1Ready

		wg.Go(func() {
			_ = pac.EvaluateMemoryPressure()
		})
		<-g2Ready

		wg.Go(func() {
			_ = pac.EvaluateMemoryPressure()
		})
		<-g3Ready

		// Act: Release overflow sampler G3 to run re-entrant Compact(), then release G1 and G2.
		close(releaseG3)
		<-g3Compacted
		close(releaseG1AndG2)
		wg.Wait()

		// Assert: Neither k1 nor k2 was shed because G3's overflow re-entrant Compact() invalidated G1 and G2's epochs.
		assert.True(t, hasKey(cache, "k1"))
		assert.True(t, hasKey(cache, "k2"))
	})

	t.Run("ExternalReclamationAfterSamplerReentrantCompactClearsExemptionAndForcesResample", func(t *testing.T) {
		// Arrange: Primary sampler G1 performs a re-entrant Compact() inside PressureFunc, and before G1 returns 0.95,
		// an external caller creates and compacts new dirty slack. The external reclamation must clear G1's
		// re-entrant exemption so G1 is forced to resample (0.10) in lockWithPressure.
		var pac PressureAwareCache[testData]
		var cache Cache[testData]
		var armed atomic.Bool
		var calls atomic.Int32
		g1AfterReentrantCompact := make(chan struct{})
		releaseG1 := make(chan struct{})

		cache = b.fn(
			200,
			WithInvariantChecking(true),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				if !armed.Load() {
					return 0.10
				}
				if calls.Add(1) == 1 {
					pac.Compact() // G1's own re-entrant Compact()
					close(g1AfterReentrantCompact)
					<-releaseG1
					return 0.95
				}
				return 0.10
			}),
		)
		pac = cache.(PressureAwareCache[testData])

		_, err := cache.Put("k1", testData{value: 1, dataSize: 60})
		require.NoError(t, err)
		_, err = cache.Put("k2", testData{value: 2, dataSize: 60})
		require.NoError(t, err)
		_, err = cache.Put("tmp1", testData{value: 3, dataSize: 10})
		require.NoError(t, err)
		_, ok := cache.Delete("tmp1")
		require.True(t, ok)
		armed.Store(true)

		g1Done := make(chan struct{})
		go func() {
			defer close(g1Done)
			_ = pac.EvaluateMemoryPressure()
		}()
		<-g1AfterReentrantCompact

		// External caller creates dirty slack and compacts while G1 is still inside PressureFunc.
		armed.Store(false)
		_, err = cache.Put("tmp2", testData{value: 4, dataSize: 10})
		require.NoError(t, err)
		_, ok = cache.Delete("tmp2")
		require.True(t, ok)
		armed.Store(true)
		pac.Compact()

		// Act: Release G1 to return stale 0.95; lockWithPressure must reject the cleared exemption and resample 0.10.
		close(releaseG1)
		<-g1Done

		// Assert: G1 resampled (calls >= 2) and both k1 and k2 survived.
		assert.GreaterOrEqual(t, calls.Load(), int32(2))
		assert.True(t, hasKey(cache, "k1"))
		assert.True(t, hasKey(cache, "k2"))
	})

	t.Run("ContendedEvaluateMemoryPressureDoesNotHijackOlderPreSpikeSample", func(t *testing.T) {
		// Arrange: G1 holds the primary sampler slot prepared to return a pre-spike 0.10 reading,
		// while G2 completes a fallback 0.95 sample. When G3 subsequently calls EvaluateMemoryPressure()
		// while G1 is still holding the primary slot, G3 must invoke PressureFunc itself (0.95) and shed k1
		// rather than hijacking G1's stale 0.10 sample.
		var armed atomic.Bool
		var calls atomic.Int32
		g1Entered := make(chan struct{})
		releaseG1 := make(chan struct{})

		cache := b.fn(
			100,
			WithInvariantChecking(true),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				if !armed.Load() {
					return 0.10
				}
				if calls.Add(1) == 1 {
					close(g1Entered)
					<-releaseG1
					return 0.10
				}
				return 0.95
			}),
		)
		pac := cache.(PressureAwareCache[testData])

		_, err := cache.Put("k1", testData{value: 1, dataSize: 40})
		require.NoError(t, err)
		_, err = cache.Put("k2", testData{value: 2, dataSize: 40})
		require.NoError(t, err)
		armed.Store(true)

		g1Done := make(chan struct{})
		go func() {
			defer close(g1Done)
			_, _ = cache.Delete("miss-1")
		}()
		<-g1Entered

		// G2 completes a 0.95 sample via the fallback slot while G1 is blocked in the primary slot.
		_, _ = cache.Delete("miss-2")

		// Act: G3 calls EvaluateMemoryPressure() while G1 is still in the primary slot.
		evicted := pac.EvaluateMemoryPressure()
		callsWhileG1Blocked := calls.Load()
		close(releaseG1)
		<-g1Done

		// Assert: G3 sampled 0.95 while G1 was still blocked (3 calls before releasing G1) and shed k1.
		assert.Equal(t, int32(3), callsWhileG1Blocked)
		assert.Len(t, evicted, 1)
		assert.False(t, hasKey(cache, "k1"))
		assert.True(t, hasKey(cache, "k2"))
	})
}

func TestPressure_ZeroSizeWatermarksAndIdempotency(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("ProtectedZeroSizePutDoesNotLeakFloorOneIntoSubsequentEvaluateMemoryPressure", func(t *testing.T) {
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.10))
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.0),
					WithPressureFunc(func() float64 {
						return math.Float64frombits(pressureBits.Load())
					}),
				)
				pac := cache.(PressureAwareCache[testData])

				for _, k := range []string{"z1", "z2", "z3"} {
					_, err := cache.Put(k, testData{value: 0, dataSize: 0})
					require.NoError(t, err)
				}

				// Act 1: Under Tier 2 (0.95) with RetentionRatio = 0.0, inserting "z4" (0B) protects the newly inserted
				// MRU entry ("z4") while shedding "z1", "z2", "z3".
				pressureBits.Store(math.Float64bits(0.95))
				evictedPut, err := cache.Put("z4", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				require.Len(t, evictedPut, 3)
				require.True(t, hasKey(cache, "z4"))

				// Act 2: Immediately call unprotected EvaluateMemoryPressure(). Because Put did not leak its
				// temporary floor of 1 into lastReclaimedZeroCount, "z4" must now be shed (RetentionRatio = 0.0 -> targetZero = 0).
				evictedEval := pac.EvaluateMemoryPressure()
				assert.Len(t, evictedEval, 1)
				assert.False(t, hasKey(cache, "z4"))
			})

			t.Run("TargetLenBoundedZeroSheddingIsIdempotentUntilPositiveSurvivorDeleted", func(t *testing.T) {
				// Arrange: 2 positive-size entries at LRU tail (p1, p2: 30B each) + 2 zero-size entries in middle (z1, z2: 0B)
				// + 1 positive-size entry at MRU head (p3: 40B). Total = 5 entries, 100B in 100B maxSize, RetentionRatio = 0.60.
				// Under Tier 2 (0.95), targetSize = 60B, targetLen = int(5 * 0.60) = 3, targetZero = int(2 * 0.60) = 1.
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.10))
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.60),
					WithPressureFunc(func() float64 {
						return math.Float64frombits(pressureBits.Load())
					}),
				)
				pac := cache.(PressureAwareCache[testData])

				for _, k := range []string{"p1", "p2"} {
					_, err := cache.Put(k, testData{value: 1, dataSize: 30})
					require.NoError(t, err)
				}
				for _, k := range []string{"z1", "z2"} {
					_, err := cache.Put(k, testData{value: 0, dataSize: 0})
					require.NoError(t, err)
				}
				_, err := cache.Put("p3", testData{value: 3, dataSize: 40})
				require.NoError(t, err)

				// Act 1: First EvaluateMemoryPressure() at 0.95 sheds p1 and p2 (60B) to reach 40B <= targetSize (60B),
				// stopping at c.len == 3 == targetLen with z1, z2, p3 surviving (zeroCount = 2 > targetZero = 1).
				pressureBits.Store(math.Float64bits(0.95))
				evictedFirst := pac.EvaluateMemoryPressure()
				require.Len(t, evictedFirst, 2)
				require.True(t, hasKey(cache, "z1"))
				require.True(t, hasKey(cache, "z2"))
				require.True(t, hasKey(cache, "p3"))

				// Act 2: Immediate second EvaluateMemoryPressure() at 0.95 must be 100% idempotent (0 evictions).
				evictedSecond := pac.EvaluateMemoryPressure()
				assert.Empty(t, evictedSecond)
				assert.True(t, hasKey(cache, "z1"))
				assert.True(t, hasKey(cache, "z2"))
				assert.True(t, hasKey(cache, "p3"))

				// Act 3: Delete positive-size survivor "p3". Deferred zero-size entry "z1" is now shed, leaving "z2".
				_, ok := cache.Delete("p3")
				require.True(t, ok)
				_ = pac.EvaluateMemoryPressure()
				assert.False(t, hasKey(cache, "z1"))
				assert.True(t, hasKey(cache, "z2"))
			})

			t.Run("InterleavedZeroSizeAtTailShedsInTrueLRUOrderDuringByteShedding", func(t *testing.T) {
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.10))
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
					WithPressureFunc(func() float64 {
						return math.Float64frombits(pressureBits.Load())
					}),
				)
				pac := cache.(PressureAwareCache[testData])

				// Populate in LRU-to-MRU order: z1 (0B), p1 (80B), p2 (20B).
				_, err := cache.Put("z1", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				_, err = cache.Put("p1", testData{value: 1, dataSize: 80})
				require.NoError(t, err)
				_, err = cache.Put("p2", testData{value: 2, dataSize: 20})
				require.NoError(t, err)

				// Act: EvaluateMemoryPressure() at 0.95 (targetSize = 50B) must evict z1 and p1 in True LRU order.
				pressureBits.Store(math.Float64bits(0.95))
				evicted := pac.EvaluateMemoryPressure()
				require.Len(t, evicted, 2)
				assert.False(t, hasKey(cache, "z1"))
				assert.False(t, hasKey(cache, "p1"))
				assert.True(t, hasKey(cache, "p2"))
			})

			t.Run("InterleavedZeroSizeDuringByteSheddingPreservesLRUOrderWithoutOverEviction", func(t *testing.T) {
				// Arrange: Populate in LRU-to-MRU order with maxSize = 100, retention = 0.60:
				// p1=5B, p2=5B, p3=5B, p4=5B, z1=0B, p5=25B, z2=0B, z3=0B, p6=55B (total 100B, 9 entries, 3 zero-size).
				// Under Tier 2 (0.95): targetSize = 60B, targetLen = int(9*0.60) = 5, targetZeroCount = int(3*0.60) = 1.
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.10))
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.60),
					WithPressureFunc(func() float64 {
						return math.Float64frombits(pressureBits.Load())
					}),
				)
				pac := cache.(PressureAwareCache[testData])

				entries := []struct {
					key  string
					val  int64
					size uint64
				}{
					{"p1", 1, 5},
					{"p2", 2, 5},
					{"p3", 3, 5},
					{"p4", 4, 5},
					{"z1", 5, 0},
					{"p5", 6, 25},
					{"z2", 7, 0},
					{"z3", 8, 0},
					{"p6", 9, 55},
				}
				for _, e := range entries {
					_, err := cache.Put(e.key, testData{value: e.val, dataSize: e.size})
					require.NoError(t, err)
				}

				// Act: EvaluateMemoryPressure() at 0.95 must evict [p1, p2, p3, p4, z1, p5] in strict LRU order,
				// without evicting p5 before z1 and without over-evicting z2 after byte shedding reaches 55B <= 60B.
				pressureBits.Store(math.Float64bits(0.95))
				evicted := pac.EvaluateMemoryPressure()

				// Assert
				assertEvictedValues(t, evicted, []int64{1, 2, 3, 4, 5, 6})
				for _, k := range []string{"p1", "p2", "p3", "p4", "z1", "p5"} {
					assert.False(t, hasKey(cache, k))
				}
				for _, k := range []string{"z2", "z3", "p6"} {
					assert.True(t, hasKey(cache, k))
				}
			})

			t.Run("NoOpReplaceAfterEvaluateDoesNotRatchetZeroEntries", func(t *testing.T) {
				// Arrange: Populate [p1: 30B, p2: 30B, z1: 0B, z2: 0B, p3: 40B] (100B, 5 entries, retention = 0.60).
				// First EvaluateMemoryPressure() at 0.95 sheds p1, p2 and stops at targetLen = 3 with [z1, z2, p3] (40B <= 60B).
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.10))
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.60),
					WithPressureFunc(func() float64 {
						return math.Float64frombits(pressureBits.Load())
					}),
				)
				pac := cache.(PressureAwareCache[testData])

				for _, k := range []string{"p1", "p2"} {
					_, err := cache.Put(k, testData{value: 1, dataSize: 30})
					require.NoError(t, err)
				}
				for _, k := range []string{"z1", "z2"} {
					_, err := cache.Put(k, testData{value: 0, dataSize: 0})
					require.NoError(t, err)
				}
				_, err := cache.Put("p3", testData{value: 3, dataSize: 40})
				require.NoError(t, err)

				pressureBits.Store(math.Float64bits(0.95))
				require.Len(t, pac.EvaluateMemoryPressure(), 2)

				// Act: Call Replace("p3", 40B) (no-op delta) and Replace("p3", 50B) (grows p3 to 50B <= targetSize 60B) under 0.95.
				require.NoError(t, cache.Replace("p3", testData{value: 3, dataSize: 40}))
				require.NoError(t, cache.Replace("p3", testData{value: 4, dataSize: 50}))

				// Assert: Protected mutation with protectedSize <= targetSize must not ratchet-evict z1 or z2.
				assert.True(t, hasKey(cache, "z1"))
				assert.True(t, hasKey(cache, "z2"))
				assert.True(t, hasKey(cache, "p3"))
			})

			t.Run("PositiveToZeroOverwriteResetsLastReclaimedLen", func(t *testing.T) {
				// Arrange: Populate [p1: 30B, p2: 30B, z1: 0B, z2: 0B, p3: 40B] (100B, 5 entries, retention = 0.60).
				// First EvaluateMemoryPressure() at 0.95 sheds p1, p2 and stops at targetLen = 3 with [z1, z2, p3]
				// (lastReclaimedLen = 3, lastReclaimedZeroCount = 1).
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.10))
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.60),
					WithPressureFunc(func() float64 {
						return math.Float64frombits(pressureBits.Load())
					}),
				)
				pac := cache.(PressureAwareCache[testData])

				for _, k := range []string{"p1", "p2"} {
					_, err := cache.Put(k, testData{value: 1, dataSize: 30})
					require.NoError(t, err)
				}
				for _, k := range []string{"z1", "z2"} {
					_, err := cache.Put(k, testData{value: 0, dataSize: 0})
					require.NoError(t, err)
				}
				_, err := cache.Put("p3", testData{value: 3, dataSize: 40})
				require.NoError(t, err)

				pressureBits.Store(math.Float64bits(0.95))
				evictedFirst := pac.EvaluateMemoryPressure()
				require.Len(t, evictedFirst, 2)

				// Act: Convert positive survivor p3 (40B) into a 0B entry via overwrite under 0.95,
				// then verify EvaluateMemoryPressure() is idempotent.
				evictedOverwrite, err := cache.Put("p3", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				evictedSecond := pac.EvaluateMemoryPressure()

				// Assert: Overwriting p3 from 40B to 0B reset lastReclaimedLen from 3 to 0, allowing
				// the excess zero-size entries (z1, z2) to be shed down to targetZeroCount = 1 ("p3").
				assert.Len(t, evictedOverwrite, 2)
				assert.Empty(t, evictedSecond)
				assert.False(t, hasKey(cache, "z1"))
				assert.False(t, hasKey(cache, "z2"))
				assert.True(t, hasKey(cache, "p3"))
			})

			t.Run("ValidZeroPressureSampleWhileRefreshFlagSetResetsZeroWatermark", func(t *testing.T) {
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.10))
				cache := b.fn(
					1000,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
					WithPressureFunc(func() float64 {
						return math.Float64frombits(pressureBits.Load())
					}),
				)
				pac := cache.(PressureAwareCache[testData])

				for i := range 4 {
					_, err := cache.Put(fmt.Sprintf("z-%d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}

				// Shed 2 of 4 zero-size entries at 0.95 (sets lastReclaimedZeroCount = 2 and pressureNeedsRefresh = true).
				pressureBits.Store(math.Float64bits(0.95))
				require.Len(t, pac.EvaluateMemoryPressure(), 2)

				// Act: Drop pressure to exact 0.0 while pressureNeedsRefresh is set, and call EvaluateMemoryPressure().
				// Because 0.0 is a genuine sample (hasValidSample == true), lastReclaimedZeroCount must reset to 0.
				pressureBits.Store(math.Float64bits(0.0))
				assert.Empty(t, pac.EvaluateMemoryPressure())

				// Spike pressure back to 0.95: 1 of the 2 remaining zero-size entries ("z-2") must be shed.
				pressureBits.Store(math.Float64bits(0.95))
				evicted := pac.EvaluateMemoryPressure()
				assert.Len(t, evicted, 1)
				assert.False(t, hasKey(cache, "z-2"))
				assert.True(t, hasKey(cache, "z-3"))
			})
		})
	}
}

func TestPressure_TargetSizeBoundsAndSafeSizeCallbacks(t *testing.T) {
	for _, b := range allBackends[testData]() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("ReplaceSamplesValueSizeOutsideLock", func(t *testing.T) {
				// Arrange
				var c Cache[testData]
				var onWeigh func()
				c = b.fn(
					100,
					WithInvariantChecking(true),
					WithWeigher(func(_ string, v testData) uint64 {
						if onWeigh != nil {
							onWeigh()
						}
						return v.dataSize
					}),
				)
				_, err := c.Put("k1", testData{value: 1, dataSize: 10})
				require.NoError(t, err)

				readSucceededInsideWeigher := false
				onWeigh = func() {
					_, ok1 := c.Peek("k1")
					_, ok2 := c.Get("k1")
					if ok1 && ok2 {
						readSucceededInsideWeigher = true
					}
				}

				// Act
				err = c.Replace("k1", testData{value: 2, dataSize: 10})

				// Assert
				require.NoError(t, err)
				assert.True(t, readSucceededInsideWeigher)
			})

			t.Run("SafeSizeCallbackAndPressureAwareCache", func(t *testing.T) {
				// Arrange
				pressure := 0.10
				var cache Cache[testData]
				var onWeigh func()
				cache = b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionRetentionRatio(0.50),
					WithWeigher(func(_ string, v testData) uint64 {
						if onWeigh != nil {
							onWeigh()
						}
						return v.dataSize
					}),
				)
				for i := range 10 {
					_, err := cache.Put(fmt.Sprintf("k-%d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Re-entrant Peek inside Weigher and PressureAwareCache evaluation.
				onWeigh = func() {
					_, _ = cache.Peek("k-0")
				}
				err := cache.Replace("k-0", testData{value: 10, dataSize: 10})
				onWeigh = nil
				pac, ok := cache.(PressureAwareCache[testData])
				require.True(t, ok)
				pac.Compact()
				pressure = 0.95
				evicted := pac.EvaluateMemoryPressure()

				// Assert
				require.NoError(t, err)
				assert.Len(t, evicted, 5)
			})

			t.Run("ComputeTargetSizeBoundsAndZeroSizeEntryShedding", func(t *testing.T) {
				// Arrange & Act 1: Zero-size entry shedding at 50% retention.
				pressure := 0.10
				c := b.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionRetentionRatio(0.50),
					testDataWeigher,
				).(PressureAwareCache[testData])
				for i := range 10 {
					_, err := c.Put(fmt.Sprintf("zero-%d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}
				pressure = 0.95
				evicted := c.EvaluateMemoryPressure()

				// Assert 1
				assert.Len(t, evicted, 5)
				for i := range 5 {
					assert.False(t, hasKey(c, fmt.Sprintf("zero-%d", i)))
				}
				for i := 5; i < 10; i++ {
					assert.True(t, hasKey(c, fmt.Sprintf("zero-%d", i)))
				}

				// Arrange & Act 2: maxSize == 1 with positive retention (0.50) clamps targetSize to 1
				// instead of truncating to 0, so a 1-byte entry survives EvaluateMemoryPressure().
				minCache := b.fn(
					1,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return 0.95 }),
					WithEvictionRetentionRatio(0.50),
					testDataWeigher,
				).(PressureAwareCache[testData])
				_, err := minCache.Put("one-byte", testData{value: 1, dataSize: 1})
				require.NoError(t, err)
				assert.Empty(t, minCache.EvaluateMemoryPressure())
				assert.True(t, hasKey(minCache, "one-byte"))

				// Arrange & Act 3: maxSize == math.MaxUint64 with retention 1.0, 0.50, and Nextafter(1.0, 0.0)
				// avoids float64-to-uint64 overflow in targetSize calculation.
				for _, retention := range []float64{1.0, 0.50, math.Nextafter(1.0, 0.0)} {
					maxCache := b.fn(
						math.MaxUint64,
						WithInvariantChecking(true),
						WithPressureFunc(func() float64 { return 0.95 }),
						WithEvictionRetentionRatio(retention),
						testDataWeigher,
					).(PressureAwareCache[testData])
					_, err := maxCache.Put("large", testData{value: 1, dataSize: 1 << 62})
					require.NoError(t, err)
					assert.Empty(t, maxCache.EvaluateMemoryPressure())
					assert.True(t, hasKey(maxCache, "large"))
				}
			})

			t.Run("EvaluateMemoryPressureWithZeroRetentionFlushesAllEntries", func(t *testing.T) {
				// Arrange
				pressure := 0.10
				c := b.fn(
					1000,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionRetentionRatio(0.0),
					testDataWeigher,
				).(PressureAwareCache[testData])
				_, err := c.Put("only-key", testData{value: 1, dataSize: 100})
				require.NoError(t, err)

				// Act: EvaluateMemoryPressure passes the unprotected sentinel so all entries are shed when retention == 0.0.
				pressure = 0.95
				evicted := c.EvaluateMemoryPressure()

				// Assert
				assert.Len(t, evicted, 1)
				assert.False(t, hasKey(c, "only-key"))
			})
		})
	}
}

func TestPressure_BelowTier2ResetsZeroWatermarks(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("SingleSurvivorDeletionResetsWatermarks", func(t *testing.T) {
				probe := newPressureProbe(0.10)
				cache := b.fn(
					100,
					WithInvariantChecking(true),
					probe.Option(),
					WithCompactionThreshold(0.70),
					WithEvictionThreshold(0.80),
					WithEvictionRetentionRatio(0.50),
				).(PressureAwareCache[testData])

				for i := range 127 {
					_, err := cache.Put(fmt.Sprintf("p-%03d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}
				_, err := cache.Put("survivor", testData{value: 999, dataSize: 0})
				require.NoError(t, err)

				// At pressure 0.80 (retention 0.50), 64 of 128 zero-size entries survive,
				// setting lastReclaimedZeroCount = 64, lastReclaimedLen = 64, peakEntryLen = 64.
				probe.Set(0.80)
				evicted := cache.EvaluateMemoryPressure()
				require.Len(t, evicted, 64)
				require.True(t, hasKey(cache, "survivor"))

				// Drop below Tier 2 and delete all 63 remaining "p-" entries in one operation,
				// triggering shouldReclaimSingleSurvivorOnDelete == true with "survivor" as the sole entry.
				probe.Set(0.10)
				cache.DeletePrefix("p-")
				require.True(t, hasKey(cache, "survivor"))

				// Return to Tier 2 (retention 0.75 => int(1 * 0.75) == 0).
				// Because the single-survivor deletion below Tier 2 reset lastReclaimedZeroCount to 0,
				// "survivor" must now be shed.
				probe.Set(0.875)
				evictedAfter := cache.EvaluateMemoryPressure()
				assert.Len(t, evictedAfter, 1)
				assert.False(t, hasKey(cache, "survivor"))
			})

			t.Run("MissedMutationsBelowTier2ResetsWatermarks", func(t *testing.T) {
				ops := []struct {
					name string
					act  func(c Cache[testData])
				}{
					{"DeleteMiss", func(c Cache[testData]) { _, _ = c.Delete("non-existent-key") }},
					{"DeletePrefixMiss", func(c Cache[testData]) { c.DeletePrefix("non-existent-prefix") }},
					{"ReplaceMiss", func(c Cache[testData]) {
						_ = c.Replace("non-existent-key", testData{value: 1, dataSize: 1})
					}},
				}

				for _, op := range ops {
					t.Run(op.name, func(t *testing.T) {
						probe := newPressureProbe(0.10)
						cache := b.fn(
							100,
							WithInvariantChecking(true),
							probe.Option(),
							WithCompactionThreshold(0.70),
							WithEvictionThreshold(0.80),
							WithEvictionRetentionRatio(0.50),
						).(PressureAwareCache[testData])

						_, err := cache.Put("z1", testData{value: 1, dataSize: 0})
						require.NoError(t, err)
						_, err = cache.Put("z2", testData{value: 2, dataSize: 0})
						require.NoError(t, err)

						probe.Set(0.875)
						evicted := cache.EvaluateMemoryPressure()
						require.Len(t, evicted, 1)
						require.True(t, hasKey(cache, "z2"))

						probe.Set(0.10)
						op.act(cache)

						probe.Set(0.875)
						evictedAfter := cache.EvaluateMemoryPressure()
						assert.Len(t, evictedAfter, 1)
						assert.False(t, hasKey(cache, "z2"))
					})
				}
			})

			t.Run("PostRetryPressureResolutionAndSamplingGoroutineWatermarkGuard", func(t *testing.T) {
				// Arrange: Seed 4 zero-size entries and shed 2 under Tier 2 (0.95) so lastReclaimedZeroCount == 2.
				// Then exercise post-retry pressure resolution when a re-entrant operation inside PressureFunc
				// runs while watermarks are active, followed by a non-sampling below-Tier-2 post-retry resolution.
				var cache Cache[testData]
				var pac PressureAwareCache[testData]
				var pressureBits atomic.Uint64
				pressureBits.Store(math.Float64bits(0.10))
				var reentrantMiss atomic.Bool

				cache = b.fn(
					1000,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.50),
					WithPressureFunc(func() float64 {
						if reentrantMiss.CompareAndSwap(true, false) && cache != nil {
							_, _ = cache.Delete("absent-during-sampling")
						}
						return math.Float64frombits(pressureBits.Load())
					}),
				)
				pac = cache.(PressureAwareCache[testData])

				for i := range 4 {
					_, err := cache.Put(fmt.Sprintf("z-%d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}

				pressureBits.Store(math.Float64bits(0.95))
				require.Len(t, pac.EvaluateMemoryPressure(), 2)

				// Act 1: Re-entrant Delete("absent-during-sampling") inside PressureFunc while outer pressure is 0.95
				// must not clear lastReclaimedZeroCount, so EvaluateMemoryPressure() remains idempotent (0 evictions).
				reentrantMiss.Store(true)
				assert.Empty(t, pac.EvaluateMemoryPressure())
				assert.True(t, hasKey(cache, "z-2"))
				assert.True(t, hasKey(cache, "z-3"))

				// Act 2: Non-sampling operation below Tier 2 (0.10) resets the watermark so a subsequent 0.95 evaluation sheds z-2.
				pressureBits.Store(math.Float64bits(0.10))
				_, _ = cache.Delete("absent-below-tier2")
				pressureBits.Store(math.Float64bits(0.95))
				evictedAfterReset := pac.EvaluateMemoryPressure()
				assert.Len(t, evictedAfterReset, 1)
				assert.False(t, hasKey(cache, "z-2"))
				assert.True(t, hasKey(cache, "z-3"))
			})
		})
	}
}

func TestPressure_EvictionCallbacks_Tier2ExplicitAndForeground(t *testing.T) {
	for _, b := range allBackends[string]() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("ExplicitEvaluateMemoryPressureAndZeroSizeShedding", func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.10)
				var valEvents []recordedEvictValue
				var entryEvents []recordedEvictEntry

				cache := b.fn(
					100,
					WithInvariantChecking(true),
					probe.Option(),
					WithEvictionThreshold(0.80),
					WithEvictionRetentionRatio(0.50),
					WithWeigher(func(_ string, v string) uint64 { return uint64(len(v)) }),
					WithOnEvictValue(func(v string, r EvictionReason) {
						valEvents = append(valEvents, recordedEvictValue{val: v, reason: r})
					}),
					WithOnEvictEntry(func(k, v string, r EvictionReason) {
						entryEvents = append(entryEvents, recordedEvictEntry{key: k, val: v, reason: r})
					}),
				).(PressureAwareCache[string])

				_, err := cache.Put("pos1", "123456789012345678901234567890") // 30B (LRU)
				require.NoError(t, err)
				_, err = cache.Put("pos2", "123456789012345678901234567890") // 30B
				require.NoError(t, err)
				_, err = cache.Put("zero1", "") // 0B
				require.NoError(t, err)
				_, err = cache.Put("zero2", "") // 0B (MRU)
				require.NoError(t, err)
				require.Empty(t, valEvents)
				require.Empty(t, entryEvents)

				// Act: Spike pressure to 1.0 (retention ratio 0.50 -> targetSize=30B, targetZeroCount=1)
				probe.Set(1.0)
				evicted := cache.EvaluateMemoryPressure()

				// Assert
				assert.Equal(t, []string{"123456789012345678901234567890", ""}, evicted)
				assert.Equal(t, []recordedEvictValue{
					{val: "123456789012345678901234567890", reason: EvictionReasonPressure},
					{val: "", reason: EvictionReasonPressure},
				}, valEvents)
				assert.Equal(t, []recordedEvictEntry{
					{key: "pos1", val: "123456789012345678901234567890", reason: EvictionReasonPressure},
					{key: "zero1", val: "", reason: EvictionReasonPressure},
				}, entryEvents)
			})

			t.Run("ForegroundTier2SheddingOnPutReplaceDeleteAndDeletePrefix", func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.10)
				var entryEvents []recordedEvictEntry

				cache := b.fn(
					30,
					WithInvariantChecking(true),
					probe.Option(),
					WithEvictionThreshold(0.80),
					WithEvictionRetentionRatio(0.50),
					WithWeigher(func(_ string, v string) uint64 { return uint64(len(v)) }),
					WithOnEvictEntry(func(k, v string, r EvictionReason) {
						entryEvents = append(entryEvents, recordedEvictEntry{key: k, val: v, reason: r})
					}),
				)

				// Seed 3 entries of 10B each (total 30B): k1 (LRU), k2, k3 (MRU)
				_, err := cache.Put("p/k1", "0123456789")
				require.NoError(t, err)
				_, err = cache.Put("p/k2", "abcdefghij")
				require.NoError(t, err)
				_, err = cache.Put("p/k3", "klmnopqrst")
				require.NoError(t, err)

				// 1. Replace LRU tail ("p/k1") while under Tier 2 (1.0 -> retention 0.50 -> targetSize=15B):
				// "p/k1" is replaced first (EvictionReasonReplaced with old value "0123456789"),
				// and then Tier 2 pressure sheds the tail ("p/k1" with new value "BBBBBBBBBB" and "p/k2") with EvictionReasonPressure!
				entryEvents = nil
				probe.Set(1.0)
				require.NoError(t, cache.Replace("p/k1", "BBBBBBBBBB"))
				assert.Equal(t, []recordedEvictEntry{
					{key: "p/k1", val: "0123456789", reason: EvictionReasonReplaced},
					{key: "p/k1", val: "BBBBBBBBBB", reason: EvictionReasonPressure},
					{key: "p/k2", val: "abcdefghij", reason: EvictionReasonPressure},
				}, entryEvents)

				// 2. Re-populate at normal pressure (0.10): d1(10B), d2(10B), d3(10B)
				probe.Set(0.10)
				_, _ = cache.Delete("p/k3")
				_, err = cache.Put("p/d1", "1111111111")
				require.NoError(t, err)
				_, err = cache.Put("p/d2", "2222222222")
				require.NoError(t, err)
				_, err = cache.Put("p/d3", "3333333333")
				require.NoError(t, err)

				// Delete "p/d3" while under Tier 2 (1.0 -> retention 0.50 of remaining 20B = 10B -> sheds "p/d1")
				entryEvents = nil
				probe.Set(1.0)
				delVal, ok := cache.Delete("p/d3")
				require.True(t, ok)
				assert.Equal(t, "3333333333", delVal)
				assert.Equal(t, []recordedEvictEntry{
					{key: "p/d3", val: "3333333333", reason: EvictionReasonDeleted},
					{key: "p/d1", val: "1111111111", reason: EvictionReasonPressure},
				}, entryEvents)

				// 3. Re-populate at normal pressure (0.10): sub/1(10B), keep/1(10B), keep/2(10B)
				probe.Set(0.10)
				_, _ = cache.Delete("p/d2")
				_, err = cache.Put("keep/1", "aaaaaaaaaa")
				require.NoError(t, err)
				_, err = cache.Put("keep/2", "bbbbbbbbbb")
				require.NoError(t, err)
				_, err = cache.Put("sub/1", "cccccccccc")
				require.NoError(t, err)

				// DeletePrefix("sub/") while under Tier 2 (1.0 -> retention 0.50 of remaining 20B = 10B -> sheds "keep/1")
				entryEvents = nil
				probe.Set(1.0)
				cache.DeletePrefix("sub/")
				assert.Equal(t, []recordedEvictEntry{
					{key: "sub/1", val: "cccccccccc", reason: EvictionReasonDeleted},
					{key: "keep/1", val: "aaaaaaaaaa", reason: EvictionReasonPressure},
				}, entryEvents)

				// 4. Re-populate at normal pressure (0.10): keep/2(10B, LRU), f1(10B), f2(10B, MRU) = 30B
				probe.Set(0.10)
				_, err = cache.Put("p/f1", "dddddddddd")
				require.NoError(t, err)
				_, err = cache.Put("p/f2", "eeeeeeeeee")
				require.NoError(t, err)

				// Put new entry "p/f3" (10B) while under Tier 2 (1.0 -> retention 0.50 -> targetSize=15B):
				// First evicts LRU "keep/2" for capacity (EvictionReasonCapacity), then inline Tier 2
				// pressure shedding evicts "p/f1" and "p/f2" (EvictionReasonPressure) while protecting MRU "p/f3"!
				entryEvents = nil
				probe.Set(1.0)
				evictedPut, err := cache.Put("p/f3", "ffffffffff")
				require.NoError(t, err)
				assert.Equal(t, []string{"bbbbbbbbbb", "dddddddddd", "eeeeeeeeee"}, evictedPut)
				assert.Equal(t, []recordedEvictEntry{
					{key: "keep/2", val: "bbbbbbbbbb", reason: EvictionReasonCapacity},
					{key: "p/f1", val: "dddddddddd", reason: EvictionReasonPressure},
					{key: "p/f2", val: "eeeeeeeeee", reason: EvictionReasonPressure},
				}, entryEvents)
			})
		})
	}
}

func TestClonePrefix_AllByteValuesAndZeroAllocations(t *testing.T) {
	t.Run("All256SingleByteStringsPreserveExactByteAndBackingIsolation", func(t *testing.T) {
		// Arrange
		var raw [256]byte
		for i := range 256 {
			raw[i] = byte(i)
		}
		callerBacking := string(raw[:])

		// Act
		var cloned [256]string
		for i := range 256 {
			cloned[i] = clonePrefix(callerBacking[i : i+1])
		}
		clonedEmpty := clonePrefix("")
		clonedMulti := clonePrefix(callerBacking[128:132])

		// Assert
		assert.Empty(t, clonedEmpty)
		assert.Equal(t, callerBacking[128:132], clonedMulti)
		assert.NotSame(t, unsafe.StringData(callerBacking[128:132]), unsafe.StringData(clonedMulti))
		for i := range 256 {
			sub := callerBacking[i : i+1]
			require.Len(t, byteStrings[i], 1, "byteStrings[%d] must be 1 byte", i)
			require.Equal(t, byte(i), byteStrings[i][0], "byteStrings[%d] byte mismatch", i)
			require.Len(t, cloned[i], 1, "clonePrefix for byte 0x%02x must have length 1", i)
			require.Equal(t, sub, cloned[i], "clonePrefix for byte 0x%02x must equal input", i)
			assert.Equal(t, byte(i), cloned[i][0], "clonePrefix for byte 0x%02x must preserve exact raw byte", i)
			assert.NotSame(t, unsafe.StringData(callerBacking), unsafe.StringData(cloned[i]),
				"clonePrefix for byte 0x%02x must not alias callerBacking", i)
			assert.NotSame(t, unsafe.StringData(sub), unsafe.StringData(cloned[i]),
				"clonePrefix for byte 0x%02x must not alias substring slice data pointer", i)
		}
	})

	t.Run("ZeroAllocationsAcrossAll256SingleBytePrefixes", func(t *testing.T) {
		// Arrange
		var raw [256]byte
		for i := range 256 {
			raw[i] = byte(i)
		}
		callerBacking := string(raw[:])

		// Act
		var sink string
		allocsAll := testing.AllocsPerRun(100, func() {
			for i := range 256 {
				sink = clonePrefix(callerBacking[i : i+1])
			}
		})
		allocsHighBytes := testing.AllocsPerRun(100, func() {
			for i := 128; i < 256; i++ {
				sink = clonePrefix(callerBacking[i : i+1])
			}
		})
		_ = sink

		// Assert
		assert.Zero(t, allocsAll, "clonePrefix across all 256 1-byte strings (0x00..0xFF) must allocate 0 objects")
		assert.Zero(t, allocsHighBytes, "clonePrefix across high-bit 1-byte strings (0x80..0xFF) must allocate 0 objects")
	})

	t.Run("ZeroAllocationsOnHotPathsWithNonASCIIAndUTF8Keys", testZeroAllocationsOnHotPathsWithNonASCII)
}

func testZeroAllocationsOnHotPathsWithNonASCII(t *testing.T) {
	backends := []struct {
		name string
		newC func() Cache[int]
	}{
		{name: "MapCache", newC: func() Cache[int] { return NewMapCache[int](64) }},
		{name: "RadixCache", newC: func() Cache[int] { return NewRadixCache[int](64) }},
		{name: "ArenaRadixCache", newC: func() Cache[int] { return NewArenaRadixCache[int](64) }},
	}

	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			verifyBackendZeroAllocHotPaths(t, b.name, b.newC())
		})
	}
}

func verifyBackendZeroAllocHotPaths(t *testing.T, name string, cache Cache[int]) {
	t.Helper()
	keys := []string{"\x80", "\xaf", "\xff", "café", "cafè"}
	for idx, k := range keys {
		_, err := cache.Put(k, idx+1)
		require.NoError(t, err)
	}
	valSeq := cache.Values()

	getAllocs := testing.AllocsPerRun(100, func() {
		for _, k := range keys {
			v, ok := cache.Get(k)
			if !ok || v == 0 {
				panic("unexpected Get miss")
			}
		}
	})
	peekAllocs := testing.AllocsPerRun(100, func() {
		for _, k := range keys {
			v, ok := cache.Peek(k)
			if !ok || v == 0 {
				panic("unexpected Peek miss")
			}
		}
	})
	putInPlaceAllocs := testing.AllocsPerRun(100, func() {
		for _, k := range keys {
			if _, err := cache.Put(k, 42); err != nil {
				panic(err)
			}
		}
	})
	replaceAllocs := testing.AllocsPerRun(100, func() {
		for _, k := range keys {
			if err := cache.Replace(k, 99); err != nil {
				panic(err)
			}
		}
	})
	valuesAllocs := testing.AllocsPerRun(100, func() {
		valSeq(func(v int) bool {
			return v > 0
		})
	})
	statsAllocs := testing.AllocsPerRun(100, func() {
		st := cache.Stats()
		if st.Len != len(keys) {
			panic("unexpected Stats().Len")
		}
	})

	assert.Zero(t, getAllocs, "%s Get must be 0 allocs/op", name)
	assert.Zero(t, peekAllocs, "%s Peek must be 0 allocs/op", name)
	assert.Zero(t, putInPlaceAllocs, "%s in-place Put must be 0 allocs/op", name)
	assert.Zero(t, replaceAllocs, "%s Replace must be 0 allocs/op", name)
	assert.Zero(t, valuesAllocs, "%s Values() must be 0 allocs/op", name)
	assert.Zero(t, statsAllocs, "%s Stats() must be 0 allocs/op", name)
}
