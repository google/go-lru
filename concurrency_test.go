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

package lru_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-lru"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type concValue struct {
	id   string
	size uint64
}

var concWeigher = lru.WithWeigher(func(_ string, v concValue) uint64 {
	return v.size
})

func allEngines() []struct {
	name        string
	constructor func(maxSize uint64, opts ...lru.Option) lru.Cache[concValue]
} {
	wrap := func(fn func(uint64, ...lru.Option) lru.Cache[concValue]) func(uint64, ...lru.Option) lru.Cache[concValue] {
		return func(maxSize uint64, opts ...lru.Option) lru.Cache[concValue] {
			return fn(maxSize, slices.Concat([]lru.Option{concWeigher}, opts)...)
		}
	}
	return []struct {
		name        string
		constructor func(maxSize uint64, opts ...lru.Option) lru.Cache[concValue]
	}{
		{"MapCache", wrap(lru.NewMapCache[concValue])},
		{"RadixCache", wrap(lru.NewRadixCache[concValue])},
		{"ArenaRadixCache", wrap(lru.NewArenaRadixCache[concValue])},
	}
}

func exerciseConcurrentIterators(t *testing.T, cache lru.Cache[concValue], step, maxItems int) {
	t.Helper()
	switch step % 3 {
	case 0:
		count := 0
		for k, v := range cache.All() {
			assert.NotEmpty(t, k)
			assert.NotEmpty(t, v.id)
			count++
			if step%2 == 0 && count >= maxItems {
				break
			}
		}
	case 1:
		count := 0
		for k := range cache.Keys() {
			assert.NotEmpty(t, k)
			count++
			if step%2 == 0 && count >= maxItems {
				break
			}
		}
	case 2:
		count := 0
		for v := range cache.Values() {
			assert.NotEmpty(t, v.id)
			count++
			if step%2 == 0 && count >= maxItems {
				break
			}
		}
	}
}

func putConcurrentEntry(t *testing.T, cache lru.Cache[concValue], key string) {
	t.Helper()
	_, err := cache.Put(key, concValue{id: key, size: 10})
	if err != nil {
		assert.ErrorIs(t, err, lru.ErrInvalidEntrySize) //nolint:testifylint // wg.Go runs in a child goroutine
	}
}

func replaceConcurrentEntry(t *testing.T, cache lru.Cache[concValue], key, id string, size uint64) {
	t.Helper()
	err := cache.Replace(key, concValue{id: id, size: size})
	if err != nil {
		assert.ErrorIs(t, err, lru.ErrEntryNotExist) //nolint:testifylint // wg.Go runs in a child goroutine
	}
}

func runMixedOpStep(t *testing.T, cache lru.Cache[concValue], r *rand.Rand, step, numKeys int) {
	t.Helper()
	op := r.IntN(100)
	kIdx := r.IntN(numKeys * 2)
	dirIdx := kIdx % 5
	subIdx := (kIdx / 5) % 10
	key := fmt.Sprintf("dir_%02d/sub_%02d/file_%03d.txt", dirIdx, subIdx, kIdx)

	switch {
	case op < 28:
		putConcurrentEntry(t, cache, key)
	case op < 50:
		_, _ = cache.Get(key)
	case op < 64:
		_, _ = cache.Peek(key)
	case op < 74:
		replaceConcurrentEntry(t, cache, key, key+"_upd", 10)
	case op < 82:
		sz := uint64(5 + (kIdx%3)*10) // 5, 15, or 25 (shrinks or grows weight)
		replaceConcurrentEntry(t, cache, key, key+"_sz", sz)
	case op < 88:
		_, _ = cache.Delete(key)
	case op < 94:
		exerciseConcurrentIterators(t, cache, step, 5)
	default:
		prefix := fmt.Sprintf("dir_%02d/", dirIdx)
		cache.DeletePrefix(prefix)
	}
}

// TestConcurrency_MixedOperations exercises all Cache methods concurrently across
// multiple goroutines on all three cache engines.
func TestConcurrency_MixedOperations(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			// Arrange
			const (
				numGoroutines = 16
				opsPerWorker  = 300
				numKeys       = 100
				capacity      = 100000
			)

			cache := eng.constructor(capacity)

			for i := range numKeys {
				key := fmt.Sprintf("dir_%02d/sub_%02d/file_%03d.txt", i%5, (i/5)%10, i)
				_, err := cache.Put(key, concValue{id: key, size: 10})
				require.NoError(t, err)
			}

			// Act
			var wg sync.WaitGroup
			for g := range numGoroutines {
				wg.Go(func() {
					r := rand.New(rand.NewPCG(uint64(g*10007+42), 0))
					for step := range opsPerWorker {
						runMixedOpStep(t, cache, r, step, numKeys)
					}
				})
			}

			wg.Wait()

			// Assert
			_, err := cache.Put("post_conc_check", concValue{id: "post_conc_check", size: 10})
			require.NoError(t, err)
			val, ok := cache.Get("post_conc_check")
			assert.True(t, ok)
			assert.Equal(t, "post_conc_check", val.id)
		})
	}
}

// TestConcurrency_DeletePrefixAtomicity verifies that DeletePrefix
// atomically removes all matching keys without affecting unrelated prefixes under
// concurrent writer load.
func TestConcurrency_DeletePrefixAtomicity(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			// Arrange
			const (
				numWriters   = 8
				opsPerWriter = 100
			)

			cache := eng.constructor(100000)

			for i := range 20 {
				kTarget := fmt.Sprintf("/target/init_%d", i)
				_, err := cache.Put(kTarget, concValue{id: kTarget, size: 10})
				require.NoError(t, err)
				kKeep := fmt.Sprintf("/keep/init_%d", i)
				_, err = cache.Put(kKeep, concValue{id: kKeep, size: 10})
				require.NoError(t, err)
			}

			// Act
			var writerWg sync.WaitGroup
			for w := range numWriters {
				writerWg.Go(func() {
					for op := range opsPerWriter {
						key := fmt.Sprintf("/target/w%d_%d", w, op)
						_, err := cache.Put(key, concValue{id: key, size: 10})
						assert.NoError(t, err) //nolint:testifylint // wg.Go runs in a child goroutine
					}
				})
			}

			for range 10 {
				cache.DeletePrefix("/target/")
			}

			writerWg.Wait()

			// Final prefix delete after writers finish must remove all /target/ keys.
			cache.DeletePrefix("/target/")

			// Assert
			for i := range 20 {
				kTarget := fmt.Sprintf("/target/init_%d", i)
				_, okTarget := cache.Get(kTarget)
				assert.False(t, okTarget)
				kKeep := fmt.Sprintf("/keep/init_%d", i)
				_, okKeep := cache.Get(kKeep)
				assert.True(t, okKeep)
			}

			for w := range numWriters {
				for op := range opsPerWriter {
					key := fmt.Sprintf("/target/w%d_%d", w, op)
					_, ok := cache.Get(key)
					assert.False(t, ok)
				}
			}
		})
	}
}

// TestConcurrency_ParallelReadersPeek verifies concurrent
// Peek calls execute safely and preserve LRU eviction order.
func TestConcurrency_ParallelReadersPeek(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			// Arrange
			const (
				totalKeys  = 200
				numReaders = 16
				readsPerG  = 500
				capacity   = 50000
			)

			cache := eng.constructor(capacity)

			for i := range totalKeys {
				k := fmt.Sprintf("key_%04d", i)
				_, err := cache.Put(k, concValue{id: k, size: 10})
				require.NoError(t, err)
			}

			// Act
			var readerWg sync.WaitGroup
			for r := range numReaders {
				readerWg.Go(func() {
					for i := range readsPerG {
						targetKey := "key_0000"
						if i%2 == 1 {
							targetKey = fmt.Sprintf("key_%04d", (r*17+i)%totalKeys)
						}
						_, ok := cache.Peek(targetKey)
						if !assert.True(t, ok) {
							return
						}
					}
				})
			}
			readerWg.Wait()

			// Fill remaining capacity (50,000 - 2,000 = 48,000 bytes).
			for i := range 48 {
				k := fmt.Sprintf("filler_%03d", i)
				_, err := cache.Put(k, concValue{id: k, size: 1000})
				require.NoError(t, err)
			}

			// Next 10-byte write must evict key_0000 (the untouched LRU tail).
			evicted, err := cache.Put("overflow_trigger", concValue{id: "overflow", size: 10})

			// Assert
			require.NoError(t, err)
			require.Len(t, evicted, 1)
			assert.Equal(t, "key_0000", evicted[0].id)
		})
	}
}

func verifyConcurrentAllOrder(t *testing.T, cache lru.Cache[concValue], totalKeys, limit int, earlyBreak bool) {
	t.Helper()
	idx := totalKeys - 1
	count := 0
	for k, v := range cache.All() {
		expected := fmt.Sprintf("dir_%02d/key_%04d", idx%5, idx)
		if !assert.Equal(t, expected, k) || !assert.Equal(t, expected, v.id) {
			return
		}
		idx--
		count++
		if earlyBreak && count >= limit {
			break
		}
	}
	if !earlyBreak {
		assert.Equal(t, totalKeys, count)
	}
}

func verifyConcurrentKeysOrder(t *testing.T, cache lru.Cache[concValue], totalKeys, limit int, earlyBreak bool) {
	t.Helper()
	idx := totalKeys - 1
	count := 0
	for k := range cache.Keys() {
		expected := fmt.Sprintf("dir_%02d/key_%04d", idx%5, idx)
		if !assert.Equal(t, expected, k) {
			return
		}
		idx--
		count++
		if earlyBreak && count >= limit {
			break
		}
	}
	if !earlyBreak {
		assert.Equal(t, totalKeys, count)
	}
}

func verifyConcurrentValuesOrder(t *testing.T, cache lru.Cache[concValue], totalKeys, limit int, earlyBreak bool) {
	t.Helper()
	idx := totalKeys - 1
	count := 0
	for v := range cache.Values() {
		expected := fmt.Sprintf("dir_%02d/key_%04d", idx%5, idx)
		if !assert.Equal(t, expected, v.id) {
			return
		}
		idx--
		count++
		if earlyBreak && count >= limit {
			break
		}
	}
	if !earlyBreak {
		assert.Equal(t, totalKeys, count)
	}
}

// TestConcurrency_ParallelIteratorsPreservesLRUOrder verifies that concurrent
// All(), Keys(), and Values() iterators (both full traversals and early breaks)
// observe deterministic MRU-to-LRU order under RLock and never alter LRU eviction order.
func TestConcurrency_ParallelIteratorsPreservesLRUOrder(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			// Arrange
			const (
				totalKeys  = 100
				numReaders = 16
				itersPerG  = 50
				capacity   = 1000 // exact capacity for 100 * 10-byte entries
			)

			cache := eng.constructor(capacity, lru.WithInvariantChecking(true))

			for i := range totalKeys {
				k := fmt.Sprintf("dir_%02d/key_%04d", i%5, i)
				_, err := cache.Put(k, concValue{id: k, size: 10})
				require.NoError(t, err)
			}

			// Act
			var readerWg sync.WaitGroup
			for r := range numReaders {
				readerWg.Go(func() {
					for i := range itersPerG {
						earlyBreak := i%2 == 1
						limit := 1 + (r+i)%10

						switch (r + i) % 3 {
						case 0:
							verifyConcurrentAllOrder(t, cache, totalKeys, limit, earlyBreak)
						case 1:
							verifyConcurrentKeysOrder(t, cache, totalKeys, limit, earlyBreak)
						case 2:
							verifyConcurrentValuesOrder(t, cache, totalKeys, limit, earlyBreak)
						}
					}
				})
			}
			readerWg.Wait()

			// Assert: Inserting a new 10-byte entry must evict the original LRU tail (dir_00/key_0000).
			evicted, err := cache.Put("overflow_trigger", concValue{id: "overflow", size: 10})
			require.NoError(t, err)
			require.Len(t, evicted, 1)
			assert.Equal(t, "dir_00/key_0000", evicted[0].id)
		})
	}
}

// TestConcurrency_EvictionThrashingWithInvariants stresses concurrent evictions under
// tight capacity with invariant checking enabled.
func TestConcurrency_EvictionThrashingWithInvariants(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			// Arrange
			const (
				numGoroutines = 8
				opsPerWorker  = 150
				capacity      = 300
			)

			cache := eng.constructor(capacity, lru.WithInvariantChecking(true))
			var wg sync.WaitGroup
			var totalEvictions atomic.Int64

			// Act
			for range numGoroutines {
				wg.Go(func() {
					for i := range opsPerWorker {
						key := fmt.Sprintf("inv/p%d/item_%d", i%5, i)
						evicted, err := cache.Put(key, concValue{id: key, size: 10})
						assert.NoError(t, err) //nolint:testifylint // wg.Go runs in a child goroutine
						totalEvictions.Add(int64(len(evicted)))
						if i%20 == 0 {
							cache.DeletePrefix(fmt.Sprintf("inv/p%d/", i%5))
						}
					}
				})
			}

			wg.Wait()

			// Assert
			assert.Positive(t, totalEvictions.Load())
		})
	}
}

func oscillateTestPressure(setPressure func(float64), idx int, normal float64) {
	switch idx % 3 {
	case 0:
		setPressure(normal)
	case 1:
		setPressure(0.80)
	case 2:
		setPressure(0.95)
	}
}

func runPressureCompactionStep(t *testing.T, cache lru.Cache[concValue], reclaimer lru.PressureAwareCache[concValue], r *rand.Rand, step, numKeys int, capacity uint64) {
	t.Helper()
	op := r.IntN(100)
	kIdx := r.IntN(numKeys)
	dirIdx := kIdx % 6
	subIdx := (kIdx / 6) % 5
	key := fmt.Sprintf("mp_dir_%02d/sub_%02d/file_%03d.dat", dirIdx, subIdx, kIdx)

	switch {
	case op < 28:
		putConcurrentEntry(t, cache, key)
	case op < 48:
		_, _ = cache.Get(key)
	case op < 64:
		_, _ = cache.Peek(key)
	case op < 72:
		replaceConcurrentEntry(t, cache, key, key+"_u", 10)
	case op < 80:
		sz := uint64(5 + (step%2)*10) // 5 or 15
		replaceConcurrentEntry(t, cache, key, key+"_sz", sz)
	case op < 86:
		_, _ = cache.Delete(key)
	case op < 91:
		exerciseConcurrentIterators(t, cache, step, 4)
	case op < 95:
		prefix := fmt.Sprintf("mp_dir_%02d/", dirIdx)
		cache.DeletePrefix(prefix)
	case op < 97:
		st := cache.Stats()
		assert.Equal(t, capacity, st.MaxSize)
	case op < 99:
		reclaimer.Compact()
	default:
		_ = reclaimer.EvaluateMemoryPressure()
	}
}

// TestConcurrency_MemoryPressureCompactionAndEviction exercises concurrent reads, writes,
// replacements, size updates, deletions, prefix deletions, and explicit/automatic compactions
// while memory pressure dynamically oscillates across normal, moderate, and critical tiers
// with WithInvariantChecking(true) enabled.
func TestConcurrency_MemoryPressureCompactionAndEviction(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			// Arrange
			const (
				numGoroutines = 16
				opsPerWorker  = 250
				numKeys       = 120
				capacity      = 4000
			)

			var pressureBits atomic.Uint64
			setPressure := func(p float64) {
				pressureBits.Store(uint64(p * 1000))
			}
			getPressure := func() float64 {
				return float64(pressureBits.Load()) / 1000.0
			}
			setPressure(0.20)

			cache := eng.constructor(
				capacity,
				lru.WithInvariantChecking(true),
				lru.WithPressureFunc(getPressure),
				lru.WithCompactionThreshold(0.75),
				lru.WithEvictionThreshold(0.90),
				lru.WithEvictionRetentionRatio(0.50),
			)

			reclaimer, ok := cache.(lru.PressureAwareCache[concValue])
			require.True(t, ok, "expected %s to implement PressureAwareCache", eng.name)

			// Act
			var wg sync.WaitGroup
			for g := range numGoroutines {
				wg.Go(func() {
					r := rand.New(rand.NewPCG(uint64(g*13337+99), 0))
					for step := range opsPerWorker {
						oscillateTestPressure(setPressure, g+step, 0.20)
						runPressureCompactionStep(t, cache, reclaimer, r, step, numKeys, capacity)
					}
				})
			}

			wg.Wait()

			// Assert: Final compaction and invariant check on quiescent cache succeed cleanly.
			reclaimer.Compact()
			st := cache.Stats()
			assert.LessOrEqual(t, st.CurrentSize, st.MaxSize)
		})
	}
}

func runEvictionCallbackRaceStep(t *testing.T, cache lru.Cache[concValue], reclaimer lru.PressureAwareCache[concValue], r *rand.Rand, step, numKeys int, capacity uint64) {
	t.Helper()
	op := r.IntN(100)
	kIdx := r.IntN(numKeys)
	dirIdx := kIdx % 6
	subIdx := (kIdx / 6) % 5
	key := fmt.Sprintf("cb_dir_%02d/sub_%02d/file_%03d.dat", dirIdx, subIdx, kIdx)
	if kIdx == 0 {
		key = ""
	}

	switch {
	case op < 35:
		putConcurrentEntry(t, cache, key)
	case op < 52:
		_, _ = cache.Get(key)
	case op < 65:
		_, _ = cache.Peek(key)
	case op < 78:
		sz := uint64(5 + (step%3)*10) // 5, 15, or 25
		replaceConcurrentEntry(t, cache, key, key+"_r", sz)
	case op < 86:
		// Self-evicting Replace (> capacity or !canFit alongside newer entries)
		sz := capacity + 10
		if step%2 == 1 {
			sz = capacity - 15
		}
		replaceConcurrentEntry(t, cache, key, key+"_r", sz)
	case op < 92:
		_, _ = cache.Delete(key)
	case op < 96:
		prefix := fmt.Sprintf("cb_dir_%02d/", dirIdx)
		cache.DeletePrefix(prefix)
	case op < 98:
		reclaimer.Compact()
	default:
		_ = reclaimer.EvaluateMemoryPressure()
	}
}

// TestConcurrency_EvictionCallbacksUnderRace verifies that WithOnEvictValue and
// WithOnEvictEntry callbacks execute free of data races under concurrent mixed
// operations and oscillating memory pressure across all three backends.
func TestConcurrency_EvictionCallbacksUnderRace(t *testing.T) {
	// Arrange
	engines := allEngines()

	for _, eng := range engines {
		t.Run(eng.name, func(t *testing.T) {
			const (
				numGoroutines = 16
				opsPerWorker  = 300
				numKeys       = 120
				capacity      = 400
			)

			var pressureBits atomic.Uint64
			setPressure := func(p float64) {
				pressureBits.Store(uint64(p * 1000))
			}
			getPressure := func() float64 {
				return float64(pressureBits.Load()) / 1000.0
			}
			setPressure(0.20)

			var valReasonCounts [4]atomic.Uint64
			var entryReasonCounts [4]atomic.Uint64
			var entryReasonWeights [4]atomic.Uint64
			var lastVal concValue
			var lastReason lru.EvictionReason

			cache := eng.constructor(
				capacity,
				lru.WithInvariantChecking(true),
				lru.WithPressureFunc(getPressure),
				lru.WithCompactionThreshold(0.75),
				lru.WithEvictionThreshold(0.90),
				lru.WithEvictionRetentionRatio(0.50),
				lru.WithOnEvictValue(func(v concValue, r lru.EvictionReason) {
					lastVal = v
					lastReason = r
					if int(r) < len(valReasonCounts) {
						valReasonCounts[r].Add(1)
					}
				}),
				lru.WithOnEvictEntry(func(k string, v concValue, r lru.EvictionReason) {
					assert.Equal(t, lastVal, v, "OnEvictEntry must observe the exact value just passed to OnEvictValue under lock")
					assert.Equal(t, lastReason, r, "OnEvictEntry must observe the exact reason just passed to OnEvictValue under lock")
					assert.True(t, v.id == k || v.id == k+"_r", "reconstructed key %q must match evicted value id %q", k, v.id)
					if int(r) < len(entryReasonCounts) {
						entryReasonCounts[r].Add(1)
						entryReasonWeights[r].Add(v.size)
					}
				}),
			)

			reclaimer, ok := cache.(lru.PressureAwareCache[concValue])
			require.True(t, ok, "expected %s to implement PressureAwareCache", eng.name)

			// Act
			var wg sync.WaitGroup
			for g := range numGoroutines {
				wg.Go(func() {
					r := rand.New(rand.NewPCG(uint64(g*17777+42), 0))
					for step := range opsPerWorker {
						oscillateTestPressure(setPressure, g+step, 0.20)
						runEvictionCallbackRaceStep(t, cache, reclaimer, r, step, numKeys, capacity)
					}
				})
			}

			wg.Wait()
			setPressure(0.20)
			cache.DeletePrefix("")
			reclaimer.Compact()

			// Assert: Value and entry callback counts and weights match Stats() for every EvictionReason, and all 4 reasons fired.
			st := cache.Stats()
			for reason := range 4 {
				r := lru.EvictionReason(reason)
				vCount := valReasonCounts[reason].Load()
				eCount := entryReasonCounts[reason].Load()
				eWeight := entryReasonWeights[reason].Load()
				assert.Equalf(t, vCount, eCount, "reason %s count mismatch between OnEvictValue and OnEvictEntry", r)
				assert.Equalf(t, eCount, st.Evictions(r), "reason %s count mismatch between OnEvictEntry and Stats()", r)
				assert.Equalf(t, eWeight, st.EvictedWeight(r), "reason %s weight mismatch between OnEvictEntry and Stats()", r)
				assert.Positivef(t, eCount, "expected EvictionReason %s to be exercised under concurrency", r)
			}
		})
	}
}

func assertMonotonicStats(t *testing.T, cur, prev lru.Stats, capacity uint64) {
	t.Helper()
	assert.Equal(t, capacity, cur.MaxSize)
	assert.LessOrEqual(t, cur.CurrentSize, cur.MaxSize)
	assert.GreaterOrEqual(t, cur.Len, 0)
	assert.GreaterOrEqual(t, cur.ZeroSizeCount, 0)
	assert.LessOrEqual(t, cur.ZeroSizeCount, cur.Len)

	// Monotonic cumulative counters within a single reader timeline:
	assert.GreaterOrEqual(t, cur.GetHits, prev.GetHits)
	assert.GreaterOrEqual(t, cur.GetMisses, prev.GetMisses)
	assert.GreaterOrEqual(t, cur.PeekHits, prev.PeekHits)
	assert.GreaterOrEqual(t, cur.PeekMisses, prev.PeekMisses)
	assert.GreaterOrEqual(t, cur.EvictionsCapacity, prev.EvictionsCapacity)
	assert.GreaterOrEqual(t, cur.EvictionsPressure, prev.EvictionsPressure)
	assert.GreaterOrEqual(t, cur.EvictionsDeleted, prev.EvictionsDeleted)
	assert.GreaterOrEqual(t, cur.EvictionsReplaced, prev.EvictionsReplaced)
	assert.GreaterOrEqual(t, cur.EvictedWeightCapacity, prev.EvictedWeightCapacity)
	assert.GreaterOrEqual(t, cur.EvictedWeightPressure, prev.EvictedWeightPressure)
	assert.GreaterOrEqual(t, cur.EvictedWeightDeleted, prev.EvictedWeightDeleted)
	assert.GreaterOrEqual(t, cur.EvictedWeightReplaced, prev.EvictedWeightReplaced)
	assert.GreaterOrEqual(t, cur.PutInserted, prev.PutInserted)
	assert.GreaterOrEqual(t, cur.PutUpdated, prev.PutUpdated)
	assert.GreaterOrEqual(t, cur.PutRejectedOversized, prev.PutRejectedOversized)
	assert.GreaterOrEqual(t, cur.ReplaceUpdated, prev.ReplaceUpdated)
	assert.GreaterOrEqual(t, cur.ReplaceNotFound, prev.ReplaceNotFound)
	assert.GreaterOrEqual(t, cur.ReplaceSelfEvicted, prev.ReplaceSelfEvicted)
	assert.GreaterOrEqual(t, cur.DeleteDeleted, prev.DeleteDeleted)
	assert.GreaterOrEqual(t, cur.DeleteNotFound, prev.DeleteNotFound)
	assert.GreaterOrEqual(t, cur.DeletePrefixExecuted, prev.DeletePrefixExecuted)
	assert.GreaterOrEqual(t, cur.CompactionsExplicit, prev.CompactionsExplicit)
	assert.GreaterOrEqual(t, cur.CompactionsPressureTier1, prev.CompactionsPressureTier1)
	assert.GreaterOrEqual(t, cur.CompactionsPressureTier2, prev.CompactionsPressureTier2)
	assert.GreaterOrEqual(t, cur.CompactionsAutoSlack, prev.CompactionsAutoSlack)
	assert.GreaterOrEqual(t, cur.PressureShedsInline, prev.PressureShedsInline)
	assert.GreaterOrEqual(t, cur.PressureShedsExplicit, prev.PressureShedsExplicit)
	assert.GreaterOrEqual(t, cur.ReclaimEpoch, prev.ReclaimEpoch)
}

func runStatsRaceWriterStep(cache lru.Cache[concValue], reclaimer lru.PressureAwareCache[concValue], r *rand.Rand, step, numKeys int, capacity uint64) {
	op := r.IntN(100)
	kIdx := r.IntN(numKeys)
	key := fmt.Sprintf("st_dir_%02d/item_%03d", kIdx%5, kIdx)

	switch {
	case op < 30:
		sz := uint64(10)
		if step%15 == 0 {
			sz = 0
		} else if step%19 == 0 {
			sz = capacity + 50
		}
		_, _ = cache.Put(key, concValue{id: key, size: sz})
	case op < 50:
		_, _ = cache.Get(key)
	case op < 65:
		_, _ = cache.Peek(key)
	case op < 80:
		sz := uint64(15)
		if step%11 == 0 {
			sz = capacity + 20
		}
		_ = cache.Replace(key, concValue{id: key + "_u", size: sz})
	case op < 90:
		_, _ = cache.Delete(key)
	case op < 95:
		cache.DeletePrefix(fmt.Sprintf("st_dir_%02d/", kIdx%5))
	case op < 98:
		reclaimer.Compact()
	default:
		_ = reclaimer.EvaluateMemoryPressure()
	}
}

// TestConcurrency_StatsAndPressureUnderRace verifies that dedicated reader goroutines
// continuously polling cache.Stats() observe monotonic counter progression and zero data
// races while writer goroutines concurrently execute Put, Get, Peek, Replace, Delete,
// DeletePrefix, Compact, and EvaluateMemoryPressure across all three backends.
func TestConcurrency_StatsAndPressureUnderRace(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			// Arrange
			const (
				numWriters   = 12
				numReaders   = 4
				opsPerWorker = 250
				numKeys      = 80
				capacity     = 300
			)

			var pressureBits atomic.Uint64
			setPressure := func(p float64) {
				pressureBits.Store(uint64(p * 1000))
			}
			getPressure := func() float64 {
				return float64(pressureBits.Load()) / 1000.0
			}
			setPressure(0.25)

			var evictCounts [4]atomic.Uint64
			var evictWeights [4]atomic.Uint64

			cache := eng.constructor(
				capacity,
				lru.WithInvariantChecking(true),
				lru.WithPressureFunc(getPressure),
				lru.WithCompactionThreshold(0.75),
				lru.WithEvictionThreshold(0.90),
				lru.WithEvictionRetentionRatio(0.50),
				lru.WithOnEvictEntry(func(_ string, v concValue, r lru.EvictionReason) {
					if int(r) < len(evictCounts) {
						evictCounts[r].Add(1)
						evictWeights[r].Add(v.size)
					}
				}),
			)

			reclaimer, ok := cache.(lru.PressureAwareCache[concValue])
			require.True(t, ok)

			var stopReaders atomic.Bool
			var readerWg sync.WaitGroup
			for range numReaders {
				readerWg.Go(func() {
					var prev lru.Stats
					for !stopReaders.Load() {
						cur := cache.Stats()
						assertMonotonicStats(t, cur, prev, capacity)
						prev = cur
					}
				})
			}

			// Act
			var writerWg sync.WaitGroup
			for g := range numWriters {
				writerWg.Go(func() {
					r := rand.New(rand.NewPCG(uint64(g*31337+7), 0))
					for step := range opsPerWorker {
						oscillateTestPressure(setPressure, g+step, 0.25)
						runStatsRaceWriterStep(cache, reclaimer, r, step, numKeys, capacity)
					}
				})
			}

			writerWg.Wait()
			stopReaders.Store(true)
			readerWg.Wait()

			// Assert final quiescent state parity with eviction callbacks
			st := cache.Stats()
			for reason := range 4 {
				r := lru.EvictionReason(reason)
				assert.Equalf(t, evictCounts[reason].Load(), st.Evictions(r), "final Evictions(%s) mismatch", r)
				assert.Equalf(t, evictWeights[reason].Load(), st.EvictedWeight(r), "final EvictedWeight(%s) mismatch", r)
			}
			assert.Zero(t, st.Evictions(lru.EvictionReason(99)))
			assert.Zero(t, st.EvictedWeight(lru.EvictionReason(99)))
		})
	}
}
