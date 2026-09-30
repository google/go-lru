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
	"math/rand"
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
			allOpts := make([]lru.Option, 0, len(opts)+1)
			allOpts = append(allOpts, concWeigher)
			allOpts = append(allOpts, opts...)
			return fn(maxSize, allOpts...)
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
				_, err := cache.Insert(key, concValue{id: key, size: 10})
				require.NoError(t, err)
			}

			// Act
			var wg sync.WaitGroup
			for g := range numGoroutines {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					r := rand.New(rand.NewSource(int64(workerID*10007 + 42)))

					for range opsPerWorker {
						op := r.Intn(100)
						kIdx := r.Intn(numKeys * 2)
						dirIdx := kIdx % 5
						subIdx := (kIdx / 5) % 10
						key := fmt.Sprintf("dir_%02d/sub_%02d/file_%03d.txt", dirIdx, subIdx, kIdx)

						switch {
						case op < 30:
							_, err := cache.Insert(key, concValue{id: key, size: 10})
							if err != nil {
								assert.ErrorIs(t, err, lru.ErrInvalidEntrySize)
							}
						case op < 55:
							_, _ = cache.LookUp(key)
						case op < 70:
							_, _ = cache.LookUpWithoutChangingOrder(key)
						case op < 80:
							err := cache.UpdateWithoutChangingOrder(key, concValue{id: key + "_upd", size: 10})
							if err != nil {
								assert.ErrorIs(t, err, lru.ErrEntryNotExist)
							}
						case op < 88:
							sz := uint64(5 + (kIdx%3)*10) // 5, 15, or 25 (shrinks or grows weight)
							err := cache.UpdateWithoutChangingOrder(key, concValue{id: key + "_sz", size: sz})
							if err != nil {
								assert.ErrorIs(t, err, lru.ErrEntryNotExist)
							}
						case op < 95:
							_, _ = cache.Erase(key)
						default:
							prefix := fmt.Sprintf("dir_%02d/", dirIdx)
							cache.EraseEntriesWithGivenPrefix(prefix)
						}
					}
				}(g)
			}

			wg.Wait()

			// Assert
			_, err := cache.Insert("post_conc_check", concValue{id: "post_conc_check", size: 10})
			require.NoError(t, err)
			val, ok := cache.LookUp("post_conc_check")
			assert.True(t, ok)
			assert.Equal(t, "post_conc_check", val.id)
		})
	}
}

// TestConcurrency_PrefixErasureAtomicity verifies that EraseEntriesWithGivenPrefix
// atomically removes all matching keys without affecting unrelated prefixes under
// concurrent writer load.
func TestConcurrency_PrefixErasureAtomicity(t *testing.T) {
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
				_, err := cache.Insert(kTarget, concValue{id: kTarget, size: 10})
				require.NoError(t, err)
				kKeep := fmt.Sprintf("/keep/init_%d", i)
				_, err = cache.Insert(kKeep, concValue{id: kKeep, size: 10})
				require.NoError(t, err)
			}

			// Act
			var writerWg sync.WaitGroup
			for w := range numWriters {
				writerWg.Add(1)
				go func(workerID int) {
					defer writerWg.Done()
					for op := range opsPerWriter {
						key := fmt.Sprintf("/target/w%d_%d", workerID, op)
						_, err := cache.Insert(key, concValue{id: key, size: 10})
						assert.NoError(t, err)
					}
				}(w)
			}

			for range 10 {
				cache.EraseEntriesWithGivenPrefix("/target/")
			}

			writerWg.Wait()

			// Final prefix erase after writers finish must remove all /target/ keys.
			cache.EraseEntriesWithGivenPrefix("/target/")

			// Assert
			for i := range 20 {
				kTarget := fmt.Sprintf("/target/init_%d", i)
				_, okTarget := cache.LookUp(kTarget)
				assert.False(t, okTarget)
				kKeep := fmt.Sprintf("/keep/init_%d", i)
				_, okKeep := cache.LookUp(kKeep)
				assert.True(t, okKeep)
			}

			for w := range numWriters {
				for op := range opsPerWriter {
					key := fmt.Sprintf("/target/w%d_%d", w, op)
					_, ok := cache.LookUp(key)
					assert.False(t, ok)
				}
			}
		})
	}
}

// TestConcurrency_ParallelReadersWithoutChangingOrder verifies concurrent
// LookUpWithoutChangingOrder calls execute safely and preserve LRU eviction order.
func TestConcurrency_ParallelReadersWithoutChangingOrder(t *testing.T) {
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
				_, err := cache.Insert(k, concValue{id: k, size: 10})
				require.NoError(t, err)
			}

			// Act
			var readerWg sync.WaitGroup
			for r := range numReaders {
				readerWg.Add(1)
				go func(readerID int) {
					defer readerWg.Done()
					for i := range readsPerG {
						targetKey := "key_0000"
						if i%2 == 1 {
							targetKey = fmt.Sprintf("key_%04d", (readerID*17+i)%totalKeys)
						}
						_, ok := cache.LookUpWithoutChangingOrder(targetKey)
						if !assert.True(t, ok) {
							return
						}
					}
				}(r)
			}
			readerWg.Wait()

			// Fill remaining capacity (50,000 - 2,000 = 48,000 bytes).
			for i := range 48 {
				k := fmt.Sprintf("filler_%03d", i)
				_, err := cache.Insert(k, concValue{id: k, size: 1000})
				require.NoError(t, err)
			}

			// Next 10-byte insert must evict key_0000 (the untouched LRU tail).
			evicted, err := cache.Insert("overflow_trigger", concValue{id: "overflow", size: 10})

			// Assert
			require.NoError(t, err)
			require.Len(t, evicted, 1)
			assert.Equal(t, "key_0000", evicted[0].id)
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
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range opsPerWorker {
						key := fmt.Sprintf("inv/p%d/item_%d", i%5, i)
						evicted, err := cache.Insert(key, concValue{id: key, size: 10})
						assert.NoError(t, err)
						totalEvictions.Add(int64(len(evicted)))
						if i%20 == 0 {
							cache.EraseEntriesWithGivenPrefix(fmt.Sprintf("inv/p%d/", i%5))
						}
					}
				}()
			}

			wg.Wait()

			// Assert
			assert.Positive(t, totalEvictions.Load())
		})
	}
}

// TestConcurrency_MemoryPressureCompactionAndEviction exercises concurrent reads, inserts,
// updates, size updates, erasures, prefix deletions, and explicit/automatic compactions
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
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					r := rand.New(rand.NewSource(int64(workerID*13337 + 99)))

					for step := range opsPerWorker {
						// Dynamically oscillate simulated pressure across Normal (0.20),
						// Moderate (0.80), and Critical (0.95) tiers.
						switch (workerID + step) % 3 {
						case 0:
							setPressure(0.20)
						case 1:
							setPressure(0.80)
						case 2:
							setPressure(0.95)
						}

						op := r.Intn(100)
						kIdx := r.Intn(numKeys)
						dirIdx := kIdx % 6
						subIdx := (kIdx / 6) % 5
						key := fmt.Sprintf("mp_dir_%02d/sub_%02d/file_%03d.dat", dirIdx, subIdx, kIdx)

						switch {
						case op < 30:
							_, err := cache.Insert(key, concValue{id: key, size: 10})
							if err != nil {
								assert.ErrorIs(t, err, lru.ErrInvalidEntrySize)
							}
						case op < 50:
							_, _ = cache.LookUp(key)
						case op < 68:
							_, _ = cache.LookUpWithoutChangingOrder(key)
						case op < 76:
							err := cache.UpdateWithoutChangingOrder(key, concValue{id: key + "_u", size: 10})
							if err != nil {
								assert.ErrorIs(t, err, lru.ErrEntryNotExist)
							}
						case op < 84:
							sz := uint64(5 + (step%2)*10) // 5 or 15
							err := cache.UpdateWithoutChangingOrder(key, concValue{id: key + "_sz", size: sz})
							if err != nil {
								assert.ErrorIs(t, err, lru.ErrEntryNotExist)
							}
						case op < 90:
							_, _ = cache.Erase(key)
						case op < 95:
							prefix := fmt.Sprintf("mp_dir_%02d/", dirIdx)
							cache.EraseEntriesWithGivenPrefix(prefix)
						case op < 98:
							reclaimer.Compact()
						default:
							_ = reclaimer.EvaluateMemoryPressure()
						}
					}
				}(g)
			}

			wg.Wait()

			// Assert: Final compaction and invariant check on quiescent cache succeed cleanly.
			reclaimer.Compact()
		})
	}
}
