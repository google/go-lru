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

package lrus_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	lrus "github.com/googlecloudplatform/gcsfuse/v3/internal/cache/lru"
)

// benchValue implements lrus.ValueType for benchmarking.
type benchValue struct {
	val      int64
	dataSize uint64
}

func (v benchValue) Size() uint64 {
	return v.dataSize
}

// generateBenchmarkKeys creates test keys partitioned by prefix with configurable directory depth.
func generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth int) (keys []string, prefixMap map[string][]string, prefixes []string) {
	prefixMap = make(map[string][]string, prefixCount)
	prefixes = make([]string, 0, prefixCount)
	totalKeys := prefixCount * itemsPerPrefix
	keys = make([]string, 0, totalKeys)

	for p := range prefixCount {
		var prefix string
		if depth == 0 {
			prefix = fmt.Sprintf("prefix%d-", p)
		} else {
			for d := range depth {
				prefix += fmt.Sprintf("dir%d/", p*depth+d)
			}
		}
		prefixes = append(prefixes, prefix)

		keysForPrefix := make([]string, 0, itemsPerPrefix)
		for i := range itemsPerPrefix {
			key := fmt.Sprintf("%sfile%d.dat", prefix, i)
			keysForPrefix = append(keysForPrefix, key)
			keys = append(keys, key)
		}
		prefixMap[prefix] = keysForPrefix
	}

	// Shuffle keys to simulate realistic, unpredictable access patterns
	r := rand.New(rand.NewSource(42))
	r.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys, prefixMap, prefixes
}

// ============================================================================
// 1. Sequential Insertion Benchmarks
// ============================================================================

func runBenchmarkInsert(b *testing.B, constructor func(uint64, ...lrus.Option) lrus.Cache, depth int) {
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, _, _ := generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys) * 100)

	cache := constructor(capacity)

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		key := keys[i%len(keys)]
		_, _ = cache.Insert(key, data)
		i++
	}
}

func Benchmark_Insert_MapCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkInsert(b, lrus.NewMapCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkInsert(b, lrus.NewMapCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkInsert(b, lrus.NewMapCache, 10) })
}

func Benchmark_Insert_RadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkInsert(b, lrus.NewRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkInsert(b, lrus.NewRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkInsert(b, lrus.NewRadixCache, 10) })
}

func Benchmark_Insert_ArenaRadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkInsert(b, lrus.NewArenaRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkInsert(b, lrus.NewArenaRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkInsert(b, lrus.NewArenaRadixCache, 10) })
}

// ============================================================================
// 2. Point Lookup Latency Benchmarks
// ============================================================================

func runBenchmarkLookUp(b *testing.B, constructor func(uint64, ...lrus.Option) lrus.Cache, depth int) {
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, _, _ := generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys) * 100)

	cache := constructor(capacity)
	for _, key := range keys {
		_, _ = cache.Insert(key, data)
	}

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		key := keys[i%len(keys)]
		_ = cache.LookUp(key)
		i++
	}
}

func Benchmark_LookUp_MapCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkLookUp(b, lrus.NewMapCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkLookUp(b, lrus.NewMapCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkLookUp(b, lrus.NewMapCache, 10) })
}

func Benchmark_LookUp_RadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkLookUp(b, lrus.NewRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkLookUp(b, lrus.NewRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkLookUp(b, lrus.NewRadixCache, 10) })
}

func Benchmark_LookUp_ArenaRadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkLookUp(b, lrus.NewArenaRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkLookUp(b, lrus.NewArenaRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkLookUp(b, lrus.NewArenaRadixCache, 10) })
}

// ============================================================================
// 3. LookUpWithoutChangingOrder Benchmarks
// ============================================================================

func runBenchmarkLookUpWithoutChangingOrder(b *testing.B, constructor func(uint64, ...lrus.Option) lrus.Cache, depth int) {
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, _, _ := generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys) * 100)

	cache := constructor(capacity)
	for _, key := range keys {
		_, _ = cache.Insert(key, data)
	}

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		key := keys[i%len(keys)]
		_ = cache.LookUpWithoutChangingOrder(key)
		i++
	}
}

func Benchmark_LookUpWithoutChangingOrder_MapCache(b *testing.B) {
	runBenchmarkLookUpWithoutChangingOrder(b, lrus.NewMapCache, 2)
}

func Benchmark_LookUpWithoutChangingOrder_RadixCache(b *testing.B) {
	runBenchmarkLookUpWithoutChangingOrder(b, lrus.NewRadixCache, 2)
}

func Benchmark_LookUpWithoutChangingOrder_ArenaRadixCache(b *testing.B) {
	runBenchmarkLookUpWithoutChangingOrder(b, lrus.NewArenaRadixCache, 2)
}

// ============================================================================
// 4. Update Without Changing Order Benchmarks
// ============================================================================

func runBenchmarkUpdate(b *testing.B, constructor func(uint64, ...lrus.Option) lrus.Cache) {
	const numKeys = 10000
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = fmt.Sprintf("bench_update_%06d", i)
	}
	data := benchValue{val: 1, dataSize: 10}
	updatedData := benchValue{val: 2, dataSize: 10}

	cache := constructor(uint64(numKeys * 100))
	for _, key := range keys {
		_, _ = cache.Insert(key, data)
	}

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		key := keys[i%numKeys]
		_ = cache.UpdateWithoutChangingOrder(key, updatedData)
		i++
	}
}

func Benchmark_Update_MapCache(b *testing.B)        { runBenchmarkUpdate(b, lrus.NewMapCache) }
func Benchmark_Update_RadixCache(b *testing.B)      { runBenchmarkUpdate(b, lrus.NewRadixCache) }
func Benchmark_Update_ArenaRadixCache(b *testing.B) { runBenchmarkUpdate(b, lrus.NewArenaRadixCache) }

// ============================================================================
// 5. Individual Erase Benchmarks
// ============================================================================

func runBenchmarkErase(b *testing.B, constructor func(uint64, ...lrus.Option) lrus.Cache) {
	const numKeys = 10000
	data := benchValue{val: 1, dataSize: 10}

	cache := constructor(uint64(numKeys * 100))

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		b.StopTimer()
		key := fmt.Sprintf("erase_key_%d", i)
		_, _ = cache.Insert(key, data)
		b.StartTimer()

		_ = cache.Erase(key)
	}
}

func Benchmark_Erase_MapCache(b *testing.B)        { runBenchmarkErase(b, lrus.NewMapCache) }
func Benchmark_Erase_RadixCache(b *testing.B)      { runBenchmarkErase(b, lrus.NewRadixCache) }
func Benchmark_Erase_ArenaRadixCache(b *testing.B) { runBenchmarkErase(b, lrus.NewArenaRadixCache) }

// ============================================================================
// 6. Prefix Deletion Benchmarks Across Topologies (Untimed Key Restoration)
// ============================================================================

func runBenchmarkErasePrefix(b *testing.B, constructor func(uint64, ...lrus.Option) lrus.Cache, depth int) {
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, prefixMap, prefixes := generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys) * 100)

	cache := constructor(capacity)
	for _, key := range keys {
		_, _ = cache.Insert(key, data)
	}

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		prefix := prefixes[i%len(prefixes)]
		cache.EraseEntriesWithGivenPrefix(prefix)

		// UNTIMED: Pause clock, restore erased keys, and restart timer for next iteration
		b.StopTimer()
		for _, key := range prefixMap[prefix] {
			_, _ = cache.Insert(key, data)
		}
		i++
		b.StartTimer()
	}
}

func Benchmark_ErasePrefix_MapCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkErasePrefix(b, lrus.NewMapCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkErasePrefix(b, lrus.NewMapCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkErasePrefix(b, lrus.NewMapCache, 10) })
}

func Benchmark_ErasePrefix_RadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkErasePrefix(b, lrus.NewRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkErasePrefix(b, lrus.NewRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkErasePrefix(b, lrus.NewRadixCache, 10) })
}

func Benchmark_ErasePrefix_ArenaRadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkErasePrefix(b, lrus.NewArenaRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkErasePrefix(b, lrus.NewArenaRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkErasePrefix(b, lrus.NewArenaRadixCache, 10) })
}

// ============================================================================
// 7. Parallel Multi-Core Throughput Benchmarks (b.RunParallel)
// ============================================================================

func runParallelWorkload(b *testing.B, constructor func(uint64, ...lrus.Option) lrus.Cache, insertPct, lookupPct int) {
	const cacheSize = 50000000
	const keySpace = 20000
	data := benchValue{val: 1, dataSize: 10}

	cache := constructor(cacheSize)

	// Pre-populate
	for i := range keySpace / 2 {
		_, _ = cache.Insert(fmt.Sprintf("key_%d", i), data)
	}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			op := r.Intn(100)
			key := fmt.Sprintf("key_%d", r.Intn(keySpace))
			switch {
			case op < insertPct:
				_, _ = cache.Insert(key, data)
			case op < insertPct+lookupPct:
				_ = cache.LookUp(key)
			default:
				_ = cache.Erase(key)
			}
		}
	})
}

func Benchmark_ParallelThroughput_Mixed(b *testing.B) {
	b.Run("MapCache", func(b *testing.B) { runParallelWorkload(b, lrus.NewMapCache, 30, 60) })
	b.Run("RadixCache", func(b *testing.B) { runParallelWorkload(b, lrus.NewRadixCache, 30, 60) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runParallelWorkload(b, lrus.NewArenaRadixCache, 30, 60) })
}

func Benchmark_ParallelThroughput_ReadHeavy(b *testing.B) {
	b.Run("MapCache", func(b *testing.B) { runParallelWorkload(b, lrus.NewMapCache, 5, 90) })
	b.Run("RadixCache", func(b *testing.B) { runParallelWorkload(b, lrus.NewRadixCache, 5, 90) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runParallelWorkload(b, lrus.NewArenaRadixCache, 5, 90) })
}

func Benchmark_ParallelThroughput_WriteHeavy(b *testing.B) {
	b.Run("MapCache", func(b *testing.B) { runParallelWorkload(b, lrus.NewMapCache, 80, 15) })
	b.Run("RadixCache", func(b *testing.B) { runParallelWorkload(b, lrus.NewRadixCache, 80, 15) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runParallelWorkload(b, lrus.NewArenaRadixCache, 80, 15) })
}

// ============================================================================
// 8. High-Volume 100K Operations Scale Benchmarks
// ============================================================================

func Benchmark_LargeScale_Insert_100K(b *testing.B) {
	const numEntries = 100000
	data := benchValue{val: 1, dataSize: 10}
	cacheMaxSize := uint64(numEntries * 20)

	b.Run("MapCache", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			cache := lrus.NewMapCache(cacheMaxSize)
			b.StartTimer()
			for j := range numEntries {
				_, _ = cache.Insert(fmt.Sprintf("prefix/key-%d", j), data)
			}
		}
	})

	b.Run("RadixCache", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			cache := lrus.NewRadixCache(cacheMaxSize)
			b.StartTimer()
			for j := range numEntries {
				_, _ = cache.Insert(fmt.Sprintf("prefix/key-%d", j), data)
			}
		}
	})

	b.Run("ArenaRadixCache", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			cache := lrus.NewArenaRadixCache(cacheMaxSize)
			b.StartTimer()
			for j := range numEntries {
				_, _ = cache.Insert(fmt.Sprintf("prefix/key-%d", j), data)
			}
		}
	})
}

func Benchmark_LargeScale_PrefixErase_100K(b *testing.B) {
	const numEntries = 100000
	data := benchValue{val: 1, dataSize: 10}
	cacheMaxSize := uint64(numEntries * 20)

	runPrefixErase100K := func(b *testing.B, constructor func(uint64, ...lrus.Option) lrus.Cache) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			cache := constructor(cacheMaxSize)
			for j := range numEntries {
				var key string
				if j%2 == 0 {
					key = fmt.Sprintf("target_prefix/key-%d", j)
				} else {
					key = fmt.Sprintf("other_prefix/key-%d", j)
				}
				_, _ = cache.Insert(key, data)
			}
			b.StartTimer()

			// Delete target_prefix/ (50,000 entries)
			cache.EraseEntriesWithGivenPrefix("target_prefix/")
		}
	}

	b.Run("MapCache", func(b *testing.B) { runPrefixErase100K(b, lrus.NewMapCache) })
	b.Run("RadixCache", func(b *testing.B) { runPrefixErase100K(b, lrus.NewRadixCache) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runPrefixErase100K(b, lrus.NewArenaRadixCache) })
}
