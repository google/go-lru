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
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/google/go-lru"
)

var (
	benchSinkVal lru.ValueType
	benchSinkErr error
)

// benchValue implements lru.ValueType for benchmarking.
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

func runBenchmarkInsert(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache, depth int) {
	b.Helper()
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, _, _ := generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys) * 5)

	cache := constructor(capacity)

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	var lastErr error
	for b.Loop() {
		key := keys[i%len(keys)]
		_, lastErr = cache.Insert(key, data)
		i++
	}
	benchSinkErr = lastErr
}

func Benchmark_Insert_MapCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkInsert(b, lru.NewMapCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkInsert(b, lru.NewMapCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkInsert(b, lru.NewMapCache, 10) })
}

func Benchmark_Insert_RadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkInsert(b, lru.NewRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkInsert(b, lru.NewRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkInsert(b, lru.NewRadixCache, 10) })
}

func Benchmark_Insert_ArenaRadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkInsert(b, lru.NewArenaRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkInsert(b, lru.NewArenaRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkInsert(b, lru.NewArenaRadixCache, 10) })
}

// ============================================================================
// 2. Point Lookup Latency Benchmarks
// ============================================================================

func runBenchmarkLookUp(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache, depth int) {
	b.Helper()
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
	var lastVal lru.ValueType
	for b.Loop() {
		key := keys[i%len(keys)]
		lastVal = cache.LookUp(key)
		i++
	}
	benchSinkVal = lastVal
}

func Benchmark_LookUp_MapCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkLookUp(b, lru.NewMapCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkLookUp(b, lru.NewMapCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkLookUp(b, lru.NewMapCache, 10) })
}

func Benchmark_LookUp_RadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkLookUp(b, lru.NewRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkLookUp(b, lru.NewRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkLookUp(b, lru.NewRadixCache, 10) })
}

func Benchmark_LookUp_ArenaRadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkLookUp(b, lru.NewArenaRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkLookUp(b, lru.NewArenaRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkLookUp(b, lru.NewArenaRadixCache, 10) })
}

// ============================================================================
// 3. LookUpWithoutChangingOrder Benchmarks
// ============================================================================

func runBenchmarkLookUpWithoutChangingOrder(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache, depth int) {
	b.Helper()
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
	var lastVal lru.ValueType
	for b.Loop() {
		key := keys[i%len(keys)]
		lastVal = cache.LookUpWithoutChangingOrder(key)
		i++
	}
	benchSinkVal = lastVal
}

func Benchmark_LookUpWithoutChangingOrder_MapCache(b *testing.B) {
	runBenchmarkLookUpWithoutChangingOrder(b, lru.NewMapCache, 2)
}

func Benchmark_LookUpWithoutChangingOrder_RadixCache(b *testing.B) {
	runBenchmarkLookUpWithoutChangingOrder(b, lru.NewRadixCache, 2)
}

func Benchmark_LookUpWithoutChangingOrder_ArenaRadixCache(b *testing.B) {
	runBenchmarkLookUpWithoutChangingOrder(b, lru.NewArenaRadixCache, 2)
}

// ============================================================================
// 4. Update Without Changing Order Benchmarks
// ============================================================================

func runBenchmarkUpdate(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache) {
	b.Helper()
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
	var lastErr error
	for b.Loop() {
		key := keys[i%numKeys]
		lastErr = cache.UpdateWithoutChangingOrder(key, updatedData)
		i++
	}
	benchSinkErr = lastErr
}

func Benchmark_Update_MapCache(b *testing.B)        { runBenchmarkUpdate(b, lru.NewMapCache) }
func Benchmark_Update_RadixCache(b *testing.B)      { runBenchmarkUpdate(b, lru.NewRadixCache) }
func Benchmark_Update_ArenaRadixCache(b *testing.B) { runBenchmarkUpdate(b, lru.NewArenaRadixCache) }

// ============================================================================
// 5. Individual Erase Benchmarks
// ============================================================================

func runBenchmarkErase(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache) {
	b.Helper()
	const batchSize = 10000
	keys := make([]string, batchSize)
	for i := range batchSize {
		keys[i] = fmt.Sprintf("erase_key_%d", i)
	}
	data := benchValue{val: 1, dataSize: 10}
	cache := constructor(uint64(batchSize * 100))

	b.ReportAllocs()
	b.ResetTimer()

	var lastVal lru.ValueType
	for i := range b.N {
		idx := i % batchSize
		if idx == 0 {
			b.StopTimer()
			for _, k := range keys {
				_, _ = cache.Insert(k, data)
			}
			b.StartTimer()
		}
		lastVal = cache.Erase(keys[idx])
	}
	benchSinkVal = lastVal
}

func Benchmark_Erase_MapCache(b *testing.B)        { runBenchmarkErase(b, lru.NewMapCache) }
func Benchmark_Erase_RadixCache(b *testing.B)      { runBenchmarkErase(b, lru.NewRadixCache) }
func Benchmark_Erase_ArenaRadixCache(b *testing.B) { runBenchmarkErase(b, lru.NewArenaRadixCache) }

// ============================================================================
// 6. Prefix Deletion Benchmarks Across Topologies (Batched Untimed Restoration)
// ============================================================================

func runBenchmarkErasePrefix(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache, depth int) {
	b.Helper()
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, _, prefixes := generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys) * 100)

	cache := constructor(capacity)

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		idx := i % len(prefixes)
		if idx == 0 {
			b.StopTimer()
			for _, key := range keys {
				_, _ = cache.Insert(key, data)
			}
			b.StartTimer()
		}
		cache.EraseEntriesWithGivenPrefix(prefixes[idx])
	}
}

func Benchmark_ErasePrefix_MapCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkErasePrefix(b, lru.NewMapCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkErasePrefix(b, lru.NewMapCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkErasePrefix(b, lru.NewMapCache, 10) })
}

func Benchmark_ErasePrefix_RadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkErasePrefix(b, lru.NewRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkErasePrefix(b, lru.NewRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkErasePrefix(b, lru.NewRadixCache, 10) })
}

func Benchmark_ErasePrefix_ArenaRadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkErasePrefix(b, lru.NewArenaRadixCache, 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkErasePrefix(b, lru.NewArenaRadixCache, 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkErasePrefix(b, lru.NewArenaRadixCache, 10) })
}

// ============================================================================
// 7. Parallel Multi-Core Throughput Benchmarks (b.RunParallel)
// ============================================================================

func runParallelWorkload(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache, insertPct, lookupPct int) {
	b.Helper()
	const cacheSize = 50000000
	const keySpace = 20000
	data := benchValue{val: 1, dataSize: 10}
	keys := make([]string, keySpace)
	for i := range keySpace {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	cache := constructor(cacheSize)

	// Pre-populate
	for i := range keySpace / 2 {
		_, _ = cache.Insert(keys[i], data)
	}

	var workerSeq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		workerID := workerSeq.Add(1)
		r := rand.New(rand.NewSource(42 + workerID*10007))
		for pb.Next() {
			op := r.Intn(100)
			key := keys[r.Intn(keySpace)]
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
	b.Run("MapCache", func(b *testing.B) { runParallelWorkload(b, lru.NewMapCache, 30, 60) })
	b.Run("RadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewRadixCache, 30, 60) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewArenaRadixCache, 30, 60) })
}

func Benchmark_ParallelThroughput_ReadHeavy(b *testing.B) {
	b.Run("MapCache", func(b *testing.B) { runParallelWorkload(b, lru.NewMapCache, 5, 90) })
	b.Run("RadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewRadixCache, 5, 90) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewArenaRadixCache, 5, 90) })
}

func Benchmark_ParallelThroughput_WriteHeavy(b *testing.B) {
	b.Run("MapCache", func(b *testing.B) { runParallelWorkload(b, lru.NewMapCache, 80, 15) })
	b.Run("RadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewRadixCache, 80, 15) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewArenaRadixCache, 80, 15) })
}

// ============================================================================
// 8. High-Volume 100K Operations Scale Benchmarks
// ============================================================================

func Benchmark_LargeScale_Insert_100K(b *testing.B) {
	const numEntries = 100000
	data := benchValue{val: 1, dataSize: 10}
	cacheMaxSize := uint64(numEntries * 20)
	keys := make([]string, numEntries)
	for j := range numEntries {
		keys[j] = fmt.Sprintf("prefix/key-%d", j)
	}

	runInsert100K := func(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache) {
		b.Helper()
		b.ReportAllocs()
		var mBefore, mAfter runtime.MemStats
		var lastCache lru.Cache
		for i := range b.N {
			b.StopTimer()
			if i == b.N-1 {
				runtime.GC()
				runtime.ReadMemStats(&mBefore)
			}
			cache := constructor(cacheMaxSize)
			b.StartTimer()
			for j := range numEntries {
				_, _ = cache.Insert(keys[j], data)
			}
			if i == b.N-1 {
				b.StopTimer()
				lastCache = cache
				runtime.GC()
				runtime.ReadMemStats(&mAfter)
				runtime.KeepAlive(lastCache)
				b.StartTimer()
			}
		}
		if mAfter.HeapAlloc > mBefore.HeapAlloc {
			b.ReportMetric(float64(mAfter.HeapAlloc-mBefore.HeapAlloc)/float64(numEntries), "heap-B/entry")
		}
	}

	b.Run("MapCache", func(b *testing.B) { runInsert100K(b, lru.NewMapCache) })
	b.Run("RadixCache", func(b *testing.B) { runInsert100K(b, lru.NewRadixCache) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runInsert100K(b, lru.NewArenaRadixCache) })
}

func Benchmark_LargeScale_PrefixErase_100K(b *testing.B) {
	const numEntries = 100000
	data := benchValue{val: 1, dataSize: 10}
	cacheMaxSize := uint64(numEntries * 20)
	keys := make([]string, numEntries)
	for j := range numEntries {
		if j%2 == 0 {
			keys[j] = fmt.Sprintf("target_prefix/key-%d", j)
		} else {
			keys[j] = fmt.Sprintf("other_prefix/key-%d", j)
		}
	}

	runPrefixErase100K := func(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache) {
		b.Helper()
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			cache := constructor(cacheMaxSize)
			for j := range numEntries {
				_, _ = cache.Insert(keys[j], data)
			}
			b.StartTimer()

			// Delete target_prefix/ (50,000 entries)
			cache.EraseEntriesWithGivenPrefix("target_prefix/")
		}
	}

	b.Run("MapCache", func(b *testing.B) { runPrefixErase100K(b, lru.NewMapCache) })
	b.Run("RadixCache", func(b *testing.B) { runPrefixErase100K(b, lru.NewRadixCache) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runPrefixErase100K(b, lru.NewArenaRadixCache) })
}

// ============================================================================
// 9. Memory Pressure Compaction & Reclamation Benchmarks
// ============================================================================

func Benchmark_ArenaRadixCache_Compact(b *testing.B) {
	const numKeys = 10000
	data := benchValue{val: 1, dataSize: 10}
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = fmt.Sprintf("dir_%02d/file_%05d", i%50, i)
	}

	b.ReportAllocs()
	var reclaimedBytes uint64
	for iter := range b.N {
		b.StopTimer()
		cache := lru.NewArenaRadixCache(uint64(numKeys * 20)).(lru.PressureAwareCache)
		for i := range numKeys {
			_, _ = cache.Insert(keys[i], data)
		}
		for i := range numKeys / 2 {
			_ = cache.Erase(keys[i])
		}
		var mBefore, mAfter runtime.MemStats
		if iter == b.N-1 {
			runtime.GC()
			runtime.ReadMemStats(&mBefore)
		}
		b.StartTimer()

		cache.Compact()

		if iter == b.N-1 {
			b.StopTimer()
			runtime.GC()
			runtime.ReadMemStats(&mAfter)
			runtime.KeepAlive(cache)
			if mBefore.HeapAlloc > mAfter.HeapAlloc {
				reclaimedBytes = mBefore.HeapAlloc - mAfter.HeapAlloc
			}
			b.StartTimer()
		}
	}
	b.ReportMetric(float64(reclaimedBytes), "reclaimed-B/op")
}

func Benchmark_ArenaRadixCache_InsertUnderPressure(b *testing.B) {
	const numKeys = 10000
	const keyPool = numKeys * 2
	keys := make([]string, keyPool)
	for i := range keyPool {
		keys[i] = fmt.Sprintf("dir_%02d/file_%05d", i%50, i)
	}
	data := benchValue{val: 1, dataSize: 10}
	cache := lru.NewArenaRadixCache(
		uint64(numKeys*10),
		lru.WithPressureFunc(func() float64 { return 0.92 }),
		lru.WithEvictionRetentionRatio(0.50),
	)

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for b.Loop() {
		_, _ = cache.Insert(keys[i%keyPool], data)
		i++
	}
}
