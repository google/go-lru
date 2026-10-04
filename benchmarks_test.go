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
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-lru"
)

var (
	benchSinkVal benchValue
	benchSinkOK  bool
	benchSinkErr error
)

// benchValue is the value payload used across benchmarks.
type benchValue struct {
	val      int64
	dataSize uint64
}

var benchWeigher = lru.WithWeigher(func(_ string, v benchValue) uint64 {
	return v.dataSize
})

// generateBenchmarkKeys creates test keys partitioned by prefix with configurable directory depth.
func generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth int) (keys, prefixes []string) {
	prefixes = make([]string, 0, prefixCount)
	totalKeys := prefixCount * itemsPerPrefix
	keys = make([]string, 0, totalKeys)

	for p := range prefixCount {
		var prefix string
		if depth == 0 {
			prefix = fmt.Sprintf("prefix%d-", p)
		} else {
			var sb strings.Builder
			for d := range depth {
				fmt.Fprintf(&sb, "dir%d/", p*depth+d)
			}
			prefix = sb.String()
		}
		prefixes = append(prefixes, prefix)

		for i := range itemsPerPrefix {
			key := fmt.Sprintf("%sfile%d.dat", prefix, i)
			keys = append(keys, key)
		}
	}

	// Shuffle keys to simulate realistic, unpredictable access patterns
	r := rand.New(rand.NewPCG(42, 0))
	r.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys, prefixes
}

// ============================================================================
// 1. Sequential Put Benchmarks
// ============================================================================

func runBenchmarkPut(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue], depth int) {
	b.Helper()
	runBenchmarkPutWithOptions(b, constructor, depth, 5, benchWeigher)
}

func runBenchmarkPutWithOptions(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue], depth int, capMultiplier uint64, opts ...lru.Option) {
	b.Helper()
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, _ := generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys)) * capMultiplier

	cache := constructor(capacity, opts...)

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	var lastErr error
	for b.Loop() {
		key := keys[i%len(keys)]
		_, lastErr = cache.Put(key, data)
		i++
	}
	benchSinkErr = lastErr
}

func Benchmark_Put_MapCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkPut(b, lru.NewMapCache[benchValue], 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkPut(b, lru.NewMapCache[benchValue], 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkPut(b, lru.NewMapCache[benchValue], 10) })
}

func Benchmark_Put_RadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkPut(b, lru.NewRadixCache[benchValue], 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkPut(b, lru.NewRadixCache[benchValue], 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkPut(b, lru.NewRadixCache[benchValue], 10) })
}

func Benchmark_Put_ArenaRadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkPut(b, lru.NewArenaRadixCache[benchValue], 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkPut(b, lru.NewArenaRadixCache[benchValue], 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkPut(b, lru.NewArenaRadixCache[benchValue], 10) })
}

func Benchmark_Put_UnitWeight_Map(b *testing.B) {
	runBenchmarkPutWithOptions(b, lru.NewMapCache[benchValue], 2, 1)
}

func Benchmark_Put_UnitWeight_Radix(b *testing.B) {
	runBenchmarkPutWithOptions(b, lru.NewRadixCache[benchValue], 2, 1)
}

func Benchmark_Put_UnitWeight_ArenaRadix(b *testing.B) {
	runBenchmarkPutWithOptions(b, lru.NewArenaRadixCache[benchValue], 2, 1)
}

// ============================================================================
// 2. Point Get Latency Benchmarks
// ============================================================================

func runBenchmarkGet(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue], depth int) {
	b.Helper()
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, _ := generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys) * 100)

	cache := constructor(capacity, benchWeigher)
	for _, key := range keys {
		_, _ = cache.Put(key, data)
	}

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	var lastVal benchValue
	var lastOK bool
	for b.Loop() {
		key := keys[i%len(keys)]
		lastVal, lastOK = cache.Get(key)
		i++
	}
	benchSinkVal = lastVal
	benchSinkOK = lastOK
}

func Benchmark_Get_MapCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkGet(b, lru.NewMapCache[benchValue], 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkGet(b, lru.NewMapCache[benchValue], 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkGet(b, lru.NewMapCache[benchValue], 10) })
}

func Benchmark_Get_RadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkGet(b, lru.NewRadixCache[benchValue], 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkGet(b, lru.NewRadixCache[benchValue], 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkGet(b, lru.NewRadixCache[benchValue], 10) })
}

func Benchmark_Get_ArenaRadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkGet(b, lru.NewArenaRadixCache[benchValue], 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkGet(b, lru.NewArenaRadixCache[benchValue], 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkGet(b, lru.NewArenaRadixCache[benchValue], 10) })
}

// ============================================================================
// 3. Peek Benchmarks
// ============================================================================

func runBenchmarkPeek(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue], depth int) {
	b.Helper()
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, _ := generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys) * 100)

	cache := constructor(capacity, benchWeigher)
	for _, key := range keys {
		_, _ = cache.Put(key, data)
	}

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	var lastVal benchValue
	var lastOK bool
	for b.Loop() {
		key := keys[i%len(keys)]
		lastVal, lastOK = cache.Peek(key)
		i++
	}
	benchSinkVal = lastVal
	benchSinkOK = lastOK
}

func Benchmark_Peek_MapCache(b *testing.B) {
	runBenchmarkPeek(b, lru.NewMapCache[benchValue], 2)
}

func Benchmark_Peek_RadixCache(b *testing.B) {
	runBenchmarkPeek(b, lru.NewRadixCache[benchValue], 2)
}

func Benchmark_Peek_ArenaRadixCache(b *testing.B) {
	runBenchmarkPeek(b, lru.NewArenaRadixCache[benchValue], 2)
}

// ============================================================================
// 4. Replace Benchmarks
// ============================================================================

func runBenchmarkReplace(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue]) {
	b.Helper()
	runBenchmarkReplaceWithOptions(b, constructor, benchWeigher)
}

func runBenchmarkReplaceWithOptions(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue], opts ...lru.Option) {
	b.Helper()
	const numKeys = 10000
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = fmt.Sprintf("bench_update_%06d", i)
	}
	data := benchValue{val: 1, dataSize: 10}
	updatedData := benchValue{val: 2, dataSize: 10}

	cache := constructor(uint64(numKeys*100), opts...)
	for _, key := range keys {
		_, _ = cache.Put(key, data)
	}

	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	var lastErr error
	for b.Loop() {
		key := keys[i%numKeys]
		lastErr = cache.Replace(key, updatedData)
		i++
	}
	benchSinkErr = lastErr
}

func Benchmark_Replace_MapCache(b *testing.B) { runBenchmarkReplace(b, lru.NewMapCache[benchValue]) }
func Benchmark_Replace_RadixCache(b *testing.B) {
	runBenchmarkReplace(b, lru.NewRadixCache[benchValue])
}
func Benchmark_Replace_ArenaRadixCache(b *testing.B) {
	runBenchmarkReplace(b, lru.NewArenaRadixCache[benchValue])
}

func Benchmark_Replace_UnitWeight_Map(b *testing.B) {
	runBenchmarkReplaceWithOptions(b, lru.NewMapCache[benchValue])
}

func Benchmark_Replace_UnitWeight_Radix(b *testing.B) {
	runBenchmarkReplaceWithOptions(b, lru.NewRadixCache[benchValue])
}

func Benchmark_Replace_UnitWeight_ArenaRadix(b *testing.B) {
	runBenchmarkReplaceWithOptions(b, lru.NewArenaRadixCache[benchValue])
}

// ============================================================================
// 5. Individual Delete Benchmarks
// ============================================================================

func runBenchmarkDelete(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue]) {
	b.Helper()
	const batchSize = 10000
	keys := make([]string, batchSize)
	for i := range batchSize {
		keys[i] = fmt.Sprintf("erase_key_%d", i)
	}
	data := benchValue{val: 1, dataSize: 10}
	cache := constructor(uint64(batchSize*100), benchWeigher)

	b.ReportAllocs()
	b.ResetTimer()

	var lastVal benchValue
	var lastOK bool
	for i := range b.N {
		idx := i % batchSize
		if idx == 0 {
			b.StopTimer()
			for _, k := range keys {
				_, _ = cache.Put(k, data)
			}
			b.StartTimer()
		}
		lastVal, lastOK = cache.Delete(keys[idx])
	}
	benchSinkVal = lastVal
	benchSinkOK = lastOK
}

func Benchmark_Delete_MapCache(b *testing.B)   { runBenchmarkDelete(b, lru.NewMapCache[benchValue]) }
func Benchmark_Delete_RadixCache(b *testing.B) { runBenchmarkDelete(b, lru.NewRadixCache[benchValue]) }
func Benchmark_Delete_ArenaRadixCache(b *testing.B) {
	runBenchmarkDelete(b, lru.NewArenaRadixCache[benchValue])
}

// ============================================================================
// 6. Prefix Deletion Benchmarks Across Topologies (Batched Untimed Restoration)
// ============================================================================

func runBenchmarkDeletePrefix(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue], depth int) {
	b.Helper()
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, prefixes := generateBenchmarkKeys(prefixCount, itemsPerPrefix, depth)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys) * 100)

	cache := constructor(capacity, benchWeigher)

	b.ReportAllocs()
	b.ResetTimer()

	for i := range b.N {
		idx := i % len(prefixes)
		if idx == 0 {
			b.StopTimer()
			for _, key := range keys {
				_, _ = cache.Put(key, data)
			}
			b.StartTimer()
		}
		cache.DeletePrefix(prefixes[idx])
	}
}

func Benchmark_DeletePrefix_MapCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkDeletePrefix(b, lru.NewMapCache[benchValue], 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkDeletePrefix(b, lru.NewMapCache[benchValue], 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkDeletePrefix(b, lru.NewMapCache[benchValue], 10) })
}

func Benchmark_DeletePrefix_RadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkDeletePrefix(b, lru.NewRadixCache[benchValue], 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkDeletePrefix(b, lru.NewRadixCache[benchValue], 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkDeletePrefix(b, lru.NewRadixCache[benchValue], 10) })
}

func Benchmark_DeletePrefix_ArenaRadixCache(b *testing.B) {
	b.Run("Flat", func(b *testing.B) { runBenchmarkDeletePrefix(b, lru.NewArenaRadixCache[benchValue], 0) })
	b.Run("Nested_Depth2", func(b *testing.B) { runBenchmarkDeletePrefix(b, lru.NewArenaRadixCache[benchValue], 2) })
	b.Run("DeeplyNested_Depth10", func(b *testing.B) { runBenchmarkDeletePrefix(b, lru.NewArenaRadixCache[benchValue], 10) })
}

// ============================================================================
// 7. Parallel Multi-Core Throughput Benchmarks (b.RunParallel)
// ============================================================================

func runParallelWorkload(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue], putPct, getPct int) {
	b.Helper()
	const cacheSize = 50000000
	const keySpace = 20000
	data := benchValue{val: 1, dataSize: 10}
	keys := make([]string, keySpace)
	for i := range keySpace {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	cache := constructor(cacheSize, benchWeigher)

	// Pre-populate
	for i := range keySpace / 2 {
		_, _ = cache.Put(keys[i], data)
	}

	var workerSeq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		workerID := workerSeq.Add(1)
		r := rand.New(rand.NewPCG(uint64(42+workerID*10007), 0))
		for pb.Next() {
			op := r.IntN(100)
			key := keys[r.IntN(keySpace)]
			switch {
			case op < putPct:
				_, _ = cache.Put(key, data)
			case op < putPct+getPct:
				_, _ = cache.Get(key)
			default:
				_, _ = cache.Delete(key)
			}
		}
	})
}

func Benchmark_ParallelThroughput_Mixed(b *testing.B) {
	b.Run("MapCache", func(b *testing.B) { runParallelWorkload(b, lru.NewMapCache[benchValue], 30, 60) })
	b.Run("RadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewRadixCache[benchValue], 30, 60) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewArenaRadixCache[benchValue], 30, 60) })
}

func Benchmark_ParallelThroughput_ReadHeavy(b *testing.B) {
	b.Run("MapCache", func(b *testing.B) { runParallelWorkload(b, lru.NewMapCache[benchValue], 5, 90) })
	b.Run("RadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewRadixCache[benchValue], 5, 90) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewArenaRadixCache[benchValue], 5, 90) })
}

func Benchmark_ParallelThroughput_WriteHeavy(b *testing.B) {
	b.Run("MapCache", func(b *testing.B) { runParallelWorkload(b, lru.NewMapCache[benchValue], 80, 15) })
	b.Run("RadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewRadixCache[benchValue], 80, 15) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runParallelWorkload(b, lru.NewArenaRadixCache[benchValue], 80, 15) })
}

func runParallelPeekOnly(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue]) {
	b.Helper()
	const prefixCount = 100
	const itemsPerPrefix = 100
	keys, _ := generateBenchmarkKeys(prefixCount, itemsPerPrefix, 2)
	data := benchValue{val: 1, dataSize: 10}
	capacity := uint64(len(keys) * 100)

	cache := constructor(capacity, benchWeigher)
	for _, key := range keys {
		_, _ = cache.Put(key, data)
	}

	var workerSeq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		workerID := workerSeq.Add(1)
		var pcg rand.PCG
		pcg.Seed(42+workerID*10007, 0)
		n := uint64(len(keys))
		for pb.Next() {
			_, _ = cache.Peek(keys[pcg.Uint64()%n])
		}
	})
}

func Benchmark_ParallelThroughput_PeekOnly(b *testing.B) {
	b.Run("MapCache", func(b *testing.B) { runParallelPeekOnly(b, lru.NewMapCache[benchValue]) })
	b.Run("RadixCache", func(b *testing.B) { runParallelPeekOnly(b, lru.NewRadixCache[benchValue]) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runParallelPeekOnly(b, lru.NewArenaRadixCache[benchValue]) })
}

// ============================================================================
// 8. High-Volume 100K Operations Scale Benchmarks
// ============================================================================

func Benchmark_LargeScale_Put_100K(b *testing.B) {
	const numEntries = 100000
	data := benchValue{val: 1, dataSize: 10}
	cacheMaxSize := uint64(numEntries * 20)
	keys := make([]string, numEntries)
	for j := range numEntries {
		keys[j] = fmt.Sprintf("prefix/key-%d", j)
	}

	runPut100K := func(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue]) {
		b.Helper()
		b.ReportAllocs()
		var mBefore, mAfter runtime.MemStats
		var lastCache lru.Cache[benchValue]
		for i := range b.N {
			b.StopTimer()
			if i == b.N-1 {
				runtime.GC()
				runtime.ReadMemStats(&mBefore)
			}
			cache := constructor(cacheMaxSize, benchWeigher)
			b.StartTimer()
			for j := range numEntries {
				_, _ = cache.Put(keys[j], data)
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

	b.Run("MapCache", func(b *testing.B) { runPut100K(b, lru.NewMapCache[benchValue]) })
	b.Run("RadixCache", func(b *testing.B) { runPut100K(b, lru.NewRadixCache[benchValue]) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runPut100K(b, lru.NewArenaRadixCache[benchValue]) })
}

func Benchmark_LargeScale_DeletePrefix_100K(b *testing.B) {
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

	runDeletePrefix100K := func(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue]) {
		b.Helper()
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			cache := constructor(cacheMaxSize, benchWeigher)
			for j := range numEntries {
				_, _ = cache.Put(keys[j], data)
			}
			b.StartTimer()

			// Delete target_prefix/ (50,000 entries)
			cache.DeletePrefix("target_prefix/")
		}
	}

	b.Run("MapCache", func(b *testing.B) { runDeletePrefix100K(b, lru.NewMapCache[benchValue]) })
	b.Run("RadixCache", func(b *testing.B) { runDeletePrefix100K(b, lru.NewRadixCache[benchValue]) })
	b.Run("ArenaRadixCache", func(b *testing.B) { runDeletePrefix100K(b, lru.NewArenaRadixCache[benchValue]) })
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
		cache := lru.NewArenaRadixCache[benchValue](uint64(numKeys*20), benchWeigher).(lru.PressureAwareCache[benchValue])
		for i := range numKeys {
			_, _ = cache.Put(keys[i], data)
		}
		for i := range numKeys / 2 {
			_, _ = cache.Delete(keys[i])
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

func Benchmark_ArenaRadixCache_PutUnderPressure(b *testing.B) {
	const numKeys = 10000
	const keyPool = numKeys * 2
	keys := make([]string, keyPool)
	for i := range keyPool {
		keys[i] = fmt.Sprintf("dir_%02d/file_%05d", i%50, i)
	}
	data := benchValue{val: 1, dataSize: 10}
	cache := lru.NewArenaRadixCache[benchValue](
		uint64(numKeys*10),
		benchWeigher,
		lru.WithPressureFunc(func() float64 { return 0.92 }),
		lru.WithEvictionRetentionRatio(0.50),
	)

	b.ReportAllocs()
	b.ResetTimer()
	i := 0
	for b.Loop() {
		_, _ = cache.Put(keys[i%keyPool], data)
		i++
	}
}

// ============================================================================
// 10. Eviction Callback Benchmarks
// ============================================================================

var (
	benchSinkEvictKey    string
	benchSinkEvictReason lru.EvictionReason
)

func Benchmark_Put_WithOnEvictValue(b *testing.B) {
	cbOpt := lru.WithOnEvictValue(func(v benchValue, r lru.EvictionReason) {
		benchSinkVal = v
		benchSinkEvictReason = r
	})
	b.Run("MapCache", func(b *testing.B) {
		runBenchmarkPutWithOptions(b, lru.NewMapCache[benchValue], 2, 5, benchWeigher, cbOpt)
	})
	b.Run("RadixCache", func(b *testing.B) {
		runBenchmarkPutWithOptions(b, lru.NewRadixCache[benchValue], 2, 5, benchWeigher, cbOpt)
	})
	b.Run("ArenaRadixCache", func(b *testing.B) {
		runBenchmarkPutWithOptions(b, lru.NewArenaRadixCache[benchValue], 2, 5, benchWeigher, cbOpt)
	})
}

func Benchmark_Put_WithOnEvictEntry(b *testing.B) {
	cbOpt := lru.WithOnEvictEntry(func(k string, v benchValue, r lru.EvictionReason) {
		benchSinkEvictKey = k
		benchSinkVal = v
		benchSinkEvictReason = r
	})
	b.Run("MapCache", func(b *testing.B) {
		runBenchmarkPutWithOptions(b, lru.NewMapCache[benchValue], 2, 5, benchWeigher, cbOpt)
	})
	b.Run("RadixCache", func(b *testing.B) {
		runBenchmarkPutWithOptions(b, lru.NewRadixCache[benchValue], 2, 5, benchWeigher, cbOpt)
	})
	b.Run("ArenaRadixCache", func(b *testing.B) {
		runBenchmarkPutWithOptions(b, lru.NewArenaRadixCache[benchValue], 2, 5, benchWeigher, cbOpt)
	})
}

func Benchmark_Replace_WithOnEvictValue(b *testing.B) {
	cbOpt := lru.WithOnEvictValue(func(v benchValue, r lru.EvictionReason) {
		benchSinkVal = v
		benchSinkEvictReason = r
	})
	b.Run("MapCache", func(b *testing.B) {
		runBenchmarkReplaceWithOptions(b, lru.NewMapCache[benchValue], benchWeigher, cbOpt)
	})
	b.Run("RadixCache", func(b *testing.B) {
		runBenchmarkReplaceWithOptions(b, lru.NewRadixCache[benchValue], benchWeigher, cbOpt)
	})
	b.Run("ArenaRadixCache", func(b *testing.B) {
		runBenchmarkReplaceWithOptions(b, lru.NewArenaRadixCache[benchValue], benchWeigher, cbOpt)
	})
}

func Benchmark_Replace_WithOnEvictEntry(b *testing.B) {
	cbOpt := lru.WithOnEvictEntry(func(k string, v benchValue, r lru.EvictionReason) {
		benchSinkEvictKey = k
		benchSinkVal = v
		benchSinkEvictReason = r
	})
	b.Run("MapCache", func(b *testing.B) {
		runBenchmarkReplaceWithOptions(b, lru.NewMapCache[benchValue], benchWeigher, cbOpt)
	})
	b.Run("RadixCache", func(b *testing.B) {
		runBenchmarkReplaceWithOptions(b, lru.NewRadixCache[benchValue], benchWeigher, cbOpt)
	})
	b.Run("ArenaRadixCache", func(b *testing.B) {
		runBenchmarkReplaceWithOptions(b, lru.NewArenaRadixCache[benchValue], benchWeigher, cbOpt)
	})
}

// ============================================================================
// 11. Range-Over-Function Iterator Benchmarks (All, Keys, Values)
// ============================================================================

func setupPopulatedIteratorBenchCache(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue]) lru.Cache[benchValue] {
	b.Helper()
	const numKeys = 1000
	keys, _ := generateBenchmarkKeys(20, 50, 2)
	data := benchValue{val: 1, dataSize: 10}
	cache := constructor(uint64(numKeys*100), benchWeigher)
	for _, key := range keys[:numKeys] {
		_, _ = cache.Put(key, data)
	}
	return cache
}

func runBenchmarkAll(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue]) {
	b.Helper()
	cache := setupPopulatedIteratorBenchCache(b, constructor)
	seq := cache.All()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		seq(func(k string, v benchValue) bool {
			benchSinkEvictKey = k
			benchSinkVal = v
			return true
		})
	}
}

func runBenchmarkKeys(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue]) {
	b.Helper()
	cache := setupPopulatedIteratorBenchCache(b, constructor)
	seq := cache.Keys()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		seq(func(k string) bool {
			benchSinkEvictKey = k
			return true
		})
	}
}

func runBenchmarkValues(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue]) {
	b.Helper()
	cache := setupPopulatedIteratorBenchCache(b, constructor)
	seq := cache.Values()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		seq(func(v benchValue) bool {
			benchSinkVal = v
			return true
		})
	}
}

func Benchmark_All_MapCache(b *testing.B)   { runBenchmarkAll(b, lru.NewMapCache[benchValue]) }
func Benchmark_All_RadixCache(b *testing.B) { runBenchmarkAll(b, lru.NewRadixCache[benchValue]) }
func Benchmark_All_ArenaRadixCache(b *testing.B) {
	runBenchmarkAll(b, lru.NewArenaRadixCache[benchValue])
}

func Benchmark_Keys_MapCache(b *testing.B)   { runBenchmarkKeys(b, lru.NewMapCache[benchValue]) }
func Benchmark_Keys_RadixCache(b *testing.B) { runBenchmarkKeys(b, lru.NewRadixCache[benchValue]) }
func Benchmark_Keys_ArenaRadixCache(b *testing.B) {
	runBenchmarkKeys(b, lru.NewArenaRadixCache[benchValue])
}

func Benchmark_Values_MapCache(b *testing.B)   { runBenchmarkValues(b, lru.NewMapCache[benchValue]) }
func Benchmark_Values_RadixCache(b *testing.B) { runBenchmarkValues(b, lru.NewRadixCache[benchValue]) }
func Benchmark_Values_ArenaRadixCache(b *testing.B) {
	runBenchmarkValues(b, lru.NewArenaRadixCache[benchValue])
}

// ============================================================================
// 12. Stats() Snapshot Benchmarks
// ============================================================================

var benchSinkStats lru.Stats

func runBenchmarkStats(b *testing.B, constructor func(uint64, ...lru.Option) lru.Cache[benchValue]) {
	b.Helper()
	cache := setupPopulatedIteratorBenchCache(b, constructor)

	b.Run("Sequential", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var lastStats lru.Stats
		for b.Loop() {
			lastStats = cache.Stats()
		}
		benchSinkStats = lastStats
	})

	b.Run("Parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			var localStats lru.Stats
			for pb.Next() {
				localStats = cache.Stats()
			}
			if localStats.Len < 0 {
				b.Fatal("unreachable negative Len")
			}
		})
	})
}

func Benchmark_Stats_MapCache(b *testing.B)   { runBenchmarkStats(b, lru.NewMapCache[benchValue]) }
func Benchmark_Stats_RadixCache(b *testing.B) { runBenchmarkStats(b, lru.NewRadixCache[benchValue]) }
func Benchmark_Stats_ArenaRadixCache(b *testing.B) {
	runBenchmarkStats(b, lru.NewArenaRadixCache[benchValue])
}
