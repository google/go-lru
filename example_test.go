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

	"github.com/google/go-lru"
)

// ExampleNew demonstrates creating a generic LRU cache with default entry-count
// weighing (each entry weighs 1) and looking up values directly without type assertions.
func ExampleNew() {
	cache := lru.New[string](1024)

	_, _ = cache.Put("greeting", "hello, world")
	_, _ = cache.Put("inode-42", "42")

	if v, ok := cache.Get("greeting"); ok {
		fmt.Printf("greeting=%s (len=%d)\n", v, len(v))
	}
	if v, ok := cache.Get("inode-42"); ok {
		fmt.Printf("inode=%s\n", v)
	}
	// Output:
	// greeting=hello, world (len=12)
	// inode=42
}

// ExampleNew_eviction demonstrates custom byte-size capacity tracking via WithWeigher
// and LRU eviction order.
func ExampleNew_eviction() {
	// Create a cache with a 10-byte capacity weighed by string length.
	cache := lru.New[string](10, lru.WithWeigher(func(_ string, v string) uint64 {
		return uint64(len(v))
	}))

	_, _ = cache.Put("a", "1234") // size 4 (total 4)
	_, _ = cache.Put("b", "5678") // size 4 (total 8)

	// Access "a" so "a" becomes MRU and "b" becomes LRU.
	_, _ = cache.Get("a")

	// Putting "c" (size 4) exceeds capacity (8 + 4 > 10), evicting "b".
	evicted, err := cache.Put("c", "9012")
	if err != nil {
		panic(err)
	}

	_, okA := cache.Get("a")
	_, okB := cache.Get("b")
	_, okC := cache.Get("c")
	fmt.Printf("evicted count=%d, first=%s\n", len(evicted), evicted[0])
	fmt.Printf("a present=%v, b present=%v, c present=%v\n", okA, okB, okC)
	// Output:
	// evicted count=1, first=5678
	// a present=true, b present=false, c present=true
}

// ExampleNew_backendsAndDeletePrefix demonstrates selecting the RadixCache backend
// via WithBackend and performing fast hierarchical prefix deletion.
func ExampleNew_backendsAndDeletePrefix() {
	cache := lru.New[string](4096, lru.WithBackend(lru.BackendRadix))

	_, _ = cache.Put("bucket/dirA/file1.txt", "data-1")
	_, _ = cache.Put("bucket/dirA/file2.txt", "data-2")
	_, _ = cache.Put("bucket/dirB/file3.txt", "data-3")

	// Purge only the "bucket/dirA/" subtree.
	cache.DeletePrefix("bucket/dirA/")

	_, okA := cache.Get("bucket/dirA/file1.txt")
	_, okB := cache.Get("bucket/dirB/file3.txt")
	fmt.Printf("dirA/file1=%v, dirB/file3=%v\n", okA, okB)
	// Output:
	// dirA/file1=false, dirB/file3=true
}

// ExampleNew_memoryPressure demonstrates configuring the ArenaRadixCache backend
// with a custom weigher and memory-pressure callback to trigger two-tier reclamation.
func ExampleNew_memoryPressure() {
	pressure := 0.50 // Normal pressure
	cache := lru.New[string](
		100,
		lru.WithBackend(lru.BackendArenaRadix),
		lru.WithWeigher(func(_, _ string) uint64 { return 40 }),
		lru.WithPressureFunc(func() float64 { return pressure }),
		lru.WithCompactionThreshold(0.75),
		lru.WithEvictionThreshold(0.90),
		lru.WithEvictionRetentionRatio(0.50),
	)

	_, _ = cache.Put("dir/item1", "v1")
	_, _ = cache.Put("dir/item2", "v2")

	// Simulate Critical Pressure (>= 0.90) to shed LRU entries down to 50% of maxSize (50 bytes).
	pressure = 0.95
	paCache := cache.(lru.PressureAwareCache[string])
	shed := paCache.EvaluateMemoryPressure()

	_, ok1 := cache.Peek("dir/item1")
	_, ok2 := cache.Peek("dir/item2")
	fmt.Printf("shed entries=%d, item1 remaining=%v, item2 remaining=%v\n",
		len(shed),
		ok1,
		ok2,
	)
	// Output:
	// shed entries=1, item1 remaining=false, item2 remaining=true
}

// ExampleNew_onEvict demonstrates observing entry removals and value replacements
// using WithOnEvictValue and WithOnEvictEntry along with EvictionReason.
func ExampleNew_onEvict() {
	cache := lru.New[string](
		2,
		lru.WithBackend(lru.BackendRadix),
		lru.WithOnEvictValue(func(val string, reason lru.EvictionReason) {
			fmt.Printf("value callback: val=%s reason=%s\n", val, reason)
		}),
		lru.WithOnEvictEntry(func(key, val string, reason lru.EvictionReason) {
			fmt.Printf("entry callback: key=%s val=%s reason=%s\n", key, val, reason)
		}),
	)

	_, _ = cache.Put("dir/a", "v1")
	_, _ = cache.Put("dir/b", "v2")

	// Overwriting "dir/a" triggers EvictionReasonReplaced for the old value "v1".
	_, _ = cache.Put("dir/a", "v1-updated")

	// Inserting "dir/c" exceeds capacity (2), evicting LRU "dir/b" with EvictionReasonCapacity.
	_, _ = cache.Put("dir/c", "v3")

	// Explicitly deleting "dir/a" triggers EvictionReasonDeleted.
	_, _ = cache.Delete("dir/a")
	// Output:
	// value callback: val=v1 reason=Replaced
	// entry callback: key=dir/a val=v1 reason=Replaced
	// value callback: val=v2 reason=Capacity
	// entry callback: key=dir/b val=v2 reason=Capacity
	// value callback: val=v1-updated reason=Deleted
	// entry callback: key=dir/a val=v1-updated reason=Deleted
}

// ExampleNew_iterators demonstrates iterating over cache entries, keys, and values
// in deterministic MRU-to-LRU order using Go 1.23 range-over-function iterators
// without altering LRU recency order.
func ExampleNew_iterators() {
	cache := lru.New[int](10, lru.WithBackend(lru.BackendRadix))

	_, _ = cache.Put("dir/a", 1)
	_, _ = cache.Put("dir/b", 2)
	_, _ = cache.Put("dir/c", 3)

	// Promote "dir/a" to MRU; order is now: dir/a (MRU), dir/c, dir/b (LRU).
	_, _ = cache.Get("dir/a")

	for k, v := range cache.All() {
		fmt.Printf("%s=%d\n", k, v)
	}

	// Early break is supported and leaves LRU order unchanged.
	for k := range cache.Keys() {
		fmt.Printf("mru=%s\n", k)
		break
	}

	var sum int
	for v := range cache.Values() {
		sum += v
	}
	fmt.Printf("sum=%d\n", sum)
	// Output:
	// dir/a=1
	// dir/c=3
	// dir/b=2
	// mru=dir/a
	// sum=6
}

// ExampleNew_stats demonstrates inspecting point-in-time cache telemetry via Stats()
// and the non-generic StatsProvider interface.
func ExampleNew_stats() {
	cache := lru.New[string](10, lru.WithWeigher(func(_ string, v string) uint64 {
		return uint64(len(v))
	}))

	_, _ = cache.Put("a", "1234") // size 4 (total 4)
	_, _ = cache.Put("b", "5678") // size 4 (total 8)
	_, _ = cache.Get("a")         // hit ("a" becomes MRU)
	_, _ = cache.Get("missing")   // miss

	// Inserting "c" (size 4) exceeds maxSize (10), evicting LRU entry "b" (size 4).
	_, _ = cache.Put("c", "9012")

	var provider lru.StatsProvider = cache
	st := provider.Stats()

	fmt.Printf("backend=%s len=%d size=%d/%d\n", st.Backend, st.Len, st.CurrentSize, st.MaxSize)
	fmt.Printf("hits=%d misses=%d\n", st.GetHits, st.GetMisses)
	fmt.Printf("capacity_evictions=%d capacity_evicted_weight=%d\n",
		st.Evictions(lru.EvictionReasonCapacity),
		st.EvictedWeight(lru.EvictionReasonCapacity),
	)
	// Output:
	// backend=MapCache len=2 size=8/10
	// hits=1 misses=1
	// capacity_evictions=1 capacity_evicted_weight=4
}
