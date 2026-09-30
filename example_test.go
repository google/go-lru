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

	_, _ = cache.Insert("greeting", "hello, world")
	_, _ = cache.Insert("inode-42", "42")

	if v, ok := cache.LookUp("greeting"); ok {
		fmt.Printf("greeting=%s (len=%d)\n", v, len(v))
	}
	if v, ok := cache.LookUp("inode-42"); ok {
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

	_, _ = cache.Insert("a", "1234") // size 4 (total 4)
	_, _ = cache.Insert("b", "5678") // size 4 (total 8)

	// Access "a" so "a" becomes MRU and "b" becomes LRU.
	_, _ = cache.LookUp("a")

	// Inserting "c" (size 4) exceeds capacity (8 + 4 > 10), evicting "b".
	evicted, err := cache.Insert("c", "9012")
	if err != nil {
		panic(err)
	}

	_, okA := cache.LookUp("a")
	_, okB := cache.LookUp("b")
	_, okC := cache.LookUp("c")
	fmt.Printf("evicted count=%d, first=%s\n", len(evicted), evicted[0])
	fmt.Printf("a present=%v, b present=%v, c present=%v\n", okA, okB, okC)
	// Output:
	// evicted count=1, first=5678
	// a present=true, b present=false, c present=true
}

// ExampleNew_backendsAndPrefixErase demonstrates selecting the RadixCache backend
// via WithBackend and performing fast hierarchical prefix erasure.
func ExampleNew_backendsAndPrefixErase() {
	cache := lru.New[string](4096, lru.WithBackend(lru.BackendRadix))

	_, _ = cache.Insert("bucket/dirA/file1.txt", "data-1")
	_, _ = cache.Insert("bucket/dirA/file2.txt", "data-2")
	_, _ = cache.Insert("bucket/dirB/file3.txt", "data-3")

	// Purge only the "bucket/dirA/" subtree.
	cache.EraseEntriesWithGivenPrefix("bucket/dirA/")

	_, okA := cache.LookUp("bucket/dirA/file1.txt")
	_, okB := cache.LookUp("bucket/dirB/file3.txt")
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

	_, _ = cache.Insert("dir/item1", "v1")
	_, _ = cache.Insert("dir/item2", "v2")

	// Simulate Critical Pressure (>= 0.90) to shed LRU entries down to 50% of maxSize (50 bytes).
	pressure = 0.95
	paCache := cache.(lru.PressureAwareCache[string])
	shed := paCache.EvaluateMemoryPressure()

	_, ok1 := cache.LookUpWithoutChangingOrder("dir/item1")
	_, ok2 := cache.LookUpWithoutChangingOrder("dir/item2")
	fmt.Printf("shed entries=%d, item1 remaining=%v, item2 remaining=%v\n",
		len(shed),
		ok1,
		ok2,
	)
	// Output:
	// shed entries=1, item1 remaining=false, item2 remaining=true
}
