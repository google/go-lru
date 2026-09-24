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

// ExampleNew demonstrates creating an LRU cache with the unified constructor
// and caching common types using StringValue, BytesValue, and NewValue.
func ExampleNew() {
	cache := lru.New(1024)

	_, _ = cache.Insert("greeting", lru.StringValue("hello, world"))
	_, _ = cache.Insert("payload", lru.BytesValue([]byte{0xDE, 0xAD, 0xBE, 0xEF}))
	_, _ = cache.Insert("inode-42", lru.NewValue(42, 64))

	if v := cache.LookUp("greeting"); v != nil {
		fmt.Printf("greeting=%s (size=%d)\n", v.(lru.StringValue), v.Size())
	}
	if v := cache.LookUp("inode-42"); v != nil {
		fmt.Printf("inode=%d (size=%d)\n", v.(lru.SizedValue[int]).Value, v.Size())
	}
	// Output:
	// greeting=hello, world (size=12)
	// inode=42 (size=64)
}

// ExampleNew_eviction demonstrates size-based capacity tracking and LRU eviction order.
func ExampleNew_eviction() {
	// Create a cache with a 10-byte capacity.
	cache := lru.New(10)

	_, _ = cache.Insert("a", lru.StringValue("1234")) // size 4 (total 4)
	_, _ = cache.Insert("b", lru.StringValue("5678")) // size 4 (total 8)

	// Access "a" so "a" becomes MRU and "b" becomes LRU.
	_ = cache.LookUp("a")

	// Inserting "c" (size 4) exceeds capacity (8 + 4 > 10), evicting "b".
	evicted, err := cache.Insert("c", lru.StringValue("9012"))
	if err != nil {
		panic(err)
	}

	fmt.Printf("evicted count=%d, first=%s\n", len(evicted), evicted[0].(lru.StringValue))
	fmt.Printf("a present=%v, b present=%v, c present=%v\n",
		cache.LookUp("a") != nil,
		cache.LookUp("b") != nil,
		cache.LookUp("c") != nil,
	)
	// Output:
	// evicted count=1, first=5678
	// a present=true, b present=false, c present=true
}

// ExampleNew_backendsAndPrefixErase demonstrates selecting the RadixCache backend
// via WithBackend and performing fast hierarchical prefix erasure.
func ExampleNew_backendsAndPrefixErase() {
	cache := lru.New(4096, lru.WithBackend(lru.BackendRadix))

	_, _ = cache.Insert("bucket/dirA/file1.txt", lru.StringValue("data-1"))
	_, _ = cache.Insert("bucket/dirA/file2.txt", lru.StringValue("data-2"))
	_, _ = cache.Insert("bucket/dirB/file3.txt", lru.StringValue("data-3"))

	// Purge only the "bucket/dirA/" subtree.
	cache.EraseEntriesWithGivenPrefix("bucket/dirA/")

	fmt.Printf("dirA/file1=%v, dirB/file3=%v\n",
		cache.LookUp("bucket/dirA/file1.txt") != nil,
		cache.LookUp("bucket/dirB/file3.txt") != nil,
	)
	// Output:
	// dirA/file1=false, dirB/file3=true
}

// ExampleNew_memoryPressure demonstrates configuring the ArenaRadixCache backend
// with a custom memory-pressure callback and triggering two-tier reclamation.
func ExampleNew_memoryPressure() {
	pressure := 0.50 // Normal pressure
	cache := lru.New(
		100,
		lru.WithBackend(lru.BackendArenaRadix),
		lru.WithPressureFunc(func() float64 { return pressure }),
		lru.WithCompactionThreshold(0.75),
		lru.WithEvictionThreshold(0.90),
		lru.WithEvictionRetentionRatio(0.50),
	)

	_, _ = cache.Insert("dir/item1", lru.NewValue("v1", 40))
	_, _ = cache.Insert("dir/item2", lru.NewValue("v2", 40))

	// Simulate Critical Pressure (>= 0.90) to shed LRU entries down to 50% of maxSize (50 bytes).
	pressure = 0.95
	paCache := cache.(lru.PressureAwareCache)
	shed := paCache.EvaluateMemoryPressure()

	fmt.Printf("shed entries=%d, item1 remaining=%v, item2 remaining=%v\n",
		len(shed),
		cache.LookUpWithoutChangingOrder("dir/item1") != nil,
		cache.LookUpWithoutChangingOrder("dir/item2") != nil,
	)
	// Output:
	// shed entries=1, item1 remaining=false, item2 remaining=true
}

// 7c02
