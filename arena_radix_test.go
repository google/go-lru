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
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const arenaTestMaxSize = 50

type arenaTestData struct {
	Value    int64
	DataSize uint64
}

func (td arenaTestData) Size() uint64 {
	return td.DataSize
}

func setupArenaRadixCacheTest(t *testing.T) Cache {
	t.Helper()
	return NewArenaRadixCache(arenaTestMaxSize, WithInvariantChecking(true))
}

func insertAndAssertArena(t *testing.T, cache Cache, key string, val ValueType, evictedValues []int64, expectedError error) {
	t.Helper()
	ret, err := cache.Insert(key, val)

	if expectedError != nil {
		if !errors.Is(err, expectedError) {
			t.Fatalf("expected error %v, got %v", expectedError, err)
		}
	} else if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(ret) != len(evictedValues) {
		t.Fatalf("expected %d evicted entries, got %d", len(evictedValues), len(ret))
	}
	for i, v := range ret {
		td, ok := v.(arenaTestData)
		if !ok {
			t.Fatalf("expected arenaTestData type, got %T", v)
		}
		if td.Value != evictedValues[i] {
			t.Fatalf("expected evicted value at index %d to be %d, got %d", i, evictedValues[i], td.Value)
		}
	}
}

func TestArenaRadixCache_Constructor(t *testing.T) {
	t.Run("DefaultOptions", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic on NewArenaRadixCache(0) with default options, got nil")
			}
		}()
		_ = NewArenaRadixCache(0)
	})

	t.Run("InvariantsEnabled", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic on NewArenaRadixCache(0) with invariant checking enabled, got nil")
			}
		}()
		_ = NewArenaRadixCache(0, WithInvariantChecking(true))
	})

	t.Run("InvariantsDisabled", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic on NewArenaRadixCache(0) with invariant checking disabled, got nil")
			}
		}()
		_ = NewArenaRadixCache(0, WithInvariantChecking(false))
	})
}

func TestArenaRadixCache_LookUpInEmptyCache(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	if v := cache.LookUp(""); v != nil {
		t.Fatalf("expected nil lookup for empty key on empty cache, got %v", v)
	}
	if v := cache.LookUp("taco"); v != nil {
		t.Fatalf("expected nil lookup for 'taco' on empty cache, got %v", v)
	}
}

func TestArenaRadixCache_InsertNilValue(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "taco", nil, []int64{}, ErrInvalidEntry)
}

func TestArenaRadixCache_InsertEmptyKey(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)

	insertAndAssertArena(t, cache, "", arenaTestData{Value: 42, DataSize: 10}, []int64{}, nil)

	val := cache.LookUp("")
	if val == nil {
		t.Fatalf("expected value for empty key, got nil")
	}
	if td, ok := val.(arenaTestData); !ok || td.Value != 42 {
		t.Fatalf("expected value 42, got %v", val)
	}
	if v := cache.LookUp("taco"); v != nil {
		t.Fatalf("expected nil for 'taco', got %v", v)
	}
}

func TestArenaRadixCache_LookUpUnknownKey(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "burrito", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)
	insertAndAssertArena(t, cache, "taco", arenaTestData{Value: 23, DataSize: 8}, []int64{}, nil)

	if v := cache.LookUp(""); v != nil {
		t.Fatalf("expected nil for empty key, got %v", v)
	}
	if v := cache.LookUp("enchilada"); v != nil {
		t.Fatalf("expected nil for 'enchilada', got %v", v)
	}
}

func TestArenaRadixCache_FillUpToCapacity(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "burrito", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)
	insertAndAssertArena(t, cache, "taco", arenaTestData{Value: 26, DataSize: 20}, []int64{}, nil)
	insertAndAssertArena(t, cache, "enchilada", arenaTestData{Value: 28, DataSize: 26}, []int64{}, nil)

	if v := cache.LookUp("burrito"); v == nil || v.(arenaTestData).Value != 23 {
		t.Fatalf("expected 23 for 'burrito', got %v", v)
	}
	if v := cache.LookUp("taco"); v == nil || v.(arenaTestData).Value != 26 {
		t.Fatalf("expected 26 for 'taco', got %v", v)
	}
	if v := cache.LookUp("enchilada"); v == nil || v.(arenaTestData).Value != 28 {
		t.Fatalf("expected 28 for 'enchilada', got %v", v)
	}
}

func TestArenaRadixCache_ExpiresLeastRecentlyUsed(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "burrito", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)
	insertAndAssertArena(t, cache, "taco", arenaTestData{Value: 26, DataSize: 20}, []int64{}, nil)
	insertAndAssertArena(t, cache, "enchilada", arenaTestData{Value: 28, DataSize: 26}, []int64{}, nil)

	// Promote burrito to MRU
	if v := cache.LookUp("burrito"); v == nil || v.(arenaTestData).Value != 23 {
		t.Fatalf("expected 23 for 'burrito', got %v", v)
	}

	// Insert another item; taco (least recent) should be evicted
	insertAndAssertArena(t, cache, "queso", arenaTestData{Value: 34, DataSize: 5}, []int64{26}, nil)

	if v := cache.LookUp("taco"); v != nil {
		t.Fatalf("expected 'taco' to be evicted, got %v", v)
	}
	if v := cache.LookUp("burrito"); v == nil || v.(arenaTestData).Value != 23 {
		t.Fatalf("expected 23 for 'burrito', got %v", v)
	}
	if v := cache.LookUp("enchilada"); v == nil || v.(arenaTestData).Value != 28 {
		t.Fatalf("expected 28 for 'enchilada', got %v", v)
	}
	if v := cache.LookUp("queso"); v == nil || v.(arenaTestData).Value != 34 {
		t.Fatalf("expected 34 for 'queso', got %v", v)
	}
}

func TestArenaRadixCache_Overwrite(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "burrito", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)
	insertAndAssertArena(t, cache, "taco", arenaTestData{Value: 26, DataSize: 20}, []int64{}, nil)
	insertAndAssertArena(t, cache, "enchilada", arenaTestData{Value: 28, DataSize: 20}, []int64{}, nil)
	insertAndAssertArena(t, cache, "burrito", arenaTestData{Value: 33, DataSize: 6}, []int64{}, nil)

	// Increase size during overwrite; taco should be evicted
	insertAndAssertArena(t, cache, "burrito", arenaTestData{Value: 33, DataSize: 12}, []int64{26}, nil)

	if v := cache.LookUp("taco"); v != nil {
		t.Fatalf("expected 'taco' to be evicted, got %v", v)
	}
	if v := cache.LookUp("burrito"); v == nil || v.(arenaTestData).Value != 33 {
		t.Fatalf("expected 33 for 'burrito', got %v", v)
	}
	if v := cache.LookUp("enchilada"); v == nil || v.(arenaTestData).Value != 28 {
		t.Fatalf("expected 28 for 'enchilada', got %v", v)
	}
}

func TestArenaRadixCache_MultipleEviction(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "burrito", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)
	insertAndAssertArena(t, cache, "taco", arenaTestData{Value: 26, DataSize: 20}, []int64{}, nil)
	insertAndAssertArena(t, cache, "enchilada", arenaTestData{Value: 28, DataSize: 20}, []int64{}, nil)

	// Large insert requiring all previous entries to be evicted
	insertAndAssertArena(t, cache, "large_data", arenaTestData{Value: 33, DataSize: 45}, []int64{23, 26, 28}, nil)

	if v := cache.LookUp("taco"); v != nil {
		t.Fatalf("expected 'taco' evicted, got %v", v)
	}
	if v := cache.LookUp("burrito"); v != nil {
		t.Fatalf("expected 'burrito' evicted, got %v", v)
	}
	if v := cache.LookUp("enchilada"); v != nil {
		t.Fatalf("expected 'enchilada' evicted, got %v", v)
	}
	if v := cache.LookUp("large_data"); v == nil || v.(arenaTestData).Value != 33 {
		t.Fatalf("expected 33 for 'large_data', got %v", v)
	}
}

func TestArenaRadixCache_WhenEntrySizeMoreThanCacheMaxSize(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "burrito", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)

	// Attempt inserting item with size > arenaTestMaxSize
	insertAndAssertArena(t, cache, "taco", arenaTestData{Value: 26, DataSize: arenaTestMaxSize + 1}, []int64{}, ErrInvalidEntrySize)

	if v := cache.LookUp("burrito"); v == nil || v.(arenaTestData).Value != 23 {
		t.Fatalf("expected 'burrito' to be retained, got %v", v)
	}
}

func TestArenaRadixCache_EraseWhenKeyPresent(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "burrito", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)

	deletedEntry := cache.Erase("burrito")
	if deletedEntry == nil || deletedEntry.(arenaTestData).Value != 23 {
		t.Fatalf("expected erased value 23, got %v", deletedEntry)
	}
	if v := cache.LookUp("burrito"); v != nil {
		t.Fatalf("expected 'burrito' to be nil after erase, got %v", v)
	}
}

func TestArenaRadixCache_EraseWhenKeyNotPresent(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "burrito", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)

	deletedEntry := cache.Erase("taco")
	if deletedEntry != nil {
		t.Fatalf("expected nil for non-existent erase, got %v", deletedEntry)
	}
	if v := cache.LookUp("burrito"); v == nil || v.(arenaTestData).Value != 23 {
		t.Fatalf("expected 'burrito' to remain, got %v", v)
	}
}

func TestArenaRadixCache_EraseCacheWithGivenPrefix(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "a", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)
	insertAndAssertArena(t, cache, "a/b", arenaTestData{Value: 26, DataSize: 5}, []int64{}, nil)
	insertAndAssertArena(t, cache, "a/b/d", arenaTestData{Value: 22, DataSize: 6}, []int64{}, nil)
	insertAndAssertArena(t, cache, "a/c", arenaTestData{Value: 20, DataSize: 6}, []int64{}, nil)
	insertAndAssertArena(t, cache, "b", arenaTestData{Value: 21, DataSize: 2}, []int64{}, nil)

	cache.EraseEntriesWithGivenPrefix("a")

	if v := cache.LookUp("a"); v != nil {
		t.Fatalf("expected 'a' erased, got %v", v)
	}
	if v := cache.LookUp("a/b"); v != nil {
		t.Fatalf("expected 'a/b' erased, got %v", v)
	}
	if v := cache.LookUp("a/b/d"); v != nil {
		t.Fatalf("expected 'a/b/d' erased, got %v", v)
	}
	if v := cache.LookUp("a/c"); v != nil {
		t.Fatalf("expected 'a/c' erased, got %v", v)
	}
	if v := cache.LookUp("b"); v == nil || v.Size() != 2 {
		t.Fatalf("expected 'b' to remain with size 2, got %v", v)
	}
}

func TestArenaRadixCache_EraseCacheWithEmptyPrefix(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "a", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)
	insertAndAssertArena(t, cache, "a/b", arenaTestData{Value: 26, DataSize: 5}, []int64{}, nil)
	insertAndAssertArena(t, cache, "b", arenaTestData{Value: 21, DataSize: 2}, []int64{}, nil)

	cache.EraseEntriesWithGivenPrefix("")

	if v := cache.LookUp("a"); v != nil {
		t.Fatalf("expected 'a' erased, got %v", v)
	}
	if v := cache.LookUp("a/b"); v != nil {
		t.Fatalf("expected 'a/b' erased, got %v", v)
	}
	if v := cache.LookUp("b"); v != nil {
		t.Fatalf("expected 'b' erased, got %v", v)
	}
}

func TestArenaRadixCache_EraseCacheWhereNoEntriesExistWithGivenPrefix(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "a", arenaTestData{Value: 23, DataSize: 4}, []int64{}, nil)
	insertAndAssertArena(t, cache, "a/b", arenaTestData{Value: 26, DataSize: 5}, []int64{}, nil)
	insertAndAssertArena(t, cache, "b", arenaTestData{Value: 21, DataSize: 2}, []int64{}, nil)

	cache.EraseEntriesWithGivenPrefix("c")

	if v := cache.LookUp("a"); v == nil || v.Size() != 4 {
		t.Fatalf("expected 'a' size 4, got %v", v)
	}
	if v := cache.LookUp("a/b"); v == nil || v.Size() != 5 {
		t.Fatalf("expected 'a/b' size 5, got %v", v)
	}
	if v := cache.LookUp("b"); v == nil || v.Size() != 2 {
		t.Fatalf("expected 'b' size 2, got %v", v)
	}
}

func TestArenaRadixCache_EraseCacheWithGivenPrefixWithSomeEntriesEvictedDueToCacheSize(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	insertAndAssertArena(t, cache, "a", arenaTestData{Value: 23, DataSize: 20}, []int64{}, nil)
	insertAndAssertArena(t, cache, "a/b", arenaTestData{Value: 26, DataSize: 10}, []int64{}, nil)
	insertAndAssertArena(t, cache, "a/b/d", arenaTestData{Value: 22, DataSize: 5}, []int64{}, nil)
	insertAndAssertArena(t, cache, "a/c", arenaTestData{Value: 20, DataSize: 10}, []int64{}, nil)
	insertAndAssertArena(t, cache, "b", arenaTestData{Value: 21, DataSize: 15}, []int64{23}, nil)

	// "a" was evicted by "b", remaining "a/b", "a/b/d", "a/c" should be erased
	cache.EraseEntriesWithGivenPrefix("a")

	if v := cache.LookUp("a"); v != nil {
		t.Fatalf("expected 'a' nil, got %v", v)
	}
	if v := cache.LookUp("a/b"); v != nil {
		t.Fatalf("expected 'a/b' nil, got %v", v)
	}
	if v := cache.LookUp("a/b/d"); v != nil {
		t.Fatalf("expected 'a/b/d' nil, got %v", v)
	}
	if v := cache.LookUp("a/c"); v != nil {
		t.Fatalf("expected 'a/c' nil, got %v", v)
	}
	if v := cache.LookUp("b"); v == nil || v.Size() != 15 {
		t.Fatalf("expected 'b' size 15, got %v", v)
	}
}

func TestArenaRadixCache_UpdateSize(t *testing.T) {
	t.Run("NonExistentKey", func(t *testing.T) {
		cache := NewArenaRadixCache(100, WithInvariantChecking(true))
		err := cache.UpdateSize("key1", 20)
		if !errors.Is(err, ErrEntryNotExist) {
			t.Fatalf("expected ErrEntryNotExist, got %v", err)
		}
	})

	t.Run("Immediate Eviction", func(t *testing.T) {
		cache := NewArenaRadixCache(100, WithInvariantChecking(true))
		data1 := arenaTestData{Value: 1, DataSize: 10}
		data2 := arenaTestData{Value: 2, DataSize: 70}
		_, _ = cache.Insert("key1", data1)
		_, _ = cache.Insert("key2", data2)

		errUpdate := cache.UpdateSize("key1", 30)
		if errUpdate != nil {
			t.Fatalf("unexpected error: %v", errUpdate)
		}
		if v := cache.LookUp("key1"); v != nil {
			t.Fatalf("expected 'key1' evicted, got %v", v)
		}
		if v := cache.LookUp("key2"); v == nil {
			t.Fatalf("expected 'key2' present, got nil")
		}
	})
}

func TestArenaRadixCache_UpdateSize_ExceedsMaxSize(t *testing.T) {
	cache := NewArenaRadixCache(100, WithInvariantChecking(true))
	data := &arenaTestData{Value: 1, DataSize: 50}
	_, err := cache.Insert("file.txt", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data.DataSize = 150
	err = cache.UpdateSize("file.txt", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v := cache.LookUp("file.txt"); v != nil {
		t.Fatalf("expected 'file.txt' evicted, got %v", v)
	}
}

func TestArenaRadixCache_UpdateSize_MultipleEvictions(t *testing.T) {
	cache := NewArenaRadixCache(100, WithInvariantChecking(true))

	_, _ = cache.Insert("k1", arenaTestData{Value: 1, DataSize: 20})
	_, _ = cache.Insert("k2", arenaTestData{Value: 2, DataSize: 20})
	_, _ = cache.Insert("k3", arenaTestData{Value: 3, DataSize: 20})
	_, _ = cache.Insert("k4", arenaTestData{Value: 4, DataSize: 20})

	err := cache.UpdateSize("k4", 50)
	if err != nil {
		t.Fatalf("unexpected error updating size: %v", err)
	}

	if v := cache.LookUp("k1"); v != nil {
		t.Fatalf("expected k1 evicted, got %v", v)
	}
	if v := cache.LookUp("k2"); v != nil {
		t.Fatalf("expected k2 evicted, got %v", v)
	}
	if v := cache.LookUp("k3"); v == nil {
		t.Fatalf("expected k3 present, got nil")
	}
	if v := cache.LookUp("k4"); v == nil {
		t.Fatalf("expected k4 present, got nil")
	}
}

func TestArenaRadixCache_UpdateWhenKeyPresent(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	key := "burrito"
	data := arenaTestData{Value: 23, DataSize: 4}
	insertAndAssertArena(t, cache, key, data, []int64{}, nil)

	newData := arenaTestData{Value: 2, DataSize: 4}
	err := cache.UpdateWithoutChangingOrder(key, newData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v := cache.LookUp(key); v == nil || v.(arenaTestData).Value != 2 {
		t.Fatalf("expected value 2, got %v", v)
	}
}

func TestArenaRadixCache_UpdateWhenKeyNotPresent(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	key := "burrito"
	data := arenaTestData{Value: 23, DataSize: 4}

	err := cache.UpdateWithoutChangingOrder(key, data)
	if !errors.Is(err, ErrEntryNotExist) {
		t.Fatalf("expected ErrEntryNotExist, got %v", err)
	}
}

func TestArenaRadixCache_UpdateWhenSizeIsDifferent(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	key := "burrito"
	data := arenaTestData{Value: 23, DataSize: 4}
	insertAndAssertArena(t, cache, key, data, []int64{}, nil)

	newData := arenaTestData{Value: 2, DataSize: 3}
	err := cache.UpdateWithoutChangingOrder(key, newData)
	if !errors.Is(err, ErrInvalidUpdateEntrySize) {
		t.Fatalf("expected ErrInvalidUpdateEntrySize, got %v", err)
	}
}

func TestArenaRadixCache_UpdateNilValue(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	err := cache.UpdateWithoutChangingOrder("key", nil)
	if !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("expected ErrInvalidEntry, got %v", err)
	}
}

func TestArenaRadixCache_UpdateNotChangeOrder(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)
	key1 := "burrito1"
	data1 := arenaTestData{Value: 23, DataSize: 10}
	insertAndAssertArena(t, cache, key1, data1, []int64{}, nil)
	key2 := "burrito2"
	data2 := arenaTestData{Value: 2, DataSize: 40}
	insertAndAssertArena(t, cache, key2, data2, []int64{}, nil)

	newData := arenaTestData{Value: 7, DataSize: 10}
	err := cache.UpdateWithoutChangingOrder(key1, newData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Inserting key3 (size 5) should evict key1 (value 7) because key1 remained the LRU element
	key3 := "burrito3"
	data3 := arenaTestData{Value: 3, DataSize: 5}
	insertAndAssertArena(t, cache, key3, data3, []int64{7}, nil)
}

func TestArenaRadixCache_LookUpWithoutChangingOrder(t *testing.T) {
	cache := setupArenaRadixCacheTest(t)

	// Absent key
	if v := cache.LookUpWithoutChangingOrder("absent"); v != nil {
		t.Fatalf("expected nil for absent key, got %v", v)
	}

	key1 := "burrito1"
	data1 := arenaTestData{Value: 23, DataSize: 10}
	insertAndAssertArena(t, cache, key1, data1, []int64{}, nil)
	key2 := "burrito2"
	data2 := arenaTestData{Value: 2, DataSize: 40}
	insertAndAssertArena(t, cache, key2, data2, []int64{}, nil)

	// LookUpWithoutChangingOrder on key1
	val := cache.LookUpWithoutChangingOrder(key1)
	if val == nil || val.(arenaTestData).Value != 23 {
		t.Fatalf("expected 23 for key1, got %v", val)
	}

	// Inserting key3 (size 5) should evict key1 because its LRU position was not altered
	key3 := "burrito3"
	data3 := arenaTestData{Value: 3, DataSize: 5}
	insertAndAssertArena(t, cache, key3, data3, []int64{23}, nil)
}

func TestArenaRadixCache_RadixEdgeSplitsAndCompression(t *testing.T) {
	cache := NewArenaRadixCache(1000, WithInvariantChecking(true))

	keys := []string{
		"apple",
		"app",
		"application",
		"apply",
		"apt",
		"banana",
		"band",
		"bandana",
	}

	for i, k := range keys {
		_, err := cache.Insert(k, arenaTestData{Value: int64(i + 1), DataSize: 10})
		if err != nil {
			t.Fatalf("failed inserting key %q: %v", k, err)
		}
	}

	for i, k := range keys {
		v := cache.LookUp(k)
		if v == nil || v.(arenaTestData).Value != int64(i+1) {
			t.Fatalf("expected %d for key %q, got %v", i+1, k, v)
		}
	}

	// Erase leaves and intermediate nodes to exercise compressPathUpwards
	if v := cache.Erase("application"); v == nil || v.(arenaTestData).Value != 3 {
		t.Fatalf("failed erasing 'application', got %v", v)
	}
	if v := cache.Erase("apply"); v == nil || v.(arenaTestData).Value != 4 {
		t.Fatalf("failed erasing 'apply', got %v", v)
	}
	if v := cache.Erase("app"); v == nil || v.(arenaTestData).Value != 2 {
		t.Fatalf("failed erasing 'app', got %v", v)
	}

	// Verify remaining keys still accessible
	remaining := []struct {
		key string
		val int64
	}{
		{"apple", 1},
		{"apt", 5},
		{"banana", 6},
		{"band", 7},
		{"bandana", 8},
	}

	for _, tc := range remaining {
		v := cache.LookUp(tc.key)
		if v == nil || v.(arenaTestData).Value != tc.val {
			t.Fatalf("expected %d for key %q, got %v", tc.val, tc.key, v)
		}
	}
}

func TestArenaRadixCache_FreeListRecycling(t *testing.T) {
	cache := NewArenaRadixCache(10000, WithInvariantChecking(true))

	for cycle := 0; cycle < 10; cycle++ {
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("prefix/subdir_%d/file_%d.txt", i%5, i)
			_, err := cache.Insert(key, arenaTestData{Value: int64(i), DataSize: 10})
			if err != nil {
				t.Fatalf("insert failed at cycle %d, item %d: %v", cycle, i, err)
			}
		}

		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("prefix/subdir_%d/file_%d.txt", i%5, i)
			v := cache.Erase(key)
			if v == nil {
				t.Fatalf("erase failed at cycle %d, item %d", cycle, i)
			}
		}
	}
}

func TestArenaRadixCache_DeepHierarchy(t *testing.T) {
	cache := NewArenaRadixCache(10000, WithInvariantChecking(true))

	path1 := "a/b/c/d/e/f/g/h/i/j/file1.txt"
	path2 := "a/b/c/d/e/f/g/h/i/j/file2.txt"
	path3 := "a/b/c/d/other/file3.txt"
	path4 := "x/y/z/file4.txt"

	_, _ = cache.Insert(path1, arenaTestData{Value: 1, DataSize: 10})
	_, _ = cache.Insert(path2, arenaTestData{Value: 2, DataSize: 10})
	_, _ = cache.Insert(path3, arenaTestData{Value: 3, DataSize: 10})
	_, _ = cache.Insert(path4, arenaTestData{Value: 4, DataSize: 10})

	cache.EraseEntriesWithGivenPrefix("a/b/c/d/e/")

	if v := cache.LookUp(path1); v != nil {
		t.Fatalf("expected path1 erased, got %v", v)
	}
	if v := cache.LookUp(path2); v != nil {
		t.Fatalf("expected path2 erased, got %v", v)
	}
	if v := cache.LookUp(path3); v == nil || v.(arenaTestData).Value != 3 {
		t.Fatalf("expected path3 value 3, got %v", v)
	}
	if v := cache.LookUp(path4); v == nil || v.(arenaTestData).Value != 4 {
		t.Fatalf("expected path4 value 4, got %v", v)
	}
}

func TestArenaRadixCache_EraseRootValue(t *testing.T) {
	cache := NewArenaRadixCache(1000, WithInvariantChecking(true))

	_, _ = cache.Insert("", arenaTestData{Value: 100, DataSize: 10})
	_, _ = cache.Insert("child", arenaTestData{Value: 200, DataSize: 20})

	erased := cache.Erase("")
	if erased == nil || erased.(arenaTestData).Value != 100 {
		t.Fatalf("expected erased root value 100, got %v", erased)
	}

	if v := cache.LookUp(""); v != nil {
		t.Fatalf("expected root nil after erase, got %v", v)
	}
	if v := cache.LookUp("child"); v == nil || v.(arenaTestData).Value != 200 {
		t.Fatalf("expected child value 200 preserved, got %v", v)
	}
}

func TestArenaRadixCache_CheckInvariants_PanicScenarios(t *testing.T) {
	t.Run("CurrentSizeExceedsMaxSize", func(t *testing.T) {
		c := NewArenaRadixCache(10).(*arenaRadix)
		c.currentSize = 20
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on currentSize > maxSize")
			}
		}()
		c.checkInvariants()
	})

	t.Run("CorruptLRULinks", func(t *testing.T) {
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		_, _ = c.Insert("k2", arenaTestData{Value: 2, DataSize: 10})
		secondID := c.nodes[c.head].next
		c.nodes[secondID].prev = nilNode
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on corrupt prev pointer")
			}
		}()
		c.checkInvariants()
	})

	t.Run("CorruptTreeParent", func(t *testing.T) {
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("a/b", arenaTestData{Value: 1, DataSize: 10})
		childID := c.nodes[c.root].child
		if childID != nilNode {
			c.nodes[childID].parent = nilNode
		}
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on corrupt parent pointer")
			}
		}()
		c.checkInvariants()
	})

	t.Run("SizeSumMismatch", func(t *testing.T) {
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		c.currentSize += 1
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on size sum mismatch")
			}
		}()
		c.checkInvariants()
	})

	t.Run("CompactnessViolation", func(t *testing.T) {
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("ab", arenaTestData{Value: 1, DataSize: 10})
		_, _ = c.Insert("ac", arenaTestData{Value: 2, DataSize: 10})
		childID := c.nodes[c.root].child
		if childID != nilNode && c.nodes[childID].value == nil {
			firstGrandchild := c.nodes[childID].child
			if firstGrandchild != nilNode {
				c.nodes[firstGrandchild].sibling = nilNode
			}
		}
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on intermediate node compactness violation")
			}
		}()
		c.checkInvariants()
	})
}

func TestArenaRadixCache_ModeratePressureLosslessCompaction(t *testing.T) {
	// Arrange
	pressure := 0.10
	c := NewArenaRadixCache(
		20000,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 { return pressure }),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
	).(*arenaRadix)

	const totalKeys = 1000
	const survivingStart = 900 // Keep keys 900..999 (100 keys)
	for i := range totalKeys {
		key := fmt.Sprintf("bucket_%02d/dir_%02d/sub_%02d/obj_%04d.bin", i%10, (i/10)%10, (i/100)%10, i)
		_, err := c.Insert(key, arenaTestData{Value: int64(i), DataSize: 10})
		require.NoError(t, err)
	}

	peakNodeLen := len(c.nodes)
	peakNodeCap := cap(c.nodes)
	require.Greater(t, peakNodeLen, 1000)

	for i := range survivingStart {
		key := fmt.Sprintf("bucket_%02d/dir_%02d/sub_%02d/obj_%04d.bin", i%10, (i/10)%10, (i/100)%10, i)
		require.NotNil(t, c.Erase(key))
	}
	require.NotEqual(t, nilNode, c.freeHead)
	require.Greater(t, c.freeCount, uint32(0))
	require.Equal(t, peakNodeLen, len(c.nodes))

	// Act
	pressure = 0.80
	evicted := c.EvaluateMemoryPressure()

	// Assert
	assert.Empty(t, evicted)
	assert.Equal(t, nilNode, c.freeHead)
	assert.Equal(t, uint32(0), c.freeCount)
	assert.Equal(t, len(c.nodes), cap(c.nodes))
	assert.Less(t, cap(c.nodes), peakNodeCap)
	assert.Equal(t, 100, c.len)
	assert.Equal(t, uint64(1000), c.currentSize)

	for i := survivingStart; i < totalKeys; i++ {
		key := fmt.Sprintf("bucket_%02d/dir_%02d/sub_%02d/obj_%04d.bin", i%10, (i/10)%10, (i/100)%10, i)
		v := c.LookUpWithoutChangingOrder(key)
		require.NotNil(t, v)
		assert.Equal(t, int64(i), v.(arenaTestData).Value)
	}

	pressure = 0.10
	_, err := c.Insert("filler", arenaTestData{Value: 999999, DataSize: 19000})
	require.NoError(t, err)
	for expectedID := survivingStart; expectedID < totalKeys; expectedID++ {
		ev, err := c.Insert(fmt.Sprintf("trigger_%d", expectedID), arenaTestData{Value: -1, DataSize: 10})
		require.NoError(t, err)
		require.Len(t, ev, 1)
		assert.Equal(t, int64(expectedID), ev[0].(arenaTestData).Value)
	}
}

func TestArenaRadixCache_CriticalPressureLRUShedding(t *testing.T) {
	// Arrange
	pressure := 0.10
	c := NewArenaRadixCache(
		1000,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 { return pressure }),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.40),
	).(*arenaRadix)

	for i := range 100 {
		key := fmt.Sprintf("dir_%d/item_%03d", i%5, i)
		_, err := c.Insert(key, arenaTestData{Value: int64(i), DataSize: 10})
		require.NoError(t, err)
	}

	// Act
	pressure = 0.95
	evicted := c.EvaluateMemoryPressure()

	// Assert
	require.Len(t, evicted, 60)
	for i, ev := range evicted {
		assert.Equal(t, int64(i), ev.(arenaTestData).Value)
	}
	assert.Equal(t, uint64(400), c.currentSize)
	assert.Equal(t, 40, c.len)
	assert.Equal(t, nilNode, c.freeHead)
	assert.Equal(t, uint32(0), c.freeCount)
	assert.Equal(t, len(c.nodes), cap(c.nodes))

	for i := range 60 {
		key := fmt.Sprintf("dir_%d/item_%03d", i%5, i)
		assert.Nil(t, c.LookUpWithoutChangingOrder(key))
	}
	for i := 60; i < 100; i++ {
		key := fmt.Sprintf("dir_%d/item_%03d", i%5, i)
		v := c.LookUpWithoutChangingOrder(key)
		require.NotNil(t, v)
		assert.Equal(t, int64(i), v.(arenaTestData).Value)
	}
}

func TestArenaRadixCache_AutomaticPressureTriggersAndReentrancy(t *testing.T) {
	// Arrange
	pressure := 0.10
	var cacheRef Cache
	reentrantReads := 0

	c := NewArenaRadixCache(
		200,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			if cacheRef != nil {
				_ = cacheRef.LookUpWithoutChangingOrder("probe_key")
				// Also verify mutating re-entrancy is guarded against infinite recursion.
				_, _ = cacheRef.Insert("reentrant_probe", arenaTestData{Value: 1, DataSize: 0})
				_ = cacheRef.Erase("reentrant_probe")
				reentrantReads++
			}
			return pressure
		}),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
	).(*arenaRadix)
	cacheRef = c

	for i := range 20 {
		_, err := c.Insert(fmt.Sprintf("k_%02d", i), arenaTestData{Value: int64(i), DataSize: 10})
		require.NoError(t, err)
	}
	for i := range 9 {
		require.NotNil(t, c.Erase(fmt.Sprintf("k_%02d", i)))
	}
	require.NotEqual(t, nilNode, c.freeHead)

	// Act 1: Automatic Tier 1 compaction on Erase of existing key under moderate pressure.
	pressure = 0.80
	require.NotNil(t, c.Erase("k_09"))

	// Assert 1
	assert.Equal(t, nilNode, c.freeHead)
	assert.Equal(t, len(c.nodes), cap(c.nodes))
	assert.Equal(t, uint64(100), c.currentSize)

	// Act 2: Automatic Tier 2 shedding on Insert under critical pressure.
	// maxSize = 200, retention = 0.50 -> targetSize = 100 bytes.
	// Before insert: 10 entries (100 bytes). Insert "new_mru" (60 bytes) -> 160 bytes -> sheds 6 oldest entries (60 bytes) down to 100 bytes.
	pressure = 0.95
	evicted, err := c.Insert("new_mru", arenaTestData{Value: 999, DataSize: 60})

	// Assert 2
	require.NoError(t, err)
	assert.Len(t, evicted, 6)
	assert.Equal(t, uint64(100), c.currentSize)
	assert.Equal(t, nilNode, c.freeHead)
	assert.NotNil(t, c.LookUpWithoutChangingOrder("new_mru"))
	assert.Greater(t, reentrantReads, 0)
}

func TestArenaRadixCache_CompactionAndSheddingEdgeCases(t *testing.T) {
	t.Run("EmptyCacheCompactionAndShedding", func(t *testing.T) {
		// Arrange
		pressure := 0.95
		c := NewArenaRadixCache(
			100,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return pressure }),
		).(*arenaRadix)

		// Act
		c.Compact()
		ev := c.EvaluateMemoryPressure()

		// Assert
		assert.Empty(t, ev)
		assert.Equal(t, 1, len(c.nodes))
		assert.Equal(t, 1, cap(c.nodes))
		assert.Equal(t, uint32(0), c.root)
		assert.Equal(t, nilNode, c.freeHead)
	})

	t.Run("SingleLargeEntryShedding", func(t *testing.T) {
		// Arrange
		pressure := 0.0
		c := NewArenaRadixCache(
			100,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix)
		_, err := c.Insert("only_item", arenaTestData{Value: 42, DataSize: 80})
		require.NoError(t, err)

		// Act
		pressure = 0.95
		ev := c.EvaluateMemoryPressure()

		// Assert
		require.Len(t, ev, 1)
		assert.Equal(t, int64(42), ev[0].(arenaTestData).Value)
		assert.Equal(t, 0, c.len)
		assert.Equal(t, uint64(0), c.currentSize)
		assert.Equal(t, 1, len(c.nodes))
		assert.Equal(t, 1, cap(c.nodes))
	})

	t.Run("EmptyStringKeyAtRoot", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(100, WithInvariantChecking(true)).(*arenaRadix)
		_, err := c.Insert("", arenaTestData{Value: 777, DataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert("a/b/c", arenaTestData{Value: 888, DataSize: 10})
		require.NoError(t, err)
		_ = c.Erase("a/b/c")

		// Act
		c.Compact()
		v := c.LookUpWithoutChangingOrder("")

		// Assert
		require.NotNil(t, v)
		assert.Equal(t, int64(777), v.(arenaTestData).Value)
	})

	t.Run("ZeroSizeEntriesTerminationAndFullEvictionWhenRetentionZero", func(t *testing.T) {
		// Arrange
		pressure := 0.0
		c := NewArenaRadixCache(
			100,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.0),
		).(*arenaRadix)

		for i := range 5 {
			_, err := c.Insert(fmt.Sprintf("zero_%d", i), arenaTestData{Value: int64(i), DataSize: 0})
			require.NoError(t, err)
		}
		_, err := c.Insert("nonzero", arenaTestData{Value: 99, DataSize: 40})
		require.NoError(t, err)

		// Act
		pressure = 0.95
		ev := c.EvaluateMemoryPressure()

		// Assert
		assert.Len(t, ev, 6)
		assert.Equal(t, uint64(0), c.currentSize)
		assert.Equal(t, 0, c.len)
	})
}

type deadlockProbeValue struct {
	size   uint64
	onSize func()
}

func (v deadlockProbeValue) Size() uint64 {
	if v.onSize != nil {
		v.onSize()
	}
	return v.size
}

func TestArenaRadixCache_AdversarialAuditRegressions(t *testing.T) {
	t.Run("InsertIntoEmptyOrUnderTargetCacheDoesNotSelfEvict", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(
			1000,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return 0.95 }),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix)

		// Act
		evicted, err := c.Insert("first_key", arenaTestData{Value: 42, DataSize: 10})
		lookedUp := c.LookUpWithoutChangingOrder("first_key")

		// Assert
		require.NoError(t, err)
		assert.Empty(t, evicted)
		require.NotNil(t, lookedUp)
		assert.Equal(t, int64(42), lookedUp.(arenaTestData).Value)
		assert.Equal(t, uint64(10), c.currentSize)
		assert.Equal(t, 1, c.len)
	})

	t.Run("SustainedCriticalPressureConvergesIdempotentlyWithoutCompoundDecay", func(t *testing.T) {
		// Arrange
		pressure := 0.0
		c := NewArenaRadixCache(
			1000,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix)
		for i := range 100 {
			_, err := c.Insert(fmt.Sprintf("k-%03d", i), arenaTestData{Value: int64(i), DataSize: 10})
			require.NoError(t, err)
		}
		require.Equal(t, uint64(1000), c.currentSize)
		pressure = 0.95

		// Act: Evaluate critical pressure 10 times consecutively.
		firstEvicted := c.EvaluateMemoryPressure()
		for range 9 {
			subsequentEvicted := c.EvaluateMemoryPressure()
			assert.Empty(t, subsequentEvicted)
		}

		// Assert: Stabilizes idempotently at maxSize * retentionRatio (500 bytes, 50 entries).
		assert.Len(t, firstEvicted, 50)
		assert.Equal(t, uint64(500), c.currentSize)
		assert.Equal(t, 50, c.len)
	})

	t.Run("KeyMissesOnEraseUpdateSizeAndPrefixDoNotEvictLiveEntries", func(t *testing.T) {
		// Arrange
		pressure := 0.0
		c := NewArenaRadixCache(
			1000,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix)
		for i := range 10 {
			_, err := c.Insert(fmt.Sprintf("k-%d", i), arenaTestData{Value: int64(i), DataSize: 10})
			require.NoError(t, err)
		}
		pressure = 0.95

		// Act
		erased := c.Erase("absent_key")
		err := c.UpdateSize("absent_key", 10)
		c.EraseEntriesWithGivenPrefix("absent_prefix/")

		// Assert
		assert.Nil(t, erased)
		assert.ErrorIs(t, err, ErrEntryNotExist)
		assert.Equal(t, 10, c.len)
		assert.Equal(t, uint64(100), c.currentSize)
	})

	t.Run("CompactFastPathPerformsZeroAllocationsWhenAlreadyCompact", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(10000, WithInvariantChecking(true)).(*arenaRadix)
		for i := range 200 {
			_, err := c.Insert(fmt.Sprintf("key-%03d", i), arenaTestData{Value: int64(i), DataSize: 10})
			require.NoError(t, err)
		}
		c.Compact()
		require.Equal(t, nilNode, c.freeHead)
		require.Equal(t, len(c.nodes), cap(c.nodes))
		ptrBefore := &c.nodes[0]

		// Act
		allocsPerRun := testing.AllocsPerRun(20, func() {
			c.Compact()
		})
		ptrAfter := &c.nodes[0]

		// Assert
		assert.Equal(t, 0.0, allocsPerRun)
		assert.Same(t, ptrBefore, ptrAfter)
	})

	t.Run("UpdateWithoutChangingOrderSamplesValueSizeOutsideLock", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(100, WithInvariantChecking(true)).(*arenaRadix)
		_, err := c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		require.NoError(t, err)

		readBlockedInsideSize := true
		probe := deadlockProbeValue{
			size: 10,
			onSize: func() {
				if c.mu.TryRLock() {
					c.mu.RUnlock()
					_ = c.LookUpWithoutChangingOrder("k1")
					readBlockedInsideSize = false
				}
			},
		}

		// Act
		err = c.UpdateWithoutChangingOrder("k1", probe)

		// Assert
		require.NoError(t, err)
		assert.False(t, readBlockedInsideSize)
	})

	t.Run("FNV1aHashCollisionPreservedAndHealedAcrossCompaction", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(1000, WithInvariantChecking(true)).(*arenaRadix)
		_, err := c.Insert("alpha/one", arenaTestData{Value: 101, DataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert("alpha/two", arenaTestData{Value: 202, DataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert("beta/three", arenaTestData{Value: 303, DataSize: 10})
		require.NoError(t, err)
		_ = c.Erase("alpha/two")

		// Simulate hash collision deletion removing "alpha/one" from nodeMap.
		delete(c.nodeMap, hashString("alpha/one"))
		_, inMapBefore := c.nodeMap[hashString("alpha/one")]
		require.False(t, inMapBefore)

		// Act
		c.Compact()

		// Assert: both trie lookup and O(1) nodeMap entry are healed after compaction.
		_, inMapAfter := c.nodeMap[hashString("alpha/one")]
		assert.True(t, inMapAfter)
		v1 := c.LookUpWithoutChangingOrder("alpha/one")
		v3 := c.LookUpWithoutChangingOrder("beta/three")
		require.NotNil(t, v1)
		require.NotNil(t, v3)
		assert.Equal(t, int64(101), v1.(arenaTestData).Value)
		assert.Equal(t, int64(303), v3.(arenaTestData).Value)
	})
}

func TestArenaRadixCache_PrincipalReviewFixes(t *testing.T) {
	t.Run("F1_NoCompactionThrashingOnSteadyStateInsertsUnderPressure", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(
			2000,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return 0.95 }),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix)

		// Act: Insert 100 entries (1000 bytes == targetSize) under critical pressure.
		allocsPerInsert := testing.AllocsPerRun(1, func() {
			for i := range 100 {
				_, err := c.Insert(fmt.Sprintf("k-%04d", i), arenaTestData{Value: int64(i), DataSize: 10})
				require.NoError(t, err)
			}
		})

		// Assert: Without O(N^2) compaction thrashing on every Insert, 100 inserts allocate ~252 objects total (vs > 2000 previously).
		assert.Less(t, allocsPerInsert, 350.0)
		assert.Equal(t, 100, c.len)
		assert.Equal(t, uint64(1000), c.currentSize)
	})

	t.Run("F2_ConcurrentPressureSampleReturnsCachedPressureInsteadOfZero", func(t *testing.T) {
		// Arrange
		shouldBlock := false
		inCallback := make(chan struct{})
		releaseCallback := make(chan struct{})
		c := NewArenaRadixCache(
			1000,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				if shouldBlock {
					close(inCallback)
					<-releaseCallback
				}
				return 0.95
			}),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix)

		for i := range 10 {
			_, err := c.Insert(fmt.Sprintf("item-%d", i), arenaTestData{Value: int64(i), DataSize: 100})
			require.NoError(t, err)
		}

		// Act: Block Goroutine A inside PressureFunc while Goroutine B calls samplePressureFresh().
		shouldBlock = true
		doneA := make(chan struct{})
		go func() {
			_ = c.samplePressureFresh()
			close(doneA)
		}()
		<-inCallback

		concurrentSample := c.samplePressureFresh()
		close(releaseCallback)
		<-doneA

		// Assert: Concurrent caller receives cached 0.95 instead of false 0.0.
		assert.Equal(t, 0.95, concurrentSample)
	})

	t.Run("F3_ZeroRetentionRatioDoesNotSelfEvictNewlyInsertedKey", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(
			1000,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return 0.95 }),
			WithEvictionRetentionRatio(0.0),
		).(*arenaRadix)

		_, err := c.Insert("old_key", arenaTestData{Value: 1, DataSize: 100})
		require.NoError(t, err)

		// Act: Insert "new_key" under critical pressure with retention == 0.0.
		evicted, err := c.Insert("new_key", arenaTestData{Value: 2, DataSize: 100})

		// Assert: "old_key" is evicted, but "new_key" is preserved.
		require.NoError(t, err)
		require.Len(t, evicted, 1)
		assert.Equal(t, int64(1), evicted[0].(arenaTestData).Value)
		assert.Nil(t, c.LookUpWithoutChangingOrder("old_key"))
		assert.NotNil(t, c.LookUpWithoutChangingOrder("new_key"))

		// Explicit EvaluateMemoryPressure still flushes 100% of entries when retention == 0.0.
		flushed := c.EvaluateMemoryPressure()
		require.Len(t, flushed, 1)
		assert.Equal(t, 0, c.len)
	})

	t.Run("F4_PostReclamationImmediatelyRefreshesAmortizedPressure", func(t *testing.T) {
		// Arrange: Simulate default amortized sampling path (hasCustomPressureFunc = false).
		pressure := 0.95
		c := NewArenaRadixCache(1000, WithInvariantChecking(true)).(*arenaRadix)
		c.options.PressureFunc = func() float64 { return pressure }
		c.options.hasCustomPressureFunc = false

		for i := range 10 {
			_, err := c.Insert(fmt.Sprintf("k-%d", i), arenaTestData{Value: int64(i), DataSize: 100})
			require.NoError(t, err)
		}

		// Act: After reclamation resolves pressure to 0.20, the very next samplePressure() refreshes immediately.
		pressure = 0.20
		sampled := c.samplePressure()

		// Assert
		assert.Equal(t, 0.20, sampled)
	})

	t.Run("F5_And_F10_MapCacheAndRadixCacheSafeSizeCallbackAndPressureAwareCache", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			fn   func(uint64, ...Option) Cache
		}{
			{"MapCache", NewMapCache},
			{"RadixCache", NewRadixCache},
		} {
			t.Run(tc.name, func(t *testing.T) {
				pressure := 0.10
				cache := tc.fn(
					100,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 { return pressure }),
					WithEvictionRetentionRatio(0.50),
				)
				for i := range 10 {
					_, err := cache.Insert(fmt.Sprintf("k-%d", i), arenaTestData{Value: int64(i), DataSize: 10})
					require.NoError(t, err)
				}

				// F5: Re-entrant LookUpWithoutChangingOrder inside ValueType.Size() does not deadlock.
				err := cache.UpdateWithoutChangingOrder("k-0", deadlockProbeValue{
					size: 10,
					onSize: func() {
						_ = cache.LookUpWithoutChangingOrder("k-0")
					},
				})
				require.NoError(t, err)

				// F10: Implements PressureAwareCache.
				pac, ok := cache.(PressureAwareCache)
				require.True(t, ok)
				pac.Compact()
				pressure = 0.95
				evicted := pac.EvaluateMemoryPressure()
				assert.Len(t, evicted, 5)
			})
		}
	})

	t.Run("F6_UpdateSizeUnderCriticalPressureTriggersTier2Shedding", func(t *testing.T) {
		// Arrange
		pressure := 0.10
		c := NewArenaRadixCache(
			1000,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix)
		for i := range 8 {
			_, err := c.Insert(fmt.Sprintf("k-%d", i), arenaTestData{Value: int64(i), DataSize: 100})
			require.NoError(t, err)
		}
		require.Equal(t, uint64(800), c.currentSize)

		// Act: Grow k-7 by 100 bytes under critical pressure (0.95).
		pressure = 0.95
		err := c.UpdateSize("k-7", 100)

		// Assert: Sheds down to targetSize (500 bytes) while keeping k-7.
		require.NoError(t, err)
		assert.LessOrEqual(t, c.currentSize, uint64(500))
		assert.NotNil(t, c.LookUpWithoutChangingOrder("k-7"))
	})

	t.Run("F7_ComputeTargetSizeBoundsAndZeroSizeEntryShedding", func(t *testing.T) {
		// MaxUint64 does not overflow float64 -> uint64 conversion.
		assert.Equal(t, uint64(math.MaxUint64), computeTargetSize(math.MaxUint64, 1.0))
		assert.Greater(t, computeTargetSize(math.MaxUint64, 0.5), uint64(math.MaxUint64/4))
		// maxSize == 1 with retention > 0 clamps to 1 instead of truncating to 0.
		assert.Equal(t, uint64(1), computeTargetSize(1, 0.50))

		// Zero-size entries (DataSize == 0) are shed down to retention ratio under critical pressure.
		pressure := 0.10
		c := NewArenaRadixCache(
			100,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix)
		for i := range 10 {
			_, err := c.Insert(fmt.Sprintf("zero-%d", i), arenaTestData{Value: int64(i), DataSize: 0})
			require.NoError(t, err)
		}
		pressure = 0.95
		evicted := c.EvaluateMemoryPressure()
		assert.Len(t, evicted, 5)
		assert.Equal(t, 5, c.len)
	})
}

func TestArenaRadixCache_CheckInvariants_ExtendedChecks(t *testing.T) {
	t.Run("NodeMapOutOfBounds", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		c.nodeMap[hashString("k1")] = 999

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("NodeMapNilValue", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		c.nodeMap[hashString("k1")] = c.root

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("NodeMapHashMismatch", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		id := c.nodeMap[hashString("k1")]
		c.nodeMap[12345] = id

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("LeakedNodeAccountingMismatch", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		c.nodes = append(c.nodes, arenaRadixNode{
			parent:  nilNode,
			child:   nilNode,
			sibling: nilNode,
			prev:    nilNode,
			next:    nilNode,
		})

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("FreeCountDriftMismatch", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		_ = c.Erase("k1")
		c.freeCount = 999

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})
}
