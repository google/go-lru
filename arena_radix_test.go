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

package lrus

import (
	"errors"
	"fmt"
	"testing"
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
	pressure := 0.10
	c := NewArenaRadixCache(
		20000,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 { return pressure }),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
	).(*arenaRadix)

	// 1. Insert 1,000 hierarchical keys across shared prefixes.
	const totalKeys = 1000
	const survivingStart = 900 // Keep keys 900..999 (100 keys)
	for i := range totalKeys {
		key := fmt.Sprintf("bucket_%02d/dir_%02d/sub_%02d/obj_%04d.bin", i%10, (i/10)%10, (i/100)%10, i)
		_, err := c.Insert(key, arenaTestData{Value: int64(i), DataSize: 10})
		if err != nil {
			t.Fatalf("failed inserting key %d: %v", i, err)
		}
	}

	peakNodeLen := len(c.nodes)
	peakNodeCap := cap(c.nodes)
	if peakNodeLen <= 1000 {
		t.Fatalf("expected >1000 nodes with routing splits, got %d", peakNodeLen)
	}

	// 2. Erase first 900 keys under low pressure (accumulating a large free-list).
	for i := range survivingStart {
		key := fmt.Sprintf("bucket_%02d/dir_%02d/sub_%02d/obj_%04d.bin", i%10, (i/10)%10, (i/100)%10, i)
		if v := c.Erase(key); v == nil {
			t.Fatalf("expected key %d to be erased", i)
		}
	}

	if c.freeHead == nilNode {
		t.Fatalf("expected non-empty freeHead before compaction")
	}
	if len(c.nodes) != peakNodeLen {
		t.Fatalf("expected uncompacted nodes len %d to match peak %d", len(c.nodes), peakNodeLen)
	}

	// 3. Raise pressure to moderate (0.80 >= 0.75) and evaluate memory pressure.
	pressure = 0.80
	evicted := c.EvaluateMemoryPressure()
	if len(evicted) != 0 {
		t.Fatalf("expected zero evictions under moderate pressure, got %d", len(evicted))
	}

	// 4. Verify arena slice and map compaction post-conditions.
	if c.freeHead != nilNode {
		t.Fatalf("expected freeHead == nilNode after compaction, got %d", c.freeHead)
	}
	if len(c.nodes) != cap(c.nodes) {
		t.Fatalf("expected len(nodes) == cap(nodes) after compaction, got len=%d cap=%d", len(c.nodes), cap(c.nodes))
	}
	if cap(c.nodes) >= peakNodeCap {
		t.Fatalf("expected compacted cap(nodes) (%d) < peakNodeCap (%d)", cap(c.nodes), peakNodeCap)
	}
	if c.len != 100 || c.currentSize != 1000 {
		t.Fatalf("expected 100 live entries (size 1000), got len=%d size=%d", c.len, c.currentSize)
	}

	// 5. Verify 100% of surviving keys return exact values without altering LRU order.
	for i := survivingStart; i < totalKeys; i++ {
		key := fmt.Sprintf("bucket_%02d/dir_%02d/sub_%02d/obj_%04d.bin", i%10, (i/10)%10, (i/100)%10, i)
		v := c.LookUpWithoutChangingOrder(key)
		if v == nil || v.(arenaTestData).Value != int64(i) {
			t.Fatalf("expected surviving key %d to have value %d, got %v", i, i, v)
		}
	}

	// 6. Verify exact LRU eviction order of surviving keys (900 is oldest, 999 is newest).
	pressure = 0.10
	// Fill remaining capacity (20000 - 1000 = 19000 bytes).
	_, err := c.Insert("filler", arenaTestData{Value: 999999, DataSize: 19000})
	if err != nil {
		t.Fatalf("failed inserting filler: %v", err)
	}
	// Each subsequent 10-byte insert must evict keys 900, 901, ..., 999 in exact LRU order.
	for expectedID := survivingStart; expectedID < totalKeys; expectedID++ {
		ev, err := c.Insert(fmt.Sprintf("trigger_%d", expectedID), arenaTestData{Value: -1, DataSize: 10})
		if err != nil {
			t.Fatalf("trigger insert failed at %d: %v", expectedID, err)
		}
		if len(ev) != 1 || ev[0].(arenaTestData).Value != int64(expectedID) {
			t.Fatalf("expected evicted value %d at step %d, got %v", expectedID, expectedID, ev)
		}
	}
}

func TestArenaRadixCache_CriticalPressureLRUShedding(t *testing.T) {
	pressure := 0.10
	c := NewArenaRadixCache(
		1000,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 { return pressure }),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.40),
	).(*arenaRadix)

	// Insert 100 entries of size 10 (total 1000 bytes).
	// Entry 0 is LRU tail; Entry 99 is MRU head.
	for i := range 100 {
		key := fmt.Sprintf("dir_%d/item_%03d", i%5, i)
		_, err := c.Insert(key, arenaTestData{Value: int64(i), DataSize: 10})
		if err != nil {
			t.Fatalf("insert failed for %d: %v", i, err)
		}
	}

	// Trigger critical pressure (0.95 >= 0.90): should shed down to 40% of 1000 = 400 bytes (evicting 60 items: 0..59).
	pressure = 0.95
	evicted := c.EvaluateMemoryPressure()

	if len(evicted) != 60 {
		t.Fatalf("expected exactly 60 entries evicted under critical pressure, got %d", len(evicted))
	}
	for i, ev := range evicted {
		if ev.(arenaTestData).Value != int64(i) {
			t.Fatalf("expected evicted[%d] to be oldest entry %d, got %d", i, i, ev.(arenaTestData).Value)
		}
	}

	// Verify post-shedding state and compaction post-conditions.
	if c.currentSize != 400 || c.len != 40 {
		t.Fatalf("expected currentSize=400 and len=40 after shedding, got currentSize=%d len=%d", c.currentSize, c.len)
	}
	if c.freeHead != nilNode {
		t.Fatalf("expected freeHead == nilNode after critical shedding + compaction")
	}
	if len(c.nodes) != cap(c.nodes) {
		t.Fatalf("expected len(nodes) == cap(nodes) after shedding + compaction")
	}

	// Verify entries 0..59 are gone and entries 60..99 are intact.
	for i := range 60 {
		key := fmt.Sprintf("dir_%d/item_%03d", i%5, i)
		if v := c.LookUpWithoutChangingOrder(key); v != nil {
			t.Fatalf("expected evicted key %s to be absent, got %v", key, v)
		}
	}
	for i := 60; i < 100; i++ {
		key := fmt.Sprintf("dir_%d/item_%03d", i%5, i)
		if v := c.LookUpWithoutChangingOrder(key); v == nil || v.(arenaTestData).Value != int64(i) {
			t.Fatalf("expected retained MRU key %s to have value %d, got %v", key, i, v)
		}
	}
}

func TestArenaRadixCache_AutomaticPressureTriggersAndReentrancy(t *testing.T) {
	pressure := 0.10
	var cacheRef Cache
	reentrantReads := 0

	// Custom PressureFunc that calls LookUpWithoutChangingOrder on the cache itself
	// to prove lock-free sampling prevents re-entrant RWMutex deadlocks.
	c := NewArenaRadixCache(
		1000,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			if cacheRef != nil {
				_ = cacheRef.LookUpWithoutChangingOrder("probe_key")
				reentrantReads++
			}
			return pressure
		}),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
	).(*arenaRadix)
	cacheRef = c

	// Populate 20 keys of size 10 (total 200 bytes) and erase 10 of them under low pressure.
	for i := range 20 {
		_, _ = c.Insert(fmt.Sprintf("k_%02d", i), arenaTestData{Value: int64(i), DataSize: 10})
	}
	for i := range 10 {
		_ = c.Erase(fmt.Sprintf("k_%02d", i))
	}
	if c.freeHead == nilNode {
		t.Fatalf("expected non-empty free list before automatic moderate compaction")
	}

	// 1. Automatic Tier 1 compaction on Erase under moderate pressure.
	pressure = 0.80
	_ = c.Erase("non_existent_key")
	if c.freeHead != nilNode || len(c.nodes) != cap(c.nodes) {
		t.Fatalf("expected Erase under moderate pressure to compact arena")
	}

	// 2. Automatic Tier 2 shedding on Insert under critical pressure.
	// Before insert: 10 entries (100 bytes). Insert 1 entry (10 bytes) -> 110 bytes.
	// Retention 50% of 110 bytes = 55 bytes -> sheds down to 50 bytes (5 entries).
	pressure = 0.95
	evicted, err := c.Insert("new_mru", arenaTestData{Value: 999, DataSize: 10})
	if err != nil {
		t.Fatalf("Insert under critical pressure failed: %v", err)
	}
	if len(evicted) != 6 {
		t.Fatalf("expected 6 LRU entries evicted during Insert under critical pressure, got %d", len(evicted))
	}
	if c.currentSize != 50 || c.freeHead != nilNode {
		t.Fatalf("expected currentSize=50 and freeHead==nilNode, got size=%d freeHead=%d", c.currentSize, c.freeHead)
	}
	if reentrantReads == 0 {
		t.Fatalf("expected reentrant PressureFunc callback to have executed")
	}
}

func TestArenaRadixCache_CompactionAndSheddingEdgeCases(t *testing.T) {
	t.Run("EmptyCacheCompactionAndShedding", func(t *testing.T) {
		pressure := 0.95
		c := NewArenaRadixCache(
			100,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return pressure }),
		).(*arenaRadix)

		c.Compact()
		ev := c.EvaluateMemoryPressure()
		if len(ev) != 0 {
			t.Fatalf("expected 0 evictions on empty cache, got %d", len(ev))
		}
		if len(c.nodes) != 1 || cap(c.nodes) != 1 || c.root != 0 || c.freeHead != nilNode {
			t.Fatalf("expected single root node after empty cache compaction, got len=%d cap=%d root=%d", len(c.nodes), cap(c.nodes), c.root)
		}
	})

	t.Run("SingleLargeEntryShedding", func(t *testing.T) {
		pressure := 0.95
		c := NewArenaRadixCache(
			100,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix)

		pressure = 0.0
		_, _ = c.Insert("only_item", arenaTestData{Value: 42, DataSize: 80})

		pressure = 0.95
		ev := c.EvaluateMemoryPressure()
		if len(ev) != 1 || ev[0].(arenaTestData).Value != 42 {
			t.Fatalf("expected single large entry to be shed, got %v", ev)
		}
		if c.len != 0 || c.currentSize != 0 || len(c.nodes) != 1 || cap(c.nodes) != 1 {
			t.Fatalf("expected cache reduced to root node only, got len=%d size=%d nodes=%d", c.len, c.currentSize, len(c.nodes))
		}
	})

	t.Run("EmptyStringKeyAtRoot", func(t *testing.T) {
		c := NewArenaRadixCache(100, WithInvariantChecking(true)).(*arenaRadix)
		_, _ = c.Insert("", arenaTestData{Value: 777, DataSize: 10})
		_, _ = c.Insert("a/b/c", arenaTestData{Value: 888, DataSize: 10})
		_ = c.Erase("a/b/c")

		c.Compact()
		if v := c.LookUpWithoutChangingOrder(""); v == nil || v.(arenaTestData).Value != 777 {
			t.Fatalf("expected root empty key value 777 preserved across compaction, got %v", v)
		}
	})

	t.Run("ZeroSizeEntriesTermination", func(t *testing.T) {
		pressure := 0.0
		c := NewArenaRadixCache(
			100,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.25),
		).(*arenaRadix)

		for i := range 5 {
			_, _ = c.Insert(fmt.Sprintf("zero_%d", i), arenaTestData{Value: int64(i), DataSize: 0})
		}
		_, _ = c.Insert("nonzero", arenaTestData{Value: 99, DataSize: 40})

		pressure = 0.95
		ev := c.EvaluateMemoryPressure()
		if c.currentSize > 10 {
			t.Fatalf("expected currentSize <= 10 after shedding, got %d (evicted=%d)", c.currentSize, len(ev))
		}
	})
}

func TestArenaRadixCache_CheckInvariants_ExtendedChecks(t *testing.T) {
	t.Run("NodeMapOutOfBounds", func(t *testing.T) {
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		c.nodeMap[hashString("k1")] = 999
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on nodeMap out-of-bounds index")
			}
		}()
		c.checkInvariants()
	})

	t.Run("NodeMapNilValue", func(t *testing.T) {
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		c.nodeMap[hashString("k1")] = c.root // c.root has nil value
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on nodeMap pointing to nil-value node")
			}
		}()
		c.checkInvariants()
	})

	t.Run("NodeMapHashMismatch", func(t *testing.T) {
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		id := c.nodeMap[hashString("k1")]
		c.nodeMap[12345] = id
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on nodeMap hash mismatch")
			}
		}()
		c.checkInvariants()
	})

	t.Run("LeakedNodeAccountingMismatch", func(t *testing.T) {
		c := NewArenaRadixCache(50).(*arenaRadix)
		_, _ = c.Insert("k1", arenaTestData{Value: 1, DataSize: 10})
		// Append an unlinked orphan node that is neither in tree nor in free-list.
		c.nodes = append(c.nodes, arenaRadixNode{
			parent:  nilNode,
			child:   nilNode,
			sibling: nilNode,
			prev:    nilNode,
			next:    nilNode,
		})
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on liveTreeNodes + freeListCount != len(nodes)")
			}
		}()
		c.checkInvariants()
	})
}

