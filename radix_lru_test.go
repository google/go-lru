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
	"testing"
)

const (
	radixTestMaxSize        = 50
	radixTestOperationCount = 100
)

type radixTestData struct {
	value    int64
	dataSize uint64
}

func (d radixTestData) Size() uint64 {
	return d.dataSize
}

func setupRadixCacheTest(t *testing.T) Cache {
	t.Helper()
	return NewRadixCache(radixTestMaxSize, WithInvariantChecking(true))
}

func insertAndAssertRadix(t *testing.T, cache Cache, key string, val ValueType, expectedEvictedValues []int64, expectedErr error) {
	t.Helper()
	evicted, err := cache.Insert(key, val)
	if expectedErr != nil {
		if err == nil {
			t.Fatalf("expected error %v, got nil", expectedErr)
		}
		if !errors.Is(err, expectedErr) && err.Error() != expectedErr.Error() {
			t.Fatalf("expected error %v, got %v", expectedErr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(expectedEvictedValues) == 0 {
		if len(evicted) != 0 {
			t.Fatalf("expected no evicted values, got %d", len(evicted))
		}
		return
	}

	if len(evicted) != len(expectedEvictedValues) {
		t.Fatalf("expected %d evicted values, got %d", len(expectedEvictedValues), len(evicted))
	}

	for i, exp := range expectedEvictedValues {
		actualData, ok := evicted[i].(radixTestData)
		if !ok {
			// Also support pointer if used
			ptrData, ptrOk := evicted[i].(*radixTestData)
			if ptrOk {
				actualData = *ptrData
			} else {
				t.Fatalf("evicted value %d is not of type radixTestData: %T", i, evicted[i])
			}
		}
		if actualData.value != exp {
			t.Fatalf("evicted[%d] expected value %d, got %d", i, exp, actualData.value)
		}
	}
}

func TestRadixCache_LookUpInEmptyCache(t *testing.T) {
	cache := setupRadixCacheTest(t)
	if v := cache.LookUp(""); v != nil {
		t.Fatalf("expected nil for empty key in empty cache, got %v", v)
	}
	if v := cache.LookUp("taco"); v != nil {
		t.Fatalf("expected nil for non-existent key, got %v", v)
	}
}

func TestRadixCache_InsertNilValue(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "taco", nil, []int64{}, ErrInvalidEntry)
}

func TestRadixCache_InsertEmptyKey(t *testing.T) {
	cache := setupRadixCacheTest(t)

	insertAndAssertRadix(t, cache, "", radixTestData{value: 42, dataSize: 10}, []int64{}, nil)

	lookupVal := cache.LookUp("")
	if lookupVal == nil {
		t.Fatalf("expected value for empty key, got nil")
	}
	if lookupVal.(radixTestData).value != 42 {
		t.Fatalf("expected 42, got %d", lookupVal.(radixTestData).value)
	}
	if cache.LookUp("taco") != nil {
		t.Fatalf("expected nil for taco")
	}
}

func TestRadixCache_LookUpUnknownKey(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "burrito", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "taco", radixTestData{value: 23, dataSize: 8}, []int64{}, nil)

	if cache.LookUp("") != nil {
		t.Fatalf("expected nil for empty key")
	}
	if cache.LookUp("enchilada") != nil {
		t.Fatalf("expected nil for enchilada")
	}
}

func TestRadixCache_FillUpToCapacity(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "burrito", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "taco", radixTestData{value: 26, dataSize: 20}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "enchilada", radixTestData{value: 28, dataSize: 26}, []int64{}, nil)

	if v := cache.LookUp("burrito"); v == nil || v.(radixTestData).value != 23 {
		t.Fatalf("expected 23 for burrito, got %v", v)
	}
	if v := cache.LookUp("taco"); v == nil || v.(radixTestData).value != 26 {
		t.Fatalf("expected 26 for taco, got %v", v)
	}
	if v := cache.LookUp("enchilada"); v == nil || v.(radixTestData).value != 28 {
		t.Fatalf("expected 28 for enchilada, got %v", v)
	}
}

func TestRadixCache_ExpiresLeastRecentlyUsed(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "burrito", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)

	// Least recent.
	insertAndAssertRadix(t, cache, "taco", radixTestData{value: 26, dataSize: 20}, []int64{}, nil)

	// Second most recent.
	insertAndAssertRadix(t, cache, "enchilada", radixTestData{value: 28, dataSize: 26}, []int64{}, nil)

	if v := cache.LookUp("burrito"); v == nil || v.(radixTestData).value != 23 {
		t.Fatalf("expected 23 for burrito, got %v", v)
	}

	// Insert another, should evict taco (value 26).
	insertAndAssertRadix(t, cache, "queso", radixTestData{value: 34, dataSize: 5}, []int64{26}, nil)

	// See what's left.
	if cache.LookUp("taco") != nil {
		t.Fatalf("expected taco to be evicted")
	}
	if v := cache.LookUp("burrito"); v == nil || v.(radixTestData).value != 23 {
		t.Fatalf("expected 23 for burrito, got %v", v)
	}
	if v := cache.LookUp("enchilada"); v == nil || v.(radixTestData).value != 28 {
		t.Fatalf("expected 28 for enchilada, got %v", v)
	}
	if v := cache.LookUp("queso"); v == nil || v.(radixTestData).value != 34 {
		t.Fatalf("expected 34 for queso, got %v", v)
	}
}

func TestRadixCache_Overwrite(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "burrito", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "taco", radixTestData{value: 26, dataSize: 20}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "enchilada", radixTestData{value: 28, dataSize: 20}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "burrito", radixTestData{value: 33, dataSize: 6}, []int64{}, nil)

	// Increase dataSize while modifying, so eviction should happen (taco evicted)
	insertAndAssertRadix(t, cache, "burrito", radixTestData{value: 33, dataSize: 12}, []int64{26}, nil)

	if cache.LookUp("taco") != nil {
		t.Fatalf("expected taco to be evicted")
	}
	if v := cache.LookUp("burrito"); v == nil || v.(radixTestData).value != 33 {
		t.Fatalf("expected 33 for burrito, got %v", v)
	}
	if v := cache.LookUp("enchilada"); v == nil || v.(radixTestData).value != 28 {
		t.Fatalf("expected 28 for enchilada, got %v", v)
	}
}

func TestRadixCache_MultipleEviction(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "burrito", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "taco", radixTestData{value: 26, dataSize: 20}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "enchilada", radixTestData{value: 28, dataSize: 20}, []int64{}, nil)

	// Inserting large data evicts all three existing items
	insertAndAssertRadix(t, cache, "large_data", radixTestData{value: 33, dataSize: 45}, []int64{23, 26, 28}, nil)

	if cache.LookUp("taco") != nil {
		t.Fatalf("expected taco to be evicted")
	}
	if cache.LookUp("burrito") != nil {
		t.Fatalf("expected burrito to be evicted")
	}
	if cache.LookUp("enchilada") != nil {
		t.Fatalf("expected enchilada to be evicted")
	}
	if v := cache.LookUp("large_data"); v == nil || v.(radixTestData).value != 33 {
		t.Fatalf("expected 33 for large_data, got %v", v)
	}
}

func TestRadixCache_WhenEntrySizeMoreThanCacheMaxSize(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "burrito", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)

	// Insert entry with size greater than maxSize of cache.
	insertAndAssertRadix(t, cache, "taco", radixTestData{value: 26, dataSize: radixTestMaxSize + 1}, []int64{}, ErrInvalidEntrySize)

	if v := cache.LookUp("burrito"); v == nil || v.(radixTestData).value != 23 {
		t.Fatalf("expected 23 for burrito, got %v", v)
	}
}

func TestRadixCache_EraseWhenKeyPresent(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "burrito", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)

	deletedEntry := cache.Erase("burrito")
	if deletedEntry == nil || deletedEntry.(radixTestData).value != 23 {
		t.Fatalf("expected erased value 23, got %v", deletedEntry)
	}
	if cache.LookUp("burrito") != nil {
		t.Fatalf("expected burrito to be gone")
	}
}

func TestRadixCache_EraseCacheWithGivenPrefix(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "a", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "a/b", radixTestData{value: 26, dataSize: 5}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "a/b/d", radixTestData{value: 22, dataSize: 6}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "a/c", radixTestData{value: 20, dataSize: 6}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "b", radixTestData{value: 21, dataSize: 2}, []int64{}, nil)

	cache.EraseEntriesWithGivenPrefix("a")

	if cache.LookUp("a") != nil {
		t.Fatalf("expected 'a' to be erased")
	}
	if cache.LookUp("a/b") != nil {
		t.Fatalf("expected 'a/b' to be erased")
	}
	if cache.LookUp("a/b/d") != nil {
		t.Fatalf("expected 'a/b/d' to be erased")
	}
	if cache.LookUp("a/c") != nil {
		t.Fatalf("expected 'a/c' to be erased")
	}
	if v := cache.LookUp("b"); v == nil || v.Size() != 2 {
		t.Fatalf("expected 'b' to remain with size 2, got %v", v)
	}
}

func TestRadixCache_EraseCacheWithEmptyPrefix(t *testing.T) {
	cache := setupRadixCacheTest(t)

	insertAndAssertRadix(t, cache, "a", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "a/b", radixTestData{value: 26, dataSize: 5}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "b", radixTestData{value: 21, dataSize: 2}, []int64{}, nil)

	cache.EraseEntriesWithGivenPrefix("")

	if cache.LookUp("a") != nil {
		t.Fatalf("expected 'a' to be erased")
	}
	if cache.LookUp("a/b") != nil {
		t.Fatalf("expected 'a/b' to be erased")
	}
	if cache.LookUp("b") != nil {
		t.Fatalf("expected 'b' to be erased")
	}
}

func TestRadixCache_EraseCacheWhereNoEntriesExistWithGivenPrefix(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "a", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "a/b", radixTestData{value: 26, dataSize: 5}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "b", radixTestData{value: 21, dataSize: 2}, []int64{}, nil)

	cache.EraseEntriesWithGivenPrefix("c")

	if v := cache.LookUp("a"); v == nil || v.Size() != 4 {
		t.Fatalf("expected 'a' to have size 4, got %v", v)
	}
	if v := cache.LookUp("a/b"); v == nil || v.Size() != 5 {
		t.Fatalf("expected 'a/b' to have size 5, got %v", v)
	}
	if v := cache.LookUp("b"); v == nil || v.Size() != 2 {
		t.Fatalf("expected 'b' to have size 2, got %v", v)
	}
}

func TestRadixCache_EraseCacheWithGivenPrefixWithSomeEntriesEvictedDueToCacheSize(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "a", radixTestData{value: 23, dataSize: 20}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "a/b", radixTestData{value: 26, dataSize: 10}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "a/b/d", radixTestData{value: 22, dataSize: 5}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "a/c", radixTestData{value: 20, dataSize: 10}, []int64{}, nil)
	insertAndAssertRadix(t, cache, "b", radixTestData{value: 21, dataSize: 15}, []int64{23}, nil)

	// Entry "a" was already evicted by insertion of "b". Erasing prefix "a" cleans remaining descendants.
	cache.EraseEntriesWithGivenPrefix("a")

	if cache.LookUp("a") != nil {
		t.Fatalf("expected 'a' to be nil")
	}
	if cache.LookUp("a/b") != nil {
		t.Fatalf("expected 'a/b' to be nil")
	}
	if cache.LookUp("a/b/d") != nil {
		t.Fatalf("expected 'a/b/d' to be nil")
	}
	if cache.LookUp("a/c") != nil {
		t.Fatalf("expected 'a/c' to be nil")
	}
	if v := cache.LookUp("b"); v == nil || v.Size() != 15 {
		t.Fatalf("expected 'b' to remain with size 15, got %v", v)
	}
}

func TestRadixCache_EraseWhenKeyNotPresent(t *testing.T) {
	cache := setupRadixCacheTest(t)
	insertAndAssertRadix(t, cache, "burrito", radixTestData{value: 23, dataSize: 4}, []int64{}, nil)

	deletedEntry := cache.Erase("taco")
	if deletedEntry != nil {
		t.Fatalf("expected nil for non-existent key, got %v", deletedEntry)
	}
	if v := cache.LookUp("burrito"); v == nil || v.(radixTestData).value != 23 {
		t.Fatalf("expected burrito to remain 23, got %v", v)
	}
}

func TestRadixCache_UpdateSize(t *testing.T) {
	t.Run("NonExistentKey", func(t *testing.T) {
		cache := NewRadixCache(100, WithInvariantChecking(true))

		err := cache.UpdateSize("key1", 20)
		if !errors.Is(err, ErrEntryNotExist) {
			t.Fatalf("expected ErrEntryNotExist, got %v", err)
		}
	})

	t.Run("Immediate Eviction", func(t *testing.T) {
		cache := NewRadixCache(100, WithInvariantChecking(true))
		data1 := radixTestData{value: 1, dataSize: 10}
		data2 := radixTestData{value: 2, dataSize: 70}
		_, _ = cache.Insert("key1", data1)
		_, _ = cache.Insert("key2", data2)

		errUpdate := cache.UpdateSize("key1", 30)
		if errUpdate != nil {
			t.Fatalf("unexpected error: %v", errUpdate)
		}
		if cache.LookUp("key1") != nil {
			t.Fatalf("expected key1 to be evicted")
		}
		if cache.LookUp("key2") == nil {
			t.Fatalf("expected key2 to be present")
		}
	})
}

func TestRadixCache_UpdateSize_ExceedsMaxSize(t *testing.T) {
	cache := NewRadixCache(100, WithInvariantChecking(true))
	data := &radixTestData{value: 1, dataSize: 50}
	_, err := cache.Insert("file.txt", data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data.dataSize = 150
	err = cache.UpdateSize("file.txt", 100)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cache.LookUp("file.txt") != nil {
		t.Fatalf("expected file.txt to be evicted")
	}
}

func TestRadixCache_UpdateWhenKeyPresent(t *testing.T) {
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := radixTestData{value: 23, dataSize: 4}
	insertAndAssertRadix(t, cache, key, data, []int64{}, nil)
	newData := radixTestData{value: 2, dataSize: 4}

	err := cache.UpdateWithoutChangingOrder(key, newData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v := cache.LookUp(key); v == nil || v.(radixTestData).value != 2 {
		t.Fatalf("expected value 2, got %v", v)
	}
}

func TestRadixCache_UpdateWhenKeyNotPresent(t *testing.T) {
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := radixTestData{value: 23, dataSize: 4}

	err := cache.UpdateWithoutChangingOrder(key, data)
	if !errors.Is(err, ErrEntryNotExist) {
		t.Fatalf("expected ErrEntryNotExist, got %v", err)
	}
}

func TestRadixCache_UpdateWhenSizeIsDifferent(t *testing.T) {
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := radixTestData{value: 23, dataSize: 4}
	insertAndAssertRadix(t, cache, key, data, []int64{}, nil)
	newData := radixTestData{value: 2, dataSize: 3}

	err := cache.UpdateWithoutChangingOrder(key, newData)
	if !errors.Is(err, ErrInvalidUpdateEntrySize) {
		t.Fatalf("expected ErrInvalidUpdateEntrySize, got %v", err)
	}
}

func TestRadixCache_UpdateNotChangeOrder(t *testing.T) {
	cache := setupRadixCacheTest(t)
	key1 := "burrito1"
	data1 := radixTestData{value: 23, dataSize: 10}
	insertAndAssertRadix(t, cache, key1, data1, []int64{}, nil)
	key2 := "burrito2"
	data2 := radixTestData{value: 2, dataSize: 40}
	insertAndAssertRadix(t, cache, key2, data2, []int64{}, nil)

	newData := radixTestData{value: 7, dataSize: 10}
	err := cache.UpdateWithoutChangingOrder(key1, newData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Inserting again should evict key1 because key1 was updated without changing order (still at tail)
	key3 := "burrito3"
	data3 := radixTestData{value: 3, dataSize: 5}
	insertAndAssertRadix(t, cache, key3, data3, []int64{7}, nil)
}

func TestRadixCache_UpdateSize_DoubleCountingDivergence(t *testing.T) {
	const maxSize = 100
	const initialSize = 50
	const sizeDelta = 50 // New total size will be 50 + 50 = 100 (exactly at maxSize)

	radixCache := NewRadixCache(maxSize, WithInvariantChecking(true))
	radixVal := &radixTestData{value: 2, dataSize: initialSize}

	// 1. Insert initial entry
	_, err := radixCache.Insert("file.txt", radixVal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 2. Simulate incremental file growth in memory
	radixVal.dataSize += sizeDelta

	// 3. Notify radixCache of the growth
	err = radixCache.UpdateSize("file.txt", sizeDelta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if v := radixCache.LookUp("file.txt"); v == nil {
		t.Fatalf("expected file.txt to remain in cache")
	}
}

func TestRadixCache_LookUpWithoutChangingOrder_WhenKeyPresent(t *testing.T) {
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := radixTestData{value: 23, dataSize: 4}
	insertAndAssertRadix(t, cache, key, data, []int64{}, nil)

	value := cache.LookUpWithoutChangingOrder(key)
	if value == nil || value.(radixTestData).value != 23 {
		t.Fatalf("expected 23, got %v", value)
	}
}

func TestRadixCache_LookUpWithoutChangingOrder_WhenKeyNotPresent(t *testing.T) {
	cache := setupRadixCacheTest(t)
	key := "burrito"

	value := cache.LookUpWithoutChangingOrder(key)
	if value != nil {
		t.Fatalf("expected nil for non-existent key, got %v", value)
	}
}

func TestRadixCache_LookUpWithoutChangingOrder_NotChangeOrder(t *testing.T) {
	cache := setupRadixCacheTest(t)
	key1 := "burrito1"
	data1 := radixTestData{value: 23, dataSize: 10}
	insertAndAssertRadix(t, cache, key1, data1, []int64{}, nil)
	key2 := "burrito2"
	data2 := radixTestData{value: 2, dataSize: 40}
	insertAndAssertRadix(t, cache, key2, data2, []int64{}, nil)

	value := cache.LookUpWithoutChangingOrder(key1)
	if value == nil || value.(radixTestData).value != 23 {
		t.Fatalf("expected 23, got %v", value)
	}

	// Inserting again should evict key1 because key1 was looked up without changing order
	key3 := "burrito3"
	data3 := radixTestData{value: 3, dataSize: 5}
	insertAndAssertRadix(t, cache, key3, data3, []int64{23}, nil)
}

func TestRadixCache_NewRadixCache_InvalidMaxSize(t *testing.T) {
	t.Run("DefaultOptions", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic on maxSize == 0 with default options, got none")
			}
		}()
		_ = NewRadixCache(0)
	})

	t.Run("InvariantsEnabled", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic on maxSize == 0 with invariant checking enabled, got none")
			}
		}()
		_ = NewRadixCache(0, WithInvariantChecking(true))
	})

	t.Run("InvariantsDisabled", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic on maxSize == 0 with invariant checking disabled, got none")
			}
		}()
		_ = NewRadixCache(0, WithInvariantChecking(false))
	})
}

func TestRadixCache_ComplexEdgeSplitsAndMerges(t *testing.T) {
	cache := NewRadixCache(1000, WithInvariantChecking(true))

	keys := []string{
		"car",
		"cart",
		"card",
		"carpet",
		"care",
		"careful",
		"cat",
		"catch",
		"dog",
		"door",
		"dorm",
	}

	for i, k := range keys {
		_, err := cache.Insert(k, radixTestData{value: int64(i + 1), dataSize: 10})
		if err != nil {
			t.Fatalf("failed to insert %q: %v", k, err)
		}
	}

	// Verify all keys are present
	for i, k := range keys {
		v := cache.LookUp(k)
		if v == nil || v.(radixTestData).value != int64(i+1) {
			t.Fatalf("expected %d for %q, got %v", i+1, k, v)
		}
	}

	// Erase prefix "car" -> should remove "car", "cart", "card", "carpet", "care", "careful"
	cache.EraseEntriesWithGivenPrefix("car")

	carKeys := []string{"car", "cart", "card", "carpet", "care", "careful"}
	for _, k := range carKeys {
		if v := cache.LookUp(k); v != nil {
			t.Fatalf("expected %q to be erased, got %v", k, v)
		}
	}

	// Remaining keys should still be present
	nonCarKeys := []string{"cat", "catch", "dog", "door", "dorm"}
	for _, k := range nonCarKeys {
		if v := cache.LookUp(k); v == nil {
			t.Fatalf("expected %q to remain in cache", k)
		}
	}

	// Erase remaining keys one by one to verify compressPathUpwards in reverse
	for _, k := range nonCarKeys {
		v := cache.Erase(k)
		if v == nil {
			t.Fatalf("expected non-nil erased value for %q", k)
		}
	}

	// Cache should now be completely empty
	for _, k := range keys {
		if cache.LookUp(k) != nil {
			t.Fatalf("expected cache to be empty, found %q", k)
		}
	}
}

func TestRadixCache_BinaryAndUnicodeKeys(t *testing.T) {
	cache := NewRadixCache(1000, WithInvariantChecking(true))

	unicodeKeys := []string{
		"日本語/ディレクトリ/ファイル1",
		"日本語/ディレクトリ/ファイル2",
		"日本語/別のディレクトリ/ファイル3",
		"🚀/rocket/one",
		"🚀/rocket/two",
		"\x00\x01\x02\x03",
		"\x00\x01\x02\x04",
		"\xff\xfe\xfd",
	}

	for i, k := range unicodeKeys {
		_, err := cache.Insert(k, radixTestData{value: int64(i + 1), dataSize: 10})
		if err != nil {
			t.Fatalf("failed to insert key %q: %v", k, err)
		}
	}

	for i, k := range unicodeKeys {
		v := cache.LookUp(k)
		if v == nil || v.(radixTestData).value != int64(i+1) {
			t.Fatalf("expected value %d for %q, got %v", i+1, k, v)
		}
	}

	// Erase prefix "日本語/"
	cache.EraseEntriesWithGivenPrefix("日本語/")
	if cache.LookUp("日本語/ディレクトリ/ファイル1") != nil {
		t.Fatalf("expected unicode key to be erased")
	}
	if cache.LookUp("日本語/ディレクトリ/ファイル2") != nil {
		t.Fatalf("expected unicode key to be erased")
	}
	if cache.LookUp("日本語/別のディレクトリ/ファイル3") != nil {
		t.Fatalf("expected unicode key to be erased")
	}
	if cache.LookUp("🚀/rocket/one") == nil {
		t.Fatalf("expected rocket key to remain")
	}
}

func TestRadixCache_UpdateWithoutChangingOrder_Errors(t *testing.T) {
	cache := NewRadixCache(100, WithInvariantChecking(true))

	// Nil value returns ErrInvalidEntry
	err := cache.UpdateWithoutChangingOrder("any", nil)
	if !errors.Is(err, ErrInvalidEntry) {
		t.Fatalf("expected ErrInvalidEntry, got %v", err)
	}
}

func TestRadixCache_OptionsToggle(t *testing.T) {
	// Without invariant checking (production mode)
	cacheProd := NewRadixCache(100, WithInvariantChecking(false))
	_, err := cacheProd.Insert("k1", radixTestData{value: 1, dataSize: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// With invariant checking (debug/test mode)
	cacheDebug := NewRadixCache(100, WithInvariantChecking(true))
	_, err = cacheDebug.Insert("k1", radixTestData{value: 1, dataSize: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRadixCache_CheckInvariants_PanicScenarios(t *testing.T) {
	t.Run("CurrentSizeExceedsMaxSize", func(t *testing.T) {
		c := NewRadixCache(10).(*radixCache)
		c.currentSize = 20
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on currentSize > maxSize")
			}
		}()
		c.checkInvariants()
	})

	t.Run("CorruptLRULinks", func(t *testing.T) {
		c := NewRadixCache(50).(*radixCache)
		_, _ = c.Insert("k1", radixTestData{value: 1, dataSize: 10})
		_, _ = c.Insert("k2", radixTestData{value: 2, dataSize: 10})
		// Corrupt prev pointer
		c.head.next.prev = nil
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on corrupt prev pointer")
			}
		}()
		c.checkInvariants()
	})

	t.Run("CorruptTreeParent", func(t *testing.T) {
		c := NewRadixCache(50).(*radixCache)
		_, _ = c.Insert("a/b", radixTestData{value: 1, dataSize: 10})
		// Corrupt child's parent pointer
		if c.root.child != nil {
			c.root.child.parent = nil
		}
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on corrupt parent pointer")
			}
		}()
		c.checkInvariants()
	})

	t.Run("SizeSumMismatch", func(t *testing.T) {
		c := NewRadixCache(50).(*radixCache)
		_, _ = c.Insert("k1", radixTestData{value: 1, dataSize: 10})
		// Corrupt currentSize by 1 so sumSize != currentSize
		c.currentSize += 1
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on size sum mismatch")
			}
		}()
		c.checkInvariants()
	})
}
