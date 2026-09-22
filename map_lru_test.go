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

const (
	testMaxSize        = 50
	testOperationCount = 100
)

type testData struct {
	value    int64
	dataSize uint64
}

func (td testData) Size() uint64 {
	return td.dataSize
}

func setupCacheTest(t *testing.T) Cache {
	t.Helper()
	return NewMapCache(testMaxSize, WithInvariantChecking(true))
}

// insertAndAssert inserts key, val into the cache and asserts expected eviction list and error.
func insertAndAssert(t *testing.T, cache Cache, key string, val ValueType, evictedValues []int64, expectedError error) {
	t.Helper()
	ret, err := cache.Insert(key, val)

	if expectedError != nil {
		if !errors.Is(err, expectedError) {
			t.Fatalf("expected error %v, got %v", expectedError, err)
		}
	} else if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(evictedValues) != len(ret) {
		t.Fatalf("eviction count mismatch: expected %d, got %d", len(evictedValues), len(ret))
	}
	for i, evicted := range ret {
		td, ok := evicted.(testData)
		if !ok {
			t.Fatalf("evicted value at index %d is not testData: %T", i, evicted)
		}
		if td.value != evictedValues[i] {
			t.Errorf("evicted value at index %d: expected %d, got %d", i, evictedValues[i], td.value)
		}
	}
}

func TestLookUpInEmptyCache(t *testing.T) {
	cache := setupCacheTest(t)
	if val := cache.LookUp(""); val != nil {
		t.Errorf("expected nil, got %v", val)
	}
	if val := cache.LookUp("taco"); val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

func TestInsertNilValue(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "taco", nil, nil, ErrInvalidEntry)
}

func TestLookUpUnknownKey(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "burrito", testData{value: 23, dataSize: 4}, nil, nil)
	insertAndAssert(t, cache, "taco", testData{value: 23, dataSize: 8}, nil, nil)

	if val := cache.LookUp(""); val != nil {
		t.Errorf("expected nil, got %v", val)
	}
	if val := cache.LookUp("enchilada"); val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

func TestFillUpToCapacity(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "burrito", testData{value: 23, dataSize: 4}, nil, nil)
	insertAndAssert(t, cache, "taco", testData{value: 26, dataSize: 20}, nil, nil)
	insertAndAssert(t, cache, "enchilada", testData{value: 28, dataSize: 26}, nil, nil)

	if val := cache.LookUp("burrito"); val == nil || val.(testData).value != 23 {
		t.Errorf("burrito: expected 23, got %v", val)
	}
	if val := cache.LookUp("taco"); val == nil || val.(testData).value != 26 {
		t.Errorf("taco: expected 26, got %v", val)
	}
	if val := cache.LookUp("enchilada"); val == nil || val.(testData).value != 28 {
		t.Errorf("enchilada: expected 28, got %v", val)
	}
}

func TestExpiresLeastRecentlyUsed(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "burrito", testData{value: 23, dataSize: 4}, nil, nil)

	// Least recent.
	insertAndAssert(t, cache, "taco", testData{value: 26, dataSize: 20}, nil, nil)

	// Second most recent.
	insertAndAssert(t, cache, "enchilada", testData{value: 28, dataSize: 26}, nil, nil)

	if val := cache.LookUp("burrito"); val == nil || val.(testData).value != 23 {
		t.Errorf("burrito: expected 23, got %v", val)
	} // burrito is now most recent

	// Insert another, requiring eviction of taco (size 20) to fit queso (size 5).
	insertAndAssert(t, cache, "queso", testData{value: 34, dataSize: 5}, []int64{26}, nil)

	// See what's left.
	if val := cache.LookUp("taco"); val != nil {
		t.Errorf("expected taco to be evicted, got %v", val)
	}
	if val := cache.LookUp("burrito"); val == nil || val.(testData).value != 23 {
		t.Errorf("burrito: expected 23, got %v", val)
	}
	if val := cache.LookUp("enchilada"); val == nil || val.(testData).value != 28 {
		t.Errorf("enchilada: expected 28, got %v", val)
	}
	if val := cache.LookUp("queso"); val == nil || val.(testData).value != 34 {
		t.Errorf("queso: expected 34, got %v", val)
	}
}

func TestOverwrite(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "burrito", testData{value: 23, dataSize: 4}, nil, nil)
	insertAndAssert(t, cache, "taco", testData{value: 26, dataSize: 20}, nil, nil)
	insertAndAssert(t, cache, "enchilada", testData{value: 28, dataSize: 20}, nil, nil)
	insertAndAssert(t, cache, "burrito", testData{value: 33, dataSize: 6}, nil, nil)

	// Increase the DataSize while modifying, so eviction of taco should happen.
	insertAndAssert(t, cache, "burrito", testData{value: 33, dataSize: 12}, []int64{26}, nil)

	if val := cache.LookUp("taco"); val != nil {
		t.Errorf("expected taco to be evicted, got %v", val)
	}
	if val := cache.LookUp("burrito"); val == nil || val.(testData).value != 33 {
		t.Errorf("burrito: expected 33, got %v", val)
	}
	if val := cache.LookUp("enchilada"); val == nil || val.(testData).value != 28 {
		t.Errorf("enchilada: expected 28, got %v", val)
	}
}

func TestMultipleEviction(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "burrito", testData{value: 23, dataSize: 4}, nil, nil)
	insertAndAssert(t, cache, "taco", testData{value: 26, dataSize: 20}, nil, nil)
	insertAndAssert(t, cache, "enchilada", testData{value: 28, dataSize: 20}, nil, nil)

	// Inserting large entry requires evicting burrito, taco, and enchilada in oldest-first order.
	insertAndAssert(t, cache, "large_data", testData{value: 33, dataSize: 45}, []int64{23, 26, 28}, nil)

	if val := cache.LookUp("taco"); val != nil {
		t.Errorf("expected taco to be evicted, got %v", val)
	}
	if val := cache.LookUp("burrito"); val != nil {
		t.Errorf("expected burrito to be evicted, got %v", val)
	}
	if val := cache.LookUp("enchilada"); val != nil {
		t.Errorf("expected enchilada to be evicted, got %v", val)
	}
	if val := cache.LookUp("large_data"); val == nil || val.(testData).value != 33 {
		t.Errorf("large_data: expected 33, got %v", val)
	}
}

func TestWhenEntrySizeMoreThanCacheMaxSize(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "burrito", testData{value: 23, dataSize: 4}, nil, nil)

	// Insert entry with size greater than maxSize of cache.
	insertAndAssert(t, cache, "taco", testData{value: 26, dataSize: testMaxSize + 1}, nil, ErrInvalidEntrySize)

	if val := cache.LookUp("burrito"); val == nil || val.(testData).value != 23 {
		t.Errorf("burrito: expected 23, got %v", val)
	}
}

func TestEraseWhenKeyPresent(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "burrito", testData{value: 23, dataSize: 4}, nil, nil)

	deletedEntry := cache.Erase("burrito")
	if deletedEntry == nil || deletedEntry.(testData).value != 23 {
		t.Errorf("expected erased value 23, got %v", deletedEntry)
	}
	if val := cache.LookUp("burrito"); val != nil {
		t.Errorf("expected nil after erase, got %v", val)
	}
}

func TestEraseCacheWithGivenPrefix(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "a", testData{value: 23, dataSize: 4}, nil, nil)
	insertAndAssert(t, cache, "a/b", testData{value: 26, dataSize: 5}, nil, nil)
	insertAndAssert(t, cache, "a/b/d", testData{value: 22, dataSize: 6}, nil, nil)
	insertAndAssert(t, cache, "a/c", testData{value: 20, dataSize: 6}, nil, nil)
	insertAndAssert(t, cache, "b", testData{value: 21, dataSize: 2}, nil, nil)

	cache.EraseEntriesWithGivenPrefix("a")

	if val := cache.LookUp("a"); val != nil {
		t.Errorf("expected nil for a, got %v", val)
	}
	if val := cache.LookUp("a/b"); val != nil {
		t.Errorf("expected nil for a/b, got %v", val)
	}
	if val := cache.LookUp("a/b/d"); val != nil {
		t.Errorf("expected nil for a/b/d, got %v", val)
	}
	if val := cache.LookUp("a/c"); val != nil {
		t.Errorf("expected nil for a/c, got %v", val)
	}
	if val := cache.LookUp("b"); val == nil || val.Size() != 2 {
		t.Errorf("expected b size 2, got %v", val)
	}
}

func TestEraseCacheWhereNoEntriesExistWithGivenPrefix(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "a", testData{value: 23, dataSize: 4}, nil, nil)
	insertAndAssert(t, cache, "a/b", testData{value: 26, dataSize: 5}, nil, nil)
	insertAndAssert(t, cache, "b", testData{value: 21, dataSize: 2}, nil, nil)

	cache.EraseEntriesWithGivenPrefix("c")

	if val := cache.LookUp("a"); val == nil || val.Size() != 4 {
		t.Errorf("expected a size 4, got %v", val)
	}
	if val := cache.LookUp("a/b"); val == nil || val.Size() != 5 {
		t.Errorf("expected a/b size 5, got %v", val)
	}
	if val := cache.LookUp("b"); val == nil || val.Size() != 2 {
		t.Errorf("expected b size 2, got %v", val)
	}
}

func TestEraseCacheWithGivenPrefixWithSomeEntriesEvictedDueToCacheSize(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "a", testData{value: 23, dataSize: 20}, nil, nil)
	insertAndAssert(t, cache, "a/b", testData{value: 26, dataSize: 10}, nil, nil)
	insertAndAssert(t, cache, "a/b/d", testData{value: 22, dataSize: 5}, nil, nil)
	insertAndAssert(t, cache, "a/c", testData{value: 20, dataSize: 10}, nil, nil)
	insertAndAssert(t, cache, "b", testData{value: 21, dataSize: 15}, []int64{23}, nil)

	// As entry "a" was already evicted by the insertion of "b", only three entries will be removed.
	cache.EraseEntriesWithGivenPrefix("a")

	if val := cache.LookUp("a"); val != nil {
		t.Errorf("expected nil for a, got %v", val)
	}
	if val := cache.LookUp("a/b"); val != nil {
		t.Errorf("expected nil for a/b, got %v", val)
	}
	if val := cache.LookUp("a/b/d"); val != nil {
		t.Errorf("expected nil for a/b/d, got %v", val)
	}
	if val := cache.LookUp("a/c"); val != nil {
		t.Errorf("expected nil for a/c, got %v", val)
	}
	if val := cache.LookUp("b"); val == nil || val.Size() != 15 {
		t.Errorf("expected b size 15, got %v", val)
	}
}

func TestEraseCacheWithEmptyPrefix(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "a", testData{value: 1, dataSize: 10}, nil, nil)
	insertAndAssert(t, cache, "b", testData{value: 2, dataSize: 10}, nil, nil)
	insertAndAssert(t, cache, "c", testData{value: 3, dataSize: 10}, nil, nil)

	cache.EraseEntriesWithGivenPrefix("")

	if val := cache.LookUp("a"); val != nil {
		t.Errorf("expected nil for a, got %v", val)
	}
	if val := cache.LookUp("b"); val != nil {
		t.Errorf("expected nil for b, got %v", val)
	}
	if val := cache.LookUp("c"); val != nil {
		t.Errorf("expected nil for c, got %v", val)
	}
}

func TestEraseWhenKeyNotPresent(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "burrito", testData{value: 23, dataSize: 4}, nil, nil)

	deletedEntry := cache.Erase("taco")
	if deletedEntry != nil {
		t.Errorf("expected nil when erasing non-existent key, got %v", deletedEntry)
	}

	if val := cache.LookUp("burrito"); val == nil || val.(testData).value != 23 {
		t.Errorf("burrito: expected 23, got %v", val)
	}
}

func TestUpdateWhenKeyPresent(t *testing.T) {
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	insertAndAssert(t, cache, key, data, nil, nil)
	newData := testData{value: 2, dataSize: 4}

	err := cache.UpdateWithoutChangingOrder(key, newData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val := cache.LookUp(key); val == nil || val.(testData).value != 2 {
		t.Errorf("expected updated value 2, got %v", val)
	}
}

func TestUpdateWhenKeyNotPresent(t *testing.T) {
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}

	err := cache.UpdateWithoutChangingOrder(key, data)
	if !errors.Is(err, ErrEntryNotExist) {
		t.Errorf("expected ErrEntryNotExist, got %v", err)
	}
}

func TestUpdateNilValue(t *testing.T) {
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	insertAndAssert(t, cache, key, data, nil, nil)

	err := cache.UpdateWithoutChangingOrder(key, nil)
	if !errors.Is(err, ErrInvalidEntry) {
		t.Errorf("expected ErrInvalidEntry, got %v", err)
	}
}

func TestUpdateWhenSizeIsDifferent(t *testing.T) {
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	insertAndAssert(t, cache, key, data, nil, nil)
	newData := testData{value: 2, dataSize: 3}

	err := cache.UpdateWithoutChangingOrder(key, newData)
	if !errors.Is(err, ErrInvalidUpdateEntrySize) {
		t.Errorf("expected ErrInvalidUpdateEntrySize, got %v", err)
	}
}

func TestUpdateNotChangeOrder(t *testing.T) {
	cache := setupCacheTest(t)
	key1 := "burrito1"
	data1 := testData{value: 23, dataSize: 10}
	insertAndAssert(t, cache, key1, data1, nil, nil)
	key2 := "burrito2"
	data2 := testData{value: 2, dataSize: 40}
	insertAndAssert(t, cache, key2, data2, nil, nil)

	newData := testData{value: 7, dataSize: 10}
	err := cache.UpdateWithoutChangingOrder(key1, newData)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Inserting again should evict key1 because key1 was updated without changing order (still LRU).
	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	insertAndAssert(t, cache, key3, data3, []int64{7}, nil)
}

func TestLookUpWithoutChangingOrder_WhenKeyPresent(t *testing.T) {
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	insertAndAssert(t, cache, key, data, nil, nil)

	value := cache.LookUpWithoutChangingOrder(key)
	if value == nil || value.(testData).value != 23 {
		t.Errorf("expected 23, got %v", value)
	}
}

func TestLookUpWithoutChangingOrder_WhenKeyNotPresent(t *testing.T) {
	cache := setupCacheTest(t)
	key := "burrito"

	value := cache.LookUpWithoutChangingOrder(key)
	if value != nil {
		t.Errorf("expected nil, got %v", value)
	}
}

func TestLookUpWithoutChangingOrder_NotChangeOrder(t *testing.T) {
	cache := setupCacheTest(t)
	key1 := "burrito1"
	data1 := testData{value: 23, dataSize: 10}
	insertAndAssert(t, cache, key1, data1, nil, nil)
	key2 := "burrito2"
	data2 := testData{value: 2, dataSize: 40}
	insertAndAssert(t, cache, key2, data2, nil, nil)

	value := cache.LookUpWithoutChangingOrder(key1)
	if value == nil || value.(testData).value != 23 {
		t.Errorf("expected 23, got %v", value)
	}

	// Inserting again should evict key1 because key1 was looked up without changing order.
	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	insertAndAssert(t, cache, key3, data3, []int64{23}, nil)
}

func TestUpdateSize_Success(t *testing.T) {
	cache := setupCacheTest(t)
	insertAndAssert(t, cache, "file1", testData{value: 10, dataSize: 10}, nil, nil)
	insertAndAssert(t, cache, "file2", testData{value: 20, dataSize: 20}, nil, nil)

	err := cache.UpdateSize("file1", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Order should be preserved: file1 is still LRU.
	// Inserting 20 more units (total size was 10+10+20 = 40, now 40+20 = 60 > 50) evicts file1.
	insertAndAssert(t, cache, "file3", testData{value: 30, dataSize: 20}, []int64{10}, nil)
}

func TestUpdateSize_NonExistentKey(t *testing.T) {
	cache := setupCacheTest(t)
	err := cache.UpdateSize("nonexistent", 10)
	if !errors.Is(err, ErrEntryNotExist) {
		t.Errorf("expected ErrEntryNotExist, got %v", err)
	}
}

func TestUpdateSize_ExceedsMaxSize_Evicts(t *testing.T) {
	cache := setupCacheTest(t) // maxSize = 50
	insertAndAssert(t, cache, "key1", testData{value: 1, dataSize: 20}, nil, nil)
	insertAndAssert(t, cache, "key2", testData{value: 2, dataSize: 25}, nil, nil)

	// currentSize was 45. Increasing key2 size by 10 makes currentSize = 55 > 50.
	// key1 (LRU) must be evicted immediately by UpdateSize to maintain the size invariant.
	err := cache.UpdateSize("key2", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if val := cache.LookUp("key1"); val != nil {
		t.Errorf("expected key1 to be evicted, got %v", val)
	}
	if val := cache.LookUp("key2"); val == nil || val.(testData).value != 2 {
		t.Errorf("expected key2 to be present with value 2, got %v", val)
	}
}

func TestNewAlias(t *testing.T) {
	c := New(100)
	if c == nil {
		t.Fatal("expected New to return non-nil Cache")
	}
	_, err := c.Insert("k", testData{value: 1, dataSize: 10})
	if err != nil {
		t.Fatalf("unexpected error on Insert: %v", err)
	}
	if val := c.LookUp("k"); val == nil || val.(testData).value != 1 {
		t.Errorf("expected value 1, got %v", val)
	}
}

func TestCheckInvariants_InvalidMaxSizePanic(t *testing.T) {
	t.Run("DefaultOptions", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on zero maxSize with default options")
			}
		}()
		_ = NewMapCache(0)
	})

	t.Run("InvariantsEnabled", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on zero maxSize with invariant checking enabled")
			}
		}()
		_ = NewMapCache(0, WithInvariantChecking(true))
	})

	t.Run("InvariantsDisabled", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on zero maxSize with invariant checking disabled")
			}
		}()
		_ = NewMapCache(0, WithInvariantChecking(false))
	})

	t.Run("NewAliasDefaultOptions", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on New(0) with default options")
			}
		}()
		_ = New(0)
	})
}

func TestCheckInvariants_PanicOnCorruption(t *testing.T) {
	t.Run("CurrentSizeExceedsMaxSize", func(t *testing.T) {
		c := NewMapCache(10).(*mapCache)
		c.currentSize = 20
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on currentSize > maxSize")
			}
		}()
		c.checkInvariants()
	})

	t.Run("LengthMismatch", func(t *testing.T) {
		c := NewMapCache(10).(*mapCache)
		c.index["dummy"] = nil
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on length mismatch")
			}
		}()
		c.checkInvariants()
	})

	t.Run("KeyMismatch", func(t *testing.T) {
		c := NewMapCache(50).(*mapCache)
		e := c.entries.PushFront(entry{key: "correctKey", value: testData{1, 5}})
		c.index["wrongKey"] = e
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on key mismatch")
			}
		}()
		c.checkInvariants()
	})

	t.Run("InvalidElementType", func(t *testing.T) {
		c := NewMapCache(50).(*mapCache)
		e := c.entries.PushFront("not-an-entry-struct")
		c.index["someKey"] = e
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on invalid element type")
			}
		}()
		c.checkInvariants()
	})

	t.Run("SizeSumMismatch", func(t *testing.T) {
		c := NewMapCache(500).(*mapCache)
		_, _ = c.Insert("k1", testData{value: 1, dataSize: 10})
		c.currentSize += 1
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on size sum mismatch")
			}
		}()
		c.checkInvariants()
	})
}

func TestMapCache_EraseEntriesWithGivenPrefix_EmptyPrefixFastPath(t *testing.T) {
	c := NewMapCache(1000, WithInvariantChecking(true)).(*mapCache)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("entry_%d", i)
		_, err := c.Insert(key, testData{value: int64(i), dataSize: 10})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}
	if c.currentSize != 200 {
		t.Fatalf("expected currentSize 200, got %d", c.currentSize)
	}

	c.EraseEntriesWithGivenPrefix("")

	if c.currentSize != 0 {
		t.Errorf("expected currentSize 0 after empty prefix erase, got %d", c.currentSize)
	}
	if c.entries.Len() != 0 {
		t.Errorf("expected entries.Len() 0, got %d", c.entries.Len())
	}
	if len(c.index) != 0 {
		t.Errorf("expected len(index) 0, got %d", len(c.index))
	}

	// Erasing empty prefix on an already empty cache must be a safe no-op.
	c.EraseEntriesWithGivenPrefix("")
	if c.currentSize != 0 || c.entries.Len() != 0 || len(c.index) != 0 {
		t.Errorf("expected empty cache state after erasing empty cache")
	}
}
