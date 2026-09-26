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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testMaxSize = 50

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

func assertEvictedValues(t *testing.T, evicted []ValueType, expectedValues []int64) {
	t.Helper()
	if len(expectedValues) == 0 {
		assert.Empty(t, evicted)
		return
	}
	require.Len(t, evicted, len(expectedValues))
	for i, exp := range expectedValues {
		td, ok := evicted[i].(testData)
		require.True(t, ok, "evicted value at index %d is not testData: %T", i, evicted[i])
		assert.Equal(t, exp, td.value)
	}
}

func TestLookUpInEmptyCache(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)

	// Act
	valEmpty := cache.LookUp("")
	valTaco := cache.LookUp("taco")

	// Assert
	assert.Nil(t, valEmpty)
	assert.Nil(t, valTaco)
}

func TestInsertNilValue(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)

	// Act
	evicted, err := cache.Insert("taco", nil)

	// Assert
	require.ErrorIs(t, err, ErrInvalidEntry)
	assertEvictedValues(t, evicted, nil)
}

func TestLookUpUnknownKey(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	evicted, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Insert("taco", testData{value: 23, dataSize: 8})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act
	valEmpty := cache.LookUp("")
	valEnchilada := cache.LookUp("enchilada")

	// Assert
	assert.Nil(t, valEmpty)
	assert.Nil(t, valEnchilada)
}

func TestFillUpToCapacity(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)

	// Act
	evicted1, err1 := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	evicted2, err2 := cache.Insert("taco", testData{value: 26, dataSize: 20})
	evicted3, err3 := cache.Insert("enchilada", testData{value: 28, dataSize: 26})

	// Assert
	require.NoError(t, err1)
	assertEvictedValues(t, evicted1, nil)
	require.NoError(t, err2)
	assertEvictedValues(t, evicted2, nil)
	require.NoError(t, err3)
	assertEvictedValues(t, evicted3, nil)

	assert.Equal(t, testData{value: 23, dataSize: 4}, cache.LookUp("burrito"))
	assert.Equal(t, testData{value: 26, dataSize: 20}, cache.LookUp("taco"))
	assert.Equal(t, testData{value: 28, dataSize: 26}, cache.LookUp("enchilada"))
}

func TestExpiresLeastRecentlyUsed(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	evicted, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Least recent.
	evicted, err = cache.Insert("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Second most recent.
	evicted, err = cache.Insert("enchilada", testData{value: 28, dataSize: 26})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Promote burrito to MRU.
	assert.Equal(t, testData{value: 23, dataSize: 4}, cache.LookUp("burrito"))

	// Act: Insert another, requiring eviction of taco (size 20) to fit queso (size 5).
	evicted, err = cache.Insert("queso", testData{value: 34, dataSize: 5})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{26})
	assert.Nil(t, cache.LookUp("taco"))
	assert.Equal(t, testData{value: 23, dataSize: 4}, cache.LookUp("burrito"))
	assert.Equal(t, testData{value: 28, dataSize: 26}, cache.LookUp("enchilada"))
	assert.Equal(t, testData{value: 34, dataSize: 5}, cache.LookUp("queso"))
}

func TestOverwrite(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	evicted, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Insert("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Insert("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Insert("burrito", testData{value: 33, dataSize: 6})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act: Increase the DataSize while modifying, so eviction of taco should happen.
	evicted, err = cache.Insert("burrito", testData{value: 33, dataSize: 12})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{26})
	assert.Nil(t, cache.LookUp("taco"))
	assert.Equal(t, testData{value: 33, dataSize: 12}, cache.LookUp("burrito"))
	assert.Equal(t, testData{value: 28, dataSize: 20}, cache.LookUp("enchilada"))
}

func TestMultipleEviction(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	evicted, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Insert("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Insert("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act: Inserting large entry requires evicting burrito, taco, and enchilada in oldest-first order.
	evicted, err = cache.Insert("large_data", testData{value: 33, dataSize: 45})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{23, 26, 28})
	assert.Nil(t, cache.LookUp("taco"))
	assert.Nil(t, cache.LookUp("burrito"))
	assert.Nil(t, cache.LookUp("enchilada"))
	assert.Equal(t, testData{value: 33, dataSize: 45}, cache.LookUp("large_data"))
}

func TestWhenEntrySizeMoreThanCacheMaxSize(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	evicted, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act: Insert entry with size greater than maxSize of cache.
	evicted, err = cache.Insert("taco", testData{value: 26, dataSize: testMaxSize + 1})

	// Assert
	require.ErrorIs(t, err, ErrInvalidEntrySize)
	assertEvictedValues(t, evicted, nil)
	assert.Equal(t, testData{value: 23, dataSize: 4}, cache.LookUp("burrito"))
}

func TestEraseWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	evicted, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act
	deletedEntry := cache.Erase("burrito")

	// Assert
	assert.Equal(t, testData{value: 23, dataSize: 4}, deletedEntry)
	assert.Nil(t, cache.LookUp("burrito"))
}

func TestEraseCacheWithGivenPrefix(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	_, err := cache.Insert("a", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("a/b", testData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Insert("a/b/d", testData{value: 22, dataSize: 6})
	require.NoError(t, err)
	_, err = cache.Insert("a/c", testData{value: 20, dataSize: 6})
	require.NoError(t, err)
	_, err = cache.Insert("b", testData{value: 21, dataSize: 2})
	require.NoError(t, err)

	// Act
	cache.EraseEntriesWithGivenPrefix("a")

	// Assert
	assert.Nil(t, cache.LookUp("a"))
	assert.Nil(t, cache.LookUp("a/b"))
	assert.Nil(t, cache.LookUp("a/b/d"))
	assert.Nil(t, cache.LookUp("a/c"))
	valB := cache.LookUp("b")
	require.NotNil(t, valB)
	assert.Equal(t, uint64(2), valB.Size())
}

func TestEraseCacheWhereNoEntriesExistWithGivenPrefix(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	_, err := cache.Insert("a", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("a/b", testData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Insert("b", testData{value: 21, dataSize: 2})
	require.NoError(t, err)

	// Act
	cache.EraseEntriesWithGivenPrefix("c")

	// Assert
	valA := cache.LookUp("a")
	require.NotNil(t, valA)
	assert.Equal(t, uint64(4), valA.Size())

	valAB := cache.LookUp("a/b")
	require.NotNil(t, valAB)
	assert.Equal(t, uint64(5), valAB.Size())

	valB := cache.LookUp("b")
	require.NotNil(t, valB)
	assert.Equal(t, uint64(2), valB.Size())
}

func TestEraseCacheWithGivenPrefixWithSomeEntriesEvictedDueToCacheSize(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	_, err := cache.Insert("a", testData{value: 23, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Insert("a/b", testData{value: 26, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Insert("a/b/d", testData{value: 22, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Insert("a/c", testData{value: 20, dataSize: 10})
	require.NoError(t, err)
	evicted, err := cache.Insert("b", testData{value: 21, dataSize: 15})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{23})

	// Act: As entry "a" was already evicted by the insertion of "b", only three entries will be removed.
	cache.EraseEntriesWithGivenPrefix("a")

	// Assert
	assert.Nil(t, cache.LookUp("a"))
	assert.Nil(t, cache.LookUp("a/b"))
	assert.Nil(t, cache.LookUp("a/b/d"))
	assert.Nil(t, cache.LookUp("a/c"))
	valB := cache.LookUp("b")
	require.NotNil(t, valB)
	assert.Equal(t, uint64(15), valB.Size())
}

func TestEraseCacheWithEmptyPrefix(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	_, err := cache.Insert("a", testData{value: 1, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Insert("b", testData{value: 2, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Insert("c", testData{value: 3, dataSize: 10})
	require.NoError(t, err)

	// Act
	cache.EraseEntriesWithGivenPrefix("")

	// Assert
	assert.Nil(t, cache.LookUp("a"))
	assert.Nil(t, cache.LookUp("b"))
	assert.Nil(t, cache.LookUp("c"))
}

func TestEraseWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	_, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)

	// Act
	deletedEntry := cache.Erase("taco")

	// Assert
	assert.Nil(t, deletedEntry)
	assert.Equal(t, testData{value: 23, dataSize: 4}, cache.LookUp("burrito"))
}

func TestUpdateWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	_, err := cache.Insert(key, data)
	require.NoError(t, err)
	newData := testData{value: 2, dataSize: 4}

	// Act
	err = cache.UpdateWithoutChangingOrder(key, newData)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, newData, cache.LookUp(key))
}

func TestUpdateWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}

	// Act
	err := cache.UpdateWithoutChangingOrder(key, data)

	// Assert
	require.ErrorIs(t, err, ErrEntryNotExist)
}

func TestUpdateNilValue(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	_, err := cache.Insert(key, data)
	require.NoError(t, err)

	// Act
	err = cache.UpdateWithoutChangingOrder(key, nil)

	// Assert
	require.ErrorIs(t, err, ErrInvalidEntry)
}

func TestUpdateWhenSizeIsDifferent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	_, err := cache.Insert(key, data)
	require.NoError(t, err)
	newData := testData{value: 2, dataSize: 3}

	// Act
	err = cache.UpdateWithoutChangingOrder(key, newData)

	// Assert
	require.ErrorIs(t, err, ErrInvalidUpdateEntrySize)
}

func TestUpdateNotChangeOrder(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key1 := "burrito1"
	data1 := testData{value: 23, dataSize: 10}
	_, err := cache.Insert(key1, data1)
	require.NoError(t, err)

	key2 := "burrito2"
	data2 := testData{value: 2, dataSize: 40}
	_, err = cache.Insert(key2, data2)
	require.NoError(t, err)

	// Act
	newData := testData{value: 7, dataSize: 10}
	err = cache.UpdateWithoutChangingOrder(key1, newData)
	require.NoError(t, err)

	// Inserting again should evict key1 because key1 was updated without changing order (still LRU).
	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	evicted, err := cache.Insert(key3, data3)

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{7})
}

func TestLookUpWithoutChangingOrder_WhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	_, err := cache.Insert(key, data)
	require.NoError(t, err)

	// Act
	value := cache.LookUpWithoutChangingOrder(key)

	// Assert
	assert.Equal(t, data, value)
}

func TestLookUpWithoutChangingOrder_WhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key := "burrito"

	// Act
	value := cache.LookUpWithoutChangingOrder(key)

	// Assert
	assert.Nil(t, value)
}

func TestLookUpWithoutChangingOrder_NotChangeOrder(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key1 := "burrito1"
	data1 := testData{value: 23, dataSize: 10}
	_, err := cache.Insert(key1, data1)
	require.NoError(t, err)

	key2 := "burrito2"
	data2 := testData{value: 2, dataSize: 40}
	_, err = cache.Insert(key2, data2)
	require.NoError(t, err)

	// Act
	value := cache.LookUpWithoutChangingOrder(key1)
	assert.Equal(t, data1, value)

	// Inserting again should evict key1 because key1 was looked up without changing order.
	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	evicted, err := cache.Insert(key3, data3)

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{23})
}

func TestUpdateSize_Success(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	_, err := cache.Insert("file1", testData{value: 10, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Insert("file2", testData{value: 20, dataSize: 20})
	require.NoError(t, err)

	// Act
	err = cache.UpdateSize("file1", 10)
	require.NoError(t, err)

	// Order should be preserved: file1 is still LRU.
	// Inserting 20 more units (total size was 10+10+20 = 40, now 40+20 = 60 > 50) evicts file1.
	evicted, err := cache.Insert("file3", testData{value: 30, dataSize: 20})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{10})
}

func TestUpdateSize_NonExistentKey(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)

	// Act
	err := cache.UpdateSize("nonexistent", 10)

	// Assert
	require.ErrorIs(t, err, ErrEntryNotExist)
}

func TestUpdateSize_ExceedsMaxSize_Evicts(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t) // maxSize = 50
	_, err := cache.Insert("key1", testData{value: 1, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Insert("key2", testData{value: 2, dataSize: 25})
	require.NoError(t, err)

	// Act: currentSize was 45. Increasing key2 size by 10 makes currentSize = 55 > 50.
	// key1 (LRU) must be evicted immediately by UpdateSize to maintain the size invariant.
	err = cache.UpdateSize("key2", 10)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, cache.LookUp("key1"))
	assert.Equal(t, testData{value: 2, dataSize: 25}, cache.LookUp("key2"))
}

func TestNewAlias(t *testing.T) {
	// Arrange
	c := New(100)
	require.NotNil(t, c)

	// Act
	evicted, err := c.Insert("k", testData{value: 1, dataSize: 10})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)
	assert.Equal(t, testData{value: 1, dataSize: 10}, c.LookUp("k"))
}

func TestCheckInvariants_InvalidMaxSizePanic(t *testing.T) {
	t.Run("DefaultOptions", func(t *testing.T) {
		// Arrange, Act & Assert
		assert.Panics(t, func() {
			_ = NewMapCache(0)
		})
	})

	t.Run("InvariantsEnabled", func(t *testing.T) {
		// Arrange, Act & Assert
		assert.Panics(t, func() {
			_ = NewMapCache(0, WithInvariantChecking(true))
		})
	})

	t.Run("InvariantsDisabled", func(t *testing.T) {
		// Arrange, Act & Assert
		assert.Panics(t, func() {
			_ = NewMapCache(0, WithInvariantChecking(false))
		})
	})

	t.Run("NewAliasDefaultOptions", func(t *testing.T) {
		// Arrange, Act & Assert
		assert.Panics(t, func() {
			_ = New(0)
		})
	})
}

func TestCheckInvariants_PanicOnCorruption(t *testing.T) {
	t.Run("CurrentSizeExceedsMaxSize", func(t *testing.T) {
		// Arrange
		c := NewMapCache(10).(*mapCache)
		c.currentSize = 20

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("LengthMismatch", func(t *testing.T) {
		// Arrange
		c := NewMapCache(10).(*mapCache)
		c.index["dummy"] = nil

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("KeyMismatch", func(t *testing.T) {
		// Arrange
		c := NewMapCache(50).(*mapCache)
		e := c.entries.PushFront(&entry{key: "correctKey", value: testData{1, 5}, size: 5})
		c.index["wrongKey"] = e
		c.currentSize = 5

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("InvalidElementType", func(t *testing.T) {
		// Arrange
		c := NewMapCache(50).(*mapCache)
		e := c.entries.PushFront("not-an-entry-struct")
		c.index["someKey"] = e

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("SizeSumMismatch", func(t *testing.T) {
		// Arrange
		c := NewMapCache(500).(*mapCache)
		_, err := c.Insert("k1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		c.currentSize++

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})
}

func TestMapCache_EraseEntriesWithGivenPrefix_EmptyPrefixFastPath(t *testing.T) {
	// Arrange
	c := NewMapCache(1000, WithInvariantChecking(true)).(*mapCache)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("entry_%d", i)
		_, err := c.Insert(key, testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}
	require.Equal(t, uint64(200), c.currentSize)

	// Act
	c.EraseEntriesWithGivenPrefix("")

	// Assert
	assert.Equal(t, uint64(0), c.currentSize)
	assert.Equal(t, 0, c.entries.Len())
	assert.Empty(t, c.index)

	// Act & Assert: Erasing empty prefix on an already empty cache must be a safe no-op.
	c.EraseEntriesWithGivenPrefix("")
	assert.Equal(t, uint64(0), c.currentSize)
	assert.Equal(t, 0, c.entries.Len())
	assert.Empty(t, c.index)
}

func TestMapCache_Compact(t *testing.T) {
	// Arrange
	c := NewMapCache(1000, WithInvariantChecking(true)).(*mapCache)
	for i := 0; i < 20; i++ {
		_, err := c.Insert(fmt.Sprintf("entry_%d", i), testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}
	require.False(t, c.dirtyIndex)

	// Act & Assert 1: Clean Compact is a zero-allocation no-op (F07)
	allocs := testing.AllocsPerRun(20, func() {
		c.Compact()
	})
	assert.InDelta(t, 0.0, allocs, 1e-9)

	// Arrange 2: Delete half the entries to mark dirtyIndex
	for i := 0; i < 10; i++ {
		erased := c.Erase(fmt.Sprintf("entry_%d", i))
		require.NotNil(t, erased)
	}
	require.True(t, c.dirtyIndex)

	// Act 2: Compact dirty index
	c.Compact()

	// Assert 2
	assert.False(t, c.dirtyIndex)
	assert.Equal(t, uint64(100), c.currentSize)
	assert.Equal(t, 10, c.entries.Len())
	assert.Len(t, c.index, 10)
	for i := 10; i < 20; i++ {
		assert.Equal(t, testData{value: int64(i), dataSize: 10}, c.LookUp(fmt.Sprintf("entry_%d", i)))
	}
}

func TestMapCache_EvaluateMemoryPressure(t *testing.T) {
	// Arrange
	currentPressure := 0.10
	c := NewMapCache(
		100,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 { return currentPressure }),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
	).(*mapCache)

	for i := 0; i < 10; i++ {
		_, err := c.Insert(fmt.Sprintf("k%d", i), testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}
	_ = c.Erase("k0")
	_ = c.Erase("k1")
	require.True(t, c.dirtyIndex)
	require.Equal(t, uint64(80), c.currentSize)

	// Act & Assert 1: Below CompactionThreshold (0.50 < 0.75) does nothing
	currentPressure = 0.50
	evicted := c.EvaluateMemoryPressure()
	assert.Empty(t, evicted)
	assert.True(t, c.dirtyIndex)
	assert.Equal(t, uint64(80), c.currentSize)

	// Act & Assert 2: Tier 1 CompactionThreshold (0.80 in [0.75, 0.90)) compacts without evicting
	currentPressure = 0.80
	evicted = c.EvaluateMemoryPressure()
	assert.Empty(t, evicted)
	assert.False(t, c.dirtyIndex)
	assert.Equal(t, uint64(80), c.currentSize)

	// Act & Assert 3: Tier 2 EvictionThreshold (0.95 >= 0.90) sheds down to targetSize = 50 and compacts
	currentPressure = 0.95
	evicted = c.EvaluateMemoryPressure()
	assertEvictedValues(t, evicted, []int64{2, 3, 4})
	assert.False(t, c.dirtyIndex)
	assert.Equal(t, uint64(50), c.currentSize)

	// Act & Assert 4: Repeated EvaluateMemoryPressure when clean and at targetSize is a no-op (F07)
	epochBefore := c.reclaimEpoch.Load()
	evicted = c.EvaluateMemoryPressure()
	assert.Empty(t, evicted)
	assert.False(t, c.dirtyIndex)
	assert.Equal(t, epochBefore, c.reclaimEpoch.Load())
}
