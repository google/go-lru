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

var testDataWeigher = WithWeigher(func(_ string, td testData) uint64 {
	return td.dataSize
})

func setupCacheTest(t *testing.T) Cache[testData] {
	t.Helper()
	return NewMapCache[testData](testMaxSize, WithInvariantChecking(true), testDataWeigher)
}

func assertEvictedValues(t *testing.T, evicted []testData, expectedValues []int64) {
	t.Helper()
	if len(expectedValues) == 0 {
		assert.Empty(t, evicted)
		return
	}
	require.Len(t, evicted, len(expectedValues))
	for i, exp := range expectedValues {
		assert.Equal(t, exp, evicted[i].value)
	}
}

func TestMapCache_LookUpInEmptyCache(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)

	// Act
	valEmpty, okEmpty := cache.LookUp("")
	valTaco, okTaco := cache.LookUp("taco")

	// Assert
	assert.False(t, okEmpty)
	assert.Equal(t, testData{}, valEmpty)
	assert.False(t, okTaco)
	assert.Equal(t, testData{}, valTaco)
}

func TestMapCache_InsertZeroAndNilSliceValue(t *testing.T) {
	// Arrange
	cache := NewMapCache[[]byte](testMaxSize, WithInvariantChecking(true), WithWeigher(func(_ string, b []byte) uint64 {
		return uint64(len(b))
	}))

	// Act: nil slice is a valid value in a generic cache.
	evicted, err := cache.Insert("taco", nil)

	// Assert
	require.NoError(t, err)
	assert.Empty(t, evicted)
	val, ok := cache.LookUp("taco")
	assert.True(t, ok)
	assert.Nil(t, val)
}

func TestMapCache_LookUpUnknownKey(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	evicted, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Insert("taco", testData{value: 23, dataSize: 8})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act
	valEmpty, okEmpty := cache.LookUp("")
	valEnchilada, okEnchilada := cache.LookUp("enchilada")

	// Assert
	assert.False(t, okEmpty)
	assert.Equal(t, testData{}, valEmpty)
	assert.False(t, okEnchilada)
	assert.Equal(t, testData{}, valEnchilada)
}

func TestMapCache_FillUpToCapacity(t *testing.T) {
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

	val, ok := cache.LookUp("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, val)
	val, ok = cache.LookUp("taco")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 26, dataSize: 20}, val)
	val, ok = cache.LookUp("enchilada")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 28, dataSize: 26}, val)
}

func TestMapCache_ExpiresLeastRecentlyUsed(t *testing.T) {
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
	val, ok := cache.LookUp("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, val)

	// Act: Insert another, requiring eviction of taco (size 20) to fit queso (size 5).
	evicted, err = cache.Insert("queso", testData{value: 34, dataSize: 5})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{26})
	_, ok = cache.LookUp("taco")
	assert.False(t, ok)
	val, ok = cache.LookUp("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, val)
	val, ok = cache.LookUp("enchilada")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 28, dataSize: 26}, val)
	val, ok = cache.LookUp("queso")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 34, dataSize: 5}, val)
}

func TestMapCache_Overwrite(t *testing.T) {
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
	_, ok := cache.LookUp("taco")
	assert.False(t, ok)
	val, ok := cache.LookUp("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 33, dataSize: 12}, val)
	val, ok = cache.LookUp("enchilada")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 28, dataSize: 20}, val)
}

func TestMapCache_MultipleEviction(t *testing.T) {
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
	_, ok := cache.LookUp("taco")
	assert.False(t, ok)
	_, ok = cache.LookUp("burrito")
	assert.False(t, ok)
	_, ok = cache.LookUp("enchilada")
	assert.False(t, ok)
	val, ok := cache.LookUp("large_data")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 33, dataSize: 45}, val)
}

func TestMapCache_WhenEntrySizeMoreThanCacheMaxSize(t *testing.T) {
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
	val, ok := cache.LookUp("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, val)
}

func TestMapCache_EraseWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	evicted, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act
	deletedEntry, ok := cache.Erase("burrito")

	// Assert
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, deletedEntry)
	_, ok = cache.LookUp("burrito")
	assert.False(t, ok)
}

func TestMapCache_EraseCacheWithGivenPrefix(t *testing.T) {
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
	_, ok := cache.LookUp("a")
	assert.False(t, ok)
	_, ok = cache.LookUp("a/b")
	assert.False(t, ok)
	_, ok = cache.LookUp("a/b/d")
	assert.False(t, ok)
	_, ok = cache.LookUp("a/c")
	assert.False(t, ok)
	valB, ok := cache.LookUp("b")
	require.True(t, ok)
	assert.Equal(t, uint64(2), valB.dataSize)
}

func TestMapCache_EraseCacheWhereNoEntriesExistWithGivenPrefix(t *testing.T) {
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
	valA, ok := cache.LookUp("a")
	require.True(t, ok)
	assert.Equal(t, uint64(4), valA.dataSize)

	valAB, ok := cache.LookUp("a/b")
	require.True(t, ok)
	assert.Equal(t, uint64(5), valAB.dataSize)

	valB, ok := cache.LookUp("b")
	require.True(t, ok)
	assert.Equal(t, uint64(2), valB.dataSize)
}

func TestMapCache_EraseCacheWithGivenPrefixWithSomeEntriesEvictedDueToCacheSize(t *testing.T) {
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
	_, ok := cache.LookUp("a")
	assert.False(t, ok)
	_, ok = cache.LookUp("a/b")
	assert.False(t, ok)
	_, ok = cache.LookUp("a/b/d")
	assert.False(t, ok)
	_, ok = cache.LookUp("a/c")
	assert.False(t, ok)
	valB, ok := cache.LookUp("b")
	require.True(t, ok)
	assert.Equal(t, uint64(15), valB.dataSize)
}

func TestMapCache_EraseCacheWithEmptyPrefix(t *testing.T) {
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
	_, ok := cache.LookUp("a")
	assert.False(t, ok)
	_, ok = cache.LookUp("b")
	assert.False(t, ok)
	_, ok = cache.LookUp("c")
	assert.False(t, ok)
}

func TestMapCache_EraseWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	_, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)

	// Act
	deletedEntry, ok := cache.Erase("taco")

	// Assert
	assert.False(t, ok)
	assert.Equal(t, testData{}, deletedEntry)
	val, ok := cache.LookUp("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, val)
}

func TestMapCache_UpdateWhenKeyPresent(t *testing.T) {
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
	val, ok := cache.LookUp(key)
	assert.True(t, ok)
	assert.Equal(t, newData, val)
}

func TestMapCache_UpdateWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}

	// Act
	err := cache.UpdateWithoutChangingOrder(key, data)

	// Assert
	require.ErrorIs(t, err, ErrEntryNotExist)
}

func TestMapCache_UpdateWhenSizeShrinks(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	_, err := cache.Insert("burrito", testData{value: 23, dataSize: 30})
	require.NoError(t, err)
	_, err = cache.Insert("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)

	// Act: Shrink burrito from 30 to 10 (total size 50 -> 30).
	newData := testData{value: 99, dataSize: 10}
	err = cache.UpdateWithoutChangingOrder("burrito", newData)
	require.NoError(t, err)

	// Inserting 20 more units now fits without eviction because total size is 30+20=50.
	evicted, err := cache.Insert("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	val, ok := cache.LookUpWithoutChangingOrder("burrito")
	assert.True(t, ok)
	assert.Equal(t, newData, val)
}

func TestMapCache_UpdateNotChangeOrder(t *testing.T) {
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

func TestMapCache_LookUpWithoutChangingOrder_WhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	_, err := cache.Insert(key, data)
	require.NoError(t, err)

	// Act
	value, ok := cache.LookUpWithoutChangingOrder(key)

	// Assert
	assert.True(t, ok)
	assert.Equal(t, data, value)
}

func TestMapCache_LookUpWithoutChangingOrder_WhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	key := "burrito"

	// Act
	value, ok := cache.LookUpWithoutChangingOrder(key)

	// Assert
	assert.False(t, ok)
	assert.Equal(t, testData{}, value)
}

func TestMapCache_LookUpWithoutChangingOrder_NotChangeOrder(t *testing.T) {
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
	value, ok := cache.LookUpWithoutChangingOrder(key1)
	assert.True(t, ok)
	assert.Equal(t, data1, value)

	// Inserting again should evict key1 because key1 was looked up without changing order.
	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	evicted, err := cache.Insert(key3, data3)

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{23})
}

func TestMapCache_UpdateGrowSize_Success(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	_, err := cache.Insert("file1", testData{value: 10, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Insert("file2", testData{value: 20, dataSize: 20})
	require.NoError(t, err)

	// Act: Grow file1 from 10 to 20 without changing order.
	err = cache.UpdateWithoutChangingOrder("file1", testData{value: 15, dataSize: 20})
	require.NoError(t, err)

	// Order should be preserved: file1 is still LRU.
	// Inserting 20 more units (total size was 20+20 = 40, now 40+20 = 60 > 50) evicts file1.
	evicted, err := cache.Insert("file3", testData{value: 30, dataSize: 20})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{15})
}

func TestMapCache_UpdateGrowSize_ExceedsMaxSize_SelfEvicts(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t)
	_, err := cache.Insert("file1", testData{value: 10, dataSize: 10})
	require.NoError(t, err)

	// Act
	err = cache.UpdateWithoutChangingOrder("file1", testData{value: 99, dataSize: testMaxSize + 1})

	// Assert: Exceeding maxSize self-evicts the entry and returns nil.
	require.NoError(t, err)
	_, ok := cache.LookUpWithoutChangingOrder("file1")
	assert.False(t, ok)
}

func TestMapCache_UpdateGrowSize_EvictsOlderAndSelf(t *testing.T) {
	// Arrange
	cache := setupCacheTest(t) // maxSize = 50
	_, err := cache.Insert("key1", testData{value: 1, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Insert("key2", testData{value: 2, dataSize: 25})
	require.NoError(t, err)

	// Act 1: Increasing key2 size from 25 to 35 makes total 55 > 50.
	// key1 (LRU) must be evicted immediately to maintain the size invariant.
	err = cache.UpdateWithoutChangingOrder("key2", testData{value: 22, dataSize: 35})

	// Assert 1
	require.NoError(t, err)
	_, ok := cache.LookUp("key1")
	assert.False(t, ok)
	val, ok := cache.LookUp("key2")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 22, dataSize: 35}, val)

	// Arrange 2: Insert key3 (size 15), so key2 (size 35) is now LRU tail and key3 is MRU.
	_, err = cache.Insert("key3", testData{value: 3, dataSize: 15})
	require.NoError(t, err)

	// Act 2: Grow key2 (at LRU tail) from 35 to 40 -> total 55 > 50 -> key2 evicts itself!
	err = cache.UpdateWithoutChangingOrder("key2", testData{value: 222, dataSize: 40})
	require.NoError(t, err)
	_, ok = cache.LookUp("key2")
	assert.False(t, ok)
	val3, ok := cache.LookUp("key3")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 3, dataSize: 15}, val3)
}

// TestMapCache_CheckInvariants_PanicOnCorruption verifies that mapCache.checkInvariants() detects and panics
// on internal data structure corruption across all checked invariants.
//
// White-box testing rationale:
// All public Cache methods maintain internal size, cardinality, pointer, and type invariants.
// Exercising the panic branches inside checkInvariants() therefore requires directly mutating
// unexported mapCache fields (currentSize, index, entries, zeroSizeCount) to inject synthetic
// corruption. Without direct white-box testing of checkInvariants(), a defect in the invariant
// validator itself could mask silent state corruption during development and fuzzing.
func TestMapCache_CheckInvariants_PanicOnCorruption(t *testing.T) {
	t.Run("CurrentSizeExceedsMaxSize", func(t *testing.T) {
		// Arrange
		c := NewMapCache[testData](10, testDataWeigher).(*mapCache[testData])
		c.currentSize = 20

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("LengthMismatch", func(t *testing.T) {
		// Arrange
		c := NewMapCache[testData](10, testDataWeigher).(*mapCache[testData])
		c.index["dummy"] = nil

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("KeyMismatch", func(t *testing.T) {
		// Arrange
		c := NewMapCache[testData](50, testDataWeigher).(*mapCache[testData])
		e := c.entries.PushFront(&entry[testData]{key: "correctKey", value: testData{1, 5}, size: 5})
		c.index["wrongKey"] = e
		c.currentSize = 5

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("InvalidElementType", func(t *testing.T) {
		// Arrange
		c := NewMapCache[testData](50, testDataWeigher).(*mapCache[testData])
		e := c.entries.PushFront("not-an-entry-struct")
		c.index["someKey"] = e

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("ValueTypeEntryInsteadOfPointer", func(t *testing.T) {
		// Arrange
		c := NewMapCache[testData](100, WithInvariantChecking(true), testDataWeigher).(*mapCache[testData])
		el := c.entries.PushFront(entry[testData]{
			key:   "bad_value_type",
			value: testData{value: 1, dataSize: 5},
			size:  5,
		})
		c.index["bad_value_type"] = el
		c.currentSize = 5

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("ZeroSizeCountMismatch", func(t *testing.T) {
		// Arrange
		c := NewMapCache[testData](100, testDataWeigher).(*mapCache[testData])
		_, err := c.Insert("z", testData{value: 0, dataSize: 0})
		require.NoError(t, err)
		c.zeroSizeCount = 0

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("SizeSumMismatch", func(t *testing.T) {
		// Arrange
		c := NewMapCache[testData](500, testDataWeigher).(*mapCache[testData])
		_, err := c.Insert("k1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		c.currentSize++

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})
}

func TestMapCache_Compact(t *testing.T) {
	// Arrange
	probe := newPressureProbe(0.10)
	c := NewMapCache[testData](1000, WithInvariantChecking(true), testDataWeigher, probe.Option()).(PressureAwareCache[testData])
	for i := 0; i < 20; i++ {
		_, err := c.Insert(fmt.Sprintf("entry_%d", i), testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}

	// Act & Assert 1: Clean Compact is a zero-allocation no-op
	assertAlreadyCompacted(t, c, probe)

	// Arrange 2: Delete half the entries to dirty the index
	for i := 0; i < 10; i++ {
		_, ok := c.Erase(fmt.Sprintf("entry_%d", i))
		require.True(t, ok)
	}

	// Act 2: Compact dirty index (re-allocates map once and advances reclaimEpoch, then subsequent Compact is zero-alloc)
	assert.True(t, probe.ObserveEpochAdvance(t, c, c.Compact))

	// Assert 2
	assertAlreadyCompacted(t, c, probe)
	for i := 0; i < 10; i++ {
		_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("entry_%d", i))
		assert.False(t, ok)
	}
	for i := 10; i < 20; i++ {
		val, ok := c.LookUp(fmt.Sprintf("entry_%d", i))
		assert.True(t, ok)
		assert.Equal(t, testData{value: int64(i), dataSize: 10}, val)
	}
}

func TestMapCache_EvaluateMemoryPressure(t *testing.T) {
	// Arrange
	probe := newPressureProbe(0.10)
	c := NewMapCache[testData](
		100,
		WithInvariantChecking(true),
		testDataWeigher,
		probe.Option(),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
	).(PressureAwareCache[testData])

	for i := 0; i < 10; i++ {
		_, err := c.Insert(fmt.Sprintf("k%d", i), testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}
	_, ok := c.Erase("k0")
	require.True(t, ok)
	_, ok = c.Erase("k1")
	require.True(t, ok)

	// Act & Assert 1: Below CompactionThreshold (0.50 < 0.75) does nothing
	probe.Set(0.50)
	var evicted []testData
	advancedBelow := probe.ObserveEpochAdvance(t, c, func() {
		evicted = c.EvaluateMemoryPressure()
	})
	assert.Empty(t, evicted)
	assert.False(t, advancedBelow)

	// Act & Assert 2: Tier 1 CompactionThreshold (0.80 in [0.75, 0.90)) compacts without evicting
	probe.Set(0.80)
	advancedTier1 := probe.ObserveEpochAdvance(t, c, func() {
		evicted = c.EvaluateMemoryPressure()
	})
	assert.Empty(t, evicted)
	assert.True(t, advancedTier1)
	assertAlreadyCompacted(t, c, probe)
	for i := 2; i < 10; i++ {
		_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k%d", i))
		assert.True(t, ok)
	}

	// Act & Assert 3: Tier 2 EvictionThreshold (0.95 >= 0.90) sheds down to targetSize = 50 and compacts
	probe.Set(0.95)
	advancedTier2 := probe.ObserveEpochAdvance(t, c, func() {
		evicted = c.EvaluateMemoryPressure()
	})
	assertEvictedValues(t, evicted, []int64{2, 3, 4})
	assert.True(t, advancedTier2)
	assertAlreadyCompacted(t, c, probe)
	for i := 2; i < 5; i++ {
		_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k%d", i))
		assert.False(t, ok)
	}
	for i := 5; i < 10; i++ {
		_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k%d", i))
		assert.True(t, ok)
	}

	// Act & Assert 4: Repeated EvaluateMemoryPressure when clean and at targetSize is a no-op
	advancedRepeat := probe.ObserveEpochAdvance(t, c, func() {
		evicted = c.EvaluateMemoryPressure()
	})
	assert.Empty(t, evicted)
	assert.False(t, advancedRepeat)
	assertAlreadyCompacted(t, c, probe)
}
