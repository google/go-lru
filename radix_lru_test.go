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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const radixTestMaxSize = 50

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

func assertEvictedRadixValues(t *testing.T, evicted []ValueType, expectedValues []int64) {
	t.Helper()
	if len(expectedValues) == 0 {
		assert.Empty(t, evicted)
		return
	}
	require.Len(t, evicted, len(expectedValues))
	for i, exp := range expectedValues {
		actualData, ok := evicted[i].(radixTestData)
		if !ok {
			ptrData, ptrOk := evicted[i].(*radixTestData)
			require.True(t, ptrOk, "evicted value %d is not of type radixTestData: %T", i, evicted[i])
			actualData = *ptrData
		}
		assert.Equal(t, exp, actualData.value)
	}
}

func TestRadixCache_LookUpInEmptyCache(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)

	// Act
	valEmpty := cache.LookUp("")
	valTaco := cache.LookUp("taco")

	// Assert
	assert.Nil(t, valEmpty)
	assert.Nil(t, valTaco)
}

func TestRadixCache_InsertNilValue(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)

	// Act
	evicted, err := cache.Insert("taco", nil)

	// Assert
	require.ErrorIs(t, err, ErrInvalidEntry)
	assertEvictedRadixValues(t, evicted, nil)
}

func TestRadixCache_InsertEmptyKey(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)

	// Act
	evicted, err := cache.Insert("", radixTestData{value: 42, dataSize: 10})

	// Assert
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)
	assert.Equal(t, radixTestData{value: 42, dataSize: 10}, cache.LookUp(""))
	assert.Nil(t, cache.LookUp("taco"))
}

func TestRadixCache_LookUpUnknownKey(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Insert("burrito", radixTestData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	evicted, err = cache.Insert("taco", radixTestData{value: 23, dataSize: 8})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	// Act
	valEmpty := cache.LookUp("")
	valEnchilada := cache.LookUp("enchilada")

	// Assert
	assert.Nil(t, valEmpty)
	assert.Nil(t, valEnchilada)
}

func TestRadixCache_FillUpToCapacity(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)

	// Act
	evicted1, err1 := cache.Insert("burrito", radixTestData{value: 23, dataSize: 4})
	evicted2, err2 := cache.Insert("taco", radixTestData{value: 26, dataSize: 20})
	evicted3, err3 := cache.Insert("enchilada", radixTestData{value: 28, dataSize: 26})

	// Assert
	require.NoError(t, err1)
	assertEvictedRadixValues(t, evicted1, nil)
	require.NoError(t, err2)
	assertEvictedRadixValues(t, evicted2, nil)
	require.NoError(t, err3)
	assertEvictedRadixValues(t, evicted3, nil)

	assert.Equal(t, radixTestData{value: 23, dataSize: 4}, cache.LookUp("burrito"))
	assert.Equal(t, radixTestData{value: 26, dataSize: 20}, cache.LookUp("taco"))
	assert.Equal(t, radixTestData{value: 28, dataSize: 26}, cache.LookUp("enchilada"))
}

func TestRadixCache_ExpiresLeastRecentlyUsed(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Insert("burrito", radixTestData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	// Least recent.
	evicted, err = cache.Insert("taco", radixTestData{value: 26, dataSize: 20})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	// Second most recent.
	evicted, err = cache.Insert("enchilada", radixTestData{value: 28, dataSize: 26})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	assert.Equal(t, radixTestData{value: 23, dataSize: 4}, cache.LookUp("burrito"))

	// Act: Insert another, should evict taco (value 26).
	evicted, err = cache.Insert("queso", radixTestData{value: 34, dataSize: 5})

	// Assert
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, []int64{26})
	assert.Nil(t, cache.LookUp("taco"))
	assert.Equal(t, radixTestData{value: 23, dataSize: 4}, cache.LookUp("burrito"))
	assert.Equal(t, radixTestData{value: 28, dataSize: 26}, cache.LookUp("enchilada"))
	assert.Equal(t, radixTestData{value: 34, dataSize: 5}, cache.LookUp("queso"))
}

func TestRadixCache_Overwrite(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Insert("burrito", radixTestData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	evicted, err = cache.Insert("taco", radixTestData{value: 26, dataSize: 20})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	evicted, err = cache.Insert("enchilada", radixTestData{value: 28, dataSize: 20})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	evicted, err = cache.Insert("burrito", radixTestData{value: 33, dataSize: 6})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	// Act: Increase dataSize while modifying, so eviction should happen (taco evicted)
	evicted, err = cache.Insert("burrito", radixTestData{value: 33, dataSize: 12})

	// Assert
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, []int64{26})
	assert.Nil(t, cache.LookUp("taco"))
	assert.Equal(t, radixTestData{value: 33, dataSize: 12}, cache.LookUp("burrito"))
	assert.Equal(t, radixTestData{value: 28, dataSize: 20}, cache.LookUp("enchilada"))
}

func TestRadixCache_MultipleEviction(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Insert("burrito", radixTestData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	evicted, err = cache.Insert("taco", radixTestData{value: 26, dataSize: 20})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	evicted, err = cache.Insert("enchilada", radixTestData{value: 28, dataSize: 20})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	// Act: Inserting large data evicts all three existing items
	evicted, err = cache.Insert("large_data", radixTestData{value: 33, dataSize: 45})

	// Assert
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, []int64{23, 26, 28})
	assert.Nil(t, cache.LookUp("taco"))
	assert.Nil(t, cache.LookUp("burrito"))
	assert.Nil(t, cache.LookUp("enchilada"))
	assert.Equal(t, radixTestData{value: 33, dataSize: 45}, cache.LookUp("large_data"))
}

func TestRadixCache_WhenEntrySizeMoreThanCacheMaxSize(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Insert("burrito", radixTestData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	// Act: Insert entry with size greater than maxSize of cache.
	evicted, err = cache.Insert("taco", radixTestData{value: 26, dataSize: radixTestMaxSize + 1})

	// Assert
	require.ErrorIs(t, err, ErrInvalidEntrySize)
	assertEvictedRadixValues(t, evicted, nil)
	assert.Equal(t, radixTestData{value: 23, dataSize: 4}, cache.LookUp("burrito"))
}

func TestRadixCache_EraseWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Insert("burrito", radixTestData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, nil)

	// Act
	deletedEntry := cache.Erase("burrito")

	// Assert
	assert.Equal(t, radixTestData{value: 23, dataSize: 4}, deletedEntry)
	assert.Nil(t, cache.LookUp("burrito"))
}

func TestRadixCache_EraseCacheWithGivenPrefix(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	_, err := cache.Insert("a", radixTestData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("a/b", radixTestData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Insert("a/b/d", radixTestData{value: 22, dataSize: 6})
	require.NoError(t, err)
	_, err = cache.Insert("a/c", radixTestData{value: 20, dataSize: 6})
	require.NoError(t, err)
	_, err = cache.Insert("b", radixTestData{value: 21, dataSize: 2})
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

func TestRadixCache_EraseCacheWithEmptyPrefix(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	_, err := cache.Insert("a", radixTestData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("a/b", radixTestData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Insert("b", radixTestData{value: 21, dataSize: 2})
	require.NoError(t, err)

	// Act
	cache.EraseEntriesWithGivenPrefix("")

	// Assert
	assert.Nil(t, cache.LookUp("a"))
	assert.Nil(t, cache.LookUp("a/b"))
	assert.Nil(t, cache.LookUp("b"))
}

func TestRadixCache_EraseCacheWhereNoEntriesExistWithGivenPrefix(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	_, err := cache.Insert("a", radixTestData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("a/b", radixTestData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Insert("b", radixTestData{value: 21, dataSize: 2})
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

func TestRadixCache_EraseCacheWithGivenPrefixWithSomeEntriesEvictedDueToCacheSize(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	_, err := cache.Insert("a", radixTestData{value: 23, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Insert("a/b", radixTestData{value: 26, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Insert("a/b/d", radixTestData{value: 22, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Insert("a/c", radixTestData{value: 20, dataSize: 10})
	require.NoError(t, err)
	evicted, err := cache.Insert("b", radixTestData{value: 21, dataSize: 15})
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, []int64{23})

	// Act: Entry "a" was already evicted by insertion of "b". Erasing prefix "a" cleans remaining descendants.
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

func TestRadixCache_EraseWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	_, err := cache.Insert("burrito", radixTestData{value: 23, dataSize: 4})
	require.NoError(t, err)

	// Act
	deletedEntry := cache.Erase("taco")

	// Assert
	assert.Nil(t, deletedEntry)
	assert.Equal(t, radixTestData{value: 23, dataSize: 4}, cache.LookUp("burrito"))
}

func TestRadixCache_UpdateSize(t *testing.T) {
	t.Run("NonExistentKey", func(t *testing.T) {
		// Arrange
		cache := NewRadixCache(100, WithInvariantChecking(true))

		// Act
		err := cache.UpdateSize("key1", 20)

		// Assert
		require.ErrorIs(t, err, ErrEntryNotExist)
	})

	t.Run("Immediate Eviction", func(t *testing.T) {
		// Arrange
		cache := NewRadixCache(100, WithInvariantChecking(true))
		data1 := radixTestData{value: 1, dataSize: 10}
		data2 := radixTestData{value: 2, dataSize: 70}
		_, err := cache.Insert("key1", data1)
		require.NoError(t, err)
		_, err = cache.Insert("key2", data2)
		require.NoError(t, err)

		// Act
		errUpdate := cache.UpdateSize("key1", 30)

		// Assert
		require.NoError(t, errUpdate)
		assert.Nil(t, cache.LookUp("key1"))
		assert.NotNil(t, cache.LookUp("key2"))
	})
}

func TestRadixCache_UpdateSize_ExceedsMaxSize(t *testing.T) {
	// Arrange
	cache := NewRadixCache(100, WithInvariantChecking(true))
	data := radixTestData{value: 1, dataSize: 50}
	_, err := cache.Insert("file.txt", data)
	require.NoError(t, err)

	// Act
	err = cache.UpdateSize("file.txt", 100)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, cache.LookUp("file.txt"))
}

func TestRadixCache_UpdateWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := radixTestData{value: 23, dataSize: 4}
	_, err := cache.Insert(key, data)
	require.NoError(t, err)
	newData := radixTestData{value: 2, dataSize: 4}

	// Act
	err = cache.UpdateWithoutChangingOrder(key, newData)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, newData, cache.LookUp(key))
}

func TestRadixCache_UpdateWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := radixTestData{value: 23, dataSize: 4}

	// Act
	err := cache.UpdateWithoutChangingOrder(key, data)

	// Assert
	require.ErrorIs(t, err, ErrEntryNotExist)
}

func TestRadixCache_UpdateWhenSizeIsDifferent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := radixTestData{value: 23, dataSize: 4}
	_, err := cache.Insert(key, data)
	require.NoError(t, err)
	newData := radixTestData{value: 2, dataSize: 3}

	// Act
	err = cache.UpdateWithoutChangingOrder(key, newData)

	// Assert
	require.ErrorIs(t, err, ErrInvalidUpdateEntrySize)
}

func TestRadixCache_UpdateNotChangeOrder(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key1 := "burrito1"
	data1 := radixTestData{value: 23, dataSize: 10}
	_, err := cache.Insert(key1, data1)
	require.NoError(t, err)

	key2 := "burrito2"
	data2 := radixTestData{value: 2, dataSize: 40}
	_, err = cache.Insert(key2, data2)
	require.NoError(t, err)

	// Act
	newData := radixTestData{value: 7, dataSize: 10}
	err = cache.UpdateWithoutChangingOrder(key1, newData)
	require.NoError(t, err)

	// Inserting again should evict key1 because key1 was updated without changing order (still at tail)
	key3 := "burrito3"
	data3 := radixTestData{value: 3, dataSize: 5}
	evicted, err := cache.Insert(key3, data3)

	// Assert
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, []int64{7})
}

func TestRadixCache_UpdateSize_DoubleCountingDivergence(t *testing.T) {
	// Arrange
	const maxSize = 100
	const initialSize = 50
	const sizeDelta = 50 // New total size will be 50 + 50 = 100 (exactly at maxSize)

	radixCache := NewRadixCache(maxSize, WithInvariantChecking(true))
	_, err := radixCache.Insert("file.txt", radixTestData{value: 1, dataSize: initialSize})
	require.NoError(t, err)

	// Act: Grow tracked size via UpdateSize first, then update value payload at the new size
	err = radixCache.UpdateSize("file.txt", sizeDelta)
	require.NoError(t, err)

	err = radixCache.UpdateWithoutChangingOrder("file.txt", radixTestData{value: 2, dataSize: initialSize + sizeDelta})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, radixTestData{value: 2, dataSize: initialSize + sizeDelta}, radixCache.LookUp("file.txt"))
}

func TestRadixCache_LookUpWithoutChangingOrder_WhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := radixTestData{value: 23, dataSize: 4}
	_, err := cache.Insert(key, data)
	require.NoError(t, err)

	// Act
	value := cache.LookUpWithoutChangingOrder(key)

	// Assert
	assert.Equal(t, data, value)
}

func TestRadixCache_LookUpWithoutChangingOrder_WhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key := "burrito"

	// Act
	value := cache.LookUpWithoutChangingOrder(key)

	// Assert
	assert.Nil(t, value)
}

func TestRadixCache_LookUpWithoutChangingOrder_NotChangeOrder(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key1 := "burrito1"
	data1 := radixTestData{value: 23, dataSize: 10}
	_, err := cache.Insert(key1, data1)
	require.NoError(t, err)

	key2 := "burrito2"
	data2 := radixTestData{value: 2, dataSize: 40}
	_, err = cache.Insert(key2, data2)
	require.NoError(t, err)

	// Act
	value := cache.LookUpWithoutChangingOrder(key1)
	assert.Equal(t, data1, value)

	// Inserting again should evict key1 because key1 was looked up without changing order
	key3 := "burrito3"
	data3 := radixTestData{value: 3, dataSize: 5}
	evicted, err := cache.Insert(key3, data3)

	// Assert
	require.NoError(t, err)
	assertEvictedRadixValues(t, evicted, []int64{23})
}

func TestRadixCache_NewRadixCache_InvalidMaxSize(t *testing.T) {
	t.Run("DefaultOptions", func(t *testing.T) {
		// Arrange, Act & Assert
		assert.Panics(t, func() {
			_ = NewRadixCache(0)
		})
	})

	t.Run("InvariantsEnabled", func(t *testing.T) {
		// Arrange, Act & Assert
		assert.Panics(t, func() {
			_ = NewRadixCache(0, WithInvariantChecking(true))
		})
	})

	t.Run("InvariantsDisabled", func(t *testing.T) {
		// Arrange, Act & Assert
		assert.Panics(t, func() {
			_ = NewRadixCache(0, WithInvariantChecking(false))
		})
	})
}

func TestRadixCache_ComplexEdgeSplitsAndMerges(t *testing.T) {
	// Arrange
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
		require.NoError(t, err)
	}

	for i, k := range keys {
		assert.Equal(t, radixTestData{value: int64(i + 1), dataSize: 10}, cache.LookUp(k))
	}

	// Act 1: Erase prefix "car" -> should remove "car", "cart", "card", "carpet", "care", "careful"
	cache.EraseEntriesWithGivenPrefix("car")

	// Assert 1
	carKeys := []string{"car", "cart", "card", "carpet", "care", "careful"}
	for _, k := range carKeys {
		assert.Nil(t, cache.LookUp(k))
	}

	nonCarKeys := []string{"cat", "catch", "dog", "door", "dorm"}
	for _, k := range nonCarKeys {
		assert.NotNil(t, cache.LookUp(k))
	}

	// Act 2: Erase remaining keys one by one to verify compressPathUpwards in reverse
	for _, k := range nonCarKeys {
		v := cache.Erase(k)
		require.NotNil(t, v)
	}

	// Assert 2: Cache should now be completely empty
	for _, k := range keys {
		assert.Nil(t, cache.LookUp(k))
	}
}

func TestRadixCache_BinaryAndUnicodeKeys(t *testing.T) {
	// Arrange
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
		require.NoError(t, err)
	}

	for i, k := range unicodeKeys {
		assert.Equal(t, radixTestData{value: int64(i + 1), dataSize: 10}, cache.LookUp(k))
	}

	// Act: Erase prefix "日本語/"
	cache.EraseEntriesWithGivenPrefix("日本語/")

	// Assert
	assert.Nil(t, cache.LookUp("日本語/ディレクトリ/ファイル1"))
	assert.Nil(t, cache.LookUp("日本語/ディレクトリ/ファイル2"))
	assert.Nil(t, cache.LookUp("日本語/別のディレクトリ/ファイル3"))
	assert.NotNil(t, cache.LookUp("🚀/rocket/one"))
}

func TestRadixCache_UpdateWithoutChangingOrder_Errors(t *testing.T) {
	// Arrange
	cache := NewRadixCache(100, WithInvariantChecking(true))

	// Act
	err := cache.UpdateWithoutChangingOrder("any", nil)

	// Assert
	require.ErrorIs(t, err, ErrInvalidEntry)
}

func TestRadixCache_OptionsToggle(t *testing.T) {
	// Arrange
	cacheProd := NewRadixCache(100, WithInvariantChecking(false))
	cacheDebug := NewRadixCache(100, WithInvariantChecking(true))

	// Act
	_, errProd := cacheProd.Insert("k1", radixTestData{value: 1, dataSize: 10})
	_, errDebug := cacheDebug.Insert("k1", radixTestData{value: 1, dataSize: 10})

	// Assert
	require.NoError(t, errProd)
	require.NoError(t, errDebug)
}

func TestRadixCache_CheckInvariants_PanicScenarios(t *testing.T) {
	t.Run("CurrentSizeExceedsMaxSize", func(t *testing.T) {
		// Arrange
		c := NewRadixCache(10).(*radixCache)
		c.currentSize = 20

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("CorruptLRULinks", func(t *testing.T) {
		// Arrange
		c := NewRadixCache(50).(*radixCache)
		_, err := c.Insert("k1", radixTestData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert("k2", radixTestData{value: 2, dataSize: 10})
		require.NoError(t, err)
		c.head.next.prev = nil

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("CorruptTreeParent", func(t *testing.T) {
		// Arrange
		c := NewRadixCache(50).(*radixCache)
		_, err := c.Insert("a/b", radixTestData{value: 1, dataSize: 10})
		require.NoError(t, err)
		if c.root.child != nil {
			c.root.child.parent = nil
		}

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("SizeSumMismatch", func(t *testing.T) {
		// Arrange
		c := NewRadixCache(50).(*radixCache)
		_, err := c.Insert("k1", radixTestData{value: 1, dataSize: 10})
		require.NoError(t, err)
		c.currentSize++

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})
}
