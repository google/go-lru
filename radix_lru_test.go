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
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRadixCacheTest(t *testing.T) Cache[testData] {
	t.Helper()
	return NewRadixCache[testData](testMaxSize, WithInvariantChecking(true), testDataWeigher)
}

func TestRadixCache_GetInEmptyCache(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)

	// Act
	valEmpty, okEmpty := cache.Get("")
	valTaco, okTaco := cache.Get("taco")

	// Assert
	assert.False(t, okEmpty)
	assert.Equal(t, testData{}, valEmpty)
	assert.False(t, okTaco)
	assert.Equal(t, testData{}, valTaco)
}

func TestRadixCache_PutZeroAndNilSliceValue(t *testing.T) {
	// Arrange
	cache := NewRadixCache[[]byte](testMaxSize, WithInvariantChecking(true), WithWeigher(func(_ string, b []byte) uint64 {
		return uint64(len(b))
	}))

	// Act: nil slice is a valid value in a generic cache.
	evicted, err := cache.Put("taco", nil)

	// Assert
	require.NoError(t, err)
	assert.Empty(t, evicted)
	val, ok := cache.Get("taco")
	assert.True(t, ok)
	assert.Nil(t, val)
}

func TestRadixCache_PutEmptyKey(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)

	// Act
	evicted, err := cache.Put("", testData{value: 42, dataSize: 10})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)
	val, ok := cache.Get("")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 42, dataSize: 10}, val)
	_, ok = cache.Get("taco")
	assert.False(t, ok)
}

func TestRadixCache_GetUnknownKey(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Put("taco", testData{value: 23, dataSize: 8})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act
	valEmpty, okEmpty := cache.Get("")
	valEnchilada, okEnchilada := cache.Get("enchilada")

	// Assert
	assert.False(t, okEmpty)
	assert.Equal(t, testData{}, valEmpty)
	assert.False(t, okEnchilada)
	assert.Equal(t, testData{}, valEnchilada)
}

func TestRadixCache_FillUpToCapacity(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)

	// Act
	evicted1, err1 := cache.Put("burrito", testData{value: 23, dataSize: 4})
	evicted2, err2 := cache.Put("taco", testData{value: 26, dataSize: 20})
	evicted3, err3 := cache.Put("enchilada", testData{value: 28, dataSize: 26})

	// Assert
	require.NoError(t, err1)
	assertEvictedValues(t, evicted1, nil)
	require.NoError(t, err2)
	assertEvictedValues(t, evicted2, nil)
	require.NoError(t, err3)
	assertEvictedValues(t, evicted3, nil)

	val, ok := cache.Get("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, val)
	val, ok = cache.Get("taco")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 26, dataSize: 20}, val)
	val, ok = cache.Get("enchilada")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 28, dataSize: 26}, val)
}

func TestRadixCache_ExpiresLeastRecentlyUsed(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Least recent.
	evicted, err = cache.Put("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Second most recent.
	evicted, err = cache.Put("enchilada", testData{value: 28, dataSize: 26})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	val, ok := cache.Get("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, val)

	// Act: Put another, should evict taco (value 26).
	evicted, err = cache.Put("queso", testData{value: 34, dataSize: 5})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{26})
	_, ok = cache.Get("taco")
	assert.False(t, ok)
	val, ok = cache.Get("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, val)
	val, ok = cache.Get("enchilada")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 28, dataSize: 26}, val)
	val, ok = cache.Get("queso")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 34, dataSize: 5}, val)
}

func TestRadixCache_Overwrite(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Put("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Put("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Put("burrito", testData{value: 33, dataSize: 6})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act: Increase dataSize while modifying, so eviction should happen (taco evicted)
	evicted, err = cache.Put("burrito", testData{value: 33, dataSize: 12})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{26})
	_, ok := cache.Get("taco")
	assert.False(t, ok)
	val, ok := cache.Get("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 33, dataSize: 12}, val)
	val, ok = cache.Get("enchilada")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 28, dataSize: 20}, val)
}

func TestRadixCache_MultipleEviction(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Put("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	evicted, err = cache.Put("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act: Putting large data evicts all three existing items
	evicted, err = cache.Put("large_data", testData{value: 33, dataSize: 45})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{23, 26, 28})
	_, ok := cache.Get("taco")
	assert.False(t, ok)
	_, ok = cache.Get("burrito")
	assert.False(t, ok)
	_, ok = cache.Get("enchilada")
	assert.False(t, ok)
	val, ok := cache.Get("large_data")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 33, dataSize: 45}, val)
}

func TestRadixCache_WhenEntrySizeMoreThanCacheMaxSize(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act: Put entry with size greater than maxSize of cache.
	evicted, err = cache.Put("taco", testData{value: 26, dataSize: testMaxSize + 1})

	// Assert
	require.ErrorIs(t, err, ErrInvalidEntrySize)
	assertEvictedValues(t, evicted, nil)
	val, ok := cache.Get("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, val)
}

func TestRadixCache_DeleteWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	evicted, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	// Act
	deletedEntry, ok := cache.Delete("burrito")

	// Assert
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, deletedEntry)
	_, ok = cache.Get("burrito")
	assert.False(t, ok)
}

func TestRadixCache_DeletePrefix(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	_, err := cache.Put("a", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Put("a/b", testData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Put("a/b/d", testData{value: 22, dataSize: 6})
	require.NoError(t, err)
	_, err = cache.Put("a/c", testData{value: 20, dataSize: 6})
	require.NoError(t, err)
	_, err = cache.Put("b", testData{value: 21, dataSize: 2})
	require.NoError(t, err)

	// Act
	cache.DeletePrefix("a")

	// Assert
	_, ok := cache.Get("a")
	assert.False(t, ok)
	_, ok = cache.Get("a/b")
	assert.False(t, ok)
	_, ok = cache.Get("a/b/d")
	assert.False(t, ok)
	_, ok = cache.Get("a/c")
	assert.False(t, ok)
	valB, ok := cache.Get("b")
	require.True(t, ok)
	assert.Equal(t, uint64(2), valB.dataSize)
}

func TestRadixCache_DeletePrefixWithEmptyPrefix(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	_, err := cache.Put("a", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Put("a/b", testData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Put("b", testData{value: 21, dataSize: 2})
	require.NoError(t, err)

	// Act
	cache.DeletePrefix("")

	// Assert
	_, ok := cache.Get("a")
	assert.False(t, ok)
	_, ok = cache.Get("a/b")
	assert.False(t, ok)
	_, ok = cache.Get("b")
	assert.False(t, ok)
}

func TestRadixCache_DeletePrefixWhereNoEntriesExist(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	_, err := cache.Put("a", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Put("a/b", testData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Put("b", testData{value: 21, dataSize: 2})
	require.NoError(t, err)

	// Act
	cache.DeletePrefix("c")

	// Assert
	valA, ok := cache.Get("a")
	require.True(t, ok)
	assert.Equal(t, uint64(4), valA.dataSize)

	valAB, ok := cache.Get("a/b")
	require.True(t, ok)
	assert.Equal(t, uint64(5), valAB.dataSize)

	valB, ok := cache.Get("b")
	require.True(t, ok)
	assert.Equal(t, uint64(2), valB.dataSize)
}

func TestRadixCache_DeletePrefixWithSomeEntriesEvictedDueToCacheSize(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	_, err := cache.Put("a", testData{value: 23, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Put("a/b", testData{value: 26, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Put("a/b/d", testData{value: 22, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Put("a/c", testData{value: 20, dataSize: 10})
	require.NoError(t, err)
	evicted, err := cache.Put("b", testData{value: 21, dataSize: 15})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{23})

	// Act: Entry "a" was already evicted by insertion of "b". Deleting prefix "a" cleans remaining descendants.
	cache.DeletePrefix("a")

	// Assert
	_, ok := cache.Get("a")
	assert.False(t, ok)
	_, ok = cache.Get("a/b")
	assert.False(t, ok)
	_, ok = cache.Get("a/b/d")
	assert.False(t, ok)
	_, ok = cache.Get("a/c")
	assert.False(t, ok)
	valB, ok := cache.Get("b")
	require.True(t, ok)
	assert.Equal(t, uint64(15), valB.dataSize)
}

func TestRadixCache_DeleteWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	_, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)

	// Act
	deletedEntry, ok := cache.Delete("taco")

	// Assert
	assert.False(t, ok)
	assert.Equal(t, testData{}, deletedEntry)
	val, ok := cache.Get("burrito")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 23, dataSize: 4}, val)
}

func TestRadixCache_ReplaceGrowSize(t *testing.T) {
	t.Run("NonExistentKey", func(t *testing.T) {
		// Arrange
		cache := NewRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)

		// Act
		err := cache.Replace("key1", testData{value: 1, dataSize: 20})

		// Assert
		require.ErrorIs(t, err, ErrEntryNotExist)
	})

	t.Run("ImmediateEviction", func(t *testing.T) {
		// Arrange
		cache := NewRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)
		data1 := testData{value: 1, dataSize: 10}
		data2 := testData{value: 2, dataSize: 70}
		_, err := cache.Put("key1", data1)
		require.NoError(t, err)
		_, err = cache.Put("key2", data2)
		require.NoError(t, err)

		// Act: Grow key1 (at LRU tail) from 10 to 40 -> total 110 > 100 -> key1 evicts itself!
		errUpdate := cache.Replace("key1", testData{value: 11, dataSize: 40})

		// Assert
		require.NoError(t, errUpdate)
		_, ok := cache.Get("key1")
		assert.False(t, ok)
		_, ok = cache.Get("key2")
		assert.True(t, ok)
	})
}

func TestRadixCache_ReplaceGrowSize_ExceedsMaxSize(t *testing.T) {
	// Arrange
	cache := NewRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)
	data := testData{value: 1, dataSize: 50}
	_, err := cache.Put("file.txt", data)
	require.NoError(t, err)

	// Act
	err = cache.Replace("file.txt", testData{value: 2, dataSize: 150})

	// Assert: Exceeding maxSize self-evicts the entry and returns nil.
	require.NoError(t, err)
	_, ok := cache.Get("file.txt")
	assert.False(t, ok)
}

func TestRadixCache_ReplaceWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	_, err := cache.Put(key, data)
	require.NoError(t, err)
	newData := testData{value: 2, dataSize: 4}

	// Act
	err = cache.Replace(key, newData)

	// Assert
	require.NoError(t, err)
	val, ok := cache.Get(key)
	assert.True(t, ok)
	assert.Equal(t, newData, val)
}

func TestRadixCache_ReplaceWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}

	// Act
	err := cache.Replace(key, data)

	// Assert
	require.ErrorIs(t, err, ErrEntryNotExist)
}

func TestRadixCache_ReplaceWhenSizeShrinks(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 30}
	_, err := cache.Put(key, data)
	require.NoError(t, err)
	_, err = cache.Put("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)

	// Act: Shrink burrito from 30 to 10 (total size 50 -> 30).
	newData := testData{value: 2, dataSize: 10}
	err = cache.Replace(key, newData)
	require.NoError(t, err)

	// Putting 20 more units now fits without eviction.
	evicted, err := cache.Put("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	val, ok := cache.Peek(key)
	assert.True(t, ok)
	assert.Equal(t, newData, val)
}

func TestRadixCache_ReplaceNotChangeOrder(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key1 := "burrito1"
	data1 := testData{value: 23, dataSize: 10}
	_, err := cache.Put(key1, data1)
	require.NoError(t, err)

	key2 := "burrito2"
	data2 := testData{value: 2, dataSize: 40}
	_, err = cache.Put(key2, data2)
	require.NoError(t, err)

	// Act
	newData := testData{value: 7, dataSize: 10}
	err = cache.Replace(key1, newData)
	require.NoError(t, err)

	// Putting again should evict key1 because key1 was updated without changing order (still at tail)
	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	evicted, err := cache.Put(key3, data3)

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{7})
}

func TestRadixCache_ReplaceGrowToExactMaxSize(t *testing.T) {
	// Arrange
	const maxSize = 100
	const initialSize = 50
	const sizeDelta = 50 // New total size will be 50 + 50 = 100 (exactly at maxSize)

	radixCache := NewRadixCache[testData](maxSize, WithInvariantChecking(true), testDataWeigher)
	_, err := radixCache.Put("file.txt", testData{value: 1, dataSize: initialSize})
	require.NoError(t, err)

	// Act: Grow entry via Replace to exact maxSize
	err = radixCache.Replace("file.txt", testData{value: 2, dataSize: initialSize + sizeDelta})

	// Assert
	require.NoError(t, err)
	val, ok := radixCache.Get("file.txt")
	assert.True(t, ok)
	assert.Equal(t, testData{value: 2, dataSize: initialSize + sizeDelta}, val)
}

func TestRadixCache_Peek_WhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	_, err := cache.Put(key, data)
	require.NoError(t, err)

	// Act
	value, ok := cache.Peek(key)

	// Assert
	assert.True(t, ok)
	assert.Equal(t, data, value)
}

func TestRadixCache_Peek_WhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key := "burrito"

	// Act
	value, ok := cache.Peek(key)

	// Assert
	assert.False(t, ok)
	assert.Equal(t, testData{}, value)
}

func TestRadixCache_Peek_NotChangeOrder(t *testing.T) {
	// Arrange
	cache := setupRadixCacheTest(t)
	key1 := "burrito1"
	data1 := testData{value: 23, dataSize: 10}
	_, err := cache.Put(key1, data1)
	require.NoError(t, err)

	key2 := "burrito2"
	data2 := testData{value: 2, dataSize: 40}
	_, err = cache.Put(key2, data2)
	require.NoError(t, err)

	// Act
	value, ok := cache.Peek(key1)
	assert.True(t, ok)
	assert.Equal(t, data1, value)

	// Putting again should evict key1 because key1 was looked up without changing order
	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	evicted, err := cache.Put(key3, data3)

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{23})
}

func TestRadixCache_ComplexEdgeSplitsAndMerges(t *testing.T) {
	// Arrange
	cache := NewRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher)
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
		_, err := cache.Put(k, testData{value: int64(i + 1), dataSize: 10})
		require.NoError(t, err)
	}

	for i, k := range keys {
		val, ok := cache.Get(k)
		assert.True(t, ok)
		assert.Equal(t, testData{value: int64(i + 1), dataSize: 10}, val)
	}

	// Act 1: Delete prefix "car" -> should remove "car", "cart", "card", "carpet", "care", "careful"
	cache.DeletePrefix("car")

	// Assert 1
	carKeys := []string{"car", "cart", "card", "carpet", "care", "careful"}
	for _, k := range carKeys {
		_, ok := cache.Get(k)
		assert.False(t, ok)
	}

	nonCarKeys := []string{"cat", "catch", "dog", "door", "dorm"}
	for _, k := range nonCarKeys {
		_, ok := cache.Get(k)
		assert.True(t, ok)
	}

	// Act 2: Delete remaining keys one by one to verify compressPathUpwards in reverse
	for _, k := range nonCarKeys {
		_, ok := cache.Delete(k)
		require.True(t, ok)
	}

	// Assert 2: Cache should now be completely empty
	for _, k := range keys {
		_, ok := cache.Get(k)
		assert.False(t, ok)
	}
}

func TestRadixCache_BinaryAndUnicodeKeys(t *testing.T) {
	// Arrange
	cache := NewRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher)
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
		_, err := cache.Put(k, testData{value: int64(i + 1), dataSize: 10})
		require.NoError(t, err)
	}

	for i, k := range unicodeKeys {
		val, ok := cache.Get(k)
		assert.True(t, ok)
		assert.Equal(t, testData{value: int64(i + 1), dataSize: 10}, val)
	}

	// Act: Delete prefix "日本語/"
	cache.DeletePrefix("日本語/")

	// Assert
	_, ok := cache.Get("日本語/ディレクトリ/ファイル1")
	assert.False(t, ok)
	_, ok = cache.Get("日本語/ディレクトリ/ファイル2")
	assert.False(t, ok)
	_, ok = cache.Get("日本語/別のディレクトリ/ファイル3")
	assert.False(t, ok)
	_, ok = cache.Get("🚀/rocket/one")
	assert.True(t, ok)
}

func TestRadixCache_OptionsToggle(t *testing.T) {
	// Arrange
	cacheProd := NewRadixCache[testData](100, WithInvariantChecking(false), testDataWeigher)
	cacheDebug := NewRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)

	// Act
	_, errProd := cacheProd.Put("k1", testData{value: 1, dataSize: 10})
	_, errDebug := cacheDebug.Put("k1", testData{value: 1, dataSize: 10})

	// Assert
	require.NoError(t, errProd)
	require.NoError(t, errDebug)
}

// TestRadixCache_CheckInvariants_PanicScenarios verifies that radixCache.checkInvariants() detects
// and panics on internal tree, LRU list, and size accounting corruption.
//
// White-box testing rationale:
// All public Cache operations preserve radix tree and doubly-linked LRU invariants. Triggering
// the panic branches inside checkInvariants() requires directly mutating unexported radixCache
// and radixNode fields (currentSize, head.next.prev, root.child.parent, zeroSizeCount) to inject
// synthetic corruption. Without direct white-box verification, bugs in the invariant checker
// itself would go undetected.
func TestRadixCache_CheckInvariants_PanicScenarios(t *testing.T) {
	t.Run("CurrentSizeExceedsMaxSize", func(t *testing.T) {
		// Arrange
		c := NewRadixCache[testData](10, testDataWeigher).(*radixCache[testData])
		c.currentSize = 20

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("CorruptLRULinks", func(t *testing.T) {
		// Arrange
		c := NewRadixCache[testData](50, testDataWeigher).(*radixCache[testData])
		_, err := c.Put("k1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("k2", testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		c.head.next.prev = nil

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("CorruptTreeParent", func(t *testing.T) {
		// Arrange
		c := NewRadixCache[testData](50, testDataWeigher).(*radixCache[testData])
		_, err := c.Put("a/b", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		if c.root.child != nil {
			c.root.child.parent = nil
		}

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("ZeroSizeCountMismatch", func(t *testing.T) {
		// Arrange
		c := NewRadixCache[testData](100, testDataWeigher).(*radixCache[testData])
		_, err := c.Put("z", testData{value: 0, dataSize: 0})
		require.NoError(t, err)
		c.zeroSizeCount = 0

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("SizeSumMismatch", func(t *testing.T) {
		// Arrange
		c := NewRadixCache[testData](50, testDataWeigher).(*radixCache[testData])
		_, err := c.Put("k1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		c.currentSize++

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("RoutingNodeRetainsNonZeroValue", func(t *testing.T) {
		// Arrange
		c := NewRadixCache[testData](50, testDataWeigher).(*radixCache[testData])
		_, err := c.Put("ab", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("ac", testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		require.NotNil(t, c.root.child)
		require.False(t, c.root.child.hasValue)

		// Act
		c.root.child.value = testData{value: 99, dataSize: 10}

		// Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})
}

// TestRadixCache_RoutingPrefixDoesNotPinLargeKeyBackingArray verifies that splitting an edge
// created by a 64 KiB key clones the internal routing node's prefix string so erasing the 64 KiB
// key does not pin the caller's 64 KiB backing array in memory.
//
// White-box testing rationale:
// Public Cache lookups only inspect entry values and cannot observe whether an internal non-value
// routing node's prefix string header aliases the backing byte array of an erased key. Inspecting
// unsafe.StringData(c.root.child.prefix) deterministically verifies zero substring backing-array retention.
func TestRadixCache_RoutingPrefixDoesNotPinLargeKeyBackingArray(t *testing.T) {
	// Arrange
	largeKey := strings.Clone("dir/" + strings.Repeat("X", 1<<16))
	largeStart := uintptr(unsafe.Pointer(unsafe.StringData(largeKey)))
	largeEnd := largeStart + uintptr(len(largeKey))

	c := NewRadixCache[string](1<<20, WithInvariantChecking(true)).(*radixCache[string])
	_, err := c.Put(largeKey, "v")
	require.NoError(t, err)

	// Act: Split "dir/..." at "dir/" and erase the large key.
	_, err = c.Put("dir/a", "va")
	require.NoError(t, err)
	_, err = c.Put("dir/b", "vb")
	require.NoError(t, err)
	_, _ = c.Delete(largeKey)

	// Assert: Routing node prefix "dir/" does not alias largeKey's backing array.
	routingNode := c.root.child
	require.NotNil(t, routingNode)
	assert.Equal(t, "dir/", routingNode.prefix)
	prefixPtr := uintptr(unsafe.Pointer(unsafe.StringData(routingNode.prefix)))
	assert.True(t, prefixPtr < largeStart || prefixPtr >= largeEnd, "routing node prefix must not pin largeKey backing array")
}

func TestRadixCache_PrePutEvictionAvoidsRedundantSplitAndMergeAllocations(t *testing.T) {
	// Arrange
	c := NewRadixCache[testData](50, testDataWeigher)
	alphaKey := "prefix/alpha"
	betaKey := "prefix/beta"
	v1 := testData{value: 1, dataSize: 40}
	v2 := testData{value: 2, dataSize: 40}

	_, err := c.Put(alphaKey, v1)
	require.NoError(t, err)
	toggle := false

	// Act: Alternate inserting "prefix/beta" and "prefix/alpha" (40B each in a 50B cache),
	// forcing eviction of the sibling before insertNode.
	allocs := testing.AllocsPerRun(20, func() {
		if toggle {
			_, _ = c.Put(alphaKey, v1)
		} else {
			_, _ = c.Put(betaKey, v2)
		}
		toggle = !toggle
	})

	// Assert: Pre-eviction avoids splitNode + prefix split + compressPathUpwards string re-concatenation.
	assert.LessOrEqual(t, allocs, 3.0)
	_, ok := c.Peek(alphaKey)
	assert.False(t, ok)
	_, ok = c.Peek(betaKey)
	assert.True(t, ok)
}

// TestRadixCache_DetachedNodesClearPrefixesAndTreePointers verifies that detached and merged
// radixNode structs have their prefix strings, values, tree pointers, and LRU links cleared upon removal.
//
// White-box testing rationale:
// Public Cache methods return zero values for erased keys regardless of whether detached radixNode structs
// retain non-nil parent, child, sibling, prev, next, or prefix references. Inspecting the detached
// radixNode fields directly is necessary to deterministically verify that no pointer chains remain
// to pin adjacent live subtrees or key buffers in the Go heap.
func TestRadixCache_DetachedNodesClearPrefixesAndTreePointers(t *testing.T) {
	t.Run("CompressPathUpwardsClearsDetachedLeafAndMergedRoutingNodeReferences", func(t *testing.T) {
		// Arrange
		rc := NewRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*radixCache[testData])
		_, err := rc.Put("group/sub/item1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = rc.Put("group/sub/item2", testData{value: 2, dataSize: 10})
		require.NoError(t, err)

		routingNode := rc.root.getChild('g')
		require.NotNil(t, routingNode)
		leaf1 := routingNode.getChild('1')
		require.NotNil(t, leaf1)

		// Act: Delete "group/sub/item1", pruning leaf1 and merging routingNode into leaf2.
		_, ok := rc.Delete("group/sub/item1")

		// Assert: Both pruned leaf1 and merged routingNode must clear prefix string headers and tree pointers.
		require.True(t, ok)
		assert.Empty(t, leaf1.prefix)
		assert.Nil(t, leaf1.parent)
		assert.Empty(t, routingNode.prefix)
		assert.Nil(t, routingNode.child)
		assert.Nil(t, routingNode.parent)
	})

	t.Run("DeletePrefixClearsDetachedSubtreePrefixesAndPointers", func(t *testing.T) {
		// Arrange
		rc := NewRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*radixCache[testData])
		_, err := rc.Put("dir/sub/a", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = rc.Put("dir/sub/b", testData{value: 2, dataSize: 10})
		require.NoError(t, err)

		subtreeRoot := rc.root.getChild('d')
		require.NotNil(t, subtreeRoot)
		leafA := subtreeRoot.getChild('a')
		require.NotNil(t, leafA)
		leafB := subtreeRoot.getChild('b')
		require.NotNil(t, leafB)

		// Act
		rc.DeletePrefix("dir/")

		// Assert: Every node in the detached subtree must clear its prefix string header and tree pointers.
		assert.Empty(t, subtreeRoot.prefix)
		assert.Nil(t, subtreeRoot.child)
		assert.Nil(t, subtreeRoot.parent)
		assert.Empty(t, leafA.prefix)
		assert.Nil(t, leafA.parent)
		assert.Nil(t, leafA.sibling)
		assert.Empty(t, leafB.prefix)
		assert.Nil(t, leafB.parent)
	})

	t.Run("DeleteEmptyPrefixSweepsAndZeroesDetachedTrieNodes", func(t *testing.T) {
		// Arrange
		rc := NewRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*radixCache[testData])
		_, err := rc.Put("dir/sub/a", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = rc.Put("dir/sub/b", testData{value: 2, dataSize: 10})
		require.NoError(t, err)

		routingNode := rc.root.getChild('d')
		require.NotNil(t, routingNode)
		leafA := routingNode.getChild('a')
		require.NotNil(t, leafA)

		// Act
		rc.DeletePrefix("")

		// Assert: Detached radixNode structs must have prefix, value, tree pointers, and LRU pointers cleared.
		assert.Empty(t, routingNode.prefix)
		assert.Nil(t, routingNode.child)
		assert.Nil(t, routingNode.parent)
		assert.Empty(t, leafA.prefix)
		assert.False(t, leafA.hasValue)
		assert.Equal(t, testData{}, leafA.value)
		assert.Nil(t, leafA.parent)
		assert.Nil(t, leafA.prev)
		assert.Nil(t, leafA.next)
	})
}
