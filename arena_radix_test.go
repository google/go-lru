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
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupArenaRadixCacheTest(t *testing.T) Cache[testData] {
	t.Helper()
	return NewArenaRadixCache[testData](testMaxSize, WithInvariantChecking(true), testDataWeigher)
}

func TestArenaRadixCache_LookUpInEmptyCache(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)

	// Act
	emptyVal, okEmpty := cache.LookUp("")
	tacoVal, okTaco := cache.LookUp("taco")

	// Assert
	assert.False(t, okEmpty)
	assert.Equal(t, testData{}, emptyVal)
	assert.False(t, okTaco)
	assert.Equal(t, testData{}, tacoVal)
}

func TestArenaRadixCache_InsertZeroAndNilSliceValue(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[[]byte](testMaxSize, WithInvariantChecking(true), WithWeigher(func(_ string, b []byte) uint64 {
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

func TestArenaRadixCache_InsertEmptyKey(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)

	// Act
	evicted, err := cache.Insert("", testData{value: 42, dataSize: 10})
	val, ok := cache.LookUp("")
	_, okTaco := cache.LookUp("taco")

	// Assert
	require.NoError(t, err)
	assert.Empty(t, evicted)
	require.True(t, ok)
	assert.Equal(t, int64(42), val.value)
	assert.False(t, okTaco)
}

func TestArenaRadixCache_LookUpUnknownKey(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("taco", testData{value: 23, dataSize: 8})
	require.NoError(t, err)

	// Act
	emptyVal, okEmpty := cache.LookUp("")
	enchiladaVal, okEnchilada := cache.LookUp("enchilada")

	// Assert
	assert.False(t, okEmpty)
	assert.Equal(t, testData{}, emptyVal)
	assert.False(t, okEnchilada)
	assert.Equal(t, testData{}, enchiladaVal)
}

func TestArenaRadixCache_FillUpToCapacity(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)

	// Act
	ev1, err1 := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	ev2, err2 := cache.Insert("taco", testData{value: 26, dataSize: 20})
	ev3, err3 := cache.Insert("enchilada", testData{value: 28, dataSize: 26})

	// Assert
	require.NoError(t, err1)
	assert.Empty(t, ev1)
	require.NoError(t, err2)
	assert.Empty(t, ev2)
	require.NoError(t, err3)
	assert.Empty(t, ev3)

	v1, ok := cache.LookUp("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(23), v1.value)

	v2, ok := cache.LookUp("taco")
	require.True(t, ok)
	assert.Equal(t, int64(26), v2.value)

	v3, ok := cache.LookUp("enchilada")
	require.True(t, ok)
	assert.Equal(t, int64(28), v3.value)
}

func TestArenaRadixCache_ExpiresLeastRecentlyUsed(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Insert("enchilada", testData{value: 28, dataSize: 26})
	require.NoError(t, err)

	// Promote burrito to MRU
	promoted, ok := cache.LookUp("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(23), promoted.value)

	// Act: Insert another item; taco (least recent) should be evicted
	evicted, err := cache.Insert("queso", testData{value: 34, dataSize: 5})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{26})
	_, ok = cache.LookUp("taco")
	assert.False(t, ok)

	vBurrito, ok := cache.LookUp("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(23), vBurrito.value)

	vEnchilada, ok := cache.LookUp("enchilada")
	require.True(t, ok)
	assert.Equal(t, int64(28), vEnchilada.value)

	vQueso, ok := cache.LookUp("queso")
	require.True(t, ok)
	assert.Equal(t, int64(34), vQueso.value)
}

func TestArenaRadixCache_Overwrite(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Insert("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)
	ev1, err := cache.Insert("burrito", testData{value: 33, dataSize: 6})
	require.NoError(t, err)
	assert.Empty(t, ev1)

	// Act: Increase size during overwrite; taco should be evicted
	evicted, err := cache.Insert("burrito", testData{value: 33, dataSize: 12})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{26})
	_, ok := cache.LookUp("taco")
	assert.False(t, ok)

	vBurrito, ok := cache.LookUp("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(33), vBurrito.value)

	vEnchilada, ok := cache.LookUp("enchilada")
	require.True(t, ok)
	assert.Equal(t, int64(28), vEnchilada.value)
}

func TestArenaRadixCache_MultipleEviction(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Insert("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)

	// Act: Large insert requiring all previous entries to be evicted
	evicted, err := cache.Insert("large_data", testData{value: 33, dataSize: 45})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{23, 26, 28})
	_, ok := cache.LookUp("taco")
	assert.False(t, ok)
	_, ok = cache.LookUp("burrito")
	assert.False(t, ok)
	_, ok = cache.LookUp("enchilada")
	assert.False(t, ok)

	vLarge, ok := cache.LookUp("large_data")
	require.True(t, ok)
	assert.Equal(t, int64(33), vLarge.value)
}

func TestArenaRadixCache_WhenEntrySizeMoreThanCacheMaxSize(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)

	// Act: Attempt inserting item with size > testMaxSize
	evicted, err := cache.Insert("taco", testData{value: 26, dataSize: testMaxSize + 1})

	// Assert
	require.ErrorIs(t, err, ErrInvalidEntrySize)
	assert.Empty(t, evicted)

	vBurrito, ok := cache.LookUp("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(23), vBurrito.value)
}

func TestArenaRadixCache_EraseWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)

	// Act
	deletedEntry, ok := cache.Erase("burrito")

	// Assert
	require.True(t, ok)
	assert.Equal(t, int64(23), deletedEntry.value)
	_, ok = cache.LookUp("burrito")
	assert.False(t, ok)
}

func TestArenaRadixCache_EraseWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Insert("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)

	// Act
	deletedEntry, ok := cache.Erase("taco")

	// Assert
	assert.False(t, ok)
	assert.Equal(t, testData{}, deletedEntry)
	vBurrito, ok := cache.LookUp("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(23), vBurrito.value)
}

func TestArenaRadixCache_EraseCacheWithGivenPrefix(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
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

	vb, ok := cache.LookUp("b")
	require.True(t, ok)
	assert.Equal(t, uint64(2), vb.dataSize)
}

func TestArenaRadixCache_EraseCacheWithEmptyPrefix(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Insert("a", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("a/b", testData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Insert("b", testData{value: 21, dataSize: 2})
	require.NoError(t, err)

	// Act
	cache.EraseEntriesWithGivenPrefix("")

	// Assert
	_, ok := cache.LookUp("a")
	assert.False(t, ok)
	_, ok = cache.LookUp("a/b")
	assert.False(t, ok)
	_, ok = cache.LookUp("b")
	assert.False(t, ok)
}

func TestArenaRadixCache_EraseCacheWhereNoEntriesExistWithGivenPrefix(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Insert("a", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Insert("a/b", testData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Insert("b", testData{value: 21, dataSize: 2})
	require.NoError(t, err)

	// Act
	cache.EraseEntriesWithGivenPrefix("c")

	// Assert
	va, ok := cache.LookUp("a")
	require.True(t, ok)
	assert.Equal(t, uint64(4), va.dataSize)

	vab, ok := cache.LookUp("a/b")
	require.True(t, ok)
	assert.Equal(t, uint64(5), vab.dataSize)

	vb, ok := cache.LookUp("b")
	require.True(t, ok)
	assert.Equal(t, uint64(2), vb.dataSize)
}

func TestArenaRadixCache_EraseCacheWithGivenPrefixWithSomeEntriesEvictedDueToCacheSize(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
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

	// Act: "a" was evicted by "b", remaining "a/b", "a/b/d", "a/c" should be erased
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

	vb, ok := cache.LookUp("b")
	require.True(t, ok)
	assert.Equal(t, uint64(15), vb.dataSize)
}

func TestArenaRadixCache_UpdateGrowSize(t *testing.T) {
	t.Run("NonExistentKey", func(t *testing.T) {
		// Arrange
		cache := NewArenaRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)

		// Act
		err := cache.UpdateWithoutChangingOrder("key1", testData{value: 1, dataSize: 20})

		// Assert
		require.ErrorIs(t, err, ErrEntryNotExist)
	})

	t.Run("ImmediateEviction", func(t *testing.T) {
		// Arrange
		cache := NewArenaRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)
		data1 := testData{value: 1, dataSize: 10}
		data2 := testData{value: 2, dataSize: 70}
		_, err := cache.Insert("key1", data1)
		require.NoError(t, err)
		_, err = cache.Insert("key2", data2)
		require.NoError(t, err)

		// Act: Grow key1 (at LRU tail) from 10 to 40 -> total 110 > 100 -> key1 evicts itself!
		errUpdate := cache.UpdateWithoutChangingOrder("key1", testData{value: 11, dataSize: 40})

		// Assert
		require.NoError(t, errUpdate)
		_, ok := cache.LookUp("key1")
		assert.False(t, ok)
		_, ok = cache.LookUp("key2")
		assert.True(t, ok)
	})
}

func TestArenaRadixCache_UpdateGrowSize_ExceedsMaxSize(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)
	data := testData{value: 1, dataSize: 50}
	_, err := cache.Insert("file.txt", data)
	require.NoError(t, err)

	// Act
	err = cache.UpdateWithoutChangingOrder("file.txt", testData{value: 2, dataSize: 150})

	// Assert: Exceeding maxSize self-evicts the entry and returns nil.
	require.NoError(t, err)
	_, ok := cache.LookUp("file.txt")
	assert.False(t, ok)
}

func TestArenaRadixCache_UpdateGrowSize_MultipleEvictions(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)
	_, err := cache.Insert("k1", testData{value: 1, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Insert("k2", testData{value: 2, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Insert("k3", testData{value: 3, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Insert("k4", testData{value: 4, dataSize: 20})
	require.NoError(t, err)

	// Act: Grow k4 from 20 to 70 (+50 delta) -> evicts k1 and k2.
	err = cache.UpdateWithoutChangingOrder("k4", testData{value: 44, dataSize: 70})

	// Assert
	require.NoError(t, err)
	_, ok := cache.LookUp("k1")
	assert.False(t, ok)
	_, ok = cache.LookUp("k2")
	assert.False(t, ok)
	_, ok = cache.LookUp("k3")
	assert.True(t, ok)
	_, ok = cache.LookUp("k4")
	assert.True(t, ok)
}

func TestArenaRadixCache_UpdateWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	_, err := cache.Insert(key, data)
	require.NoError(t, err)

	// Act
	newData := testData{value: 2, dataSize: 4}
	err = cache.UpdateWithoutChangingOrder(key, newData)

	// Assert
	require.NoError(t, err)
	v, ok := cache.LookUp(key)
	require.True(t, ok)
	assert.Equal(t, int64(2), v.value)
}

func TestArenaRadixCache_UpdateWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}

	// Act
	err := cache.UpdateWithoutChangingOrder(key, data)

	// Assert
	require.ErrorIs(t, err, ErrEntryNotExist)
}

func TestArenaRadixCache_UpdateWhenSizeShrinks(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 30}
	_, err := cache.Insert(key, data)
	require.NoError(t, err)
	_, err = cache.Insert("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)

	// Act: Shrink burrito from 30 to 10 (total size 50 -> 30).
	newData := testData{value: 2, dataSize: 10}
	err = cache.UpdateWithoutChangingOrder(key, newData)
	require.NoError(t, err)

	// Inserting 20 more units now fits without eviction.
	evicted, err := cache.Insert("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, nil)

	val, ok := cache.LookUpWithoutChangingOrder(key)
	assert.True(t, ok)
	assert.Equal(t, newData, val)
}

func TestArenaRadixCache_UpdateNotChangeOrder(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	key1 := "burrito1"
	data1 := testData{value: 23, dataSize: 10}
	_, err := cache.Insert(key1, data1)
	require.NoError(t, err)
	key2 := "burrito2"
	data2 := testData{value: 2, dataSize: 40}
	_, err = cache.Insert(key2, data2)
	require.NoError(t, err)

	// Act: Update key1 without changing order, then insert key3 (size 5)
	newData := testData{value: 7, dataSize: 10}
	err = cache.UpdateWithoutChangingOrder(key1, newData)
	require.NoError(t, err)

	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	evicted, err := cache.Insert(key3, data3)

	// Assert: key1 (value 7) is evicted because key1 remained the LRU element
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{7})
}

func TestArenaRadixCache_LookUpWithoutChangingOrder(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, ok := cache.LookUpWithoutChangingOrder("absent")
	assert.False(t, ok)

	key1 := "burrito1"
	data1 := testData{value: 23, dataSize: 10}
	_, err := cache.Insert(key1, data1)
	require.NoError(t, err)
	key2 := "burrito2"
	data2 := testData{value: 2, dataSize: 40}
	_, err = cache.Insert(key2, data2)
	require.NoError(t, err)

	// Act: LookUpWithoutChangingOrder on key1, then insert key3 (size 5)
	val, ok := cache.LookUpWithoutChangingOrder(key1)
	require.True(t, ok)
	assert.Equal(t, int64(23), val.value)

	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	evicted, err := cache.Insert(key3, data3)

	// Assert: key1 is evicted because its LRU position was not altered
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{23})
}

func TestArenaRadixCache_RadixEdgeSplitsAndCompression(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher)
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
		_, err := cache.Insert(k, testData{value: int64(i + 1), dataSize: 10})
		require.NoError(t, err)
	}

	for i, k := range keys {
		v, ok := cache.LookUp(k)
		require.True(t, ok)
		assert.Equal(t, int64(i+1), v.value)
	}

	// Act: Erase leaves and intermediate nodes to exercise compressPathUpwards
	vAppn, okAppn := cache.Erase("application")
	vApply, okApply := cache.Erase("apply")
	vApp, okApp := cache.Erase("app")

	// Assert
	require.True(t, okAppn)
	assert.Equal(t, int64(3), vAppn.value)
	require.True(t, okApply)
	assert.Equal(t, int64(4), vApply.value)
	require.True(t, okApp)
	assert.Equal(t, int64(2), vApp.value)

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
		v, ok := cache.LookUp(tc.key)
		require.True(t, ok)
		assert.Equal(t, tc.val, v.value)
	}
}

func TestArenaRadixCache_FreeListRecycling(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[testData](10000, WithInvariantChecking(true), testDataWeigher)

	// Act & Assert
	for range 10 {
		for i := range 50 {
			key := fmt.Sprintf("prefix/subdir_%d/file_%d.txt", i%5, i)
			_, err := cache.Insert(key, testData{value: int64(i), dataSize: 10})
			require.NoError(t, err)
		}

		for i := range 50 {
			key := fmt.Sprintf("prefix/subdir_%d/file_%d.txt", i%5, i)
			_, ok := cache.Erase(key)
			require.True(t, ok)
		}
	}
}

func TestArenaRadixCache_DeepHierarchy(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[testData](10000, WithInvariantChecking(true), testDataWeigher)

	path1 := "a/b/c/d/e/f/g/h/i/j/file1.txt"
	path2 := "a/b/c/d/e/f/g/h/i/j/file2.txt"
	path3 := "a/b/c/d/other/file3.txt"
	path4 := "x/y/z/file4.txt"

	_, err := cache.Insert(path1, testData{value: 1, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Insert(path2, testData{value: 2, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Insert(path3, testData{value: 3, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Insert(path4, testData{value: 4, dataSize: 10})
	require.NoError(t, err)

	// Act
	cache.EraseEntriesWithGivenPrefix("a/b/c/d/e/")

	// Assert
	_, ok := cache.LookUp(path1)
	assert.False(t, ok)
	_, ok = cache.LookUp(path2)
	assert.False(t, ok)

	v3, ok := cache.LookUp(path3)
	require.True(t, ok)
	assert.Equal(t, int64(3), v3.value)

	v4, ok := cache.LookUp(path4)
	require.True(t, ok)
	assert.Equal(t, int64(4), v4.value)
}

func TestArenaRadixCache_EraseRootValue(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher)
	_, err := cache.Insert("", testData{value: 100, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Insert("child", testData{value: 200, dataSize: 20})
	require.NoError(t, err)

	// Act
	erased, ok := cache.Erase("")

	// Assert
	require.True(t, ok)
	assert.Equal(t, int64(100), erased.value)
	_, ok = cache.LookUp("")
	assert.False(t, ok)

	vChild, ok := cache.LookUp("child")
	require.True(t, ok)
	assert.Equal(t, int64(200), vChild.value)
}

// TestArenaRadixCache_CheckInvariants_PanicScenarios verifies that checkInvariants() detects and panics
// on internal structural, nodeMap, freeCount, and zeroSizeCount corruption scenarios.
//
// White-box testing rationale: Correct public Cache operations preserve all tree, LRU list, free-list,
// nodeMap, and byte-accounting invariants by construction. Triggering checkInvariants() failure paths
// therefore requires injecting synthetic corruption into internal struct fields after setup.
func TestArenaRadixCache_CheckInvariants_PanicScenarios(t *testing.T) {
	t.Run("CurrentSizeExceedsMaxSize", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](10, testDataWeigher).(*arenaRadix[testData])
		c.currentSize = 20

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("CorruptLRULinks", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Insert("k1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert("k2", testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		secondID := c.nodes[c.head].next
		c.nodes[secondID].prev = nilNode

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("CorruptTreeParent", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Insert("a/b", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		childID := c.nodes[c.root].child
		if childID != nilNode {
			c.nodes[childID].parent = nilNode
		}

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("SizeSumMismatch", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Insert("k1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		c.currentSize++

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("CompactnessViolation", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Insert("ab", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert("ac", testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		childID := c.nodes[c.root].child
		if childID != nilNode && !c.nodes[childID].hasValue {
			firstGrandchild := c.nodes[childID].child
			if firstGrandchild != nilNode {
				c.nodes[firstGrandchild].sibling = nilNode
			}
		}

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("NodeMapOutOfBounds", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Insert("k1", testData{value: 1, dataSize: 10})
		c.nodeMap[hashString("k1")] = 999

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("NodeMapNilValue", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Insert("k1", testData{value: 1, dataSize: 10})
		c.nodeMap[hashString("k1")] = c.root

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("NodeMapHashMismatch", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Insert("k1", testData{value: 1, dataSize: 10})
		id := c.nodeMap[hashString("k1")]
		c.nodeMap[12345] = id

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("LeakedNodeAccountingMismatch", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Insert("k1", testData{value: 1, dataSize: 10})
		c.nodes = append(c.nodes, arenaRadixNode[testData]{
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
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Insert("k1", testData{value: 1, dataSize: 10})
		_, _ = c.Erase("k1")
		c.freeCount = 999

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("ZeroSizeCountMismatch", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](100, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Insert("k1", testData{value: 1, dataSize: 0})
		require.NoError(t, err)
		c.zeroSizeCount = 99

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})
}

func TestArenaRadixCache_ModeratePressureLosslessCompaction(t *testing.T) {
	// Arrange
	pressure := 0.10
	c := NewArenaRadixCache[testData](
		20000,
		WithInvariantChecking(true),
		testDataWeigher,
		WithPressureFunc(func() float64 { return pressure }),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
	).(PressureAwareCache[testData])

	const totalKeys = 1000
	const survivingStart = 900 // Keep keys 900..999 (100 keys)
	for i := range totalKeys {
		key := fmt.Sprintf("bucket_%02d/dir_%02d/sub_%02d/obj_%04d.bin", i%10, (i/10)%10, (i/100)%10, i)
		_, err := c.Insert(key, testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}

	for i := range survivingStart {
		key := fmt.Sprintf("bucket_%02d/dir_%02d/sub_%02d/obj_%04d.bin", i%10, (i/10)%10, (i/100)%10, i)
		_, ok := c.Erase(key)
		require.True(t, ok)
	}

	// Act
	pressure = 0.80
	evicted := c.EvaluateMemoryPressure()

	// Assert
	assert.Empty(t, evicted)
	assertAlreadyCompacted(t, c)

	for i := survivingStart; i < totalKeys; i++ {
		key := fmt.Sprintf("bucket_%02d/dir_%02d/sub_%02d/obj_%04d.bin", i%10, (i/10)%10, (i/100)%10, i)
		v, ok := c.LookUpWithoutChangingOrder(key)
		require.True(t, ok)
		assert.Equal(t, int64(i), v.value)
	}

	pressure = 0.10
	evFiller, err := c.Insert("filler", testData{value: 999999, dataSize: 19000})
	require.NoError(t, err)
	assert.Empty(t, evFiller)
	for expectedID := survivingStart; expectedID < totalKeys; expectedID++ {
		ev, err := c.Insert(fmt.Sprintf("trigger_%d", expectedID), testData{value: -1, dataSize: 10})
		require.NoError(t, err)
		require.Len(t, ev, 1)
		assert.Equal(t, int64(expectedID), ev[0].value)
	}
}

func TestArenaRadixCache_CriticalPressureLRUShedding(t *testing.T) {
	// Arrange
	pressure := 0.10
	c := NewArenaRadixCache[testData](
		1000,
		WithInvariantChecking(true),
		testDataWeigher,
		WithPressureFunc(func() float64 { return pressure }),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.40),
	).(PressureAwareCache[testData])

	for i := range 100 {
		key := fmt.Sprintf("dir_%d/item_%03d", i%5, i)
		_, err := c.Insert(key, testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}

	// Act
	pressure = 0.95
	evicted := c.EvaluateMemoryPressure()

	// Assert
	require.Len(t, evicted, 60)
	for i, ev := range evicted {
		assert.Equal(t, int64(i), ev.value)
	}
	assertAlreadyCompacted(t, c)

	for i := range 60 {
		key := fmt.Sprintf("dir_%d/item_%03d", i%5, i)
		_, ok := c.LookUpWithoutChangingOrder(key)
		assert.False(t, ok)
	}
	for i := 60; i < 100; i++ {
		key := fmt.Sprintf("dir_%d/item_%03d", i%5, i)
		v, ok := c.LookUpWithoutChangingOrder(key)
		require.True(t, ok)
		assert.Equal(t, int64(i), v.value)
	}
}

func TestArenaRadixCache_AutomaticPressureTriggersAndReentrancy(t *testing.T) {
	// Arrange
	pressure := 0.10
	var cacheRef Cache[testData]
	reentrantReads := 0

	c := NewArenaRadixCache[testData](
		200,
		WithInvariantChecking(true),
		testDataWeigher,
		WithPressureFunc(func() float64 {
			if cacheRef != nil {
				_, _ = cacheRef.LookUpWithoutChangingOrder("probe_key")
				// Also verify mutating re-entrancy is guarded against infinite recursion.
				_, _ = cacheRef.Insert("reentrant_probe", testData{value: 1, dataSize: 0})
				_, _ = cacheRef.Erase("reentrant_probe")
				reentrantReads++
			}
			return pressure
		}),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
	).(PressureAwareCache[testData])
	cacheRef = c

	for i := range 20 {
		_, err := c.Insert(fmt.Sprintf("k_%02d", i), testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}
	for i := range 9 {
		_, ok := c.Erase(fmt.Sprintf("k_%02d", i))
		require.True(t, ok)
	}

	// Act 1: Automatic Tier 1 compaction on Erase of existing key under moderate pressure.
	pressure = 0.80
	_, ok := c.Erase("k_09")
	require.True(t, ok)

	// Assert 1
	assertAlreadyCompacted(t, c)
	for i := 10; i < 20; i++ {
		_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k_%02d", i))
		require.True(t, ok)
	}

	// Act 2: Automatic Tier 2 shedding on Insert under critical pressure.
	// maxSize = 200, retention = 0.50 -> targetSize = 100 bytes.
	// Before insert: 10 entries (100 bytes). Insert "new_mru" (60 bytes) -> 160 bytes -> sheds 6 oldest entries (60 bytes) down to 100 bytes.
	pressure = 0.95
	evicted, err := c.Insert("new_mru", testData{value: 999, dataSize: 60})

	// Assert 2
	require.NoError(t, err)
	assert.Len(t, evicted, 6)
	assertAlreadyCompacted(t, c)
	for i := 10; i < 16; i++ {
		_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k_%02d", i))
		assert.False(t, ok)
	}
	for i := 16; i < 20; i++ {
		_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k_%02d", i))
		assert.True(t, ok)
	}
	_, ok = c.LookUpWithoutChangingOrder("new_mru")
	assert.True(t, ok)
	assert.Positive(t, reentrantReads)
}

func TestArenaRadixCache_CompactionAndSheddingEdgeCases(t *testing.T) {
	t.Run("EmptyCacheCompactionAndShedding", func(t *testing.T) {
		// Arrange
		pressure := 0.95
		c := NewArenaRadixCache[testData](
			100,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return pressure }),
		).(PressureAwareCache[testData])

		// Act
		c.Compact()
		ev := c.EvaluateMemoryPressure()

		// Assert
		assert.Empty(t, ev)
		assertAlreadyCompacted(t, c)
		_, ok := c.LookUpWithoutChangingOrder("")
		assert.False(t, ok)
	})

	t.Run("SingleLargeEntryShedding", func(t *testing.T) {
		// Arrange
		pressure := 0.0
		c := NewArenaRadixCache[testData](
			100,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.50),
		).(PressureAwareCache[testData])
		_, err := c.Insert("only_item", testData{value: 42, dataSize: 80})
		require.NoError(t, err)

		// Act
		pressure = 0.95
		ev := c.EvaluateMemoryPressure()

		// Assert
		require.Len(t, ev, 1)
		assert.Equal(t, int64(42), ev[0].value)
		_, ok := c.LookUpWithoutChangingOrder("only_item")
		assert.False(t, ok)
		assertAlreadyCompacted(t, c)
	})

	t.Run("EmptyStringKeyAtRoot", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher).(PressureAwareCache[testData])
		_, err := c.Insert("", testData{value: 777, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert("a/b/c", testData{value: 888, dataSize: 10})
		require.NoError(t, err)
		_, _ = c.Erase("a/b/c")

		// Act
		c.Compact()
		v, ok := c.LookUpWithoutChangingOrder("")

		// Assert
		require.True(t, ok)
		assert.Equal(t, int64(777), v.value)
		assertAlreadyCompacted(t, c)
	})

	t.Run("ZeroSizeEntriesTerminationAndFullEvictionWhenRetentionZero", func(t *testing.T) {
		// Arrange
		pressure := 0.0
		c := NewArenaRadixCache[testData](
			100,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.0),
		).(PressureAwareCache[testData])

		for i := range 5 {
			_, err := c.Insert(fmt.Sprintf("zero_%d", i), testData{value: int64(i), dataSize: 0})
			require.NoError(t, err)
		}
		_, err := c.Insert("nonzero", testData{value: 99, dataSize: 40})
		require.NoError(t, err)

		// Act
		pressure = 0.95
		ev := c.EvaluateMemoryPressure()

		// Assert
		assert.Len(t, ev, 6)
		_, ok := c.LookUpWithoutChangingOrder("nonzero")
		assert.False(t, ok)
		for i := range 5 {
			_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("zero_%d", i))
			assert.False(t, ok)
		}
		assertAlreadyCompacted(t, c)
	})
}

func TestArenaRadixCache_PressureAndConcurrencyEdgeCases(t *testing.T) {
	t.Run("InsertIntoEmptyOrUnderTargetCacheDoesNotSelfEvict", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](
			1000,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return 0.95 }),
			WithEvictionRetentionRatio(0.50),
		)

		// Act
		evicted, err := c.Insert("first_key", testData{value: 42, dataSize: 10})
		lookedUp, ok := c.LookUpWithoutChangingOrder("first_key")

		// Assert
		require.NoError(t, err)
		assert.Empty(t, evicted)
		require.True(t, ok)
		assert.Equal(t, int64(42), lookedUp.value)
	})

	t.Run("SustainedCriticalPressureConvergesIdempotentlyWithoutCompoundDecay", func(t *testing.T) {
		// Arrange
		pressure := 0.0
		c := NewArenaRadixCache[testData](
			1000,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.50),
		).(PressureAwareCache[testData])
		for i := range 100 {
			_, err := c.Insert(fmt.Sprintf("k-%03d", i), testData{value: int64(i), dataSize: 10})
			require.NoError(t, err)
		}
		pressure = 0.95

		// Act: Evaluate critical pressure 10 times consecutively.
		firstEvicted := c.EvaluateMemoryPressure()
		for range 9 {
			subsequentEvicted := c.EvaluateMemoryPressure()
			assert.Empty(t, subsequentEvicted)
		}

		// Assert: Stabilizes idempotently at maxSize * retentionRatio (500 bytes, 50 entries).
		assert.Len(t, firstEvicted, 50)
		for i := range 50 {
			_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k-%03d", i))
			assert.False(t, ok)
		}
		for i := 50; i < 100; i++ {
			_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k-%03d", i))
			assert.True(t, ok)
		}
	})

	t.Run("CompactFastPathPerformsZeroAllocationsWhenAlreadyCompact", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](10000, WithInvariantChecking(true), testDataWeigher).(PressureAwareCache[testData])
		for i := range 200 {
			_, err := c.Insert(fmt.Sprintf("key-%03d", i), testData{value: int64(i), dataSize: 10})
			require.NoError(t, err)
		}

		// Act: First Compact eliminates geometric slice growth slack; second Compact via assertAlreadyCompacted
		// must take the clean fast-path with zero heap allocations.
		c.Compact()

		// Assert
		assertAlreadyCompacted(t, c)
	})
}

func TestArenaRadixCache_PressureSamplingAndReclamationBehaviors(t *testing.T) {
	t.Run("NoCompactionThrashingOnSteadyStateInsertsUnderPressure", func(t *testing.T) {
		// Arrange
		var c Cache[testData]

		// Act: Construct cache and insert 100 entries (1000 bytes == targetSize) inside AllocsPerRun.
		allocsPerInsert := testing.AllocsPerRun(1, func() {
			c = NewArenaRadixCache[testData](
				2000,
				WithInvariantChecking(true),
				testDataWeigher,
				WithPressureFunc(func() float64 { return 0.95 }),
				WithEvictionRetentionRatio(0.50),
			)
			for i := range 100 {
				_, err := c.Insert(fmt.Sprintf("k-%04d", i), testData{value: int64(i), dataSize: 10})
				require.NoError(t, err)
			}
		})

		// Assert: Without O(N^2) compaction thrashing on every Insert, 100 inserts allocate ~252 objects total and preserve all 100 keys.
		assert.Less(t, allocsPerInsert, 350.0)
		for i := range 100 {
			_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k-%04d", i))
			require.True(t, ok)
		}
	})

	t.Run("ZeroRetentionRatioDoesNotSelfEvictNewlyInsertedKey", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](
			1000,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return 0.95 }),
			WithEvictionRetentionRatio(0.0),
		).(PressureAwareCache[testData])

		_, err := c.Insert("old_key", testData{value: 1, dataSize: 100})
		require.NoError(t, err)

		// Act: Insert "new_key" under critical pressure with retention == 0.0.
		evicted, err := c.Insert("new_key", testData{value: 2, dataSize: 100})

		// Assert: "old_key" is evicted, but "new_key" is preserved.
		require.NoError(t, err)
		require.Len(t, evicted, 1)
		assert.Equal(t, int64(1), evicted[0].value)
		_, ok := c.LookUpWithoutChangingOrder("old_key")
		assert.False(t, ok)
		_, ok = c.LookUpWithoutChangingOrder("new_key")
		assert.True(t, ok)

		// Explicit EvaluateMemoryPressure still flushes 100% of entries when retention == 0.0.
		flushed := c.EvaluateMemoryPressure()
		require.Len(t, flushed, 1)
		_, ok = c.LookUpWithoutChangingOrder("new_key")
		assert.False(t, ok)
	})

	t.Run("UpdateGrowUnderCriticalPressureTriggersTier2Shedding", func(t *testing.T) {
		// Arrange
		pressure := 0.10
		c := NewArenaRadixCache[testData](
			1000,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionRetentionRatio(0.50),
		)
		for i := range 8 {
			_, err := c.Insert(fmt.Sprintf("k-%d", i), testData{value: int64(i), dataSize: 100})
			require.NoError(t, err)
		}

		// Act: Grow k-7 from 100 to 200 bytes under critical pressure (0.95).
		pressure = 0.95
		err := c.UpdateWithoutChangingOrder("k-7", testData{value: 77, dataSize: 200})

		// Assert: Sheds 4 oldest entries (k-0..k-3, 400B) down to targetSize (500 bytes: k-4, k-5, k-6 at 100B + k-7 at 200B).
		require.NoError(t, err)
		for i := range 4 {
			_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k-%d", i))
			assert.False(t, ok)
		}
		for i := 4; i < 8; i++ {
			_, ok := c.LookUpWithoutChangingOrder(fmt.Sprintf("k-%d", i))
			assert.True(t, ok)
		}
	})
}

func TestArenaRadixCache_UpdateGrowAtMRUHeadUnderCriticalPressure(t *testing.T) {
	// Arrange: Insert k1 (oldest, at tail), k2, k3 (newest, at MRU head).
	pressure := 0.10
	c := NewArenaRadixCache[testData](
		1000,
		WithInvariantChecking(true),
		testDataWeigher,
		WithPressureFunc(func() float64 { return pressure }),
		WithEvictionRetentionRatio(0.50),
	)

	_, err := c.Insert("k1", testData{value: 1, dataSize: 200})
	require.NoError(t, err)
	_, err = c.Insert("k2", testData{value: 2, dataSize: 200})
	require.NoError(t, err)
	_, err = c.Insert("k3", testData{value: 3, dataSize: 200})
	require.NoError(t, err)

	// Act: Grow k3 (the MRU head) from 200 to 300 bytes under critical pressure.
	pressure = 0.95
	err = c.UpdateWithoutChangingOrder("k3", testData{value: 33, dataSize: 300})

	// Assert: MRU head k3 is protected and preserved (300 bytes), while oldest tail entry k1 (200 bytes)
	// is shed so total size drops from 700 bytes down to targetSize (500 bytes: k2=200B + k3=300B).
	require.NoError(t, err)
	_, ok := c.LookUpWithoutChangingOrder("k1")
	assert.False(t, ok)
	_, ok = c.LookUpWithoutChangingOrder("k2")
	assert.True(t, ok)
	_, ok = c.LookUpWithoutChangingOrder("k3")
	assert.True(t, ok)
}

// TestArenaRadixCache_FNV1aHashCollisionAndNodeMapHealing verifies that ArenaRadixCache preserves key
// reachability, LRU invariants, and compaction guarantees when 64-bit FNV-1a hash collisions displace
// entries in the O(1) c.nodeMap accelerator.
//
// White-box testing rationale:
//  1. ArenaRadixCache hashes keys internally using unseeded 64-bit FNV-1a (hashString). By the birthday bound,
//     finding two distinct variable-length strings with the exact same 64-bit FNV-1a hash in a 2^64 hash space
//     requires ~2^32 (~4.29 billion) evaluations, which is computationally infeasible at unit-test runtime even
//     though collisions can occur in production across billions of cumulative keys.
//  2. Furthermore, c.nodeMap is an internal O(1) hash accelerator backed by an exact O(K) radix-trie walk fallback
//     (findNodeByTrieWalk). If Erase(K1) or a self-evicting/failing UpdateWithoutChangingOrder(K1, ...) erroneously overwrites
//     c.nodeMap[h] = idK1 before eraseInternalWithHash deletes c.nodeMap[h], public LookUp / LookUpWithoutChangingOrder
//     calls still return functionally identical values via the slow-path trie walk while silently evicting the
//     surviving colliding peer K2 from c.nodeMap and breaking the len(c.nodeMap) == c.len Pigeonhole Principle O(1)
//     cache-miss fast-path across the entire cache.
//     These subtests therefore seed a synthetic collision/displacement in c.nodeMap in setup and inspect c.nodeMap
//     in assertions alongside public API LookUp/ LookUpWithoutChangingOrder/Insert/UpdateWithoutChangingOrder/Erase/Compact calls.
func TestArenaRadixCache_FNV1aHashCollisionAndNodeMapHealing(t *testing.T) {
	const keyA = "prefix/key-alpha"
	const keyB = "prefix/key-beta"

	t.Run("HashCollisionDoesNotThrashCompactionAndPreservesInvariant", func(t *testing.T) {
		// Arrange: Insert keyA, keyB, and a temporary key, simulate a 64-bit FNV-1a hash collision
		// displacing keyA from nodeMap, and trigger a real deletion via Erase.
		c := NewArenaRadixCache[testData](10000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := c.Insert(keyA, testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert(keyB, testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert("prefix/key-temp", testData{value: 3, dataSize: 10})
		require.NoError(t, err)

		delete(c.nodeMap, hashString(keyA))
		_, ok := c.Erase("prefix/key-temp")
		require.True(t, ok)

		// Act & Assert: Both keys remain accessible (keyA via trie fallback), and Compact() heals keyA
		// back into nodeMap without thrashing on subsequent Compact() calls.
		valA, ok := c.LookUpWithoutChangingOrder(keyA)
		require.True(t, ok)
		assert.Equal(t, int64(1), valA.value)

		c.Compact()
		_, ok = c.nodeMap[hashString(keyA)]
		assert.True(t, ok)
		assertAlreadyCompacted(t, c)

		valAAfter, okA := c.LookUp(keyA)
		valBAfter, okB := c.LookUp(keyB)
		require.True(t, okA)
		require.True(t, okB)
		assert.Equal(t, int64(1), valAAfter.value)
		assert.Equal(t, int64(2), valBAfter.value)
	})

	t.Run("OverwriteDoesNotCorruptCollidingPeerInNodeMap", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := c.Insert("k1", testData{value: 10, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert("k2", testData{value: 20, dataSize: 10})
		require.NoError(t, err)

		// Simulate hash collision: point k1's hash slot to k2's node ID.
		idK2 := c.nodeMap[hashString("k2")]
		c.nodeMap[hashString("k1")] = idK2

		// Act: Overwrite k1 via trie fallback and verify k1 reclaims its nodeMap slot.
		evicted, err := c.Insert("k1", testData{value: 11, dataSize: 15})
		require.NoError(t, err)
		assert.Empty(t, evicted)

		// Assert
		v1, ok1 := c.LookUpWithoutChangingOrder("k1")
		v2, ok2 := c.LookUpWithoutChangingOrder("k2")
		require.True(t, ok1)
		require.True(t, ok2)
		assert.Equal(t, int64(11), v1.value)
		assert.Equal(t, int64(20), v2.value)
		assert.NotEqual(t, idK2, c.nodeMap[hashString("k1")])
	})

	t.Run("UpdateMethodsHealNodeMapOnCollisionOrMissingSlot", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := c.Insert("key-alpha", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Insert("key-beta", testData{value: 2, dataSize: 10})
		require.NoError(t, err)

		hAlpha := hashString("key-alpha")
		hBeta := hashString("key-beta")
		idAlpha := c.nodeMap[hAlpha]
		idBeta := c.nodeMap[hBeta]

		// Act & Assert 1: Missing slot healed by LookUp.
		delete(c.nodeMap, hAlpha)
		_, ok := c.LookUp("key-alpha")
		require.True(t, ok)
		assert.Equal(t, idAlpha, c.nodeMap[hAlpha])

		// Act & Assert 2: Missing slot healed by UpdateWithoutChangingOrder (same size).
		delete(c.nodeMap, hAlpha)
		err = c.UpdateWithoutChangingOrder("key-alpha", testData{value: 11, dataSize: 10})
		require.NoError(t, err)
		assert.Equal(t, idAlpha, c.nodeMap[hAlpha])

		// Act & Assert 3: Colliding slot pointing to idBeta healed by UpdateWithoutChangingOrder("key-alpha", ...) with size growth.
		c.nodeMap[hAlpha] = idBeta
		err = c.UpdateWithoutChangingOrder("key-alpha", testData{value: 12, dataSize: 15})
		require.NoError(t, err)
		assert.Equal(t, idAlpha, c.nodeMap[hAlpha])
	})

	t.Run("EraseAndUpdateSelfEvictionOrExceedMaxSizePreserveCollidingPeerInNodeMap", func(t *testing.T) {
		hA := hashString(keyA)
		hB := hashString(keyB)

		// Case 1: Erase(keyA) when collision bucket hA points to surviving peer keyB.
		cErase := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err := cErase.Insert(keyA, testData{value: 1, dataSize: 100})
		require.NoError(t, err)
		_, err = cErase.Insert(keyB, testData{value: 2, dataSize: 100})
		require.NoError(t, err)
		idB := cErase.nodeMap[hB]
		cErase.nodeMap[hA] = idB

		erased, ok := cErase.Erase(keyA)
		require.True(t, ok)
		assert.Equal(t, int64(1), erased.value)
		assert.Equal(t, idB, cErase.nodeMap[hA], "Erase(keyA) must not overwrite or delete surviving peer keyB from shared hash bucket")
		delete(cErase.nodeMap, hA)
		assert.Len(t, cErase.nodeMap, cErase.len, "len(nodeMap) == c.len bijection must hold for O(1) cache-miss fast-path")
		cErase.checkInvariants()
		_, ok = cErase.LookUpWithoutChangingOrder(keyA)
		assert.False(t, ok)
		_, ok = cErase.LookUpWithoutChangingOrder("missing-key")
		assert.False(t, ok)
		_, ok = cErase.LookUpWithoutChangingOrder(keyB)
		require.True(t, ok)

		// Case 2: UpdateWithoutChangingOrder(keyA, >maxSize) self-eviction via newSize > maxSize.
		cMaxEvict := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err = cMaxEvict.Insert(keyA, testData{value: 1, dataSize: 100})
		require.NoError(t, err)
		_, err = cMaxEvict.Insert(keyB, testData{value: 2, dataSize: 100})
		require.NoError(t, err)
		idB = cMaxEvict.nodeMap[hB]
		cMaxEvict.nodeMap[hA] = idB

		err = cMaxEvict.UpdateWithoutChangingOrder(keyA, testData{value: 99, dataSize: 1001})
		require.NoError(t, err)
		assert.Equal(t, idB, cMaxEvict.nodeMap[hA], "UpdateWithoutChangingOrder self-eviction (newSize > maxSize) must preserve surviving peer in nodeMap")
		delete(cMaxEvict.nodeMap, hA)
		assert.Len(t, cMaxEvict.nodeMap, cMaxEvict.len)
		cMaxEvict.checkInvariants()
		_, ok = cMaxEvict.LookUpWithoutChangingOrder(keyA)
		assert.False(t, ok)
		_, ok = cMaxEvict.LookUpWithoutChangingOrder("missing-key")
		assert.False(t, ok)
		_, ok = cMaxEvict.LookUpWithoutChangingOrder(keyB)
		require.True(t, ok)

		// Case 3: UpdateWithoutChangingOrder(keyA, ...) self-eviction via !canFit (newer entry keyB occupies capacity).
		cCannotFit := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err = cCannotFit.Insert(keyA, testData{value: 1, dataSize: 100})
		require.NoError(t, err)
		_, err = cCannotFit.Insert(keyB, testData{value: 2, dataSize: 900})
		require.NoError(t, err)
		idB = cCannotFit.nodeMap[hB]
		cCannotFit.nodeMap[hA] = idB

		err = cCannotFit.UpdateWithoutChangingOrder(keyA, testData{value: 11, dataSize: 300})
		require.NoError(t, err)
		assert.Equal(t, idB, cCannotFit.nodeMap[hA], "UpdateWithoutChangingOrder self-eviction (!canFit) must preserve surviving peer in nodeMap")
		delete(cCannotFit.nodeMap, hA)
		assert.Len(t, cCannotFit.nodeMap, cCannotFit.len)
		cCannotFit.checkInvariants()
		_, ok = cCannotFit.LookUpWithoutChangingOrder(keyA)
		assert.False(t, ok)
		_, ok = cCannotFit.LookUpWithoutChangingOrder("missing-key")
		assert.False(t, ok)
		_, ok = cCannotFit.LookUpWithoutChangingOrder(keyB)
		require.True(t, ok)
	})

	t.Run("DisplacedNodeMapSlotOnEraseStillTriggersChurnCompactionAndPreservesPeers", func(t *testing.T) {
		// Arrange: Keep keyB as the surviving entry while inserting and erasing two batches of 32 keys
		// whose nodeMap slots were displaced by simulated hash collisions. Because peak live entries stays
		// at 33 (< 64) and free-list length never exceeds 32 (< 63), single-survivor auto-compaction on the
		// 64th Erase occurs if and only if erasing displaced keys still accounts for deletion churn.
		c := NewArenaRadixCache[testData](10000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := c.Insert(keyB, testData{value: 2, dataSize: 10})
		require.NoError(t, err)

		for batch := range 2 {
			for i := range 32 {
				k := fmt.Sprintf("displaced-%d-%02d", batch, i)
				_, err := c.Insert(k, testData{value: int64(i + 10), dataSize: 10})
				require.NoError(t, err)
				delete(c.nodeMap, hashString(k))
			}
			for i := range 32 {
				k := fmt.Sprintf("displaced-%d-%02d", batch, i)
				_, ok := c.Erase(k)
				require.True(t, ok)
				_, ok = c.LookUp(k)
				assert.False(t, ok)
			}
		}

		// Assert: The 64th displaced Erase auto-compacted the cache down to the single survivor keyB,
		// healed nodeMap, and preserved keyB for subsequent public LookUp, Insert, and Erase calls.
		assertAlreadyCompacted(t, c)
		valB, ok := c.LookUp(keyB)
		require.True(t, ok)
		assert.Equal(t, int64(2), valB.value)

		_, err = c.Insert(keyA, testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, ok = c.LookUp(keyA)
		require.True(t, ok)
		_, ok = c.Erase(keyA)
		require.True(t, ok)
		_, ok = c.LookUp(keyA)
		assert.False(t, ok)
		_, ok = c.LookUp(keyB)
		assert.True(t, ok)
	})

	t.Run("RootNodeEraseWithoutMapEntryPreservesNodeMap", func(t *testing.T) {
		// Arrange
		ac := NewArenaRadixCache[string](100, WithInvariantChecking(true)).(*arenaRadix[string])
		_, err := ac.Insert("", "root_val")
		require.NoError(t, err)
		_, err = ac.Insert("other", "other_val")
		require.NoError(t, err)
		delete(ac.nodeMap, hashString(""))

		// Act: Erase "" (which resides at c.root == 0) while hash("") is not in c.nodeMap.
		erased, ok := ac.Erase("")

		// Assert: nodeMap retains the remaining entry intact.
		require.True(t, ok)
		assert.Equal(t, "root_val", erased)
		assert.Len(t, ac.nodeMap, 1)
	})
}

func TestArenaRadixCache_DeepHierarchyOver64LevelsAndRoutingPrefixCloning(t *testing.T) {
	t.Run("DeepRadixTreeOver64LevelsAndFreeSubtree", func(t *testing.T) {
		// Arrange: Build a radix tree with > 80 branching levels so both hashNodeKey and freeSubtree exceed 64 levels.
		probe := newPressureProbe(0.10)
		c := NewArenaRadixCache[testData](100000, WithInvariantChecking(true), testDataWeigher, probe.Option()).(PressureAwareCache[testData])
		const depth = 80
		for i := 1; i <= depth; i++ {
			key := strings.Repeat("a", i) + "b"
			_, err := c.Insert(key, testData{value: int64(i), dataSize: 10})
			require.NoError(t, err)
		}

		// Act 1: Compact the 80-level tree (exercises hashNodeKey with > 64 segments) and confirm a second Compact is a no-op.
		assert.True(t, probe.ObserveEpochAdvance(t, c, func() {
			c.Compact()
		}))
		assert.False(t, probe.ObserveEpochAdvance(t, c, func() {
			c.Compact()
		}))
		for i := 1; i <= depth; i++ {
			key := strings.Repeat("a", i) + "b"
			val, ok := c.LookUpWithoutChangingOrder(key)
			require.True(t, ok)
			assert.Equal(t, int64(i), val.value)
		}

		// Act 2: Erase all entries under prefix "a" (exercises iterative freeSubtree with > 64 levels).
		c.EraseEntriesWithGivenPrefix("a")

		// Assert
		for i := 1; i <= depth; i++ {
			key := strings.Repeat("a", i) + "b"
			_, ok := c.LookUpWithoutChangingOrder(key)
			assert.False(t, ok)
		}
		assertAlreadyCompacted(t, c)
	})

	t.Run("RoutingPrefixDoesNotPinLargeKeyBackingArray", func(t *testing.T) {
		// Arrange: Insert a 64 KiB key ("dir/" + 64 KiB suffix), then split the edge
		// with two sibling keys ("dir/a" and "dir/b") and erase the 64 KiB key so
		// the "dir/" intermediate routing node survives.
		//
		// White-box testing rationale: String backing-array aliasing on internal routing nodes is
		// invisible to public LookUp results, so verifying that the 4-byte "dir/" routing prefix does not pin
		// the 64 KiB key buffer requires inspecting unsafe.StringData on the internal routing node.
		c := NewArenaRadixCache[string](1<<20, WithInvariantChecking(true)).(*arenaRadix[string])
		largeKey := strings.Clone("dir/" + strings.Repeat("X", 1<<16))

		_, err := c.Insert(largeKey, "v")
		require.NoError(t, err)
		pinnedInitialPrefix := c.nodes[1].prefix
		largeStart := uintptr(unsafe.Pointer(unsafe.StringData(pinnedInitialPrefix)))
		largeEnd := largeStart + uintptr(len(pinnedInitialPrefix))

		// Act: Split the edge and erase the 64 KiB key.
		_, err = c.Insert("dir/a", "va")
		require.NoError(t, err)
		_, err = c.Insert("dir/b", "vb")
		require.NoError(t, err)
		_, _ = c.Erase(largeKey)

		// Assert: Routing node prefix "dir/" does not alias pinnedInitialPrefix's 64 KiB backing array.
		routingID := c.nodes[c.root].child
		require.NotEqual(t, nilNode, routingID)
		assert.Equal(t, "dir/", c.nodes[routingID].prefix)
		prefixPtr := uintptr(unsafe.Pointer(unsafe.StringData(c.nodes[routingID].prefix)))
		assert.True(t, prefixPtr < largeStart || prefixPtr >= largeEnd, "routing node prefix must not pin 64 KiB backing array")
		runtime.KeepAlive(pinnedInitialPrefix)
	})
}

// TestArenaRadixCache_InsertSplitsPrefixAndRecyclesFreeSlot verifies that normal insertions splitting radix edges
// and recycling free-list slots succeed without falsely triggering the uint32 arena node-limit guard.
//
// White-box testing rationale:
//  1. Why the node-limit boundary cannot be reproduced solely through the public interface on standard hardware:
//     In ArenaRadixCache.Insert, the pre-allocation node-limit guard is:
//     uint64(len(c.nodes)) - uint64(c.freeCount) + 2 > uint64(foregroundNoProtect)
//     where foregroundNoProtect == nilNode - 1 == math.MaxUint32 - 1 (4,294,967,294).
//     Reaching this boundary through public Cache.Insert calls requires populating
//     len(c.nodes) - int(c.freeCount) == math.MaxUint32 - 2 (4,294,967,293) active arenaRadixNode structs.
//     Because unsafe.Sizeof(arenaRadixNode[testData]{}) == 64 bytes on 64-bit platforms, backing the c.nodes slice alone
//     requires 4,294,967,293 * 64 == 274,877,906,752 bytes (~274.88 GiB) of contiguous heap memory (plus >100 GiB
//     for c.nodeMap), which is infeasible to allocate in a unit test on standard hardware.
//  2. Why the guard is nevertheless essential for correctness:
//     a) Sentinel Collision & Panic Prevention: Arena node indices are 32-bit unsigned integers where
//     nilNode == math.MaxUint32 and foregroundNoProtect == math.MaxUint32 - 1. A new-key Insert that splits an
//     existing edge allocates 2 nodes (one intermediate routing node + one leaf node). If the guard checked
//     against math.MaxUint32 instead of foregroundNoProtect, an Insert at 4,294,967,293 active nodes would
//     attempt to allocate node index foregroundNoProtect (math.MaxUint32 - 1), which panics in allocateNode()
//     (if id >= foregroundNoProtect) or, if allocated, collides with the foregroundNoProtect sentinel passed to
//     foreground pressure reclamation / shedAndCompactLocked (stripping MRU head protection from the newly
//     inserted entry during Tier 2 pressure shedding).
//     b) Free-List Recycling Accuracy: Subtracting uint64(c.freeCount) in
//     uint64(len(c.nodes)) - uint64(c.freeCount) + 2 > uint64(foregroundNoProtect) ensures that when recycled
//     free-list slots exist (c.freeCount >= 2), Insert pops from c.freeHead without falsely evicting live entries.
func TestArenaRadixCache_InsertSplitsPrefixAndRecyclesFreeSlot(t *testing.T) {
	// Arrange: Populate entries that split a shared prefix ("alpha/") and recycle free-list slots via Erase.
	c := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher)
	_, err := c.Insert("alpha/one", testData{value: 1, dataSize: 10})
	require.NoError(t, err)
	_, err = c.Insert("beta/two", testData{value: 2, dataSize: 10})
	require.NoError(t, err)
	_, ok := c.Erase("beta/two")
	require.True(t, ok)

	// Act: Insert a sibling key that splits "alpha/one" into routing node "alpha/" + two leaves, reusing the free slot.
	evicted, err := c.Insert("alpha/three", testData{value: 3, dataSize: 10})

	// Assert
	require.NoError(t, err)
	assert.Empty(t, evicted)
	_, ok = c.LookUpWithoutChangingOrder("alpha/one")
	assert.True(t, ok)
	_, ok = c.LookUpWithoutChangingOrder("beta/two")
	assert.False(t, ok)
	_, ok = c.LookUpWithoutChangingOrder("alpha/three")
	assert.True(t, ok)
}
