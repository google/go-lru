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
	"slices"
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

func TestArenaRadixCache_GetInEmptyCache(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)

	// Act
	emptyVal, okEmpty := cache.Get("")
	tacoVal, okTaco := cache.Get("taco")

	// Assert
	assert.False(t, okEmpty)
	assert.Equal(t, testData{}, emptyVal)
	assert.False(t, okTaco)
	assert.Equal(t, testData{}, tacoVal)
}

func TestArenaRadixCache_PutZeroAndNilSliceValue(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[[]byte](testMaxSize, WithInvariantChecking(true), WithWeigher(func(_ string, b []byte) uint64 {
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

func TestArenaRadixCache_PutEmptyKey(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)

	// Act
	evicted, err := cache.Put("", testData{value: 42, dataSize: 10})
	val, ok := cache.Get("")
	_, okTaco := cache.Get("taco")

	// Assert
	require.NoError(t, err)
	assert.Empty(t, evicted)
	require.True(t, ok)
	assert.Equal(t, int64(42), val.value)
	assert.False(t, okTaco)
}

func TestArenaRadixCache_GetUnknownKey(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Put("taco", testData{value: 23, dataSize: 8})
	require.NoError(t, err)

	// Act
	emptyVal, okEmpty := cache.Get("")
	enchiladaVal, okEnchilada := cache.Get("enchilada")

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
	ev1, err1 := cache.Put("burrito", testData{value: 23, dataSize: 4})
	ev2, err2 := cache.Put("taco", testData{value: 26, dataSize: 20})
	ev3, err3 := cache.Put("enchilada", testData{value: 28, dataSize: 26})

	// Assert
	require.NoError(t, err1)
	assert.Empty(t, ev1)
	require.NoError(t, err2)
	assert.Empty(t, ev2)
	require.NoError(t, err3)
	assert.Empty(t, ev3)

	v1, ok := cache.Get("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(23), v1.value)

	v2, ok := cache.Get("taco")
	require.True(t, ok)
	assert.Equal(t, int64(26), v2.value)

	v3, ok := cache.Get("enchilada")
	require.True(t, ok)
	assert.Equal(t, int64(28), v3.value)
}

func TestArenaRadixCache_ExpiresLeastRecentlyUsed(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Put("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Put("enchilada", testData{value: 28, dataSize: 26})
	require.NoError(t, err)

	// Promote burrito to MRU
	promoted, ok := cache.Get("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(23), promoted.value)

	// Act: Put another item; taco (least recent) should be evicted
	evicted, err := cache.Put("queso", testData{value: 34, dataSize: 5})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{26})
	_, ok = cache.Get("taco")
	assert.False(t, ok)

	vBurrito, ok := cache.Get("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(23), vBurrito.value)

	vEnchilada, ok := cache.Get("enchilada")
	require.True(t, ok)
	assert.Equal(t, int64(28), vEnchilada.value)

	vQueso, ok := cache.Get("queso")
	require.True(t, ok)
	assert.Equal(t, int64(34), vQueso.value)
}

func TestArenaRadixCache_Overwrite(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Put("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Put("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)
	ev1, err := cache.Put("burrito", testData{value: 33, dataSize: 6})
	require.NoError(t, err)
	assert.Empty(t, ev1)

	// Act: Increase size during overwrite; taco should be evicted
	evicted, err := cache.Put("burrito", testData{value: 33, dataSize: 12})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{26})
	_, ok := cache.Get("taco")
	assert.False(t, ok)

	vBurrito, ok := cache.Get("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(33), vBurrito.value)

	vEnchilada, ok := cache.Get("enchilada")
	require.True(t, ok)
	assert.Equal(t, int64(28), vEnchilada.value)
}

func TestArenaRadixCache_MultipleEviction(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Put("taco", testData{value: 26, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Put("enchilada", testData{value: 28, dataSize: 20})
	require.NoError(t, err)

	// Act: Large insert requiring all previous entries to be evicted
	evicted, err := cache.Put("large_data", testData{value: 33, dataSize: 45})

	// Assert
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{23, 26, 28})
	_, ok := cache.Get("taco")
	assert.False(t, ok)
	_, ok = cache.Get("burrito")
	assert.False(t, ok)
	_, ok = cache.Get("enchilada")
	assert.False(t, ok)

	vLarge, ok := cache.Get("large_data")
	require.True(t, ok)
	assert.Equal(t, int64(33), vLarge.value)
}

func TestArenaRadixCache_WhenEntrySizeMoreThanCacheMaxSize(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)

	// Act: Attempt inserting item with size > testMaxSize
	evicted, err := cache.Put("taco", testData{value: 26, dataSize: testMaxSize + 1})

	// Assert
	require.ErrorIs(t, err, ErrInvalidEntrySize)
	assert.Empty(t, evicted)

	vBurrito, ok := cache.Get("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(23), vBurrito.value)
}

func TestArenaRadixCache_DeleteWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)

	// Act
	deletedEntry, ok := cache.Delete("burrito")

	// Assert
	require.True(t, ok)
	assert.Equal(t, int64(23), deletedEntry.value)
	_, ok = cache.Get("burrito")
	assert.False(t, ok)
}

func TestArenaRadixCache_DeleteWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Put("burrito", testData{value: 23, dataSize: 4})
	require.NoError(t, err)

	// Act
	deletedEntry, ok := cache.Delete("taco")

	// Assert
	assert.False(t, ok)
	assert.Equal(t, testData{}, deletedEntry)
	vBurrito, ok := cache.Get("burrito")
	require.True(t, ok)
	assert.Equal(t, int64(23), vBurrito.value)
}

func TestArenaRadixCache_DeletePrefix(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
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

	vb, ok := cache.Get("b")
	require.True(t, ok)
	assert.Equal(t, uint64(2), vb.dataSize)
}

func TestArenaRadixCache_DeletePrefixWithEmptyPrefix(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
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

func TestArenaRadixCache_DeletePrefixWhereNoEntriesExist(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, err := cache.Put("a", testData{value: 23, dataSize: 4})
	require.NoError(t, err)
	_, err = cache.Put("a/b", testData{value: 26, dataSize: 5})
	require.NoError(t, err)
	_, err = cache.Put("b", testData{value: 21, dataSize: 2})
	require.NoError(t, err)

	// Act
	cache.DeletePrefix("c")

	// Assert
	va, ok := cache.Get("a")
	require.True(t, ok)
	assert.Equal(t, uint64(4), va.dataSize)

	vab, ok := cache.Get("a/b")
	require.True(t, ok)
	assert.Equal(t, uint64(5), vab.dataSize)

	vb, ok := cache.Get("b")
	require.True(t, ok)
	assert.Equal(t, uint64(2), vb.dataSize)
}

func TestArenaRadixCache_DeletePrefixWithSomeEntriesEvictedDueToCacheSize(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
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

	// Act: "a" was evicted by "b", remaining "a/b", "a/b/d", "a/c" should be erased
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

	vb, ok := cache.Get("b")
	require.True(t, ok)
	assert.Equal(t, uint64(15), vb.dataSize)
}

func TestArenaRadixCache_ReplaceGrowSize(t *testing.T) {
	t.Run("NonExistentKey", func(t *testing.T) {
		// Arrange
		cache := NewArenaRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)

		// Act
		err := cache.Replace("key1", testData{value: 1, dataSize: 20})

		// Assert
		require.ErrorIs(t, err, ErrEntryNotExist)
	})

	t.Run("ImmediateEviction", func(t *testing.T) {
		// Arrange
		cache := NewArenaRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)
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

func TestArenaRadixCache_ReplaceGrowSize_ExceedsMaxSize(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)
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

func TestArenaRadixCache_ReplaceGrowSize_MultipleEvictions(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher)
	_, err := cache.Put("k1", testData{value: 1, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Put("k2", testData{value: 2, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Put("k3", testData{value: 3, dataSize: 20})
	require.NoError(t, err)
	_, err = cache.Put("k4", testData{value: 4, dataSize: 20})
	require.NoError(t, err)

	// Act: Grow k4 from 20 to 70 (+50 delta) -> evicts k1 and k2.
	err = cache.Replace("k4", testData{value: 44, dataSize: 70})

	// Assert
	require.NoError(t, err)
	_, ok := cache.Get("k1")
	assert.False(t, ok)
	_, ok = cache.Get("k2")
	assert.False(t, ok)
	_, ok = cache.Get("k3")
	assert.True(t, ok)
	_, ok = cache.Get("k4")
	assert.True(t, ok)
}

func TestArenaRadixCache_ReplaceWhenKeyPresent(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}
	_, err := cache.Put(key, data)
	require.NoError(t, err)

	// Act
	newData := testData{value: 2, dataSize: 4}
	err = cache.Replace(key, newData)

	// Assert
	require.NoError(t, err)
	v, ok := cache.Get(key)
	require.True(t, ok)
	assert.Equal(t, int64(2), v.value)
}

func TestArenaRadixCache_ReplaceWhenKeyNotPresent(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	key := "burrito"
	data := testData{value: 23, dataSize: 4}

	// Act
	err := cache.Replace(key, data)

	// Assert
	require.ErrorIs(t, err, ErrEntryNotExist)
}

func TestArenaRadixCache_ReplaceWhenSizeShrinks(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
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

func TestArenaRadixCache_ReplaceNotChangeOrder(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	key1 := "burrito1"
	data1 := testData{value: 23, dataSize: 10}
	_, err := cache.Put(key1, data1)
	require.NoError(t, err)
	key2 := "burrito2"
	data2 := testData{value: 2, dataSize: 40}
	_, err = cache.Put(key2, data2)
	require.NoError(t, err)

	// Act: Update key1 without changing order, then insert key3 (size 5)
	newData := testData{value: 7, dataSize: 10}
	err = cache.Replace(key1, newData)
	require.NoError(t, err)

	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	evicted, err := cache.Put(key3, data3)

	// Assert: key1 (value 7) is evicted because key1 remained the LRU element
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{7})
}

func TestArenaRadixCache_ReplaceGrowToExactMaxSize(t *testing.T) {
	// Arrange
	const maxSize = 100
	const initialSize = 50
	const sizeDelta = 50 // New total size will be 50 + 50 = 100 (exactly at maxSize)

	cache := NewArenaRadixCache[testData](maxSize, WithInvariantChecking(true), testDataWeigher)
	_, err := cache.Put("file.txt", testData{value: 1, dataSize: initialSize})
	require.NoError(t, err)

	// Act: Grow entry via Replace to exact maxSize
	err = cache.Replace("file.txt", testData{value: 2, dataSize: initialSize + sizeDelta})

	// Assert
	require.NoError(t, err)
	val, ok := cache.Get("file.txt")
	require.True(t, ok)
	assert.Equal(t, testData{value: 2, dataSize: initialSize + sizeDelta}, val)
}

func TestArenaRadixCache_Peek(t *testing.T) {
	// Arrange
	cache := setupArenaRadixCacheTest(t)
	_, ok := cache.Peek("absent")
	assert.False(t, ok)

	key1 := "burrito1"
	data1 := testData{value: 23, dataSize: 10}
	_, err := cache.Put(key1, data1)
	require.NoError(t, err)
	key2 := "burrito2"
	data2 := testData{value: 2, dataSize: 40}
	_, err = cache.Put(key2, data2)
	require.NoError(t, err)

	// Act: Peek on key1, then insert key3 (size 5)
	val, ok := cache.Peek(key1)
	require.True(t, ok)
	assert.Equal(t, int64(23), val.value)

	key3 := "burrito3"
	data3 := testData{value: 3, dataSize: 5}
	evicted, err := cache.Put(key3, data3)

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
		_, err := cache.Put(k, testData{value: int64(i + 1), dataSize: 10})
		require.NoError(t, err)
	}

	for i, k := range keys {
		v, ok := cache.Get(k)
		require.True(t, ok)
		assert.Equal(t, int64(i+1), v.value)
	}

	// Act: Delete leaves and intermediate nodes to exercise compressPathUpwards
	vAppn, okAppn := cache.Delete("application")
	vApply, okApply := cache.Delete("apply")
	vApp, okApp := cache.Delete("app")

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
		v, ok := cache.Get(tc.key)
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
			_, err := cache.Put(key, testData{value: int64(i), dataSize: 10})
			require.NoError(t, err)
		}

		for i := range 50 {
			key := fmt.Sprintf("prefix/subdir_%d/file_%d.txt", i%5, i)
			_, ok := cache.Delete(key)
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

	_, err := cache.Put(path1, testData{value: 1, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Put(path2, testData{value: 2, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Put(path3, testData{value: 3, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Put(path4, testData{value: 4, dataSize: 10})
	require.NoError(t, err)

	// Act
	cache.DeletePrefix("a/b/c/d/e/")

	// Assert
	_, ok := cache.Get(path1)
	assert.False(t, ok)
	_, ok = cache.Get(path2)
	assert.False(t, ok)

	v3, ok := cache.Get(path3)
	require.True(t, ok)
	assert.Equal(t, int64(3), v3.value)

	v4, ok := cache.Get(path4)
	require.True(t, ok)
	assert.Equal(t, int64(4), v4.value)
}

func TestArenaRadixCache_DeleteRootValue(t *testing.T) {
	// Arrange
	cache := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher)
	_, err := cache.Put("", testData{value: 100, dataSize: 10})
	require.NoError(t, err)
	_, err = cache.Put("child", testData{value: 200, dataSize: 20})
	require.NoError(t, err)

	// Act
	erased, ok := cache.Delete("")

	// Assert
	require.True(t, ok)
	assert.Equal(t, int64(100), erased.value)
	_, ok = cache.Get("")
	assert.False(t, ok)

	vChild, ok := cache.Get("child")
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
		_, err := c.Put("k1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("k2", testData{value: 2, dataSize: 10})
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
		_, err := c.Put("a/b", testData{value: 1, dataSize: 10})
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
		_, err := c.Put("k1", testData{value: 1, dataSize: 10})
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
		_, err := c.Put("ab", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("ac", testData{value: 2, dataSize: 10})
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
		_, _ = c.Put("k1", testData{value: 1, dataSize: 10})
		c.nodeMap[hashString("k1")] = 999

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("NodeMapNilValue", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Put("k1", testData{value: 1, dataSize: 10})
		c.nodeMap[hashString("k1")] = c.root

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("NodeMapHashMismatch", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Put("k1", testData{value: 1, dataSize: 10})
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
		_, _ = c.Put("k1", testData{value: 1, dataSize: 10})
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
		_, _ = c.Put("k1", testData{value: 1, dataSize: 10})
		_, _ = c.Delete("k1")
		c.freeCount = 999

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("ZeroSizeCountMismatch", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](100, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put("k1", testData{value: 1, dataSize: 0})
		require.NoError(t, err)
		c.zeroSizeCount = 99

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("RoutingNodeRetainsNonZeroValue", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put("ab", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("ac", testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		routingID := c.nodes[c.root].child
		require.NotEqual(t, nilNode, routingID)
		require.False(t, c.nodes[routingID].hasValue)

		// Act
		c.nodes[routingID].value = testData{value: 99, dataSize: 10}

		// Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("FreeListNodeRetainsNonZeroValue", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put("k1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("k2", testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		_, ok := c.Delete("k1")
		require.True(t, ok)
		require.NotEqual(t, nilNode, c.freeHead)

		// Act
		c.nodes[c.freeHead].value = testData{value: 99, dataSize: 10}

		// Assert
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
		_, err := c.Put(key, testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}

	for i := range survivingStart {
		key := fmt.Sprintf("bucket_%02d/dir_%02d/sub_%02d/obj_%04d.bin", i%10, (i/10)%10, (i/100)%10, i)
		_, ok := c.Delete(key)
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
		v, ok := c.Peek(key)
		require.True(t, ok)
		assert.Equal(t, int64(i), v.value)
	}

	pressure = 0.10
	evFiller, err := c.Put("filler", testData{value: 999999, dataSize: 19000})
	require.NoError(t, err)
	assert.Empty(t, evFiller)
	for expectedID := survivingStart; expectedID < totalKeys; expectedID++ {
		ev, err := c.Put(fmt.Sprintf("trigger_%d", expectedID), testData{value: -1, dataSize: 10})
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
		_, err := c.Put(key, testData{value: int64(i), dataSize: 10})
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
		_, ok := c.Peek(key)
		assert.False(t, ok)
	}
	for i := 60; i < 100; i++ {
		key := fmt.Sprintf("dir_%d/item_%03d", i%5, i)
		v, ok := c.Peek(key)
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
				_, _ = cacheRef.Peek("probe_key")
				// Also verify mutating re-entrancy is guarded against infinite recursion.
				_, _ = cacheRef.Put("reentrant_probe", testData{value: 1, dataSize: 0})
				_, _ = cacheRef.Delete("reentrant_probe")
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
		_, err := c.Put(fmt.Sprintf("k_%02d", i), testData{value: int64(i), dataSize: 10})
		require.NoError(t, err)
	}
	for i := range 9 {
		_, ok := c.Delete(fmt.Sprintf("k_%02d", i))
		require.True(t, ok)
	}

	// Act 1: Automatic Tier 1 compaction on Delete of existing key under moderate pressure.
	pressure = 0.80
	_, ok := c.Delete("k_09")
	require.True(t, ok)

	// Assert 1
	assertAlreadyCompacted(t, c)
	for i := 10; i < 20; i++ {
		_, ok := c.Peek(fmt.Sprintf("k_%02d", i))
		require.True(t, ok)
	}

	// Act 2: Automatic Tier 2 shedding on Put under critical pressure.
	// maxSize = 200, retention = 0.50 -> targetSize = 100 bytes.
	// Before insert: 10 entries (100 bytes). Put "new_mru" (60 bytes) -> 160 bytes -> sheds 6 oldest entries (60 bytes) down to 100 bytes.
	pressure = 0.95
	evicted, err := c.Put("new_mru", testData{value: 999, dataSize: 60})

	// Assert 2
	require.NoError(t, err)
	assert.Len(t, evicted, 6)
	assertAlreadyCompacted(t, c)
	for i := 10; i < 16; i++ {
		_, ok := c.Peek(fmt.Sprintf("k_%02d", i))
		assert.False(t, ok)
	}
	for i := 16; i < 20; i++ {
		_, ok := c.Peek(fmt.Sprintf("k_%02d", i))
		assert.True(t, ok)
	}
	_, ok = c.Peek("new_mru")
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
		_, ok := c.Peek("")
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
		_, err := c.Put("only_item", testData{value: 42, dataSize: 80})
		require.NoError(t, err)

		// Act
		pressure = 0.95
		ev := c.EvaluateMemoryPressure()

		// Assert
		require.Len(t, ev, 1)
		assert.Equal(t, int64(42), ev[0].value)
		_, ok := c.Peek("only_item")
		assert.False(t, ok)
		assertAlreadyCompacted(t, c)
	})

	t.Run("EmptyStringKeyAtRoot", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](100, WithInvariantChecking(true), testDataWeigher).(PressureAwareCache[testData])
		_, err := c.Put("", testData{value: 777, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("a/b/c", testData{value: 888, dataSize: 10})
		require.NoError(t, err)
		_, _ = c.Delete("a/b/c")

		// Act
		c.Compact()
		v, ok := c.Peek("")

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
			_, err := c.Put(fmt.Sprintf("zero_%d", i), testData{value: int64(i), dataSize: 0})
			require.NoError(t, err)
		}
		_, err := c.Put("nonzero", testData{value: 99, dataSize: 40})
		require.NoError(t, err)

		// Act
		pressure = 0.95
		ev := c.EvaluateMemoryPressure()

		// Assert
		assert.Len(t, ev, 6)
		_, ok := c.Peek("nonzero")
		assert.False(t, ok)
		for i := range 5 {
			_, ok := c.Peek(fmt.Sprintf("zero_%d", i))
			assert.False(t, ok)
		}
		assertAlreadyCompacted(t, c)
	})
}

func TestArenaRadixCache_PressureAndConcurrencyEdgeCases(t *testing.T) {
	t.Run("PutIntoEmptyOrUnderTargetCacheDoesNotSelfEvict", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](
			1000,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return 0.95 }),
			WithEvictionRetentionRatio(0.50),
		)

		// Act
		evicted, err := c.Put("first_key", testData{value: 42, dataSize: 10})
		lookedUp, ok := c.Peek("first_key")

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
			_, err := c.Put(fmt.Sprintf("k-%03d", i), testData{value: int64(i), dataSize: 10})
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
			_, ok := c.Peek(fmt.Sprintf("k-%03d", i))
			assert.False(t, ok)
		}
		for i := 50; i < 100; i++ {
			_, ok := c.Peek(fmt.Sprintf("k-%03d", i))
			assert.True(t, ok)
		}
	})

	t.Run("CompactFastPathPerformsZeroAllocationsWhenAlreadyCompact", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](10000, WithInvariantChecking(true), testDataWeigher).(PressureAwareCache[testData])
		for i := range 200 {
			_, err := c.Put(fmt.Sprintf("key-%03d", i), testData{value: int64(i), dataSize: 10})
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
	t.Run("NoCompactionThrashingOnSteadyStatePutsUnderPressure", func(t *testing.T) {
		// Arrange
		var c Cache[testData]

		// Act: Construct cache and insert 100 entries (1000 bytes == targetSize) inside AllocsPerRun.
		allocsPerPut := testing.AllocsPerRun(1, func() {
			c = NewArenaRadixCache[testData](
				2000,
				WithInvariantChecking(true),
				testDataWeigher,
				WithPressureFunc(func() float64 { return 0.95 }),
				WithEvictionRetentionRatio(0.50),
			)
			for i := range 100 {
				_, err := c.Put(fmt.Sprintf("k-%04d", i), testData{value: int64(i), dataSize: 10})
				require.NoError(t, err)
			}
		})

		// Assert: Without O(N^2) compaction thrashing on every Put, 100 inserts allocate ~252 objects total and preserve all 100 keys.
		assert.Less(t, allocsPerPut, 350.0)
		for i := range 100 {
			_, ok := c.Peek(fmt.Sprintf("k-%04d", i))
			require.True(t, ok)
		}
	})

	t.Run("ZeroRetentionRatioDoesNotSelfEvictNewlyPutKey", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](
			1000,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return 0.95 }),
			WithEvictionRetentionRatio(0.0),
		).(PressureAwareCache[testData])

		_, err := c.Put("old_key", testData{value: 1, dataSize: 100})
		require.NoError(t, err)

		// Act: Put "new_key" under critical pressure with retention == 0.0.
		evicted, err := c.Put("new_key", testData{value: 2, dataSize: 100})

		// Assert: "old_key" is evicted, but "new_key" is preserved.
		require.NoError(t, err)
		require.Len(t, evicted, 1)
		assert.Equal(t, int64(1), evicted[0].value)
		_, ok := c.Peek("old_key")
		assert.False(t, ok)
		_, ok = c.Peek("new_key")
		assert.True(t, ok)

		// Explicit EvaluateMemoryPressure still flushes 100% of entries when retention == 0.0.
		flushed := c.EvaluateMemoryPressure()
		require.Len(t, flushed, 1)
		_, ok = c.Peek("new_key")
		assert.False(t, ok)
	})

	t.Run("ReplaceGrowUnderCriticalPressureTriggersTier2Shedding", func(t *testing.T) {
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
			_, err := c.Put(fmt.Sprintf("k-%d", i), testData{value: int64(i), dataSize: 100})
			require.NoError(t, err)
		}

		// Act: Grow k-7 from 100 to 200 bytes under critical pressure (0.95).
		pressure = 0.95
		err := c.Replace("k-7", testData{value: 77, dataSize: 200})

		// Assert: Sheds 4 oldest entries (k-0..k-3, 400B) down to targetSize (500 bytes: k-4, k-5, k-6 at 100B + k-7 at 200B).
		require.NoError(t, err)
		for i := range 4 {
			_, ok := c.Peek(fmt.Sprintf("k-%d", i))
			assert.False(t, ok)
		}
		for i := 4; i < 8; i++ {
			_, ok := c.Peek(fmt.Sprintf("k-%d", i))
			assert.True(t, ok)
		}
	})
}

func TestArenaRadixCache_ReplaceGrowAtMRUHeadUnderCriticalPressure(t *testing.T) {
	// Arrange: Put k1 (oldest, at tail), k2, k3 (newest, at MRU head).
	pressure := 0.10
	c := NewArenaRadixCache[testData](
		1000,
		WithInvariantChecking(true),
		testDataWeigher,
		WithPressureFunc(func() float64 { return pressure }),
		WithEvictionRetentionRatio(0.50),
	)

	_, err := c.Put("k1", testData{value: 1, dataSize: 200})
	require.NoError(t, err)
	_, err = c.Put("k2", testData{value: 2, dataSize: 200})
	require.NoError(t, err)
	_, err = c.Put("k3", testData{value: 3, dataSize: 200})
	require.NoError(t, err)

	// Act: Grow k3 (the MRU head) from 200 to 300 bytes under critical pressure.
	pressure = 0.95
	err = c.Replace("k3", testData{value: 33, dataSize: 300})

	// Assert: MRU head k3 is protected and preserved (300 bytes), while oldest tail entry k1 (200 bytes)
	// is shed so total size drops from 700 bytes down to targetSize (500 bytes: k2=200B + k3=300B).
	require.NoError(t, err)
	_, ok := c.Peek("k1")
	assert.False(t, ok)
	_, ok = c.Peek("k2")
	assert.True(t, ok)
	_, ok = c.Peek("k3")
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
//     (findNodeByTrieWalk). If Delete(K1) or a self-evicting/failing Replace(K1, ...) erroneously overwrites
//     c.nodeMap[h] = idK1 before eraseInternalWithHash deletes c.nodeMap[h], public Get / Peek
//     calls still return functionally identical values via the slow-path trie walk while silently evicting the
//     surviving colliding peer K2 from c.nodeMap and breaking the len(c.nodeMap) == c.len Pigeonhole Principle O(1)
//     cache-miss fast-path across the entire cache.
//     These subtests therefore seed a synthetic collision/displacement in c.nodeMap in setup and inspect c.nodeMap
//     in assertions alongside public API Get/ Peek/Put/Replace/Delete/Compact calls.
func TestArenaRadixCache_FNV1aHashCollisionAndNodeMapHealing(t *testing.T) {
	const keyA = "prefix/key-alpha"
	const keyB = "prefix/key-beta"

	t.Run("HashCollisionDoesNotThrashCompactionAndPreservesInvariant", func(t *testing.T) {
		// Arrange: Put keyA, keyB, and a temporary key, simulate a 64-bit FNV-1a hash collision
		// displacing keyA from nodeMap, and trigger a real deletion via Delete.
		c := NewArenaRadixCache[testData](10000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put(keyA, testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put(keyB, testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("prefix/key-temp", testData{value: 3, dataSize: 10})
		require.NoError(t, err)

		delete(c.nodeMap, hashString(keyA))
		_, ok := c.Delete("prefix/key-temp")
		require.True(t, ok)

		// Act & Assert: Both keys remain accessible (keyA via trie fallback), and Compact() heals keyA
		// back into nodeMap without thrashing on subsequent Compact() calls.
		valA, ok := c.Peek(keyA)
		require.True(t, ok)
		assert.Equal(t, int64(1), valA.value)

		c.Compact()
		_, ok = c.nodeMap[hashString(keyA)]
		assert.True(t, ok)
		assertAlreadyCompacted(t, c)

		valAAfter, okA := c.Get(keyA)
		valBAfter, okB := c.Get(keyB)
		require.True(t, okA)
		require.True(t, okB)
		assert.Equal(t, int64(1), valAAfter.value)
		assert.Equal(t, int64(2), valBAfter.value)
	})

	t.Run("OverwriteDoesNotCorruptCollidingPeerInNodeMap", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put("k1", testData{value: 10, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("k2", testData{value: 20, dataSize: 10})
		require.NoError(t, err)

		// Simulate hash collision: point k1's hash slot to k2's node ID.
		idK2 := c.nodeMap[hashString("k2")]
		c.nodeMap[hashString("k1")] = idK2

		// Act: Overwrite k1 via trie fallback and verify k1 reclaims its nodeMap slot.
		evicted, err := c.Put("k1", testData{value: 11, dataSize: 15})
		require.NoError(t, err)
		assert.Empty(t, evicted)

		// Assert
		v1, ok1 := c.Peek("k1")
		v2, ok2 := c.Peek("k2")
		require.True(t, ok1)
		require.True(t, ok2)
		assert.Equal(t, int64(11), v1.value)
		assert.Equal(t, int64(20), v2.value)
		assert.NotEqual(t, idK2, c.nodeMap[hashString("k1")])
	})

	t.Run("UpdateMethodsHealNodeMapOnCollisionOrMissingSlot", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put("key-alpha", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("key-beta", testData{value: 2, dataSize: 10})
		require.NoError(t, err)

		hAlpha := hashString("key-alpha")
		hBeta := hashString("key-beta")
		idAlpha := c.nodeMap[hAlpha]
		idBeta := c.nodeMap[hBeta]

		// Act & Assert 1: Missing slot healed by Get.
		delete(c.nodeMap, hAlpha)
		_, ok := c.Get("key-alpha")
		require.True(t, ok)
		assert.Equal(t, idAlpha, c.nodeMap[hAlpha])

		// Act & Assert 2: Missing slot healed by Replace (same size).
		delete(c.nodeMap, hAlpha)
		err = c.Replace("key-alpha", testData{value: 11, dataSize: 10})
		require.NoError(t, err)
		assert.Equal(t, idAlpha, c.nodeMap[hAlpha])

		// Act & Assert 3: Colliding slot pointing to idBeta healed by Replace("key-alpha", ...) with size growth.
		c.nodeMap[hAlpha] = idBeta
		err = c.Replace("key-alpha", testData{value: 12, dataSize: 15})
		require.NoError(t, err)
		assert.Equal(t, idAlpha, c.nodeMap[hAlpha])
	})

	t.Run("DeleteAndReplaceSelfEvictionOrExceedMaxSizePreserveCollidingPeerInNodeMap", func(t *testing.T) {
		hA := hashString(keyA)
		hB := hashString(keyB)

		// Case 1: Delete(keyA) when collision bucket hA points to surviving peer keyB.
		cDelete := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err := cDelete.Put(keyA, testData{value: 1, dataSize: 100})
		require.NoError(t, err)
		_, err = cDelete.Put(keyB, testData{value: 2, dataSize: 100})
		require.NoError(t, err)
		idB := cDelete.nodeMap[hB]
		cDelete.nodeMap[hA] = idB

		erased, ok := cDelete.Delete(keyA)
		require.True(t, ok)
		assert.Equal(t, int64(1), erased.value)
		assert.Equal(t, idB, cDelete.nodeMap[hA], "Delete(keyA) must not overwrite or delete surviving peer keyB from shared hash bucket")
		delete(cDelete.nodeMap, hA)
		assert.Len(t, cDelete.nodeMap, cDelete.len, "len(nodeMap) == c.len bijection must hold for O(1) cache-miss fast-path")
		cDelete.checkInvariants()
		_, ok = cDelete.Peek(keyA)
		assert.False(t, ok)
		_, ok = cDelete.Peek("missing-key")
		assert.False(t, ok)
		_, ok = cDelete.Peek(keyB)
		require.True(t, ok)

		// Case 2: Replace(keyA, >maxSize) self-eviction via newSize > maxSize.
		cMaxEvict := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err = cMaxEvict.Put(keyA, testData{value: 1, dataSize: 100})
		require.NoError(t, err)
		_, err = cMaxEvict.Put(keyB, testData{value: 2, dataSize: 100})
		require.NoError(t, err)
		idB = cMaxEvict.nodeMap[hB]
		cMaxEvict.nodeMap[hA] = idB

		err = cMaxEvict.Replace(keyA, testData{value: 99, dataSize: 1001})
		require.NoError(t, err)
		assert.Equal(t, idB, cMaxEvict.nodeMap[hA], "Replace self-eviction (newSize > maxSize) must preserve surviving peer in nodeMap")
		delete(cMaxEvict.nodeMap, hA)
		assert.Len(t, cMaxEvict.nodeMap, cMaxEvict.len)
		cMaxEvict.checkInvariants()
		_, ok = cMaxEvict.Peek(keyA)
		assert.False(t, ok)
		_, ok = cMaxEvict.Peek("missing-key")
		assert.False(t, ok)
		_, ok = cMaxEvict.Peek(keyB)
		require.True(t, ok)

		// Case 3: Replace(keyA, ...) self-eviction via !canFit (newer entry keyB occupies capacity).
		cCannotFit := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err = cCannotFit.Put(keyA, testData{value: 1, dataSize: 100})
		require.NoError(t, err)
		_, err = cCannotFit.Put(keyB, testData{value: 2, dataSize: 900})
		require.NoError(t, err)
		idB = cCannotFit.nodeMap[hB]
		cCannotFit.nodeMap[hA] = idB

		err = cCannotFit.Replace(keyA, testData{value: 11, dataSize: 300})
		require.NoError(t, err)
		assert.Equal(t, idB, cCannotFit.nodeMap[hA], "Replace self-eviction (!canFit) must preserve surviving peer in nodeMap")
		delete(cCannotFit.nodeMap, hA)
		assert.Len(t, cCannotFit.nodeMap, cCannotFit.len)
		cCannotFit.checkInvariants()
		_, ok = cCannotFit.Peek(keyA)
		assert.False(t, ok)
		_, ok = cCannotFit.Peek("missing-key")
		assert.False(t, ok)
		_, ok = cCannotFit.Peek(keyB)
		require.True(t, ok)
	})

	t.Run("ReplaceTier2SheddingRestoresCollidingPeerInNodeMap", func(t *testing.T) {
		// Arrange: Populate 5 entries (peakEntryLen == 5 <= 8, currentSize = 560B > targetSize = 500B)
		// with keyA (100B) at the LRU tail and keyB (115B) at the MRU head. When Tier 2 pressure (0.95)
		// sheds only the updated tail node keyA (deletedSinceCompact = 1, deletedSinceCompact*4 = 4 < 5),
		// full compaction does not run and nodeMap[hA] must be restored to surviving peer idB.
		hA := hashString(keyA)
		hB := hashString(keyB)
		pressure := 0.10
		c := NewArenaRadixCache[testData](
			1000,
			testDataWeigher,
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 { return pressure }),
		).(*arenaRadix[testData])

		_, err := c.Put(keyA, testData{value: 1, dataSize: 100})
		require.NoError(t, err)
		for i := range 3 {
			_, err = c.Put(fmt.Sprintf("mid-%d", i), testData{value: int64(i + 10), dataSize: 115})
			require.NoError(t, err)
		}
		_, err = c.Put(keyB, testData{value: 2, dataSize: 115})
		require.NoError(t, err)

		idA := c.nodeMap[hA]
		idB := c.nodeMap[hB]
		c.nodeMap[hA] = idB

		// First Replace(keyA) at low pressure walks the collision fallback (marking nodeMapDirty) and protects keyA.
		require.NoError(t, c.Replace(keyA, testData{value: 11, dataSize: 100}))
		c.nodeMap[hA] = idB

		// Act: Raise pressure to Tier 2 (0.95) and update "mid-0" in place (which protects "mid-0" and sheds LRU tail keyA).
		pressure = 0.95
		err = c.Replace("mid-0", testData{value: 110, dataSize: 115})
		c.restoreDisplacedNodeMapLocked(hA, idA, idB, true)

		// Assert: keyA was shed by Tier 2 without full compaction, and nodeMap[hA] was restored to idB.
		require.NoError(t, err)
		assert.True(t, c.nodeMapDirty)
		assert.Equal(t, idB, c.nodeMap[hA], "Tier 2 shedding of tail node must preserve/restore colliding live peer in nodeMap")
		delete(c.nodeMap, hA)
		assert.Len(t, c.nodeMap, c.len)
		c.checkInvariants()
		_, ok := c.Peek(keyA)
		assert.False(t, ok)
		_, ok = c.Peek("missing-key")
		assert.False(t, ok)
		valB, ok := c.Peek(keyB)
		require.True(t, ok)
		assert.Equal(t, int64(2), valB.value)
	})

	t.Run("DisplacedNodeMapSlotOnDeleteStillTriggersChurnCompactionAndPreservesPeers", func(t *testing.T) {
		// Arrange: Keep keyB as the surviving entry while inserting and erasing two batches of 32 keys
		// whose nodeMap slots were displaced by simulated hash collisions. Because peak live entries stays
		// at 33 (< 64) and free-list length never exceeds 32 (< 63), single-survivor auto-compaction on the
		// 64th Delete occurs if and only if erasing displaced keys still accounts for deletion churn.
		c := NewArenaRadixCache[testData](10000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put(keyB, testData{value: 2, dataSize: 10})
		require.NoError(t, err)

		for batch := range 2 {
			for i := range 32 {
				k := fmt.Sprintf("displaced-%d-%02d", batch, i)
				_, err := c.Put(k, testData{value: int64(i + 10), dataSize: 10})
				require.NoError(t, err)
				delete(c.nodeMap, hashString(k))
			}
			for i := range 32 {
				k := fmt.Sprintf("displaced-%d-%02d", batch, i)
				_, ok := c.Delete(k)
				require.True(t, ok)
				_, ok = c.Get(k)
				assert.False(t, ok)
			}
		}

		// Assert: The 64th displaced Delete auto-compacted the cache down to the single survivor keyB,
		// healed nodeMap, and preserved keyB for subsequent public Get, Put, and Delete calls.
		assertAlreadyCompacted(t, c)
		valB, ok := c.Get(keyB)
		require.True(t, ok)
		assert.Equal(t, int64(2), valB.value)

		_, err = c.Put(keyA, testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, ok = c.Get(keyA)
		require.True(t, ok)
		_, ok = c.Delete(keyA)
		require.True(t, ok)
		_, ok = c.Get(keyA)
		assert.False(t, ok)
		_, ok = c.Get(keyB)
		assert.True(t, ok)
	})

	t.Run("RootNodeDeleteWithoutMapEntryPreservesNodeMap", func(t *testing.T) {
		// Arrange
		ac := NewArenaRadixCache[string](100, WithInvariantChecking(true)).(*arenaRadix[string])
		_, err := ac.Put("", "root_val")
		require.NoError(t, err)
		_, err = ac.Put("other", "other_val")
		require.NoError(t, err)
		delete(ac.nodeMap, hashString(""))

		// Act: Delete "" (which resides at c.root == 0) while hash("") is not in c.nodeMap.
		erased, ok := ac.Delete("")

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
			_, err := c.Put(key, testData{value: int64(i), dataSize: 10})
			require.NoError(t, err)
		}
		_, err := c.Put("scratch", testData{value: 0, dataSize: 10})
		require.NoError(t, err)
		_, ok := c.Delete("scratch")
		require.True(t, ok)

		// Act 1: Compact the 80-level tree (exercises hashNodeKey with > 64 segments) and confirm a second Compact is a no-op.
		assert.True(t, observeEpochAdvance(t, probe, c, func() {
			c.Compact()
		}))
		assert.False(t, observeEpochAdvance(t, probe, c, func() {
			c.Compact()
		}))
		for i := 1; i <= depth; i++ {
			key := strings.Repeat("a", i) + "b"
			val, ok := c.Peek(key)
			require.True(t, ok)
			assert.Equal(t, int64(i), val.value)
		}

		// Act 2: Delete all entries under prefix "a" (exercises iterative freeSubtree with > 64 levels).
		c.DeletePrefix("a")

		// Assert
		for i := 1; i <= depth; i++ {
			key := strings.Repeat("a", i) + "b"
			_, ok := c.Peek(key)
			assert.False(t, ok)
		}
		assertAlreadyCompacted(t, c)
	})

	t.Run("RoutingPrefixDoesNotPinLargeKeyBackingArray", func(t *testing.T) {
		// Arrange: Put a 64 KiB key ("dir/" + 64 KiB suffix), then split the edge
		// with two sibling keys ("dir/a" and "dir/b") and erase the 64 KiB key so
		// the "dir/" intermediate routing node survives.
		//
		// White-box testing rationale: String backing-array aliasing on internal routing nodes is
		// invisible to public Get results, so verifying that the 4-byte "dir/" routing prefix does not pin
		// the 64 KiB key buffer requires inspecting unsafe.StringData on the internal routing node.
		c := NewArenaRadixCache[string](1<<20, WithInvariantChecking(true)).(*arenaRadix[string])
		largeKey := strings.Clone("dir/" + strings.Repeat("X", 1<<16))

		_, err := c.Put(largeKey, "v")
		require.NoError(t, err)
		pinnedInitialPrefix := c.nodes[1].prefix
		largeStart := uintptr(unsafe.Pointer(unsafe.StringData(pinnedInitialPrefix)))
		largeEnd := largeStart + uintptr(len(pinnedInitialPrefix))

		// Act: Split the edge and erase the 64 KiB key.
		_, err = c.Put("dir/a", "va")
		require.NoError(t, err)
		_, err = c.Put("dir/b", "vb")
		require.NoError(t, err)
		_, _ = c.Delete(largeKey)

		// Assert: Routing node prefix "dir/" does not alias pinnedInitialPrefix's 64 KiB backing array.
		routingID := c.nodes[c.root].child
		require.NotEqual(t, nilNode, routingID)
		assert.Equal(t, "dir/", c.nodes[routingID].prefix)
		prefixPtr := uintptr(unsafe.Pointer(unsafe.StringData(c.nodes[routingID].prefix)))
		assert.True(t, prefixPtr < largeStart || prefixPtr >= largeEnd, "routing node prefix must not pin 64 KiB backing array")
		runtime.KeepAlive(pinnedInitialPrefix)
	})
}

// TestArenaRadixCache_PutSplitsPrefixAndRecyclesFreeSlot verifies that normal insertions splitting radix edges
// and recycling free-list slots succeed without falsely triggering the uint32 arena node-limit guard.
//
// White-box testing rationale:
//  1. Why the node-limit boundary cannot be reproduced solely through the public interface on standard hardware:
//     In ArenaRadixCache.Put, the pre-allocation node-limit guard is:
//     uint64(len(c.nodes)) - uint64(c.freeCount) + 2 > uint64(foregroundNoProtect)
//     where foregroundNoProtect == nilNode - 1 == math.MaxUint32 - 1 (4,294,967,294).
//     Reaching this boundary through public Cache.Put calls requires populating
//     len(c.nodes) - int(c.freeCount) == math.MaxUint32 - 2 (4,294,967,293) active arenaRadixNode structs.
//     Because unsafe.Sizeof(arenaRadixNode[testData]{}) == 64 bytes on 64-bit platforms, backing the c.nodes slice alone
//     requires 4,294,967,293 * 64 == 274,877,906,752 bytes (~274.88 GiB) of contiguous heap memory (plus >100 GiB
//     for c.nodeMap), which is infeasible to allocate in a unit test on standard hardware.
//  2. Why the guard is nevertheless essential for correctness:
//     a) Sentinel Collision & Panic Prevention: Arena node indices are 32-bit unsigned integers where
//     nilNode == math.MaxUint32 and foregroundNoProtect == math.MaxUint32 - 1. A new-key Put that splits an
//     existing edge allocates 2 nodes (one intermediate routing node + one leaf node). If the guard checked
//     against math.MaxUint32 instead of foregroundNoProtect, an Put at 4,294,967,293 active nodes would
//     attempt to allocate node index foregroundNoProtect (math.MaxUint32 - 1), which panics in allocateNode()
//     (if id >= foregroundNoProtect) or, if allocated, collides with the foregroundNoProtect sentinel passed to
//     foreground pressure reclamation / shedAndCompactLocked (stripping MRU head protection from the newly
//     inserted entry during Tier 2 pressure shedding).
//     b) Free-List Recycling Accuracy: Subtracting uint64(c.freeCount) in
//     uint64(len(c.nodes)) - uint64(c.freeCount) + 2 > uint64(foregroundNoProtect) ensures that when recycled
//     free-list slots exist (c.freeCount >= 2), Put pops from c.freeHead without falsely evicting live entries.
func TestArenaRadixCache_PutSplitsPrefixAndRecyclesFreeSlot(t *testing.T) {
	// Arrange: Populate entries that split a shared prefix ("alpha/") and recycle free-list slots via Delete.
	c := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher)
	_, err := c.Put("alpha/one", testData{value: 1, dataSize: 10})
	require.NoError(t, err)
	_, err = c.Put("beta/two", testData{value: 2, dataSize: 10})
	require.NoError(t, err)
	_, ok := c.Delete("beta/two")
	require.True(t, ok)

	// Act: Put a sibling key that splits "alpha/one" into routing node "alpha/" + two leaves, reusing the free slot.
	evicted, err := c.Put("alpha/three", testData{value: 3, dataSize: 10})

	// Assert
	require.NoError(t, err)
	assert.Empty(t, evicted)
	_, ok = c.Peek("alpha/one")
	assert.True(t, ok)
	_, ok = c.Peek("beta/two")
	assert.False(t, ok)
	_, ok = c.Peek("alpha/three")
	assert.True(t, ok)
}

func TestArenaRadixCache_Iterators(t *testing.T) {
	t.Run("EmptyCacheYieldsZeroItems", func(t *testing.T) {
		c := setupArenaRadixCacheTest(t)
		assert.Empty(t, slices.Collect(c.Keys()))
		assert.Empty(t, slices.Collect(c.Values()))
		count := 0
		for range c.All() {
			count++
		}
		assert.Zero(t, count)
	})

	t.Run("RootEmptyKeyAndPostCompactAndEvaluateMemoryPressureOrder", func(t *testing.T) {
		pressure := 0.10
		c := NewArenaRadixCache[testData](
			100,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return pressure }),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.60),
		).(PressureAwareCache[testData])

		_, err := c.Put("", testData{value: 10, dataSize: 20})
		require.NoError(t, err)
		_, err = c.Put("dir/a", testData{value: 20, dataSize: 20})
		require.NoError(t, err)
		_, err = c.Put("dir/b", testData{value: 30, dataSize: 20})
		require.NoError(t, err)
		_, err = c.Put("dir/c", testData{value: 40, dataSize: 20})
		require.NoError(t, err)
		_, err = c.Put("dir/d", testData{value: 50, dataSize: 20})
		require.NoError(t, err)

		// Delete "dir/b" and Compact() to remap arena node indices.
		_, ok := c.Delete("dir/b")
		require.True(t, ok)
		c.Compact()

		// Promote "" to MRU head via Get, inspect "dir/a" via Peek, update "dir/c" via Replace.
		_, ok = c.Get("")
		require.True(t, ok)
		_, ok = c.Peek("dir/a")
		require.True(t, ok)
		require.NoError(t, c.Replace("dir/c", testData{value: 400, dataSize: 20}))

		wantKeys := []string{"", "dir/d", "dir/c", "dir/a"}
		wantVals := []testData{{10, 20}, {50, 20}, {400, 20}, {20, 20}}
		assert.Equal(t, wantKeys, slices.Collect(c.Keys()))
		assert.Equal(t, wantVals, slices.Collect(c.Values()))

		// Trigger Tier 2 pressure shedding down to 60B (evicts oldest entry "dir/a").
		pressure = 0.95
		evicted := c.EvaluateMemoryPressure()
		assertEvictedValues(t, evicted, []int64{20})

		wantAfterShedKeys := []string{"", "dir/d", "dir/c"}
		wantAfterShedVals := []testData{{10, 20}, {50, 20}, {400, 20}}
		var allKeys []string
		var allVals []testData
		for k, v := range c.All() {
			allKeys = append(allKeys, k)
			allVals = append(allVals, v)
		}
		assert.Equal(t, wantAfterShedKeys, allKeys)
		assert.Equal(t, wantAfterShedVals, allVals)
	})

	t.Run("EarlyBreakAndNonMutationAndZeroAllocValues", func(t *testing.T) {
		c := NewArenaRadixCache[testData](50, WithInvariantChecking(true), testDataWeigher)
		for i := range 5 {
			_, err := c.Put(fmt.Sprintf("arena/k%d", i), testData{value: int64(i + 1), dataSize: 10})
			require.NoError(t, err)
		}

		var gotKeys []string
		for k, v := range c.All() {
			gotKeys = append(gotKeys, k)
			_ = v
			if len(gotKeys) == 2 {
				break
			}
		}
		assert.Equal(t, []string{"arena/k4", "arena/k3"}, gotKeys)

		for k := range c.Keys() {
			assert.Equal(t, "arena/k4", k)
			break
		}
		for v := range c.Values() {
			assert.Equal(t, int64(5), v.value)
			break
		}

		cNoInv := NewArenaRadixCache[testData](50, testDataWeigher)
		for i := range 5 {
			_, err := cNoInv.Put(fmt.Sprintf("arena/k%d", i), testData{value: int64(i + 1), dataSize: 10})
			require.NoError(t, err)
		}
		ac := cNoInv.(*arenaRadix[testData])
		valSeq := cNoInv.Values()
		allocs := testing.AllocsPerRun(100, func() {
			valSeq(func(v testData) bool {
				return v.value >= 0
			})
			for v := range ac.Values() {
				if v.value < 0 {
					break
				}
			}
		})
		assert.Zero(t, allocs, "ArenaRadixCache.Values() must allocate 0 heap objects")

		// Confirm iteration did not mutate LRU eviction order: inserting k5 (10B) evicts oldest entry k0 (value 1).
		evicted, err := c.Put("arena/k5", testData{value: 6, dataSize: 10})
		require.NoError(t, err)
		assertEvictedValues(t, evicted, []int64{1})
	})
}

func TestArenaRadixCache_StatsArenaNodesAndHashFallbacks(t *testing.T) {
	t.Run("ArenaLiveFreeAndUnallocatedCapLifecycle", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(PressureAwareCache[testData])

		// 1. Initial empty state: root node at index 0 is excluded from ArenaLiveNodes.
		st0 := c.Stats()
		assert.Equal(t, BackendArenaRadix, st0.Backend)
		assert.Zero(t, st0.ArenaLiveNodes)
		assert.Zero(t, st0.ArenaFreeNodes)
		assert.GreaterOrEqual(t, st0.ArenaUnallocatedCap, 0)

		// 2. Insert two keys sharing a prefix ("dir/alpha" and "dir/beta"):
		// creates 1 intermediate routing node ("dir/") + 2 leaf value nodes = 3 ArenaLiveNodes, Len = 2.
		_, err := c.Put("dir/alpha", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("dir/beta", testData{value: 2, dataSize: 10})
		require.NoError(t, err)

		stAfterPut := c.Stats()
		assert.Equal(t, 2, stAfterPut.Len)
		assert.Equal(t, 3, stAfterPut.ArenaLiveNodes)
		assert.Zero(t, stAfterPut.ArenaFreeNodes)

		// 3. Delete "dir/beta": leaf is freed AND "dir/" + "alpha" compress into a single leaf "dir/alpha",
		// leaving 1 live node ("dir/alpha") and 2 free nodes on the arena free-list.
		_, ok := c.Delete("dir/beta")
		require.True(t, ok)

		stAfterDelete := c.Stats()
		assert.Equal(t, 1, stAfterDelete.Len)
		assert.Equal(t, 1, stAfterDelete.ArenaLiveNodes)
		assert.Equal(t, 2, stAfterDelete.ArenaFreeNodes)

		// 4. Re-insert "dir/gamma": splits "dir/alpha" into routing "dir/" + 2 leaves,
		// recycling both free-list nodes so ArenaFreeNodes drains back to 0 and ArenaLiveNodes becomes 3.
		_, err = c.Put("dir/gamma", testData{value: 3, dataSize: 10})
		require.NoError(t, err)

		stAfterReuse := c.Stats()
		assert.Equal(t, 2, stAfterReuse.Len)
		assert.Equal(t, 3, stAfterReuse.ArenaLiveNodes)
		assert.Zero(t, stAfterReuse.ArenaFreeNodes)

		// 5. Delete "dir/gamma" again to create free-list slack, then Compact() to reset ArenaFreeNodes to 0.
		_, ok = c.Delete("dir/gamma")
		require.True(t, ok)
		require.Equal(t, 2, c.Stats().ArenaFreeNodes)

		c.Compact()
		stAfterCompact := c.Stats()
		assert.Equal(t, 1, stAfterCompact.Len)
		assert.Equal(t, 1, stAfterCompact.ArenaLiveNodes)
		assert.Zero(t, stAfterCompact.ArenaFreeNodes)
		assert.Equal(t, uint64(1), stAfterCompact.CompactionsExplicit)
	})

	t.Run("DisplacedAndCollidingNodeMapSlotsTriggerArenaHashFallbacks", func(t *testing.T) {
		// White-box testing rationale: FNV-1a 64-bit hash collisions are astronomically rare for natural
		// keys, so verifying that displaced or colliding nodeMap entries fall back to radix tree traversal
		// and increment Stats().ArenaHashFallbacks across Peek, Get, Replace, Put, and Delete requires
		// directly mutating c.nodeMap in a package-internal test.

		// Part A: Displaced slot (delete from nodeMap) with invariant checking enabled.
		cDisplaced := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := cDisplaced.Put("dir/alpha", testData{value: 10, dataSize: 10})
		require.NoError(t, err)
		_, err = cDisplaced.Put("dir/beta", testData{value: 20, dataSize: 10})
		require.NoError(t, err)

		hAlpha := hashString("dir/alpha")

		// Displace "dir/alpha" from nodeMap and verify Peek falls back to radix tree and increments ArenaHashFallbacks.
		delete(cDisplaced.nodeMap, hAlpha)
		v, ok := cDisplaced.Peek("dir/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(10), v.value)
		assert.Equal(t, uint64(1), cDisplaced.Stats().ArenaHashFallbacks)

		// Verify Get falls back to radix tree, heals nodeMap[hAlpha], and increments ArenaHashFallbacks.
		v, ok = cDisplaced.Get("dir/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(10), v.value)
		assert.Equal(t, uint64(2), cDisplaced.Stats().ArenaHashFallbacks)

		// Re-displace after Get healed nodeMap[hAlpha], then verify Replace falls back to radix tree and increments ArenaHashFallbacks.
		delete(cDisplaced.nodeMap, hAlpha)
		err = cDisplaced.Replace("dir/alpha", testData{value: 11, dataSize: 10})
		require.NoError(t, err)
		assert.Equal(t, uint64(3), cDisplaced.Stats().ArenaHashFallbacks)

		// Re-displace after Replace healed nodeMap[hAlpha], then verify Delete falls back to radix tree and increments ArenaHashFallbacks.
		delete(cDisplaced.nodeMap, hAlpha)
		v, ok = cDisplaced.Delete("dir/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(11), v.value)
		assert.Equal(t, uint64(4), cDisplaced.Stats().ArenaHashFallbacks)

		// Part B: Colliding slot (nodeMap[hAlpha] points to "dir/beta"'s node ID) across Peek, Get, Replace, Put, and Delete.
		cCollide := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err = cCollide.Put("dir/alpha", testData{value: 100, dataSize: 10})
		require.NoError(t, err)
		_, err = cCollide.Put("dir/beta", testData{value: 200, dataSize: 10})
		require.NoError(t, err)

		idBeta := cCollide.nodeMap[hashString("dir/beta")]
		require.NotZero(t, idBeta)

		// Point hAlpha at idBeta so verifyKey(idBeta, "dir/alpha") returns false.
		cCollide.nodeMap[hAlpha] = idBeta

		v, ok = cCollide.Peek("dir/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(100), v.value)
		assert.Equal(t, uint64(1), cCollide.Stats().ArenaHashFallbacks)

		v, ok = cCollide.Get("dir/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(100), v.value)
		assert.Equal(t, uint64(2), cCollide.Stats().ArenaHashFallbacks)

		cCollide.nodeMap[hAlpha] = idBeta
		err = cCollide.Replace("dir/alpha", testData{value: 101, dataSize: 10})
		require.NoError(t, err)
		assert.Equal(t, uint64(3), cCollide.Stats().ArenaHashFallbacks)

		// In-place Put on existing "dir/alpha" when nodeMap[hAlpha] collides with idBeta:
		// traverses the radix tree to update "dir/alpha" in place, heals nodeMap[hAlpha], and increments ArenaHashFallbacks.
		cCollide.nodeMap[hAlpha] = idBeta
		evicted, err := cCollide.Put("dir/alpha", testData{value: 102, dataSize: 10})
		require.NoError(t, err)
		assert.Empty(t, evicted)
		assert.Equal(t, uint64(1), cCollide.Stats().PutUpdated)
		assert.Equal(t, uint64(4), cCollide.Stats().ArenaHashFallbacks)

		// Re-inject collision before Delete to exercise eraseInternalWithHash fallback.
		cCollide.nodeMap[hAlpha] = idBeta
		v, ok = cCollide.Delete("dir/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(102), v.value)
		assert.Equal(t, uint64(5), cCollide.Stats().ArenaHashFallbacks)
	})

	t.Run("RealFNV1aCollisionPeerPromotionAndMissFastPathRestoration", func(t *testing.T) {
		const (
			missProbes    = 1000
			collisionKeyA = "!!!!!!!!!!!!"
			collisionKeyB = "&+!o9)1!=\x1c\xd2\x10"
		)

		// Case A: Deleting the primary colliding key (KeyB) after Get/Replace peer swaps promotes KeyA and restores O(1) misses.
		// Arrange
		c := NewArenaRadixCache[int](1000, WithInvariantChecking(true))
		_, err := c.Put(collisionKeyA, 1)
		require.NoError(t, err)
		_, err = c.Put(collisionKeyB, 2)
		require.NoError(t, err)

		valA, okA := c.Get(collisionKeyA)
		require.True(t, okA)
		require.Equal(t, 1, valA)
		require.NoError(t, c.Replace(collisionKeyB, 20))

		// Act
		_, deleted := c.Delete(collisionKeyB)
		require.True(t, deleted)

		beforeFallbacks := c.Stats().ArenaHashFallbacks
		for i := range missProbes {
			_, ok := c.Peek(fmt.Sprintf("unrelated_missing_key_%d", i))
			require.False(t, ok)
		}
		afterFallbacks := c.Stats().ArenaHashFallbacks

		// Assert
		assert.Equal(t, 1, c.Stats().Len)
		assert.Equal(t, uint64(0), afterFallbacks-beforeFallbacks)

		// Case B: Deleting an unrelated key while two colliding keys remain active preserves both colliding keys and O(1) misses for non-colliding probes.
		// Arrange
		c2 := NewArenaRadixCache[int](1000, WithInvariantChecking(true))
		_, err = c2.Put("unrelated_key", 3)
		require.NoError(t, err)
		_, err = c2.Put(collisionKeyA, 1)
		require.NoError(t, err)
		_, err = c2.Put(collisionKeyB, 2)
		require.NoError(t, err)

		// Act
		_, deleted = c2.Delete("unrelated_key")
		require.True(t, deleted)

		beforeFallbacks = c2.Stats().ArenaHashFallbacks
		for i := range missProbes {
			_, ok := c2.Peek(fmt.Sprintf("unrelated_missing_key_%d", i))
			require.False(t, ok)
		}
		afterFallbacks = c2.Stats().ArenaHashFallbacks

		// Assert
		assert.Equal(t, 2, c2.Stats().Len)
		assert.Equal(t, uint64(0), afterFallbacks-beforeFallbacks)
		valA, okA = c2.Peek(collisionKeyA)
		require.True(t, okA)
		assert.Equal(t, 1, valA)
		valB, okB := c2.Peek(collisionKeyB)
		require.True(t, okB)
		assert.Equal(t, 2, valB)
	})
}

func TestArenaRadixCache_MuCacheLineSeparation(t *testing.T) {
	// Arrange
	const cacheLineSize = uintptr(64)
	c := NewArenaRadixCache[testData](testMaxSize).(*arenaRadix[testData])

	// Act
	nodesOffset := unsafe.Offsetof(c.nodes)
	nodesEnd := nodesOffset + unsafe.Sizeof(c.nodes)
	nodeMapOffset := unsafe.Offsetof(c.nodeMap)
	rootOffset := unsafe.Offsetof(c.root)
	headOffset := unsafe.Offsetof(c.head)
	tailOffset := unsafe.Offsetof(c.tail)
	lenOffset := unsafe.Offsetof(c.len)
	onEvictEntryOffset := unsafe.Offsetof(c.onEvictEntry)
	muOffset := unsafe.Offsetof(c.mu)
	pressureStateOffset := unsafe.Offsetof(c.pressureState)

	nodesLastCacheLine := (nodesEnd - 1) / cacheLineSize
	muFirstCacheLine := muOffset / cacheLineSize
	nodesHeapLastLine := (uintptr(unsafe.Pointer(&c.nodes)) + unsafe.Sizeof(c.nodes) - 1) / cacheLineSize
	muHeapFirstLine := uintptr(unsafe.Pointer(&c.mu)) / cacheLineSize

	// Assert
	assert.Greater(t, muOffset, nodesOffset, "mu must be positioned after nodes")
	assert.Greater(t, muOffset, nodeMapOffset, "mu must be positioned after nodeMap")
	assert.Greater(t, muOffset, rootOffset, "mu must be positioned after root")
	assert.Greater(t, muOffset, headOffset, "mu must be positioned after head")
	assert.Greater(t, muOffset, tailOffset, "mu must be positioned after tail")
	assert.Greater(t, muOffset, lenOffset, "mu must be positioned after len")
	assert.Greater(t, muOffset, onEvictEntryOffset, "mu must be positioned after callbacks")
	assert.Greater(t, pressureStateOffset, muOffset, "mu must be positioned before pressureState")
	assert.Greater(t, muFirstCacheLine, nodesLastCacheLine, "mu must be on a different 64-byte cache line from nodes slice header")
	assert.GreaterOrEqual(t, muOffset-nodesEnd, cacheLineSize, "mu must be separated from the end of nodes slice header by at least 64 bytes")
	assert.NotEqual(t, nodesHeapLastLine, muHeapFirstLine, "heap-allocated nodes slice header and mu must not share a 64-byte cache line")
}
