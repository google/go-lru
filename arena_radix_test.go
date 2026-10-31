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
	"reflect"
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
		if childID != nilNode && !c.nodes[childID].hasValue() {
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
		c.nodeMapSet(c.hashKey("k1"), 999)

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("NodeMapNilValue", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Put("k1", testData{value: 1, dataSize: 10})
		c.nodeMapSet(c.hashKey("k1"), c.root)

		// Act & Assert
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("NodeMapHashMismatch", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Put("k1", testData{value: 1, dataSize: 10})
		id := c.nodeMapAt(c.hashKey("k1"))
		c.nodeMapSet(12345, id)

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
		require.False(t, c.nodes[routingID].hasValue())

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

	t.Run("RootNonZeroKeyLen", func(t *testing.T) {
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		c.nodes[c.root].setKeyLen(1)
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("ChildInvalidKeyLen", func(t *testing.T) {
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put("a/b", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		childID := c.nodes[c.root].child
		require.NotEqual(t, nilNode, childID)
		c.nodes[childID].setKeyLen(99)
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("FreeListNodeNonZeroKeyLen", func(t *testing.T) {
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Put("k1", testData{value: 1, dataSize: 10})
		_, _ = c.Put("k2", testData{value: 2, dataSize: 10})
		_, _ = c.Delete("k1")
		require.NotEqual(t, nilNode, c.freeHead)
		c.nodes[c.freeHead].setKeyLen(2)
		assert.Panics(t, func() {
			c.checkInvariants()
		})
	})

	t.Run("CollisionPeerBucketInvariantPanics", func(t *testing.T) {
		c := NewArenaRadixCache[testData](50, testDataWeigher).(*arenaRadix[testData])
		_, _ = c.Put("k1", testData{value: 1, dataSize: 10})
		_, _ = c.Put("k2", testData{value: 2, dataSize: 10})
		h1 := c.hashKey("k1")
		h2 := c.hashKey("k2")
		id1 := c.nodeMapAt(h1)
		id2 := c.nodeMapAt(h2)

		// 1. Empty slice in collisionPeers[h1]
		c.collisionPeers = map[uint64][]uint32{h1: {}}
		assert.Panics(t, func() { c.checkNodeMapInvariants() })

		// 2. collisionPeers bucket without corresponding nodeMap entry
		c.collisionPeers = map[uint64][]uint32{99999: {id1}}
		assert.Panics(t, func() { c.checkNodeMapInvariants() })

		// 3. Peer hash mismatch (hashNodeKey(id2) != h1)
		c.collisionPeers = map[uint64][]uint32{h1: {id2}}
		assert.Panics(t, func() { c.checkNodeMapInvariants() })

		// 4. Peer duplicates mappedID in nodeMap[h1]
		c.collisionPeers = map[uint64][]uint32{h1: {id1}}
		assert.Panics(t, func() { c.checkNodeMapInvariants() })

		// 5. Duplicate peer IDs in collisionPeers[h1]
		c.nodeMapDelete(h2)
		c.setNodePrefix(id2, "1")
		c.collisionPeers = map[uint64][]uint32{h1: {id2, id2}}
		c.collisionCount = 2
		assert.Panics(t, func() { c.checkNodeMapInvariants() })

		// 6. collisionCount != totalPeers
		c.collisionPeers = map[uint64][]uint32{h1: {id2}}
		c.collisionCount = 5
		assert.Panics(t, func() { c.checkNodeMapInvariants() })

		// 7. len(nodeMap) + collisionCount > c.len
		c.collisionCount = 1
		c.len = 1
		assert.Panics(t, func() { c.checkNodeMapInvariants() })
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

// TestArenaRadixCache_HashCollisionAndNodeMapHealing verifies that ArenaRadixCache preserves key
// reachability, LRU invariants, and compaction guarantees when 64-bit maphash collisions displace
// entries in the O(1) c.hashSlots accelerator.
//
// White-box testing rationale:
//  1. ArenaRadixCache hashes keys internally using seeded 64-bit hash/maphash (c.hashKey). By the birthday bound,
//     finding two distinct variable-length strings with the exact same 64-bit maphash in a 2^64 hash space
//     requires ~2^32 (~4.29 billion) evaluations, which is computationally infeasible at unit-test runtime even
//     though collisions can occur in production across billions of cumulative keys.
//  2. Furthermore, c.hashSlots is an internal O(1) hash accelerator backed by an exact O(K) radix-trie walk fallback
//     (findNodeByTrieWalk). If Delete(K1) or a self-evicting/failing Replace(K1, ...) erroneously overwrites
//     the hash slot for h with idK1 before eraseInternalWithHash deletes h, public Get / Peek
//     calls still return functionally identical values via the slow-path trie walk while silently evicting the
//     surviving colliding peer K2 from c.hashSlots and breaking the c.hashCount+c.collisionCount == c.len Pigeonhole Principle O(1)
//     cache-miss fast-path across the entire cache.
//     These subtests therefore seed a synthetic collision/displacement in c.hashSlots (via the c.nodeMap* helpers) in setup
//     and inspect c.hashSlots in assertions alongside public API Get/Peek/Put/Replace/Delete/Compact calls.
func TestArenaRadixCache_HashCollisionAndNodeMapHealing(t *testing.T) {
	const keyA = "prefix/key-alpha"
	const keyB = "prefix/key-beta"

	t.Run("HashCollisionDoesNotThrashCompactionAndPreservesInvariant", func(t *testing.T) {
		// Arrange: Put keyA, keyB, and a temporary key, simulate a 64-bit hash collision
		// displacing keyA from nodeMap, and trigger a real deletion via Delete.
		c := NewArenaRadixCache[testData](10000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put(keyA, testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put(keyB, testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("prefix/key-temp", testData{value: 3, dataSize: 10})
		require.NoError(t, err)

		c.nodeMapDelete(c.hashKey(keyA))
		_, ok := c.Delete("prefix/key-temp")
		require.True(t, ok)

		// Act & Assert: Both keys remain accessible (keyA via trie fallback), and Compact() heals keyA
		// back into nodeMap without thrashing on subsequent Compact() calls.
		valA, ok := c.Peek(keyA)
		require.True(t, ok)
		assert.Equal(t, int64(1), valA.value)

		c.Compact()
		_, ok = c.nodeMapGet(c.hashKey(keyA))
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
		idK2 := c.nodeMapAt(c.hashKey("k2"))
		c.nodeMapSet(c.hashKey("k1"), idK2)

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
		assert.NotEqual(t, idK2, c.nodeMapAt(c.hashKey("k1")))
	})

	t.Run("UpdateMethodsHealNodeMapOnCollisionOrMissingSlot", func(t *testing.T) {
		// Arrange
		c := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put("key-alpha", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("key-beta", testData{value: 2, dataSize: 10})
		require.NoError(t, err)

		hAlpha := c.hashKey("key-alpha")
		hBeta := c.hashKey("key-beta")
		idAlpha := c.nodeMapAt(hAlpha)
		idBeta := c.nodeMapAt(hBeta)

		// Act & Assert 1: Missing slot healed by Get.
		c.nodeMapDelete(hAlpha)
		_, ok := c.Get("key-alpha")
		require.True(t, ok)
		assert.Equal(t, idAlpha, c.nodeMapAt(hAlpha))

		// Act & Assert 2: Missing slot healed by Replace (same size).
		c.nodeMapDelete(hAlpha)
		err = c.Replace("key-alpha", testData{value: 11, dataSize: 10})
		require.NoError(t, err)
		assert.Equal(t, idAlpha, c.nodeMapAt(hAlpha))

		// Act & Assert 3: Colliding slot pointing to idBeta healed by Replace("key-alpha", ...) with size growth.
		c.nodeMapSet(hAlpha, idBeta)
		err = c.Replace("key-alpha", testData{value: 12, dataSize: 15})
		require.NoError(t, err)
		assert.Equal(t, idAlpha, c.nodeMapAt(hAlpha))
	})

	t.Run("DeleteAndReplaceSelfEvictionOrExceedMaxSizePreserveCollidingPeerInNodeMap", func(t *testing.T) {
		// Case 1: Delete(keyA) when collision bucket hA points to surviving peer keyB.
		cDelete := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err := cDelete.Put(keyA, testData{value: 1, dataSize: 100})
		require.NoError(t, err)
		_, err = cDelete.Put(keyB, testData{value: 2, dataSize: 100})
		require.NoError(t, err)
		hADelete := cDelete.hashKey(keyA)
		hBDelete := cDelete.hashKey(keyB)
		idB := cDelete.nodeMapAt(hBDelete)
		cDelete.nodeMapSet(hADelete, idB)

		erased, ok := cDelete.Delete(keyA)
		require.True(t, ok)
		assert.Equal(t, int64(1), erased.value)
		assert.Equal(t, idB, cDelete.nodeMapAt(hADelete), "Delete(keyA) must not overwrite or delete surviving peer keyB from shared hash bucket")
		cDelete.nodeMapDelete(hADelete)
		assert.Equal(t, cDelete.len, cDelete.nodeMapLen(), "len(nodeMap) == c.len bijection must hold for O(1) cache-miss fast-path")
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
		hAMaxEvict := cMaxEvict.hashKey(keyA)
		hBMaxEvict := cMaxEvict.hashKey(keyB)
		idB = cMaxEvict.nodeMapAt(hBMaxEvict)
		cMaxEvict.nodeMapSet(hAMaxEvict, idB)

		err = cMaxEvict.Replace(keyA, testData{value: 99, dataSize: 1001})
		require.NoError(t, err)
		assert.Equal(t, idB, cMaxEvict.nodeMapAt(hAMaxEvict), "Replace self-eviction (newSize > maxSize) must preserve surviving peer in nodeMap")
		cMaxEvict.nodeMapDelete(hAMaxEvict)
		assert.Equal(t, cMaxEvict.len, cMaxEvict.nodeMapLen())
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
		hACannotFit := cCannotFit.hashKey(keyA)
		hBCannotFit := cCannotFit.hashKey(keyB)
		idB = cCannotFit.nodeMapAt(hBCannotFit)
		cCannotFit.nodeMapSet(hACannotFit, idB)

		err = cCannotFit.Replace(keyA, testData{value: 11, dataSize: 300})
		require.NoError(t, err)
		assert.Equal(t, idB, cCannotFit.nodeMapAt(hACannotFit), "Replace self-eviction (!canFit) must preserve surviving peer in nodeMap")
		cCannotFit.nodeMapDelete(hACannotFit)
		assert.Equal(t, cCannotFit.len, cCannotFit.nodeMapLen())
		cCannotFit.checkInvariants()
		_, ok = cCannotFit.Peek(keyA)
		assert.False(t, ok)
		_, ok = cCannotFit.Peek("missing-key")
		assert.False(t, ok)
		_, ok = cCannotFit.Peek(keyB)
		require.True(t, ok)
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
				c.nodeMapDelete(c.hashKey(k))
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
		ac.nodeMapDelete(ac.hashKey(""))

		// Act: Delete "" (which resides at c.root == 0) while hash("") is not in c.hashSlots.
		erased, ok := ac.Delete("")

		// Assert: nodeMap retains the remaining entry intact.
		require.True(t, ok)
		assert.Equal(t, "root_val", erased)
		assert.Equal(t, 1, ac.nodeMapLen())
	})

	t.Run("ActiveCollisionPeerPromotionPopAndRemoveHelpers", func(t *testing.T) {
		c := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put("k1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("k2", testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("k3", testData{value: 3, dataSize: 10})
		require.NoError(t, err)

		h1 := c.hashKey("k1")
		h2 := c.hashKey("k2")
		h3 := c.hashKey("k3")
		id1 := c.nodeMapAt(h1)
		id2 := c.nodeMapAt(h2)
		id3 := c.nodeMapAt(h3)
		c.nodeMapDelete(h2)
		c.nodeMapDelete(h3)
		c.setNodePrefix(id2, "1")
		c.setNodePrefix(id3, "1")

		// Promote id2 and id3 into h1's bucket so id1 and id2 become active collisionPeers[h1].
		c.promoteCollisionPeerLocked(h1, id2)
		c.promoteCollisionPeerLocked(h1, id2) // no-op when prevID == nodeID
		c.promoteCollisionPeerLocked(h1, id3)
		assert.Equal(t, id3, c.nodeMapAt(h1))
		assert.Equal(t, 2, c.collisionCount)
		assert.Equal(t, []uint32{id1, id2}, c.collisionPeers[h1])
		c.checkNodeMapInvariants()

		// Inserting a new key while c.collisionPeers != nil exercises the recordNodeMapInsertLocked path in insertNewOnPutLocked.
		_, err = c.Put("k4", testData{value: 4, dataSize: 10})
		require.NoError(t, err)
		c.checkNodeMapInvariants()

		// Promoting an existing peer (id1 at idx 0) while prevValid swaps prevID (id3) into peers[0].
		c.promoteCollisionPeerLocked(h1, id1)
		assert.Equal(t, id1, c.nodeMapAt(h1))
		assert.Equal(t, []uint32{id3, id2}, c.collisionPeers[h1])

		// Removing non-primary peer id2 shrinks peers to [id3] without emptying the bucket.
		c.removeOrPromoteNodeMapLocked(h1, id2)
		assert.Equal(t, []uint32{id3}, c.collisionPeers[h1])
		assert.Equal(t, 1, c.collisionCount)

		// Re-promote id2 so peers has 2 elements [id3, id1], then pop primary id2 (leaving [id3]).
		c.promoteCollisionPeerLocked(h1, id2)
		assert.Equal(t, 2, c.collisionCount)
		c.removeOrPromoteNodeMapLocked(h1, id2)
		assert.Equal(t, id1, c.nodeMapAt(h1))
		assert.Equal(t, []uint32{id3}, c.collisionPeers[h1])
		assert.Equal(t, 1, c.collisionCount)

		// Popping primary id1 promotes the last peer id3 and clears collisionPeers to nil.
		c.removeOrPromoteNodeMapLocked(h1, id1)
		assert.Equal(t, id3, c.nodeMapAt(h1))
		assert.Zero(t, c.collisionCount)
		assert.Nil(t, c.collisionPeers)

		// Promoting peer id1 when current nodeMap[h1] holds an invalid node ID removes id1 from collisionPeers.
		c.nodeMapSet(h1, 999)
		c.collisionPeers = map[uint64][]uint32{h1: {id1}}
		c.collisionCount = 1
		c.promoteCollisionPeerLocked(h1, id1)
		assert.Equal(t, id1, c.nodeMapAt(h1))
		assert.Zero(t, c.collisionCount)
		assert.Nil(t, c.collisionPeers)

		// Exhausting an invalid/excluded peer bucket during removeOrPromoteNodeMapLocked deletes nodeMap[h1].
		c.collisionPeers = map[uint64][]uint32{h1: {id1}}
		c.collisionCount = 1
		c.removeOrPromoteNodeMapLocked(h1, id1)
		_, exists := c.nodeMapGet(h1)
		assert.False(t, exists)
		assert.Zero(t, c.collisionCount)
		assert.Nil(t, c.collisionPeers)

		// Calling removeOrPromoteNodeMapLocked when hash is absent from nodeMap is a safe no-op.
		c.removeOrPromoteNodeMapLocked(h1, id1)
	})

	t.Run("CompactionRebuildsCollisionPeersInContiguousAndFragmentedModes", func(t *testing.T) {
		c := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err := c.Put("k1", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("k2", testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("k3", testData{value: 3, dataSize: 10})
		require.NoError(t, err)

		h1 := c.hashKey("k1")
		h2 := c.hashKey("k2")
		id1 := c.nodeMapAt(h1)
		id2 := c.nodeMapAt(h2)
		c.nodeMapDelete(h2)
		c.setNodePrefix(id2, "1")
		c.nodeMapDirty = true

		// 1. Contiguous compaction (c.freeHead == nilNode) rebuilds collisionPeers and collisionCount.
		c.Compact()
		assert.Equal(t, 1, c.collisionCount)
		assert.Equal(t, []uint32{id1}, c.collisionPeers[h1])
		assert.Equal(t, id2, c.nodeMapAt(h1))
		c.checkNodeMapInvariants()

		// 2. Fragmented compaction (c.freeHead != nilNode) remaps node IDs and rebuilds collisionPeers.
		_, ok := c.Delete("k3")
		require.True(t, ok)
		require.NotEqual(t, nilNode, c.freeHead)
		c.Compact()
		assert.Equal(t, nilNode, c.freeHead)
		assert.Equal(t, 1, c.collisionCount)
		require.Len(t, c.collisionPeers[h1], 1)
		c.checkNodeMapInvariants()
	})

	t.Run("ReplaceTier2SheddingRestoresCollidingPeerInNodeMap", func(t *testing.T) {
		pressure := 0.10
		c := NewArenaRadixCache[testData](
			1000,
			testDataWeigher,
			WithPressureFunc(func() float64 { return pressure }),
			WithCompactionThreshold(0.75),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
		).(*arenaRadix[testData])

		_, err := c.Put("tail-coll", testData{value: 1, dataSize: 100})
		require.NoError(t, err)
		for i := range 3 {
			_, err = c.Put(fmt.Sprintf("mid-%d", i), testData{value: int64(10 + i), dataSize: 115})
			require.NoError(t, err)
		}
		_, err = c.Put("head-coll", testData{value: 2, dataSize: 115})
		require.NoError(t, err)

		hTail := c.hashKey("tail-coll")
		hColl := c.hashKey("head-coll")
		idTail := c.nodeMapAt(hTail)
		idHead := c.nodeMapAt(hColl)
		c.nodeMapDelete(hTail)
		c.setNodePrefix(idTail, "head-coll")
		c.promoteCollisionPeerLocked(hColl, idTail)
		require.Equal(t, idTail, c.nodeMapAt(hColl))
		require.Equal(t, []uint32{idHead}, c.collisionPeers[hColl])
		require.Equal(t, 1, c.collisionCount)
		c.checkNodeMapInvariants()

		// Trigger Tier 2 pressure shedding during Replace("mid-0"): sheds LRU tail idTail and promotes idHead.
		pressure = 0.95
		err = c.Replace("mid-0", testData{value: 110, dataSize: 115})
		require.NoError(t, err)

		assert.Equal(t, idHead, c.nodeMapAt(hColl))
		assert.Zero(t, c.collisionCount)
		assert.Nil(t, c.collisionPeers)
		c.checkInvariants()
		vHead, ok := c.Peek("head-coll")
		require.True(t, ok)
		assert.Equal(t, int64(2), vHead.value)
		_, ok = c.Peek("missing-key")
		assert.False(t, ok)
	})
}

func TestArenaRadixCache_DeepHierarchyOver64LevelsAndRoutingPrefixCloning(t *testing.T) {
	t.Run("DeepRadixTreeOver64LevelsAndFreeSubtree", func(t *testing.T) {
		// Arrange: Build a radix tree with > 80 branching levels so freeSubtree exceeds 64 levels.
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

		// Act 1: Compact the 80-level tree and confirm a second Compact is a no-op.
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
		// Arrange: Put two 64 KiB keys ("dir/A" + 64 KiB suffix and "dir/B" + 64 KiB suffix)
		// so splitting at "dir/" copies both split halves into c.prefixArena, then insert short sibling keys
		// ("dir/a" and "dir/b") and erase both 64 KiB keys so the "dir/" routing node survives.
		//
		// White-box testing rationale: String backing-array aliasing on internal routing and leaf nodes is
		// invisible to public Get results, so verifying that the 4-byte "dir/" routing prefix and leaf suffixes
		// do not pin caller key buffers requires inspecting unsafe.StringData on the internal arena nodes.
		c := NewArenaRadixCache[string](1<<20, WithInvariantChecking(true)).(*arenaRadix[string])
		largeKeyA := strings.Clone("dir/A" + strings.Repeat("X", 1<<16))
		largeKeyB := strings.Clone("dir/B" + strings.Repeat("Y", 1<<16))
		largeStartA := uintptr(unsafe.Pointer(unsafe.StringData(largeKeyA)))
		largeEndA := largeStartA + uintptr(len(largeKeyA))
		largeStartB := uintptr(unsafe.Pointer(unsafe.StringData(largeKeyB)))
		largeEndB := largeStartB + uintptr(len(largeKeyB))

		_, err := c.Put(largeKeyA, "va_large")
		require.NoError(t, err)
		_, err = c.Put(largeKeyB, "vb_large")
		require.NoError(t, err)

		// Also verify leaf node prefix is detached from the caller's backing array while live.
		leafIDB := c.nodeMapAt(c.hashKey(largeKeyB))
		leafPrefixPtrB := uintptr(unsafe.Pointer(unsafe.StringData(c.nodePrefix(leafIDB))))
		assert.True(t, leafPrefixPtrB < largeStartB || leafPrefixPtrB >= largeEndB, "leaf node prefix must not pin largeKeyB backing array")

		// Act: Insert short sibling keys and erase both 64 KiB keys.
		_, err = c.Put("dir/a", "va")
		require.NoError(t, err)
		_, err = c.Put("dir/b", "vb")
		require.NoError(t, err)
		_, _ = c.Delete(largeKeyA)
		_, _ = c.Delete(largeKeyB)

		// Assert: Routing node prefix "dir/" does not alias either 64 KiB key's backing array.
		routingID := c.nodes[c.root].child
		require.NotEqual(t, nilNode, routingID)
		assert.Equal(t, "dir/", c.nodePrefix(routingID))
		prefixPtr := uintptr(unsafe.Pointer(unsafe.StringData(c.nodePrefix(routingID))))
		assert.True(t, prefixPtr < largeStartA || prefixPtr >= largeEndA, "routing node prefix must not pin largeKeyA backing array")
		assert.True(t, prefixPtr < largeStartB || prefixPtr >= largeEndB, "routing node prefix must not pin largeKeyB backing array")
		runtime.KeepAlive(largeKeyA)
		runtime.KeepAlive(largeKeyB)
	})
}

func TestArenaRadixCache_LargeKeysAndSharedPrefixesOver64KiB(t *testing.T) {
	t.Run("Exact64KiBSharedPrefixAtRoot", func(t *testing.T) {
		c := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(PressureAwareCache[testData])
		prefix64K := strings.Repeat("p", 1<<16) // 65,536 bytes (previously wrapped uint16 to 0)
		k1 := prefix64K + "alpha"
		k2 := prefix64K + "beta"

		_, err := c.Put(k1, testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put(k2, testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put(prefix64K, testData{value: 3, dataSize: 10})
		require.NoError(t, err)

		v1, ok := c.Get(k1)
		require.True(t, ok)
		assert.Equal(t, int64(1), v1.value)
		v2, ok := c.Peek(k2)
		require.True(t, ok)
		assert.Equal(t, int64(2), v2.value)
		vExact, ok := c.Get(prefix64K)
		require.True(t, ok)
		assert.Equal(t, int64(3), vExact.value)
		_, ok = c.Peek(prefix64K + "gamma")
		assert.False(t, ok)

		_, ok = c.Delete(prefix64K)
		require.True(t, ok)
		c.Compact()

		// Deleting k1 merges the 65,536-byte routing node with k2 via compressPathUpwards.
		_, ok = c.Delete(k1)
		require.True(t, ok)
		v2After, ok := c.Get(k2)
		require.True(t, ok)
		assert.Equal(t, int64(2), v2After.value)

		c.DeletePrefix(prefix64K)
		_, ok = c.Peek(k2)
		assert.False(t, ok)
	})

	t.Run("GreaterThan64KiBSharedPrefixAndMultiLevelSplitsPast64KiB", func(t *testing.T) {
		c := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		base70K := strings.Repeat("a", 70000)
		mid135K := base70K + "/" + strings.Repeat("b", 1<<16) // second edge is 65,537 bytes
		kA := base70K + "/leafA"
		kB1 := mid135K + "/sub1/x"
		kB2 := mid135K + "/sub1/y"
		kB3 := mid135K + "/sub2/z"

		for i, k := range []string{kA, kB1, kB2, kB3} {
			_, err := c.Put(k, testData{value: int64(i + 10), dataSize: 10})
			require.NoError(t, err)
		}

		// Force trie-walk fallback across >64 KiB edges for kB2.
		c.nodeMapDelete(c.hashKey(kB2))
		vB2, ok := c.Get(kB2)
		require.True(t, ok)
		assert.Equal(t, int64(12), vB2.value)

		c.Compact()
		_, ok = c.Delete(kB1)
		require.True(t, ok)
		_, ok = c.Peek(kB2)
		require.True(t, ok)

		c.DeletePrefix(mid135K + "/sub")
		_, ok = c.Peek(kB2)
		assert.False(t, ok)
		_, ok = c.Peek(kB3)
		assert.False(t, ok)
		vA, ok := c.Get(kA)
		require.True(t, ok)
		assert.Equal(t, int64(10), vA.value)
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
//     Because unsafe.Sizeof(arenaRadixNode[testData]{}) == 56 bytes on 64-bit platforms, backing the c.nodes slice alone
//     requires 4,294,967,293 * 56 == 240,518,168,408 bytes (~240.52 GiB) of contiguous heap memory (plus >32 GiB
//     for c.hashSlots), which is infeasible to allocate in a unit test on standard hardware.
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
		assert.Empty(t, ac.reconstructKey(nilNode))
		assert.Empty(t, ac.reconstructKey(ac.root))
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
		// White-box testing rationale: 64-bit maphash collisions are astronomically rare for natural
		// keys, so verifying that displaced or colliding c.hashSlots entries fall back to radix tree traversal
		// and increment Stats().ArenaHashFallbacks across Peek, Get, Replace, Put, and Delete requires
		// directly mutating c.hashSlots (via the c.nodeMap* helpers) in a package-internal test.

		// Part A: Displaced slot (delete from nodeMap) with invariant checking enabled.
		cDisplaced := NewArenaRadixCache[testData](1000, WithInvariantChecking(true), testDataWeigher).(*arenaRadix[testData])
		_, err := cDisplaced.Put("dir/alpha", testData{value: 10, dataSize: 10})
		require.NoError(t, err)
		_, err = cDisplaced.Put("dir/beta", testData{value: 20, dataSize: 10})
		require.NoError(t, err)

		hAlpha := cDisplaced.hashKey("dir/alpha")

		// Displace "dir/alpha" from nodeMap and verify Peek falls back to radix tree and increments ArenaHashFallbacks.
		cDisplaced.nodeMapDelete(hAlpha)
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
		cDisplaced.nodeMapDelete(hAlpha)
		err = cDisplaced.Replace("dir/alpha", testData{value: 11, dataSize: 10})
		require.NoError(t, err)
		assert.Equal(t, uint64(3), cDisplaced.Stats().ArenaHashFallbacks)

		// Re-displace after Replace healed nodeMap[hAlpha], then verify Delete falls back to radix tree and increments ArenaHashFallbacks.
		cDisplaced.nodeMapDelete(hAlpha)
		v, ok = cDisplaced.Delete("dir/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(11), v.value)
		assert.Equal(t, uint64(4), cDisplaced.Stats().ArenaHashFallbacks)

		// Part B: Colliding slot (nodeMap[hAlphaCollide] points to "dir/beta"'s node ID) across Peek, Get, Replace, Put, and Delete.
		cCollide := NewArenaRadixCache[testData](1000, testDataWeigher).(*arenaRadix[testData])
		_, err = cCollide.Put("dir/alpha", testData{value: 100, dataSize: 10})
		require.NoError(t, err)
		_, err = cCollide.Put("dir/beta", testData{value: 200, dataSize: 10})
		require.NoError(t, err)

		hAlphaCollide := cCollide.hashKey("dir/alpha")
		idBeta := cCollide.nodeMapAt(cCollide.hashKey("dir/beta"))
		require.NotZero(t, idBeta)

		// Point hAlphaCollide at idBeta so verifyKey(idBeta, "dir/alpha") returns false.
		cCollide.nodeMapSet(hAlphaCollide, idBeta)

		v, ok = cCollide.Peek("dir/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(100), v.value)
		assert.Equal(t, uint64(1), cCollide.Stats().ArenaHashFallbacks)

		v, ok = cCollide.Get("dir/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(100), v.value)
		assert.Equal(t, uint64(2), cCollide.Stats().ArenaHashFallbacks)

		cCollide.nodeMapSet(hAlphaCollide, idBeta)
		err = cCollide.Replace("dir/alpha", testData{value: 101, dataSize: 10})
		require.NoError(t, err)
		assert.Equal(t, uint64(3), cCollide.Stats().ArenaHashFallbacks)

		// In-place Put on existing "dir/alpha" when nodeMap[hAlphaCollide] collides with idBeta:
		// traverses the radix tree to update "dir/alpha" in place, heals nodeMap[hAlphaCollide], and increments ArenaHashFallbacks.
		cCollide.nodeMapSet(hAlphaCollide, idBeta)
		evicted, err := cCollide.Put("dir/alpha", testData{value: 102, dataSize: 10})
		require.NoError(t, err)
		assert.Empty(t, evicted)
		assert.Equal(t, uint64(1), cCollide.Stats().PutUpdated)
		assert.Equal(t, uint64(4), cCollide.Stats().ArenaHashFallbacks)

		// Re-inject collision before Delete to exercise eraseInternalWithHash fallback.
		cCollide.nodeMapSet(hAlphaCollide, idBeta)
		v, ok = cCollide.Delete("dir/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(102), v.value)
		assert.Equal(t, uint64(5), cCollide.Stats().ArenaHashFallbacks)
		cCollide.nodeMapDelete(hAlphaCollide)

		// Equal-length multi-level leaf suffix mismatch ("dir/sub/alpha" vs "dir/sub/bravo", both keyLen == 13).
		_, err = cCollide.Put("dir/sub/alpha", testData{value: 300, dataSize: 10})
		require.NoError(t, err)
		_, err = cCollide.Put("dir/sub/bravo", testData{value: 400, dataSize: 10})
		require.NoError(t, err)
		hSubAlpha := cCollide.hashKey("dir/sub/alpha")
		idSubBravo := cCollide.nodeMapAt(cCollide.hashKey("dir/sub/bravo"))
		require.NotZero(t, idSubBravo)
		assert.False(t, cCollide.verifyKey(idSubBravo, "dir/sub/alpha"))

		cCollide.nodeMapSet(hSubAlpha, idSubBravo)
		v, ok = cCollide.Peek("dir/sub/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(300), v.value)
		assert.Equal(t, uint64(6), cCollide.Stats().ArenaHashFallbacks)

		v, ok = cCollide.Get("dir/sub/alpha")
		require.True(t, ok)
		assert.Equal(t, int64(300), v.value)
		assert.Equal(t, uint64(7), cCollide.Stats().ArenaHashFallbacks)

		// Equal-length multi-level ancestor prefix mismatch with identical leaf suffixes ("dirA/item1" vs "dirB/item1", both keyLen == 10, leaf suffix "1").
		_, err = cCollide.Put("dirA/item1", testData{value: 500, dataSize: 10})
		require.NoError(t, err)
		_, err = cCollide.Put("dirA/item2", testData{value: 501, dataSize: 10})
		require.NoError(t, err)
		_, err = cCollide.Put("dirB/item1", testData{value: 600, dataSize: 10})
		require.NoError(t, err)
		_, err = cCollide.Put("dirB/item2", testData{value: 601, dataSize: 10})
		require.NoError(t, err)

		hDirA1 := cCollide.hashKey("dirA/item1")
		idDirA1 := cCollide.nodeMapAt(hDirA1)
		idDirB1 := cCollide.nodeMapAt(cCollide.hashKey("dirB/item1"))
		require.NotZero(t, idDirA1)
		require.NotZero(t, idDirB1)
		assert.False(t, cCollide.verifyKey(idDirB1, "dirA/item1"))

		cCollide.nodeMapSet(hDirA1, idDirB1)
		v, ok = cCollide.Peek("dirA/item1")
		require.True(t, ok)
		assert.Equal(t, int64(500), v.value)
		assert.Equal(t, uint64(8), cCollide.Stats().ArenaHashFallbacks)

		v, ok = cCollide.Get("dirA/item1")
		require.True(t, ok)
		assert.Equal(t, int64(500), v.value)
		assert.Equal(t, uint64(9), cCollide.Stats().ArenaHashFallbacks)

		// Corrupt keyLen guards in verifyKey: (a) end < len(prefix) at an ancestor, and (b) end > 0 at c.root.
		savedDirA1KeyLen := cCollide.nodes[idDirA1].keyLen()
		cCollide.nodes[idDirA1].setKeyLen(2)
		assert.False(t, cCollide.verifyKey(idDirA1, "x1"))
		cCollide.nodes[idDirA1].setKeyLen(12)
		assert.False(t, cCollide.verifyKey(idDirA1, "xxdirA/item1"))
		cCollide.nodes[idDirA1].setKeyLen(savedDirA1KeyLen)
	})
}

func TestArenaRadixCache_MuCacheLineSeparation(t *testing.T) {
	// Arrange
	const cacheLineSize = uintptr(64)
	c := NewArenaRadixCache[testData](testMaxSize).(*arenaRadix[testData])

	// Act
	nodesOffset := unsafe.Offsetof(c.nodes)
	nodesEnd := nodesOffset + unsafe.Sizeof(c.nodes)
	hashSlotsOffset := unsafe.Offsetof(c.hashSlots)
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
	assert.Greater(t, muOffset, hashSlotsOffset, "mu must be positioned after hashSlots")
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

func TestArenaRadix_CacheLineLayoutAndStructAlignment(t *testing.T) {
	// Arrange
	const cacheLineSize = uintptr(64)

	mc := NewMapCache[uint64](100).(*mapCache[uint64])
	rc := NewRadixCache[uint64](100).(*radixCache[uint64])
	ac := NewArenaRadixCache[uint64](100).(*arenaRadix[uint64])
	require.NotNil(t, mc)
	require.NotNil(t, rc)
	require.NotNil(t, ac)

	// Act & Assert 1: Verify ArenaRadixCache separates c.nodes from c.mu across 64-byte cache lines.
	acNodesEnd := unsafe.Offsetof(ac.nodes) + unsafe.Sizeof(ac.nodes)
	acMuOffset := unsafe.Offsetof(ac.mu)
	assert.GreaterOrEqual(t, acMuOffset-acNodesEnd, cacheLineSize,
		"arenaRadix.mu must be separated from arenaRadix.nodes by at least 64 bytes")

	// Act & Assert 2: Characterize pressureState atomic counter packing:
	// peekHits, peekMisses, and putRejectedOversized are packed contiguously (8 bytes apart)
	// in the same 64-byte cache line, and arenaHashFallbacks shares a 64-byte cache line with pressureWriteMu.
	peekHitsOff := unsafe.Offsetof(mc.peekHits)
	peekMissesOff := unsafe.Offsetof(mc.peekMisses)
	rejectedOff := unsafe.Offsetof(mc.putRejectedOversized)
	fallbacksOff := unsafe.Offsetof(mc.arenaHashFallbacks)
	pressureMuOff := unsafe.Offsetof(mc.pressureWriteMu)
	assert.Equal(t, uintptr(8), peekMissesOff-peekHitsOff,
		"peekHits and peekMisses are adjacent 8-byte fields sharing a cache line")
	assert.Equal(t, peekHitsOff/cacheLineSize, peekMissesOff/cacheLineSize,
		"peekHits and peekMisses sit in the same 64-byte cache line")
	assert.Equal(t, peekHitsOff/cacheLineSize, rejectedOff/cacheLineSize,
		"peekHits and putRejectedOversized sit in the same 64-byte cache line")
	assert.Equal(t, fallbacksOff/cacheLineSize, pressureMuOff/cacheLineSize,
		"arenaHashFallbacks and pressureWriteMu sit in the same 64-byte cache line")

	t.Logf("Layout offsets (bytes): mapCache{index=%d, mu=%d, peekHits=%d, peekMisses=%d, pressureWriteMu=%d} | radixCache{root=%d, mu=%d} | arenaRadix{nodes=%d, mu=%d}",
		unsafe.Offsetof(mc.index), unsafe.Offsetof(mc.mu), peekHitsOff, peekMissesOff, pressureMuOff,
		unsafe.Offsetof(rc.root), unsafe.Offsetof(rc.mu),
		unsafe.Offsetof(ac.nodes), acMuOffset)

	// Act & Assert 3: arenaRadixNode[uint64] embeds `prefixOff uint32` at offset 0 and `keyLenAndFlags uint32` at offset 36 (48 bytes total).
	var node arenaRadixNode[uint64]
	assert.Equal(t, uintptr(0), unsafe.Offsetof(node.prefixOff))
	assert.Equal(t, uintptr(4), unsafe.Sizeof(node.prefixOff))
	assert.Equal(t, uintptr(36), unsafe.Offsetof(node.keyLenAndFlags))
	assert.Equal(t, uintptr(4), unsafe.Sizeof(node.keyLenAndFlags))
	assert.Equal(t, uintptr(48), unsafe.Sizeof(node))
	nodeType := reflect.TypeFor[arenaRadixNode[uint64]]()
	assert.Equal(t, reflect.Uint32, nodeType.Field(0).Type.Kind())
}

func TestArenaRadix_AllocationCharacterization_PathCompressionAndDeepKeys(t *testing.T) {
	t.Run("ZeroAllocSteadyStateHotPaths", func(t *testing.T) {
		for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			// Arrange
			c := New[uint64](100, WithBackend(b))
			_, _ = c.Put("dir/sub/k1", 1)
			_, _ = c.Put("dir/sub/k2", 2)
			allSeq := c.All()
			keySeq := c.Keys()
			valSeq := c.Values()

			// Act & Assert
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				_, _ = c.Get("dir/sub/k1")
			}), "backend %v Get must be 0 allocs/op", b)
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				_, _ = c.Peek("dir/sub/k2")
			}), "backend %v Peek must be 0 allocs/op", b)
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				_, _ = c.Put("dir/sub/k1", 3)
			}), "backend %v in-place Put must be 0 allocs/op", b)
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				_ = c.Replace("dir/sub/k2", 4)
			}), "backend %v Replace must be 0 allocs/op", b)
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				_ = c.Stats()
			}), "backend %v Stats must be 0 allocs/op", b)
			assert.Zero(t, testing.AllocsPerRun(50, func() {
				valSeq(func(v uint64) bool { return v > 0 })
			}), "backend %v Values must be 0 allocs/op", b)
			if b == BackendMap {
				assert.Zero(t, testing.AllocsPerRun(50, func() {
					allSeq(func(k string, v uint64) bool { return k != "" && v > 0 })
				}), "backend %v All must be 0 allocs/op", b)
				assert.Zero(t, testing.AllocsPerRun(50, func() {
					keySeq(func(k string) bool { return k != "" })
				}), "backend %v Keys must be 0 allocs/op", b)
			} else {
				cDepth1 := New[uint64](100, WithBackend(b))
				_, _ = cDepth1.Put("alpha", 1)
				_, _ = cDepth1.Put("bravo", 2)
				allDepth1Seq := cDepth1.All()
				keyDepth1Seq := cDepth1.Keys()
				assert.Zero(t, testing.AllocsPerRun(50, func() {
					allDepth1Seq(func(k string, v uint64) bool { return k != "" && v > 0 })
				}), "backend %v depth-1 All must be 0 allocs/op", b)
				assert.Zero(t, testing.AllocsPerRun(50, func() {
					keyDepth1Seq(func(k string) bool { return k != "" })
				}), "backend %v depth-1 Keys must be 0 allocs/op", b)
			}
		}
	})

	t.Run("RadixSplitAndCompressAndDeepHashNodeKey", func(t *testing.T) {
		// Arrange: In ArenaRadixCache, edge prefixes are stored in prefixArena and split/merged in place
		// with zero heap allocations in steady state.
		arenaC := NewArenaRadixCache[int](100)
		_, _ = arenaC.Put("service/users/profile/1001_alpha", 1)
		k2 := "service/users/profile/1002_beta"
		_, _ = arenaC.Put(k2, 2)
		_, _ = arenaC.Delete(k2)

		// Act
		allocs := testing.AllocsPerRun(100, func() {
			_, _ = arenaC.Put(k2, 2)
			_, _ = arenaC.Delete(k2)
		})

		// Assert
		assert.Zero(t, allocs,
			"ArenaRadixCache split+compress cycle must allocate 0 heap objects per Put+Delete in steady state")

		// Verify zero-allocation hashNodeKey across root/nilNode, stack-buffered deep keys (depth 75),
		// and streaming maphash.Hash fallback for keys > 512 bytes.
		arenaDeep := NewArenaRadixCache[int](200).(*arenaRadix[int])
		assert.Equal(t, arenaDeep.hashKey(""), arenaDeep.hashNodeKey(nilNode))
		assert.Equal(t, arenaDeep.hashKey(""), arenaDeep.hashNodeKey(arenaDeep.root))

		var deepSB strings.Builder
		for i := 1; i <= 75; i++ {
			fmt.Fprintf(&deepSB, "/%02d", i)
			_, _ = arenaDeep.Put(deepSB.String(), i)
		}
		depth2Key := "/01/02"
		depth2ID := arenaDeep.nodeMapAt(arenaDeep.hashKey(depth2Key))
		depth10Key := "/01/02/03/04/05/06/07/08/09/10"
		depth10ID := arenaDeep.nodeMapAt(arenaDeep.hashKey(depth10Key))
		deepLeafKey := deepSB.String()
		deepLeafID := arenaDeep.nodeMapAt(arenaDeep.hashKey(deepLeafKey))

		assert.Equal(t, arenaDeep.hashKey(depth2Key), arenaDeep.hashNodeKey(depth2ID))
		assert.Zero(t, testing.AllocsPerRun(100, func() {
			_ = arenaDeep.hashNodeKey(depth2ID)
		}), "hashNodeKey is 0 allocs for depth == 2 (totalLen <= 64)")

		assert.Equal(t, arenaDeep.hashKey(depth10Key), arenaDeep.hashNodeKey(depth10ID))
		assert.Zero(t, testing.AllocsPerRun(100, func() {
			_ = arenaDeep.hashNodeKey(depth10ID)
		}), "hashNodeKey is 0 allocs for depth == 10 >= 3 (totalLen <= 64)")

		savedDepth2KeyLen := arenaDeep.nodes[depth2ID].keyLen()
		arenaDeep.nodes[depth2ID].setKeyLen(2)
		assert.Equal(t, arenaDeep.hashKey(depth2Key), arenaDeep.hashNodeKey(depth2ID))
		arenaDeep.nodes[depth2ID].setKeyLen(40)
		assert.Equal(t, arenaDeep.hashKey(depth2Key), arenaDeep.hashNodeKey(depth2ID))
		arenaDeep.nodes[depth2ID].setKeyLen(savedDepth2KeyLen)

		savedDepth10KeyLen := arenaDeep.nodes[depth10ID].keyLen()
		arenaDeep.nodes[depth10ID].setKeyLen(2)
		assert.Equal(t, arenaDeep.hashKey(depth10Key), arenaDeep.hashNodeKey(depth10ID))
		arenaDeep.nodes[depth10ID].setKeyLen(40)
		assert.Equal(t, arenaDeep.hashKey(depth10Key), arenaDeep.hashNodeKey(depth10ID))
		arenaDeep.nodes[depth10ID].setKeyLen(savedDepth10KeyLen)

		deepHashAllocs := testing.AllocsPerRun(100, func() {
			_ = arenaDeep.hashNodeKey(deepLeafID)
		})
		assert.Zero(t, deepHashAllocs, "hashNodeKey is 0 allocs for keys <= 512 bytes even at depth > 64")

		// Also verify hashNodeKey and reconstructKey fallbacks when keyLen mismatches prefix chain (e.g. white-box mutation).
		savedKeyLen := arenaDeep.nodes[deepLeafID].keyLen()
		arenaDeep.nodes[deepLeafID].setKeyLen(2)
		assert.Equal(t, arenaDeep.hashKey(deepLeafKey), arenaDeep.hashNodeKey(deepLeafID))
		assert.Equal(t, deepLeafKey, arenaDeep.reconstructKey(deepLeafID))
		arenaDeep.nodes[deepLeafID].setKeyLen(100)
		assert.Equal(t, arenaDeep.hashKey(deepLeafKey), arenaDeep.hashNodeKey(deepLeafID))
		arenaDeep.nodes[deepLeafID].setKeyLen(500)
		assert.Equal(t, deepLeafKey, arenaDeep.reconstructKey(deepLeafID))
		arenaDeep.nodes[deepLeafID].setKeyLen(savedKeyLen)

		// Verify DeletePrefix / freeSubtree / initSubtreeKeyBuf fallback paths for keys > 512 bytes
		// (both when subtree root keyLen <= 512 with > 512B descendants and when subtree root keyLen > 512),
		// as well as the single-leaf fast path in freeSubtree (when matched prefix node has child == nilNode).
		var evictedLargeKeys []string
		arenaLargePrefix := NewArenaRadixCache[int](100, WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			evictedLargeKeys = append(evictedLargeKeys, k)
		})).(*arenaRadix[int])
		longBase := "large_prefix/" + strings.Repeat("x", 500)
		kLargeA := longBase + "/child_a_" + strings.Repeat("a", 50)
		kLargeB := longBase + "/child_b_" + strings.Repeat("b", 50)
		_, _ = arenaLargePrefix.Put("large_prefix/short", 1)
		_, _ = arenaLargePrefix.Put(kLargeA, 2)
		_, _ = arenaLargePrefix.Put(kLargeB, 3)
		arenaLargePrefix.DeletePrefix("large_prefix/short")
		assert.Equal(t, []string{"large_prefix/short"}, evictedLargeKeys)
		evictedLargeKeys = nil
		_, _ = arenaLargePrefix.Put("large_prefix/short", 1)
		arenaLargePrefix.DeletePrefix("large_prefix/")
		assert.Zero(t, arenaLargePrefix.len)
		assert.ElementsMatch(t, []string{"large_prefix/short", kLargeA, kLargeB}, evictedLargeKeys)

		// Also verify initSubtreeKeyBuf fallback when subtree root itself exceeds 512 bytes or has mismatched keyLen,
		// and single-leaf DeletePrefix without an onEvictEntry callback.
		arenaLargeRoot := NewArenaRadixCache[int](100).(*arenaRadix[int])
		_, _ = arenaLargeRoot.Put("large_prefix/short", 1)
		arenaLargeRoot.DeletePrefix("large_prefix/short")
		assert.Zero(t, arenaLargeRoot.len)
		_, _ = arenaLargeRoot.Put(kLargeA, 2)
		_, _ = arenaLargeRoot.Put(kLargeB, 3)
		var testBuf [512]byte
		rootChildID := arenaLargeRoot.nodes[arenaLargeRoot.root].child
		_, okBuf := arenaLargeRoot.initSubtreeKeyBuf(rootChildID, &testBuf)
		assert.False(t, okBuf, "initSubtreeKeyBuf must return false when node keyLen > 512")
		arenaLargeRoot.nodes[rootChildID].setKeyLen(2)
		_, okBuf = arenaLargeRoot.initSubtreeKeyBuf(rootChildID, &testBuf)
		assert.False(t, okBuf, "initSubtreeKeyBuf must return false when keyLen is smaller than prefix length")
		arenaLargeRoot.nodes[rootChildID].setKeyLen(500)
		_, okBuf = arenaLargeRoot.initSubtreeKeyBuf(rootChildID, &testBuf)
		assert.False(t, okBuf, "initSubtreeKeyBuf must return false when keyLen does not match ancestor prefixes")
		arenaLargeRoot.nodes[rootChildID].setKeyLen(len(longBase + "/child_"))
		arenaLargeRoot.DeletePrefix(longBase)
		assert.Zero(t, arenaLargeRoot.len)
	})
}

func TestArenaRadix_OnEvictEntry_Depth1KeyImmutabilityAndDepth2SpanReuse(t *testing.T) {
	t.Run("DeletePrefixAllPreservesEvictedDepth1KeyAgainstSubsequentPut", func(t *testing.T) {
		var evictedKeys []string
		c := NewArenaRadixCache[int](100,
			WithInvariantChecking(true),
			WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
				evictedKeys = append(evictedKeys, k)
			}),
		)

		_, err := c.Put("alpha_key", 1)
		require.NoError(t, err)
		c.DeletePrefix("")
		_, err = c.Put("zzzzzzzzz", 2)
		require.NoError(t, err)

		require.Equal(t, []string{"alpha_key"}, evictedKeys)
	})

	t.Run("DeletePrefixNonEmptyToEmptyPreservesEvictedDepth1Key", func(t *testing.T) {
		var evictedKeys []string
		c := NewArenaRadixCache[int](100,
			WithInvariantChecking(true),
			WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
				evictedKeys = append(evictedKeys, k)
			}),
		)

		_, err := c.Put("alpha_key", 1)
		require.NoError(t, err)
		c.DeletePrefix("alpha")
		_, err = c.Put("zzzzzzzzz", 2)
		require.NoError(t, err)

		require.Equal(t, []string{"alpha_key"}, evictedKeys)
	})

	t.Run("PreInsertEvictionToEmptyUnderPressurePreservesEvictedDepth1Key", func(t *testing.T) {
		var evictedKeys []string
		c := NewArenaRadixCache[testData](10,
			WithInvariantChecking(true),
			testDataWeigher,
			WithPressureFunc(func() float64 { return 0.80 }),
			WithCompactionThreshold(0.75),
			WithEvictionThreshold(0.95),
			WithOnEvictEntry(func(k string, _ testData, _ EvictionReason) {
				evictedKeys = append(evictedKeys, k)
			}),
		)

		_, err := c.Put("alpha_key", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("zzzzzzzzz", testData{value: 2, dataSize: 10})
		require.NoError(t, err)

		require.Equal(t, []string{"alpha_key"}, evictedKeys)
	})

	t.Run("Depth2EvictionDoesNotPinPrefixSpan", func(t *testing.T) {
		var evictedKeys []string
		c := NewArenaRadixCache[testData](30,
			WithInvariantChecking(true),
			testDataWeigher,
			WithOnEvictEntry(func(k string, _ testData, _ EvictionReason) {
				evictedKeys = append(evictedKeys, k)
			}),
		).(*arenaRadix[testData])

		_, err := c.Put("dir/sub/alpha", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("dir/sub/bravo", testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("dir/other", testData{value: 3, dataSize: 10})
		require.NoError(t, err)

		arenaLenBeforeEvict := len(c.prefixArena)
		// Inserting "dir/third" (10B) evicts "dir/sub/alpha" (depth >= 2).
		// Because "dir/sub/alpha" is at depth >= 2, its key is reconstructed into a fresh []byte,
		// and its suffix span in prefixArena must be recycled rather than pinned.
		_, err = c.Put("dir/third", testData{value: 4, dataSize: 10})
		require.NoError(t, err)
		require.Equal(t, []string{"dir/sub/alpha"}, evictedKeys)
		assert.LessOrEqual(t, len(c.prefixArena), arenaLenBeforeEvict+len("third"))
	})

	t.Run("LargePrefixNonTailSpanReuseAndInPlaceSplitHeaderTrim", func(t *testing.T) {
		c := NewArenaRadixCache[int](100, WithInvariantChecking(true)).(*arenaRadix[int])
		largeA := "A" + strings.Repeat("a", 65535)
		largeB := "B" + strings.Repeat("b", 65535)
		smallTail := "C_tail"

		_, err := c.Put(largeA, 1)
		require.NoError(t, err)
		_, err = c.Put(smallTail, 2)
		require.NoError(t, err)
		arenaLenWithTwo := len(c.prefixArena)

		// Deleting non-tail largeA records its (len+4)-byte span in freePrefixSpans,
		// which is reused in-place when largeB of equal length is inserted.
		_, ok := c.Delete(largeA)
		require.True(t, ok)
		require.Equal(t, 1, c.freePrefixCount)

		_, err = c.Put(largeB, 3)
		require.NoError(t, err)
		assert.Zero(t, c.freePrefixCount)
		assert.Len(t, c.prefixArena, arenaLenWithTwo)

		// Splitting a large prefix in-place into two < 65,535B halves records the orphaned 4-byte length header
		// in freePrefixSpans so deleting the split nodes trims prefixArena past the split prefix.
		splitKey1 := strings.Repeat("X", 40000) + strings.Repeat("Y", 30000)
		splitKey2 := strings.Repeat("X", 40000) + strings.Repeat("Z", 100)
		_, err = c.Put(splitKey1, 4)
		require.NoError(t, err)
		_, err = c.Put(splitKey2, 5)
		require.NoError(t, err)

		_, ok = c.Delete(splitKey2)
		require.True(t, ok)
		_, ok = c.Delete(splitKey1)
		require.True(t, ok)
		assert.Len(t, c.prefixArena, arenaLenWithTwo)
	})

	t.Run("NonContiguousMergeWithPinnedParentLeavesMergedSpanUnpinned", func(t *testing.T) {
		var evictedKeys []string
		c := NewArenaRadixCache[testData](30,
			WithInvariantChecking(true),
			testDataWeigher,
			WithOnEvictEntry(func(k string, _ testData, _ EvictionReason) {
				evictedKeys = append(evictedKeys, k)
			}),
		).(*arenaRadix[testData])

		// 1. Small combined <= 64 non-contiguous merge branch:
		// Insert parent first, then an unrelated key, then child so parent.prefixOff + parent.prefixLen != child.prefixOff.
		_, err := c.Put("root_prefix", testData{value: 1, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("separator_1", testData{value: 2, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("root_prefix/child_suffix", testData{value: 3, dataSize: 10})
		require.NoError(t, err)

		// Inserting "other" evicts "root_prefix" (pinning its old span) and merges "root_prefix" + "/child_suffix"
		// via the non-contiguous combined <= 64 branch into a newly allocated span on the surviving child node.
		_, err = c.Put("other", testData{value: 4, dataSize: 10})
		require.NoError(t, err)
		require.Equal(t, []string{"root_prefix"}, evictedKeys)
		childID := c.nodeMapAt(c.hashKey("root_prefix/child_suffix"))
		assert.Zero(t, c.nodes[childID].prefixFlags&prefixFlagPinned)

		// 2. General append combined > 64 non-contiguous merge branch:
		longParent := "P_" + strings.Repeat("p", 40)
		longChild := longParent + "/" + strings.Repeat("c", 40)
		_, err = c.Put(longParent, testData{value: 5, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put("separator_2", testData{value: 6, dataSize: 10})
		require.NoError(t, err)
		_, err = c.Put(longChild, testData{value: 7, dataSize: 10})
		require.NoError(t, err)
		// Touch separator_2 so longParent is at LRU tail.
		_, ok := c.Get("separator_2")
		require.True(t, ok)

		_, err = c.Put("other2", testData{value: 8, dataSize: 10})
		require.NoError(t, err)
		require.Contains(t, evictedKeys, longParent)
		longChildID := c.nodeMapAt(c.hashKey(longChild))
		assert.Zero(t, c.nodes[longChildID].prefixFlags&prefixFlagPinned)
	})
}

func TestArenaRadix_HashTag32CollisionAndSlotBelongsToOtherHash(t *testing.T) {
	c := NewArenaRadixCache[int](100, WithInvariantChecking(true)).(*arenaRadix[int])

	_, err := c.Put("key1", 10)
	require.NoError(t, err)
	_, err = c.Put("key2", 20)
	require.NoError(t, err)

	h1 := c.hashKey("key1")
	// Flip bit 0 and bit 32 so h2 != h1 while uint32(h2 ^ (h2 >> 32)) == uint32(h1 ^ (h1 >> 32)).
	h2 := h1 ^ 0x00000001_00000001
	require.NotEqual(t, h1, h2)
	require.Equal(t, arenaHashTag(h1), arenaHashTag(h2))

	idx1, ok := c.findHashSlotIndex(h1)
	require.True(t, ok)
	id1 := c.hashSlots[idx1].nodeID
	id2, ok := c.nodeMapGet(c.hashKey("key2"))
	require.True(t, ok)

	// 1. Verify slotBelongsToOtherHash boundary conditions and 32-bit tag vs 64-bit hash distinction.
	assert.False(t, c.slotBelongsToOtherHash(arenaHashSlot{tag: arenaHashTag(h1), nodeID: nilNode}, h2))
	assert.False(t, c.slotBelongsToOtherHash(arenaHashSlot{tag: arenaHashTag(h1), nodeID: c.root}, h2))
	assert.False(t, c.slotBelongsToOtherHash(c.hashSlots[idx1], h1), "same 64-bit hash must not be reported as another hash")
	assert.True(t, c.slotBelongsToOtherHash(c.hashSlots[idx1], h2), "distinct 64-bit hash with identical 32-bit tag must be reported as another hash")

	// 2. Verify lookupHashSlot and public lookup/get/delete with h2 treat idx1 as belonging to another hash
	// (slotMatched == false), avoiding false-positive 64-bit collision fallback walks.
	fallbacksBefore := c.Stats().ArenaHashFallbacks
	gotID, slotMatched, verified := c.lookupHashSlot("missing_key", h2)
	assert.Equal(t, nilNode, gotID)
	assert.False(t, slotMatched, "32-bit tag collision with a different 64-bit hash must not set slotMatched")
	assert.False(t, verified)

	_, ok = c.peekWithHash("missing_key", h2)
	assert.False(t, ok)
	_, ok = c.getWithHash("missing_key", h2)
	assert.False(t, ok)
	_, ok = c.deleteWithHash("missing_key", h2)
	assert.False(t, ok)
	assert.Equal(t, fallbacksBefore, c.Stats().ArenaHashFallbacks, "tag-only collision miss must not trigger trie walk fallback")

	// Contrast with a 64-bit hash collision on h1: slotBelongsToOtherHash is false, so slotMatched is true.
	gotID, slotMatched, verified = c.lookupHashSlot("missing_key", h1)
	assert.Equal(t, nilNode, gotID)
	assert.True(t, slotMatched, "64-bit hash collision must set slotMatched")
	assert.False(t, verified)

	// 3. Verify findHashSlotIndex, nodeMapSet, nodeMapGet, lookupHashSlot, and removeOrPromoteNodeMapLocked
	// when both h1 (at idx1) and h2 (probed past idx1 to idx2) occupy the probe chain with the same 32-bit tag.
	hKey2Natural := c.hashKey("key2")
	c.nodeMapDelete(hKey2Natural)

	idx2Probe, exists := c.findHashSlotIndex(h2)
	assert.False(t, exists, "findHashSlotIndex(h2) must probe past h1's slot at idx1")
	assert.NotEqual(t, idx1, idx2Probe)

	c.nodeMapSet(h2, id2)
	assert.Equal(t, 2, c.hashCount)

	gotID1, ok1 := c.nodeMapGet(h1)
	require.True(t, ok1)
	assert.Equal(t, id1, gotID1)

	gotID2, ok2 := c.nodeMapGet(h2)
	require.True(t, ok2)
	assert.Equal(t, id2, gotID2)

	gotLookupID2, slotMatched2, verified2 := c.lookupHashSlot("key2", h2)
	assert.Equal(t, id2, gotLookupID2)
	assert.True(t, slotMatched2)
	assert.True(t, verified2)

	// Removing (h2, id2) via removeOrPromoteNodeMapLocked must skip idx1 (which belongs to h1)
	// and delete idx2, leaving (h1, id1) intact.
	c.removeOrPromoteNodeMapLocked(h2, id2)
	assert.Equal(t, 1, c.hashCount)
	gotID1After, ok1After := c.nodeMapGet(h1)
	require.True(t, ok1After)
	assert.Equal(t, id1, gotID1After)
	_, ok2After := c.nodeMapGet(h2)
	assert.False(t, ok2After)

	// Restore key2's natural hash slot and verify all cache invariants hold.
	c.nodeMapSet(hKey2Natural, id2)
	c.checkInvariants()
}

func TestArenaRadix_ReconstructKeyExceedingNodeKeyLenMask(t *testing.T) {
	var evictedKeys []string
	var evictedReasons []EvictionReason
	c := NewArenaRadixCache[testData](30,
		WithInvariantChecking(true),
		testDataWeigher,
		WithOnEvictEntry(func(k string, _ testData, reason EvictionReason) {
			evictedKeys = append(evictedKeys, k)
			evictedReasons = append(evictedReasons, reason)
		}),
	).(*arenaRadix[testData])

	// Construct multi-segment keys (depth >= 2) whose total length is 140,001 bytes > nodeKeyLenMask (131,071).
	sharedPrefix := strings.Repeat("p", 70000) + "/"
	keyA := sharedPrefix + strings.Repeat("a", 70000)
	keyB := sharedPrefix + strings.Repeat("b", 70000)
	require.Greater(t, len(keyA), int(nodeKeyLenMask))
	require.Greater(t, len(keyB), int(nodeKeyLenMask))

	_, err := c.Put(keyA, testData{value: 1, dataSize: 10})
	require.NoError(t, err)
	_, err = c.Put(keyB, testData{value: 2, dataSize: 10})
	require.NoError(t, err)

	idA, ok := c.nodeMapGet(c.hashKey(keyA))
	require.True(t, ok)
	idB, ok := c.nodeMapGet(c.hashKey(keyB))
	require.True(t, ok)

	// Both leaf nodes are at depth 2 (parent != root) and saturate the 17-bit keyLen field at nodeKeyLenMask.
	assert.NotEqual(t, c.root, c.nodes[idA].parent)
	assert.NotEqual(t, c.root, c.nodes[idB].parent)
	assert.Equal(t, int(nodeKeyLenMask), c.nodes[idA].keyLen())
	assert.Equal(t, int(nodeKeyLenMask), c.nodes[idB].keyLen())

	// 1. Direct reconstructKey() must return the full untruncated 140,001-byte keys.
	assert.Equal(t, keyA, c.reconstructKey(idA))
	assert.Equal(t, keyB, c.reconstructKey(idB))

	// 2. Keys() and All() iterators must yield the exact full keys in MRU-to-LRU order.
	assert.Equal(t, []string{keyB, keyA}, slices.Collect(c.Keys()))
	var allKeys []string
	var allVals []int64
	for k, v := range c.All() {
		allKeys = append(allKeys, k)
		allVals = append(allVals, v.value)
	}
	assert.Equal(t, []string{keyB, keyA}, allKeys)
	assert.Equal(t, []int64{2, 1}, allVals)

	// 3. Get and Peek must verify the full > 131,071-byte multi-segment key.
	gotB, ok := c.Peek(keyB)
	require.True(t, ok)
	assert.Equal(t, int64(2), gotB.value)
	gotA, ok := c.Get(keyA) // Promotes keyA to MRU so keyB becomes the LRU tail victim.
	require.True(t, ok)
	assert.Equal(t, int64(1), gotA.value)

	// 4. Capacity eviction of depth-2 node idB (via Put of a 25-byte entry into maxSize=30)
	// must reconstruct and pass the exact 140,001-byte keyB to WithOnEvictEntry.
	evicted, err := c.Put("survivor", testData{value: 3, dataSize: 20})
	require.NoError(t, err)
	assertEvictedValues(t, evicted, []int64{2})
	require.Equal(t, []string{keyB}, evictedKeys)
	require.Equal(t, []EvictionReason{EvictionReasonCapacity}, evictedReasons)
}

func TestArenaRadix_PrefixArenaFreeSpansCompactionAndLargePrefixChurn(t *testing.T) {
	t.Run("CoalesceAdjacentSpansBothDirections", func(t *testing.T) {
		// Direction 1: sp.off + sp.len == other.off in mergeAdjacentFreeSpanLocked
		// Free A, then C, then B (where B attaches to A's end and bridges to C's start).
		c1 := NewArenaRadixCache[int](100, WithInvariantChecking(true)).(*arenaRadix[int])
		keyA := "A_" + strings.Repeat("a", 8)
		keyB := "B_" + strings.Repeat("b", 8)
		keyC := "C_" + strings.Repeat("c", 8)
		keyD := "D_" + strings.Repeat("d", 8)

		for i, k := range []string{keyA, keyB, keyC, keyD} {
			_, err := c1.Put(k, i+1)
			require.NoError(t, err)
		}
		_, ok := c1.Delete(keyA)
		require.True(t, ok)
		_, ok = c1.Delete(keyC)
		require.True(t, ok)
		require.Equal(t, 2, c1.freePrefixCount)

		_, ok = c1.Delete(keyB)
		require.True(t, ok)
		require.Equal(t, 1, c1.freePrefixCount)
		assert.Equal(t, prefixSpan{off: 0, len: 30}, c1.freePrefixSpans[0])

		// Deleting tail keyD trims D and then trims the coalesced [0, 30) span.
		_, ok = c1.Delete(keyD)
		require.True(t, ok)
		assert.Zero(t, c1.freePrefixCount)
		assert.Empty(t, c1.prefixArena)

		// Direction 2: other.off + other.len == sp.off in mergeAdjacentFreeSpanLocked
		// Free C, then A, then B (where B attaches to C's start and bridges back to A's end).
		c2 := NewArenaRadixCache[int](100, WithInvariantChecking(true)).(*arenaRadix[int])
		for i, k := range []string{keyA, keyB, keyC, keyD} {
			_, err := c2.Put(k, i+1)
			require.NoError(t, err)
		}
		_, ok = c2.Delete(keyC)
		require.True(t, ok)
		_, ok = c2.Delete(keyA)
		require.True(t, ok)
		require.Equal(t, 2, c2.freePrefixCount)

		_, ok = c2.Delete(keyB)
		require.True(t, ok)
		require.Equal(t, 1, c2.freePrefixCount)
		assert.Equal(t, prefixSpan{off: 0, len: 30}, c2.freePrefixSpans[0])
	})

	t.Run("FreePrefixSpans32SlotOverflowReplacesSmallestAndIgnoresSmaller", func(t *testing.T) {
		c := NewArenaRadixCache[int](500, WithInvariantChecking(true)).(*arenaRadix[int])

		const totalHoles = 34
		holeKeys := make([]string, totalHoles)
		for i := range totalHoles {
			// Give every hole and separator a unique first byte so all nodes remain direct children of root without splitting.
			holeFirst := byte(1 + i*2)
			sepFirst := byte(2 + i*2)
			var holeLen int
			switch i {
			case 0:
				holeLen = 14
			case 1:
				holeLen = 10 // Smallest among first 32 holes (at index 1).
			case 32:
				holeLen = 60 // 33rd hole: larger than minLen (10), must replace index 1.
			case 33:
				holeLen = 8 // 34th hole: smaller than new minLen (14), must be ignored.
			default:
				holeLen = 14 + i
			}
			holeKeys[i] = string([]byte{holeFirst}) + strings.Repeat("h", holeLen-1)
			sepKey := string([]byte{sepFirst}) + "sep!"

			_, err := c.Put(holeKeys[i], i+1)
			require.NoError(t, err)
			_, err = c.Put(sepKey, 1000+i)
			require.NoError(t, err)
		}

		for i := range 32 {
			_, ok := c.Delete(holeKeys[i])
			require.True(t, ok)
		}
		require.Equal(t, 32, c.freePrefixCount)
		assert.Equal(t, uint32(10), c.freePrefixSpans[1].len)

		// Freeing the 33rd hole (len 60 > minLen 10) replaces the smallest slot at index 1.
		_, ok := c.Delete(holeKeys[32])
		require.True(t, ok)
		require.Equal(t, 32, c.freePrefixCount)
		assert.Equal(t, uint32(60), c.freePrefixSpans[1].len)

		// Freeing the 34th hole (len 8 < new minLen 14) leaves all 32 slots unchanged.
		spansSnapshot := c.freePrefixSpans
		_, ok = c.Delete(holeKeys[33])
		require.True(t, ok)
		require.Equal(t, 32, c.freePrefixCount)
		assert.Equal(t, spansSnapshot, c.freePrefixSpans)

		// Allocating a new key of length 15 carves from the best-fit 16-byte span (hole 2).
		newKey := string([]byte{200}) + strings.Repeat("n", 14)
		_, err := c.Put(newKey, 9999)
		require.NoError(t, err)
		assert.Equal(t, uint32(1), c.freePrefixSpans[2].len)
	})

	t.Run("EnsurePrefixArenaCapAutoCompactionAndPrefixExposedReset", func(t *testing.T) {
		c := NewArenaRadixCache[int](100, WithInvariantChecking(true)).(*arenaRadix[int])

		// Insert 4 keys of 350 bytes each -> len(prefixArena) == 1400 >= 1024.
		k1 := "1_" + strings.Repeat("a", 348)
		k2 := "2_" + strings.Repeat("b", 348)
		k3 := "3_" + strings.Repeat("c", 348)
		k4 := "4_" + strings.Repeat("d", 348)
		for i, k := range []string{k1, k2, k3, k4} {
			_, err := c.Put(k, i+1)
			require.NoError(t, err)
		}

		// 1. Non-exposed >= 1024 auto-compaction branch:
		// Delete k1 and k3 (leaving two 350B non-adjacent free spans; livePrefixBytes == 700 <= 1400/2).
		_, ok := c.Delete(k1)
		require.True(t, ok)
		_, ok = c.Delete(k3)
		require.True(t, ok)
		require.Equal(t, 2, c.freePrefixCount)
		require.Equal(t, 700, c.livePrefixBytes)
		require.Len(t, c.prefixArena, 1400)

		// Insert k5 (> 350B so neither free span fits, and > remaining cap slack) to trigger auto-compaction.
		slack := cap(c.prefixArena) - len(c.prefixArena)
		k5 := "5_" + strings.Repeat("e", max(slack+100, 400))
		_, err := c.Put(k5, 5)
		require.NoError(t, err)
		assert.Zero(t, c.freePrefixCount, "compaction during ensurePrefixArenaCap must clear freePrefixSpans")
		assert.Len(t, c.prefixArena, c.livePrefixBytes)

		// 2. prefixExposed auto-compaction branch:
		// Expose depth-1 prefixes via Keys() iterator so prefixExposed is set to true.
		for range c.Keys() {
			break
		}
		require.True(t, c.prefixExposed.Load())

		// Delete k2: because prefixExposed is true, its 350 bytes remain dead in prefixArena without entering freePrefixSpans.
		_, ok = c.Delete(k2)
		require.True(t, ok)
		require.Zero(t, c.freePrefixCount)
		require.Less(t, c.livePrefixBytes, len(c.prefixArena))

		// Insert k6 exceeding remaining capacity slack, triggering ensurePrefixArenaCap compaction and resetting prefixExposed.
		slack2 := cap(c.prefixArena) - len(c.prefixArena)
		k6 := "6_" + strings.Repeat("f", slack2+100)
		_, err = c.Put(k6, 6)
		require.NoError(t, err)
		assert.False(t, c.prefixExposed.Load(), "prefixExposed must be reset after prefixArena compaction")
		assert.Len(t, c.prefixArena, c.livePrefixBytes)
	})

	t.Run("LargePrefixSplitAndMergeChurnKeepsArenaCapacityBounded", func(t *testing.T) {
		c := NewArenaRadixCache[int](100, WithInvariantChecking(true)).(*arenaRadix[int])

		// Part 1: In-place split and merge of a large prefix (lcp >= 65,535, remLen < 65,535).
		largeShared := strings.Repeat("L", int(largePrefixSentinel)+500) + "/"
		inPlaceKey1 := largeShared + "alpha_" + strings.Repeat("a", 100)
		inPlaceKey2 := largeShared + "bravo_" + strings.Repeat("b", 100)

		_, err := c.Put(inPlaceKey1, 1)
		require.NoError(t, err)
		_, err = c.Put(inPlaceKey2, 2)
		require.NoError(t, err)
		_, ok := c.Delete(inPlaceKey2)
		require.True(t, ok)
		capAfterWarmup := cap(c.prefixArena)

		for i := range 25 {
			_, err = c.Put(inPlaceKey2, i+10)
			require.NoError(t, err)
			_, ok = c.Delete(inPlaceKey2)
			require.True(t, ok)
		}
		assert.Equal(t, capAfterWarmup, cap(c.prefixArena), "in-place large-prefix split/merge churn must not grow prefixArena capacity")
		v1, ok := c.Get(inPlaceKey1)
		require.True(t, ok)
		assert.Equal(t, 1, v1)

		// Part 2: Split and merge where both lcp >= 65,535 and remLen >= 65,535 (exercising general large-prefix split/merge + compaction).
		bothLargeKey1 := largeShared + "alpha_" + strings.Repeat("a", int(largePrefixSentinel)+200)
		bothLargeKey2 := largeShared + "bravo_" + strings.Repeat("b", 100)
		_, ok = c.Delete(inPlaceKey1)
		require.True(t, ok)

		_, err = c.Put(bothLargeKey1, 100)
		require.NoError(t, err)
		for i := range 15 {
			_, err = c.Put(bothLargeKey2, 200+i)
			require.NoError(t, err)
			_, ok = c.Delete(bothLargeKey2)
			require.True(t, ok)
		}
		// Live prefix bytes are ~132KB; capacity must remain bounded (<= 1MB) rather than ratcheting exponentially.
		assert.LessOrEqual(t, cap(c.prefixArena), 1024*1024)
		vLarge1, ok := c.Get(bothLargeKey1)
		require.True(t, ok)
		assert.Equal(t, 100, vLarge1)
	})
}
