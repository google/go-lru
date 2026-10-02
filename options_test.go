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
	"math"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackendSelectionAndUnifiedNew verifies that New[V]() dispatches to the expected
// concrete backend implementation (*mapCache[V], *radixCache[V], *arenaRadix[V]) for each Backend option.
func TestBackendSelectionAndUnifiedNew(t *testing.T) {
	// Arrange & Act
	defaultCache := New[string](1024)
	mapCacheInstance := New[string](1024, WithBackend(BackendMap))
	radixCacheInstance := New[string](1024, WithBackend(BackendRadix))
	arenaCacheInstance := New[string](1024, WithBackend(BackendArenaRadix))
	unknownBackendCache := New[string](1024, WithBackend(Backend(99)))

	// Assert concrete backend types & String() representations
	assert.Equal(t, "MapCache", BackendMap.String())
	assert.Equal(t, "RadixCache", BackendRadix.String())
	assert.Equal(t, "ArenaRadixCache", BackendArenaRadix.String())
	assert.Equal(t, "UnknownBackend", Backend(99).String())

	_, isDefaultMap := defaultCache.(*mapCache[string])
	assert.True(t, isDefaultMap, "default New() should construct *mapCache")

	_, isMap := mapCacheInstance.(*mapCache[string])
	assert.True(t, isMap, "WithBackend(BackendMap) should construct *mapCache")
	_, mapImplementsPressureAware := mapCacheInstance.(PressureAwareCache[string])
	assert.True(t, mapImplementsPressureAware, "MapCache returned by New should implement PressureAwareCache")

	_, isRadix := radixCacheInstance.(*radixCache[string])
	assert.True(t, isRadix, "WithBackend(BackendRadix) should construct *radixCache")
	_, radixImplementsPressureAware := radixCacheInstance.(PressureAwareCache[string])
	assert.True(t, radixImplementsPressureAware, "RadixCache returned by New should implement PressureAwareCache")

	_, isArena := arenaCacheInstance.(*arenaRadix[string])
	assert.True(t, isArena, "WithBackend(BackendArenaRadix) should construct *arenaRadix")
	_, implementsPressureAware := arenaCacheInstance.(PressureAwareCache[string])
	assert.True(t, implementsPressureAware, "ArenaRadixCache returned by New should implement PressureAwareCache")

	_, isUnknownNormalizedToMap := unknownBackendCache.(*mapCache[string])
	assert.True(t, isUnknownNormalizedToMap, "unrecognized Backend value should normalize to BackendMap")
}

func TestOptions_WithWeigher(t *testing.T) {
	// Arrange
	fn := func(k, v string) uint64 { return uint64(len(k) + len(v)) }
	var typedWeigher Weigher[string] = fn
	type customWeigher func(string, string) uint64
	var typedNilFunc func(string, int) uint64
	var typedNilWeigher Weigher[int]
	var typedNilAnyFunc func(string, any) uint64
	var typedNilAnyWeigher Weigher[any]
	var typedNilCustom customWeigher

	// Act
	optsDefault := ApplyOptions()
	optsCustom := ApplyOptions(WithWeigher(fn))
	optsNil := ApplyOptions(WithWeigher(fn), WithWeigher[string](nil))
	optsTypedNilDirect := ApplyOptions(func(o *Options) { o.Weigher = typedNilFunc })
	optsTypedNilWeigherDirect := ApplyOptions(func(o *Options) { o.Weigher = typedNilWeigher })
	optsTypedNilCustomDirect := ApplyOptions(func(o *Options) { o.Weigher = typedNilCustom })
	optsTypedDirect := ApplyOptions(func(o *Options) { o.Weigher = typedWeigher })
	optsAnyDirect := ApplyOptions(WithWeigher(func(k string, _ any) uint64 { return uint64(len(k)) }))
	optsWeigherAnyDirect := ApplyOptions(func(o *Options) {
		o.Weigher = Weigher[any](func(k string, _ any) uint64 { return uint64(len(k)) })
	})

	// Assert
	assert.Nil(t, optsDefault.Weigher)
	assert.NotNil(t, optsCustom.Weigher)
	assert.Nil(t, optsNil.Weigher)
	assert.Nil(t, optsTypedNilDirect.Weigher)
	assert.Nil(t, optsTypedNilWeigherDirect.Weigher)
	assert.Nil(t, optsTypedNilCustomDirect.Weigher)
	assert.Nil(t, resolveWeigher[string](optsTypedNilDirect))
	assert.Nil(t, resolveWeigher[string](Options{Weigher: typedNilFunc}))
	assert.Nil(t, resolveWeigher[string](Options{Weigher: typedNilWeigher}))
	assert.Nil(t, resolveWeigher[string](Options{Weigher: typedNilAnyFunc}))
	assert.Nil(t, resolveWeigher[string](Options{Weigher: typedNilAnyWeigher}))
	assert.Nil(t, resolveWeigher[string](Options{Weigher: typedNilCustom}))
	assert.Equal(t, uint64(5), resolveWeigher[string](optsCustom)("ab", "cde"))
	assert.Equal(t, uint64(5), resolveWeigher[string](optsTypedDirect)("ab", "cde"))
	assert.Equal(t, uint64(2), resolveWeigher[string](optsAnyDirect)("ab", "cde"))
	assert.Equal(t, uint64(2), resolveWeigher[string](optsWeigherAnyDirect)("ab", "cde"))

	assert.PanicsWithValue(t,
		"lru: WithWeigher function type func(string, int) uint64 does not match cache value type string",
		func() {
			_ = New[string](10, WithWeigher(func(_ string, v int) uint64 { return uint64(v) }))
		},
	)
	assert.PanicsWithValue(t,
		"lru: WithWeigher function type func(string, int) uint64 does not match cache value type *lru.testData",
		func() {
			_ = New[*testData](10, WithWeigher(func(_ string, v int) uint64 { return uint64(v) }))
		},
	)
	assert.PanicsWithValue(t,
		"lru: WithWeigher function type func(string, int) uint64 does not match cache value type interface {}",
		func() {
			_ = New[any](10, WithWeigher(func(_ string, v int) uint64 { return uint64(v) }))
		},
	)
}

func TestEvictionReason_String(t *testing.T) {
	// Arrange & Act & Assert
	assert.Equal(t, "Capacity", EvictionReasonCapacity.String())
	assert.Equal(t, "Pressure", EvictionReasonPressure.String())
	assert.Equal(t, "Deleted", EvictionReasonDeleted.String())
	assert.Equal(t, "Replaced", EvictionReasonReplaced.String())
	assert.Equal(t, "UnknownEvictionReason", EvictionReason(99).String())
}

func TestOptions_WithOnEvictValue(t *testing.T) {
	// Arrange
	var capturedVal string
	var capturedReason EvictionReason
	fn := func(v string, r EvictionReason) {
		capturedVal = v
		capturedReason = r
	}
	var typedCb OnEvictValue[string] = fn
	type customCb func(string, EvictionReason)
	var typedNilFunc func(int, EvictionReason)
	var typedNilCb OnEvictValue[int]
	var typedNilAnyFunc func(any, EvictionReason)
	var typedNilAnyCb OnEvictValue[any]
	var typedNilCustom customCb

	// Act
	optsDefault := ApplyOptions()
	optsCustom := ApplyOptions(WithOnEvictValue(fn))
	optsNil := ApplyOptions(WithOnEvictValue(fn), WithOnEvictValue[string](nil))
	optsIndependent := ApplyOptions(
		WithOnEvictEntry(func(_ string, _ string, _ EvictionReason) {}),
		WithOnEvictValue(fn),
		WithOnEvictValue[string](nil),
	)
	optsTypedNilDirect := ApplyOptions(func(o *Options) { o.OnEvictValue = typedNilFunc })
	optsTypedNilCbDirect := ApplyOptions(func(o *Options) { o.OnEvictValue = typedNilCb })
	optsTypedNilCustomDirect := ApplyOptions(func(o *Options) { o.OnEvictValue = typedNilCustom })
	optsTypedDirect := ApplyOptions(func(o *Options) { o.OnEvictValue = typedCb })
	var anyCaptured any
	optsAnyDirect := ApplyOptions(WithOnEvictValue(func(v any, _ EvictionReason) { anyCaptured = v }))
	optsCbAnyDirect := ApplyOptions(func(o *Options) {
		o.OnEvictValue = OnEvictValue[any](func(v any, _ EvictionReason) { anyCaptured = v })
	})

	// Assert
	assert.Nil(t, optsDefault.OnEvictValue)
	assert.NotNil(t, optsCustom.OnEvictValue)
	assert.Nil(t, optsNil.OnEvictValue)
	assert.Nil(t, optsIndependent.OnEvictValue)
	assert.NotNil(t, optsIndependent.OnEvictEntry, "clearing OnEvictValue must not clear OnEvictEntry")
	assert.Nil(t, optsTypedNilDirect.OnEvictValue)
	assert.Nil(t, optsTypedNilCbDirect.OnEvictValue)
	assert.Nil(t, optsTypedNilCustomDirect.OnEvictValue)
	assert.Nil(t, resolveOnEvictValue[string](optsTypedNilDirect))
	assert.Nil(t, resolveOnEvictValue[string](Options{OnEvictValue: typedNilFunc}))
	assert.Nil(t, resolveOnEvictValue[string](Options{OnEvictValue: typedNilCb}))
	assert.Nil(t, resolveOnEvictValue[string](Options{OnEvictValue: typedNilAnyFunc}))
	assert.Nil(t, resolveOnEvictValue[string](Options{OnEvictValue: typedNilAnyCb}))
	assert.Nil(t, resolveOnEvictValue[string](Options{OnEvictValue: typedNilCustom}))

	resolveOnEvictValue[string](optsCustom)("v1", EvictionReasonCapacity)
	assert.Equal(t, "v1", capturedVal)
	assert.Equal(t, EvictionReasonCapacity, capturedReason)

	resolveOnEvictValue[string](optsTypedDirect)("v2", EvictionReasonDeleted)
	assert.Equal(t, "v2", capturedVal)
	assert.Equal(t, EvictionReasonDeleted, capturedReason)

	resolveOnEvictValue[string](optsAnyDirect)("v3", EvictionReasonPressure)
	assert.Equal(t, "v3", anyCaptured)

	resolveOnEvictValue[string](optsCbAnyDirect)("v4", EvictionReasonReplaced)
	assert.Equal(t, "v4", anyCaptured)

	expectedPanic := "lru: WithOnEvictValue function type func(int, lru.EvictionReason) does not match cache value type string"
	mismatchedOpt := WithOnEvictValue(func(_ int, _ EvictionReason) {})
	assert.PanicsWithValue(t, expectedPanic, func() { _ = New[string](10, mismatchedOpt) })
	assert.PanicsWithValue(t, expectedPanic, func() { _ = NewMapCache[string](10, mismatchedOpt) })
	assert.PanicsWithValue(t, expectedPanic, func() { _ = NewRadixCache[string](10, mismatchedOpt) })
	assert.PanicsWithValue(t, expectedPanic, func() { _ = NewArenaRadixCache[string](10, mismatchedOpt) })
}

func TestOptions_WithOnEvictEntry(t *testing.T) {
	// Arrange
	var capturedKey, capturedVal string
	var capturedReason EvictionReason
	fn := func(k, v string, r EvictionReason) {
		capturedKey = k
		capturedVal = v
		capturedReason = r
	}
	var typedCb OnEvictEntry[string] = fn
	type customCb func(string, string, EvictionReason)
	var typedNilFunc func(string, int, EvictionReason)
	var typedNilCb OnEvictEntry[int]
	var typedNilAnyFunc func(string, any, EvictionReason)
	var typedNilAnyCb OnEvictEntry[any]
	var typedNilCustom customCb

	// Act
	optsDefault := ApplyOptions()
	optsCustom := ApplyOptions(WithOnEvictEntry(fn))
	optsNil := ApplyOptions(WithOnEvictEntry(fn), WithOnEvictEntry[string](nil))
	optsIndependent := ApplyOptions(
		WithOnEvictValue(func(_ string, _ EvictionReason) {}),
		WithOnEvictEntry(fn),
		WithOnEvictEntry[string](nil),
	)
	optsTypedNilDirect := ApplyOptions(func(o *Options) { o.OnEvictEntry = typedNilFunc })
	optsTypedNilCbDirect := ApplyOptions(func(o *Options) { o.OnEvictEntry = typedNilCb })
	optsTypedNilCustomDirect := ApplyOptions(func(o *Options) { o.OnEvictEntry = typedNilCustom })
	optsTypedDirect := ApplyOptions(func(o *Options) { o.OnEvictEntry = typedCb })
	var anyKey string
	var anyVal any
	optsAnyDirect := ApplyOptions(WithOnEvictEntry(func(k string, v any, _ EvictionReason) {
		anyKey = k
		anyVal = v
	}))
	optsCbAnyDirect := ApplyOptions(func(o *Options) {
		o.OnEvictEntry = OnEvictEntry[any](func(k string, v any, _ EvictionReason) {
			anyKey = k
			anyVal = v
		})
	})

	// Assert
	assert.Nil(t, optsDefault.OnEvictEntry)
	assert.NotNil(t, optsCustom.OnEvictEntry)
	assert.Nil(t, optsNil.OnEvictEntry)
	assert.Nil(t, optsIndependent.OnEvictEntry)
	assert.NotNil(t, optsIndependent.OnEvictValue, "clearing OnEvictEntry must not clear OnEvictValue")
	assert.Nil(t, optsTypedNilDirect.OnEvictEntry)
	assert.Nil(t, optsTypedNilCbDirect.OnEvictEntry)
	assert.Nil(t, optsTypedNilCustomDirect.OnEvictEntry)
	assert.Nil(t, resolveOnEvictEntry[string](optsTypedNilDirect))
	assert.Nil(t, resolveOnEvictEntry[string](Options{OnEvictEntry: typedNilFunc}))
	assert.Nil(t, resolveOnEvictEntry[string](Options{OnEvictEntry: typedNilCb}))
	assert.Nil(t, resolveOnEvictEntry[string](Options{OnEvictEntry: typedNilAnyFunc}))
	assert.Nil(t, resolveOnEvictEntry[string](Options{OnEvictEntry: typedNilAnyCb}))
	assert.Nil(t, resolveOnEvictEntry[string](Options{OnEvictEntry: typedNilCustom}))

	resolveOnEvictEntry[string](optsCustom)("k1", "v1", EvictionReasonCapacity)
	assert.Equal(t, "k1", capturedKey)
	assert.Equal(t, "v1", capturedVal)
	assert.Equal(t, EvictionReasonCapacity, capturedReason)

	resolveOnEvictEntry[string](optsTypedDirect)("k2", "v2", EvictionReasonDeleted)
	assert.Equal(t, "k2", capturedKey)
	assert.Equal(t, "v2", capturedVal)
	assert.Equal(t, EvictionReasonDeleted, capturedReason)

	resolveOnEvictEntry[string](optsAnyDirect)("k3", "v3", EvictionReasonPressure)
	assert.Equal(t, "k3", anyKey)
	assert.Equal(t, "v3", anyVal)

	resolveOnEvictEntry[string](optsCbAnyDirect)("k4", "v4", EvictionReasonReplaced)
	assert.Equal(t, "k4", anyKey)
	assert.Equal(t, "v4", anyVal)

	expectedPanic := "lru: WithOnEvictEntry function type func(string, int, lru.EvictionReason) does not match cache value type string"
	mismatchedOpt := WithOnEvictEntry(func(_ string, _ int, _ EvictionReason) {})
	assert.PanicsWithValue(t, expectedPanic, func() { _ = New[string](10, mismatchedOpt) })
	assert.PanicsWithValue(t, expectedPanic, func() { _ = NewMapCache[string](10, mismatchedOpt) })
	assert.PanicsWithValue(t, expectedPanic, func() { _ = NewRadixCache[string](10, mismatchedOpt) })
	assert.PanicsWithValue(t, expectedPanic, func() { _ = NewArenaRadixCache[string](10, mismatchedOpt) })
}

func TestOptions_Default(t *testing.T) {
	// Arrange & Act
	opts := ApplyOptions()

	// Assert
	assert.False(t, opts.EnableInvariantChecking)
	assert.Nil(t, opts.Weigher)
	assert.Nil(t, opts.OnEvictValue)
	assert.Nil(t, opts.OnEvictEntry)
}

func TestOptions_WithInvariantChecking(t *testing.T) {
	// Arrange & Act
	optsTrue := ApplyOptions(WithInvariantChecking(true))
	optsFalse := ApplyOptions(WithInvariantChecking(false))
	optsChained := ApplyOptions(nil, WithInvariantChecking(false), nil, WithInvariantChecking(true))

	// Assert
	assert.True(t, optsTrue.EnableInvariantChecking)
	assert.False(t, optsFalse.EnableInvariantChecking)
	assert.True(t, optsChained.EnableInvariantChecking)
}

func TestSentinelErrors(t *testing.T) {
	// Arrange
	tests := []struct {
		err      error
		expected string
	}{
		{
			err:      ErrInvalidEntrySize,
			expected: "size of the entry is more than the cache's maxSize",
		},
		{
			err:      ErrEntryNotExist,
			expected: "entry with given key does not exist",
		},
	}

	// Act & Assert
	for _, tc := range tests {
		require.Error(t, tc.err)
		assert.EqualError(t, tc.err, tc.expected)
	}
}

func TestConstructors_Validation(t *testing.T) {
	// Arrange
	constructors := append(allBackends[testData](), struct {
		name string
		fn   func(uint64, ...Option) Cache[testData]
	}{"New", New[testData]})

	// Act & Assert
	for _, c := range constructors {
		t.Run(c.name, func(t *testing.T) {
			t.Run("ZeroMaxSize_DefaultOptions", func(t *testing.T) {
				// Arrange, Act & Assert
				assert.Panics(t, func() {
					_ = c.fn(0)
				})
			})

			t.Run("ZeroMaxSize_InvariantsDisabled", func(t *testing.T) {
				// Arrange, Act & Assert
				assert.Panics(t, func() {
					_ = c.fn(0, WithInvariantChecking(false))
				})
			})

			t.Run("ZeroMaxSize_InvariantsEnabled", func(t *testing.T) {
				// Arrange, Act & Assert
				assert.Panics(t, func() {
					_ = c.fn(0, WithInvariantChecking(true))
				})
			})

			t.Run("PositiveBoundary_MaxSize1", func(t *testing.T) {
				// Arrange & Act
				cache := c.fn(1)

				// Assert
				require.NotNil(t, cache)
			})
		})
	}
}

func TestOptions_MemoryPressureDefaults(t *testing.T) {
	// Arrange & Act
	opts := ApplyOptions()

	// Assert
	assert.InDelta(t, 0.75, DefaultCompactionThreshold, 1e-9)
	assert.InDelta(t, 0.90, DefaultEvictionThreshold, 1e-9)
	assert.InDelta(t, 0.50, DefaultEvictionRetentionRatio, 1e-9)
	assert.InDelta(t, DefaultCompactionThreshold, opts.CompactionThreshold, 1e-9)
	assert.InDelta(t, DefaultEvictionThreshold, opts.EvictionThreshold, 1e-9)
	assert.InDelta(t, DefaultEvictionRetentionRatio, opts.EvictionRetentionRatio, 1e-9)
	require.NotNil(t, opts.PressureFunc)
	assert.GreaterOrEqual(t, opts.PressureFunc(), 0.0)
}

func TestOptions_MemoryPressureCustomAndValidation(t *testing.T) {
	// Arrange
	var customCalls int
	customFn := func() float64 {
		customCalls++
		return 0.88
	}

	// Act
	opts := ApplyOptions(
		WithPressureFunc(customFn),
		WithMemoryBudget(256*1024*1024),
		WithCompactionThreshold(0.60),
		WithEvictionThreshold(0.85),
		WithEvictionRetentionRatio(0.35),
	)
	optsZeroRetention := ApplyOptions(WithEvictionRetentionRatio(0.0))
	optsClamped := ApplyOptions(
		WithCompactionThreshold(-0.1),
		WithEvictionThreshold(0),
		WithEvictionRetentionRatio(1.5),
	)
	optsNegativeRetention := ApplyOptions(WithEvictionRetentionRatio(-0.25))
	optsNaNAndInf := ApplyOptions(
		WithCompactionThreshold(math.NaN()),
		WithEvictionThreshold(math.Inf(1)),
		WithEvictionRetentionRatio(math.NaN()),
	)
	optsDirectStructPressure := ApplyOptions(func(o *Options) {
		o.PressureFunc = customFn
	})

	// Assert
	assert.Equal(t, uint64(256*1024*1024), opts.MemoryBudget)
	assert.InDelta(t, 0.60, opts.CompactionThreshold, 1e-9)
	assert.InDelta(t, 0.85, opts.EvictionThreshold, 1e-9)
	assert.InDelta(t, 0.35, opts.EvictionRetentionRatio, 1e-9)
	require.NotNil(t, opts.PressureFunc)
	assert.InDelta(t, 0.88, opts.PressureFunc(), 1e-9)

	assert.InDelta(t, 0.0, optsZeroRetention.EvictionRetentionRatio, 1e-9)

	assert.InDelta(t, DefaultCompactionThreshold, optsClamped.CompactionThreshold, 1e-9)
	assert.InDelta(t, DefaultEvictionThreshold, optsClamped.EvictionThreshold, 1e-9)
	assert.InDelta(t, 1.0, optsClamped.EvictionRetentionRatio, 1e-9)

	assert.InDelta(t, DefaultEvictionRetentionRatio, optsNegativeRetention.EvictionRetentionRatio, 1e-9)

	assert.InDelta(t, DefaultCompactionThreshold, optsNaNAndInf.CompactionThreshold, 1e-9)
	assert.InDelta(t, DefaultEvictionThreshold, optsNaNAndInf.EvictionThreshold, 1e-9)
	assert.InDelta(t, DefaultEvictionRetentionRatio, optsNaNAndInf.EvictionRetentionRatio, 1e-9)

	require.NotNil(t, optsDirectStructPressure.PressureFunc)
	callsBefore := customCalls
	c := NewMapCache[string](100, func(o *Options) { o.PressureFunc = customFn })
	_, err := c.Put("k", "v")
	require.NoError(t, err)
	assert.Greater(t, customCalls, callsBefore)
}

func TestCache_DefaultUnitWeigher_AllBackends(t *testing.T) {
	for _, b := range allBackends[string]() {
		t.Run(b.name, func(t *testing.T) {
			// Default weigher assigns weight 1 to every entry regardless of string length.
			cache := b.fn(3, WithInvariantChecking(true))

			ev, err := cache.Put("k1", "a")
			require.NoError(t, err)
			assert.Empty(t, ev)

			ev, err = cache.Put("k2", "bb")
			require.NoError(t, err)
			assert.Empty(t, ev)

			ev, err = cache.Put("k3", strings.Repeat("c", 4096))
			require.NoError(t, err)
			assert.Empty(t, ev)

			// Promote k1; k2 becomes LRU tail.
			v1, ok := cache.Get("k1")
			require.True(t, ok)
			assert.Equal(t, "a", v1)

			// Replace k1 in place with 10KB string; under default weigher its weight stays 1.
			require.NoError(t, cache.Replace("k1", strings.Repeat("x", 10240)))

			// Putting 4th entry evicts k2 ("bb").
			ev, err = cache.Put("k4", "dddd")
			require.NoError(t, err)
			assert.Equal(t, []string{"bb"}, ev)

			_, ok = cache.Peek("k2")
			assert.False(t, ok)

			v1After, ok := cache.Peek("k1")
			require.True(t, ok)
			assert.Equal(t, strings.Repeat("x", 10240), v1After)
		})
	}

	// Verify arbitrary value types (primitives, slices including nil, pointers including nil) under default weigher.
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"_ArbitraryTypesAndNilValues", func(t *testing.T) {
			intCache := New[int](2, WithBackend(backend), WithInvariantChecking(true))
			_, err := intCache.Put("zero", 0)
			require.NoError(t, err)
			vInt, ok := intCache.Get("zero")
			require.True(t, ok)
			assert.Equal(t, 0, vInt)

			sliceCache := New[[]byte](2, WithBackend(backend), WithInvariantChecking(true))
			_, err = sliceCache.Put("nil_slice", nil)
			require.NoError(t, err)
			_, err = sliceCache.Put("empty_slice", []byte{})
			require.NoError(t, err)
			vNilSlice, ok := sliceCache.Peek("nil_slice")
			require.True(t, ok)
			assert.Nil(t, vNilSlice)
			vEmptySlice, ok := sliceCache.Peek("empty_slice")
			require.True(t, ok)
			assert.NotNil(t, vEmptySlice)
			assert.Empty(t, vEmptySlice)

			ptrCache := New[*testData](2, WithBackend(backend), WithInvariantChecking(true))
			_, err = ptrCache.Put("nil_ptr", nil)
			require.NoError(t, err)
			vPtr, ok := ptrCache.Get("nil_ptr")
			require.True(t, ok)
			assert.Nil(t, vPtr)
			deletedPtr, ok := ptrCache.Delete("nil_ptr")
			require.True(t, ok)
			assert.Nil(t, deletedPtr)
		})
	}
}

func TestCache_CustomWeigher_AllBackends(t *testing.T) {
	for _, b := range allBackends[string]() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("KeyAndValueWeigherAndPutExceedingMaxSize", func(t *testing.T) {
				cache := b.fn(20, WithInvariantChecking(true), WithWeigher(func(k, v string) uint64 {
					return uint64(len(k) + len(v))
				}))

				// "k1" (2) + "12345678" (8) = 10
				ev, err := cache.Put("k1", "12345678")
				require.NoError(t, err)
				assert.Empty(t, ev)

				// Entry exceeding maxSize (20) returns ErrInvalidEntrySize without mutating state.
				ev, err = cache.Put("huge", strings.Repeat("z", 20))
				require.ErrorIs(t, err, ErrInvalidEntrySize)
				assert.Nil(t, ev)

				v1, ok := cache.Peek("k1")
				require.True(t, ok)
				assert.Equal(t, "12345678", v1)
			})

			t.Run("Replace_ShrinkGrowAndSelfEvict", func(t *testing.T) {
				cache := b.fn(50, WithInvariantChecking(true), WithWeigher(func(_ string, v string) uint64 {
					return uint64(len(v))
				}))

				// Put k1 (20B, LRU) and k2 (30B, MRU) -> total 50B.
				_, err := cache.Put("k1", strings.Repeat("a", 20))
				require.NoError(t, err)
				_, err = cache.Put("k2", strings.Repeat("b", 30))
				require.NoError(t, err)

				// 1. Shrink k1 from 20B to 5B -> total drops to 35B while k1 stays at LRU tail.
				require.NoError(t, cache.Replace("k1", strings.Repeat("a", 5)))

				// Put k3 (15B) -> fits in remaining 15B without evicting k1 or k2!
				ev, err := cache.Put("k3", strings.Repeat("c", 15))
				require.NoError(t, err)
				assert.Empty(t, ev)

				// 2. Transition k1 weight to 0 and back to 5B (verifying zeroSizeCount invariants).
				require.NoError(t, cache.Replace("k1", ""))
				require.NoError(t, cache.Replace("k1", strings.Repeat("a", 5)))

				// 3. Grow k3 (MRU) from 15B to 25B (total 5+30+25 = 60 > 50) -> evicts oldest k1 (5B) and k2 (30B).
				require.NoError(t, cache.Replace("k3", strings.Repeat("c", 25)))
				_, ok := cache.Peek("k1")
				assert.False(t, ok)
				_, ok = cache.Peek("k2")
				assert.False(t, ok)
				v3, ok := cache.Peek("k3")
				require.True(t, ok)
				assert.Len(t, v3, 25)

				// 4. Re-populate: k_old (10B, LRU), k_mid (15B, mid), k3 (25B, MRU).
				_, err = cache.Put("k_old", strings.Repeat("o", 10))
				require.NoError(t, err)
				_, err = cache.Put("k_mid", strings.Repeat("m", 15))
				require.NoError(t, err)
				// Promote k3 so order is k_old (10B, LRU) -> k_mid (15B) -> k3 (25B, MRU).
				_, _ = cache.Get("k3")

				// Grow k_mid from 15B to 30B: k_mid (30B) + newer k3 (25B) = 55B > 50B (!canFit).
				// k_mid must self-evict while both k_old and k3 survive!
				require.NoError(t, cache.Replace("k_mid", strings.Repeat("m", 30)))
				_, ok = cache.Peek("k_mid")
				assert.False(t, ok, "k_mid should self-evict when unable to fit alongside newer k3")
				_, ok = cache.Peek("k_old")
				assert.True(t, ok, "older k_old must not be evicted when k_mid self-evicts")
				_, ok = cache.Peek("k3")
				assert.True(t, ok, "newer k3 must survive")

				// 5. Grow k3 beyond maxSize (60B > 50B) -> k3 self-evicts while k_old survives.
				require.NoError(t, cache.Replace("k3", strings.Repeat("c", 60)))
				_, ok = cache.Peek("k3")
				assert.False(t, ok)
				_, ok = cache.Peek("k_old")
				assert.True(t, ok)
			})
		})
	}
}

func TestDefaultRuntimePressureFunc(t *testing.T) {
	// Arrange
	prevLimit := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prevLimit)

	// Act & Assert 1: Unbounded GOMEMLIMIT (math.MaxInt64) with MemoryBudget == 0 returns 0.0 in 0 allocs.
	debug.SetMemoryLimit(math.MaxInt64)
	probeUnbounded := DefaultRuntimePressureFunc(0)
	assert.InDelta(t, 0.0, probeUnbounded(), 1e-9)
	assert.InDelta(t, 0.0, testing.AllocsPerRun(50, func() {
		_ = probeUnbounded()
	}), 1e-9)

	// Act & Assert 2: Configured 1 GiB GOMEMLIMIT returns positive normalized pressure in (0.0, 1.0).
	const oneGiB = int64(1 << 30)
	debug.SetMemoryLimit(oneGiB)
	pLimit := probeUnbounded()
	assert.Greater(t, pLimit, 0.0)
	assert.Less(t, pLimit, 1.0)

	// Act & Assert 3: Custom MemoryBudget overrides GOMEMLIMIT and allocates 0 heap objects per call.
	probeBudget512MB := DefaultRuntimePressureFunc(512 << 20)
	probeBudget2GB := DefaultRuntimePressureFunc(2 << 30)
	p512 := probeBudget512MB()
	p2G := probeBudget2GB()
	require.Greater(t, p512, 0.0)
	require.Greater(t, p2G, 0.0)
	assert.Greater(t, p512, p2G)

	allocsPerRun := testing.AllocsPerRun(50, func() {
		_ = probeBudget512MB()
	})
	assert.InDelta(t, 0.0, allocsPerRun, 1e-9)

	// Act & Assert 4: Concurrent race-free reads across 16 goroutines.
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				assert.Greater(t, probeBudget512MB(), 0.0)
			}
		}()
	}
	wg.Wait()
}

func TestOptions_ThresholdReconciliationAndEdgeCases(t *testing.T) {
	subnormal2 := math.Float64frombits(2) // 2 * math.SmallestNonzeroFloat64
	compactionOneULPBelowEviction := math.Nextafter(DefaultEvictionThreshold, 0)
	compactionOneULPBelowDefault := math.Nextafter(DefaultCompactionThreshold, 0)

	tests := []struct {
		name           string
		opts           []Option
		wantCompaction float64
		wantEviction   float64
		exactBits      bool
		wantStrictLess bool
	}{
		{
			name:           "SmallestNonzeroFloat64SingleEvictionThreshold",
			opts:           []Option{WithEvictionThreshold(math.SmallestNonzeroFloat64)},
			wantCompaction: math.SmallestNonzeroFloat64,
			wantEviction:   math.SmallestNonzeroFloat64,
			exactBits:      true,
		},
		{
			name:           "SmallestNonzeroFloat64InvertedWithCompactionThreshold",
			opts:           []Option{WithCompactionThreshold(0.50), WithEvictionThreshold(math.SmallestNonzeroFloat64)},
			wantCompaction: math.SmallestNonzeroFloat64,
			wantEviction:   math.SmallestNonzeroFloat64,
			exactBits:      true,
		},
		{
			name:           "SmallestNonzeroFloat64CompactionThresholdOnly",
			opts:           []Option{WithCompactionThreshold(math.SmallestNonzeroFloat64)},
			wantCompaction: math.SmallestNonzeroFloat64,
			wantEviction:   DefaultEvictionThreshold,
			exactBits:      true,
		},
		{
			name:           "MaxFloat64CompactionThresholdClampsEvictionThresholdWithoutInf",
			opts:           []Option{WithCompactionThreshold(math.MaxFloat64)},
			wantCompaction: math.MaxFloat64,
			wantEviction:   math.MaxFloat64,
			exactBits:      true,
		},
		{
			name:           "SubnormalEvictionThresholdSingleOptionMaintainsStrictInequality",
			opts:           []Option{WithEvictionThreshold(subnormal2)},
			wantCompaction: math.SmallestNonzeroFloat64,
			wantEviction:   subnormal2,
			exactBits:      true,
			wantStrictLess: true,
		},
		{
			name:           "SubnormalEvictionThresholdInvertedWithCompactionMaintainsStrictInequality",
			opts:           []Option{WithCompactionThreshold(0.50), WithEvictionThreshold(subnormal2)},
			wantCompaction: math.SmallestNonzeroFloat64,
			wantEviction:   subnormal2,
			exactBits:      true,
			wantStrictLess: true,
		},
		{
			name:           "OneULPBelowDefaultEvictionThresholdPreservedBitForBit",
			opts:           []Option{WithCompactionThreshold(compactionOneULPBelowEviction)},
			wantCompaction: compactionOneULPBelowEviction,
			wantEviction:   DefaultEvictionThreshold,
			exactBits:      true,
			wantStrictLess: true,
		},
		{
			name:           "OneULPBelowDefaultCompactionThresholdPreservedBitForBit",
			opts:           []Option{WithCompactionThreshold(compactionOneULPBelowDefault)},
			wantCompaction: compactionOneULPBelowDefault,
			wantEviction:   DefaultEvictionThreshold,
			exactBits:      true,
			wantStrictLess: true,
		},
		{
			name:           "WithCompactionThreshold95AdvancesEvictionThresholdTo110",
			opts:           []Option{WithCompactionThreshold(0.95)},
			wantCompaction: 0.95,
			wantEviction:   1.10,
		},
		{
			name:           "WithCompactionThreshold100AdvancesEvictionThresholdTo115",
			opts:           []Option{WithCompactionThreshold(1.0)},
			wantCompaction: 1.0,
			wantEviction:   1.15,
		},
		{
			name:           "WithLowEvictionThreshold60ScalesCompactionBelow60",
			opts:           []Option{WithEvictionThreshold(0.60)},
			wantCompaction: 0.60 * (DefaultCompactionThreshold / DefaultEvictionThreshold),
			wantEviction:   0.60,
			wantStrictLess: true,
		},
		{
			name: "InvertedCompaction85AndEviction70ScalesCompactionBelow70",
			opts: []Option{
				WithCompactionThreshold(0.85),
				WithEvictionThreshold(0.70),
			},
			wantCompaction: 0.70 * (DefaultCompactionThreshold / DefaultEvictionThreshold),
			wantEviction:   0.70,
			wantStrictLess: true,
		},
		{
			name: "ExplicitEqualThresholdsAt90PreservesEquality",
			opts: []Option{
				WithCompactionThreshold(DefaultEvictionThreshold),
				WithEvictionThreshold(DefaultEvictionThreshold),
			},
			wantCompaction: 0.90,
			wantEviction:   0.90,
		},
		{
			name: "ExplicitEqualThresholdsAt75PreservesEquality",
			opts: []Option{
				WithCompactionThreshold(DefaultCompactionThreshold),
				WithEvictionThreshold(DefaultCompactionThreshold),
			},
			wantCompaction: 0.75,
			wantEviction:   0.75,
		},
		{
			name: "ResetCompactionThresholdToZeroBeforeEviction75ScalesDefaultCompaction",
			opts: []Option{
				WithCompactionThreshold(0.80),
				WithCompactionThreshold(0),
				WithEvictionThreshold(0.75),
			},
			wantCompaction: 0.625,
			wantEviction:   0.75,
		},
		{
			name:           "DirectStructCompactionMutationDoesNotPinAndScalesBackToDefault",
			opts:           []Option{func(o *Options) { o.CompactionThreshold = 0.95 }},
			wantCompaction: DefaultCompactionThreshold,
			wantEviction:   DefaultEvictionThreshold,
		},
		{
			name: "WithEvictionPinAndDirectCompactionStructMutationScalesBelowPinnedEviction",
			opts: []Option{
				WithEvictionThreshold(DefaultEvictionThreshold),
				func(o *Options) { o.CompactionThreshold = 0.95 },
			},
			wantCompaction: DefaultCompactionThreshold,
			wantEviction:   DefaultEvictionThreshold,
		},
		{
			name: "MultiOptionExplicitDefaultEvictionBeforeCompaction95ScalesToDefault",
			opts: []Option{
				WithEvictionThreshold(DefaultEvictionThreshold),
				WithCompactionThreshold(0.95),
			},
			wantCompaction: 0.75,
			wantEviction:   0.90,
		},
		{
			name: "MultiOptionExplicitDefaultEvictionAfterCompaction95ScalesToDefault",
			opts: []Option{
				WithCompactionThreshold(0.95),
				WithEvictionThreshold(DefaultEvictionThreshold),
			},
			wantCompaction: 0.75,
			wantEviction:   0.90,
		},
		{
			name: "MultiOptionEviction90OverriddenBy80WithCompaction95ScalesBelow80",
			opts: []Option{
				WithEvictionThreshold(DefaultEvictionThreshold),
				WithEvictionThreshold(0.80),
				WithCompactionThreshold(0.95),
			},
			wantCompaction: 0.80 * (DefaultCompactionThreshold / DefaultEvictionThreshold),
			wantEviction:   0.80,
		},
		{
			name: "MultiOptionEviction80OverriddenBy90WithCompaction95ScalesBelow90",
			opts: []Option{
				WithEvictionThreshold(0.80),
				WithEvictionThreshold(DefaultEvictionThreshold),
				WithCompactionThreshold(0.95),
			},
			wantCompaction: 0.75,
			wantEviction:   0.90,
		},
		{
			name: "MultiStepOptionToDefaultCompactionPreservesEqualityWithEviction75",
			opts: []Option{
				WithCompactionThreshold(0.60),
				WithEvictionThreshold(DefaultCompactionThreshold),
				WithCompactionThreshold(DefaultCompactionThreshold),
			},
			wantCompaction: 0.75,
			wantEviction:   0.75,
		},
		{
			name: "EqualThresholdsAfterEarlierEvictionResetPreservesEquality",
			opts: []Option{
				WithEvictionThreshold(0.80),
				WithEvictionThreshold(0),
				func(o *Options) {
					WithCompactionThreshold(0.90)(o)
					WithEvictionThreshold(0.90)(o)
				},
			},
			wantCompaction: 0.90,
			wantEviction:   0.90,
		},
		{
			name: "WithCompaction80FollowedByDirectEvictionMutation60ScalesCompactionBelow60",
			opts: []Option{
				WithCompactionThreshold(0.80),
				func(o *Options) { o.EvictionThreshold = 0.60 },
			},
			wantCompaction: 0.60 * (DefaultCompactionThreshold / DefaultEvictionThreshold),
			wantEviction:   0.60,
		},
		{
			name: "WithCompaction80FollowedByDirectCompactionMutation95ScalesBelowDefaultEviction",
			opts: []Option{
				WithCompactionThreshold(0.80),
				func(o *Options) { o.CompactionThreshold = 0.95 },
			},
			wantCompaction: DefaultCompactionThreshold,
			wantEviction:   DefaultEvictionThreshold,
		},
		{
			name: "WithEviction80FollowedByDirectEvictionMutationToDefaultAdvancesWithCompaction95",
			opts: []Option{
				WithEvictionThreshold(0.80),
				func(o *Options) { o.EvictionThreshold = DefaultEvictionThreshold },
				WithCompactionThreshold(0.95),
			},
			wantCompaction: 0.95,
			wantEviction:   1.10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ApplyOptions(tc.opts...)
			assert.Greater(t, got.CompactionThreshold, 0.0)
			assert.False(t, math.IsInf(got.EvictionThreshold, 0))
			if tc.exactBits {
				assert.Equal(t, math.Float64bits(tc.wantCompaction), math.Float64bits(got.CompactionThreshold))
				assert.Equal(t, math.Float64bits(tc.wantEviction), math.Float64bits(got.EvictionThreshold))
			} else {
				assert.InDelta(t, tc.wantCompaction, got.CompactionThreshold, 1e-9)
				assert.InDelta(t, tc.wantEviction, got.EvictionThreshold, 1e-9)
			}
			if tc.wantStrictLess {
				assert.Less(t, got.CompactionThreshold, got.EvictionThreshold)
			}
		})
	}

	t.Run("NegativeZeroRetentionRatioNormalizedToPositiveZero", func(t *testing.T) {
		optsNegZero := ApplyOptions(WithEvictionRetentionRatio(math.Copysign(0.0, -1.0)))
		assert.False(t, math.Signbit(optsNegZero.EvictionRetentionRatio))
		assert.Equal(t, uint64(0), math.Float64bits(optsNegZero.EvictionRetentionRatio))
	})
}

func TestOptions_CustomAndConditionalOptionClosures(t *testing.T) {
	t.Run("StatefulClosuresExecutedExactlyOnceAndObserveExactDefaults", func(t *testing.T) {
		// Arrange
		invocations := 0
		var observedCompaction, observedEviction float64
		statefulOpt := func(o *Options) {
			invocations++
			observedCompaction = o.CompactionThreshold
			observedEviction = o.EvictionThreshold
			WithCompactionThreshold(o.CompactionThreshold + 0.20)(o)
		}

		// Act
		got := ApplyOptions(statefulOpt)

		// Assert
		assert.Equal(t, 1, invocations)
		assert.Equal(t, math.Float64bits(DefaultCompactionThreshold), math.Float64bits(observedCompaction))
		assert.Equal(t, math.Float64bits(DefaultEvictionThreshold), math.Float64bits(observedEviction))
		assert.InDelta(t, 0.95, got.CompactionThreshold, 1e-9)
		assert.InDelta(t, 1.10, got.EvictionThreshold, 1e-9)
	})

	t.Run("RelativeOptionSettingCompactionEqualToEvictionPreservesEquality", func(t *testing.T) {
		// Arrange
		matchEvictionOpt := func(o *Options) {
			WithEvictionThreshold(DefaultEvictionThreshold)(o)
			WithCompactionThreshold(o.EvictionThreshold)(o)
		}

		// Act
		got := ApplyOptions(matchEvictionOpt)

		// Assert
		assert.InDelta(t, 0.90, got.CompactionThreshold, 1e-9)
		assert.InDelta(t, 0.90, got.EvictionThreshold, 1e-9)
	})

	t.Run("TakenAndUntakenConditionalBranchesPinOnlyWhenExecuted", func(t *testing.T) {
		// Arrange
		conditionalPinEviction := func(o *Options) {
			if o.EnableInvariantChecking {
				WithEvictionThreshold(DefaultEvictionThreshold)(o)
			}
		}
		combinedUntakenBranch := func(o *Options) {
			WithCompactionThreshold(0.95)(o)
			if o.MemoryBudget > 0 {
				WithEvictionThreshold(DefaultEvictionThreshold)(o)
			}
		}

		// Act
		gotUntaken := ApplyOptions(conditionalPinEviction, WithCompactionThreshold(0.95))
		gotTaken := ApplyOptions(WithInvariantChecking(true), conditionalPinEviction, WithCompactionThreshold(0.95))
		gotCombinedUntaken := ApplyOptions(combinedUntakenBranch)

		// Assert
		assert.InDelta(t, 0.95, gotUntaken.CompactionThreshold, 1e-9)
		assert.InDelta(t, 1.10, gotUntaken.EvictionThreshold, 1e-9)
		assert.InDelta(t, 0.75, gotTaken.CompactionThreshold, 1e-9)
		assert.InDelta(t, 0.90, gotTaken.EvictionThreshold, 1e-9)
		assert.InDelta(t, 0.95, gotCombinedUntaken.CompactionThreshold, 1e-9)
		assert.InDelta(t, 1.10, gotCombinedUntaken.EvictionThreshold, 1e-9)
	})
}

func TestGenericCache_ScalarValueTypesAndCompactNodeLayout(t *testing.T) {
	testGenericCacheWithScalar(t, byte(1), byte(2))
	testGenericCacheWithScalar(t, uint16(1), uint16(2))
	testGenericCacheWithScalar(t, uint32(1), uint32(2))

	if unsafe.Sizeof(uintptr(0)) == 8 {
		assert.Equal(t, uintptr(72), unsafe.Sizeof(radixNode[byte]{}))
		assert.Equal(t, uintptr(72), unsafe.Sizeof(radixNode[uint16]{}))
		assert.Equal(t, uintptr(72), unsafe.Sizeof(radixNode[uint32]{}))
		assert.Equal(t, uintptr(80), unsafe.Sizeof(radixNode[uint64]{}))
		assert.Equal(t, uintptr(48), unsafe.Sizeof(arenaRadixNode[byte]{}))
		assert.Equal(t, uintptr(48), unsafe.Sizeof(arenaRadixNode[uint16]{}))
		assert.Equal(t, uintptr(56), unsafe.Sizeof(arenaRadixNode[uint32]{}))
		assert.Equal(t, uintptr(56), unsafe.Sizeof(arenaRadixNode[uint64]{}))
		assert.Equal(t, unsafe.Sizeof(radixCache[byte]{}), unsafe.Sizeof(radixCache[[1024]byte]{}))
	}
}

func testGenericCacheWithScalar[V any](t *testing.T, val1, val2 V) {
	t.Helper()
	backends := []Backend{BackendMap, BackendRadix, BackendArenaRadix}
	for _, backend := range backends {
		t.Run(fmt.Sprintf("%T/%s", val1, backend), func(t *testing.T) {
			c1 := New[V](10, WithBackend(backend), WithInvariantChecking(true))
			exerciseScalarCache(t, c1, val1, val2)

			c2 := New[V](10, WithBackend(backend), WithInvariantChecking(true), WithWeigher(func(_ string, _ V) uint64 {
				return 1
			}))
			exerciseScalarCache(t, c2, val1, val2)
		})
	}
}

func exerciseScalarCache[V any](t *testing.T, c Cache[V], val1, val2 V) {
	t.Helper()
	evicted, err := c.Put("key1", val1)
	require.NoError(t, err)
	assert.Empty(t, evicted)

	evicted, err = c.Put("key2", val2)
	require.NoError(t, err)
	assert.Empty(t, evicted)

	v, ok := c.Get("key1")
	require.True(t, ok)
	assert.Equal(t, val1, v)

	v, ok = c.Peek("key2")
	require.True(t, ok)
	assert.Equal(t, val2, v)

	require.NoError(t, c.Replace("key1", val2))

	v, ok = c.Get("key1")
	require.True(t, ok)
	assert.Equal(t, val2, v)

	v, ok = c.Delete("key1")
	require.True(t, ok)
	assert.Equal(t, val2, v)

	_, ok = c.Get("key1")
	assert.False(t, ok)

	_, err = c.Put("prefix/1", val1)
	require.NoError(t, err)
	_, err = c.Put("prefix/2", val2)
	require.NoError(t, err)

	c.DeletePrefix("prefix/")

	_, ok = c.Get("prefix/1")
	assert.False(t, ok)
}

type recordedEvictValue struct {
	val    string
	reason EvictionReason
}

type recordedEvictEntry struct {
	key    string
	val    string
	reason EvictionReason
}

func TestCache_OnEvictCallbacks_AllBackends(t *testing.T) {
	type callbackMode struct {
		name     string
		useValue bool
		useEntry bool
	}
	modes := []callbackMode{
		{name: "ValueOnly", useValue: true, useEntry: false},
		{name: "EntryOnly", useValue: false, useEntry: true},
		{name: "BothValueAndEntry", useValue: true, useEntry: true},
	}

	for _, b := range allBackends[string]() {
		for _, mode := range modes {
			t.Run(fmt.Sprintf("%s/%s", b.name, mode.name), func(t *testing.T) {
				// Arrange
				var callOrder []string
				var valEvents []recordedEvictValue
				var entryEvents []recordedEvictEntry

				resetEvents := func() {
					callOrder = nil
					valEvents = nil
					entryEvents = nil
				}

				assertRecorded := func(expectedEntries []recordedEvictEntry, multiset bool) {
					t.Helper()
					if len(expectedEntries) == 0 {
						assert.Empty(t, valEvents)
						assert.Empty(t, entryEvents)
						assert.Empty(t, callOrder)
						return
					}
					expectedVals := make([]recordedEvictValue, len(expectedEntries))
					for i, e := range expectedEntries {
						expectedVals[i] = recordedEvictValue{val: e.val, reason: e.reason}
					}
					if mode.useValue {
						if multiset {
							assert.ElementsMatch(t, expectedVals, valEvents)
						} else {
							assert.Equal(t, expectedVals, valEvents)
						}
					} else {
						assert.Empty(t, valEvents)
					}
					if mode.useEntry {
						if multiset {
							assert.ElementsMatch(t, expectedEntries, entryEvents)
						} else {
							assert.Equal(t, expectedEntries, entryEvents)
						}
					} else {
						assert.Empty(t, entryEvents)
					}
					if mode.useValue && mode.useEntry {
						require.Len(t, callOrder, 2*len(expectedEntries))
						for i := 0; i < len(expectedEntries); i++ {
							assert.Equal(t, "value", callOrder[2*i], "OnEvictValue must be invoked before OnEvictEntry")
							assert.Equal(t, "entry", callOrder[2*i+1], "OnEvictEntry must be invoked immediately after OnEvictValue")
						}
					}
				}

				byteWeigher := WithWeigher(func(k, v string) uint64 {
					return uint64(len(k) + len(v))
				})
				buildOpts := func(extra ...Option) []Option {
					var opts []Option
					opts = append(opts, WithInvariantChecking(true))
					if mode.useValue {
						opts = append(opts, WithOnEvictValue(func(v string, r EvictionReason) {
							callOrder = append(callOrder, "value")
							valEvents = append(valEvents, recordedEvictValue{val: v, reason: r})
						}))
					}
					if mode.useEntry {
						opts = append(opts, WithOnEvictEntry(func(k, v string, r EvictionReason) {
							callOrder = append(callOrder, "entry")
							entryEvents = append(entryEvents, recordedEvictEntry{key: k, val: v, reason: r})
						}))
					}
					opts = append(opts, extra...)
					return opts
				}

				// 1. Put within capacity, capacity eviction, overwrite without eviction, overwrite with capacity eviction, ErrInvalidEntrySize
				c := b.fn(12, buildOpts(byteWeigher)...)

				// Act & Assert: Put within capacity (maxSize=12: "k1"+"v1"=4, "k2"+"v2"=4, "k3"+"v3"=4 -> total 12)
				evicted, err := c.Put("k1", "v1")
				require.NoError(t, err)
				assert.Empty(t, evicted)
				evicted, err = c.Put("k2", "v2")
				require.NoError(t, err)
				assert.Empty(t, evicted)
				evicted, err = c.Put("k3", "v3")
				require.NoError(t, err)
				assert.Empty(t, evicted)
				assertRecorded(nil, false)

				// Promote "k1" so LRU order from MRU to LRU is: k1, k3, k2
				val, ok := c.Get("k1")
				require.True(t, ok)
				assert.Equal(t, "v1", val)

				// Put "k4" ("k4"+"val456"=8) -> must evict "k2" (4) then "k3" (4) with EvictionReasonCapacity
				resetEvents()
				evicted, err = c.Put("k4", "val456")
				require.NoError(t, err)
				assert.Equal(t, []string{"v2", "v3"}, evicted)
				assertRecorded([]recordedEvictEntry{
					{key: "k2", val: "v2", reason: EvictionReasonCapacity},
					{key: "k3", val: "v3", reason: EvictionReasonCapacity},
				}, false)

				// Put overwrite "k1" with same size ("k1"+"v1" -> "k1"+"v9") -> EvictionReasonReplaced only
				resetEvents()
				evicted, err = c.Put("k1", "v9")
				require.NoError(t, err)
				assert.Empty(t, evicted)
				assertRecorded([]recordedEvictEntry{
					{key: "k1", val: "v1", reason: EvictionReasonReplaced},
				}, false)

				// Put overwrite "k1" with larger value ("k1"+"v9" (4) -> "k1"+"longval8" (10)), requiring eviction of "k4" ("val456", 8) first
				resetEvents()
				evicted, err = c.Put("k1", "longval8")
				require.NoError(t, err)
				assert.Equal(t, []string{"val456"}, evicted)
				assertRecorded([]recordedEvictEntry{
					{key: "k4", val: "val456", reason: EvictionReasonCapacity},
					{key: "k1", val: "v9", reason: EvictionReasonReplaced},
				}, false)

				// Put failing with ErrInvalidEntrySize (both new key and existing key "k1") -> no callbacks fired, "k1" preserved
				resetEvents()
				evicted, err = c.Put("huge", "this-value-exceeds-max-size")
				require.ErrorIs(t, err, ErrInvalidEntrySize)
				assert.Nil(t, evicted)
				evicted, err = c.Put("k1", "this-value-exceeds-max-size")
				require.ErrorIs(t, err, ErrInvalidEntrySize)
				assert.Nil(t, evicted)
				assertRecorded(nil, false)
				val, ok = c.Peek("k1")
				require.True(t, ok)
				assert.Equal(t, "longval8", val)

				// 2. Replace: non-existent key, shrink/same, grow with capacity eviction, and self-eviction (> maxSize and !canFit)
				resetEvents()
				err = c.Replace("missing", "v")
				require.ErrorIs(t, err, ErrEntryNotExist)
				assertRecorded(nil, false)

				// Shrink "k1" from "longval8" (10) to "v1" (4)
				resetEvents()
				require.NoError(t, c.Replace("k1", "v1"))
				assertRecorded([]recordedEvictEntry{
					{key: "k1", val: "longval8", reason: EvictionReasonReplaced},
				}, false)

				// Add "k2":"v2" (4) and "k3":"v3" (4) -> cache has k3(4, MRU), k2(4, mid), k1(4, LRU) = 12
				_, err = c.Put("k2", "v2")
				require.NoError(t, err)
				_, err = c.Put("k3", "v3")
				require.NoError(t, err)

				// Replace "k3" with "longval8" (10) -> evicts older LRU "k1" (4) and "k2" (4) with Capacity, then replaces "k3" ("v3") with Replaced
				resetEvents()
				require.NoError(t, c.Replace("k3", "longval8"))
				assertRecorded([]recordedEvictEntry{
					{key: "k1", val: "v1", reason: EvictionReasonCapacity},
					{key: "k2", val: "v2", reason: EvictionReasonCapacity},
					{key: "k3", val: "v3", reason: EvictionReasonReplaced},
				}, false)

				// Replace "k3" with oversized value (> maxSize) -> self-evicts "k3" with EvictionReasonCapacity and returns nil
				resetEvents()
				require.NoError(t, c.Replace("k3", "this-value-exceeds-max-size"))
				assertRecorded([]recordedEvictEntry{
					{key: "k3", val: "longval8", reason: EvictionReasonCapacity},
				}, false)
				_, ok = c.Get("k3")
				assert.False(t, ok)

				// Replace when !canFit: populate k1(4, LRU), k2(4, mid), k3(4, MRU) = 12, then grow k2 to 10B ("k2"+"longval8")
				// Since k2 (10B) + newer k3 (4B) = 14B > 12B, k2 cannot fit by evicting only older entries (k1), so k2 self-evicts with EvictionReasonCapacity!
				_, err = c.Put("k1", "v1")
				require.NoError(t, err)
				_, err = c.Put("k2", "v2")
				require.NoError(t, err)
				_, err = c.Put("k3", "v3")
				require.NoError(t, err)
				resetEvents()
				require.NoError(t, c.Replace("k2", "longval8"))
				assertRecorded([]recordedEvictEntry{
					{key: "k2", val: "v2", reason: EvictionReasonCapacity},
				}, false)
				assert.True(t, hasKey(c, "k1"), "older k1 must survive when k2 self-evicts")
				assert.False(t, hasKey(c, "k2"))
				assert.True(t, hasKey(c, "k3"), "newer k3 must survive when k2 self-evicts")

				// 3. Delete (miss, root empty key "", and deep multi-segment keys) & 4. DeletePrefix
				treeCache := b.fn(1024, buildOpts()...)
				_, err = treeCache.Put("", "rootval")
				require.NoError(t, err)
				_, err = treeCache.Put("app/service/v1/users", "u1")
				require.NoError(t, err)
				_, err = treeCache.Put("app/service/v1/users/profile", "u2")
				require.NoError(t, err)
				_, err = treeCache.Put("app/service/v2/orders", "o1")
				require.NoError(t, err)
				_, err = treeCache.Put("other/key", "ok1")
				require.NoError(t, err)

				// Delete miss
				resetEvents()
				_, ok = treeCache.Delete("app/service/v1/nonexistent")
				assert.False(t, ok)
				assertRecorded(nil, false)

				// Delete empty root key ""
				resetEvents()
				delVal, ok := treeCache.Delete("")
				require.True(t, ok)
				assert.Equal(t, "rootval", delVal)
				assertRecorded([]recordedEvictEntry{
					{key: "", val: "rootval", reason: EvictionReasonDeleted},
				}, false)

				// DeletePrefix miss
				resetEvents()
				treeCache.DeletePrefix("missing/")
				assertRecorded(nil, false)

				// DeletePrefix matching partial edge / subtree ("app/service/v1/")
				resetEvents()
				treeCache.DeletePrefix("app/service/v1/")
				assertRecorded([]recordedEvictEntry{
					{key: "app/service/v1/users", val: "u1", reason: EvictionReasonDeleted},
					{key: "app/service/v1/users/profile", val: "u2", reason: EvictionReasonDeleted},
				}, true)

				// DeletePrefix("") clearing all remaining entries
				resetEvents()
				treeCache.DeletePrefix("")
				assertRecorded([]recordedEvictEntry{
					{key: "app/service/v2/orders", val: "o1", reason: EvictionReasonDeleted},
					{key: "other/key", val: "ok1", reason: EvictionReasonDeleted},
				}, true)

				// DeletePrefix("") on already-empty cache
				resetEvents()
				treeCache.DeletePrefix("")
				assertRecorded(nil, false)
			})
		}
	}
}

func TestCache_OnEvictDeepKeyReconstruction_ExceedsStackBuffer(t *testing.T) {
	for _, b := range allBackends[string]() {
		t.Run(b.name, func(t *testing.T) {
			// Arrange: create a chain of 70 prefix keys ("a", "aa", ..., 70 'a's) so radix depth > 64 stack buffer
			const depth = 70
			var evictedEntries []recordedEvictEntry
			c := b.fn(uint64(depth),
				WithInvariantChecking(true),
				WithWeigher(func(_ string, _ string) uint64 { return 1 }),
				WithOnEvictEntry(func(k, v string, r EvictionReason) {
					evictedEntries = append(evictedEntries, recordedEvictEntry{
						key:    k,
						val:    v,
						reason: r,
					})
				}),
			)

			for i := 1; i <= depth; i++ {
				key := strings.Repeat("a", i)
				_, err := c.Put(key, strings.Repeat("v", i))
				require.NoError(t, err)
			}

			// Promote keys 1..depth-1 so the deepest key (depth=70) becomes the LRU tail
			for i := 1; i < depth; i++ {
				_, ok := c.Get(strings.Repeat("a", i))
				require.True(t, ok)
			}

			// Act: insert one more key to evict the depth-70 key via EvictionReasonCapacity
			evictedEntries = nil
			_, err := c.Put("z", "vz")
			require.NoError(t, err)

			// Assert
			require.Len(t, evictedEntries, 1)
			assert.Equal(t, strings.Repeat("a", depth), evictedEntries[0].key)
			assert.Equal(t, strings.Repeat("v", depth), evictedEntries[0].val)
			assert.Equal(t, EvictionReasonCapacity, evictedEntries[0].reason)

			// Act: DeletePrefix("a") to exercise deep key reconstruction across all remaining 69 levels
			evictedEntries = nil
			c.DeletePrefix("a")
			require.Len(t, evictedEntries, depth-1)
		})
	}
}

func TestCache_OnEvictZeroKeyReconstructionAllocs(t *testing.T) {
	// 1. Steady-state Replace (caller already has key in hand) with WithOnEvictEntry across all 3 backends -> 0 allocs
	for _, b := range allBackends[string]() {
		t.Run(b.name+"/ReplaceWithOnEvictEntry", func(t *testing.T) {
			// Arrange
			var sinkKey, sinkVal string
			var sinkReason EvictionReason
			c := b.fn(1024,
				WithOnEvictEntry(func(k, v string, r EvictionReason) {
					sinkKey = k
					sinkVal = v
					sinkReason = r
				}),
			)
			_, err := c.Put("alpha/beta/gamma/delta", "val1")
			require.NoError(t, err)

			// Act
			allocs := testing.AllocsPerRun(100, func() {
				_ = c.Replace("alpha/beta/gamma/delta", "val2")
			})

			// Assert
			assert.Zero(t, allocs, "Replace with WithOnEvictEntry must not allocate to reconstruct key")
			assert.Equal(t, "alpha/beta/gamma/delta", sinkKey)
			assert.Equal(t, "val2", sinkVal)
			assert.Equal(t, EvictionReasonReplaced, sinkReason)
		})

		t.Run(b.name+"/PutOverwriteWithOnEvictEntry", func(t *testing.T) {
			// Arrange
			var sinkKey, sinkVal string
			var sinkReason EvictionReason
			c := b.fn(1024,
				WithOnEvictEntry(func(k, v string, r EvictionReason) {
					sinkKey = k
					sinkVal = v
					sinkReason = r
				}),
			)
			_, err := c.Put("alpha/beta/gamma/delta", "val1")
			require.NoError(t, err)

			// Act
			allocs := testing.AllocsPerRun(100, func() {
				_, _ = c.Put("alpha/beta/gamma/delta", "val2")
			})

			// Assert
			assert.Zero(t, allocs, "Put overwrite with WithOnEvictEntry must not allocate to reconstruct key")
			assert.Equal(t, "alpha/beta/gamma/delta", sinkKey)
			assert.Equal(t, "val2", sinkVal)
			assert.Equal(t, EvictionReasonReplaced, sinkReason)
		})
	}

	// 2. Capacity eviction on RadixCache and ArenaRadixCache with WithOnEvictValue only adds 0 key-reconstruction allocations compared to baseline
	for _, b := range allBackends[int]() {
		t.Run(b.name+"/CapacityEvictWithOnEvictValueAddsZeroAllocs", func(t *testing.T) {
			// Arrange
			var sinkVal int
			var sinkReason EvictionReason
			runTurnover := func(opts ...Option) float64 {
				allOpts := append([]Option{WithWeigher(func(_ string, _ int) uint64 { return 1 })}, opts...)
				c := b.fn(2, allOpts...)
				_, _ = c.Put("prefix/sub/pin", 1)
				_, _ = c.Put("prefix/sub/a", 2)
				toggle := false
				return testing.AllocsPerRun(100, func() {
					_, _ = c.Get("prefix/sub/pin")
					if toggle {
						_, _ = c.Put("prefix/sub/a", 2)
					} else {
						_, _ = c.Put("prefix/sub/b", 3)
					}
					toggle = !toggle
				})
			}

			// Act
			baselineAllocs := runTurnover()
			valueCbAllocs := runTurnover(WithOnEvictValue(func(v int, r EvictionReason) {
				sinkVal = v
				sinkReason = r
			}))

			// Assert
			assert.InDelta(t, baselineAllocs, valueCbAllocs, 1e-9, "WithOnEvictValue must add zero key-reconstruction allocations over baseline")
			assert.NotZero(t, sinkVal)
			assert.Equal(t, EvictionReasonCapacity, sinkReason)
		})
	}
}
