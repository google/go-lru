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

func TestOptions_Default(t *testing.T) {
	// Arrange & Act
	opts := ApplyOptions()

	// Assert
	assert.False(t, opts.EnableInvariantChecking)
	assert.Nil(t, opts.Weigher)
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
	_, err := c.Insert("k", "v")
	require.NoError(t, err)
	assert.Greater(t, customCalls, callsBefore)
}

func TestCache_DefaultUnitWeigher_AllBackends(t *testing.T) {
	for _, b := range allBackends[string]() {
		t.Run(b.name, func(t *testing.T) {
			// Default weigher assigns weight 1 to every entry regardless of string length.
			cache := b.fn(3, WithInvariantChecking(true))

			ev, err := cache.Insert("k1", "a")
			require.NoError(t, err)
			assert.Empty(t, ev)

			ev, err = cache.Insert("k2", "bb")
			require.NoError(t, err)
			assert.Empty(t, ev)

			ev, err = cache.Insert("k3", strings.Repeat("c", 4096))
			require.NoError(t, err)
			assert.Empty(t, ev)

			// Promote k1; k2 becomes LRU tail.
			v1, ok := cache.LookUp("k1")
			require.True(t, ok)
			assert.Equal(t, "a", v1)

			// Update k1 in place to 10KB string; under default weigher its weight stays 1.
			require.NoError(t, cache.UpdateWithoutChangingOrder("k1", strings.Repeat("x", 10240)))

			// Inserting 4th entry evicts k2 ("bb").
			ev, err = cache.Insert("k4", "dddd")
			require.NoError(t, err)
			assert.Equal(t, []string{"bb"}, ev)

			_, ok = cache.LookUpWithoutChangingOrder("k2")
			assert.False(t, ok)

			v1After, ok := cache.LookUpWithoutChangingOrder("k1")
			require.True(t, ok)
			assert.Equal(t, strings.Repeat("x", 10240), v1After)
		})
	}

	// Verify arbitrary value types (primitives, slices including nil, pointers including nil) under default weigher.
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"_ArbitraryTypesAndNilValues", func(t *testing.T) {
			intCache := New[int](2, WithBackend(backend), WithInvariantChecking(true))
			_, err := intCache.Insert("zero", 0)
			require.NoError(t, err)
			vInt, ok := intCache.LookUp("zero")
			require.True(t, ok)
			assert.Equal(t, 0, vInt)

			sliceCache := New[[]byte](2, WithBackend(backend), WithInvariantChecking(true))
			_, err = sliceCache.Insert("nil_slice", nil)
			require.NoError(t, err)
			_, err = sliceCache.Insert("empty_slice", []byte{})
			require.NoError(t, err)
			vNilSlice, ok := sliceCache.LookUpWithoutChangingOrder("nil_slice")
			require.True(t, ok)
			assert.Nil(t, vNilSlice)
			vEmptySlice, ok := sliceCache.LookUpWithoutChangingOrder("empty_slice")
			require.True(t, ok)
			assert.NotNil(t, vEmptySlice)
			assert.Empty(t, vEmptySlice)

			ptrCache := New[*testData](2, WithBackend(backend), WithInvariantChecking(true))
			_, err = ptrCache.Insert("nil_ptr", nil)
			require.NoError(t, err)
			vPtr, ok := ptrCache.LookUp("nil_ptr")
			require.True(t, ok)
			assert.Nil(t, vPtr)
			erasedPtr, ok := ptrCache.Erase("nil_ptr")
			require.True(t, ok)
			assert.Nil(t, erasedPtr)
		})
	}
}

func TestCache_CustomWeigher_AllBackends(t *testing.T) {
	for _, b := range allBackends[string]() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("KeyAndValueWeigherAndInsertExceedingMaxSize", func(t *testing.T) {
				cache := b.fn(20, WithInvariantChecking(true), WithWeigher(func(k, v string) uint64 {
					return uint64(len(k) + len(v))
				}))

				// "k1" (2) + "12345678" (8) = 10
				ev, err := cache.Insert("k1", "12345678")
				require.NoError(t, err)
				assert.Empty(t, ev)

				// Entry exceeding maxSize (20) returns ErrInvalidEntrySize without mutating state.
				ev, err = cache.Insert("huge", strings.Repeat("z", 20))
				require.ErrorIs(t, err, ErrInvalidEntrySize)
				assert.Nil(t, ev)

				v1, ok := cache.LookUpWithoutChangingOrder("k1")
				require.True(t, ok)
				assert.Equal(t, "12345678", v1)
			})

			t.Run("UpdateWithoutChangingOrder_ShrinkGrowAndSelfEvict", func(t *testing.T) {
				cache := b.fn(50, WithInvariantChecking(true), WithWeigher(func(_ string, v string) uint64 {
					return uint64(len(v))
				}))

				// Insert k1 (20B, LRU) and k2 (30B, MRU) -> total 50B.
				_, err := cache.Insert("k1", strings.Repeat("a", 20))
				require.NoError(t, err)
				_, err = cache.Insert("k2", strings.Repeat("b", 30))
				require.NoError(t, err)

				// 1. Shrink k1 from 20B to 5B -> total drops to 35B while k1 stays at LRU tail.
				require.NoError(t, cache.UpdateWithoutChangingOrder("k1", strings.Repeat("a", 5)))

				// Insert k3 (15B) -> fits in remaining 15B without evicting k1 or k2!
				ev, err := cache.Insert("k3", strings.Repeat("c", 15))
				require.NoError(t, err)
				assert.Empty(t, ev)

				// 2. Transition k1 weight to 0 and back to 5B (verifying zeroSizeCount invariants).
				require.NoError(t, cache.UpdateWithoutChangingOrder("k1", ""))
				require.NoError(t, cache.UpdateWithoutChangingOrder("k1", strings.Repeat("a", 5)))

				// 3. Grow k3 (MRU) from 15B to 25B (total 5+30+25 = 60 > 50) -> evicts oldest k1 (5B) and k2 (30B).
				require.NoError(t, cache.UpdateWithoutChangingOrder("k3", strings.Repeat("c", 25)))
				_, ok := cache.LookUpWithoutChangingOrder("k1")
				assert.False(t, ok)
				_, ok = cache.LookUpWithoutChangingOrder("k2")
				assert.False(t, ok)
				v3, ok := cache.LookUpWithoutChangingOrder("k3")
				require.True(t, ok)
				assert.Len(t, v3, 25)

				// 4. Re-populate: k_old (10B, LRU), k_mid (15B, mid), k3 (25B, MRU).
				_, err = cache.Insert("k_old", strings.Repeat("o", 10))
				require.NoError(t, err)
				_, err = cache.Insert("k_mid", strings.Repeat("m", 15))
				require.NoError(t, err)
				// Promote k3 so order is k_old (10B, LRU) -> k_mid (15B) -> k3 (25B, MRU).
				_, _ = cache.LookUp("k3")

				// Grow k_mid from 15B to 30B: k_mid (30B) + newer k3 (25B) = 55B > 50B (!canFit).
				// k_mid must self-evict while both k_old and k3 survive!
				require.NoError(t, cache.UpdateWithoutChangingOrder("k_mid", strings.Repeat("m", 30)))
				_, ok = cache.LookUpWithoutChangingOrder("k_mid")
				assert.False(t, ok, "k_mid should self-evict when unable to fit alongside newer k3")
				_, ok = cache.LookUpWithoutChangingOrder("k_old")
				assert.True(t, ok, "older k_old must not be evicted when k_mid self-evicts")
				_, ok = cache.LookUpWithoutChangingOrder("k3")
				assert.True(t, ok, "newer k3 must survive")

				// 5. Grow k3 beyond maxSize (60B > 50B) -> k3 self-evicts while k_old survives.
				require.NoError(t, cache.UpdateWithoutChangingOrder("k3", strings.Repeat("c", 60)))
				_, ok = cache.LookUpWithoutChangingOrder("k3")
				assert.False(t, ok)
				_, ok = cache.LookUpWithoutChangingOrder("k_old")
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
	evicted, err := c.Insert("key1", val1)
	require.NoError(t, err)
	assert.Empty(t, evicted)

	evicted, err = c.Insert("key2", val2)
	require.NoError(t, err)
	assert.Empty(t, evicted)

	v, ok := c.LookUp("key1")
	require.True(t, ok)
	assert.Equal(t, val1, v)

	v, ok = c.LookUpWithoutChangingOrder("key2")
	require.True(t, ok)
	assert.Equal(t, val2, v)

	require.NoError(t, c.UpdateWithoutChangingOrder("key1", val2))

	v, ok = c.LookUp("key1")
	require.True(t, ok)
	assert.Equal(t, val2, v)

	v, ok = c.Erase("key1")
	require.True(t, ok)
	assert.Equal(t, val2, v)

	_, ok = c.LookUp("key1")
	assert.False(t, ok)

	_, err = c.Insert("prefix/1", val1)
	require.NoError(t, err)
	_, err = c.Insert("prefix/2", val2)
	require.NoError(t, err)

	c.EraseEntriesWithGivenPrefix("prefix/")

	_, ok = c.LookUp("prefix/1")
	assert.False(t, ok)
}
