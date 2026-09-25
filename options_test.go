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
	"math"
	"runtime/debug"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testValue struct {
	size uint64
}

func (v testValue) Size() uint64 {
	return v.size
}

func TestValueTypeInterface(t *testing.T) {
	// Arrange
	var val ValueType = testValue{size: 42}

	// Act
	size := val.Size()

	// Assert
	assert.Equal(t, uint64(42), size)
}

func TestOptions_Default(t *testing.T) {
	// Arrange & Act
	opts := ApplyOptions()

	// Assert
	assert.False(t, opts.EnableInvariantChecking)
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
			err:      ErrInvalidEntry,
			expected: "nil values are not supported",
		},
		{
			err:      ErrInvalidUpdateEntrySize,
			expected: "size of entry to be updated is not same as existing size",
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
	constructors := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"NewMapCache", NewMapCache},
		{"New", New},
		{"NewRadixCache", NewRadixCache},
		{"NewArenaRadixCache", NewArenaRadixCache},
	}

	// Act & Assert
	for _, c := range constructors {
		t.Run(c.name+"/ZeroMaxSize_DefaultOptions", func(t *testing.T) {
			// Arrange, Act & Assert
			assert.Panics(t, func() {
				_ = c.fn(0)
			})
		})

		t.Run(c.name+"/ZeroMaxSize_InvariantsDisabled", func(t *testing.T) {
			// Arrange, Act & Assert
			assert.Panics(t, func() {
				_ = c.fn(0, WithInvariantChecking(false))
			})
		})

		t.Run(c.name+"/ZeroMaxSize_InvariantsEnabled", func(t *testing.T) {
			// Arrange, Act & Assert
			assert.Panics(t, func() {
				_ = c.fn(0, WithInvariantChecking(true))
			})
		})

		t.Run(c.name+"/PositiveBoundary_MaxSize1", func(t *testing.T) {
			// Arrange & Act
			cache := c.fn(1)

			// Assert
			require.NotNil(t, cache)
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
	assert.False(t, opts.hasCustomPressureFunc)
}

func TestOptions_MemoryPressureCustomAndValidation(t *testing.T) {
	// Arrange
	customFn := func() float64 { return 0.88 }

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
	optsInverted := ApplyOptions(
		WithCompactionThreshold(0.85),
		WithEvictionThreshold(0.70),
	)
	optsNaNAndInf := ApplyOptions(
		WithCompactionThreshold(math.NaN()),
		WithEvictionThreshold(math.Inf(1)),
		WithEvictionRetentionRatio(math.NaN()),
	)
	optsHighCompactionOnly := ApplyOptions(WithCompactionThreshold(0.92))
	optsCompaction100 := ApplyOptions(WithCompactionThreshold(1.0))
	optsLowEvictionOnly := ApplyOptions(WithEvictionThreshold(0.60))
	optsDirectStructPressure := ApplyOptions(func(o *Options) {
		o.PressureFunc = customFn
	})

	// Assert
	assert.Equal(t, uint64(256*1024*1024), opts.MemoryBudget)
	assert.InDelta(t, 0.60, opts.CompactionThreshold, 1e-9)
	assert.InDelta(t, 0.85, opts.EvictionThreshold, 1e-9)
	assert.InDelta(t, 0.35, opts.EvictionRetentionRatio, 1e-9)
	require.NotNil(t, opts.PressureFunc)
	assert.True(t, opts.hasCustomPressureFunc)
	assert.InDelta(t, 0.88, opts.PressureFunc(), 1e-9)

	assert.InDelta(t, 0.0, optsZeroRetention.EvictionRetentionRatio, 1e-9)

	assert.InDelta(t, DefaultCompactionThreshold, optsClamped.CompactionThreshold, 1e-9)
	assert.InDelta(t, DefaultEvictionThreshold, optsClamped.EvictionThreshold, 1e-9)
	assert.InDelta(t, 1.0, optsClamped.EvictionRetentionRatio, 1e-9)

	assert.InDelta(t, DefaultEvictionRetentionRatio, optsNegativeRetention.EvictionRetentionRatio, 1e-9)

	assert.InDelta(t, 0.70*(DefaultCompactionThreshold/DefaultEvictionThreshold), optsInverted.CompactionThreshold, 1e-9)
	assert.InDelta(t, 0.70, optsInverted.EvictionThreshold, 1e-9)
	assert.Less(t, optsInverted.CompactionThreshold, optsInverted.EvictionThreshold)

	assert.InDelta(t, DefaultCompactionThreshold, optsNaNAndInf.CompactionThreshold, 1e-9)
	assert.InDelta(t, DefaultEvictionThreshold, optsNaNAndInf.EvictionThreshold, 1e-9)
	assert.InDelta(t, DefaultEvictionRetentionRatio, optsNaNAndInf.EvictionRetentionRatio, 1e-9)

	// F08: Single threshold customization preserves Tier 1 compaction window, including at 1.0.
	assert.InDelta(t, 0.92, optsHighCompactionOnly.CompactionThreshold, 1e-9)
	assert.Greater(t, optsHighCompactionOnly.EvictionThreshold, optsHighCompactionOnly.CompactionThreshold)

	assert.InDelta(t, 1.0, optsCompaction100.CompactionThreshold, 1e-9)
	assert.InDelta(t, 1.15, optsCompaction100.EvictionThreshold, 1e-9)

	assert.InDelta(t, 0.60, optsLowEvictionOnly.EvictionThreshold, 1e-9)
	assert.Less(t, optsLowEvictionOnly.CompactionThreshold, optsLowEvictionOnly.EvictionThreshold)

	assert.True(t, optsDirectStructPressure.hasCustomPressureFunc)
}

func TestDefaultRuntimePressureFunc(t *testing.T) {
	// Arrange
	prevLimit := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prevLimit)

	// Act & Assert 1: Unbounded GOMEMLIMIT (math.MaxInt64) with MemoryBudget == 0 returns 0.0.
	debug.SetMemoryLimit(math.MaxInt64)
	probeUnbounded := DefaultRuntimePressureFunc(0)
	assert.InDelta(t, 0.0, probeUnbounded(), 1e-9)

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
