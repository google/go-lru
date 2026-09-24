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
	var val ValueType = testValue{size: 42}
	if val.Size() != 42 {
		t.Errorf("expected size 42, got %d", val.Size())
	}
}

func TestOptions_Default(t *testing.T) {
	opts := ApplyOptions()
	if opts.EnableInvariantChecking {
		t.Errorf("expected EnableInvariantChecking to be false by default, got true")
	}
}

func TestOptions_WithInvariantChecking(t *testing.T) {
	optsTrue := ApplyOptions(WithInvariantChecking(true))
	if !optsTrue.EnableInvariantChecking {
		t.Errorf("expected EnableInvariantChecking to be true, got false")
	}

	optsFalse := ApplyOptions(WithInvariantChecking(false))
	if optsFalse.EnableInvariantChecking {
		t.Errorf("expected EnableInvariantChecking to be false, got true")
	}

	// Chained options and nil resilience.
	optsChained := ApplyOptions(nil, WithInvariantChecking(false), nil, WithInvariantChecking(true))
	if !optsChained.EnableInvariantChecking {
		t.Errorf("expected EnableInvariantChecking to be true after chained options, got false")
	}
}

func TestSentinelErrors(t *testing.T) {
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

	for _, tc := range tests {
		if tc.err == nil {
			t.Errorf("expected non-nil error")
		} else if tc.err.Error() != tc.expected {
			t.Errorf("expected error message %q, got %q", tc.expected, tc.err.Error())
		}
	}
}

func TestConstructors_Validation(t *testing.T) {
	constructors := []struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{
		{"NewMapCache", NewMapCache},
		{"New", New},
		{"NewRadixCache", NewRadixCache},
		{"NewArenaRadixCache", NewArenaRadixCache},
	}

	for _, c := range constructors {
		t.Run(c.name+"/ZeroMaxSize_DefaultOptions", func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("[%s] expected panic on maxSize == 0 with default options, got none", c.name)
				}
			}()
			_ = c.fn(0)
		})

		t.Run(c.name+"/ZeroMaxSize_InvariantsDisabled", func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("[%s] expected panic on maxSize == 0 with invariants disabled, got none", c.name)
				}
			}()
			_ = c.fn(0, WithInvariantChecking(false))
		})

		t.Run(c.name+"/ZeroMaxSize_InvariantsEnabled", func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatalf("[%s] expected panic on maxSize == 0 with invariants enabled, got none", c.name)
				}
			}()
			_ = c.fn(0, WithInvariantChecking(true))
		})

		t.Run(c.name+"/PositiveBoundary_MaxSize1", func(t *testing.T) {
			cache := c.fn(1)
			if cache == nil {
				t.Fatalf("[%s] expected non-nil cache for maxSize == 1", c.name)
			}
		})
	}
}

func TestOptions_MemoryPressureDefaults(t *testing.T) {
	// Arrange & Act
	opts := ApplyOptions()

	// Assert
	assert.Equal(t, 0.75, DefaultCompactionThreshold)
	assert.Equal(t, 0.90, DefaultEvictionThreshold)
	assert.Equal(t, 0.50, DefaultEvictionRetentionRatio)
	assert.Equal(t, DefaultCompactionThreshold, opts.CompactionThreshold)
	assert.Equal(t, DefaultEvictionThreshold, opts.EvictionThreshold)
	assert.Equal(t, DefaultEvictionRetentionRatio, opts.EvictionRetentionRatio)
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

	// Assert
	assert.Equal(t, uint64(256*1024*1024), opts.MemoryBudget)
	assert.Equal(t, 0.60, opts.CompactionThreshold)
	assert.Equal(t, 0.85, opts.EvictionThreshold)
	assert.Equal(t, 0.35, opts.EvictionRetentionRatio)
	require.NotNil(t, opts.PressureFunc)
	assert.True(t, opts.hasCustomPressureFunc)
	assert.Equal(t, 0.88, opts.PressureFunc())

	assert.Equal(t, 0.0, optsZeroRetention.EvictionRetentionRatio)

	assert.Equal(t, DefaultCompactionThreshold, optsClamped.CompactionThreshold)
	assert.Equal(t, DefaultEvictionThreshold, optsClamped.EvictionThreshold)
	assert.Equal(t, 1.0, optsClamped.EvictionRetentionRatio)

	assert.Equal(t, DefaultEvictionRetentionRatio, optsNegativeRetention.EvictionRetentionRatio)

	assert.Equal(t, 0.70, optsInverted.CompactionThreshold)
	assert.Equal(t, 0.70, optsInverted.EvictionThreshold)

	assert.Equal(t, DefaultCompactionThreshold, optsNaNAndInf.CompactionThreshold)
	assert.Equal(t, DefaultEvictionThreshold, optsNaNAndInf.EvictionThreshold)
	assert.Equal(t, DefaultEvictionRetentionRatio, optsNaNAndInf.EvictionRetentionRatio)

	// F8: Single threshold customization preserves Tier 1 compaction window.
	optsHighCompactionOnly := ApplyOptions(WithCompactionThreshold(0.92))
	assert.Equal(t, 0.92, optsHighCompactionOnly.CompactionThreshold)
	assert.Greater(t, optsHighCompactionOnly.EvictionThreshold, optsHighCompactionOnly.CompactionThreshold)

	optsLowEvictionOnly := ApplyOptions(WithEvictionThreshold(0.60))
	assert.Equal(t, 0.60, optsLowEvictionOnly.EvictionThreshold)
	assert.Less(t, optsLowEvictionOnly.CompactionThreshold, optsLowEvictionOnly.EvictionThreshold)

	optsDirectStructPressure := ApplyOptions(func(o *Options) {
		o.PressureFunc = customFn
	})
	assert.True(t, optsDirectStructPressure.hasCustomPressureFunc)
}

func TestDefaultRuntimePressureFunc(t *testing.T) {
	// Arrange
	prevLimit := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prevLimit)

	// Act & Assert 1: Unbounded GOMEMLIMIT (math.MaxInt64) with MemoryBudget == 0 returns 0.0.
	debug.SetMemoryLimit(math.MaxInt64)
	probeUnbounded := DefaultRuntimePressureFunc(0)
	assert.Equal(t, 0.0, probeUnbounded())

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
	assert.Equal(t, 0.0, allocsPerRun)

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

// 172a4
