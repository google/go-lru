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
	constructors := append(allBackends(), struct {
		name string
		fn   func(uint64, ...Option) Cache
	}{"New", New})

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
	c := NewMapCache(100, func(o *Options) { o.PressureFunc = customFn })
	_, err := c.Insert("k", NewSizedValue("v", 10))
	require.NoError(t, err)
	assert.Greater(t, customCalls, callsBefore)
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
