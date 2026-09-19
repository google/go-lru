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

package lrus

import (
	"math"
	"runtime/debug"
	"sync"
	"testing"
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
	if DefaultCompactionThreshold != 0.75 {
		t.Errorf("expected DefaultCompactionThreshold == 0.75, got %v", DefaultCompactionThreshold)
	}
	if DefaultEvictionThreshold != 0.90 {
		t.Errorf("expected DefaultEvictionThreshold == 0.90, got %v", DefaultEvictionThreshold)
	}
	if DefaultEvictionRetentionRatio != 0.50 {
		t.Errorf("expected DefaultEvictionRetentionRatio == 0.50, got %v", DefaultEvictionRetentionRatio)
	}

	opts := ApplyOptions()
	if opts.CompactionThreshold != DefaultCompactionThreshold {
		t.Errorf("expected default CompactionThreshold %v, got %v", DefaultCompactionThreshold, opts.CompactionThreshold)
	}
	if opts.EvictionThreshold != DefaultEvictionThreshold {
		t.Errorf("expected default EvictionThreshold %v, got %v", DefaultEvictionThreshold, opts.EvictionThreshold)
	}
	if opts.EvictionRetentionRatio != DefaultEvictionRetentionRatio {
		t.Errorf("expected default EvictionRetentionRatio %v, got %v", DefaultEvictionRetentionRatio, opts.EvictionRetentionRatio)
	}
	if opts.PressureFunc == nil {
		t.Fatalf("expected non-nil default PressureFunc")
	}
}

func TestOptions_MemoryPressureCustomAndValidation(t *testing.T) {
	customFn := func() float64 { return 0.88 }
	opts := ApplyOptions(
		WithPressureFunc(customFn),
		WithMemoryBudget(256*1024*1024),
		WithCompactionThreshold(0.60),
		WithEvictionThreshold(0.85),
		WithEvictionRetentionRatio(0.35),
	)

	if opts.MemoryBudget != 256*1024*1024 {
		t.Errorf("expected MemoryBudget 268435456, got %d", opts.MemoryBudget)
	}
	if opts.CompactionThreshold != 0.60 {
		t.Errorf("expected CompactionThreshold 0.60, got %v", opts.CompactionThreshold)
	}
	if opts.EvictionThreshold != 0.85 {
		t.Errorf("expected EvictionThreshold 0.85, got %v", opts.EvictionThreshold)
	}
	if opts.EvictionRetentionRatio != 0.35 {
		t.Errorf("expected EvictionRetentionRatio 0.35, got %v", opts.EvictionRetentionRatio)
	}
	if opts.PressureFunc == nil || opts.PressureFunc() != 0.88 {
		t.Errorf("expected custom PressureFunc returning 0.88, got %v", opts.PressureFunc())
	}

	// Verify explicit 0.0 retention ratio is preserved (shed 100%).
	optsZeroRetention := ApplyOptions(WithEvictionRetentionRatio(0.0))
	if optsZeroRetention.EvictionRetentionRatio != 0.0 {
		t.Errorf("expected explicit 0.0 EvictionRetentionRatio to be preserved, got %v", optsZeroRetention.EvictionRetentionRatio)
	}

	// Verify out-of-range normalization.
	optsClamped := ApplyOptions(
		WithCompactionThreshold(-0.1),
		WithEvictionThreshold(0),
		WithEvictionRetentionRatio(1.5),
	)
	if optsClamped.CompactionThreshold != DefaultCompactionThreshold {
		t.Errorf("expected negative CompactionThreshold normalized to default, got %v", optsClamped.CompactionThreshold)
	}
	if optsClamped.EvictionThreshold != DefaultEvictionThreshold {
		t.Errorf("expected zero EvictionThreshold normalized to default, got %v", optsClamped.EvictionThreshold)
	}
	if optsClamped.EvictionRetentionRatio != 1.0 {
		t.Errorf("expected >1.0 EvictionRetentionRatio clamped to 1.0, got %v", optsClamped.EvictionRetentionRatio)
	}
}

func TestDefaultRuntimePressureFunc(t *testing.T) {
	// Save and restore GOMEMLIMIT around test.
	prevLimit := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prevLimit)

	// 1. When GOMEMLIMIT is unset (math.MaxInt64) and MemoryBudget == 0, pressure must be 0.0.
	debug.SetMemoryLimit(math.MaxInt64)
	probeUnbounded := DefaultRuntimePressureFunc(0)
	if p := probeUnbounded(); p != 0.0 {
		t.Errorf("expected 0.0 pressure when GOMEMLIMIT is unset and budget is 0, got %v", p)
	}

	// 2. When GOMEMLIMIT is set to 1 GiB, DefaultRuntimePressureFunc(0) returns positive pressure.
	const oneGiB = int64(1 << 30)
	debug.SetMemoryLimit(oneGiB)
	pLimit := probeUnbounded()
	if pLimit <= 0.0 || pLimit >= 1.0 {
		t.Errorf("expected runtime pressure in (0.0, 1.0) with 1 GiB GOMEMLIMIT, got %v", pLimit)
	}

	// 3. Custom MemoryBudget overrides GOMEMLIMIT.
	probeBudget512MB := DefaultRuntimePressureFunc(512 << 20)
	probeBudget2GB := DefaultRuntimePressureFunc(2 << 30)
	p512 := probeBudget512MB()
	p2G := probeBudget2GB()
	if p512 <= 0.0 || p2G <= 0.0 {
		t.Fatalf("expected positive pressures for custom budgets, got p512=%v, p2G=%v", p512, p2G)
	}
	if p512 <= p2G {
		t.Errorf("expected smaller budget (512MB) to report higher pressure than larger budget (2GB): p512=%v, p2G=%v", p512, p2G)
	}

	// 4. Concurrent race-free reads across multiple goroutines.
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				v := probeBudget512MB()
				if v <= 0.0 {
					t.Errorf("expected positive pressure in concurrent read, got %v", v)
				}
			}
		}()
	}
	wg.Wait()
}

