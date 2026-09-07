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
