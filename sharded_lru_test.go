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
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"net"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
	"weak"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prefixShardFunc(key string, numShards int) int {
	if before, _, ok := strings.Cut(key, "/"); ok && len(before) > 0 {
		var sum int
		for i := range len(before) {
			sum = sum*31 + int(before[i])
		}
		if sum < 0 {
			sum = -sum
		}
		return sum % numShards
	}
	return DefaultShardFunc(key, numShards)
}

func explicitTagShardFunc(key string, numShards int) int {
	if len(key) >= 3 && key[0] == 's' {
		if slash := strings.IndexByte(key, '/'); slash > 1 {
			n := 0
			valid := true
			for i := 1; i < slash; i++ {
				ch := key[i]
				if ch < '0' || ch > '9' {
					valid = false
					break
				}
				n = n*10 + int(ch-'0')
			}
			if valid {
				return n % numShards
			}
		}
	}
	return DefaultShardFunc(key, numShards)
}

func TestDefaultShardFunc(t *testing.T) {
	assert.PanicsWithValue(t, "numShards must be greater than zero", func() {
		DefaultShardFunc("key", 0)
	})
	assert.PanicsWithValue(t, "numShards must be greater than zero", func() {
		DefaultShardFunc("key", -4)
	})

	assert.Equal(t, 0, DefaultShardFunc("", 1))
	assert.Equal(t, 0, DefaultShardFunc("any-key", 1))

	for _, numShards := range []int{2, 3, 4, 5, 7, 8, 12, 16, 31, 64} {
		seen := make(map[int]int, numShards)
		for i := range numShards * 64 {
			k := fmt.Sprintf("tenant-%d/item-%d", i%17, i)
			idx1 := DefaultShardFunc(k, numShards)
			idx2 := DefaultShardFunc(k, numShards)
			require.Equal(t, idx1, idx2, "DefaultShardFunc must be deterministic")
			require.GreaterOrEqual(t, idx1, 0)
			require.Less(t, idx1, numShards)
			seen[idx1]++
		}
		assert.Len(t, seen, numShards, "expected keys to hit all %d shards", numShards)
	}
}

func TestNewShardedCache_PreconditionsAndOptions(t *testing.T) {
	assert.PanicsWithValue(t, "maxSize must be greater than zero", func() {
		_ = NewShardedCache[string](0, 4, nil)
	})
	assert.PanicsWithValue(t, "shards must be greater than zero", func() {
		_ = NewShardedCache[string](100, 0, nil)
	})
	assert.PanicsWithValue(t, "shards must be greater than zero", func() {
		_ = NewShardedCache[string](100, -2, nil)
	})
	assert.PanicsWithValue(t, "shards must not exceed maxSize", func() {
		_ = NewShardedCache[string](4, 5, nil)
	})

	constructors := []struct {
		name string
		fn   func(uint64, ...Option) Cache[string]
	}{
		{"New", New[string]},
		{"NewMapCache", NewMapCache[string]},
		{"NewRadixCache", NewRadixCache[string]},
		{"NewArenaRadixCache", NewArenaRadixCache[string]},
	}
	for _, ctor := range constructors {
		t.Run(ctor.name, func(t *testing.T) {
			assert.PanicsWithValue(t, "shards must be greater than zero", func() {
				_ = ctor.fn(100, WithShards(0))
			})
			assert.PanicsWithValue(t, "shards must be greater than zero", func() {
				_ = ctor.fn(100, WithShards(-1))
			})
			assert.PanicsWithValue(t, "shards must be greater than zero", func() {
				_ = ctor.fn(100, func(o *Options) { o.Shards = -5 })
			})
			assert.PanicsWithValue(t, "shards must not exceed maxSize", func() {
				_ = ctor.fn(8, WithShards(9))
			})
		})
	}
}

func TestShardedCache_ShardFuncPrecedenceAndBoundsCheck(t *testing.T) {
	var calledExplicit, calledOption bool
	explicitFn := func(string, int) int {
		calledExplicit = true
		return 0
	}
	optionFn := func(string, int) int {
		calledOption = true
		return 1
	}

	// Explicit non-nil shardFn overrides WithShardFunc.
	c1 := NewShardedCache[string](10, 2, explicitFn, WithShardFunc(optionFn), WithInvariantChecking(true))
	_, err := c1.Put("k", "v")
	require.NoError(t, err)
	assert.True(t, calledExplicit)
	assert.False(t, calledOption)

	// Nil shardFn falls back to WithShardFunc.
	c2 := NewShardedCache[string](10, 2, nil, WithShardFunc(optionFn), WithInvariantChecking(true))
	_, err = c2.Put("k", "v")
	require.NoError(t, err)
	assert.True(t, calledOption)

	// WithShardFunc(nil) resets back to DefaultShardFunc.
	optsReset := ApplyOptions(WithShardFunc(optionFn), WithShardFunc(nil))
	assert.NotNil(t, optsReset.ShardFunc)

	// Unnormalized Options with nil ShardFunc falls back to DefaultShardFunc.
	cRaw := newShardedCacheWithOptions[string](10, 2, Options{Backend: BackendMap})
	_, err = cRaw.Put("k", "v")
	require.NoError(t, err)

	// Out-of-bounds ShardFunc return values panic cleanly on all point operations.
	badNeg := NewShardedCache[string](10, 4, func(string, int) int { return -1 })
	badHigh := NewShardedCache[string](10, 4, func(string, int) int { return 4 })

	assert.PanicsWithValue(t, "lru: ShardFunc returned shard index -1 out of range [0, 4)", func() {
		_, _ = badNeg.Put("k", "v")
	})
	assert.PanicsWithValue(t, "lru: ShardFunc returned shard index 4 out of range [0, 4)", func() {
		_, _ = badHigh.Get("k")
	})
	assert.PanicsWithValue(t, "lru: ShardFunc returned shard index -1 out of range [0, 4)", func() {
		_, _ = badNeg.Peek("k")
	})
	assert.PanicsWithValue(t, "lru: ShardFunc returned shard index 4 out of range [0, 4)", func() {
		_ = badHigh.Replace("k", "v")
	})
	assert.PanicsWithValue(t, "lru: ShardFunc returned shard index -1 out of range [0, 4)", func() {
		_, _ = badNeg.Delete("k")
	})
}

func TestShardedCache_CapacityPartitioningAndShardIsolation(t *testing.T) {
	// maxSize = 10 across 3 shards -> shard capacities: 4, 3, 3.
	c := NewShardedCache[string](
		10,
		3,
		explicitTagShardFunc,
		WithInvariantChecking(true),
		WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
	)

	st := c.Stats()
	assert.Equal(t, uint64(10), st.MaxSize)

	// Shard 0 has capacity 4: weight 4 fits, weight 5 fails with ErrInvalidEntrySize.
	_, err := c.Put("s0/big", "12345")
	require.ErrorIs(t, err, ErrInvalidEntrySize)
	_, err = c.Put("s0/exact", "1234")
	require.NoError(t, err)

	// Shard 1 and Shard 2 each have capacity 3: weight 4 fails, weight 3 fits.
	_, err = c.Put("s1/too-big", "1234")
	require.ErrorIs(t, err, ErrInvalidEntrySize)
	_, err = c.Put("s1/a", "12")
	require.NoError(t, err)
	_, err = c.Put("s2/a", "123")
	require.NoError(t, err)

	// Churning Shard 1 evicts only Shard 1's entries, leaving Shard 0 and Shard 2 untouched.
	evicted, err := c.Put("s1/b", "12")
	require.NoError(t, err)
	assert.Equal(t, []string{"12"}, evicted)

	_, okS0 := c.Peek("s0/exact")
	_, okS1A := c.Peek("s1/a")
	_, okS1B := c.Peek("s1/b")
	_, okS2A := c.Peek("s2/a")
	assert.True(t, okS0)
	assert.False(t, okS1A)
	assert.True(t, okS1B)
	assert.True(t, okS2A)

	st = c.Stats()
	assert.Equal(t, uint64(2), st.PutRejectedOversized)
	assert.Equal(t, uint64(9), st.CurrentSize) // 4 + 2 + 3
	assert.Equal(t, 3, st.Len)
}

func allShardedBackendConfigs(t *testing.T, maxSize uint64, shards int, extraOpts ...Option) []struct {
	name    string
	backend Backend
	cache   Cache[string]
} {
	t.Helper()
	return []struct {
		name    string
		backend Backend
		cache   Cache[string]
	}{
		{
			name:    "NewSharded_Map",
			backend: BackendMap,
			cache:   NewShardedCache[string](maxSize, shards, prefixShardFunc, append([]Option{WithBackend(BackendMap), WithInvariantChecking(true)}, extraOpts...)...),
		},
		{
			name:    "NewSharded_Radix",
			backend: BackendRadix,
			cache:   NewShardedCache[string](maxSize, shards, prefixShardFunc, append([]Option{WithBackend(BackendRadix), WithInvariantChecking(true)}, extraOpts...)...),
		},
		{
			name:    "NewSharded_ArenaRadix",
			backend: BackendArenaRadix,
			cache:   NewShardedCache[string](maxSize, shards, prefixShardFunc, append([]Option{WithBackend(BackendArenaRadix), WithInvariantChecking(true)}, extraOpts...)...),
		},
		{
			name:    "New_WithShards_Map",
			backend: BackendMap,
			cache:   New[string](maxSize, append([]Option{WithShards(shards), WithShardFunc(DefaultShardFunc), WithInvariantChecking(true)}, extraOpts...)...),
		},
		{
			name:    "NewMapCache_WithShards",
			backend: BackendMap,
			cache:   NewMapCache[string](maxSize, append([]Option{WithShards(shards), WithBackend(BackendRadix), WithInvariantChecking(true)}, extraOpts...)...),
		},
		{
			name:    "NewRadixCache_WithShards",
			backend: BackendRadix,
			cache:   NewRadixCache[string](maxSize, append([]Option{WithShards(shards), WithInvariantChecking(true)}, extraOpts...)...),
		},
		{
			name:    "NewArenaRadixCache_WithShards",
			backend: BackendArenaRadix,
			cache:   NewArenaRadixCache[string](maxSize, append([]Option{WithShards(shards), WithInvariantChecking(true)}, extraOpts...)...),
		},
	}
}

func TestShardedCache_CRUD_Iterators_And_DeletePrefix_AllBackends(t *testing.T) {
	configs := allShardedBackendConfigs(t, 64, 4, WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }))

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			c := cfg.cache
			assert.Equal(t, cfg.backend, c.Stats().Backend)

			// Empty state checks
			_, ok := c.Get("missing")
			assert.False(t, ok)
			_, ok = c.Peek("missing")
			assert.False(t, ok)
			require.ErrorIs(t, c.Replace("missing", "val"), ErrEntryNotExist)
			_, ok = c.Delete("missing")
			assert.False(t, ok)

			// Populate across multiple prefixes and zero-weight entries
			for i := range 12 {
				key := fmt.Sprintf("ns%d/sub/key-%02d", i%4, i)
				val := "v"
				if i%4 == 0 {
					val = "" // zero-weight entry
				}
				_, err := c.Put(key, val)
				require.NoError(t, err)
			}

			// Update and Replace existing entry
			_, err := c.Put("ns1/sub/key-01", "vv")
			require.NoError(t, err)
			require.NoError(t, c.Replace("ns1/sub/key-01", "v"))

			v, ok := c.Get("ns1/sub/key-01")
			require.True(t, ok)
			assert.Equal(t, "v", v)

			vPeek, ok := c.Peek("ns2/sub/key-02")
			require.True(t, ok)
			assert.Equal(t, "v", vPeek)

			// Iterators: full collection and early break
			allMap := maps.Collect(c.All())
			assert.Len(t, allMap, 12)

			keysSlice := slices.Collect(c.Keys())
			assert.Len(t, keysSlice, 12)

			valsSlice := slices.Collect(c.Values())
			assert.Len(t, valsSlice, 12)

			countAll := 0
			for range c.All() {
				countAll++
				if countAll == 3 {
					break
				}
			}
			assert.Equal(t, 3, countAll)

			countKeys := 0
			for range c.Keys() {
				countKeys++
				if countKeys == 2 {
					break
				}
			}
			assert.Equal(t, 2, countKeys)

			countVals := 0
			for range c.Values() {
				countVals++
				if countVals == 4 {
					break
				}
			}
			assert.Equal(t, 4, countVals)

			// Delete single entry
			delVal, delOK := c.Delete("ns3/sub/key-03")
			require.True(t, delOK)
			assert.Equal(t, "v", delVal)

			// DeletePrefix for "ns1/" -> deletes 3 entries, increments DeletePrefixExecuted by 1 (not 4!)
			c.DeletePrefix("ns1/")
			stAfterPrefix := c.Stats()
			assert.Equal(t, uint64(1), stAfterPrefix.DeletePrefixExecuted)
			assert.Equal(t, 8, stAfterPrefix.Len)

			// DeletePrefix("") clears entire cache across all shards and increments DeletePrefixExecuted to 2
			c.DeletePrefix("")
			stAfterClear := c.Stats()
			assert.Equal(t, uint64(2), stAfterClear.DeletePrefixExecuted)
			assert.Equal(t, 0, stAfterClear.Len)
			assert.Equal(t, uint64(0), stAfterClear.CurrentSize)
		})
	}
}

func TestShardedCache_MemoryPressureAndCompaction(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var currentPressure atomic.Uint64
			setPressure := func(p float64) {
				currentPressure.Store(math.Float64bits(p))
			}
			setPressure(0.10)

			c := NewShardedCache[string](
				40,
				4,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(currentPressure.Load())
				}),
				WithCompactionThreshold(0.70),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
			)

			pa, ok := c.(PressureAwareCache[string])
			require.True(t, ok, "shardedCache must implement PressureAwareCache")

			// Populate 8 items into shard 0, 8 items into shard 1, 8 items into shard 2 (capacity 10 per shard).
			for i := range 8 {
				_, err := c.Put(fmt.Sprintf("s0/k%d", i), "v")
				require.NoError(t, err)
				_, err = c.Put(fmt.Sprintf("s1/k%d", i), "v")
				require.NoError(t, err)
				_, err = c.Put(fmt.Sprintf("s2/k%d", i), "v")
				require.NoError(t, err)
			}
			assert.InDelta(t, 0.10, c.Stats().MemoryPressure, 1e-9)

			// Delete several entries on shard 0 to create map/arena slack, then run Compact().
			for i := range 6 {
				_, _ = c.Delete(fmt.Sprintf("s0/k%d", i))
			}
			pa.Compact()
			if backend != BackendRadix {
				assert.Positive(t, c.Stats().CompactionsExplicit)
			}

			// Raise pressure to 0.95 and write to shard 0 ONLY: Stats().MemoryPressure must report 0.95.
			setPressure(0.95)
			_, err := c.Put("s0/hot", "v")
			require.NoError(t, err)
			assert.InDelta(t, 0.95, c.Stats().MemoryPressure, 1e-9)

			// Explicit EvaluateMemoryPressure sheds shard 1 and shard 2 down to 50% retention (<= 5 entries each).
			shed := pa.EvaluateMemoryPressure()
			assert.NotEmpty(t, shed)
			assert.LessOrEqual(t, c.Stats().CurrentSize, uint64(20))

			// Now drop pressure to 0.25 and write to shard 2 ONLY: Stats().MemoryPressure must reflect 0.25 (not stale 0.95 from shard 0!).
			setPressure(0.25)
			_, err = c.Put("s2/cool", "v")
			require.NoError(t, err)
			assert.InDelta(t, 0.25, c.Stats().MemoryPressure, 1e-9)

			// Drop pressure to 0.0 and write to shard 1 ONLY: Stats().MemoryPressure must reflect 0.0.
			setPressure(0.0)
			_, err = c.Put("s1/zero", "v")
			require.NoError(t, err)
			assert.InDelta(t, 0.0, c.Stats().MemoryPressure, 1e-9)
		})
	}
}

func TestShardedCache_EvictCallbacksAndReentrancy(t *testing.T) {
	var cache Cache[int]
	var valCalls, entryCalls atomic.Int64

	cache = NewShardedCache[int](
		8, // 4 shards * 2 capacity each
		4,
		explicitTagShardFunc,
		WithBackend(BackendArenaRadix),
		WithInvariantChecking(true),
		WithOnEvictValue(func(_ int, _ EvictionReason) {
			valCalls.Add(1)
		}),
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			entryCalls.Add(1)
			// Re-entrant cross-shard and same-shard calls inside OnEvictEntry must not deadlock.
			if r == EvictionReasonCapacity && strings.HasPrefix(k, "s0/") {
				_, _ = cache.Put("s2/from-callback", 99)
				_, _ = cache.Get("s0/c")
				_ = cache.Stats()
			}
		}),
	)

	_, err := cache.Put("s0/a", 1)
	require.NoError(t, err)
	_, err = cache.Put("s0/b", 2)
	require.NoError(t, err)
	// Triggers capacity eviction on shard 0, which re-entrantly writes to shard 2 and reads shard 0.
	evicted, err := cache.Put("s0/c", 3)
	require.NoError(t, err)
	assert.Equal(t, []int{1}, evicted)

	v, ok := cache.Get("s2/from-callback")
	require.True(t, ok)
	assert.Equal(t, 99, v)
	assert.Equal(t, valCalls.Load(), entryCalls.Load())
	assert.Positive(t, entryCalls.Load())
}

func TestShardedCache_ZeroAllocHotPaths(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			c := NewShardedCache[int](1024, 8, nil, WithBackend(backend))
			for i := range 128 {
				_, err := c.Put(fmt.Sprintf("dir/key-%03d", i), i)
				require.NoError(t, err)
			}

			allocsGet := testing.AllocsPerRun(100, func() {
				_, _ = c.Get("dir/key-042")
			})
			assert.Zero(t, allocsGet, "Get should allocate 0 B/op")

			allocsPeek := testing.AllocsPerRun(100, func() {
				_, _ = c.Peek("dir/key-042")
			})
			assert.Zero(t, allocsPeek, "Peek should allocate 0 B/op")

			allocsReplace := testing.AllocsPerRun(100, func() {
				_ = c.Replace("dir/key-042", 777)
			})
			assert.Zero(t, allocsReplace, "Replace should allocate 0 B/op")

			seqVals := c.Values()
			allocsValues := testing.AllocsPerRun(100, func() {
				seqVals(func(int) bool { return true })
			})
			assert.Zero(t, allocsValues, "Values iterator should allocate 0 B/op")

			allocsStats := testing.AllocsPerRun(100, func() {
				_ = c.Stats()
			})
			assert.Zero(t, allocsStats, "Stats should allocate 0 B/op")

			if backend == BackendMap {
				seqAll := c.All()
				allocsAll := testing.AllocsPerRun(100, func() {
					seqAll(func(string, int) bool { return true })
				})
				assert.Zero(t, allocsAll, "MapCache sharded All iterator should allocate 0 B/op")
			}
		})
	}
}

func TestShardedCache_InvariantCheckerPanicsOnCorruption(t *testing.T) {
	c := newShardedCacheWithOptions[int](16, 4, ApplyOptions(WithInvariantChecking(true)))

	assert.Panics(t, func() {
		bad := &shardedCache[int]{maxSize: 16}
		bad.checkInvariants()
	})
	assert.Panics(t, func() {
		bad := &shardedCache[int]{shards: c.shards, maxSize: 16, shardFn: nil}
		bad.checkInvariants()
	})

	base := c.Stats()

	t.Run("MaxSizeMismatch", func(t *testing.T) {
		st := base
		st.MaxSize = 999
		assert.Panics(t, func() { c.checkAggregateStatsInvariants(&st) })
	})
	t.Run("CurrentSizeExceedsMaxSize", func(t *testing.T) {
		st := base
		st.CurrentSize = st.MaxSize + 1
		assert.Panics(t, func() { c.checkAggregateStatsInvariants(&st) })
	})
	t.Run("InvalidZeroSizeCount", func(t *testing.T) {
		st := base
		st.ZeroSizeCount = 1
		st.Len = 0
		assert.Panics(t, func() { c.checkAggregateStatsInvariants(&st) })
	})
	t.Run("NegativeWatermark", func(t *testing.T) {
		st := base
		st.DeletedSinceCompact = -1
		assert.Panics(t, func() { c.checkAggregateStatsInvariants(&st) })
	})
	t.Run("InvalidMemoryPressure", func(t *testing.T) {
		st := base
		st.MemoryPressure = math.NaN()
		assert.Panics(t, func() { c.checkAggregateStatsInvariants(&st) })
	})
	t.Run("ReplacedMismatch", func(t *testing.T) {
		st := base
		st.PutUpdated = 1
		assert.Panics(t, func() { c.checkAggregateStatsInvariants(&st) })
	})
	t.Run("EntryConservationMismatch", func(t *testing.T) {
		st := base
		st.Len = 1
		assert.Panics(t, func() { c.checkAggregateStatsInvariants(&st) })
	})
	t.Run("DeleteDeletedExceedsEvictionsDeleted", func(t *testing.T) {
		st := base
		st.PutInserted = 1
		st.EvictionsDeleted = 1
		st.DeleteDeleted = 2
		assert.Panics(t, func() { c.checkAggregateStatsInvariants(&st) })
	})
	t.Run("ReplaceSelfEvictedExceedsEvictionsCapacity", func(t *testing.T) {
		st := base
		st.PutInserted = 1
		st.EvictionsCapacity = 1
		st.ReplaceSelfEvicted = 2
		assert.Panics(t, func() { c.checkAggregateStatsInvariants(&st) })
	})
	t.Run("PressureShedsExceedEvictionsPressure", func(t *testing.T) {
		st := base
		st.PutInserted = 1
		st.EvictionsPressure = 1
		st.PressureShedsInline = 1
		st.PressureShedsExplicit = 1
		assert.Panics(t, func() { c.checkAggregateStatsInvariants(&st) })
	})
}

func runShardedRaceWorkerStep(c Cache[uint64], pa PressureAwareCache[uint64], pressureBits *atomic.Uint64, r *rand.Rand, step int) {
	kIdx := r.IntN(96)
	key := fmt.Sprintf("grp_%02d/item_%03d", kIdx%6, kIdx)
	op := r.IntN(100)
	switch {
	case op < 35:
		wgt := uint64(r.IntN(12))
		if step%29 == 0 {
			wgt = 50 // exceeds per-shard capacity (40)
		}
		_, _ = c.Put(key, wgt)
	case op < 55:
		_, _ = c.Get(key)
	case op < 70:
		_, _ = c.Peek(key)
	case op < 82:
		_ = c.Replace(key, uint64(r.IntN(45)))
	case op < 90:
		_, _ = c.Delete(key)
	case op < 94:
		c.DeletePrefix(fmt.Sprintf("grp_%02d/", kIdx%6))
	case op < 97:
		n := 0
		for range c.Values() {
			n++
			if n >= 5 {
				break
			}
		}
	case op < 99:
		pa.Compact()
	default:
		pressureBits.Store(math.Float64bits(0.95))
		_ = pa.EvaluateMemoryPressure()
		pressureBits.Store(math.Float64bits(0.20))
	}
}

func startShardedStatsMonotonicReader(t *testing.T, c Cache[uint64], capacity uint64, stopReader <-chan struct{}, readerWG *sync.WaitGroup) {
	t.Helper()
	readerWG.Go(func() {
		prev := c.Stats()
		for {
			select {
			case <-stopReader:
				return
			default:
				cur := c.Stats()
				assert.Equal(t, capacity, cur.MaxSize)
				assert.LessOrEqual(t, cur.CurrentSize, cur.MaxSize)
				assert.GreaterOrEqual(t, cur.GetHits, prev.GetHits)
				assert.GreaterOrEqual(t, cur.GetMisses, prev.GetMisses)
				assert.GreaterOrEqual(t, cur.PeekHits, prev.PeekHits)
				assert.GreaterOrEqual(t, cur.PutInserted, prev.PutInserted)
				assert.GreaterOrEqual(t, cur.DeletePrefixExecuted, prev.DeletePrefixExecuted)
				prev = cur
			}
		}
	})
}

func TestShardedCache_ConcurrentRaceAndTelemetryParity(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.20))

			var valCounts [4]atomic.Uint64
			var entryCounts [4]atomic.Uint64
			var entryWeights [4]atomic.Uint64

			const capacity = uint64(320)
			const shards = 8

			c := NewShardedCache[uint64](
				capacity,
				shards,
				nil,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(_ string, v uint64) uint64 { return v }),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
				WithOnEvictValue(func(_ uint64, r EvictionReason) {
					valCounts[r].Add(1)
				}),
				WithOnEvictEntry(func(_ string, v uint64, r EvictionReason) {
					entryCounts[r].Add(1)
					entryWeights[r].Add(v)
				}),
			)
			pa := c.(PressureAwareCache[uint64])

			var wg sync.WaitGroup
			stopReader := make(chan struct{})
			var readerWG sync.WaitGroup
			startShardedStatsMonotonicReader(t, c, capacity, stopReader, &readerWG)

			const workers = 12
			const opsPerWorker = 250
			for w := range workers {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					r := rand.New(rand.NewPCG(uint64(workerID+1), 99))
					for step := range opsPerWorker {
						runShardedRaceWorkerStep(c, pa, &pressureBits, r, step)
					}
				}(w)
			}

			wg.Wait()
			close(stopReader)
			readerWG.Wait()

			st := c.Stats()
			for reason := range 4 {
				r := EvictionReason(reason)
				assert.Equal(t, valCounts[reason].Load(), entryCounts[reason].Load())
				assert.Equal(t, entryCounts[reason].Load(), st.Evictions(r))
				assert.Equal(t, entryWeights[reason].Load(), st.EvictedWeight(r))
			}
		})
	}
}

func TestNewShardedCache_ExplicitShardsOverridesWithShardsOption(t *testing.T) {
	for _, opt := range []Option{WithShards(0), WithShards(-5), WithShards(999)} {
		c := NewShardedCache[int](16, 4, nil, opt, WithInvariantChecking(true))
		sc, ok := c.(*shardedCache[int])
		require.True(t, ok)
		assert.Len(t, sc.shards, 4)
		assert.Equal(t, uint64(16), c.Stats().MaxSize)
	}
}

func runCrossShardABBACallbackTest(t *testing.T, backend Backend) {
	t.Helper()
	var cache Cache[int]
	var unsyncedCallbackCounter int // Protected solely by per-cache evictCallbackMu serialization (-race verifies no data race).
	var reentered atomic.Bool

	cache = NewShardedCache[int](
		4, // 2 shards * capacity 2 each
		2,
		explicitTagShardFunc,
		WithBackend(backend),
		WithInvariantChecking(true),
		WithOnEvictValue(func(_ int, _ EvictionReason) {
			unsyncedCallbackCounter++
		}),
		WithOnEvictEntry(func(k string, v int, r EvictionReason) {
			unsyncedCallbackCounter++
			if r != EvictionReasonCapacity {
				return
			}
			// Cross-shard mutual re-entrancy + child goroutine cross-shard mutation.
			if strings.HasPrefix(k, "s0/w-") || strings.HasPrefix(k, "s0/init-") {
				_, _ = cache.Put(fmt.Sprintf("s1/re-%d", v), v+100)
			} else if strings.HasPrefix(k, "s1/w-") || strings.HasPrefix(k, "s1/init-") {
				_, _ = cache.Put(fmt.Sprintf("s0/re-%d", v), v+200)
			}
			if reentered.CompareAndSwap(false, true) {
				var childWG sync.WaitGroup
				childWG.Go(func() {
					cache.DeletePrefix("s1/re-")
				})
				childWG.Wait()
			}
		}),
	)

	for i := range 2 {
		_, err := cache.Put(fmt.Sprintf("s0/init-%d", i), i)
		require.NoError(t, err)
		_, err = cache.Put(fmt.Sprintf("s1/init-%d", i), i)
		require.NoError(t, err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range 40 {
				_, _ = cache.Put(fmt.Sprintf("s0/w-%d", i), i)
			}
		})
		wg.Go(func() {
			for i := range 40 {
				_, _ = cache.Put(fmt.Sprintf("s1/w-%d", i), i)
			}
		})
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.Failf(t, "deadlock", "cross-shard eviction callbacks deadlocked on %v", backend)
	}
	assert.Positive(t, unsyncedCallbackCounter)
}

func TestShardedCache_CrossShardEvictCallbackSerializationAndABBAReentrancy(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			runCrossShardABBACallbackTest(t, backend)
		})
	}
}

func TestShardedCache_PressureFuncReentrancyAndSingleProbe(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var cache Cache[int]
			var sc *shardedCache[int]
			var pa PressureAwareCache[int]
			var probeCalls atomic.Int64
			var extAfterReentrantOps atomic.Uint64
			var enableReentrancy atomic.Bool

			cache = NewShardedCache[int](
				64,
				16,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(k string, _ int) uint64 {
					if strings.HasPrefix(k, "s2/zero-") {
						return 0
					}
					return 1
				}),
				WithCompactionThreshold(0.70),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					probeCalls.Add(1)
					if !enableReentrancy.Load() {
						return 0.10
					}
					// Re-entrant multi-shard EvaluateMemoryPressure, DeletePrefix, cross-shard Put, and child goroutine Delete.
					_ = pa.EvaluateMemoryPressure()
					cache.DeletePrefix("s1/tmp-")
					_, _ = cache.Put("s2/reentrant", 42)
					var childWG sync.WaitGroup
					childWG.Go(func() {
						_, _ = cache.Delete("s0/tmp")
					})
					childWG.Wait()
					extAfterReentrantOps.Store(sc.coord.externalReclaimEpoch.Load())
					return 0.95
				}),
			)
			sc = cache.(*shardedCache[int])
			pa = cache.(PressureAwareCache[int])

			// Clean empty DeletePrefix("") must invoke PressureFunc 0 times.
			probeCalls.Store(0)
			cache.DeletePrefix("")
			assert.Zero(t, probeCalls.Load(), "clean empty DeletePrefix(\"\") must not sample PressureFunc")

			// Populate entries across shards, including zero-weight entries on shard 2.
			_, err := cache.Put("s0/tmp", 1)
			require.NoError(t, err)
			_, err = cache.Put("s1/tmp-1", 2)
			require.NoError(t, err)
			for i := range 4 {
				_, err = cache.Put(fmt.Sprintf("s0/keep-%d", i), i)
				require.NoError(t, err)
				_, err = cache.Put(fmt.Sprintf("s2/zero-%d", i), i)
				require.NoError(t, err)
			}

			// Non-reentrant EvaluateMemoryPressure and DeletePrefix must each invoke PressureFunc once (not 16 times).
			probeCalls.Store(0)
			_ = pa.EvaluateMemoryPressure()
			assert.Equal(t, int64(1), probeCalls.Load(), "EvaluateMemoryPressure must sample PressureFunc once across all shards")

			probeCalls.Store(0)
			cache.DeletePrefix("nonexistent/")
			assert.Equal(t, int64(1), probeCalls.Load(), "DeletePrefix must sample PressureFunc once across all shards")

			// Re-entrant PressureFunc must complete in O(N) with 1 PressureFunc invocation (no O(N!) livelock or spurious external retry).
			extBefore := sc.coord.externalReclaimEpoch.Load()
			probeCalls.Store(0)
			enableReentrancy.Store(true)

			_ = pa.EvaluateMemoryPressure()
			assert.Equal(t, int64(1), probeCalls.Load(), "re-entrant PressureFunc must not recurse across shards or trigger external-reclaim retry")
			assert.Equal(t, extBefore, extAfterReentrantOps.Load(), "cross-shard re-entrant Delete/DeletePrefix inside PressureFunc must not increment externalReclaimEpoch")

			_, ok := cache.Peek("s2/reentrant")
			assert.True(t, ok, "re-entrant cross-shard Put must survive")
		})
	}

	t.Run("LargeShardCountNoStackOverflow", func(t *testing.T) {
		var pa PressureAwareCache[int]
		var calls atomic.Int64
		c := NewShardedCache[int](
			512,
			256,
			nil,
			WithPressureFunc(func() float64 {
				calls.Add(1)
				_ = pa.EvaluateMemoryPressure()
				return 0.95
			}),
		)
		pa = c.(PressureAwareCache[int])
		_ = pa.EvaluateMemoryPressure()
		assert.Equal(t, int64(1), calls.Load())
	})
}

func TestShardedCache_Tier2RetentionRatioAcrossShards(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"/UnitShardMax", func(t *testing.T) {
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.10))

			// maxSize == shards == 8 -> shardMax == 1 on every shard; 50% retention must retain 4 entries globally (not 8).
			c := NewShardedCache[int](
				8,
				8,
				func(key string, numShards int) int {
					var idx int
					_, _ = fmt.Sscanf(key, "k%d", &idx)
					return idx % numShards
				},
				WithBackend(backend),
				WithInvariantChecking(true),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
			)
			for i := range 8 {
				_, err := c.Put(fmt.Sprintf("k%d", i), i)
				require.NoError(t, err)
			}
			require.Equal(t, uint64(8), c.Stats().CurrentSize)

			pressureBits.Store(math.Float64bits(0.95))
			evicted := c.(PressureAwareCache[int]).EvaluateMemoryPressure()
			assert.Len(t, evicted, 4)
			assert.Equal(t, uint64(4), c.Stats().CurrentSize)

			// Inline foreground Put onto shard 7 (whose shardTier2Target == 0) must still protect the newly inserted entry.
			_, err := c.Put("k7", 777)
			require.NoError(t, err)
			v, ok := c.Peek("k7")
			require.True(t, ok)
			assert.Equal(t, 777, v)
		})

		t.Run(backend.String()+"/SmallRetentionGlobalFloorOne", func(t *testing.T) {
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.10))

			// maxSize = 8, shards = 8, retention = 0.01 -> globalTarget = 1 (not 8).
			c := NewShardedCache[int](
				8,
				8,
				func(key string, numShards int) int {
					var idx int
					_, _ = fmt.Sscanf(key, "k%d", &idx)
					return idx % numShards
				},
				WithBackend(backend),
				WithInvariantChecking(true),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.01),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
			)
			for i := range 8 {
				_, err := c.Put(fmt.Sprintf("k%d", i), i)
				require.NoError(t, err)
			}

			pressureBits.Store(math.Float64bits(0.95))
			evicted := c.(PressureAwareCache[int]).EvaluateMemoryPressure()
			assert.Len(t, evicted, 7)
			assert.Equal(t, uint64(1), c.Stats().CurrentSize)
		})

		t.Run(backend.String()+"/FractionalPerShardQuota", func(t *testing.T) {
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.10))

			// maxSize = 192 across 64 shards (shardMax = 3 each) at 0.50 retention -> must retain 96 (50%), not 64 (33.3%).
			c := NewShardedCache[int](
				192,
				64,
				func(key string, numShards int) int {
					var shard int
					_, _ = fmt.Sscanf(key, "s%d/", &shard)
					return shard % numShards
				},
				WithBackend(backend),
				WithInvariantChecking(true),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
			)
			for s := range 64 {
				for j := range 3 {
					_, err := c.Put(fmt.Sprintf("s%d/k%d", s, j), j)
					require.NoError(t, err)
				}
			}
			require.Equal(t, uint64(192), c.Stats().CurrentSize)

			pressureBits.Store(math.Float64bits(0.95))
			evicted := c.(PressureAwareCache[int]).EvaluateMemoryPressure()
			assert.Len(t, evicted, 96)
			assert.Equal(t, uint64(96), c.Stats().CurrentSize)
		})
	}
}

func runMultiShardDeferredCallbackTest(t *testing.T, backend Backend) {
	t.Helper()
	var pressureBits atomic.Uint64
	pressureBits.Store(math.Float64bits(0.10))

	var firstDeleteSawComplete, firstPressureSawComplete atomic.Bool
	var deleteCBCount, pressureCBCount atomic.Int32
	var panicOnDelete atomic.Bool

	var cacheBySuffix Cache[int]
	cacheBySuffix = NewShardedCache[int](
		12, // 3 shards * capacity 4 each
		3,
		func(key string, numShards int) int {
			switch {
			case strings.HasSuffix(key, "/s0"):
				return 0 % numShards
			case strings.HasSuffix(key, "/s1"):
				return 1 % numShards
			default:
				return 2 % numShards
			}
		},
		WithBackend(backend),
		WithInvariantChecking(true),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			return math.Float64frombits(pressureBits.Load())
		}),
		WithOnEvictEntry(func(_ string, _ int, r EvictionReason) {
			if r == EvictionReasonDeleted {
				if panicOnDelete.Load() {
					panic("boom in DeletePrefix callback")
				}
				if deleteCBCount.Add(1) == 1 {
					_, ok0 := cacheBySuffix.Peek("batch/s0")
					_, ok1 := cacheBySuffix.Peek("batch/s1")
					_, ok2 := cacheBySuffix.Peek("batch/s2")
					firstDeleteSawComplete.Store(!ok0 && !ok1 && !ok2)
					_, _ = cacheBySuffix.Put("batch/s2", 999)
				}
			}
			if r == EvictionReasonPressure && pressureCBCount.Add(1) == 1 {
				firstPressureSawComplete.Store(cacheBySuffix.Stats().CurrentSize <= 6)
			}
		}),
	)

	for _, suf := range []string{"/s0", "/s1", "/s2"} {
		for _, pfx := range []string{"batch", "k1", "k2", "k3"} {
			_, err := cacheBySuffix.Put(pfx+suf, 1)
			require.NoError(t, err)
		}
	}

	cacheBySuffix.DeletePrefix("batch/")
	assert.True(t, firstDeleteSawComplete.Load(), "first DeletePrefix callback must observe all shards already mutated")
	rebuiltVal, rebuiltOK := cacheBySuffix.Peek("batch/s2")
	require.True(t, rebuiltOK, "key re-inserted into later shard during DeletePrefix callback must survive")
	assert.Equal(t, 999, rebuiltVal)

	// Fill shard 0 and shard 1 back to 4 entries each and run EvaluateMemoryPressure at 0.95.
	_, err := cacheBySuffix.Put("k4/s0", 1)
	require.NoError(t, err)
	_, err = cacheBySuffix.Put("k4/s1", 1)
	require.NoError(t, err)
	pressureBits.Store(math.Float64bits(0.95))
	_ = cacheBySuffix.(PressureAwareCache[int]).EvaluateMemoryPressure()
	assert.True(t, firstPressureSawComplete.Load(), "first EvaluateMemoryPressure callback must observe all shards already shed")

	// Callback panic during DeletePrefix must still leave all shards mutated and evictCallbackMu unlocked.
	pressureBits.Store(math.Float64bits(0.10))
	for _, suf := range []string{"/s0", "/s1", "/s2"} {
		_, err = cacheBySuffix.Put("panicpfx"+suf, 1)
		require.NoError(t, err)
	}
	panicOnDelete.Store(true)
	assert.PanicsWithValue(t, "boom in DeletePrefix callback", func() {
		cacheBySuffix.DeletePrefix("panicpfx/")
	})
	panicOnDelete.Store(false)
	for _, suf := range []string{"/s0", "/s1", "/s2"} {
		_, stillThere := cacheBySuffix.Peek("panicpfx" + suf)
		assert.False(t, stillThere, "all shards must be mutated before DeletePrefix callback panics")
	}
	// Subsequent deletion + callback must not deadlock.
	_, err = cacheBySuffix.Put("after-panic/s0", 1)
	require.NoError(t, err)
	_, deleted := cacheBySuffix.Delete("after-panic/s0")
	assert.True(t, deleted)
}

func TestShardedCache_MultiShardDeferredCallbacksAndPanicSafety(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			runMultiShardDeferredCallbackTest(t, backend)
		})
	}
}

func TestShardedCache_MemoryPressureTelemetryCausalOrdering(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"/StalledOlderProbeDoesNotOverwriteNewerCrossShardProbe", func(t *testing.T) {
			var seq atomic.Int32
			g1Started := make(chan struct{})
			allowG1Finish := make(chan struct{})

			c := NewShardedCache[int](
				16,
				2,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					switch seq.Add(1) {
					case 1:
						close(g1Started)
						<-allowG1Finish
						return 0.85 // Older invocation on shard 0 finishes AFTER newer invocation on shard 1
					default:
						return 0.20 // Newer invocation on shard 1
					}
				}),
			)

			g1Done := make(chan struct{})
			go func() {
				defer close(g1Done)
				_, err := c.Put("s0/k", 1)
				assert.NoError(t, err)
			}()
			<-g1Started

			_, err := c.Put("s1/k", 2)
			require.NoError(t, err)
			assert.InDelta(t, 0.20, c.Stats().MemoryPressure, 1e-9)

			close(allowG1Finish)
			<-g1Done

			assert.InDelta(t, 0.20, c.Stats().MemoryPressure, 1e-9, "older stalled probe on shard 0 must not overwrite newer probe on shard 1")
		})

		t.Run(backend.String()+"/RejectedPreReclamationProbeDoesNotOverwriteStats", func(t *testing.T) {
			if backend == BackendRadix {
				return // RadixCache.Compact() does not advance reclaimEpoch on pointer-based nodes
			}
			var armed atomic.Bool
			var sampleNum atomic.Int32
			g1Entered := make(chan struct{})
			reclaimed := make(chan struct{})
			retryStarted := make(chan struct{})
			allowRetryFinish := make(chan struct{})

			c := NewShardedCache[int](
				32,
				2,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if !armed.Load() {
						return 0.30
					}
					switch sampleNum.Add(1) {
					case 1:
						close(g1Entered)
						<-reclaimed
						return 0.95 // Stale pre-reclamation reading rejected at storeSampledPressureWithSeq
					default:
						close(retryStarted)
						<-allowRetryFinish
						return 0.15
					}
				}),
			)
			pa := c.(PressureAwareCache[int])

			for i := range 10 {
				_, err := c.Put(fmt.Sprintf("s1/k%d", i), i)
				require.NoError(t, err)
			}
			for i := range 8 {
				_, _ = c.Delete(fmt.Sprintf("s1/k%d", i))
			}
			require.InDelta(t, 0.30, c.Stats().MemoryPressure, 1e-9)

			armed.Store(true)
			g1Done := make(chan struct{})
			go func() {
				defer close(g1Done)
				_, err := c.Put("s0/write", 1)
				assert.NoError(t, err)
			}()
			<-g1Entered

			// Compact shard 1 while G1's pre-reclamation probe (0.95) is in flight.
			pa.Compact()
			close(reclaimed)

			// Wait until G1 has finished storeSampledPressureWithSeq(0.95) and entered its retry sample.
			<-retryStarted
			assert.InDelta(t, 0.30, c.Stats().MemoryPressure, 1e-9, "rejected pre-reclamation 0.95 sample must not overwrite Stats().MemoryPressure")

			close(allowRetryFinish)
			<-g1Done
			assert.InDelta(t, 0.15, c.Stats().MemoryPressure, 1e-9)
		})
	}
}

func runCrossShardDeleteContentionTest(t *testing.T, backend Backend) {
	t.Helper()
	var armed atomic.Bool
	var inWorker atomic.Bool
	stepReq := make(chan int, 1)
	stepAck := make(chan struct{}, 1)

	c := NewShardedCache[int](
		80, // 8 shards * capacity 10 each, Tier 2 target = 5 each
		8,
		explicitTagShardFunc,
		WithBackend(backend),
		WithInvariantChecking(true),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.85),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.10
			}
			if inWorker.Load() {
				return 0.95
			}
			select {
			case stepReq <- 1:
				<-stepAck
			default:
			}
			return 0.95
		}),
	)

	for s := range 8 {
		for j := range 9 {
			_, err := c.Put(fmt.Sprintf("s%d/k%d", s, j), j)
			require.NoError(t, err)
		}
	}

	stopWorker := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		shardIdx := 1
		for {
			select {
			case <-stopWorker:
				return
			case <-stepReq:
				if shardIdx <= 6 {
					inWorker.Store(true)
					_, _ = c.Delete(fmt.Sprintf("s%d/k0", shardIdx))
					inWorker.Store(false)
					shardIdx++
				}
				stepAck <- struct{}{}
			}
		}
	}()

	armed.Store(true)
	evicted, err := c.Put("s0/hot", 999)
	close(stopWorker)
	<-workerDone
	require.NoError(t, err)
	assert.Len(t, evicted, 5, "shard 0 must still shed from 10 down to shardTier2Target=5 despite cross-shard epoch contention")
	sc := c.(*shardedCache[int])
	assert.Equal(t, uint64(5), sc.shards[0].Stats().CurrentSize)
}

func runCrossShardCompactContentionTest(t *testing.T, backend Backend) {
	t.Helper()
	if backend == BackendRadix {
		return // Pointer-based RadixCache.Compact() does not advance reclaimEpoch.
	}
	var armed atomic.Bool
	stepReq := make(chan struct{}, 1)
	stepAck := make(chan struct{}, 1)

	c := NewShardedCache[int](
		80,
		8,
		explicitTagShardFunc,
		WithBackend(backend),
		WithInvariantChecking(true),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.85),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.10
			}
			select {
			case stepReq <- struct{}{}:
				<-stepAck
			default:
			}
			return 0.95
		}),
	)

	// Create slack on shards 1..6 and populate shard 0 with 9 entries at healthy 0.10 pressure.
	for s := 1; s <= 6; s++ {
		for j := range 10 {
			_, err := c.Put(fmt.Sprintf("s%d/tmp%d", s, j), j)
			require.NoError(t, err)
		}
		for j := range 8 {
			_, _ = c.Delete(fmt.Sprintf("s%d/tmp%d", s, j))
		}
	}
	for j := range 9 {
		_, err := c.Put(fmt.Sprintf("s0/k%d", j), j)
		require.NoError(t, err)
	}
	require.InDelta(t, 0.10, c.Stats().MemoryPressure, 1e-9)

	sc := c.(*shardedCache[int])
	stopWorker := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		shardIdx := 1
		for {
			select {
			case <-stopWorker:
				return
			case <-stepReq:
				if shardIdx <= 6 {
					sc.shards[shardIdx].Compact()
					shardIdx++
				}
				stepAck <- struct{}{}
			}
		}
	}()

	armed.Store(true)
	evicted, err := c.Put("s0/hot", 999)
	close(stopWorker)
	<-workerDone
	require.NoError(t, err)
	assert.Len(t, evicted, 5, "shard 0 must shed to shardTier2Target=5 despite 6 cross-shard compactions")
	assert.Equal(t, uint64(5), sc.shards[0].Stats().CurrentSize)
	assert.InDelta(t, 0.95, c.Stats().MemoryPressure, 1e-9, "Stats().MemoryPressure must reflect accepted 0.95 spike after cross-shard compactions")
}

func TestShardedCache_CrossShardEpochContentionPreservesSheddingAndTelemetry(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"/ConcurrentCrossShardDeletes", func(t *testing.T) {
			runCrossShardDeleteContentionTest(t, backend)
		})
		t.Run(backend.String()+"/ConcurrentCrossShardCompactions", func(t *testing.T) {
			runCrossShardCompactContentionTest(t, backend)
		})
	}
}

func runSameShardRetryExhaustionMultiShardOpTest(t *testing.T, backend Backend, mode string) {
	t.Helper()
	var armed atomic.Bool
	var inWorker atomic.Bool
	stepReq := make(chan struct{}, 1)
	stepAck := make(chan struct{}, 1)

	c := NewShardedCache[int](
		120, // 8 shards * capacity 15 each, Tier 2 target = 7..8 (globalTarget = 60 -> 4 shards of 8, 4 shards of 7)
		8,
		explicitTagShardFunc,
		WithBackend(backend),
		WithInvariantChecking(true),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.85),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.10
			}
			if inWorker.Load() {
				return 0.95
			}
			select {
			case stepReq <- struct{}{}:
				<-stepAck
			default:
			}
			return 0.95
		}),
	)

	for s := range 8 {
		_, err := c.Put(fmt.Sprintf("s%d/match", s), 100)
		require.NoError(t, err)
		for j := range 13 {
			_, err = c.Put(fmt.Sprintf("s%d/k%d", s, j), j)
			require.NoError(t, err)
		}
	}

	// Exhaust sameShardRetries on shard 0 alone (3 same-shard reclamations on shard 0).
	stopWorker := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		delIdx := 0
		for {
			select {
			case <-stopWorker:
				return
			case <-stepReq:
				if delIdx < 3 {
					inWorker.Store(true)
					_, _ = c.Delete(fmt.Sprintf("s0/k%d", delIdx))
					inWorker.Store(false)
					delIdx++
				}
				stepAck <- struct{}{}
			}
		}
	}()

	armed.Store(true)
	pa := c.(PressureAwareCache[int])
	switch mode {
	case "evaluate":
		evicted := pa.EvaluateMemoryPressure()
		assert.NotEmpty(t, evicted, "EvaluateMemoryPressure must still shed shards 1..7 when shard 0 exhausts same-shard retries")
		assert.LessOrEqual(t, c.Stats().CurrentSize, uint64(60))
	case "prefix_nonempty":
		c.DeletePrefix("s1/match")
		sc := c.(*shardedCache[int])
		assert.LessOrEqual(t, sc.shards[1].Stats().CurrentSize, uint64(8), "DeletePrefix must still shed matching later shards")
	case "prefix_empty":
		c.DeletePrefix("")
		st := c.Stats()
		assert.Zero(t, st.Len)
		if backend != BackendRadix {
			assert.GreaterOrEqual(t, st.CompactionsPressureTier2, uint64(7), "DeletePrefix(\"\") on shards 1..7 must attribute slack compactions to Tier 2 pressure")
		}
	}
	close(stopWorker)
	<-workerDone
}

func TestShardedCache_SameShardRetryExhaustionDoesNotPoisonRemainingShards(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		for _, mode := range []string{"evaluate", "prefix_nonempty", "prefix_empty"} {
			t.Run(fmt.Sprintf("%s/%s", backend.String(), mode), func(t *testing.T) {
				runSameShardRetryExhaustionMultiShardOpTest(t, backend, mode)
			})
		}
	}
}

func runZeroWeightEpisodeRecoveryTest(t *testing.T, backend Backend, includeWeighted bool) {
	t.Helper()
	var pressureBits atomic.Uint64
	pressureBits.Store(math.Float64bits(0.10))

	c := NewShardedCache[string](
		100,
		2,
		explicitTagShardFunc,
		WithBackend(backend),
		WithInvariantChecking(true),
		WithWeigher(func(_, v string) uint64 {
			return uint64(len(v))
		}),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			return math.Float64frombits(pressureBits.Load())
		}),
	)
	pa := c.(PressureAwareCache[string])

	if includeWeighted {
		for i := range 2 {
			_, err := c.Put(fmt.Sprintf("s1/w%d", i), "x")
			require.NoError(t, err)
		}
	}
	for i := range 4 {
		_, err := c.Put(fmt.Sprintf("s1/z%d", i), "")
		require.NoError(t, err)
	}
	require.Equal(t, 4, c.Stats().ZeroSizeCount)

	// Episode 1: critical pressure (0.95) sheds 4 -> 2 zero-weight entries on shard 1, and repeat is idempotent.
	pressureBits.Store(math.Float64bits(0.95))
	_ = pa.EvaluateMemoryPressure()
	require.Equal(t, 2, c.Stats().ZeroSizeCount)
	assert.Empty(t, pa.EvaluateMemoryPressure(), "repeated evaluation in Episode 1 must remain idempotent")
	require.Equal(t, 2, c.Stats().ZeroSizeCount)

	// Healthy window (0.10): foreground writes/deletes hit shard 0 only; shard 1 only serves Get/Peek reads.
	pressureBits.Store(math.Float64bits(0.10))
	_, err := c.Put("s0/healthy", "ok")
	require.NoError(t, err)
	_, _ = c.Delete("s0/missing")
	_, _ = c.Get("s1/z2")
	_, _ = c.Peek("s1/z3")
	require.InDelta(t, 0.10, c.Stats().MemoryPressure, 1e-9)

	// Episode 2: critical pressure (0.95) must shed 2 -> 1 zero-weight entry on shard 1 despite no healthy-window writes on shard 1.
	pressureBits.Store(math.Float64bits(0.95))
	evictedEp2 := pa.EvaluateMemoryPressure()
	assert.NotEmpty(t, evictedEp2, "Episode 2 must shed zero-weight entries on shard 1 after cache-wide healthy recovery")
	assert.Equal(t, 1, c.Stats().ZeroSizeCount)
	assert.Empty(t, pa.EvaluateMemoryPressure(), "repeated evaluation in Episode 2 must remain idempotent")
}

func TestShardedCache_ZeroWeightWatermarkResetsAcrossPressureEpisodes(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"/PureZeroWeight", func(t *testing.T) {
			runZeroWeightEpisodeRecoveryTest(t, backend, false)
		})
		t.Run(backend.String()+"/MixedWeightedAndZeroWeight", func(t *testing.T) {
			runZeroWeightEpisodeRecoveryTest(t, backend, true)
		})
	}
}

func holdShardRLockViaKeys[V any](t *testing.T, c Cache[V], shardPrefix string) func() {
	t.Helper()
	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for k := range c.Keys() {
			if strings.HasPrefix(k, shardPrefix) {
				close(locked)
				<-release
				return
			}
		}
	}()
	<-locked
	releaseFn := sync.OnceFunc(func() {
		close(release)
		<-done
	})
	t.Cleanup(releaseFn)
	return releaseFn
}

func runConcurrentHealthyAndCriticalEpisodeTest(t *testing.T, backend Backend, sharded bool) {
	t.Helper()
	var mode atomic.Int32
	g1Entered := make(chan struct{})
	releaseG1 := make(chan struct{})
	g2Entered := make(chan struct{})
	releaseG2 := make(chan struct{})

	opts := []Option{
		WithBackend(backend),
		WithInvariantChecking(true),
		WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			switch mode.Load() {
			case 0:
				return 0.10
			case 1:
				return 0.95
			case 2, 3:
				if mode.CompareAndSwap(2, 3) {
					close(g1Entered)
					<-releaseG1
					return 0.10
				}
				if mode.CompareAndSwap(3, 4) {
					close(g2Entered)
					<-releaseG2
					return 0.95
				}
				return 0.95
			default:
				return 0.95
			}
		}),
	}

	var c Cache[string]
	if sharded {
		c = NewShardedCache[string](100, 2, explicitTagShardFunc, opts...)
	} else {
		c = New[string](100, opts...)
	}
	pa := c.(PressureAwareCache[string])

	for i := range 4 {
		_, err := c.Put(fmt.Sprintf("s1/z%d", i), "")
		require.NoError(t, err)
	}

	if sharded {
		// Episode 1 sheds 4 -> 2 on shard 1.
		mode.Store(1)
		require.Len(t, pa.EvaluateMemoryPressure(), 2)
		require.Equal(t, 2, c.Stats().ZeroSizeCount)
	}

	// Coordinate G1 (healthy 0.10) and G2 (critical 0.95) so both enter PressureFunc concurrently,
	// G1 commits 0.10 first, and G2 commits 0.95 second.
	mode.Store(2)
	g1Done := make(chan struct{})
	go func() {
		defer close(g1Done)
		_, _ = c.Put("s0/healthy", "x")
	}()
	<-g1Entered

	g2Done := make(chan []string, 1)
	go func() {
		g2Done <- pa.EvaluateMemoryPressure()
	}()
	<-g2Entered

	close(releaseG1)
	<-g1Done
	require.InDelta(t, 0.10, c.Stats().MemoryPressure, 1e-9)

	close(releaseG2)
	evictedG2 := <-g2Done
	require.InDelta(t, 0.95, c.Stats().MemoryPressure, 1e-9)

	if sharded {
		assert.Len(t, evictedG2, 1, "Episode 2 must shed shard 1 (2 -> 1) after G1 committed healthy recovery")
		assert.Equal(t, 1, c.Stats().ZeroSizeCount)
	} else {
		assert.Len(t, evictedG2, 2, "Episode 1 must shed 4 -> 2 zero-weight entries")
		assert.Equal(t, 2, c.Stats().ZeroSizeCount)
	}
	assert.Empty(t, pa.EvaluateMemoryPressure(), "subsequent evaluation in the same uninterrupted 0.95 episode must not over-shed")
}

func TestShardedCache_ConcurrentHealthyAndCriticalProbesPreserveEpisodeEpochs(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"/ShardedCrossEpisodeNotSuppressed", func(t *testing.T) {
			runConcurrentHealthyAndCriticalEpisodeTest(t, backend, true)
		})
		t.Run(backend.String()+"/UnshardedSingleEpisodeNoOverShed", func(t *testing.T) {
			runConcurrentHealthyAndCriticalEpisodeTest(t, backend, false)
		})
	}
}

func TestShardedCache_ConcurrentCrossShardInvalidationDoesNotProduceTornZeroSample(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var armed atomic.Bool
			c := NewShardedCache[string](
				800,
				8,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
				WithCompactionThreshold(0.70),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					if !armed.Load() {
						return 0.10
					}
					return 0.95
				}),
			)
			pa := c.(PressureAwareCache[string])
			sc := c.(*shardedCache[string])

			for i := range 4 {
				_, err := c.Put(fmt.Sprintf("s7/z%d", i), "")
				require.NoError(t, err)
			}

			// Enter sustained 0.95 Episode 1 and shed shard 7 from 4 -> 2 zero-weight entries.
			armed.Store(true)
			require.Len(t, pa.EvaluateMemoryPressure(), 2)
			require.Equal(t, 2, sc.shards[7].Stats().ZeroSizeCount)

			stop := make(chan struct{})
			var wg sync.WaitGroup
			for s := 1; s <= 5; s++ {
				shardID := s
				wg.Go(func() {
					val := strings.Repeat("x", 10)
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
							k := fmt.Sprintf("s%d/churn-%d", shardID, i&7)
							_, _ = c.Put(k, val)
							_, _ = c.Delete(k)
						}
					}
				})
			}
			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
						_, _ = c.Delete("s6/nonexistent")
					}
				}
			})

			for range 150 {
				_ = pa.EvaluateMemoryPressure()
				require.Equal(t, 2, sc.shards[7].Stats().ZeroSizeCount,
					"concurrent cross-shard invalidation must never produce a torn 0.0 sample that wipes shard 7 watermarks mid-episode")
			}

			close(stop)
			wg.Wait()
		})
	}
}

func TestShardedCache_ReentrantShardReclaimInPressureFuncPreservesCrossShardFallback(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var armed atomic.Bool
			var inWorker atomic.Bool
			stepReq := make(chan struct{}, 1)
			stepAck := make(chan struct{}, 1)

			var c Cache[int]
			c = NewShardedCache[int](
				40, // 4 shards * capacity 10 each, shardTier2Target = 5
				4,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithCompactionThreshold(0.70),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					if !armed.Load() {
						return 0.10
					}
					if inWorker.Load() {
						return 0.95
					}
					// Perform a supported re-entrant Put + Delete on target shard 0 before measuring 0.95.
					_, _ = c.Put("s0/reentrant-scratch", 1)
					_, _ = c.Delete("s0/reentrant-scratch")
					select {
					case stepReq <- struct{}{}:
						<-stepAck
					default:
					}
					return 0.95
				}),
			)

			for j := range 9 {
				_, err := c.Put(fmt.Sprintf("s0/k%d", j), j)
				require.NoError(t, err)
			}
			for s := 1; s <= 3; s++ {
				for j := range 3 {
					_, err := c.Put(fmt.Sprintf("s%d/k%d", s, j), j)
					require.NoError(t, err)
				}
			}

			stopWorker := make(chan struct{})
			workerDone := make(chan struct{})
			go func() {
				defer close(workerDone)
				step := 0
				for {
					select {
					case <-stopWorker:
						return
					case <-stepReq:
						if step < 6 {
							shardIdx := (step % 3) + 1
							keyIdx := step / 3
							inWorker.Store(true)
							_, _ = c.Delete(fmt.Sprintf("s%d/k%d", shardIdx, keyIdx))
							inWorker.Store(false)
							step++
						}
						stepAck <- struct{}{}
					}
				}
			}()

			armed.Store(true)
			evicted, err := c.Put("s0/hot", 999)
			close(stopWorker)
			<-workerDone

			require.NoError(t, err)
			assert.Len(t, evicted, 5, "re-entrant PressureFunc reclaim on shard 0 must not cause cross-shard deletes to drop 0.95 fallback")
			sc := c.(*shardedCache[int])
			assert.Equal(t, uint64(5), sc.shards[0].Stats().CurrentSize)
		})
	}
}

func TestShardedCache_StaleFallbackAfterReclaimDoesNotClobberTelemetryOrOverShed(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var g1Armed atomic.Bool
			var inWorker atomic.Bool
			var spikeCritical atomic.Bool
			var g1SampleCount atomic.Int32
			stepReq := make(chan struct{}, 1)
			stepAck := make(chan struct{}, 1)
			fifthSampleReady := make(chan struct{})
			releaseFifthSample := make(chan struct{})

			c := NewShardedCache[string](
				80,
				8,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
				WithCompactionThreshold(0.70),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					if spikeCritical.Load() {
						return 0.95
					}
					if !g1Armed.Load() {
						return 0.10
					}
					if inWorker.Load() {
						return 0.95
					}
					n := g1SampleCount.Add(1)
					if n <= 4 {
						stepReq <- struct{}{}
						<-stepAck
						return 0.10
					}
					if n == 5 {
						close(fifthSampleReady)
						<-releaseFifthSample
						return 0.10
					}
					return 0.10
				}),
			)
			pa := c.(PressureAwareCache[string])
			sc := c.(*shardedCache[string])

			_, err := c.Put("s1/anchor", "a")
			require.NoError(t, err)
			for s := 2; s <= 5; s++ {
				_, err = c.Put(fmt.Sprintf("s%d/del", s), "x")
				require.NoError(t, err)
			}
			for i := range 4 {
				_, err = c.Put(fmt.Sprintf("s7/z%d", i), "")
				require.NoError(t, err)
			}

			stopWorker := make(chan struct{})
			workerDone := make(chan struct{})
			go func() {
				defer close(workerDone)
				shardIdx := 2
				for {
					select {
					case <-stopWorker:
						return
					case <-stepReq:
						if shardIdx <= 5 {
							inWorker.Store(true)
							_, _ = c.Delete(fmt.Sprintf("s%d/del", shardIdx))
							inWorker.Store(false)
							shardIdx++
						}
						stepAck <- struct{}{}
					}
				}
			}()

			g1Armed.Store(true)
			g1Done := make(chan struct{})
			go func() {
				defer close(g1Done)
				_, _ = c.Put("s1/item", "v")
			}()
			<-fifthSampleReady

			// Hold RLock on shard 1 so G1 finishes its 5th (0.10) sample and parks on s1.mu.Lock().
			releaseS1 := holdShardRLockViaKeys(t, c, "s1/")
			close(releaseFifthSample)

			// Wait until G1's 0.10 sample has committed to telemetry (query shard 0 to avoid blocking behind G1 on s1.mu.Lock()).
			require.Eventually(t, func() bool {
				return math.Abs(sc.shards[0].Stats().MemoryPressure-0.10) < 1e-9
			}, time.Second, time.Millisecond)

			// Spike pressure permanently to 0.95 and shed shard 7 (5 -> 2 zero-weight entries), advancing reclaimEpoch.
			spikeCritical.Store(true)
			evictedS7, err := c.Put("s7/z4", "")
			require.NoError(t, err)
			require.Len(t, evictedS7, 3)
			require.Equal(t, 2, sc.shards[7].Stats().ZeroSizeCount)
			require.InDelta(t, 0.95, sc.shards[0].Stats().MemoryPressure, 1e-9)

			// Release shard 1 so G1 enters resolvePostRetryPressureLocked -> recordAcceptedFallbackSample with its older 0.10 sample.
			releaseS1()
			<-g1Done
			close(stopWorker)
			<-workerDone

			assert.InDelta(t, 0.95, c.Stats().MemoryPressure, 1e-9,
				"stale pre-spike fallback sample must not overwrite Stats().MemoryPressure backwards from 0.95 to 0.10")
			assert.Empty(t, pa.EvaluateMemoryPressure(),
				"stale pre-spike fallback sample must not advance belowTier2Epoch or cause repeated zero-weight shedding in the same 0.95 crisis")
			assert.Equal(t, 2, sc.shards[7].Stats().ZeroSizeCount)
		})
	}
}

func runStragglerMultiShardWatermarkTest(t *testing.T, backend Backend, healthyDipStraggler bool) {
	t.Helper()
	var pressureBits atomic.Uint64
	pressureBits.Store(math.Float64bits(0.10))

	c := NewShardedCache[string](
		90,
		3,
		explicitTagShardFunc,
		WithBackend(backend),
		WithInvariantChecking(true),
		WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			return math.Float64frombits(pressureBits.Load())
		}),
	)
	pa := c.(PressureAwareCache[string])
	sc := c.(*shardedCache[string])

	_, err := c.Put("s0/anchor", "a")
	require.NoError(t, err)
	_, err = c.Put("s1/hold", "b")
	require.NoError(t, err)
	for i := range 4 {
		_, err = c.Put(fmt.Sprintf("s2/z%d", i), "")
		require.NoError(t, err)
	}

	// Episode 1: spike to 0.95.
	pressureBits.Store(math.Float64bits(0.95))
	if healthyDipStraggler {
		// Shed shard 2 in Episode 1 (4 -> 2), then replenish to 4 during the dip before straggler starts.
		require.Len(t, pa.EvaluateMemoryPressure(), 2)
		pressureBits.Store(math.Float64bits(0.10))
		_, err = c.Put("s2/z4", "")
		require.NoError(t, err)
		_, err = c.Put("s2/z5", "")
		require.NoError(t, err)
		require.Equal(t, 4, sc.shards[2].Stats().ZeroSizeCount)
	}

	// Hold RLock on shard 1 so a multi-shard operation completes shard 0 and parks waiting for shard 1.
	releaseS1 := holdShardRLockViaKeys(t, c, "s1/")
	epochBeforeStraggler := sc.shards[0].Stats().DeletePrefixExecuted
	stragglerDone := make(chan struct{})
	go func() {
		defer close(stragglerDone)
		if healthyDipStraggler {
			c.DeletePrefix("nomatch/")
		} else {
			_ = pa.EvaluateMemoryPressure()
		}
	}()

	if healthyDipStraggler {
		require.Eventually(t, func() bool {
			return sc.shards[0].Stats().DeletePrefixExecuted > epochBeforeStraggler && !shardTryRLock[string](sc.shards[1])
		}, time.Second, time.Millisecond)
	} else {
		// EvaluateMemoryPressure visits shards[0] then shards[1] sequentially;
		// shards[1] TryRLock fails only after shards[0] has unlocked and shards[1].mu.Lock() is queued.
		require.Eventually(t, func() bool {
			return !shardTryRLock[string](sc.shards[1])
		}, time.Second, time.Millisecond)
		// Recover briefly to 0.10 on shard 0 to advance belowTier2Epoch before Episode 2.
		pressureBits.Store(math.Float64bits(0.10))
		_, err = c.Put("s0/dip", "d")
		require.NoError(t, err)
	}

	// Enter Episode 2 (0.95) and shed shard 2 via foreground Put("s2/ep2", "") from 5 -> 2 zero-weight entries.
	pressureBits.Store(math.Float64bits(0.95))
	evictedEp2, err := c.Put("s2/ep2", "")
	require.NoError(t, err)
	require.Len(t, evictedEp2, 3)
	require.Equal(t, 2, sc.shards[2].Stats().ZeroSizeCount)

	// Release shard 1 so the older multi-shard straggler reaches shard 2.
	releaseS1()
	<-stragglerDone

	assert.Equal(t, 2, sc.shards[2].Stats().ZeroSizeCount,
		"older multi-shard straggler must not shed shard 2 a second time in Episode 2")
	assert.Empty(t, pa.EvaluateMemoryPressure(),
		"older multi-shard straggler must not rewind or clear shard 2 watermark for subsequent Episode 2 evaluations")
	assert.Equal(t, 2, sc.shards[2].Stats().ZeroSizeCount)
}

func shardTryRLock[V any](s any) bool {
	switch impl := s.(type) {
	case *mapCache[V]:
		if impl.mu.TryRLock() {
			impl.mu.RUnlock()
			return true
		}
		return false
	case *radixCache[V]:
		if impl.mu.TryRLock() {
			impl.mu.RUnlock()
			return true
		}
		return false
	case *arenaRadix[V]:
		if impl.mu.TryRLock() {
			impl.mu.RUnlock()
			return true
		}
		return false
	default:
		panic(fmt.Sprintf("unexpected cache type %T", s))
	}
}

func TestShardedCache_StragglerMultiShardSampleDoesNotRewindOrWipeEpisodeWatermarks(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"/CriticalEpisode1Straggler", func(t *testing.T) {
			runStragglerMultiShardWatermarkTest(t, backend, false)
		})
		t.Run(backend.String()+"/HealthyDipStraggler", func(t *testing.T) {
			runStragglerMultiShardWatermarkTest(t, backend, true)
		})
	}
}

func TestShardedCache_OverflowSamplerCrossShardReentrantReclaimDoesNotLeakGIDs(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var armed atomic.Bool
			var slotCall atomic.Int32
			slot1Ready := make(chan struct{})
			slot2Ready := make(chan struct{})
			releaseSlots := make(chan struct{})

			var c Cache[string]
			c = NewShardedCache[string](
				120,
				3,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if !armed.Load() {
						return 0.10
					}
					switch slotCall.Add(1) {
					case 1:
						close(slot1Ready)
						<-releaseSlots
						return 0.10
					case 2:
						close(slot2Ready)
						<-releaseSlots
						return 0.10
					default:
						// Overflow sampler targeting shard 0 re-entrantly reclaims on shards 1 and 2.
						_, _ = c.Put("s1/tmp", "val")
						_, _ = c.Delete("s1/tmp")
						var childWG sync.WaitGroup
						childWG.Go(func() {
							_, _ = c.Put("s2/tmp", "val")
							_, _ = c.Delete("s2/tmp")
						})
						childWG.Wait()
						return 0.10
					}
				}),
			)
			pa := c.(PressureAwareCache[string])
			sc := c.(*shardedCache[string])

			_, err := c.Put("s0/anchor", "a")
			require.NoError(t, err)
			armed.Store(true)

			var holderWG sync.WaitGroup
			holderWG.Go(func() {
				_, _ = c.Put("s0/h1", "1")
			})
			<-slot1Ready
			holderWG.Go(func() {
				_, _ = c.Put("s0/h2", "2")
			})
			<-slot2Ready

			var overflowWG sync.WaitGroup
			for range 16 {
				overflowWG.Go(func() {
					_ = pa.EvaluateMemoryPressure()
				})
			}
			overflowWG.Wait()

			close(releaseSlots)
			holderWG.Wait()

			assert.Zero(t, sc.coord.reentrantReclaimCount.Load())
			coordLeaked := 0
			sc.coord.reentrantReclaimGIDs.Range(func(_, _ any) bool {
				coordLeaked++
				return true
			})
			assert.Zero(t, coordLeaked, "coord.reentrantReclaimGIDs must not retain exited overflow GIDs")

			for i, shard := range sc.shards {
				ps := shard.pressureStatePtr()
				assert.Zero(t, ps.reentrantReclaimCount.Load(), "shard %d reentrantReclaimCount must be 0", i)
				leaked := 0
				ps.reentrantReclaimGIDs.Range(func(_, _ any) bool {
					leaked++
					return true
				})
				assert.Zero(t, leaked, "shard %d reentrantReclaimGIDs must not leak exited overflow GIDs", i)
			}
		})
	}
}

func TestShardedCache_DeletePrefixPressurePanicTelemetryAndCallbacks(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"/InitialSamplePanicDoesNotIncrementExecuted", func(t *testing.T) {
			var shouldPanic atomic.Bool
			probe := func() float64 {
				if shouldPanic.Load() {
					panic("boom-initial-sample")
				}
				return 0.10
			}

			unsharded := New[string](100, WithBackend(backend), WithInvariantChecking(true), WithPressureFunc(probe))
			sharded := NewShardedCache[string](100, 2, explicitTagShardFunc, WithBackend(backend), WithInvariantChecking(true), WithPressureFunc(probe))

			// Populate only shard 1 on the sharded cache so shard 0 is clean-empty for DeletePrefix("").
			_, err := unsharded.Put("s1/item", "v")
			require.NoError(t, err)
			_, err = sharded.Put("s1/item", "v")
			require.NoError(t, err)

			shouldPanic.Store(true)
			for _, prefix := range []string{"s1/", ""} {
				assert.PanicsWithValue(t, "boom-initial-sample", func() {
					unsharded.DeletePrefix(prefix)
				})
				assert.PanicsWithValue(t, "boom-initial-sample", func() {
					sharded.DeletePrefix(prefix)
				})
				assert.Zero(t, unsharded.Stats().DeletePrefixExecuted)
				assert.Zero(t, sharded.Stats().DeletePrefixExecuted, "prefix %q must not increment DeletePrefixExecuted when initial PressureFunc panics", prefix)
				assert.Equal(t, 1, sharded.Stats().Len)
			}
		})

		t.Run(backend.String()+"/ResamplePanicOnLaterShardDeliversEarlierShardCallbacks", func(t *testing.T) {
			var armed atomic.Bool
			var inWorker atomic.Bool
			var nonWorkerSamples atomic.Int32
			var evictValCount atomic.Int32
			var evictEntryKeys sync.Map
			deleteReq := make(chan string)
			deleteAck := make(chan struct{})

			c := NewShardedCache[string](
				100,
				2,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithCompactionThreshold(0.50),
				WithOnEvictValue(func(_ string, _ EvictionReason) {
					evictValCount.Add(1)
				}),
				WithOnEvictEntry(func(k, _ string, _ EvictionReason) {
					evictEntryKeys.Store(k, true)
				}),
				WithPressureFunc(func() float64 {
					if !armed.Load() || inWorker.Load() {
						return 0.60
					}
					n := nonWorkerSamples.Add(1)
					if n <= 3 {
						// Signal pre-spawned non-descendant worker to delete on shard 0, exhausting sameShardRetries (> 2).
						deleteReq <- fmt.Sprintf("s0/r%d", n)
						<-deleteAck
						return 0.60
					}
					panic("boom-shard1-resample")
				}),
			)

			var workerWG sync.WaitGroup
			workerWG.Go(func() {
				for key := range deleteReq {
					inWorker.Store(true)
					_, _ = c.Delete(key)
					inWorker.Store(false)
					deleteAck <- struct{}{}
				}
			})
			t.Cleanup(func() {
				close(deleteReq)
				workerWG.Wait()
			})

			_, err := c.Put("s0/target", "v0")
			require.NoError(t, err)
			for i := 1; i <= 3; i++ {
				_, err = c.Put(fmt.Sprintf("s0/r%d", i), "r")
				require.NoError(t, err)
			}
			_, err = c.Put("s1/target", "v1")
			require.NoError(t, err)

			// Run DeletePrefix on its own goroutine so workerWG is not a child goroutine of the sampler.
			armed.Store(true)
			panicCh := make(chan any, 1)
			go func() {
				defer func() {
					panicCh <- recover()
				}()
				c.DeletePrefix("s")
			}()
			assert.Equal(t, "boom-shard1-resample", <-panicCh)

			_, ok0 := c.Peek("s0/target")
			assert.False(t, ok0, "shard 0 target was deleted before shard 1 resample panic")
			_, ok1 := c.Peek("s1/target")
			assert.True(t, ok1, "shard 1 target must remain intact after shard 1 pre-lock panic")

			st := c.Stats()
			assert.Equal(t, uint64(1), st.DeletePrefixExecuted)
			assert.Equal(t, uint64(4), st.EvictionsDeleted)
			assert.Equal(t, int32(4), evictValCount.Load(), "OnEvictValue must be delivered for shard 0 deletions despite shard 1 panic")
			_, deliveredS0 := evictEntryKeys.Load("s0/target")
			assert.True(t, deliveredS0, "OnEvictEntry must be delivered for s0/target despite shard 1 panic")
		})
	}
}

func TestShardedCache_ReentrantFastForwardPreservesCausalOrderAndWatermarks(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var mode atomic.Int32
			g1Sampled := make(chan struct{})
			g2Done := make(chan struct{})

			var c Cache[string]
			c = NewShardedCache[string](
				90,
				3,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
				WithCompactionThreshold(0.70),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					switch mode.Load() {
					case 0:
						return 0.10
					case 1:
						return 0.95
					case 2:
						// G1 re-entrantly reclaims on shard 0, reads transient 0.10, and pauses while G2 samples 0.95.
						mode.Store(3)
						_, _ = c.Delete("s0/reclaim")
						close(g1Sampled)
						<-g2Done
						return 0.10
					default:
						return 0.95
					}
				}),
			)
			pa := c.(PressureAwareCache[string])
			sc := c.(*shardedCache[string])

			_, err := c.Put("s0/reclaim", "rr")
			require.NoError(t, err)
			_, err = c.Put("s0/anchor", "aa")
			require.NoError(t, err)
			for i := range 4 {
				_, err = c.Put(fmt.Sprintf("s1/z%d", i), "")
				require.NoError(t, err)
			}

			// Episode 1: shed shard 1 from 4 -> 2 zero-weight entries at 0.95.
			mode.Store(1)
			require.Len(t, pa.EvaluateMemoryPressure(), 2)
			require.Equal(t, 2, sc.shards[1].Stats().ZeroSizeCount)

			// Start G1 on shard 0 (mode 2 -> re-entrant Delete + transient 0.10).
			mode.Store(2)
			g1Finished := make(chan struct{})
			go func() {
				defer close(g1Finished)
				_, putErr := c.Put("s0/new", "n")
				assert.NoError(t, putErr)
			}()
			<-g1Sampled

			// G2 runs at mode 3 (0.95) on shard 2 without reclaiming, publishing newer seq at 0.95.
			_, err = c.Put("s2/item", "m")
			require.NoError(t, err)
			close(g2Done)
			<-g1Finished

			assert.InDelta(t, 0.95, c.Stats().MemoryPressure, 1e-9,
				"stale re-entrant fast-forwarded 0.10 sample must not overwrite newer 0.95 telemetry")
			assert.Empty(t, pa.EvaluateMemoryPressure(),
				"stale re-entrant fast-forwarded 0.10 sample must not advance belowTier2Epoch or over-shed shard 1")
			assert.Equal(t, 2, sc.shards[1].Stats().ZeroSizeCount)
		})
	}
}

func TestShardedCache_ChildSamplerRetryExhaustionDoesNotPropagateStaleWatermark(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.10))
			var trapParent atomic.Bool
			parentEntered := make(chan struct{})
			releaseParent := make(chan struct{})
			childDone := make(chan struct{})

			var c Cache[string]
			var pa PressureAwareCache[string]
			c = NewShardedCache[string](
				120,
				4,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
				WithCompactionThreshold(0.50),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					if trapParent.CompareAndSwap(true, false) {
						go func() {
							defer close(childDone)
							_ = pa.EvaluateMemoryPressure()
						}()
						close(parentEntered)
						<-releaseParent
					}
					return math.Float64frombits(pressureBits.Load())
				}),
			)
			pa = c.(PressureAwareCache[string])
			sc := c.(*shardedCache[string])

			_, err := c.Put("s0/anchor", "a")
			require.NoError(t, err)
			_, err = c.Put("s1/hold", "b")
			require.NoError(t, err)
			for i := range 4 {
				_, err = c.Put(fmt.Sprintf("s2/z%d", i), "")
				require.NoError(t, err)
			}
			for i := range 5 {
				_, err = c.Put(fmt.Sprintf("s3/r%d", i), "r")
				require.NoError(t, err)
			}

			// Episode 1: shed shard 2 from 4 -> 2 zero-weight entries.
			pressureBits.Store(math.Float64bits(0.95))
			require.Len(t, pa.EvaluateMemoryPressure(), 2)
			require.Equal(t, 2, sc.shards[2].Stats().ZeroSizeCount)

			// Healthy recovery on shard 3 advances coord.belowTier2Epoch without touching shard 0 or shard 2.
			pressureBits.Store(math.Float64bits(0.10))
			_, err = c.Put("s3/dip", "d")
			require.NoError(t, err)

			// Episode 2: spike to 0.95, hold RLocks on shards 0 and 1, and trap parent PressureFunc.
			pressureBits.Store(math.Float64bits(0.95))
			releaseS0 := holdShardRLockViaKeys(t, c, "s0/")
			releaseS1 := holdShardRLockViaKeys(t, c, "s1/")
			trapParent.Store(true)

			parentDone := make(chan struct{})
			go func() {
				defer close(parentDone)
				_, putErr := c.Put("s3/trigger", "t")
				assert.NoError(t, putErr)
			}()
			<-parentEntered

			// Wait until the child goroutine queues on shard 0's write lock.
			require.Eventually(t, func() bool {
				return !shardTryRLock[string](sc.shards[0])
			}, time.Second, time.Millisecond)

			// Exhaust crossShardRetries (> 4) on shard 3 and cache Episode 2 (0.95) on coord.
			for i := range 5 {
				_, ok := c.Delete(fmt.Sprintf("s3/r%d", i))
				require.True(t, ok)
			}
			_, err = c.Put("s3/cache_ep2", "c")
			require.NoError(t, err)

			// Release shard 0 so child completes shard 0 while parent is still inside PressureFunc,
			// and wait until child queues on shard 1's write lock.
			releaseS0()
			require.Eventually(t, func() bool {
				return !shardTryRLock[string](sc.shards[1])
			}, time.Second, time.Millisecond)

			// Exit parent PressureFunc, then release shard 1 so child proceeds to shard 2.
			close(releaseParent)
			<-parentDone
			releaseS1()
			<-childDone

			assert.Equal(t, 1, sc.shards[2].Stats().ZeroSizeCount,
				"once parent PressureFunc exits, child EvaluateMemoryPressure must shed shard 2 in Episode 2 (2 -> 1)")
		})
	}
}

func TestPressure_StalePreRecoverySampleDoesNotOverShedOrSuppressNextEpisode(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"/Sharded", func(t *testing.T) {
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.10))

			c := NewShardedCache[string](
				90,
				3,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
				WithCompactionThreshold(0.70),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
			)
			pa := c.(PressureAwareCache[string])
			sc := c.(*shardedCache[string])

			_, err := c.Put("s0/anchor", "a")
			require.NoError(t, err)
			_, err = c.Put("s1/hold", "b")
			require.NoError(t, err)
			for i := range 4 {
				_, err = c.Put(fmt.Sprintf("s2/z%d", i), "")
				require.NoError(t, err)
			}

			// Episode 1: start EvaluateMemoryPressure at 0.95; wait until shard 0 completes and it parks on shard 1.
			pressureBits.Store(math.Float64bits(0.95))
			releaseS1 := holdShardRLockViaKeys(t, c, "s1/")
			ep1Done := make(chan struct{})
			go func() {
				defer close(ep1Done)
				_ = pa.EvaluateMemoryPressure()
			}()
			require.Eventually(t, func() bool {
				return !shardTryRLock[string](sc.shards[1])
			}, time.Second, time.Millisecond)

			// Recover to 0.10 and run a non-reclaiming op on shard 2, advancing shard 2's causal sequence.
			pressureBits.Store(math.Float64bits(0.10))
			_, _ = c.Delete("s2/absent")

			// Release shard 1 while system pressure is healthy (0.10): stale Episode 1 sample must not shed shard 2.
			releaseS1()
			<-ep1Done
			assert.Equal(t, 4, sc.shards[2].Stats().ZeroSizeCount,
				"stale pre-recovery Episode 1 sample must not shed shard 2 after shard 2 recovered to 0.10")

			// Episode 2: spike back to 0.95; shard 2 must shed 4 -> 2 cleanly without suppression.
			pressureBits.Store(math.Float64bits(0.95))
			evictedEp2 := pa.EvaluateMemoryPressure()
			assert.Len(t, evictedEp2, 2)
			assert.Equal(t, 2, sc.shards[2].Stats().ZeroSizeCount,
				"Episode 2 must shed shard 2 from 4 -> 2 without watermark suppression")
		})

		t.Run(backend.String()+"/Unsharded", func(t *testing.T) {
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.10))

			c := New[string](
				30,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
				WithCompactionThreshold(0.70),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
			)
			pa := c.(PressureAwareCache[string])

			_, err := c.Put("hold", "b")
			require.NoError(t, err)
			for i := range 4 {
				_, err = c.Put(fmt.Sprintf("z%d", i), "")
				require.NoError(t, err)
			}

			// Hold RLock so foreground Put at 0.95 samples Episode 1 and parks waiting for mu.Lock().
			releaseRLock := holdShardRLockViaKeys(t, c, "hold")
			pressureBits.Store(math.Float64bits(0.95))
			ep1Done := make(chan struct{})
			go func() {
				defer close(ep1Done)
				_, putErr := c.Put("ep1_item", "x")
				assert.NoError(t, putErr)
			}()
			require.Eventually(t, func() bool {
				return !shardTryRLock[string](c)
			}, time.Second, time.Millisecond)

			// Recover to 0.10 and launch Delete("absent") which samples 0.10 (advancing belowTier2Epoch) before mu.Lock().
			pressureBits.Store(math.Float64bits(0.10))
			healthyDone := make(chan struct{})
			go func() {
				defer close(healthyDone)
				_, _ = c.Delete("absent")
			}()
			require.Eventually(t, func() bool {
				return math.Abs(math.Float64frombits(c.(interface{ pressureStatePtr() *pressureState }).pressureStatePtr().lastSampledPressureBits.Load())-0.10) < 1e-9
			}, time.Second, time.Millisecond)

			releaseRLock()
			<-ep1Done
			<-healthyDone

			assert.Equal(t, 4, c.Stats().ZeroSizeCount,
				"stale pre-recovery Episode 1 Put sample must not shed zero-weight entries after recovery to 0.10")

			// Episode 2: spike back to 0.95; cache must shed 4 -> 2 cleanly.
			pressureBits.Store(math.Float64bits(0.95))
			evictedEp2 := pa.EvaluateMemoryPressure()
			assert.Len(t, evictedEp2, 2)
			assert.Equal(t, 2, c.Stats().ZeroSizeCount,
				"Episode 2 must shed unsharded cache from 4 -> 2 without watermark suppression")
		})
	}
}

type callbackWrapperService struct {
	cache Cache[int]
}

func (s *callbackWrapperService) writeEntry(key string, val int, asyncGo, asyncWG bool) error {
	if asyncGo {
		errCh := make(chan error, 1)
		go func() {
			_, err := s.cache.Put(key, val)
			errCh <- err
		}()
		return <-errCh
	}
	if asyncWG {
		var putErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			_, putErr = s.cache.Put(key, val)
		})
		wg.Wait()
		return putErr
	}
	_, err := s.cache.Put(key, val)
	return err
}

func TestShardedCache_CallbackChildGoroutineLineageSiblingClosureAndWrapperMethod(t *testing.T) {
	t.Run("SiblingClosureViaWaitGroupGo", func(t *testing.T) {
		var evictCount atomic.Int32
		var cache Cache[int]
		worker := func() {
			_, _ = cache.Delete("s1/k2")
		}
		cache = NewShardedCache[int](
			4,
			2,
			explicitTagShardFunc,
			WithInvariantChecking(true),
			WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
				evictCount.Add(1)
				if k == "s0/k1" {
					var wg sync.WaitGroup
					wg.Go(worker)
					wg.Wait()
				}
			}),
		)

		_, err := cache.Put("s0/k1", 1)
		require.NoError(t, err)
		_, err = cache.Put("s1/k2", 2)
		require.NoError(t, err)

		done := make(chan struct{})
		go func() {
			defer close(done)
			// Execute both puts/deletes directly from a single caller frame owning both closures.
			var localCache Cache[int]
			var localEvicts atomic.Int32
			localWorker := func() {
				_, _ = localCache.Delete("s1/k2")
			}
			localCache = NewShardedCache[int](
				4,
				2,
				explicitTagShardFunc,
				WithInvariantChecking(true),
				WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
					localEvicts.Add(1)
					if k == "s0/k1" {
						var wg sync.WaitGroup
						wg.Go(localWorker)
						wg.Wait()
					}
				}),
			)
			_, _ = localCache.Put("s0/k1", 1)
			_, _ = localCache.Put("s1/k2", 2)
			_, _ = localCache.Delete("s0/k1")
			assert.Equal(t, int32(2), localEvicts.Load())
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("deadlocked in OnEvictEntry when spawning sibling closure via sync.WaitGroup.Go")
		}

		_, ok := cache.Delete("s0/k1")
		require.True(t, ok)
		assert.Equal(t, int32(2), evictCount.Load())
	})

	for _, useWG := range []bool{false, true} {
		modeName := "GoStmt"
		if useWG {
			modeName = "WaitGroupGo"
		}
		t.Run("WrapperServiceMethod/"+modeName, func(t *testing.T) {
			svc := &callbackWrapperService{}
			var evictCount atomic.Int32
			var probeCalls atomic.Int32
			var probeReentered atomic.Bool

			svc.cache = NewShardedCache[int](
				4,
				2,
				explicitTagShardFunc,
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					probeCalls.Add(1)
					if probeReentered.CompareAndSwap(false, true) {
						assert.NoError(t, svc.writeEntry("s1/probe", 99, !useWG, useWG))
					}
					return 0.10
				}),
				WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
					evictCount.Add(1)
					if k == "s0/k1" && r == EvictionReasonCapacity {
						assert.NoError(t, svc.writeEntry("s1/audit", 2, !useWG, useWG))
					}
				}),
			)

			// Populate s1/audit so overwriting it inside OnEvictEntry triggers EvictionReasonReplaced.
			require.NoError(t, svc.writeEntry("s1/audit", 1, false, false))
			probeCalls.Store(0)
			probeReentered.Store(false)
			require.NoError(t, svc.writeEntry("s0/k1", 10, false, false))
			assert.Equal(t, int32(1), probeCalls.Load(),
				"PressureFunc child goroutine launched through wrapper method must be recognized as re-entrant")

			require.NoError(t, svc.writeEntry("s0/k2", 20, false, false))

			done := make(chan struct{})
			go func() {
				defer close(done)
				// Shard 0 maxSize is 2; inserting s0/k3 evicts s0/k1 and re-enters svc.writeEntry asynchronously.
				assert.NoError(t, svc.writeEntry("s0/k3", 30, false, false))
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("deadlocked on wrapper method re-entrancy (%s)", modeName)
			}

			val, ok := svc.cache.Peek("s1/audit")
			require.True(t, ok)
			assert.Equal(t, 2, val)
			assert.GreaterOrEqual(t, evictCount.Load(), int32(2))
		})
	}
}

func TestShardedCache_ConcurrentCompactInvalidatesStaleCrossShardSample(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendArenaRadix} {
		t.Run(backend.String()+"/EvaluateMemoryPressure", func(t *testing.T) {
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.10))

			c := NewShardedCache[string](
				300,
				3,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
				WithCompactionThreshold(0.75),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
			)
			pa := c.(PressureAwareCache[string])
			sc := c.(*shardedCache[string])

			_, err := c.Put("s0/anchor", "a")
			require.NoError(t, err)
			_, err = c.Put("s1/hold", "b")
			require.NoError(t, err)
			// Create compaction slack on shard 2 (peakEntryLen=20 > currentLen=18) plus two 40B live entries (80B > shardTier2Target 50B).
			for i := range 20 {
				_, err = c.Put(fmt.Sprintf("s2/slack_%02d", i), "")
				require.NoError(t, err)
			}
			for i := range 4 {
				_, _ = c.Delete(fmt.Sprintf("s2/slack_%02d", i))
			}
			_, err = c.Put("s2/live1", strings.Repeat("a", 40))
			require.NoError(t, err)
			_, err = c.Put("s2/live2", strings.Repeat("b", 40))
			require.NoError(t, err)

			pressureBits.Store(math.Float64bits(0.95))
			releaseS1 := holdShardRLockViaKeys(t, c, "s1/")
			evalDone := make(chan []string, 1)
			go func() {
				evalDone <- pa.EvaluateMemoryPressure()
			}()
			require.Eventually(t, func() bool {
				return !shardTryRLock[string](sc.shards[1])
			}, time.Second, time.Millisecond)

			// Concurrent Compact() on shard 2 reclaims slack and relieves memory pressure to 0.10
			// while EvaluateMemoryPressure is parked on shard 1.
			sc.shards[2].Compact()
			pressureBits.Store(math.Float64bits(0.10))

			releaseS1()
			evicted := <-evalDone

			assert.Empty(t, evicted, "Shard 2 must resample after Compact() and preserve live entries")
			st := c.Stats()
			assert.Zero(t, st.EvictionsPressure)
			assert.Equal(t, uint64(1), st.CompactionsExplicit)
			assert.InDelta(t, 0.10, st.MemoryPressure, 1e-9)
		})

		t.Run(backend.String()+"/DeletePrefixEmptyTelemetry", func(t *testing.T) {
			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.10))

			c := NewShardedCache[string](
				300,
				3,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithCompactionThreshold(0.75),
				WithEvictionThreshold(0.90),
				WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
			)
			pa := c.(PressureAwareCache[string])
			sc := c.(*shardedCache[string])

			_, err := c.Put("s0/item", "a")
			require.NoError(t, err)
			_, err = c.Put("s1/hold", "b")
			require.NoError(t, err)
			for i := range 16 {
				_, err = c.Put(fmt.Sprintf("s2/k_%02d", i), "v")
				require.NoError(t, err)
			}

			pressureBits.Store(math.Float64bits(0.95))
			releaseS1 := holdShardRLockViaKeys(t, c, "s1/")
			delDone := make(chan struct{})
			go func() {
				defer close(delDone)
				c.DeletePrefix("")
			}()
			require.Eventually(t, func() bool {
				return !shardTryRLock[string](sc.shards[1])
			}, time.Second, time.Millisecond)

			// Fragment shard 0 (peakEntryLen=20 > currentLen=18 without triggering foreground auto-compact)
			// and run cache-wide Compact() in a goroutine so shard 0 compacts immediately before releasing shard 1.
			_, err = c.Put("s0/keep", "k")
			require.NoError(t, err)
			for i := range 19 {
				_, err = c.Put(fmt.Sprintf("s0/s_%02d", i), "")
				require.NoError(t, err)
			}
			_, _ = c.Delete("s0/s_00")
			_, _ = c.Delete("s0/s_01")
			compactDone := make(chan struct{})
			go func() {
				defer close(compactDone)
				pa.Compact()
			}()
			require.Eventually(t, func() bool {
				return sc.shards[0].Stats().CompactionsExplicit == 1
			}, time.Second, time.Millisecond)
			pressureBits.Store(math.Float64bits(0.10))

			releaseS1()
			<-delDone
			<-compactDone

			s2Stats := sc.shards[2].Stats()
			assert.Equal(t, uint64(1), s2Stats.CompactionsAutoSlack,
				"Shard 2 DeletePrefix(\"\") after pressure dropped to 0.10 must record CompactionsAutoSlack")
			assert.Zero(t, s2Stats.CompactionsPressureTier2)
		})
	}
}

func TestPressure_Tier1ModeratePressureDoesNotSelfInvalidate(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"/SingleProbeShardedEvaluation", func(t *testing.T) {
			var inEval atomic.Bool
			var evalProbes atomic.Int32

			c := NewShardedCache[int](
				400,
				4,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithCompactionThreshold(0.75),
				WithEvictionThreshold(0.90),
				WithPressureFunc(func() float64 {
					if inEval.Load() {
						evalProbes.Add(1)
					}
					return 0.80
				}),
			)
			pa := c.(PressureAwareCache[int])
			sc := c.(*shardedCache[int])

			_, err := c.Put("s0/a", 1)
			require.NoError(t, err)
			_, err = c.Put("s1/hold", 2)
			require.NoError(t, err)

			releaseS1 := holdShardRLockViaKeys(t, c, "s1/")
			evalDone := make(chan struct{})
			go func() {
				inEval.Store(true)
				defer func() {
					inEval.Store(false)
					close(evalDone)
				}()
				_ = pa.EvaluateMemoryPressure()
			}()
			require.Eventually(t, func() bool {
				return !shardTryRLock[int](sc.shards[1])
			}, time.Second, time.Millisecond)

			// Concurrent Tier 1 probe on shard 3 must not invalidate EvaluateMemoryPressure's Tier 1 sample.
			inEval.Store(false)
			_, err = c.Put("s3/other", 3)
			require.NoError(t, err)
			inEval.Store(true)

			releaseS1()
			<-evalDone

			assert.Equal(t, int32(1), evalProbes.Load(),
				"EvaluateMemoryPressure at steady Tier 1 pressure must sample PressureFunc once across all shards")
		})

		for _, sharded := range []bool{true, false} {
			mode := "Unsharded"
			if sharded {
				mode = "Sharded"
			}
			t.Run(backend.String()+"/ContendedCompaction_"+mode, func(t *testing.T) {
				runContendedTier1CompactionTest(t, backend, sharded)
			})
		}
	}
}

func runContendedTier1CompactionTest(t *testing.T, backend Backend, sharded bool) {
	t.Helper()
	var pressureBits atomic.Uint64
	pressureBits.Store(math.Float64bits(0.10))
	opts := []Option{
		WithBackend(backend),
		WithInvariantChecking(true),
		WithCompactionThreshold(0.75),
		WithEvictionThreshold(0.90),
		WithPressureFunc(func() float64 { return math.Float64frombits(pressureBits.Load()) }),
	}
	var c Cache[int]
	if sharded {
		c = NewShardedCache[int](400, 4, explicitTagShardFunc, opts...)
	} else {
		c = New[int](100, opts...)
	}
	pa := c.(PressureAwareCache[int])

	// Insert 20 keys and delete 4 keys on shard 0 at 0.10 (20% slack: below foreground 25% auto-compact,
	// so ReclaimEpoch stays 0 and DeletedSinceCompact == 4, while background Tier 1 compacts).
	for i := range 20 {
		_, err := c.Put(fmt.Sprintf("s0/k_%02d", i), i)
		require.NoError(t, err)
	}
	for i := range 4 {
		_, _ = c.Delete(fmt.Sprintf("s0/k_%02d", i))
	}
	pressureBits.Store(math.Float64bits(0.80))

	var targetShard any = c
	var coord *pressureState
	if sharded {
		sc := c.(*shardedCache[int])
		targetShard = sc.shards[0]
		coord = &sc.coord
		require.Equal(t, 4, sc.shards[0].Stats().DeletedSinceCompact)
	} else {
		coord = c.(shardEngine[int]).pressureStatePtr()
		require.Equal(t, 4, c.Stats().DeletedSinceCompact)
	}
	require.Zero(t, coord.reclaimEpoch.Load())

	releaseRLock := holdShardRLockViaKeys(t, c, "s0/")
	defer releaseRLock()
	baseBelowTier2 := coord.belowTier2Epoch.Load()

	evalDone := make(chan struct{})
	go func() {
		defer close(evalDone)
		_ = pa.EvaluateMemoryPressure()
	}()
	require.Eventually(t, func() bool {
		return !shardTryRLock[int](targetShard)
	}, time.Second, time.Millisecond)

	var putWG sync.WaitGroup
	if sharded {
		for i := range 6 {
			_, err := c.Put(fmt.Sprintf("s3/p_%d", i), i)
			require.NoError(t, err)
		}
	} else {
		for i := range 6 {
			var putGID atomic.Uint64
			putStarted := make(chan struct{})
			putWG.Add(1)
			go func(idx int) {
				defer putWG.Done()
				putGID.Store(currentGoroutineID())
				close(putStarted)
				_, _ = c.Put(fmt.Sprintf("s3/p_%d", idx), idx)
			}(i)
			<-putStarted
			waitForGoroutineParkedInRWMutexLock(t, putGID.Load())
		}
		require.Equal(t, baseBelowTier2+7, coord.belowTier2Epoch.Load())
	}

	releaseRLock()
	<-evalDone
	putWG.Wait()

	var st Stats
	if sharded {
		st = c.(*shardedCache[int]).shards[0].Stats()
	} else {
		st = c.Stats()
	}
	assert.Zero(t, st.DeletedSinceCompact,
		"Tier 1 EvaluateMemoryPressure must compact fragmented shard despite concurrent Tier 1 probes")
	if backend != BackendRadix {
		assert.Equal(t, uint64(1), st.CompactionsPressureTier1)
	}
	if backend == BackendArenaRadix {
		assert.Zero(t, st.ArenaFreeNodes)
	}
}

func TestShardedCache_StaleElevatedSampleDoesNotAdoptPostRecoveryEpoch(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			runStaleElevatedSamplePostRecoveryTest(t, backend)
		})
	}
}

func runStaleElevatedSamplePostRecoveryTest(t *testing.T, backend Backend) {
	t.Helper()
	var g1Armed atomic.Bool
	var inWorker atomic.Bool
	var healthyRecovered atomic.Bool
	var genuineSpike atomic.Bool
	var g1Samples atomic.Int32

	stepReq := make(chan int, 1)
	stepAck := make(chan struct{}, 1)
	fifthReady := make(chan struct{})
	releaseFifth := make(chan struct{})

	c := NewShardedCache[string](
		80,
		4,
		explicitTagShardFunc,
		WithBackend(backend),
		WithInvariantChecking(true),
		WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			if genuineSpike.Load() {
				return 0.95
			}
			if !g1Armed.Load() || healthyRecovered.Load() {
				return 0.10
			}
			if inWorker.Load() {
				return 0.95
			}
			n := int(g1Samples.Add(1))
			if n <= 4 {
				stepReq <- n - 1
				<-stepAck
				return 0.95
			}
			if n == 5 {
				close(fifthReady)
				<-releaseFifth
				return 0.95
			}
			return 0.10
		}),
	)
	pa := c.(PressureAwareCache[string])
	sc := c.(*shardedCache[string])

	// Populate s0 and s1 (maxSize=20, tier2Target=10) with weighted (16B) and zero-weight (4) entries.
	for _, prefix := range []string{"s0", "s1"} {
		for i := range 2 {
			_, err := c.Put(fmt.Sprintf("%s/live%d", prefix, i), "12345678")
			require.NoError(t, err)
		}
		for i := range 4 {
			_, err := c.Put(fmt.Sprintf("%s/z%d", prefix, i), "")
			require.NoError(t, err)
		}
	}
	for i := range 5 {
		_, err := c.Put(fmt.Sprintf("s2/r%d", i), "x")
		require.NoError(t, err)
	}

	// Pre-spawn external worker before G1 so it is never a descendant of G1.
	stopWorker := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for {
			select {
			case <-stopWorker:
				return
			case idx := <-stepReq:
				inWorker.Store(true)
				_, _ = c.Delete(fmt.Sprintf("s2/r%d", idx))
				inWorker.Store(false)
				stepAck <- struct{}{}
			}
		}
	}()

	g1Armed.Store(true)
	evictedCh := make(chan []string, 1)
	go func() {
		evictedCh <- pa.EvaluateMemoryPressure()
	}()
	<-fifthReady

	// Recover to 0.10 on s3 (advancing belowTier1Epoch & belowTier2Epoch with newer invokeSeq),
	// then advance coord.reclaimEpoch externally on s2 while G1 is still an active sampler.
	healthyRecovered.Store(true)
	_, err := c.Put("s3/healthy", "h")
	require.NoError(t, err)

	stepReq <- 4
	<-stepAck

	close(releaseFifth)
	evicted := <-evictedCh
	close(stopWorker)
	<-workerDone

	assert.Empty(t, evicted, "stale pre-recovery 0.95 sample must not trigger spurious Tier 2 shedding")
	assert.Zero(t, c.Stats().EvictionsPressure)
	assert.InDelta(t, 0.10, c.Stats().MemoryPressure, 1e-9)
	assert.Equal(t, 4, sc.shards[0].Stats().ZeroSizeCount)
	assert.Equal(t, 4, sc.shards[1].Stats().ZeroSizeCount)

	// Verify zeroWatermarkTier2Epoch was not poisoned against the next genuine 0.95 episode.
	genuineSpike.Store(true)
	spikeEvicted := pa.EvaluateMemoryPressure()
	assert.NotEmpty(t, spikeEvicted)
	assert.LessOrEqual(t, sc.shards[0].Stats().ZeroSizeCount, 2,
		"subsequent genuine critical episode must still shed zero-weight entries on shard 0")
	assert.LessOrEqual(t, sc.shards[1].Stats().ZeroSizeCount, 2,
		"subsequent genuine critical episode must still shed zero-weight entries on shard 1")
}

func TestShardedCache_PanickingSamplerDoesNotMisattributeChildReclaimToConcurrentSampler(t *testing.T) {
	t.Run("UnregisteredGIDDoesNotFallbackToWildcardSlot", func(t *testing.T) {
		c := NewShardedCache[string](
			90,
			3,
			explicitTagShardFunc,
			WithPressureFunc(func() float64 { return 0.95 }),
		)
		sc := c.(*shardedCache[string])
		coord := &sc.coord
		shard1 := sc.shards[1].pressureStatePtr()

		// Simulate concurrent unrelated sampler G2 occupying fallback slot while G1 already unwound its slot.
		coord.fallbackSampling.Store(true)
		coord.fallbackGID.Store(999002)
		t.Cleanup(func() {
			coord.fallbackGID.Store(0)
			coord.fallbackReentrantReclaim.Store(false)
			coord.fallbackSampling.Store(false)
		})

		coord.pressureWriteMu.Lock()
		matched := shard1.recordReentrantReclaimGIDOn(coord, 999001)
		coord.pressureWriteMu.Unlock()

		assert.False(t, matched,
			"unregistered sampler GID after panic teardown must not fall back to wildcard child-reclaim attribution")
		assert.False(t, coord.fallbackReentrantReclaim.Load(),
			"unrelated concurrent fallback sampler must not be marked as a re-entrant reclaimer")
	})

	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			runPanickingSamplerConcurrentChildReclaimTest(t, backend)
		})
	}
}

func runPanickingSamplerConcurrentChildReclaimTest(t *testing.T, backend Backend) {
	t.Helper()
	var g1Role atomic.Uint64
	var g2Role atomic.Uint64
	var pressureHealthy atomic.Bool

	g1Entered := make(chan struct{})
	triggerG1Child := make(chan struct{})
	g2ReadStale := make(chan struct{})
	releaseG2 := make(chan struct{})

	var c Cache[string]
	c = NewShardedCache[string](
		90,
		3,
		explicitTagShardFunc,
		WithBackend(backend),
		WithInvariantChecking(true),
		WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			gid := currentGoroutineID()
			if gid != 0 && gid == g1Role.Load() {
				close(g1Entered)
				<-triggerG1Child
				childStarted := make(chan struct{})
				go func() {
					close(childStarted)
					pressureHealthy.Store(true)
					_, _ = c.Delete("s1/victim")
				}()
				<-childStarted
				panic("g1-pressure-panic")
			}
			if gid != 0 && gid == g2Role.Load() {
				if !pressureHealthy.Load() {
					close(g2ReadStale)
					<-releaseG2
					return 0.95
				}
				return 0.10
			}
			if pressureHealthy.Load() {
				return 0.10
			}
			return 0.10
		}),
	)
	sc := c.(*shardedCache[string])

	// Populate s2 (maxSize=30, tier2Target=15) with 24B of live entries and s1 with a 20B victim.
	_, err := c.Put("s2/live0", "123456789012")
	require.NoError(t, err)
	_, err = c.Put("s2/live1", "123456789012")
	require.NoError(t, err)
	_, err = c.Put("s1/victim", "12345678901234567890")
	require.NoError(t, err)

	g1Done := make(chan any, 1)
	go func() {
		g1Role.Store(currentGoroutineID())
		defer func() {
			g1Done <- recover()
		}()
		_, _ = c.Put("s0/trigger", "t")
	}()
	<-g1Entered

	g2Done := make(chan struct{})
	go func() {
		g2Role.Store(currentGoroutineID())
		defer close(g2Done)
		_, _ = c.Put("s2/probe", "p")
	}()
	<-g2ReadStale

	close(triggerG1Child)
	recovered := <-g1Done
	require.Equal(t, "g1-pressure-panic", recovered)

	// Wait until G1's child goroutine has deleted "s1/victim" and advanced reclaimEpoch.
	require.Eventually(t, func() bool {
		return sc.coord.reclaimEpoch.Load() >= 1 && sc.shards[1].Stats().Len == 0
	}, time.Second, time.Millisecond)

	close(releaseG2)
	<-g2Done

	assert.Zero(t, c.Stats().EvictionsPressure,
		"G2's pre-reclamation 0.95 sample must not be fast-forwarded after G1 panicked and G1's child reclaimed s1")
	assert.InDelta(t, 0.10, c.Stats().MemoryPressure, 1e-9)
}

func TestOptions_DirectShardsZeroOverrideRestoresUnshardedMode(t *testing.T) {
	preset := []Option{WithShards(4)}
	resetToUnsharded := append(append([]Option{}, preset...), func(o *Options) {
		o.Shards = 0
	})

	applied := ApplyOptions(resetToUnsharded...)
	assert.Zero(t, applied.Shards)
	assert.False(t, applied.shouldShard())

	constructors := []struct {
		name string
		fn   func() Cache[int]
	}{
		{"New", func() Cache[int] { return New[int](100, resetToUnsharded...) }},
		{"NewMapCache", func() Cache[int] { return NewMapCache[int](100, resetToUnsharded...) }},
		{"NewRadixCache", func() Cache[int] { return NewRadixCache[int](100, resetToUnsharded...) }},
		{"NewArenaRadixCache", func() Cache[int] { return NewArenaRadixCache[int](100, resetToUnsharded...) }},
	}
	for _, tc := range constructors {
		t.Run(tc.name, func(t *testing.T) {
			var c Cache[int]
			require.NotPanics(t, func() {
				c = tc.fn()
			})
			_, isSharded := c.(*shardedCache[int])
			assert.False(t, isSharded, "setting Options.Shards = 0 via custom Option must restore unsharded mode")
		})
	}

	overridePositive := append(append([]Option{}, preset...), func(o *Options) {
		o.Shards = 8
	})
	sc, ok := New[int](100, overridePositive...).(*shardedCache[int])
	require.True(t, ok)
	assert.Len(t, sc.shards, 8)

	assert.PanicsWithValue(t, "shards must be greater than zero", func() {
		_ = New[int](100, WithShards(0))
	})
	assert.PanicsWithValue(t, "shards must be greater than zero", func() {
		_ = New[int](100, WithShards(0), WithBackend(BackendRadix))
	})
	assert.PanicsWithValue(t, "shards must be greater than zero", func() {
		_ = New[int](100, WithShards(0), func(o *Options) {
			o.EnableInvariantChecking = true
		})
	})

	// Custom Option after WithShards(0) must observe Shards == 0 (never an internal sentinel)
	// and must not have explicit Shards overrides clobbered in composite Option closures.
	observedShards := -1
	sentinelCheck := ApplyOptions(WithShards(0), func(o *Options) {
		observedShards = o.Shards
	})
	assert.Zero(t, observedShards, "custom Option inspecting Options.Shards after WithShards(0) must observe 0")
	assert.Zero(t, sentinelCheck.Shards)
	assert.True(t, sentinelCheck.shouldShard())

	compositePositiveOpts := [][]Option{
		{
			WithShards(0),
			func(o *Options) {
				WithBackend(BackendRadix)(o)
				o.Shards = 4
			},
		},
		{
			WithShards(0),
			func(o *Options) {
				if o.Shards == 0 {
					o.Shards = 4
				}
			},
		},
		{
			func(o *Options) {
				WithShards(0)(o)
				o.Shards = 4
			},
		},
	}
	for _, seqOpts := range compositePositiveOpts {
		appliedPos := ApplyOptions(seqOpts...)
		assert.Equal(t, 4, appliedPos.Shards)
		assert.True(t, appliedPos.shouldShard())
		scPos, isSharded := New[int](100, seqOpts...).(*shardedCache[int])
		require.True(t, isSharded)
		assert.Len(t, scPos.shards, 4)
	}

	resetAfterZeroAndPositive := [][]Option{
		{
			WithShards(0),
			func(o *Options) { o.Shards = 4 },
			func(o *Options) { o.Shards = 0 },
		},
		{
			WithShards(0),
			func(o *Options) {
				WithBackend(BackendRadix)(o)
				o.Shards = 4
			},
			func(o *Options) {
				WithInvariantChecking(true)(o)
				o.Shards = 0
			},
		},
		{
			func(o *Options) {
				WithShards(4)(o)
				o.Shards = 0
			},
		},
	}
	for _, seqOpts := range resetAfterZeroAndPositive {
		appliedZero := ApplyOptions(seqOpts...)
		assert.Zero(t, appliedZero.Shards)
		assert.False(t, appliedZero.shouldShard())
		for _, fn := range []func(...Option) Cache[int]{
			func(opts ...Option) Cache[int] { return New[int](100, opts...) },
			func(opts ...Option) Cache[int] { return NewMapCache[int](100, opts...) },
			func(opts ...Option) Cache[int] { return NewRadixCache[int](100, opts...) },
			func(opts ...Option) Cache[int] { return NewArenaRadixCache[int](100, opts...) },
		} {
			var c Cache[int]
			require.NotPanics(t, func() {
				c = fn(seqOpts...)
			})
			_, isSharded := c.(*shardedCache[int])
			assert.False(t, isSharded, "resetting Options.Shards from a non-zero value back to 0 must restore unsharded mode")
		}
	}
}

func runCrossShardFallbackEpochIsolationCase(t *testing.T, backend Backend, useDeletePrefix bool) {
	t.Helper()
	var armed atomic.Bool
	var healthy atomic.Bool
	probeEntered := make(chan struct{})
	probeRelease := make(chan struct{})
	var g1GID atomic.Uint64

	c := NewShardedCache[int](
		300,
		3,
		explicitTagShardFunc,
		WithBackend(backend),
		WithInvariantChecking(true),
		WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
		WithCompactionThreshold(0.80),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			if !armed.Load() || healthy.Load() {
				return 0.10
			}
			if g1GID.Load() != 0 && currentGoroutineID() == g1GID.Load() {
				probeEntered <- struct{}{}
				<-probeRelease
				return 0.95
			}
			return 0.95
		}),
	)
	pa := c.(PressureAwareCache[int])

	_, err := c.Put("s2/live", 80)
	require.NoError(t, err)
	for i := range 5 {
		_, err = c.Put(fmt.Sprintf("s1/victim%d", i), 2)
		require.NoError(t, err)
	}
	if useDeletePrefix {
		_, err = c.Put("s2/drop/target", 2)
		require.NoError(t, err)
	}

	armed.Store(true)
	opDone := make(chan struct{})
	go func() {
		defer close(opDone)
		g1GID.Store(currentGoroutineID())
		if useDeletePrefix {
			c.DeletePrefix("s2/drop/")
		} else {
			_ = pa.EvaluateMemoryPressure()
		}
	}()

	for i := range 5 {
		<-probeEntered
		_, ok := c.Delete(fmt.Sprintf("s1/victim%d", i))
		require.True(t, ok)
		if i == 4 {
			healthy.Store(true)
		}
		probeRelease <- struct{}{}
	}

	<-opDone
	_, present := c.Peek("s2/live")
	assert.True(t, present, "subsequent shard must resample 0.10 instead of adopting shard 0's pre-reclamation 0.95 fallback")
	stats := c.Stats()
	assert.Zero(t, stats.EvictionsPressure)
	assert.Zero(t, stats.CompactionsPressureTier2)
	assert.InDelta(t, 0.10, stats.MemoryPressure, 1e-9)
}

func TestShardedCache_CrossShardRetryExhaustionFallbackDoesNotPropagateStaleEpoch(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String()+"/EvaluateMemoryPressure", func(t *testing.T) {
			runCrossShardFallbackEpochIsolationCase(t, backend, false)
		})
		t.Run(backend.String()+"/DeletePrefix", func(t *testing.T) {
			runCrossShardFallbackEpochIsolationCase(t, backend, true)
		})
	}
}

type decoyLineageStage struct {
	park <-chan struct{}
}

func (s *decoyLineageStage) run(fn func()) {
	if s.park != nil {
		<-s.park
		return
	}
	fn()
}

func TestShardedCache_BacktrackOnRejectedDecoyAncestorPreservesMultiHopLineage(t *testing.T) {
	decoyReady := make(chan struct{})
	decoyRelease := make(chan struct{})
	setupReady := make(chan struct{})

	var armed atomic.Bool
	var setupMode atomic.Bool
	var probeCalls atomic.Int32
	var cache Cache[int]

	cache = NewShardedCache[int](
		64,
		2,
		explicitTagShardFunc,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			if setupMode.Load() {
				done := make(chan struct{})
				go func() {
					defer close(done)
					_, _ = cache.Put("s1/setup_child", 1)
				}()
				<-done
				return 0.10
			}
			if !armed.Load() {
				return 0.10
			}
			if probeCalls.Add(1) > 1 {
				return 0.10
			}
			leafDone := make(chan struct{})
			stage := &decoyLineageStage{}
			go stage.run(func() {
				midExited := make(chan struct{})
				go func() {
					go func() {
						<-midExited
						defer close(leafDone)
						_, _ = cache.Put("s1/reentrant", 1)
					}()
					close(midExited)
				}()
				<-leafDone
			})
			<-leafDone
			return 0.10
		}),
	)

	setupDone := make(chan struct{})
	go func() {
		defer close(setupDone)
		decoy := &decoyLineageStage{park: decoyRelease}
		go func() {
			close(decoyReady)
			decoy.run(func() {})
		}()
		<-decoyReady
		setupMode.Store(true)
		_, _ = cache.Put("s0/setup", 1)
		setupMode.Store(false)
		close(setupReady)
		<-decoyRelease
	}()
	<-setupReady

	armed.Store(true)
	_, err := cache.Put("s0/trigger", 2)
	require.NoError(t, err)
	assert.Equal(t, int32(1), probeCalls.Load(),
		"multi-hop PressureFunc descendant must backtrack past decoy goroutine and preserve re-entrancy guard")
	close(decoyRelease)
	<-setupDone
}

func runPanickingSamplerOrphanIsolationCase(t *testing.T, ringChurn bool) {
	t.Helper()
	var armed atomic.Bool
	var healthy atomic.Bool
	var g1GID, g2GID atomic.Uint64
	g1Ready := make(chan struct{})
	g2ReadHigh := make(chan struct{})
	releaseG1Panic := make(chan struct{})
	releaseOrphan := make(chan struct{})
	orphanDone := make(chan struct{})
	releaseG2Return := make(chan struct{})

	var cache Cache[int]
	spawnOrphanHelper := func() {
		if ringChurn {
			go func() {
				<-releaseOrphan
				healthy.Store(true)
				_, _ = cache.Delete("s1/victim")
				close(orphanDone)
			}()
			return
		}
		midDone := make(chan struct{})
		go func() {
			go func() {
				<-midDone
				<-releaseOrphan
				healthy.Store(true)
				_, _ = cache.Delete("s1/victim")
				close(orphanDone)
			}()
			close(midDone)
		}()
		<-midDone
	}

	cache = NewShardedCache[int](
		300,
		3,
		explicitTagShardFunc,
		WithInvariantChecking(true),
		WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
		WithCompactionThreshold(0.80),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			if !armed.Load() || healthy.Load() {
				return 0.10
			}
			gid := currentGoroutineID()
			if g1GID.Load() != 0 && gid == g1GID.Load() {
				spawnOrphanHelper()
				close(g1Ready)
				<-releaseG1Panic
				panic("boom from G1 sampler")
			}
			if g2GID.Load() != 0 && gid == g2GID.Load() {
				close(g2ReadHigh)
				<-releaseG2Return
				return 0.95
			}
			childDone := make(chan struct{})
			go func() {
				defer close(childDone)
				_, _ = cache.Peek("s0/noop")
			}()
			<-childDone
			return 0.95
		}),
	)

	_, err := cache.Put("s1/victim", 20)
	require.NoError(t, err)
	_, err = cache.Put("s2/live", 80)
	require.NoError(t, err)

	armed.Store(true)
	g1Done := make(chan struct{})
	go func() {
		defer close(g1Done)
		g1GID.Store(currentGoroutineID())
		assert.PanicsWithValue(t, "boom from G1 sampler", func() {
			_, _ = cache.Put("s0/g1", 10)
		})
	}()
	<-g1Ready

	if ringChurn {
		close(releaseG1Panic)
		<-g1Done
		for range 70 {
			_, putErr := cache.Put("s0/churn", 1)
			require.NoError(t, putErr)
		}
	}

	g2Done := make(chan struct{})
	go func() {
		defer close(g2Done)
		g2GID.Store(currentGoroutineID())
		_, putErr := cache.Put("s2/trigger", 10)
		assert.NoError(t, putErr)
	}()
	<-g2ReadHigh

	if !ringChurn {
		close(releaseG1Panic)
		<-g1Done
	}

	close(releaseOrphan)
	<-orphanDone
	close(releaseG2Return)
	<-g2Done

	_, present := cache.Peek("s2/live")
	assert.True(t, present, "orphan descendant of panicked sampler must not fast-forward G2's stale 0.95 sample")
	assert.Zero(t, cache.Stats().EvictionsPressure)
	assert.InDelta(t, 0.10, cache.Stats().MemoryPressure, 1e-9)
}

func TestShardedCache_PanickingSamplerGrandchildAndPost64RingOrphanIsolation(t *testing.T) {
	t.Run("GrandchildViaExitedIntermediate", func(t *testing.T) {
		runPanickingSamplerOrphanIsolationCase(t, false)
	})
	t.Run("Post64SampleRingEviction", func(t *testing.T) {
		runPanickingSamplerOrphanIsolationCase(t, true)
	})
}

func TestShardedCache_ConcurrentInvertedInvokeSeqPreservesPreSampleRecoveryEpochs(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var armed atomic.Bool
			var healthy atomic.Bool
			var probeSeq atomic.Int32
			gRecoverEntered := make(chan struct{})
			releaseGRecover := make(chan struct{})
			gSpikeReadHigh := make(chan struct{})
			releaseGSpike := make(chan struct{})

			c := NewShardedCache[int](
				80,
				4,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
				WithCompactionThreshold(0.80),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					if !armed.Load() {
						return 0.10
					}
					switch probeSeq.Add(1) {
					case 1:
						close(gRecoverEntered)
						<-releaseGRecover
						if healthy.Load() {
							return 0.10
						}
						return 0.95
					case 2:
						val := 0.95
						if healthy.Load() {
							val = 0.10
						}
						close(gSpikeReadHigh)
						<-releaseGSpike
						return val
					default:
						if healthy.Load() {
							return 0.10
						}
						return 0.95
					}
				}),
			)

			_, err := c.Put("s0/live", 16)
			require.NoError(t, err)

			armed.Store(true)
			gRecoverDone := make(chan struct{})
			go func() {
				defer close(gRecoverDone)
				_, putErr := c.Put("s3/recover", 2)
				assert.NoError(t, putErr)
			}()
			<-gRecoverEntered

			gSpikeDone := make(chan struct{})
			go func() {
				defer close(gSpikeDone)
				_, putErr := c.Put("s0/spike", 2)
				assert.NoError(t, putErr)
			}()
			<-gSpikeReadHigh

			healthy.Store(true)
			close(releaseGRecover)
			<-gRecoverDone
			require.InDelta(t, 0.10, c.Stats().MemoryPressure, 1e-9)

			close(releaseGSpike)
			<-gSpikeDone

			_, present := c.Peek("s0/live")
			assert.True(t, present, "stale pre-recovery 0.95 spike must not shed s0/live after recovery")
			assert.Zero(t, c.Stats().EvictionsPressure)
			assert.InDelta(t, 0.10, c.Stats().MemoryPressure, 1e-9)
		})
	}
}

func TestShardedCache_ReentrantFastForwardEnforcesInvokeSeqAndRecoveryCausality(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			var armed atomic.Bool
			var healthy atomic.Bool
			var probeSeq atomic.Int32
			g1ReadHigh := make(chan struct{})
			releaseG1Delete := make(chan struct{})
			var cache Cache[int]

			cache = NewShardedCache[int](
				80,
				4,
				explicitTagShardFunc,
				WithBackend(backend),
				WithInvariantChecking(true),
				WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
				WithCompactionThreshold(0.80),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				WithPressureFunc(func() float64 {
					if !armed.Load() {
						return 0.10
					}
					if probeSeq.Add(1) == 1 {
						val := 0.95
						close(g1ReadHigh)
						<-releaseG1Delete
						_, _ = cache.Delete("s1/scratch")
						return val
					}
					if healthy.Load() {
						return 0.10
					}
					return 0.95
				}),
			)

			_, err := cache.Put("s0/live", 16)
			require.NoError(t, err)
			_, err = cache.Put("s1/scratch", 2)
			require.NoError(t, err)

			armed.Store(true)
			g1Done := make(chan struct{})
			go func() {
				defer close(g1Done)
				_, putErr := cache.Put("s0/trigger", 2)
				assert.NoError(t, putErr)
			}()
			<-g1ReadHigh

			healthy.Store(true)
			_, err = cache.Put("s3/healthy", 2)
			require.NoError(t, err)
			require.InDelta(t, 0.10, cache.Stats().MemoryPressure, 1e-9)

			close(releaseG1Delete)
			<-g1Done

			_, present := cache.Peek("s0/live")
			assert.True(t, present, "re-entrant fast-forwarded pre-recovery 0.95 sample must not shed s0/live")
			assert.Zero(t, cache.Stats().EvictionsPressure)
			assert.InDelta(t, 0.10, cache.Stats().MemoryPressure, 1e-9)
		})
	}
}

func runAcceptedHealthyFallbackInvalidatesCachedSpikeCase(t *testing.T, backend Backend) {
	t.Helper()
	var armed atomic.Bool
	var healthy atomic.Bool
	var gRecoverGID, gSpikeGID, delPrefGID atomic.Uint64
	var gRecoverSamples atomic.Int32
	stepReq := make(chan struct{}, 1)
	stepAck := make(chan struct{}, 1)
	fifthReady := make(chan struct{})
	releaseFifth := make(chan struct{})
	spikeEntered := make(chan struct{})
	releaseSpike := make(chan struct{})

	var cache Cache[string]
	cache = NewShardedCache[string](
		80,
		4,
		explicitTagShardFunc,
		WithBackend(backend),
		WithInvariantChecking(true),
		WithWeigher(func(_, v string) uint64 { return uint64(len(v)) }),
		WithCompactionThreshold(0.80),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.10
			}
			gid := currentGoroutineID()
			if gRecoverGID.Load() != 0 && gid == gRecoverGID.Load() {
				n := gRecoverSamples.Add(1)
				if n <= 4 {
					stepReq <- struct{}{}
					<-stepAck
					return 0.95
				}
				if n == 5 {
					close(fifthReady)
					<-releaseFifth
					return 0.10
				}
			}
			if gSpikeGID.Load() != 0 && gid == gSpikeGID.Load() {
				close(spikeEntered)
				<-releaseSpike
				_, _ = cache.Delete("s2/reentrant_del")
				return 0.95
			}
			if healthy.Load() {
				return 0.10
			}
			return 0.95
		}),
	)

	_, err := cache.Put("s3/live", "0123456789abcdef") // 16B > tier2Target 10B
	require.NoError(t, err)
	_, err = cache.Put("s3/del/target", "ab")
	require.NoError(t, err)
	_, err = cache.Put("s1/anchor", "a")
	require.NoError(t, err)
	_, err = cache.Put("s2/reentrant_del", "ab")
	require.NoError(t, err)
	for i := range 4 {
		_, err = cache.Put(fmt.Sprintf("s2/v%d", i), "a")
		require.NoError(t, err)
	}

	releaseS3 := holdShardRLockViaKeys(t, cache, "s3/")
	delPrefReady := make(chan struct{})
	delPrefDone := make(chan struct{})
	go func() {
		defer close(delPrefDone)
		delPrefGID.Store(currentGoroutineID())
		close(delPrefReady)
		cache.DeletePrefix("s3/del/")
	}()
	<-delPrefReady
	waitForGoroutineParkedInRWMutexLock(t, delPrefGID.Load())

	spikeDone := make(chan struct{})
	stopWorker := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for idx := 0; ; {
			select {
			case <-stopWorker:
				return
			case <-stepReq:
				if idx < 4 {
					_, _ = cache.Delete(fmt.Sprintf("s2/v%d", idx))
					idx++
					if idx == 4 {
						// Start G_spike at epoch 4 so its invokeSeq (M) precedes G_recover's 5th sample (M+1).
						go func() {
							defer close(spikeDone)
							gSpikeGID.Store(currentGoroutineID())
							_, putErr := cache.Put("s0/spike_empty", "x")
							assert.NoError(t, putErr)
						}()
						<-spikeEntered
					}
				}
				stepAck <- struct{}{}
			}
		}
	}()

	armed.Store(true)
	gRecoverDone := make(chan struct{})
	go func() {
		defer close(gRecoverDone)
		gRecoverGID.Store(currentGoroutineID())
		_, putErr := cache.Put("s1/recover_put", "v")
		assert.NoError(t, putErr)
	}()
	<-fifthReady

	close(releaseSpike)
	<-spikeDone

	healthy.Store(true)
	close(releaseFifth)
	<-gRecoverDone
	close(stopWorker)
	<-workerDone

	releaseS3()
	<-delPrefDone

	_, present := cache.Peek("s3/live")
	assert.True(t, present, "DeletePrefix on shard 3 must not adopt stale cached 0.95 after healthy fallback recovery")
	assert.Zero(t, cache.Stats().EvictionsPressure)
	assert.InDelta(t, 0.10, cache.Stats().MemoryPressure, 1e-9)
}

func TestShardedCache_AcceptedHealthyFallbackInvalidatesStaleCachedElevatedPressure(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(backend.String(), func(t *testing.T) {
			runAcceptedHealthyFallbackInvalidatesCachedSpikeCase(t, backend)
		})
	}
}

func TestShardedCache_PreExistingWaitGroupWorkerNotMisclassifiedDuringCallbackWait(t *testing.T) {
	t.Run("OnEvictSynchronousDeliveryAndPanicIsolation", func(t *testing.T) {
		bgReady := make(chan struct{})
		triggerBg := make(chan struct{})
		bgFinished := make(chan struct{})
		var bgGID atomic.Uint64
		var bgCallbackDone atomic.Bool
		var bgCallbackRan atomic.Bool
		var asyncViolation atomic.Bool

		var c Cache[int]
		var bgWG sync.WaitGroup
		bgWG.Go(func() {
			bgGID.Store(currentGoroutineID())
			close(bgReady)
			<-triggerBg
			_, ok := c.Delete("s1/bg_key")
			assert.True(t, ok)
			if !bgCallbackDone.Load() {
				asyncViolation.Store(true)
			}
			close(bgFinished)
		})
		<-bgReady

		c = NewShardedCache[int](
			16,
			2,
			explicitTagShardFunc,
			WithInvariantChecking(true),
			WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
				if key == "s1/bg_key" {
					bgCallbackRan.Store(true)
					bgCallbackDone.Store(true)
					return
				}
				if key == "s0/k1" {
					var innerWG sync.WaitGroup
					innerWG.Go(func() {
						close(triggerBg)
						waitForGoroutineParkedInFrame(t, bgGID.Load(), "lockEvictCallbackSlow")
					})
					innerWG.Wait()
					panic("boom in s0/k1 callback")
				}
			}),
		)

		_, err := c.Put("s0/k1", 1)
		require.NoError(t, err)
		_, err = c.Put("s1/bg_key", 2)
		require.NoError(t, err)

		assert.PanicsWithValue(t, "boom in s0/k1 callback", func() {
			_, _ = c.Delete("s0/k1")
		})
		bgWG.Wait()
		<-bgFinished

		assert.True(t, bgCallbackRan.Load(), "pre-existing WaitGroup.Go worker callback must not be lost on G0 panic")
		assert.False(t, asyncViolation.Load(), "pre-existing WaitGroup.Go worker Delete must not return before its OnEvictEntry finishes")
	})

	t.Run("PressureFuncNotBypassedUnderCriticalPressure", func(t *testing.T) {
		triggerBg := make(chan struct{})
		bgEvalDone := make(chan struct{})
		var armed atomic.Bool
		var g0Sampling atomic.Bool

		var c Cache[int]
		var pa PressureAwareCache[int]
		var bgWG sync.WaitGroup
		bgWG.Go(func() {
			<-triggerBg
			_ = pa.EvaluateMemoryPressure()
			close(bgEvalDone)
		})

		c = NewShardedCache[int](
			200,
			2,
			explicitTagShardFunc,
			WithInvariantChecking(true),
			WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
			WithCompactionThreshold(0.80),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				if !armed.Load() {
					return 0.10
				}
				if g0Sampling.CompareAndSwap(true, false) {
					var innerWG sync.WaitGroup
					innerWG.Go(func() {
						close(triggerBg)
						<-bgEvalDone
					})
					innerWG.Wait()
					return 0.95
				}
				return 0.95
			}),
		)
		pa = c.(PressureAwareCache[int])

		_, err := c.Put("s1/live", 80)
		require.NoError(t, err)

		armed.Store(true)
		g0Sampling.Store(true)
		_, err = c.Put("s0/trigger", 5)
		require.NoError(t, err)
		bgWG.Wait()

		_, s1Present := c.Peek("s1/live")
		assert.False(t, s1Present, "pre-existing WaitGroup.Go worker must not bypass PressureFunc under 0.95 pressure")
	})

	t.Run("ExternalCompactAdvancesEpochAndInvalidatesStaleSample", func(t *testing.T) {
		triggerBg := make(chan struct{})
		bgCompactDone := make(chan struct{})
		var armed atomic.Bool
		var healthy atomic.Bool
		var g0Sampling atomic.Bool

		var c Cache[int]
		var pa PressureAwareCache[int]
		var bgWG sync.WaitGroup
		bgWG.Go(func() {
			<-triggerBg
			healthy.Store(true)
			pa.Compact()
			close(bgCompactDone)
		})

		c = NewShardedCache[int](
			200,
			2,
			explicitTagShardFunc,
			WithBackend(BackendArenaRadix),
			WithInvariantChecking(true),
			WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
			WithCompactionThreshold(0.80),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				if !armed.Load() || healthy.Load() {
					return 0.10
				}
				if g0Sampling.CompareAndSwap(true, false) {
					_, _ = c.Delete("s0/reentrant_del")
					var innerWG sync.WaitGroup
					innerWG.Go(func() {
						close(triggerBg)
						<-bgCompactDone
					})
					innerWG.Wait()
					return 0.95
				}
				return 0.95
			}),
		)
		pa = c.(PressureAwareCache[int])

		_, err := c.Put("s0/reentrant_del", 2)
		require.NoError(t, err)
		for i := range 10 {
			_, err = c.Put(fmt.Sprintf("s0/tmp_%d", i), 2)
			require.NoError(t, err)
		}
		for i := range 9 {
			_, _ = c.Delete(fmt.Sprintf("s0/tmp_%d", i))
		}
		_, err = c.Put("s1/keep_after_compact", 80)
		require.NoError(t, err)

		armed.Store(true)
		g0Sampling.Store(true)
		_, err = c.Put("s1/trigger", 5)
		require.NoError(t, err)
		bgWG.Wait()

		_, s1Present := c.Peek("s1/keep_after_compact")
		assert.True(t, s1Present, "external Compact on pre-existing WaitGroup.Go worker must advance externalReclaimEpoch and invalidate G0's 0.95 sample")
		assert.InDelta(t, 0.10, c.Stats().MemoryPressure, 1e-9)
	})
}

type reenteredOpService struct {
	t              *testing.T
	cache          Cache[int]
	bgGID          atomic.Uint64
	bgReady        chan struct{}
	triggerBg      chan struct{}
	bgFinished     chan struct{}
	bgCallbackDone *atomic.Bool
	asyncViolation *atomic.Bool
	cbFirst        bool
}

func (s *reenteredOpService) runOp(isCallback bool) {
	if s.cbFirst {
		if isCallback {
			close(s.triggerBg)
			waitForGoroutineParkedInFrame(s.t, s.bgGID.Load(), "lockEvictCallbackSlow")
			return
		}
		go func() {
			s.bgGID.Store(currentGoroutineID())
			close(s.bgReady)
			<-s.triggerBg
			_, _ = s.cache.Delete("s1/bg_key")
			if !s.bgCallbackDone.Load() {
				s.asyncViolation.Store(true)
			}
			close(s.bgFinished)
		}()
		<-s.bgReady
		_, _ = s.cache.Delete("s0/k1")
		return
	}
	if !isCallback {
		go func() {
			s.bgGID.Store(currentGoroutineID())
			close(s.bgReady)
			<-s.triggerBg
			_, _ = s.cache.Delete("s1/bg_key")
			if !s.bgCallbackDone.Load() {
				s.asyncViolation.Store(true)
			}
			close(s.bgFinished)
		}()
		<-s.bgReady
		_, _ = s.cache.Delete("s0/k1")
		return
	}
	close(s.triggerBg)
	waitForGoroutineParkedInFrame(s.t, s.bgGID.Load(), "lockEvictCallbackSlow")
}

func spawnInSharedHelper(fn func(), wait <-chan struct{}) {
	go fn()
	if wait != nil {
		<-wait
	}
}

func TestShardedCache_PreExistingGoroutineNotMisclassifiedAcrossReenteredMethodAndSpawnHelper(t *testing.T) {
	for _, cbFirst := range []bool{true, false} {
		name := "ReEnteredServiceMethodNonCallbackFirst"
		if cbFirst {
			name = "ReEnteredServiceMethodCallbackFirst"
		}
		t.Run(name, func(t *testing.T) {
			var bgCallbackDone atomic.Bool
			var bgCallbackRan atomic.Bool
			var asyncViolation atomic.Bool

			svc := &reenteredOpService{
				t:              t,
				bgReady:        make(chan struct{}),
				triggerBg:      make(chan struct{}),
				bgFinished:     make(chan struct{}),
				bgCallbackDone: &bgCallbackDone,
				asyncViolation: &asyncViolation,
				cbFirst:        cbFirst,
			}
			svc.cache = NewShardedCache[int](
				16,
				2,
				explicitTagShardFunc,
				WithInvariantChecking(true),
				WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
					if key == "s1/bg_key" {
						bgCallbackRan.Store(true)
						bgCallbackDone.Store(true)
						return
					}
					if key == "s0/k1" {
						svc.runOp(true)
						panic("boom in reentered runOp callback")
					}
				}),
			)

			_, err := svc.cache.Put("s0/k1", 1)
			require.NoError(t, err)
			_, err = svc.cache.Put("s1/bg_key", 2)
			require.NoError(t, err)

			assert.PanicsWithValue(t, "boom in reentered runOp callback", func() {
				svc.runOp(false)
			})
			<-svc.bgFinished
			assert.True(t, bgCallbackRan.Load(), "pre-existing goroutine callback must not be lost when active callback panics")
			assert.False(t, asyncViolation.Load(), "pre-existing goroutine Delete must wait synchronously for OnEvictEntry")
		})
	}

	for _, sharded := range []bool{false, true} {
		mode := "UnshardedMapCache"
		if sharded {
			mode = "ShardedCache"
		}
		t.Run("SharedSpawnHelper/"+mode, func(t *testing.T) {
			bgReady := make(chan struct{})
			triggerBg := make(chan struct{})
			bgFinished := make(chan struct{})
			var bgGID atomic.Uint64
			var bgCallbackDone atomic.Bool
			var bgCallbackRan atomic.Bool
			var asyncViolation atomic.Bool

			var c Cache[int]
			spawnInSharedHelper(func() {
				bgGID.Store(currentGoroutineID())
				close(bgReady)
				<-triggerBg
				_, ok := c.Delete("s1/bg_key")
				assert.True(t, ok)
				if !bgCallbackDone.Load() {
					asyncViolation.Store(true)
				}
				close(bgFinished)
			}, nil)
			<-bgReady

			evictOpt := WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
				if key == "s1/bg_key" {
					bgCallbackRan.Store(true)
					bgCallbackDone.Store(true)
					return
				}
				if key == "s0/k1" {
					innerDone := make(chan struct{})
					spawnInSharedHelper(func() {
						defer close(innerDone)
						close(triggerBg)
						waitForGoroutineParkedInFrame(t, bgGID.Load(), "lockEvictCallbackSlow")
					}, innerDone)
					panic("boom in shared helper callback")
				}
			})
			if sharded {
				c = NewShardedCache[int](16, 2, explicitTagShardFunc, WithInvariantChecking(true), evictOpt)
			} else {
				c = NewMapCache[int](16, WithInvariantChecking(true), evictOpt)
			}

			_, err := c.Put("s0/k1", 1)
			require.NoError(t, err)
			_, err = c.Put("s1/bg_key", 2)
			require.NoError(t, err)

			assert.PanicsWithValue(t, "boom in shared helper callback", func() {
				_, _ = c.Delete("s0/k1")
			})
			<-bgFinished

			assert.True(t, bgCallbackRan.Load(), "pre-existing goroutine spawned via shared helper must not lose OnEvictEntry")
			assert.False(t, asyncViolation.Load(), "pre-existing goroutine Delete must wait synchronously for OnEvictEntry")
		})
	}
}

func runDirectCallbackHelper(done <-chan struct{}, fn func()) {
	go fn()
	<-done
}

func waitChanHelper(done <-chan struct{}) {
	<-done
}

type reenteredWaitHelperService struct {
	c       Cache[int]
	cbFirst bool
}

func (s *reenteredWaitHelperService) runOp(inCallback bool, evictKey string) {
	if s.cbFirst {
		if inCallback {
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = s.c.Delete(evictKey)
			}()
			waitChanHelper(done)
			return
		}
		_, _ = s.c.Put("s0/k3", 3)
		return
	}
	if !inCallback {
		_, _ = s.c.Put("s0/k3", 3)
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.c.Delete(evictKey)
	}()
	waitChanHelper(done)
}

func TestShardedCache_QueuedPendingEvictCallbackSpawnsSiblingClosureWithoutDeadlock(t *testing.T) {
	for _, typeErased := range []bool{false, true} {
		name := "TypedCallback"
		if typeErased {
			name = "TypeErasedCallback"
		}
		t.Run(name, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				defer close(done)
				var c Cache[int]
				var evicted []string
				var evictMu sync.Mutex
				workerDone := make(chan struct{})
				worker := func() {
					defer close(workerDone)
					_, _ = c.Delete("s1/k3")
				}

				handleEvict := func(key string) {
					evictMu.Lock()
					evicted = append(evicted, key)
					evictMu.Unlock()
					switch key {
					case "s0/k1":
						_, _ = c.Delete("s1/k2")
					case "s1/k2":
						go worker()
						<-workerDone
					}
				}

				var evictOpt Option
				if typeErased {
					evictOpt = WithOnEvictEntry[any](func(key string, _ any, _ EvictionReason) {
						handleEvict(key)
					})
				} else {
					evictOpt = WithOnEvictEntry[int](func(key string, _ int, _ EvictionReason) {
						handleEvict(key)
					})
				}

				c = NewShardedCache[int](16, 2, explicitTagShardFunc, WithInvariantChecking(true), evictOpt)
				_, err := c.Put("s0/k1", 1)
				assert.NoError(t, err)
				_, err = c.Put("s1/k2", 2)
				assert.NoError(t, err)
				_, err = c.Put("s1/k3", 3)
				assert.NoError(t, err)

				_, ok := c.Delete("s0/k1")
				assert.True(t, ok)

				evictMu.Lock()
				defer evictMu.Unlock()
				assert.Equal(t, []string{"s0/k1", "s1/k2", "s1/k3"}, evicted)
			}()

			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("deadlocked when queued pending eviction callback spawned sibling closure")
			}
		})
	}
}

func TestShardedCache_RecoveredPressureFuncPanicDoesNotPoisonSiblingSamplerOrLeakGIDs(t *testing.T) {
	t.Run("SiblingSamplerChildReentrancyPreservedAfterParentPanic", func(t *testing.T) {
		var g0GID, g2GID atomic.Uint64
		var probeMu sync.Mutex
		var reenteredCount atomic.Int32
		c2Ready := make(chan struct{})
		releaseC2 := make(chan struct{})
		c2Done := make(chan struct{})
		g0Entered := make(chan struct{})
		releaseG0Panic := make(chan struct{})

		var c Cache[int]
		var pa PressureAwareCache[int]
		c = NewShardedCache[int](
			64,
			2,
			explicitTagShardFunc,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				gid := currentGoroutineID()
				if gid != 0 && gid == g2GID.Load() {
					probeMu.Lock()
					defer probeMu.Unlock()
					go func() {
						defer close(c2Done)
						close(c2Ready)
						<-releaseC2
						_, _ = c.Delete("s1/existing")
						_ = pa.EvaluateMemoryPressure()
					}()
					<-c2Done
					return 0.10
				}
				if gid != 0 && gid == g0GID.Load() {
					close(g0Entered)
					<-releaseG0Panic
					panic("transient g0 pressure panic")
				}
				if !probeMu.TryLock() {
					reenteredCount.Add(1)
					return 0.10
				}
				probeMu.Unlock()
				return 0.10
			}),
		)
		pa = c.(PressureAwareCache[int])
		sc := c.(*shardedCache[int])

		_, err := c.Put("s1/existing", 10)
		require.NoError(t, err)

		allDone := make(chan struct{})
		go func() {
			defer close(allDone)
			g0GID.Store(currentGoroutineID())

			g2Done := make(chan struct{})
			go func() {
				defer close(g2Done)
				g2GID.Store(currentGoroutineID())
				_ = pa.EvaluateMemoryPressure()
			}()
			<-c2Ready

			var recovered any
			func() {
				defer func() {
					recovered = recover()
				}()
				go func() {
					<-g0Entered
					close(releaseG0Panic)
				}()
				_ = pa.EvaluateMemoryPressure()
			}()
			assert.Equal(t, "transient g0 pressure panic", recovered)

			extBefore := sc.coord.externalReclaimEpoch.Load()
			close(releaseC2)
			<-g2Done
			assert.Equal(t, extBefore, sc.coord.externalReclaimEpoch.Load(),
				"sibling sampler child Delete must be recognized as re-entrant rather than external reclamation")
		}()

		select {
		case <-allDone:
		case <-time.After(3 * time.Second):
			t.Fatal("deadlocked or poisoned sibling sampler child after recovered PressureFunc panic")
		}
		assert.Zero(t, reenteredCount.Load(), "healthy sibling sampler child must not bypass PressureFunc re-entrancy guard")
	})

	t.Run("BoundedPanickedTrackingAfterRecoveredDispatcherPanic", func(t *testing.T) {
		var dispGID, bgSamplerGID atomic.Uint64
		bgEntered := make(chan struct{})
		releaseBg := make(chan struct{})
		var shouldPanic atomic.Bool

		c := NewShardedCache[int](
			512,
			2,
			explicitTagShardFunc,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				gid := currentGoroutineID()
				if gid != 0 && gid == dispGID.Load() && shouldPanic.CompareAndSwap(true, false) {
					panic("dispatcher transient panic")
				}
				if gid != 0 && gid == bgSamplerGID.Load() {
					close(bgEntered)
					<-releaseBg
					return 0.10
				}
				return 0.10
			}),
		)
		pa := c.(PressureAwareCache[int])
		sc := c.(*shardedCache[int])

		dispDone := make(chan struct{})
		go func() {
			defer close(dispDone)
			dispGID.Store(currentGoroutineID())
			shouldPanic.Store(true)
			func() {
				defer func() { _ = recover() }()
				_ = pa.EvaluateMemoryPressure()
			}()

			bgDone := make(chan struct{})
			go func() {
				defer close(bgDone)
				bgSamplerGID.Store(currentGoroutineID())
				_ = pa.EvaluateMemoryPressure()
			}()
			<-bgEntered

			descSeqBefore := sc.coord.panickedDescendantSeq.Load()
			rootSeqBefore := sc.coord.panickedRootSeq.Load()
			for i := range 250 {
				reqDone := make(chan struct{})
				go func(idx int) {
					defer close(reqDone)
					_, _ = c.Put(fmt.Sprintf("s0/req_%d", idx), idx)
				}(i)
				<-reqDone
			}
			assert.Equal(t, descSeqBefore, sc.coord.panickedDescendantSeq.Load(),
				"post-recovery request goroutines must not virally insert into panickedDescendantGIDs")
			assert.Equal(t, rootSeqBefore, sc.coord.panickedRootSeq.Load(),
				"post-recovery request goroutines must not advance panickedRootSeq")

			close(releaseBg)
			<-bgDone
		}()
		<-dispDone
	})
}

func runCallbackHelperPreHookClosureCase(t *testing.T, sharded bool) {
	t.Helper()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		var c Cache[int]
		done := make(chan struct{})
		cleanup := func() {
			defer close(done)
			_, _ = c.Delete("s1/k2")
		}
		evictOpt := WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s0/k1" && r == EvictionReasonCapacity {
				runDirectCallbackHelper(done, cleanup)
			}
		})
		if sharded {
			c = NewShardedCache[int](2, 2, explicitTagShardFunc, WithInvariantChecking(true), evictOpt)
		} else {
			c = New[int](2, WithInvariantChecking(true), evictOpt)
		}
		_, _ = c.Put("s0/k1", 1)
		_, _ = c.Put("s1/k2", 2)
		_, _ = c.Put("s0/k3", 3)
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("deadlocked when callback helper spawned pre-hook closure")
	}
}

func runCallbackMixedWaitGroupGoCase(t *testing.T, sharded bool) {
	t.Helper()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		var c Cache[int]
		hold := make(chan struct{})
		cleanup := func() {
			defer close(hold)
			_, _ = c.Delete("s1/k2")
		}
		evictOpt := WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s0/k1" && r == EvictionReasonCapacity {
				var wg sync.WaitGroup
				wg.Go(func() { <-hold })
				wg.Go(cleanup)
				wg.Wait()
			}
		})
		if sharded {
			c = NewShardedCache[int](2, 2, explicitTagShardFunc, WithInvariantChecking(true), evictOpt)
		} else {
			c = New[int](2, WithInvariantChecking(true), evictOpt)
		}
		_, _ = c.Put("s0/k1", 1)
		_, _ = c.Put("s1/k2", 2)
		_, _ = c.Put("s0/k3", 3)
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("deadlocked when callback WaitGroup.Go launched both inline and outer closures")
	}
}

func runCallbackReenteredWaitHelperCase(t *testing.T, sharded, cbFirst bool) {
	t.Helper()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		svc := &reenteredWaitHelperService{cbFirst: cbFirst}
		var probeArmed atomic.Bool
		var probeReentered atomic.Int32
		var inPrimaryProbe atomic.Bool
		opts := []Option{
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				if !probeArmed.Load() {
					return 0.10
				}
				if inPrimaryProbe.CompareAndSwap(false, true) {
					defer inPrimaryProbe.Store(false)
					probeArmed.Store(false)
					svc.runOp(true, "s1/probe_del")
					return 0.10
				}
				probeReentered.Add(1)
				return 0.10
			}),
			WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
				if k == "s0/k1" && r == EvictionReasonCapacity {
					svc.runOp(true, "s1/k2")
				}
			}),
		}
		if sharded {
			svc.c = NewShardedCache[int](2, 2, explicitTagShardFunc, opts...)
		} else {
			svc.c = New[int](2, opts...)
		}
		_, _ = svc.c.Put("s0/k1", 1)
		_, _ = svc.c.Put("s1/k2", 2)
		_, _ = svc.c.Delete("s1/k2")
		_, _ = svc.c.Put("s1/k2", 2)
		probeArmed.Store(true)
		svc.runOp(false, "")
		assert.Zero(t, probeReentered.Load(),
			"re-entered method child waiting via helper inside PressureFunc must not re-enter PressureFunc")
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("deadlocked when re-entered method waited via helper function")
	}
}

func TestShardedCache_CallbackChildLineageHelperSpawnMixedWaitGroupAndReenteredWaitHelper(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		t.Run("HelperSpawningPreHookClosure/"+mode, func(t *testing.T) {
			runCallbackHelperPreHookClosureCase(t, sharded)
		})
		t.Run("MixedWaitGroupGoInlineAndOuterClosure/"+mode, func(t *testing.T) {
			runCallbackMixedWaitGroupGoCase(t, sharded)
		})
		for _, cbFirst := range []bool{true, false} {
			orderName := "NonCallbackFirst"
			if cbFirst {
				orderName = "CallbackFirst"
			}
			t.Run("ReenteredMethodWaitingViaHelper/"+mode+"/"+orderName, func(t *testing.T) {
				runCallbackReenteredWaitHelperCase(t, sharded, cbFirst)
			})
		}
	}
}

func pkgNoopCallbackWorker() {}

func runTwoInCallbackHelper(fn1, fn2 func()) {
	var wg sync.WaitGroup
	wg.Go(fn1)
	wg.Go(fn2)
	wg.Wait()
}

func makeShardedOrUnshardedIntCacheWithBackend(sharded bool, backend Backend, maxSize uint64, opts ...Option) Cache[int] {
	allOpts := make([]Option, 0, len(opts)+1)
	allOpts = append(allOpts, WithBackend(backend))
	allOpts = append(allOpts, opts...)
	if sharded {
		return NewShardedCache[int](maxSize, 2, explicitTagShardFunc, allOpts...)
	}
	return New[int](maxSize, allOpts...)
}

func makeShardedOrUnshardedIntCache(sharded bool, maxSize uint64, opts ...Option) Cache[int] {
	if sharded {
		return NewShardedCache[int](maxSize, 2, explicitTagShardFunc, opts...)
	}
	return New[int](maxSize, opts...)
}

func warmupUncontendedReplaceEvictions(c Cache[int], count int) {
	_, _ = c.Put("s0/warm", 0)
	for i := range count {
		_ = c.Replace("s0/warm", i+1)
	}
	_, _ = c.Delete("s0/warm")
}

func warmupUncontendedPressurePuts(c Cache[int], count int) {
	for i := range count {
		_, _ = c.Put("s0/warm", i+1)
	}
	_, _ = c.Delete("s0/warm")
}

type multiChildReenteredRunner struct {
	c        Cache[int]
	evictHit *atomic.Int32
}

func (r *multiChildReenteredRunner) execStep(inCallback bool, outerFn func()) {
	if !inCallback {
		_, _ = r.c.Put("s0/trigger", 99)
		return
	}
	var wg sync.WaitGroup
	wg.Go(outerFn)
	wg.Go(func() {
		_, _ = r.c.Get("s1/other")
	})
	wg.Wait()
	r.evictHit.Add(1)
}

func runC1EvictMultiChildVariant(t *testing.T, sharded, warmup bool, variant int) {
	t.Helper()
	var c Cache[int]
	var childCallbacks atomic.Int32
	var stepHits atomic.Int32
	armed := atomic.Bool{}

	outerOwnedClosure := func() {
		_, ok := c.Delete("s1/victim")
		assert.True(t, ok)
	}
	runner := &multiChildReenteredRunner{evictHit: &stepHits}
	c = makeShardedOrUnshardedIntCache(sharded, 2,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s1/victim" && r == EvictionReasonDeleted {
				childCallbacks.Add(1)
				return
			}
			if !armed.Load() || k != "s0/seed" || r != EvictionReasonCapacity {
				return
			}
			switch variant {
			case 0:
				var wg sync.WaitGroup
				wg.Go(outerOwnedClosure)
				wg.Go(func() { _, _ = c.Get("s1/other") })
				wg.Go(pkgNoopCallbackWorker)
				wg.Wait()
			case 1:
				runTwoInCallbackHelper(outerOwnedClosure, func() { _, _ = c.Get("s1/other") })
			default:
				runner.execStep(true, outerOwnedClosure)
			}
		}),
	)
	runner.c = c
	if warmup {
		warmupUncontendedReplaceEvictions(c, 75)
	}
	_, _ = c.Put("s0/seed", 1)
	_, _ = c.Put("s1/victim", 2)
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if variant == 2 {
			runner.execStep(false, outerOwnedClosure)
		} else {
			_, _ = c.Put("s0/trigger", 3)
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("deadlocked in C1 OnEvict variant=%d sharded=%v warmup=%v", variant, sharded, warmup)
	}
	assert.Equal(t, int32(1), childCallbacks.Load())
}

func runC1PressureMultiChildVariant(t *testing.T, sharded, warmup bool) {
	t.Helper()
	var c Cache[int]
	var armed atomic.Bool
	var reentered atomic.Int32
	inHook := atomic.Bool{}

	outerOwnedClosure := func() {
		_, _ = c.Put("s1/from_child", 10)
	}
	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.10
			}
			if !inHook.CompareAndSwap(false, true) {
				reentered.Add(1)
				return 0.10
			}
			defer inHook.Store(false)
			var wg sync.WaitGroup
			wg.Go(outerOwnedClosure)
			wg.Go(func() { _, _ = c.Get("s1/other") })
			wg.Go(pkgNoopCallbackWorker)
			wg.Wait()
			return 0.10
		}),
	)
	if warmup {
		warmupUncontendedPressurePuts(c, 75)
	}
	armed.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.Put("s0/trigger", 1)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("deadlocked in C1 PressureFunc sharded=%v warmup=%v", sharded, warmup)
	}
	assert.Zero(t, reentered.Load())
}

func TestShardedCache_MultiChildCallbackDescendantsLineage(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, warmup := range []bool{false, true} {
			warmName := "Cold"
			if warmup {
				warmName = "PostWarmup"
			}
			t.Run("OnEvict_WG/"+mode+"/"+warmName, func(t *testing.T) {
				runC1EvictMultiChildVariant(t, sharded, warmup, 0)
			})
			t.Run("OnEvict_Helper/"+mode+"/"+warmName, func(t *testing.T) {
				runC1EvictMultiChildVariant(t, sharded, warmup, 1)
			})
			t.Run("OnEvict_ReenteredTwoGo/"+mode+"/"+warmName, func(t *testing.T) {
				runC1EvictMultiChildVariant(t, sharded, warmup, 2)
			})
			t.Run("PressureFunc_WG/"+mode+"/"+warmName, func(t *testing.T) {
				runC1PressureMultiChildVariant(t, sharded, warmup)
			})
		}
	}
}

func coordPressureState(c Cache[int]) *pressureState {
	if sc, ok := c.(*shardedCache[int]); ok {
		return &sc.coord
	}
	if provider, ok := c.(interface{ pressureStatePtr() *pressureState }); ok {
		return provider.pressureStatePtr()
	}
	return nil
}

func waitUntilEvictSlowWaiterParked(c Cache[int]) bool {
	p := coordPressureState(c)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p != nil && p.evictSlowWaiters.Load() > 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func waitUntilEvictContendedFor(c Cache[int], hold time.Duration) {
	waitUntilEvictSlowWaiterParked(c)
	if hold > 0 {
		<-time.After(hold)
	}
}

func waitUntilEvictContended(c Cache[int]) {
	waitUntilEvictContendedFor(c, 5*time.Millisecond)
}

func launchC2HelperWorker(done chan struct{}, fn func()) {
	go func() {
		defer close(done)
		fn()
	}()
}

func runC2PreExistingEvictCase(t *testing.T, sharded, warmup bool, spawnMode int, shouldPanic bool) {
	t.Helper()
	var c Cache[int]
	armed := atomic.Bool{}
	inFgEvict := make(chan struct{})
	var fgInEvict atomic.Bool
	var bgEvictDone atomic.Bool
	var bgSawSyncEvict atomic.Bool
	var bgReturnedDuringFg atomic.Bool
	var sawParked atomic.Bool

	holdDur := 10 * time.Millisecond
	if warmup && spawnMode == 0 && shouldPanic {
		holdDur = 380 * time.Millisecond
	}

	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s1/bg" && r == EvictionReasonDeleted {
				bgEvictDone.Store(true)
				return
			}
			if armed.Load() && k == "s0/fg" && r == EvictionReasonReplaced {
				fgInEvict.Store(true)
				defer fgInEvict.Store(false)
				close(inFgEvict)
				var innerWG sync.WaitGroup
				innerWG.Go(func() {
					pkgNoopCallbackWorker()
					sawParked.Store(waitUntilEvictSlowWaiterParked(c))
					<-time.After(holdDur)
				})
				innerWG.Wait()
				if shouldPanic {
					panic("fg boom")
				}
			}
		}),
	)
	if warmup {
		warmupUncontendedReplaceEvictions(c, 75)
	}
	_, _ = c.Put("s0/fg", 1)
	_, _ = c.Put("s1/bg", 2)
	armed.Store(true)

	var outerWG sync.WaitGroup
	bgDone := make(chan struct{})
	bgWork := func() {
		<-inFgEvict
		_, ok := c.Delete("s1/bg")
		assert.True(t, ok)
		bgReturnedDuringFg.Store(fgInEvict.Load())
		bgSawSyncEvict.Store(bgEvictDone.Load())
	}
	switch spawnMode {
	case 0:
		outerWG.Go(func() {
			defer close(bgDone)
			bgWork()
		})
	case 1:
		go func() {
			defer close(bgDone)
			bgWork()
		}()
	default:
		launchC2HelperWorker(bgDone, bgWork)
	}
	if shouldPanic {
		assert.PanicsWithValue(t, "fg boom", func() {
			_ = c.Replace("s0/fg", 10)
		})
	} else {
		require.NoError(t, c.Replace("s0/fg", 10))
	}
	<-bgDone
	outerWG.Wait()
	assert.True(t, sawParked.Load(), "peer goroutine must park on evictCallbackMu.Lock rather than enqueueing as a descendant")
	assert.False(t, bgReturnedDuringFg.Load(), "pre-existing goroutine Delete must not return while foreground OnEvict still holds evictCallbackMu")
	assert.True(t, bgEvictDone.Load(), "pre-existing goroutine must not lose its OnEvict callback")
	assert.True(t, bgSawSyncEvict.Load(), "pre-existing goroutine Delete must run OnEvict synchronously before returning")
}

func runC2OuterWGBeforeEvictPanic(t *testing.T, sharded, warmup bool) {
	t.Helper()
	runC2PreExistingEvictCase(t, sharded, warmup, 0, true)
}

type c2BranchOrderService struct {
	c         Cache[int]
	inFgEvict chan struct{}
	bgDone    chan struct{}
	evictRan  *atomic.Bool
	syncRan   *atomic.Bool
}

func (s *c2BranchOrderService) dispatch(mode int) {
	switch mode {
	case 0:
		_, _ = s.c.Delete("s0/fg")
	case 1:
		go func() {
			defer close(s.bgDone)
			<-s.inFgEvict
			_, ok := s.c.Delete("s1/bg")
			if ok && s.evictRan.Load() {
				s.syncRan.Store(true)
			}
		}()
	default:
		close(s.inFgEvict)
		waitUntilEvictContended(s.c)
		panic("branch boom")
	}
}

func runC2MultiBranchSpawnLineOrder(t *testing.T, sharded bool) {
	t.Helper()
	var evictRan, syncRan atomic.Bool
	svc := &c2BranchOrderService{
		inFgEvict: make(chan struct{}),
		bgDone:    make(chan struct{}),
		evictRan:  &evictRan,
		syncRan:   &syncRan,
	}
	svc.c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s1/bg" && r == EvictionReasonDeleted {
				evictRan.Store(true)
				return
			}
			if k == "s0/fg" && r == EvictionReasonDeleted {
				svc.dispatch(2)
			}
		}),
	)
	_, _ = svc.c.Put("s0/fg", 1)
	_, _ = svc.c.Put("s1/bg", 2)
	svc.dispatch(1)
	assert.PanicsWithValue(t, "branch boom", func() {
		svc.dispatch(0)
	})
	<-svc.bgDone
	assert.True(t, evictRan.Load())
	assert.True(t, syncRan.Load())
}

func createCompactionSlackOnS0(c Cache[int]) {
	for i := range 20 {
		_, _ = c.Put(fmt.Sprintf("s0/slack_%02d", i), i)
	}
	for i := range 20 {
		_, _ = c.Delete(fmt.Sprintf("s0/slack_%02d", i))
	}
}

func runC2OuterWGBeforePressureAndCompact(t *testing.T, sharded, warmup bool) {
	t.Helper()
	var c Cache[int]
	var armed atomic.Bool
	var pressureBits atomic.Uint64
	pressureBits.Store(0x3fb999999999999a) // 0.10
	fgEntered := make(chan struct{})
	releaseFg := make(chan struct{})
	var fgPaused atomic.Bool
	var bgProbeCalls atomic.Int32

	c = makeShardedOrUnshardedIntCache(sharded, 64,
		WithBackend(BackendMap),
		WithEvictionRetentionRatio(0.0),
		WithPressureFunc(func() float64 {
			if armed.Load() && fgPaused.CompareAndSwap(false, true) {
				close(fgEntered)
				<-releaseFg
				return 0.95
			}
			if armed.Load() {
				bgProbeCalls.Add(1)
			}
			return float64FromBitsLocal(pressureBits.Load())
		}),
	)
	if warmup {
		warmupUncontendedPressurePuts(c, 75)
	}
	createCompactionSlackOnS0(c)
	_, _ = c.Put("s1/live", 42)
	armed.Store(true)

	var outerWG sync.WaitGroup
	outerWG.Go(func() {
		<-fgEntered
		c.(PressureAwareCache[int]).Compact()
		_, _ = c.Put("s1/bg_probe", 7)
		close(releaseFg)
	})
	_, _ = c.Put("s1/fg_write", 9)
	outerWG.Wait()

	assert.GreaterOrEqual(t, bgProbeCalls.Load(), int32(1), "pre-existing goroutine must not bypass PressureFunc")
	_, ok := c.Peek("s1/live")
	assert.True(t, ok, "external Compact from pre-existing goroutine must invalidate foreground stale 0.95 sample")
}

func float64FromBitsLocal(b uint64) float64 {
	if b == 0x3fee666666666666 {
		return 0.95
	}
	return 0.10
}

func TestShardedCache_PreExistingGoroutineNotMisclassifiedAsCallbackChild(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, warmup := range []bool{false, true} {
			warmName := "Cold"
			if warmup {
				warmName = "PostWarmup"
			}
			t.Run("OnEvictPanicAndSync/"+mode+"/"+warmName, func(t *testing.T) {
				runC2OuterWGBeforeEvictPanic(t, sharded, warmup)
			})
			t.Run("OnEvictNormalReturnSync/"+mode+"/"+warmName, func(t *testing.T) {
				runC2PreExistingEvictCase(t, sharded, warmup, 0, false)
			})
			t.Run("DirectGoEvictPanicAndNormal/"+mode+"/"+warmName, func(t *testing.T) {
				runC2PreExistingEvictCase(t, sharded, warmup, 1, true)
				runC2PreExistingEvictCase(t, sharded, warmup, 1, false)
			})
			t.Run("HelperGoEvictPanicAndNormal/"+mode+"/"+warmName, func(t *testing.T) {
				runC2PreExistingEvictCase(t, sharded, warmup, 2, true)
				runC2PreExistingEvictCase(t, sharded, warmup, 2, false)
			})
			t.Run("PressureAndCompact/"+mode+"/"+warmName, func(t *testing.T) {
				runC2OuterWGBeforePressureAndCompact(t, sharded, warmup)
			})
		}
		t.Run("MultiBranchSpawnLineOrder/"+mode, func(t *testing.T) {
			runC2MultiBranchSpawnLineOrder(t, sharded)
		})
	}
}

func spawnHelperAsyncForSleepPoll(fn func()) {
	go fn()
}

func sleepPollUntilDone(done *atomic.Bool) {
	for !done.Load() {
		time.Sleep(1 * time.Millisecond)
	}
}

func runPostWarmupSleepPollChildCase(t *testing.T, sharded, useReplace bool, variant int) {
	t.Helper()
	var c Cache[int]
	var childEvicts atomic.Int32
	var armed atomic.Bool
	var childDone atomic.Bool

	outerChild := func() {
		defer childDone.Store(true)
		_, ok := c.Delete("s1/victim")
		assert.True(t, ok)
	}

	c = makeShardedOrUnshardedIntCache(sharded, 2,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s1/victim" && r == EvictionReasonDeleted {
				childEvicts.Add(1)
				return
			}
			if !armed.Load() || k != "s0/seed" {
				return
			}
			switch variant {
			case 0:
				go func() {
					defer childDone.Store(true)
					_, ok := c.Delete("s1/victim")
					assert.True(t, ok)
				}()
				sleepPollUntilDone(&childDone)
			case 1:
				var wg sync.WaitGroup
				wg.Go(func() {
					defer childDone.Store(true)
					_, ok := c.Delete("s1/victim")
					assert.True(t, ok)
				})
				sleepPollUntilDone(&childDone)
				wg.Wait()
			default:
				spawnHelperAsyncForSleepPoll(outerChild)
				sleepPollUntilDone(&childDone)
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 75)
	_, _ = c.Put("s0/seed", 1)
	_, _ = c.Put("s1/victim", 2)
	armed.Store(true)

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		if useReplace {
			assert.NoError(t, c.Replace("s0/seed", 10))
		} else {
			_, _ = c.Put("s0/trigger", 3)
		}
		_, _ = c.Put("s1/after", 4)
		_, ok := c.Delete("s1/after")
		assert.True(t, ok)
	}()

	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatalf("deadlocked in post-warmup sleep-polling OnEvict sharded=%v replace=%v variant=%d", sharded, useReplace, variant)
	}
	assert.Equal(t, int32(1), childEvicts.Load())
}

func TestShardedCache_PostWarmupSleepPollingCallbackChildNoDeadlock(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		t.Run("DirectGo_Replace/"+mode, func(t *testing.T) {
			runPostWarmupSleepPollChildCase(t, sharded, true, 0)
		})
		t.Run("DirectGo_Put/"+mode, func(t *testing.T) {
			runPostWarmupSleepPollChildCase(t, sharded, false, 0)
		})
		t.Run("InlineWG_Replace/"+mode, func(t *testing.T) {
			runPostWarmupSleepPollChildCase(t, sharded, true, 1)
		})
		t.Run("HelperOuterClosure_Replace/"+mode, func(t *testing.T) {
			runPostWarmupSleepPollChildCase(t, sharded, true, 2)
		})
	}
}

func TestShardedCache_PressureFuncWithoutOnEvictPrunesPreHookLiveGIDs(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		t.Run(mode, func(t *testing.T) {
			c := makeShardedOrUnshardedIntCache(sharded, 64,
				WithPressureFunc(func() float64 {
					return 0.10
				}),
			)
			for batch := range 250 {
				var wg sync.WaitGroup
				for j := range 10 {
					idx := batch*10 + j
					wg.Go(func() {
						_, _ = c.Put(fmt.Sprintf("s%d/k_%d", idx%2, idx%96), idx)
					})
				}
				wg.Wait()
			}
			p := coordPressureState(c)
			require.NotNil(t, p)
			p.preHookMu.Lock()
			boundedLen := len(p.preHookLiveGIDs)
			p.preHookMu.Unlock()
			assert.LessOrEqual(t, boundedLen, 530, "preHookLiveGIDs must stay bounded across 2,500 short-lived goroutines")

			pa := c.(PressureAwareCache[int])
			for range 520 {
				var wg sync.WaitGroup
				wg.Go(func() {
					_ = pa.EvaluateMemoryPressure()
				})
				wg.Wait()
				p.preHookMu.Lock()
				curLen := len(p.preHookLiveGIDs)
				p.preHookMu.Unlock()
				if curLen < 64 {
					break
				}
			}
			p.preHookMu.Lock()
			prunedLen := len(p.preHookLiveGIDs)
			p.preHookMu.Unlock()
			assert.Less(t, prunedLen, 64, "preHookLiveGIDs must prune exited goroutines down to live set once crossing 512")
		})
	}
}

type c3OrphanSyncChans struct {
	startG3      chan struct{}
	g1Entered    chan struct{}
	g2Entered    chan struct{}
	releaseG2    chan struct{}
	orphanAct    chan struct{}
	orphanDone   chan struct{}
	g3Entered    chan struct{}
	releaseG3    chan struct{}
	mExited      chan struct{}
	allowOrphans chan struct{}
}

func newC3OrphanSyncChans() c3OrphanSyncChans {
	return c3OrphanSyncChans{
		startG3:      make(chan struct{}),
		g1Entered:    make(chan struct{}),
		g2Entered:    make(chan struct{}),
		releaseG2:    make(chan struct{}),
		orphanAct:    make(chan struct{}),
		orphanDone:   make(chan struct{}),
		g3Entered:    make(chan struct{}),
		releaseG3:    make(chan struct{}),
		mExited:      make(chan struct{}),
		allowOrphans: make(chan struct{}),
	}
}

func invokePutViaSharedFrame(c Cache[int], key string, val int) {
	_, _ = c.Put(key, val)
}

func spawnC3OrphanAndOverflow(c Cache[int], ch c3OrphanSyncChans, extrasWG *sync.WaitGroup) {
	go func() {
		defer close(ch.mExited)
		go func() {
			<-ch.orphanAct
			orphanExited := make(chan struct{})
			go func() {
				defer close(ch.orphanDone)
				<-orphanExited
				c.(PressureAwareCache[int]).Compact()
				_, _ = c.Put("s0/orphan_probe", 5)
			}()
			close(orphanExited)
		}()
		for range 140 {
			extrasWG.Go(func() {
				<-ch.allowOrphans
			})
		}
	}()
	<-ch.mExited
}

func TestShardedCache_PanickedSamplerDeadParentOrphanWithConcurrentSamplerAndOverflow(t *testing.T) {
	ch := newC3OrphanSyncChans()
	var extrasWG sync.WaitGroup
	var phase atomic.Int32
	var c Cache[int]

	g3Done := make(chan struct{})
	go func() {
		defer close(g3Done)
		<-ch.startG3
		invokePutViaSharedFrame(c, "s1/g3_write", 99)
	}()

	c = NewShardedCache[int](64, 2, explicitTagShardFunc,
		WithBackend(BackendMap),
		WithEvictionRetentionRatio(0.0),
		WithPressureFunc(func() float64 {
			switch phase.Load() {
			case 1:
				phase.Store(2)
				close(ch.g1Entered)
				<-ch.g2Entered
				spawnC3OrphanAndOverflow(c, ch, &extrasWG)
				panic("g1 sampler boom")
			case 2:
				phase.Store(3)
				close(ch.g2Entered)
				<-ch.releaseG2
				return 0.10
			case 4:
				phase.Store(5)
				close(ch.g3Entered)
				<-ch.releaseG3
				return 0.95
			default:
				return 0.10
			}
		}),
	)

	createCompactionSlackOnS0(c)
	_, _ = c.Put("s1/live", 777)

	phase.Store(1)
	g2Done := make(chan struct{})
	g1Done := make(chan struct{})
	go func() {
		defer close(g1Done)
		assert.PanicsWithValue(t, "g1 sampler boom", func() {
			invokePutViaSharedFrame(c, "s0/g1", 1)
		})
	}()
	<-ch.g1Entered
	go func() {
		defer close(g2Done)
		_, _ = c.Put("s1/g2", 2)
	}()
	<-g1Done
	close(ch.releaseG2)
	<-g2Done

	phase.Store(4)
	close(ch.startG3)
	<-ch.g3Entered

	close(ch.orphanAct)
	<-ch.orphanDone
	close(ch.releaseG3)
	<-g3Done
	close(ch.allowOrphans)
	extrasWG.Wait()

	_, ok := c.Peek("s1/live")
	assert.True(t, ok, "orphaned grandchild of panicked sampler G1 must not be misattributed to later sampler G3")
	assert.InDelta(t, 0.10, c.Stats().MemoryPressure, 1e-9)
}

type r11NamedBgWorker struct {
	c          Cache[int]
	inFg       chan struct{}
	done       chan struct{}
	fgInEvict  *atomic.Bool
	bgEvictRan *atomic.Bool
	duringFg   *atomic.Bool
	sawSync    *atomic.Bool
}

func (w *r11NamedBgWorker) run() {
	defer close(w.done)
	<-w.inFg
	_, ok := w.c.Delete("s1/bg")
	if ok {
		w.duringFg.Store(w.fgInEvict.Load())
		w.sawSync.Store(w.bgEvictRan.Load())
	}
}

func spawnR11NamedWorkerViaHelper(w *r11NamedBgWorker) {
	go w.run()
}

func runR11NamedMethodPreExistingCase(t *testing.T, sharded bool) {
	t.Helper()
	var c Cache[int]
	var armed, fgInEvict, bgEvictRan, duringFg, sawSync atomic.Bool
	inFg := make(chan struct{})

	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s1/bg" && r == EvictionReasonDeleted {
				bgEvictRan.Store(true)
				return
			}
			if armed.Load() && k == "s0/fg" && r == EvictionReasonReplaced {
				fgInEvict.Store(true)
				defer fgInEvict.Store(false)
				close(inFg)
				assert.True(t, waitUntilEvictSlowWaiterParked(c))
			}
		}),
	)
	warmupUncontendedReplaceEvictions(c, 75)
	_, _ = c.Put("s0/fg", 1)
	_, _ = c.Put("s1/bg", 2)
	armed.Store(true)

	w := &r11NamedBgWorker{
		c:          c,
		inFg:       inFg,
		done:       make(chan struct{}),
		fgInEvict:  &fgInEvict,
		bgEvictRan: &bgEvictRan,
		duringFg:   &duringFg,
		sawSync:    &sawSync,
	}
	spawnR11NamedWorkerViaHelper(w)
	require.NoError(t, c.Replace("s0/fg", 10))
	<-w.done

	assert.False(t, duringFg.Load(), "helper-spawned method goroutine must block until foreground OnEvict finishes")
	assert.True(t, sawSync.Load(), "helper-spawned method goroutine must run its own OnEvict synchronously")
}

func runR11PreExistingPanicIsolationCase(t *testing.T, sharded, fgPanics bool) {
	t.Helper()
	var c Cache[int]
	var armed atomic.Bool
	inFg := make(chan struct{})

	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s1/bg" && r == EvictionReasonDeleted {
				panic("bg boom")
			}
			if armed.Load() && k == "s0/fg" && r == EvictionReasonReplaced {
				close(inFg)
				if fgPanics {
					assert.True(t, waitUntilEvictSlowWaiterParked(c))
					panic("fg boom")
				}
				var innerWG sync.WaitGroup
				innerWG.Go(func() {
					assert.True(t, waitUntilEvictSlowWaiterParked(c))
				})
				innerWG.Wait()
			}
		}),
	)
	warmupUncontendedReplaceEvictions(c, 75)
	_, _ = c.Put("s0/fg", 1)
	_, _ = c.Put("s1/bg", 2)
	armed.Store(true)

	var bgRecovered atomic.Value
	var outerWG sync.WaitGroup
	outerWG.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				bgRecovered.Store(r)
			}
		}()
		<-inFg
		_, _ = c.Delete("s1/bg")
	})

	if fgPanics {
		assert.PanicsWithValue(t, "fg boom", func() {
			_ = c.Replace("s0/fg", 10)
		})
	} else {
		require.NoError(t, c.Replace("s0/fg", 10))
	}
	outerWG.Wait()
	assert.Equal(t, "bg boom", bgRecovered.Load(), "background OnEvict panic must stay on background goroutine and never be stolen or swallowed")
}

func runR11PriorHoldDetachedIsolationCase(t *testing.T, sharded bool) {
	t.Helper()
	var c Cache[int]
	var phase atomic.Int32
	var fgInHold2, bgEvictRan, duringHold2, sawSync atomic.Bool
	startDetached := make(chan struct{})
	inHold2 := make(chan struct{})
	detachedDone := make(chan struct{})

	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s1/bg" && r == EvictionReasonDeleted {
				bgEvictRan.Store(true)
				return
			}
			if k != "s0/fg" || r != EvictionReasonReplaced {
				return
			}
			switch phase.Load() {
			case 1:
				go func() {
					defer close(detachedDone)
					<-startDetached
					<-inHold2
					_, ok := c.Delete("s1/bg")
					if ok {
						duringHold2.Store(fgInHold2.Load())
						sawSync.Store(bgEvictRan.Load())
					}
				}()
			case 2:
				fgInHold2.Store(true)
				defer fgInHold2.Store(false)
				close(inHold2)
				waitUntilEvictContendedFor(c, 25*time.Millisecond)
			}
		}),
	)
	warmupUncontendedReplaceEvictions(c, 75)
	_, _ = c.Put("s0/fg", 1)
	_, _ = c.Put("s1/bg", 2)

	phase.Store(1)
	require.NoError(t, c.Replace("s0/fg", 10))
	close(startDetached)

	phase.Store(2)
	require.NoError(t, c.Replace("s0/fg", 20))
	<-detachedDone

	assert.False(t, duringHold2.Load(), "detached goroutine from an earlier OnEvict hold must not be treated as child of a later hold")
	assert.True(t, sawSync.Load())
}

func TestShardedCache_PostWarmupPreExistingGoroutinesSyncAndPanicIsolation(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		t.Run("HelperMethodChannelWait/"+mode, func(t *testing.T) {
			runR11NamedMethodPreExistingCase(t, sharded)
		})
		t.Run("BgPanicNotStolen/"+mode, func(t *testing.T) {
			runR11PreExistingPanicIsolationCase(t, sharded, false)
		})
		t.Run("DualPanicNeverSwallowed/"+mode, func(t *testing.T) {
			runR11PreExistingPanicIsolationCase(t, sharded, true)
		})
		t.Run("PriorHoldDetachedIsolated/"+mode, func(t *testing.T) {
			runR11PriorHoldDetachedIsolationCase(t, sharded)
		})
	}
}

func makeShardedOrUnshardedPtrCache(sharded bool, maxSize uint64, opts ...Option) (Cache[*[4]int], *pressureState) {
	if sharded {
		sc := NewShardedCache[*[4]int](maxSize, 2, explicitTagShardFunc, opts...).(*shardedCache[*[4]int])
		return sc, &sc.coord
	}
	c := New[*[4]int](maxSize, opts...)
	return c, extractPressureStateForIter2Test(c)
}

//go:noinline
func seedR11WeakVictim(t *testing.T, c Cache[*[4]int]) weak.Pointer[[4]int] {
	t.Helper()
	v := new([4]int)
	wp := weak.Make(v)
	_, err := c.Put("s1/victim", v)
	require.NoError(t, err)
	return wp
}

func runR11ChildCallbackPanicUnwindCase(t *testing.T, sharded, primaryPanics bool) {
	t.Helper()
	var c Cache[*[4]int]
	var armed, childRan atomic.Bool

	outerChildDelete := func() {
		_, _ = c.Delete("s1/victim")
	}

	c, p := makeShardedOrUnshardedPtrCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ *[4]int, r EvictionReason) {
			if k == "s1/victim" && r == EvictionReasonDeleted {
				childRan.Store(true)
				panic("child boom")
			}
			if armed.Load() && k == "s0/fg" && r == EvictionReasonReplaced {
				var wg sync.WaitGroup
				wg.Go(outerChildDelete)
				wg.Wait()
				if primaryPanics {
					panic("primary boom")
				}
			}
		}),
	)

	dummy := new([4]int)
	_, err := c.Put("s0/warm", dummy)
	require.NoError(t, err)
	for range 75 {
		require.NoError(t, c.Replace("s0/warm", dummy))
	}
	_, err = c.Put("s0/fg", dummy)
	require.NoError(t, err)
	wpVictim := seedR11WeakVictim(t, c)
	armed.Store(true)

	expectedPanic := "child boom"
	if primaryPanics {
		expectedPanic = "primary boom"
	}
	assert.PanicsWithValue(t, expectedPanic, func() {
		_ = c.Replace("s0/fg", dummy)
	})
	assert.Zero(t, p.evictPendingEnqueues.Load())

	if primaryPanics {
		assert.False(t, childRan.Load(), "pending child callback must not execute during primary callback panic unwind")
		for range 6 {
			runtime.GC()
			runtime.Gosched()
		}
		assert.Nil(t, wpVictim.Value(), "discarded pending child callback must release captured value references")
	} else {
		assert.True(t, childRan.Load(), "pending child callback must execute and propagate its panic when primary succeeds")
	}
}

func TestShardedCache_PostWarmupChildCallbackPanicUnwindAndPropagation(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		t.Run("PrimaryPanicDiscardsChild/"+mode, func(t *testing.T) {
			runR11ChildCallbackPanicUnwindCase(t, sharded, true)
		})
		t.Run("ChildPanicPropagates/"+mode, func(t *testing.T) {
			runR11ChildCallbackPanicUnwindCase(t, sharded, false)
		})
	}
}

//go:noinline
func enqueueR11WeakPendingEvent(p *pressureState, delivered *atomic.Int32) weak.Pointer[[8]int] {
	v := new([8]int)
	wp := weak.Make(v)
	var q evictCallbackQueue[*[8]int]
	q.enqueue("s0/pending", v, EvictionReasonDeleted)
	q.enqueuePendingCallback(p, nil, func(_ string, val *[8]int, _ EvictionReason) {
		if val != nil {
			delivered.Add(1)
		}
	})
	return wp
}

func populateOver512PreHookGIDs(c Cache[int]) {
	for batch := range 55 {
		var wg sync.WaitGroup
		for j := range 10 {
			idx := batch*10 + j
			wg.Go(func() {
				_, _ = c.Put(fmt.Sprintf("s%d/p_%d", idx%2, idx%32), idx)
			})
		}
		wg.Wait()
	}
}

func TestShardedCache_PreHookPruneDoesNotStrandPendingEvictOrLatchContention(t *testing.T) {
	t.Run("NoStrandedPendingCallbacks", func(t *testing.T) {
		c := NewShardedCache[int](64, 2, explicitTagShardFunc,
			WithPressureFunc(func() float64 { return 0.10 }),
			WithOnEvictEntry(func(string, int, EvictionReason) {}),
		)
		populateOver512PreHookGIDs(c)
		p := coordPressureState(c)
		pa := c.(PressureAwareCache[int])

		var stop atomic.Bool
		var prunerWG sync.WaitGroup
		prunerWG.Go(func() {
			for !stop.Load() {
				_ = p.isEvictIdleForPrune(false)
				_ = pa.EvaluateMemoryPressure()
			}
		})

		const total = 80
		var delivered atomic.Int32
		wps := make([]weak.Pointer[[8]int], total)
		for i := range total {
			wps[i] = enqueueR11WeakPendingEvent(p, &delivered)
		}
		stop.Store(true)
		prunerWG.Wait()

		assert.Equal(t, int32(total), delivered.Load(), "concurrent preHook pruning must never strand queued OnEvict callbacks")
		assert.Zero(t, p.evictPendingEnqueues.Load())
		for range 6 {
			runtime.GC()
			runtime.Gosched()
		}
		for i := range wps {
			assert.Nil(t, wps[i].Value(), "queued callback value %d must be collectible after delivery", i)
		}
	})

	t.Run("NoSpuriousContentionLatch", func(t *testing.T) {
		c := NewShardedCache[int](64, 2, explicitTagShardFunc,
			WithPressureFunc(func() float64 { return 0.10 }),
			WithOnEvictEntry(func(string, int, EvictionReason) {}),
		)
		p := coordPressureState(c)
		_, _ = c.Put("s0/hot", 1)

		var stop atomic.Bool
		var wg sync.WaitGroup
		wg.Go(func() {
			for !stop.Load() {
				_ = p.isEvictIdleForPrune(false)
				runtime.Gosched()
			}
		})
		for i := range 200 {
			require.NoError(t, c.Replace("s0/hot", i+2))
		}
		stop.Store(true)
		wg.Wait()
		assert.False(t, p.evictCallbackEverContended.Load(), "isEvictIdleForPrune must not touch evictCallbackMu or latch evictCallbackEverContended")
	})
}

func spawnR11HelperChild(fn func()) {
	go fn()
}

type r11ReenteredChannelService struct {
	c       Cache[int]
	armed   atomic.Bool
	evicts  atomic.Int32
	useRepl bool
}

func (s *r11ReenteredChannelService) handle(entry bool) {
	if entry {
		if s.useRepl {
			_ = s.c.Replace("s0/seed", 99)
		} else {
			_, _ = s.c.Put("s0/trigger", 99)
		}
		return
	}
	if !s.armed.Load() {
		return
	}
	resCh := make(chan bool, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		_, ok := s.c.Delete("s1/victim")
		resCh <- ok
	})
	<-resCh
	wg.Wait()
}

func runR11ChannelWaitChildSubcase(t *testing.T, sharded, useReplace bool, subcase int) {
	t.Helper()
	if subcase == 2 {
		svc := &r11ReenteredChannelService{useRepl: useReplace}
		svc.c = makeShardedOrUnshardedIntCache(sharded, 2,
			WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
				if k == "s1/victim" && r == EvictionReasonDeleted {
					svc.evicts.Add(1)
					return
				}
				if k == "s0/seed" {
					svc.handle(false)
				}
			}),
		)
		warmupUncontendedReplaceEvictions(svc.c, 75)
		_, _ = svc.c.Put("s0/seed", 1)
		_, _ = svc.c.Put("s1/victim", 2)
		svc.armed.Store(true)
		done := make(chan struct{})
		go func() {
			defer close(done)
			svc.handle(true)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("deadlocked in re-entered method channel-wait OnEvict sharded=%v replace=%v", sharded, useReplace)
		}
		assert.Equal(t, int32(1), svc.evicts.Load())
		return
	}

	var childEvicts atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		var c Cache[int]
		var armed atomic.Bool
		resCh := make(chan bool, 1)
		outerChildFn := func() {
			_, ok := c.Delete("s1/victim")
			resCh <- ok
		}

		c = makeShardedOrUnshardedIntCache(sharded, 2,
			WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
				if k == "s1/victim" && r == EvictionReasonDeleted {
					childEvicts.Add(1)
					return
				}
				if !armed.Load() || k != "s0/seed" {
					return
				}
				if subcase == 0 {
					var wg sync.WaitGroup
					wg.Go(outerChildFn)
					<-resCh
					wg.Wait()
					return
				}
				spawnR11HelperChild(outerChildFn)
				<-resCh
			}),
		)
		warmupUncontendedReplaceEvictions(c, 75)
		_, _ = c.Put("s0/seed", 1)
		_, _ = c.Put("s1/victim", 2)
		armed.Store(true)
		if useReplace {
			_ = c.Replace("s0/seed", 10)
		} else {
			_, _ = c.Put("s0/trigger", 3)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("deadlocked in channel-wait OnEvict sharded=%v replace=%v subcase=%d", sharded, useReplace, subcase)
	}
	assert.Equal(t, int32(1), childEvicts.Load())
}

func runR11PprofLabelAndRotationCase(t *testing.T, sharded bool) {
	t.Helper()
	var c Cache[int]
	var armed, panicMode atomic.Bool
	var hookLabelPtr, childLabelPtr atomic.Uintptr
	inFg := make(chan struct{})
	bgDone := make(chan struct{})
	var fgInEvict, bgDuringFg atomic.Bool

	resCh := make(chan bool, 1)
	outerChild := func() {
		childLabelPtr.Store(uintptr(runtimeGetProfLabel()))
		_, ok := c.Delete("s1/child")
		resCh <- ok
	}

	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if !armed.Load() || k != "s0/fg" || r != EvictionReasonReplaced {
				return
			}
			hookLabelPtr.Store(uintptr(runtimeGetProfLabel()))
			if panicMode.Load() {
				panic("pprof panic")
			}
			fgInEvict.Store(true)
			defer fgInEvict.Store(false)
			close(inFg)
			waitUntilEvictContendedFor(c, 20*time.Millisecond)
			var wg sync.WaitGroup
			wg.Go(outerChild)
			<-resCh
			wg.Wait()
		}),
	)
	warmupUncontendedReplaceEvictions(c, 75)
	_, _ = c.Put("s0/fg", 1)
	_, _ = c.Put("s1/bg", 2)
	_, _ = c.Put("s1/child", 3)
	armed.Store(true)

	var expectedPtr uintptr
	pprof.Do(context.Background(), pprof.Labels("req_id", "abc-123"), func(ctx context.Context) {
		val, ok := pprof.Label(ctx, "req_id")
		assert.True(t, ok)
		assert.Equal(t, "abc-123", val)
		expectedPtr = uintptr(runtimeGetProfLabel())
		assert.NotZero(t, expectedPtr)
		go func() {
			defer close(bgDone)
			<-inFg
			_, _ = c.Delete("s1/bg")
			bgDuringFg.Store(fgInEvict.Load())
		}()
		require.NoError(t, c.Replace("s0/fg", 10))
		assert.Equal(t, expectedPtr, uintptr(runtimeGetProfLabel()))
		panicMode.Store(true)
		assert.PanicsWithValue(t, "pprof panic", func() {
			_ = c.Replace("s0/fg", 20)
		})
		assert.Equal(t, expectedPtr, uintptr(runtimeGetProfLabel()))
	})
	<-bgDone
	assert.Zero(t, uintptr(runtimeGetProfLabel()))
	assert.Equal(t, expectedPtr, hookLabelPtr.Load())
	assert.Equal(t, expectedPtr, childLabelPtr.Load())
	assert.False(t, bgDuringFg.Load())

	// Also verify >1024 evictions across the 1024-slot label-ring rotation with a live detached goroutine.
	releaseDetached := make(chan struct{})
	var detachedWG sync.WaitGroup
	var rotCache Cache[int]
	var spawnOnce atomic.Bool
	rotCache = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s0/rot" && r == EvictionReasonReplaced && spawnOnce.CompareAndSwap(false, true) {
				detachedWG.Go(func() {
					<-releaseDetached
				})
			}
		}),
	)
	_, _ = rotCache.Put("s0/rot", 1)
	for i := range 1060 {
		if i == 80 {
			spawnOnce.Store(false)
		}
		require.NoError(t, rotCache.Replace("s0/rot", i+2))
	}
	close(releaseDetached)
	detachedWG.Wait()
}

func runR11NestedCachesPostWarmupCase(t *testing.T, sharded bool) {
	t.Helper()
	var cacheA, cacheB Cache[int]
	var armed atomic.Bool
	var childDeletes atomic.Int32

	cacheB = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if !armed.Load() || k != "s0/b" || r != EvictionReasonReplaced {
				return
			}
			ch := make(chan struct{})
			go func() {
				defer close(ch)
				if _, ok := cacheA.Delete("s1/a_victim"); ok {
					childDeletes.Add(1)
				}
				if _, ok := cacheB.Delete("s1/b_victim"); ok {
					childDeletes.Add(1)
				}
			}()
			<-ch
		}),
	)
	cacheA = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if armed.Load() && k == "s0/a" && r == EvictionReasonReplaced {
				_ = cacheB.Replace("s0/b", 20)
			}
		}),
	)
	warmupUncontendedReplaceEvictions(cacheA, 75)
	warmupUncontendedReplaceEvictions(cacheB, 75)
	_, _ = cacheA.Put("s0/a", 1)
	_, _ = cacheA.Put("s1/a_victim", 2)
	_, _ = cacheB.Put("s0/b", 3)
	_, _ = cacheB.Put("s1/b_victim", 4)
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cacheA.Replace("s0/a", 10)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("deadlocked in nested cacheA->cacheB post-warmup OnEvict sharded=%v", sharded)
	}
	assert.Equal(t, int32(2), childDeletes.Load())
}

func TestShardedCache_PostWarmupChannelWaitChildNoDeadlockAndPprofLabels(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, useReplace := range []bool{true, false} {
			op := "Replace"
			if !useReplace {
				op = "Put"
			}
			t.Run("WGGoOuter_"+op+"/"+mode, func(t *testing.T) {
				runR11ChannelWaitChildSubcase(t, sharded, useReplace, 0)
			})
			t.Run("HelperOuter_"+op+"/"+mode, func(t *testing.T) {
				runR11ChannelWaitChildSubcase(t, sharded, useReplace, 1)
			})
			t.Run("ReenteredMethod_"+op+"/"+mode, func(t *testing.T) {
				runR11ChannelWaitChildSubcase(t, sharded, useReplace, 2)
			})
		}
		t.Run("PprofLabelsAndRingRotation/"+mode, func(t *testing.T) {
			runR11PprofLabelAndRotationCase(t, sharded)
		})
		t.Run("NestedCaches/"+mode, func(t *testing.T) {
			runR11NestedCachesPostWarmupCase(t, sharded)
		})
	}
}

func execR12PprofChildBehavior(subcase int, c Cache[int], spawnInlineWG func(*sync.WaitGroup, func()), outerChildFn, outerChildPprofDo func(), resCh <-chan bool) {
	bg := context.Background()
	lbls := pprof.Labels("op", "evict")
	switch subcase {
	case 0:
		pprof.Do(bg, lbls, func(context.Context) {
			ch := make(chan bool, 1)
			go func() {
				_, ok := c.Delete("s1/victim")
				ch <- ok
			}()
			<-ch
		})
	case 1:
		pprof.Do(bg, lbls, func(context.Context) {
			var wg sync.WaitGroup
			wg.Go(outerChildFn)
			<-resCh
			wg.Wait()
		})
	case 2:
		var wg sync.WaitGroup
		pprof.Do(bg, lbls, func(context.Context) {
			spawnInlineWG(&wg, outerChildFn)
		})
		<-resCh
		wg.Wait()
	case 3:
		pprof.Do(bg, lbls, func(context.Context) {})
		ch := make(chan bool, 1)
		go func() {
			_, ok := c.Delete("s1/victim")
			ch <- ok
		}()
		<-ch
	default:
		execR12PprofChildBehaviorMore(subcase, spawnInlineWG, outerChildFn, outerChildPprofDo, resCh)
	}
}

func execR12PprofChildBehaviorMore(subcase int, spawnInlineWG func(*sync.WaitGroup, func()), outerChildFn, outerChildPprofDo func(), resCh <-chan bool) {
	bg := context.Background()
	lbls := pprof.Labels("op", "evict")
	switch subcase {
	case 4:
		pprof.Do(bg, lbls, func(context.Context) {})
		var wg sync.WaitGroup
		spawnInlineWG(&wg, outerChildFn)
		<-resCh
		wg.Wait()
	case 5:
		pprof.SetGoroutineLabels(pprof.WithLabels(bg, pprof.Labels("trace", "xyz")))
		var wg sync.WaitGroup
		wg.Go(outerChildFn)
		<-resCh
		wg.Wait()
	case 6:
		pprof.SetGoroutineLabels(bg)
		var wg sync.WaitGroup
		spawnInlineWG(&wg, outerChildFn)
		<-resCh
		wg.Wait()
	case 7:
		var wg sync.WaitGroup
		spawnInlineWG(&wg, outerChildPprofDo)
		<-resCh
		wg.Wait()
	}
}

func triggerR12PprofOuterOp(c Cache[int], useReplace, outerPprofScope bool, subcase int) {
	runOp := func() {
		if useReplace {
			_ = c.Replace("s0/seed", 10)
			return
		}
		_, _ = c.Put("s0/trigger", 3)
	}
	if subcase == 8 {
		_ = c.Replace("s0/seed", 9)
		runOp()
		return
	}
	if outerPprofScope {
		pprof.Do(context.Background(), pprof.Labels("outer", "req"), func(context.Context) {
			runOp()
		})
		return
	}
	runOp()
}

func runR12PprofDoChildSubcaseWithBackend(t *testing.T, sharded bool, backend Backend, useReplace, outerPprofScope bool, subcase int) {
	t.Helper()
	var childEvicts atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer pprof.SetGoroutineLabels(context.Background())

		var c Cache[int]
		var armed atomic.Bool
		var step atomic.Int32
		resCh := make(chan bool, 1)
		outerChildFn := func() {
			_, ok := c.Delete("s1/victim")
			resCh <- ok
		}
		outerChildPprofDo := func() {
			pprof.Do(context.Background(), pprof.Labels("worker", "1"), func(context.Context) {
				_, ok := c.Delete("s1/victim")
				resCh <- ok
			})
		}

		c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 2,
			WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
				if k == "s1/victim" && r == EvictionReasonDeleted {
					childEvicts.Add(1)
					return
				}
				if !armed.Load() {
					return
				}
				if k != "s0/seed" {
					return
				}
				spawnInlineWG := func(wg *sync.WaitGroup, fn func()) {
					wg.Go(func() { fn() })
				}
				if subcase == 8 {
					if step.Add(1) == 1 {
						pprof.Do(context.Background(), pprof.Labels("k", "v"), func(context.Context) {})
						return
					}
					var wg sync.WaitGroup
					wg.Go(outerChildFn)
					<-resCh
					wg.Wait()
					return
				}
				execR12PprofChildBehavior(subcase, c, spawnInlineWG, outerChildFn, outerChildPprofDo, resCh)
			}),
		)

		warmupUncontendedReplaceEvictions(c, 75)
		_, _ = c.Put("s0/seed", 0)
		_ = c.Replace("s0/seed", 1)
		assert.True(t, coordPressureState(c).evictFastMode.Load())
		_, _ = c.Put("s1/victim", 2)
		armed.Store(true)
		triggerR12PprofOuterOp(c, useReplace, outerPprofScope, subcase)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("deadlocked in pprof OnEvict sharded=%v backend=%s replace=%v outerScope=%v subcase=%d", sharded, backend, useReplace, outerPprofScope, subcase)
	}
	assert.Equal(t, int32(1), childEvicts.Load())
}

func runR12PprofDoChildSubcase(t *testing.T, sharded, useReplace bool, subcase int) {
	t.Helper()
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		runR12PprofDoChildSubcaseWithBackend(t, sharded, backend, useReplace, true, subcase)
		if subcase != 8 && backend == BackendArenaRadix {
			runR12PprofDoChildSubcaseWithBackend(t, sharded, backend, useReplace, false, subcase)
		}
	}
}

func verifyR12CallerGoroutineLabelInPprof(t *testing.T, wantSubstr string) {
	t.Helper()
	var buf strings.Builder
	prof := pprof.Lookup("goroutine")
	if assert.NotNil(t, prof) && assert.NoError(t, prof.WriteTo(&buf, 1)) {
		assert.Contains(t, buf.String(), wantSubstr)
	}
}

func runR12SetGoroutineLabelsPreservedCase(t *testing.T, sharded, shouldPanic bool) {
	t.Helper()
	var armed atomic.Bool
	var expectedPtr atomic.Uintptr
	wantVal := "norm-123"
	if shouldPanic {
		wantVal = "panic-456"
	}

	c := makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictValue(func(_ int, r EvictionReason) {
			if !armed.Load() || r != EvictionReasonReplaced {
				return
			}
			ctx := pprof.WithLabels(context.Background(), pprof.Labels("trace_id", wantVal))
			pprof.SetGoroutineLabels(ctx)
			expectedPtr.Store(uintptr(runtimeGetProfLabel()))
			if shouldPanic {
				panic("c2-label-panic")
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 75)
	_, _ = c.Put("s0/k", 0)
	_ = c.Replace("s0/k", 1)
	p := coordPressureState(c)
	require.True(t, p.evictFastMode.Load())
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer pprof.SetGoroutineLabels(context.Background())
		if shouldPanic {
			assert.PanicsWithValue(t, "c2-label-panic", func() {
				_ = c.Replace("s0/k", 2)
			})
		} else {
			assert.NoError(t, c.Replace("s0/k", 2))
		}
		gotPtr := uintptr(runtimeGetProfLabel())
		assert.NotZero(t, expectedPtr.Load())
		assert.Equal(t, expectedPtr.Load(), gotPtr, "user goroutine labels set via SetGoroutineLabels inside OnEvict must not be erased")
		verifyR12CallerGoroutineLabelInPprof(t, `"trace_id":"`+wantVal+`"`)
	}()
	<-done
	assert.True(t, p.evictLabelTampered.Load())
	assert.False(t, p.evictFastMode.Load())
}

func runR12BgIsolationWithPprofDoCase(t *testing.T, sharded bool) {
	t.Helper()
	var c Cache[int]
	var armed, fgInEvict, bgDuringFg atomic.Bool
	inFg := make(chan struct{})
	bgDone := make(chan struct{})

	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if !armed.Load() || k != "s0/fg" || r != EvictionReasonReplaced {
				return
			}
			fgInEvict.Store(true)
			defer fgInEvict.Store(false)
			pprof.Do(context.Background(), pprof.Labels("fg", "1"), func(context.Context) {
				close(inFg)
				assert.True(t, waitUntilEvictSlowWaiterParked(c))
			})
		}),
	)

	warmupUncontendedReplaceEvictions(c, 75)
	_, _ = c.Put("s0/fg", 1)
	_, _ = c.Put("s1/bg", 2)
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		go func() {
			defer close(bgDone)
			<-inFg
			_, _ = c.Delete("s1/bg")
			bgDuringFg.Store(fgInEvict.Load())
		}()
		assert.NoError(t, c.Replace("s0/fg", 10))
	}()
	<-done
	<-bgDone
	assert.False(t, bgDuringFg.Load(), "unrelated background sibling must serialize behind OnEvict even when OnEvict uses pprof.Do")

	// Also verify pre-existing outerWG.Go panic isolation when post-warmup OnEvict calls pprof.Do and panics
	// without relying on time.Sleep frames on the holder stack.
	for _, useHelper := range []bool{false, true} {
		var panicArmed atomic.Bool
		inPanicFg := make(chan struct{})
		bgEntering := make(chan struct{})
		bgPanicErr := make(chan any, 1)
		var outerWG sync.WaitGroup
		c2 := makeShardedOrUnshardedIntCache(sharded, 4,
			WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
				if !panicArmed.Load() {
					return
				}
				if k == "s0/fg" {
					pprof.Do(context.Background(), pprof.Labels("fg", "panic"), func(context.Context) {
						var blockMu sync.Mutex
						blockMu.Lock()
						close(inPanicFg)
						<-bgEntering
						time.AfterFunc(35*time.Millisecond, blockMu.Unlock)
						blockMu.Lock()
						defer blockMu.Unlock()
					})
					panic("fg-panic")
				}
				if k == "s1/bg" {
					panic("bg-panic")
				}
			}),
		)
		warmupUncontendedReplaceEvictions(c2, 75)
		_, _ = c2.Put("s0/fg", 0)
		_ = c2.Replace("s0/fg", 1)
		require.True(t, coordPressureState(c2).evictFastMode.Load())
		_, _ = c2.Put("s1/bg", 2)
		panicArmed.Store(true)

		bgFn := func() {
			defer func() { bgPanicErr <- recover() }()
			<-inPanicFg
			close(bgEntering)
			_, _ = c2.Delete("s1/bg")
		}
		if useHelper {
			outerWG.Add(1)
			spawnR11HelperChild(func() {
				defer outerWG.Done()
				bgFn()
			})
		} else {
			outerWG.Go(bgFn)
		}
		assert.PanicsWithValue(t, "fg-panic", func() {
			_ = c2.Replace("s0/fg", 10)
		})
		outerWG.Wait()
		assert.Equal(t, "bg-panic", <-bgPanicErr)
	}
}

func TestShardedCache_PostWarmupPprofDoAndSetLabels(t *testing.T) {
	subcaseNames := []string{
		"PprofDo_GoInline",
		"PprofDo_WGGoOuter_WaitInside",
		"PprofDo_SpawnInside_WaitAfterReturn",
		"PprofDo_ReturnThenGoInline",
		"PprofDo_ReturnThenWGGoOuter",
		"SetLabels_NonEmpty_WGGoOuter",
		"SetLabels_Background_WGGoOuter",
		"ChildCallsPprofDo",
		"PprofDoEvict76_ChildEvict77",
	}
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for idx, name := range subcaseNames {
			t.Run(name+"_Replace/"+mode, func(t *testing.T) {
				runR12PprofDoChildSubcase(t, sharded, true, idx)
			})
			if idx <= 4 {
				t.Run(name+"_Put/"+mode, func(t *testing.T) {
					runR12PprofDoChildSubcase(t, sharded, false, idx)
				})
			}
		}
		t.Run("SetLabelsPreserved_Return/"+mode, func(t *testing.T) {
			runR12SetGoroutineLabelsPreservedCase(t, sharded, false)
		})
		t.Run("SetLabelsPreserved_Panic/"+mode, func(t *testing.T) {
			runR12SetGoroutineLabelsPreservedCase(t, sharded, true)
		})
		t.Run("BgIsolationAndDualPanic/"+mode, func(t *testing.T) {
			runR12BgIsolationWithPprofDoCase(t, sharded)
		})
	}
}

type r13TaskRunner struct {
	c Cache[int]
}

//go:noinline
func (r *r13TaskRunner) exec(key string, val int, done chan<- struct{}) {
	defer close(done)
	_, _ = r.c.Put(key, val)
}

//go:noinline
func (r *r13TaskRunner) run(async bool, key string, val int) <-chan struct{} {
	if !async {
		_, _ = r.c.Put(key, val)
		return nil
	}
	done := make(chan struct{})
	go r.exec(key, val, done)
	return done
}

func runR13ChildClearsLabelsCase(t *testing.T, sharded, afterPprofDo, holderScopedChan bool) {
	t.Helper()
	var c Cache[int]
	var armed atomic.Bool
	var childEvicts atomic.Int32

	c = makeShardedOrUnshardedIntCache(sharded, 8,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if strings.HasPrefix(k, "s1/victim") {
				childEvicts.Add(1)
				return
			}
			if !armed.Load() || k != "s0/trigger" || r != EvictionReasonReplaced {
				return
			}
			if holderScopedChan {
				if afterPprofDo {
					pprof.Do(context.Background(), pprof.Labels("phase", "evict"), func(context.Context) {
						done := make(chan struct{})
						go func() {
							defer close(done)
							_, _ = c.Delete("s1/victim0")
							_, _ = c.Delete("s1/victim1")
							_, _ = c.Put("s1/victim2", 99)
						}()
						<-done
					})
					return
				}
				pprof.SetGoroutineLabels(context.Background())
				done := make(chan struct{})
				go func() {
					defer close(done)
					_, _ = c.Delete("s1/victim0")
					_, _ = c.Delete("s1/victim1")
					_, _ = c.Put("s1/victim2", 99)
				}()
				<-done
				return
			}
			var wg sync.WaitGroup
			wg.Go(func() {
				if afterPprofDo {
					pprof.Do(context.Background(), pprof.Labels("phase", "prep"), func(context.Context) {})
				} else {
					pprof.SetGoroutineLabels(context.Background())
				}
				_, _ = c.Delete("s1/victim0")
				_, _ = c.Delete("s1/victim1")
				_, _ = c.Put("s1/victim2", 99)
			})
			wg.Wait()
		}),
	)

	warmupUncontendedReplaceEvictions(c, 10)
	_, _ = c.Put("s0/trigger", 1)
	_, _ = c.Put("s1/victim0", 2)
	_, _ = c.Put("s1/victim1", 3)
	_, _ = c.Put("s1/victim2", 4)
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		pprof.Do(context.Background(), pprof.Labels("outer", "r13"), func(context.Context) {
			assert.NoError(t, c.Replace("s0/trigger", 20))
		})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock when OnEvict child clears or restores nil pprof labels")
	}
	assert.Equal(t, int32(3), childEvicts.Load())
}

func runR13SleepPollWGGoWithPprofCase(t *testing.T, sharded bool) {
	t.Helper()
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		var victimEvicts atomic.Int32
		done := make(chan struct{})
		go func() {
			defer close(done)
			var c Cache[int]
			var armed, childDone atomic.Bool
			outerChildFn := func() {
				_, _ = c.Delete("s1/victim")
				childDone.Store(true)
			}

			c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 4,
				WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
					if k == "s1/victim" && r == EvictionReasonDeleted {
						victimEvicts.Add(1)
						return
					}
					if !armed.Load() || k != "s0/seed" || r != EvictionReasonReplaced {
						return
					}
					pprof.Do(context.Background(), pprof.Labels("op", "evict"), func(context.Context) {
						var wg sync.WaitGroup
						wg.Go(outerChildFn)
						for !childDone.Load() {
							time.Sleep(time.Millisecond)
						}
						wg.Wait()
					})
				}),
			)

			warmupUncontendedReplaceEvictions(c, 10)
			_, _ = c.Put("s0/seed", 1)
			_, _ = c.Put("s1/victim", 2)
			armed.Store(true)
			pprof.Do(context.Background(), pprof.Labels("outer", "r13-sleep"), func(context.Context) {
				assert.NoError(t, c.Replace("s0/seed", 10))
			})
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("deadlock when OnEvict combines pprof.Do with wg.Go and time.Sleep polling (backend=%s)", backend)
		}
		assert.Equal(t, int32(1), victimEvicts.Load())
	}
}

func runR13ReenteredRunnerWithPprofCase(t *testing.T, sharded bool) {
	t.Helper()
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		runner := &r13TaskRunner{}
		var armed atomic.Bool
		var victimReplaced atomic.Int32

		runner.c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 4,
			WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
				if k == "s1/victim" && r == EvictionReasonReplaced {
					victimReplaced.Add(1)
					return
				}
				if !armed.Load() || k != "s0/trigger" || r != EvictionReasonReplaced {
					return
				}
				ctx := pprof.WithLabels(context.Background(), pprof.Labels("runner", "reenter"))
				pprof.SetGoroutineLabels(ctx)
				defer pprof.SetGoroutineLabels(context.Background())
				ch := runner.run(true, "s1/victim", 99)
				<-ch
			}),
		)

		warmupUncontendedReplaceEvictions(runner.c, 10)
		_, _ = runner.c.Put("s0/trigger", 1)
		_, _ = runner.c.Put("s1/victim", 2)
		armed.Store(true)

		done := make(chan struct{})
		go func() {
			defer close(done)
			pprof.Do(context.Background(), pprof.Labels("outer", "r13-runner"), func(context.Context) {
				_ = runner.run(false, "s0/trigger", 11)
			})
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("deadlock when OnEvict sets pprof labels and re-enters shared helper method (backend=%s)", backend)
		}
		assert.Equal(t, int32(1), victimReplaced.Load())
	}
}

func runR13PeerLabeledWithUntamperedHolderCase(t *testing.T, sharded, fgPanics bool) {
	t.Helper()
	var armed, fgActive, peerReturnedDuringFg, peerEvictRan atomic.Bool
	inFg := make(chan struct{})
	peerEntering := make(chan struct{})
	peerPanicCh := make(chan any, 1)

	var c Cache[int]
	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armed.Load() {
				return
			}
			if k == "s0/fg" {
				fgActive.Store(true)
				defer fgActive.Store(false)
				close(inFg)
				<-peerEntering
				assert.True(t, waitUntilEvictSlowWaiterParked(c))
				if fgPanics && !sharded {
					<-time.After(380 * time.Millisecond)
				}
				if fgPanics {
					panic("fg-boom")
				}
				return
			}
			if k == "s1/peer" {
				peerEvictRan.Store(true)
				if !fgPanics {
					panic("peer-boom")
				}
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 10)
	_, _ = c.Put("s0/fg", 1)
	_, _ = c.Put("s1/peer", 2)
	armed.Store(true)

	var outerWG sync.WaitGroup
	outerWG.Go(func() {
		defer func() { peerPanicCh <- recover() }()
		<-inFg
		pprof.Do(context.Background(), pprof.Labels("rpc", "peer"), func(context.Context) {
			close(peerEntering)
			_, _ = c.Delete("s1/peer")
			peerReturnedDuringFg.Store(fgActive.Load())
		})
	})

	if fgPanics {
		assert.PanicsWithValue(t, "fg-boom", func() {
			_ = c.Replace("s0/fg", 10)
		})
		outerWG.Wait()
		assert.Nil(t, <-peerPanicCh)
		assert.True(t, peerEvictRan.Load(), "peer OnEvict must not be dropped when foreground OnEvict panics")
		assert.False(t, peerReturnedDuringFg.Load())
		return
	}
	require.NoError(t, c.Replace("s0/fg", 10))
	outerWG.Wait()
	assert.Equal(t, "peer-boom", <-peerPanicCh)
	assert.True(t, peerEvictRan.Load())
}

func runR13PriorHoldDetachedPprofIsolationCase(t *testing.T, sharded bool) {
	t.Helper()
	var c Cache[int]
	var step atomic.Int32
	var detachedEvictRan atomic.Bool
	startDetached := make(chan struct{})
	detachedEntering := make(chan struct{})
	detachedDone := make(chan struct{})

	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			cur := step.Load()
			if cur == 1 && k == "s0/fg" {
				go func() {
					defer close(detachedDone)
					<-startDetached
					pprof.Do(context.Background(), pprof.Labels("bg", "detached"), func(context.Context) {
						close(detachedEntering)
						_, _ = c.Delete("s1/detached")
					})
				}()
				return
			}
			if cur == 2 && k == "s0/fg" {
				var blockMu sync.Mutex
				blockMu.Lock()
				close(startDetached)
				<-detachedEntering
				time.AfterFunc(35*time.Millisecond, blockMu.Unlock)
				blockMu.Lock()
				defer blockMu.Unlock()
				panic("hold2-panic")
			}
			if k == "s1/detached" {
				detachedEvictRan.Store(true)
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 10)
	_, _ = c.Put("s0/fg", 1)
	_, _ = c.Put("s1/detached", 2)

	step.Store(1)
	require.NoError(t, c.Replace("s0/fg", 2))

	step.Store(2)
	assert.PanicsWithValue(t, "hold2-panic", func() {
		_ = c.Replace("s0/fg", 3)
	})
	<-detachedDone
	assert.True(t, detachedEvictRan.Load(), "prior-hold detached goroutine callback must not be dropped by subsequent hold panic")
}

func runR13CrossCacheRingTagLeakCase(t *testing.T, sharded bool) {
	t.Helper()
	var cacheB Cache[int]
	var armedA, armedB, bVictimEvicted atomic.Bool
	inBHold := make(chan struct{})
	bgEntering := make(chan struct{})
	bgDone := make(chan struct{})

	cacheA := makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armedA.Load() || k != "s0/spawn" {
				return
			}
			go func() {
				defer close(bgDone)
				<-inBHold
				close(bgEntering)
				_, _ = cacheB.Delete("s1/victim")
			}()
		}),
	)
	cacheB = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armedB.Load() {
				return
			}
			if k == "s0/hold" {
				close(inBHold)
				<-bgEntering
				assert.True(t, waitUntilEvictSlowWaiterParked(cacheB))
				panic("cacheB-hold-panic")
			}
			if k == "s1/victim" {
				bVictimEvicted.Store(true)
			}
		}),
	)

	warmupUncontendedReplaceEvictions(cacheA, 10)
	warmupUncontendedReplaceEvictions(cacheB, 10)
	_, _ = cacheA.Put("s0/spawn", 1)
	_, _ = cacheB.Put("s0/hold", 1)
	_, _ = cacheB.Put("s1/victim", 2)

	armedA.Store(true)
	require.NoError(t, cacheA.Replace("s0/spawn", 2))

	armedB.Store(true)
	assert.PanicsWithValue(t, "cacheB-hold-panic", func() {
		_ = cacheB.Replace("s0/hold", 2)
	})
	<-bgDone
	assert.True(t, bVictimEvicted.Load(), "cross-cache ring tag must never misclassify background goroutine on cacheB")
}

func runR13EmptyUserLabelsPeerIsolationCase(t *testing.T, sharded bool) {
	t.Helper()
	var armed, peerEvicted atomic.Bool
	inFg := make(chan struct{})
	peerEntering := make(chan struct{})
	peerDone := make(chan struct{})

	var c Cache[int]
	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armed.Load() {
				return
			}
			if k == "s0/fg" {
				close(inFg)
				<-peerEntering
				assert.True(t, waitUntilEvictSlowWaiterParked(c))
				panic("fg-empty-label-panic")
			}
			if k == "s1/peer" {
				peerEvicted.Store(true)
			}
		}),
	)
	warmupUncontendedReplaceEvictions(c, 10)
	_, _ = c.Put("s0/fg", 1)
	_, _ = c.Put("s1/peer", 2)
	armed.Store(true)

	emptyCtx := pprof.WithLabels(context.Background(), pprof.Labels())
	go func() {
		defer close(peerDone)
		pprof.SetGoroutineLabels(emptyCtx)
		defer pprof.SetGoroutineLabels(context.Background())
		<-inFg
		close(peerEntering)
		_, _ = c.Delete("s1/peer")
	}()

	assert.PanicsWithValue(t, "fg-empty-label-panic", func() {
		_ = c.Replace("s0/fg", 10)
	})
	<-peerDone
	assert.True(t, peerEvicted.Load(), "empty user pprof.Labels() must not be confused with internal ring tags")
}

func runR13SlowDrainQueuedChildPprofDoCase(t *testing.T, sharded, useChanWait bool) {
	t.Helper()
	var c Cache[int]
	var armed atomic.Bool
	var childTargetEvicted atomic.Int32

	c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armed.Load() {
				return
			}
			switch k {
			case "s0/trigger":
				_, _ = c.Delete("s1/reenter")
			case "s1/reenter":
				work := func() {
					pprof.Do(context.Background(), pprof.Labels("req", "1"), func(context.Context) {
						_, _ = c.Delete("s0/child-target")
					})
				}
				if useChanWait {
					done := make(chan struct{})
					go func() {
						defer close(done)
						work()
					}()
					<-done
					return
				}
				var wg sync.WaitGroup
				wg.Go(work)
				wg.Wait()
			case "s0/child-target":
				childTargetEvicted.Add(1)
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 10)
	_, _ = c.Put("s0/trigger", 1)
	_, _ = c.Put("s1/reenter", 2)
	_, _ = c.Put("s0/child-target", 3)
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.NoError(t, c.Replace("s0/trigger", 99))
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock in drainPendingEvictCallbacksSlow when queued OnEvict child uses pprof.Do")
	}
	assert.Equal(t, int32(1), childTargetEvicted.Load())
}

func runR13HoldSeqRaceDropCheck(t *testing.T, sharded bool) {
	t.Helper()
	for range 30 {
		var c Cache[int]
		var childEvicted atomic.Int32
		childDone := make(chan struct{})
		g2Start := make(chan struct{})

		c = makeShardedOrUnshardedIntCache(sharded, 8,
			WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
				switch k {
				case "s0/g1":
					go func() {
						defer close(childDone)
						_, _ = c.Delete("s1/child")
					}()
					close(g2Start)
				case "s0/g2":
					panic("g2-unrelated-panic")
				case "s1/child":
					childEvicted.Add(1)
				}
			}),
		)
		_, _ = c.Put("s0/g1", 1)
		_, _ = c.Put("s0/g2", 2)
		_, _ = c.Put("s1/child", 3)

		var wg sync.WaitGroup
		wg.Go(func() {
			<-g2Start
			assert.PanicsWithValue(t, "g2-unrelated-panic", func() {
				_, _ = c.Delete("s0/g2")
			})
		})
		_, _ = c.Delete("s0/g1")
		<-childDone
		wg.Wait()
		require.Equal(t, int32(1), childEvicted.Load(), "detached child callback must never be dropped by unrelated G2 panic")
	}
}

func runR13HoldSeqRaceBatchPanicIsolationCheck(t *testing.T, sharded bool) {
	t.Helper()
	for range 25 {
		var c Cache[int]
		childDone := make(chan any, 1)
		g2Start := make(chan struct{})

		c = makeShardedOrUnshardedIntCache(sharded, 64,
			WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
				if k == "s0/g1" {
					go func() {
						defer func() { childDone <- recover() }()
						c.DeletePrefix("s1/batch/")
					}()
					close(g2Start)
					return
				}
				if strings.HasPrefix(k, "s1/batch/") {
					panic("detached-batch-panic")
				}
			}),
		)
		_, _ = c.Put("s0/g1", 1)
		_, _ = c.Put("s0/g2", 2)
		for i := range 32 {
			_, _ = c.Put(fmt.Sprintf("s1/batch/%02d", i), i)
		}

		var wg sync.WaitGroup
		wg.Go(func() {
			<-g2Start
			assert.NotPanics(t, func() {
				_, _ = c.Delete("s0/g2")
			})
		})
		var g1Panicked any
		func() {
			defer func() { g1Panicked = recover() }()
			_, _ = c.Delete("s0/g1")
		}()
		childPanicked := <-childDone
		wg.Wait()
		assert.True(t, g1Panicked == "detached-batch-panic" || childPanicked == "detached-batch-panic",
			"detached batch panic must stay on G1 or detached child and never hijack unrelated G2")
	}
}

func TestShardedCache_R13EvictFastModePprofAndHoldSeqCorrectness(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		t.Run("C1_HolderSetNil_ChanWait/"+mode, func(t *testing.T) {
			runR13ChildClearsLabelsCase(t, sharded, false, true)
		})
		t.Run("C1_SetLabelsNil_WGWait/"+mode, func(t *testing.T) {
			runR13ChildClearsLabelsCase(t, sharded, false, false)
		})
		t.Run("C1_HolderPprofDo_ChanWait/"+mode, func(t *testing.T) {
			runR13ChildClearsLabelsCase(t, sharded, true, true)
		})
		t.Run("C1_AfterPprofDo_WGWait/"+mode, func(t *testing.T) {
			runR13ChildClearsLabelsCase(t, sharded, true, false)
		})
		t.Run("C2_PprofDoSleepPollWGGo/"+mode, func(t *testing.T) {
			runR13SleepPollWGGoWithPprofCase(t, sharded)
		})
		t.Run("C3_PprofLabelsReenteredRunner/"+mode, func(t *testing.T) {
			runR13ReenteredRunnerWithPprofCase(t, sharded)
		})
		t.Run("C4_UntamperedHolderLabeledPeer_FgPanics/"+mode, func(t *testing.T) {
			runR13PeerLabeledWithUntamperedHolderCase(t, sharded, true)
		})
		t.Run("C4_UntamperedHolderLabeledPeer_PeerPanics/"+mode, func(t *testing.T) {
			runR13PeerLabeledWithUntamperedHolderCase(t, sharded, false)
		})
		t.Run("C4_PriorHoldDetachedPprofIsolation/"+mode, func(t *testing.T) {
			runR13PriorHoldDetachedPprofIsolationCase(t, sharded)
		})
		t.Run("C5_CrossCacheRingTagLeak/"+mode, func(t *testing.T) {
			runR13CrossCacheRingTagLeakCase(t, sharded)
		})
		t.Run("C5_EmptyUserLabelsPeerIsolation/"+mode, func(t *testing.T) {
			runR13EmptyUserLabelsPeerIsolationCase(t, sharded)
		})
		t.Run("C6_SlowDrainQueuedChildPprofDo_WG/"+mode, func(t *testing.T) {
			runR13SlowDrainQueuedChildPprofDoCase(t, sharded, false)
		})
		t.Run("C6_SlowDrainQueuedChildPprofDo_Chan/"+mode, func(t *testing.T) {
			runR13SlowDrainQueuedChildPprofDoCase(t, sharded, true)
		})
		t.Run("C7_HoldSeqRaceNoDroppedChild/"+mode, func(t *testing.T) {
			runR13HoldSeqRaceDropCheck(t, sharded)
		})
		t.Run("C7_HoldSeqRaceBatchPanicIsolation/"+mode, func(t *testing.T) {
			runR13HoldSeqRaceBatchPanicIsolationCheck(t, sharded)
		})
	}
}

func makeR14BackendIntCache(backend Backend, sharded bool, maxSize uint64, opts ...Option) Cache[int] {
	allOpts := make([]Option, 0, len(opts)+1)
	allOpts = append(allOpts, WithBackend(backend))
	allOpts = append(allOpts, opts...)
	if sharded {
		return NewShardedCache[int](maxSize, 2, explicitTagShardFunc, allOpts...)
	}
	return New[int](maxSize, allOpts...)
}

func waitInsideR14SyncPrimitive(useCond bool) {
	if !useCond {
		var wg sync.WaitGroup
		wg.Add(1)
		time.AfterFunc(8*time.Millisecond, wg.Done)
		wg.Wait()
		return
	}
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	ready := false
	mu.Lock()
	time.AfterFunc(8*time.Millisecond, func() {
		mu.Lock()
		ready = true
		cond.Broadcast()
		mu.Unlock()
	})
	for !ready {
		cond.Wait()
	}
	mu.Unlock()
}

func runR14AfterFuncWaitGroupAndCondPanicCase(t *testing.T, sharded, useCond bool, labelKind int) {
	t.Helper()
	var c Cache[int]
	var armed, fgActive, peerDuringFg, peerEvicted atomic.Bool
	inFg := make(chan struct{})
	peerDone := make(chan struct{})

	c = makeShardedOrUnshardedIntCache(sharded, 8,
		WithPressureFunc(func() float64 {
			if armed.Load() {
				return 0.99
			}
			return 0.10
		}),
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armed.Load() {
				return
			}
			if k == "s0/fg" {
				fgActive.Store(true)
				defer fgActive.Store(false)
				close(inFg)
				waitInsideR14SyncPrimitive(useCond)
				panic("r14-afterfunc-fg-panic")
			}
			if strings.HasPrefix(k, "s1/peer") {
				peerEvicted.Store(true)
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 12)
	_, _ = c.Put("s0/fg", 0)
	_ = c.Replace("s0/fg", 1)
	_, _ = c.Put("s1/peer", 2)
	_, _ = c.Put("s1/peer2", 3)
	require.True(t, coordPressureState(c).evictFastMode.Load())
	armed.Store(true)

	var outerWG sync.WaitGroup
	outerWG.Go(func() {
		defer close(peerDone)
		<-inFg
		runOp := func() {
			switch labelKind {
			case 0:
				_, _ = c.Delete("s1/peer")
			case 1:
				_ = c.Replace("s1/peer", 22)
			default:
				c.(PressureAwareCache[int]).EvaluateMemoryPressure()
				c.DeletePrefix("s1/peer")
			}
			peerDuringFg.Store(fgActive.Load())
		}
		if labelKind == 1 {
			pprof.Do(context.Background(), pprof.Labels("peer", "r14"), func(context.Context) { runOp() })
			return
		}
		if labelKind == 2 {
			pprof.SetGoroutineLabels(pprof.WithLabels(context.Background(), pprof.Labels()))
			defer pprof.SetGoroutineLabels(context.Background())
		}
		runOp()
	})

	assert.PanicsWithValue(t, "r14-afterfunc-fg-panic", func() {
		_ = c.Replace("s0/fg", 10)
	})
	<-peerDone
	outerWG.Wait()
	assert.False(t, peerDuringFg.Load(), "peer operation must serialize behind AfterFunc+WaitGroup/Cond OnEvict")
	assert.True(t, peerEvicted.Load(), "peer OnEvict callback must never be dropped when foreground OnEvict panics")
}

func runR14InnerChildContendsWhilePanickingPeerSerializesCase(t *testing.T, sharded, childModifiesPprof bool) {
	t.Helper()
	var c Cache[int]
	var armed, childEvicted atomic.Bool
	inFg := make(chan struct{})
	peerReady := make(chan struct{})
	peerPanicCh := make(chan any, 1)

	c = makeShardedOrUnshardedIntCache(sharded, 8,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armed.Load() {
				return
			}
			switch k {
			case "s0/fg":
				close(inFg)
				<-peerReady
				assert.True(t, waitUntilEvictSlowWaiterParked(c))
				var innerWG sync.WaitGroup
				innerWG.Go(func() {
					if childModifiesPprof {
						pprof.Do(context.Background(), pprof.Labels("inner", "child"), func(context.Context) {
							_, _ = c.Delete("s1/child")
						})
						return
					}
					_, _ = c.Delete("s1/child")
				})
				innerWG.Wait()
			case "s1/child":
				childEvicted.Store(true)
			case "s1/peer":
				panic("peer-only-panic")
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 12)
	_, _ = c.Put("s0/fg", 0)
	_ = c.Replace("s0/fg", 1)
	_, _ = c.Put("s1/child", 2)
	_, _ = c.Put("s1/peer", 3)
	require.True(t, coordPressureState(c).evictFastMode.Load())
	armed.Store(true)

	var outerWG sync.WaitGroup
	outerWG.Go(func() {
		defer func() { peerPanicCh <- recover() }()
		<-inFg
		pprof.Do(context.Background(), pprof.Labels("peer", "contend"), func(context.Context) {
			close(peerReady)
			_, _ = c.Delete("s1/peer")
		})
	})

	require.NotPanics(t, func() {
		assert.NoError(t, c.Replace("s0/fg", 10))
	})
	assert.True(t, childEvicted.Load(), "inner wg.Go child OnEvict must drain before foreground Replace returns")
	outerWG.Wait()
	assert.Equal(t, "peer-only-panic", <-peerPanicCh, "peer panic must propagate exclusively on peer goroutine")
}

func runR14LongHoldBackendPeerDeletePrefixCase(t *testing.T, backend Backend, sharded bool) {
	t.Helper()
	var c Cache[int]
	var armed, fgActive, peerDuringFg atomic.Bool
	var peerCount atomic.Int32
	inFg := make(chan struct{})

	c = makeR14BackendIntCache(backend, sharded, 8,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armed.Load() {
				return
			}
			if k == "s0/fg" {
				fgActive.Store(true)
				defer fgActive.Store(false)
				close(inFg)
				assert.True(t, waitUntilEvictSlowWaiterParked(c))
				<-time.After(400 * time.Millisecond)
				panic("r14-long-hold-panic")
			}
			if strings.HasPrefix(k, "s1/pfx/") {
				peerCount.Add(1)
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 12)
	_, _ = c.Put("s0/fg", 0)
	_ = c.Replace("s0/fg", 1)
	_, _ = c.Put("s1/pfx/a", 2)
	_, _ = c.Put("s1/pfx/b", 3)
	require.True(t, coordPressureState(c).evictFastMode.Load())
	armed.Store(true)

	var outerWG sync.WaitGroup
	outerWG.Go(func() {
		<-inFg
		pprof.Do(context.Background(), pprof.Labels("op", "delete-prefix"), func(context.Context) {
			c.DeletePrefix("s1/pfx/")
			peerDuringFg.Store(fgActive.Load())
		})
	})

	assert.PanicsWithValue(t, "r14-long-hold-panic", func() {
		_ = c.Replace("s0/fg", 10)
	})
	outerWG.Wait()
	assert.False(t, peerDuringFg.Load(), "peer DeletePrefix must block past >350ms OnEvict hold")
	assert.Equal(t, int32(2), peerCount.Load(), "peer DeletePrefix callbacks must not be dropped after >=400ms foreground panic")
}

func runR14ForeignExitedHopAndNestedDeliverCase(t *testing.T, sharded bool) {
	t.Helper()
	var cacheB Cache[int]
	var armedA, armedB, bPeerEvicted atomic.Bool
	inB := make(chan struct{})
	g1Exited := make(chan struct{})
	g2Done := make(chan struct{})

	cacheA := makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armedA.Load() || k != "s0/a" {
				return
			}
			go func() {
				go func() {
					defer close(g2Done)
					<-g1Exited
					<-inB
					_, _ = cacheB.Delete("s1/from-g2")
				}()
				close(g1Exited)
			}()
		}),
	)
	cacheB = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armedB.Load() {
				return
			}
			if k == "s0/b" {
				close(inB)
				assert.True(t, waitUntilEvictSlowWaiterParked(cacheB))
				panic("cacheB-exited-hop-panic")
			}
			if k == "s1/from-g2" {
				bPeerEvicted.Store(true)
			}
		}),
	)

	warmupUncontendedReplaceEvictions(cacheA, 10)
	warmupUncontendedReplaceEvictions(cacheB, 10)
	_, _ = cacheA.Put("s0/a", 1)
	_, _ = cacheB.Put("s0/b", 1)
	_, _ = cacheB.Put("s1/from-g2", 2)
	armedA.Store(true)
	require.NoError(t, cacheA.Replace("s0/a", 2))
	<-g1Exited

	armedB.Store(true)
	assert.PanicsWithValue(t, "cacheB-exited-hop-panic", func() {
		_ = cacheB.Replace("s0/b", 2)
	})
	<-g2Done
	assert.True(t, bPeerEvicted.Load(), "exited intermediate hop with foreign ring tag must not be misclassified as cacheB child")

	var innerCache Cache[int]
	var outerArmed, innerArmed, nestedPeerEvicted atomic.Bool
	inInner := make(chan struct{})
	nestedPeerDone := make(chan struct{})

	innerCache = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !innerArmed.Load() {
				return
			}
			if k == "s0/inner" {
				close(inInner)
				assert.True(t, waitUntilEvictSlowWaiterParked(innerCache))
				panic("inner-nested-panic")
			}
			if k == "s1/nested-peer" {
				nestedPeerEvicted.Store(true)
			}
		}),
	)
	outerCache := makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !outerArmed.Load() || k != "s0/outer" {
				return
			}
			pprof.SetGoroutineLabels(context.Background())
			go func() {
				defer close(nestedPeerDone)
				<-inInner
				pprof.Do(context.Background(), pprof.Labels("nested", "peer"), func(context.Context) {
					_, _ = innerCache.Delete("s1/nested-peer")
				})
			}()
			_ = innerCache.Replace("s0/inner", 2)
		}),
	)

	warmupUncontendedReplaceEvictions(outerCache, 10)
	warmupUncontendedReplaceEvictions(innerCache, 10)
	_, _ = outerCache.Put("s0/outer", 0)
	_ = outerCache.Replace("s0/outer", 1)
	_, _ = innerCache.Put("s0/inner", 0)
	_ = innerCache.Replace("s0/inner", 1)
	_, _ = innerCache.Put("s1/nested-peer", 2)
	require.True(t, coordPressureState(innerCache).evictFastMode.Load())
	outerArmed.Store(true)
	innerArmed.Store(true)

	assert.PanicsWithValue(t, "inner-nested-panic", func() {
		_ = outerCache.Replace("s0/outer", 2)
	})
	<-nestedPeerDone
	assert.True(t, nestedPeerEvicted.Load(), "peer spawned in outerCache OnEvict before innerCache Replace must not be misclassified as innerCache child")
}

func TestShardedCache_R14EvictFastModePeerAndDetachedIsolation(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, useCond := range []bool{false, true} {
			prim := "WaitGroup"
			if useCond {
				prim = "Cond"
			}
			for labelKind := range 3 {
				t.Run(fmt.Sprintf("AfterFunc_%s_Label%d/%s", prim, labelKind, mode), func(t *testing.T) {
					runR14AfterFuncWaitGroupAndCondPanicCase(t, sharded, useCond, labelKind)
				})
			}
		}
		t.Run("InnerChildContendsWhilePanickingPeerSerializes_FastMode/"+mode, func(t *testing.T) {
			runR14InnerChildContendsWhilePanickingPeerSerializesCase(t, sharded, false)
		})
		t.Run("InnerChildContendsWhilePanickingPeerSerializes_PprofChild/"+mode, func(t *testing.T) {
			runR14InnerChildContendsWhilePanickingPeerSerializesCase(t, sharded, true)
		})
		t.Run("ForeignExitedHopAndNestedDeliver/"+mode, func(t *testing.T) {
			runR14ForeignExitedHopAndNestedDeliverCase(t, sharded)
		})
	}
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run("LongHold400ms_"+backend.String()+"/Unsharded", func(t *testing.T) {
			runR14LongHoldBackendPeerDeletePrefixCase(t, backend, false)
		})
		t.Run("LongHold400ms_"+backend.String()+"/Sharded", func(t *testing.T) {
			runR14LongHoldBackendPeerDeletePrefixCase(t, backend, true)
		})
	}
}

//go:noinline
func r15SpawnWGWorkerHelper(wg *sync.WaitGroup, c Cache[int], updateLabels bool) {
	wg.Go(func() {
		if updateLabels {
			pprof.Do(context.Background(), pprof.Labels("worker", "helper"), func(context.Context) {
				_, _ = c.Delete("s1/victim")
			})
			return
		}
		_, _ = c.Delete("s1/victim")
	})
}

func runR15C1ContractCases(t *testing.T, sharded bool, mode int) {
	t.Helper()
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		var c Cache[int]
		var armed atomic.Bool
		var victimEvicts atomic.Int32

		c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 4,
			WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
				if k == "s1/victim" && r == EvictionReasonDeleted {
					victimEvicts.Add(1)
					return
				}
				if !armed.Load() {
					return
				}
				if k != "s0/trigger" || r != EvictionReasonReplaced {
					return
				}
				var wg sync.WaitGroup
				switch mode {
				case 0:
					r15SpawnWGWorkerHelper(&wg, c, false)
				case 1:
					r15SpawnWGWorkerHelper(&wg, c, true)
				case 2:
					wg.Go(func() {
						pprof.Do(context.Background(), pprof.Labels("worker", "inline"), func(context.Context) {
							_, _ = c.Delete("s1/victim")
						})
					})
				}
				wg.Wait()
			}),
		)

		warmupUncontendedReplaceEvictions(c, 10)
		_, _ = c.Put("s0/trigger", 0)
		_ = c.Replace("s0/trigger", 1)
		require.True(t, coordPressureState(c).evictFastMode.Load())
		_, _ = c.Put("s1/victim", 2)
		armed.Store(true)

		done := make(chan struct{})
		go func() {
			defer close(done)
			if mode == 1 {
				pprof.Do(context.Background(), pprof.Labels("outer", "req"), func(context.Context) {
					assert.NoError(t, c.Replace("s0/trigger", 10))
				})
				return
			}
			assert.NoError(t, c.Replace("s0/trigger", 10))
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("deadlocked in R15 C1 sharded=%v backend=%s mode=%d", sharded, backend, mode)
		}
		assert.Equal(t, int32(1), victimEvicts.Load())
	}
}

func runR15C2PprofScopeExitAndExitedWGHop(t *testing.T, sharded, exitedWGHop, twoHopGo bool) {
	t.Helper()
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		var c Cache[int]
		var armed atomic.Bool
		var victimEvicts atomic.Int32

		c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 4,
			WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
				if k == "s1/victim" && r == EvictionReasonDeleted {
					victimEvicts.Add(1)
					return
				}
				if !armed.Load() || k != "s0/trigger" || r != EvictionReasonReplaced {
					return
				}
				if exitedWGHop {
					var wg1, wg2 sync.WaitGroup
					hop1Done := make(chan struct{})
					wg1.Go(func() {
						wg2.Go(func() {
							<-hop1Done
							pprof.Do(context.Background(), pprof.Labels("hop", "2"), func(context.Context) {
								_, _ = c.Delete("s1/victim")
							})
						})
					})
					wg1.Wait()
					close(hop1Done)
					wg2.Wait()
					return
				}
				start := make(chan struct{})
				done := make(chan struct{})
				pprof.Do(context.Background(), pprof.Labels("op", "evict"), func(context.Context) {
					if twoHopGo {
						hopExited := make(chan struct{})
						go func() {
							defer close(hopExited)
							go func() {
								<-start
								_, _ = c.Delete("s1/victim")
								close(done)
							}()
						}()
						<-hopExited
						return
					}
					go func() {
						<-start
						_, _ = c.Delete("s1/victim")
						close(done)
					}()
				})
				close(start)
				<-done
			}),
		)

		warmupUncontendedReplaceEvictions(c, 10)
		_, _ = c.Put("s0/trigger", 0)
		_ = c.Replace("s0/trigger", 1)
		require.True(t, coordPressureState(c).evictFastMode.Load())
		_, _ = c.Put("s1/victim", 2)
		armed.Store(true)

		done := make(chan struct{})
		go func() {
			defer close(done)
			pprof.Do(context.Background(), pprof.Labels("outer", "r15-c2"), func(context.Context) {
				assert.NoError(t, c.Replace("s0/trigger", 10))
			})
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("deadlocked in R15 C2 sharded=%v backend=%s exitedWGHop=%v twoHopGo=%v", sharded, backend, exitedWGHop, twoHopGo)
		}
		assert.Equal(t, int32(1), victimEvicts.Load())
	}
}

type r15C3Service struct {
	t            *testing.T
	c            Cache[int]
	armed        atomic.Bool
	fgActive     atomic.Bool
	peerDuringFg atomic.Bool
	peerEvictRan atomic.Bool
	inFg         chan struct{}
	peerEntering chan struct{}
	peerPanicCh  chan any
	panicMode    int // 0: normal, 1: fg panics, 2: peer panics
	peerLabels   bool
}

//go:noinline
func (s *r15C3Service) runOp(inCallback bool) error {
	if inCallback {
		var blockMu sync.Mutex
		blockMu.Lock()
		close(s.inFg)
		<-s.peerEntering
		time.AfterFunc(35*time.Millisecond, blockMu.Unlock)
		blockMu.Lock()
		defer blockMu.Unlock()
		if s.panicMode == 1 {
			panic("r15-fg-callback-boom")
		}
		return nil
	}
	var outerWG sync.WaitGroup
	outerWG.Go(func() {
		defer func() { s.peerPanicCh <- recover() }()
		<-s.inFg
		runPeerDelete := func() {
			close(s.peerEntering)
			_, _ = s.c.Delete("s1/peer")
			s.peerDuringFg.Store(s.fgActive.Load())
		}
		if s.peerLabels {
			pprof.Do(context.Background(), pprof.Labels("peer", "1"), func(context.Context) {
				runPeerDelete()
			})
			return
		}
		runPeerDelete()
	})

	var err error
	if s.panicMode == 1 {
		assert.PanicsWithValue(s.t, "r15-fg-callback-boom", func() {
			_ = s.c.Replace("s0/fg", 10)
		})
	} else {
		err = s.c.Replace("s0/fg", 10)
	}
	outerWG.Wait()
	return err
}

func runR15C3ReenteredMethodPeerIsolationCase(t *testing.T, sharded, peerLabels bool, panicMode int) {
	t.Helper()
	svc := &r15C3Service{
		t:            t,
		inFg:         make(chan struct{}),
		peerEntering: make(chan struct{}),
		peerPanicCh:  make(chan any, 1),
		panicMode:    panicMode,
		peerLabels:   peerLabels,
	}
	svc.c = makeShardedOrUnshardedIntCache(sharded, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !svc.armed.Load() {
				return
			}
			if k == "s0/fg" {
				svc.fgActive.Store(true)
				defer svc.fgActive.Store(false)
				_ = svc.runOp(true)
				return
			}
			if k == "s1/peer" {
				svc.peerEvictRan.Store(true)
				if svc.panicMode == 2 {
					panic("r15-peer-callback-boom")
				}
			}
		}),
	)

	warmupUncontendedReplaceEvictions(svc.c, 10)
	_, _ = svc.c.Put("s0/fg", 0)
	_ = svc.c.Replace("s0/fg", 1)
	require.True(t, coordPressureState(svc.c).evictFastMode.Load())
	_, _ = svc.c.Put("s1/peer", 2)
	svc.armed.Store(true)

	errCh := make(chan error, 1)
	go func() {
		errCh <- svc.runOp(false)
	}()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatalf("deadlocked in R15 C3 sharded=%v peerLabels=%v panicMode=%d", sharded, peerLabels, panicMode)
	}

	assert.False(t, svc.peerDuringFg.Load(), "independent outerWG.Go peer must never return while foreground OnEvict is active")
	assert.True(t, svc.peerEvictRan.Load(), "independent outerWG.Go peer OnEvict must never be dropped")
	peerRecovered := <-svc.peerPanicCh
	if panicMode == 2 {
		assert.Equal(t, "r15-peer-callback-boom", peerRecovered, "peer panic must propagate on peer goroutine, not foreground holder")
	} else {
		assert.Nil(t, peerRecovered)
	}
}

func TestShardedCache_R15EvictFastModeLineageAndPprofFixes(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		t.Run("C1_HelperNoLabelOverwrite/"+mode, func(t *testing.T) {
			runR15C1ContractCases(t, sharded, 0)
		})
		t.Run("C1_HelperWithOuterPprofScope/"+mode, func(t *testing.T) {
			runR15C1ContractCases(t, sharded, 1)
		})
		t.Run("C1_InlineWGWithChildPprofDo/"+mode, func(t *testing.T) {
			runR15C1ContractCases(t, sharded, 2)
		})
		t.Run("C2_ScenarioA_PprofDoScopeExit/"+mode, func(t *testing.T) {
			runR15C2PprofScopeExitAndExitedWGHop(t, sharded, false, false)
		})
		t.Run("C2_ScenarioA_TwoHopExitedGo/"+mode, func(t *testing.T) {
			runR15C2PprofScopeExitAndExitedWGHop(t, sharded, false, true)
		})
		t.Run("C2_ScenarioB_ExitedIntermediateWGGo/"+mode, func(t *testing.T) {
			runR15C2PprofScopeExitAndExitedWGHop(t, sharded, true, false)
		})
		for _, peerLabels := range []bool{false, true} {
			lblSuffix := "UnlabeledPeer"
			if peerLabels {
				lblSuffix = "LabeledPeer"
			}
			t.Run("C3_NoAsyncReturn_"+lblSuffix+"/"+mode, func(t *testing.T) {
				runR15C3ReenteredMethodPeerIsolationCase(t, sharded, peerLabels, 0)
			})
			t.Run("C3_NoCallbackLossOnFgPanic_"+lblSuffix+"/"+mode, func(t *testing.T) {
				runR15C3ReenteredMethodPeerIsolationCase(t, sharded, peerLabels, 1)
			})
			t.Run("C3_NoPanicHijackOnPeerPanic_"+lblSuffix+"/"+mode, func(t *testing.T) {
				runR15C3ReenteredMethodPeerIsolationCase(t, sharded, peerLabels, 2)
			})
		}
	}
}

func applyR16DetachedLabelMode(sharedCtx context.Context, labelMode int, fn func()) {
	switch labelMode {
	case 0:
		pprof.Do(context.Background(), pprof.Labels("bg", "r16-wg"), func(context.Context) { fn() })
	case 1:
		pprof.SetGoroutineLabels(context.Background())
		fn()
	default:
		pprof.SetGoroutineLabels(sharedCtx)
		defer pprof.SetGoroutineLabels(context.Background())
		fn()
	}
}

func applyR16Hold2LabelMode(sharedCtx context.Context, labelMode int) func() {
	switch labelMode {
	case 1:
		pprof.SetGoroutineLabels(context.Background())
		return func() {}
	case 2:
		pprof.SetGoroutineLabels(sharedCtx)
		return func() { pprof.SetGoroutineLabels(context.Background()) }
	default:
		return func() {}
	}
}

func runR16C1SingleDetachedCase(t *testing.T, sharded bool, backend Backend, useLongWG bool, labelMode, panicMode int) {
	t.Helper()
	var c Cache[int]
	var bgWG sync.WaitGroup
	var holdIdx atomic.Int32
	var fgInHold2, returnedEarly atomic.Bool
	var victimEvicted atomic.Int32
	inHold2 := make(chan struct{})
	detachedDone := make(chan any, 1)
	sharedCtx := pprof.WithLabels(context.Background(), pprof.Labels("shared", "r16"))

	spawnDetached := func() {
		task := func() {
			var rec any
			defer func() { detachedDone <- rec }()
			defer func() { rec = recover() }()
			<-inHold2
			applyR16DetachedLabelMode(sharedCtx, labelMode, func() {
				_, _ = c.Delete("s1/victim")
				returnedEarly.Store(fgInHold2.Load())
			})
		}
		if useLongWG {
			bgWG.Go(task)
			return
		}
		go task()
	}

	c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 8,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if k == "s1/victim" {
				victimEvicted.Add(1)
				if panicMode == 2 {
					panic("r16-detached-victim-panic")
				}
				return
			}
			if k != "s0/slot" {
				return
			}
			switch holdIdx.Add(1) {
			case 1:
				spawnDetached()
			case 2:
				fgInHold2.Store(true)
				defer fgInHold2.Store(false)
				restore := applyR16Hold2LabelMode(sharedCtx, labelMode)
				defer restore()
				var localWG sync.WaitGroup
				localWG.Add(1)
				close(inHold2)
				time.AfterFunc(35*time.Millisecond, localWG.Done)
				localWG.Wait()
				if panicMode == 1 {
					panic("r16-hold2-fg-panic")
				}
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 8)
	_, _ = c.Put("s0/slot", 0)
	_, _ = c.Put("s1/victim", 99)
	require.NoError(t, c.Replace("s0/slot", 1)) // Hold 1: spawns prior-hold detached goroutine
	if panicMode == 1 {
		assert.PanicsWithValue(t, "r16-hold2-fg-panic", func() {
			_ = c.Replace("s0/slot", 2) // Hold 2 on same goroutine
		})
	} else {
		require.NotPanics(t, func() {
			assert.NoError(t, c.Replace("s0/slot", 2))
		})
	}
	detachedRecovered := <-detachedDone
	bgWG.Wait()
	assert.False(t, returnedEarly.Load(), "prior-hold detached goroutine must never return while Hold 2 holds evictCallbackMu")
	assert.Equal(t, int32(1), victimEvicted.Load(), "prior-hold detached goroutine OnEvict callback must never be dropped")
	if panicMode == 2 {
		assert.Equal(t, "r16-detached-victim-panic", detachedRecovered)
	} else {
		assert.Nil(t, detachedRecovered)
	}
}

func TestShardedCache_R16C1DetachedGoroutineIsolation(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			bName := backend.String()
			t.Run("LongLivedWG_PprofDo_Hold2Panics/"+mode+"/"+bName, func(t *testing.T) {
				runR16C1SingleDetachedCase(t, sharded, backend, true, 0, 1)
			})
			t.Run("LongLivedWG_PprofDo_DetachedPanics/"+mode+"/"+bName, func(t *testing.T) {
				runR16C1SingleDetachedCase(t, sharded, backend, true, 0, 2)
			})
			t.Run("PlainGo_ClearedLabels_Hold2Panics/"+mode+"/"+bName, func(t *testing.T) {
				runR16C1SingleDetachedCase(t, sharded, backend, false, 1, 1)
			})
			t.Run("PlainGo_ClearedLabels_DetachedPanics/"+mode+"/"+bName, func(t *testing.T) {
				runR16C1SingleDetachedCase(t, sharded, backend, false, 1, 2)
			})
			t.Run("PlainGo_SharedLabels_Hold2Panics/"+mode+"/"+bName, func(t *testing.T) {
				runR16C1SingleDetachedCase(t, sharded, backend, false, 2, 1)
			})
		}
	}
}

type r16ArenaWorker struct {
	c          Cache[int]
	useReplace bool
}

//go:noinline
func (w *r16ArenaWorker) mutateOther() {
	if w.useReplace {
		_ = w.c.Replace("s1/other", 99)
		return
	}
	_, _ = w.c.Put("s1/other-new", 99)
}

func runR16C2ArenaRadixMethodMatchCase(t *testing.T, sharded, useReplace, initTamper bool) {
	t.Helper()
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		worker := &r16ArenaWorker{useReplace: useReplace}
		var armed atomic.Bool
		var childEvicts atomic.Int32
		worker.c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 2,
			WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
				if strings.HasPrefix(k, "s1/") {
					childEvicts.Add(1)
					return
				}
				if !armed.Load() {
					return
				}
				if k != "s0/seed" {
					return
				}
				pprof.Do(context.Background(), pprof.Labels("op", "evict"), func(context.Context) {
					var wg sync.WaitGroup
					wg.Go(worker.mutateOther)
					wg.Wait()
				})
			}),
		)
		warmupUncontendedReplaceEvictions(worker.c, 8)
		_, _ = worker.c.Put("s0/seed", 0)
		_ = worker.c.Replace("s0/seed", 1)
		_, _ = worker.c.Put("s1/other", 2)
		require.True(t, coordPressureState(worker.c).evictFastMode.Load())
		armed.Store(true)

		done := make(chan struct{})
		go func() {
			defer close(done)
			runTrigger := func() {
				if useReplace {
					assert.NoError(t, worker.c.Replace("s0/seed", 10))
				} else {
					_, _ = worker.c.Put("s0/trigger", 10)
				}
			}
			if !initTamper {
				pprof.Do(context.Background(), pprof.Labels("outer", "c2"), func(context.Context) { runTrigger() })
				return
			}
			runTrigger()
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("deadlocked in R16 C2 sharded=%v backend=%s replace=%v initTamper=%v", sharded, backend, useReplace, initTamper)
		}
		assert.Equal(t, int32(1), childEvicts.Load())
	}
}

func runR16C3InlineGoLabelTransitionCase(t *testing.T, sharded, variantB, initTamper bool) {
	t.Helper()
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		var c Cache[int]
		var armed atomic.Bool
		var victimEvicts atomic.Int32
		c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 4,
			WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
				if k == "s1/victim" && r == EvictionReasonDeleted {
					victimEvicts.Add(1)
					return
				}
				if !armed.Load() {
					return
				}
				if k != "s0/trigger" || r != EvictionReasonReplaced {
					return
				}
				childDone := make(chan struct{})
				if !variantB {
					lbl := pprof.WithLabels(context.Background(), pprof.Labels("op", "evict-a"))
					pprof.SetGoroutineLabels(lbl)
					go func() {
						defer close(childDone)
						_, _ = c.Delete("s1/victim")
					}()
					pprof.SetGoroutineLabels(context.Background())
					<-childDone
					return
				}
				pprof.SetGoroutineLabels(context.Background())
				go func() {
					defer close(childDone)
					_, _ = c.Delete("s1/victim")
				}()
				pprof.Do(context.Background(), pprof.Labels("op", "wait-b"), func(context.Context) {
					<-childDone
				})
			}),
		)
		warmupUncontendedReplaceEvictions(c, 8)
		_, _ = c.Put("s0/trigger", 0)
		_ = c.Replace("s0/trigger", 1)
		_, _ = c.Put("s1/victim", 2)
		require.True(t, coordPressureState(c).evictFastMode.Load())
		armed.Store(true)

		done := make(chan struct{})
		go func() {
			defer close(done)
			if !initTamper {
				pprof.Do(context.Background(), pprof.Labels("outer", "c3"), func(context.Context) {
					assert.NoError(t, c.Replace("s0/trigger", 10))
				})
				return
			}
			assert.NoError(t, c.Replace("s0/trigger", 10))
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("deadlocked in R16 C3 sharded=%v backend=%s variantB=%v initTamper=%v", sharded, backend, variantB, initTamper)
		}
		assert.Equal(t, int32(1), victimEvicts.Load())
	}
}

func TestShardedCache_R16C2AndC3ArenaRadixAndPprofContracts(t *testing.T) {
	t.Run("InternalLRUMethodFrameClassification", func(t *testing.T) {
		positives := []string{
			"github.com/google/go-lru.(*arenaRadix[int]).Put",
			"github.com/google/go-lru.(*arenaRadix[go.shape.int]).Replace",
			"github.com/google/go-lru.(*mapCache[int]).Delete",
			"github.com/google/go-lru.(*radixCache[int]).DeletePrefix",
			"github.com/google/go-lru.(*shardedCache[int]).EvaluateMemoryPressure",
			"github.com/google/go-lru.(*evictCallbackQueue[int]).invokeNonEmpty",
			"github.com/google/go-lru.(*pressureState).lockWithPressure",
			"github.com/google/go-lru.Cache[int].Put-fm",
			"github.com/google/go-lru.PressureAwareCache[int].EvaluateMemoryPressure-fm",
			"github.com/google/go-lru.StatsProvider.Stats-fm",
		}
		for _, sym := range positives {
			assert.True(t, isInternalLRUMethodFrame([]byte(sym)), sym)
		}
		negatives := []string{
			"github.com/google/go-lru.TestShardedCache_R16C2AndC3ArenaRadixAndPprofContracts",
			"github.com/google/go-lru.(*r16ArenaWorker).mutateOther",
			"main.worker",
		}
		for _, sym := range negatives {
			assert.False(t, isInternalLRUMethodFrame([]byte(sym)), sym)
		}
	})
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, useReplace := range []bool{true, false} {
			op := "Put"
			if useReplace {
				op = "Replace"
			}
			t.Run("C2_OuterScope_"+op+"/"+mode, func(t *testing.T) {
				runR16C2ArenaRadixMethodMatchCase(t, sharded, useReplace, false)
			})
			t.Run("C2_InitTamper_"+op+"/"+mode, func(t *testing.T) {
				runR16C2ArenaRadixMethodMatchCase(t, sharded, useReplace, true)
			})
		}
		t.Run("C3_VariantA_OuterScope/"+mode, func(t *testing.T) {
			runR16C3InlineGoLabelTransitionCase(t, sharded, false, false)
		})
		t.Run("C3_VariantA_InitTamper/"+mode, func(t *testing.T) {
			runR16C3InlineGoLabelTransitionCase(t, sharded, false, true)
		})
		t.Run("C3_VariantB_OuterScope/"+mode, func(t *testing.T) {
			runR16C3InlineGoLabelTransitionCase(t, sharded, true, false)
		})
		t.Run("C3_VariantB_InitTamper/"+mode, func(t *testing.T) {
			runR16C3InlineGoLabelTransitionCase(t, sharded, true, true)
		})
	}
}

func runR16C4NestedOnEvictExitedParentCase(t *testing.T, sharded bool, backend Backend) {
	t.Helper()
	var outerCache Cache[int]
	var outerArmed, outerFgActive, peerDuringFg, peerEvicted atomic.Bool
	inInner := make(chan struct{})
	parentExited := make(chan struct{})
	peerDone := make(chan struct{})

	innerCache := New[string](4,
		WithBackend(backend),
		WithOnEvictEntry(func(k, _ string, _ EvictionReason) {
			if k == "inner-k" && outerArmed.Load() {
				close(inInner)
				assert.True(t, waitUntilEvictSlowWaiterParked(outerCache))
				panic("r16-c4-inner-panic")
			}
		}),
	)
	_, _ = innerCache.Put("inner-k", "v1")

	outerCache = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 4,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !outerArmed.Load() {
				return
			}
			if k == "s0/outer-fg" {
				outerFgActive.Store(true)
				defer outerFgActive.Store(false)
				_ = innerCache.Replace("inner-k", "v2")
				return
			}
			if k == "s1/outer-peer" {
				peerEvicted.Store(true)
			}
		}),
	)
	_, _ = outerCache.Put("s0/outer-fg", 1)
	_, _ = outerCache.Put("s1/outer-peer", 2)
	outerArmed.Store(true)

	go func() {
		<-inInner
		go func() {
			defer close(peerDone)
			<-parentExited
			_, _ = outerCache.Delete("s1/outer-peer")
			peerDuringFg.Store(outerFgActive.Load())
		}()
		close(parentExited)
	}()

	assert.PanicsWithValue(t, "r16-c4-inner-panic", func() {
		_ = outerCache.Replace("s0/outer-fg", 10)
	})
	<-peerDone
	assert.False(t, peerDuringFg.Load(), "unrelated caller with exited parent must not return while outerCache OnEvict is active")
	assert.True(t, peerEvicted.Load(), "unrelated caller OnEvict callback must not be dropped when nested cache panics")
}

func runR16C4NestedPressureExitedParentCase(t *testing.T, sharded bool, backend Backend) {
	t.Helper()
	innerCache := New[string](8,
		WithBackend(backend),
		WithPressureFunc(func() float64 { return 0.10 }),
	)
	_, _ = innerCache.Put("ik", "iv")

	var armed atomic.Bool
	inSampler := make(chan struct{})
	releaseSampler := make(chan struct{})
	var firstEntered atomic.Bool

	outerCache := makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 8,
		WithEvictionRetentionRatio(0.25),
		WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.10
			}
			_ = innerCache.(PressureAwareCache[string]).EvaluateMemoryPressure()
			if firstEntered.CompareAndSwap(false, true) {
				close(inSampler)
				<-releaseSampler
			}
			return 0.99
		}),
	)
	for i := range 8 {
		_, _ = outerCache.Put(fmt.Sprintf("s%d/k%d", i%2, i), i+1)
	}
	armed.Store(true)

	fgDone := make(chan struct{})
	go func() {
		defer close(fgDone)
		_ = outerCache.(PressureAwareCache[int]).EvaluateMemoryPressure()
	}()
	<-inSampler

	parentExited := make(chan struct{})
	peerEvictedCh := make(chan int, 1)
	go func() {
		go func() {
			<-parentExited
			evicted := outerCache.(PressureAwareCache[int]).EvaluateMemoryPressure()
			peerEvictedCh <- len(evicted)
			close(releaseSampler)
		}()
		close(parentExited)
	}()

	peerEvicted := <-peerEvictedCh
	<-fgDone
	assert.Positive(t, peerEvicted, "unrelated caller with exited parent must execute critical Tier 2 shedding instead of re-entrant fallback")
	assert.LessOrEqual(t, outerCache.Stats().Len, 2)
}

func TestShardedCache_R16C4ExitedParentNestedCacheIsolation(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			bName := backend.String()
			t.Run("ScenarioA_OnEvict/"+mode+"/"+bName, func(t *testing.T) {
				runR16C4NestedOnEvictExitedParentCase(t, sharded, backend)
			})
			t.Run("ScenarioB_PressureFunc/"+mode+"/"+bName, func(t *testing.T) {
				runR16C4NestedPressureExitedParentCase(t, sharded, backend)
			})
		}
	}
}

type r16C5PeerTask struct {
	c            Cache[int]
	parentExited chan struct{}
	done         chan struct{}
	fgActive     *atomic.Bool
	duringFg     *atomic.Bool
}

//go:noinline
func r16SingleCachePeerPut(task *r16C5PeerTask) {
	defer close(task.done)
	<-task.parentExited
	_, _ = task.c.Put("s1/peer-old", 99)
	task.duringFg.Store(task.fgActive.Load())
}

func runR16C5SingleCacheExitedParentCase(t *testing.T, sharded bool, backend Backend) {
	t.Helper()
	var c Cache[int]
	var armed, fgActive, peerDuringFg, peerEvicted atomic.Bool
	var childWeighOnce sync.Once
	inChildWeigh := make(chan struct{})
	releaseChildWeigh := make(chan struct{})

	c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 4,
		WithWeigher(func(k string, _ int) uint64 {
			if k == "s0/child-put" {
				childWeighOnce.Do(func() {
					close(inChildWeigh)
					<-releaseChildWeigh
				})
			}
			return 1
		}),
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if !armed.Load() {
				return
			}
			if k == "s0/fg" && r == EvictionReasonReplaced {
				fgActive.Store(true)
				defer fgActive.Store(false)
				var wg sync.WaitGroup
				wg.Go(func() {
					_, _ = c.Put("s0/child-put", 10)
				})
				<-inChildWeigh
				assert.True(t, waitUntilEvictSlowWaiterParked(c))
				close(releaseChildWeigh)
				wg.Wait()
				panic("r16-c5-fg-panic")
			}
			if k == "s1/peer-old" && r == EvictionReasonReplaced {
				peerEvicted.Store(true)
			}
		}),
	)

	_, _ = c.Put("s0/fg", 1)
	_, _ = c.Put("s1/peer-old", 2)
	armed.Store(true)

	task := &r16C5PeerTask{
		c:            c,
		parentExited: make(chan struct{}),
		done:         make(chan struct{}),
		fgActive:     &fgActive,
		duringFg:     &peerDuringFg,
	}
	go func() {
		<-inChildWeigh
		go r16SingleCachePeerPut(task)
		close(task.parentExited)
	}()

	assert.PanicsWithValue(t, "r16-c5-fg-panic", func() {
		_ = c.Replace("s0/fg", 11)
	})
	<-task.done
	assert.False(t, peerDuringFg.Load(), "named peer entry calling Put at frame index 1 must not bridge onto live OnEvict child")
	assert.True(t, peerEvicted.Load(), "named peer entry OnEvict callback must never be dropped")
}

func TestShardedCache_R16C5BlockOwnsExitedChildSingleCacheIsolation(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			t.Run(mode+"/"+backend.String(), func(t *testing.T) {
				runR16C5SingleCacheExitedParentCase(t, sharded, backend)
			})
		}
	}
}

type r16SharedEvictRecorder struct {
	t            *testing.T
	cacheB       Cache[int]
	armed        atomic.Bool
	bFgActive    atomic.Bool
	peerDuringFg atomic.Bool
	bPeerEvicted atomic.Bool
	inBQueued    chan struct{}
	aExited      chan struct{}
	peerDone     chan struct{}
}

//go:noinline
func (r *r16SharedEvictRecorder) onEvict(k string, _ int, _ EvictionReason) {
	if !r.armed.Load() {
		return
	}
	switch k {
	case "s0/b-first":
		_, _ = r.cacheB.Delete("s0/b-second")
	case "s0/b-second":
		r.bFgActive.Store(true)
		defer r.bFgActive.Store(false)
		close(r.inBQueued)
		assert.True(r.t, waitUntilEvictSlowWaiterParked(r.cacheB))
		panic("r16-c6-queued-panic")
	case "s0/a-trigger":
		go func() {
			defer close(r.peerDone)
			<-r.aExited
			_, _ = r.cacheB.Delete("s1/b-peer")
			r.peerDuringFg.Store(r.bFgActive.Load())
		}()
	case "s1/b-peer":
		r.bPeerEvicted.Store(true)
	}
}

func runR16C6PendingBatchSharedCallbackCase(t *testing.T, sharded bool, backend Backend) {
	t.Helper()
	rec := &r16SharedEvictRecorder{
		t:         t,
		inBQueued: make(chan struct{}),
		aExited:   make(chan struct{}),
		peerDone:  make(chan struct{}),
	}
	cacheA := makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 4, WithOnEvictEntry(rec.onEvict))
	rec.cacheB = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 4, WithOnEvictEntry(rec.onEvict))

	_, _ = cacheA.Put("s0/a-trigger", 1)
	_, _ = rec.cacheB.Put("s0/b-first", 1)
	_, _ = rec.cacheB.Put("s0/b-second", 2)
	_, _ = rec.cacheB.Put("s1/b-peer", 3)
	rec.armed.Store(true)

	go func() {
		<-rec.inBQueued
		_, _ = cacheA.Delete("s0/a-trigger")
		close(rec.aExited)
	}()

	assert.PanicsWithValue(t, "r16-c6-queued-panic", func() {
		_, _ = rec.cacheB.Delete("s0/b-first")
	})
	<-rec.peerDone
	assert.False(t, rec.peerDuringFg.Load(), "shared callback spawned from cacheA must not enqueue as child of cacheB pending batch")
	assert.True(t, rec.bPeerEvicted.Load(), "b-peer OnEvict callback must never be dropped when cacheB pending batch panics")
}

func TestShardedCache_R16C6InternalCallbackDispatchFramesIsolation(t *testing.T) {
	t.Run("InternalCallbackDispatchFrameClassification", func(t *testing.T) {
		positives := []string{
			"github.com/google/go-lru.(*evictCallbackQueue[int]).tryEnqueuePendingCallback.func1",
			"github.com/google/go-lru.(*evictCallbackQueue[int]).enqueuePendingCallback",
			"github.com/google/go-lru.(*evictCallbackQueue[int]).deliverCallbacks",
			"github.com/google/go-lru.(*evictCallbackQueue[int]).deliverCallbacksSlow",
			"github.com/google/go-lru.(*evictCallbackQueue[int]).deliverMultiCallbacks",
			"github.com/google/go-lru.resolveOnEvictEntry[int]",
			"github.com/google/go-lru.(*pressureState).dispatchFastPrimaryPressure",
			"github.com/google/go-lru.(*pressureState).invokeFastPrimaryPressure0",
		}
		for _, sym := range positives {
			assert.True(t, isInternalCallbackDispatchFrame([]byte(sym)), sym)
		}
		negatives := []string{
			"github.com/google/go-lru.(*r16SharedEvictRecorder).onEvict",
			"example.com/pkg.tryEnqueuePendingCallback.func1",
		}
		for _, sym := range negatives {
			assert.False(t, isInternalCallbackDispatchFrame([]byte(sym)), sym)
		}
	})
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			t.Run(mode+"/"+backend.String(), func(t *testing.T) {
				runR16C6PendingBatchSharedCallbackCase(t, sharded, backend)
			})
		}
	}
}

//go:noinline
func startR17AsyncPprofDelete(c Cache[int], key string) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		pprof.Do(context.Background(), pprof.Labels("r17", "returned-helper"), func(context.Context) {
			_, _ = c.Delete(key)
		})
	}()
	return done
}

func runR17C1LeafDelete(c Cache[int], childPprof bool, release, done chan struct{}) {
	defer close(done)
	<-release
	if childPprof {
		pprof.Do(context.Background(), pprof.Labels("r17", "cascade-leaf"), func(context.Context) {
			_, _ = c.Delete("s1/k3")
		})
		return
	}
	_, _ = c.Delete("s1/k3")
}

func runR17C1MidHoldCascadeDrainCase(t *testing.T, sharded bool, backend Backend, grandchild, exitedHop, childPprof bool) {
	t.Helper()
	var c Cache[int]
	var armed atomic.Bool
	var k3Evicted atomic.Int32
	spawned := make(chan struct{})
	release := make(chan struct{})
	workerDone := make(chan struct{})

	c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 8,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armed.Load() {
				return
			}
			switch k {
			case "s0/k1":
				if !grandchild {
					go func() {
						close(spawned)
						runR17C1LeafDelete(c, childPprof, release, workerDone)
					}()
				} else {
					go func() {
						hopExited := make(chan struct{})
						go func() {
							if exitedHop {
								<-hopExited
							}
							runR17C1LeafDelete(c, childPprof, release, workerDone)
						}()
						close(spawned)
						if exitedHop {
							close(hopExited)
							return
						}
						<-workerDone
					}()
				}
				<-spawned
				require.NoError(t, c.Replace("s0/k2", 20))
			case "s0/k2":
				close(release)
				<-workerDone
			case "s1/k3":
				k3Evicted.Add(1)
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 8)
	_, _ = c.Put("s0/k1", 0)
	_ = c.Replace("s0/k1", 1)
	_, _ = c.Put("s0/k2", 2)
	_, _ = c.Put("s1/k3", 3)
	p := coordPressureState(c)
	require.True(t, p.evictFastMode.Load())
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.NoError(t, c.Replace("s0/k1", 10))
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("deadlocked in R17 C1 sharded=%v backend=%s grandchild=%v exitedHop=%v childPprof=%v",
			sharded, backend, grandchild, exitedHop, childPprof)
	}
	assert.Equal(t, int32(1), k3Evicted.Load())
	assert.Zero(t, p.evictHoldSeq.Load()&1)
}

func runR17C2LabeledWarmupUnlabeledTriggerCase(t *testing.T, sharded bool, backend Backend, extraUnlabeled int) {
	t.Helper()
	var c Cache[int]
	var armed atomic.Bool
	var victimEvicted atomic.Int32

	c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 4,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s1/victim" && r == EvictionReasonDeleted {
				victimEvicted.Add(1)
				return
			}
			if !armed.Load() || k != "s0/slot" {
				return
			}
			pprof.Do(context.Background(), pprof.Labels("r17", "c2-cb"), func(context.Context) {
				var wg sync.WaitGroup
				wg.Go(func() {
					_, _ = c.Delete("s1/victim")
				})
				wg.Wait()
			})
		}),
	)

	_, _ = c.Put("s0/slot", 0)
	_, _ = c.Put("s1/victim", 99)
	for i := range 8 {
		pprof.Do(context.Background(), pprof.Labels("req", "labeled-warmup"), func(context.Context) {
			require.NoError(t, c.Replace("s0/slot", i+1))
		})
	}
	p := coordPressureState(c)
	assert.Zero(t, p.evictProbedUnlabeledHolds)
	assert.False(t, p.evictLabelTampered.Load())

	for i := range extraUnlabeled {
		require.NoError(t, c.Replace("s0/slot", 100+i))
	}
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer pprof.SetGoroutineLabels(context.Background())
		assert.NoError(t, c.Replace("s0/slot", 999))
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("deadlocked in R17 C2 sharded=%v backend=%s extraUnlabeled=%d", sharded, backend, extraUnlabeled)
	}
	assert.Equal(t, int32(1), victimEvicted.Load())
}

func runR17C3ConditionalCapacityReasonPprofCase(t *testing.T, sharded bool, backend Backend) {
	t.Helper()
	var c Cache[int]
	var childDeleted atomic.Int32

	c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 2,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s1/keep" && r == EvictionReasonDeleted {
				childDeleted.Add(1)
				return
			}
			if r != EvictionReasonCapacity {
				return
			}
			pprof.Do(context.Background(), pprof.Labels("reason", "capacity"), func(context.Context) {
				var wg sync.WaitGroup
				wg.Go(func() {
					_, _ = c.Delete("s1/keep")
				})
				wg.Wait()
			})
		}),
	)

	_, _ = c.Put("s0/a", 1)
	for i := range 10 {
		require.NoError(t, c.Replace("s0/a", i+2))
	}
	p := coordPressureState(c)
	require.True(t, p.evictFastMode.Load())
	require.False(t, p.evictLabelTampered.Load())

	_, _ = c.Put("s1/keep", 50)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if sharded {
			_, _ = c.Put("s0/b", 60)
		}
		_, _ = c.Put("s0/overflow", 70)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("deadlocked in R17 C3 conditional capacity sharded=%v backend=%s", sharded, backend)
	}
	assert.Equal(t, int32(1), childDeleted.Load())
	assert.True(t, p.evictLabelTampered.Load())
}

func execR17ChildOnlyPattern(c Cache[int], pattern int) {
	switch pattern {
	case 0:
		var wg sync.WaitGroup
		wg.Go(func() {
			pprof.Do(context.Background(), pprof.Labels("w", "1"), func(context.Context) {
				_, _ = c.Delete("s1/v1")
			})
		})
		wg.Go(func() {
			pprof.Do(context.Background(), pprof.Labels("w", "2"), func(context.Context) {
				_, _ = c.Delete("s1/v2")
			})
		})
		wg.Wait()
	case 1:
		done := startR17AsyncPprofDelete(c, "s1/v1")
		<-done
	default:
		var finished atomic.Bool
		go func() {
			pprof.SetGoroutineLabels(context.Background())
			_, _ = c.Delete("s1/v1")
			finished.Store(true)
		}()
		for !finished.Load() {
			time.Sleep(time.Millisecond)
		}
	}
}

func runR17C3ChildOnlyPatternsCase(t *testing.T, sharded bool, backend Backend, pattern int) {
	t.Helper()
	var c Cache[int]
	var armed atomic.Bool
	var deleted atomic.Int32

	c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 8,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if strings.HasPrefix(k, "s1/v") && r == EvictionReasonDeleted {
				deleted.Add(1)
				return
			}
			if !armed.Load() || k != "s0/trigger" || r != EvictionReasonReplaced {
				return
			}
			execR17ChildOnlyPattern(c, pattern)
		}),
	)

	warmupUncontendedReplaceEvictions(c, 8)
	_, _ = c.Put("s0/trigger", 0)
	_ = c.Replace("s0/trigger", 1)
	_, _ = c.Put("s1/v1", 2)
	_, _ = c.Put("s1/v2", 3)
	require.True(t, coordPressureState(c).evictFastMode.Load())
	armed.Store(true)

	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.NoError(t, c.Replace("s0/trigger", 10))
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("deadlocked in R17 C3 child-only sharded=%v backend=%s pattern=%d", sharded, backend, pattern)
	}
	want := int32(1)
	if pattern == 0 {
		want = 2
	}
	assert.Equal(t, want, deleted.Load())
}

func runR17BlackBoxDetachedTimerPanicCase(t *testing.T, sharded bool, backend Backend) {
	t.Helper()
	var c Cache[int]
	var holdNum atomic.Int32
	var armed, fgInHold2, detachedReturnedEarly, detachedEvicted atomic.Bool
	startDetached := make(chan struct{})
	detachedEntering := make(chan struct{})
	detachedDone := make(chan struct{})

	c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 8,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armed.Load() {
				return
			}
			if k == "s1/detached-victim" {
				detachedEvicted.Store(true)
				return
			}
			if k != "s0/slot" {
				return
			}
			switch holdNum.Add(1) {
			case 1:
				go func() {
					defer close(detachedDone)
					<-startDetached
					pprof.Do(context.Background(), pprof.Labels("bg", "r17-detached"), func(context.Context) {
						close(detachedEntering)
						_, _ = c.Delete("s1/detached-victim")
						detachedReturnedEarly.Store(fgInHold2.Load())
					})
				}()
			case 2:
				fgInHold2.Store(true)
				defer fgInHold2.Store(false)
				close(startDetached)
				<-detachedEntering
				if backend == BackendMap && !sharded {
					<-time.After(500 * time.Millisecond)
				} else {
					var blockMu sync.Mutex
					blockMu.Lock()
					time.AfterFunc(40*time.Millisecond, blockMu.Unlock)
					blockMu.Lock()
					defer blockMu.Unlock()
				}
				panic("r17-hold2-timer-panic")
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 8)
	_, _ = c.Put("s0/slot", -1)
	_ = c.Replace("s0/slot", 0)
	_, _ = c.Put("s1/detached-victim", 99)
	require.True(t, coordPressureState(c).evictFastMode.Load())
	armed.Store(true)

	require.NoError(t, c.Replace("s0/slot", 1))
	require.True(t, coordPressureState(c).evictFastMode.Load())
	assert.PanicsWithValue(t, "r17-hold2-timer-panic", func() {
		_ = c.Replace("s0/slot", 2)
	})
	<-detachedDone
	assert.False(t, detachedReturnedEarly.Load(), "detached goroutine must not return while Hold 2 is active")
	assert.True(t, detachedEvicted.Load(), "detached goroutine OnEvict callback must never be dropped on Hold 2 timer panic")
}

func runR17FastEvictTagLeaseStealAndOverflowCase(t *testing.T, sharded bool, backend Backend) {
	t.Helper()
	const totalCaches = numPressureHookTags + 4
	caches := make([]Cache[int], totalCaches)
	armed := make([]atomic.Bool, totalCaches)
	var callbacksFired atomic.Int32

	for idx := range totalCaches {
		myIdx := idx
		caches[idx] = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 32,
			WithOnEvictEntry(func(key string, _ int, reason EvictionReason) {
				if !armed[myIdx].Load() || reason != EvictionReasonReplaced {
					return
				}
				callbacksFired.Add(1)
				var wg sync.WaitGroup
				wg.Go(func() {
					pprof.Do(context.Background(), pprof.Labels("lease-idx", fmt.Sprintf("%d", myIdx)), func(context.Context) {
						_, _ = caches[myIdx].Delete("s0/target")
					})
				})
				wg.Wait()
				if myIdx+1 < totalCaches && key == "s0/chain" {
					_ = caches[myIdx+1].Replace("s0/chain", 2)
				}
			}),
		)
		warmupUncontendedReplaceEvictions(caches[idx], 6)
		_, _ = caches[idx].Put("s0/warm", 1)
		_ = caches[idx].Replace("s0/warm", 2)
	}

	// Sequential re-entrancy across all 12 caches forces idle lease stealing and re-acquisition.
	for idx := range totalCaches {
		_, _ = caches[idx].Put("s0/slot", 1)
		_, _ = caches[idx].Put("s0/target", 10)
		armed[idx].Store(true)
		require.NoError(t, caches[idx].Replace("s0/slot", 2))
		armed[idx].Store(false)
	}
	assert.Equal(t, int32(totalCaches), callbacksFired.Load())

	// 12-deep nested chain holds >8 eviction locks simultaneously, exercising slow-tag fallback.
	callbacksFired.Store(0)
	for idx := range totalCaches {
		_, _ = caches[idx].Put("s0/chain", 1)
		_, _ = caches[idx].Put("s0/target", 20)
		armed[idx].Store(true)
	}
	require.NoError(t, caches[0].Replace("s0/chain", 2))
	assert.Equal(t, int32(totalCaches), callbacksFired.Load())
}

func TestShardedCache_R17MidHoldSnapAndUnconditionalPprof(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			bName := backend.String()
			t.Run("C1_MidHoldCascadeDrain/"+mode+"/"+bName, func(t *testing.T) {
				runR17C1MidHoldCascadeDrainCase(t, sharded, backend, false, false, false)
				runR17C1MidHoldCascadeDrainCase(t, sharded, backend, true, false, false)
				runR17C1MidHoldCascadeDrainCase(t, sharded, backend, true, true, true)
			})
			t.Run("C2_LabeledWarmupUnlabeledTrigger/"+mode+"/"+bName, func(t *testing.T) {
				runR17C2LabeledWarmupUnlabeledTriggerCase(t, sharded, backend, 0)
				runR17C2LabeledWarmupUnlabeledTriggerCase(t, sharded, backend, 5)
			})
			t.Run("C3_ConditionalCapacityPprof/"+mode+"/"+bName, func(t *testing.T) {
				runR17C3ConditionalCapacityReasonPprofCase(t, sharded, backend)
			})
			t.Run("C3_ChildOnlyPatterns/"+mode+"/"+bName, func(t *testing.T) {
				for pattern := range 3 {
					runR17C3ChildOnlyPatternsCase(t, sharded, backend, pattern)
				}
			})
			t.Run("C3_BlackBoxDetachedTimerPanic/"+mode+"/"+bName, func(t *testing.T) {
				runR17BlackBoxDetachedTimerPanicCase(t, sharded, backend)
			})
			t.Run("FastEvictTagLeaseStealAndOverflow/"+mode+"/"+bName, func(t *testing.T) {
				runR17FastEvictTagLeaseStealAndOverflowCase(t, sharded, backend)
			})
		}
	}
}

//go:noinline
func spawnR18PeerViaReturnedHelper(fn func()) {
	go fn()
}

func runR18C1ChildPprofDelete(c Cache[int], clearLabels bool) {
	if clearLabels {
		pprof.SetGoroutineLabels(context.Background())
		_, _ = c.Delete("s1/victim")
		return
	}
	pprof.Do(context.Background(), pprof.Labels("r18", "awaited-child"), func(context.Context) {
		_, _ = c.Delete("s1/victim")
	})
}

func execR18C1SubtreeActivity(c Cache[int], pattern int, clearLabels bool) {
	switch pattern {
	case 0:
		stopSpin := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			for {
				select {
				case <-stopSpin:
					return
				default:
					runtime.Gosched()
				}
			}
		})
		wg.Go(func() {
			defer close(stopSpin)
			runR18C1ChildPprofDelete(c, clearLabels)
		})
		wg.Wait()
	case 1:
		var wg sync.WaitGroup
		wg.Go(func() {
			runR18C1ChildPprofDelete(c, clearLabels)
		})
		wg.Wait()
	case 2:
		var childDone atomic.Bool
		go func() {
			defer childDone.Store(true)
			runR18C1ChildPprofDelete(c, clearLabels)
		}()
		for !childDone.Load() {
			runtime.Gosched()
		}
	default:
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			runR18C1ChildPprofDelete(c, clearLabels)
			return
		}
		defer func() { _ = ln.Close() }()
		srvReady := make(chan net.Conn, 1)
		go func() {
			conn, aErr := ln.Accept()
			if aErr == nil {
				srvReady <- conn
			}
		}()
		clientConn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			runR18C1ChildPprofDelete(c, clearLabels)
			return
		}
		defer func() { _ = clientConn.Close() }()
		srvConn := <-srvReady
		defer func() { _ = srvConn.Close() }()
		ioEntered := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			close(ioEntered)
			var buf [1]byte
			_, _ = clientConn.Read(buf[:])
		})
		wg.Go(func() {
			<-ioEntered
			time.Sleep(5 * time.Millisecond)
			runR18C1ChildPprofDelete(c, clearLabels)
			_, _ = srvConn.Write([]byte{1})
		})
		wg.Wait()
	}
}

func runR18C1AwaitedChildPprofActivityCase(t *testing.T, sharded bool, backend Backend, pattern int, clearLabels bool) {
	t.Helper()
	var c Cache[int]
	var armed atomic.Bool
	var victimDeleted atomic.Int32

	c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 8,
		WithOnEvictEntry(func(k string, _ int, r EvictionReason) {
			if k == "s1/victim" && r == EvictionReasonDeleted {
				victimDeleted.Add(1)
				return
			}
			if !armed.Load() || k != "s0/trigger" || r != EvictionReasonReplaced {
				return
			}
			execR18C1SubtreeActivity(c, pattern, clearLabels)
		}),
	)

	warmupUncontendedReplaceEvictions(c, 8)
	_, _ = c.Put("s0/trigger", 0)
	_ = c.Replace("s0/trigger", 1)
	_, _ = c.Put("s1/victim", 2)
	require.True(t, coordPressureState(c).evictFastMode.Load())

	stopBg := make(chan struct{})
	var bgWG sync.WaitGroup
	if pattern == 1 {
		bgReady := make(chan struct{})
		bgWG.Go(func() {
			close(bgReady)
			for {
				select {
				case <-stopBg:
					return
				default:
					runtime.Gosched()
				}
			}
		})
		<-bgReady
	}
	defer func() {
		close(stopBg)
		bgWG.Wait()
	}()

	armed.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.NoError(t, c.Replace("s0/trigger", 99))
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("deadlocked in R18 C1 sharded=%v backend=%s pattern=%d clearLabels=%v", sharded, backend, pattern, clearLabels)
	}
	assert.Equal(t, int32(1), victimDeleted.Load())
}

func runR18C2PeerIsolationCase(t *testing.T, sharded bool, backend Backend, useHelper, panicPeer bool) {
	t.Helper()
	var c Cache[int]
	var armed, fgHolding, peerReturnedEarly, peerEvictRan atomic.Bool
	fgEntered := make(chan struct{})
	peerEntering := make(chan struct{})
	peerDone := make(chan struct{})
	var peerPanicVal atomic.Value

	c = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 8,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armed.Load() {
				return
			}
			if k == "s1/peer" {
				peerEvictRan.Store(true)
				if panicPeer {
					panic("r18-peer-panic")
				}
				return
			}
			if k != "s0/fg" {
				return
			}
			fgHolding.Store(true)
			defer fgHolding.Store(false)
			close(fgEntered)
			<-peerEntering
			var blockMu sync.Mutex
			blockMu.Lock()
			time.AfterFunc(45*time.Millisecond, blockMu.Unlock)
			blockMu.Lock()
			defer blockMu.Unlock()
			if !panicPeer {
				panic("r18-fg-panic")
			}
		}),
	)

	warmupUncontendedReplaceEvictions(c, 8)
	_, _ = c.Put("s0/fg", 0)
	_ = c.Replace("s0/fg", 1)
	_, _ = c.Put("s1/peer", 2)
	require.True(t, coordPressureState(c).evictFastMode.Load())
	armed.Store(true)

	peerFn := func() {
		defer close(peerDone)
		defer func() {
			if r := recover(); r != nil {
				peerPanicVal.Store(r)
			}
		}()
		<-fgEntered
		pprof.Do(context.Background(), pprof.Labels("r18", "peer"), func(context.Context) {
			close(peerEntering)
			_, _ = c.Delete("s1/peer")
			peerReturnedEarly.Store(fgHolding.Load())
		})
	}
	var outerWG sync.WaitGroup
	if useHelper {
		spawnR18PeerViaReturnedHelper(peerFn)
	} else {
		outerWG.Go(peerFn)
	}

	if panicPeer {
		require.NoError(t, c.Replace("s0/fg", 10))
	} else {
		assert.PanicsWithValue(t, "r18-fg-panic", func() {
			_ = c.Replace("s0/fg", 10)
		})
	}
	<-peerDone
	outerWG.Wait()
	assert.False(t, peerReturnedEarly.Load(), "peer must not return while foreground holds OnEvict")
	assert.True(t, peerEvictRan.Load(), "peer OnEvict callback must not be dropped")
	if panicPeer {
		assert.Equal(t, "r18-peer-panic", peerPanicVal.Load())
	} else {
		assert.Nil(t, peerPanicVal.Load())
	}
}

func runR18C3EphemeralCacheGCAndRingRotationTagCase(t *testing.T, sharded bool, backend Backend) {
	t.Helper()
	bgReady := make(chan unsafe.Pointer, 1)
	startBgDelete := make(chan struct{})
	bgEntering := make(chan struct{})
	bgDone := make(chan struct{})

	var c2 Cache[int]
	var armed2, fg2Holding, bgReturnedEarly, bgEvicted atomic.Bool

	c2 = makeShardedOrUnshardedIntCacheWithBackend(sharded, backend, 8,
		WithOnEvictEntry(func(k string, _ int, _ EvictionReason) {
			if !armed2.Load() {
				return
			}
			if k == "s1/bg-victim" {
				bgEvicted.Store(true)
				return
			}
			if k != "s0/fg" {
				return
			}
			fg2Holding.Store(true)
			defer fg2Holding.Store(false)
			close(startBgDelete)
			<-bgEntering
			var blockMu sync.Mutex
			blockMu.Lock()
			time.AfterFunc(45*time.Millisecond, blockMu.Unlock)
			blockMu.Lock()
			defer blockMu.Unlock()
			panic("r18-c2-fg-panic")
		}),
	)

	warmupUncontendedReplaceEvictions(c2, 8)
	_, _ = c2.Put("s0/fg", 0)
	_ = c2.Replace("s0/fg", 1)
	_, _ = c2.Put("s1/bg-victim", 2)
	require.True(t, coordPressureState(c2).evictFastMode.Load())

	func() {
		var spawned atomic.Bool
		c1 := New[int](4, WithOnEvictEntry(func(_ string, _ int, _ EvictionReason) {
			if spawned.CompareAndSwap(false, true) {
				go func() {
					defer close(bgDone)
					bgReady <- runtimeGetProfLabel()
					<-startBgDelete
					close(bgEntering)
					_, _ = c2.Delete("s1/bg-victim")
					bgReturnedEarly.Store(fg2Holding.Load())
				}()
			}
		}))
		warmupUncontendedReplaceEvictions(c1, 6)
		_, _ = c1.Put("k", 1)
		_ = c1.Replace("k", 2)
	}()

	bgTag := <-bgReady
	require.NotNil(t, bgTag)
	for range 4 {
		runtime.GC()
		time.Sleep(2 * time.Millisecond)
	}
	assert.True(t, isAnyEvictRingTag(bgTag), "foreign ring tag held by live goroutine must remain recognized after C1 GC")

	armed2.Store(true)
	assert.PanicsWithValue(t, "r18-c2-fg-panic", func() {
		_ = c2.Replace("s0/fg", 10)
	})
	<-bgDone
	assert.False(t, bgReturnedEarly.Load(), "background goroutine carrying C1 ring tag must not return during C2 OnEvict")
	assert.True(t, bgEvicted.Load(), "background goroutine OnEvict callback on C2 must not be dropped")
	assert.False(t, coordPressureState(c2).evictLabelTampered.Load(), "foreign ring tag must not latch evictLabelTampered on C2")
}

func TestShardedCache_R18EvictFastModePprofAndGlobalTagLifecycle(t *testing.T) {
	for _, sharded := range []bool{false, true} {
		mode := "Unsharded"
		if sharded {
			mode = "Sharded"
		}
		for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			bName := backend.String()
			t.Run("C1_AwaitedChildPprofActivity/"+mode+"/"+bName, func(t *testing.T) {
				for pattern := range 4 {
					runR18C1AwaitedChildPprofActivityCase(t, sharded, backend, pattern, false)
					runR18C1AwaitedChildPprofActivityCase(t, sharded, backend, pattern, true)
				}
			})
			t.Run("C2_PeerIsolation/"+mode+"/"+bName, func(t *testing.T) {
				runR18C2PeerIsolationCase(t, sharded, backend, false, false)
				runR18C2PeerIsolationCase(t, sharded, backend, true, false)
				runR18C2PeerIsolationCase(t, sharded, backend, true, true)
			})
			t.Run("C3_EphemeralCacheGCTagLifecycle/"+mode+"/"+bName, func(t *testing.T) {
				runR18C3EphemeralCacheGCAndRingRotationTagCase(t, sharded, backend)
			})
		}
	}
}

func holdShardRLockViaAll[V any](t *testing.T, shard shardEngine[V]) func() {
	t.Helper()
	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range shard.All() {
			close(locked)
			<-release
			break
		}
	}()
	<-locked
	return func() {
		close(release)
		<-done
	}
}

func runR19C1CrossShardSpikeCase(t *testing.T, backend Backend, useEvaluate bool, initialTier1, forceRefresh bool) {
	t.Helper()
	var pressureBits atomic.Uint64
	pressureBits.Store(math.Float64bits(0.10))
	if initialTier1 {
		pressureBits.Store(math.Float64bits(0.80))
	}

	c := NewShardedCache[int](90, 3, explicitTagShardFunc,
		WithBackend(backend),
		WithWeigher(func(key string, _ int) uint64 {
			if strings.HasSuffix(key, "/match") {
				return 0
			}
			return 1
		}),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			return math.Float64frombits(pressureBits.Load())
		}),
	)
	pa := c.(PressureAwareCache[int])

	// Populate shard 2 above its Tier 2 target (target = 15) and create compaction slack.
	for i := range 30 {
		_, err := c.Put(fmt.Sprintf("s2/k%02d", i), i)
		require.NoError(t, err)
	}
	for i := range 6 {
		_, _ = c.Delete(fmt.Sprintf("s2/k%02d", i))
	}
	_, err := c.Put("s2/match", 1)
	require.NoError(t, err)
	// Shard 2 now has 24 weighted entries (> target 15), 1 zero-weight match key, and deletion slack.
	_, err = c.Put("s0/hold", 1)
	require.NoError(t, err)
	_, err = c.Put("s1/hold", 1)
	require.NoError(t, err)

	sc := c.(*shardedCache[int])
	s0PS := sc.shards[0].pressureStatePtr()
	var baselineS0Seq uint64
	for range sc.shards[0].All() {
		baselineS0Seq = s0PS.sampledPressureSeq
		break
	}
	unlockS1 := holdShardRLockViaAll(t, sc.shards[1])

	opDone := make(chan struct{})
	go func() {
		defer close(opDone)
		if useEvaluate {
			_ = pa.EvaluateMemoryPressure()
		} else {
			c.DeletePrefix("s2/match")
		}
	}()

	// Wait until Shard 0 has applied the initial sample and released s0.mu.Lock(), blocking on Shard 1.
	require.Eventually(t, func() bool {
		var seq uint64
		for range sc.shards[0].All() {
			seq = s0PS.sampledPressureSeq
			break
		}
		return seq > baselineS0Seq
	}, 2*time.Second, time.Millisecond)

	// Spike pressure to Tier 2 (0.95) and run a non-reclaiming operation on Shard 2.
	pressureBits.Store(math.Float64bits(0.95))
	_, _ = c.Delete("s2/absent")
	if forceRefresh {
		sc.coord.pressureNeedsRefresh.Store(true)
	}

	unlockS1()
	<-opDone

	s2Stats := sc.shards[2].Stats()
	assert.LessOrEqual(t, s2Stats.CurrentSize, uint64(15),
		"Shard 2 must shed down to its Tier 2 retention target (<= 15) after observing a newer Tier 2 spike")
	assert.Positive(t, s2Stats.EvictionsPressure,
		"Shard 2 must record Tier 2 pressure evictions")
}

func runR19C2OutOfOrderLockAcquisitionCase(t *testing.T, backend Backend, initialTier1 bool) {
	t.Helper()
	var (
		pressureBits atomic.Uint64
		probeCount   atomic.Int32
	)
	initialVal := 0.10
	if initialTier1 {
		initialVal = 0.80
	}
	pressureBits.Store(math.Float64bits(initialVal))

	c := NewShardedCache[int](60, 2, explicitTagShardFunc,
		WithBackend(backend),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			probeCount.Add(1)
			return math.Float64frombits(pressureBits.Load())
		}),
	)
	pa := c.(PressureAwareCache[int])

	for i := range 24 {
		_, err := c.Put(fmt.Sprintf("s0/k%02d", i), i)
		require.NoError(t, err)
		_, err = c.Put(fmt.Sprintf("s1/k%02d", i), i)
		require.NoError(t, err)
	}

	sc := c.(*shardedCache[int])
	s0PS := sc.shards[0].pressureStatePtr()

	// Hold s0 RLock so G1 finishes sampleForLock on s0 (seq = N) and waits before acquiring s0.mu.Lock().
	probeBefore := probeCount.Load()
	unlockS0 := holdShardRLockViaAll(t, sc.shards[0])
	evalDone := make(chan struct{})
	go func() {
		defer close(evalDone)
		_ = pa.EvaluateMemoryPressure()
	}()

	require.Eventually(t, func() bool {
		return probeCount.Load() == probeBefore+1 && !sc.coord.samplingPressure.Load()
	}, 2*time.Second, time.Millisecond)

	// Simulate G2 having sampled a newer Tier 2 spike (0.95, seq = N+1) and acquired s0.mu.Lock() before G1.
	newSeq := sc.coord.pressureInvokeSeq.Add(1)
	sc.coord.lastSampledSeq.Store(newSeq)
	sc.coord.lastSampledPressureBits.Store(math.Float64bits(0.95))
	sc.coord.cachedPressureBits.Store(math.Float64bits(0.95))
	sc.coord.cachedPressureSeq.Store(newSeq)
	sc.coord.cachedPressureEpoch.Store(sc.coord.reclaimEpoch.Load())
	sc.coord.pressureInitialized.Store(true)
	sc.coord.pressureNeedsRefresh.Store(false)
	s0PS.sampledPressureSeq = newSeq
	pressureBits.Store(math.Float64bits(0.95))

	unlockS0()
	<-evalDone

	assert.LessOrEqual(t, sc.shards[0].Stats().CurrentSize, uint64(15),
		"Shard 0 must adopt the newer Tier 2 sample and shed to <= 15")
	assert.LessOrEqual(t, sc.shards[1].Stats().CurrentSize, uint64(15),
		"Shard 1 must reuse the adopted Tier 2 sample and shed to <= 15")
	assert.Equal(t, probeBefore+1, probeCount.Load(),
		"Shard 1 must reuse the adopted valid sample without invoking PressureFunc a second time")
}

func runR19C2ReentrantEpochFastForwardCase(t *testing.T, backend Backend) {
	t.Helper()
	var (
		c          Cache[int]
		armed      atomic.Bool
		evalProbes atomic.Int32
	)
	c = NewShardedCache[int](60, 2, explicitTagShardFunc,
		WithBackend(backend),
		WithCompactionThreshold(0.70),
		WithEvictionThreshold(0.90),
		WithEvictionRetentionRatio(0.50),
		WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.10
			}
			n := evalProbes.Add(1)
			if n == 1 {
				// Simulate a concurrent non-reclaiming operation on s0 having recorded a higher invokeSeq
				// at originalEpoch before this sampler performs a sole re-entrant reclamation on s1.
				sc := c.(*shardedCache[int])
				s0PS := sc.shards[0].pressureStatePtr()
				higherSeq := sc.coord.pressureInvokeSeq.Add(1)
				sc.coord.lastSampledSeq.Store(higherSeq)
				sc.coord.lastSampledEpoch = sc.coord.reclaimEpoch.Load()
				sc.coord.lastSampledPressureBits.Store(math.Float64bits(0.10))
				s0PS.sampledPressureSeq = higherSeq

				// Re-entrantly reclaim on s1 so coord.reclaimEpoch advances via sole re-entrant reclamation.
				_, _ = c.Delete("s1/reentrant-victim")
			}
			return 0.95
		}),
	)
	pa := c.(PressureAwareCache[int])

	for i := range 24 {
		_, err := c.Put(fmt.Sprintf("s0/k%02d", i), i)
		require.NoError(t, err)
		_, err = c.Put(fmt.Sprintf("s1/k%02d", i), i)
		require.NoError(t, err)
	}
	_, err := c.Put("s1/reentrant-victim", 99)
	require.NoError(t, err)

	armed.Store(true)
	evicted := pa.EvaluateMemoryPressure()
	assert.NotEmpty(t, evicted)

	sc := c.(*shardedCache[int])
	assert.LessOrEqual(t, sc.shards[0].Stats().CurrentSize, uint64(15),
		"Shard 0 must shed under Tier 2 after re-entrant epoch fast-forward")
	assert.LessOrEqual(t, sc.shards[1].Stats().CurrentSize, uint64(15),
		"Shard 1 must shed under Tier 2 after re-entrant epoch fast-forward")
	assert.Equal(t, int32(1), evalProbes.Load(),
		"EvaluateMemoryPressure must reuse the fast-forwarded valid sample on Shard 1 without a second probe")
}

func TestShardedCache_R19CrossShardAndOutOfOrderSampleSeqOrdering(t *testing.T) {
	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		bName := backend.String()
		t.Run("C1_CrossShardSpike/"+bName, func(t *testing.T) {
			for _, useEval := range []bool{true, false} {
				for _, tier1 := range []bool{false, true} {
					for _, forceRefresh := range []bool{false, true} {
						runR19C1CrossShardSpikeCase(t, backend, useEval, tier1, forceRefresh)
					}
				}
			}
		})
		t.Run("C2_OutOfOrderLock/"+bName, func(t *testing.T) {
			runR19C2OutOfOrderLockAcquisitionCase(t, backend, false)
			runR19C2OutOfOrderLockAcquisitionCase(t, backend, true)
		})
		t.Run("C2_ReentrantEpochFastForward/"+bName, func(t *testing.T) {
			runR19C2ReentrantEpochFastForwardCase(t, backend)
		})
	}
}

func BenchmarkShardedParallelThroughput(b *testing.B) {
	const capTotal = 16384
	keys := make([]string, capTotal*2)
	for i := range keys {
		keys[i] = fmt.Sprintf("tenant_%02d/obj_%06d", i%32, i)
	}

	configs := []struct {
		name   string
		shards int
	}{
		{"Unsharded", 0},
		{"Shards_16", 16},
		{"Shards_64", 64},
	}

	for _, backend := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		for _, cfg := range configs {
			b.Run(fmt.Sprintf("%s/%s", backend.String(), cfg.name), func(b *testing.B) {
				var c Cache[int]
				if cfg.shards == 0 {
					c = New[int](capTotal, WithBackend(backend))
				} else {
					c = NewShardedCache[int](capTotal, cfg.shards, nil, WithBackend(backend))
				}
				for i := range capTotal {
					_, _ = c.Put(keys[i], i)
				}
				var counter atomic.Uint64
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					n := counter.Add(1)
					step := n * 1315423911
					for pb.Next() {
						step++
						k := keys[step%uint64(len(keys))]
						if step%8 == 0 {
							_, _ = c.Put(k, int(step))
						} else {
							_, _ = c.Get(k)
						}
					}
				})
			})
		}
	}
}
