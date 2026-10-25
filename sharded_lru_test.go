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
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	return func() {
		close(release)
		<-done
	}
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
			putWG.Add(1)
			go func(idx int) {
				defer putWG.Done()
				_, _ = c.Put(fmt.Sprintf("s3/p_%d", idx), idx)
			}(i)
		}
		require.Eventually(t, func() bool {
			return coord.belowTier2Epoch.Load() >= baseBelowTier2+7
		}, time.Second, time.Millisecond)
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
