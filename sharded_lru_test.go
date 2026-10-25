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
	switch {
	case strings.HasPrefix(key, "s0/"):
		return 0 % numShards
	case strings.HasPrefix(key, "s1/"):
		return 1 % numShards
	case strings.HasPrefix(key, "s2/"):
		return 2 % numShards
	default:
		return DefaultShardFunc(key, numShards)
	}
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
