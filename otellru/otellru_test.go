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

package otellru_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-lru"
	"github.com/google/go-lru/otellru"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type weightedItem struct {
	id     int
	weight uint64
}

func itemWeigher(_ string, v weightedItem) uint64 {
	return v.weight
}

func collectMetricsByName(t *testing.T, reader *sdkmetric.ManualReader, expectedScope string) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		if expectedScope != "" {
			assert.Equal(t, expectedScope, sm.Scope.Name)
		}
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func requireInt64SumPoints(t *testing.T, m metricdata.Metrics, wantUnit string) map[attribute.Set]int64 {
	t.Helper()
	assert.Equal(t, wantUnit, m.Unit)
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok, "metric %s must be metricdata.Sum[int64]", m.Name)
	assert.True(t, sum.IsMonotonic, "counter %s must be monotonic", m.Name)
	assert.Equal(t, metricdata.CumulativeTemporality, sum.Temporality)

	pts := make(map[attribute.Set]int64, len(sum.DataPoints))
	for _, dp := range sum.DataPoints {
		pts[dp.Attributes] = dp.Value
	}
	return pts
}

func requireInt64GaugePoints(t *testing.T, m metricdata.Metrics, wantUnit string) map[attribute.Set]int64 {
	t.Helper()
	assert.Equal(t, wantUnit, m.Unit)
	gauge, ok := m.Data.(metricdata.Gauge[int64])
	require.True(t, ok, "metric %s must be metricdata.Gauge[int64]", m.Name)

	pts := make(map[attribute.Set]int64, len(gauge.DataPoints))
	for _, dp := range gauge.DataPoints {
		pts[dp.Attributes] = dp.Value
	}
	return pts
}

func requireFloat64GaugePoints(t *testing.T, m metricdata.Metrics, wantUnit string) map[attribute.Set]float64 {
	t.Helper()
	assert.Equal(t, wantUnit, m.Unit)
	gauge, ok := m.Data.(metricdata.Gauge[float64])
	require.True(t, ok, "metric %s must be metricdata.Gauge[float64]", m.Name)

	pts := make(map[attribute.Set]float64, len(gauge.DataPoints))
	for _, dp := range gauge.DataPoints {
		pts[dp.Attributes] = dp.Value
	}
	return pts
}

func TestRegister_AllBackendsAndInstruments(t *testing.T) {
	backends := []struct {
		name        string
		backend     lru.Backend
		backendAttr string
		isArena     bool
	}{
		{name: "MapCache", backend: lru.BackendMap, backendAttr: "map", isArena: false},
		{name: "RadixCache", backend: lru.BackendRadix, backendAttr: "radix", isArena: false},
		{name: "ArenaRadixCache", backend: lru.BackendArenaRadix, backendAttr: "arena_radix", isArena: true},
	}

	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			// Arrange: Set up ManualReader, MeterProvider, and pressure-aware cache.
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() {
				_ = mp.Shutdown(context.Background())
			})

			var pressureBits atomic.Uint64
			setPressure := func(p float64) {
				pressureBits.Store(math.Float64bits(p))
			}
			setPressure(0.10)

			cache := lru.New[weightedItem](
				100,
				lru.WithBackend(b.backend),
				lru.WithWeigher(itemWeigher),
				lru.WithInvariantChecking(true),
				lru.WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
				lru.WithCompactionThreshold(0.75),
				lru.WithEvictionThreshold(0.90),
				lru.WithEvictionRetentionRatio(0.50),
			)
			paCache, ok := cache.(lru.PressureAwareCache[weightedItem])
			require.True(t, ok)

			reg, err := otellru.Register(
				cache,
				otellru.WithMeterProvider(mp),
				otellru.WithName("primary-cache"),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = reg.Unregister()
			})

			// Act: Execute a comprehensive sequence covering all lookup, mutation, eviction,
			// compaction, and memory-pressure shedding paths.
			// 1. Misses on empty cache
			_, ok = cache.Get("dir/missing")
			assert.False(t, ok)
			_, ok = cache.Peek("dir/missing")
			assert.False(t, ok)

			// 2. Put insertions (including a zero-weight entry)
			_, err = cache.Put("dir/a", weightedItem{id: 1, weight: 30})
			require.NoError(t, err)
			_, err = cache.Put("dir/b", weightedItem{id: 2, weight: 30})
			require.NoError(t, err)
			_, err = cache.Put("dir/zero", weightedItem{id: 3, weight: 0})
			require.NoError(t, err)

			// 3. Get & Peek hits
			_, ok = cache.Get("dir/a")
			assert.True(t, ok)
			_, ok = cache.Peek("dir/b")
			assert.True(t, ok)

			// 4. Put update (Replaced) and Put rejected oversized
			_, err = cache.Put("dir/a", weightedItem{id: 10, weight: 40})
			require.NoError(t, err)
			_, err = cache.Put("dir/oversized", weightedItem{id: 99, weight: 101})
			require.ErrorIs(t, err, lru.ErrInvalidEntrySize)

			// 5. Replace update (Replaced), Replace not found, and Replace self-evicted (Capacity)
			require.NoError(t, cache.Replace("dir/b", weightedItem{id: 20, weight: 35}))
			require.ErrorIs(t, cache.Replace("dir/absent", weightedItem{id: 0, weight: 10}), lru.ErrEntryNotExist)
			require.NoError(t, cache.Replace("dir/b", weightedItem{id: 21, weight: 120})) // > maxSize -> self-evicts dir/b

			// 6. Capacity eviction via Put ("dir/a" is 40B; inserting 70B evicts "dir/a")
			_, err = cache.Put("dir/c", weightedItem{id: 4, weight: 70})
			require.NoError(t, err)

			// 7. Delete hit and miss, plus DeletePrefix
			_, ok = cache.Delete("dir/nonexistent")
			assert.False(t, ok)
			_, err = cache.Put("purge/x", weightedItem{id: 5, weight: 10})
			require.NoError(t, err)
			_, err = cache.Put("purge/y", weightedItem{id: 6, weight: 10})
			require.NoError(t, err)
			_, ok = cache.Delete("purge/x")
			assert.True(t, ok)
			cache.DeletePrefix("purge/")

			// 8. Explicit compaction (cache is dirty from prior deletions)
			paCache.Compact()

			// 9. Tier 1 pressure compaction: dirty the cache, then raise pressure to 0.80
			_, err = cache.Put("temp/1", weightedItem{id: 7, weight: 10})
			require.NoError(t, err)
			_, ok = cache.Delete("temp/1")
			assert.True(t, ok)
			setPressure(0.80)
			evicted := paCache.EvaluateMemoryPressure()
			assert.Empty(t, evicted)

			// 10. Tier 2 explicit pressure shed + compaction:
			// Current weight is 70B ("dir/c"). At pressure 0.95, target is 50B -> sheds "dir/c".
			setPressure(0.95)
			evicted = paCache.EvaluateMemoryPressure()
			assert.Len(t, evicted, 1)

			// 11. Tier 2 inline pressure shed during foreground Put:
			setPressure(0.10)
			_, err = cache.Put("inline/1", weightedItem{id: 8, weight: 30})
			require.NoError(t, err)
			_, err = cache.Put("inline/2", weightedItem{id: 9, weight: 30})
			require.NoError(t, err)
			setPressure(0.95)
			_, err = cache.Put("inline/3", weightedItem{id: 10, weight: 20}) // 30+30+20=80 > 50 -> sheds inline/1
			require.NoError(t, err)

			// 12. Auto-slack inline compaction at normal pressure (peakEntryLen > 8 -> empty reset),
			// followed by live and deleted entries so all gauges and counters are non-zero:
			setPressure(0.10)
			for i := range 9 {
				_, err = cache.Put(fmt.Sprintf("slack/%d", i), weightedItem{id: 100 + i, weight: 1})
				require.NoError(t, err)
			}
			cache.DeletePrefix("")
			_, err = cache.Put("live/a", weightedItem{id: 201, weight: 25})
			require.NoError(t, err)
			_, err = cache.Put("live/zero", weightedItem{id: 202, weight: 0})
			require.NoError(t, err)
			_, err = cache.Put("live/temp", weightedItem{id: 203, weight: 10})
			require.NoError(t, err)
			_, ok = cache.Delete("live/temp")
			assert.True(t, ok)

			// Assert: Collect OTel metrics and compare every instrument against cache.Stats().
			st := cache.Stats()
			assert.Positive(t, st.CompactionsAutoSlack)
			metricsByName := collectMetricsByName(t, reader, otellru.ScopeName)

			if b.isArena {
				assert.Len(t, metricsByName, 16)
			} else {
				assert.Len(t, metricsByName, 14)
				assert.NotContains(t, metricsByName, "lru.cache.arena.nodes")
				assert.NotContains(t, metricsByName, "lru.cache.arena.hash_fallbacks")
			}

			baseAttrs := []attribute.KeyValue{
				attribute.String("cache.backend", b.backendAttr),
				attribute.String("cache.name", "primary-cache"),
			}
			setWith := func(extra ...attribute.KeyValue) attribute.Set {
				all := make([]attribute.KeyValue, 0, len(baseAttrs)+len(extra))
				all = append(all, baseAttrs...)
				all = append(all, extra...)
				return attribute.NewSet(all...)
			}
			baseSet := setWith()

			// 1. lru.cache.requests
			reqPts := requireInt64SumPoints(t, metricsByName["lru.cache.requests"], "{request}")
			assert.Len(t, reqPts, 4)
			assert.Equal(t, int64(st.GetHits), reqPts[setWith(attribute.String("operation", "get"), attribute.String("result", "hit"))])
			assert.Equal(t, int64(st.GetMisses), reqPts[setWith(attribute.String("operation", "get"), attribute.String("result", "miss"))])
			assert.Equal(t, int64(st.PeekHits), reqPts[setWith(attribute.String("operation", "peek"), attribute.String("result", "hit"))])
			assert.Equal(t, int64(st.PeekMisses), reqPts[setWith(attribute.String("operation", "peek"), attribute.String("result", "miss"))])

			// 2. lru.cache.evictions
			evictPts := requireInt64SumPoints(t, metricsByName["lru.cache.evictions"], "{entry}")
			assert.Len(t, evictPts, 4)
			assert.Equal(t, int64(st.EvictionsCapacity), evictPts[setWith(attribute.String("reason", "capacity"))])
			assert.Equal(t, int64(st.EvictionsPressure), evictPts[setWith(attribute.String("reason", "pressure"))])
			assert.Equal(t, int64(st.EvictionsDeleted), evictPts[setWith(attribute.String("reason", "deleted"))])
			assert.Equal(t, int64(st.EvictionsReplaced), evictPts[setWith(attribute.String("reason", "replaced"))])

			// 3. lru.cache.evicted_weight
			weightPts := requireInt64SumPoints(t, metricsByName["lru.cache.evicted_weight"], "{weight}")
			assert.Len(t, weightPts, 4)
			assert.Equal(t, int64(st.EvictedWeightCapacity), weightPts[setWith(attribute.String("reason", "capacity"))])
			assert.Equal(t, int64(st.EvictedWeightPressure), weightPts[setWith(attribute.String("reason", "pressure"))])
			assert.Equal(t, int64(st.EvictedWeightDeleted), weightPts[setWith(attribute.String("reason", "deleted"))])
			assert.Equal(t, int64(st.EvictedWeightReplaced), weightPts[setWith(attribute.String("reason", "replaced"))])

			// 4–7. Capacity & entry gauges
			sizePts := requireInt64GaugePoints(t, metricsByName["lru.cache.size"], "{weight}")
			assert.Equal(t, int64(st.CurrentSize), sizePts[baseSet])

			maxSizePts := requireInt64GaugePoints(t, metricsByName["lru.cache.max_size"], "{weight}")
			assert.Equal(t, int64(st.MaxSize), maxSizePts[baseSet])

			entriesPts := requireInt64GaugePoints(t, metricsByName["lru.cache.entries"], "{entry}")
			assert.Equal(t, int64(st.Len), entriesPts[baseSet])

			zeroWeightPts := requireInt64GaugePoints(t, metricsByName["lru.cache.zero_weight_entries"], "{entry}")
			assert.Equal(t, int64(st.ZeroSizeCount), zeroWeightPts[baseSet])

			// 8. lru.cache.mutations
			mutPts := requireInt64SumPoints(t, metricsByName["lru.cache.mutations"], "{operation}")
			assert.Len(t, mutPts, 9)
			assert.Equal(t, int64(st.PutInserted), mutPts[setWith(attribute.String("operation", "put"), attribute.String("outcome", "inserted"))])
			assert.Equal(t, int64(st.PutUpdated), mutPts[setWith(attribute.String("operation", "put"), attribute.String("outcome", "updated"))])
			assert.Equal(t, int64(st.PutRejectedOversized), mutPts[setWith(attribute.String("operation", "put"), attribute.String("outcome", "rejected_oversized"))])
			assert.Equal(t, int64(st.ReplaceUpdated), mutPts[setWith(attribute.String("operation", "replace"), attribute.String("outcome", "updated"))])
			assert.Equal(t, int64(st.ReplaceNotFound), mutPts[setWith(attribute.String("operation", "replace"), attribute.String("outcome", "not_found"))])
			assert.Equal(t, int64(st.ReplaceSelfEvicted), mutPts[setWith(attribute.String("operation", "replace"), attribute.String("outcome", "self_evicted"))])
			assert.Equal(t, int64(st.DeleteDeleted), mutPts[setWith(attribute.String("operation", "delete"), attribute.String("outcome", "deleted"))])
			assert.Equal(t, int64(st.DeleteNotFound), mutPts[setWith(attribute.String("operation", "delete"), attribute.String("outcome", "not_found"))])
			assert.Equal(t, int64(st.DeletePrefixExecuted), mutPts[setWith(attribute.String("operation", "delete_prefix"), attribute.String("outcome", "executed"))])

			// 9. lru.cache.memory_pressure
			pressurePts := requireFloat64GaugePoints(t, metricsByName["lru.cache.memory_pressure"], "1")
			assert.InDelta(t, st.MemoryPressure, pressurePts[baseSet], 1e-9)

			// 10. lru.cache.compactions
			compactPts := requireInt64SumPoints(t, metricsByName["lru.cache.compactions"], "{compaction}")
			assert.Len(t, compactPts, 4)
			assert.Equal(t, int64(st.CompactionsExplicit), compactPts[setWith(attribute.String("trigger", "explicit"))])
			assert.Equal(t, int64(st.CompactionsPressureTier1), compactPts[setWith(attribute.String("trigger", "pressure_tier1"))])
			assert.Equal(t, int64(st.CompactionsPressureTier2), compactPts[setWith(attribute.String("trigger", "pressure_tier2"))])
			assert.Equal(t, int64(st.CompactionsAutoSlack), compactPts[setWith(attribute.String("trigger", "auto_slack"))])

			// 11. lru.cache.pressure_sheds
			shedPts := requireInt64SumPoints(t, metricsByName["lru.cache.pressure_sheds"], "{event}")
			assert.Len(t, shedPts, 2)
			assert.Equal(t, int64(st.PressureShedsInline), shedPts[setWith(attribute.String("trigger", "inline"))])
			assert.Equal(t, int64(st.PressureShedsExplicit), shedPts[setWith(attribute.String("trigger", "explicit"))])

			// 12–14. Reclamation epoch & structural watermark gauges
			epochPts := requireInt64SumPoints(t, metricsByName["lru.cache.reclaim_epochs"], "{epoch}")
			assert.Equal(t, int64(st.ReclaimEpoch), epochPts[baseSet])

			delPts := requireInt64GaugePoints(t, metricsByName["lru.cache.deleted_since_compact"], "{entry}")
			assert.Equal(t, int64(st.DeletedSinceCompact), delPts[baseSet])

			peakPts := requireInt64GaugePoints(t, metricsByName["lru.cache.peak_entries"], "{entry}")
			assert.Equal(t, int64(st.PeakEntryLen), peakPts[baseSet])

			// 15–16. ArenaRadixCache-only instruments
			if b.isArena {
				nodePts := requireInt64GaugePoints(t, metricsByName["lru.cache.arena.nodes"], "{node}")
				assert.Len(t, nodePts, 3)
				assert.Equal(t, int64(st.ArenaLiveNodes), nodePts[setWith(attribute.String("state", "live"))])
				assert.Equal(t, int64(st.ArenaFreeNodes), nodePts[setWith(attribute.String("state", "free"))])
				assert.Equal(t, int64(st.ArenaUnallocatedCap), nodePts[setWith(attribute.String("state", "unallocated_cap"))])

				fallbackPts := requireInt64SumPoints(t, metricsByName["lru.cache.arena.hash_fallbacks"], "{lookup}")
				assert.Equal(t, int64(st.ArenaHashFallbacks), fallbackPts[baseSet])
			}
		})
	}
}

type stubStatsProvider struct {
	stats lru.Stats
}

func (s stubStatsProvider) Stats() lru.Stats {
	return s.stats
}

func TestRegister_OptionsAndAttributePrecedence(t *testing.T) {
	t.Run("NilCacheReturnsErrNilCache", func(t *testing.T) {
		// Arrange
		var typedNilCache lru.Cache[string]
		var typedNilPtr *stubStatsProvider

		// Act & Assert
		reg1, err1 := otellru.Register(nil)
		require.ErrorIs(t, err1, otellru.ErrNilCache)
		assert.Nil(t, reg1)

		reg2, err2 := otellru.Register(typedNilCache)
		require.ErrorIs(t, err2, otellru.ErrNilCache)
		assert.Nil(t, reg2)

		reg3, err3 := otellru.Register(typedNilPtr)
		require.ErrorIs(t, err3, otellru.ErrNilCache)
		assert.Nil(t, reg3)
	})

	t.Run("WithMeterAndCustomAttributesAndAuthoritativePrecedence", func(t *testing.T) {
		// Arrange
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		t.Cleanup(func() {
			_ = mp.Shutdown(context.Background())
		})

		cache := lru.NewMapCache[string](50)
		_, err := cache.Put("k1", "v1")
		require.NoError(t, err)

		customMeter := mp.Meter("custom.instrumentation.scope")
		reg, err := otellru.Register(
			cache,
			nil, // nil Option must be safely ignored
			otellru.WithMeterProvider(mp),
			otellru.WithMeter(customMeter), // takes precedence over WithMeterProvider
			otellru.WithName(""),           // empty name omits cache.name
			otellru.WithAttributes(
				attribute.String("service.env", "staging"),
				attribute.String("cache.backend", "spoofed-backend"),
				attribute.String("cache.name", "spoofed-name"),
				attribute.String("operation", "spoofed-op"),
				attribute.String("result", "spoofed-result"),
				attribute.String("reason", "spoofed-reason"),
				attribute.String("outcome", "spoofed-outcome"),
				attribute.String("trigger", "spoofed-trigger"),
				attribute.String("state", "spoofed-state"),
			),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = reg.Unregister()
		})

		// Act 1: Scrape with empty WithName("") and spoofed reserved attributes
		metricsByName := collectMetricsByName(t, reader, "custom.instrumentation.scope")

		// Assert 1: service.env="staging" is present, cache.backend="map" is authoritative,
		// cache.name is omitted, and reserved keys (reason, state, etc.) are stripped from baseOpts gauges.
		expectedSet := attribute.NewSet(
			attribute.String("service.env", "staging"),
			attribute.String("cache.backend", "map"),
		)
		entriesPts := requireInt64GaugePoints(t, metricsByName["lru.cache.entries"], "{entry}")
		assert.Len(t, entriesPts, 1)
		assert.Equal(t, int64(1), entriesPts[expectedSet])

		// Act 2: Unregister first registration and register with non-empty WithName("authoritative-name")
		// alongside spoofed cache.name in WithAttributes.
		require.NoError(t, reg.Unregister())

		namedReg, err := otellru.Register(
			cache,
			otellru.WithMeter(customMeter),
			otellru.WithName("authoritative-name"),
			otellru.WithAttributes(
				attribute.String("service.env", "staging"),
				attribute.String("cache.name", "spoofed-name"),
			),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = namedReg.Unregister()
		})

		namedMetricsByName := collectMetricsByName(t, reader, "custom.instrumentation.scope")

		// Assert 2: cache.name="authoritative-name" takes precedence over WithAttributes.
		expectedNamedSet := attribute.NewSet(
			attribute.String("service.env", "staging"),
			attribute.String("cache.backend", "map"),
			attribute.String("cache.name", "authoritative-name"),
		)
		namedEntriesPts := requireInt64GaugePoints(t, namedMetricsByName["lru.cache.entries"], "{entry}")
		assert.Len(t, namedEntriesPts, 1)
		assert.Equal(t, int64(1), namedEntriesPts[expectedNamedSet])
	})

	t.Run("DefaultGlobalMeterProviderAndUnknownBackendSaturation", func(t *testing.T) {
		// Arrange: stubStatsProvider with unknown Backend enum and MaxUint64 gauge values,
		// plus an ArenaRadix stubStatsProvider with non-zero CompactionsAutoSlack and ArenaHashFallbacks.
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		t.Cleanup(func() {
			_ = mp.Shutdown(context.Background())
		})

		stub := stubStatsProvider{
			stats: lru.Stats{
				Backend:      lru.Backend(99),
				GetHits:      42,
				CurrentSize:  math.MaxUint64,
				MaxSize:      math.MaxUint64,
				Len:          7,
				ReclaimEpoch: 9,
			},
		}

		// Register on global provider first (smoke check), then on ManualReader provider.
		globalReg, err := otellru.Register(stub)
		require.NoError(t, err)
		require.NoError(t, globalReg.Unregister())

		reg, err := otellru.Register(stub, otellru.WithMeterProvider(mp))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = reg.Unregister()
		})

		arenaStub := stubStatsProvider{
			stats: lru.Stats{
				Backend:              lru.BackendArenaRadix,
				CompactionsAutoSlack: 17,
				ArenaLiveNodes:       12,
				ArenaFreeNodes:       3,
				ArenaUnallocatedCap:  49,
				ArenaHashFallbacks:   29,
			},
		}
		arenaReg, err := otellru.Register(arenaStub, otellru.WithMeterProvider(mp), otellru.WithName("arena-stub"))
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = arenaReg.Unregister()
		})

		// Act
		metricsByName := collectMetricsByName(t, reader, otellru.ScopeName)

		// Assert: unknown backend maps to "unknown" and MaxUint64 clamps to MaxInt64.
		baseSet := attribute.NewSet(attribute.String("cache.backend", "unknown"))
		sizePts := requireInt64GaugePoints(t, metricsByName["lru.cache.size"], "{weight}")
		assert.Equal(t, int64(math.MaxInt64), sizePts[baseSet])

		maxSizePts := requireInt64GaugePoints(t, metricsByName["lru.cache.max_size"], "{weight}")
		assert.Equal(t, int64(math.MaxInt64), maxSizePts[baseSet])

		reqPts := requireInt64SumPoints(t, metricsByName["lru.cache.requests"], "{request}")
		hitSet := attribute.NewSet(
			attribute.String("cache.backend", "unknown"),
			attribute.String("operation", "get"),
			attribute.String("result", "hit"),
		)
		assert.Equal(t, int64(42), reqPts[hitSet])

		// Assert non-zero CompactionsAutoSlack and ArenaHashFallbacks on the ArenaRadix stub.
		arenaBaseSet := attribute.NewSet(
			attribute.String("cache.backend", "arena_radix"),
			attribute.String("cache.name", "arena-stub"),
		)
		autoSlackSet := attribute.NewSet(
			attribute.String("cache.backend", "arena_radix"),
			attribute.String("cache.name", "arena-stub"),
			attribute.String("trigger", "auto_slack"),
		)
		compactPts := requireInt64SumPoints(t, metricsByName["lru.cache.compactions"], "{compaction}")
		assert.Equal(t, int64(17), compactPts[autoSlackSet])

		fallbackPts := requireInt64SumPoints(t, metricsByName["lru.cache.arena.hash_fallbacks"], "{lookup}")
		assert.Equal(t, int64(29), fallbackPts[arenaBaseSet])
	})
}

func TestRegistration_UnregisterLifecycle(t *testing.T) {
	// Arrange
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
	})

	cache := lru.NewArenaRadixCache[string](100)
	_, err := cache.Put("key1", "val1")
	require.NoError(t, err)

	reg, err := otellru.Register(cache, otellru.WithMeterProvider(mp), otellru.WithName("ephemeral"))
	require.NoError(t, err)

	// Act 1: Collect while registered
	metricsBefore := collectMetricsByName(t, reader, otellru.ScopeName)
	assert.Len(t, metricsBefore, 16)

	// Act 2: Unregister and verify idempotency + nil receiver safety
	require.NoError(t, reg.Unregister())
	require.NoError(t, reg.Unregister())
	var nilReg *otellru.Registration
	require.NoError(t, nilReg.Unregister())

	// Mutate cache after Unregister and collect again
	_, err = cache.Put("key2", "val2")
	require.NoError(t, err)
	metricsAfter := collectMetricsByName(t, reader, "")

	// Assert: No metrics are collected after Unregister
	assert.Empty(t, metricsAfter)
}

type zeroAllocObserver struct {
	noop.Observer
	int64Calls   int
	float64Calls int
}

func (o *zeroAllocObserver) ObserveInt64(_ metric.Int64Observable, _ int64, _ ...metric.ObserveOption) {
	o.int64Calls++
}

func (o *zeroAllocObserver) ObserveFloat64(_ metric.Float64Observable, _ float64, _ ...metric.ObserveOption) {
	o.float64Calls++
}

type callbackCapturingMeter struct {
	noop.Meter
	cb metric.Callback
}

func (m *callbackCapturingMeter) RegisterCallback(cb metric.Callback, _ ...metric.Observable) (metric.Registration, error) {
	m.cb = cb
	return noop.Registration{}, nil
}

func TestRegister_ZeroAllocScrapeCallback(t *testing.T) {
	// Arrange: Register an ArenaRadixCache (which exercises all 16 instruments / 39 observations)
	// against a lightweight callback-capturing Meter to isolate scrape callback allocations.
	cache := lru.NewArenaRadixCache[string](100)
	_, err := cache.Put("dir/k1", "v1")
	require.NoError(t, err)
	_, _ = cache.Get("dir/k1")

	capturingMeter := &callbackCapturingMeter{}
	reg, err := otellru.Register(
		cache,
		otellru.WithMeter(capturingMeter),
		otellru.WithName("zero-alloc-cache"),
		otellru.WithAttributes(attribute.String("region", "us-east1")),
	)
	require.NoError(t, err)
	require.NotNil(t, capturingMeter.cb)
	t.Cleanup(func() {
		_ = reg.Unregister()
	})

	obs := &zeroAllocObserver{}
	ctx := context.Background()

	// Act: Measure allocations per scrape callback execution
	allocs := testing.AllocsPerRun(100, func() {
		obs.int64Calls = 0
		obs.float64Calls = 0
		if err := capturingMeter.cb(ctx, obs); err != nil {
			t.Fatal(err)
		}
	})

	// Assert: 0 heap allocations and all 39 series (38 int64 + 1 float64) observed
	assert.Zero(t, allocs, "otellru scrape callback must perform 0 heap allocations per collection cycle")
	assert.Equal(t, 38, obs.int64Calls)
	assert.Equal(t, 1, obs.float64Calls)
}

func TestRegister_ConcurrentOperationsAndScrapesUnderRace(t *testing.T) {
	// Arrange
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
	})

	cache := lru.NewArenaRadixCache[weightedItem](
		50,
		lru.WithWeigher(itemWeigher),
		lru.WithPressureFunc(func() float64 { return 0.80 }),
		lru.WithCompactionThreshold(0.75),
		lru.WithEvictionThreshold(0.90),
	)
	paCache, ok := cache.(lru.PressureAwareCache[weightedItem])
	require.True(t, ok)

	reg, err := otellru.Register(cache, otellru.WithMeterProvider(mp), otellru.WithName("race-cache"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = reg.Unregister()
	})

	// Act: Run concurrent mutators, readers, and ManualReader scrapes via wg.Go
	var wg sync.WaitGroup
	for workerID := range 6 {
		wg.Go(func() {
			keys := []string{"a/1", "a/2", "b/1", "b/2", "c/1"}
			for i := range 80 {
				k := keys[(workerID+i)%len(keys)]
				_, _ = cache.Put(k, weightedItem{id: i, weight: 15})
				_, _ = cache.Get(k)
				_, _ = cache.Peek(k)
				_ = cache.Replace(k, weightedItem{id: i + 1, weight: 20})
				if i%10 == 0 {
					_, _ = cache.Delete(k)
				}
				if i%25 == 0 {
					cache.DeletePrefix("a/")
				}
				if i%20 == 0 {
					paCache.Compact()
				}
				if i%30 == 0 {
					_ = paCache.EvaluateMemoryPressure()
				}
			}
		})
	}

	for range 3 {
		wg.Go(func() {
			var prevGetHits int64
			hitSet := attribute.NewSet(
				attribute.String("cache.backend", "arena_radix"),
				attribute.String("cache.name", "race-cache"),
				attribute.String("operation", "get"),
				attribute.String("result", "hit"),
			)
			for range 25 {
				var rm metricdata.ResourceMetrics
				err := reader.Collect(context.Background(), &rm)
				assert.NoError(t, err) //nolint:testifylint // wg.Go runs in a child goroutine
				for _, sm := range rm.ScopeMetrics {
					for _, m := range sm.Metrics {
						if m.Name == "lru.cache.requests" {
							if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
								for _, dp := range sum.DataPoints {
									if dp.Attributes.Equals(&hitSet) {
										assert.GreaterOrEqual(t, dp.Value, prevGetHits)
										prevGetHits = dp.Value
									}
								}
							}
						}
					}
				}
			}
		})
	}

	wg.Wait()

	// Assert: Final quiescent scrape succeeds and matches cache.Stats()
	st := cache.Stats()
	metricsByName := collectMetricsByName(t, reader, otellru.ScopeName)
	assert.Len(t, metricsByName, 16)
	baseSet := attribute.NewSet(
		attribute.String("cache.backend", "arena_radix"),
		attribute.String("cache.name", "race-cache"),
	)
	entriesPts := requireInt64GaugePoints(t, metricsByName["lru.cache.entries"], "{entry}")
	assert.Equal(t, int64(st.Len), entriesPts[baseSet])
}

type failingMeter struct {
	noop.Meter
	callCount int
	failOn    int
	failErr   error
}

func (m *failingMeter) Int64ObservableCounter(name string, opts ...metric.Int64ObservableCounterOption) (metric.Int64ObservableCounter, error) {
	m.callCount++
	if m.callCount == m.failOn {
		return nil, m.failErr
	}
	return m.Meter.Int64ObservableCounter(name, opts...)
}

func (m *failingMeter) Int64ObservableGauge(name string, opts ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	m.callCount++
	if m.callCount == m.failOn {
		return nil, m.failErr
	}
	return m.Meter.Int64ObservableGauge(name, opts...)
}

func (m *failingMeter) Float64ObservableGauge(name string, opts ...metric.Float64ObservableGaugeOption) (metric.Float64ObservableGauge, error) {
	m.callCount++
	if m.callCount == m.failOn {
		return nil, m.failErr
	}
	return m.Meter.Float64ObservableGauge(name, opts...)
}

func (m *failingMeter) RegisterCallback(cb metric.Callback, insts ...metric.Observable) (metric.Registration, error) {
	m.callCount++
	if m.callCount == m.failOn {
		return nil, m.failErr
	}
	return m.Meter.RegisterCallback(cb, insts...)
}

func TestRegister_MeterErrorPropagation(t *testing.T) {
	// Arrange: ArenaRadixCache creates 16 instruments + 1 RegisterCallback = 17 Meter calls.
	cache := lru.NewArenaRadixCache[string](10)
	expectedErr := errors.New("synthetic meter error")

	// Act & Assert: Verify every instrument creation and callback registration error is propagated.
	for callIdx := 1; callIdx <= 17; callIdx++ {
		fm := &failingMeter{
			failOn:  callIdx,
			failErr: expectedErr,
		}
		reg, err := otellru.Register(cache, otellru.WithMeter(fm))
		require.ErrorIs(t, err, expectedErr)
		assert.Nil(t, reg)
	}
}
