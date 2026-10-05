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

type recordingObserver struct {
	noop.Observer
	int64Vals   []int64
	float64Vals []float64
}

func (o *recordingObserver) ObserveInt64(_ metric.Int64Observable, v int64, _ ...metric.ObserveOption) {
	o.int64Vals = append(o.int64Vals, v)
}

func (o *recordingObserver) ObserveFloat64(_ metric.Float64Observable, v float64, _ ...metric.ObserveOption) {
	o.float64Vals = append(o.float64Vals, v)
}

// TestAdversarial_MultiCacheRegistrationAndSelectiveUnregister verifies that
// registering MapCache, RadixCache, and ArenaRadixCache simultaneously on the
// same MeterProvider with distinct WithName values isolates all metric series,
// emits arena-only instruments exclusively for ArenaRadixCache, and cleanly
// removes only the unregistered cache's series upon selective Unregister().
func TestAdversarial_MultiCacheRegistrationAndSelectiveUnregister(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
	})

	mapC := lru.NewMapCache[int](100)
	radixC := lru.NewRadixCache[int](200)
	arenaC := lru.NewArenaRadixCache[int](300)

	sharedAttr := attribute.String("env", "prod")

	regMap, err := otellru.Register(mapC, otellru.WithMeterProvider(mp), otellru.WithName("cache-map"), otellru.WithAttributes(sharedAttr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = regMap.Unregister() })

	regRadix, err := otellru.Register(radixC, otellru.WithMeterProvider(mp), otellru.WithName("cache-radix"), otellru.WithAttributes(sharedAttr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = regRadix.Unregister() })

	regArena, err := otellru.Register(arenaC, otellru.WithMeterProvider(mp), otellru.WithName("cache-arena"), otellru.WithAttributes(sharedAttr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = regArena.Unregister() })

	// Mutate each cache with distinct entry counts and lookups.
	_, _ = mapC.Put("m/1", 1)
	_, _ = mapC.Get("m/1")

	_, _ = radixC.Put("r/1", 1)
	_, _ = radixC.Put("r/2", 2)
	_, _ = radixC.Peek("r/1")

	_, _ = arenaC.Put("a/1", 1)
	_, _ = arenaC.Put("a/2", 2)
	_, _ = arenaC.Put("a/3", 3)
	_, _ = arenaC.Get("a/missing")

	metricsByName := collectMetricsByName(t, reader, otellru.ScopeName)
	require.Len(t, metricsByName, 16)

	mapBase := attribute.NewSet(sharedAttr, attribute.String("cache.backend", "map"), attribute.String("cache.name", "cache-map"))
	radixBase := attribute.NewSet(sharedAttr, attribute.String("cache.backend", "radix"), attribute.String("cache.name", "cache-radix"))
	arenaBase := attribute.NewSet(sharedAttr, attribute.String("cache.backend", "arena_radix"), attribute.String("cache.name", "cache-arena"))

	entriesPts := requireInt64GaugePoints(t, metricsByName["lru.cache.entries"], "{entry}")
	assert.Len(t, entriesPts, 3)
	assert.Equal(t, int64(1), entriesPts[mapBase])
	assert.Equal(t, int64(2), entriesPts[radixBase])
	assert.Equal(t, int64(3), entriesPts[arenaBase])

	// Verify arena-only instruments are emitted ONLY for cache-arena (3 state series for nodes, 1 for hash_fallbacks).
	arenaNodePts := requireInt64GaugePoints(t, metricsByName["lru.cache.arena.nodes"], "{node}")
	assert.Len(t, arenaNodePts, 3)
	for attrSet := range arenaNodePts {
		val, ok := attrSet.Value("cache.name")
		require.True(t, ok)
		assert.Equal(t, "cache-arena", val.AsString())
	}

	fallbackPts := requireInt64SumPoints(t, metricsByName["lru.cache.arena.hash_fallbacks"], "{lookup}")
	assert.Len(t, fallbackPts, 1)
	_, hasArenaFallback := fallbackPts[arenaBase]
	assert.True(t, hasArenaFallback)

	// Selectively unregister cache-radix and verify subsequent scrapes only report cache-map and cache-arena.
	require.NoError(t, regRadix.Unregister())
	_, _ = mapC.Put("m/2", 2)
	_, _ = radixC.Put("r/3", 3)
	_, _ = arenaC.Put("a/4", 4)

	afterMetrics := collectMetricsByName(t, reader, otellru.ScopeName)
	afterEntriesPts := requireInt64GaugePoints(t, afterMetrics["lru.cache.entries"], "{entry}")
	assert.Len(t, afterEntriesPts, 2)
	assert.Equal(t, int64(2), afterEntriesPts[mapBase])
	assert.Equal(t, int64(4), afterEntriesPts[arenaBase])
	_, hasRadixAfter := afterEntriesPts[radixBase]
	assert.False(t, hasRadixAfter, "unregistered cache-radix must not appear in subsequent scrapes")
}

func runChurnMutator(c lru.PressureAwareCache[weightedItem], w int, stopCh <-chan struct{}, pressureBits *atomic.Uint64) {
	levels := []float64{0.20, 0.80, 0.95}
	for i := 0; ; i++ {
		select {
		case <-stopCh:
			return
		default:
		}
		k := fmt.Sprintf("ns%d/k%d", (w+i)%4, i%24)
		weight := uint64((i % 4) * 10) // includes 0-weight when i%4 == 0
		_, _ = c.Put(k, weightedItem{id: i, weight: weight})
		_, _ = c.Get(k)
		_, _ = c.Peek(k)
		_ = c.Replace(k, weightedItem{id: i + 1, weight: weight + 5})
		if i%7 == 0 {
			_, _ = c.Delete(k)
		}
		if i%19 == 0 {
			c.DeletePrefix("ns0/")
		}
		if i%43 == 0 {
			c.DeletePrefix("")
		}
		if i%17 == 0 {
			c.Compact()
		}
		if i%13 == 0 {
			pressureBits.Store(math.Float64bits(levels[i%3]))
			_ = c.EvaluateMemoryPressure()
		}
	}
}

// TestAdversarial_HighChurnMonotonicityAndGaugeConsistencyUnderRace runs 8 concurrent
// mutator goroutines (including DeletePrefix(""), Compact(), and Tier 1/2 pressure oscillations)
// alongside 4 concurrent scraper goroutines across all 3 backends, asserting strict counter
// monotonicity and gauge structural invariants on 50 consecutive scrapes.
func TestAdversarial_HighChurnMonotonicityAndGaugeConsistencyUnderRace(t *testing.T) {
	for _, b := range []lru.Backend{lru.BackendMap, lru.BackendRadix, lru.BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			t.Cleanup(func() {
				_ = mp.Shutdown(context.Background())
			})

			var pressureBits atomic.Uint64
			pressureBits.Store(math.Float64bits(0.20))

			c := lru.New[weightedItem](
				80,
				lru.WithBackend(b),
				lru.WithWeigher(itemWeigher),
				lru.WithPressureFunc(func() float64 {
					return math.Float64frombits(pressureBits.Load())
				}),
				lru.WithCompactionThreshold(0.75),
				lru.WithEvictionThreshold(0.90),
				lru.WithEvictionRetentionRatio(0.50),
			).(lru.PressureAwareCache[weightedItem])

			reg, err := otellru.Register(c, otellru.WithMeterProvider(mp), otellru.WithName("monotonic-cache"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = reg.Unregister() })

			stopCh := make(chan struct{})
			var mutWG sync.WaitGroup

			// 8 concurrent mutators
			for w := range 8 {
				mutWG.Go(func() {
					runChurnMutator(c, w, stopCh, &pressureBits)
				})
			}

			// 3 background concurrent scrapers exercising ManualReader under -race
			var scrapeWG sync.WaitGroup
			for range 3 {
				scrapeWG.Go(func() {
					for range 20 {
						var rm metricdata.ResourceMetrics
						_ = reader.Collect(context.Background(), &rm)
					}
				})
			}

			// Foreground verifier performing 50 sequential scrapes and checking monotonicity of all counters
			type counterKey struct {
				metricName string
				attrs      attribute.Set
			}
			prevCounters := make(map[counterKey]int64)

			counterNames := []string{
				"lru.cache.requests",
				"lru.cache.evictions",
				"lru.cache.evicted_weight",
				"lru.cache.mutations",
				"lru.cache.compactions",
				"lru.cache.pressure_sheds",
				"lru.cache.reclaim_epochs",
				"lru.cache.arena.hash_fallbacks",
			}

			for scrapeIdx := range 50 {
				var rm metricdata.ResourceMetrics
				require.NoError(t, reader.Collect(context.Background(), &rm))

				byName := make(map[string]metricdata.Metrics)
				for _, sm := range rm.ScopeMetrics {
					for _, m := range sm.Metrics {
						byName[m.Name] = m
					}
				}

				for _, cName := range counterNames {
					m, ok := byName[cName]
					if !ok {
						continue
					}
					sum, ok := m.Data.(metricdata.Sum[int64])
					require.True(t, ok)
					for _, dp := range sum.DataPoints {
						ck := counterKey{metricName: cName, attrs: dp.Attributes}
						prev, seen := prevCounters[ck]
						if seen {
							assert.GreaterOrEqual(t, dp.Value, prev,
								"counter %s regressed at scrape %d (%d < %d)", cName, scrapeIdx, dp.Value, prev)
						}
						prevCounters[ck] = dp.Value
					}
				}

				// Verify gauge invariants on each snapshot
				sizePts := requireInt64GaugePoints(t, byName["lru.cache.size"], "{weight}")
				maxSizePts := requireInt64GaugePoints(t, byName["lru.cache.max_size"], "{weight}")
				entriesPts := requireInt64GaugePoints(t, byName["lru.cache.entries"], "{entry}")
				zeroPts := requireInt64GaugePoints(t, byName["lru.cache.zero_weight_entries"], "{entry}")
				pressurePts := requireFloat64GaugePoints(t, byName["lru.cache.memory_pressure"], "1")

				for attrSet, sz := range sizePts {
					assert.GreaterOrEqual(t, sz, int64(0))
					assert.LessOrEqual(t, sz, maxSizePts[attrSet], "size must be <= max_size")
					assert.LessOrEqual(t, zeroPts[attrSet], entriesPts[attrSet], "zero_weight_entries must be <= entries")
					assert.GreaterOrEqual(t, pressurePts[attrSet], 0.0)
				}
			}

			scrapeWG.Wait()
			close(stopCh)
			mutWG.Wait()
		})
	}
}

// TestAdversarial_Uint64OverflowClampingAllInstruments verifies that when all 26 uint64
// fields in lru.Stats are set to math.MaxUint64, every single int64 counter and gauge
// data point emitted by otellru clamps to math.MaxInt64 and never wraps negative.
func TestAdversarial_Uint64OverflowClampingAllInstruments(t *testing.T) {
	stub := stubStatsProvider{
		stats: lru.Stats{
			Backend:                  lru.BackendArenaRadix,
			GetHits:                  math.MaxUint64,
			GetMisses:                math.MaxUint64,
			PeekHits:                 math.MaxUint64,
			PeekMisses:               math.MaxUint64,
			EvictionsCapacity:        math.MaxUint64,
			EvictionsPressure:        math.MaxUint64,
			EvictionsDeleted:         math.MaxUint64,
			EvictionsReplaced:        math.MaxUint64,
			EvictedWeightCapacity:    math.MaxUint64,
			EvictedWeightPressure:    math.MaxUint64,
			EvictedWeightDeleted:     math.MaxUint64,
			EvictedWeightReplaced:    math.MaxUint64,
			CurrentSize:              math.MaxUint64,
			MaxSize:                  math.MaxUint64,
			Len:                      math.MaxInt,
			ZeroSizeCount:            math.MaxInt,
			PutInserted:              math.MaxUint64,
			PutUpdated:               math.MaxUint64,
			PutRejectedOversized:     math.MaxUint64,
			ReplaceUpdated:           math.MaxUint64,
			ReplaceNotFound:          math.MaxUint64,
			ReplaceSelfEvicted:       math.MaxUint64,
			DeleteDeleted:            math.MaxUint64,
			DeleteNotFound:           math.MaxUint64,
			DeletePrefixExecuted:     math.MaxUint64,
			MemoryPressure:           1.25,
			CompactionsExplicit:      math.MaxUint64,
			CompactionsPressureTier1: math.MaxUint64,
			CompactionsPressureTier2: math.MaxUint64,
			CompactionsAutoSlack:     math.MaxUint64,
			PressureShedsInline:      math.MaxUint64,
			PressureShedsExplicit:    math.MaxUint64,
			ReclaimEpoch:             math.MaxUint64,
			DeletedSinceCompact:      math.MaxInt,
			PeakEntryLen:             math.MaxInt,
			ArenaLiveNodes:           math.MaxInt,
			ArenaFreeNodes:           math.MaxInt,
			ArenaUnallocatedCap:      math.MaxInt,
			ArenaHashFallbacks:       math.MaxUint64,
		},
	}

	// 1. Verify directly via callbackCapturingMeter that otellru's clampCounterToInt64 clamps
	// cumulative counters to (1<<63)-1024 (avoiding OTel SDK int64 addition overflow in sum.go)
	// and uint64ToInt64 clamps gauges to math.MaxInt64 (never wrapping negative).
	const maxClampedCounter = int64((uint64(1) << 63) - 1024)
	capturingMeter := &callbackCapturingMeter{}
	regDirect, err := otellru.Register(stub, otellru.WithMeter(capturingMeter), otellru.WithName("overflow-direct"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = regDirect.Unregister() })

	recObs := &recordingObserver{}
	require.NoError(t, capturingMeter.cb(context.Background(), recObs))
	require.Len(t, recObs.int64Vals, 38, "all 38 int64 observations must be recorded")
	for idx, v := range recObs.int64Vals {
		assert.True(t, v == maxClampedCounter || v == math.MaxInt64,
			"observation %d (%d) must clamp to maxClampedCounter or MaxInt64", idx, v)
	}
	require.Len(t, recObs.float64Vals, 1)
	assert.InDelta(t, 1.25, recObs.float64Vals[0], 1e-9)

	// 2. Also verify through sdkmetric.ManualReader that all 16 instruments (38 int64 + 1 float64 data points)
	// are emitted and that counter/gauge saturation works end-to-end without SDK int64 overflow across multiple scrapes.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
	})

	regSDK, err := otellru.Register(stub, otellru.WithMeterProvider(mp), otellru.WithName("overflow-sdk"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = regSDK.Unregister() })

	metricsByName := collectMetricsByName(t, reader, otellru.ScopeName)
	require.Len(t, metricsByName, 16)

	totalInt64Points := 0
	for name, m := range metricsByName {
		switch data := m.Data.(type) {
		case metricdata.Sum[int64]:
			require.NotEmpty(t, data.DataPoints, "counter %s must emit data points", name)
			for _, dp := range data.DataPoints {
				totalInt64Points++
				assert.Equal(t, maxClampedCounter, dp.Value, "counter %s must clamp to maxClampedCounter", name)
			}
		case metricdata.Gauge[int64]:
			for _, dp := range data.DataPoints {
				totalInt64Points++
				assert.Equal(t, int64(math.MaxInt64), dp.Value, "gauge %s must equal MaxInt64", name)
			}
		case metricdata.Gauge[float64]:
			for _, dp := range data.DataPoints {
				assert.InDelta(t, 1.25, dp.Value, 1e-9)
			}
		default:
			t.Fatalf("unexpected metric data type for %s: %T", name, m.Data)
		}
	}
	assert.Equal(t, 38, totalInt64Points, "all 38 int64 data points must be verified")
}

// TestDefect_L01_DuplicateRegistrationWithoutNameCollision characterizes L-01 / F-13 (Bucket 3 OTel SDK behavior):
// Registering multiple caches of the same backend on the same MeterProvider without
// unique WithName or WithAttributes emits identical attribute sets (`{"cache.backend": "map"}`),
// which the OTel SDK deduplicates into a single series.
func TestDefect_L01_DuplicateRegistrationWithoutNameCollision(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = mp.Shutdown(context.Background())
	})

	c1 := lru.NewMapCache[int](100)
	c2 := lru.NewMapCache[int](500)
	_, _ = c1.Put("k1", 1)
	for i := range 5 {
		_, _ = c2.Put(fmt.Sprintf("k_%d", i), i)
	}

	reg1, err1 := otellru.Register(c1, otellru.WithMeterProvider(mp))
	require.NoError(t, err1)
	t.Cleanup(func() { _ = reg1.Unregister() })

	reg2, err2 := otellru.Register(c2, otellru.WithMeterProvider(mp))
	require.NoError(t, err2)
	t.Cleanup(func() { _ = reg2.Unregister() })

	metricsByName := collectMetricsByName(t, reader, otellru.ScopeName)
	entriesPts := requireInt64GaugePoints(t, metricsByName["lru.cache.entries"], "{entry}")
	assert.Len(t, entriesPts, 1)
}

// ============================================================================
// Benchmarks: Direct Callback vs Full OTel SDK ManualReader.Collect
// ============================================================================

func BenchmarkOtelScrape_DirectCallback(b *testing.B) {
	backends := []struct {
		name string
		make func() lru.StatsProvider
	}{
		{name: "MapCache", make: func() lru.StatsProvider { return lru.NewMapCache[int](1000) }},
		{name: "RadixCache", make: func() lru.StatsProvider { return lru.NewRadixCache[int](1000) }},
		{name: "ArenaRadixCache", make: func() lru.StatsProvider { return lru.NewArenaRadixCache[int](1000) }},
	}

	for _, bk := range backends {
		b.Run(bk.name, func(b *testing.B) {
			cache := bk.make()
			capturingMeter := &callbackCapturingMeter{}
			reg, err := otellru.Register(
				cache,
				otellru.WithMeter(capturingMeter),
				otellru.WithName("bench-cache"),
				otellru.WithAttributes(attribute.String("env", "bench")),
			)
			require.NoError(b, err)
			b.Cleanup(func() { _ = reg.Unregister() })

			obs := &zeroAllocObserver{}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = capturingMeter.cb(ctx, obs)
			}
		})
	}
}

func BenchmarkOtelScrape_FullSDKCollect(b *testing.B) {
	for _, numCaches := range []int{1, 10} {
		b.Run(fmt.Sprintf("Caches_%d", numCaches), func(b *testing.B) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			b.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

			for i := range numCaches {
				var c lru.Cache[int]
				switch i % 3 {
				case 0:
					c = lru.NewArenaRadixCache[int](1000)
				case 1:
					c = lru.NewMapCache[int](1000)
				default:
					c = lru.NewRadixCache[int](1000)
				}
				_, _ = c.Put("dir/k1", 1)
				reg, err := otellru.Register(c, otellru.WithMeterProvider(mp), otellru.WithName(fmt.Sprintf("cache-%d", i)))
				require.NoError(b, err)
				b.Cleanup(func() { _ = reg.Unregister() })
			}

			ctx := context.Background()
			var rm metricdata.ResourceMetrics
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = reader.Collect(ctx, &rm)
			}
		})
	}
}
