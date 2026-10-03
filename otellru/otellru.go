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

// Package otellru provides asynchronous OpenTelemetry metric instrumentation
// for github.com/google/go-lru caches using batch observable callbacks.
package otellru

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"

	"github.com/google/go-lru"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ScopeName is the OpenTelemetry instrumentation scope name for otellru.
const ScopeName = "github.com/google/go-lru/otellru"

// ErrNilCache is returned by Register when the provided cache is nil.
var ErrNilCache = errors.New("otellru: cache must not be nil")

// Attribute keys emitted by otellru instruments.
const (
	attrKeyCacheBackend = attribute.Key("cache.backend")
	attrKeyCacheName    = attribute.Key("cache.name")
	attrKeyOperation    = attribute.Key("operation")
	attrKeyResult       = attribute.Key("result")
	attrKeyReason       = attribute.Key("reason")
	attrKeyOutcome      = attribute.Key("outcome")
	attrKeyTrigger      = attribute.Key("trigger")
	attrKeyState        = attribute.Key("state")
)

// StatsProvider is implemented by any lru.Cache[V] or lru.PressureAwareCache[V]
// that exposes a point-in-time lru.Stats snapshot.
type StatsProvider interface {
	Stats() lru.Stats
}

type config struct {
	meterProvider metric.MeterProvider
	meter         metric.Meter
	name          string
	attrs         []attribute.KeyValue
}

// Option configures an OpenTelemetry cache metric registration.
type Option func(*config)

// WithMeterProvider configures the metric.MeterProvider used to create a Meter
// when WithMeter is not explicitly provided. If nil, the global otel.GetMeterProvider() is used.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(c *config) {
		c.meterProvider = provider
	}
}

// WithMeter configures a specific metric.Meter to register observable instruments on.
// When non-nil, WithMeter takes precedence over WithMeterProvider.
func WithMeter(meter metric.Meter) Option {
	return func(c *config) {
		c.meter = meter
	}
}

// WithName configures an optional logical cache name emitted as the "cache.name" attribute.
// Passing an empty string omits the "cache.name" attribute.
func WithName(name string) Option {
	return func(c *config) {
		c.name = name
	}
}

// WithAttributes appends additional static attributes to every metric observation
// for the registered cache instance. Built-in "cache.backend" and "cache.name"
// (when WithName is non-empty) attributes take precedence over duplicate keys.
func WithAttributes(attrs ...attribute.KeyValue) Option {
	return func(c *config) {
		c.attrs = append(c.attrs, attrs...)
	}
}

// Registration manages the lifecycle of a registered OpenTelemetry batch metric callback.
type Registration struct {
	mu  sync.Mutex
	reg metric.Registration
}

// Unregister removes the batch metric callback from the Meter so future metric
// collection cycles no longer scrape the cache.
// Calling Unregister on a nil *Registration or calling it multiple times is safe.
func (r *Registration) Unregister() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reg == nil {
		return nil
	}
	err := r.reg.Unregister()
	r.reg = nil
	return err
}

func isNilStatsProvider(cache StatsProvider) bool {
	if cache == nil {
		return true
	}
	v := reflect.ValueOf(cache)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return v.IsNil()
	default:
		return false
	}
}

func backendAttributeValue(b lru.Backend) string {
	switch b {
	case lru.BackendMap:
		return "map"
	case lru.BackendRadix:
		return "radix"
	case lru.BackendArenaRadix:
		return "arena_radix"
	default:
		return "unknown"
	}
}

func uint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// makeObserveOpts pre-allocates an immutable attribute.Set and wraps it in a 1-element
// []metric.ObserveOption slice so calling o.ObserveInt64(inst, val, opts...) unpacks the
// slice header with 0 heap allocations during scrape callbacks.
func makeObserveOpts(base []attribute.KeyValue, extra ...attribute.KeyValue) []metric.ObserveOption {
	combined := make([]attribute.KeyValue, 0, len(base)+len(extra))
	combined = append(combined, base...)
	combined = append(combined, extra...)
	set := attribute.NewSet(combined...)
	return []metric.ObserveOption{metric.WithAttributeSet(set)}
}

// Register instruments cache using purely asynchronous OpenTelemetry observable counters
// and gauges backed by a single batch Meter.RegisterCallback that reads cache.Stats() once
// per collection cycle.
func Register(cache StatsProvider, opts ...Option) (*Registration, error) {
	if isNilStatsProvider(cache) {
		return nil, ErrNilCache
	}

	var cfg config
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	meter := cfg.meter
	if meter == nil {
		mp := cfg.meterProvider
		if mp == nil {
			mp = otel.GetMeterProvider()
		}
		meter = mp.Meter(ScopeName)
	}

	initialStats := cache.Stats()
	isArena := initialStats.Backend == lru.BackendArenaRadix

	// Build base attributes: custom attributes first, then authoritative cache.name and cache.backend.
	baseAttrs := make([]attribute.KeyValue, 0, len(cfg.attrs)+2)
	baseAttrs = append(baseAttrs, cfg.attrs...)
	if cfg.name != "" {
		baseAttrs = append(baseAttrs, attrKeyCacheName.String(cfg.name))
	}
	baseAttrs = append(baseAttrs, attrKeyCacheBackend.String(backendAttributeValue(initialStats.Backend)))

	// Pre-allocate all attribute.Set / []metric.ObserveOption slices once at registration time.
	baseOpts := makeObserveOpts(baseAttrs)

	reqGetHitOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("get"), attrKeyResult.String("hit"))
	reqGetMissOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("get"), attrKeyResult.String("miss"))
	reqPeekHitOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("peek"), attrKeyResult.String("hit"))
	reqPeekMissOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("peek"), attrKeyResult.String("miss"))

	reasonCapacityOpts := makeObserveOpts(baseAttrs, attrKeyReason.String("capacity"))
	reasonPressureOpts := makeObserveOpts(baseAttrs, attrKeyReason.String("pressure"))
	reasonDeletedOpts := makeObserveOpts(baseAttrs, attrKeyReason.String("deleted"))
	reasonReplacedOpts := makeObserveOpts(baseAttrs, attrKeyReason.String("replaced"))

	mutPutInsertedOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("put"), attrKeyOutcome.String("inserted"))
	mutPutUpdatedOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("put"), attrKeyOutcome.String("updated"))
	mutPutRejectedOversizedOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("put"), attrKeyOutcome.String("rejected_oversized"))
	mutReplaceUpdatedOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("replace"), attrKeyOutcome.String("updated"))
	mutReplaceNotFoundOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("replace"), attrKeyOutcome.String("not_found"))
	mutReplaceSelfEvictedOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("replace"), attrKeyOutcome.String("self_evicted"))
	mutDeleteDeletedOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("delete"), attrKeyOutcome.String("deleted"))
	mutDeleteNotFoundOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("delete"), attrKeyOutcome.String("not_found"))
	mutDeletePrefixExecutedOpts := makeObserveOpts(baseAttrs, attrKeyOperation.String("delete_prefix"), attrKeyOutcome.String("executed"))

	compactExplicitOpts := makeObserveOpts(baseAttrs, attrKeyTrigger.String("explicit"))
	compactTier1Opts := makeObserveOpts(baseAttrs, attrKeyTrigger.String("pressure_tier1"))
	compactTier2Opts := makeObserveOpts(baseAttrs, attrKeyTrigger.String("pressure_tier2"))
	compactAutoSlackOpts := makeObserveOpts(baseAttrs, attrKeyTrigger.String("auto_slack"))

	shedInlineOpts := makeObserveOpts(baseAttrs, attrKeyTrigger.String("inline"))
	shedExplicitOpts := makeObserveOpts(baseAttrs, attrKeyTrigger.String("explicit"))

	// Create the 14 general cache observable instruments.
	requestsCounter, err := meter.Int64ObservableCounter(
		"lru.cache.requests",
		metric.WithUnit("{request}"),
		metric.WithDescription("Total number of cache lookup requests."),
	)
	if err != nil {
		return nil, err
	}

	evictionsCounter, err := meter.Int64ObservableCounter(
		"lru.cache.evictions",
		metric.WithUnit("{entry}"),
		metric.WithDescription("Total number of cache entries evicted, deleted, or displaced."),
	)
	if err != nil {
		return nil, err
	}

	evictedWeightCounter, err := meter.Int64ObservableCounter(
		"lru.cache.evicted_weight",
		metric.WithUnit("{weight}"),
		metric.WithDescription("Total weight of cache entries evicted, deleted, or displaced."),
	)
	if err != nil {
		return nil, err
	}

	sizeGauge, err := meter.Int64ObservableGauge(
		"lru.cache.size",
		metric.WithUnit("{weight}"),
		metric.WithDescription("Current total weight of live entries in the cache."),
	)
	if err != nil {
		return nil, err
	}

	maxSizeGauge, err := meter.Int64ObservableGauge(
		"lru.cache.max_size",
		metric.WithUnit("{weight}"),
		metric.WithDescription("Maximum configured capacity weight of the cache."),
	)
	if err != nil {
		return nil, err
	}

	entriesGauge, err := meter.Int64ObservableGauge(
		"lru.cache.entries",
		metric.WithUnit("{entry}"),
		metric.WithDescription("Current number of live entries in the cache."),
	)
	if err != nil {
		return nil, err
	}

	zeroWeightEntriesGauge, err := meter.Int64ObservableGauge(
		"lru.cache.zero_weight_entries",
		metric.WithUnit("{entry}"),
		metric.WithDescription("Current number of live zero-weight entries in the cache."),
	)
	if err != nil {
		return nil, err
	}

	mutationsCounter, err := meter.Int64ObservableCounter(
		"lru.cache.mutations",
		metric.WithUnit("{operation}"),
		metric.WithDescription("Total number of cache mutation operations by outcome."),
	)
	if err != nil {
		return nil, err
	}

	memoryPressureGauge, err := meter.Float64ObservableGauge(
		"lru.cache.memory_pressure",
		metric.WithUnit("1"),
		metric.WithDescription("Latest sampled normalized memory pressure reading."),
	)
	if err != nil {
		return nil, err
	}

	compactionsCounter, err := meter.Int64ObservableCounter(
		"lru.cache.compactions",
		metric.WithUnit("{compaction}"),
		metric.WithDescription("Total number of internal index or arena compactions performed."),
	)
	if err != nil {
		return nil, err
	}

	pressureShedsCounter, err := meter.Int64ObservableCounter(
		"lru.cache.pressure_sheds",
		metric.WithUnit("{event}"),
		metric.WithDescription("Total number of Tier 2 critical memory-pressure shedding events."),
	)
	if err != nil {
		return nil, err
	}

	reclaimEpochsCounter, err := meter.Int64ObservableCounter(
		"lru.cache.reclaim_epochs",
		metric.WithUnit("{epoch}"),
		metric.WithDescription("Total number of memory reclamation epochs completed."),
	)
	if err != nil {
		return nil, err
	}

	deletedSinceCompactGauge, err := meter.Int64ObservableGauge(
		"lru.cache.deleted_since_compact",
		metric.WithUnit("{entry}"),
		metric.WithDescription("Number of entries deleted or evicted since the last compaction or reset."),
	)
	if err != nil {
		return nil, err
	}

	peakEntriesGauge, err := meter.Int64ObservableGauge(
		"lru.cache.peak_entries",
		metric.WithUnit("{entry}"),
		metric.WithDescription("Peak number of live entries since the last compaction or reset."),
	)
	if err != nil {
		return nil, err
	}

	observables := make([]metric.Observable, 0, 16)
	observables = append(observables,
		requestsCounter,
		evictionsCounter,
		evictedWeightCounter,
		sizeGauge,
		maxSizeGauge,
		entriesGauge,
		zeroWeightEntriesGauge,
		mutationsCounter,
		memoryPressureGauge,
		compactionsCounter,
		pressureShedsCounter,
		reclaimEpochsCounter,
		deletedSinceCompactGauge,
		peakEntriesGauge,
	)

	var (
		arenaNodesGauge           metric.Int64ObservableGauge
		arenaHashFallbacksCounter metric.Int64ObservableCounter
		arenaLiveOpts             []metric.ObserveOption
		arenaFreeOpts             []metric.ObserveOption
		arenaUnallocatedCapOpts   []metric.ObserveOption
	)

	if isArena {
		arenaNodesGauge, err = meter.Int64ObservableGauge(
			"lru.cache.arena.nodes",
			metric.WithUnit("{node}"),
			metric.WithDescription("Current number of arena radix tree nodes by allocation state."),
		)
		if err != nil {
			return nil, err
		}

		arenaHashFallbacksCounter, err = meter.Int64ObservableCounter(
			"lru.cache.arena.hash_fallbacks",
			metric.WithUnit("{lookup}"),
			metric.WithDescription("Total number of FNV-1a hash-index lookups that fell back to radix trie traversal."),
		)
		if err != nil {
			return nil, err
		}

		arenaLiveOpts = makeObserveOpts(baseAttrs, attrKeyState.String("live"))
		arenaFreeOpts = makeObserveOpts(baseAttrs, attrKeyState.String("free"))
		arenaUnallocatedCapOpts = makeObserveOpts(baseAttrs, attrKeyState.String("unallocated_cap"))

		observables = append(observables, arenaNodesGauge, arenaHashFallbacksCounter)
	}

	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := cache.Stats()

		// 1. lru.cache.requests
		o.ObserveInt64(requestsCounter, uint64ToInt64(st.GetHits), reqGetHitOpts...)
		o.ObserveInt64(requestsCounter, uint64ToInt64(st.GetMisses), reqGetMissOpts...)
		o.ObserveInt64(requestsCounter, uint64ToInt64(st.PeekHits), reqPeekHitOpts...)
		o.ObserveInt64(requestsCounter, uint64ToInt64(st.PeekMisses), reqPeekMissOpts...)

		// 2. lru.cache.evictions
		o.ObserveInt64(evictionsCounter, uint64ToInt64(st.EvictionsCapacity), reasonCapacityOpts...)
		o.ObserveInt64(evictionsCounter, uint64ToInt64(st.EvictionsPressure), reasonPressureOpts...)
		o.ObserveInt64(evictionsCounter, uint64ToInt64(st.EvictionsDeleted), reasonDeletedOpts...)
		o.ObserveInt64(evictionsCounter, uint64ToInt64(st.EvictionsReplaced), reasonReplacedOpts...)

		// 3. lru.cache.evicted_weight
		o.ObserveInt64(evictedWeightCounter, uint64ToInt64(st.EvictedWeightCapacity), reasonCapacityOpts...)
		o.ObserveInt64(evictedWeightCounter, uint64ToInt64(st.EvictedWeightPressure), reasonPressureOpts...)
		o.ObserveInt64(evictedWeightCounter, uint64ToInt64(st.EvictedWeightDeleted), reasonDeletedOpts...)
		o.ObserveInt64(evictedWeightCounter, uint64ToInt64(st.EvictedWeightReplaced), reasonReplacedOpts...)

		// 4–7. Capacity & entry gauges
		o.ObserveInt64(sizeGauge, uint64ToInt64(st.CurrentSize), baseOpts...)
		o.ObserveInt64(maxSizeGauge, uint64ToInt64(st.MaxSize), baseOpts...)
		o.ObserveInt64(entriesGauge, int64(st.Len), baseOpts...)
		o.ObserveInt64(zeroWeightEntriesGauge, int64(st.ZeroSizeCount), baseOpts...)

		// 8. lru.cache.mutations
		o.ObserveInt64(mutationsCounter, uint64ToInt64(st.PutInserted), mutPutInsertedOpts...)
		o.ObserveInt64(mutationsCounter, uint64ToInt64(st.PutUpdated), mutPutUpdatedOpts...)
		o.ObserveInt64(mutationsCounter, uint64ToInt64(st.PutRejectedOversized), mutPutRejectedOversizedOpts...)
		o.ObserveInt64(mutationsCounter, uint64ToInt64(st.ReplaceUpdated), mutReplaceUpdatedOpts...)
		o.ObserveInt64(mutationsCounter, uint64ToInt64(st.ReplaceNotFound), mutReplaceNotFoundOpts...)
		o.ObserveInt64(mutationsCounter, uint64ToInt64(st.ReplaceSelfEvicted), mutReplaceSelfEvictedOpts...)
		o.ObserveInt64(mutationsCounter, uint64ToInt64(st.DeleteDeleted), mutDeleteDeletedOpts...)
		o.ObserveInt64(mutationsCounter, uint64ToInt64(st.DeleteNotFound), mutDeleteNotFoundOpts...)
		o.ObserveInt64(mutationsCounter, uint64ToInt64(st.DeletePrefixExecuted), mutDeletePrefixExecutedOpts...)

		// 9. lru.cache.memory_pressure
		o.ObserveFloat64(memoryPressureGauge, st.MemoryPressure, baseOpts...)

		// 10. lru.cache.compactions
		o.ObserveInt64(compactionsCounter, uint64ToInt64(st.CompactionsExplicit), compactExplicitOpts...)
		o.ObserveInt64(compactionsCounter, uint64ToInt64(st.CompactionsPressureTier1), compactTier1Opts...)
		o.ObserveInt64(compactionsCounter, uint64ToInt64(st.CompactionsPressureTier2), compactTier2Opts...)
		o.ObserveInt64(compactionsCounter, uint64ToInt64(st.CompactionsAutoSlack), compactAutoSlackOpts...)

		// 11. lru.cache.pressure_sheds
		o.ObserveInt64(pressureShedsCounter, uint64ToInt64(st.PressureShedsInline), shedInlineOpts...)
		o.ObserveInt64(pressureShedsCounter, uint64ToInt64(st.PressureShedsExplicit), shedExplicitOpts...)

		// 12–14. Reclamation epoch & structural watermark gauges
		o.ObserveInt64(reclaimEpochsCounter, uint64ToInt64(st.ReclaimEpoch), baseOpts...)
		o.ObserveInt64(deletedSinceCompactGauge, int64(st.DeletedSinceCompact), baseOpts...)
		o.ObserveInt64(peakEntriesGauge, int64(st.PeakEntryLen), baseOpts...)

		// 15–16. ArenaRadixCache-only instruments
		if isArena {
			o.ObserveInt64(arenaNodesGauge, int64(st.ArenaLiveNodes), arenaLiveOpts...)
			o.ObserveInt64(arenaNodesGauge, int64(st.ArenaFreeNodes), arenaFreeOpts...)
			o.ObserveInt64(arenaNodesGauge, int64(st.ArenaUnallocatedCap), arenaUnallocatedCapOpts...)
			o.ObserveInt64(arenaHashFallbacksCounter, uint64ToInt64(st.ArenaHashFallbacks), baseOpts...)
		}

		return nil
	}, observables...)
	if err != nil {
		return nil, err
	}

	return &Registration{reg: reg}, nil
}
