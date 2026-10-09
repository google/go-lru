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

// StatsProvider is an alias for lru.StatsProvider, implemented by any lru.Cache[V]
// or lru.PressureAwareCache[V] that exposes a point-in-time lru.Stats snapshot.
type StatsProvider = lru.StatsProvider

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
// for the registered cache instance. Reserved package attribute keys ("cache.backend",
// "cache.name", "operation", "result", "reason", "outcome", "trigger", and "state")
// in attrs are ignored so they cannot spoof cache identity or leak metric-specific
// dimensions onto unrelated instruments.
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

func isReservedAttributeKey(k attribute.Key) bool {
	switch k {
	case attrKeyCacheBackend,
		attrKeyCacheName,
		attrKeyOperation,
		attrKeyResult,
		attrKeyReason,
		attrKeyOutcome,
		attrKeyTrigger,
		attrKeyState:
		return true
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

// maxOtelCounterInt64 is the largest int64 value (2^63 - 1024 = 9223372036854774784)
// whose float64 conversion does not round up to 2^63. The OpenTelemetry Go SDK's
// delta cumulative-to-delta rate calculator converts int64 counter deltas via float64,
// and int64(float64(math.MaxInt64)) wraps to math.MinInt64 on x86_64.
const maxOtelCounterInt64 = (1 << 63) - 1024

func uint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

func uint64ToCounterInt64(v uint64) int64 {
	if v > uint64(maxOtelCounterInt64) {
		return maxOtelCounterInt64
	}
	return int64(v)
}

func clampNonNegativeInt64(v int) int64 {
	if v < 0 {
		return 0
	}
	return int64(v)
}

func clampNormalizedPressure(v float64) float64 {
	if math.IsNaN(v) || v <= 0.0 {
		return 0.0
	}
	if v > 1.0 {
		return 1.0
	}
	return v
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

func resolveMeter(cfg *config) metric.Meter {
	if cfg.meter != nil {
		return cfg.meter
	}
	mp := cfg.meterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	return mp.Meter(ScopeName)
}

func buildBaseAttrs(cfg *config, backend lru.Backend) []attribute.KeyValue {
	baseAttrs := make([]attribute.KeyValue, 0, len(cfg.attrs)+2)
	for _, kv := range cfg.attrs {
		if !isReservedAttributeKey(kv.Key) {
			baseAttrs = append(baseAttrs, kv)
		}
	}
	if cfg.name != "" {
		baseAttrs = append(baseAttrs, attrKeyCacheName.String(cfg.name))
	}
	return append(baseAttrs, attrKeyCacheBackend.String(backendAttributeValue(backend)))
}

type observeSets struct {
	base                    []metric.ObserveOption
	reqGetHit               []metric.ObserveOption
	reqGetMiss              []metric.ObserveOption
	reqPeekHit              []metric.ObserveOption
	reqPeekMiss             []metric.ObserveOption
	reasonCapacity          []metric.ObserveOption
	reasonPressure          []metric.ObserveOption
	reasonDeleted           []metric.ObserveOption
	reasonReplaced          []metric.ObserveOption
	mutPutInserted          []metric.ObserveOption
	mutPutUpdated           []metric.ObserveOption
	mutPutRejectedOversized []metric.ObserveOption
	mutReplaceUpdated       []metric.ObserveOption
	mutReplaceNotFound      []metric.ObserveOption
	mutReplaceSelfEvicted   []metric.ObserveOption
	mutDeleteDeleted        []metric.ObserveOption
	mutDeleteNotFound       []metric.ObserveOption
	mutDeletePrefixExecuted []metric.ObserveOption
	compactExplicit         []metric.ObserveOption
	compactTier1            []metric.ObserveOption
	compactTier2            []metric.ObserveOption
	compactAutoSlack        []metric.ObserveOption
	shedInline              []metric.ObserveOption
	shedExplicit            []metric.ObserveOption
	arenaLive               []metric.ObserveOption
	arenaFree               []metric.ObserveOption
	arenaUnallocatedCap     []metric.ObserveOption
}

func newObserveSets(baseAttrs []attribute.KeyValue, isArena bool) observeSets {
	sets := observeSets{
		base:                    makeObserveOpts(baseAttrs),
		reqGetHit:               makeObserveOpts(baseAttrs, attrKeyOperation.String("get"), attrKeyResult.String("hit")),
		reqGetMiss:              makeObserveOpts(baseAttrs, attrKeyOperation.String("get"), attrKeyResult.String("miss")),
		reqPeekHit:              makeObserveOpts(baseAttrs, attrKeyOperation.String("peek"), attrKeyResult.String("hit")),
		reqPeekMiss:             makeObserveOpts(baseAttrs, attrKeyOperation.String("peek"), attrKeyResult.String("miss")),
		reasonCapacity:          makeObserveOpts(baseAttrs, attrKeyReason.String("capacity")),
		reasonPressure:          makeObserveOpts(baseAttrs, attrKeyReason.String("pressure")),
		reasonDeleted:           makeObserveOpts(baseAttrs, attrKeyReason.String("deleted")),
		reasonReplaced:          makeObserveOpts(baseAttrs, attrKeyReason.String("replaced")),
		mutPutInserted:          makeObserveOpts(baseAttrs, attrKeyOperation.String("put"), attrKeyOutcome.String("inserted")),
		mutPutUpdated:           makeObserveOpts(baseAttrs, attrKeyOperation.String("put"), attrKeyOutcome.String("updated")),
		mutPutRejectedOversized: makeObserveOpts(baseAttrs, attrKeyOperation.String("put"), attrKeyOutcome.String("rejected_oversized")),
		mutReplaceUpdated:       makeObserveOpts(baseAttrs, attrKeyOperation.String("replace"), attrKeyOutcome.String("updated")),
		mutReplaceNotFound:      makeObserveOpts(baseAttrs, attrKeyOperation.String("replace"), attrKeyOutcome.String("not_found")),
		mutReplaceSelfEvicted:   makeObserveOpts(baseAttrs, attrKeyOperation.String("replace"), attrKeyOutcome.String("self_evicted")),
		mutDeleteDeleted:        makeObserveOpts(baseAttrs, attrKeyOperation.String("delete"), attrKeyOutcome.String("deleted")),
		mutDeleteNotFound:       makeObserveOpts(baseAttrs, attrKeyOperation.String("delete"), attrKeyOutcome.String("not_found")),
		mutDeletePrefixExecuted: makeObserveOpts(baseAttrs, attrKeyOperation.String("delete_prefix"), attrKeyOutcome.String("executed")),
		compactExplicit:         makeObserveOpts(baseAttrs, attrKeyTrigger.String("explicit")),
		compactTier1:            makeObserveOpts(baseAttrs, attrKeyTrigger.String("pressure_tier1")),
		compactTier2:            makeObserveOpts(baseAttrs, attrKeyTrigger.String("pressure_tier2")),
		compactAutoSlack:        makeObserveOpts(baseAttrs, attrKeyTrigger.String("auto_slack")),
		shedInline:              makeObserveOpts(baseAttrs, attrKeyTrigger.String("inline")),
		shedExplicit:            makeObserveOpts(baseAttrs, attrKeyTrigger.String("explicit")),
	}
	if isArena {
		sets.arenaLive = makeObserveOpts(baseAttrs, attrKeyState.String("live"))
		sets.arenaFree = makeObserveOpts(baseAttrs, attrKeyState.String("free"))
		sets.arenaUnallocatedCap = makeObserveOpts(baseAttrs, attrKeyState.String("unallocated_cap"))
	}
	return sets
}

type cacheInstruments struct {
	requestsCounter           metric.Int64ObservableCounter
	evictionsCounter          metric.Int64ObservableCounter
	evictedWeightCounter      metric.Int64ObservableCounter
	sizeGauge                 metric.Int64ObservableGauge
	maxSizeGauge              metric.Int64ObservableGauge
	entriesGauge              metric.Int64ObservableGauge
	zeroWeightEntriesGauge    metric.Int64ObservableGauge
	mutationsCounter          metric.Int64ObservableCounter
	memoryPressureGauge       metric.Float64ObservableGauge
	compactionsCounter        metric.Int64ObservableCounter
	pressureShedsCounter      metric.Int64ObservableCounter
	reclaimEpochsCounter      metric.Int64ObservableCounter
	deletedSinceCompactGauge  metric.Int64ObservableGauge
	peakEntriesGauge          metric.Int64ObservableGauge
	arenaNodesGauge           metric.Int64ObservableGauge
	arenaHashFallbacksCounter metric.Int64ObservableCounter
	observables               []metric.Observable
}

func newInt64Counter(meter metric.Meter, name, unit, desc string) (metric.Int64ObservableCounter, error) {
	return meter.Int64ObservableCounter(name, metric.WithUnit(unit), metric.WithDescription(desc))
}

func newInt64Gauge(meter metric.Meter, name, unit, desc string) (metric.Int64ObservableGauge, error) {
	return meter.Int64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(desc))
}

func (inst *cacheInstruments) initLookupAndCapacityInstruments(meter metric.Meter) error {
	var err error
	if inst.requestsCounter, err = newInt64Counter(meter, "lru.cache.requests", "{request}", "Total number of cache lookup requests."); err != nil {
		return err
	}
	if inst.evictionsCounter, err = newInt64Counter(meter, "lru.cache.evictions", "{entry}", "Total number of cache entries evicted, deleted, or displaced."); err != nil {
		return err
	}
	if inst.evictedWeightCounter, err = newInt64Counter(meter, "lru.cache.evicted_weight", "{weight}", "Total weight of cache entries evicted, deleted, or displaced."); err != nil {
		return err
	}
	if inst.sizeGauge, err = newInt64Gauge(meter, "lru.cache.size", "{weight}", "Current total weight of live entries in the cache."); err != nil {
		return err
	}
	if inst.maxSizeGauge, err = newInt64Gauge(meter, "lru.cache.max_size", "{weight}", "Maximum configured capacity weight of the cache."); err != nil {
		return err
	}
	if inst.entriesGauge, err = newInt64Gauge(meter, "lru.cache.entries", "{entry}", "Current number of live entries in the cache."); err != nil {
		return err
	}
	inst.zeroWeightEntriesGauge, err = newInt64Gauge(meter, "lru.cache.zero_weight_entries", "{entry}", "Current number of live zero-weight entries in the cache.")
	return err
}

func (inst *cacheInstruments) initMutationAndPressureInstruments(meter metric.Meter) error {
	var err error
	if inst.mutationsCounter, err = newInt64Counter(meter, "lru.cache.mutations", "{operation}", "Total number of cache mutation operations by outcome."); err != nil {
		return err
	}
	if inst.memoryPressureGauge, err = meter.Float64ObservableGauge(
		"lru.cache.memory_pressure",
		metric.WithUnit("1"),
		metric.WithDescription("Latest sampled normalized memory pressure reading."),
	); err != nil {
		return err
	}
	if inst.compactionsCounter, err = newInt64Counter(meter, "lru.cache.compactions", "{compaction}", "Total number of internal index or arena compactions performed."); err != nil {
		return err
	}
	if inst.pressureShedsCounter, err = newInt64Counter(meter, "lru.cache.pressure_sheds", "{event}", "Total number of Tier 2 critical memory-pressure shedding events."); err != nil {
		return err
	}
	if inst.reclaimEpochsCounter, err = newInt64Counter(meter, "lru.cache.reclaim_epochs", "{epoch}", "Total number of memory reclamation epochs completed."); err != nil {
		return err
	}
	if inst.deletedSinceCompactGauge, err = newInt64Gauge(meter, "lru.cache.deleted_since_compact", "{entry}", "Number of entries deleted or evicted since the last compaction or reset."); err != nil {
		return err
	}
	inst.peakEntriesGauge, err = newInt64Gauge(meter, "lru.cache.peak_entries", "{entry}", "Peak number of live entries since the last compaction or reset.")
	return err
}

func (inst *cacheInstruments) initArenaInstruments(meter metric.Meter, isArena bool) error {
	inst.observables = make([]metric.Observable, 0, 16)
	inst.observables = append(inst.observables,
		inst.requestsCounter,
		inst.evictionsCounter,
		inst.evictedWeightCounter,
		inst.sizeGauge,
		inst.maxSizeGauge,
		inst.entriesGauge,
		inst.zeroWeightEntriesGauge,
		inst.mutationsCounter,
		inst.memoryPressureGauge,
		inst.compactionsCounter,
		inst.pressureShedsCounter,
		inst.reclaimEpochsCounter,
		inst.deletedSinceCompactGauge,
		inst.peakEntriesGauge,
	)
	if !isArena {
		return nil
	}
	var err error
	if inst.arenaNodesGauge, err = newInt64Gauge(meter, "lru.cache.arena.nodes", "{node}", "Current number of arena radix tree nodes by allocation state."); err != nil {
		return err
	}
	if inst.arenaHashFallbacksCounter, err = newInt64Counter(meter, "lru.cache.arena.hash_fallbacks", "{lookup}", "Total number of FNV-1a hash-index lookups that fell back to radix trie traversal."); err != nil {
		return err
	}
	inst.observables = append(inst.observables, inst.arenaNodesGauge, inst.arenaHashFallbacksCounter)
	return nil
}

func (inst *cacheInstruments) observe(o metric.Observer, st lru.Stats, opts *observeSets, isArena bool) {
	// 1. lru.cache.requests
	o.ObserveInt64(inst.requestsCounter, uint64ToCounterInt64(st.GetHits), opts.reqGetHit...)
	o.ObserveInt64(inst.requestsCounter, uint64ToCounterInt64(st.GetMisses), opts.reqGetMiss...)
	o.ObserveInt64(inst.requestsCounter, uint64ToCounterInt64(st.PeekHits), opts.reqPeekHit...)
	o.ObserveInt64(inst.requestsCounter, uint64ToCounterInt64(st.PeekMisses), opts.reqPeekMiss...)

	// 2. lru.cache.evictions
	o.ObserveInt64(inst.evictionsCounter, uint64ToCounterInt64(st.EvictionsCapacity), opts.reasonCapacity...)
	o.ObserveInt64(inst.evictionsCounter, uint64ToCounterInt64(st.EvictionsPressure), opts.reasonPressure...)
	o.ObserveInt64(inst.evictionsCounter, uint64ToCounterInt64(st.EvictionsDeleted), opts.reasonDeleted...)
	o.ObserveInt64(inst.evictionsCounter, uint64ToCounterInt64(st.EvictionsReplaced), opts.reasonReplaced...)

	// 3. lru.cache.evicted_weight
	o.ObserveInt64(inst.evictedWeightCounter, uint64ToCounterInt64(st.EvictedWeightCapacity), opts.reasonCapacity...)
	o.ObserveInt64(inst.evictedWeightCounter, uint64ToCounterInt64(st.EvictedWeightPressure), opts.reasonPressure...)
	o.ObserveInt64(inst.evictedWeightCounter, uint64ToCounterInt64(st.EvictedWeightDeleted), opts.reasonDeleted...)
	o.ObserveInt64(inst.evictedWeightCounter, uint64ToCounterInt64(st.EvictedWeightReplaced), opts.reasonReplaced...)

	// 4–7. Capacity & entry gauges
	o.ObserveInt64(inst.sizeGauge, uint64ToInt64(st.CurrentSize), opts.base...)
	o.ObserveInt64(inst.maxSizeGauge, uint64ToInt64(st.MaxSize), opts.base...)
	o.ObserveInt64(inst.entriesGauge, clampNonNegativeInt64(st.Len), opts.base...)
	o.ObserveInt64(inst.zeroWeightEntriesGauge, clampNonNegativeInt64(st.ZeroSizeCount), opts.base...)

	// 8. lru.cache.mutations
	o.ObserveInt64(inst.mutationsCounter, uint64ToCounterInt64(st.PutInserted), opts.mutPutInserted...)
	o.ObserveInt64(inst.mutationsCounter, uint64ToCounterInt64(st.PutUpdated), opts.mutPutUpdated...)
	o.ObserveInt64(inst.mutationsCounter, uint64ToCounterInt64(st.PutRejectedOversized), opts.mutPutRejectedOversized...)
	o.ObserveInt64(inst.mutationsCounter, uint64ToCounterInt64(st.ReplaceUpdated), opts.mutReplaceUpdated...)
	o.ObserveInt64(inst.mutationsCounter, uint64ToCounterInt64(st.ReplaceNotFound), opts.mutReplaceNotFound...)
	o.ObserveInt64(inst.mutationsCounter, uint64ToCounterInt64(st.ReplaceSelfEvicted), opts.mutReplaceSelfEvicted...)
	o.ObserveInt64(inst.mutationsCounter, uint64ToCounterInt64(st.DeleteDeleted), opts.mutDeleteDeleted...)
	o.ObserveInt64(inst.mutationsCounter, uint64ToCounterInt64(st.DeleteNotFound), opts.mutDeleteNotFound...)
	o.ObserveInt64(inst.mutationsCounter, uint64ToCounterInt64(st.DeletePrefixExecuted), opts.mutDeletePrefixExecuted...)

	// 9. lru.cache.memory_pressure
	o.ObserveFloat64(inst.memoryPressureGauge, clampNormalizedPressure(st.MemoryPressure), opts.base...)

	// 10. lru.cache.compactions
	o.ObserveInt64(inst.compactionsCounter, uint64ToCounterInt64(st.CompactionsExplicit), opts.compactExplicit...)
	o.ObserveInt64(inst.compactionsCounter, uint64ToCounterInt64(st.CompactionsPressureTier1), opts.compactTier1...)
	o.ObserveInt64(inst.compactionsCounter, uint64ToCounterInt64(st.CompactionsPressureTier2), opts.compactTier2...)
	o.ObserveInt64(inst.compactionsCounter, uint64ToCounterInt64(st.CompactionsAutoSlack), opts.compactAutoSlack...)

	// 11. lru.cache.pressure_sheds
	o.ObserveInt64(inst.pressureShedsCounter, uint64ToCounterInt64(st.PressureShedsInline), opts.shedInline...)
	o.ObserveInt64(inst.pressureShedsCounter, uint64ToCounterInt64(st.PressureShedsExplicit), opts.shedExplicit...)

	// 12–14. Reclamation epoch & structural watermark gauges
	o.ObserveInt64(inst.reclaimEpochsCounter, uint64ToCounterInt64(st.ReclaimEpoch), opts.base...)
	o.ObserveInt64(inst.deletedSinceCompactGauge, clampNonNegativeInt64(st.DeletedSinceCompact), opts.base...)
	o.ObserveInt64(inst.peakEntriesGauge, clampNonNegativeInt64(st.PeakEntryLen), opts.base...)

	// 15–16. ArenaRadixCache-only instruments
	if isArena {
		o.ObserveInt64(inst.arenaNodesGauge, clampNonNegativeInt64(st.ArenaLiveNodes), opts.arenaLive...)
		o.ObserveInt64(inst.arenaNodesGauge, clampNonNegativeInt64(st.ArenaFreeNodes), opts.arenaFree...)
		o.ObserveInt64(inst.arenaNodesGauge, clampNonNegativeInt64(st.ArenaUnallocatedCap), opts.arenaUnallocatedCap...)
		o.ObserveInt64(inst.arenaHashFallbacksCounter, uint64ToCounterInt64(st.ArenaHashFallbacks), opts.base...)
	}
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

	meter := resolveMeter(&cfg)
	initialStats := cache.Stats()
	isArena := initialStats.Backend == lru.BackendArenaRadix
	baseAttrs := buildBaseAttrs(&cfg, initialStats.Backend)
	optSets := newObserveSets(baseAttrs, isArena)

	var inst cacheInstruments
	if err := inst.initLookupAndCapacityInstruments(meter); err != nil {
		return nil, err
	}
	if err := inst.initMutationAndPressureInstruments(meter); err != nil {
		return nil, err
	}
	if err := inst.initArenaInstruments(meter, isArena); err != nil {
		return nil, err
	}

	reg, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		inst.observe(o, cache.Stats(), &optSets, isArena)
		return nil
	}, inst.observables...)
	if err != nil {
		return nil, err
	}

	return &Registration{reg: reg}, nil
}
