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
	"math"
	"reflect"
	"runtime/metrics"
	"sync"
)

// Weigher calculates the logical or byte weight of a cache entry.
type Weigher[V any] func(key string, value V) uint64

// EvictionReason identifies why a cache entry or value was removed or displaced.
type EvictionReason uint8

const (
	// EvictionReasonCapacity indicates an entry was evicted to respect maxSize capacity limits
	// (LRU tail eviction during Put or Replace, or self-eviction of key during Replace when its
	// updated weight exceeds maxSize or cannot fit alongside entries more recent than key).
	EvictionReasonCapacity EvictionReason = iota

	// EvictionReasonPressure indicates an entry was evicted during Tier 2 critical memory-pressure
	// shedding (both explicit EvaluateMemoryPressure() calls and inline foreground pressure shedding
	// during Put, Replace, Delete, or DeletePrefix).
	EvictionReasonPressure

	// EvictionReasonDeleted indicates an entry was explicitly removed via Delete(key) or
	// DeletePrefix(prefix) (including DeletePrefix("")).
	EvictionReasonDeleted

	// EvictionReasonReplaced indicates an existing entry's displaced old value was overwritten in place
	// when Put(key, value) or Replace(key, value) updated an existing key without evicting it.
	EvictionReasonReplaced
)

// String returns the human-readable name of the eviction reason.
func (r EvictionReason) String() string {
	switch r {
	case EvictionReasonCapacity:
		return "Capacity"
	case EvictionReasonPressure:
		return "Pressure"
	case EvictionReasonDeleted:
		return "Deleted"
	case EvictionReasonReplaced:
		return "Replaced"
	default:
		return "UnknownEvictionReason"
	}
}

// OnEvictValue is a callback invoked with the value and EvictionReason whenever a cache entry
// is evicted, deleted, or has its value replaced.
// The callback runs synchronously under the cache's exclusive write lock and must not re-enter
// the same Cache instance.
type OnEvictValue[V any] func(value V, reason EvictionReason)

// OnEvictEntry is a callback invoked with the key, value, and EvictionReason whenever a cache entry
// is evicted, deleted, or has its value replaced.
// The callback runs synchronously under the cache's exclusive write lock and must not re-enter
// the same Cache instance.
type OnEvictEntry[V any] func(key string, value V, reason EvictionReason)

// Backend identifies the underlying cache data structure engine constructed by New.
type Backend uint8

const (
	// BackendMap selects the hash-map + doubly-linked list LRU engine (MapCache).
	// This is the default backend when WithBackend is not specified.
	BackendMap Backend = iota

	// BackendRadix selects the pointer-based Left-Child Right-Sibling (LCRS) radix tree
	// LRU engine (RadixCache), optimized for hierarchical keys and fast prefix eviction.
	BackendRadix

	// BackendArenaRadix selects the contiguous-slice 32-bit index arena-backed radix tree
	// LRU engine (ArenaRadixCache) with two-tier memory-pressure reclamation.
	BackendArenaRadix
)

// String returns the human-readable name of the cache backend.
func (b Backend) String() string {
	switch b {
	case BackendMap:
		return "MapCache"
	case BackendRadix:
		return "RadixCache"
	case BackendArenaRadix:
		return "ArenaRadixCache"
	default:
		return "UnknownBackend"
	}
}

// Default thresholds for two-tier memory-pressure reclamation.
const (
	// DefaultCompactionThreshold is the normalized memory pressure [0.0, 1.0+]
	// at or above which Moderate Pressure (Tier 1) lossless arena & map compaction is triggered.
	DefaultCompactionThreshold = 0.75

	// DefaultEvictionThreshold is the normalized memory pressure [0.0, 1.0+]
	// at or above which Critical Pressure (Tier 2) proactive LRU shedding + compaction is triggered.
	DefaultEvictionThreshold = 0.90

	// DefaultEvictionRetentionRatio is the target fraction [0.0, 1.0] of maxSize
	// to retain when shedding LRU entries under Critical Pressure.
	DefaultEvictionRetentionRatio = 0.50
)

// PressureFunc returns a normalized memory pressure reading in [0.0, 1.0+],
// where 0.0 indicates no memory pressure, values in [CompactionThreshold, EvictionThreshold)
// indicate moderate pressure, and values >= EvictionThreshold indicate critical pressure.
type PressureFunc func() float64

// gomemlimitSamplePool pools 1-element runtime/metrics sample arrays so DefaultRuntimePressureFunc(0)
// can check /gc/gomemlimit:bytes without acquiring runtime.metricsLock or sweeping per-P heap stats
// when GOMEMLIMIT is unbounded.
var gomemlimitSamplePool = sync.Pool{
	New: func() any {
		return &[1]metrics.Sample{
			{Name: "/gc/gomemlimit:bytes"},
		}
	},
}

// metricsSamplePool pools 4-element runtime/metrics sample arrays so DefaultRuntimePressureFunc
// executes with 0 heap allocations per call (avoiding slice escape to heap in metrics.Read).
var metricsSamplePool = sync.Pool{
	New: func() any {
		return &[4]metrics.Sample{
			{Name: "/memory/classes/total:bytes"},
			{Name: "/memory/classes/heap/released:bytes"},
			{Name: "/memory/classes/heap/free:bytes"},
			{Name: "/memory/classes/heap/objects:bytes"},
		}
	},
}

// Options contains configuration parameters for Cache instances.
// Memory-pressure reclamation options (PressureFunc, MemoryBudget, CompactionThreshold,
// EvictionThreshold, EvictionRetentionRatio) configure both automatic amortized foreground
// reclamation (on Put, Delete, Replace, and DeletePrefix) and explicit
// EvaluateMemoryPressure() / Compact() calls across ArenaRadixCache, MapCache, and RadixCache.
type Options struct {
	// Backend selects the underlying cache engine when calling New.
	// Defaults to BackendMap.
	Backend Backend

	// Weigher holds the optional entry weigher function configured via WithWeigher.
	// When nil (or a typed-nil function), every entry defaults to a weight of 1.
	// Prefer a typed Weigher[V] / func(string, V) uint64 matching Cache[V] for zero-allocation
	// weighing; a type-erased Weigher[any] / func(string, any) uint64 is also accepted but boxes
	// non-pointer concrete V values into any on each Put and Replace.
	Weigher any

	// OnEvictValue holds the optional value eviction callback configured via WithOnEvictValue.
	// Prefer a typed OnEvictValue[V] / func(V, EvictionReason) matching Cache[V] for zero-allocation
	// invocation; a type-erased OnEvictValue[any] / func(any, EvictionReason) is also accepted.
	OnEvictValue any

	// OnEvictEntry holds the optional entry eviction callback configured via WithOnEvictEntry.
	// Prefer a typed OnEvictEntry[V] / func(string, V, EvictionReason) matching Cache[V] for zero-allocation
	// invocation; a type-erased OnEvictEntry[any] / func(string, any, EvictionReason) is also accepted.
	OnEvictEntry any

	// EnableInvariantChecking enables internal data structure integrity and invariant validation.
	// When enabled, cache operations execute comprehensive validation checks (e.g. bidirectional pointer
	// consistency, tree structure validity, size accounting parity) and panic if corruption is detected.
	// Intended primarily for testing and debugging.
	// Note: Fundamental constructor preconditions (such as requiring maxSize > 0) are enforced
	// unconditionally regardless of this setting.
	EnableInvariantChecking bool

	// PressureFunc is an optional custom callback returning normalized memory pressure in [0.0, 1.0+].
	// If nil, DefaultRuntimePressureFunc(MemoryBudget) is used.
	PressureFunc PressureFunc

	// hasCustomPressureFunc records whether PressureFunc was explicitly supplied by the caller.
	hasCustomPressureFunc bool

	// MemoryBudget specifies an optional memory limit in bytes for the default runtime/metrics
	// pressure probe. If 0, the probe falls back to the Go runtime's GOMEMLIMIT (/gc/gomemlimit:bytes).
	MemoryBudget uint64

	// CompactionThreshold specifies the normalized pressure threshold for Tier 1 lossless compaction.
	// Defaults to DefaultCompactionThreshold (0.75) if <= 0, NaN, or Inf.
	// If CompactionThreshold > EvictionThreshold, thresholds are reconciled to preserve ordering.
	CompactionThreshold          float64
	hasCustomCompactionThreshold bool
	customCompactionBits         uint64

	// EvictionThreshold specifies the normalized pressure threshold for Tier 2 LRU shedding + compaction.
	// Defaults to DefaultEvictionThreshold (0.90) if <= 0, NaN, or Inf.
	EvictionThreshold          float64
	hasCustomEvictionThreshold bool
	customEvictionBits         uint64

	// EvictionRetentionRatio specifies the fraction [0.0, 1.0] of cache maxSize to retain
	// when Critical Pressure (EvictionThreshold) is reached.
	// Defaults to DefaultEvictionRetentionRatio (0.50) if unset, negative, or NaN.
	EvictionRetentionRatio float64
}

// Option is a functional option for configuring a Cache instance.
type Option func(*Options)

// WithBackend configures the cache engine backend constructed by New.
// Supported backends: BackendMap (default), BackendRadix, BackendArenaRadix.
func WithBackend(backend Backend) Option {
	return func(o *Options) {
		o.Backend = backend
	}
}

// WithWeigher configures a custom function to compute the logical or byte weight of each cache entry
// on Put and Replace.
// If not configured (or if fn is nil), every entry defaults to a weight of 1.
//
// For zero heap allocations on hot paths, pass a function whose value parameter type V matches
// the target Cache[V]. A type-erased WithWeigher[any] is also supported for shared Options across
// caches, but binding a Weigher[any] / func(string, any) uint64 to a concrete non-pointer Cache[V]
// (V != any) boxes V into any on every Put and Replace call.
func WithWeigher[V any](fn func(key string, value V) uint64) Option {
	return func(o *Options) {
		if fn == nil {
			o.Weigher = nil
			return
		}
		o.Weigher = fn
	}
}

// isNilFunc reports whether fn is nil or a typed-nil function value wrapped in
// a non-nil interface.
func isNilFunc(fn any) bool {
	if fn == nil {
		return true
	}
	v := reflect.ValueOf(fn)
	return v.Kind() == reflect.Func && v.IsNil()
}

// isZeroValue reports whether *v equals the zero value of V. Taking a pointer
// ensures interface types V (such as any) holding nil are inspected via a valid
// reflect.Interface Value rather than an invalid zero reflect.Value.
func isZeroValue[V any](v *V) bool {
	return reflect.ValueOf(v).Elem().IsZero()
}

func resolveWeigher[V any](options Options) func(string, V) uint64 {
	if options.Weigher == nil {
		return nil
	}
	if fn, ok := options.Weigher.(func(string, V) uint64); ok {
		return fn
	}
	if fn, ok := options.Weigher.(Weigher[V]); ok {
		return fn
	}
	if fnAny, ok := options.Weigher.(func(string, any) uint64); ok {
		if fnAny == nil {
			return nil
		}
		return func(k string, v V) uint64 {
			return fnAny(k, v)
		}
	}
	if fnAny, ok := options.Weigher.(Weigher[any]); ok {
		if fnAny == nil {
			return nil
		}
		return func(k string, v V) uint64 {
			return fnAny(k, v)
		}
	}
	if isNilFunc(options.Weigher) {
		return nil
	}
	panic(fmt.Sprintf("lru: WithWeigher function type %T does not match cache value type %s", options.Weigher, reflect.TypeFor[V]()))
}

// WithOnEvictValue configures a callback invoked whenever an entry's value is evicted (due to
// capacity or critical memory pressure), explicitly deleted (via Delete or DeletePrefix), or
// displaced in place (via Put or Replace).
// Passing nil clears any previously configured OnEvictValue callback.
//
// The callback executes synchronously under the cache's exclusive write lock and must not invoke
// methods on the same Cache instance (doing so will deadlock). When both WithOnEvictValue and
// WithOnEvictEntry are configured, OnEvictValue is invoked before OnEvictEntry. Capacity and
// pressure evictions invoke callbacks in LRU-to-MRU order; DeletePrefix invokes callbacks once
// per removed entry in an unspecified order. Prefer WithOnEvictValue when the key is not needed,
// as it avoids key reconstruction overhead on RadixCache and ArenaRadixCache.
//
// For zero heap allocations on hot paths, pass a function whose value parameter type V matches
// the target Cache[V]. A type-erased WithOnEvictValue[any] is also supported for shared Options
// across caches, but binding an OnEvictValue[any] / func(any, EvictionReason) to a concrete
// non-pointer Cache[V] (V != any) boxes V into any on each invocation.
func WithOnEvictValue[V any](fn func(value V, reason EvictionReason)) Option {
	return func(o *Options) {
		if fn == nil {
			o.OnEvictValue = nil
			return
		}
		o.OnEvictValue = fn
	}
}

func resolveOnEvictValue[V any](options Options) func(V, EvictionReason) {
	if options.OnEvictValue == nil {
		return nil
	}
	if fn, ok := options.OnEvictValue.(func(V, EvictionReason)); ok {
		return fn
	}
	if fn, ok := options.OnEvictValue.(OnEvictValue[V]); ok {
		return fn
	}
	if fnAny, ok := options.OnEvictValue.(func(any, EvictionReason)); ok {
		if fnAny == nil {
			return nil
		}
		return func(v V, r EvictionReason) {
			fnAny(v, r)
		}
	}
	if fnAny, ok := options.OnEvictValue.(OnEvictValue[any]); ok {
		if fnAny == nil {
			return nil
		}
		return func(v V, r EvictionReason) {
			fnAny(v, r)
		}
	}
	if isNilFunc(options.OnEvictValue) {
		return nil
	}
	panic(fmt.Sprintf("lru: WithOnEvictValue function type %T does not match cache value type %s", options.OnEvictValue, reflect.TypeFor[V]()))
}

// WithOnEvictEntry configures a callback invoked with the entry's key, value, and EvictionReason
// whenever an entry is evicted (due to capacity or critical memory pressure), explicitly deleted
// (via Delete or DeletePrefix), or displaced in place (via Put or Replace).
// Passing nil clears any previously configured OnEvictEntry callback.
//
// The callback executes synchronously under the cache's exclusive write lock and must not invoke
// methods on the same Cache instance (doing so will deadlock). When both WithOnEvictValue and
// WithOnEvictEntry are configured, OnEvictValue is invoked before OnEvictEntry. Capacity and
// pressure evictions invoke callbacks in LRU-to-MRU order; DeletePrefix invokes callbacks once
// per removed entry in an unspecified order.
//
// For zero heap allocations on hot paths, pass a function whose value parameter type V matches
// the target Cache[V]. A type-erased WithOnEvictEntry[any] is also supported for shared Options
// across caches, but binding an OnEvictEntry[any] / func(string, any, EvictionReason) to a concrete
// non-pointer Cache[V] (V != any) boxes V into any on each invocation.
func WithOnEvictEntry[V any](fn func(key string, value V, reason EvictionReason)) Option {
	return func(o *Options) {
		if fn == nil {
			o.OnEvictEntry = nil
			return
		}
		o.OnEvictEntry = fn
	}
}

func resolveOnEvictEntry[V any](options Options) func(string, V, EvictionReason) {
	if options.OnEvictEntry == nil {
		return nil
	}
	if fn, ok := options.OnEvictEntry.(func(string, V, EvictionReason)); ok {
		return fn
	}
	if fn, ok := options.OnEvictEntry.(OnEvictEntry[V]); ok {
		return fn
	}
	if fnAny, ok := options.OnEvictEntry.(func(string, any, EvictionReason)); ok {
		if fnAny == nil {
			return nil
		}
		return func(k string, v V, r EvictionReason) {
			fnAny(k, v, r)
		}
	}
	if fnAny, ok := options.OnEvictEntry.(OnEvictEntry[any]); ok {
		if fnAny == nil {
			return nil
		}
		return func(k string, v V, r EvictionReason) {
			fnAny(k, v, r)
		}
	}
	if isNilFunc(options.OnEvictEntry) {
		return nil
	}
	panic(fmt.Sprintf("lru: WithOnEvictEntry function type %T does not match cache value type %s", options.OnEvictEntry, reflect.TypeFor[V]()))
}

// WithInvariantChecking returns an Option that enables or disables internal invariant checking.
func WithInvariantChecking(enabled bool) Option {
	return func(o *Options) {
		o.EnableInvariantChecking = enabled
	}
}

// WithPressureFunc configures a custom memory-pressure probe function.
func WithPressureFunc(fn PressureFunc) Option {
	return func(o *Options) {
		o.PressureFunc = fn
		o.hasCustomPressureFunc = (fn != nil)
	}
}

// WithMemoryBudget configures a process-wide runtime memory budget ceiling in bytes for the built-in
// runtime/metrics probe when GOMEMLIMIT is unset or higher than the desired process ceiling.
func WithMemoryBudget(bytes uint64) Option {
	return func(o *Options) {
		o.MemoryBudget = bytes
	}
}

func isValidPositiveThreshold(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v > 0
}

// WithCompactionThreshold configures the Moderate Pressure threshold for lossless arena/map compaction.
// Passing a non-positive, NaN, or infinite value resets CompactionThreshold to DefaultCompactionThreshold.
func WithCompactionThreshold(threshold float64) Option {
	return func(o *Options) {
		if !isValidPositiveThreshold(threshold) {
			o.CompactionThreshold = DefaultCompactionThreshold
			o.hasCustomCompactionThreshold = false
			o.customCompactionBits = 0
			return
		}
		o.CompactionThreshold = threshold
		o.hasCustomCompactionThreshold = true
		o.customCompactionBits = math.Float64bits(threshold)
	}
}

// WithEvictionThreshold configures the Critical Pressure threshold for proactive LRU shedding.
// Passing a non-positive, NaN, or infinite value resets EvictionThreshold to DefaultEvictionThreshold.
func WithEvictionThreshold(threshold float64) Option {
	return func(o *Options) {
		if !isValidPositiveThreshold(threshold) {
			o.EvictionThreshold = DefaultEvictionThreshold
			o.hasCustomEvictionThreshold = false
			o.customEvictionBits = 0
			return
		}
		o.EvictionThreshold = threshold
		o.hasCustomEvictionThreshold = true
		o.customEvictionBits = math.Float64bits(threshold)
	}
}

// WithEvictionRetentionRatio configures the target retention ratio [0.0, 1.0] of maxSize
// retained during Critical Pressure LRU shedding.
func WithEvictionRetentionRatio(ratio float64) Option {
	return func(o *Options) {
		o.EvictionRetentionRatio = ratio
	}
}

// DefaultRuntimePressureFunc returns a lock-free, zero-allocation, STW-free PressureFunc backed by runtime/metrics.
// It compares active Go runtime memory (/memory/classes/total:bytes - (/memory/classes/heap/released:bytes + /memory/classes/heap/free:bytes))
// against memoryBudget (if > 0) or /gc/gomemlimit:bytes (if configured < math.MaxInt64).
func DefaultRuntimePressureFunc(memoryBudget uint64) PressureFunc {
	return func() float64 {
		limitBytes := memoryBudget
		if limitBytes == 0 {
			limitSample := gomemlimitSamplePool.Get().(*[1]metrics.Sample)
			metrics.Read(limitSample[:])
			gomemlimit := limitSample[0].Value.Uint64()
			gomemlimitSamplePool.Put(limitSample)
			if gomemlimit == 0 || gomemlimit >= uint64(math.MaxInt64) {
				return 0.0
			}
			limitBytes = gomemlimit
		}

		samples := metricsSamplePool.Get().(*[4]metrics.Sample)
		metrics.Read(samples[:])

		totalBytes := samples[0].Value.Uint64()
		releasedBytes := samples[1].Value.Uint64()
		heapFreeBytes := samples[2].Value.Uint64()
		heapObjectsBytes := samples[3].Value.Uint64()
		metricsSamplePool.Put(samples)

		var usedBytes uint64
		reclaimedOrFree := releasedBytes + heapFreeBytes
		if totalBytes > reclaimedOrFree {
			usedBytes = totalBytes - reclaimedOrFree
		} else {
			usedBytes = heapObjectsBytes
		}

		return float64(usedBytes) / float64(limitBytes)
	}
}

func sanitizeThresholds(options *Options) {
	if options.hasCustomCompactionThreshold && math.Float64bits(options.CompactionThreshold) != options.customCompactionBits {
		options.hasCustomCompactionThreshold = false
	}
	if options.hasCustomEvictionThreshold && math.Float64bits(options.EvictionThreshold) != options.customEvictionBits {
		options.hasCustomEvictionThreshold = false
	}
	if !isValidPositiveThreshold(options.CompactionThreshold) {
		options.CompactionThreshold = DefaultCompactionThreshold
		options.hasCustomCompactionThreshold = false
	}
	if !isValidPositiveThreshold(options.EvictionThreshold) {
		options.EvictionThreshold = DefaultEvictionThreshold
		options.hasCustomEvictionThreshold = false
	}
}

func advanceEvictionThreshold(compaction float64) float64 {
	eviction := compaction + (DefaultEvictionThreshold - DefaultCompactionThreshold)
	if math.IsInf(eviction, 1) {
		eviction = math.MaxFloat64
	}
	if eviction <= compaction {
		if compaction < math.MaxFloat64 {
			return math.Nextafter(compaction, math.MaxFloat64)
		}
		return compaction
	}
	return eviction
}

func scaleCompactionThreshold(eviction float64) float64 {
	compaction := eviction * (DefaultCompactionThreshold / DefaultEvictionThreshold)
	if compaction == 0 {
		compaction = math.SmallestNonzeroFloat64
	}
	if compaction >= eviction && eviction > math.SmallestNonzeroFloat64 {
		compaction = math.Nextafter(eviction, 0)
	}
	return compaction
}

func reconcileThresholdWindow(options *Options) {
	if options.CompactionThreshold < options.EvictionThreshold {
		return
	}
	if options.CompactionThreshold == options.EvictionThreshold && options.hasCustomCompactionThreshold == options.hasCustomEvictionThreshold {
		return
	}
	if options.hasCustomCompactionThreshold && !options.hasCustomEvictionThreshold && options.EvictionThreshold == DefaultEvictionThreshold {
		// Caller raised CompactionThreshold at or above default EvictionThreshold; advance EvictionThreshold
		// to preserve the Tier 1 compaction window.
		options.EvictionThreshold = advanceEvictionThreshold(options.CompactionThreshold)
		return
	}
	// Scale CompactionThreshold proportionally below EvictionThreshold to preserve a non-empty Tier 1 window.
	options.CompactionThreshold = scaleCompactionThreshold(options.EvictionThreshold)
}

func normalizeRetentionRatio(ratio float64) float64 {
	switch {
	case math.IsNaN(ratio) || math.IsInf(ratio, -1) || ratio < 0:
		return DefaultEvictionRetentionRatio
	case math.IsInf(ratio, 1) || ratio > 1.0:
		return 1.0
	case ratio == 0:
		return 0.0
	default:
		return ratio
	}
}

func normalizeCallbacks(options *Options) {
	if isNilFunc(options.Weigher) {
		options.Weigher = nil
	}
	if isNilFunc(options.OnEvictValue) {
		options.OnEvictValue = nil
	}
	if isNilFunc(options.OnEvictEntry) {
		options.OnEvictEntry = nil
	}
	if options.PressureFunc == nil {
		options.PressureFunc = DefaultRuntimePressureFunc(options.MemoryBudget)
		options.hasCustomPressureFunc = false
	} else {
		options.hasCustomPressureFunc = true
	}
}

// ApplyOptions parses and applies the provided slice of Option functions onto a default Options configuration.
func ApplyOptions(opts ...Option) Options {
	options := Options{
		Backend:                BackendMap,
		CompactionThreshold:    DefaultCompactionThreshold,
		EvictionThreshold:      DefaultEvictionThreshold,
		EvictionRetentionRatio: DefaultEvictionRetentionRatio,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	if options.Backend != BackendMap && options.Backend != BackendRadix && options.Backend != BackendArenaRadix {
		options.Backend = BackendMap
	}
	sanitizeThresholds(&options)
	reconcileThresholdWindow(&options)
	options.EvictionRetentionRatio = normalizeRetentionRatio(options.EvictionRetentionRatio)
	normalizeCallbacks(&options)
	return options
}
