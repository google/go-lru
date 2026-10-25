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
	"iter"
	"math"
	"sync/atomic"
)

// shardEngine is implemented by all concrete single-shard backends (*mapCache[V],
// *radixCache[V], and *arenaRadix[V]), enabling zero-allocation cross-shard
// iteration and telemetry aggregation without per-call method-value closures.
type shardEngine[V any] interface {
	PressureAwareCache[V]
	pressureStatePtr() *pressureState
	iterateAll(yield func(string, V) bool) bool
	iterateKeys(yield func(string) bool) bool
	iterateValues(yield func(V) bool) bool
	deletePrefixShard(evictQ *evictCallbackQueue[V], prefix string, sample shardSample) (shardSample, bool)
	evaluateShardPressure(evictQ *evictCallbackQueue[V], sample shardSample) ([]V, shardSample)
}

// DefaultShardFunc computes a zero-allocation 64-bit FNV-1a hash of key and maps it
// into [0, numShards).
//
// numShards must be greater than zero; otherwise DefaultShardFunc panics.
func DefaultShardFunc(key string, numShards int) int {
	if numShards <= 0 {
		panic("numShards must be greater than zero")
	}
	if numShards == 1 {
		return 0
	}
	h := hashString(key)
	if (numShards & (numShards - 1)) == 0 {
		return int(h & uint64(numShards-1))
	}
	return int(h % uint64(numShards))
}

// shardedCache partitions keys and capacity across N independent single-backend
// LRU cache shards to eliminate lock contention under high-concurrency workloads.
//
// Each shard maintains its own independent capacity quota, recency order, mutex,
// and compaction watermarks, while c.coord coordinates cache-wide memory-pressure
// sampling, reclamation epochs, and serialized eviction callback delivery.
type shardedCache[V any] struct {
	shards                  []shardEngine[V]
	shardFn                 ShardFunc
	onEvictValue            func(value V, reason EvictionReason)
	onEvictEntry            func(key string, value V, reason EvictionReason)
	maxSize                 uint64
	backend                 Backend
	enableInvariantChecking bool

	// Cache-line padding isolates read-only hot-path routing fields (shards, shardFn)
	// from atomic counters and coordinator state mutated during DeletePrefix, eviction
	// callbacks, or memory-pressure sampling.
	_                    [64]byte
	deletePrefixExecuted atomic.Uint64
	_                    [64]byte
	coord                pressureState
}

// NewShardedCache creates and returns a sharded LRU Cache[V] that partitions maxSize
// across shards independent instances of the configured Backend (BackendMap by default,
// or BackendRadix / BackendArenaRadix via WithBackend). The explicit shards parameter
// overrides any WithShards option supplied in opts.
//
// Total capacity maxSize is distributed evenly across shards: each shard receives
// maxSize / uint64(shards), with the first maxSize % uint64(shards) shards receiving +1
// so the sum of shard capacities equals maxSize. Because capacity is enforced per shard,
// Put returns ErrInvalidEntrySize when an entry's weight exceeds its target shard's capacity.
//
// Keys are routed via shardFn(key, shards). If shardFn is nil, the function configured via
// WithShardFunc is used, or DefaultShardFunc if neither is provided.
//
// All returned instances also implement PressureAwareCache[V]. Eviction callbacks are
// serialized per cache across all shards (supporting safe cross-shard re-entrant calls),
// and multi-shard operations (DeletePrefix, EvaluateMemoryPressure) invoke eviction
// callbacks after all shard mutations have completed.
// Iteration methods (All, Keys, Values) visit shards sequentially from shard 0 to shards-1,
// yielding each shard's entries in MRU-to-LRU order while holding at most one shard's RLock
// at a time.
//
// Panics if maxSize == 0, shards <= 0, or uint64(shards) > maxSize.
func NewShardedCache[V any](maxSize uint64, shards int, shardFn ShardFunc, opts ...Option) Cache[V] {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	validateShards(maxSize, shards)
	options := ApplyOptions(opts...)
	options.Shards = shards
	options.hasCustomShards = true
	if shardFn != nil {
		options.ShardFunc = shardFn
	}
	return newShardedCacheWithOptions[V](maxSize, shards, options)
}

func validateShards(maxSize uint64, shards int) {
	if shards <= 0 {
		panic("shards must be greater than zero")
	}
	if uint64(shards) > maxSize {
		panic("shards must not exceed maxSize")
	}
}

func newSingleShardEngine[V any](maxSize uint64, options Options) shardEngine[V] {
	switch options.Backend {
	case BackendRadix:
		return newRadixCacheWithOptions[V](maxSize, options)
	case BackendArenaRadix:
		return newArenaRadixCacheWithOptions[V](maxSize, options)
	case BackendMap:
		fallthrough
	default:
		return newMapCacheWithOptions[V](maxSize, options)
	}
}

func newShardedCacheWithOptions[V any](maxSize uint64, shards int, options Options) *shardedCache[V] {
	validateShards(maxSize, shards)
	shardFn := options.ShardFunc
	if shardFn == nil {
		shardFn = DefaultShardFunc
	}

	coordOpts := options
	coordOpts.Shards = 0
	coordOpts.hasCustomShards = false
	coordOpts.sharedPressure = nil
	coordOpts.hasShardTier2Target = false
	coordOpts.shardTier2Target = 0

	c := &shardedCache[V]{
		shards:                  make([]shardEngine[V], shards),
		shardFn:                 shardFn,
		onEvictValue:            resolveOnEvictValue[V](options),
		onEvictEntry:            resolveOnEvictEntry[V](options),
		maxSize:                 maxSize,
		backend:                 options.Backend,
		enableInvariantChecking: options.EnableInvariantChecking,
		coord:                   pressureState{options: coordOpts},
	}

	shardOpts := coordOpts
	shardOpts.sharedPressure = &c.coord
	shardOpts.hasShardTier2Target = true

	globalTarget := computeTargetSize(maxSize, options.EvictionRetentionRatio)
	baseTarget := globalTarget / uint64(shards)
	targetRem := globalTarget % uint64(shards)

	c.coord.childShards = make([]*pressureState, shards)
	baseCap := maxSize / uint64(shards)
	rem := maxSize % uint64(shards)
	for i := range shards {
		shardMax := baseCap
		if uint64(i) < rem {
			shardMax++
		}
		shardTarget := baseTarget
		if uint64(i) < targetRem {
			shardTarget++
		}
		shardOpts.shardTier2Target = shardTarget
		s := newSingleShardEngine[V](shardMax, shardOpts)
		c.shards[i] = s
		c.coord.childShards[i] = s.pressureStatePtr()
	}

	if c.enableInvariantChecking {
		c.checkInvariants()
	}
	return c
}

func (c *shardedCache[V]) shardFor(key string) shardEngine[V] {
	idx := c.shardFn(key, len(c.shards))
	if uint(idx) >= uint(len(c.shards)) {
		panic(fmt.Sprintf("lru: ShardFunc returned shard index %d out of range [0, %d)", idx, len(c.shards)))
	}
	return c.shards[idx]
}

// Put inserts or updates key and value in the target shard determined by ShardFunc.
func (c *shardedCache[V]) Put(key string, value V) ([]V, error) {
	return c.shardFor(key).Put(key, value)
}

// Delete removes key from its target shard.
func (c *shardedCache[V]) Delete(key string) (V, bool) {
	return c.shardFor(key).Delete(key)
}

// Get retrieves key from its target shard and promotes it to MRU within that shard.
func (c *shardedCache[V]) Get(key string) (V, bool) {
	return c.shardFor(key).Get(key)
}

// Peek retrieves key from its target shard without modifying its LRU recency order.
func (c *shardedCache[V]) Peek(key string) (V, bool) {
	return c.shardFor(key).Peek(key)
}

// Replace updates an existing key's value and weight in its target shard without modifying recency.
func (c *shardedCache[V]) Replace(key string, value V) error {
	return c.shardFor(key).Replace(key, value)
}

// DeletePrefix deletes all entries whose keys begin with prefix across all shards,
// invoking any configured eviction callbacks after all shard mutations have completed.
func (c *shardedCache[V]) DeletePrefix(prefix string) {
	var evictQ evictCallbackQueue[V]
	executed := false
	defer func() {
		if executed {
			c.deletePrefixExecuted.Add(1)
		}
		evictQ.invoke(&c.coord, c.onEvictValue, c.onEvictEntry)
	}()
	var sample shardSample
	for _, s := range c.shards {
		var didExec bool
		sample, didExec = s.deletePrefixShard(&evictQ, prefix, sample)
		if didExec {
			executed = true
		}
	}
	executed = true
}

// All returns an iterator over all live key-value pairs across all shards, visiting shards
// sequentially and yielding each shard's entries in MRU-to-LRU order under that shard's RLock.
func (c *shardedCache[V]) All() iter.Seq2[string, V] {
	return c.all
}

func (c *shardedCache[V]) all(yield func(string, V) bool) {
	for _, s := range c.shards {
		if !s.iterateAll(yield) {
			return
		}
	}
}

// Keys returns an iterator over all live keys across all shards, visiting shards
// sequentially and yielding each shard's keys in MRU-to-LRU order under that shard's RLock.
func (c *shardedCache[V]) Keys() iter.Seq[string] {
	return c.keys
}

func (c *shardedCache[V]) keys(yield func(string) bool) {
	for _, s := range c.shards {
		if !s.iterateKeys(yield) {
			return
		}
	}
}

// Values returns a zero-allocation iterator over all live values across all shards, visiting
// shards sequentially and yielding each shard's values in MRU-to-LRU order under that shard's RLock.
func (c *shardedCache[V]) Values() iter.Seq[V] {
	return c.values
}

func (c *shardedCache[V]) values(yield func(V) bool) {
	for _, s := range c.shards {
		if !s.iterateValues(yield) {
			return
		}
	}
}

// Compact performs lossless compaction across all shards sequentially.
func (c *shardedCache[V]) Compact() {
	for _, s := range c.shards {
		s.Compact()
	}
}

// EvaluateMemoryPressure evaluates memory pressure once and executes Tier 1 or Tier 2 reclamation
// across all shards sequentially, invoking eviction callbacks after all shard mutations complete
// and returning all values evicted during Tier 2 shedding.
func (c *shardedCache[V]) EvaluateMemoryPressure() []V {
	var evicted []V
	var evictQ evictCallbackQueue[V]
	defer evictQ.invoke(&c.coord, c.onEvictValue, c.onEvictEntry)
	var sample shardSample
	for _, s := range c.shards {
		var shardEvicted []V
		shardEvicted, sample = s.evaluateShardPressure(&evictQ, sample)
		evicted = appendEvicted(evicted, shardEvicted)
	}
	return evicted
}

// Stats aggregates point-in-time telemetry snapshots across all shards with zero heap allocations.
func (c *shardedCache[V]) Stats() Stats {
	st := Stats{
		Backend:              c.backend,
		DeletePrefixExecuted: c.deletePrefixExecuted.Load(),
	}
	for _, s := range c.shards {
		shardStats := s.Stats()
		addShardStats(&st, &shardStats)
	}
	st.MemoryPressure = math.Float64frombits(c.coord.lastSampledPressureBits.Load())
	st.ReclaimEpoch = c.coord.reclaimEpoch.Load()
	if c.enableInvariantChecking {
		c.checkAggregateStatsInvariants(&st)
	}
	return st
}

func addShardStats(dst, src *Stats) {
	dst.GetHits += src.GetHits
	dst.GetMisses += src.GetMisses
	dst.PeekHits += src.PeekHits
	dst.PeekMisses += src.PeekMisses

	dst.EvictionsCapacity += src.EvictionsCapacity
	dst.EvictionsPressure += src.EvictionsPressure
	dst.EvictionsDeleted += src.EvictionsDeleted
	dst.EvictionsReplaced += src.EvictionsReplaced
	dst.EvictedWeightCapacity += src.EvictedWeightCapacity
	dst.EvictedWeightPressure += src.EvictedWeightPressure
	dst.EvictedWeightDeleted += src.EvictedWeightDeleted
	dst.EvictedWeightReplaced += src.EvictedWeightReplaced

	dst.CurrentSize += src.CurrentSize
	dst.MaxSize += src.MaxSize
	dst.Len += src.Len
	dst.ZeroSizeCount += src.ZeroSizeCount

	dst.PutInserted += src.PutInserted
	dst.PutUpdated += src.PutUpdated
	dst.PutRejectedOversized += src.PutRejectedOversized
	dst.ReplaceUpdated += src.ReplaceUpdated
	dst.ReplaceNotFound += src.ReplaceNotFound
	dst.ReplaceSelfEvicted += src.ReplaceSelfEvicted
	dst.DeleteDeleted += src.DeleteDeleted
	dst.DeleteNotFound += src.DeleteNotFound

	dst.CompactionsExplicit += src.CompactionsExplicit
	dst.CompactionsPressureTier1 += src.CompactionsPressureTier1
	dst.CompactionsPressureTier2 += src.CompactionsPressureTier2
	dst.CompactionsAutoSlack += src.CompactionsAutoSlack
	dst.PressureShedsInline += src.PressureShedsInline
	dst.PressureShedsExplicit += src.PressureShedsExplicit

	dst.DeletedSinceCompact += src.DeletedSinceCompact
	dst.PeakEntryLen += src.PeakEntryLen
	dst.ArenaLiveNodes += src.ArenaLiveNodes
	dst.ArenaFreeNodes += src.ArenaFreeNodes
	dst.ArenaUnallocatedCap += src.ArenaUnallocatedCap
	dst.ArenaHashFallbacks += src.ArenaHashFallbacks
}

func (c *shardedCache[V]) checkInvariants() {
	if len(c.shards) == 0 || uint64(len(c.shards)) > c.maxSize {
		panic(fmt.Sprintf("shardedCache invariant violation: invalid shard count %d for maxSize %d", len(c.shards), c.maxSize))
	}
	if c.shardFn == nil {
		panic("shardedCache invariant violation: nil shardFn")
	}
	_ = c.Stats()
}

func (c *shardedCache[V]) checkAggregateStatsInvariants(st *Stats) {
	c.checkCapacityAndWatermarkInvariants(st)
	c.checkCounterConservationInvariants(st)
}

func (c *shardedCache[V]) checkCapacityAndWatermarkInvariants(st *Stats) {
	if st.MaxSize != c.maxSize {
		panic(fmt.Sprintf("shardedCache invariant violation: aggregate MaxSize %d != %d", st.MaxSize, c.maxSize))
	}
	if st.CurrentSize > st.MaxSize {
		panic(fmt.Sprintf("shardedCache invariant violation: aggregate CurrentSize %d > MaxSize %d", st.CurrentSize, st.MaxSize))
	}
	if st.ZeroSizeCount < 0 || st.ZeroSizeCount > st.Len {
		panic(fmt.Sprintf("shardedCache invariant violation: invalid ZeroSizeCount %d for Len %d", st.ZeroSizeCount, st.Len))
	}
	if st.DeletedSinceCompact < 0 || st.PeakEntryLen < 0 {
		panic(fmt.Sprintf("shardedCache invariant violation: negative watermark DeletedSinceCompact=%d PeakEntryLen=%d", st.DeletedSinceCompact, st.PeakEntryLen))
	}
	if math.IsNaN(st.MemoryPressure) || math.IsInf(st.MemoryPressure, 0) || st.MemoryPressure < 0.0 {
		panic(fmt.Sprintf("shardedCache invariant violation: invalid MemoryPressure %v", st.MemoryPressure))
	}
}

func (c *shardedCache[V]) checkCounterConservationInvariants(st *Stats) {
	if st.PutUpdated+st.ReplaceUpdated != st.EvictionsReplaced {
		panic(fmt.Sprintf("shardedCache invariant violation: PutUpdated (%d) + ReplaceUpdated (%d) != EvictionsReplaced (%d)", st.PutUpdated, st.ReplaceUpdated, st.EvictionsReplaced))
	}
	totalRemovals := st.EvictionsCapacity + st.EvictionsPressure + st.EvictionsDeleted
	if uint64(st.Len)+totalRemovals != st.PutInserted {
		panic(fmt.Sprintf("shardedCache invariant violation: Len (%d) + totalRemovals (%d) != PutInserted (%d)", st.Len, totalRemovals, st.PutInserted))
	}
	if st.DeleteDeleted > st.EvictionsDeleted {
		panic(fmt.Sprintf("shardedCache invariant violation: DeleteDeleted %d > EvictionsDeleted %d", st.DeleteDeleted, st.EvictionsDeleted))
	}
	if st.ReplaceSelfEvicted > st.EvictionsCapacity {
		panic(fmt.Sprintf("shardedCache invariant violation: ReplaceSelfEvicted %d > EvictionsCapacity %d", st.ReplaceSelfEvicted, st.EvictionsCapacity))
	}
	if st.PressureShedsInline+st.PressureShedsExplicit > st.EvictionsPressure {
		panic(fmt.Sprintf("shardedCache invariant violation: PressureSheds (%d) > EvictionsPressure (%d)", st.PressureShedsInline+st.PressureShedsExplicit, st.EvictionsPressure))
	}
}
