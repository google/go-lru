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

// Package lru provides high-performance, concurrent, zero-dependency LRU cache implementations.
//
// The package defines a unified Cache interface satisfied by three specialized engines:
//   - MapCache: A standard doubly-linked list + map implementation with O(1) operations.
//   - RadixCache: A Left-Child Right-Sibling (LCRS) radix tree with intrusive LRU pointers.
//   - ArenaRadixCache: A flat-slice arena-allocated radix tree with 32-bit node indices that
//     eliminate internal tree pointer traversal during garbage collection, while stored values
//     and prefixes retain standard Go GC properties.
//
// All cache constructors require maxSize > 0 and unconditionally panic if maxSize == 0.
package lru

import "iter"

// Cache defines the unified generic interface for LRU cache engines.
// All implementations are safe for concurrent access by multiple goroutines.
type Cache[V any] interface {
	// Put inserts or updates the given key and value in the cache.
	// If key already exists, its value is replaced, its weight is recomputed via the configured
	// weigher (or defaults to 1), and the entry is moved to the most recently used (MRU) position.
	// If the cache exceeds capacity after insertion, least recently used (LRU) entries are evicted
	// and returned in the slice.
	//
	// On error, no entries are evicted, the cache state remains unmodified, and (nil, err) is returned.
	// Returns ErrInvalidEntrySize if the entry's weight exceeds the cache's maximum size.
	Put(key string, value V) ([]V, error)

	// Delete removes the entry associated with the given key from the cache.
	// Returns the value of the deleted entry and true if found, or the zero value of V and false if not found.
	Delete(key string) (value V, ok bool)

	// Get retrieves the value associated with key and updates its position to MRU.
	// Returns the zero value of V and false if key is not found in the cache.
	Get(key string) (value V, ok bool)

	// Peek retrieves the value associated with key without altering its LRU position.
	// Returns the zero value of V and false if key is not found in the cache.
	Peek(key string) (value V, ok bool)

	// Replace updates the value of an existing key and recomputes its weight
	// using the configured weigher (or 1 if no weigher is configured) without modifying its LRU position.
	//
	// If the updated entry's weight is smaller than its previous weight, currentSize is reduced accordingly.
	// If the updated entry's weight is larger than its previous weight, older LRU entries are evicted as
	// needed to fit within maxSize, or key itself is evicted if its new weight exceeds maxSize or cannot
	// fit alongside entries more recent than key.
	//
	// Returns ErrEntryNotExist if key is not present in the cache.
	Replace(key string, value V) error

	// DeletePrefix deletes all entries whose keys begin with prefix.
	// If prefix is an empty string (""), all entries in the cache are deleted.
	DeletePrefix(prefix string)

	// All returns an iterator over all live key-value pairs in deterministic
	// most-recently-used (MRU) to least-recently-used (LRU) order.
	//
	// Iteration acquires a read lock (RLock) for the duration of the loop, does not
	// alter LRU recency order, and supports early termination (break). Callers must
	// not invoke write-locking methods (Put, Get, Delete, Replace, DeletePrefix,
	// Compact, EvaluateMemoryPressure) on the same Cache while iterating.
	All() iter.Seq2[string, V]

	// Keys returns an iterator over all live keys in deterministic MRU-to-LRU order.
	//
	// Iteration acquires a read lock (RLock) for the duration of the loop, does not
	// alter LRU recency order, and supports early termination (break). Callers must
	// not invoke write-locking methods on the same Cache while iterating.
	Keys() iter.Seq[string]

	// Values returns an iterator over all live values in deterministic MRU-to-LRU order.
	//
	// Iteration acquires a read lock (RLock) for the duration of the loop, does not
	// alter LRU recency order, skips key reconstruction on radix backends (0 allocs/op),
	// and supports early termination (break). Callers must not invoke write-locking
	// methods on the same Cache while iterating.
	Values() iter.Seq[V]

	// Stats returns a point-in-time telemetry snapshot of the cache's lookups,
	// mutations, evictions, capacity, memory-pressure compactions, and structural state.
	Stats() Stats
}

// StatsProvider is a non-generic interface implemented by all Cache[V] instances,
// allowing observability integrations (such as otellru) to read telemetry snapshots
// without depending on the cache's value type parameter V.
type StatsProvider interface {
	Stats() Stats
}

// Stats captures a point-in-time, zero-allocation telemetry snapshot of a Cache instance.
type Stats struct {
	// Backend identifies the underlying cache engine (BackendMap, BackendRadix, or BackendArenaRadix).
	Backend Backend

	// 1. Lookups
	GetHits    uint64
	GetMisses  uint64
	PeekHits   uint64
	PeekMisses uint64

	// 2. Evictions & Removals (entry counts and total weight by EvictionReason)
	EvictionsCapacity     uint64
	EvictionsPressure     uint64
	EvictionsDeleted      uint64
	EvictionsReplaced     uint64
	EvictedWeightCapacity uint64
	EvictedWeightPressure uint64
	EvictedWeightDeleted  uint64
	EvictedWeightReplaced uint64

	// 3. Capacity & State
	CurrentSize   uint64
	MaxSize       uint64
	Len           int
	ZeroSizeCount int

	// 4. Mutations & Outcomes
	PutInserted          uint64
	PutUpdated           uint64
	PutRejectedOversized uint64
	ReplaceUpdated       uint64
	ReplaceNotFound      uint64
	ReplaceSelfEvicted   uint64
	DeleteDeleted        uint64
	DeleteNotFound       uint64
	DeletePrefixExecuted uint64

	// 5. Two-Tier Memory Pressure & Compaction
	MemoryPressure           float64
	CompactionsExplicit      uint64
	CompactionsPressureTier1 uint64
	CompactionsPressureTier2 uint64
	CompactionsAutoSlack     uint64
	PressureShedsInline      uint64
	PressureShedsExplicit    uint64
	ReclaimEpoch             uint64

	// 6. Backend Structural & Fragmentation State
	DeletedSinceCompact int
	PeakEntryLen        int
	ArenaLiveNodes      int
	ArenaFreeNodes      int
	ArenaUnallocatedCap int
	ArenaHashFallbacks  uint64
}

// Evictions returns the cumulative eviction/removal entry count for the given EvictionReason.
func (s Stats) Evictions(reason EvictionReason) uint64 {
	switch reason {
	case EvictionReasonCapacity:
		return s.EvictionsCapacity
	case EvictionReasonPressure:
		return s.EvictionsPressure
	case EvictionReasonDeleted:
		return s.EvictionsDeleted
	case EvictionReasonReplaced:
		return s.EvictionsReplaced
	default:
		return 0
	}
}

// EvictedWeight returns the cumulative evicted/removed entry weight for the given EvictionReason.
func (s Stats) EvictedWeight(reason EvictionReason) uint64 {
	switch reason {
	case EvictionReasonCapacity:
		return s.EvictedWeightCapacity
	case EvictionReasonPressure:
		return s.EvictedWeightPressure
	case EvictionReasonDeleted:
		return s.EvictedWeightDeleted
	case EvictionReasonReplaced:
		return s.EvictedWeightReplaced
	default:
		return 0
	}
}

// PressureAwareCache extends Cache[V] with explicit arena/map compaction and memory-pressure reclamation.
// All three cache backends (MapCache, RadixCache, and ArenaRadixCache) implement this interface
// and perform both automatic amortized foreground reclamation (on Put, Delete, Replace,
// and DeletePrefix) and explicit reclamation via EvaluateMemoryPressure() and Compact().
type PressureAwareCache[V any] interface {
	Cache[V]

	// Compact performs lossless compaction of the internal node arena and/or lookup index.
	Compact()

	// EvaluateMemoryPressure samples the configured memory-pressure probe and executes
	// Tier 1 (lossless compaction) or Tier 2 (LRU tail shedding + compaction) reclamation,
	// returning any values evicted during Tier 2 shedding.
	EvaluateMemoryPressure() []V
}

// New creates and returns a new generic LRU Cache[V] bounded by maxSize.
// By default, New constructs a MapCache (BackendMap) and assigns each entry a default weight of 1
// unless a custom weigher is provided via WithWeigher. Callers can select an alternative
// engine via WithBackend(BackendRadix) or WithBackend(BackendArenaRadix), or invoke
// NewMapCache, NewRadixCache, or NewArenaRadixCache directly.
//
// All returned Cache[V] instances also implement PressureAwareCache[V].
//
// maxSize must be greater than zero; otherwise New panics.
func New[V any](maxSize uint64, opts ...Option) Cache[V] {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	options := ApplyOptions(opts...)
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
