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

// Cache defines the unified generic interface for LRU cache engines.
// All implementations are safe for concurrent access by multiple goroutines.
type Cache[V any] interface {
	// Insert inserts or updates the given key and value in the cache.
	// If key already exists, its value is replaced, its weight is recomputed via the configured
	// weigher (or defaults to 1), and the entry is moved to the most recently used (MRU) position.
	// If the cache exceeds capacity after insertion, least recently used (LRU) entries are evicted
	// and returned in the slice.
	//
	// On error, no entries are evicted, the cache state remains unmodified, and (nil, err) is returned.
	// Returns ErrInvalidEntrySize if the entry's weight exceeds the cache's maximum size.
	Insert(key string, value V) ([]V, error)

	// Erase removes the entry associated with the given key from the cache.
	// Returns the value of the erased entry and true if found, or the zero value of V and false if not found.
	Erase(key string) (value V, ok bool)

	// LookUp retrieves the value associated with key and updates its position to MRU.
	// Returns the zero value of V and false if key is not found in the cache.
	LookUp(key string) (value V, ok bool)

	// LookUpWithoutChangingOrder retrieves the value associated with key without altering its LRU position.
	// Returns the zero value of V and false if key is not found in the cache.
	LookUpWithoutChangingOrder(key string) (value V, ok bool)

	// UpdateWithoutChangingOrder updates the value of an existing key and recomputes its weight
	// using the configured weigher (or 1 if no weigher is configured) without modifying its LRU position.
	//
	// If the updated entry's weight is smaller than its previous weight, currentSize is reduced accordingly.
	// If the updated entry's weight is larger than its previous weight, older LRU entries are evicted as
	// needed to fit within maxSize, or key itself is evicted if its new weight exceeds maxSize or cannot
	// fit alongside entries more recent than key.
	//
	// Returns ErrEntryNotExist if key is not present in the cache.
	UpdateWithoutChangingOrder(key string, value V) error

	// EraseEntriesWithGivenPrefix deletes all entries whose keys begin with prefix.
	// If prefix is an empty string (""), all entries in the cache are deleted.
	EraseEntriesWithGivenPrefix(prefix string)
}

// PressureAwareCache extends Cache[V] with explicit arena/map compaction and memory-pressure reclamation.
// All three cache backends (MapCache, RadixCache, and ArenaRadixCache) implement this interface
// and perform both automatic amortized foreground reclamation (on Insert, Erase, UpdateWithoutChangingOrder,
// and EraseEntriesWithGivenPrefix) and explicit reclamation via EvaluateMemoryPressure() and Compact().
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
