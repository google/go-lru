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

// Package lru provides high-performance, concurrent, zero-dependency LRU cache implementations
// adapted from Google Cloud Storage FUSE (GCSFuse).
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

// ValueType represents an entry stored in a Cache that reports its logical or memory size.
// The cache uses Size() to calculate total capacity and trigger LRU eviction when capacity is exceeded.
type ValueType interface {
	// Size returns the logical or byte size of the cache entry.
	Size() uint64
}

// Cache defines the unified interface for LRU cache engines.
// All implementations must be safe for concurrent access by multiple goroutines.
type Cache interface {
	// Insert inserts or updates the given key and value in the cache.
	// If key already exists, its value is replaced and moved to the most recently used (MRU) position.
	// If the cache exceeds capacity after insertion, least recently used (LRU) entries are evicted
	// and returned in the slice.
	//
	// On error, no entries are evicted, the cache state remains unmodified, and (nil, err) is returned.
	// Returns ErrInvalidEntry if value is nil.
	// Returns ErrInvalidEntrySize if value.Size() exceeds the cache's maximum size.
	Insert(key string, value ValueType) ([]ValueType, error)

	// Erase removes the entry associated with the given key from the cache.
	// Returns the value of the erased entry, or nil if the key was not found.
	Erase(key string) (value ValueType)

	// LookUp retrieves the value associated with key and updates its position to MRU.
	// Returns nil if key is not found in the cache.
	LookUp(key string) (value ValueType)

	// LookUpWithoutChangingOrder retrieves the value associated with key without altering its LRU position.
	// Returns nil if key is not found in the cache.
	LookUpWithoutChangingOrder(key string) (value ValueType)

	// UpdateWithoutChangingOrder updates the value of an existing key without modifying its LRU position.
	//
	// Returns ErrInvalidEntry if value is nil.
	// Returns ErrEntryNotExist if key is not present in the cache.
	// Returns ErrInvalidUpdateEntrySize if value.Size() does not match the existing entry's size.
	UpdateWithoutChangingOrder(key string, value ValueType) error

	// UpdateSize adjusts the size accounting for an existing key by sizeDelta without altering its LRU position.
	// Useful for entries whose size grows incrementally (e.g. sparse files).
	// If the entry's updated size (existingSize + sizeDelta) exceeds maxSize (or cannot fit alongside
	// entries more recent than key), the entry itself is evicted immediately without evicting older entries.
	// Otherwise, if total cache capacity is exceeded, least recently used (LRU) entries are evicted
	// immediately to maintain capacity invariants.
	//
	// Returns ErrEntryNotExist if key is not present in the cache.
	// Returns ErrInvalidUpdateEntrySize if sizeDelta causes uint64 integer overflow.
	UpdateSize(key string, sizeDelta uint64) error

	// EraseEntriesWithGivenPrefix deletes all entries whose keys begin with prefix.
	// If prefix is an empty string (""), all entries in the cache are deleted.
	EraseEntriesWithGivenPrefix(prefix string)
}

// PressureAwareCache extends Cache with explicit arena/map compaction and memory-pressure reclamation.
// All three cache backends (MapCache, RadixCache, and ArenaRadixCache) implement this interface
// and perform both automatic amortized foreground reclamation (on Insert, Erase, UpdateSize,
// and EraseEntriesWithGivenPrefix) and explicit reclamation via EvaluateMemoryPressure() and Compact().
type PressureAwareCache interface {
	Cache

	// Compact performs lossless compaction of the internal node arena and/or lookup index.
	Compact()

	// EvaluateMemoryPressure samples the configured memory-pressure probe and executes
	// Tier 1 (lossless compaction) or Tier 2 (LRU tail shedding + compaction) reclamation,
	// returning any values evicted during Tier 2 shedding.
	EvaluateMemoryPressure() []ValueType
}

// New creates and returns a new LRU Cache bounded by maxSize.
// By default, New constructs a MapCache (BackendMap). Callers can select an alternative
// engine via WithBackend(BackendRadix) or WithBackend(BackendArenaRadix), or invoke
// NewMapCache, NewRadixCache, or NewArenaRadixCache directly.
//
// All returned Cache instances also implement PressureAwareCache.
//
// maxSize must be greater than zero; otherwise New panics.
func New(maxSize uint64, opts ...Option) Cache {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	options := ApplyOptions(opts...)
	switch options.Backend {
	case BackendRadix:
		return newRadixCacheWithOptions(maxSize, options)
	case BackendArenaRadix:
		return newArenaRadixCacheWithOptions(maxSize, options)
	case BackendMap:
		fallthrough
	default:
		return newMapCacheWithOptions(maxSize, options)
	}
}
