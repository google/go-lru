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

package lrus

import (
	"container/list"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
)

// entry holds the key and value pair stored in the doubly-linked list.
type entry struct {
	key   string
	value ValueType
	size  uint64
}

// mapCache is a map-based LRU cache implementation that indexes entries
// using a Go hash map and tracks access order with a doubly-linked list (container/list).
//
// It provides O(1) time complexity for Insert, Erase, LookUp, LookUpWithoutChangingOrder,
// UpdateWithoutChangingOrder, and UpdateSize. Prefix erasure operates in O(N) where N is
// the total number of entries in the cache.
//
// mapCache is safe for concurrent use by multiple goroutines via sync.RWMutex.
type mapCache struct {
	// maxSize is the maximum total size of entries the cache can hold.
	// Invariant: maxSize > 0
	maxSize uint64

	// currentSize is the running sum of all entry values' Size() in the cache.
	// Invariant: currentSize <= maxSize
	currentSize uint64

	// entries is the doubly-linked list of cache entries ordered from most recently
	// used (MRU, at the front) to least recently used (LRU, at the back).
	entries list.List

	// index maps string keys to their corresponding list elements.
	// Invariant: len(index) == entries.Len()
	index map[string]*list.Element

	// mu synchronizes access to internal state.
	mu sync.RWMutex

	// checkInvariantsEnabled indicates whether invariant checks are executed on lock/unlock.
	checkInvariantsEnabled bool

	// options holds the parsed cache configuration.
	options Options
}

// NewMapCache returns a new map-based LRU Cache initialized with the supplied maxSize.
// Optional configuration parameters can be passed via opts (e.g. WithInvariantChecking).
//
// maxSize must be greater than zero; otherwise NewMapCache panics.
func NewMapCache(maxSize uint64, opts ...Option) Cache {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	options := ApplyOptions(opts...)
	c := &mapCache{
		maxSize:                maxSize,
		index:                  make(map[string]*list.Element),
		checkInvariantsEnabled: options.EnableInvariantChecking,
		options:                options,
	}
	if c.checkInvariantsEnabled {
		c.checkInvariants()
	}
	return c
}

// New returns a new Cache instance with the default MapCache implementation.
// It is an alias to NewMapCache.
//
// maxSize must be greater than zero; otherwise New panics.
func New(maxSize uint64, opts ...Option) Cache {
	return NewMapCache(maxSize, opts...)
}

// checkInvariants validates internal data structure consistency and panics if any invariant is violated.
func (c *mapCache) checkInvariants() {
	// Invariant 1: maxSize > 0
	if c.maxSize == 0 {
		panic(fmt.Sprintf("Invalid maxSize: %v", c.maxSize))
	}

	// Invariant 2: currentSize <= maxSize
	if c.currentSize > c.maxSize {
		panic(fmt.Sprintf("CurrentSize %v over maxSize %v", c.currentSize, c.maxSize))
	}

	// Invariant 3: Element payload type safety
	for e := c.entries.Front(); e != nil; e = e.Next() {
		switch e.Value.(type) {
		case entry:
		default:
			panic(fmt.Sprintf("Unexpected element type: %v", reflect.TypeOf(e.Value)))
		}
	}

	// Invariant 4: Map-to-list cardinality bijection
	if c.entries.Len() != len(c.index) {
		panic(fmt.Sprintf("Length mismatch: %v vs. %v", c.entries.Len(), len(c.index)))
	}

	// Invariant 5: List pointer integrity (empty vs non-empty state)
	if c.entries.Len() == 0 {
		if c.entries.Front() != nil || c.entries.Back() != nil {
			panic("mapCache invariant violation: Front or Back is non-nil when entries.Len() == 0")
		}
	} else {
		if c.entries.Front() == nil || c.entries.Back() == nil {
			panic("mapCache invariant violation: Front or Back is nil when entries.Len() > 0")
		}
	}

	// Invariant 6: Bidirectional pointer linkage, map-to-list consistency, and size sum parity
	var prevElem *list.Element
	var sumSize uint64
	lruCount := 0

	for e := c.entries.Front(); e != nil; e = e.Next() {
		lruCount++

		// Bidirectional pointer validation
		if e.Prev() != prevElem {
			panic("mapCache invariant violation: corrupt prev pointer in LRU list")
		}
		if prevElem == nil {
			if c.entries.Front() != e {
				panic("mapCache invariant violation: head mismatch in LRU list")
			}
		} else {
			if prevElem.Next() != e {
				panic("mapCache invariant violation: corrupt next pointer in LRU list")
			}
		}
		if e.Next() == nil {
			if c.entries.Back() != e {
				panic("mapCache invariant violation: tail mismatch in LRU list")
			}
		}
		prevElem = e

		entryVal := e.Value.(entry)
		if c.index[entryVal.key] != e {
			panic(fmt.Sprintf("Mismatch for key %v", entryVal.key))
		}
		sumSize += entryVal.size
	}

	if lruCount != c.entries.Len() {
		panic(fmt.Sprintf("mapCache invariant violation: LRU list count %d does not match entries.Len() %d", lruCount, c.entries.Len()))
	}

	// Invariant 7: Sum of entry sizes matches currentSize
	if sumSize != c.currentSize {
		panic(fmt.Sprintf("Size sum mismatch: sum of entry sizes %d vs. currentSize %d", sumSize, c.currentSize))
	}
}

func (c *mapCache) lock() {
	c.mu.Lock()
	if c.checkInvariantsEnabled {
		c.checkInvariants()
	}
}

func (c *mapCache) unlock() {
	if c.checkInvariantsEnabled {
		c.checkInvariants()
	}
	c.mu.Unlock()
}

func (c *mapCache) rLock() {
	c.mu.RLock()
	if c.checkInvariantsEnabled {
		c.checkInvariants()
	}
}

func (c *mapCache) rUnlock() {
	if c.checkInvariantsEnabled {
		c.checkInvariants()
	}
	c.mu.RUnlock()
}

// evictOne removes and returns the least recently used entry from the cache.
// Caller must hold c.mu (write lock).
func (c *mapCache) evictOne() ValueType {
	e := c.entries.Back()
	if e == nil {
		return nil
	}
	entryVal := e.Value.(entry)
	key := entryVal.key
	evictedValue := entryVal.value
	c.currentSize -= entryVal.size

	c.entries.Remove(e)
	delete(c.index, key)

	return evictedValue
}

// Insert inserts or updates the given key and value in the cache.
// If the key already exists, its value is replaced and moved to the most recently used (MRU) position.
// If the cache exceeds capacity after insertion, least recently used (LRU) entries are evicted
// and returned in the slice.
//
// Returns ErrInvalidEntry if value is nil.
// Returns ErrInvalidEntrySize if value.Size() exceeds maxSize.
func (c *mapCache) Insert(key string, value ValueType) ([]ValueType, error) {
	if value == nil {
		return nil, ErrInvalidEntry
	}

	valueSize := value.Size()
	if valueSize > c.maxSize {
		return nil, ErrInvalidEntrySize
	}

	c.lock()
	defer c.unlock()

	e, ok := c.index[key]
	if ok {
		// Update existing entry.
		c.currentSize -= e.Value.(entry).size
		c.currentSize += valueSize
		e.Value = entry{key: key, value: value, size: valueSize}
		c.entries.MoveToFront(e)
	} else {
		// Add new entry at MRU (front).
		e := c.entries.PushFront(entry{key: key, value: value, size: valueSize})
		c.index[key] = e
		c.currentSize += valueSize
	}

	var evictedValues []ValueType
	for c.currentSize > c.maxSize && c.entries.Len() > 0 {
		evicted := c.evictOne()
		if evicted != nil {
			evictedValues = append(evictedValues, evicted)
		}
	}

	return evictedValues, nil
}

// eraseInternal removes any entry for the supplied key from the cache without acquiring locks.
// It returns the value of the erased key, or nil if not present.
// Caller must hold c.mu (write lock).
func (c *mapCache) eraseInternal(key string) ValueType {
	e, ok := c.index[key]
	if !ok {
		return nil
	}

	entryVal := e.Value.(entry)
	deletedEntry := entryVal.value
	c.currentSize -= entryVal.size

	delete(c.index, key)
	c.entries.Remove(e)

	return deletedEntry
}

// Erase removes the entry associated with the given key from the cache.
// Returns the value of the erased entry, or nil if the key was not found.
func (c *mapCache) Erase(key string) ValueType {
	c.lock()
	defer c.unlock()

	return c.eraseInternal(key)
}

// LookUp retrieves the value associated with key and updates its position to MRU.
// Returns nil if key is not found in the cache.
func (c *mapCache) LookUp(key string) ValueType {
	c.lock()
	defer c.unlock()

	e, ok := c.index[key]
	if !ok {
		return nil
	}
	c.entries.MoveToFront(e)
	return e.Value.(entry).value
}

// LookUpWithoutChangingOrder retrieves the value associated with key without altering its LRU position.
// Returns nil if key is not found in the cache.
func (c *mapCache) LookUpWithoutChangingOrder(key string) ValueType {
	c.rLock()
	defer c.rUnlock()

	e, ok := c.index[key]
	if !ok {
		return nil
	}
	return e.Value.(entry).value
}

// UpdateWithoutChangingOrder updates the value of an existing key without modifying its LRU position.
//
// Returns ErrInvalidEntry if value is nil.
// Returns ErrEntryNotExist if key is not present in the cache.
// Returns ErrInvalidUpdateEntrySize if value.Size() does not match the existing entry's size.
func (c *mapCache) UpdateWithoutChangingOrder(key string, value ValueType) error {
	if value == nil {
		return ErrInvalidEntry
	}

	valueSize := value.Size()

	c.lock()
	defer c.unlock()

	e, ok := c.index[key]
	if !ok {
		return ErrEntryNotExist
	}

	entryVal := e.Value.(entry)
	if valueSize != entryVal.size {
		return ErrInvalidUpdateEntrySize
	}

	e.Value = entry{key: key, value: value, size: entryVal.size}
	return nil
}

// UpdateSize adjusts the size accounting for an existing key by sizeDelta without altering its LRU position.
// If the cache capacity is exceeded as a result of the size adjustment, least recently used entries
// are evicted immediately to ensure the size invariant holds.
//
// Returns ErrEntryNotExist if key is not present in the cache.
func (c *mapCache) UpdateSize(key string, sizeDelta uint64) error {
	c.lock()
	defer c.unlock()

	e, ok := c.index[key]
	if !ok {
		return ErrEntryNotExist
	}

	entryVal := e.Value.(entry)
	if math.MaxUint64-entryVal.size < sizeDelta || math.MaxUint64-c.currentSize < sizeDelta {
		return ErrInvalidUpdateEntrySize
	}

	entryVal.size += sizeDelta
	e.Value = entryVal

	c.currentSize += sizeDelta
	for c.currentSize > c.maxSize && c.entries.Len() > 0 {
		c.evictOne()
	}

	return nil
}

// EraseEntriesWithGivenPrefix removes all entries from the cache whose keys start with prefix.
// If prefix is empty (""), all entries in the cache are erased.
func (c *mapCache) EraseEntriesWithGivenPrefix(prefix string) {
	c.lock()
	defer c.unlock()

	if prefix == "" {
		c.entries.Init()
		c.index = make(map[string]*list.Element)
		c.currentSize = 0
		return
	}

	for key := range c.index {
		if strings.HasPrefix(key, prefix) {
			c.eraseInternal(key)
		}
	}
}

// Compact reallocates the internal hash index to reclaim Go map bucket slack while preserving all live entries.
func (c *mapCache) Compact() {
	c.lock()
	defer c.unlock()
	c.compactLocked()
}

func (c *mapCache) compactLocked() {
	newIndex := make(map[string]*list.Element, len(c.index))
	for k, v := range c.index {
		newIndex[k] = v
	}
	c.index = newIndex
}

// EvaluateMemoryPressure samples the configured memory-pressure probe and executes
// Tier 2 (LRU shedding + map compaction) or Tier 1 (lossless map compaction) if thresholds are met.
func (c *mapCache) EvaluateMemoryPressure() []ValueType {
	var pressure float64
	if c.options.PressureFunc != nil {
		pressure = c.options.PressureFunc()
		if math.IsNaN(pressure) || pressure < 0.0 {
			pressure = 0.0
		}
	}

	c.lock()
	defer c.unlock()

	if pressure >= c.options.EvictionThreshold {
		retention := c.options.EvictionRetentionRatio
		targetSize := computeTargetSize(c.maxSize, retention)
		targetLen := 0
		if c.currentSize == 0 && c.entries.Len() > 0 && retention > 0.0 {
			targetLen = int(float64(c.entries.Len()) * retention)
		}
		var evicted []ValueType
		for c.entries.Len() > 0 {
			needByteShed := c.currentSize > targetSize
			needZeroSizeShed := c.currentSize == 0 && c.entries.Len() > targetLen
			needFullFlush := retention == 0.0
			if !needByteShed && !needZeroSizeShed && !needFullFlush {
				break
			}
			evicted = append(evicted, c.evictOne())
		}
		c.compactLocked()
		return evicted
	}
	if pressure >= c.options.CompactionThreshold {
		c.compactLocked()
	}
	return nil
}
