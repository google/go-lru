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
	"container/list"
	"fmt"
	"maps"
	"math"
	"strings"
	"sync"
)

// entry holds the key and value pair stored in the doubly-linked list.
type entry[V any] struct {
	key   string
	value V
	size  uint64
}

// mapCache is a map-based LRU cache implementation that indexes entries
// using a Go hash map and tracks access order with a doubly-linked list (container/list).
//
// It provides O(1) time complexity for Put, Delete, Get, Peek,
// and Replace. DeletePrefix operates in O(N) where N is
// the total number of entries in the cache.
//
// mapCache is safe for concurrent use by multiple goroutines via sync.RWMutex.
type mapCache[V any] struct {
	// maxSize is the maximum total weight/size of entries the cache can hold.
	// Invariant: maxSize > 0
	maxSize uint64

	// currentSize is the running sum of all entry weights in the cache.
	// Invariant: currentSize <= maxSize
	currentSize uint64

	// entries is the doubly-linked list of cache entries ordered from most recently
	// used (MRU, at the front) to least recently used (LRU, at the back).
	entries list.List

	// index maps string keys to their corresponding list elements.
	// Invariant: len(index) == entries.Len()
	index map[string]*list.Element

	// dirtyIndex records whether entries have been deleted since the last index reallocation.
	dirtyIndex bool

	// weigher computes the weight of a cache entry, or is nil when using the default unit weight of 1.
	weigher func(key string, value V) uint64

	// onEvictValue is the optional value eviction callback configured via WithOnEvictValue.
	onEvictValue func(value V, reason EvictionReason)

	// onEvictEntry is the optional entry eviction callback configured via WithOnEvictEntry.
	onEvictEntry func(key string, value V, reason EvictionReason)

	// mu synchronizes access to internal state.
	mu sync.RWMutex

	pressureState
}

var foregroundNoProtectElem list.Element

// NewMapCache returns a new map-based LRU Cache[V] initialized with the supplied maxSize.
// Optional configuration parameters can be passed via opts (e.g. WithWeigher, WithInvariantChecking).
//
// maxSize must be greater than zero; otherwise NewMapCache panics.
func NewMapCache[V any](maxSize uint64, opts ...Option) Cache[V] {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	return newMapCacheWithOptions[V](maxSize, ApplyOptions(opts...))
}

func newMapCacheWithOptions[V any](maxSize uint64, options Options) Cache[V] {
	c := &mapCache[V]{
		maxSize:       maxSize,
		index:         make(map[string]*list.Element),
		weigher:       resolveWeigher[V](options),
		onEvictValue:  resolveOnEvictValue[V](options),
		onEvictEntry:  resolveOnEvictEntry[V](options),
		pressureState: pressureState{options: options},
	}
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
	return c
}

func (c *mapCache[V]) weigh(key string, value V) uint64 {
	if c.weigher != nil {
		return c.weigher(key, value)
	}
	return 1
}

func (c *mapCache[V]) notifyEvict(key string, value V, reason EvictionReason) {
	if c.onEvictValue != nil {
		c.onEvictValue(value, reason)
	}
	if c.onEvictEntry != nil {
		c.onEvictEntry(key, value, reason)
	}
}

// checkInvariants validates internal data structure consistency and panics if any invariant is violated.
func (c *mapCache[V]) checkInvariants() {
	// Invariant 1: maxSize > 0
	if c.maxSize == 0 {
		panic(fmt.Sprintf("Invalid maxSize: %v", c.maxSize))
	}

	// Invariant 2: currentSize <= maxSize
	if c.currentSize > c.maxSize {
		panic(fmt.Sprintf("CurrentSize %v over maxSize %v", c.currentSize, c.maxSize))
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

	// Invariant 3 & 6: Element payload type safety (*entry[V]), bidirectional pointer linkage,
	// map-to-list consistency, and size sum parity.
	var prevElem *list.Element
	var sumSize uint64
	lruCount := 0
	zeroCount := 0

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

		entryPtr, ok := e.Value.(*entry[V])
		if !ok || entryPtr == nil {
			panic(fmt.Sprintf("Unexpected element type: %T", e.Value))
		}
		entryVal := *entryPtr
		if c.index[entryVal.key] != e {
			panic(fmt.Sprintf("Mismatch for key %v", entryVal.key))
		}
		if math.MaxUint64-sumSize < entryVal.size {
			panic("mapCache invariant violation: sumSize uint64 overflow")
		}
		sumSize += entryVal.size
		if entryVal.size == 0 {
			zeroCount++
		}
	}

	if lruCount != c.entries.Len() {
		panic(fmt.Sprintf("mapCache invariant violation: LRU list count %d does not match entries.Len() %d", lruCount, c.entries.Len()))
	}

	if zeroCount != c.zeroSizeCount {
		panic(fmt.Sprintf("mapCache invariant violation: zeroSizeCount %d does not match live zero-size entries %d", c.zeroSizeCount, zeroCount))
	}

	// Invariant 7: Sum of entry sizes matches currentSize
	if sumSize != c.currentSize {
		panic(fmt.Sprintf("Size sum mismatch: sum of entry sizes %d vs. currentSize %d", sumSize, c.currentSize))
	}
}

func (c *mapCache[V]) lock() {
	c.mu.Lock()
}

func (c *mapCache[V]) unlock() {
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
	c.mu.Unlock()
}

func (c *mapCache[V]) rLock() {
	c.mu.RLock()
}

func (c *mapCache[V]) rUnlock() {
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
	c.mu.RUnlock()
}

// evictOne removes and returns the least recently used entry from the cache.
// Caller must hold c.mu (write lock).
func (c *mapCache[V]) evictOne() (V, bool) {
	e := c.entries.Back()
	if e == nil {
		var zero V
		return zero, false
	}
	entryVal := e.Value.(*entry[V])
	return c.eraseInternal(entryVal.key, EvictionReasonCapacity)
}

func (c *mapCache[V]) finishDeleteReclaimLocked(sizeBefore, sampledEpoch uint64, pressure float64) {
	if c.entries.Len() == 0 {
		hadSlack := c.hasEmptyDeleteSlack(false)
		c.clearEmptyIndexStateLocked()
		if (hadSlack || sizeBefore > 0) && c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
			c.markReclaimedLocked()
		}
		return
	}
	reclaimedSingleSurvivor := false
	if c.shouldReclaimSingleSurvivorOnDelete(c.entries.Len(), c.dirtyIndex, false) {
		c.compactDataStructuresLocked()
		reclaimedSingleSurvivor = true
	}
	c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectElem)
	if (reclaimedSingleSurvivor || c.currentSize < sizeBefore) && c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
		c.markReclaimedLocked()
	}
}

func (c *mapCache[V]) finishMutationReclaimLocked(evictedValues []V, protectedElem *list.Element, reclaimedPre bool, sizeBefore, sampledEpoch uint64, pressure float64) []V {
	evictedByPressure := c.maybeReclaimUnderPressureLocked(pressure, protectedElem)
	if len(evictedValues) == 0 {
		evictedValues = evictedByPressure
	} else if len(evictedByPressure) > 0 {
		evictedValues = append(evictedValues, evictedByPressure...)
	}
	netByteReduced := c.currentSize < sizeBefore
	if c.shouldCompactAfterMutation(reclaimedPre, netByteReduced, c.dirtyIndex, pressure) {
		c.compactDataStructuresLocked()
	}
	if c.shouldMarkReclaimedAfterMutation(reclaimedPre, netByteReduced, sampledEpoch, pressure) {
		c.markReclaimedLocked()
	}
	return evictedValues
}

// Put inserts or updates the given key and value in the cache.
// If the key already exists, its value is replaced and moved to the most recently used (MRU) position.
// If the cache exceeds capacity after insertion, least recently used (LRU) entries are evicted
// and returned in the slice.
//
// Returns ErrInvalidEntrySize if the entry's weight exceeds maxSize.
func (c *mapCache[V]) Put(key string, value V) ([]V, error) {
	valueSize := c.weigh(key, value)
	if valueSize > c.maxSize {
		return nil, ErrInvalidEntrySize
	}

	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	var evictedValues []V
	sizeBefore := c.currentSize
	reclaimedPrePut := false
	evictedPrePut := false

	e, ok := c.index[key]
	if ok {
		// Update existing entry in place (0 heap allocations).
		entryVal := e.Value.(*entry[V])
		oldValue := entryVal.value
		c.onEntrySizeUpdated(entryVal.size, valueSize)
		c.entries.MoveToFront(e)
		c.currentSize -= entryVal.size
		for valueSize > c.maxSize-c.currentSize && c.entries.Len() > 1 {
			if evicted, evictedOK := c.evictOne(); evictedOK {
				evictedValues = append(evictedValues, evicted)
				evictedPrePut = true
			}
		}
		entryVal.value = value
		entryVal.size = valueSize
		c.currentSize += valueSize
		c.notifyEvict(entryVal.key, oldValue, EvictionReasonReplaced)
		if c.shouldReclaimSingleSurvivorOnMutation(c.entries.Len(), c.dirtyIndex, false, evictedPrePut, c.currentSize, sizeBefore, pressure) {
			reclaimedPrePut = true
		}
	} else {
		// Evict prior to adding new entry if valueSize would exceed remaining capacity (prevents uint64 overflow).
		for valueSize > c.maxSize-c.currentSize && c.entries.Len() > 0 {
			if evicted, evictedOK := c.evictOne(); evictedOK {
				evictedValues = append(evictedValues, evicted)
				evictedPrePut = true
			}
		}
		if c.shouldReclaimEmptyPrePut(c.entries.Len(), false, evictedPrePut, valueSize, sizeBefore, pressure) {
			c.clearEmptyIndexStateLocked()
			reclaimedPrePut = true
		} else if c.shouldReclaimSingleSurvivorOnMutation(c.entries.Len(), c.dirtyIndex, false, evictedPrePut, c.currentSize+valueSize, sizeBefore, pressure) {
			reclaimedPrePut = true
		}
		// Clone key to prevent substring keys from pinning large caller backing arrays.
		clonedKey := clonePrefix(key)
		e = c.entries.PushFront(&entry[V]{key: clonedKey, value: value, size: valueSize})
		c.index[clonedKey] = e
		c.onEntryPut(len(c.index), valueSize)
		c.currentSize += valueSize
	}

	evictedValues = c.finishMutationReclaimLocked(evictedValues, e, reclaimedPrePut, sizeBefore, sampledEpoch, pressure)
	return evictedValues, nil
}

// eraseInternal removes any entry for the supplied key from the cache without acquiring locks.
// It returns the value of the erased key and true, or the zero value of V and false if not present.
// Caller must hold c.mu (write lock).
func (c *mapCache[V]) eraseInternal(key string, reason EvictionReason) (V, bool) {
	e, ok := c.index[key]
	if !ok {
		var zero V
		return zero, false
	}

	entryVal := e.Value.(*entry[V])
	evictedKey := entryVal.key
	deletedEntry := entryVal.value
	c.onEntryDeleted(entryVal.size)
	c.currentSize -= entryVal.size

	delete(c.index, key)
	c.dirtyIndex = true
	c.entries.Remove(e)
	var zero V
	entryVal.key = ""
	entryVal.value = zero
	entryVal.size = 0
	e.Value = nil

	c.notifyEvict(evictedKey, deletedEntry, reason)
	return deletedEntry, true
}

func (c *mapCache[V]) clearEmptyIndexStateLocked() {
	if c.peakEntryLen > 8 || c.deletedSinceCompact >= 64 {
		c.index = make(map[string]*list.Element)
	} else {
		clear(c.index)
	}
	c.dirtyIndex = false
	c.resetWatermarks()
}

// resetEmptyIndexLocked releases peak map bucket memory when the cache transitions to empty.
func (c *mapCache[V]) resetEmptyIndexLocked() {
	c.clearEmptyIndexStateLocked()
	c.markReclaimedLocked()
}

// Delete removes the entry associated with the given key from the cache.
// Returns the value of the deleted entry and true, or the zero value of V and false if the key was not found.
func (c *mapCache[V]) Delete(key string) (V, bool) {
	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	sizeBefore := c.currentSize
	deleted, ok := c.eraseInternal(key, EvictionReasonDeleted)
	if !ok {
		return deleted, false
	}
	c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
	return deleted, true
}

// Get retrieves the value associated with key and updates its position to MRU.
// Returns the zero value of V and false if key is not found in the cache.
func (c *mapCache[V]) Get(key string) (V, bool) {
	c.lock()
	defer c.unlock()

	e, ok := c.index[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.entries.MoveToFront(e)
	return e.Value.(*entry[V]).value, true
}

// Peek retrieves the value associated with key without altering its LRU position.
// Returns the zero value of V and false if key is not found in the cache.
func (c *mapCache[V]) Peek(key string) (V, bool) {
	c.rLock()
	defer c.rUnlock()

	e, ok := c.index[key]
	if !ok {
		var zero V
		return zero, false
	}
	return e.Value.(*entry[V]).value, true
}

// Replace updates the value of an existing key and recomputes its weight
// without modifying its LRU position.
// If the entry's updated weight exceeds maxSize (or cannot fit alongside entries more recent than key),
// the entry itself is evicted immediately without evicting older entries. Otherwise, if total cache
// capacity is exceeded, least recently used entries are evicted to ensure the size invariant holds.
//
// Returns ErrEntryNotExist if key is not present in the cache.
func (c *mapCache[V]) Replace(key string, value V) error {
	newSize := c.weigh(key, value)

	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	e, ok := c.index[key]
	if !ok {
		return ErrEntryNotExist
	}

	entryVal := e.Value.(*entry[V])
	oldSize := entryVal.size
	oldValue := entryVal.value

	if newSize > c.maxSize {
		sizeBefore := c.currentSize
		c.eraseInternal(key, EvictionReasonCapacity)
		c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
		return nil
	}

	sizeBefore := c.currentSize
	evictedAny := false
	reclaimedPreUpdate := false

	switch {
	case newSize > oldSize:
		sizeDelta := newSize - oldSize
		avail := c.maxSize - c.currentSize
		if sizeDelta > avail {
			maxNewer := c.maxSize - newSize
			var newerSize uint64
			headCurr := c.entries.Front()
			tailCurr := c.entries.Back()
			canFit := false
			for {
				if headCurr == e {
					canFit = newerSize <= maxNewer
					break
				}
				if headCurr != nil {
					newerSize += headCurr.Value.(*entry[V]).size
					if newerSize > maxNewer {
						canFit = false
						break
					}
					headCurr = headCurr.Next()
				}
				if tailCurr == nil || tailCurr == e {
					canFit = sizeDelta <= avail
					break
				}
				avail += tailCurr.Value.(*entry[V]).size
				if sizeDelta <= avail {
					canFit = true
					break
				}
				tailCurr = tailCurr.Prev()
			}
			if !canFit {
				c.eraseInternal(key, EvictionReasonCapacity)
				c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
				return nil
			}
		}

		for sizeDelta > c.maxSize-c.currentSize && c.entries.Len() > 0 {
			if c.entries.Back() == e {
				break
			}
			c.evictOne()
			evictedAny = true
		}

		c.onEntrySizeUpdated(oldSize, newSize)
		entryVal.value = value
		entryVal.size = newSize
		c.currentSize += sizeDelta
	case newSize < oldSize:
		sizeDiff := oldSize - newSize
		c.onEntrySizeUpdated(oldSize, newSize)
		entryVal.value = value
		entryVal.size = newSize
		c.currentSize -= sizeDiff
	default:
		entryVal.value = value
	}

	c.notifyEvict(entryVal.key, oldValue, EvictionReasonReplaced)

	if c.shouldReclaimSingleSurvivorOnMutation(c.entries.Len(), c.dirtyIndex, false, evictedAny, c.currentSize, sizeBefore, pressure) {
		reclaimedPreUpdate = true
	}

	protectedElem := &foregroundNoProtectElem
	if e == c.entries.Front() {
		protectedElem = e
	}
	c.finishMutationReclaimLocked(nil, protectedElem, reclaimedPreUpdate, sizeBefore, sampledEpoch, pressure)

	return nil
}

// DeletePrefix removes all entries from the cache whose keys start with prefix.
// If prefix is empty (""), all entries in the cache are deleted.
func (c *mapCache[V]) DeletePrefix(prefix string) {
	if prefix == "" {
		c.mu.Lock()
		defer c.unlock()

		hadEntries := c.entries.Len() > 0
		hadDirtySlack := c.dirtyIndex || c.peakEntryLen > 8 || c.deletedSinceCompact > 0
		if !hadEntries && !hadDirtySlack && c.peakEntryLen == 0 {
			return
		}
		hadReclaimable := c.currentSize > 0 || hadDirtySlack
		var zero V
		for e := c.entries.Front(); e != nil; {
			next := e.Next()
			if entryVal, ok := e.Value.(*entry[V]); ok && entryVal != nil {
				evictedKey := entryVal.key
				evictedVal := entryVal.value
				c.onEntryDeleted(entryVal.size)
				c.currentSize -= entryVal.size
				entryVal.key = ""
				entryVal.value = zero
				entryVal.size = 0
				c.entries.Remove(e)
				e.Value = nil
				c.notifyEvict(evictedKey, evictedVal, EvictionReasonDeleted)
			} else {
				c.entries.Remove(e)
				e.Value = nil
			}
			e = next
		}
		c.entries.Init()
		c.currentSize = 0
		c.clearEmptyIndexStateLocked()
		if hadReclaimable && c.hasElevatedPressureToInvalidate(0.0) {
			c.markReclaimedLocked()
		}
		return
	}

	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	sizeBefore := c.currentSize
	erasedAny := false
	for key := range c.index {
		if strings.HasPrefix(key, prefix) {
			c.eraseInternal(key, EvictionReasonDeleted)
			erasedAny = true
		}
	}
	if !erasedAny {
		return
	}
	c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
}

func (c *mapCache[V]) shouldAutoCompactLocked(protectedElem *list.Element) bool {
	return c.shouldAutoCompactEntryCounts(c.dirtyIndex, protectedElem == nil, len(c.index))
}

// Compact reallocates the internal hash index to reclaim Go map bucket slack while preserving all live entries.
func (c *mapCache[V]) Compact() {
	c.lock()
	defer c.unlock()
	c.compactLocked()
}

func (c *mapCache[V]) compactDataStructuresLocked() bool {
	if !c.dirtyIndex {
		return false
	}
	newIndex := make(map[string]*list.Element, len(c.index))
	maps.Copy(newIndex, c.index)
	c.index = newIndex
	c.dirtyIndex = false
	c.onCompacted(len(c.index))
	return true
}

func (c *mapCache[V]) compactLocked() {
	if c.compactDataStructuresLocked() {
		c.markReclaimedLocked()
	}
}

func (c *mapCache[V]) shedAndCompactLocked(targetSize uint64, retention float64, protectedElem *list.Element) []V {
	autoCompactElem := protectedElem
	if protectedElem != nil && (protectedElem != c.entries.Front() || protectedElem.Prev() != nil) {
		protectedElem = nil
	}

	var protectedSize uint64
	hasProtected := protectedElem != nil
	if hasProtected {
		protectedSize = protectedElem.Value.(*entry[V]).size
	}
	effectiveTarget, targetLen, targetZeroCount, unprotectedZeroTarget := c.computeShedTargets(targetSize, retention, c.entries.Len(), hasProtected, protectedSize)

	needFullFlush := retention == 0.0
	var evicted []V
	victim := c.entries.Back()
	for victim != nil {
		needByteShed := c.currentSize > effectiveTarget || needFullFlush
		needZeroShed := !needFullFlush && c.zeroSizeCount > targetZeroCount && c.entries.Len() > targetLen
		if !needByteShed && !needZeroShed {
			break
		}
		switch {
		case needFullFlush || (needByteShed && c.zeroSizeCount > targetZeroCount):
			for victim != nil && victim == protectedElem {
				victim = victim.Prev()
			}
		case needByteShed:
			for victim != nil && (victim == protectedElem || victim.Value.(*entry[V]).size == 0) {
				victim = victim.Prev()
			}
		default:
			for victim != nil && (victim == protectedElem || victim.Value.(*entry[V]).size > 0) {
				victim = victim.Prev()
			}
		}
		if victim == nil {
			break
		}
		nextVictim := victim.Prev()
		if val, ok := c.eraseInternal(victim.Value.(*entry[V]).key, EvictionReasonPressure); ok {
			evicted = append(evicted, val)
		}
		victim = nextVictim
	}

	if len(evicted) > 0 && c.entries.Len() == 0 {
		c.resetEmptyIndexLocked()
		return evicted
	}

	if len(evicted) > 0 {
		c.updateZeroWatermarkAfterShed(retention, c.entries.Len(), !hasProtected || effectiveTarget <= targetSize, unprotectedZeroTarget, targetLen)
	}
	compacted := false
	if c.shouldAutoCompactLocked(autoCompactElem) {
		compacted = c.compactDataStructuresLocked()
	}
	if len(evicted) > 0 || compacted {
		c.markReclaimedLocked()
	}
	return evicted
}

func (c *mapCache[V]) maybeReclaimUnderPressureLocked(pressure float64, protectedElem *list.Element) []V {
	if c.isSamplingGoroutine() {
		return nil
	}
	if pressure >= c.options.EvictionThreshold {
		retention := c.options.EvictionRetentionRatio
		targetSize := computeTargetSize(c.maxSize, retention)
		return c.shedAndCompactLocked(targetSize, retention, protectedElem)
	}
	c.resetZeroWatermarkBelowTier2(pressure)
	if pressure >= c.options.CompactionThreshold && c.shouldAutoCompactLocked(protectedElem) {
		c.compactLocked()
	}
	return nil
}

// EvaluateMemoryPressure samples the configured memory-pressure probe and executes
// Tier 2 (LRU shedding + map compaction) or Tier 1 (lossless map compaction) if thresholds are met.
func (c *mapCache[V]) EvaluateMemoryPressure() []V {
	_, pressure := c.lockWithPressure(&c.mu, true)
	defer c.unlock()

	return c.maybeReclaimUnderPressureLocked(pressure, nil)
}
