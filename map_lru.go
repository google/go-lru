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

	// dirtyIndex records whether entries have been deleted since the last index reallocation.
	dirtyIndex bool

	// mu synchronizes access to internal state.
	mu sync.RWMutex

	pressureState
}

var foregroundNoProtectElem list.Element

// NewMapCache returns a new map-based LRU Cache initialized with the supplied maxSize.
// Optional configuration parameters can be passed via opts (e.g. WithInvariantChecking).
//
// maxSize must be greater than zero; otherwise NewMapCache panics.
func NewMapCache(maxSize uint64, opts ...Option) Cache {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	return newMapCacheWithOptions(maxSize, ApplyOptions(opts...))
}

func newMapCacheWithOptions(maxSize uint64, options Options) Cache {
	c := &mapCache{
		maxSize:       maxSize,
		index:         make(map[string]*list.Element),
		pressureState: pressureState{options: options},
	}
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
	return c
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

	// Invariant 3 & 6: Element payload type safety (*entry), bidirectional pointer linkage,
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

		entryPtr, ok := e.Value.(*entry)
		if !ok || entryPtr == nil {
			panic(fmt.Sprintf("Unexpected element type: %T", e.Value))
		}
		entryVal := *entryPtr
		if entryVal.value == nil {
			panic(fmt.Sprintf("mapCache invariant violation: unexpected nil value in LRU list for key '%s'", entryVal.key))
		}
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

func (c *mapCache) lock() {
	c.mu.Lock()
}

func (c *mapCache) unlock() {
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
	c.mu.Unlock()
}

func (c *mapCache) rLock() {
	c.mu.RLock()
}

func (c *mapCache) rUnlock() {
	if c.options.EnableInvariantChecking {
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
	entryVal := e.Value.(*entry)
	return c.eraseInternal(entryVal.key)
}

func (c *mapCache) finishDeleteReclaimLocked(sizeBefore, sampledEpoch uint64, pressure float64) {
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

func (c *mapCache) finishMutationReclaimLocked(evictedValues []ValueType, protectedElem *list.Element, reclaimedPre bool, sizeBefore, sampledEpoch uint64, pressure float64) []ValueType {
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

	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	var evictedValues []ValueType
	sizeBefore := c.currentSize
	reclaimedPreInsert := false
	evictedPreInsert := false

	e, ok := c.index[key]
	if ok {
		// Update existing entry in place (0 heap allocations).
		entryVal := e.Value.(*entry)
		c.onEntrySizeUpdated(entryVal.size, valueSize)
		c.entries.MoveToFront(e)
		c.currentSize -= entryVal.size
		for valueSize > c.maxSize-c.currentSize && c.entries.Len() > 1 {
			evicted := c.evictOne()
			if evicted != nil {
				evictedValues = append(evictedValues, evicted)
				evictedPreInsert = true
			}
		}
		entryVal.value = value
		entryVal.size = valueSize
		c.currentSize += valueSize
		if c.shouldReclaimSingleSurvivorOnMutation(c.entries.Len(), c.dirtyIndex, false, evictedPreInsert, c.currentSize, sizeBefore, pressure) {
			reclaimedPreInsert = true
		}
	} else {
		// Evict prior to adding new entry if valueSize would exceed remaining capacity (prevents uint64 overflow).
		for valueSize > c.maxSize-c.currentSize && c.entries.Len() > 0 {
			evicted := c.evictOne()
			if evicted != nil {
				evictedValues = append(evictedValues, evicted)
				evictedPreInsert = true
			}
		}
		if c.shouldReclaimEmptyPreInsert(c.entries.Len(), false, evictedPreInsert, valueSize, sizeBefore, pressure) {
			c.clearEmptyIndexStateLocked()
			reclaimedPreInsert = true
		} else if c.shouldReclaimSingleSurvivorOnMutation(c.entries.Len(), c.dirtyIndex, false, evictedPreInsert, c.currentSize+valueSize, sizeBefore, pressure) {
			reclaimedPreInsert = true
		}
		// Clone key to prevent substring keys from pinning large caller backing arrays.
		clonedKey := clonePrefix(key)
		e = c.entries.PushFront(&entry{key: clonedKey, value: value, size: valueSize})
		c.index[clonedKey] = e
		c.onEntryInserted(len(c.index), valueSize)
		c.currentSize += valueSize
	}

	evictedValues = c.finishMutationReclaimLocked(evictedValues, e, reclaimedPreInsert, sizeBefore, sampledEpoch, pressure)
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

	entryVal := e.Value.(*entry)
	deletedEntry := entryVal.value
	c.onEntryDeleted(entryVal.size)
	c.currentSize -= entryVal.size

	delete(c.index, key)
	c.dirtyIndex = true
	c.entries.Remove(e)
	entryVal.key = ""
	entryVal.value = nil
	entryVal.size = 0
	e.Value = nil

	return deletedEntry
}

func (c *mapCache) clearEmptyIndexStateLocked() {
	if c.peakEntryLen > 8 || c.deletedSinceCompact >= 64 {
		c.index = make(map[string]*list.Element)
	} else {
		clear(c.index)
	}
	c.dirtyIndex = false
	c.resetWatermarks()
}

// resetEmptyIndexLocked releases peak map bucket memory when the cache transitions to empty.
func (c *mapCache) resetEmptyIndexLocked() {
	c.clearEmptyIndexStateLocked()
	c.markReclaimedLocked()
}

// Erase removes the entry associated with the given key from the cache.
// Returns the value of the erased entry, or nil if the key was not found.
func (c *mapCache) Erase(key string) ValueType {
	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	sizeBefore := c.currentSize
	deleted := c.eraseInternal(key)
	if deleted == nil {
		return nil
	}
	c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
	return deleted
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
	return e.Value.(*entry).value
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
	return e.Value.(*entry).value
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

	entryVal := e.Value.(*entry)
	if valueSize != entryVal.size {
		return ErrInvalidUpdateEntrySize
	}

	entryVal.value = value
	return nil
}

// UpdateSize adjusts the size accounting for an existing key by sizeDelta without altering its LRU position.
// If the entry's updated size exceeds maxSize (or cannot fit alongside entries more recent than key),
// the entry itself is evicted immediately without evicting older entries. Otherwise, least recently used
// entries are evicted to ensure the size invariant holds.
//
// Returns ErrEntryNotExist if key is not present in the cache.
// Returns ErrInvalidUpdateEntrySize if sizeDelta causes uint64 integer overflow.
func (c *mapCache) UpdateSize(key string, sizeDelta uint64) error {
	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	e, ok := c.index[key]
	if !ok {
		return ErrEntryNotExist
	}

	entryVal := e.Value.(*entry)
	if math.MaxUint64-entryVal.size < sizeDelta {
		return ErrInvalidUpdateEntrySize
	}

	if entryVal.size+sizeDelta > c.maxSize {
		sizeBefore := c.currentSize
		c.eraseInternal(entryVal.key)
		c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
		return nil
	}

	avail := c.maxSize - c.currentSize
	if sizeDelta > avail {
		maxNewer := c.maxSize - (entryVal.size + sizeDelta)
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
				newerSize += headCurr.Value.(*entry).size
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
			avail += tailCurr.Value.(*entry).size
			if sizeDelta <= avail {
				canFit = true
				break
			}
			tailCurr = tailCurr.Prev()
		}
		if !canFit {
			sizeBefore := c.currentSize
			c.eraseInternal(entryVal.key)
			c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
			return nil
		}
	}

	sizeBefore := c.currentSize
	evictedAny := false
	reclaimedPreUpdate := false
	for sizeDelta > c.maxSize-c.currentSize && c.entries.Len() > 0 {
		if c.entries.Back() == e {
			break
		}
		c.evictOne()
		evictedAny = true
	}

	c.onEntrySizeUpdated(entryVal.size, entryVal.size+sizeDelta)
	entryVal.size += sizeDelta
	c.currentSize += sizeDelta
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

// EraseEntriesWithGivenPrefix removes all entries from the cache whose keys start with prefix.
// If prefix is empty (""), all entries in the cache are erased.
func (c *mapCache) EraseEntriesWithGivenPrefix(prefix string) {
	if prefix == "" {
		c.mu.Lock()
		defer c.unlock()

		hadEntries := c.entries.Len() > 0
		hadDirtySlack := c.dirtyIndex || c.peakEntryLen > 8 || c.deletedSinceCompact > 0
		if !hadEntries && !hadDirtySlack && c.peakEntryLen == 0 {
			return
		}
		hadReclaimable := c.currentSize > 0 || hadDirtySlack
		for e := c.entries.Front(); e != nil; {
			next := e.Next()
			if entryVal, ok := e.Value.(*entry); ok && entryVal != nil {
				entryVal.key = ""
				entryVal.value = nil
				entryVal.size = 0
			}
			c.entries.Remove(e)
			e.Value = nil
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
			c.eraseInternal(key)
			erasedAny = true
		}
	}
	if !erasedAny {
		return
	}
	c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
}

func (c *mapCache) shouldAutoCompactLocked(protectedElem *list.Element) bool {
	return c.shouldAutoCompactEntryCounts(c.dirtyIndex, protectedElem == nil, len(c.index))
}

// Compact reallocates the internal hash index to reclaim Go map bucket slack while preserving all live entries.
func (c *mapCache) Compact() {
	c.lock()
	defer c.unlock()
	c.compactLocked()
}

func (c *mapCache) compactDataStructuresLocked() bool {
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

func (c *mapCache) compactLocked() {
	if c.compactDataStructuresLocked() {
		c.markReclaimedLocked()
	}
}

func (c *mapCache) shedAndCompactLocked(targetSize uint64, retention float64, protectedElem *list.Element) []ValueType {
	autoCompactElem := protectedElem
	if protectedElem != nil && (protectedElem != c.entries.Front() || protectedElem.Prev() != nil) {
		protectedElem = nil
	}

	var protectedSize uint64
	hasProtected := protectedElem != nil
	if hasProtected {
		protectedSize = protectedElem.Value.(*entry).size
	}
	effectiveTarget, targetLen, targetZeroCount, unprotectedZeroTarget := c.computeShedTargets(targetSize, retention, c.entries.Len(), hasProtected, protectedSize)

	needFullFlush := retention == 0.0
	var evicted []ValueType
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
			for victim != nil && (victim == protectedElem || victim.Value.(*entry).size == 0) {
				victim = victim.Prev()
			}
		default:
			for victim != nil && (victim == protectedElem || victim.Value.(*entry).size > 0) {
				victim = victim.Prev()
			}
		}
		if victim == nil {
			break
		}
		nextVictim := victim.Prev()
		if val := c.eraseInternal(victim.Value.(*entry).key); val != nil {
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

func (c *mapCache) maybeReclaimUnderPressureLocked(pressure float64, protectedElem *list.Element) []ValueType {
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
func (c *mapCache) EvaluateMemoryPressure() []ValueType {
	_, pressure := c.lockWithPressure(&c.mu, true)
	defer c.unlock()

	return c.maybeReclaimUnderPressureLocked(pressure, nil)
}
