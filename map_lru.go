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
	"maps"
	"math"
	"strings"
	"sync"
)

// entry holds a key-value pair and intrusive doubly-linked list pointers for LRU ordering.
type entry[V any] struct {
	key   string
	prev  *entry[V]
	next  *entry[V]
	size  uint64
	value V
}

// entryList is a strongly typed generic intrusive doubly-linked list of entry[V] nodes,
// ordered from most recently used (head) to least recently used (tail).
type entryList[V any] struct {
	head *entry[V]
	tail *entry[V]
	len  int
}

func (l *entryList[V]) Len() int         { return l.len }
func (l *entryList[V]) Front() *entry[V] { return l.head }
func (l *entryList[V]) Back() *entry[V]  { return l.tail }

func (l *entryList[V]) Init() {
	l.head = nil
	l.tail = nil
	l.len = 0
}

func (l *entryList[V]) PushFront(e *entry[V]) *entry[V] {
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	} else {
		l.tail = e
	}
	l.head = e
	l.len++
	return e
}

func (l *entryList[V]) MoveToFront(e *entry[V]) {
	if l.head == e {
		return
	}
	if e.prev != nil {
		e.prev.next = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	} else {
		l.tail = e
	}
	l.head = e
}

func (l *entryList[V]) Remove(e *entry[V]) {
	if e.prev != nil {
		e.prev.next = e.next
	} else if l.head == e {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else if l.tail == e {
		l.tail = e.prev
	}
	e.prev = nil
	e.next = nil
	l.len--
}

// mapCache is a map-based LRU cache implementation that indexes entries
// using a Go hash map and tracks access order with a generic intrusive doubly-linked list.
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

	// entries is the intrusive doubly-linked list of cache entries ordered from most recently
	// used (MRU, at the front) to least recently used (LRU, at the back).
	entries entryList[V]

	// index maps string keys to their corresponding entry nodes.
	// Invariant: len(index) == entries.Len()
	index map[string]*entry[V]

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
		index:         make(map[string]*entry[V]),
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

func (c *mapCache[V]) notifyEvict(evictQ *evictCallbackQueue[V], key string, value V, size uint64, reason EvictionReason) {
	c.recordEviction(reason, size)
	if c.onEvictValue != nil || c.onEvictEntry != nil {
		evictQ.enqueue(key, value, reason)
	}
}

func (c *mapCache[V]) hasSlackLocked() bool {
	return c.dirtyIndex && (len(c.index) < c.peakEntryLen || c.deletedSinceCompact >= minChurnCompactDeletes)
}

func (c *mapCache[V]) checkListAndIndexInvariants() {
	var prevElem *entry[V]
	var sumSize uint64
	lruCount := 0
	zeroCount := 0

	for e := c.entries.Front(); e != nil; e = e.next {
		lruCount++

		// Bidirectional pointer validation
		if e.prev != prevElem {
			panic("mapCache invariant violation: corrupt prev pointer in LRU list")
		}
		if prevElem == nil {
			if c.entries.Front() != e {
				panic("mapCache invariant violation: head mismatch in LRU list")
			}
		} else if prevElem.next != e {
			panic("mapCache invariant violation: corrupt next pointer in LRU list")
		}
		if e.next == nil && c.entries.Back() != e {
			panic("mapCache invariant violation: tail mismatch in LRU list")
		}
		prevElem = e

		if c.index[e.key] != e {
			panic(fmt.Sprintf("Mismatch for key %v", e.key))
		}
		if math.MaxUint64-sumSize < e.size {
			panic("mapCache invariant violation: sumSize uint64 overflow")
		}
		sumSize += e.size
		if e.size == 0 {
			zeroCount++
		}
	}

	if lruCount != c.entries.Len() {
		panic(fmt.Sprintf("mapCache invariant violation: LRU list count %d does not match entries.Len() %d", lruCount, c.entries.Len()))
	}

	if zeroCount != c.zeroSizeCount {
		panic(fmt.Sprintf("mapCache invariant violation: zeroSizeCount %d does not match live zero-size entries %d", c.zeroSizeCount, zeroCount))
	}

	// Invariant 6: Sum of entry sizes matches currentSize
	if sumSize != c.currentSize {
		panic(fmt.Sprintf("Size sum mismatch: sum of entry sizes %d vs. currentSize %d", sumSize, c.currentSize))
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

	// Invariant 3: Map-to-list cardinality bijection
	if c.entries.Len() != len(c.index) {
		panic(fmt.Sprintf("Length mismatch: %v vs. %v", c.entries.Len(), len(c.index)))
	}

	// Invariant 4: List pointer integrity (empty vs non-empty state)
	if c.entries.Len() == 0 {
		if c.entries.Front() != nil || c.entries.Back() != nil {
			panic("mapCache invariant violation: Front or Back is non-nil when entries.Len() == 0")
		}
	} else if c.entries.Front() == nil || c.entries.Back() == nil {
		panic("mapCache invariant violation: Front or Back is nil when entries.Len() > 0")
	}

	// Invariant 5 & 6: Bidirectional pointer linkage, map-to-list consistency, and size sum parity.
	c.checkListAndIndexInvariants()

	c.checkTelemetryInvariants(c.entries.Len())
}

func (c *mapCache[V]) unlock() {
	defer c.mu.Unlock()
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
}

func (c *mapCache[V]) rUnlock() {
	defer c.mu.RUnlock()
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
}

// evictOne removes and returns the least recently used entry from the cache.
// Caller must hold c.mu (write lock).
func (c *mapCache[V]) evictOne(evictQ *evictCallbackQueue[V]) (V, bool) {
	e := c.entries.Back()
	if e == nil {
		var zero V
		return zero, false
	}
	return c.eraseInternal(evictQ, e.key, EvictionReasonCapacity)
}

func (c *mapCache[V]) finishDeleteReclaimLocked(evictQ *evictCallbackQueue[V], sizeBefore, sampledEpoch uint64, pressure float64) {
	if c.entries.Len() == 0 {
		hadSlack := c.hasEmptyDeleteSlack(false)
		if hadSlack {
			c.recordCompactionByPressure(pressure)
		}
		c.clearEmptyIndexStateLocked()
		c.maybeMarkReclaimed(hadSlack || sizeBefore > 0, sampledEpoch, pressure)
		return
	}
	reclaimedSingleSurvivor := false
	if c.shouldReclaimSingleSurvivorOnDelete(c.entries.Len(), c.hasSlackLocked(), false) {
		if c.compactDataStructuresLocked() {
			c.recordCompactionByPressure(pressure)
		}
		reclaimedSingleSurvivor = true
	}
	c.maybeReclaimUnderPressureLocked(evictQ, pressure, nil, false)
	c.maybeMarkReclaimed(reclaimedSingleSurvivor || c.currentSize < sizeBefore, sampledEpoch, pressure)
}

func (c *mapCache[V]) finishMutationReclaimLocked(evictQ *evictCallbackQueue[V], evictedValues []V, protectedElem *entry[V], reclaimedPre, compactedPre bool, sizeBefore, sampledEpoch uint64, pressure float64) []V {
	evictedByPressure := c.maybeReclaimUnderPressureLocked(evictQ, pressure, protectedElem, false)
	evictedValues = appendEvicted(evictedValues, evictedByPressure)
	netByteReduced := c.currentSize < sizeBefore
	if c.shouldCompactAfterMutation(reclaimedPre, netByteReduced, c.hasSlackLocked(), pressure) {
		if c.compactDataStructuresLocked() && !compactedPre {
			c.recordCompactionByPressure(pressure)
		}
	}
	if c.shouldMarkReclaimedAfterMutation(reclaimedPre, netByteReduced, sampledEpoch, pressure) {
		c.markReclaimedLocked()
	}
	return evictedValues
}

func (c *mapCache[V]) updateExistingOnPutLocked(
	evictQ *evictCallbackQueue[V],
	e *entry[V],
	value V,
	valueSize, sizeBefore uint64,
	pressure float64,
) (evictedValues []V, reclaimedPrePut bool) {
	oldValue := e.value
	oldSize := e.size
	hadZeroCap := c.shouldEvictOnZeroWeightShrink(oldSize, valueSize, c.maxSize)
	c.onEntrySizeUpdated(oldSize, valueSize)
	c.entries.MoveToFront(e)
	c.currentSize -= oldSize
	evictedPrePut := false
	for (valueSize > c.maxSize-c.currentSize || (hadZeroCap && !evictedPrePut)) && c.entries.Len() > 1 {
		if evicted, evictedOK := c.evictOne(evictQ); evictedOK {
			evictedValues = append(evictedValues, evicted)
			evictedPrePut = true
		}
	}
	e.value = value
	e.size = valueSize
	c.currentSize += valueSize
	c.putUpdated++
	c.notifyEvict(evictQ, e.key, oldValue, oldSize, EvictionReasonReplaced)
	reclaimedPrePut = c.shouldReclaimSingleSurvivorOnMutation(c.entries.Len(), c.hasSlackLocked(), false, evictedPrePut, c.currentSize, sizeBefore, pressure)
	return evictedValues, reclaimedPrePut
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
		c.putRejectedOversized.Add(1)
		return nil, ErrInvalidEntrySize
	}

	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	var evictQ evictCallbackQueue[V]
	defer evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	defer c.unlock()

	var evictedValues []V
	sizeBefore := c.currentSize
	reclaimedPrePut := false
	compactedPrePut := false
	evictedPrePut := false

	e, ok := c.index[key]
	if ok {
		evictedValues, reclaimedPrePut = c.updateExistingOnPutLocked(&evictQ, e, value, valueSize, sizeBefore, pressure)
	} else {
		// Evict prior to adding new entry if valueSize would exceed remaining capacity (prevents uint64 overflow).
		for c.shouldEvictPreInsertOnPut(valueSize, c.maxSize, c.currentSize, c.entries.Len(), evictedPrePut) && c.entries.Len() > 0 {
			if evicted, evictedOK := c.evictOne(&evictQ); evictedOK {
				evictedValues = append(evictedValues, evicted)
				evictedPrePut = true
			}
		}
		if c.shouldReclaimEmptyPrePut(c.entries.Len(), false, evictedPrePut, valueSize, sizeBefore, pressure) {
			c.recordCompactionByPressure(pressure)
			c.clearEmptyIndexStateLocked()
			reclaimedPrePut = true
			compactedPrePut = true
		} else if c.shouldReclaimSingleSurvivorOnMutation(c.entries.Len(), c.hasSlackLocked(), false, evictedPrePut, c.currentSize+valueSize, sizeBefore, pressure) {
			reclaimedPrePut = true
		}
		// Clone key to prevent substring keys from pinning large caller backing arrays.
		clonedKey := clonePrefix(key)
		e = c.entries.PushFront(&entry[V]{key: clonedKey, value: value, size: valueSize})
		c.index[clonedKey] = e
		c.putInserted++
		c.onEntryPut(len(c.index), valueSize)
		c.currentSize += valueSize
	}

	evictedValues = c.finishMutationReclaimLocked(&evictQ, evictedValues, e, reclaimedPrePut, compactedPrePut, sizeBefore, sampledEpoch, pressure)
	return evictedValues, nil
}

// eraseInternal removes any entry for the supplied key from the cache without acquiring locks.
// It returns the value of the erased key and true, or the zero value of V and false if not present.
// Caller must hold c.mu (write lock).
func (c *mapCache[V]) eraseInternal(evictQ *evictCallbackQueue[V], key string, reason EvictionReason) (V, bool) {
	e, ok := c.index[key]
	if !ok {
		var zero V
		return zero, false
	}

	evictedKey := e.key
	deletedEntry := e.value
	evictedSize := e.size
	c.onEntryDeleted(evictedSize)
	c.currentSize -= evictedSize

	delete(c.index, key)
	c.dirtyIndex = true
	c.entries.Remove(e)
	var zero V
	e.key = ""
	e.value = zero
	e.size = 0

	c.notifyEvict(evictQ, evictedKey, deletedEntry, evictedSize, reason)
	return deletedEntry, true
}

func (c *mapCache[V]) clearEmptyIndexStateLocked() {
	if c.peakEntryLen > minPeakSlackEntries || c.deletedSinceCompact >= minChurnCompactDeletes {
		c.index = make(map[string]*entry[V])
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
	var evictQ evictCallbackQueue[V]
	defer evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	defer c.unlock()

	sizeBefore := c.currentSize
	deleted, ok := c.eraseInternal(&evictQ, key, EvictionReasonDeleted)
	if !ok {
		c.deleteNotFound++
		return deleted, false
	}
	c.deleteDeleted++
	c.finishDeleteReclaimLocked(&evictQ, sizeBefore, sampledEpoch, pressure)
	return deleted, true
}

// Get retrieves the value associated with key and updates its position to MRU.
// Returns the zero value of V and false if key is not found in the cache.
func (c *mapCache[V]) Get(key string) (V, bool) {
	c.mu.Lock()
	defer c.unlock()

	e, ok := c.index[key]
	if !ok {
		c.getMisses++
		var zero V
		return zero, false
	}
	c.getHits++
	c.entries.MoveToFront(e)
	return e.value, true
}

// Peek retrieves the value associated with key without altering its LRU position.
// Returns the zero value of V and false if key is not found in the cache.
func (c *mapCache[V]) Peek(key string) (V, bool) {
	c.mu.RLock()
	defer c.rUnlock()

	e, ok := c.index[key]
	if !ok {
		c.peekMisses.Add(1)
		var zero V
		return zero, false
	}
	c.peekHits.Add(1)
	return e.value, true
}

func (c *mapCache[V]) canFitGrowthLocked(e *entry[V], newSize, sizeDelta, avail uint64) bool {
	if sizeDelta <= avail {
		return true
	}
	maxNewer := c.maxSize - newSize
	var newerSize uint64
	headCurr := c.entries.Front()
	tailCurr := c.entries.Back()
	for {
		if headCurr == e {
			return newerSize <= maxNewer
		}
		if headCurr != nil {
			newerSize += headCurr.size
			if newerSize > maxNewer {
				return false
			}
			headCurr = headCurr.next
		}
		if tailCurr == nil || tailCurr == e {
			return sizeDelta <= avail
		}
		avail += tailCurr.size
		if sizeDelta <= avail {
			return true
		}
		tailCurr = tailCurr.prev
	}
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
	var evictQ evictCallbackQueue[V]
	defer evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	defer c.unlock()

	e, ok := c.index[key]
	if !ok {
		c.replaceNotFound++
		return ErrEntryNotExist
	}

	oldSize := e.size
	oldValue := e.value

	if newSize > c.maxSize {
		c.replaceSelfEvicted++
		sizeBefore := c.currentSize
		c.eraseInternal(&evictQ, key, EvictionReasonCapacity)
		c.finishDeleteReclaimLocked(&evictQ, sizeBefore, sampledEpoch, pressure)
		return nil
	}

	sizeBefore := c.currentSize
	evictedAny := false
	reclaimedPreUpdate := false

	switch {
	case newSize > oldSize:
		sizeDelta := newSize - oldSize
		if !c.canFitGrowthLocked(e, newSize, sizeDelta, c.maxSize-c.currentSize) {
			c.replaceSelfEvicted++
			c.eraseInternal(&evictQ, key, EvictionReasonCapacity)
			c.finishDeleteReclaimLocked(&evictQ, sizeBefore, sampledEpoch, pressure)
			return nil
		}

		for sizeDelta > c.maxSize-c.currentSize && c.entries.Len() > 0 && c.entries.Back() != e {
			c.evictOne(&evictQ)
			evictedAny = true
		}

		c.onEntrySizeUpdated(oldSize, newSize)
		c.currentSize += sizeDelta
	case newSize < oldSize:
		if c.shouldEvictOnZeroWeightShrink(oldSize, newSize, c.maxSize) && c.entries.Back() != e {
			c.evictOne(&evictQ)
			evictedAny = true
		}
		c.onEntrySizeUpdated(oldSize, newSize)
		c.currentSize -= oldSize - newSize
	}
	e.value = value
	e.size = newSize

	c.replaceUpdated++
	c.notifyEvict(&evictQ, e.key, oldValue, oldSize, EvictionReasonReplaced)

	if c.shouldReclaimSingleSurvivorOnMutation(c.entries.Len(), c.hasSlackLocked(), false, evictedAny, c.currentSize, sizeBefore, pressure) {
		reclaimedPreUpdate = true
	}

	c.finishMutationReclaimLocked(&evictQ, nil, e, reclaimedPreUpdate, false, sizeBefore, sampledEpoch, pressure)

	return nil
}

func (c *mapCache[V]) deleteAllPrefixLocked(evictQ *evictCallbackQueue[V], sampledEpoch uint64, pressure float64) {
	c.deletePrefixExecuted++
	hadEntries := c.entries.Len() > 0
	hadDirtySlack := c.dirtyIndex || c.peakEntryLen > minPeakSlackEntries || c.deletedSinceCompact > 0
	if !hadEntries && !hadDirtySlack && c.peakEntryLen == 0 {
		return
	}
	hadReclaimable := c.currentSize > 0 || hadDirtySlack
	c.deletedSinceCompact += c.entries.Len()
	hadCompactionSlack := c.hasEmptyDeleteSlack(false)
	var zero V
	for e := c.entries.Front(); e != nil; {
		next := e.next
		evictedKey := e.key
		evictedVal := e.value
		evictedSize := e.size
		c.currentSize -= evictedSize
		e.key = ""
		e.value = zero
		e.size = 0
		e.prev = nil
		e.next = nil
		c.notifyEvict(evictQ, evictedKey, evictedVal, evictedSize, EvictionReasonDeleted)
		e = next
	}
	c.entries.Init()
	c.currentSize = 0
	c.clearEmptyIndexStateLocked()
	c.finishClearAllLocked(hadCompactionSlack, hadReclaimable, sampledEpoch, pressure)
}

// DeletePrefix removes all entries from the cache whose keys start with prefix.
// If prefix is empty (""), all entries in the cache are deleted.
func (c *mapCache[V]) DeletePrefix(prefix string) {
	if prefix == "" {
		c.mu.Lock()
		hadEntries := c.entries.Len() > 0
		hadDirtySlack := c.dirtyIndex || c.peakEntryLen > minPeakSlackEntries || c.deletedSinceCompact > 0
		if !hadEntries && !hadDirtySlack && c.peakEntryLen == 0 {
			c.deletePrefixExecuted++
			c.unlock()
			return
		}
		c.mu.Unlock()

		sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
		var evictQ evictCallbackQueue[V]
		defer evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
		defer c.unlock()
		c.deleteAllPrefixLocked(&evictQ, sampledEpoch, pressure)
		return
	}

	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	var evictQ evictCallbackQueue[V]
	defer evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	defer c.unlock()

	c.deletePrefixExecuted++
	sizeBefore := c.currentSize
	erasedAny := false
	for key := range c.index {
		if strings.HasPrefix(key, prefix) {
			c.eraseInternal(&evictQ, key, EvictionReasonDeleted)
			erasedAny = true
		}
	}
	if !erasedAny {
		return
	}
	c.finishDeleteReclaimLocked(&evictQ, sizeBefore, sampledEpoch, pressure)
}

// All returns an iterator over all key-value pairs in MRU-to-LRU order without modifying recency.
func (c *mapCache[V]) All() iter.Seq2[string, V] {
	return c.all
}

func (c *mapCache[V]) all(yield func(string, V) bool) {
	c.mu.RLock()
	defer c.rUnlock()

	for e := c.entries.Front(); e != nil; e = e.next {
		if !yield(e.key, e.value) {
			return
		}
	}
}

// Keys returns an iterator over all keys in MRU-to-LRU order without modifying recency.
func (c *mapCache[V]) Keys() iter.Seq[string] {
	return c.keys
}

func (c *mapCache[V]) keys(yield func(string) bool) {
	c.mu.RLock()
	defer c.rUnlock()

	for e := c.entries.Front(); e != nil; e = e.next {
		if !yield(e.key) {
			return
		}
	}
}

// Values returns an iterator over all values in MRU-to-LRU order without modifying recency.
func (c *mapCache[V]) Values() iter.Seq[V] {
	return c.values
}

func (c *mapCache[V]) values(yield func(V) bool) {
	c.mu.RLock()
	defer c.rUnlock()

	for e := c.entries.Front(); e != nil; e = e.next {
		if !yield(e.value) {
			return
		}
	}
}

// Stats returns a point-in-time telemetry snapshot of the cache.
func (c *mapCache[V]) Stats() Stats {
	c.mu.RLock()
	defer c.rUnlock()
	return c.snapshotBaseStats(BackendMap, c.currentSize, c.maxSize, c.entries.Len())
}

func (c *mapCache[V]) shouldAutoCompactLocked(isBackground bool) bool {
	return c.shouldAutoCompactEntryCounts(c.hasSlackLocked(), isBackground, len(c.index))
}

// Compact reallocates the internal hash index to reclaim Go map bucket slack while preserving all live entries.
func (c *mapCache[V]) Compact() {
	c.mu.Lock()
	defer c.unlock()
	c.compactLocked()
}

func (c *mapCache[V]) compactDataStructuresLocked() bool {
	if !c.hasSlackLocked() {
		return false
	}
	newIndex := make(map[string]*entry[V], len(c.index))
	maps.Copy(newIndex, c.index)
	c.index = newIndex
	c.dirtyIndex = false
	c.onCompacted(len(c.index))
	return true
}

func (c *mapCache[V]) compactLocked() {
	if c.compactDataStructuresLocked() {
		c.compactionsExplicit++
		c.markReclaimedLocked()
	}
}

func (c *mapCache[V]) completeShedAndCompactLocked(evictedCount int, retention float64, unadjusted bool, unprotectedZeroTarget, targetLen int, isBackground bool) {
	if evictedCount > 0 {
		c.recordPressureShed(isBackground)
		if c.entries.Len() == 0 {
			if c.shouldAutoCompactEntryCounts(true, isBackground, 0) {
				c.compactionsPressureTier2++
			}
			c.resetEmptyIndexLocked()
			return
		}
		c.updateZeroWatermarkAfterShed(retention, c.entries.Len(), unadjusted, unprotectedZeroTarget, targetLen)
	}
	compacted := false
	if c.shouldAutoCompactLocked(isBackground) {
		compacted = c.compactDataStructuresLocked()
		if compacted {
			c.compactionsPressureTier2++
		}
	}
	if evictedCount > 0 || compacted {
		c.markReclaimedLocked()
	}
}

func (c *mapCache[V]) shedAndCompactLocked(evictQ *evictCallbackQueue[V], targetSize uint64, retention float64, protectedElem *entry[V], isBackground bool) []V {
	var protectedSize uint64
	hasProtected := protectedElem != nil
	if hasProtected {
		protectedSize = protectedElem.size
	}
	effectiveTarget, targetLen, targetZeroCount, unprotectedZeroTarget := c.computeShedTargets(targetSize, retention, c.entries.Len(), hasProtected, protectedSize)

	needFullFlush := retention == 0.0
	var evicted []V
	for victim := c.entries.Back(); victim != nil; {
		needByteShed, needZeroShed := c.shouldContinueShedding(c.currentSize, effectiveTarget, needFullFlush, targetZeroCount, c.entries.Len(), targetLen)
		if !needByteShed && !needZeroShed {
			break
		}
		nextVictim := victim.prev
		if c.shouldEvictShedVictim(victim == protectedElem, victim.size, needFullFlush, needByteShed, targetZeroCount) {
			if val, ok := c.eraseInternal(evictQ, victim.key, EvictionReasonPressure); ok {
				evicted = append(evicted, val)
			}
		}
		victim = nextVictim
	}

	c.completeShedAndCompactLocked(len(evicted), retention, !hasProtected || effectiveTarget <= targetSize, unprotectedZeroTarget, targetLen, isBackground)
	return evicted
}

func (c *mapCache[V]) maybeReclaimUnderPressureLocked(evictQ *evictCallbackQueue[V], pressure float64, protectedElem *entry[V], isBackground bool) []V {
	if c.isSamplingGoroutine() {
		return nil
	}
	if pressure >= c.options.EvictionThreshold {
		retention := c.options.EvictionRetentionRatio
		targetSize := computeTargetSize(c.maxSize, retention)
		return c.shedAndCompactLocked(evictQ, targetSize, retention, protectedElem, isBackground)
	}
	c.resetZeroWatermarkBelowTier2(pressure)
	if pressure >= c.options.CompactionThreshold && c.shouldAutoCompactLocked(isBackground) {
		if c.compactDataStructuresLocked() {
			c.compactionsPressureTier1++
			c.markReclaimedLocked()
		}
	}
	return nil
}

// EvaluateMemoryPressure samples the configured memory-pressure probe and executes
// Tier 2 (LRU shedding + map compaction) or Tier 1 (lossless map compaction) if thresholds are met.
func (c *mapCache[V]) EvaluateMemoryPressure() []V {
	_, pressure := c.lockWithPressure(&c.mu, true)
	var evictQ evictCallbackQueue[V]
	defer evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	defer c.unlock()

	return c.maybeReclaimUnderPressureLocked(&evictQ, pressure, nil, true)
}
