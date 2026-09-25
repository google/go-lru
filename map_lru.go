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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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
	dirtyIndex             bool
	deletedSinceCompact    int
	peakIndexLen           int
	zeroSizeCount          int
	lastReclaimedZeroCount int

	// mu synchronizes access to internal state.
	mu sync.RWMutex

	// checkInvariantsEnabled indicates whether invariant checks are executed on unlock.
	checkInvariantsEnabled bool

	// options holds the parsed cache configuration.
	options Options

	// Atomic state for lock-free pressure sampling and reclamation epoch synchronization.
	samplingPressure        atomic.Bool
	fallbackSampling        atomic.Bool
	pressureInitialized     atomic.Bool
	lastSampledInitialized  atomic.Bool
	pressureNeedsRefresh    atomic.Bool
	overflowSamplingCount   atomic.Int32
	samplingGID             atomic.Uint64
	fallbackGID             atomic.Uint64
	overflowSamplingGIDs    sync.Map
	pressureSampleSeq       atomic.Uint64
	cachedPressureBits      atomic.Uint64
	cachedPressureEpoch     atomic.Uint64
	lastSampledPressureBits atomic.Uint64
	lastSampledEpoch        atomic.Uint64
	reclaimEpoch            atomic.Uint64
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
	if c.checkInvariantsEnabled {
		c.checkInvariants()
	}
	c.mu.Unlock()
}

func (c *mapCache) rLock() {
	c.mu.RLock()
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
	entryVal := e.Value.(*entry)
	return c.eraseInternal(entryVal.key)
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

	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressure()

	c.lock()
	for sampledEpoch != c.reclaimEpoch.Load() {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressure()
		c.mu.Lock()
	}
	defer c.unlock()

	var evictedValues []ValueType

	e, ok := c.index[key]
	if ok {
		// Update existing entry in place (0 heap allocations).
		entryVal := e.Value.(*entry)
		if entryVal.size == 0 && valueSize > 0 && c.zeroSizeCount > 0 {
			c.zeroSizeCount--
			if c.zeroSizeCount < c.lastReclaimedZeroCount {
				c.lastReclaimedZeroCount = c.zeroSizeCount
			}
		} else if entryVal.size > 0 && valueSize == 0 {
			c.zeroSizeCount++
		}
		c.entries.MoveToFront(e)
		c.currentSize -= entryVal.size
		for valueSize > c.maxSize-c.currentSize && c.entries.Len() > 1 {
			evicted := c.evictOne()
			if evicted != nil {
				evictedValues = append(evictedValues, evicted)
			}
		}
		entryVal.value = value
		entryVal.size = valueSize
		c.currentSize += valueSize
	} else {
		// Evict prior to adding new entry if valueSize would exceed remaining capacity (prevents uint64 overflow).
		for valueSize > c.maxSize-c.currentSize && c.entries.Len() > 0 {
			evicted := c.evictOne()
			if evicted != nil {
				evictedValues = append(evictedValues, evicted)
			}
		}
		// Clone key to prevent substring keys from pinning large caller backing arrays.
		clonedKey := strings.Clone(key)
		e = c.entries.PushFront(&entry{key: clonedKey, value: value, size: valueSize})
		c.index[clonedKey] = e
		if len(c.index) > c.peakIndexLen {
			c.peakIndexLen = len(c.index)
		}
		if valueSize == 0 {
			c.zeroSizeCount++
		}
		c.currentSize += valueSize
	}

	if evictedByPressure := c.maybeReclaimUnderPressureLocked(pressure, e); len(evictedByPressure) > 0 {
		evictedValues = append(evictedValues, evictedByPressure...)
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

	entryVal := e.Value.(*entry)
	deletedEntry := entryVal.value
	if entryVal.size == 0 && c.zeroSizeCount > 0 {
		c.zeroSizeCount--
		if c.zeroSizeCount < c.lastReclaimedZeroCount {
			c.lastReclaimedZeroCount = c.zeroSizeCount
		}
	}
	c.currentSize -= entryVal.size

	delete(c.index, key)
	c.dirtyIndex = true
	c.deletedSinceCompact++
	c.entries.Remove(e)

	return deletedEntry
}

// resetEmptyIndexLocked releases peak map bucket memory when the cache transitions to empty.
func (c *mapCache) resetEmptyIndexLocked() {
	c.index = make(map[string]*list.Element)
	c.dirtyIndex = false
	c.deletedSinceCompact = 0
	c.peakIndexLen = 0
	c.zeroSizeCount = 0
	c.lastReclaimedZeroCount = 0
	c.markReclaimedLocked()
}

// Erase removes the entry associated with the given key from the cache.
// Returns the value of the erased entry, or nil if the key was not found.
func (c *mapCache) Erase(key string) ValueType {
	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressure()

	c.lock()
	for sampledEpoch != c.reclaimEpoch.Load() {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressure()
		c.mu.Lock()
	}
	defer c.unlock()

	deleted := c.eraseInternal(key)
	if deleted == nil {
		c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectElem)
		return nil
	}
	if c.entries.Len() == 0 {
		c.resetEmptyIndexLocked()
		return deleted
	}
	c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectElem)
	if c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
		c.markReclaimedLocked()
	}
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
	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressure()

	c.lock()
	for sampledEpoch != c.reclaimEpoch.Load() {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressure()
		c.mu.Lock()
	}
	defer c.unlock()

	e, ok := c.index[key]
	if !ok {
		c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectElem)
		return ErrEntryNotExist
	}

	entryVal := e.Value.(*entry)
	if math.MaxUint64-entryVal.size < sizeDelta {
		return ErrInvalidUpdateEntrySize
	}

	avail := c.maxSize - c.currentSize
	if entryVal.size+sizeDelta > c.maxSize {
		avail = 0
	} else {
		for curr := c.entries.Back(); curr != nil && curr != e && sizeDelta > avail; curr = curr.Prev() {
			avail += curr.Value.(*entry).size
		}
	}
	if entryVal.size+sizeDelta > c.maxSize || sizeDelta > avail {
		c.eraseInternal(entryVal.key)
		if c.entries.Len() == 0 {
			c.resetEmptyIndexLocked()
			return nil
		}
		c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectElem)
		if c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
			c.markReclaimedLocked()
		}
		return nil
	}

	evictedAny := false
	for sizeDelta > c.maxSize-c.currentSize && c.entries.Len() > 0 {
		if c.entries.Back() == e {
			break
		}
		c.evictOne()
		evictedAny = true
	}

	if math.MaxUint64-c.currentSize < sizeDelta {
		return ErrInvalidUpdateEntrySize
	}

	if entryVal.size == 0 && sizeDelta > 0 && c.zeroSizeCount > 0 {
		c.zeroSizeCount--
		if c.zeroSizeCount < c.lastReclaimedZeroCount {
			c.lastReclaimedZeroCount = c.zeroSizeCount
		}
	}
	entryVal.size += sizeDelta
	c.currentSize += sizeDelta

	protectedElem := &foregroundNoProtectElem
	if sizeDelta > 0 && e == c.entries.Front() {
		protectedElem = e
	}
	c.maybeReclaimUnderPressureLocked(pressure, protectedElem)
	if evictedAny && c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
		c.markReclaimedLocked()
	}

	return nil
}

// EraseEntriesWithGivenPrefix removes all entries from the cache whose keys start with prefix.
// If prefix is empty (""), all entries in the cache are erased.
func (c *mapCache) EraseEntriesWithGivenPrefix(prefix string) {
	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressure()

	c.lock()
	for sampledEpoch != c.reclaimEpoch.Load() {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressure()
		c.mu.Lock()
	}
	defer c.unlock()

	if prefix == "" {
		c.entries.Init()
		c.currentSize = 0
		c.resetEmptyIndexLocked()
		return
	}

	erasedAny := false
	for key := range c.index {
		if strings.HasPrefix(key, prefix) {
			c.eraseInternal(key)
			erasedAny = true
		}
	}
	if c.entries.Len() == 0 && c.dirtyIndex {
		c.resetEmptyIndexLocked()
		return
	}
	c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectElem)
	if erasedAny && c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
		c.markReclaimedLocked()
	}
}

func (c *mapCache) hasOverflowSamplingGID(gid uint64) bool {
	if gid == 0 || c.overflowSamplingCount.Load() <= 0 {
		return false
	}
	_, ok := c.overflowSamplingGIDs.Load(gid)
	return ok
}

func (c *mapCache) isCurrentGoroutineSampling(gid uint64) bool {
	if gid == 0 {
		return false
	}
	return c.samplingGID.Load() == gid ||
		c.fallbackGID.Load() == gid ||
		c.hasOverflowSamplingGID(gid)
}

func (c *mapCache) isSamplingGoroutine() bool {
	if !c.options.hasCustomPressureFunc {
		return false
	}
	sGID := c.samplingGID.Load()
	fGID := c.fallbackGID.Load()
	oCount := c.overflowSamplingCount.Load()
	if sGID == 0 && fGID == 0 && oCount <= 0 {
		return false
	}
	return c.isCurrentGoroutineSampling(currentGoroutineID())
}

func (c *mapCache) markReclaimedLocked() {
	if c.isSamplingGoroutine() {
		return
	}
	c.reclaimEpoch.Add(1)
	c.pressureNeedsRefresh.Store(true)
	c.pressureInitialized.Store(false)
	c.lastSampledInitialized.Store(false)
	c.cachedPressureBits.Store(0)
	c.lastSampledPressureBits.Store(0)
}

func (c *mapCache) hasElevatedPressureToInvalidate(pressure float64) bool {
	thresh := c.options.CompactionThreshold
	return pressure >= thresh ||
		math.Float64frombits(c.cachedPressureBits.Load()) >= thresh ||
		math.Float64frombits(c.lastSampledPressureBits.Load()) >= thresh ||
		c.pressureNeedsRefresh.Load() ||
		c.samplingPressure.Load() ||
		c.fallbackSampling.Load() ||
		c.overflowSamplingCount.Load() > 0
}

func (c *mapCache) shouldAutoCompactLocked(protectedElem *list.Element) bool {
	if protectedElem == nil {
		return c.dirtyIndex
	}
	return c.dirtyIndex && c.peakIndexLen > len(c.index) && uint64(c.peakIndexLen-len(c.index))*4 >= uint64(c.peakIndexLen)
}

// Compact reallocates the internal hash index to reclaim Go map bucket slack while preserving all live entries.
func (c *mapCache) Compact() {
	c.lock()
	defer c.unlock()
	c.compactLocked()
}

func (c *mapCache) compactLocked() {
	if !c.dirtyIndex {
		return
	}
	newIndex := make(map[string]*list.Element, len(c.index))
	maps.Copy(newIndex, c.index)
	c.index = newIndex
	c.dirtyIndex = false
	c.deletedSinceCompact = 0
	c.peakIndexLen = len(c.index)
	c.markReclaimedLocked()
}

func (c *mapCache) storeSampledPressure(epoch uint64, p float64) {
	if c.reclaimEpoch.Load() == epoch {
		bits := math.Float64bits(p)
		c.lastSampledPressureBits.Store(bits)
		c.lastSampledEpoch.Store(epoch)
		c.lastSampledInitialized.Store(true)
		c.cachedPressureBits.Store(bits)
		c.cachedPressureEpoch.Store(epoch)
		c.pressureInitialized.Store(true)
		c.pressureNeedsRefresh.Store(false)
		if c.reclaimEpoch.Load() != epoch {
			c.pressureNeedsRefresh.Store(true)
			c.pressureInitialized.Store(false)
			c.lastSampledInitialized.Store(false)
			c.cachedPressureBits.Store(0)
			c.lastSampledPressureBits.Store(0)
		}
	} else {
		c.pressureNeedsRefresh.Store(true)
	}
}

// samplePressureFresh evaluates c.options.PressureFunc lock-free outside c.mu.Lock()
// with a goroutine-aware re-entrancy guard and cold-start fallback.
func (c *mapCache) samplePressureFresh() float64 {
	if c.options.PressureFunc == nil {
		return 0.0
	}
	if c.isSamplingGoroutine() {
		return 0.0
	}
	if !c.samplingPressure.CompareAndSwap(false, true) {
		var gid uint64
		if c.options.hasCustomPressureFunc {
			gid = currentGoroutineID()
			if c.isCurrentGoroutineSampling(gid) {
				return 0.0
			}
		}
		if epoch := c.reclaimEpoch.Load(); c.pressureInitialized.Load() && c.cachedPressureEpoch.Load() == epoch {
			bits := c.cachedPressureBits.Load()
			if c.pressureInitialized.Load() && c.cachedPressureEpoch.Load() == epoch && c.reclaimEpoch.Load() == epoch {
				return math.Float64frombits(bits)
			}
		}
		if epoch := c.reclaimEpoch.Load(); c.lastSampledInitialized.Load() && c.lastSampledEpoch.Load() == epoch {
			bits := c.lastSampledPressureBits.Load()
			if c.lastSampledInitialized.Load() && c.lastSampledEpoch.Load() == epoch && c.reclaimEpoch.Load() == epoch {
				return math.Float64frombits(bits)
			}
		}
		for range 100 {
			epoch := c.reclaimEpoch.Load()
			if !c.samplingPressure.Load() ||
				(c.pressureInitialized.Load() && c.cachedPressureEpoch.Load() == epoch) ||
				(c.lastSampledInitialized.Load() && c.lastSampledEpoch.Load() == epoch) {
				break
			}
			runtime.Gosched()
		}
		if epoch := c.reclaimEpoch.Load(); c.pressureInitialized.Load() && c.cachedPressureEpoch.Load() == epoch {
			bits := c.cachedPressureBits.Load()
			if c.pressureInitialized.Load() && c.cachedPressureEpoch.Load() == epoch && c.reclaimEpoch.Load() == epoch {
				return math.Float64frombits(bits)
			}
		}
		if epoch := c.reclaimEpoch.Load(); c.lastSampledInitialized.Load() && c.lastSampledEpoch.Load() == epoch {
			bits := c.lastSampledPressureBits.Load()
			if c.lastSampledInitialized.Load() && c.lastSampledEpoch.Load() == epoch && c.reclaimEpoch.Load() == epoch {
				return math.Float64frombits(bits)
			}
		}
		if c.options.hasCustomPressureFunc && c.isCurrentGoroutineSampling(gid) {
			return 0.0
		}
		if c.fallbackSampling.CompareAndSwap(false, true) {
			if c.options.hasCustomPressureFunc {
				c.fallbackGID.Store(gid)
				defer func() {
					c.fallbackGID.Store(0)
					c.fallbackSampling.Store(false)
				}()
			} else {
				defer c.fallbackSampling.Store(false)
			}
		} else {
			if c.options.hasCustomPressureFunc {
				c.overflowSamplingGIDs.Store(gid, struct{}{})
			}
			c.overflowSamplingCount.Add(1)
			defer func() {
				if c.options.hasCustomPressureFunc {
					c.overflowSamplingGIDs.Delete(gid)
				}
				c.overflowSamplingCount.Add(-1)
			}()
		}
		epoch := c.reclaimEpoch.Load()
		p := c.options.PressureFunc()
		if math.IsNaN(p) || p < 0.0 {
			p = 0.0
		}
		c.storeSampledPressure(epoch, p)
		return p
	}
	if c.options.hasCustomPressureFunc {
		c.samplingGID.Store(currentGoroutineID())
		defer func() {
			c.samplingGID.Store(0)
			c.samplingPressure.Store(false)
		}()
	} else {
		defer c.samplingPressure.Store(false)
	}

	epoch := c.reclaimEpoch.Load()
	pressure := c.options.PressureFunc()
	if math.IsNaN(pressure) || pressure < 0.0 {
		pressure = 0.0
	}
	c.storeSampledPressure(epoch, pressure)
	return pressure
}

func (c *mapCache) samplePressure() float64 {
	if c.options.hasCustomPressureFunc {
		return c.samplePressureFresh()
	}
	epoch := c.reclaimEpoch.Load()
	seq := c.pressureSampleSeq.Add(1)
	if (seq&255) == 1 || !c.pressureInitialized.Load() || c.pressureNeedsRefresh.Load() || c.cachedPressureEpoch.Load() != epoch {
		return c.samplePressureFresh()
	}
	bits := c.cachedPressureBits.Load()
	if !c.pressureInitialized.Load() || c.pressureNeedsRefresh.Load() || c.cachedPressureEpoch.Load() != epoch || c.reclaimEpoch.Load() != epoch {
		return c.samplePressureFresh()
	}
	return math.Float64frombits(bits)
}

func (c *mapCache) shedAndCompactLocked(targetSize uint64, retention float64, protectedElem *list.Element) []ValueType {
	autoCompactElem := protectedElem
	if protectedElem != nil && (protectedElem != c.entries.Front() || protectedElem.Prev() != nil) {
		protectedElem = nil
	}

	effectiveTarget := targetSize
	if protectedElem != nil && retention > 0.0 {
		if protectedSize := protectedElem.Value.(*entry).size; protectedSize > effectiveTarget {
			effectiveTarget = protectedSize
		}
	}

	targetLen := 0
	targetZeroCount := 0
	if retention > 0.0 {
		if c.entries.Len() > 0 {
			targetLen = int(float64(c.entries.Len()) * retention)
			if protectedElem != nil && targetLen < 1 {
				targetLen = 1
			}
		}
		if c.zeroSizeCount > 0 {
			targetZeroCount = int(float64(c.zeroSizeCount) * retention)
			if protectedElem != nil && protectedElem.Value.(*entry).size == 0 && targetZeroCount < 1 {
				targetZeroCount = 1
			}
			if c.lastReclaimedZeroCount > targetZeroCount {
				targetZeroCount = c.lastReclaimedZeroCount
			}
		}
	}

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
		case needFullFlush || (needByteShed && needZeroShed):
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

	if retention > 0.0 && c.zeroSizeCount > 0 {
		c.lastReclaimedZeroCount = c.zeroSizeCount
	} else {
		c.lastReclaimedZeroCount = 0
	}

	c.markReclaimedLocked()
	if c.shouldAutoCompactLocked(autoCompactElem) {
		c.compactLocked()
	}
	return evicted
}

func (c *mapCache) maybeReclaimUnderPressureLocked(pressure float64, protectedElem *list.Element) []ValueType {
	autoCompactElem := protectedElem
	shedProtectedElem := protectedElem
	if shedProtectedElem != nil && (shedProtectedElem != c.entries.Front() || shedProtectedElem.Prev() != nil) {
		shedProtectedElem = nil
	}
	epochBefore := c.reclaimEpoch.Load()
	var evicted []ValueType
	if pressure >= c.options.EvictionThreshold {
		retention := c.options.EvictionRetentionRatio
		targetSize := computeTargetSize(c.maxSize, retention)
		targetLen := int(float64(c.entries.Len()) * retention)
		if shedProtectedElem != nil && retention > 0.0 && targetLen < 1 {
			targetLen = 1
		}
		targetZeroCount := int(float64(c.zeroSizeCount) * retention)
		if shedProtectedElem != nil && retention > 0.0 && shedProtectedElem.Value.(*entry).size == 0 && targetZeroCount < 1 {
			targetZeroCount = 1
		}
		if retention > 0.0 && c.lastReclaimedZeroCount > targetZeroCount {
			targetZeroCount = c.lastReclaimedZeroCount
		}
		if c.currentSize > targetSize || (c.zeroSizeCount > targetZeroCount && c.entries.Len() > targetLen) || (retention == 0.0 && c.entries.Len() > 0) {
			evicted = c.shedAndCompactLocked(targetSize, retention, autoCompactElem)
		} else if c.shouldAutoCompactLocked(autoCompactElem) {
			c.compactLocked()
		}
	} else {
		c.lastReclaimedZeroCount = 0
		if pressure >= c.options.CompactionThreshold {
			if c.shouldAutoCompactLocked(autoCompactElem) {
				c.compactLocked()
			}
		}
	}
	if autoCompactElem != nil && autoCompactElem != &foregroundNoProtectElem && c.reclaimEpoch.Load() == epochBefore {
		c.lastSampledPressureBits.Store(math.Float64bits(pressure))
		c.lastSampledEpoch.Store(epochBefore)
		c.lastSampledInitialized.Store(true)
		if c.reclaimEpoch.Load() != epochBefore {
			c.lastSampledInitialized.Store(false)
			c.lastSampledPressureBits.Store(0)
		}
	}
	return evicted
}

// EvaluateMemoryPressure samples the configured memory-pressure probe and executes
// Tier 2 (LRU shedding + map compaction) or Tier 1 (lossless map compaction) if thresholds are met.
func (c *mapCache) EvaluateMemoryPressure() []ValueType {
	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressureFresh()

	c.lock()
	defer c.unlock()

	for sampledEpoch != c.reclaimEpoch.Load() {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressureFresh()
		c.mu.Lock()
	}

	return c.maybeReclaimUnderPressureLocked(pressure, nil)
}
