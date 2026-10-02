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
)

// NewArenaRadixCache returns a new arena-backed radix LRU Cache[V] bounded by maxSize.
// Optional configuration parameters can be passed via opts (e.g. WithWeigher, WithInvariantChecking).
//
// maxSize must be greater than zero; otherwise NewArenaRadixCache panics.
func NewArenaRadixCache[V any](maxSize uint64, opts ...Option) Cache[V] {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	return newArenaRadixCacheWithOptions[V](maxSize, ApplyOptions(opts...))
}

func newArenaRadixCacheWithOptions[V any](maxSize uint64, options Options) Cache[V] {
	c := &arenaRadix[V]{
		maxSize:       maxSize,
		weigher:       resolveWeigher[V](options),
		onEvictValue:  resolveOnEvictValue[V](options),
		onEvictEntry:  resolveOnEvictEntry[V](options),
		pressureState: pressureState{options: options},
	}

	c.clearEmptyArenaStateLocked()

	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}

	return c
}

func (c *arenaRadix[V]) weigh(key string, value V) uint64 {
	if c.weigher != nil {
		return c.weigher(key, value)
	}
	return 1
}

// checkInvariants performs full-tree iterative invariant validation:
// 1. maxSize > 0 and currentSize <= maxSize
// 2. Bidirectional LRU list pointer integrity and size sum parity
// 3. Root structure validity
// 4. LCRS tree hierarchy (parent-child links, prefix non-emptiness, sorted siblings, compactness)
// 5. Value-bearing node reachability and tree-to-LRU bijection
// Uses non-recursive O(1)-space pre-order tree traversal to prevent stack overflows.
func (c *arenaRadix[V]) checkInvariants() {
	// INVARIANT 1: maxSize > 0
	if c.maxSize == 0 {
		panic("arenaRadix invariant violation: maxSize must be greater than 0")
	}

	// INVARIANT 2: currentSize <= maxSize
	if c.currentSize > c.maxSize {
		panic(fmt.Sprintf("arenaRadix invariant violation: currentSize %d exceeds maxSize %d", c.currentSize, c.maxSize))
	}

	// INVARIANT 3: LRU list validation
	lruCount := 0
	zeroCount := 0
	var sumSize uint64
	prevID := nilNode

	for currID := c.head; currID != nilNode; currID = c.nodes[currID].next {
		if currID >= uint32(len(c.nodes)) {
			panic(fmt.Sprintf("arenaRadix invariant violation: LRU list contains out-of-bounds index %d (len=%d)", currID, len(c.nodes)))
		}
		lruCount++
		if math.MaxUint64-sumSize < c.nodes[currID].size {
			panic("arenaRadix invariant violation: sumSize uint64 overflow")
		}
		sumSize += c.nodes[currID].size
		if c.nodes[currID].size == 0 {
			zeroCount++
		}
		if !c.nodes[currID].hasValue {
			panic(fmt.Sprintf("arenaRadix invariant violation: unexpected hasValue=false in LRU list for prefix '%s'", c.nodes[currID].prefix))
		}

		// Bidirectional link validation
		if c.nodes[currID].prev != prevID {
			panic(fmt.Sprintf("arenaRadix invariant violation: corrupt prev pointer in LRU list for prefix '%s'", c.nodes[currID].prefix))
		}
		if prevID == nilNode {
			if c.head != currID {
				panic("arenaRadix invariant violation: head mismatch in LRU list")
			}
		} else {
			if c.nodes[prevID].next != currID {
				panic(fmt.Sprintf("arenaRadix invariant violation: corrupt next pointer in LRU list for prefix '%s'", c.nodes[prevID].prefix))
			}
		}

		if c.nodes[currID].next == nilNode {
			if c.tail != currID {
				panic("arenaRadix invariant violation: tail mismatch in LRU list")
			}
		}

		prevID = currID
	}

	if lruCount != c.len {
		panic(fmt.Sprintf("arenaRadix invariant violation: LRU list count %d does not match tracked len %d", lruCount, c.len))
	}

	if zeroCount != c.zeroSizeCount {
		panic(fmt.Sprintf("arenaRadix invariant violation: zeroSizeCount %d does not match live zero-size entries %d", c.zeroSizeCount, zeroCount))
	}

	if sumSize != c.currentSize {
		panic(fmt.Sprintf("arenaRadix: currentSize drift: currentSize=%d sumSize=%d", c.currentSize, sumSize))
	}

	if c.len == 0 {
		if c.head != nilNode || c.tail != nilNode {
			panic("arenaRadix invariant violation: head or tail is non-nilNode when len is 0")
		}
	} else {
		if c.head == nilNode || c.tail == nilNode {
			panic("arenaRadix invariant violation: head or tail is nilNode when len > 0")
		}
		if c.head >= uint32(len(c.nodes)) || c.tail >= uint32(len(c.nodes)) {
			panic("arenaRadix invariant violation: head or tail index out of bounds")
		}
		if c.nodes[c.head].prev != nilNode {
			panic("arenaRadix invariant violation: head prev pointer is not nilNode")
		}
		if c.nodes[c.tail].next != nilNode {
			panic("arenaRadix invariant violation: tail next pointer is not nilNode")
		}
	}

	// INVARIANT 4: Root structure checks
	if c.root == nilNode || c.root >= uint32(len(c.nodes)) {
		panic("arenaRadix invariant violation: root node is nilNode or out of bounds")
	}
	if c.nodes[c.root].prefix != "" {
		panic("arenaRadix invariant violation: root node must have empty prefix")
	}
	if c.nodes[c.root].parent != nilNode {
		panic("arenaRadix invariant violation: root node must not have a parent")
	}
	if c.nodes[c.root].sibling != nilNode {
		panic("arenaRadix invariant violation: root node must not have siblings")
	}

	// INVARIANT 5: Iterative pre-order traversal using parent/sibling pointers (O(1) space).
	// Validates tree integrity, sibling sorted order, parent pointers, compactness,
	// and 1:1 bijection between value-bearing nodes and LRU list elements.
	treeCount := 0
	treeNodeCount := 0
	var treeSumSize uint64
	currID := c.root
	for currID != nilNode {
		if currID >= uint32(len(c.nodes)) {
			panic(fmt.Sprintf("arenaRadix invariant violation: tree contains out-of-bounds index %d (len=%d)", currID, len(c.nodes)))
		}
		treeNodeCount++
		if c.nodes[currID].hasValue {
			treeCount++
			if math.MaxUint64-treeSumSize < c.nodes[currID].size {
				panic("arenaRadix invariant violation: treeSumSize uint64 overflow")
			}
			treeSumSize += c.nodes[currID].size
			// A node is verifiably in the LRU list iff it is the head (with nilNode prev) or its prev's next points back to it.
			prev := c.nodes[currID].prev
			inLRU := (c.head == currID && prev == nilNode) || (prev != nilNode && prev < uint32(len(c.nodes)) && c.nodes[prev].next == currID)
			if !inLRU {
				panic(fmt.Sprintf("arenaRadix invariant violation: node with prefix '%s' has value but is missing from LRU list", c.nodes[currID].prefix))
			}
		} else {
			if c.nodes[currID].size != 0 {
				panic(fmt.Sprintf("arenaRadix invariant violation: routing node %d has non-zero size %d", currID, c.nodes[currID].size))
			}
			if c.nodes[currID].prev != nilNode || c.nodes[currID].next != nilNode || c.head == currID || c.tail == currID {
				panic(fmt.Sprintf("arenaRadix invariant violation: routing node %d has non-nilNode LRU pointers", currID))
			}
			if !isZeroValue(&c.nodes[currID].value) {
				panic(fmt.Sprintf("arenaRadix invariant violation: routing node %d with prefix '%s' retains non-zero value", currID, c.nodes[currID].prefix))
			}
		}

		// Validate child pointers and sibling ordering
		prevSiblingID := nilNode
		for chID := c.nodes[currID].child; chID != nilNode; chID = c.nodes[chID].sibling {
			if chID >= uint32(len(c.nodes)) {
				panic(fmt.Sprintf("arenaRadix invariant violation: child index %d out of bounds (len=%d)", chID, len(c.nodes)))
			}
			if c.nodes[chID].parent != currID {
				panic(fmt.Sprintf("arenaRadix invariant violation: child with prefix '%s' has incorrect parent pointer", c.nodes[chID].prefix))
			}
			if len(c.nodes[chID].prefix) == 0 {
				panic("arenaRadix invariant violation: non-root child node has empty prefix")
			}
			if prevSiblingID != nilNode && c.nodes[prevSiblingID].prefix[0] >= c.nodes[chID].prefix[0] {
				panic(fmt.Sprintf("arenaRadix invariant violation: siblings not sorted lexicographically ('%s' >= '%s')", c.nodes[prevSiblingID].prefix, c.nodes[chID].prefix))
			}
			prevSiblingID = chID
		}

		// Validate tree compactness: non-root routing nodes without a value must have >= 2 children.
		if currID != c.root && !c.nodes[currID].hasValue {
			if c.nodes[currID].child == nilNode || c.nodes[c.nodes[currID].child].sibling == nilNode {
				panic(fmt.Sprintf("arenaRadix invariant violation: intermediate routing node with prefix '%s' has fewer than 2 children", c.nodes[currID].prefix))
			}
		}

		// Advance to child if present
		if c.nodes[currID].child != nilNode {
			currID = c.nodes[currID].child
			continue
		}

		// Backtrack up parent chain until finding an ancestor with an unvisited sibling
		for currID != c.root && c.nodes[currID].sibling == nilNode {
			currID = c.nodes[currID].parent
		}
		if currID == c.root {
			break
		}
		currID = c.nodes[currID].sibling
	}

	if treeCount != c.len {
		panic(fmt.Sprintf("arenaRadix invariant violation: tree value count %d does not match LRU length %d", treeCount, c.len))
	}

	if treeSumSize != c.currentSize {
		panic(fmt.Sprintf("arenaRadix: currentSize drift in tree: currentSize=%d treeSumSize=%d", c.currentSize, treeSumSize))
	}

	// INVARIANT 6: Hash accelerator map (nodeMap) index bounds, value presence, and hash consistency.
	for h, id := range c.nodeMap {
		if id >= uint32(len(c.nodes)) {
			panic(fmt.Sprintf("arenaRadix invariant violation: nodeMap contains out-of-bounds index %d (len=%d)", id, len(c.nodes)))
		}
		if !c.nodes[id].hasValue {
			panic(fmt.Sprintf("arenaRadix invariant violation: nodeMap points to node %d with hasValue=false", id))
		}
		if c.hashNodeKey(id) != h {
			panic(fmt.Sprintf("arenaRadix invariant violation: nodeMap hash mismatch for node %d", id))
		}
	}

	// INVARIANT 7: Free-list integrity and total node accounting (liveTreeNodes + freeListCount == len(nodes)).
	freeCount := 0
	for freeID := c.freeHead; freeID != nilNode; freeID = c.nodes[freeID].next {
		if freeID >= uint32(len(c.nodes)) {
			panic(fmt.Sprintf("arenaRadix invariant violation: free-list contains out-of-bounds index %d (len=%d)", freeID, len(c.nodes)))
		}
		if freeID == c.root {
			panic("arenaRadix invariant violation: free-list contains root node")
		}
		if c.nodes[freeID].hasValue {
			panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d has hasValue=true", freeID))
		}
		if !isZeroValue(&c.nodes[freeID].value) {
			panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d retains non-zero value", freeID))
		}
		if c.nodes[freeID].size != 0 {
			panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d has non-zero size %d", freeID, c.nodes[freeID].size))
		}
		if c.nodes[freeID].prefix != "" {
			panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d has non-empty prefix '%s'", freeID, c.nodes[freeID].prefix))
		}
		if c.nodes[freeID].parent != nilNode || c.nodes[freeID].child != nilNode || c.nodes[freeID].sibling != nilNode || c.nodes[freeID].prev != nilNode {
			panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d has non-nilNode tree/LRU pointers", freeID))
		}
		freeCount++
		if freeCount > len(c.nodes) {
			panic("arenaRadix invariant violation: cycle detected in free-list")
		}
	}

	if uint32(freeCount) != c.freeCount {
		panic(fmt.Sprintf("arenaRadix invariant violation: free-list walk count (%d) != tracked freeCount (%d)", freeCount, c.freeCount))
	}

	if treeNodeCount+freeCount != len(c.nodes) {
		panic(fmt.Sprintf("arenaRadix invariant violation: live tree nodes (%d) + free list nodes (%d) != len(nodes) (%d)", treeNodeCount, freeCount, len(c.nodes)))
	}
}

func (c *arenaRadix[V]) unlock() {
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
	c.mu.Unlock()
}

func (c *arenaRadix[V]) rUnlock() {
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
	c.mu.RUnlock()
}

// Compact performs lossless O(N) compaction of the arena node slice and hash lookup map
// under exclusive write lock while preserving 100% of live entries, sizes, and exact LRU order.
func (c *arenaRadix[V]) Compact() {
	c.mu.Lock()
	defer c.unlock()
	c.compactLocked()
}

// EvaluateMemoryPressure samples the configured memory-pressure probe lock-free,
// then acquires the exclusive write lock to execute Tier 2 (LRU tail shedding + compaction)
// or Tier 1 (lossless compaction) if pressure meets or exceeds the configured thresholds.
// Returns any values evicted during Tier 2 critical-pressure shedding.
func (c *arenaRadix[V]) EvaluateMemoryPressure() []V {
	_, pressure := c.lockWithPressure(&c.mu, true)
	defer c.unlock()

	return c.maybeReclaimUnderPressureLocked(pressure, nilNode)
}

func (c *arenaRadix[V]) finishDeleteReclaimLocked(sizeBefore, sampledEpoch uint64, pressure float64) {
	if c.len == 0 {
		hadSlack := c.hasEmptyDeleteSlack(c.freeCount >= 64)
		c.clearEmptyArenaStateLocked()
		if (hadSlack || sizeBefore > 0) && c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
			c.markReclaimedLocked()
		}
		return
	}
	reclaimedSingleSurvivor := false
	if c.shouldReclaimSingleSurvivorOnDelete(c.len, c.freeCount > 0 || c.nodeMapDirty, c.freeCount >= 63) {
		c.compactDataStructuresLocked()
		reclaimedSingleSurvivor = true
	}
	c.maybeReclaimUnderPressureLocked(pressure, foregroundNoProtect)
	if (reclaimedSingleSurvivor || c.currentSize < sizeBefore) && c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
		c.markReclaimedLocked()
	}
}

func (c *arenaRadix[V]) finishMutationReclaimLocked(evictedValues []V, protectedID uint32, reclaimedPre bool, sizeBefore, sampledEpoch uint64, pressure float64) []V {
	evictedByPressure := c.maybeReclaimUnderPressureLocked(pressure, protectedID)
	if len(evictedValues) == 0 {
		evictedValues = evictedByPressure
	} else if len(evictedByPressure) > 0 {
		evictedValues = append(evictedValues, evictedByPressure...)
	}
	netByteReduced := c.currentSize < sizeBefore
	if c.shouldCompactAfterMutation(reclaimedPre, netByteReduced, c.isDirtyLocked(), pressure) {
		c.compactDataStructuresLocked()
	}
	if c.shouldMarkReclaimedAfterMutation(reclaimedPre, netByteReduced, sampledEpoch, pressure) {
		c.markReclaimedLocked()
	}
	return evictedValues
}

// Put inserts or updates the given key and value in the cache.
// If the key already exists, its value is replaced and promoted to MRU position.
// If the cache exceeds capacity after insertion, LRU entries are evicted and returned.
//
// Returns ErrInvalidEntrySize if the entry's weight exceeds maxSize.
func (c *arenaRadix[V]) Put(key string, value V) ([]V, error) {
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
	keyHash := hashString(key)

	nodeID, exists := c.getNodeKeyWithHash(key, keyHash)
	if exists {
		// Updating an existing key requires 0 new node allocations and 0 trie walks.
		oldValue := c.nodes[nodeID].value
		c.onEntrySizeUpdated(c.nodes[nodeID].size, valueSize)
		c.moveToFront(nodeID)
		c.currentSize -= c.nodes[nodeID].size
		for valueSize > c.maxSize-c.currentSize && c.tail != nilNode && c.tail != nodeID {
			if evicted, ok := c.evictOne(); ok {
				evictedValues = append(evictedValues, evicted)
				evictedPrePut = true
			}
		}
		c.nodes[nodeID].value = value
		c.nodes[nodeID].hasValue = true
		c.nodes[nodeID].size = valueSize
		c.currentSize += valueSize
		if evictedPrePut {
			c.nodeMap[keyHash] = nodeID
		}
		c.notifyEvict(key, oldValue, EvictionReasonReplaced)
		reclaimedPrePut = c.shouldReclaimSingleSurvivorOnMutation(c.len, c.freeCount > 0 || c.nodeMapDirty, c.freeCount >= 63, evictedPrePut, c.currentSize, sizeBefore, pressure)
	} else {
		// A single new-key insert can allocate up to 2 nodes (one routing node, one leaf).
		// If the slice has reached its physical uint32 maximum, ensure enough free slots exist.
		for uint64(len(c.nodes))-uint64(c.freeCount)+2 > uint64(foregroundNoProtect) && c.tail != nilNode {
			if evicted, ok := c.evictOne(); ok {
				evictedValues = append(evictedValues, evicted)
				evictedPrePut = true
			}
		}

		// Evict from the LRU tail before allocating new arena nodes when valueSize would exceed remaining capacity
		// (using subtraction to avoid uint64 addition overflow when maxSize is near math.MaxUint64).
		for valueSize > c.maxSize-c.currentSize && c.tail != nilNode {
			if evicted, ok := c.evictOne(); ok {
				evictedValues = append(evictedValues, evicted)
				evictedPrePut = true
			}
		}
		if c.shouldReclaimEmptyPrePut(c.len, c.freeCount >= 64, evictedPrePut, valueSize, sizeBefore, pressure) {
			c.clearEmptyArenaStateLocked()
			reclaimedPrePut = true
		} else if c.shouldReclaimSingleSurvivorOnMutation(c.len, c.freeCount > 0 || c.nodeMapDirty, c.freeCount >= 63, evictedPrePut, c.currentSize+valueSize, sizeBefore, pressure) {
			reclaimedPrePut = true
		}

		nodeID = c.insertNode(key, value)
		c.nodes[nodeID].size = valueSize
		c.pushFront(nodeID)
		c.onEntryPut(c.len, valueSize)
		c.currentSize += valueSize
		c.nodeMap[keyHash] = nodeID
	}

	evictedValues = c.finishMutationReclaimLocked(evictedValues, nodeID, reclaimedPrePut, sizeBefore, sampledEpoch, pressure)
	return evictedValues, nil
}

// Delete removes the entry associated with key from the cache, returning its value and true (or the zero value of V and false if not found).
func (c *arenaRadix[V]) Delete(key string) (value V, ok bool) {
	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	keyHash := hashString(key)
	nodeID, found := c.lookupNodeKeyWithHash(key, keyHash)
	if !found {
		return value, false
	}

	sizeBefore := c.currentSize
	deleted, erased := c.eraseInternalWithHash(nodeID, keyHash, key, EvictionReasonDeleted)
	if !erased {
		return value, false
	}
	c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
	return deleted, true
}

// Get retrieves the value associated with key and promotes it to MRU position.
// Returns the zero value of V and false if the key is not found in the cache.
func (c *arenaRadix[V]) Get(key string) (value V, ok bool) {
	c.mu.Lock()
	defer c.unlock()

	keyHash := hashString(key)
	nodeID, found := c.getNodeKeyWithHash(key, keyHash)
	if !found {
		return value, false
	}
	c.moveToFront(nodeID)

	return c.nodes[nodeID].value, true
}

// Peek retrieves the value associated with key without altering its LRU position.
// Returns the zero value of V and false if the key is not found in the cache.
func (c *arenaRadix[V]) Peek(key string) (value V, ok bool) {
	c.mu.RLock()
	defer c.rUnlock()

	nodeID, found := c.getNodeKey(key)
	if !found {
		return value, false
	}

	return c.nodes[nodeID].value, true
}

// Replace updates the value of an existing key and recomputes its weight
// without modifying its LRU position.
// If the entry's updated weight exceeds maxSize (or cannot fit alongside entries more recent than node),
// only the entry itself is evicted without evicting older entries.
// Otherwise, if the updated cache size exceeds maxSize, excess LRU entries are evicted to maintain capacity invariants.
//
// Returns ErrEntryNotExist if key is not present in the cache.
func (c *arenaRadix[V]) Replace(key string, value V) error {
	newSize := c.weigh(key, value)

	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	keyHash := hashString(key)
	nodeID, ok := c.lookupNodeKeyWithHash(key, keyHash)
	if !ok {
		return ErrEntryNotExist
	}

	oldSize := c.nodes[nodeID].size
	oldValue := c.nodes[nodeID].value

	if newSize > c.maxSize {
		sizeBefore := c.currentSize
		c.eraseInternalWithHash(nodeID, keyHash, key, EvictionReasonCapacity)
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
			headCurr := c.head
			tailCurr := c.tail
			canFit := false
			for {
				if headCurr == nodeID {
					canFit = newerSize <= maxNewer
					break
				}
				if headCurr != nilNode {
					newerSize += c.nodes[headCurr].size
					if newerSize > maxNewer {
						canFit = false
						break
					}
					headCurr = c.nodes[headCurr].next
				}
				if tailCurr == nilNode || tailCurr == nodeID {
					canFit = sizeDelta <= avail
					break
				}
				avail += c.nodes[tailCurr].size
				if sizeDelta <= avail {
					canFit = true
					break
				}
				tailCurr = c.nodes[tailCurr].prev
			}
			if !canFit {
				c.eraseInternalWithHash(nodeID, keyHash, key, EvictionReasonCapacity)
				c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
				return nil
			}
		}

		for sizeDelta > c.maxSize-c.currentSize && c.tail != nilNode {
			if c.tail == nodeID {
				break
			}
			c.evictOne()
			evictedAny = true
		}

		c.onEntrySizeUpdated(oldSize, newSize)
		c.nodes[nodeID].value = value
		c.nodes[nodeID].hasValue = true
		c.nodes[nodeID].size = newSize
		c.currentSize += sizeDelta
	case newSize < oldSize:
		sizeDiff := oldSize - newSize
		c.onEntrySizeUpdated(oldSize, newSize)
		c.nodes[nodeID].value = value
		c.nodes[nodeID].hasValue = true
		c.nodes[nodeID].size = newSize
		c.currentSize -= sizeDiff
	default:
		c.nodes[nodeID].value = value
		c.nodes[nodeID].hasValue = true
	}

	prevMappedID, hadPrevMapped := c.nodeMap[keyHash]
	if !hadPrevMapped || prevMappedID != nodeID {
		c.nodeMap[keyHash] = nodeID
	}
	c.notifyEvict(key, oldValue, EvictionReasonReplaced)
	reclaimedPreUpdate = c.shouldReclaimSingleSurvivorOnMutation(c.len, c.freeCount > 0 || c.nodeMapDirty, c.freeCount >= 63, evictedAny, c.currentSize, sizeBefore, pressure)

	protectedID := foregroundNoProtect
	if nodeID == c.head && c.nodes[nodeID].hasValue {
		protectedID = nodeID
	}
	c.finishMutationReclaimLocked(nil, protectedID, reclaimedPreUpdate, sizeBefore, sampledEpoch, pressure)
	if c.nodeMapDirty && (nodeID >= uint32(len(c.nodes)) || !c.nodes[nodeID].hasValue) &&
		hadPrevMapped && prevMappedID != nodeID && prevMappedID < uint32(len(c.nodes)) && c.nodes[prevMappedID].hasValue {
		c.nodeMap[keyHash] = prevMappedID
	}
	return nil
}

// DeletePrefix deletes all entries whose keys begin with prefix.
// It severs the matching subtree in O(1) and iteratively reclaims all nodes into the free-list.
func (c *arenaRadix[V]) DeletePrefix(prefix string) {
	if prefix == "" {
		c.mu.Lock()
		defer c.unlock()

		hadEntries := c.len > 0
		hadDirtySlack := c.freeCount > 0 || c.nodeMapDirty || c.peakEntryLen > 8 || c.deletedSinceCompact > 0 || (c.len == 0 && cap(c.nodes) > 1)
		if !hadEntries && !hadDirtySlack && c.peakEntryLen == 0 && len(c.nodes) <= 1 {
			return
		}
		hadReclaimable := c.currentSize > 0 || hadDirtySlack
		if (c.onEvictValue != nil || c.onEvictEntry != nil) && c.len > 0 {
			var zero V
			for currID := c.head; currID != nilNode; {
				nextID := c.nodes[currID].next
				var key string
				if c.onEvictEntry != nil {
					key = c.reconstructKey(currID)
				}
				evictedVal := c.nodes[currID].value
				c.currentSize -= c.nodes[currID].size
				c.remove(currID)
				c.nodes[currID].value = zero
				c.nodes[currID].hasValue = false
				c.nodes[currID].size = 0
				c.notifyEvict(key, evictedVal, EvictionReasonDeleted)
				currID = nextID
			}
		}
		c.clearEmptyArenaStateLocked()
		if hadReclaimable && c.hasElevatedPressureToInvalidate(0.0) {
			c.markReclaimedLocked()
		}
		return
	}

	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	nodeID := c.root
	search := prefix

	for len(search) > 0 {
		childID := c.getChild(nodeID, search[0])
		if childID == nilNode {
			return // Prefix doesn't exist
		}

		lcp := longestCommonPrefix(search, c.nodes[childID].prefix)

		if lcp == len(search) {
			sizeBefore := c.currentSize
			// We found the exact node where the prefix ends.
			// Sever it entirely from the tree structure
			c.removeChild(nodeID, childID)

			// Temporarily restore upward parent pointer for non-recursive traversal
			c.nodes[childID].parent = nodeID

			// Now sweep the detached subtree to fix LRU, nodeMap, and currentSize
			c.freeSubtree(childID)
			c.compressPathUpwards(nodeID)
			c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
			return
		}

		if lcp == len(c.nodes[childID].prefix) {
			search = search[len(c.nodes[childID].prefix):]
			nodeID = childID
			continue
		}

		return
	}
}

// freeSubtree iteratively reclaims all nodes in a detached subtree using O(1) stack space
// and incremental FNV-1a prefix hashing (avoiding O(leaves * depth) ancestor walks),
// removing active values from the LRU list, nodeMap, and accounting, and returning nodes to the free-list.
func (c *arenaRadix[V]) freeSubtree(nodeID uint32) {
	if nodeID == nilNode {
		return
	}
	var zero V
	baseHash := c.hashNodeKey(c.nodes[nodeID].parent)
	var hashStackBuf [64]uint64
	hashStack := hashStackBuf[:0]
	currHash := hashStringCont(baseHash, c.nodes[nodeID].prefix)

	currID := nodeID
	for currID != nilNode {
		if c.nodes[currID].hasValue {
			var key string
			if c.onEvictEntry != nil {
				key = c.reconstructKey(currID)
			}
			evictedVal := c.nodes[currID].value
			c.onEntryDeleted(c.nodes[currID].size)
			c.currentSize -= c.nodes[currID].size
			c.remove(currID)
			c.nodeMapDirty = true
			if mappedID, ok := c.nodeMap[currHash]; ok && mappedID == currID {
				delete(c.nodeMap, currHash)
			}
			c.nodes[currID].value = zero
			c.nodes[currID].hasValue = false
			c.nodes[currID].size = 0
			c.notifyEvict(key, evictedVal, EvictionReasonDeleted)
		}

		if c.nodes[currID].child != nilNode {
			hashStack = append(hashStack, currHash)
			currID = c.nodes[currID].child
			currHash = hashStringCont(currHash, c.nodes[currID].prefix)
			continue
		}

		for currID != nodeID && c.nodes[currID].sibling == nilNode {
			parentID := c.nodes[currID].parent
			c.freeNode(currID)
			currID = parentID
			hashStack = hashStack[:len(hashStack)-1]
		}

		if currID == nodeID {
			c.freeNode(currID)
			return
		}

		siblingID := c.nodes[currID].sibling
		c.freeNode(currID)
		currID = siblingID
		parentHash := hashStack[len(hashStack)-1]
		currHash = hashStringCont(parentHash, c.nodes[currID].prefix)
	}
}

// All returns an iterator over all key-value pairs in MRU-to-LRU order without modifying recency.
func (c *arenaRadix[V]) All() iter.Seq2[string, V] {
	return c.all
}

func (c *arenaRadix[V]) all(yield func(string, V) bool) {
	c.mu.RLock()
	defer c.rUnlock()

	for currID := c.head; currID != nilNode; currID = c.nodes[currID].next {
		if !yield(c.reconstructKey(currID), c.nodes[currID].value) {
			return
		}
	}
}

// Keys returns an iterator over all keys in MRU-to-LRU order without modifying recency.
func (c *arenaRadix[V]) Keys() iter.Seq[string] {
	return c.keys
}

func (c *arenaRadix[V]) keys(yield func(string) bool) {
	c.mu.RLock()
	defer c.rUnlock()

	for currID := c.head; currID != nilNode; currID = c.nodes[currID].next {
		if !yield(c.reconstructKey(currID)) {
			return
		}
	}
}

// Values returns an iterator over all values in MRU-to-LRU order without modifying recency.
// It skips key reconstruction completely, executing with zero heap allocations.
func (c *arenaRadix[V]) Values() iter.Seq[V] {
	return c.values
}

func (c *arenaRadix[V]) values(yield func(V) bool) {
	c.mu.RLock()
	defer c.rUnlock()

	for currID := c.head; currID != nilNode; currID = c.nodes[currID].next {
		if !yield(c.nodes[currID].value) {
			return
		}
	}
}
