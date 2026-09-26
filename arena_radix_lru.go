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
	"math"
)

// NewArenaRadixCache returns a new arena-backed radix LRU Cache bounded by maxSize.
// Optional configuration parameters can be passed via opts (e.g. WithInvariantChecking).
//
// maxSize must be greater than zero; otherwise NewArenaRadixCache panics.
func NewArenaRadixCache(maxSize uint64, opts ...Option) Cache {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	return newArenaRadixCacheWithOptions(maxSize, ApplyOptions(opts...))
}

func newArenaRadixCacheWithOptions(maxSize uint64, options Options) Cache {
	c := &arenaRadix{
		maxSize:  maxSize,
		nodes:    make([]arenaRadixNode, 0, 16),
		freeHead: nilNode,
		head:     nilNode,
		tail:     nilNode,
		nodeMap:  make(map[uint64]uint32, 16),
		options:  options,
	}

	c.root = c.allocateNode()

	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}

	return c
}

// checkInvariants performs full-tree iterative invariant validation:
// 1. maxSize > 0 and currentSize <= maxSize
// 2. Bidirectional LRU list pointer integrity and size sum parity
// 3. Root structure validity
// 4. LCRS tree hierarchy (parent-child links, prefix non-emptiness, sorted siblings, compactness)
// 5. Value-bearing node reachability and tree-to-LRU bijection
// Uses non-recursive O(1)-space pre-order tree traversal to prevent stack overflows.
func (c *arenaRadix) checkInvariants() {
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
		if c.nodes[currID].value == nil {
			panic(fmt.Sprintf("arenaRadix invariant violation: unexpected nil value in LRU list for prefix '%s'", c.nodes[currID].prefix))
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
		if c.nodes[currID].value != nil {
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
		if currID != c.root && c.nodes[currID].value == nil {
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

	// INVARIANT 6: Hash accelerator map (nodeMap) index bounds, non-nil value, hash consistency, and expectedNodeMapLen parity.
	if c.expectedNodeMapLen != len(c.nodeMap) {
		panic(fmt.Sprintf("arenaRadix invariant violation: expectedNodeMapLen %d does not match len(nodeMap) %d", c.expectedNodeMapLen, len(c.nodeMap)))
	}
	for h, id := range c.nodeMap {
		if id >= uint32(len(c.nodes)) {
			panic(fmt.Sprintf("arenaRadix invariant violation: nodeMap contains out-of-bounds index %d (len=%d)", id, len(c.nodes)))
		}
		if c.nodes[id].value == nil {
			panic(fmt.Sprintf("arenaRadix invariant violation: nodeMap points to node %d with nil value", id))
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
		if c.nodes[freeID].value != nil {
			panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d has non-nil value", freeID))
		}
		if c.nodes[freeID].size != 0 {
			panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d has non-zero size %d", freeID, c.nodes[freeID].size))
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

// Compact performs lossless O(N) compaction of the arena node slice and hash lookup map
// under exclusive write lock while preserving 100% of live entries, sizes, and exact LRU order.
func (c *arenaRadix) Compact() {
	c.mu.Lock()
	defer func() {
		if c.options.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()
	c.compactLocked()
}

// EvaluateMemoryPressure samples the configured memory-pressure probe lock-free,
// then acquires the exclusive write lock to execute Tier 2 (LRU tail shedding + compaction)
// or Tier 1 (lossless compaction) if pressure meets or exceeds the configured thresholds.
// Returns any values evicted during Tier 2 critical-pressure shedding.
func (c *arenaRadix) EvaluateMemoryPressure() []ValueType {
	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressureFresh()

	c.mu.Lock()
	for retries := 0; sampledEpoch != c.reclaimEpoch.Load() && retries < 2; retries++ {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressureFresh()
		c.mu.Lock()
	}
	defer func() {
		if c.options.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	return c.maybeReclaimUnderPressureLocked(pressure, nilNode)
}

// Insert inserts or updates the given key and value in the cache.
// If the key already exists, its value is replaced and promoted to MRU position.
// If the cache exceeds capacity after insertion, LRU entries are evicted and returned.
//
// Returns ErrInvalidEntry if value is nil.
// Returns ErrInvalidEntrySize if value.Size() exceeds maxSize.
func (c *arenaRadix) Insert(key string, value ValueType) ([]ValueType, error) {
	if value == nil {
		return nil, ErrInvalidEntry
	}

	valueSize := value.Size()
	if valueSize > c.maxSize {
		return nil, ErrInvalidEntrySize
	}

	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressure()

	c.mu.Lock()
	for retries := 0; sampledEpoch != c.reclaimEpoch.Load() && retries < 2; retries++ {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressure()
		c.mu.Lock()
	}
	defer func() {
		if c.options.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	var evictedValues []ValueType
	keyHash := hashString(key)

	nodeID, exists := c.getNodeKeyWithHash(key, keyHash)
	if exists {
		// Updating an existing key requires 0 new node allocations and 0 trie walks.
		if c.nodes[nodeID].size == 0 && valueSize > 0 && c.zeroSizeCount > 0 {
			c.zeroSizeCount--
			if c.zeroSizeCount < c.lastReclaimedZeroCount {
				c.lastReclaimedZeroCount = c.zeroSizeCount
			}
		} else if c.nodes[nodeID].size > 0 && valueSize == 0 {
			c.zeroSizeCount++
		}
		c.moveToFront(nodeID)
		c.currentSize -= c.nodes[nodeID].size
		for valueSize > c.maxSize-c.currentSize && c.tail != nilNode && c.tail != nodeID {
			evictedValues = append(evictedValues, c.evictOne())
		}
		c.nodes[nodeID].value = value
		c.nodes[nodeID].size = valueSize
		c.currentSize += valueSize
		if _, occupied := c.nodeMap[keyHash]; !occupied {
			c.expectedNodeMapLen++
		}
		c.nodeMap[keyHash] = nodeID
	} else {
		// A single new-key insert can allocate up to 2 nodes (one routing node, one leaf).
		// If the slice has reached its physical uint32 maximum, ensure at least 2 free slots exist.
		for uint32(len(c.nodes)) >= foregroundNoProtect-2 && c.freeCount < 2 && c.tail != nilNode {
			prevFree := c.freeCount
			evictedValues = append(evictedValues, c.evictOne())
			if c.freeCount == prevFree && c.tail == nilNode {
				break
			}
		}

		// Evict from the LRU tail before allocating new arena nodes when valueSize would exceed remaining capacity
		// (using subtraction to avoid uint64 addition overflow when maxSize is near math.MaxUint64).
		for valueSize > c.maxSize-c.currentSize && c.tail != nilNode {
			evictedValues = append(evictedValues, c.evictOne())
		}
		if c.len == 0 && c.freeCount >= 64 {
			c.nodes = make([]arenaRadixNode, 0, 16)
			c.freeHead = nilNode
			c.freeCount = 0
			c.root = c.allocateNode()
			c.head = nilNode
			c.tail = nilNode
			c.currentSize = 0
			c.zeroSizeCount = 0
			c.lastReclaimedZeroCount = 0
			c.nodeMap = make(map[uint64]uint32, 16)
			c.expectedNodeMapLen = 0
			c.nodeMapDirty = false
		}

		nodeID, _ = c.insertNode(key, value)
		c.nodes[nodeID].size = valueSize
		if valueSize == 0 {
			c.zeroSizeCount++
		}
		c.pushFront(nodeID)
		c.currentSize += valueSize
		if _, occupied := c.nodeMap[keyHash]; !occupied {
			c.expectedNodeMapLen++
		}
		c.nodeMap[keyHash] = nodeID
	}

	// Evict until we're at or below maxSize
	for c.currentSize > c.maxSize && c.tail != nilNode {
		evictedValues = append(evictedValues, c.evictOne())
	}

	if evictedByPressure := c.maybeReclaimUnderPressureLocked(pressure, nodeID); len(evictedByPressure) > 0 {
		evictedValues = append(evictedValues, evictedByPressure...)
	}

	return evictedValues, nil
}

// Erase removes the entry associated with key from the cache, returning its value (or nil if not found).
func (c *arenaRadix) Erase(key string) (value ValueType) {
	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressure()

	c.mu.Lock()
	for retries := 0; sampledEpoch != c.reclaimEpoch.Load() && retries < 2; retries++ {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressure()
		c.mu.Lock()
	}
	defer func() {
		if c.options.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	nodeID, ok := c.getNodeKey(key)
	if !ok {
		c.maybeReclaimUnderPressureLocked(pressure, foregroundNoProtect)
		return nil
	}

	deleted := c.eraseInternal(nodeID)
	if c.len == 0 {
		c.resetEmptyArenaLocked()
		return deleted
	}
	c.maybeReclaimUnderPressureLocked(pressure, foregroundNoProtect)
	if c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
		c.markReclaimedLocked()
	}
	return deleted
}

// LookUp retrieves the value associated with key and promotes it to MRU position.
// Returns nil if the key is not found in the cache.
func (c *arenaRadix) LookUp(key string) (value ValueType) {
	c.mu.Lock()
	defer func() {
		if c.options.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	keyHash := hashString(key)
	nodeID, ok := c.getNodeKeyWithHash(key, keyHash)
	if !ok {
		return nil
	}
	if prevID, occupied := c.nodeMap[keyHash]; !occupied {
		c.nodeMap[keyHash] = nodeID
		c.expectedNodeMapLen++
	} else if prevID != nodeID {
		c.nodeMap[keyHash] = nodeID
	}
	c.moveToFront(nodeID)

	return c.nodes[nodeID].value
}

// LookUpWithoutChangingOrder retrieves the value associated with key without altering its LRU position.
// Returns nil if the key is not found in the cache.
func (c *arenaRadix) LookUpWithoutChangingOrder(key string) (value ValueType) {
	c.mu.RLock()
	defer func() {
		if c.options.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.RUnlock()
	}()

	nodeID, ok := c.getNodeKey(key)
	if !ok {
		return nil
	}

	return c.nodes[nodeID].value
}

// UpdateWithoutChangingOrder updates the value of an existing key without modifying its LRU position.
//
// Returns ErrInvalidEntry if value is nil.
// Returns ErrEntryNotExist if key is not present in the cache.
// Returns ErrInvalidUpdateEntrySize if value.Size() does not match the existing entry's size.
func (c *arenaRadix) UpdateWithoutChangingOrder(key string, value ValueType) error {
	if value == nil {
		return ErrInvalidEntry
	}

	valueSize := value.Size()

	c.mu.Lock()
	defer func() {
		if c.options.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	nodeID, ok := c.getNodeKey(key)
	if !ok {
		return ErrEntryNotExist
	}

	if valueSize != c.nodes[nodeID].size {
		return ErrInvalidUpdateEntrySize
	}

	c.nodes[nodeID].value = value
	return nil
}

// UpdateSize adjusts the size accounting for an existing key by sizeDelta without altering its LRU position.
// If node.size + sizeDelta exceeds maxSize (or cannot fit alongside entries more recent than node),
// only the entry itself is evicted without evicting older entries.
// Otherwise, if the updated cache size exceeds maxSize, excess LRU entries are evicted to maintain capacity invariants.
//
// Returns ErrEntryNotExist if key is not present in the cache.
// Returns ErrInvalidUpdateEntrySize if sizeDelta causes uint64 integer overflow.
func (c *arenaRadix) UpdateSize(key string, sizeDelta uint64) error {
	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressure()

	c.mu.Lock()
	for retries := 0; sampledEpoch != c.reclaimEpoch.Load() && retries < 2; retries++ {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressure()
		c.mu.Lock()
	}
	defer func() {
		if c.options.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	nodeID, ok := c.getNodeKey(key)
	if !ok {
		c.maybeReclaimUnderPressureLocked(pressure, foregroundNoProtect)
		return ErrEntryNotExist
	}

	if math.MaxUint64-c.nodes[nodeID].size < sizeDelta {
		return ErrInvalidUpdateEntrySize
	}

	avail := c.maxSize - c.currentSize
	if c.nodes[nodeID].size+sizeDelta > c.maxSize {
		avail = 0
	} else {
		for currID := c.tail; currID != nilNode && currID != nodeID && sizeDelta > avail; currID = c.nodes[currID].prev {
			avail += c.nodes[currID].size
		}
	}
	if c.nodes[nodeID].size+sizeDelta > c.maxSize || sizeDelta > avail {
		c.eraseInternal(nodeID)
		if c.len == 0 {
			c.resetEmptyArenaLocked()
			return nil
		}
		c.maybeReclaimUnderPressureLocked(pressure, foregroundNoProtect)
		if c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
			c.markReclaimedLocked()
		}
		return nil
	}

	evictedAny := false
	for sizeDelta > c.maxSize-c.currentSize && c.tail != nilNode {
		if c.tail == nodeID {
			break
		}
		c.evictOne()
		evictedAny = true
	}

	if math.MaxUint64-c.currentSize < sizeDelta {
		return ErrInvalidUpdateEntrySize
	}

	if c.nodes[nodeID].size == 0 && sizeDelta > 0 && c.zeroSizeCount > 0 {
		c.zeroSizeCount--
		if c.zeroSizeCount < c.lastReclaimedZeroCount {
			c.lastReclaimedZeroCount = c.zeroSizeCount
		}
	}
	c.nodes[nodeID].size += sizeDelta
	c.currentSize += sizeDelta

	protectedID := foregroundNoProtect
	if sizeDelta > 0 && nodeID == c.head && c.nodes[nodeID].value != nil {
		protectedID = nodeID
	}
	c.maybeReclaimUnderPressureLocked(pressure, protectedID)
	if evictedAny && c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
		c.markReclaimedLocked()
	}
	return nil
}

// EraseEntriesWithGivenPrefix deletes all entries whose keys begin with prefix.
// It severs the matching subtree in O(1) and iteratively reclaims all nodes into the free-list.
func (c *arenaRadix) EraseEntriesWithGivenPrefix(prefix string) {
	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressure()

	c.mu.Lock()
	for retries := 0; sampledEpoch != c.reclaimEpoch.Load() && retries < 2; retries++ {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressure()
		c.mu.Lock()
	}
	defer func() {
		if c.options.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	if prefix == "" {
		c.resetEmptyArenaLocked()
		return
	}

	nodeID := c.root
	search := prefix

	for len(search) > 0 {
		childID := c.getChild(nodeID, search[0])
		if childID == nilNode {
			c.maybeReclaimUnderPressureLocked(pressure, foregroundNoProtect)
			return // Prefix doesn't exist
		}

		lcp := c.longestCommonPrefix(search, c.nodes[childID].prefix)

		if lcp == len(search) {
			// We found the exact node where the prefix ends.
			// Sever it entirely from the tree structure
			c.removeChild(nodeID, childID)

			// Temporarily restore upward parent pointer for non-recursive traversal
			c.nodes[childID].parent = nodeID

			// Now sweep the detached subtree to fix LRU, nodeMap, and currentSize
			c.freeSubtree(childID)
			c.compressPathUpwards(nodeID)
			if c.len == 0 {
				c.resetEmptyArenaLocked()
				return
			}
			c.maybeReclaimUnderPressureLocked(pressure, foregroundNoProtect)
			if c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
				c.markReclaimedLocked()
			}
			return
		}

		if lcp == len(c.nodes[childID].prefix) {
			search = search[len(c.nodes[childID].prefix):]
			nodeID = childID
			continue
		}

		c.maybeReclaimUnderPressureLocked(pressure, foregroundNoProtect)
		return
	}
}

// freeSubtree iteratively reclaims all nodes in a detached subtree using O(1) stack space
// and incremental FNV-1a prefix hashing (avoiding O(leaves * depth) ancestor walks),
// removing active values from the LRU list, nodeMap, and accounting, and returning nodes to the free-list.
func (c *arenaRadix) freeSubtree(nodeID uint32) {
	if nodeID == nilNode {
		return
	}
	baseHash := c.hashNodeKey(c.nodes[nodeID].parent)
	var hashStackBuf [64]uint64
	hashStack := hashStackBuf[:0]
	currHash := hashStringCont(baseHash, c.nodes[nodeID].prefix)

	currID := nodeID
	for currID != nilNode {
		if c.nodes[currID].value != nil {
			if c.nodes[currID].size == 0 && c.zeroSizeCount > 0 {
				c.zeroSizeCount--
				if c.zeroSizeCount < c.lastReclaimedZeroCount {
					c.lastReclaimedZeroCount = c.zeroSizeCount
				}
			}
			c.currentSize -= c.nodes[currID].size
			c.remove(currID)
			if mappedID, ok := c.nodeMap[currHash]; ok && mappedID == currID {
				delete(c.nodeMap, currHash)
				c.nodeMapDirty = true
				if c.expectedNodeMapLen > 0 {
					c.expectedNodeMapLen--
				}
			}
			c.nodes[currID].value = nil
			c.nodes[currID].size = 0
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
