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
	"hash/maphash"
	"iter"
	"math"
	"slices"
)

// NewArenaRadixCache returns a new arena-backed radix LRU Cache[V] bounded by maxSize.
// Optional configuration parameters can be passed via opts (e.g. WithWeigher, WithShards, WithInvariantChecking).
//
// maxSize must be greater than zero; otherwise NewArenaRadixCache panics.
func NewArenaRadixCache[V any](maxSize uint64, opts ...Option) Cache[V] {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	options := ApplyOptions(opts...)
	options.Backend = BackendArenaRadix
	if options.shouldShard() {
		return newShardedCacheWithOptions[V](maxSize, options.Shards, options)
	}
	return newArenaRadixCacheWithOptions[V](maxSize, options)
}

func newArenaRadixCacheWithOptions[V any](maxSize uint64, options Options) *arenaRadix[V] {
	if !options.hasShardTier2Target {
		options.hasShardTier2Target = true
		options.shardTier2Target = computeTargetSize(maxSize, options.EvictionRetentionRatio)
	}
	c := &arenaRadix[V]{
		maxSize:       maxSize,
		seed:          maphash.MakeSeed(),
		weigher:       resolveWeigher[V](options),
		onEvictValue:  resolveOnEvictValue[V](options),
		onEvictEntry:  resolveOnEvictEntry[V](options),
		pressureState: pressureState{options: options},
	}
	c.initCoordinator(c.onEvictValue != nil || c.onEvictEntry != nil)

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
func (c *arenaRadix[V]) checkLRUNodeLinkInvariants(currID, prevID uint32) {
	if !c.nodes[currID].hasValue() {
		panic(fmt.Sprintf("arenaRadix invariant violation: unexpected hasValue=false in LRU list for prefix '%s'", c.nodePrefix(currID)))
	}
	if c.nodes[currID].prev != prevID {
		panic(fmt.Sprintf("arenaRadix invariant violation: corrupt prev pointer in LRU list for prefix '%s'", c.nodePrefix(currID)))
	}
	if prevID == nilNode {
		if c.head != currID {
			panic("arenaRadix invariant violation: head mismatch in LRU list")
		}
	} else if c.nodes[prevID].next != currID {
		panic(fmt.Sprintf("arenaRadix invariant violation: corrupt next pointer in LRU list for prefix '%s'", c.nodePrefix(prevID)))
	}
	if c.nodes[currID].next == nilNode && c.tail != currID {
		panic("arenaRadix invariant violation: tail mismatch in LRU list")
	}
}

func (c *arenaRadix[V]) checkLRUListWalkInvariants() {
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
		c.checkLRUNodeLinkInvariants(currID, prevID)
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
}

func (c *arenaRadix[V]) checkLRUEndpointsInvariants() {
	if c.len == 0 {
		if c.head != nilNode || c.tail != nilNode {
			panic("arenaRadix invariant violation: head or tail is non-nilNode when len is 0")
		}
		return
	}
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

func (c *arenaRadix[V]) checkRootInvariants() {
	if c.root == nilNode || c.root >= uint32(len(c.nodes)) {
		panic("arenaRadix invariant violation: root node is nilNode or out of bounds")
	}
	if c.nodes[c.root].prefixLen != 0 || c.nodes[c.root].keyLen() != 0 {
		panic("arenaRadix invariant violation: root node must have empty prefix and zero keyLen")
	}
	if c.nodes[c.root].parent != nilNode {
		panic("arenaRadix invariant violation: root node must not have a parent")
	}
	if c.nodes[c.root].sibling != nilNode {
		panic("arenaRadix invariant violation: root node must not have siblings")
	}
}

func (c *arenaRadix[V]) checkTreeNodeInvariants(currID uint32) {
	if c.nodes[currID].hasValue() {
		// A node is verifiably in the LRU list iff it is the head (with nilNode prev) or its prev's next points back to it.
		prev := c.nodes[currID].prev
		inLRU := (c.head == currID && prev == nilNode) || (prev != nilNode && prev < uint32(len(c.nodes)) && c.nodes[prev].next == currID)
		if !inLRU {
			panic(fmt.Sprintf("arenaRadix invariant violation: node with prefix '%s' has value but is missing from LRU list", c.nodePrefix(currID)))
		}
		return
	}
	if c.nodes[currID].size != 0 {
		panic(fmt.Sprintf("arenaRadix invariant violation: routing node %d has non-zero size %d", currID, c.nodes[currID].size))
	}
	if c.nodes[currID].prev != nilNode || c.nodes[currID].next != nilNode || c.head == currID || c.tail == currID {
		panic(fmt.Sprintf("arenaRadix invariant violation: routing node %d has non-nilNode LRU pointers", currID))
	}
	if !isZeroValue(&c.nodes[currID].value) {
		panic(fmt.Sprintf("arenaRadix invariant violation: routing node %d with prefix '%s' retains non-zero value", currID, c.nodePrefix(currID)))
	}
}

func (c *arenaRadix[V]) checkSingleChildInvariants(currID, chID, prevSiblingID uint32, parentKeyLen int) {
	if chID >= uint32(len(c.nodes)) {
		panic(fmt.Sprintf("arenaRadix invariant violation: child index %d out of bounds (len=%d)", chID, len(c.nodes)))
	}
	chNode := &c.nodes[chID]
	if chNode.parent != currID {
		panic(fmt.Sprintf("arenaRadix invariant violation: child with prefix '%s' has incorrect parent pointer", c.nodePrefix(chID)))
	}
	chPrefixLen := c.nodePrefixLen(chID)
	expectedKeyLen := min(parentKeyLen+chPrefixLen, int(nodeKeyLenMask))
	if chPrefixLen == 0 || chNode.keyLen() != expectedKeyLen {
		panic("arenaRadix invariant violation: non-root child node has empty prefix or invalid keyLen")
	}
	if prevSiblingID != nilNode && c.nodes[prevSiblingID].firstByte >= chNode.firstByte {
		panic(fmt.Sprintf("arenaRadix invariant violation: siblings not sorted lexicographically ('%s' >= '%s')", c.nodePrefix(prevSiblingID), c.nodePrefix(chID)))
	}
}

func (c *arenaRadix[V]) checkChildrenAndCompactness(currID uint32) {
	// Validate child pointers and sibling ordering
	prevSiblingID := nilNode
	parentKeyLen := c.nodes[currID].keyLen()
	for chID := c.nodes[currID].child; chID != nilNode; chID = c.nodes[chID].sibling {
		c.checkSingleChildInvariants(currID, chID, prevSiblingID, parentKeyLen)
		prevSiblingID = chID
	}

	// Validate tree compactness: non-root routing nodes without a value must have >= 2 children.
	if currID != c.root && !c.nodes[currID].hasValue() &&
		(c.nodes[currID].child == nilNode || c.nodes[c.nodes[currID].child].sibling == nilNode) {
		panic(fmt.Sprintf("arenaRadix invariant violation: intermediate routing node with prefix '%s' has fewer than 2 children", c.nodePrefix(currID)))
	}
}

func (c *arenaRadix[V]) checkTreeInvariants() int {
	// Iterative pre-order traversal using parent/sibling pointers (O(1) space).
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
		if c.nodes[currID].hasValue() {
			treeCount++
			if math.MaxUint64-treeSumSize < c.nodes[currID].size {
				panic("arenaRadix invariant violation: treeSumSize uint64 overflow")
			}
			treeSumSize += c.nodes[currID].size
		}
		c.checkTreeNodeInvariants(currID)
		c.checkChildrenAndCompactness(currID)

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

	return treeNodeCount
}

func (c *arenaRadix[V]) checkCollisionPeerBucketInvariants(h uint64, peers []uint32) int {
	if len(peers) == 0 {
		panic(fmt.Sprintf("arenaRadix invariant violation: collisionPeers[%d] is empty", h))
	}
	mappedID, ok := c.nodeMapGet(h)
	if !ok {
		panic(fmt.Sprintf("arenaRadix invariant violation: collisionPeers[%d] exists without nodeMap[%d]", h, h))
	}
	for i, peerID := range peers {
		if peerID >= uint32(len(c.nodes)) || !c.nodes[peerID].hasValue() || c.hashNodeKey(peerID) != h {
			panic(fmt.Sprintf("arenaRadix invariant violation: invalid collision peer %d for hash %d", peerID, h))
		}
		if mappedID == peerID {
			panic(fmt.Sprintf("arenaRadix invariant violation: collision peer %d duplicates nodeMap[%d]", peerID, h))
		}
		if slices.Contains(peers[:i], peerID) {
			panic(fmt.Sprintf("arenaRadix invariant violation: duplicate collision peer %d for hash %d", peerID, h))
		}
	}
	return len(peers)
}

func (c *arenaRadix[V]) checkNodeMapInvariants() {
	actualCount := 0
	for _, slot := range c.hashSlots {
		if slot.tag == 0 {
			continue
		}
		actualCount++
		id := slot.nodeID
		if id >= uint32(len(c.nodes)) {
			panic(fmt.Sprintf("arenaRadix invariant violation: nodeMap contains out-of-bounds index %d (len=%d)", id, len(c.nodes)))
		}
		if !c.nodes[id].hasValue() {
			panic(fmt.Sprintf("arenaRadix invariant violation: nodeMap points to node %d with hasValue=false", id))
		}
		if arenaHashTag(c.hashNodeKey(id)) != slot.tag {
			panic(fmt.Sprintf("arenaRadix invariant violation: nodeMap hash mismatch for node %d", id))
		}
	}
	if actualCount != c.hashCount {
		panic(fmt.Sprintf("arenaRadix invariant violation: hashCount %d != actual occupied slots %d", c.hashCount, actualCount))
	}
	totalPeers := 0
	for h, peers := range c.collisionPeers {
		totalPeers += c.checkCollisionPeerBucketInvariants(h, peers)
	}
	if c.collisionCount != totalPeers {
		panic(fmt.Sprintf("arenaRadix invariant violation: collisionCount %d != totalPeers %d", c.collisionCount, totalPeers))
	}
	if c.hashCount+c.collisionCount > c.len {
		panic(fmt.Sprintf("arenaRadix invariant violation: len(nodeMap) (%d) + collisionCount (%d) > len (%d)", c.hashCount, c.collisionCount, c.len))
	}
}

func (c *arenaRadix[V]) checkFreeNodeInvariants(freeID uint32) {
	if freeID >= uint32(len(c.nodes)) {
		panic(fmt.Sprintf("arenaRadix invariant violation: free-list contains out-of-bounds index %d (len=%d)", freeID, len(c.nodes)))
	}
	if freeID == c.root {
		panic("arenaRadix invariant violation: free-list contains root node")
	}
	if c.nodes[freeID].hasValue() {
		panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d has hasValue=true", freeID))
	}
	if !isZeroValue(&c.nodes[freeID].value) {
		panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d retains non-zero value", freeID))
	}
	if c.nodes[freeID].size != 0 {
		panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d has non-zero size %d", freeID, c.nodes[freeID].size))
	}
	if c.nodes[freeID].prefixLen != 0 || c.nodes[freeID].keyLen() != 0 {
		panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d has non-empty prefix '%s' or keyLen %d", freeID, c.nodePrefix(freeID), c.nodes[freeID].keyLen()))
	}
	if c.nodes[freeID].parent != nilNode || c.nodes[freeID].child != nilNode || c.nodes[freeID].sibling != nilNode || c.nodes[freeID].prev != nilNode {
		panic(fmt.Sprintf("arenaRadix invariant violation: free-list node %d has non-nilNode tree/LRU pointers", freeID))
	}
}

func (c *arenaRadix[V]) checkFreeListInvariants(treeNodeCount int) {
	freeCount := 0
	for freeID := c.freeHead; freeID != nilNode; freeID = c.nodes[freeID].next {
		c.checkFreeNodeInvariants(freeID)
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
	c.checkLRUListWalkInvariants()
	c.checkLRUEndpointsInvariants()

	// INVARIANT 4: Root structure checks
	c.checkRootInvariants()

	// INVARIANT 5: Tree traversal & bijection checks
	treeNodeCount := c.checkTreeInvariants()

	// INVARIANT 6: Flat hash table (hashSlots) index bounds, value presence, and hash consistency.
	c.checkNodeMapInvariants()

	// INVARIANT 7: Free-list integrity and total node accounting (liveTreeNodes + freeListCount == len(nodes)).
	c.checkFreeListInvariants(treeNodeCount)

	c.checkTelemetryInvariants(c.len)
}

//go:noinline
func (c *arenaRadix[V]) unlockWithInvariants() {
	defer c.mu.Unlock()
	c.checkInvariants()
}

func (c *arenaRadix[V]) unlock() {
	if c.options.EnableInvariantChecking {
		c.unlockWithInvariants()
		return
	}
	c.mu.Unlock()
}

//go:noinline
func (c *arenaRadix[V]) rUnlockWithInvariants() {
	defer c.mu.RUnlock()
	c.checkInvariants()
}

func (c *arenaRadix[V]) rUnlock() {
	if c.options.EnableInvariantChecking {
		c.rUnlockWithInvariants()
		return
	}
	c.mu.RUnlock()
}

// Compact performs lossless O(N) compaction of the arena node slice and flat hash table
// under exclusive write lock while preserving 100% of live entries, sizes, and exact LRU order.
func (c *arenaRadix[V]) Compact() {
	c.mu.Lock()
	c.compactLocked()
	c.unlock()
}

func (c *arenaRadix[V]) evaluateShardPressure(evictQ *evictCallbackQueue[V], sample shardSample) ([]V, shardSample) {
	sampledEpoch, out := c.lockShardWithSample(&c.mu, true, sample)
	evicted := c.maybeReclaimUnderPressureLocked(evictQ, out.pressure, nilNode)
	out.advanceSelfReclaim(c.reclaimEpoch.Load(), sampledEpoch)
	c.unlock()
	return evicted, out
}

// EvaluateMemoryPressure samples the configured memory-pressure probe lock-free,
// then acquires the exclusive write lock to execute Tier 2 (LRU tail shedding + compaction)
// or Tier 1 (lossless compaction) if pressure meets or exceeds the configured thresholds.
// Returns any values evicted during Tier 2 critical-pressure shedding.
func (c *arenaRadix[V]) EvaluateMemoryPressure() []V {
	evictQ := evictCallbackQueue[V]{exactSnap: true}
	evicted, _ := c.evaluateShardPressure(&evictQ, shardSample{})
	evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	return evicted
}

func (c *arenaRadix[V]) finishDeleteReclaimLocked(evictQ *evictCallbackQueue[V], sizeBefore, sampledEpoch uint64, pressure float64) {
	if c.len == 0 {
		hadSlack := c.hasEmptyDeleteSlack(c.freeCount >= minChurnCompactDeletes)
		if hadSlack {
			c.recordCompactionByPressure(pressure)
		}
		c.clearEmptyArenaStateLocked()
		c.maybeMarkReclaimed(hadSlack || sizeBefore > 0, sampledEpoch, pressure)
		return
	}
	reclaimedSingleSurvivor := false
	if c.shouldReclaimSingleSurvivorOnDelete(c.len, c.freeCount > 0 || c.nodeMapDirty, c.freeCount >= minSingleSurvivorFreeNodes) {
		if c.compactDataStructuresLocked() {
			c.recordCompactionByPressure(pressure)
		}
		reclaimedSingleSurvivor = true
	}
	if pressure >= c.options.CompactionThreshold || pressure >= c.options.EvictionThreshold {
		c.maybeReclaimUnderPressureLocked(evictQ, pressure, foregroundNoProtect)
	} else {
		c.resetZeroWatermarkBelowTier2(pressure)
	}
	c.maybeMarkReclaimed(reclaimedSingleSurvivor || c.currentSize < sizeBefore, sampledEpoch, pressure)
}

//go:noinline
func (c *arenaRadix[V]) finishMutationReclaimLocked(evictQ *evictCallbackQueue[V], evictedValues []V, protectedID uint32, reclaimedPre, compactedPre bool, sizeBefore, sampledEpoch uint64, pressure float64) []V {
	evictedByPressure := c.maybeReclaimUnderPressureLocked(evictQ, pressure, protectedID)
	evictedValues = appendEvicted(evictedValues, evictedByPressure)
	netByteReduced := c.currentSize < sizeBefore
	if c.shouldCompactAfterMutation(reclaimedPre, netByteReduced, c.isDirtyLocked(), pressure) {
		if c.compactDataStructuresLocked() && !compactedPre {
			c.recordCompactionByPressure(pressure)
		}
	}
	if c.shouldMarkReclaimedAfterMutation(reclaimedPre, netByteReduced, sampledEpoch, pressure) {
		c.markReclaimedLocked()
	}
	return evictedValues
}

func (c *arenaRadix[V]) updateExistingOnPutLocked(evictQ *evictCallbackQueue[V], nodeID uint32, key string, keyHash uint64, value V, valueSize, sizeBefore uint64, pressure float64) ([]V, bool) {
	var evictedValues []V
	evictedPrePut := false
	oldValue := c.nodes[nodeID].value
	oldSize := c.nodes[nodeID].size
	hadZeroCap := c.shouldEvictOnZeroWeightShrink(oldSize, valueSize, c.maxSize)
	c.onEntrySizeUpdated(oldSize, valueSize)
	c.moveToFront(nodeID)
	c.currentSize -= oldSize
	for (valueSize > c.maxSize-c.currentSize || (hadZeroCap && !evictedPrePut)) && c.tail != nilNode && c.tail != nodeID {
		if evicted, ok := c.evictOne(evictQ); ok {
			evictedValues = append(evictedValues, evicted)
			evictedPrePut = true
		}
	}
	c.nodes[nodeID].value = value
	c.nodes[nodeID].setHasValue(true)
	c.nodes[nodeID].size = valueSize
	c.currentSize += valueSize
	if evictedPrePut {
		c.promoteCollisionPeerLocked(keyHash, nodeID)
	}
	c.putUpdated++
	c.notifyReplaced(evictQ, key, oldValue, oldSize)
	reclaimedPrePut := c.shouldReclaimSingleSurvivorOnMutation(c.len, c.freeCount > 0 || c.nodeMapDirty, c.freeCount >= minSingleSurvivorFreeNodes, evictedPrePut, c.currentSize, sizeBefore, pressure)
	return evictedValues, reclaimedPrePut
}

func (c *arenaRadix[V]) insertNewOnPutLocked(evictQ *evictCallbackQueue[V], key string, keyHash uint64, value V, valueSize, sizeBefore uint64, pressure float64, verifiedMapMiss bool) (uint32, []V, bool, bool) {
	var evictedValues []V
	evictedPrePut := false
	reclaimedPrePut := false
	compactedPrePut := false

	// A single new-key insert can allocate up to 2 nodes (one routing node, one leaf).
	// If the slice has reached its physical uint32 maximum, ensure enough free slots exist.
	for uint64(len(c.nodes))-uint64(c.freeCount)+2 > uint64(foregroundNoProtect) && c.tail != nilNode {
		if evicted, ok := c.evictOne(evictQ); ok {
			evictedValues = append(evictedValues, evicted)
			evictedPrePut = true
		}
	}

	// Evict from the LRU tail before allocating new arena nodes when valueSize would exceed remaining capacity
	// (using subtraction to avoid uint64 addition overflow when maxSize is near math.MaxUint64).
	for c.shouldEvictPreInsertOnPut(valueSize, c.maxSize, c.currentSize, c.len, evictedPrePut) && c.tail != nilNode {
		if evicted, ok := c.evictOne(evictQ); ok {
			evictedValues = append(evictedValues, evicted)
			evictedPrePut = true
		}
	}
	if c.shouldReclaimEmptyPrePut(c.len, c.freeCount >= minChurnCompactDeletes, evictedPrePut, valueSize, sizeBefore, pressure) {
		c.recordCompactionByPressure(pressure)
		c.clearEmptyArenaStateLocked()
		reclaimedPrePut = true
		compactedPrePut = true
	} else if c.shouldReclaimSingleSurvivorOnMutation(c.len, c.freeCount > 0 || c.nodeMapDirty, c.freeCount >= minSingleSurvivorFreeNodes, evictedPrePut, c.currentSize+valueSize, sizeBefore, pressure) {
		reclaimedPrePut = true
	}

	nodeID := c.insertNode(key, value)
	c.nodes[nodeID].size = valueSize
	c.pushFront(nodeID)
	c.putInserted++
	c.onEntryPut(c.len, valueSize)
	c.currentSize += valueSize
	if verifiedMapMiss && c.collisionPeers == nil {
		c.insertNewHashSlotLocked(keyHash, nodeID)
	} else {
		c.recordNodeMapInsertLocked(keyHash, nodeID)
	}
	return nodeID, evictedValues, reclaimedPrePut, compactedPrePut
}

// Put inserts or updates the given key and value in the cache.
// If the key already exists, its value is replaced and promoted to MRU position.
// If the cache exceeds capacity after insertion, LRU entries are evicted and returned.
//
// Returns ErrInvalidEntrySize if the entry's weight exceeds maxSize.
func (c *arenaRadix[V]) Put(key string, value V) ([]V, error) {
	return c.putWithHash(key, c.hashKey(key), value)
}

func (c *arenaRadix[V]) putWithHash(key string, keyHash uint64, value V) ([]V, error) {
	valueSize := c.weigh(key, value)
	if valueSize > c.maxSize {
		c.putRejectedOversized.Add(1)
		return nil, ErrInvalidEntrySize
	}

	var evictQ evictCallbackQueue[V]
	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)

	var evictedValues []V
	sizeBefore := c.currentSize
	reclaimedPrePut := false
	compactedPrePut := false

	nodeID, exists, mapMiss := c.getNodeKeyForPut(key, keyHash)
	if exists {
		evictedValues, reclaimedPrePut = c.updateExistingOnPutLocked(&evictQ, nodeID, key, keyHash, value, valueSize, sizeBefore, pressure)
	} else {
		nodeID, evictedValues, reclaimedPrePut, compactedPrePut = c.insertNewOnPutLocked(&evictQ, key, keyHash, value, valueSize, sizeBefore, pressure, mapMiss)
	}

	if c.needsArenaPutReclaimLocked(reclaimedPrePut, sizeBefore, pressure) {
		evictedValues = c.finishMutationReclaimLocked(&evictQ, evictedValues, nodeID, reclaimedPrePut, compactedPrePut, sizeBefore, sampledEpoch, pressure)
	}
	if c.options.EnableInvariantChecking {
		c.unlockWithInvariants()
	} else {
		c.mu.Unlock()
	}
	evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	return evictedValues, nil
}

func (c *arenaRadix[V]) canSkipElevatedPutReclaimLocked(pressure float64) bool {
	if c.isDirtyLocked() || c.zeroSizeCount > 0 || c.lastReclaimedZeroCount != 0 || c.lastReclaimedLen != 0 {
		return false
	}
	if pressure < c.options.EvictionThreshold {
		return true
	}
	return c.currentSize <= c.tier2TargetSize(c.maxSize) && c.options.EvictionRetentionRatio > 0.0
}

func (c *arenaRadix[V]) needsArenaPutReclaimLocked(reclaimedPre bool, sizeBefore uint64, pressure float64) bool {
	if reclaimedPre || c.currentSize < sizeBefore {
		return true
	}
	if pressure < c.options.CompactionThreshold && pressure < c.options.EvictionThreshold {
		return false
	}
	return !c.canSkipElevatedPutReclaimLocked(pressure)
}

// Delete removes the entry associated with key from the cache, returning its value and true (or the zero value of V and false if not found).
func (c *arenaRadix[V]) Delete(key string) (value V, ok bool) {
	return c.deleteWithHash(key, c.hashKey(key))
}

func (c *arenaRadix[V]) deleteWithHash(key string, keyHash uint64) (value V, ok bool) {
	evictQ := evictCallbackQueue[V]{exactSnap: true}
	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)

	nodeID, found := c.lookupNodeKeyWithHash(key, keyHash)
	if !found {
		c.deleteNotFound++
		c.unlock()
		return value, false
	}

	sizeBefore := c.currentSize
	deleted, erased := c.eraseInternalWithHash(&evictQ, nodeID, keyHash, key, EvictionReasonDeleted)
	if !erased {
		c.deleteNotFound++
		c.unlock()
		return value, false
	}
	c.deleteDeleted++
	c.finishDeleteReclaimLocked(&evictQ, sizeBefore, sampledEpoch, pressure)
	c.unlock()
	evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	return deleted, true
}

// Get retrieves the value associated with key and promotes it to MRU position.
// Returns the zero value of V and false if the key is not found in the cache.
func (c *arenaRadix[V]) Get(key string) (value V, ok bool) {
	return c.getWithHash(key, c.hashKey(key))
}

func (c *arenaRadix[V]) getWithHash(key string, keyHash uint64) (value V, ok bool) {
	c.mu.Lock()

	nodeID, found := c.getNodeKeyWithHash(key, keyHash)
	if found {
		c.getHits++
		c.moveToFront(nodeID)
		value = c.nodes[nodeID].value
	} else {
		c.getMisses++
	}
	if c.options.EnableInvariantChecking {
		c.unlockWithInvariants()
	} else {
		c.mu.Unlock()
	}
	return value, found
}

// Peek retrieves the value associated with key without altering its LRU position.
// Returns the zero value of V and false if the key is not found in the cache.
func (c *arenaRadix[V]) Peek(key string) (value V, ok bool) {
	return c.peekWithHash(key, c.hashKey(key))
}

func (c *arenaRadix[V]) peekWithHash(key string, keyHash uint64) (value V, ok bool) {
	c.mu.RLock()

	nodeID, found := c.lookupNodeKeyWithHash(key, keyHash)
	if found {
		c.peekHits.Add(1)
		value = c.nodes[nodeID].value
	} else {
		c.peekMisses.Add(1)
	}
	if c.options.EnableInvariantChecking {
		c.rUnlockWithInvariants()
	} else {
		c.mu.RUnlock()
	}
	return value, found
}

func (c *arenaRadix[V]) canFitGrowthLocked(nodeID uint32, maxNewer, sizeDelta, avail uint64) bool {
	if sizeDelta <= avail {
		return true
	}
	var newerSize uint64
	headCurr := c.head
	tailCurr := c.tail
	for {
		if headCurr == nodeID {
			return true
		}
		newerSize += c.nodes[headCurr].size
		if newerSize > maxNewer {
			return false
		}
		headCurr = c.nodes[headCurr].next
		if tailCurr == nodeID {
			return false
		}
		avail += c.nodes[tailCurr].size
		if sizeDelta <= avail {
			return true
		}
		tailCurr = c.nodes[tailCurr].prev
	}
}

func (c *arenaRadix[V]) replaceResizeOrEvictLocked(evictQ *evictCallbackQueue[V], nodeID uint32, key string, keyHash, oldSize, newSize, sizeBefore, sampledEpoch uint64, pressure float64) (evictedAny, selfEvicted bool) {
	if newSize > oldSize {
		sizeDelta := newSize - oldSize
		if newSize > c.maxSize || !c.canFitGrowthLocked(nodeID, c.maxSize-newSize, sizeDelta, c.maxSize-c.currentSize) {
			c.replaceSelfEvicted++
			c.eraseInternalWithHash(evictQ, nodeID, keyHash, key, EvictionReasonCapacity)
			c.finishDeleteReclaimLocked(evictQ, sizeBefore, sampledEpoch, pressure)
			return false, true
		}

		for sizeDelta > c.maxSize-c.currentSize && c.tail != nilNode && c.tail != nodeID {
			c.evictOne(evictQ)
			evictedAny = true
		}

		c.onEntrySizeUpdated(oldSize, newSize)
		c.currentSize += sizeDelta
		return evictedAny, false
	}
	if c.shouldEvictOnZeroWeightShrink(oldSize, newSize, c.maxSize) && c.tail != nodeID {
		c.evictOne(evictQ)
		evictedAny = true
	}
	c.onEntrySizeUpdated(oldSize, newSize)
	c.currentSize -= oldSize - newSize
	return evictedAny, false
}

// Replace updates the value of an existing key and recomputes its weight
// without modifying its LRU position.
// If the entry's updated weight exceeds maxSize (or cannot fit alongside entries more recent than node),
// only the entry itself is evicted without evicting older entries.
// Otherwise, if the updated cache size exceeds maxSize, excess LRU entries are evicted to maintain capacity invariants.
//
// Returns ErrEntryNotExist if key is not present in the cache.
func (c *arenaRadix[V]) Replace(key string, value V) error {
	return c.replaceWithHash(key, c.hashKey(key), value)
}

func (c *arenaRadix[V]) replaceWithHash(key string, keyHash uint64, value V) error {
	newSize := c.weigh(key, value)

	var evictQ evictCallbackQueue[V]
	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)

	nodeID, walked, ok := c.lookupOrWalkNodeKey(key, keyHash)
	if !ok {
		c.replaceNotFound++
		c.unlock()
		return ErrEntryNotExist
	}

	oldSize := c.nodes[nodeID].size
	oldValue := c.nodes[nodeID].value
	sizeBefore := c.currentSize
	evictedAny := false

	if newSize != oldSize {
		var selfEvicted bool
		evictedAny, selfEvicted = c.replaceResizeOrEvictLocked(&evictQ, nodeID, key, keyHash, oldSize, newSize, sizeBefore, sampledEpoch, pressure)
		if selfEvicted {
			c.unlock()
			evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
			return nil
		}
	}

	c.nodes[nodeID].value = value
	c.nodes[nodeID].setHasValue(true)
	c.nodes[nodeID].size = newSize

	if walked || evictedAny {
		c.promoteCollisionPeerLocked(keyHash, nodeID)
	}
	c.replaceUpdated++
	c.notifyReplaced(&evictQ, key, oldValue, oldSize)
	reclaimedPreUpdate := c.shouldReclaimSingleSurvivorOnMutation(c.len, c.freeCount > 0 || c.nodeMapDirty, c.freeCount >= minSingleSurvivorFreeNodes, evictedAny, c.currentSize, sizeBefore, pressure)

	if c.needsMutationReclaim(reclaimedPreUpdate, c.currentSize, sizeBefore, pressure) {
		c.finishMutationReclaimLocked(&evictQ, nil, nodeID, reclaimedPreUpdate, false, sizeBefore, sampledEpoch, pressure)
	}
	c.unlock()
	evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	return nil
}

func (c *arenaRadix[V]) evictAllForClearLocked(evictQ *evictCallbackQueue[V]) {
	if c.len == 0 {
		return
	}
	if c.onEvictValue == nil && c.onEvictEntry == nil {
		c.evictionsDeleted += uint64(c.len)
		c.evictedWeightDeleted += c.currentSize
		return
	}
	var zero V
	for currID := c.head; currID != nilNode; {
		nextID := c.nodes[currID].next
		var key string
		if c.onEvictEntry != nil {
			if c.nodes[currID].parent == c.root && c.nodes[currID].prefixLen > 0 {
				c.prefixExposed.Store(true)
			}
			key = c.reconstructKey(currID)
		}
		evictedVal := c.nodes[currID].value
		evictedSize := c.nodes[currID].size
		c.currentSize -= evictedSize
		c.remove(currID)
		c.nodes[currID].value = zero
		c.nodes[currID].setHasValue(false)
		c.nodes[currID].size = 0
		c.notifyEvict(evictQ, key, evictedVal, evictedSize, EvictionReasonDeleted)
		currID = nextID
	}
}

func (c *arenaRadix[V]) isCleanEmptyLocked() bool {
	hadEntries := c.len > 0
	hadDirtySlack := c.freeCount > 0 || c.nodeMapDirty || c.peakEntryLen > minPeakSlackEntries || c.deletedSinceCompact > 0 || (c.len == 0 && cap(c.nodes) > 1)
	return !hadEntries && !hadDirtySlack && c.peakEntryLen == 0 && len(c.nodes) <= 1
}

func (c *arenaRadix[V]) deleteAllPrefixLocked(evictQ *evictCallbackQueue[V], sampledEpoch uint64, pressure float64) {
	c.deletePrefixExecuted++
	if c.isCleanEmptyLocked() {
		return
	}
	hadDirtySlack := c.freeCount > 0 || c.nodeMapDirty || c.peakEntryLen > minPeakSlackEntries || c.deletedSinceCompact > 0 || (c.len == 0 && cap(c.nodes) > 1)
	hadReclaimable := c.currentSize > 0 || hadDirtySlack
	c.deletedSinceCompact += c.len
	hadCompactionSlack := c.hasEmptyDeleteSlack(len(c.nodes)-1 >= minChurnCompactDeletes)
	c.evictAllForClearLocked(evictQ)
	c.clearEmptyArenaStateLocked()
	c.finishClearAllLocked(hadCompactionSlack, hadReclaimable, sampledEpoch, pressure)
}

func (c *arenaRadix[V]) deleteNonEmptyPrefixLocked(evictQ *evictCallbackQueue[V], prefix string, sampledEpoch uint64, pressure float64) {
	c.deletePrefixExecuted++
	nodeID := c.root
	search := prefix

	for len(search) > 0 {
		childID := c.getChild(nodeID, search[0])
		if childID == nilNode {
			return // Prefix doesn't exist
		}

		childPrefix := c.nodePrefix(childID)
		lcp := longestCommonPrefix(search, childPrefix)

		if lcp == len(search) {
			sizeBefore := c.currentSize
			// We found the exact node where the prefix ends.
			// Sever it entirely from the tree structure
			c.removeChild(nodeID, childID)

			// Temporarily restore upward parent pointer for non-recursive traversal
			c.nodes[childID].parent = nodeID

			// Now sweep the detached subtree to fix LRU, hashSlots, and currentSize
			c.freeSubtree(evictQ, childID)
			c.compressPathUpwards(nodeID)
			c.finishDeleteReclaimLocked(evictQ, sizeBefore, sampledEpoch, pressure)
			return
		}

		if lcp == len(childPrefix) {
			search = search[len(childPrefix):]
			nodeID = childID
			continue
		}

		return
	}
}

func (c *arenaRadix[V]) deletePrefixShard(evictQ *evictCallbackQueue[V], prefix string, sample shardSample) (shardSample, bool) {
	if prefix == "" {
		c.mu.Lock()
		if c.isCleanEmptyLocked() {
			c.deletePrefixExecuted++
			c.unlock()
			return sample, sample.hasSample
		}
		if sample.hasSample && c.tryApplyShardSampleLocked(false, &sample) {
			preEpoch := c.reclaimEpoch.Load()
			c.deleteAllPrefixLocked(evictQ, preEpoch, sample.pressure)
			sample.advanceSelfReclaim(c.reclaimEpoch.Load(), preEpoch)
			c.unlock()
			return sample, true
		}
		c.mu.Unlock()
		sampledEpoch, out := c.lockShardWithSample(&c.mu, false, sample)
		c.deleteAllPrefixLocked(evictQ, sampledEpoch, out.pressure)
		out.advanceSelfReclaim(c.reclaimEpoch.Load(), sampledEpoch)
		c.unlock()
		return out, true
	}

	sampledEpoch, out := c.lockShardWithSample(&c.mu, false, sample)
	c.deleteNonEmptyPrefixLocked(evictQ, prefix, sampledEpoch, out.pressure)
	out.advanceSelfReclaim(c.reclaimEpoch.Load(), sampledEpoch)
	c.unlock()
	return out, true
}

// DeletePrefix deletes all entries whose keys begin with prefix.
// It severs the matching subtree in O(1) and iteratively reclaims all nodes into the free-list.
func (c *arenaRadix[V]) DeletePrefix(prefix string) {
	evictQ := evictCallbackQueue[V]{exactSnap: true}
	_, _ = c.deletePrefixShard(&evictQ, prefix, shardSample{})
	evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
}

// evictSubtreeValueNodeLocked removes an active value-bearing node encountered during subtree
// reclamation from the LRU list, hashSlots, and size accounting, and enqueues its eviction callback.
func (c *arenaRadix[V]) evictSubtreeValueNodeLocked(evictQ *evictCallbackQueue[V], currID uint32, keyBytes []byte, hasKeyBytes bool) {
	var zero V
	var key string
	fastDeleted := false
	switch {
	case c.onEvictEntry != nil && hasKeyBytes:
		key = string(keyBytes)
		c.removeOrPromoteNodeMapLocked(maphash.Bytes(c.seed, keyBytes), currID)
		fastDeleted = true
	case c.onEvictEntry != nil:
		if c.nodes[currID].parent == c.root {
			c.nodes[currID].prefixFlags |= prefixFlagPinned
		}
		key = c.reconstructKey(currID)
		c.removeOrPromoteNodeMapLocked(c.hashKey(key), currID)
		fastDeleted = true
	case c.collisionCount == 0 && c.hashMask <= hashTag21Mask:
		fastDeleted = c.deleteNodeFromHashSlotsFastLocked(currID)
	}
	if !fastDeleted {
		var currHash uint64
		if hasKeyBytes {
			currHash = maphash.Bytes(c.seed, keyBytes)
		} else {
			currHash = c.hashNodeKey(currID)
		}
		c.removeOrPromoteNodeMapLocked(currHash, currID)
	}
	evictedVal := c.nodes[currID].value
	evictedSize := c.nodes[currID].size
	c.onEntryDeleted(evictedSize)
	c.currentSize -= evictedSize
	c.remove(currID)
	c.nodeMapDirty = true
	c.nodes[currID].value = zero
	c.nodes[currID].setHasValue(false)
	c.nodes[currID].size = 0
	c.notifyEvict(evictQ, key, evictedVal, evictedSize, EvictionReasonDeleted)
}

// initSubtreeKeyBuf seeds keyBuf with the full key of the subtree root nodeID.
// It returns (keyLen, true) when nodeID's key fits within keyBuf and matches its ancestor chain.
func (c *arenaRadix[V]) initSubtreeKeyBuf(nodeID uint32, keyBuf *[512]byte) (int, bool) {
	keyPos := int(c.nodes[nodeID].keyLenAndFlags & nodeKeyLenMask)
	if keyPos <= 0 || keyPos > len(keyBuf) || !c.fillNodeKeyBuf(nodeID, keyPos, keyBuf[:keyPos]) {
		return 0, false
	}
	return keyPos, true
}

// appendSubtreeKeyBuf appends prefix into keyBuf at keyPos when bufValid is true and the
// resulting length fits within keyBuf, and always returns the advanced offset keyPos + len(prefix).
func appendSubtreeKeyBuf(keyBuf *[512]byte, keyPos int, prefix string, bufValid bool) int {
	if bufValid && keyPos+len(prefix) <= len(keyBuf) {
		copy(keyBuf[keyPos:], prefix)
	}
	return keyPos + len(prefix)
}

// freeSubtree iteratively reclaims all nodes in a detached subtree using O(1) stack space,
// removing active values from the LRU list, hashSlots, and accounting,
// and returning nodes to the free-list.
func (c *arenaRadix[V]) freeSubtree(evictQ *evictCallbackQueue[V], nodeID uint32) {
	if nodeID == nilNode {
		return
	}
	if c.nodes[nodeID].child == nilNode {
		if c.nodes[nodeID].hasValue() {
			c.evictSubtreeValueNodeLocked(evictQ, nodeID, nil, false)
		}
		c.freeNode(nodeID)
		return
	}

	var keyBuf [512]byte
	var keyPos int
	var bufValid bool
	if c.onEvictEntry != nil || c.collisionCount > 0 || c.hashMask > hashTag21Mask {
		keyPos, bufValid = c.initSubtreeKeyBuf(nodeID, &keyBuf)
	}

	currID := nodeID
	for currID != nilNode {
		if c.nodes[currID].hasValue() {
			if bufValid && keyPos <= len(keyBuf) {
				c.evictSubtreeValueNodeLocked(evictQ, currID, keyBuf[:keyPos], true)
			} else {
				c.evictSubtreeValueNodeLocked(evictQ, currID, nil, false)
			}
		}

		// Track keyPos unconditionally across descent, backtrack, and sibling transitions even
		// when keyPos exceeds len(keyBuf), so backtracking out of a >512B branch into a <=512B
		// sibling restores the exact offset against the intact prefix in keyBuf.
		if childID := c.nodes[currID].child; childID != nilNode {
			keyPos = appendSubtreeKeyBuf(&keyBuf, keyPos, c.nodePrefix(childID), bufValid)
			currID = childID
			continue
		}

		for currID != nodeID && c.nodes[currID].sibling == nilNode {
			keyPos -= c.nodePrefixLen(currID)
			parentID := c.nodes[currID].parent
			c.freeNode(currID)
			currID = parentID
		}

		if currID == nodeID {
			c.freeNode(currID)
			return
		}

		siblingID := c.nodes[currID].sibling
		keyPos = appendSubtreeKeyBuf(&keyBuf, keyPos-c.nodePrefixLen(currID), c.nodePrefix(siblingID), bufValid)
		c.freeNode(currID)
		currID = siblingID
	}
}

// All returns an iterator over all key-value pairs in MRU-to-LRU order without modifying recency.
func (c *arenaRadix[V]) All() iter.Seq2[string, V] {
	return c.all
}

func (c *arenaRadix[V]) all(yield func(string, V) bool) {
	c.iterateAll(yield)
}

func (c *arenaRadix[V]) iterateAll(yield func(string, V) bool) bool {
	c.mu.RLock()
	defer c.rUnlock()

	if c.head != nilNode {
		c.prefixExposed.Store(true)
	}
	for currID := c.head; currID != nilNode; currID = c.nodes[currID].next {
		if !yield(c.reconstructKey(currID), c.nodes[currID].value) {
			return false
		}
	}
	return true
}

// Keys returns an iterator over all keys in MRU-to-LRU order without modifying recency.
func (c *arenaRadix[V]) Keys() iter.Seq[string] {
	return c.keys
}

func (c *arenaRadix[V]) keys(yield func(string) bool) {
	c.iterateKeys(yield)
}

func (c *arenaRadix[V]) iterateKeys(yield func(string) bool) bool {
	c.mu.RLock()
	defer c.rUnlock()

	if c.head != nilNode {
		c.prefixExposed.Store(true)
	}
	for currID := c.head; currID != nilNode; currID = c.nodes[currID].next {
		if !yield(c.reconstructKey(currID)) {
			return false
		}
	}
	return true
}

// Values returns an iterator over all values in MRU-to-LRU order without modifying recency.
// It skips key reconstruction completely, executing with zero heap allocations.
func (c *arenaRadix[V]) Values() iter.Seq[V] {
	return c.values
}

func (c *arenaRadix[V]) values(yield func(V) bool) {
	c.iterateValues(yield)
}

func (c *arenaRadix[V]) iterateValues(yield func(V) bool) bool {
	c.mu.RLock()
	defer c.rUnlock()

	for currID := c.head; currID != nilNode; currID = c.nodes[currID].next {
		if !yield(c.nodes[currID].value) {
			return false
		}
	}
	return true
}

// Stats returns a point-in-time, zero-allocation telemetry snapshot of the cache's
// occupancy, lookup performance, mutations, evictions, compactions, memory pressure,
// and contiguous arena node mechanics.
func (c *arenaRadix[V]) Stats() Stats {
	c.mu.RLock()

	st := c.snapshotBaseStats(BackendArenaRadix, c.currentSize, c.maxSize, c.len)
	freeCount := int(c.freeCount)
	st.ArenaLiveNodes = max(len(c.nodes)-1-freeCount, 0)
	st.ArenaFreeNodes = freeCount
	st.ArenaUnallocatedCap = max(cap(c.nodes)-len(c.nodes), 0)
	st.ArenaHashFallbacks = c.arenaHashFallbacks.Load()
	if c.options.EnableInvariantChecking {
		c.rUnlockWithInvariants()
	} else {
		c.mu.RUnlock()
	}
	return st
}
