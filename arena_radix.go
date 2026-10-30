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
	"hash/maphash"
	"math"
	"slices"
	"strings"
	"sync"
)

// nilNode represents the sentinel null reference for 32-bit node indices.
const nilNode uint32 = math.MaxUint32

// arenaRadixNode represents a node in the arena-backed radix tree.
// It uses a Left-Child Right-Sibling (LCRS) tree representation and 32-bit indices
// within a contiguous slice to eliminate per-node heap allocations and internal tree pointer
// traversal during GC cycles, while stored values and fullKey strings retain standard Go GC properties.
// Storing fullKey at bytes 0..15 and edgeStart in the alignment padding at bytes 46..47
// enables O(1) key verification, O(1) maphash re-hashing, and 0-alloc key reconstruction and path compression.
type arenaRadixNode[V any] struct {
	fullKey   string // Full key from root up to this node (incoming edge label is fullKey[edgeStart:])
	size      uint64 // Tracked size of the value stored at this node
	parent    uint32 // Arena slice index of parent node
	child     uint32 // Arena slice index of first child (LCRS)
	sibling   uint32 // Arena slice index of next sibling (LCRS)
	prev      uint32 // Arena slice index of previous node in intrusive LRU list
	next      uint32 // Arena slice index of next node in intrusive LRU list (or free-list)
	hasValue  bool   // True if this node stores a live cache entry value
	edgeStart uint16 // Byte offset within fullKey where this node's incoming edge label begins
	value     V      // Value stored at this node
}

func (n *arenaRadixNode[V]) edge() string {
	return n.fullKey[n.edgeStart:]
}

func (n *arenaRadixNode[V]) edgeFirstByte() byte {
	return n.fullKey[n.edgeStart]
}

// arenaRadix implements the Cache[V] and PressureAwareCache[V] interfaces using a contiguous arena-backed
// radix tree coupled with an intrusive 32-bit doubly-linked LRU list and an O(1) maphash lookup accelerator.
type arenaRadix[V any] struct {
	maxSize     uint64
	currentSize uint64

	nodes     []arenaRadixNode[V]
	freeHead  uint32
	freeCount uint32

	seed           maphash.Seed
	nodeMap        map[uint64]uint32
	collisionPeers map[uint64][]uint32
	nodeMapDirty   bool
	collisionCount int

	root uint32

	head uint32
	tail uint32

	len int

	weigher      func(key string, value V) uint64
	onEvictValue func(value V, reason EvictionReason)
	onEvictEntry func(key string, value V, reason EvictionReason)

	// mu is placed after the fields read on every lookup (nodes, seed, nodeMap, root):
	// RLock/RUnlock atomically update its reader count, and sharing a cache line
	// with those fields would make every concurrent lookup miss (false sharing).
	mu sync.RWMutex

	pressureState
}

func (c *arenaRadix[V]) notifyEvict(evictQ *evictCallbackQueue[V], key string, value V, size uint64, reason EvictionReason) {
	c.recordEviction(reason, size)
	if c.onEvictValue != nil || c.onEvictEntry != nil {
		evictQ.enqueue(key, value, reason)
	}
}

func (c *arenaRadix[V]) notifyReplaced(evictQ *evictCallbackQueue[V], key string, oldValue V, oldSize uint64) {
	c.evictionsReplaced++
	c.evictedWeightReplaced += oldSize
	if c.onEvictValue != nil || c.onEvictEntry != nil {
		evictQ.enqueue(key, oldValue, EvictionReasonReplaced)
	}
}

// hashKey computes the 64-bit hardware-accelerated maphash of key using the cache's seed.
func (c *arenaRadix[V]) hashKey(key string) uint64 {
	return maphash.String(c.seed, key)
}

// hashNodeKey computes the 64-bit maphash of the full key for nodeID in O(1) time.
func (c *arenaRadix[V]) hashNodeKey(nodeID uint32) uint64 {
	if nodeID == nilNode {
		return maphash.String(c.seed, "")
	}
	return maphash.String(c.seed, c.nodes[nodeID].fullKey)
}

// allocateNode allocates a node index from the free-list or appends to the arena slice.
func (c *arenaRadix[V]) allocateNode() uint32 {
	if c.freeHead != nilNode {
		id := c.freeHead
		c.freeHead = c.nodes[id].next
		c.freeCount--
		c.nodes[id].next = nilNode
		return id
	}
	id := uint32(len(c.nodes))
	if id >= foregroundNoProtect {
		panic("arena radix capacity exceeded limit")
	}
	c.nodes = append(c.nodes, arenaRadixNode[V]{
		parent:  nilNode,
		child:   nilNode,
		sibling: nilNode,
		prev:    nilNode,
		next:    nilNode,
	})
	return id
}

// freeNode reclaims a node by clearing its data and linking it to the singly-linked free-list.
func (c *arenaRadix[V]) freeNode(id uint32) {
	var zero V
	n := &c.nodes[id]
	n.fullKey = ""
	n.edgeStart = 0
	n.value = zero
	n.hasValue = false
	n.size = 0
	n.parent = nilNode
	n.child = nilNode
	n.sibling = nilNode
	n.prev = nilNode

	n.next = c.freeHead
	c.freeHead = id
	c.freeCount++
}

// getChild finds a child node of nID whose incoming edge starts with the given byte b.
// Returns nilNode if no matching child is found.
func (c *arenaRadix[V]) getChild(nID uint32, b byte) uint32 {
	nodes := c.nodes
	for curr := nodes[nID].child; curr != nilNode; curr = nodes[curr].sibling {
		c0 := nodes[curr].edgeFirstByte()
		if c0 == b {
			return curr
		}
		// Sibling chains are maintained in sorted lexicographical order, so early exit is possible.
		if c0 > b {
			return nilNode
		}
	}
	return nilNode
}

// addChild adds a child node under nID, preserving lexicographical order by incoming edge first byte.
func (c *arenaRadix[V]) addChild(nID uint32, newChildID uint32) {
	c.nodes[newChildID].parent = nID
	newFirstByte := c.nodes[newChildID].edgeFirstByte()
	pcurr := &c.nodes[nID].child
	for *pcurr != nilNode && c.nodes[*pcurr].edgeFirstByte() < newFirstByte {
		pcurr = &c.nodes[*pcurr].sibling
	}
	c.nodes[newChildID].sibling = *pcurr
	*pcurr = newChildID
}

// removeChild removes childToRemoveID from the sibling chain of nID.
func (c *arenaRadix[V]) removeChild(nID uint32, childToRemoveID uint32) {
	for pcurr := &c.nodes[nID].child; *pcurr != nilNode; pcurr = &c.nodes[*pcurr].sibling {
		if *pcurr != childToRemoveID {
			continue
		}
		*pcurr = c.nodes[childToRemoveID].sibling
		c.nodes[childToRemoveID].sibling = nilNode
		c.nodes[childToRemoveID].parent = nilNode
		return
	}
	panic("removeChild: requested child not found in sibling list")
}

// replaceChild replaces oldChildID with newChildID in the sibling list of nID.
func (c *arenaRadix[V]) replaceChild(nID uint32, oldChildID uint32, newChildID uint32) {
	for pcurr := &c.nodes[nID].child; *pcurr != nilNode; pcurr = &c.nodes[*pcurr].sibling {
		if *pcurr != oldChildID {
			continue
		}
		c.nodes[newChildID].parent = nID
		c.nodes[newChildID].sibling = c.nodes[oldChildID].sibling
		*pcurr = newChildID
		c.nodes[oldChildID].sibling = nilNode
		c.nodes[oldChildID].parent = nilNode
		return
	}
	panic("replaceChild: requested child not found in sibling list")
}

// slicePrefix returns s[:end], cloning it when the trailing suffix exceeds 64 bytes
// so surviving intermediate routing nodes never pin large evicted leaf backing arrays.
func slicePrefix(a, b string, end uint16) string {
	src := a
	if len(b) < len(src) {
		src = b
	}
	if len(src)-int(end) > 64 {
		return clonePrefix(src[:end])
	}
	return src[:end]
}

// insertNode inserts key-value into the radix trie and returns the node index.
func (c *arenaRadix[V]) insertNode(key string, value V) uint32 {
	nodeID := c.root
	search := key
	var offset uint16

	for {
		if len(search) == 0 {
			c.nodes[nodeID].value = value
			c.nodes[nodeID].hasValue = true
			return nodeID
		}

		childID := c.getChild(nodeID, search[0])
		if childID == nilNode {
			newLeafID := c.allocateNode()
			c.nodes[newLeafID].fullKey = key
			c.nodes[newLeafID].edgeStart = offset
			c.nodes[newLeafID].value = value
			c.nodes[newLeafID].hasValue = true
			c.addChild(nodeID, newLeafID)
			return newLeafID
		}

		childEdge := c.nodes[childID].edge()
		lcp := longestCommonPrefix(search, childEdge)

		if lcp == len(childEdge) {
			search = search[lcp:]
			offset += uint16(lcp)
			nodeID = childID
			continue
		}

		// Ensure surviving intermediate routing nodes never pin large evicted leaf backing arrays.
		splitEnd := offset + uint16(lcp)
		splitPrefix := slicePrefix(key, c.nodes[childID].fullKey, splitEnd)
		splitNodeID := c.allocateNode()
		c.nodes[splitNodeID].fullKey = splitPrefix
		c.nodes[splitNodeID].edgeStart = offset
		c.nodes[splitNodeID].parent = nodeID

		c.replaceChild(nodeID, childID, splitNodeID)

		c.nodes[childID].edgeStart = splitEnd
		c.addChild(splitNodeID, childID)

		if lcp == len(search) {
			c.nodes[splitNodeID].value = value
			c.nodes[splitNodeID].hasValue = true
			return splitNodeID
		}

		newLeafID := c.allocateNode()
		c.nodes[newLeafID].fullKey = key
		c.nodes[newLeafID].edgeStart = splitEnd
		c.nodes[newLeafID].value = value
		c.nodes[newLeafID].hasValue = true
		c.addChild(splitNodeID, newLeafID)
		return newLeafID
	}
}

// verifyKey validates that nodeID represents key in O(1) with zero heap allocations.
func (c *arenaRadix[V]) verifyKey(nodeID uint32, key string) bool {
	return c.nodes[nodeID].fullKey == key
}

func (c *arenaRadix[V]) findNodeByTrieWalk(key string) (uint32, bool) {
	curr := c.root
	search := key

	for curr != nilNode {
		if len(search) == 0 {
			if c.nodes[curr].hasValue {
				return curr, true
			}
			return nilNode, false
		}

		childID := c.getChild(curr, search[0])
		if childID == nilNode {
			return nilNode, false
		}

		edge := c.nodes[childID].edge()
		if !strings.HasPrefix(search, edge) {
			return nilNode, false
		}

		search = search[len(edge):]
		curr = childID
	}

	return nilNode, false
}

//go:noinline
func (c *arenaRadix[V]) walkNodeKeyFallback(key string, keyHash uint64, heal bool) (uint32, bool) {
	c.arenaHashFallbacks.Add(1)
	curr, ok := c.findNodeByTrieWalk(key)
	if heal && ok {
		c.promoteCollisionPeerLocked(keyHash, curr)
	}
	return curr, ok
}

func (c *arenaRadix[V]) lookupOrWalkNodeKey(key string, keyHash uint64) (nodeID uint32, walked, found bool) {
	if id, ok := c.nodeMap[keyHash]; ok {
		if id < uint32(len(c.nodes)) && c.nodes[id].hasValue && c.verifyKey(id, key) {
			return id, false, true
		}
	} else if len(c.nodeMap)+c.collisionCount == c.len {
		// By Invariant 6 and the Pigeonhole Principle, when len(c.nodeMap)+c.collisionCount == c.len,
		// every live value-bearing node's hash is present in c.nodeMap,
		// so a map miss is a guaranteed cache miss.
		return nilNode, false, false
	}
	curr, ok := c.walkNodeKeyFallback(key, keyHash, false)
	return curr, true, ok
}

// lookupNodeKeyWithHash finds a value-bearing node index for key using a precomputed maphash
// without mutating c.nodeMap.
func (c *arenaRadix[V]) lookupNodeKeyWithHash(key string, keyHash uint64) (uint32, bool) {
	if id, ok := c.nodeMap[keyHash]; ok {
		if id < uint32(len(c.nodes)) && c.nodes[id].hasValue && c.verifyKey(id, key) {
			return id, true
		}
	} else if len(c.nodeMap)+c.collisionCount == c.len {
		return nilNode, false
	}
	return c.walkNodeKeyFallback(key, keyHash, false)
}

// getNodeKeyWithHash finds a value-bearing node index for key under exclusive write lock,
// healing c.nodeMap[keyHash] if resolved via the slow-path trie walk.
func (c *arenaRadix[V]) getNodeKeyWithHash(key string, keyHash uint64) (uint32, bool) {
	nodeID, found, _ := c.getNodeKeyForPut(key, keyHash)
	return nodeID, found
}

func (c *arenaRadix[V]) getNodeKeyForPut(key string, keyHash uint64) (nodeID uint32, found, mapMiss bool) {
	if id, ok := c.nodeMap[keyHash]; ok {
		if id < uint32(len(c.nodes)) && c.nodes[id].hasValue && c.verifyKey(id, key) {
			return id, true, false
		}
	} else if len(c.nodeMap)+c.collisionCount == c.len {
		return nilNode, false, true
	}
	id, ok := c.walkNodeKeyFallback(key, keyHash, true)
	return id, ok, false
}

// compressPathUpwards walks up the tree, pruning empty leaf nodes and merging single-child intermediate routing nodes.
func (c *arenaRadix[V]) compressPathUpwards(currID uint32) {
	for currID != nilNode && currID != c.root {
		if c.nodes[currID].hasValue {
			break
		}

		if c.nodes[currID].child == nilNode {
			parentID := c.nodes[currID].parent
			c.removeChild(parentID, currID)
			c.freeNode(currID)
			currID = parentID
			continue
		}

		if c.nodes[c.nodes[currID].child].sibling == nilNode {
			onlyChildID := c.nodes[currID].child
			c.nodes[onlyChildID].edgeStart = c.nodes[currID].edgeStart
			c.nodes[onlyChildID].parent = c.nodes[currID].parent

			parentID := c.nodes[currID].parent
			c.replaceChild(parentID, currID, onlyChildID)

			c.freeNode(currID)
			return
		}
		break
	}
}

// moveToFront moves an existing node in the LRU list to the MRU position (head).
func (c *arenaRadix[V]) moveToFront(nodeID uint32) {
	if c.head == nodeID {
		return
	}
	n := &c.nodes[nodeID]
	prev := n.prev
	next := n.next

	if prev != nilNode {
		c.nodes[prev].next = next
	}
	if next != nilNode {
		c.nodes[next].prev = prev
	} else {
		c.tail = prev
	}
	n.prev = nilNode
	n.next = c.head
	if c.head != nilNode {
		c.nodes[c.head].prev = nodeID
	}
	c.head = nodeID
}

// pushFront inserts a newly added node at the MRU position (head) of the LRU list.
func (c *arenaRadix[V]) pushFront(nodeID uint32) {
	c.nodes[nodeID].prev = nilNode
	c.nodes[nodeID].next = c.head
	if c.head != nilNode {
		c.nodes[c.head].prev = nodeID
	}
	c.head = nodeID
	if c.tail == nilNode {
		c.tail = nodeID
	}
	c.len++
}

// remove unlinks nodeID from the intrusive LRU list.
func (c *arenaRadix[V]) remove(nodeID uint32) {
	n := &c.nodes[nodeID]
	if c.head != nodeID && n.prev == nilNode {
		return
	}
	prev := n.prev
	next := n.next

	if prev != nilNode {
		c.nodes[prev].next = next
	} else {
		c.head = next
	}
	if next != nilNode {
		c.nodes[next].prev = prev
	} else {
		c.tail = prev
	}
	n.prev = nilNode
	n.next = nilNode
	c.len--
}

// evictOne removes and returns the least recently used entry (tail) from the cache.
func (c *arenaRadix[V]) evictOne(evictQ *evictCallbackQueue[V]) (V, bool) {
	nodeID := c.tail
	if nodeID == nilNode {
		var zero V
		return zero, false
	}

	return c.eraseInternal(evictQ, nodeID, EvictionReasonCapacity)
}

const foregroundNoProtect uint32 = nilNode - 1

func (c *arenaRadix[V]) hasContiguousSlackLocked() bool {
	return (c.nodeMapDirty || c.deletedSinceCompact > 0) &&
		(c.len < c.peakEntryLen || c.deletedSinceCompact >= minChurnCompactDeletes || len(c.nodeMap)+c.collisionCount < c.len)
}

func (c *arenaRadix[V]) isDirtyLocked() bool {
	return c.freeHead != nilNode || c.hasContiguousSlackLocked()
}

// eraseInternal handles unlinking from LRU, cleaning up nodeMap, and deleting from the tree
// when the caller does not already have the key or hash in hand.
func (c *arenaRadix[V]) eraseInternal(evictQ *evictCallbackQueue[V], nodeID uint32, reason EvictionReason) (V, bool) {
	if nodeID == nilNode || !c.nodes[nodeID].hasValue {
		var zero V
		return zero, false
	}
	key := c.nodes[nodeID].fullKey
	return c.eraseInternalWithHash(evictQ, nodeID, c.hashKey(key), key, reason)
}

func (c *arenaRadix[V]) eraseInternalWithHash(evictQ *evictCallbackQueue[V], nodeID uint32, hash uint64, key string, reason EvictionReason) (V, bool) {
	if nodeID == nilNode || !c.nodes[nodeID].hasValue {
		var zero V
		return zero, false
	}
	deletedEntry := c.nodes[nodeID].value
	evictedSize := c.nodes[nodeID].size
	c.onEntryDeleted(evictedSize)
	c.nodeMapDirty = true
	c.currentSize -= evictedSize
	c.nodes[nodeID].size = 0

	c.removeOrPromoteNodeMapLocked(hash, nodeID)

	c.remove(nodeID)
	var zero V
	c.nodes[nodeID].value = zero
	c.nodes[nodeID].hasValue = false
	c.compressPathUpwards(nodeID)

	c.notifyEvict(evictQ, key, deletedEntry, evictedSize, reason)
	return deletedEntry, true
}

func (c *arenaRadix[V]) clearEmptyArenaStateLocked() {
	c.freeHead = nilNode
	c.freeCount = 0

	if cap(c.nodes) == 1 {
		c.nodes = c.nodes[:1]
		c.nodes[0] = arenaRadixNode[V]{parent: nilNode, child: nilNode, sibling: nilNode, prev: nilNode, next: nilNode}
		c.root = 0
	} else {
		clear(c.nodes)
		c.nodes = make([]arenaRadixNode[V], 1)
		c.nodes[0] = arenaRadixNode[V]{parent: nilNode, child: nilNode, sibling: nilNode, prev: nilNode, next: nilNode}
		c.root = 0
	}

	c.head = nilNode
	c.tail = nilNode
	c.currentSize = 0
	c.len = 0
	if c.nodeMap != nil && c.peakEntryLen <= minPeakSlackEntries && c.deletedSinceCompact < minChurnCompactDeletes {
		clear(c.nodeMap)
	} else {
		c.nodeMap = make(map[uint64]uint32)
	}
	c.nodeMapDirty = false
	c.collisionPeers = nil
	c.collisionCount = 0
	c.resetWatermarks()
}

// resetEmptyArenaLocked releases peak arena slice and hash-map allocations when the cache transitions to empty.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix[V]) resetEmptyArenaLocked() {
	c.clearEmptyArenaStateLocked()
	c.markReclaimedLocked()
}

// shouldAutoCompactLocked determines whether an automatic compaction should run.
// Explicit EvaluateMemoryPressure calls (protectedNodeID == nilNode) compact whenever any
// free slots, slice slack, or unhealed hash-collision slots exist. Inline foreground mutations
// (protectedNodeID != nilNode) only count recycled free-list fragmentation (>= 25% of len(c.nodes))
// so Go's geometric slice growth capacity slack never triggers O(N^2) compaction thrashing.
func (c *arenaRadix[V]) shouldAutoCompactLocked(protectedNodeID uint32) bool {
	if protectedNodeID == nilNode {
		return c.isDirtyLocked()
	}
	if c.freeHead != nilNode && c.peakEntryLen > minPeakSlackEntries && len(c.nodes) > minPeakSlackEntries && c.freeCount >= 2 && uint64(c.freeCount)*slackQuarterMultiplier >= uint64(len(c.nodes)) {
		return true
	}
	return c.shouldAutoCompactEntryCounts(c.hasContiguousSlackLocked(), false, c.len)
}

func (c *arenaRadix[V]) compactContiguousLocked() bool {
	if !c.hasContiguousSlackLocked() {
		return false
	}
	hasSliceSlack := c.deletedSinceCompact > 0 && len(c.nodes) < cap(c.nodes)
	if !hasSliceSlack && !c.nodeMapDirty {
		return false
	}
	if hasSliceSlack {
		newNodes := make([]arenaRadixNode[V], len(c.nodes))
		copy(newNodes, c.nodes)
		c.nodes = newNodes
	}
	if c.nodeMapDirty {
		newNodeMap := make(map[uint64]uint32, c.len)
		collisions := 0
		var newCollisionPeers map[uint64][]uint32
		for id := range uint32(len(c.nodes)) {
			if c.nodes[id].hasValue {
				h := c.hashNodeKey(id)
				if prevID, exists := newNodeMap[h]; exists {
					collisions++
					if newCollisionPeers == nil {
						newCollisionPeers = make(map[uint64][]uint32)
					}
					newCollisionPeers[h] = append(newCollisionPeers[h], prevID)
				}
				newNodeMap[h] = id
			}
		}
		c.nodeMap = newNodeMap
		c.collisionPeers = newCollisionPeers
		c.collisionCount = collisions
		c.nodeMapDirty = false
	}
	c.onCompacted(c.len)
	return true
}

// compactDataStructuresLocked performs the physical slice and map compaction without updating
// the reclamation epoch, returning true if any backing structure was reallocated.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix[V]) compactDataStructuresLocked() bool {
	// Fast-path: when no nodes are on the free-list, node indices are already contiguous [0..len-1].
	if c.freeHead == nilNode {
		return c.compactContiguousLocked()
	}

	oldLen := uint32(len(c.nodes))
	oldToNew := make([]uint32, oldLen)

	var liveCount uint32
	for oldID := range oldLen {
		if oldID == c.root || c.nodes[oldID].parent != nilNode {
			oldToNew[oldID] = liveCount
			liveCount++
		} else {
			oldToNew[oldID] = nilNode
		}
	}

	remap := func(idx uint32) uint32 {
		if idx == nilNode {
			return nilNode
		}
		return oldToNew[idx]
	}

	// Allocate brand-new slice with len(newNodes) == cap(newNodes) == liveCount,
	// and brand-new hash accelerator map healing any hash-collided surviving keys.
	newNodes := make([]arenaRadixNode[V], liveCount)
	newNodeMap := make(map[uint64]uint32, c.len)
	collisions := 0
	var newCollisionPeers map[uint64][]uint32
	for oldID := range oldLen {
		newID := oldToNew[oldID]
		if newID == nilNode {
			continue
		}
		oldNode := &c.nodes[oldID]
		if oldNode.hasValue {
			h := c.hashNodeKey(oldID)
			if prevID, exists := newNodeMap[h]; exists {
				collisions++
				if newCollisionPeers == nil {
					newCollisionPeers = make(map[uint64][]uint32)
				}
				newCollisionPeers[h] = append(newCollisionPeers[h], prevID)
			}
			newNodeMap[h] = newID
		}
		newNodes[newID] = arenaRadixNode[V]{
			fullKey:   oldNode.fullKey,
			value:     oldNode.value,
			size:      oldNode.size,
			parent:    remap(oldNode.parent),
			child:     remap(oldNode.child),
			sibling:   remap(oldNode.sibling),
			prev:      remap(oldNode.prev),
			next:      remap(oldNode.next),
			hasValue:  oldNode.hasValue,
			edgeStart: oldNode.edgeStart,
		}
	}

	// Remap top-level arena indices and clear free-list state.
	c.root = remap(c.root)
	c.head = remap(c.head)
	c.tail = remap(c.tail)
	c.freeHead = nilNode
	c.freeCount = 0
	c.nodes = newNodes
	c.nodeMap = newNodeMap
	c.collisionPeers = newCollisionPeers
	c.collisionCount = collisions
	c.nodeMapDirty = false
	c.onCompacted(c.len)
	return true
}

// compactLocked performs lossless O(N) compaction of the node arena slice and hash lookup map.
// It eliminates all free-list slots so len(c.nodes) == cap(c.nodes) == liveCount and c.freeHead == nilNode,
// remaps all 8 uint32 index pointers (root, head, tail, parent, child, sibling, prev, next),
// and reallocates c.nodeMap to eliminate Go map bucket slack while preserving 100% of live entries and LRU order.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix[V]) compactLocked() {
	if c.compactDataStructuresLocked() {
		c.compactionsExplicit++
		c.markReclaimedLocked()
	}
}

func (c *arenaRadix[V]) completeShedAndCompactLocked(evictedCount int, retention float64, unadjusted bool, unprotectedZeroTarget, targetLen int, autoCompactID uint32) {
	isBackground := autoCompactID == nilNode
	if evictedCount > 0 {
		c.recordPressureShed(isBackground)
		if c.len == 0 {
			if c.shouldAutoCompactEntryCounts(true, isBackground, 0) || c.freeCount >= minChurnCompactDeletes {
				c.compactionsPressureTier2++
			}
			c.resetEmptyArenaLocked()
			return
		}
		c.updateZeroWatermarkAfterShed(retention, c.len, unadjusted, unprotectedZeroTarget, targetLen)
	}
	compacted := false
	if c.shouldAutoCompactLocked(autoCompactID) {
		compacted = c.compactDataStructuresLocked()
		if compacted {
			c.compactionsPressureTier2++
		}
	}
	if evictedCount > 0 || compacted {
		c.markReclaimedLocked()
	}
}

func (c *arenaRadix[V]) autoCompactTier2OnlyLocked(protectedNodeID uint32) {
	if c.shouldAutoCompactLocked(protectedNodeID) && c.compactDataStructuresLocked() {
		c.compactionsPressureTier2++
		c.markReclaimedLocked()
	}
}

// shedAndCompactLocked evicts least-recently-used entries strictly from c.tail in a single O(N) pass
// until c.currentSize <= targetSize (and proportionally sheds zero-size entries down to targetZeroCount),
// protecting protectedNodeID when specified by the foreground mutation caller,
// then performs lossless arena and map compaction if fragmentation warrants it.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix[V]) shedAndCompactLocked(evictQ *evictCallbackQueue[V], targetSize uint64, retention float64, protectedNodeID uint32) []V {
	if c.currentSize <= targetSize && c.zeroSizeCount == 0 && retention > 0.0 {
		c.autoCompactTier2OnlyLocked(protectedNodeID)
		return nil
	}
	autoCompactID := protectedNodeID
	if protectedNodeID == foregroundNoProtect || (protectedNodeID != nilNode && !c.nodes[protectedNodeID].hasValue) {
		protectedNodeID = nilNode
	}

	var protectedSize uint64
	hasProtected := protectedNodeID != nilNode
	if hasProtected {
		protectedSize = c.nodes[protectedNodeID].size
	}
	effectiveTarget, targetLen, targetZeroCount, unprotectedZeroTarget := c.computeShedTargets(targetSize, retention, c.len, hasProtected, protectedSize)

	needFullFlush := retention == 0.0
	var evicted []V
	for victimID := c.tail; victimID != nilNode; {
		needByteShed, needZeroShed := c.shouldContinueShedding(c.currentSize, effectiveTarget, needFullFlush, targetZeroCount, c.len, targetLen)
		if !needByteShed && !needZeroShed {
			break
		}
		nextVictimID := c.nodes[victimID].prev
		if c.shouldEvictShedVictim(victimID == protectedNodeID, c.nodes[victimID].size, needFullFlush, needByteShed, targetZeroCount) {
			if val, ok := c.eraseInternal(evictQ, victimID, EvictionReasonPressure); ok {
				evicted = append(evicted, val)
			}
		}
		victimID = nextVictimID
	}

	c.completeShedAndCompactLocked(len(evicted), retention, !hasProtected || effectiveTarget <= targetSize, unprotectedZeroTarget, targetLen, autoCompactID)
	return evicted
}

// maybeReclaimUnderPressureLocked evaluates the sampled pressure against configured thresholds:
//   - If pressure >= EvictionThreshold (Tier 2 Critical Pressure): evict from LRU tail down to
//     c.maxSize * EvictionRetentionRatio (preserving protectedNodeID if live), then compact if needed.
//   - Else if pressure >= CompactionThreshold (Tier 1 Moderate Pressure): perform lossless arena and map compaction.
//
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix[V]) maybeReclaimUnderPressureLocked(evictQ *evictCallbackQueue[V], pressure float64, protectedNodeID uint32) []V {
	if pressure >= c.options.EvictionThreshold {
		if c.isSamplingGoroutine() {
			return nil
		}
		retention := c.options.EvictionRetentionRatio
		targetSize := c.tier2TargetSize(c.maxSize)
		return c.shedAndCompactLocked(evictQ, targetSize, retention, protectedNodeID)
	}
	c.resetZeroWatermarkBelowTier2(pressure)
	if pressure >= c.options.CompactionThreshold && c.shouldAutoCompactLocked(protectedNodeID) && !c.isSamplingGoroutine() && c.compactDataStructuresLocked() {
		c.compactionsPressureTier1++
		c.markReclaimedLocked()
	}
	return nil
}

func (c *arenaRadix[V]) recordNodeMapInsertLocked(keyHash uint64, nodeID uint32) {
	c.promoteCollisionPeerLocked(keyHash, nodeID)
}

func (c *arenaRadix[V]) promoteCollisionPeerLocked(keyHash uint64, nodeID uint32) {
	prevID, ok := c.nodeMap[keyHash]
	if !ok {
		c.removeCollisionPeerLocked(keyHash, nodeID)
		c.nodeMap[keyHash] = nodeID
		return
	}
	if prevID == nodeID {
		return
	}
	prevValid := prevID < uint32(len(c.nodes)) && c.nodes[prevID].hasValue && c.hashNodeKey(prevID) == keyHash
	peers := c.collisionPeers[keyHash]
	idx := slices.Index(peers, nodeID)
	switch {
	case idx >= 0 && prevValid:
		peers[idx] = prevID
	case idx >= 0 && !prevValid:
		c.removeCollisionPeerLocked(keyHash, nodeID)
	case idx < 0 && prevValid:
		if c.collisionPeers == nil {
			c.collisionPeers = make(map[uint64][]uint32)
		}
		c.collisionPeers[keyHash] = append(peers, prevID)
		c.collisionCount++
	}
	c.nodeMap[keyHash] = nodeID
}

func (c *arenaRadix[V]) popValidCollisionPeerLocked(hash uint64, excludeID uint32) (uint32, bool) {
	if c.collisionPeers == nil {
		return nilNode, false
	}
	peers := c.collisionPeers[hash]
	for len(peers) > 0 {
		last := peers[len(peers)-1]
		peers = peers[:len(peers)-1]
		if c.collisionCount > 0 {
			c.collisionCount--
		}
		if last != excludeID && last < uint32(len(c.nodes)) && c.nodes[last].hasValue && c.hashNodeKey(last) == hash {
			if len(peers) == 0 {
				delete(c.collisionPeers, hash)
				if len(c.collisionPeers) == 0 {
					c.collisionPeers = nil
				}
			} else {
				c.collisionPeers[hash] = peers
			}
			return last, true
		}
	}
	delete(c.collisionPeers, hash)
	if len(c.collisionPeers) == 0 {
		c.collisionPeers = nil
		c.collisionCount = 0
	}
	return nilNode, false
}

func (c *arenaRadix[V]) removeCollisionPeerLocked(hash uint64, nodeID uint32) {
	if c.collisionPeers == nil {
		return
	}
	peers := c.collisionPeers[hash]
	idx := slices.Index(peers, nodeID)
	if idx < 0 {
		return
	}
	peers = slices.Delete(peers, idx, idx+1)
	if len(peers) == 0 {
		delete(c.collisionPeers, hash)
		if len(c.collisionPeers) == 0 {
			c.collisionPeers = nil
		}
	} else {
		c.collisionPeers[hash] = peers
	}
	if c.collisionCount > 0 {
		c.collisionCount--
	}
}

func (c *arenaRadix[V]) removeOrPromoteNodeMapLocked(hash uint64, nodeID uint32) {
	mappedID, ok := c.nodeMap[hash]
	if !ok {
		c.removeCollisionPeerLocked(hash, nodeID)
		return
	}
	if mappedID != nodeID {
		c.removeCollisionPeerLocked(hash, nodeID)
		return
	}
	if peerID, found := c.popValidCollisionPeerLocked(hash, nodeID); found {
		c.nodeMap[hash] = peerID
		return
	}
	delete(c.nodeMap, hash)
	if c.collisionCount > 0 && len(c.collisionPeers) == 0 {
		c.collisionCount = 0
	}
}
