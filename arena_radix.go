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
	"math"
	"strings"
	"sync"
)

// nilNode represents the sentinel null reference for 32-bit node indices.
const nilNode uint32 = math.MaxUint32

// arenaRadixNode represents a node in the arena-backed radix tree.
// It uses a Left-Child Right-Sibling (LCRS) tree representation and 32-bit indices
// within a contiguous slice to eliminate per-node heap allocations and internal tree pointer
// traversal during GC cycles, while stored values and prefixes retain standard Go GC properties.
type arenaRadixNode[V any] struct {
	prefix   string // Edge label component
	value    V      // Value stored at this node
	size     uint64 // Tracked size of the value stored at this node
	parent   uint32 // Arena slice index of parent node
	child    uint32 // Arena slice index of first child (LCRS)
	sibling  uint32 // Arena slice index of next sibling (LCRS)
	prev     uint32 // Arena slice index of previous node in intrusive LRU list
	next     uint32 // Arena slice index of next node in intrusive LRU list (or free-list)
	hasValue bool   // True if this node stores a live cache entry value
}

// arenaRadix implements the Cache[V] and PressureAwareCache[V] interfaces using a contiguous arena-backed
// radix tree coupled with an intrusive 32-bit doubly-linked LRU list and an O(1) FNV-1a hash lookup accelerator.
type arenaRadix[V any] struct {
	maxSize     uint64
	currentSize uint64
	mu          sync.RWMutex

	nodes     []arenaRadixNode[V]
	freeHead  uint32
	freeCount uint32

	nodeMap      map[uint64]uint32
	nodeMapDirty bool

	root uint32

	head uint32
	tail uint32

	len int

	weigher func(key string, value V) uint64

	pressureState
}

// FNV-1a 64-bit hashing constants.
const (
	offset64 uint64 = 14695981039346656037
	prime64  uint64 = 1099511628211
)

// hashString computes the 64-bit FNV-1a hash of string s.
func hashString(s string) uint64 {
	return hashStringCont(offset64, s)
}

// hashStringCont continues a 64-bit FNV-1a hash calculation from h over string s.
func hashStringCont(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// hashNodeKey computes the 64-bit FNV-1a hash of the full key for nodeID
// by walking the ancestor chain. Uses a 64-level stack buffer for common depths
// and seamlessly handles arbitrary depths > 64.
func (c *arenaRadix[V]) hashNodeKey(nodeID uint32) uint64 {
	var stackBuf [64]uint32
	path := stackBuf[:0]
	for curr := nodeID; curr != c.root && curr != nilNode; curr = c.nodes[curr].parent {
		path = append(path, curr)
	}
	h := offset64
	for i := len(path) - 1; i >= 0; i-- {
		h = hashStringCont(h, c.nodes[path[i]].prefix)
	}
	return h
}

// allocateNode allocates a node index from the free-list or appends to the arena slice.
func (c *arenaRadix[V]) allocateNode() uint32 {
	if c.freeHead != nilNode {
		id := c.freeHead
		c.freeHead = c.nodes[id].next
		c.freeCount--
		c.nodes[id] = arenaRadixNode[V]{
			parent:  nilNode,
			child:   nilNode,
			sibling: nilNode,
			prev:    nilNode,
			next:    nilNode,
		}
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
	n.prefix = ""
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

// getChild finds a child node of nID whose prefix starts with the given byte b.
// Returns nilNode if no matching child is found.
func (c *arenaRadix[V]) getChild(nID uint32, b byte) uint32 {
	n := &c.nodes[nID]
	for curr := n.child; curr != nilNode; curr = c.nodes[curr].sibling {
		if c.nodes[curr].prefix[0] == b {
			return curr
		}
		// Sibling chains are maintained in sorted lexicographical order, so early exit is possible.
		if c.nodes[curr].prefix[0] > b {
			return nilNode
		}
	}
	return nilNode
}

// addChild adds a child node under nID, preserving lexicographical order by prefix[0].
func (c *arenaRadix[V]) addChild(nID uint32, newChildID uint32) {
	c.nodes[newChildID].parent = nID
	pcurr := &c.nodes[nID].child
	for *pcurr != nilNode && c.nodes[*pcurr].prefix[0] < c.nodes[newChildID].prefix[0] {
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

// insertNode inserts key-value into the radix trie and returns the node index.
func (c *arenaRadix[V]) insertNode(key string, value V) uint32 {
	nodeID := c.root
	search := key

	for {
		if len(search) == 0 {
			c.nodes[nodeID].value = value
			c.nodes[nodeID].hasValue = true
			return nodeID
		}

		childID := c.getChild(nodeID, search[0])
		if childID == nilNode {
			newLeafID := c.allocateNode()
			c.nodes[newLeafID].prefix = clonePrefix(search)
			c.nodes[newLeafID].value = value
			c.nodes[newLeafID].hasValue = true
			c.addChild(nodeID, newLeafID)
			return newLeafID
		}

		lcp := longestCommonPrefix(search, c.nodes[childID].prefix)

		if lcp == len(c.nodes[childID].prefix) {
			search = search[lcp:]
			nodeID = childID
			continue
		}

		// Clone both split prefix halves so surviving intermediate routing nodes never pin
		// the underlying backing arrays of large evicted leaf keys.
		oldPrefix := c.nodes[childID].prefix
		splitNodeID := c.allocateNode()
		c.nodes[splitNodeID].prefix = clonePrefix(oldPrefix[:lcp])
		c.nodes[splitNodeID].parent = nodeID

		c.replaceChild(nodeID, childID, splitNodeID)

		c.nodes[childID].prefix = clonePrefix(oldPrefix[lcp:])
		c.addChild(splitNodeID, childID)

		if lcp == len(search) {
			c.nodes[splitNodeID].value = value
			c.nodes[splitNodeID].hasValue = true
			return splitNodeID
		}

		newLeafID := c.allocateNode()
		c.nodes[newLeafID].prefix = clonePrefix(search[lcp:])
		c.nodes[newLeafID].value = value
		c.nodes[newLeafID].hasValue = true
		c.addChild(splitNodeID, newLeafID)
		return newLeafID
	}
}

// verifyKey validates that nodeID represents key by walking upwards via parent pointers with zero heap allocations.
func (c *arenaRadix[V]) verifyKey(nodeID uint32, key string) bool {
	curr := nodeID
	end := len(key)

	for curr != c.root && curr != nilNode {
		prefix := c.nodes[curr].prefix
		prefixLen := len(prefix)

		if end < prefixLen {
			return false
		}
		if key[end-prefixLen:end] != prefix {
			return false
		}
		end -= prefixLen
		curr = c.nodes[curr].parent
	}

	return end == 0 && curr == c.root
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

		prefix := c.nodes[childID].prefix
		if !strings.HasPrefix(search, prefix) {
			return nilNode, false
		}

		search = search[len(prefix):]
		curr = childID
	}

	return nilNode, false
}

// getNodeKey finds a value-bearing node index for key using O(1) FNV-1a hash lookup with zero-alloc verification,
// falling back to top-down trie traversal only when a hash collision or dirty nodeMap state exists.
// It does not mutate c.nodeMap, making it safe under RLock.
func (c *arenaRadix[V]) getNodeKey(key string) (uint32, bool) {
	return c.lookupNodeKeyWithHash(key, hashString(key))
}

// lookupNodeKeyWithHash finds a value-bearing node index for key using a precomputed FNV-1a hash
// without mutating c.nodeMap.
func (c *arenaRadix[V]) lookupNodeKeyWithHash(key string, keyHash uint64) (uint32, bool) {
	nodeID, ok := c.nodeMap[keyHash]
	if ok {
		if nodeID < uint32(len(c.nodes)) && c.nodes[nodeID].hasValue && c.verifyKey(nodeID, key) {
			return nodeID, true
		}
	} else if len(c.nodeMap) == c.len {
		// By Invariant 6 and the Pigeonhole Principle, when len(c.nodeMap) == c.len,
		// there is an exact 1:1 bijection between c.nodeMap and all live value-bearing nodes,
		// so a map miss is a guaranteed cache miss.
		return nilNode, false
	}
	return c.findNodeByTrieWalk(key)
}

// getNodeKeyWithHash finds a value-bearing node index for key under exclusive write lock,
// healing c.nodeMap[keyHash] if resolved via the slow-path trie walk.
func (c *arenaRadix[V]) getNodeKeyWithHash(key string, keyHash uint64) (uint32, bool) {
	nodeID, ok := c.nodeMap[keyHash]
	if ok {
		if nodeID < uint32(len(c.nodes)) && c.nodes[nodeID].hasValue && c.verifyKey(nodeID, key) {
			return nodeID, true
		}
	} else if len(c.nodeMap) == c.len {
		// By Invariant 6 and the Pigeonhole Principle, when len(c.nodeMap) == c.len,
		// there is an exact 1:1 bijection between c.nodeMap and all live value-bearing nodes,
		// so a map miss is a guaranteed cache miss.
		return nilNode, false
	}

	curr, found := c.findNodeByTrieWalk(key)
	if found {
		c.nodeMap[keyHash] = curr
	}
	return curr, found
}

// deleteNode clears the value at nodeID and compresses the parent path if needed.
func (c *arenaRadix[V]) deleteNode(nodeID uint32) {
	if nodeID == nilNode || !c.nodes[nodeID].hasValue {
		return
	}

	var zero V
	c.nodes[nodeID].value = zero
	c.nodes[nodeID].hasValue = false
	c.compressPathUpwards(nodeID)
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
			c.nodes[onlyChildID].prefix = c.nodes[currID].prefix + c.nodes[onlyChildID].prefix
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
	prev := c.nodes[nodeID].prev
	next := c.nodes[nodeID].next

	if prev != nilNode {
		c.nodes[prev].next = next
	}
	if next != nilNode {
		c.nodes[next].prev = prev
	}
	if c.tail == nodeID {
		c.tail = prev
	}
	c.nodes[nodeID].prev = nilNode
	c.nodes[nodeID].next = c.head
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
	if c.head != nodeID && c.nodes[nodeID].prev == nilNode {
		return
	}
	prev := c.nodes[nodeID].prev
	next := c.nodes[nodeID].next

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
	c.nodes[nodeID].prev = nilNode
	c.nodes[nodeID].next = nilNode
	c.len--
}

// evictOne removes and returns the least recently used entry (tail) from the cache.
func (c *arenaRadix[V]) evictOne() (V, bool) {
	nodeID := c.tail
	if nodeID == nilNode {
		var zero V
		return zero, false
	}

	return c.eraseInternal(nodeID)
}

const foregroundNoProtect uint32 = nilNode - 1

func (c *arenaRadix[V]) isDirtyLocked() bool {
	return c.freeHead != nilNode || len(c.nodes) < cap(c.nodes) || c.nodeMapDirty
}

// eraseInternal handles unlinking from LRU, cleaning up nodeMap, and deleting from the tree.
func (c *arenaRadix[V]) eraseInternal(nodeID uint32) (V, bool) {
	return c.eraseInternalWithHash(nodeID, c.hashNodeKey(nodeID))
}

func (c *arenaRadix[V]) eraseInternalWithHash(nodeID uint32, hash uint64) (V, bool) {
	if nodeID == nilNode || !c.nodes[nodeID].hasValue {
		var zero V
		return zero, false
	}
	deletedEntry := c.nodes[nodeID].value
	c.onEntryDeleted(c.nodes[nodeID].size)
	c.nodeMapDirty = true
	c.currentSize -= c.nodes[nodeID].size
	c.nodes[nodeID].size = 0

	if mappedID, ok := c.nodeMap[hash]; ok && mappedID == nodeID {
		delete(c.nodeMap, hash)
	}

	c.remove(nodeID)
	c.deleteNode(nodeID)

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
	if c.nodeMap != nil && c.peakEntryLen <= 8 && c.deletedSinceCompact < 64 {
		clear(c.nodeMap)
	} else {
		c.nodeMap = make(map[uint64]uint32)
	}
	c.nodeMapDirty = false
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
	if c.freeHead != nilNode && c.peakEntryLen > 8 && len(c.nodes) > 8 && c.freeCount >= 2 && uint64(c.freeCount)*4 >= uint64(len(c.nodes)) {
		return true
	}
	return c.shouldAutoCompactEntryCounts(c.nodeMapDirty, false, c.len)
}

// compactDataStructuresLocked performs the physical slice and map compaction without updating
// the reclamation epoch, returning true if any backing structure was reallocated.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix[V]) compactDataStructuresLocked() bool {
	// Fast-path: when no nodes are on the free-list, node indices are already contiguous [0..len-1].
	if c.freeHead == nilNode {
		if len(c.nodes) == cap(c.nodes) && !c.nodeMapDirty {
			return false
		}
		if len(c.nodes) < cap(c.nodes) {
			newNodes := make([]arenaRadixNode[V], len(c.nodes))
			copy(newNodes, c.nodes)
			c.nodes = newNodes
		}
		if c.nodeMapDirty {
			newNodeMap := make(map[uint64]uint32, c.len)
			for id := uint32(0); id < uint32(len(c.nodes)); id++ {
				if c.nodes[id].hasValue {
					newNodeMap[c.hashNodeKey(id)] = id
				}
			}
			c.nodeMap = newNodeMap
			c.nodeMapDirty = false
		}
		c.onCompacted(c.len)
		return true
	}

	oldLen := uint32(len(c.nodes))
	oldToNew := make([]uint32, oldLen)

	var liveCount uint32
	for oldID := uint32(0); oldID < oldLen; oldID++ {
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
	for oldID := uint32(0); oldID < oldLen; oldID++ {
		newID := oldToNew[oldID]
		if newID == nilNode {
			continue
		}
		oldNode := &c.nodes[oldID]
		if oldNode.hasValue {
			newNodeMap[c.hashNodeKey(oldID)] = newID
		}
		newNodes[newID] = arenaRadixNode[V]{
			prefix:   oldNode.prefix,
			value:    oldNode.value,
			size:     oldNode.size,
			parent:   remap(oldNode.parent),
			child:    remap(oldNode.child),
			sibling:  remap(oldNode.sibling),
			prev:     remap(oldNode.prev),
			next:     remap(oldNode.next),
			hasValue: oldNode.hasValue,
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
		c.markReclaimedLocked()
	}
}

// shedAndCompactLocked evicts least-recently-used entries strictly from c.tail in a single O(N) pass
// until c.currentSize <= targetSize (and proportionally sheds zero-size entries down to targetZeroCount),
// protecting protectedNodeID only when it resides at the MRU head (c.head),
// then performs lossless arena and map compaction if fragmentation warrants it.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix[V]) shedAndCompactLocked(targetSize uint64, retention float64, protectedNodeID uint32) []V {
	autoCompactID := protectedNodeID
	if protectedNodeID == foregroundNoProtect || (protectedNodeID != nilNode && (protectedNodeID != c.head || c.nodes[protectedNodeID].prev != nilNode || !c.nodes[protectedNodeID].hasValue)) {
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
	victimID := c.tail
	for victimID != nilNode {
		needByteShed := c.currentSize > effectiveTarget || needFullFlush
		needZeroShed := !needFullFlush && c.zeroSizeCount > targetZeroCount && c.len > targetLen
		if !needByteShed && !needZeroShed {
			break
		}
		switch {
		case needFullFlush || (needByteShed && c.zeroSizeCount > targetZeroCount):
			for victimID != nilNode && victimID == protectedNodeID {
				victimID = c.nodes[victimID].prev
			}
		case needByteShed:
			for victimID != nilNode && (victimID == protectedNodeID || c.nodes[victimID].size == 0) {
				victimID = c.nodes[victimID].prev
			}
		default:
			for victimID != nilNode && (victimID == protectedNodeID || c.nodes[victimID].size > 0) {
				victimID = c.nodes[victimID].prev
			}
		}
		if victimID == nilNode {
			break
		}
		nextVictimID := c.nodes[victimID].prev
		if val, ok := c.eraseInternal(victimID); ok {
			evicted = append(evicted, val)
		}
		victimID = nextVictimID
	}

	if len(evicted) > 0 && c.len == 0 {
		c.resetEmptyArenaLocked()
		return evicted
	}

	if len(evicted) > 0 {
		c.updateZeroWatermarkAfterShed(retention, c.len, !hasProtected || effectiveTarget <= targetSize, unprotectedZeroTarget, targetLen)
	}
	compacted := false
	if c.shouldAutoCompactLocked(autoCompactID) {
		compacted = c.compactDataStructuresLocked()
	}
	if len(evicted) > 0 || compacted {
		c.markReclaimedLocked()
	}
	return evicted
}

// maybeReclaimUnderPressureLocked evaluates the sampled pressure against configured thresholds:
//   - If pressure >= EvictionThreshold (Tier 2 Critical Pressure): evict from LRU tail down to
//     c.maxSize * EvictionRetentionRatio (preserving protectedNodeID if at MRU head), then compact if needed.
//   - Else if pressure >= CompactionThreshold (Tier 1 Moderate Pressure): perform lossless arena and map compaction.
//
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix[V]) maybeReclaimUnderPressureLocked(pressure float64, protectedNodeID uint32) []V {
	if c.isSamplingGoroutine() {
		return nil
	}
	var evicted []V
	if pressure >= c.options.EvictionThreshold {
		retention := c.options.EvictionRetentionRatio
		targetSize := computeTargetSize(c.maxSize, retention)
		evicted = c.shedAndCompactLocked(targetSize, retention, protectedNodeID)
	} else {
		c.resetZeroWatermarkBelowTier2(pressure)
		if pressure >= c.options.CompactionThreshold {
			if c.shouldAutoCompactLocked(protectedNodeID) {
				c.compactLocked()
			}
		}
	}
	return evicted
}
