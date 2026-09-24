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
	"sync/atomic"
)

// nilNode represents the sentinel null reference for 32-bit node indices.
const nilNode uint32 = math.MaxUint32

// arenaRadixNode represents a node in the arena-backed radix tree.
// It uses a Left-Child Right-Sibling (LCRS) tree representation and 32-bit indices
// within a contiguous slice to eliminate per-node heap allocations and internal tree pointer
// traversal during GC cycles, while stored values and prefixes retain standard Go GC properties.
type arenaRadixNode struct {
	prefix  string    // Edge label component
	value   ValueType // Value stored at this node; nil for intermediate routing nodes
	size    uint64    // Tracked size of the value stored at this node
	parent  uint32    // Arena slice index of parent node
	child   uint32    // Arena slice index of first child (LCRS)
	sibling uint32    // Arena slice index of next sibling (LCRS)
	prev    uint32    // Arena slice index of previous node in intrusive LRU list
	next    uint32    // Arena slice index of next node in intrusive LRU list (or free-list)
}

// arenaRadix implements the Cache and PressureAwareCache interfaces using a contiguous arena-backed
// radix tree coupled with an intrusive 32-bit doubly-linked LRU list and an O(1) FNV-1a hash lookup accelerator.
type arenaRadix struct {
	maxSize     uint64
	currentSize uint64
	mu          sync.RWMutex
	options     Options

	nodes     []arenaRadixNode
	freeHead  uint32
	freeCount uint32

	nodeMap map[uint64]uint32

	root uint32

	head uint32
	tail uint32

	len int

	samplingPressure     atomic.Bool
	pressureNeedsRefresh atomic.Bool
	pressureSampleSeq    atomic.Uint64
	cachedPressureBits   atomic.Uint64
	reclaimEpoch         atomic.Uint64
}

// FNV-1a 64-bit hashing constants.
const (
	offset64 uint64 = 14695981039346656037
	prime64  uint64 = 1099511628211
)

// hashString computes the 64-bit FNV-1a hash of string s.
func hashString(s string) uint64 {
	h := offset64
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// hashNodeKey computes the 64-bit FNV-1a hash of the full key for nodeID
// by walking the ancestor chain with a stack-allocated buffer (0 heap allocations).
func (c *arenaRadix) hashNodeKey(nodeID uint32) uint64 {
	var stackBuf [64]uint32
	path := stackBuf[:0]
	for curr := nodeID; curr != c.root && curr != nilNode; curr = c.nodes[curr].parent {
		path = append(path, curr)
	}
	h := offset64
	for i := len(path) - 1; i >= 0; i-- {
		prefix := c.nodes[path[i]].prefix
		for j := 0; j < len(prefix); j++ {
			h ^= uint64(prefix[j])
			h *= prime64
		}
	}
	return h
}

// longestCommonPrefix finds the length of the longest common prefix of a and b.
func (c *arenaRadix) longestCommonPrefix(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

// allocateNode allocates a node index from the free-list or appends to the arena slice.
func (c *arenaRadix) allocateNode() uint32 {
	if c.freeHead != nilNode {
		id := c.freeHead
		c.freeHead = c.nodes[id].next
		c.freeCount--
		c.nodes[id] = arenaRadixNode{
			parent:  nilNode,
			child:   nilNode,
			sibling: nilNode,
			prev:    nilNode,
			next:    nilNode,
		}
		return id
	}
	id := uint32(len(c.nodes))
	if id >= nilNode {
		panic("arena radix capacity exceeded limit")
	}
	c.nodes = append(c.nodes, arenaRadixNode{
		parent:  nilNode,
		child:   nilNode,
		sibling: nilNode,
		prev:    nilNode,
		next:    nilNode,
	})
	return id
}

// freeNode reclaims a node by clearing its data and linking it to the singly-linked free-list.
func (c *arenaRadix) freeNode(id uint32) {
	n := &c.nodes[id]
	n.prefix = ""
	n.value = nil
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
func (c *arenaRadix) getChild(nID uint32, b byte) uint32 {
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
func (c *arenaRadix) addChild(nID uint32, newChildID uint32) {
	c.nodes[newChildID].parent = nID
	pcurr := &c.nodes[nID].child
	for *pcurr != nilNode && c.nodes[*pcurr].prefix[0] < c.nodes[newChildID].prefix[0] {
		pcurr = &c.nodes[*pcurr].sibling
	}
	c.nodes[newChildID].sibling = *pcurr
	*pcurr = newChildID
}

// removeChild removes childToRemoveID from the sibling chain of nID.
func (c *arenaRadix) removeChild(nID uint32, childToRemoveID uint32) {
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
func (c *arenaRadix) replaceChild(nID uint32, oldChildID uint32, newChildID uint32) {
	for pcurr := &c.nodes[nID].child; *pcurr != nilNode; pcurr = &c.nodes[*pcurr].sibling {
		if *pcurr != oldChildID {
			continue
		}
		c.nodes[newChildID].sibling = c.nodes[oldChildID].sibling
		*pcurr = newChildID
		c.nodes[oldChildID].sibling = nilNode
		c.nodes[oldChildID].parent = nilNode
		return
	}
	panic("replaceChild: requested child not found in sibling list")
}

// insertNode inserts key-value into the radix trie and returns the node index and any previous value.
func (c *arenaRadix) insertNode(key string, value ValueType) (uint32, ValueType) {
	if value == nil {
		return nilNode, nil
	}

	nodeID := c.root
	search := key

	for {
		if len(search) == 0 {
			oldValue := c.nodes[nodeID].value
			c.nodes[nodeID].value = value
			return nodeID, oldValue
		}

		childID := c.getChild(nodeID, search[0])
		if childID == nilNode {
			newLeafID := c.allocateNode()
			c.nodes[newLeafID].prefix = strings.Clone(search)
			c.nodes[newLeafID].value = value
			c.addChild(nodeID, newLeafID)
			return newLeafID, nil
		}

		lcp := c.longestCommonPrefix(search, c.nodes[childID].prefix)

		if lcp == len(c.nodes[childID].prefix) {
			search = search[lcp:]
			nodeID = childID
			continue
		}

		splitNodeID := c.allocateNode()
		c.nodes[splitNodeID].prefix = strings.Clone(c.nodes[childID].prefix[:lcp])
		c.nodes[splitNodeID].parent = nodeID

		c.replaceChild(nodeID, childID, splitNodeID)

		c.nodes[childID].prefix = strings.Clone(c.nodes[childID].prefix[lcp:])
		c.addChild(splitNodeID, childID)

		if lcp == len(search) {
			oldValue := c.nodes[splitNodeID].value
			c.nodes[splitNodeID].value = value
			return splitNodeID, oldValue
		}

		newLeafID := c.allocateNode()
		c.nodes[newLeafID].prefix = strings.Clone(search[lcp:])
		c.nodes[newLeafID].value = value
		c.addChild(splitNodeID, newLeafID)
		return newLeafID, nil
	}
}

// verifyKey validates that nodeID represents key by walking upwards via parent pointers with zero heap allocations.
func (c *arenaRadix) verifyKey(nodeID uint32, key string) bool {
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

// getNodeKey finds a value-bearing node index for key using O(1) FNV-1a hash lookup with zero-alloc verification,
// falling back to top-down trie traversal.
func (c *arenaRadix) getNodeKey(key string) (uint32, bool) {
	nodeID, ok := c.nodeMap[hashString(key)]
	if ok && c.nodes[nodeID].value != nil {
		if c.verifyKey(nodeID, key) {
			return nodeID, true
		}
	}

	curr := c.root
	search := key

	for curr != nilNode {
		if len(search) == 0 {
			if c.nodes[curr].value != nil {
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

// deleteNode clears the value at nodeID and compresses the parent path if needed.
func (c *arenaRadix) deleteNode(nodeID uint32) {
	if nodeID == nilNode || c.nodes[nodeID].value == nil {
		return
	}

	c.nodes[nodeID].value = nil
	c.compressPathUpwards(nodeID)
}

// compressPathUpwards walks up the tree, pruning empty leaf nodes and merging single-child intermediate routing nodes.
func (c *arenaRadix) compressPathUpwards(currID uint32) {
	for currID != nilNode && currID != c.root {
		if c.nodes[currID].value != nil {
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
			currID = parentID
			continue
		}
		break
	}
}

// moveToFront moves an existing node in the LRU list to the MRU position (head).
func (c *arenaRadix) moveToFront(nodeID uint32) {
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
func (c *arenaRadix) pushFront(nodeID uint32) {
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
func (c *arenaRadix) remove(nodeID uint32) {
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
func (c *arenaRadix) evictOne() ValueType {
	nodeID := c.tail
	if nodeID == nilNode {
		return nil
	}

	return c.eraseInternal(nodeID)
}

// eraseInternal handles unlinking from LRU, cleaning up nodeMap, and deleting from the tree.
func (c *arenaRadix) eraseInternal(nodeID uint32) ValueType {
	deletedEntry := c.nodes[nodeID].value
	c.currentSize -= c.nodes[nodeID].size
	c.nodes[nodeID].size = 0

	// Prevent hash collision cross-deletions with 0 heap string allocations.
	hash := c.hashNodeKey(nodeID)
	if c.nodeMap[hash] == nodeID {
		delete(c.nodeMap, hash)
	}

	c.remove(nodeID)
	c.deleteNode(nodeID)

	return deletedEntry
}

// samplePressureFresh reads normalized memory pressure lock-free outside c.mu.Lock()
// with an atomic re-entrancy guard that prevents infinite mutual recursion if a
// custom PressureFunc calls back into cache methods, while returning the latest
// cached pressure reading to concurrent goroutines instead of a false 0.0.
func (c *arenaRadix) samplePressureFresh() float64 {
	if !c.samplingPressure.CompareAndSwap(false, true) {
		return math.Float64frombits(c.cachedPressureBits.Load())
	}
	defer c.samplingPressure.Store(false)

	p := c.options.PressureFunc()
	if math.IsNaN(p) || p < 0.0 {
		p = 0.0
	}
	c.cachedPressureBits.Store(math.Float64bits(p))
	c.pressureNeedsRefresh.Store(false)
	return p
}

// samplePressure returns the current memory pressure for foreground cache operations.
// Custom PressureFunc callbacks are invoked on every call; the default runtime/metrics
// probe is amortized across a 256-operation window (and immediately refreshed after any
// reclamation/compaction cycle) to eliminate runtime.metricsLock contention on hot-path writes.
func (c *arenaRadix) samplePressure() float64 {
	if c.options.hasCustomPressureFunc {
		return c.samplePressureFresh()
	}
	seq := c.pressureSampleSeq.Add(1)
	if (seq&255) == 1 || c.pressureNeedsRefresh.CompareAndSwap(true, false) {
		return c.samplePressureFresh()
	}
	return math.Float64frombits(c.cachedPressureBits.Load())
}

// computeTargetSize safely computes uint64(float64(maxSize) * retention) without
// float64-to-uint64 overflow at math.MaxUint64 or zero-truncation when maxSize == 1 and retention > 0.
func computeTargetSize(maxSize uint64, retention float64) uint64 {
	if retention <= 0.0 {
		return 0
	}
	if retention >= 1.0 {
		return maxSize
	}
	f := float64(maxSize) * retention
	if f >= float64(math.MaxUint64) {
		return math.MaxUint64
	}
	target := uint64(f)
	if target == 0 && maxSize > 0 {
		return 1
	}
	return target
}

// shouldAutoCompactLocked determines whether an automatic compaction should run.
// Explicit EvaluateMemoryPressure calls (protectedNodeID == nilNode) compact whenever any
// free slots, slice slack, or unhealed hash-collision slots exist. Inline foreground mutations
// require at least 25% of arena slots to be on the free-list so O(N) compaction is amortized to O(1) per write.
func (c *arenaRadix) shouldAutoCompactLocked(protectedNodeID uint32) bool {
	if protectedNodeID == nilNode {
		return c.freeHead != nilNode || len(c.nodes) < cap(c.nodes) || len(c.nodeMap) < c.len
	}
	return c.freeHead != nilNode && uint64(c.freeCount)*4 >= uint64(len(c.nodes))
}

// compactLocked performs lossless O(N) compaction of the node arena slice and hash lookup map.
// It eliminates all free-list slots so len(c.nodes) == cap(c.nodes) == liveCount and c.freeHead == nilNode,
// remaps all 8 uint32 index pointers (root, head, tail, parent, child, sibling, prev, next),
// and reallocates c.nodeMap to eliminate Go map bucket slack while preserving 100% of live entries and LRU order.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix) compactLocked() {
	// Fast-path: when no nodes are on the free-list, node indices are already contiguous [0..len-1].
	if c.freeHead == nilNode {
		if len(c.nodes) == cap(c.nodes) && len(c.nodeMap) >= c.len {
			return
		}
		if len(c.nodes) < cap(c.nodes) {
			newNodes := make([]arenaRadixNode, len(c.nodes))
			copy(newNodes, c.nodes)
			c.nodes = newNodes
		}
		if len(c.nodeMap) < c.len {
			newNodeMap := make(map[uint64]uint32, c.len)
			for id := uint32(0); id < uint32(len(c.nodes)); id++ {
				if c.nodes[id].value != nil {
					newNodeMap[c.hashNodeKey(id)] = id
				}
			}
			c.nodeMap = newNodeMap
		}
		c.pressureNeedsRefresh.Store(true)
		c.reclaimEpoch.Add(1)
		return
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
	newNodes := make([]arenaRadixNode, liveCount)
	newNodeMap := make(map[uint64]uint32, c.len)
	for oldID := uint32(0); oldID < oldLen; oldID++ {
		newID := oldToNew[oldID]
		if newID == nilNode {
			continue
		}
		oldNode := &c.nodes[oldID]
		if oldNode.value != nil {
			newNodeMap[c.hashNodeKey(oldID)] = newID
		}
		newNodes[newID] = arenaRadixNode{
			prefix:  oldNode.prefix,
			value:   oldNode.value,
			size:    oldNode.size,
			parent:  remap(oldNode.parent),
			child:   remap(oldNode.child),
			sibling: remap(oldNode.sibling),
			prev:    remap(oldNode.prev),
			next:    remap(oldNode.next),
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
	c.pressureNeedsRefresh.Store(true)
	c.reclaimEpoch.Add(1)
}

// shedAndCompactLocked evicts least-recently-used entries strictly from c.tail until
// c.currentSize <= targetSize (or until the cache is completely empty when retention == 0.0),
// always protecting protectedNodeID (the newly inserted/updated entry during foreground writes),
// then performs lossless arena and map compaction if fragmentation warrants it.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix) shedAndCompactLocked(targetSize uint64, retention float64, protectedNodeID uint32) []ValueType {
	effectiveTarget := targetSize
	if protectedNodeID != nilNode && retention > 0.0 && c.nodes[protectedNodeID].size > effectiveTarget {
		if math.MaxUint64-c.nodes[protectedNodeID].size >= targetSize && c.nodes[protectedNodeID].size+targetSize <= c.maxSize {
			effectiveTarget = c.nodes[protectedNodeID].size + targetSize
		} else {
			effectiveTarget = c.nodes[protectedNodeID].size
		}
	}

	targetLen := 0
	if c.currentSize == 0 && c.len > 0 && retention > 0.0 {
		targetLen = int(float64(c.len) * retention)
		if protectedNodeID != nilNode && targetLen < 1 {
			targetLen = 1
		}
	}

	var evicted []ValueType
	for c.tail != nilNode {
		needByteShed := c.currentSize > effectiveTarget
		needZeroSizeShed := c.currentSize == 0 && c.len > targetLen
		needFullFlush := retention == 0.0
		if !needByteShed && !needZeroSizeShed && !needFullFlush {
			break
		}
		if protectedNodeID != nilNode && c.tail == protectedNodeID {
			break
		}
		if val := c.evictOne(); val != nil {
			evicted = append(evicted, val)
		}
	}

	c.pressureNeedsRefresh.Store(true)
	c.reclaimEpoch.Add(1)
	if c.shouldAutoCompactLocked(protectedNodeID) {
		c.compactLocked()
	}
	return evicted
}

// maybeCompactUnderPressureLocked executes lossless Tier 1 arena/map compaction
// when pressure >= CompactionThreshold and freed slots exceed the fragmentation threshold.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix) maybeCompactUnderPressureLocked(pressure float64) {
	if pressure >= c.options.CompactionThreshold && c.shouldAutoCompactLocked(c.root) {
		c.compactLocked()
	}
}

// maybeReclaimUnderPressureLocked evaluates the sampled pressure against configured thresholds:
//   - If pressure >= EvictionThreshold (Tier 2 Critical Pressure): evict from LRU tail down to
//     c.maxSize * EvictionRetentionRatio (always preserving protectedNodeID), then compact if needed.
//   - Else if pressure >= CompactionThreshold (Tier 1 Moderate Pressure): perform lossless arena and map compaction.
//
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix) maybeReclaimUnderPressureLocked(pressure float64, protectedNodeID uint32) []ValueType {
	if pressure >= c.options.EvictionThreshold {
		retention := c.options.EvictionRetentionRatio
		targetSize := computeTargetSize(c.maxSize, retention)
		if c.currentSize > targetSize || (c.currentSize == 0 && c.len > 0) || (retention == 0.0 && c.tail != nilNode) {
			return c.shedAndCompactLocked(targetSize, retention, protectedNodeID)
		}
		if c.shouldAutoCompactLocked(protectedNodeID) {
			c.compactLocked()
		}
		return nil
	}
	if pressure >= c.options.CompactionThreshold {
		if c.shouldAutoCompactLocked(protectedNodeID) {
			c.compactLocked()
		}
	}
	return nil
}

// e856
