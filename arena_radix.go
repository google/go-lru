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
	"runtime"
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

	nodeMap            map[uint64]uint32
	expectedNodeMapLen int
	nodeMapDirty       bool

	root uint32

	head uint32
	tail uint32

	len                    int
	zeroSizeCount          int
	lastReclaimedZeroCount int

	pressureWriteMu         sync.Mutex
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

var goroutineIDBufPool = sync.Pool{
	New: func() any {
		return new([64]byte)
	},
}

func currentGoroutineID() uint64 {
	bufPtr := goroutineIDBufPool.Get().(*[64]byte)
	n := runtime.Stack(bufPtr[:], false)
	const prefix = "goroutine "
	var id uint64
	if n > len(prefix) {
		for i := len(prefix); i < n; i++ {
			b := bufPtr[i]
			if b < '0' || b > '9' {
				break
			}
			id = id*10 + uint64(b-'0')
		}
	}
	goroutineIDBufPool.Put(bufPtr)
	return id
}

var byteStrings [256]string

func init() {
	for i := range 256 {
		byteStrings[i] = string(byte(i))
	}
}

// clonePrefix returns an independent copy of s that never shares s's underlying backing array,
// using preallocated 1-byte strings for single-character prefixes to avoid heap allocations.
func clonePrefix(s string) string {
	switch len(s) {
	case 0:
		return ""
	case 1:
		return byteStrings[s[0]]
	default:
		return strings.Clone(s)
	}
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
func (c *arenaRadix) hashNodeKey(nodeID uint32) uint64 {
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
	if id >= foregroundNoProtect {
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
		c.nodes[newChildID].parent = nID
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
			c.nodes[newLeafID].prefix = clonePrefix(search)
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
			oldValue := c.nodes[splitNodeID].value
			c.nodes[splitNodeID].value = value
			return splitNodeID, oldValue
		}

		newLeafID := c.allocateNode()
		c.nodes[newLeafID].prefix = clonePrefix(search[lcp:])
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
// falling back to top-down trie traversal only when a hash collision or dirty nodeMap state exists.
func (c *arenaRadix) getNodeKey(key string) (uint32, bool) {
	return c.getNodeKeyWithHash(key, hashString(key))
}

func (c *arenaRadix) getNodeKeyWithHash(key string, keyHash uint64) (uint32, bool) {
	nodeID, ok := c.nodeMap[keyHash]
	if ok {
		if nodeID < uint32(len(c.nodes)) && c.nodes[nodeID].value != nil && c.verifyKey(nodeID, key) {
			return nodeID, true
		}
	} else if len(c.nodeMap) == c.len {
		// By Invariant 6 and the Pigeonhole Principle, when len(c.nodeMap) == c.len,
		// there is an exact 1:1 bijection between c.nodeMap and all live value-bearing nodes,
		// so a map miss is a guaranteed cache miss.
		return nilNode, false
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

const foregroundNoProtect uint32 = nilNode - 1

// eraseInternal handles unlinking from LRU, cleaning up nodeMap, and deleting from the tree.
func (c *arenaRadix) eraseInternal(nodeID uint32) ValueType {
	deletedEntry := c.nodes[nodeID].value
	if deletedEntry != nil && c.nodes[nodeID].size == 0 && c.zeroSizeCount > 0 {
		c.zeroSizeCount--
		if c.zeroSizeCount < c.lastReclaimedZeroCount {
			c.lastReclaimedZeroCount = c.zeroSizeCount
		}
	}
	c.currentSize -= c.nodes[nodeID].size
	c.nodes[nodeID].size = 0

	// Prevent hash collision cross-deletions with 0 heap string allocations.
	hash := c.hashNodeKey(nodeID)
	if mappedID, ok := c.nodeMap[hash]; ok && mappedID == nodeID {
		delete(c.nodeMap, hash)
		c.nodeMapDirty = true
		if c.expectedNodeMapLen > 0 {
			c.expectedNodeMapLen--
		}
	}

	c.remove(nodeID)
	c.deleteNode(nodeID)

	return deletedEntry
}

func (c *arenaRadix) hasOverflowSamplingGID(gid uint64) bool {
	if gid == 0 || c.overflowSamplingCount.Load() <= 0 {
		return false
	}
	_, ok := c.overflowSamplingGIDs.Load(gid)
	return ok
}

func (c *arenaRadix) isCurrentGoroutineSampling(gid uint64) bool {
	if gid == 0 {
		return false
	}
	return c.samplingGID.Load() == gid ||
		c.fallbackGID.Load() == gid ||
		c.hasOverflowSamplingGID(gid)
}

func (c *arenaRadix) isSamplingGoroutine() bool {
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

// markReclaimedLocked invalidates cached pre-reclamation pressure and increments reclaimEpoch.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix) markReclaimedLocked() {
	c.pressureWriteMu.Lock()
	defer c.pressureWriteMu.Unlock()
	if !c.isSamplingGoroutine() {
		c.reclaimEpoch.Add(1)
	}
	c.pressureNeedsRefresh.Store(true)
	c.pressureInitialized.Store(false)
	c.lastSampledInitialized.Store(false)
	c.cachedPressureBits.Store(0)
	c.lastSampledPressureBits.Store(0)
}

func (c *arenaRadix) hasElevatedPressureToInvalidate(pressure float64) bool {
	thresh := c.options.CompactionThreshold
	return pressure >= thresh ||
		math.Float64frombits(c.cachedPressureBits.Load()) >= thresh ||
		math.Float64frombits(c.lastSampledPressureBits.Load()) >= thresh ||
		c.pressureNeedsRefresh.Load() ||
		c.samplingPressure.Load() ||
		c.fallbackSampling.Load() ||
		c.overflowSamplingCount.Load() > 0
}

// resetEmptyArenaLocked releases peak arena slice and hash-map allocations when the cache transitions to empty.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix) resetEmptyArenaLocked() {
	c.nodes = nil
	c.freeHead = nilNode
	c.freeCount = 0
	c.root = c.allocateNode()
	c.head = nilNode
	c.tail = nilNode
	c.currentSize = 0
	c.len = 0
	c.zeroSizeCount = 0
	c.lastReclaimedZeroCount = 0
	c.nodeMap = make(map[uint64]uint32)
	c.expectedNodeMapLen = 0
	c.nodeMapDirty = false
	c.markReclaimedLocked()
}

func (c *arenaRadix) storeSampledPressure(epoch uint64, p float64) {
	c.pressureWriteMu.Lock()
	defer c.pressureWriteMu.Unlock()
	if c.reclaimEpoch.Load() == epoch {
		bits := math.Float64bits(p)
		c.pressureInitialized.Store(false)
		c.lastSampledInitialized.Store(false)
		c.lastSampledEpoch.Store(epoch)
		c.lastSampledPressureBits.Store(bits)
		c.lastSampledInitialized.Store(true)
		c.cachedPressureEpoch.Store(epoch)
		c.cachedPressureBits.Store(bits)
		c.pressureInitialized.Store(true)
		c.pressureNeedsRefresh.Store(false)
	} else {
		c.pressureNeedsRefresh.Store(true)
	}
}

// samplePressureFresh reads normalized memory pressure lock-free outside c.mu.Lock()
// with a goroutine-aware re-entrancy guard that prevents infinite mutual recursion if a
// custom PressureFunc calls back into cache methods, while returning accurate pressure
// readings to concurrent goroutines without fabricating false critical pressure.
func (c *arenaRadix) samplePressureFresh() float64 {
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
	p := c.options.PressureFunc()
	if math.IsNaN(p) || p < 0.0 {
		p = 0.0
	}
	c.storeSampledPressure(epoch, p)
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
	if f >= float64(maxSize) || f >= float64(math.MaxUint64) {
		return maxSize
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
// (protectedNodeID != nilNode) only count recycled free-list fragmentation (>= 25% of len(c.nodes))
// so Go's geometric slice growth capacity slack never triggers O(N^2) compaction thrashing.
func (c *arenaRadix) shouldAutoCompactLocked(protectedNodeID uint32) bool {
	if protectedNodeID == nilNode {
		return c.freeHead != nilNode || len(c.nodes) < cap(c.nodes) || c.nodeMapDirty || len(c.nodeMap) < c.expectedNodeMapLen
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
		if len(c.nodes) == cap(c.nodes) && !c.nodeMapDirty && len(c.nodeMap) >= c.expectedNodeMapLen {
			return
		}
		if len(c.nodes) < cap(c.nodes) {
			newNodes := make([]arenaRadixNode, len(c.nodes))
			copy(newNodes, c.nodes)
			c.nodes = newNodes
		}
		if c.nodeMapDirty || len(c.nodeMap) < c.expectedNodeMapLen {
			newNodeMap := make(map[uint64]uint32, c.len)
			for id := uint32(0); id < uint32(len(c.nodes)); id++ {
				if c.nodes[id].value != nil {
					newNodeMap[c.hashNodeKey(id)] = id
				}
			}
			c.nodeMap = newNodeMap
			c.expectedNodeMapLen = len(newNodeMap)
			c.nodeMapDirty = false
		}
		c.markReclaimedLocked()
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
	c.expectedNodeMapLen = len(newNodeMap)
	c.nodeMapDirty = false
	c.markReclaimedLocked()
}

// shedAndCompactLocked evicts least-recently-used entries strictly from c.tail in a single O(N) pass
// until c.currentSize <= targetSize (and proportionally sheds zero-size entries down to targetZeroCount),
// protecting protectedNodeID only when it resides at the MRU head (c.head),
// then performs lossless arena and map compaction if fragmentation warrants it.
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix) shedAndCompactLocked(targetSize uint64, retention float64, protectedNodeID uint32) []ValueType {
	autoCompactID := protectedNodeID
	if protectedNodeID == foregroundNoProtect || (protectedNodeID != nilNode && (protectedNodeID != c.head || c.nodes[protectedNodeID].prev != nilNode)) {
		protectedNodeID = nilNode
	}

	effectiveTarget := targetSize
	if protectedNodeID != nilNode && retention > 0.0 && c.nodes[protectedNodeID].size > effectiveTarget {
		effectiveTarget = c.nodes[protectedNodeID].size
	}

	targetLen := 0
	targetZeroCount := 0
	if retention > 0.0 {
		if c.len > 0 {
			targetLen = int(float64(c.len) * retention)
			if protectedNodeID != nilNode && targetLen < 1 {
				targetLen = 1
			}
		}
		if c.zeroSizeCount > 0 {
			targetZeroCount = int(float64(c.zeroSizeCount) * retention)
			if protectedNodeID != nilNode && c.nodes[protectedNodeID].size == 0 && targetZeroCount < 1 {
				targetZeroCount = 1
			}
			if c.lastReclaimedZeroCount > targetZeroCount {
				targetZeroCount = c.lastReclaimedZeroCount
			}
		}
	}

	needFullFlush := retention == 0.0
	var evicted []ValueType
	victimID := c.tail
	for victimID != nilNode {
		needByteShed := c.currentSize > effectiveTarget || needFullFlush
		needZeroShed := !needFullFlush && c.zeroSizeCount > targetZeroCount && c.len > targetLen
		if !needByteShed && !needZeroShed {
			break
		}
		switch {
		case needFullFlush || (needByteShed && needZeroShed):
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
		if val := c.eraseInternal(victimID); val != nil {
			evicted = append(evicted, val)
		}
		victimID = nextVictimID
	}

	if c.len == 0 {
		c.resetEmptyArenaLocked()
		return evicted
	}

	if retention > 0.0 && c.zeroSizeCount > 0 {
		c.lastReclaimedZeroCount = c.zeroSizeCount
	} else {
		c.lastReclaimedZeroCount = 0
	}

	c.markReclaimedLocked()
	if c.shouldAutoCompactLocked(autoCompactID) {
		c.compactLocked()
	}
	return evicted
}

// maybeReclaimUnderPressureLocked evaluates the sampled pressure against configured thresholds:
//   - If pressure >= EvictionThreshold (Tier 2 Critical Pressure): evict from LRU tail down to
//     c.maxSize * EvictionRetentionRatio (preserving protectedNodeID if at MRU head), then compact if needed.
//   - Else if pressure >= CompactionThreshold (Tier 1 Moderate Pressure): perform lossless arena and map compaction.
//
// Caller MUST hold c.mu.Lock().
func (c *arenaRadix) maybeReclaimUnderPressureLocked(pressure float64, protectedNodeID uint32) []ValueType {
	autoCompactID := protectedNodeID
	shedProtectedID := protectedNodeID
	if shedProtectedID == foregroundNoProtect || (shedProtectedID != nilNode && (shedProtectedID != c.head || c.nodes[shedProtectedID].prev != nilNode || c.nodes[shedProtectedID].value == nil)) {
		shedProtectedID = nilNode
	}
	epochBefore := c.reclaimEpoch.Load()
	var evicted []ValueType
	if pressure >= c.options.EvictionThreshold {
		retention := c.options.EvictionRetentionRatio
		targetSize := computeTargetSize(c.maxSize, retention)
		targetLen := int(float64(c.len) * retention)
		if shedProtectedID != nilNode && retention > 0.0 && targetLen < 1 {
			targetLen = 1
		}
		targetZeroCount := int(float64(c.zeroSizeCount) * retention)
		if shedProtectedID != nilNode && retention > 0.0 && c.nodes[shedProtectedID].size == 0 && targetZeroCount < 1 {
			targetZeroCount = 1
		}
		if retention > 0.0 && c.lastReclaimedZeroCount > targetZeroCount {
			targetZeroCount = c.lastReclaimedZeroCount
		}
		if c.currentSize > targetSize || (c.zeroSizeCount > targetZeroCount && c.len > targetLen) || (retention == 0.0 && c.tail != nilNode) {
			evicted = c.shedAndCompactLocked(targetSize, retention, autoCompactID)
		} else if c.shouldAutoCompactLocked(autoCompactID) {
			c.compactLocked()
		}
	} else {
		c.lastReclaimedZeroCount = 0
		if pressure >= c.options.CompactionThreshold {
			if c.shouldAutoCompactLocked(autoCompactID) {
				c.compactLocked()
			}
		}
	}
	if autoCompactID != nilNode && autoCompactID != foregroundNoProtect && c.reclaimEpoch.Load() == epochBefore {
		c.pressureWriteMu.Lock()
		if c.reclaimEpoch.Load() == epochBefore {
			c.lastSampledInitialized.Store(false)
			c.lastSampledEpoch.Store(epochBefore)
			c.lastSampledPressureBits.Store(math.Float64bits(pressure))
			c.lastSampledInitialized.Store(true)
		}
		c.pressureWriteMu.Unlock()
	}
	return evicted
}
