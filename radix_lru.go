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
	"slices"
	"strings"
	"sync"
)

// radixNode represents a node in the compressed radix tree (trie).
// It uses a Left-Child Right-Sibling (LCRS) representation to eliminate dynamic slice/map
// allocations on branching and embeds intrusive doubly-linked list pointers for LRU eviction tracking.
type radixNode[V any] struct {
	prefix  string
	size    uint64
	parent  *radixNode[V]
	child   *radixNode[V]
	sibling *radixNode[V]

	// LRU intrusive doubly-linked list pointers.
	prev     *radixNode[V]
	next     *radixNode[V]
	hasValue bool
	value    V
}

// radixCache encapsulates a compressed prefix tree (radix trie) with an intrusive
// doubly-linked LRU list, implementing the Cache[V] interface.
type radixCache[V any] struct {
	// maxSize specifies the maximum allowable sum of entry weights/sizes.
	maxSize uint64

	// currentSize is the current sum of weights/sizes of all stored entries.
	currentSize uint64

	// root is the root node of the radix trie.
	root *radixNode[V]

	// head and tail point to the MRU (head) and LRU (tail) nodes in the eviction list.
	head *radixNode[V]
	tail *radixNode[V]

	// len is the number of value-bearing entries currently tracked in the LRU list.
	len int

	// weigher computes the weight of a cache entry, or is nil when using the default unit weight of 1.
	weigher func(key string, value V) uint64

	// onEvictValue is the optional value eviction callback configured via WithOnEvictValue.
	onEvictValue func(value V, reason EvictionReason)

	// onEvictEntry is the optional entry eviction callback configured via WithOnEvictEntry.
	onEvictEntry func(key string, value V, reason EvictionReason)

	// mu synchronizes concurrent access to all cache data structures.
	mu sync.RWMutex

	pressureState
}

// NewRadixCache creates a new RadixCache[V] instance bounded by maxSize.
// maxSize must be greater than zero; otherwise NewRadixCache panics.
func NewRadixCache[V any](maxSize uint64, opts ...Option) Cache[V] {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	options := ApplyOptions(opts...)
	options.Backend = BackendRadix
	if options.shouldShard() {
		return newShardedCacheWithOptions[V](maxSize, options.Shards, options)
	}
	return newRadixCacheWithOptions[V](maxSize, options)
}

func newRadixCacheWithOptions[V any](maxSize uint64, options Options) *radixCache[V] {
	if !options.hasShardTier2Target {
		options.hasShardTier2Target = true
		options.shardTier2Target = computeTargetSize(maxSize, options.EvictionRetentionRatio)
	}
	c := &radixCache[V]{
		maxSize:       maxSize,
		root:          &radixNode[V]{},
		weigher:       resolveWeigher[V](options),
		onEvictValue:  resolveOnEvictValue[V](options),
		onEvictEntry:  resolveOnEvictEntry[V](options),
		pressureState: pressureState{options: options},
	}
	c.initCoordinator(c.onEvictValue != nil || c.onEvictEntry != nil)

	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}

	return c
}

func (c *radixCache[V]) weigh(key string, value V) uint64 {
	if c.weigher != nil {
		return c.weigher(key, value)
	}
	return 1
}

func (c *radixCache[V]) notifyEvict(evictQ *evictCallbackQueue[V], key string, value V, size uint64, reason EvictionReason) {
	c.recordEviction(reason, size)
	if c.onEvictValue != nil || c.onEvictEntry != nil {
		evictQ.enqueue(key, value, reason)
	}
}

func (c *radixCache[V]) notifyReplaced(evictQ *evictCallbackQueue[V], key string, oldValue V, oldSize uint64) {
	c.evictionsReplaced++
	c.evictedWeightReplaced += oldSize
	if c.onEvictValue != nil || c.onEvictEntry != nil {
		evictQ.enqueue(key, oldValue, EvictionReasonReplaced)
	}
}

// reconstructKey reconstructs the full key for node by walking the ancestor chain up to c.root.
// It is invoked when onEvictEntry is non-nil (and the caller does not already have the key in hand)
// or during All() / Keys() iteration.
func (c *radixCache[V]) reconstructKey(node *radixNode[V]) string {
	if node == nil || node == c.root {
		return ""
	}
	if node.parent == c.root {
		return node.prefix
	}
	var stackBuf [64]*radixNode[V]
	path := stackBuf[:0]
	totalLen := 0
	for curr := node; curr != nil && curr != c.root; curr = curr.parent {
		path = append(path, curr)
		totalLen += len(curr.prefix)
	}
	var b strings.Builder
	b.Grow(totalLen)
	for _, n := range slices.Backward(path) {
		b.WriteString(n.prefix)
	}
	return b.String()
}

func (c *radixCache[V]) checkLRUNodeLinkInvariants(curr, prevNode *radixNode[V]) {
	if !curr.hasValue {
		panic(fmt.Sprintf("radixCache invariant violation: unexpected hasValue=false in LRU list for prefix '%s'", curr.prefix))
	}
	if curr.prev != prevNode {
		panic(fmt.Sprintf("radixCache invariant violation: corrupt prev pointer in LRU list for prefix '%s'", curr.prefix))
	}
	if prevNode == nil {
		if c.head != curr {
			panic("radixCache invariant violation: head mismatch in LRU list")
		}
	} else if prevNode.next != curr {
		panic(fmt.Sprintf("radixCache invariant violation: corrupt next pointer in LRU list for prefix '%s'", prevNode.prefix))
	}
	if curr.next == nil && c.tail != curr {
		panic("radixCache invariant violation: tail mismatch in LRU list")
	}
}

func (c *radixCache[V]) checkLRUEndpointsInvariants() {
	if c.len == 0 {
		if c.head != nil || c.tail != nil {
			panic("radixCache invariant violation: head or tail is non-nil when len is 0")
		}
	} else if c.head == nil || c.tail == nil {
		panic("radixCache invariant violation: head or tail is nil when len > 0")
	}
}

func (c *radixCache[V]) checkLRUListInvariants() {
	lruCount := 0
	zeroCount := 0
	var sumSize uint64
	var prevNode *radixNode[V]

	for curr := c.head; curr != nil; curr = curr.next {
		lruCount++
		if math.MaxUint64-sumSize < curr.size {
			panic("radixCache invariant violation: sumSize uint64 overflow")
		}
		sumSize += curr.size
		if curr.size == 0 {
			zeroCount++
		}
		c.checkLRUNodeLinkInvariants(curr, prevNode)
		prevNode = curr
	}

	if lruCount != c.len {
		panic(fmt.Sprintf("radixCache invariant violation: LRU list count %d does not match tracked len %d", lruCount, c.len))
	}

	if zeroCount != c.zeroSizeCount {
		panic(fmt.Sprintf("radixCache invariant violation: zeroSizeCount %d does not match live zero-size entries %d", c.zeroSizeCount, zeroCount))
	}

	if sumSize != c.currentSize {
		panic(fmt.Sprintf("radixCache: currentSize drift: currentSize=%d sumSize=%d", c.currentSize, sumSize))
	}

	c.checkLRUEndpointsInvariants()
}

func (c *radixCache[V]) checkRootInvariants() {
	if c.root == nil {
		panic("radixCache invariant violation: root node is nil")
	}
	if c.root.prefix != "" {
		panic("radixCache invariant violation: root node must have empty prefix")
	}
	if c.root.parent != nil {
		panic("radixCache invariant violation: root node must not have a parent")
	}
	if c.root.sibling != nil {
		panic("radixCache invariant violation: root node must not have siblings")
	}
}

func (c *radixCache[V]) checkTreeNodeInvariants(curr *radixNode[V]) {
	if curr.hasValue {
		// A node is verifiably in the LRU list iff it is the head (with nil prev) or its prev's next points back to it.
		inLRU := (c.head == curr && curr.prev == nil) || (curr.prev != nil && curr.prev.next == curr)
		if !inLRU {
			panic(fmt.Sprintf("radixCache invariant violation: node with prefix '%s' has value but is missing from LRU list", curr.prefix))
		}
		return
	}
	if curr.size != 0 {
		panic(fmt.Sprintf("radixCache invariant violation: routing node with prefix '%s' has non-zero size %d", curr.prefix, curr.size))
	}
	if curr.prev != nil || curr.next != nil || c.head == curr || c.tail == curr {
		panic(fmt.Sprintf("radixCache invariant violation: routing node with prefix '%s' has non-nil LRU pointers", curr.prefix))
	}
	if !isZeroValue(&curr.value) {
		panic(fmt.Sprintf("radixCache invariant violation: routing node with prefix '%s' retains non-zero value", curr.prefix))
	}
}

func (c *radixCache[V]) checkChildrenAndCompactness(curr *radixNode[V]) {
	// Validate child pointers and sibling ordering
	var prevSibling *radixNode[V]
	for ch := curr.child; ch != nil; ch = ch.sibling {
		if ch.parent != curr {
			panic(fmt.Sprintf("radixCache invariant violation: child with prefix '%s' has incorrect parent pointer", ch.prefix))
		}
		if len(ch.prefix) == 0 {
			panic("radixCache invariant violation: non-root child node has empty prefix")
		}
		if prevSibling != nil && prevSibling.prefix[0] >= ch.prefix[0] {
			panic(fmt.Sprintf("radixCache invariant violation: siblings not sorted lexicographically ('%s' >= '%s')", prevSibling.prefix, ch.prefix))
		}
		prevSibling = ch
	}

	// Validate tree compactness: non-root routing nodes without a value must have >= 2 children.
	if curr != c.root && !curr.hasValue && (curr.child == nil || curr.child.sibling == nil) {
		panic(fmt.Sprintf("radixCache invariant violation: intermediate routing node with prefix '%s' has fewer than 2 children", curr.prefix))
	}
}

func (c *radixCache[V]) checkTreeInvariants() {
	// Iterative pre-order traversal using parent/sibling pointers (O(1) space).
	// Validates tree integrity, sibling sorted order, parent pointers, compactness,
	// and 1:1 bijection between value-bearing nodes and LRU list elements.
	treeCount := 0
	var treeSumSize uint64
	curr := c.root
	for curr != nil {
		if curr.hasValue {
			treeCount++
			if math.MaxUint64-treeSumSize < curr.size {
				panic("radixCache invariant violation: treeSumSize uint64 overflow")
			}
			treeSumSize += curr.size
		}
		c.checkTreeNodeInvariants(curr)
		c.checkChildrenAndCompactness(curr)

		// Advance to child if present
		if curr.child != nil {
			curr = curr.child
			continue
		}

		// Backtrack up parent chain until finding an ancestor with an unvisited sibling
		for curr != c.root && curr.sibling == nil {
			curr = curr.parent
		}
		if curr == c.root {
			break
		}
		curr = curr.sibling
	}

	if treeCount != c.len {
		panic(fmt.Sprintf("radixCache invariant violation: tree value count %d does not match LRU length %d", treeCount, c.len))
	}

	if treeSumSize != c.currentSize {
		panic(fmt.Sprintf("radixCache: currentSize drift in tree: currentSize=%d treeSumSize=%d", c.currentSize, treeSumSize))
	}
}

func (c *radixCache[V]) checkInvariants() {
	// INVARIANT 1: maxSize > 0
	if c.maxSize == 0 {
		panic("radixCache invariant violation: maxSize must be greater than 0")
	}

	// INVARIANT 2: currentSize <= maxSize
	if c.currentSize > c.maxSize {
		panic(fmt.Sprintf("radixCache invariant violation: currentSize %d exceeds maxSize %d", c.currentSize, c.maxSize))
	}

	// INVARIANT 3: LRU list validation
	c.checkLRUListInvariants()

	// INVARIANT 4: Root structure checks
	c.checkRootInvariants()

	// INVARIANT 5: Tree traversal & bijection checks
	c.checkTreeInvariants()

	c.checkTelemetryInvariants(c.len)
}

// getChild finds a child node whose prefix starts with byte b.
// Takes advantage of sorted sibling order for early-exit termination.
func (n *radixNode[V]) getChild(b byte) *radixNode[V] {
	for curr := n.child; curr != nil; curr = curr.sibling {
		if curr.prefix[0] == b {
			return curr
		}
		// Siblings are arranged in strictly ascending lexicographical order
		if curr.prefix[0] > b {
			return nil
		}
	}
	return nil
}

// addChild adds a child node, maintaining sorted order by the first byte of prefix.
func (n *radixNode[V]) addChild(newChild *radixNode[V]) {
	newChild.parent = n
	pcurr := &n.child
	for *pcurr != nil && (*pcurr).prefix[0] < newChild.prefix[0] {
		pcurr = &(*pcurr).sibling
	}
	newChild.sibling = *pcurr
	*pcurr = newChild
}

// removeChild directly removes a child node by pointer reference from the sibling list.
func (n *radixNode[V]) removeChild(childToRemove *radixNode[V]) {
	for pcurr := &n.child; *pcurr != nil; pcurr = &(*pcurr).sibling {
		if *pcurr == childToRemove {
			*pcurr = childToRemove.sibling
			childToRemove.sibling = nil
			childToRemove.parent = nil
			return
		}
	}
	panic("removeChild: requested child not found in sibling list")
}

// replaceChild finds oldChild in the sibling linked list and substitutes it with newChild,
// preserving the remainder of the sibling chain.
func (n *radixNode[V]) replaceChild(oldChild, newChild *radixNode[V]) {
	for pcurr := &n.child; *pcurr != nil; pcurr = &(*pcurr).sibling {
		if *pcurr == oldChild {
			newChild.parent = n
			newChild.sibling = oldChild.sibling
			*pcurr = newChild

			oldChild.sibling = nil
			oldChild.parent = nil
			return
		}
	}
	panic("replaceChild: requested child not found in sibling list")
}

// insertNode inserts a new key into the radix tree and returns the leaf node.
func (c *radixCache[V]) insertNode(key string, value V) *radixNode[V] {
	node := c.root
	search := key

	for {
		if len(search) == 0 {
			node.value = value
			node.hasValue = true
			return node
		}

		child := node.getChild(search[0])
		if child == nil {
			// Clone the substring to prevent memory leaks from sliced string headers pinning large backing arrays
			newLeaf := &radixNode[V]{
				prefix:   clonePrefix(search),
				value:    value,
				hasValue: true,
			}
			node.addChild(newLeaf)
			return newLeaf
		}

		lcp := longestCommonPrefix(search, child.prefix)

		if lcp == len(child.prefix) {
			search = search[lcp:]
			node = child
			continue
		}

		// Clone both split prefix halves so surviving intermediate routing nodes never pin
		// the underlying backing arrays of large evicted leaf keys.
		oldPrefix := child.prefix
		splitNode := &radixNode[V]{
			prefix: clonePrefix(oldPrefix[:lcp]),
			parent: node,
		}

		node.replaceChild(child, splitNode)

		child.prefix = clonePrefix(oldPrefix[lcp:])
		child.sibling = nil
		splitNode.addChild(child)

		if lcp == len(search) {
			splitNode.value = value
			splitNode.hasValue = true
			return splitNode
		}

		newLeaf := &radixNode[V]{
			prefix:   clonePrefix(search[lcp:]),
			value:    value,
			hasValue: true,
		}
		splitNode.addChild(newLeaf)
		return newLeaf
	}
}

// getNode finds a node by key. Returns the node and true if found with a value.
func (c *radixCache[V]) getNode(key string) (*radixNode[V], bool) {
	node := c.root
	search := key

	for {
		if len(search) == 0 {
			if node.hasValue {
				return node, true
			}
			return nil, false
		}

		child := node.getChild(search[0])
		if child == nil {
			return nil, false
		}

		if strings.HasPrefix(search, child.prefix) {
			search = search[len(child.prefix):]
			node = child
			continue
		}

		return nil, false
	}
}

// compressPathUpwards walks up the tree from curr, pruning empty leaves and compressing single-child routing nodes.
func (c *radixCache[V]) compressPathUpwards(curr *radixNode[V]) {
	for curr != nil && curr != c.root {
		if curr.hasValue {
			break
		}

		if curr.child == nil {
			parent := curr.parent
			parent.removeChild(curr)
			curr.prefix = ""
			curr = parent
			continue
		}

		if curr.child.sibling == nil {
			onlyChild := curr.child
			onlyChild.prefix = concatPrefix(curr.prefix, onlyChild.prefix)
			onlyChild.parent = curr.parent

			curr.parent.replaceChild(curr, onlyChild)
			curr.child = nil
			curr.prefix = ""

			return
		}

		break
	}
}

// --- LRU Linked List Methods ---

// moveToFront moves an existing node in the LRU list to the MRU (head) position.
func (c *radixCache[V]) moveToFront(node *radixNode[V]) {
	if c.head == node {
		return
	}
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	if c.tail == node {
		c.tail = node.prev
	}
	node.prev = nil
	node.next = c.head
	if c.head != nil {
		c.head.prev = node
	}
	c.head = node
}

// pushFront inserts a newly allocated or value-bearing node at the MRU (head) position.
func (c *radixCache[V]) pushFront(node *radixNode[V]) {
	node.prev = nil
	node.next = c.head
	if c.head != nil {
		c.head.prev = node
	}
	c.head = node
	if c.tail == nil {
		c.tail = node
	}
	c.len++
}

// remove unlinks a node from the LRU doubly-linked list.
func (c *radixCache[V]) remove(node *radixNode[V]) {
	if c.head != node && node.prev == nil {
		return
	}
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		c.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.tail = node.prev
	}
	node.prev = nil
	node.next = nil
	c.len--
}

// eraseInternal removes an entry from both the LRU list and radix trie without acquiring locks,
// reconstructing the key before tree compression only when onEvictEntry is configured.
// It returns the deleted value and true if the node held a value.
func (c *radixCache[V]) eraseInternal(evictQ *evictCallbackQueue[V], node *radixNode[V], reason EvictionReason) (V, bool) {
	if node == nil || !node.hasValue {
		var zero V
		return zero, false
	}
	var key string
	if c.onEvictEntry != nil {
		key = c.reconstructKey(node)
	}
	return c.eraseInternalWithKey(evictQ, node, key, reason)
}

func (c *radixCache[V]) eraseInternalWithKey(evictQ *evictCallbackQueue[V], node *radixNode[V], key string, reason EvictionReason) (V, bool) {
	if node == nil || !node.hasValue {
		var zero V
		return zero, false
	}
	deletedEntry := node.value
	evictedSize := node.size
	c.onEntryDeleted(evictedSize)
	c.currentSize -= evictedSize
	node.size = 0

	c.remove(node)
	var zero V
	node.value = zero
	node.hasValue = false
	c.compressPathUpwards(node)

	c.notifyEvict(evictQ, key, deletedEntry, evictedSize, reason)
	return deletedEntry, true
}

func (c *radixCache[V]) clearEmptyTreeStateLocked() {
	c.resetWatermarks()
}

func (c *radixCache[V]) resetEmptyTreeLocked() {
	c.clearEmptyTreeStateLocked()
	c.markReclaimedLocked()
}

// evictOne removes and returns the least recently used entry (c.tail).
func (c *radixCache[V]) evictOne(evictQ *evictCallbackQueue[V]) (V, bool) {
	node := c.tail
	if node == nil {
		var zero V
		return zero, false
	}
	return c.eraseInternal(evictQ, node, EvictionReasonCapacity)
}

// sweepAndUnlink iteratively cleans up all value-bearing nodes in a detached subtree (O(1) space).
func (c *radixCache[V]) sweepAndUnlink(evictQ *evictCallbackQueue[V], node *radixNode[V]) {
	var zero V
	curr := node
	for curr != nil {
		if curr.hasValue {
			var key string
			if c.onEvictEntry != nil {
				key = c.reconstructKey(curr)
			}
			evictedVal := curr.value
			evictedSize := curr.size
			c.onEntryDeleted(evictedSize)
			c.currentSize -= evictedSize
			curr.size = 0
			c.remove(curr)
			curr.value = zero
			curr.hasValue = false
			c.notifyEvict(evictQ, key, evictedVal, evictedSize, EvictionReasonDeleted)
		}

		if curr.child != nil {
			curr = curr.child
			continue
		}

		for curr != node && curr.sibling == nil {
			parent := curr.parent
			curr.prefix = ""
			curr.parent = nil
			curr.child = nil
			curr = parent
		}
		if curr == node {
			curr.prefix = ""
			curr.parent = nil
			curr.child = nil
			curr.sibling = nil
			return
		}
		next := curr.sibling
		curr.prefix = ""
		curr.parent = nil
		curr.child = nil
		curr.sibling = nil
		curr = next
	}
}

// ============================================================================
// Cache Interface Implementation
// ============================================================================

//go:noinline
func (c *radixCache[V]) unlockWithInvariants() {
	defer c.mu.Unlock()
	c.checkInvariants()
}

func (c *radixCache[V]) unlock() {
	if c.options.EnableInvariantChecking {
		c.unlockWithInvariants()
		return
	}
	c.mu.Unlock()
}

//go:noinline
func (c *radixCache[V]) rUnlockWithInvariants() {
	defer c.mu.RUnlock()
	c.checkInvariants()
}

func (c *radixCache[V]) rUnlock() {
	if c.options.EnableInvariantChecking {
		c.rUnlockWithInvariants()
		return
	}
	c.mu.RUnlock()
}

func (c *radixCache[V]) finishDeleteReclaimLocked(evictQ *evictCallbackQueue[V], sizeBefore, sampledEpoch uint64, pressure float64) {
	if c.len == 0 {
		hadSlack := c.hasEmptyDeleteSlack(false)
		if hadSlack {
			c.recordCompactionByPressure(pressure)
		}
		c.clearEmptyTreeStateLocked()
		c.maybeMarkReclaimed(hadSlack || sizeBefore > 0, sampledEpoch, pressure)
		return
	}
	reclaimedSingleSurvivor := false
	if c.shouldReclaimSingleSurvivorOnDelete(c.len, c.deletedSinceCompact > 0, false) {
		if c.compactDataStructuresLocked() {
			c.recordCompactionByPressure(pressure)
		}
		reclaimedSingleSurvivor = true
	}
	if pressure >= c.options.CompactionThreshold || pressure >= c.options.EvictionThreshold {
		c.maybeReclaimUnderPressureLocked(evictQ, pressure, nil, false)
	} else {
		c.resetZeroWatermarkBelowTier2(pressure)
	}
	c.maybeMarkReclaimed(reclaimedSingleSurvivor || c.currentSize < sizeBefore, sampledEpoch, pressure)
}

//go:noinline
func (c *radixCache[V]) finishMutationReclaimLocked(evictQ *evictCallbackQueue[V], evictedValues []V, protectedNode *radixNode[V], reclaimedPre, compactedPre bool, sizeBefore, sampledEpoch uint64, pressure float64) []V {
	evictedByPressure := c.maybeReclaimUnderPressureLocked(evictQ, pressure, protectedNode, false)
	evictedValues = appendEvicted(evictedValues, evictedByPressure)
	netByteReduced := c.currentSize < sizeBefore
	if c.shouldCompactAfterMutation(reclaimedPre, netByteReduced, c.deletedSinceCompact > 0, pressure) {
		if c.compactDataStructuresLocked() && !compactedPre {
			c.recordCompactionByPressure(pressure)
		}
	}
	if c.shouldMarkReclaimedAfterMutation(reclaimedPre, netByteReduced, sampledEpoch, pressure) {
		c.markReclaimedLocked()
	}
	return evictedValues
}

func (c *radixCache[V]) updateExistingOnPutLocked(
	evictQ *evictCallbackQueue[V],
	node *radixNode[V],
	key string,
	value V,
	valueSize, sizeBefore uint64,
	pressure float64,
) (evictedValues []V, reclaimedPrePut bool) {
	oldValue := node.value
	oldSize := node.size
	hadZeroCap := c.shouldEvictOnZeroWeightShrink(oldSize, valueSize, c.maxSize)
	c.onEntrySizeUpdated(oldSize, valueSize)
	c.moveToFront(node)
	c.currentSize -= oldSize
	evictedPrePut := false
	for (valueSize > c.maxSize-c.currentSize || (hadZeroCap && !evictedPrePut)) && c.tail != nil && c.tail != node {
		if evicted, ok := c.evictOne(evictQ); ok {
			evictedValues = append(evictedValues, evicted)
			evictedPrePut = true
		}
	}
	node.value = value
	node.hasValue = true
	node.size = valueSize
	c.currentSize += valueSize
	c.putUpdated++
	c.notifyReplaced(evictQ, key, oldValue, oldSize)
	reclaimedPrePut = c.shouldReclaimSingleSurvivorOnMutation(c.len, c.deletedSinceCompact > 0, false, evictedPrePut, c.currentSize, sizeBefore, pressure)
	return evictedValues, reclaimedPrePut
}

// Put inserts or updates a key-value entry in the cache.
// If the key exists, its value is updated and moved to MRU.
// If capacity is exceeded, excess LRU entries are evicted and returned.
//
// Returns ErrInvalidEntrySize if the entry's weight exceeds maxSize.
func (c *radixCache[V]) Put(key string, value V) ([]V, error) {
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
	evictedPrePut := false

	node, exists := c.getNode(key)
	if exists {
		evictedValues, reclaimedPrePut = c.updateExistingOnPutLocked(&evictQ, node, key, value, valueSize, sizeBefore, pressure)
	} else {
		// Evict from the LRU tail before inserting into the trie to avoid redundant node splits and merges.
		for c.shouldEvictPreInsertOnPut(valueSize, c.maxSize, c.currentSize, c.len, evictedPrePut) && c.tail != nil {
			if evicted, ok := c.evictOne(&evictQ); ok {
				evictedValues = append(evictedValues, evicted)
				evictedPrePut = true
			}
		}
		if c.shouldReclaimEmptyPrePut(c.len, false, evictedPrePut, valueSize, sizeBefore, pressure) {
			c.recordCompactionByPressure(pressure)
			c.clearEmptyTreeStateLocked()
			reclaimedPrePut = true
			compactedPrePut = true
		} else if c.shouldReclaimSingleSurvivorOnMutation(c.len, c.deletedSinceCompact > 0, false, evictedPrePut, c.currentSize+valueSize, sizeBefore, pressure) {
			reclaimedPrePut = true
		}
		node = c.insertNode(key, value)
		node.size = valueSize
		c.pushFront(node)
		c.putInserted++
		c.onEntryPut(c.len, valueSize)
		c.currentSize += valueSize
	}

	if c.needsMutationReclaim(reclaimedPrePut, c.currentSize, sizeBefore, pressure) {
		evictedValues = c.finishMutationReclaimLocked(&evictQ, evictedValues, node, reclaimedPrePut, compactedPrePut, sizeBefore, sampledEpoch, pressure)
	}
	if c.options.EnableInvariantChecking {
		c.unlockWithInvariants()
	} else {
		c.mu.Unlock()
	}
	evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	return evictedValues, nil
}

// Delete removes the entry associated with key, returning its value and true (or the zero value of V and false if not found).
func (c *radixCache[V]) Delete(key string) (value V, ok bool) {
	evictQ := evictCallbackQueue[V]{exactSnap: true}
	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)

	node, found := c.getNode(key)
	if !found {
		c.deleteNotFound++
		c.unlock()
		return value, false
	}

	sizeBefore := c.currentSize
	deleted, erased := c.eraseInternalWithKey(&evictQ, node, key, EvictionReasonDeleted)
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

// Get retrieves the value for key and promotes it to the MRU position.
// Returns the zero value of V and false if key is not found.
func (c *radixCache[V]) Get(key string) (value V, ok bool) {
	c.mu.Lock()

	node, found := c.getNode(key)
	if found {
		c.getHits++
		c.moveToFront(node)
		value = node.value
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

// Peek retrieves the value for key without modifying its LRU position.
// Returns the zero value of V and false if key is not found.
func (c *radixCache[V]) Peek(key string) (value V, ok bool) {
	c.mu.RLock()

	node, found := c.getNode(key)
	if found {
		c.peekHits.Add(1)
		value = node.value
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

func (c *radixCache[V]) canFitGrowthLocked(node *radixNode[V], maxNewer, sizeDelta, avail uint64) bool {
	if sizeDelta <= avail {
		return true
	}
	var newerSize uint64
	headCurr := c.head
	tailCurr := c.tail
	for {
		if headCurr == node {
			return true
		}
		newerSize += headCurr.size
		if newerSize > maxNewer {
			return false
		}
		headCurr = headCurr.next
		if tailCurr == node {
			return false
		}
		avail += tailCurr.size
		if sizeDelta <= avail {
			return true
		}
		tailCurr = tailCurr.prev
	}
}

func (c *radixCache[V]) replaceResizeOrEvictLocked(evictQ *evictCallbackQueue[V], node *radixNode[V], key string, oldSize, newSize, sizeBefore, sampledEpoch uint64, pressure float64) (evictedAny, selfEvicted bool) {
	if newSize > oldSize {
		sizeDelta := newSize - oldSize
		if newSize > c.maxSize || !c.canFitGrowthLocked(node, c.maxSize-newSize, sizeDelta, c.maxSize-c.currentSize) {
			c.replaceSelfEvicted++
			c.eraseInternalWithKey(evictQ, node, key, EvictionReasonCapacity)
			c.finishDeleteReclaimLocked(evictQ, sizeBefore, sampledEpoch, pressure)
			return false, true
		}

		for sizeDelta > c.maxSize-c.currentSize && c.tail != nil && c.tail != node {
			c.evictOne(evictQ)
			evictedAny = true
		}

		c.onEntrySizeUpdated(oldSize, newSize)
		c.currentSize += sizeDelta
		return evictedAny, false
	}
	if c.shouldEvictOnZeroWeightShrink(oldSize, newSize, c.maxSize) && c.tail != nil && c.tail != node {
		c.evictOne(evictQ)
		evictedAny = true
	}
	c.onEntrySizeUpdated(oldSize, newSize)
	c.currentSize -= oldSize - newSize
	return evictedAny, false
}

// Replace updates the value of an existing key and recomputes its weight
// without modifying its LRU order.
// If the entry's new weight exceeds maxSize (or cannot fit alongside entries more recent than node),
// only node itself is evicted without evicting older entries. Otherwise, excess LRU entries are evicted if needed.
//
// Returns ErrEntryNotExist if key does not exist.
func (c *radixCache[V]) Replace(key string, value V) error {
	newSize := c.weigh(key, value)

	var evictQ evictCallbackQueue[V]
	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)

	node, ok := c.getNode(key)
	if !ok {
		c.replaceNotFound++
		c.unlock()
		return ErrEntryNotExist
	}

	oldSize := node.size
	oldValue := node.value
	sizeBefore := c.currentSize
	evictedAny := false

	if newSize != oldSize {
		var selfEvicted bool
		evictedAny, selfEvicted = c.replaceResizeOrEvictLocked(&evictQ, node, key, oldSize, newSize, sizeBefore, sampledEpoch, pressure)
		if selfEvicted {
			c.unlock()
			evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
			return nil
		}
	}

	node.value = value
	node.hasValue = true
	node.size = newSize

	c.replaceUpdated++
	c.notifyReplaced(&evictQ, key, oldValue, oldSize)

	reclaimedPreUpdate := c.shouldReclaimSingleSurvivorOnMutation(c.len, c.deletedSinceCompact > 0, false, evictedAny, c.currentSize, sizeBefore, pressure)
	if c.needsMutationReclaim(reclaimedPreUpdate, c.currentSize, sizeBefore, pressure) {
		c.finishMutationReclaimLocked(&evictQ, nil, node, reclaimedPreUpdate, false, sizeBefore, sampledEpoch, pressure)
	}
	c.unlock()
	evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)

	return nil
}

func (c *radixCache[V]) evictAllForClearLocked(evictQ *evictCallbackQueue[V]) {
	if c.len == 0 {
		return
	}
	if c.onEvictValue == nil && c.onEvictEntry == nil {
		c.evictionsDeleted += uint64(c.len)
		c.evictedWeightDeleted += c.currentSize
		var zero V
		for curr := c.head; curr != nil; {
			next := curr.next
			c.onEntryDeleted(curr.size)
			curr.size = 0
			curr.value = zero
			curr.hasValue = false
			curr.prev = nil
			curr.next = nil
			curr = next
		}
		return
	}
	var zero V
	for curr := c.head; curr != nil; {
		next := curr.next
		var key string
		if c.onEvictEntry != nil {
			key = c.reconstructKey(curr)
		}
		evictedVal := curr.value
		evictedSize := curr.size
		c.onEntryDeleted(evictedSize)
		c.currentSize -= evictedSize
		c.remove(curr)
		curr.value = zero
		curr.hasValue = false
		curr.size = 0
		c.notifyEvict(evictQ, key, evictedVal, evictedSize, EvictionReasonDeleted)
		curr = next
	}
}

func (c *radixCache[V]) isCleanEmptyLocked() bool {
	hadEntries := c.len > 0 || (c.root != nil && (c.root.hasValue || c.root.child != nil))
	hadDirtySlack := c.deletedSinceCompact > 0 || c.peakEntryLen > minPeakSlackEntries
	return !hadEntries && !hadDirtySlack && c.peakEntryLen == 0
}

func (c *radixCache[V]) deleteAllPrefixLocked(evictQ *evictCallbackQueue[V], sampledEpoch uint64, pressure float64) {
	c.deletePrefixExecuted++
	if c.isCleanEmptyLocked() {
		return
	}
	hadDirtySlack := c.deletedSinceCompact > 0 || c.peakEntryLen > minPeakSlackEntries
	hadReclaimable := c.currentSize > 0 || hadDirtySlack
	c.evictAllForClearLocked(evictQ)
	if c.root != nil {
		c.sweepAndUnlink(evictQ, c.root)
	} else {
		c.root = &radixNode[V]{}
	}
	c.head = nil
	c.tail = nil
	c.currentSize = 0
	c.len = 0
	hadCompactionSlack := c.hasEmptyDeleteSlack(false)
	c.clearEmptyTreeStateLocked()
	c.finishClearAllLocked(hadCompactionSlack, hadReclaimable, sampledEpoch, pressure)
}

func (c *radixCache[V]) deleteNonEmptyPrefixLocked(evictQ *evictCallbackQueue[V], prefix string, sampledEpoch uint64, pressure float64) {
	c.deletePrefixExecuted++
	node := c.root
	search := prefix

	for len(search) > 0 {
		child := node.getChild(search[0])
		if child == nil {
			return
		}

		lcp := longestCommonPrefix(search, child.prefix)

		if lcp == len(search) {
			sizeBefore := c.currentSize
			node.removeChild(child)
			child.parent = node
			c.sweepAndUnlink(evictQ, child)
			c.compressPathUpwards(node)
			c.finishDeleteReclaimLocked(evictQ, sizeBefore, sampledEpoch, pressure)
			return
		}

		if lcp == len(child.prefix) {
			search = search[len(child.prefix):]
			node = child
			continue
		}

		return
	}
}

func (c *radixCache[V]) deletePrefixShard(evictQ *evictCallbackQueue[V], prefix string, sample shardSample) (shardSample, bool) {
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

// DeletePrefix deletes all entries whose keys start with prefix.
// Prunes subtrees in O(prefix_length + subtree_size) time and sweeps detached nodes.
func (c *radixCache[V]) DeletePrefix(prefix string) {
	evictQ := evictCallbackQueue[V]{exactSnap: true}
	_, _ = c.deletePrefixShard(&evictQ, prefix, shardSample{})
	evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
}

// All returns an iterator over all key-value pairs in MRU-to-LRU order without modifying recency.
func (c *radixCache[V]) All() iter.Seq2[string, V] {
	return c.all
}

func (c *radixCache[V]) all(yield func(string, V) bool) {
	c.iterateAll(yield)
}

func (c *radixCache[V]) iterateAll(yield func(string, V) bool) bool {
	c.mu.RLock()
	defer c.rUnlock()

	for curr := c.head; curr != nil; curr = curr.next {
		if !yield(c.reconstructKey(curr), curr.value) {
			return false
		}
	}
	return true
}

// Keys returns an iterator over all keys in MRU-to-LRU order without modifying recency.
func (c *radixCache[V]) Keys() iter.Seq[string] {
	return c.keys
}

func (c *radixCache[V]) keys(yield func(string) bool) {
	c.iterateKeys(yield)
}

func (c *radixCache[V]) iterateKeys(yield func(string) bool) bool {
	c.mu.RLock()
	defer c.rUnlock()

	for curr := c.head; curr != nil; curr = curr.next {
		if !yield(c.reconstructKey(curr)) {
			return false
		}
	}
	return true
}

// Values returns an iterator over all values in MRU-to-LRU order without modifying recency.
// It skips key reconstruction completely, executing with zero heap allocations.
func (c *radixCache[V]) Values() iter.Seq[V] {
	return c.values
}

func (c *radixCache[V]) values(yield func(V) bool) {
	c.iterateValues(yield)
}

func (c *radixCache[V]) iterateValues(yield func(V) bool) bool {
	c.mu.RLock()
	defer c.rUnlock()

	for curr := c.head; curr != nil; curr = curr.next {
		if !yield(curr.value) {
			return false
		}
	}
	return true
}

// Stats returns a point-in-time telemetry snapshot of the cache.
func (c *radixCache[V]) Stats() Stats {
	c.mu.RLock()
	st := c.snapshotBaseStats(BackendRadix, c.currentSize, c.maxSize, c.len)
	if c.options.EnableInvariantChecking {
		c.rUnlockWithInvariants()
	} else {
		c.mu.RUnlock()
	}
	return st
}

// Compact satisfies PressureAwareCache on radixCache (pointer-based nodes are reclaimed directly by Go GC upon deletion).
func (c *radixCache[V]) Compact() {
	c.mu.Lock()
	c.compactLocked()
	c.unlock()
}

func (c *radixCache[V]) compactDataStructuresLocked() bool {
	c.onCompacted(c.len)
	return false
}

func (c *radixCache[V]) compactLocked() {
	if c.compactDataStructuresLocked() {
		c.compactionsExplicit++
		c.markReclaimedLocked()
	}
}

func (c *radixCache[V]) shouldAutoCompactLocked(isBackground bool) bool {
	return c.shouldAutoCompactEntryCounts(c.deletedSinceCompact > 0, isBackground, c.len)
}

func (c *radixCache[V]) completeShedAndCompactLocked(evictedCount int, retention float64, unadjusted bool, unprotectedZeroTarget, targetLen int, isBackground bool) {
	if evictedCount > 0 {
		c.recordPressureShed(isBackground)
		if c.len == 0 {
			if c.shouldAutoCompactEntryCounts(true, isBackground, 0) {
				c.compactionsPressureTier2++
			}
			c.resetEmptyTreeLocked()
			return
		}
		c.updateZeroWatermarkAfterShed(retention, c.len, unadjusted, unprotectedZeroTarget, targetLen)
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

func (c *radixCache[V]) shedAndCompactLocked(evictQ *evictCallbackQueue[V], targetSize uint64, retention float64, protectedNode *radixNode[V], isBackground bool) []V {
	if protectedNode != nil && !protectedNode.hasValue {
		protectedNode = nil
	}

	var protectedSize uint64
	hasProtected := protectedNode != nil
	if hasProtected {
		protectedSize = protectedNode.size
	}
	effectiveTarget, targetLen, targetZeroCount, unprotectedZeroTarget := c.computeShedTargets(targetSize, retention, c.len, hasProtected, protectedSize)

	needFullFlush := retention == 0.0
	var evicted []V
	for victim := c.tail; victim != nil; {
		needByteShed, needZeroShed := c.shouldContinueShedding(c.currentSize, effectiveTarget, needFullFlush, targetZeroCount, c.len, targetLen)
		if !needByteShed && !needZeroShed {
			break
		}
		nextVictim := victim.prev
		if c.shouldEvictShedVictim(victim == protectedNode, victim.size, needFullFlush, needByteShed, targetZeroCount) {
			if val, ok := c.eraseInternal(evictQ, victim, EvictionReasonPressure); ok {
				evicted = append(evicted, val)
			}
		}
		victim = nextVictim
	}

	c.completeShedAndCompactLocked(len(evicted), retention, !hasProtected || effectiveTarget <= targetSize, unprotectedZeroTarget, targetLen, isBackground)
	return evicted
}

func (c *radixCache[V]) maybeReclaimUnderPressureLocked(evictQ *evictCallbackQueue[V], pressure float64, protectedNode *radixNode[V], isBackground bool) []V {
	if pressure >= c.options.EvictionThreshold {
		if c.isSamplingGoroutine() {
			return nil
		}
		retention := c.options.EvictionRetentionRatio
		targetSize := c.tier2TargetSize(c.maxSize)
		return c.shedAndCompactLocked(evictQ, targetSize, retention, protectedNode, isBackground)
	}
	c.resetZeroWatermarkBelowTier2(pressure)
	if pressure >= c.options.CompactionThreshold && c.shouldAutoCompactLocked(isBackground) && !c.isSamplingGoroutine() && c.compactDataStructuresLocked() {
		c.compactionsPressureTier1++
		c.markReclaimedLocked()
	}
	return nil
}

func (c *radixCache[V]) evaluateShardPressure(evictQ *evictCallbackQueue[V], sample shardSample) ([]V, shardSample) {
	sampledEpoch, out := c.lockShardWithSample(&c.mu, true, sample)
	evicted := c.maybeReclaimUnderPressureLocked(evictQ, out.pressure, nil, true)
	out.advanceSelfReclaim(c.reclaimEpoch.Load(), sampledEpoch)
	c.unlock()
	return evicted, out
}

// EvaluateMemoryPressure samples the configured memory-pressure probe and sheds LRU tail entries
// down to maxSize * EvictionRetentionRatio if critical pressure is reached.
func (c *radixCache[V]) EvaluateMemoryPressure() []V {
	evictQ := evictCallbackQueue[V]{exactSnap: true}
	evicted, _ := c.evaluateShardPressure(&evictQ, shardSample{})
	evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)
	return evicted
}
