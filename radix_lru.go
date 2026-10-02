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
	return newRadixCacheWithOptions[V](maxSize, ApplyOptions(opts...))
}

func newRadixCacheWithOptions[V any](maxSize uint64, options Options) Cache[V] {
	c := &radixCache[V]{
		maxSize:       maxSize,
		root:          &radixNode[V]{},
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

func (c *radixCache[V]) weigh(key string, value V) uint64 {
	if c.weigher != nil {
		return c.weigher(key, value)
	}
	return 1
}

func (c *radixCache[V]) notifyEvict(key string, value V, reason EvictionReason) {
	if c.onEvictValue != nil {
		c.onEvictValue(value, reason)
	}
	if c.onEvictEntry != nil {
		c.onEvictEntry(key, value, reason)
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
		if !curr.hasValue {
			panic(fmt.Sprintf("radixCache invariant violation: unexpected hasValue=false in LRU list for prefix '%s'", curr.prefix))
		}

		// Bidirectional link validation
		if curr.prev != prevNode {
			panic(fmt.Sprintf("radixCache invariant violation: corrupt prev pointer in LRU list for prefix '%s'", curr.prefix))
		}
		if prevNode == nil {
			if c.head != curr {
				panic("radixCache invariant violation: head mismatch in LRU list")
			}
		} else {
			if prevNode.next != curr {
				panic(fmt.Sprintf("radixCache invariant violation: corrupt next pointer in LRU list for prefix '%s'", prevNode.prefix))
			}
		}

		if curr.next == nil {
			if c.tail != curr {
				panic("radixCache invariant violation: tail mismatch in LRU list")
			}
		}

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

	if c.len == 0 {
		if c.head != nil || c.tail != nil {
			panic("radixCache invariant violation: head or tail is non-nil when len is 0")
		}
	} else {
		if c.head == nil || c.tail == nil {
			panic("radixCache invariant violation: head or tail is nil when len > 0")
		}
	}

	// INVARIANT 4: Root structure checks
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

	// INVARIANT 5: Iterative pre-order traversal using parent/sibling pointers (O(1) space).
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
			// A node is verifiably in the LRU list iff it is the head (with nil prev) or its prev's next points back to it.
			inLRU := (c.head == curr && curr.prev == nil) || (curr.prev != nil && curr.prev.next == curr)
			if !inLRU {
				panic(fmt.Sprintf("radixCache invariant violation: node with prefix '%s' has value but is missing from LRU list", curr.prefix))
			}
		} else {
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
		if curr != c.root && !curr.hasValue {
			if curr.child == nil || curr.child.sibling == nil {
				panic(fmt.Sprintf("radixCache invariant violation: intermediate routing node with prefix '%s' has fewer than 2 children", curr.prefix))
			}
		}

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

// deleteNode clears a node's value and compresses the path upwards.
func (c *radixCache[V]) deleteNode(node *radixNode[V]) {
	if node == nil || !node.hasValue {
		return
	}

	var zero V
	node.value = zero
	node.hasValue = false
	c.compressPathUpwards(node)
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
			onlyChild.prefix = curr.prefix + onlyChild.prefix
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
func (c *radixCache[V]) eraseInternal(node *radixNode[V], reason EvictionReason) (V, bool) {
	if node == nil || !node.hasValue {
		var zero V
		return zero, false
	}
	var key string
	if c.onEvictEntry != nil {
		key = c.reconstructKey(node)
	}
	return c.eraseInternalWithKey(node, key, reason)
}

func (c *radixCache[V]) eraseInternalWithKey(node *radixNode[V], key string, reason EvictionReason) (V, bool) {
	if node == nil || !node.hasValue {
		var zero V
		return zero, false
	}
	deletedEntry := node.value
	c.onEntryDeleted(node.size)
	c.currentSize -= node.size
	node.size = 0

	c.remove(node)
	c.deleteNode(node)

	c.notifyEvict(key, deletedEntry, reason)
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
func (c *radixCache[V]) evictOne() (V, bool) {
	node := c.tail
	if node == nil {
		var zero V
		return zero, false
	}
	return c.eraseInternal(node, EvictionReasonCapacity)
}

// sweepAndUnlink iteratively cleans up all value-bearing nodes in a detached subtree (O(1) space).
func (c *radixCache[V]) sweepAndUnlink(node *radixNode[V]) {
	var zero V
	curr := node
	for curr != nil {
		if curr.hasValue {
			var key string
			if c.onEvictEntry != nil {
				key = c.reconstructKey(curr)
			}
			evictedVal := curr.value
			c.onEntryDeleted(curr.size)
			c.currentSize -= curr.size
			curr.size = 0
			c.remove(curr)
			curr.value = zero
			curr.hasValue = false
			c.notifyEvict(key, evictedVal, EvictionReasonDeleted)
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

func (c *radixCache[V]) unlock() {
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
	c.mu.Unlock()
}

func (c *radixCache[V]) rUnlock() {
	if c.options.EnableInvariantChecking {
		c.checkInvariants()
	}
	c.mu.RUnlock()
}

func (c *radixCache[V]) finishDeleteReclaimLocked(sizeBefore, sampledEpoch uint64, pressure float64) {
	if c.len == 0 {
		hadSlack := c.hasEmptyDeleteSlack(false)
		c.clearEmptyTreeStateLocked()
		if (hadSlack || sizeBefore > 0) && c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
			c.markReclaimedLocked()
		}
		return
	}
	reclaimedSingleSurvivor := false
	if c.shouldReclaimSingleSurvivorOnDelete(c.len, c.deletedSinceCompact > 0, false) {
		c.compactDataStructuresLocked()
		reclaimedSingleSurvivor = true
	}
	c.maybeReclaimUnderPressureLocked(pressure, nil, false)
	if (reclaimedSingleSurvivor || c.currentSize < sizeBefore) && c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
		c.markReclaimedLocked()
	}
}

func (c *radixCache[V]) finishMutationReclaimLocked(evictedValues []V, protectedNode *radixNode[V], reclaimedPre bool, sizeBefore, sampledEpoch uint64, pressure float64) []V {
	evictedByPressure := c.maybeReclaimUnderPressureLocked(pressure, protectedNode, false)
	if len(evictedValues) == 0 {
		evictedValues = evictedByPressure
	} else if len(evictedByPressure) > 0 {
		evictedValues = append(evictedValues, evictedByPressure...)
	}
	netByteReduced := c.currentSize < sizeBefore
	if c.shouldCompactAfterMutation(reclaimedPre, netByteReduced, c.deletedSinceCompact > 0, pressure) {
		c.compactDataStructuresLocked()
	}
	if c.shouldMarkReclaimedAfterMutation(reclaimedPre, netByteReduced, sampledEpoch, pressure) {
		c.markReclaimedLocked()
	}
	return evictedValues
}

// Put inserts or updates a key-value entry in the cache.
// If the key exists, its value is updated and moved to MRU.
// If capacity is exceeded, excess LRU entries are evicted and returned.
//
// Returns ErrInvalidEntrySize if the entry's weight exceeds maxSize.
func (c *radixCache[V]) Put(key string, value V) ([]V, error) {
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

	node, exists := c.getNode(key)
	if exists {
		oldValue := node.value
		c.onEntrySizeUpdated(node.size, valueSize)
		c.moveToFront(node)
		c.currentSize -= node.size
		for valueSize > c.maxSize-c.currentSize && c.tail != nil && c.tail != node {
			if evicted, ok := c.evictOne(); ok {
				evictedValues = append(evictedValues, evicted)
				evictedPrePut = true
			}
		}
		node.value = value
		node.hasValue = true
		node.size = valueSize
		c.currentSize += valueSize
		c.notifyEvict(key, oldValue, EvictionReasonReplaced)
		reclaimedPrePut = c.shouldReclaimSingleSurvivorOnMutation(c.len, c.deletedSinceCompact > 0, false, evictedPrePut, c.currentSize, sizeBefore, pressure)
	} else {
		// Evict from the LRU tail before inserting into the trie to avoid redundant node splits and merges.
		for valueSize > c.maxSize-c.currentSize && c.tail != nil {
			if evicted, ok := c.evictOne(); ok {
				evictedValues = append(evictedValues, evicted)
				evictedPrePut = true
			}
		}
		if c.shouldReclaimEmptyPrePut(c.len, false, evictedPrePut, valueSize, sizeBefore, pressure) {
			c.clearEmptyTreeStateLocked()
			reclaimedPrePut = true
		} else if c.shouldReclaimSingleSurvivorOnMutation(c.len, c.deletedSinceCompact > 0, false, evictedPrePut, c.currentSize+valueSize, sizeBefore, pressure) {
			reclaimedPrePut = true
		}
		node = c.insertNode(key, value)
		node.size = valueSize
		c.pushFront(node)
		c.onEntryPut(c.len, valueSize)
		c.currentSize += valueSize
	}

	evictedValues = c.finishMutationReclaimLocked(evictedValues, node, reclaimedPrePut, sizeBefore, sampledEpoch, pressure)
	return evictedValues, nil
}

// Delete removes the entry associated with key, returning its value and true (or the zero value of V and false if not found).
func (c *radixCache[V]) Delete(key string) (value V, ok bool) {
	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	node, found := c.getNode(key)
	if !found {
		return value, false
	}

	sizeBefore := c.currentSize
	deleted, erased := c.eraseInternalWithKey(node, key, EvictionReasonDeleted)
	if !erased {
		return value, false
	}
	c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
	return deleted, true
}

// Get retrieves the value for key and promotes it to the MRU position.
// Returns the zero value of V and false if key is not found.
func (c *radixCache[V]) Get(key string) (value V, ok bool) {
	c.mu.Lock()
	defer c.unlock()

	node, found := c.getNode(key)
	if !found {
		return value, false
	}
	c.moveToFront(node)

	return node.value, true
}

// Peek retrieves the value for key without modifying its LRU position.
// Returns the zero value of V and false if key is not found.
func (c *radixCache[V]) Peek(key string) (value V, ok bool) {
	c.mu.RLock()
	defer c.rUnlock()

	node, found := c.getNode(key)
	if !found {
		return value, false
	}

	return node.value, true
}

// Replace updates the value of an existing key and recomputes its weight
// without modifying its LRU order.
// If the entry's new weight exceeds maxSize (or cannot fit alongside entries more recent than node),
// only node itself is evicted without evicting older entries. Otherwise, excess LRU entries are evicted if needed.
//
// Returns ErrEntryNotExist if key does not exist.
func (c *radixCache[V]) Replace(key string, value V) error {
	newSize := c.weigh(key, value)

	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

	node, ok := c.getNode(key)
	if !ok {
		return ErrEntryNotExist
	}

	oldSize := node.size
	oldValue := node.value

	if newSize > c.maxSize {
		sizeBefore := c.currentSize
		c.eraseInternalWithKey(node, key, EvictionReasonCapacity)
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
				if headCurr == node {
					canFit = newerSize <= maxNewer
					break
				}
				if headCurr != nil {
					newerSize += headCurr.size
					if newerSize > maxNewer {
						canFit = false
						break
					}
					headCurr = headCurr.next
				}
				if tailCurr == nil || tailCurr == node {
					canFit = sizeDelta <= avail
					break
				}
				avail += tailCurr.size
				if sizeDelta <= avail {
					canFit = true
					break
				}
				tailCurr = tailCurr.prev
			}
			if !canFit {
				c.eraseInternalWithKey(node, key, EvictionReasonCapacity)
				c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
				return nil
			}
		}

		for sizeDelta > c.maxSize-c.currentSize && c.tail != nil {
			if c.tail == node {
				break
			}
			c.evictOne()
			evictedAny = true
		}

		c.onEntrySizeUpdated(oldSize, newSize)
		node.value = value
		node.hasValue = true
		node.size = newSize
		c.currentSize += sizeDelta
	case newSize < oldSize:
		sizeDiff := oldSize - newSize
		c.onEntrySizeUpdated(oldSize, newSize)
		node.value = value
		node.hasValue = true
		node.size = newSize
		c.currentSize -= sizeDiff
	default:
		node.value = value
		node.hasValue = true
	}

	c.notifyEvict(key, oldValue, EvictionReasonReplaced)

	reclaimedPreUpdate = c.shouldReclaimSingleSurvivorOnMutation(c.len, c.deletedSinceCompact > 0, false, evictedAny, c.currentSize, sizeBefore, pressure)

	var protectedNode *radixNode[V]
	if node == c.head && node.hasValue {
		protectedNode = node
	}
	c.finishMutationReclaimLocked(nil, protectedNode, reclaimedPreUpdate, sizeBefore, sampledEpoch, pressure)

	return nil
}

// DeletePrefix deletes all entries whose keys start with prefix.
// Prunes subtrees in O(prefix_length + subtree_size) time and sweeps detached nodes.
func (c *radixCache[V]) DeletePrefix(prefix string) {
	if prefix == "" {
		c.mu.Lock()
		defer c.unlock()

		hadEntries := c.len > 0 || (c.root != nil && (c.root.hasValue || c.root.child != nil))
		hadDirtySlack := c.deletedSinceCompact > 0 || c.peakEntryLen > 8
		if !hadEntries && !hadDirtySlack && c.peakEntryLen == 0 {
			return
		}
		hadReclaimable := c.currentSize > 0 || hadDirtySlack
		if c.root != nil {
			c.sweepAndUnlink(c.root)
		} else {
			c.root = &radixNode[V]{}
		}
		c.head = nil
		c.tail = nil
		c.currentSize = 0
		c.len = 0
		c.clearEmptyTreeStateLocked()
		if hadReclaimable && c.hasElevatedPressureToInvalidate(0.0) {
			c.markReclaimedLocked()
		}
		return
	}

	sampledEpoch, pressure := c.lockWithPressure(&c.mu, false)
	defer c.unlock()

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
			c.sweepAndUnlink(child)
			c.compressPathUpwards(node)
			c.finishDeleteReclaimLocked(sizeBefore, sampledEpoch, pressure)
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

// All returns an iterator over all key-value pairs in MRU-to-LRU order without modifying recency.
func (c *radixCache[V]) All() iter.Seq2[string, V] {
	return c.all
}

func (c *radixCache[V]) all(yield func(string, V) bool) {
	c.mu.RLock()
	defer c.rUnlock()

	for curr := c.head; curr != nil; curr = curr.next {
		if !yield(c.reconstructKey(curr), curr.value) {
			return
		}
	}
}

// Keys returns an iterator over all keys in MRU-to-LRU order without modifying recency.
func (c *radixCache[V]) Keys() iter.Seq[string] {
	return c.keys
}

func (c *radixCache[V]) keys(yield func(string) bool) {
	c.mu.RLock()
	defer c.rUnlock()

	for curr := c.head; curr != nil; curr = curr.next {
		if !yield(c.reconstructKey(curr)) {
			return
		}
	}
}

// Values returns an iterator over all values in MRU-to-LRU order without modifying recency.
// It skips key reconstruction completely, executing with zero heap allocations.
func (c *radixCache[V]) Values() iter.Seq[V] {
	return c.values
}

func (c *radixCache[V]) values(yield func(V) bool) {
	c.mu.RLock()
	defer c.rUnlock()

	for curr := c.head; curr != nil; curr = curr.next {
		if !yield(curr.value) {
			return
		}
	}
}

// Compact satisfies PressureAwareCache on radixCache (pointer-based nodes are reclaimed directly by Go GC upon deletion).
func (c *radixCache[V]) Compact() {
	c.mu.Lock()
	defer c.unlock()
	c.compactLocked()
}

func (c *radixCache[V]) compactDataStructuresLocked() bool {
	if c.deletedSinceCompact == 0 && c.peakEntryLen <= c.len {
		return false
	}
	c.onCompacted(c.len)
	return true
}

func (c *radixCache[V]) compactLocked() {
	if c.compactDataStructuresLocked() {
		c.markReclaimedLocked()
	}
}

func (c *radixCache[V]) shouldAutoCompactLocked(isBackground bool) bool {
	return c.shouldAutoCompactEntryCounts(c.deletedSinceCompact > 0, isBackground, c.len)
}

func (c *radixCache[V]) shedAndCompactLocked(targetSize uint64, retention float64, protectedNode *radixNode[V], isBackground bool) []V {
	if protectedNode != nil && (protectedNode != c.head || protectedNode.prev != nil || !protectedNode.hasValue) {
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
	victim := c.tail
	for victim != nil {
		needByteShed := c.currentSize > effectiveTarget || needFullFlush
		needZeroShed := !needFullFlush && c.zeroSizeCount > targetZeroCount && c.len > targetLen
		if !needByteShed && !needZeroShed {
			break
		}
		switch {
		case needFullFlush || (needByteShed && c.zeroSizeCount > targetZeroCount):
			for victim != nil && victim == protectedNode {
				victim = victim.prev
			}
		case needByteShed:
			for victim != nil && (victim == protectedNode || victim.size == 0) {
				victim = victim.prev
			}
		default:
			for victim != nil && (victim == protectedNode || victim.size > 0) {
				victim = victim.prev
			}
		}
		if victim == nil {
			break
		}
		nextVictim := victim.prev
		if val, ok := c.eraseInternal(victim, EvictionReasonPressure); ok {
			evicted = append(evicted, val)
		}
		victim = nextVictim
	}

	if len(evicted) > 0 && c.len == 0 {
		c.resetEmptyTreeLocked()
		return evicted
	}

	if len(evicted) > 0 {
		c.updateZeroWatermarkAfterShed(retention, c.len, !hasProtected || effectiveTarget <= targetSize, unprotectedZeroTarget, targetLen)
	}
	compacted := false
	if c.shouldAutoCompactLocked(isBackground) {
		compacted = c.compactDataStructuresLocked()
	}
	if len(evicted) > 0 || compacted {
		c.markReclaimedLocked()
	}
	return evicted
}

func (c *radixCache[V]) maybeReclaimUnderPressureLocked(pressure float64, protectedNode *radixNode[V], isBackground bool) []V {
	if c.isSamplingGoroutine() {
		return nil
	}
	var evicted []V
	if pressure >= c.options.EvictionThreshold {
		retention := c.options.EvictionRetentionRatio
		targetSize := computeTargetSize(c.maxSize, retention)
		evicted = c.shedAndCompactLocked(targetSize, retention, protectedNode, isBackground)
	} else {
		c.resetZeroWatermarkBelowTier2(pressure)
		if pressure >= c.options.CompactionThreshold {
			if c.shouldAutoCompactLocked(isBackground) {
				c.compactLocked()
			}
		}
	}
	return evicted
}

// EvaluateMemoryPressure samples the configured memory-pressure probe and sheds LRU tail entries
// down to maxSize * EvictionRetentionRatio if critical pressure is reached.
func (c *radixCache[V]) EvaluateMemoryPressure() []V {
	_, pressure := c.lockWithPressure(&c.mu, true)
	defer c.unlock()

	return c.maybeReclaimUnderPressureLocked(pressure, nil, true)
}
