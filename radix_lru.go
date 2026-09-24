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

package lrus

import (
	"fmt"
	"math"
	"strings"
	"sync"
)

// radixNode represents a node in the compressed radix tree (trie).
// It uses a Left-Child Right-Sibling (LCRS) representation to eliminate dynamic slice/map
// allocations on branching and embeds intrusive doubly-linked list pointers for LRU eviction tracking.
type radixNode struct {
	prefix  string
	value   ValueType
	size    uint64
	parent  *radixNode
	child   *radixNode
	sibling *radixNode

	// LRU intrusive doubly-linked list pointers.
	prev *radixNode
	next *radixNode
}

// radixCache encapsulates a compressed prefix tree (radix trie) with an intrusive
// doubly-linked LRU list, implementing the Cache interface.
type radixCache struct {
	// maxSize specifies the maximum allowable sum of entry sizes.
	maxSize uint64

	// currentSize is the current sum of sizes of all stored entries.
	currentSize uint64

	// root is the root node of the radix trie.
	root *radixNode

	// head and tail point to the MRU (head) and LRU (tail) nodes in the eviction list.
	head *radixNode
	tail *radixNode

	// len is the number of value-bearing entries currently tracked in the LRU list.
	len int

	// mu synchronizes concurrent access to all cache data structures.
	mu sync.RWMutex

	// opts contains configuration options such as invariant checking.
	opts Options
}

// NewRadixCache creates a new RadixCache instance bounded by maxSize (in bytes).
// maxSize must be greater than zero; otherwise NewRadixCache panics.
func NewRadixCache(maxSize uint64, opts ...Option) Cache {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	options := ApplyOptions(opts...)
	c := &radixCache{
		maxSize: maxSize,
		root:    &radixNode{},
		opts:    options,
	}

	if c.opts.EnableInvariantChecking {
		c.checkInvariants()
	}

	return c
}

func (c *radixCache) checkInvariants() {
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
	var sumSize uint64
	var prevNode *radixNode

	for curr := c.head; curr != nil; curr = curr.next {
		lruCount++
		sumSize += curr.size
		if curr.value == nil {
			panic(fmt.Sprintf("radixCache invariant violation: unexpected nil value in LRU list for prefix '%s'", curr.prefix))
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
		if curr.value != nil {
			treeCount++
			treeSumSize += curr.size
			// A node is verifiably in the LRU list if it is the head or has a non-nil prev pointer.
			inLRU := c.head == curr || curr.prev != nil
			if !inLRU {
				panic(fmt.Sprintf("radixCache invariant violation: node with prefix '%s' has value but is missing from LRU list", curr.prefix))
			}
		}

		// Validate child pointers and sibling ordering
		var prevSibling *radixNode
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
		if curr != c.root && curr.value == nil {
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

// longestCommonPrefix finds the length of the longest common prefix of a and b.
func longestCommonPrefix(a, b string) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}

// getChild finds a child node whose prefix starts with byte b.
// Takes advantage of sorted sibling order for early-exit termination.
func (n *radixNode) getChild(b byte) *radixNode {
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
func (n *radixNode) addChild(newChild *radixNode) {
	newChild.parent = n
	pcurr := &n.child
	for *pcurr != nil && (*pcurr).prefix[0] < newChild.prefix[0] {
		pcurr = &(*pcurr).sibling
	}
	newChild.sibling = *pcurr
	*pcurr = newChild
}

// removeChild directly removes a child node by pointer reference from the sibling list.
func (n *radixNode) removeChild(childToRemove *radixNode) {
	for pcurr := &n.child; *pcurr != nil; pcurr = &(*pcurr).sibling {
		if *pcurr == childToRemove {
			*pcurr = childToRemove.sibling
			childToRemove.sibling = nil
			childToRemove.parent = nil
			return
		}
	}
}

// replaceChild finds oldChild in the sibling linked list and substitutes it with newChild,
// preserving the remainder of the sibling chain.
func (n *radixNode) replaceChild(oldChild, newChild *radixNode) {
	for pcurr := &n.child; *pcurr != nil; pcurr = &(*pcurr).sibling {
		if *pcurr == oldChild {
			newChild.sibling = oldChild.sibling
			*pcurr = newChild

			oldChild.sibling = nil
			oldChild.parent = nil
			return
		}
	}
}

// insertNode inserts a new key into the radix tree and returns the leaf node and previous value (if any).
func (c *radixCache) insertNode(key string, value ValueType) (*radixNode, ValueType) {
	if value == nil {
		return nil, nil
	}

	node := c.root
	search := key

	for {
		if len(search) == 0 {
			oldValue := node.value
			node.value = value
			return node, oldValue
		}

		child := node.getChild(search[0])
		if child == nil {
			// Clone the substring to prevent memory leaks from sliced string headers pinning large backing arrays
			newLeaf := &radixNode{
				prefix: strings.Clone(search),
				value:  value,
			}
			node.addChild(newLeaf)
			return newLeaf, nil
		}

		lcp := longestCommonPrefix(search, child.prefix)

		if lcp == len(child.prefix) {
			search = search[lcp:]
			node = child
			continue
		}

		splitNode := &radixNode{
			prefix: strings.Clone(child.prefix[:lcp]),
			parent: node,
		}

		node.replaceChild(child, splitNode)

		child.prefix = strings.Clone(child.prefix[lcp:])
		child.sibling = nil
		splitNode.addChild(child)

		if lcp == len(search) {
			oldValue := splitNode.value
			splitNode.value = value
			return splitNode, oldValue
		}

		newLeaf := &radixNode{
			prefix: strings.Clone(search[lcp:]),
			value:  value,
		}
		splitNode.addChild(newLeaf)
		return newLeaf, nil
	}
}

// getNode finds a node by key. Returns the node and true if found with a non-nil value.
func (c *radixCache) getNode(key string) (*radixNode, bool) {
	node := c.root
	search := key

	for {
		if len(search) == 0 {
			if node.value != nil {
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
func (c *radixCache) deleteNode(node *radixNode) {
	if node == nil || node.value == nil {
		return
	}

	node.value = nil
	c.compressPathUpwards(node)
}

// compressPathUpwards walks up the tree from curr, pruning empty leaves and compressing single-child routing nodes.
func (c *radixCache) compressPathUpwards(curr *radixNode) {
	for curr != nil && curr != c.root {
		if curr.value != nil {
			break
		}

		if curr.child == nil {
			parent := curr.parent
			parent.removeChild(curr)
			curr = parent
			continue
		}

		if curr.child.sibling == nil {
			onlyChild := curr.child
			onlyChild.prefix = curr.prefix + onlyChild.prefix
			onlyChild.parent = curr.parent

			parent := curr.parent
			curr.parent.replaceChild(curr, onlyChild)

			curr = parent
			continue
		}

		break
	}
}

// --- LRU Linked List Methods ---

// moveToFront moves an existing node in the LRU list to the MRU (head) position.
func (c *radixCache) moveToFront(node *radixNode) {
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
func (c *radixCache) pushFront(node *radixNode) {
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
func (c *radixCache) remove(node *radixNode) {
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

// eraseInternal removes an entry from both the LRU list and radix trie without acquiring locks.
// It returns the deleted ValueType.
func (c *radixCache) eraseInternal(node *radixNode) ValueType {
	deletedEntry := node.value
	c.currentSize -= node.size
	node.size = 0

	c.remove(node)
	c.deleteNode(node)

	return deletedEntry
}

// evictOne removes and returns the least recently used entry (c.tail).
func (c *radixCache) evictOne() ValueType {
	node := c.tail
	if node == nil {
		return nil
	}
	return c.eraseInternal(node)
}

// sweepAndUnlink iteratively cleans up all value-bearing nodes in a detached subtree (O(1) space).
func (c *radixCache) sweepAndUnlink(node *radixNode) {
	curr := node
	for curr != nil {
		if curr.value != nil {
			c.currentSize -= curr.size
			curr.size = 0
			c.remove(curr)
			curr.value = nil
		}

		if curr.child != nil {
			curr = curr.child
			continue
		}

		for curr != node && curr.sibling == nil {
			curr = curr.parent
		}
		if curr == node {
			return
		}
		curr = curr.sibling
	}
}

// ============================================================================
// Cache Interface Implementation
// ============================================================================

// Insert inserts or updates a key-value entry in the cache.
// If the key exists, its value is updated and moved to MRU.
// If capacity is exceeded, excess LRU entries are evicted and returned.
//
// Returns ErrInvalidEntry if value is nil.
// Returns ErrInvalidEntrySize if value.Size() exceeds maxSize.
func (c *radixCache) Insert(key string, value ValueType) ([]ValueType, error) {
	if value == nil {
		return nil, ErrInvalidEntry
	}

	valueSize := value.Size()
	if valueSize > c.maxSize {
		return nil, ErrInvalidEntrySize
	}

	c.mu.Lock()
	defer func() {
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	if node, oldValue := c.insertNode(key, value); oldValue != nil {
		c.currentSize -= node.size
		c.currentSize += valueSize
		node.size = valueSize
		c.moveToFront(node)
	} else {
		node.size = valueSize
		c.pushFront(node)
		c.currentSize += valueSize
	}

	var evictedValues []ValueType
	for c.currentSize > c.maxSize && c.tail != nil {
		evictedValues = append(evictedValues, c.evictOne())
	}

	return evictedValues, nil
}

// Erase removes the entry associated with key, returning its value (or nil if not found).
func (c *radixCache) Erase(key string) (value ValueType) {
	c.mu.Lock()
	defer func() {
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	node, ok := c.getNode(key)
	if !ok {
		return nil
	}

	return c.eraseInternal(node)
}

// LookUp retrieves the value for key and promotes it to the MRU position.
// Returns nil if key is not found.
func (c *radixCache) LookUp(key string) (value ValueType) {
	c.mu.Lock()
	defer func() {
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	node, ok := c.getNode(key)
	if !ok {
		return nil
	}
	c.moveToFront(node)

	return node.value
}

// LookUpWithoutChangingOrder retrieves the value for key without modifying its LRU position.
// Returns nil if key is not found.
func (c *radixCache) LookUpWithoutChangingOrder(key string) (value ValueType) {
	c.mu.RLock()
	defer func() {
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.RUnlock()
	}()

	node, ok := c.getNode(key)
	if !ok {
		return nil
	}

	return node.value
}

// UpdateWithoutChangingOrder updates the value of an existing key without modifying its LRU order.
//
// Returns ErrInvalidEntry if value is nil.
// Returns ErrEntryNotExist if key does not exist.
// Returns ErrInvalidUpdateEntrySize if value.Size() does not match the existing entry's size.
func (c *radixCache) UpdateWithoutChangingOrder(key string, value ValueType) error {
	if value == nil {
		return ErrInvalidEntry
	}

	valueSize := value.Size()

	c.mu.Lock()
	defer func() {
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	node, ok := c.getNode(key)
	if !ok {
		return ErrEntryNotExist
	}

	if valueSize != node.size {
		return ErrInvalidUpdateEntrySize
	}

	node.value = value
	return nil
}

// UpdateSize updates the size accounting for an existing key by sizeDelta and evicts excess entries if needed.
//
// Returns ErrEntryNotExist if key does not exist.
func (c *radixCache) UpdateSize(key string, sizeDelta uint64) error {
	c.mu.Lock()
	defer func() {
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	node, ok := c.getNode(key)
	if !ok {
		return ErrEntryNotExist
	}

	if math.MaxUint64-node.size < sizeDelta || math.MaxUint64-c.currentSize < sizeDelta {
		return ErrInvalidUpdateEntrySize
	}

	node.size += sizeDelta
	c.currentSize += sizeDelta

	for c.currentSize > c.maxSize && c.tail != nil {
		c.evictOne()
	}

	return nil
}

// EraseEntriesWithGivenPrefix deletes all entries whose keys start with prefix.
// Prunes subtrees in O(prefix_length + subtree_size) time and sweeps detached nodes.
func (c *radixCache) EraseEntriesWithGivenPrefix(prefix string) {
	c.mu.Lock()
	defer func() {
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	if prefix == "" {
		c.root = &radixNode{}
		c.head = nil
		c.tail = nil
		c.currentSize = 0
		c.len = 0
		return
	}

	node := c.root
	search := prefix

	for len(search) > 0 {
		child := node.getChild(search[0])
		if child == nil {
			return
		}

		lcp := longestCommonPrefix(search, child.prefix)

		if lcp == len(search) {
			node.removeChild(child)
			c.sweepAndUnlink(child)
			c.compressPathUpwards(node)
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

// Compact satisfies PressureAwareCache on radixCache (pointer-based nodes are reclaimed directly by Go GC upon deletion).
func (c *radixCache) Compact() {
	c.mu.Lock()
	defer func() {
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()
}

// EvaluateMemoryPressure samples the configured memory-pressure probe and sheds LRU tail entries
// down to maxSize * EvictionRetentionRatio if critical pressure is reached.
func (c *radixCache) EvaluateMemoryPressure() []ValueType {
	var pressure float64
	if c.opts.PressureFunc != nil {
		pressure = c.opts.PressureFunc()
		if math.IsNaN(pressure) || pressure < 0.0 {
			pressure = 0.0
		}
	}

	c.mu.Lock()
	defer func() {
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	if pressure >= c.opts.EvictionThreshold {
		retention := c.opts.EvictionRetentionRatio
		targetSize := computeTargetSize(c.maxSize, retention)
		targetLen := 0
		if c.currentSize == 0 && c.len > 0 && retention > 0.0 {
			targetLen = int(float64(c.len) * retention)
		}
		var evicted []ValueType
		for c.tail != nil {
			needByteShed := c.currentSize > targetSize
			needZeroSizeShed := c.currentSize == 0 && c.len > targetLen
			needFullFlush := retention == 0.0
			if !needByteShed && !needZeroSizeShed && !needFullFlush {
				break
			}
			if val := c.evictOne(); val != nil {
				evicted = append(evicted, val)
			}
		}
		return evicted
	}
	return nil
}
