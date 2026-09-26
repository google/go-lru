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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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
	len                    int
	zeroSizeCount          int
	lastReclaimedZeroCount int

	// mu synchronizes concurrent access to all cache data structures.
	mu sync.RWMutex

	// opts contains configuration options such as invariant checking.
	opts Options

	// Atomic state for lock-free pressure sampling and reclamation epoch synchronization.
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

var foregroundNoProtectNode radixNode

// NewRadixCache creates a new RadixCache instance bounded by maxSize (in bytes).
// maxSize must be greater than zero; otherwise NewRadixCache panics.
func NewRadixCache(maxSize uint64, opts ...Option) Cache {
	if maxSize == 0 {
		panic("maxSize must be greater than zero")
	}
	return newRadixCacheWithOptions(maxSize, ApplyOptions(opts...))
}

func newRadixCacheWithOptions(maxSize uint64, options Options) Cache {
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
	zeroCount := 0
	var sumSize uint64
	var prevNode *radixNode

	for curr := c.head; curr != nil; curr = curr.next {
		lruCount++
		if math.MaxUint64-sumSize < curr.size {
			panic("radixCache invariant violation: sumSize uint64 overflow")
		}
		sumSize += curr.size
		if curr.size == 0 {
			zeroCount++
		}
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
		if curr.value != nil {
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
	panic("removeChild: requested child not found in sibling list")
}

// replaceChild finds oldChild in the sibling linked list and substitutes it with newChild,
// preserving the remainder of the sibling chain.
func (n *radixNode) replaceChild(oldChild, newChild *radixNode) {
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
				prefix: clonePrefix(search),
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

		// Clone both split prefix halves so surviving intermediate routing nodes never pin
		// the underlying backing arrays of large evicted leaf keys.
		oldPrefix := child.prefix
		splitNode := &radixNode{
			prefix: clonePrefix(oldPrefix[:lcp]),
			parent: node,
		}

		node.replaceChild(child, splitNode)

		child.prefix = clonePrefix(oldPrefix[lcp:])
		child.sibling = nil
		splitNode.addChild(child)

		if lcp == len(search) {
			oldValue := splitNode.value
			splitNode.value = value
			return splitNode, oldValue
		}

		newLeaf := &radixNode{
			prefix: clonePrefix(search[lcp:]),
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
	if deletedEntry != nil && node.size == 0 && c.zeroSizeCount > 0 {
		c.zeroSizeCount--
		if c.zeroSizeCount < c.lastReclaimedZeroCount {
			c.lastReclaimedZeroCount = c.zeroSizeCount
		}
	}
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
			if curr.size == 0 && c.zeroSizeCount > 0 {
				c.zeroSizeCount--
				if c.zeroSizeCount < c.lastReclaimedZeroCount {
					c.lastReclaimedZeroCount = c.zeroSizeCount
				}
			}
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

func (c *radixCache) unlock() {
	if c.opts.EnableInvariantChecking {
		c.checkInvariants()
	}
	c.mu.Unlock()
}

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

	sampledEpoch := c.reclaimEpoch.Load()
	pressure := c.samplePressure()

	c.mu.Lock()
	for retries := 0; sampledEpoch != c.reclaimEpoch.Load() && retries < 2; retries++ {
		c.mu.Unlock()
		sampledEpoch = c.reclaimEpoch.Load()
		pressure = c.samplePressure()
		c.mu.Lock()
	}
	defer c.unlock()

	var evictedValues []ValueType

	node, exists := c.getNode(key)
	if exists {
		if node.size == 0 && valueSize > 0 && c.zeroSizeCount > 0 {
			c.zeroSizeCount--
			if c.zeroSizeCount < c.lastReclaimedZeroCount {
				c.lastReclaimedZeroCount = c.zeroSizeCount
			}
		} else if node.size > 0 && valueSize == 0 {
			c.zeroSizeCount++
		}
		c.moveToFront(node)
		c.currentSize -= node.size
		for valueSize > c.maxSize-c.currentSize && c.tail != nil && c.tail != node {
			evictedValues = append(evictedValues, c.evictOne())
		}
		node.value = value
		node.size = valueSize
		c.currentSize += valueSize
	} else {
		// Evict from the LRU tail before inserting into the trie to avoid redundant node splits and merges.
		for valueSize > c.maxSize-c.currentSize && c.tail != nil {
			evictedValues = append(evictedValues, c.evictOne())
		}
		node, _ = c.insertNode(key, value)
		node.size = valueSize
		if valueSize == 0 {
			c.zeroSizeCount++
		}
		c.pushFront(node)
		c.currentSize += valueSize
	}

	for c.currentSize > c.maxSize && c.tail != nil {
		evictedValues = append(evictedValues, c.evictOne())
	}

	if evictedByPressure := c.maybeReclaimUnderPressureLocked(pressure, node); len(evictedByPressure) > 0 {
		evictedValues = append(evictedValues, evictedByPressure...)
	}

	return evictedValues, nil
}

// Erase removes the entry associated with key, returning its value (or nil if not found).
func (c *radixCache) Erase(key string) (value ValueType) {
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
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	node, ok := c.getNode(key)
	if !ok {
		c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectNode)
		return nil
	}

	deleted := c.eraseInternal(node)
	if c.len == 0 {
		c.zeroSizeCount = 0
		c.lastReclaimedZeroCount = 0
		c.markReclaimedLocked()
		return deleted
	}
	c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectNode)
	if c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
		c.markReclaimedLocked()
	}
	return deleted
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
// If node.size + sizeDelta exceeds maxSize (or cannot fit alongside entries more recent than node),
// only node itself is evicted without evicting older entries.
//
// Returns ErrEntryNotExist if key does not exist.
// Returns ErrInvalidUpdateEntrySize if sizeDelta causes uint64 integer overflow.
func (c *radixCache) UpdateSize(key string, sizeDelta uint64) error {
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
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	node, ok := c.getNode(key)
	if !ok {
		c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectNode)
		return ErrEntryNotExist
	}

	if math.MaxUint64-node.size < sizeDelta {
		return ErrInvalidUpdateEntrySize
	}

	avail := c.maxSize - c.currentSize
	if node.size+sizeDelta > c.maxSize {
		avail = 0
	} else {
		for curr := c.tail; curr != nil && curr != node && sizeDelta > avail; curr = curr.prev {
			avail += curr.size
		}
	}
	if node.size+sizeDelta > c.maxSize || sizeDelta > avail {
		c.eraseInternal(node)
		if c.len == 0 {
			c.zeroSizeCount = 0
			c.lastReclaimedZeroCount = 0
			c.markReclaimedLocked()
			return nil
		}
		c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectNode)
		if c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
			c.markReclaimedLocked()
		}
		return nil
	}

	evictedAny := false
	for sizeDelta > c.maxSize-c.currentSize && c.tail != nil {
		if c.tail == node {
			break
		}
		c.evictOne()
		evictedAny = true
	}

	if math.MaxUint64-c.currentSize < sizeDelta {
		return ErrInvalidUpdateEntrySize
	}

	if node.size == 0 && sizeDelta > 0 && c.zeroSizeCount > 0 {
		c.zeroSizeCount--
		if c.zeroSizeCount < c.lastReclaimedZeroCount {
			c.lastReclaimedZeroCount = c.zeroSizeCount
		}
	}
	node.size += sizeDelta
	c.currentSize += sizeDelta

	protectedNode := &foregroundNoProtectNode
	if sizeDelta > 0 && node == c.head {
		protectedNode = node
	}
	c.maybeReclaimUnderPressureLocked(pressure, protectedNode)
	if evictedAny && c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
		c.markReclaimedLocked()
	}

	return nil
}

// EraseEntriesWithGivenPrefix deletes all entries whose keys start with prefix.
// Prunes subtrees in O(prefix_length + subtree_size) time and sweeps detached nodes.
func (c *radixCache) EraseEntriesWithGivenPrefix(prefix string) {
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
		c.zeroSizeCount = 0
		c.lastReclaimedZeroCount = 0
		c.markReclaimedLocked()
		return
	}

	node := c.root
	search := prefix

	for len(search) > 0 {
		child := node.getChild(search[0])
		if child == nil {
			c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectNode)
			return
		}

		lcp := longestCommonPrefix(search, child.prefix)

		if lcp == len(search) {
			node.removeChild(child)
			c.sweepAndUnlink(child)
			c.compressPathUpwards(node)
			if c.len == 0 {
				c.zeroSizeCount = 0
				c.lastReclaimedZeroCount = 0
				c.markReclaimedLocked()
				return
			}
			c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectNode)
			if c.reclaimEpoch.Load() == sampledEpoch && c.hasElevatedPressureToInvalidate(pressure) {
				c.markReclaimedLocked()
			}
			return
		}

		if lcp == len(child.prefix) {
			search = search[len(child.prefix):]
			node = child
			continue
		}

		c.maybeReclaimUnderPressureLocked(pressure, &foregroundNoProtectNode)
		return
	}
}

func (c *radixCache) hasOverflowSamplingGID(gid uint64) bool {
	if gid == 0 || c.overflowSamplingCount.Load() <= 0 {
		return false
	}
	_, ok := c.overflowSamplingGIDs.Load(gid)
	return ok
}

func (c *radixCache) isCurrentGoroutineSampling(gid uint64) bool {
	if gid == 0 {
		return false
	}
	return c.samplingGID.Load() == gid ||
		c.fallbackGID.Load() == gid ||
		c.hasOverflowSamplingGID(gid)
}

func (c *radixCache) isSamplingGoroutine() bool {
	if !c.opts.hasCustomPressureFunc {
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

func (c *radixCache) markReclaimedLocked() {
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

func (c *radixCache) hasElevatedPressureToInvalidate(pressure float64) bool {
	thresh := c.opts.CompactionThreshold
	return pressure >= thresh ||
		math.Float64frombits(c.cachedPressureBits.Load()) >= thresh ||
		math.Float64frombits(c.lastSampledPressureBits.Load()) >= thresh ||
		c.pressureNeedsRefresh.Load() ||
		c.samplingPressure.Load() ||
		c.fallbackSampling.Load() ||
		c.overflowSamplingCount.Load() > 0
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
	c.markReclaimedLocked()
}

func (c *radixCache) storeSampledPressure(epoch uint64, p float64) {
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

// samplePressureFresh evaluates c.opts.PressureFunc lock-free outside c.mu.Lock()
// with a goroutine-aware re-entrancy guard and cold-start fallback.
func (c *radixCache) samplePressureFresh() float64 {
	if c.opts.PressureFunc == nil {
		return 0.0
	}
	if c.isSamplingGoroutine() {
		return 0.0
	}
	if !c.samplingPressure.CompareAndSwap(false, true) {
		var gid uint64
		if c.opts.hasCustomPressureFunc {
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
		if c.opts.hasCustomPressureFunc && c.isCurrentGoroutineSampling(gid) {
			return 0.0
		}
		if c.fallbackSampling.CompareAndSwap(false, true) {
			if c.opts.hasCustomPressureFunc {
				c.fallbackGID.Store(gid)
				defer func() {
					c.fallbackGID.Store(0)
					c.fallbackSampling.Store(false)
				}()
			} else {
				defer c.fallbackSampling.Store(false)
			}
		} else {
			if c.opts.hasCustomPressureFunc {
				c.overflowSamplingGIDs.Store(gid, struct{}{})
			}
			c.overflowSamplingCount.Add(1)
			defer func() {
				if c.opts.hasCustomPressureFunc {
					c.overflowSamplingGIDs.Delete(gid)
				}
				c.overflowSamplingCount.Add(-1)
			}()
		}
		epoch := c.reclaimEpoch.Load()
		p := c.opts.PressureFunc()
		if math.IsNaN(p) || p < 0.0 {
			p = 0.0
		}
		c.storeSampledPressure(epoch, p)
		return p
	}
	if c.opts.hasCustomPressureFunc {
		c.samplingGID.Store(currentGoroutineID())
		defer func() {
			c.samplingGID.Store(0)
			c.samplingPressure.Store(false)
		}()
	} else {
		defer c.samplingPressure.Store(false)
	}

	epoch := c.reclaimEpoch.Load()
	pressure := c.opts.PressureFunc()
	if math.IsNaN(pressure) || pressure < 0.0 {
		pressure = 0.0
	}
	c.storeSampledPressure(epoch, pressure)
	return pressure
}

func (c *radixCache) samplePressure() float64 {
	if c.opts.hasCustomPressureFunc {
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

func (c *radixCache) shedAndCompactLocked(targetSize uint64, retention float64, protectedNode *radixNode) []ValueType {
	if protectedNode != nil && (protectedNode != c.head || protectedNode.prev != nil) {
		protectedNode = nil
	}

	effectiveTarget := targetSize
	if protectedNode != nil && retention > 0.0 && protectedNode.size > effectiveTarget {
		effectiveTarget = protectedNode.size
	}

	targetLen := 0
	targetZeroCount := 0
	if retention > 0.0 {
		if c.len > 0 {
			targetLen = int(float64(c.len) * retention)
			if protectedNode != nil && targetLen < 1 {
				targetLen = 1
			}
		}
		if c.zeroSizeCount > 0 {
			targetZeroCount = int(float64(c.zeroSizeCount) * retention)
			if protectedNode != nil && protectedNode.size == 0 && targetZeroCount < 1 {
				targetZeroCount = 1
			}
			if c.lastReclaimedZeroCount > targetZeroCount {
				targetZeroCount = c.lastReclaimedZeroCount
			}
		}
	}

	needFullFlush := retention == 0.0
	var evicted []ValueType
	victim := c.tail
	for victim != nil {
		needByteShed := c.currentSize > effectiveTarget || needFullFlush
		needZeroShed := !needFullFlush && c.zeroSizeCount > targetZeroCount && c.len > targetLen
		if !needByteShed && !needZeroShed {
			break
		}
		switch {
		case needFullFlush || (needByteShed && needZeroShed):
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
		if val := c.eraseInternal(victim); val != nil {
			evicted = append(evicted, val)
		}
		victim = nextVictim
	}

	if retention > 0.0 && c.zeroSizeCount > 0 {
		c.lastReclaimedZeroCount = c.zeroSizeCount
	} else {
		c.lastReclaimedZeroCount = 0
	}

	c.markReclaimedLocked()
	return evicted
}

func (c *radixCache) maybeReclaimUnderPressureLocked(pressure float64, protectedNode *radixNode) []ValueType {
	shedProtectedNode := protectedNode
	if shedProtectedNode != nil && (shedProtectedNode != c.head || shedProtectedNode.prev != nil) {
		shedProtectedNode = nil
	}
	epochBefore := c.reclaimEpoch.Load()
	var evicted []ValueType
	if pressure >= c.opts.EvictionThreshold {
		retention := c.opts.EvictionRetentionRatio
		targetSize := computeTargetSize(c.maxSize, retention)
		targetLen := int(float64(c.len) * retention)
		if shedProtectedNode != nil && retention > 0.0 && targetLen < 1 {
			targetLen = 1
		}
		targetZeroCount := int(float64(c.zeroSizeCount) * retention)
		if shedProtectedNode != nil && retention > 0.0 && shedProtectedNode.size == 0 && targetZeroCount < 1 {
			targetZeroCount = 1
		}
		if retention > 0.0 && c.lastReclaimedZeroCount > targetZeroCount {
			targetZeroCount = c.lastReclaimedZeroCount
		}
		if c.currentSize > targetSize || (c.zeroSizeCount > targetZeroCount && c.len > targetLen) || (retention == 0.0 && c.tail != nil) {
			evicted = c.shedAndCompactLocked(targetSize, retention, shedProtectedNode)
		}
	} else {
		c.lastReclaimedZeroCount = 0
	}
	if protectedNode != nil && protectedNode != &foregroundNoProtectNode && c.reclaimEpoch.Load() == epochBefore {
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

// EvaluateMemoryPressure samples the configured memory-pressure probe and sheds LRU tail entries
// down to maxSize * EvictionRetentionRatio if critical pressure is reached.
func (c *radixCache) EvaluateMemoryPressure() []ValueType {
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
		if c.opts.EnableInvariantChecking {
			c.checkInvariants()
		}
		c.mu.Unlock()
	}()

	return c.maybeReclaimUnderPressureLocked(pressure, nil)
}
