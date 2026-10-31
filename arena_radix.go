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
	"encoding/binary"
	"hash/maphash"
	"math"
	"math/bits"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

// nilNode represents the sentinel null reference for 32-bit node indices.
const nilNode uint32 = math.MaxUint32

const (
	// nodeHasValueBit stores the hasValue boolean flag in bit 31 of
	// arenaRadixNode.keyLenAndFlags.
	nodeHasValueBit uint32 = 1 << 31
	// nodeKeyLenBits is the bit width of the key length field in bits 0..16 of keyLenAndFlags.
	nodeKeyLenBits = 17
	// nodeKeyLenMask masks bits 0..16 of arenaRadixNode.keyLenAndFlags (up to 131,071 bytes;
	// keys >= 131,071 bytes store nodeKeyLenMask as a sentinel and resolve exact length by ancestor walk).
	nodeKeyLenMask uint32 = (1 << nodeKeyLenBits) - 1
	// nodeTagLowBits is the number of low hash tag bits packed into bits 17..30 of keyLenAndFlags.
	nodeTagLowBits = 14
	// nodeTagLowMask masks bits 17..30 of arenaRadixNode.keyLenAndFlags.
	nodeTagLowMask uint32 = ((1 << nodeTagLowBits) - 1) << nodeKeyLenBits
	// hashTag21Mask masks the 21-bit inline hash tag (14 bits in keyLenAndFlags + 7 bits in prefixFlags).
	hashTag21Mask uint32 = (1 << (nodeTagLowBits + 7)) - 1

	// largePrefixSentinel is stored in arenaRadixNode.prefixLen when the edge prefix
	// length is >= 65,535 bytes; the exact 32-bit length is stored in the 4 bytes
	// immediately preceding prefixOff in prefixArena.
	largePrefixSentinel uint16 = math.MaxUint16

	// prefixFlagPinned marks a node whose prefix bytes in prefixArena have been exposed
	// directly as a string (e.g., to an onEvictEntry callback) and must not be overwritten in-place.
	prefixFlagPinned uint8 = 1 << 0

	// minHashSlotsCap is the minimum power-of-two capacity of the open-addressed hashSlots table.
	minHashSlotsCap = 16
)

// arenaHashSlot represents an 8-byte slot in the open-addressed flat hash table.
// A tag of 0 denotes an empty slot; all occupied slots have a non-zero 32-bit tag.
type arenaHashSlot struct {
	tag    uint32
	nodeID uint32
}

// prefixSpan represents a recycled contiguous byte range inside prefixArena.
type prefixSpan struct {
	off uint32
	len uint32
}

// arenaRadixNode represents a node in the arena-backed radix tree.
// It uses a Left-Child Right-Sibling (LCRS) tree representation and 32-bit indices
// within a contiguous slice to eliminate per-node heap allocations and internal tree pointer
// traversal during GC cycles, while stored values retain standard Go GC properties.
// Storing the edge suffix as (prefixOff uint32, prefixLen uint16, firstByte uint8, prefixFlags uint8) at bytes 0..7
// indexing into a contiguous prefixArena []byte and packing hasValue + 14-bit hashTag + 17-bit keyLen into keyLenAndFlags at bytes 36..39
// detaches leaf and routing nodes from caller key backing arrays while making arenaRadixNode 100% pointer-free (noscan)
// for scalar V within a 40-byte base header (48 bytes for 8-byte V, fitting within a single 64-byte CPU cache line).
type arenaRadixNode[V any] struct {
	prefixOff      uint32 // Byte offset of edge label suffix in prefixArena
	prefixLen      uint16 // Byte length of edge label suffix (largePrefixSentinel for >= 65,535 bytes)
	firstByte      uint8  // First byte of edge label suffix (0 if empty)
	prefixFlags    uint8  // Bit 0: prefixFlagPinned; bits 1..7: upper 7 bits of 21-bit hashTag
	size           uint64 // Tracked size of the value stored at this node
	parent         uint32 // Arena slice index of parent node
	child          uint32 // Arena slice index of first child (LCRS)
	sibling        uint32 // Arena slice index of next sibling (LCRS)
	prev           uint32 // Arena slice index of previous node in intrusive LRU list
	next           uint32 // Arena slice index of next node in intrusive LRU list (or free-list)
	keyLenAndFlags uint32 // Bit 31: hasValue; bits 17..30: low 14 bits of 21-bit hashTag; bits 0..16: full-key byte length
	value          V      // Value stored at this node
}

// hasValue reports whether this node holds an active cache entry value.
func (n *arenaRadixNode[V]) hasValue() bool {
	return n.keyLenAndFlags&nodeHasValueBit != 0
}

// setHasValue sets or clears the hasValue flag in bit 31 of keyLenAndFlags.
func (n *arenaRadixNode[V]) setHasValue(v bool) {
	if v {
		n.keyLenAndFlags |= nodeHasValueBit
	} else {
		n.keyLenAndFlags &^= nodeHasValueBit
	}
}

// keyLen returns the total byte length of the full key from root up to this node.
func (n *arenaRadixNode[V]) keyLen() int {
	return int(n.keyLenAndFlags & nodeKeyLenMask)
}

// setKeyLen sets the 17-bit full-key byte length in keyLenAndFlags while preserving hasValue and hashTag bits.
func (n *arenaRadixNode[V]) setKeyLen(length int) {
	kl := uint32(min(max(length, 0), int(nodeKeyLenMask)))
	n.keyLenAndFlags = (n.keyLenAndFlags &^ nodeKeyLenMask) | kl
}

// setHashTag21 stores the low 21 bits of the 32-bit hash slot tag across keyLenAndFlags and prefixFlags.
func (n *arenaRadixNode[V]) setHashTag21(tag uint32) {
	low14 := (tag & 0x3FFF) << nodeKeyLenBits
	n.keyLenAndFlags = (n.keyLenAndFlags &^ nodeTagLowMask) | low14
	high7 := uint8((tag>>nodeTagLowBits)&0x7F) << 1
	n.prefixFlags = (n.prefixFlags & prefixFlagPinned) | high7
}

// hashTag21 extracts the 21-bit hash slot tag stored across keyLenAndFlags and prefixFlags.
func (n *arenaRadixNode[V]) hashTag21() uint32 {
	low14 := (n.keyLenAndFlags & nodeTagLowMask) >> nodeKeyLenBits
	high7 := uint32(n.prefixFlags>>1) << nodeTagLowBits
	return low14 | high7
}

// arenaRadix implements the Cache[V] and PressureAwareCache[V] interfaces using a contiguous arena-backed
// radix tree coupled with an intrusive 32-bit doubly-linked LRU list and an O(1) maphash lookup accelerator.
type arenaRadix[V any] struct {
	maxSize     uint64
	currentSize uint64

	nodes     []arenaRadixNode[V]
	freeHead  uint32
	freeCount uint32

	prefixArena     []byte
	livePrefixBytes int
	freePrefixSpans [32]prefixSpan
	freePrefixCount int

	seed           maphash.Seed
	hashSlots      []arenaHashSlot
	hashMask       uint32
	hashCount      int
	collisionPeers map[uint64][]uint32
	nodeMapDirty   bool
	prefixExposed  atomic.Bool
	collisionCount int

	root uint32

	head uint32
	tail uint32

	len int

	weigher      func(key string, value V) uint64
	onEvictValue func(value V, reason EvictionReason)
	onEvictEntry func(key string, value V, reason EvictionReason)

	// mu is placed after the fields read on every lookup (nodes, seed, hashSlots, root):
	// RLock/RUnlock atomically update its reader count, and sharing a cache line
	// with those fields would make every concurrent lookup miss (false sharing).
	mu sync.RWMutex

	pressureState
}

// nodePrefixLen returns the byte length of nodeID's edge prefix.
func (c *arenaRadix[V]) nodePrefixLen(nodeID uint32) int {
	n := &c.nodes[nodeID]
	if n.prefixLen < largePrefixSentinel {
		return int(n.prefixLen)
	}
	off := n.prefixOff
	return int(binary.LittleEndian.Uint32(c.prefixArena[off-4 : off]))
}

// nodePrefix returns nodeID's edge prefix as a string backed by c.prefixArena.
func (c *arenaRadix[V]) nodePrefix(nodeID uint32) string {
	n := &c.nodes[nodeID]
	if n.prefixLen == 0 {
		return ""
	}
	if n.prefixLen < largePrefixSentinel {
		return unsafe.String(&c.prefixArena[n.prefixOff], int(n.prefixLen))
	}
	off := n.prefixOff
	pLen := int(binary.LittleEndian.Uint32(c.prefixArena[off-4 : off]))
	return unsafe.String(&c.prefixArena[off], pLen)
}

func (c *arenaRadix[V]) removeFreePrefixSpanAtLocked(idx int) {
	last := c.freePrefixCount - 1
	c.freePrefixSpans[idx] = c.freePrefixSpans[last]
	c.freePrefixSpans[last] = prefixSpan{}
	c.freePrefixCount = last
}

func (c *arenaRadix[V]) trimFreePrefixSpansAtTailLocked() {
	for {
		trimmed := false
		arenaEnd := uint32(len(c.prefixArena))
		for i := 0; i < c.freePrefixCount; i++ {
			sp := c.freePrefixSpans[i]
			if sp.off+sp.len == arenaEnd {
				c.prefixArena = c.prefixArena[:sp.off]
				c.removeFreePrefixSpanAtLocked(i)
				trimmed = true
				break
			}
		}
		if !trimmed {
			return
		}
	}
}

func (c *arenaRadix[V]) mergeAdjacentFreeSpanLocked(mergedIdx int) {
	sp := c.freePrefixSpans[mergedIdx]
	for j := 0; j < c.freePrefixCount; j++ {
		if j == mergedIdx {
			continue
		}
		other := c.freePrefixSpans[j]
		if sp.off+sp.len == other.off {
			c.freePrefixSpans[mergedIdx].len += other.len
			c.removeFreePrefixSpanAtLocked(j)
			return
		}
		if other.off+other.len == sp.off {
			c.freePrefixSpans[mergedIdx].off = other.off
			c.freePrefixSpans[mergedIdx].len += other.len
			c.removeFreePrefixSpanAtLocked(j)
			return
		}
	}
}

func (c *arenaRadix[V]) recordFreePrefixSpanLocked(off, spanLen uint32) {
	for i := 0; i < c.freePrefixCount; i++ {
		sp := &c.freePrefixSpans[i]
		if sp.off+sp.len == off {
			sp.len += spanLen
			c.mergeAdjacentFreeSpanLocked(i)
			c.trimFreePrefixSpansAtTailLocked()
			return
		}
		if off+spanLen == sp.off {
			sp.off = off
			sp.len += spanLen
			c.mergeAdjacentFreeSpanLocked(i)
			c.trimFreePrefixSpansAtTailLocked()
			return
		}
	}
	if c.freePrefixCount < len(c.freePrefixSpans) {
		c.freePrefixSpans[c.freePrefixCount] = prefixSpan{off: off, len: spanLen}
		c.freePrefixCount++
		return
	}
	minIdx := 0
	minLen := c.freePrefixSpans[0].len
	for i := 1; i < len(c.freePrefixSpans); i++ {
		if c.freePrefixSpans[i].len < minLen {
			minLen = c.freePrefixSpans[i].len
			minIdx = i
		}
	}
	if spanLen > minLen {
		c.freePrefixSpans[minIdx] = prefixSpan{off: off, len: spanLen}
	}
}

func (c *arenaRadix[V]) takeFreePrefixSpanLocked(sLen uint32) (uint32, bool) {
	bestIdx := -1
	for i := 0; i < c.freePrefixCount; i++ {
		sp := c.freePrefixSpans[i]
		if sp.len == sLen {
			c.removeFreePrefixSpanAtLocked(i)
			return sp.off, true
		}
		if sp.len > sLen && (bestIdx < 0 || sp.len < c.freePrefixSpans[bestIdx].len) {
			bestIdx = i
		}
	}
	if bestIdx >= 0 {
		off := c.freePrefixSpans[bestIdx].off
		c.freePrefixSpans[bestIdx].off += sLen
		c.freePrefixSpans[bestIdx].len -= sLen
		return off, true
	}
	return 0, false
}

func (c *arenaRadix[V]) releasePrefixSpanLocked(off uint32, pLen int, isLarge bool, flags uint8) {
	if pLen == 0 {
		return
	}
	c.livePrefixBytes -= pLen
	if flags&prefixFlagPinned != 0 || c.prefixExposed.Load() {
		return
	}
	if int(off)+pLen == len(c.prefixArena) {
		newEnd := off
		if isLarge {
			newEnd -= 4
		}
		c.prefixArena = c.prefixArena[:newEnd]
		c.trimFreePrefixSpansAtTailLocked()
		return
	}
	if !isLarge {
		c.recordFreePrefixSpanLocked(off, uint32(pLen))
	}
}

func (c *arenaRadix[V]) clearNodePrefix(nodeID uint32) {
	n := &c.nodes[nodeID]
	if n.prefixLen == 0 {
		n.prefixOff = 0
		n.firstByte = 0
		n.prefixFlags &^= prefixFlagPinned
		return
	}
	pLen := c.nodePrefixLen(nodeID)
	off := n.prefixOff
	isLarge := n.prefixLen == largePrefixSentinel
	flags := n.prefixFlags
	n.prefixOff = 0
	n.prefixLen = 0
	n.firstByte = 0
	n.prefixFlags &^= prefixFlagPinned
	c.releasePrefixSpanLocked(off, pLen, isLarge, flags)
}

func (c *arenaRadix[V]) ensurePrefixArenaCap(needed int) {
	if len(c.prefixArena)+needed <= cap(c.prefixArena) {
		return
	}
	if len(c.prefixArena) >= 1024 && c.livePrefixBytes*2 <= len(c.prefixArena) {
		c.compactPrefixArenaLocked(needed)
	}
}

func (c *arenaRadix[V]) setLargeNodePrefix(nodeID uint32, s string) {
	sLen := len(s)
	c.ensurePrefixArenaCap(sLen + 4)
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(sLen))
	c.prefixArena = append(c.prefixArena, hdr[:]...)
	off := uint32(len(c.prefixArena))
	c.prefixArena = append(c.prefixArena, s...)
	n := &c.nodes[nodeID]
	n.prefixOff = off
	n.prefixLen = largePrefixSentinel
	n.firstByte = s[0]
	n.prefixFlags &^= prefixFlagPinned
	c.livePrefixBytes += sLen
}

func (c *arenaRadix[V]) setNodePrefix(nodeID uint32, s string) {
	if c.nodes[nodeID].prefixLen != 0 {
		c.clearNodePrefix(nodeID)
	}
	sLen := len(s)
	if sLen == 0 {
		return
	}
	if sLen >= int(largePrefixSentinel) {
		c.setLargeNodePrefix(nodeID, s)
		return
	}
	if c.freePrefixCount > 0 {
		if off, ok := c.takeFreePrefixSpanLocked(uint32(sLen)); ok {
			copy(c.prefixArena[off:int(off)+sLen], s)
			n := &c.nodes[nodeID]
			n.prefixOff = off
			n.prefixLen = uint16(sLen)
			n.firstByte = s[0]
			n.prefixFlags &^= prefixFlagPinned
			c.livePrefixBytes += sLen
			return
		}
	}
	c.ensurePrefixArenaCap(sLen)
	off := uint32(len(c.prefixArena))
	c.prefixArena = append(c.prefixArena, s...)
	n := &c.nodes[nodeID]
	n.prefixOff = off
	n.prefixLen = uint16(sLen)
	n.firstByte = s[0]
	n.prefixFlags &^= prefixFlagPinned
	c.livePrefixBytes += sLen
}

func (c *arenaRadix[V]) setNodePrefixFromArenaSliceLocked(nodeID, srcOff uint32, sLen int) {
	needed := sLen
	if sLen >= int(largePrefixSentinel) {
		needed += 4
	}
	if len(c.prefixArena)+needed > cap(c.prefixArena) {
		newCap := max(cap(c.prefixArena)*2, len(c.prefixArena)+needed, 64)
		grown := make([]byte, len(c.prefixArena), newCap)
		copy(grown, c.prefixArena)
		c.prefixArena = grown
	}
	if sLen >= int(largePrefixSentinel) {
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], uint32(sLen))
		c.prefixArena = append(c.prefixArena, hdr[:]...)
	}
	dstOff := uint32(len(c.prefixArena))
	c.prefixArena = append(c.prefixArena, c.prefixArena[srcOff:int(srcOff)+sLen]...)
	n := &c.nodes[nodeID]
	n.prefixOff = dstOff
	if sLen >= int(largePrefixSentinel) {
		n.prefixLen = largePrefixSentinel
	} else {
		n.prefixLen = uint16(sLen)
	}
	n.firstByte = c.prefixArena[dstOff]
	n.prefixFlags &^= prefixFlagPinned
	c.livePrefixBytes += sLen
}

func (c *arenaRadix[V]) splitNodePrefixLocked(childID, splitNodeID uint32, lcp int) {
	oldLen := c.nodePrefixLen(childID)
	remLen := oldLen - lcp
	oldOff := c.nodes[childID].prefixOff
	oldFirst := c.nodes[childID].firstByte
	oldFlags := c.nodes[childID].prefixFlags
	wasLarge := c.nodes[childID].prefixLen == largePrefixSentinel

	if lcp < int(largePrefixSentinel) && remLen < int(largePrefixSentinel) {
		c.nodes[splitNodeID].prefixOff = oldOff
		c.nodes[splitNodeID].prefixLen = uint16(lcp)
		c.nodes[splitNodeID].firstByte = oldFirst
		c.nodes[splitNodeID].prefixFlags = oldFlags & prefixFlagPinned

		remOff := oldOff + uint32(lcp)
		c.nodes[childID].prefixOff = remOff
		c.nodes[childID].prefixLen = uint16(remLen)
		c.nodes[childID].firstByte = c.prefixArena[remOff]
		return
	}

	c.nodes[childID].prefixOff = 0
	c.nodes[childID].prefixLen = 0
	c.nodes[childID].firstByte = 0
	c.nodes[childID].prefixFlags &^= prefixFlagPinned
	c.setNodePrefixFromArenaSliceLocked(splitNodeID, oldOff, lcp)
	c.setNodePrefixFromArenaSliceLocked(childID, oldOff+uint32(lcp), remLen)
	c.releasePrefixSpanLocked(oldOff, oldLen, wasLarge, oldFlags)
}

func (c *arenaRadix[V]) mergeNodePrefixesLocked(currID, onlyChildID uint32) {
	curr := &c.nodes[currID]
	onlyChild := &c.nodes[onlyChildID]
	if curr.prefixLen < largePrefixSentinel && onlyChild.prefixLen < largePrefixSentinel {
		combined := int(curr.prefixLen) + int(onlyChild.prefixLen)
		if combined < int(largePrefixSentinel) && curr.prefixOff+uint32(curr.prefixLen) == onlyChild.prefixOff {
			onlyChild.prefixOff = curr.prefixOff
			onlyChild.prefixLen = uint16(combined)
			onlyChild.firstByte = curr.firstByte
			onlyChild.prefixFlags |= curr.prefixFlags & prefixFlagPinned
			curr.prefixOff = 0
			curr.prefixLen = 0
			curr.firstByte = 0
			curr.prefixFlags = 0
			return
		}
		if combined <= 64 {
			var buf [64]byte
			copy(buf[:], c.nodePrefix(currID))
			copy(buf[curr.prefixLen:], c.nodePrefix(onlyChildID))
			pinned := (curr.prefixFlags | onlyChild.prefixFlags) & prefixFlagPinned
			c.clearNodePrefix(onlyChildID)
			c.clearNodePrefix(currID)
			c.setNodePrefix(onlyChildID, unsafe.String(&buf[0], combined))
			c.nodes[onlyChildID].prefixFlags |= pinned
			return
		}
	}

	len1 := c.nodePrefixLen(currID)
	len2 := c.nodePrefixLen(onlyChildID)
	totalLen := len1 + len2
	needed := totalLen
	if totalLen >= int(largePrefixSentinel) {
		needed += 4
	}
	c.ensurePrefixArenaCap(needed)
	if len(c.prefixArena)+needed > cap(c.prefixArena) {
		newCap := max(cap(c.prefixArena)*2, len(c.prefixArena)+needed, 64)
		grown := make([]byte, len(c.prefixArena), newCap)
		copy(grown, c.prefixArena)
		c.prefixArena = grown
	}
	off1 := c.nodes[currID].prefixOff
	off2 := c.nodes[onlyChildID].prefixOff
	large1 := c.nodes[currID].prefixLen == largePrefixSentinel
	large2 := c.nodes[onlyChildID].prefixLen == largePrefixSentinel
	flags1 := c.nodes[currID].prefixFlags
	flags2 := c.nodes[onlyChildID].prefixFlags

	if totalLen >= int(largePrefixSentinel) {
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], uint32(totalLen))
		c.prefixArena = append(c.prefixArena, hdr[:]...)
	}
	newOff := uint32(len(c.prefixArena))
	c.prefixArena = append(c.prefixArena, c.prefixArena[off1:int(off1)+len1]...)
	c.prefixArena = append(c.prefixArena, c.prefixArena[off2:int(off2)+len2]...)

	c.nodes[currID].prefixOff = 0
	c.nodes[currID].prefixLen = 0
	c.nodes[currID].firstByte = 0
	c.nodes[currID].prefixFlags = 0

	c.nodes[onlyChildID].prefixOff = newOff
	if totalLen >= int(largePrefixSentinel) {
		c.nodes[onlyChildID].prefixLen = largePrefixSentinel
	} else {
		c.nodes[onlyChildID].prefixLen = uint16(totalLen)
	}
	c.nodes[onlyChildID].firstByte = c.prefixArena[newOff]
	c.nodes[onlyChildID].prefixFlags = (flags2 &^ prefixFlagPinned) | ((flags1 | flags2) & prefixFlagPinned)
	c.livePrefixBytes += totalLen

	c.releasePrefixSpanLocked(off2, len2, large2, flags2)
	c.releasePrefixSpanLocked(off1, len1, large1, flags1)
}

// arenaHashTag computes a non-zero 32-bit signature from a 64-bit maphash.
func arenaHashTag(h uint64) uint32 {
	t := uint32(h ^ (h >> 32))
	if t == 0 {
		return 1
	}
	return t
}

func hashSlotsCapFor(entries int) int {
	if entries <= 12 {
		return minHashSlotsCap
	}
	needed := (entries*5 + 3) / 4
	return 1 << bits.Len(uint(needed-1))
}

// slotBelongsToOtherHash reports whether slot holds a live node whose 64-bit key hash
// differs from keyHash but happens to share the same 32-bit arenaHashTag(keyHash).
func (c *arenaRadix[V]) slotBelongsToOtherHash(slot arenaHashSlot, keyHash uint64) bool {
	if slot.nodeID >= uint32(len(c.nodes)) || !c.nodes[slot.nodeID].hasValue() {
		return false
	}
	nodeHash := c.hashNodeKey(slot.nodeID)
	return nodeHash != keyHash && arenaHashTag(nodeHash) == slot.tag
}

func (c *arenaRadix[V]) lookupHashSlot(key string, keyHash uint64) (nodeID uint32, slotMatched, verified bool) {
	if len(c.hashSlots) == 0 {
		return nilNode, false, false
	}
	tag := arenaHashTag(keyHash)
	mask := c.hashMask
	idx := tag & mask
	for {
		slot := c.hashSlots[idx]
		if slot.tag == 0 {
			return nilNode, slotMatched, false
		}
		if slot.tag == tag {
			id := slot.nodeID
			if id < uint32(len(c.nodes)) && c.nodes[id].hasValue() && c.verifyKey(id, key) {
				return id, true, true
			}
			if !c.slotBelongsToOtherHash(slot, keyHash) {
				slotMatched = true
			}
		}
		idx = (idx + 1) & mask
	}
}

func (c *arenaRadix[V]) findHashSlotIndex(keyHash uint64) (uint32, bool) {
	if len(c.hashSlots) == 0 {
		return 0, false
	}
	tag := arenaHashTag(keyHash)
	mask := c.hashMask
	idx := tag & mask
	for {
		slot := c.hashSlots[idx]
		if slot.tag == 0 {
			return idx, false
		}
		if slot.tag == tag && !c.slotBelongsToOtherHash(slot, keyHash) {
			return idx, true
		}
		idx = (idx + 1) & mask
	}
}

func (c *arenaRadix[V]) nodeMapGet(keyHash uint64) (uint32, bool) {
	if idx, ok := c.findHashSlotIndex(keyHash); ok {
		return c.hashSlots[idx].nodeID, true
	}
	return 0, false
}

func (c *arenaRadix[V]) nodeMapAt(keyHash uint64) uint32 {
	id, _ := c.nodeMapGet(keyHash)
	return id
}

func (c *arenaRadix[V]) nodeMapSet(keyHash uint64, nodeID uint32) {
	if (c.hashCount+1)*5 > len(c.hashSlots)*4 {
		c.growHashSlotsLocked(hashSlotsCapFor(c.hashCount + 1))
	}
	idx, exists := c.findHashSlotIndex(keyHash)
	tag := arenaHashTag(keyHash)
	c.hashSlots[idx] = arenaHashSlot{
		tag:    tag,
		nodeID: nodeID,
	}
	if nodeID < uint32(len(c.nodes)) {
		c.nodes[nodeID].setHashTag21(tag)
	}
	if !exists {
		c.hashCount++
	}
}

func (c *arenaRadix[V]) nodeMapDelete(keyHash uint64) {
	if idx, ok := c.findHashSlotIndex(keyHash); ok {
		c.backshiftDeleteSlot(idx)
	}
}

func (c *arenaRadix[V]) nodeMapLen() int {
	return c.hashCount
}

func (c *arenaRadix[V]) insertNewHashSlotLocked(keyHash uint64, nodeID uint32) {
	if (c.hashCount+1)*5 > len(c.hashSlots)*4 {
		c.growHashSlotsLocked(hashSlotsCapFor(c.hashCount + 1))
	}
	tag := arenaHashTag(keyHash)
	mask := c.hashMask
	idx := tag & mask
	for c.hashSlots[idx].tag != 0 {
		idx = (idx + 1) & mask
	}
	c.hashSlots[idx] = arenaHashSlot{tag: tag, nodeID: nodeID}
	c.nodes[nodeID].setHashTag21(tag)
	c.hashCount++
}

func (c *arenaRadix[V]) growHashSlotsLocked(newCap int) {
	newCap = max(newCap, minHashSlotsCap)
	newSlots := make([]arenaHashSlot, newCap)
	newMask := uint32(newCap - 1)
	for _, slot := range c.hashSlots {
		if slot.tag == 0 {
			continue
		}
		idx := slot.tag & newMask
		for newSlots[idx].tag != 0 {
			idx = (idx + 1) & newMask
		}
		newSlots[idx] = slot
	}
	c.hashSlots = newSlots
	c.hashMask = newMask
}

func (c *arenaRadix[V]) backshiftDeleteSlot(delIdx uint32) {
	mask := c.hashMask
	emptyIdx := delIdx
	currIdx := (emptyIdx + 1) & mask
	for {
		slot := c.hashSlots[currIdx]
		if slot.tag == 0 {
			c.hashSlots[emptyIdx] = arenaHashSlot{}
			c.hashCount--
			return
		}
		homeIdx := slot.tag & mask
		if ((emptyIdx - homeIdx) & mask) <= ((currIdx - homeIdx) & mask) {
			c.hashSlots[emptyIdx] = slot
			emptyIdx = currIdx
		}
		currIdx = (currIdx + 1) & mask
	}
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

// fillNodeKeyBuf copies the ancestor prefixes of nodeID backward into buf[:totalLen].
// It returns true if the ancestor chain reaches c.root and its prefixes sum to totalLen.
func (c *arenaRadix[V]) fillNodeKeyBuf(nodeID uint32, totalLen int, buf []byte) bool {
	pos := totalLen
	for nodeID != c.root && nodeID != nilNode {
		p := c.nodePrefix(nodeID)
		if len(p) > pos {
			return false
		}
		pos -= len(p)
		copy(buf[pos:], p)
		nodeID = c.nodes[nodeID].parent
	}
	return pos == 0 && nodeID == c.root
}

// reconstructKey reconstructs the full key for nodeID by walking the ancestor chain up to c.root.
// It is invoked when onEvictEntry is non-nil (and the caller does not already have the key in hand)
// or during All() / Keys() iteration.
func (c *arenaRadix[V]) reconstructKey(nodeID uint32) string {
	if nodeID == nilNode || nodeID == c.root {
		return ""
	}
	if c.nodes[nodeID].parent == c.root {
		return c.nodePrefix(nodeID)
	}
	if totalLen := c.nodes[nodeID].keyLen(); totalLen > 0 && totalLen < int(nodeKeyLenMask) {
		buf := make([]byte, totalLen)
		if c.fillNodeKeyBuf(nodeID, totalLen, buf) {
			return unsafe.String(unsafe.SliceData(buf), totalLen)
		}
	}
	exactLen := 0
	for curr := nodeID; curr != c.root && curr != nilNode; curr = c.nodes[curr].parent {
		exactLen += c.nodePrefixLen(curr)
	}
	buf := make([]byte, exactLen)
	_ = c.fillNodeKeyBuf(nodeID, exactLen, buf)
	return unsafe.String(unsafe.SliceData(buf), exactLen)
}

// hashKey computes the 64-bit hardware-accelerated maphash of key using the cache's seed.
func (c *arenaRadix[V]) hashKey(key string) uint64 {
	return maphash.String(c.seed, key)
}

// hashNodeKey computes the 64-bit maphash of the full key for nodeID.
// Depth-1 children of root hash their prefix directly in O(1); deeper nodes
// copy ancestor prefixes into a stack buffer (zero heap allocations)
// or stream ancestor prefixes through maphash.Hash for keys exceeding 512 bytes.
func (c *arenaRadix[V]) hashNodeKey(nodeID uint32) uint64 {
	if nodeID == nilNode || nodeID == c.root {
		return c.hashKey("")
	}
	p1 := c.nodes[nodeID].parent
	if p1 == c.root {
		return c.hashKey(c.nodePrefix(nodeID))
	}
	totalLen := c.nodes[nodeID].keyLen()
	if totalLen > 0 && totalLen <= 64 {
		var buf [64]byte
		s0 := c.nodePrefix(nodeID)
		if p1 != nilNode && c.nodes[p1].parent == c.root {
			s1 := c.nodePrefix(p1)
			if len(s1)+len(s0) == totalLen {
				copy(buf[:], s1)
				copy(buf[len(s1):], s0)
				return c.hashKey(unsafe.String(&buf[0], totalLen))
			}
		}
		if c.fillNodeKeyBuf(nodeID, totalLen, buf[:totalLen]) {
			return c.hashKey(unsafe.String(&buf[0], totalLen))
		}
	}
	return c.hashNodeKeyLarge(nodeID, totalLen)
}

// hashNodeKeyLarge hashes keys for nodes whose totalLen exceeds the 64-byte fast-path stack buffer
// or whose stored keyLen() does not match the ancestor prefix sum.
func (c *arenaRadix[V]) hashNodeKeyLarge(nodeID uint32, totalLen int) uint64 {
	if totalLen > 0 && totalLen <= 512 {
		var buf [512]byte
		if c.fillNodeKeyBuf(nodeID, totalLen, buf[:totalLen]) {
			return c.hashKey(unsafe.String(&buf[0], totalLen))
		}
	}
	var stackBuf [64]uint32
	path := stackBuf[:0]
	for curr := nodeID; curr != c.root && curr != nilNode; curr = c.nodes[curr].parent {
		path = append(path, curr)
	}
	var h maphash.Hash
	h.SetSeed(c.seed)
	for _, id := range slices.Backward(path) {
		_, _ = h.WriteString(c.nodePrefix(id))
	}
	return h.Sum64()
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
	c.clearNodePrefix(id)
	n := &c.nodes[id]
	n.prefixFlags = 0
	n.keyLenAndFlags = 0
	n.value = zero
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
	nodes := c.nodes
	for curr := nodes[nID].child; curr != nilNode; curr = nodes[curr].sibling {
		c0 := nodes[curr].firstByte
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

// addChild adds a child node under nID, preserving lexicographical order by firstByte.
func (c *arenaRadix[V]) addChild(nID uint32, newChildID uint32) {
	c.nodes[newChildID].parent = nID
	newFirstByte := c.nodes[newChildID].firstByte
	pcurr := &c.nodes[nID].child
	for *pcurr != nilNode && c.nodes[*pcurr].firstByte < newFirstByte {
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
	offset := 0
	kl := uint32(min(len(key), int(nodeKeyLenMask)))

	for {
		if len(search) == 0 {
			c.nodes[nodeID].value = value
			c.nodes[nodeID].setHasValue(true)
			return nodeID
		}

		childID := c.getChild(nodeID, search[0])
		if childID == nilNode {
			newLeafID := c.allocateNode()
			c.setNodePrefix(newLeafID, search)
			c.nodes[newLeafID].value = value
			c.nodes[newLeafID].keyLenAndFlags = kl | nodeHasValueBit
			c.addChild(nodeID, newLeafID)
			return newLeafID
		}

		childPrefix := c.nodePrefix(childID)
		lcp := longestCommonPrefix(search, childPrefix)

		if lcp == len(childPrefix) {
			search = search[lcp:]
			offset += lcp
			nodeID = childID
			continue
		}

		// Split childID's prefix in prefixArena into splitNodeID ([:lcp]) and childID ([lcp:]).
		splitEnd := offset + lcp
		splitNodeID := c.allocateNode()
		c.splitNodePrefixLocked(childID, splitNodeID, lcp)
		c.nodes[splitNodeID].setKeyLen(splitEnd)
		c.nodes[splitNodeID].parent = nodeID

		c.replaceChild(nodeID, childID, splitNodeID)
		c.addChild(splitNodeID, childID)

		if lcp == len(search) {
			c.nodes[splitNodeID].value = value
			c.nodes[splitNodeID].setHasValue(true)
			return splitNodeID
		}

		newLeafID := c.allocateNode()
		c.setNodePrefix(newLeafID, search[lcp:])
		c.nodes[newLeafID].value = value
		c.nodes[newLeafID].keyLenAndFlags = kl | nodeHasValueBit
		c.addChild(splitNodeID, newLeafID)
		return newLeafID
	}
}

// verifyKey verifies whether nodeID represents the exact full key by comparing keyLen()
// and walking upward from nodeID to c.root matching each node's prefix suffix-first.
func (c *arenaRadix[V]) verifyKey(nodeID uint32, key string) bool {
	n := &c.nodes[nodeID]
	expectedKL := uint32(min(len(key), int(nodeKeyLenMask)))
	if (n.keyLenAndFlags & nodeKeyLenMask) != expectedKL {
		return false
	}
	if n.parent == c.root {
		return key == c.nodePrefix(nodeID)
	}
	end := len(key)
	for nodeID != c.root && nodeID != nilNode {
		p := c.nodePrefix(nodeID)
		if end < len(p) || key[end-len(p):end] != p {
			return false
		}
		end -= len(p)
		nodeID = c.nodes[nodeID].parent
	}
	return end == 0 && nodeID == c.root
}

func (c *arenaRadix[V]) findNodeByTrieWalk(key string) (uint32, bool) {
	curr := c.root
	search := key

	for curr != nilNode {
		if len(search) == 0 {
			if c.nodes[curr].hasValue() {
				return curr, true
			}
			return nilNode, false
		}

		childID := c.getChild(curr, search[0])
		if childID == nilNode {
			return nilNode, false
		}

		prefix := c.nodePrefix(childID)
		if !strings.HasPrefix(search, prefix) {
			return nilNode, false
		}

		search = search[len(prefix):]
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
	if id, slotMatched, verified := c.lookupHashSlot(key, keyHash); verified {
		return id, false, true
	} else if !slotMatched && c.hashCount+c.collisionCount == c.len {
		// By Invariant 6 and the Pigeonhole Principle, when c.hashCount+c.collisionCount == c.len,
		// every live value-bearing node's hash is present in c.hashSlots,
		// so a hash slot miss is a guaranteed cache miss.
		return nilNode, false, false
	}
	curr, ok := c.walkNodeKeyFallback(key, keyHash, false)
	return curr, true, ok
}

// lookupNodeKeyWithHash finds a value-bearing node index for key using a precomputed maphash
// without mutating c.hashSlots.
func (c *arenaRadix[V]) lookupNodeKeyWithHash(key string, keyHash uint64) (uint32, bool) {
	if id, slotMatched, verified := c.lookupHashSlot(key, keyHash); verified {
		return id, true
	} else if !slotMatched && c.hashCount+c.collisionCount == c.len {
		return nilNode, false
	}
	return c.walkNodeKeyFallback(key, keyHash, false)
}

// getNodeKeyWithHash finds a value-bearing node index for key under exclusive write lock,
// healing c.hashSlots if resolved via the slow-path trie walk.
func (c *arenaRadix[V]) getNodeKeyWithHash(key string, keyHash uint64) (uint32, bool) {
	nodeID, found, _ := c.getNodeKeyForPut(key, keyHash)
	return nodeID, found
}

func (c *arenaRadix[V]) getNodeKeyForPut(key string, keyHash uint64) (nodeID uint32, found, mapMiss bool) {
	if id, slotMatched, verified := c.lookupHashSlot(key, keyHash); verified {
		return id, true, false
	} else if !slotMatched && c.hashCount+c.collisionCount == c.len {
		return nilNode, false, true
	}
	id, ok := c.walkNodeKeyFallback(key, keyHash, true)
	return id, ok, false
}

// compressPathUpwards walks up the tree, pruning empty leaf nodes and merging single-child intermediate routing nodes.
func (c *arenaRadix[V]) compressPathUpwards(currID uint32) {
	for currID != nilNode && currID != c.root {
		if c.nodes[currID].hasValue() {
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
			c.mergeNodePrefixesLocked(currID, onlyChildID)
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
		(c.len < c.peakEntryLen || c.deletedSinceCompact >= minChurnCompactDeletes || c.hashCount+c.collisionCount < c.len)
}

func (c *arenaRadix[V]) isDirtyLocked() bool {
	return c.freeHead != nilNode || c.hasContiguousSlackLocked()
}

func (c *arenaRadix[V]) deleteNodeFromHashSlotsFastLocked(nodeID uint32) bool {
	if len(c.hashSlots) == 0 {
		return false
	}
	tag21 := c.nodes[nodeID].hashTag21()
	if tag21 == 0 {
		return false
	}
	mask := c.hashMask
	idx := tag21 & mask
	for {
		slot := c.hashSlots[idx]
		if slot.tag == 0 {
			return false
		}
		if slot.nodeID == nodeID && (slot.tag&hashTag21Mask) == tag21 {
			c.backshiftDeleteSlot(idx)
			return true
		}
		idx = (idx + 1) & mask
	}
}

func (c *arenaRadix[V]) finishEraseNodeLocked(evictQ *evictCallbackQueue[V], nodeID uint32, key string, reason EvictionReason) (V, bool) {
	deletedEntry := c.nodes[nodeID].value
	evictedSize := c.nodes[nodeID].size
	c.onEntryDeleted(evictedSize)
	c.nodeMapDirty = true
	c.currentSize -= evictedSize
	c.nodes[nodeID].size = 0

	c.remove(nodeID)
	var zero V
	c.nodes[nodeID].value = zero
	c.nodes[nodeID].setHasValue(false)
	c.compressPathUpwards(nodeID)

	c.notifyEvict(evictQ, key, deletedEntry, evictedSize, reason)
	return deletedEntry, true
}

// eraseInternal handles unlinking from LRU, cleaning up hashSlots, and deleting from the tree
// when the caller does not already have the key or hash in hand.
func (c *arenaRadix[V]) eraseInternal(evictQ *evictCallbackQueue[V], nodeID uint32, reason EvictionReason) (V, bool) {
	if nodeID == nilNode || !c.nodes[nodeID].hasValue() {
		var zero V
		return zero, false
	}
	if c.onEvictEntry != nil {
		if c.nodes[nodeID].parent == c.root {
			c.nodes[nodeID].prefixFlags |= prefixFlagPinned
		}
		key := c.reconstructKey(nodeID)
		return c.eraseInternalWithHash(evictQ, nodeID, c.hashKey(key), key, reason)
	}
	if c.collisionCount == 0 && c.hashMask <= hashTag21Mask && c.deleteNodeFromHashSlotsFastLocked(nodeID) {
		return c.finishEraseNodeLocked(evictQ, nodeID, "", reason)
	}
	return c.eraseInternalWithHash(evictQ, nodeID, c.hashNodeKey(nodeID), "", reason)
}

func (c *arenaRadix[V]) eraseInternalWithHash(evictQ *evictCallbackQueue[V], nodeID uint32, hash uint64, key string, reason EvictionReason) (V, bool) {
	if nodeID == nilNode || !c.nodes[nodeID].hasValue() {
		var zero V
		return zero, false
	}
	c.removeOrPromoteNodeMapLocked(hash, nodeID)
	return c.finishEraseNodeLocked(evictQ, nodeID, key, reason)
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
	c.livePrefixBytes = 0
	c.freePrefixCount = 0
	if !c.prefixExposed.Load() && (c.onEvictEntry == nil || len(c.prefixArena) == 0) && cap(c.prefixArena) <= 4096 && c.peakEntryLen <= minPeakSlackEntries && c.deletedSinceCompact < minChurnCompactDeletes {
		c.prefixArena = c.prefixArena[:0]
	} else {
		c.prefixArena = nil
		c.prefixExposed.Store(false)
	}
	if len(c.hashSlots) == minHashSlotsCap || (len(c.hashSlots) > 0 && c.peakEntryLen <= minPeakSlackEntries && c.deletedSinceCompact < minChurnCompactDeletes) {
		clear(c.hashSlots)
	} else {
		c.hashSlots = make([]arenaHashSlot, minHashSlotsCap)
		c.hashMask = minHashSlotsCap - 1
	}
	c.hashCount = 0
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

func (c *arenaRadix[V]) compactPrefixArenaLocked(extraCap int) {
	newCap := max(cap(c.prefixArena), (c.livePrefixBytes+extraCap)*2, 64)
	c.rebuildPrefixArenaWithCapLocked(newCap)
}

func (c *arenaRadix[V]) compactPrefixArenaExactLocked() {
	exactBytes := c.livePrefixBytes
	for id := range uint32(len(c.nodes)) {
		if (id == c.root || c.nodes[id].parent != nilNode) && c.nodes[id].prefixLen == largePrefixSentinel {
			exactBytes += 4
		}
	}
	if exactBytes == 0 {
		c.prefixArena = nil
		c.freePrefixCount = 0
		c.prefixExposed.Store(false)
		return
	}
	c.rebuildPrefixArenaWithCapLocked(exactBytes)
}

func (c *arenaRadix[V]) rebuildPrefixArenaWithCapLocked(newCap int) {
	newArena := make([]byte, 0, newCap)
	for id := range uint32(len(c.nodes)) {
		if id != c.root && c.nodes[id].parent == nilNode {
			continue
		}
		n := &c.nodes[id]
		if n.prefixLen == 0 {
			n.prefixOff = 0
			n.prefixFlags &^= prefixFlagPinned
			continue
		}
		p := c.nodePrefix(id)
		if n.prefixLen == largePrefixSentinel {
			var hdr [4]byte
			binary.LittleEndian.PutUint32(hdr[:], uint32(len(p)))
			newArena = append(newArena, hdr[:]...)
		}
		off := uint32(len(newArena))
		newArena = append(newArena, p...)
		n.prefixOff = off
		n.prefixFlags &^= prefixFlagPinned
	}
	c.prefixArena = newArena
	c.freePrefixCount = 0
	c.prefixExposed.Store(false)
}

func (c *arenaRadix[V]) rebuildHashSlotsLocked() {
	newCap := hashSlotsCapFor(c.len)
	newSlots := make([]arenaHashSlot, newCap)
	newMask := uint32(newCap - 1)
	newCount := 0
	collisions := 0
	var newCollisionPeers map[uint64][]uint32

	c.hashSlots = newSlots
	c.hashMask = newMask
	c.hashCount = 0

	for id := range uint32(len(c.nodes)) {
		if !c.nodes[id].hasValue() {
			continue
		}
		h := c.hashNodeKey(id)
		tag := arenaHashTag(h)
		c.nodes[id].setHashTag21(tag)
		idx, exists := c.findHashSlotIndex(h)
		if exists {
			prevID := c.hashSlots[idx].nodeID
			collisions++
			if newCollisionPeers == nil {
				newCollisionPeers = make(map[uint64][]uint32)
			}
			newCollisionPeers[h] = append(newCollisionPeers[h], prevID)
			c.hashSlots[idx].nodeID = id
		} else {
			c.hashSlots[idx] = arenaHashSlot{
				tag:    tag,
				nodeID: id,
			}
			newCount++
			c.hashCount = newCount
		}
	}
	c.collisionPeers = newCollisionPeers
	c.collisionCount = collisions
	c.nodeMapDirty = false
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
	if c.deletedSinceCompact > 0 && c.livePrefixBytes < cap(c.prefixArena) {
		c.compactPrefixArenaExactLocked()
	}
	if c.nodeMapDirty {
		c.rebuildHashSlotsLocked()
	}
	c.onCompacted(c.len)
	return true
}

// compactDataStructuresLocked performs the physical slice and hash index compaction without updating
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

	// Allocate brand-new slice with len(newNodes) == cap(newNodes) == liveCount.
	newNodes := make([]arenaRadixNode[V], liveCount)
	for oldID := range oldLen {
		newID := oldToNew[oldID]
		if newID == nilNode {
			continue
		}
		oldNode := &c.nodes[oldID]
		newNodes[newID] = arenaRadixNode[V]{
			prefixOff:      oldNode.prefixOff,
			prefixLen:      oldNode.prefixLen,
			firstByte:      oldNode.firstByte,
			prefixFlags:    oldNode.prefixFlags,
			value:          oldNode.value,
			size:           oldNode.size,
			parent:         remap(oldNode.parent),
			child:          remap(oldNode.child),
			sibling:        remap(oldNode.sibling),
			prev:           remap(oldNode.prev),
			next:           remap(oldNode.next),
			keyLenAndFlags: oldNode.keyLenAndFlags,
		}
	}

	// Remap top-level arena indices, compact prefixArena, and rebuild hashSlots.
	c.root = remap(c.root)
	c.head = remap(c.head)
	c.tail = remap(c.tail)
	c.freeHead = nilNode
	c.freeCount = 0
	c.nodes = newNodes
	c.compactPrefixArenaExactLocked()
	c.rebuildHashSlotsLocked()
	c.onCompacted(c.len)
	return true
}

// compactLocked performs lossless O(N) compaction of the node arena slice, prefix arena, and hash lookup table.
// It eliminates all free-list slots so len(c.nodes) == cap(c.nodes) == liveCount and c.freeHead == nilNode,
// remaps all 8 uint32 index pointers (root, head, tail, parent, child, sibling, prev, next),
// and reallocates c.hashSlots while preserving 100% of live entries and LRU order.
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
	if protectedNodeID == foregroundNoProtect || (protectedNodeID != nilNode && !c.nodes[protectedNodeID].hasValue()) {
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
	prevID, ok := c.nodeMapGet(keyHash)
	if !ok {
		c.removeCollisionPeerLocked(keyHash, nodeID)
		c.nodeMapSet(keyHash, nodeID)
		return
	}
	if prevID == nodeID {
		return
	}
	prevValid := prevID < uint32(len(c.nodes)) && c.nodes[prevID].hasValue() && c.hashNodeKey(prevID) == keyHash
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
	c.nodeMapSet(keyHash, nodeID)
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
		if last != excludeID && last < uint32(len(c.nodes)) && c.nodes[last].hasValue() && c.hashNodeKey(last) == hash {
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
	if len(c.hashSlots) == 0 {
		c.removeCollisionPeerLocked(hash, nodeID)
		return
	}
	tag := arenaHashTag(hash)
	mask := c.hashMask
	idx := tag & mask
	for {
		slot := c.hashSlots[idx]
		if slot.tag == 0 {
			c.removeCollisionPeerLocked(hash, nodeID)
			return
		}
		if slot.tag == tag {
			if slot.nodeID == nodeID {
				if peerID, found := c.popValidCollisionPeerLocked(hash, nodeID); found {
					c.hashSlots[idx].nodeID = peerID
					if peerID < uint32(len(c.nodes)) {
						c.nodes[peerID].setHashTag21(tag)
					}
					return
				}
				c.backshiftDeleteSlot(idx)
				if c.collisionCount > 0 && len(c.collisionPeers) == 0 {
					c.collisionCount = 0
				}
				return
			}
			if !c.slotBelongsToOtherHash(slot, hash) {
				c.removeCollisionPeerLocked(hash, nodeID)
				return
			}
		}
		idx = (idx + 1) & mask
	}
}
