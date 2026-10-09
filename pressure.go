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
	"bytes"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	goroutineStackBufPool = sync.Pool{
		New: func() any {
			return new([4096]byte)
		},
	}
	allGoroutineStacksBufPool = sync.Pool{
		New: func() any {
			return new([16384]byte)
		},
	}
)

func parseDecimalUint64(b []byte) (uint64, int) {
	var id uint64
	var consumed int
	for consumed < len(b) {
		c := b[consumed]
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + uint64(c-'0')
		consumed++
	}
	return id, consumed
}

func captureCurrentGoroutineStack() ([]byte, *[4096]byte) {
	bufPtr := goroutineStackBufPool.Get().(*[4096]byte)
	n := runtime.Stack(bufPtr[:], false)
	if n < len(bufPtr) {
		return bufPtr[:n], bufPtr
	}
	goroutineStackBufPool.Put(bufPtr)
	const maxSingleStackSize = 1 << 20
	for size := 8192; size <= maxSingleStackSize; size *= 2 {
		buf := make([]byte, size)
		n = runtime.Stack(buf, false)
		if n < len(buf) || size == maxSingleStackSize {
			return buf[:n], nil
		}
	}
	return nil, nil
}

func releaseCurrentGoroutineStack(bufPtr *[4096]byte) {
	if bufPtr != nil {
		goroutineStackBufPool.Put(bufPtr)
	}
}

func parseStackGoroutineAndParentID(stack []byte) (gid, parentGID uint64) {
	gid, parentGID, _ = parseStackGoroutineParentAndCreator(stack)
	return gid, parentGID
}

func parseStackGoroutineParentAndCreator(stack []byte) (gid, parentGID uint64, creatorFunc []byte) {
	const prefix = "goroutine "
	if len(stack) > len(prefix) {
		gid, _ = parseDecimalUint64(stack[len(prefix):])
	}
	const createdIn = " in goroutine "
	if idx := bytes.LastIndex(stack, []byte(createdIn)); idx >= 0 {
		parentGID, _ = parseDecimalUint64(stack[idx+len(createdIn):])
		const createdBy = "\ncreated by "
		if cIdx := bytes.LastIndex(stack[:idx], []byte(createdBy)); cIdx >= 0 {
			creatorFunc = stack[cIdx+len(createdBy) : idx]
		}
	}
	return gid, parentGID, creatorFunc
}

func captureAllGoroutineStacks() ([]byte, *[16384]byte) {
	bufPtr := allGoroutineStacksBufPool.Get().(*[16384]byte)
	n := runtime.Stack(bufPtr[:], true)
	if n < len(bufPtr) {
		return bufPtr[:n], bufPtr
	}
	allGoroutineStacksBufPool.Put(bufPtr)
	const maxAllStacksSize = 16 << 20
	for size := 32768; size <= maxAllStacksSize; size *= 2 {
		buf := make([]byte, size)
		n = runtime.Stack(buf, true)
		if n < len(buf) || size == maxAllStacksSize {
			return buf[:n], nil
		}
	}
	return nil, nil
}

func releaseAllGoroutineStacks(bufPtr *[16384]byte) {
	if bufPtr != nil {
		allGoroutineStacksBufPool.Put(bufPtr)
	}
}

func nextGoroutineBlock(rem []byte) (blk, rest []byte) {
	for len(rem) > 0 {
		if idx := bytes.Index(rem, []byte("\n\n")); idx >= 0 {
			blk = rem[:idx]
			rest = rem[idx+2:]
		} else {
			blk = rem
			rest = nil
		}
		if len(blk) > 0 {
			return blk, rest
		}
		rem = rest
	}
	return nil, nil
}

func containsCreatorCallFrame(stackPrefix, creatorFunc []byte) bool {
	if len(creatorFunc) == 0 {
		return false
	}
	if plusIdx := bytes.IndexByte(creatorFunc, '+'); plusIdx > 0 {
		creatorFunc = creatorFunc[:plusIdx]
	}
	search := stackPrefix
	for len(search) > len(creatorFunc) {
		idx := bytes.Index(search, creatorFunc)
		if idx < 0 {
			return false
		}
		afterIdx := idx + len(creatorFunc)
		if idx > 0 && search[idx-1] == '\n' && afterIdx < len(search) && (search[afterIdx] == '(' || search[afterIdx] == '[') {
			return true
		}
		search = search[idx+1:]
	}
	return false
}

func hasCallFrameAboveHook(aboveHook []byte) bool {
	firstNL := bytes.IndexByte(aboveHook, '\n')
	lastNL := bytes.LastIndexByte(aboveHook, '\n')
	return firstNL >= 0 && lastNL > firstNL
}

func isClosureSuffix(suffix []byte) bool {
	if bytes.HasPrefix(suffix, []byte("func")) {
		return true
	}
	if len(suffix) == 0 {
		return false
	}
	for _, b := range suffix {
		if b < '0' || b > '9' {
			return false
		}
	}
	return true
}

func trimClosureOffset(fn []byte) []byte {
	if plusIdx := bytes.IndexByte(fn, '+'); plusIdx > 0 {
		return fn[:plusIdx]
	}
	return fn
}

func findClosureOwnerInStack(blk, creatorFunc []byte) []byte {
	curr := trimClosureOffset(creatorFunc)
	for {
		dot := bytes.LastIndexByte(curr, '.')
		if dot <= 0 || !isClosureSuffix(curr[dot+1:]) {
			return nil
		}
		curr = curr[:dot]
		if containsCreatorCallFrame(blk, curr) {
			return curr
		}
	}
}

func trimFrameFuncName(fnLine []byte) []byte {
	for i := 0; i < len(fnLine); i++ {
		if fnLine[i] == '.' && i+2 < len(fnLine) && fnLine[i+1] == '(' && fnLine[i+2] == '*' {
			if closeIdx := bytes.IndexByte(fnLine[i+3:], ')'); closeIdx >= 0 {
				i += 3 + closeIdx
				continue
			}
		}
		if fnLine[i] == '[' {
			if closeIdx := bytes.IndexByte(fnLine[i+1:], ']'); closeIdx >= 0 {
				i += 1 + closeIdx
				continue
			}
		}
		if fnLine[i] == '(' {
			return fnLine[:i]
		}
	}
	return fnLine
}

func popBottomCallFrame(frames []byte) (fnName, rest []byte) {
	tabNL := bytes.LastIndex(frames, []byte("\n\t"))
	if tabNL <= 0 {
		return nil, nil
	}
	fnNL := bytes.LastIndexByte(frames[:tabNL], '\n')
	if fnNL < 0 {
		return nil, nil
	}
	return trimFrameFuncName(frames[fnNL+1 : tabNL]), frames[:fnNL]
}

func matchChildStackFramesAboveHook(blk, aboveHook, belowHook, childStack []byte) (matched, decided bool) {
	const createdBy = "\ncreated by "
	end := bytes.LastIndex(childStack, []byte(createdBy))
	if end <= 0 {
		return false, false
	}
	rest := childStack[:end]
	sawBelowOwner := false
	const maxBottomFrames = 4
	for range maxBottomFrames {
		var fn []byte
		fn, rest = popBottomCallFrame(rest)
		if len(fn) == 0 {
			break
		}
		if bytes.Equal(fn, []byte("sync.(*WaitGroup).Go.func1")) {
			continue
		}
		if containsCreatorCallFrame(aboveHook, fn) {
			return true, true
		}
		if owner := findClosureOwnerInStack(blk, fn); len(owner) > 0 {
			if containsCreatorCallFrame(aboveHook, owner) {
				return true, true
			}
			if containsCreatorCallFrame(belowHook, owner) {
				sawBelowOwner = true
			}
		}
	}
	if sawBelowOwner {
		return false, true
	}
	return false, false
}

func isCreatorFromCallbackAboveHook(blk []byte, hookIdx int, currCreator, childStack []byte, requirePositiveMatch bool) bool {
	aboveHook := blk[:hookIdx]
	if !hasCallFrameAboveHook(aboveHook) {
		return false
	}
	belowHook := blk[hookIdx:]
	if matched, decided := matchChildStackFramesAboveHook(blk, aboveHook, belowHook, childStack); decided {
		return matched
	}
	if len(currCreator) == 0 {
		return false
	}
	if containsCreatorCallFrame(aboveHook, currCreator) {
		return true
	}
	if containsCreatorCallFrame(belowHook, currCreator) {
		return false
	}
	if owner := findClosureOwnerInStack(blk, currCreator); len(owner) > 0 {
		return containsCreatorCallFrame(aboveHook, owner)
	}
	if requirePositiveMatch {
		return false
	}
	if bytes.Equal(currCreator, []byte("sync.(*WaitGroup).Go")) {
		return bytes.Contains(aboveHook, []byte("\nsync.(*WaitGroup)."))
	}
	return false
}

func matchAncestorInAllStacks(allStacks, currCreator, childStack, hookName []byte, matchAncestor func(uint64) bool) (uint64, bool) {
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		gid, _ := parseStackGoroutineAndParentID(blk)
		if gid == 0 || !matchAncestor(gid) {
			continue
		}
		hookIdx := bytes.LastIndex(blk, hookName)
		if hookIdx >= 0 && isCreatorFromCallbackAboveHook(blk, hookIdx, currCreator, childStack, true) {
			return gid, true
		}
	}
	return 0, false
}

func findAncestorCreatorAboveHook(allStacks, callerStack []byte, parentGID uint64, creatorFunc, hookName []byte, matchAncestor func(uint64) bool) (uint64, bool) {
	if parentGID == 0 {
		return 0, false
	}
	if len(allStacks) == 0 {
		var bufPtr *[16384]byte
		allStacks, bufPtr = captureAllGoroutineStacks()
		defer releaseAllGoroutineStacks(bufPtr)
	}
	currGID := parentGID
	currCreator := creatorFunc
	currChildStack := callerStack
	const maxAncestorHops = 8
	for range maxAncestorHops {
		if currGID == 0 {
			return 0, false
		}
		foundBlock := false
		for rem := allStacks; len(rem) > 0; {
			var blk []byte
			blk, rem = nextGoroutineBlock(rem)
			gid, nextParentGID, nextCreator := parseStackGoroutineParentAndCreator(blk)
			if gid != currGID {
				continue
			}
			foundBlock = true
			if matchAncestor(currGID) {
				hookIdx := bytes.LastIndex(blk, hookName)
				if hookIdx >= 0 && isCreatorFromCallbackAboveHook(blk, hookIdx, currCreator, currChildStack, false) {
					return currGID, true
				}
				return 0, false
			}
			currGID = nextParentGID
			currCreator = nextCreator
			currChildStack = blk
			break
		}
		if !foundBlock {
			return matchAncestorInAllStacks(allStacks, currCreator, currChildStack, hookName, matchAncestor)
		}
	}
	return 0, false
}

const numPressureHookTags = 8

var (
	nextPressureHookID  atomic.Uint64
	fastPrimaryTagOwner [numPressureHookTags]atomic.Uint64
	fastEvictTagState   [numPressureHookTags]atomic.Uint64

	fastPrimarySamplerMarkers = [numPressureHookTags][]byte{
		[]byte("invokeFastPrimaryPressure0("),
		[]byte("invokeFastPrimaryPressure1("),
		[]byte("invokeFastPrimaryPressure2("),
		[]byte("invokeFastPrimaryPressure3("),
		[]byte("invokeFastPrimaryPressure4("),
		[]byte("invokeFastPrimaryPressure5("),
		[]byte("invokeFastPrimaryPressure6("),
		[]byte("invokeFastPrimaryPressure7("),
	}

	fastEvictCallbackMarkers = [numPressureHookTags][]byte{
		[]byte("deliverCallbacksFast0("),
		[]byte("deliverCallbacksFast1("),
		[]byte("deliverCallbacksFast2("),
		[]byte("deliverCallbacksFast3("),
		[]byte("deliverCallbacksFast4("),
		[]byte("deliverCallbacksFast5("),
		[]byte("deliverCallbacksFast6("),
		[]byte("deliverCallbacksFast7("),
	}
)

var (
	byteStrings     [256]string
	twoDigitStrings [100]string
)

func init() {
	var raw [256]byte
	for i := range 256 {
		raw[i] = byte(i)
	}
	all := string(raw[:])
	for i := range 256 {
		byteStrings[i] = all[i : i+1]
	}
	var digitsRaw [200]byte
	for i := range 100 {
		digitsRaw[i*2] = byte('0' + i/10)
		digitsRaw[i*2+1] = byte('0' + i%10)
	}
	allDigits := string(digitsRaw[:])
	for i := range 100 {
		twoDigitStrings[i] = allDigits[i*2 : i*2+2]
	}
}

// clonePrefix returns an independent copy of s that never shares s's underlying backing array,
// using preallocated 1-byte and 2-digit strings to avoid heap allocations.
func clonePrefix(s string) string {
	switch len(s) {
	case 0:
		return ""
	case 1:
		return byteStrings[s[0]]
	case 2:
		d0, d1 := s[0]-'0', s[1]-'0'
		if d0 < 10 && d1 < 10 {
			return twoDigitStrings[int(d0)*10+int(d1)]
		}
		return strings.Clone(s)
	default:
		return strings.Clone(s)
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

// pressureState encapsulates lock-free memory-pressure sampling, reclamation epoch tracking,
// and shared compaction/zero-size entry watermarks embedded across all three cache backends.
type pressureState struct {
	options Options

	peakEntryLen           int
	deletedSinceCompact    int
	zeroSizeCount          int
	lastReclaimedZeroCount int
	lastReclaimedLen       int
	hasValidSample         bool

	// Telemetry counters protected by the embedding cache's exclusive write lock (mu.Lock).
	getHits                  uint64
	getMisses                uint64
	evictionsCapacity        uint64
	evictionsPressure        uint64
	evictionsDeleted         uint64
	evictionsReplaced        uint64
	evictedWeightCapacity    uint64
	evictedWeightPressure    uint64
	evictedWeightDeleted     uint64
	evictedWeightReplaced    uint64
	putInserted              uint64
	putUpdated               uint64
	replaceUpdated           uint64
	replaceNotFound          uint64
	replaceSelfEvicted       uint64
	deleteDeleted            uint64
	deleteNotFound           uint64
	deletePrefixExecuted     uint64
	compactionsExplicit      uint64
	compactionsPressureTier1 uint64
	compactionsPressureTier2 uint64
	compactionsAutoSlack     uint64
	pressureShedsInline      uint64
	pressureShedsExplicit    uint64

	// Atomic telemetry fields mutated under RLock (Peek) or outside mu.Lock (oversized Put, pressure sampling).
	peekHits                atomic.Uint64
	peekMisses              atomic.Uint64
	putRejectedOversized    atomic.Uint64
	arenaHashFallbacks      atomic.Uint64
	lastSampledPressureBits atomic.Uint64

	pressureWriteMu          sync.Mutex
	hookIDVal                atomic.Uint64
	primaryTagHint           atomic.Uint32
	evictTagHint             atomic.Uint32
	samplingPressure         atomic.Bool
	samplingActive           atomic.Bool
	samplingContended        atomic.Bool
	fallbackSampling         atomic.Bool
	pressureInitialized      atomic.Bool
	pressureNeedsRefresh     atomic.Bool
	hasReentrantReclaimGIDs  atomic.Bool
	overflowSamplingCount    atomic.Int32
	samplingGID              atomic.Uint64
	samplingParentGID        atomic.Uint64
	fallbackGID              atomic.Uint64
	fallbackParentGID        atomic.Uint64
	nonDescendantParentGID   atomic.Uint64
	nonDescendantSeq         atomic.Uint64
	samplingReentrantReclaim atomic.Bool
	fallbackReentrantReclaim atomic.Bool
	overflowSamplingGIDs     sync.Map
	reentrantReclaimGIDs     sync.Map
	pressureSampleSeq        atomic.Uint64
	pressureInvokeSeq        atomic.Uint64
	pressureMaxStoredSeq     atomic.Uint64
	lastSampledSeq           atomic.Uint64
	cachedPressureBits       atomic.Uint64
	cachedPressureEpoch      atomic.Uint64
	reclaimEpoch             atomic.Uint64
	externalReclaimEpoch     atomic.Uint64
	evictCallbackMu          sync.Mutex
	evictCallbackGID         atomic.Uint64
	evictCallbackParentGID   atomic.Uint64
	evictCallbackActive      atomic.Bool
	evictCallbackContended   atomic.Bool
	evictPendingEnqueues     atomic.Int32
	evictCallbackResolveMu   sync.Mutex
	pendingEvictCallbacks    []func()
	samplingResolveMu        sync.Mutex
}

const (
	samplerSlotPrimary = iota
	samplerSlotFallback
	samplerSlotOverflow

	pressureSampleWindowMask   = 255
	minPeakSlackEntries        = 8
	minChurnCompactDeletes     = 64
	minSingleSurvivorFreeNodes = 63
	slackQuarterMultiplier     = 4
)

func (p *pressureState) hasOverflowSamplingGID(gid uint64) bool {
	if gid == 0 || p.overflowSamplingCount.Load() <= 0 {
		return false
	}
	_, ok := p.overflowSamplingGIDs.Load(gid)
	return ok
}

func (p *pressureState) isCurrentGoroutineSampling(gid uint64) bool {
	if gid == 0 {
		return false
	}
	return p.samplingGID.Load() == gid ||
		p.fallbackGID.Load() == gid ||
		p.hasOverflowSamplingGID(gid)
}

func (p *pressureState) hookID() uint64 {
	if id := p.hookIDVal.Load(); id != 0 {
		return id
	}
	id := nextPressureHookID.Add(1)
	if p.hookIDVal.CompareAndSwap(0, id) {
		initTag := uint32(id-1) & (numPressureHookTags - 1)
		p.primaryTagHint.Store(initTag)
		p.evictTagHint.Store(initTag)
		return id
	}
	return p.hookIDVal.Load()
}

func (p *pressureState) claimFastPrimaryTag() (uint32, bool) {
	id := p.hookID()
	base := p.primaryTagHint.Load() & (numPressureHookTags - 1)
	for offset := range uint32(numPressureHookTags) {
		tag := (base + offset) & (numPressureHookTags - 1)
		if fastPrimaryTagOwner[tag].CompareAndSwap(0, id) {
			if tag != base {
				p.primaryTagHint.Store(tag)
			}
			return tag, true
		}
	}
	return 0, false
}

func (p *pressureState) activeFastPrimaryTag() (uint32, bool) {
	id := p.hookIDVal.Load()
	if id == 0 {
		return 0, false
	}
	tag := p.primaryTagHint.Load() & (numPressureHookTags - 1)
	if fastPrimaryTagOwner[tag].Load() == id {
		return tag, true
	}
	return 0, false
}

func (p *pressureState) matchCandidatePrimarySamplerGID(allStacks []byte) uint64 {
	tag, ok := p.activeFastPrimaryTag()
	if !ok {
		return 0
	}
	marker := fastPrimarySamplerMarkers[tag]
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		if !bytes.Contains(blk, marker) {
			continue
		}
		gid, parentGID := parseStackGoroutineAndParentID(blk)
		if gid == 0 || gid == p.fallbackGID.Load() || p.hasOverflowSamplingGID(gid) {
			continue
		}
		if p.samplingActive.Load() {
			p.samplingParentGID.Store(parentGID)
			p.samplingGID.Store(gid)
			return gid
		}
	}
	return 0
}

func (p *pressureState) resolvePrimaryFromAllStacksLocked() (uint64, []byte, *[16384]byte) {
	p.samplingResolveMu.Lock()
	defer p.samplingResolveMu.Unlock()
	p.samplingContended.Store(true)
	if !p.samplingActive.Load() {
		if p.samplingGID.Load() == 0 {
			p.samplingContended.Store(false)
		}
		return 0, nil, nil
	}
	if sGID := p.samplingGID.Load(); sGID != 0 {
		return sGID, nil, nil
	}
	allStacks, bufPtr := captureAllGoroutineStacks()
	if gid := p.matchCandidatePrimarySamplerGID(allStacks); gid != 0 {
		return gid, allStacks, bufPtr
	}
	return p.samplingGID.Load(), allStacks, bufPtr
}

func (p *pressureState) inspectCurrentAndResolvePrimary() (gid, parentGID uint64, creatorFunc, callerStack []byte, callerBufPtr *[4096]byte, allStacks []byte, allBufPtr *[16384]byte) {
	callerStack, callerBufPtr = captureCurrentGoroutineStack()
	gid, parentGID, creatorFunc = parseStackGoroutineParentAndCreator(callerStack)
	fastTag, hasFastTag := p.activeFastPrimaryTag()
	isFastPrimaryCaller := hasFastTag && p.samplingActive.Load() &&
		bytes.Contains(callerStack, fastPrimarySamplerMarkers[fastTag]) &&
		gid != p.fallbackGID.Load() && !p.hasOverflowSamplingGID(gid)

	if p.samplingActive.Load() && p.samplingGID.Load() == 0 {
		if isFastPrimaryCaller && gid != 0 {
			p.samplingResolveMu.Lock()
			if p.samplingActive.Load() {
				p.samplingContended.Store(true)
				if p.samplingGID.Load() == 0 {
					p.samplingParentGID.Store(parentGID)
					p.samplingGID.Store(gid)
				}
			}
			p.samplingResolveMu.Unlock()
		} else {
			_, allStacks, allBufPtr = p.resolvePrimaryFromAllStacksLocked()
		}
	}
	return gid, parentGID, creatorFunc, callerStack, callerBufPtr, allStacks, allBufPtr
}

func (p *pressureState) hasActiveSampler() bool {
	return p.samplingPressure.Load() ||
		p.fallbackSampling.Load() ||
		p.overflowSamplingCount.Load() > 0
}

func (p *pressureState) checkSamplingGoroutine() (bool, uint64) {
	sampling, rootGID, _ := p.checkSamplingGoroutineOrChild()
	if !sampling {
		return false, 0
	}
	return true, rootGID
}

func canBeDescendantOf(parentGID, ownerGID, ownerParentGID uint64) bool {
	if parentGID == 0 || ownerGID == 0 {
		return false
	}
	if parentGID == ownerGID {
		return true
	}
	return parentGID > 1 && (ownerParentGID == 0 || parentGID != ownerParentGID)
}

func (p *pressureState) canBeDescendantOfActiveSampler(parentGID uint64) bool {
	if parentGID == 0 {
		return false
	}
	if p.isCurrentGoroutineSampling(parentGID) {
		return true
	}
	if canBeDescendantOf(parentGID, p.samplingGID.Load(), p.samplingParentGID.Load()) {
		return true
	}
	if canBeDescendantOf(parentGID, p.fallbackGID.Load(), p.fallbackParentGID.Load()) {
		return true
	}
	if p.overflowSamplingCount.Load() <= 0 {
		return false
	}
	matched := false
	p.overflowSamplingGIDs.Range(func(k, v any) bool {
		oGID, _ := k.(uint64)
		oParentGID, _ := v.(uint64)
		if canBeDescendantOf(parentGID, oGID, oParentGID) {
			matched = true
			return false
		}
		return true
	})
	return matched
}

func (p *pressureState) activeSamplerSig() uint64 {
	if p.overflowSamplingCount.Load() > 0 || p.fallbackSampling.Load() {
		return 0
	}
	return p.samplingGID.Load()
}

func (p *pressureState) isCachedNonDescendantParent(parentGID, sig uint64) bool {
	return parentGID != 0 && sig != 0 &&
		p.nonDescendantSeq.Load() == sig &&
		p.nonDescendantParentGID.Load() == parentGID
}

func (p *pressureState) checkSamplingGoroutineOrChild() (bool, uint64, uint64) {
	if !p.options.hasCustomPressureFunc || !p.hasActiveSampler() {
		return false, 0, 0
	}
	gid, parentGID, creatorFunc, callerStack, callerBufPtr, allStacks, allBufPtr := p.inspectCurrentAndResolvePrimary()
	defer releaseCurrentGoroutineStack(callerBufPtr)
	defer releaseAllGoroutineStacks(allBufPtr)
	if p.isCurrentGoroutineSampling(gid) {
		return true, gid, parentGID
	}
	if p.canBeDescendantOfActiveSampler(parentGID) {
		sig := p.activeSamplerSig()
		isDirectSamplerParent := p.isCurrentGoroutineSampling(parentGID)
		if !isDirectSamplerParent && p.isCachedNonDescendantParent(parentGID, sig) {
			return false, gid, parentGID
		}
		if ancestorGID, ok := findAncestorCreatorAboveHook(allStacks, callerStack, parentGID, creatorFunc, []byte("invokeAndStorePressure"), p.isCurrentGoroutineSampling); ok {
			return true, ancestorGID, parentGID
		}
		if !isDirectSamplerParent && parentGID != 0 && sig != 0 && p.activeSamplerSig() == sig {
			p.nonDescendantParentGID.Store(parentGID)
			p.nonDescendantSeq.Store(sig)
		}
	}
	return false, gid, parentGID
}

func (p *pressureState) isSamplingGoroutine() bool {
	sampling, _, _ := p.checkSamplingGoroutineOrChild()
	return sampling
}

func (p *pressureState) recordChildSamplerReclaim() {
	if p.samplingPressure.Load() {
		p.samplingReentrantReclaim.Store(true)
	}
	if p.fallbackSampling.Load() {
		p.fallbackReentrantReclaim.Store(true)
	}
	if p.overflowSamplingCount.Load() > 0 {
		p.overflowSamplingGIDs.Range(func(k, _ any) bool {
			if oGID, ok := k.(uint64); ok && oGID != 0 {
				p.reentrantReclaimGIDs.Store(oGID, struct{}{})
				p.hasReentrantReclaimGIDs.Store(true)
			}
			return true
		})
	}
}

func (p *pressureState) recordReentrantReclaimGID(gid uint64) {
	if gid == 0 {
		return
	}
	switch {
	case p.samplingGID.Load() == gid:
		p.samplingReentrantReclaim.Store(true)
	case p.fallbackGID.Load() == gid:
		p.fallbackReentrantReclaim.Store(true)
	case p.hasOverflowSamplingGID(gid):
		p.reentrantReclaimGIDs.Store(gid, struct{}{})
		p.hasReentrantReclaimGIDs.Store(true)
	default:
		p.recordChildSamplerReclaim()
	}
}

func (p *pressureState) clearReentrantSlot(gid uint64, slot int) bool {
	switch slot {
	case samplerSlotPrimary:
		return p.samplingReentrantReclaim.Load() && p.samplingReentrantReclaim.Swap(false)
	case samplerSlotFallback:
		return p.fallbackReentrantReclaim.Load() && p.fallbackReentrantReclaim.Swap(false)
	default:
		if gid == 0 || !p.hasReentrantReclaimGIDs.Load() {
			return false
		}
		_, reentrant := p.reentrantReclaimGIDs.LoadAndDelete(gid)
		return reentrant
	}
}

// markReclaimedLocked invalidates cached pre-reclamation pressure and increments reclaimEpoch.
// Caller MUST hold the cache's write lock.
func (p *pressureState) markReclaimedLocked() {
	p.pressureWriteMu.Lock()
	defer p.pressureWriteMu.Unlock()
	sampling, gid := p.checkSamplingGoroutine()
	if sampling {
		p.recordReentrantReclaimGID(gid)
	} else {
		if p.samplingReentrantReclaim.Load() {
			p.samplingReentrantReclaim.Store(false)
		}
		if p.fallbackReentrantReclaim.Load() {
			p.fallbackReentrantReclaim.Store(false)
		}
		if p.hasReentrantReclaimGIDs.Load() && p.hasReentrantReclaimGIDs.CompareAndSwap(true, false) {
			p.reentrantReclaimGIDs.Clear()
		}
	}
	p.pressureNeedsRefresh.Store(true)
	p.pressureInitialized.Store(false)
	p.cachedPressureBits.Store(0)
	p.reclaimEpoch.Add(1)
	if !sampling {
		p.externalReclaimEpoch.Add(1)
	}
}

func (p *pressureState) hasElevatedPressureToInvalidate(pressure float64) bool {
	thresh := p.options.CompactionThreshold
	if pressure >= thresh ||
		math.Float64frombits(p.cachedPressureBits.Load()) >= thresh ||
		p.pressureNeedsRefresh.Load() {
		return true
	}
	if !p.samplingPressure.Load() &&
		!p.fallbackSampling.Load() &&
		p.overflowSamplingCount.Load() <= 0 {
		return false
	}
	return !p.isSamplingGoroutine()
}

func (p *pressureState) resolvePostRetryPressureLocked(sampledEpoch uint64, pressure float64, validSample bool) (uint64, float64) {
	currentEpoch := p.reclaimEpoch.Load()
	var resolvedEpoch uint64
	var resolvedPressure float64
	switch {
	case sampledEpoch == currentEpoch:
		p.hasValidSample = validSample
		resolvedEpoch = sampledEpoch
		resolvedPressure = pressure
	case p.pressureInitialized.Load() && !p.pressureNeedsRefresh.Load() && p.cachedPressureEpoch.Load() == currentEpoch:
		p.hasValidSample = true
		resolvedEpoch = currentEpoch
		resolvedPressure = math.Float64frombits(p.cachedPressureBits.Load())
	default:
		p.hasValidSample = false
		resolvedEpoch = currentEpoch
		resolvedPressure = 0.0
	}
	p.resetZeroWatermarkBelowTier2(resolvedPressure)
	return resolvedEpoch, resolvedPressure
}

func (p *pressureState) lockWithPressure(mu *sync.RWMutex, fresh bool) (uint64, float64) {
	var sampledEpoch uint64
	var pressure float64
	var validSample bool
	if fresh {
		sampledEpoch, pressure, validSample = p.samplePressureFreshWithEpoch()
	} else {
		sampledEpoch, pressure, validSample = p.samplePressureWithEpoch()
	}
	mu.Lock()
	for retries := 0; sampledEpoch != p.reclaimEpoch.Load() && retries < 2; retries++ {
		mu.Unlock()
		if fresh {
			sampledEpoch, pressure, validSample = p.samplePressureFreshWithEpoch()
		} else {
			sampledEpoch, pressure, validSample = p.samplePressureWithEpoch()
		}
		mu.Lock()
	}
	return p.resolvePostRetryPressureLocked(sampledEpoch, pressure, validSample)
}

func (p *pressureState) updateCachedPressureLocked(epoch, invokeSeq uint64, val float64) (float64, bool) {
	if p.reclaimEpoch.Load() != epoch {
		if !p.pressureInitialized.Load() || p.cachedPressureEpoch.Load() != p.reclaimEpoch.Load() {
			p.pressureNeedsRefresh.Store(true)
		}
		return val, false
	}
	if invokeSeq != 0 && invokeSeq < p.pressureMaxStoredSeq.Load() {
		if p.pressureInitialized.Load() && !p.pressureNeedsRefresh.Load() && p.cachedPressureEpoch.Load() == epoch {
			val = math.Float64frombits(p.cachedPressureBits.Load())
		}
		return val, false
	}
	if invokeSeq > p.pressureMaxStoredSeq.Load() {
		p.pressureMaxStoredSeq.Store(invokeSeq)
	}
	bits := math.Float64bits(val)
	p.pressureInitialized.Store(false)
	p.cachedPressureEpoch.Store(epoch)
	p.cachedPressureBits.Store(bits)
	p.pressureInitialized.Store(true)
	p.pressureNeedsRefresh.Store(false)
	return val, true
}

func (p *pressureState) storeSampledPressureWithSeq(epoch, extEpoch, invokeSeq uint64, val float64, gid uint64, slot int) (uint64, float64, bool) {
	p.pressureWriteMu.Lock()
	defer p.pressureWriteMu.Unlock()

	if invokeSeq == 0 || invokeSeq >= p.lastSampledSeq.Load() {
		if invokeSeq > p.lastSampledSeq.Load() {
			p.lastSampledSeq.Store(invokeSeq)
		}
		p.lastSampledPressureBits.Store(math.Float64bits(val))
	}

	val, stored := p.updateCachedPressureLocked(epoch, invokeSeq, val)
	if p.options.hasCustomPressureFunc {
		reentrant := p.clearReentrantSlot(gid, slot)
		if reentrant && !stored && p.externalReclaimEpoch.Load() == extEpoch {
			epoch = p.reclaimEpoch.Load()
		}
	}

	return epoch, val, stored
}

//go:noinline
func (p *pressureState) invokeAndStorePressure(gid uint64, slot int) (uint64, float64, bool) {
	if p.options.hasCustomPressureFunc {
		p.clearReentrantSlot(gid, slot)
	}
	extEpoch := p.externalReclaimEpoch.Load()
	epoch := p.reclaimEpoch.Load()
	invokeSeq := p.pressureInvokeSeq.Add(1)
	val := p.options.PressureFunc()
	if math.IsNaN(val) || val < 0.0 {
		val = 0.0
	} else if math.IsInf(val, 1) {
		val = max(1.0, p.options.CompactionThreshold, p.options.EvictionThreshold)
	}
	epoch, val, _ = p.storeSampledPressureWithSeq(epoch, extEpoch, invokeSeq, val, gid, slot)
	return epoch, val, true
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure0() (uint64, float64, bool) {
	return p.invokeAndStorePressure(0, samplerSlotPrimary)
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure1() (uint64, float64, bool) {
	return p.invokeAndStorePressure(0, samplerSlotPrimary)
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure2() (uint64, float64, bool) {
	return p.invokeAndStorePressure(0, samplerSlotPrimary)
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure3() (uint64, float64, bool) {
	return p.invokeAndStorePressure(0, samplerSlotPrimary)
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure4() (uint64, float64, bool) {
	return p.invokeAndStorePressure(0, samplerSlotPrimary)
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure5() (uint64, float64, bool) {
	return p.invokeAndStorePressure(0, samplerSlotPrimary)
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure6() (uint64, float64, bool) {
	return p.invokeAndStorePressure(0, samplerSlotPrimary)
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure7() (uint64, float64, bool) {
	return p.invokeAndStorePressure(0, samplerSlotPrimary)
}

func (p *pressureState) dispatchFastPrimaryPressure(tag uint32) (uint64, float64, bool) {
	switch tag {
	case 0:
		return p.invokeFastPrimaryPressure0()
	case 1:
		return p.invokeFastPrimaryPressure1()
	case 2:
		return p.invokeFastPrimaryPressure2()
	case 3:
		return p.invokeFastPrimaryPressure3()
	case 4:
		return p.invokeFastPrimaryPressure4()
	case 5:
		return p.invokeFastPrimaryPressure5()
	case 6:
		return p.invokeFastPrimaryPressure6()
	default:
		return p.invokeFastPrimaryPressure7()
	}
}

func (p *pressureState) ensureSamplingGID(gid, parentGID uint64) (uint64, uint64) {
	if p.options.hasCustomPressureFunc && gid == 0 {
		stack, bufPtr := captureCurrentGoroutineStack()
		gid, parentGID = parseStackGoroutineAndParentID(stack)
		releaseCurrentGoroutineStack(bufPtr)
	}
	return gid, parentGID
}

func (p *pressureState) releasePrimarySamplerSlot(tag uint32, gid uint64, fastPrimary bool) {
	p.clearReentrantSlot(gid, samplerSlotPrimary)
	p.samplingActive.Store(false)
	if fastPrimary {
		fastPrimaryTagOwner[tag].Store(0)
	}
	if !fastPrimary || p.samplingContended.Load() || p.samplingGID.Load() != 0 {
		p.samplingResolveMu.Lock()
		p.samplingGID.Store(0)
		p.samplingParentGID.Store(0)
		p.nonDescendantSeq.Store(0)
		p.samplingContended.Store(false)
		p.samplingResolveMu.Unlock()
	}
	p.samplingPressure.Store(false)
}

//go:noinline
func (p *pressureState) sampleWithSlot(gid, parentGID uint64) (uint64, float64, bool) {
	if !p.options.hasCustomPressureFunc {
		defer p.samplingPressure.Store(false)
		return p.invokeAndStorePressure(0, samplerSlotPrimary)
	}
	var tag uint32
	fastPrimary := false
	if gid == 0 {
		tag, fastPrimary = p.claimFastPrimaryTag()
	}
	if fastPrimary {
		p.samplingActive.Store(true)
	} else {
		gid, parentGID = p.ensureSamplingGID(gid, parentGID)
		p.samplingResolveMu.Lock()
		p.samplingParentGID.Store(parentGID)
		p.samplingGID.Store(gid)
		p.samplingActive.Store(true)
		p.samplingResolveMu.Unlock()
	}
	defer p.releasePrimarySamplerSlot(tag, gid, fastPrimary)
	if fastPrimary {
		return p.dispatchFastPrimaryPressure(tag)
	}
	return p.invokeAndStorePressure(gid, samplerSlotPrimary)
}

//go:noinline
func (p *pressureState) sampleWithFallbackSlot(gid, parentGID uint64) (uint64, float64, bool) {
	if p.options.hasCustomPressureFunc {
		gid, parentGID = p.ensureSamplingGID(gid, parentGID)
		p.fallbackParentGID.Store(parentGID)
		p.fallbackGID.Store(gid)
		defer func() {
			p.clearReentrantSlot(gid, samplerSlotFallback)
			p.fallbackGID.Store(0)
			p.fallbackParentGID.Store(0)
			p.fallbackSampling.Store(false)
		}()
	} else {
		defer p.fallbackSampling.Store(false)
	}
	return p.invokeAndStorePressure(gid, samplerSlotFallback)
}

func (p *pressureState) sampleWithOverflowSlot(gid, parentGID uint64) (uint64, float64, bool) {
	if p.options.hasCustomPressureFunc {
		gid, parentGID = p.ensureSamplingGID(gid, parentGID)
		p.overflowSamplingGIDs.Store(gid, parentGID)
	}
	p.overflowSamplingCount.Add(1)
	defer func() {
		if p.options.hasCustomPressureFunc {
			p.clearReentrantSlot(gid, samplerSlotOverflow)
			p.overflowSamplingGIDs.Delete(gid)
		}
		p.overflowSamplingCount.Add(-1)
	}()
	return p.invokeAndStorePressure(gid, samplerSlotOverflow)
}

// samplePressureFreshWithEpoch evaluates p.options.PressureFunc lock-free outside the cache lock
// with a goroutine-aware re-entrancy guard and cold-start fallback.
func (p *pressureState) samplePressureFreshWithEpoch() (uint64, float64, bool) {
	if p.options.PressureFunc == nil {
		return p.reclaimEpoch.Load(), 0.0, true
	}
	hadActive := p.hasActiveSampler()
	sampling, gid, parentGID := p.checkSamplingGoroutineOrChild()
	if sampling {
		return p.reclaimEpoch.Load(), 0.0, false
	}
	if p.samplingPressure.CompareAndSwap(false, true) {
		return p.sampleWithSlot(gid, parentGID)
	}
	if p.options.hasCustomPressureFunc {
		if !hadActive {
			sampling, gid, parentGID = p.checkSamplingGoroutineOrChild()
			if sampling {
				return p.reclaimEpoch.Load(), 0.0, false
			}
		}
		gid, parentGID = p.ensureSamplingGID(gid, parentGID)
	}
	if p.fallbackSampling.CompareAndSwap(false, true) {
		return p.sampleWithFallbackSlot(gid, parentGID)
	}
	return p.sampleWithOverflowSlot(gid, parentGID)
}

// samplePressureWithEpoch returns the current memory pressure and epoch for foreground cache operations.
// Custom PressureFunc callbacks are invoked on every call; the default runtime/metrics probe is amortized
// across a 256-operation window (and immediately refreshed after any reclamation/compaction cycle).
func (p *pressureState) samplePressureWithEpoch() (uint64, float64, bool) {
	if p.options.hasCustomPressureFunc {
		return p.samplePressureFreshWithEpoch()
	}
	epoch := p.reclaimEpoch.Load()
	seq := p.pressureSampleSeq.Add(1)
	if (seq&pressureSampleWindowMask) == 1 || !p.pressureInitialized.Load() || p.pressureNeedsRefresh.Load() || p.cachedPressureEpoch.Load() != epoch {
		return p.samplePressureFreshWithEpoch()
	}
	bits := p.cachedPressureBits.Load()
	if !p.pressureInitialized.Load() || p.pressureNeedsRefresh.Load() || p.cachedPressureEpoch.Load() != epoch || p.reclaimEpoch.Load() != epoch {
		return p.samplePressureFreshWithEpoch()
	}
	return epoch, math.Float64frombits(bits), true
}

func (p *pressureState) onEntryPut(newLen int, size uint64) {
	if newLen > p.peakEntryLen {
		p.peakEntryLen = newLen
	}
	if size == 0 {
		p.zeroSizeCount++
	}
}

func (p *pressureState) onEntrySizeUpdated(oldSize, newSize uint64) {
	if oldSize == 0 && newSize > 0 && p.zeroSizeCount > 0 {
		p.zeroSizeCount--
		if p.zeroSizeCount < p.lastReclaimedZeroCount {
			p.lastReclaimedZeroCount = p.zeroSizeCount
		}
	} else if oldSize > 0 && newSize == 0 {
		p.zeroSizeCount++
		p.lastReclaimedLen = 0
	}
}

func (p *pressureState) onEntryDeleted(size uint64) {
	p.deletedSinceCompact++
	if size == 0 && p.zeroSizeCount > 0 {
		p.zeroSizeCount--
		if p.zeroSizeCount < p.lastReclaimedZeroCount {
			p.lastReclaimedZeroCount = p.zeroSizeCount
		}
	} else if size > 0 {
		p.lastReclaimedLen = 0
	}
}

func (p *pressureState) resetWatermarks() {
	p.peakEntryLen = 0
	p.deletedSinceCompact = 0
	p.zeroSizeCount = 0
	p.lastReclaimedZeroCount = 0
	p.lastReclaimedLen = 0
}

func (p *pressureState) onCompacted(newLen int) {
	p.deletedSinceCompact = 0
	p.peakEntryLen = newLen
	if p.zeroSizeCount < p.lastReclaimedZeroCount {
		p.lastReclaimedZeroCount = p.zeroSizeCount
	}
	if newLen < p.lastReclaimedLen {
		p.lastReclaimedLen = newLen
	}
}

func (p *pressureState) shouldAutoCompactEntryCounts(isDirty, isBackground bool, currentLen int) bool {
	if isBackground {
		return isDirty
	}
	if !isDirty {
		return false
	}
	if p.peakEntryLen > minPeakSlackEntries && p.peakEntryLen > currentLen && p.peakEntryLen-currentLen >= 2 && uint64(p.peakEntryLen-currentLen)*slackQuarterMultiplier >= uint64(p.peakEntryLen) {
		return true
	}
	return p.deletedSinceCompact >= minChurnCompactDeletes && p.deletedSinceCompact >= currentLen
}

func (p *pressureState) hasEmptyDeleteSlack(extraSlack bool) bool {
	return extraSlack || p.peakEntryLen > minPeakSlackEntries || p.deletedSinceCompact >= minChurnCompactDeletes
}

func (p *pressureState) shouldReclaimSingleSurvivorOnDelete(currentLen int, isDirty, extraChurn bool) bool {
	return currentLen == 1 && isDirty && (extraChurn || p.peakEntryLen >= minChurnCompactDeletes || p.deletedSinceCompact >= minChurnCompactDeletes)
}

func (p *pressureState) shouldReclaimEmptyPrePut(currentLen int, extraChurn, evictedPre bool, newSize, sizeBefore uint64, pressure float64) bool {
	return currentLen == 0 && (extraChurn || p.peakEntryLen >= minChurnCompactDeletes || p.deletedSinceCompact >= minChurnCompactDeletes || (evictedPre && newSize < sizeBefore && p.hasElevatedPressureToInvalidate(pressure)))
}

func (p *pressureState) shouldReclaimSingleSurvivorOnMutation(currentLen int, isDirty, extraChurn, evictedPre bool, postSize, sizeBefore uint64, pressure float64) bool {
	return (currentLen == 1 || (evictedPre && currentLen-p.zeroSizeCount <= 1)) && isDirty &&
		(extraChurn || p.peakEntryLen >= minChurnCompactDeletes || p.deletedSinceCompact >= minChurnCompactDeletes || (currentLen == 1 && postSize < sizeBefore && p.hasElevatedPressureToInvalidate(pressure)))
}

func (p *pressureState) shouldCompactAfterMutation(reclaimedPre, netByteReduced, isDirty bool, pressure float64) bool {
	return isDirty && (reclaimedPre || (netByteReduced && pressure >= p.options.CompactionThreshold && uint64(p.deletedSinceCompact)*slackQuarterMultiplier >= uint64(p.peakEntryLen)))
}

func (p *pressureState) shouldMarkReclaimedAfterMutation(reclaimedPre, netByteReduced bool, sampledEpoch uint64, pressure float64) bool {
	return (reclaimedPre || netByteReduced) && p.reclaimEpoch.Load() == sampledEpoch && p.hasElevatedPressureToInvalidate(pressure)
}

func (p *pressureState) computeZeroShedTargets(retention float64, hasProtected bool, protectedSize uint64) (targetZeroCount, unprotectedZeroTarget int) {
	if p.zeroSizeCount <= 0 {
		return 0, 0
	}
	unprotectedZeroTarget = max(int(float64(p.zeroSizeCount)*retention), p.lastReclaimedZeroCount)
	targetZeroCount = unprotectedZeroTarget
	if hasProtected && protectedSize == 0 && targetZeroCount < 1 {
		targetZeroCount = 1
	}
	return targetZeroCount, unprotectedZeroTarget
}

func (p *pressureState) computeShedTargets(targetSize uint64, retention float64, currentLen int, hasProtected bool, protectedSize uint64) (effectiveTarget uint64, targetLen, targetZeroCount, unprotectedZeroTarget int) {
	effectiveTarget = targetSize
	if retention <= 0.0 {
		return effectiveTarget, 0, 0, 0
	}
	if hasProtected && protectedSize > effectiveTarget {
		effectiveTarget = protectedSize
	}
	if currentLen > 0 {
		targetLen = int(float64(currentLen) * retention)
		if (!hasProtected || protectedSize <= targetSize) && p.lastReclaimedLen > targetLen {
			targetLen = p.lastReclaimedLen
		}
	}
	targetZeroCount, unprotectedZeroTarget = p.computeZeroShedTargets(retention, hasProtected, protectedSize)
	return effectiveTarget, targetLen, targetZeroCount, unprotectedZeroTarget
}

func (p *pressureState) updateZeroWatermarkAfterShed(retention float64, currentLen int, unprotectedLenAllowed bool, unprotectedZeroTarget, targetLen int) {
	if retention > 0.0 && p.zeroSizeCount > 0 {
		p.lastReclaimedZeroCount = min(p.zeroSizeCount, unprotectedZeroTarget)
		if unprotectedLenAllowed && currentLen > p.zeroSizeCount {
			p.lastReclaimedLen = min(currentLen, targetLen)
		} else {
			p.lastReclaimedLen = 0
		}
	} else {
		p.lastReclaimedZeroCount = 0
		p.lastReclaimedLen = 0
	}
}

func (p *pressureState) shouldContinueShedding(currentSize, effectiveTarget uint64, needFullFlush bool, targetZeroCount, currentLen, targetLen int) (needByteShed, needZeroShed bool) {
	needByteShed = currentSize > effectiveTarget || needFullFlush
	needZeroShed = !needFullFlush && p.zeroSizeCount > targetZeroCount && currentLen > targetLen
	return needByteShed, needZeroShed
}

func (p *pressureState) shouldEvictShedVictim(isProtected bool, size uint64, needFullFlush, needByteShed bool, targetZeroCount int) bool {
	if isProtected {
		return false
	}
	if needFullFlush || (needByteShed && p.zeroSizeCount > targetZeroCount) {
		return true
	}
	if needByteShed {
		return size > 0
	}
	return size == 0
}

func (p *pressureState) recordPressureShed(isBackground bool) {
	if isBackground {
		p.pressureShedsExplicit++
	} else {
		p.pressureShedsInline++
	}
}

func (p *pressureState) shouldEvictZeroWeightOnInsert(valueSize, maxSize, currentSize uint64, currentLen int) bool {
	if currentLen <= 0 {
		return false
	}
	if valueSize == 0 {
		return (currentSize >= maxSize && uint64(currentLen) >= maxSize) || uint64(p.zeroSizeCount) >= maxSize
	}
	if uint64(p.zeroSizeCount) <= maxSize {
		return false
	}
	excessZero := uint64(p.zeroSizeCount) - maxSize
	avail := maxSize - currentSize
	return excessZero >= avail || valueSize > avail-excessZero
}

func (p *pressureState) shouldEvictPreInsertOnPut(valueSize, maxSize, currentSize uint64, currentLen int, evictedPrePut bool) bool {
	if valueSize > maxSize-currentSize {
		return true
	}
	if valueSize == 0 && evictedPrePut {
		return false
	}
	return p.shouldEvictZeroWeightOnInsert(valueSize, maxSize, currentSize, currentLen)
}

func (p *pressureState) shouldEvictOnZeroWeightShrink(oldSize, newSize, maxSize uint64) bool {
	return oldSize > 0 && newSize == 0 && uint64(p.zeroSizeCount) >= maxSize
}

func (p *pressureState) maybeMarkReclaimed(reclaimedOrReduced bool, sampledEpoch uint64, pressure float64) {
	if reclaimedOrReduced && p.reclaimEpoch.Load() == sampledEpoch && p.hasElevatedPressureToInvalidate(pressure) {
		p.markReclaimedLocked()
	}
}

func (p *pressureState) finishClearAllLocked(hadCompactionSlack, hadReclaimable bool, sampledEpoch uint64, pressure float64) {
	if hadCompactionSlack {
		p.recordCompactionByPressure(pressure)
	}
	p.maybeMarkReclaimed(hadCompactionSlack || hadReclaimable, sampledEpoch, pressure)
}

type evictEvent[V any] struct {
	key    string
	value  V
	reason EvictionReason
}

// evictCallbackQueue buffers eviction events on the stack during a cache mutation
// so user OnEvictValue / OnEvictEntry callbacks execute only after all internal
// structural invariants are restored and c.mu.Unlock() has completed.
type evictCallbackQueue[V any] struct {
	buf      [2]evictEvent[V]
	overflow []evictEvent[V]
	n        int
}

func (q *evictCallbackQueue[V]) enqueue(key string, value V, reason EvictionReason) {
	if q.n < len(q.buf) {
		q.buf[q.n] = evictEvent[V]{key: key, value: value, reason: reason}
		q.n++
		return
	}
	q.overflow = append(q.overflow, evictEvent[V]{key: key, value: value, reason: reason})
	q.n++
}

func (p *pressureState) activeFastEvictTag() (uint32, bool) {
	id := p.hookIDVal.Load()
	if id == 0 {
		return 0, false
	}
	tag := p.evictTagHint.Load() & (numPressureHookTags - 1)
	if fastEvictTagState[tag].Load() == (id<<1)|1 {
		return tag, true
	}
	return 0, false
}

func (p *pressureState) isEvictCallbackActive() bool {
	if _, ok := p.activeFastEvictTag(); ok {
		return true
	}
	return p.evictCallbackActive.Load()
}

func (p *pressureState) matchCandidateEvictCallbackGID(allStacks []byte, targetFrame []byte) uint64 {
	rem := allStacks
	for len(rem) > 0 {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		if !bytes.Contains(blk, targetFrame) {
			continue
		}
		gid, parentGID := parseStackGoroutineAndParentID(blk)
		if gid == 0 {
			continue
		}
		if p.isEvictCallbackActive() {
			p.evictCallbackParentGID.Store(parentGID)
			p.evictCallbackGID.Store(gid)
			return gid
		}
	}
	return 0
}

func (p *pressureState) resolveEvictCallbackHolderFromAllStacksLocked() uint64 {
	p.evictCallbackResolveMu.Lock()
	defer p.evictCallbackResolveMu.Unlock()
	p.evictCallbackContended.Store(true)
	if !p.isEvictCallbackActive() {
		if p.evictCallbackGID.Load() == 0 {
			p.evictCallbackContended.Store(false)
		}
		return 0
	}
	if gid := p.evictCallbackGID.Load(); gid != 0 {
		return gid
	}
	tag, ok := p.activeFastEvictTag()
	if !ok {
		return p.evictCallbackGID.Load()
	}
	allStacks, poolBuf := captureAllGoroutineStacks()
	defer releaseAllGoroutineStacks(poolBuf)
	if gid := p.matchCandidateEvictCallbackGID(allStacks, fastEvictCallbackMarkers[tag]); gid != 0 {
		return gid
	}
	return p.evictCallbackGID.Load()
}

//go:noinline
func (p *pressureState) registerSlowEvictHolder(gid, parentGID uint64) uint64 {
	if gid == 0 || parentGID == 0 {
		stack, bufPtr := captureCurrentGoroutineStack()
		parsedGID, parsedParentGID, _ := parseStackGoroutineParentAndCreator(stack)
		releaseCurrentGoroutineStack(bufPtr)
		if gid == 0 {
			gid = parsedGID
		}
		if parentGID == 0 {
			parentGID = parsedParentGID
		}
	}
	p.evictCallbackResolveMu.Lock()
	p.evictCallbackContended.Store(true)
	p.evictCallbackParentGID.Store(parentGID)
	p.evictCallbackGID.Store(gid)
	p.evictCallbackActive.Store(true)
	p.evictCallbackResolveMu.Unlock()
	return gid
}

func (p *pressureState) resolveActiveEvictCallbackHolder(gid, parentGID uint64, isMyTagFrame bool) uint64 {
	if holder := p.evictCallbackGID.Load(); holder != 0 {
		return holder
	}
	if isMyTagFrame && gid != 0 {
		p.evictCallbackResolveMu.Lock()
		if _, ok := p.activeFastEvictTag(); ok {
			p.evictCallbackContended.Store(true)
			if p.evictCallbackGID.Load() == 0 {
				p.evictCallbackParentGID.Store(parentGID)
				p.evictCallbackGID.Store(gid)
			}
		}
		p.evictCallbackResolveMu.Unlock()
	}
	if holder := p.evictCallbackGID.Load(); holder != 0 {
		return holder
	}
	if p.isEvictCallbackActive() {
		return p.resolveEvictCallbackHolderFromAllStacksLocked()
	}
	return 0
}

//go:noinline
func (p *pressureState) lockEvictCallbackSlow() (bool, uint64) {
	stack, bufPtr := captureCurrentGoroutineStack()
	gid, parentGID, creatorFunc := parseStackGoroutineParentAndCreator(stack)
	fastTag, hasFastTag := p.activeFastEvictTag()
	isMyTagFrame := hasFastTag && bytes.Contains(stack, fastEvictCallbackMarkers[fastTag])

	holder := p.resolveActiveEvictCallbackHolder(gid, parentGID, isMyTagFrame)
	if holder != 0 {
		if holder == gid {
			releaseCurrentGoroutineStack(bufPtr)
			return false, 0
		}
		if canBeDescendantOf(parentGID, holder, p.evictCallbackParentGID.Load()) {
			if _, ok := findAncestorCreatorAboveHook(nil, stack, parentGID, creatorFunc, []byte("deliverCallbacks"), func(ancGID uint64) bool {
				return ancGID == holder
			}); ok {
				releaseCurrentGoroutineStack(bufPtr)
				return false, 0
			}
		}
	}
	releaseCurrentGoroutineStack(bufPtr)

	p.evictCallbackMu.Lock()
	return true, p.registerSlowEvictHolder(gid, parentGID)
}

func (p *pressureState) runPendingEvictBatch(queue []func()) {
	defer clear(queue)
	for i, fn := range queue {
		queue[i] = nil
		fn()
	}
}

func (p *pressureState) drainPendingEvictCallbacks() {
	for p.evictPendingEnqueues.Load() != 0 {
		p.evictCallbackResolveMu.Lock()
		queue := p.pendingEvictCallbacks
		p.pendingEvictCallbacks = nil
		p.evictPendingEnqueues.Store(0)
		p.evictCallbackResolveMu.Unlock()
		if len(queue) > 0 {
			p.runPendingEvictBatch(queue)
		}
	}
}

func (p *pressureState) claimFastEvictTagSlow(base uint32, idleState, activeState uint64) (uint32, bool) {
	for offset := range uint32(numPressureHookTags) {
		tag := (base + offset) & (numPressureHookTags - 1)
		cur := fastEvictTagState[tag].Load()
		if (cur == 0 || cur == idleState) && fastEvictTagState[tag].CompareAndSwap(cur, activeState) {
			if tag != base {
				p.evictTagHint.Store(tag)
			}
			return tag, true
		}
	}
	for offset := range uint32(numPressureHookTags) {
		tag := (base + offset) & (numPressureHookTags - 1)
		cur := fastEvictTagState[tag].Load()
		if cur&1 == 0 && fastEvictTagState[tag].CompareAndSwap(cur, activeState) {
			if tag != base {
				p.evictTagHint.Store(tag)
			}
			return tag, true
		}
	}
	return 0, false
}

func (p *pressureState) claimFastEvictTag() (uint32, bool) {
	id := p.hookIDVal.Load()
	if id == 0 {
		id = p.hookID()
	}
	base := p.evictTagHint.Load() & (numPressureHookTags - 1)
	idleState := id << 1
	activeState := idleState | 1
	if fastEvictTagState[base].CompareAndSwap(idleState, activeState) {
		return base, true
	}
	return p.claimFastEvictTagSlow(base, idleState, activeState)
}

func (p *pressureState) clearActiveFastEvictTag() {
	if id := p.hookIDVal.Load(); id != 0 {
		tag := p.evictTagHint.Load() & (numPressureHookTags - 1)
		fastEvictTagState[tag].CompareAndSwap((id<<1)|1, id<<1)
	}
}

//go:noinline
func (p *pressureState) deliverCallbacksDrainPending() {
	p.unlockEvictCallbackSlow()
}

func (p *pressureState) unlockEvictCallbackSlow() {
	panicked := true
	defer func() {
		p.clearActiveFastEvictTag()
		p.evictCallbackResolveMu.Lock()
		if panicked {
			clear(p.pendingEvictCallbacks)
			p.pendingEvictCallbacks = nil
			p.evictPendingEnqueues.Store(0)
		}
		p.evictCallbackActive.Store(false)
		p.evictCallbackGID.Store(0)
		p.evictCallbackParentGID.Store(0)
		p.evictCallbackContended.Store(false)
		p.evictCallbackResolveMu.Unlock()
		p.evictCallbackMu.Unlock()
		if !panicked && p.evictPendingEnqueues.Load() != 0 && p.evictCallbackMu.TryLock() {
			p.deliverCallbacksDrainPending()
		}
	}()
	if p.evictPendingEnqueues.Load() != 0 {
		if p.evictCallbackGID.Load() == 0 || !p.evictCallbackActive.Load() {
			p.registerSlowEvictHolder(0, 0)
		}
		p.drainPendingEvictCallbacks()
	}
	panicked = false
}

func (p *pressureState) unlockFastEvictCallback() {
	if p.evictPendingEnqueues.Load() != 0 {
		p.unlockEvictCallbackSlow()
		return
	}
	id := p.hookIDVal.Load()
	tag := p.evictTagHint.Load() & (numPressureHookTags - 1)
	fastEvictTagState[tag].Store(id << 1)
	if p.evictCallbackContended.Load() || p.evictCallbackGID.Load() != 0 || p.evictPendingEnqueues.Load() != 0 {
		p.unlockEvictCallbackSlow()
		return
	}
	p.evictCallbackMu.Unlock()
}

func (q *evictCallbackQueue[V]) reset() {
	if q.n > 0 {
		q.buf = [2]evictEvent[V]{}
		if q.overflow != nil {
			clear(q.overflow)
			q.overflow = nil
		}
		q.n = 0
	}
}

//go:noinline
func (q *evictCallbackQueue[V]) enqueuePendingCallback(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	events := make([]evictEvent[V], 0, q.n)
	inlineCount := min(q.n, len(q.buf))
	for i := range inlineCount {
		events = append(events, q.buf[i])
	}
	events = append(events, q.overflow...)
	q.reset()

	p.evictCallbackResolveMu.Lock()
	p.pendingEvictCallbacks = append(p.pendingEvictCallbacks, func() {
		defer clear(events)
		for i := range events {
			ev := events[i]
			events[i] = evictEvent[V]{}
			if onVal != nil {
				onVal(ev.value, ev.reason)
			}
			if onEntry != nil {
				onEntry(ev.key, ev.value, ev.reason)
			}
		}
	})
	p.evictPendingEnqueues.Store(1)
	p.evictCallbackResolveMu.Unlock()

	if p.evictPendingEnqueues.Load() != 0 && p.evictCallbackMu.TryLock() {
		p.deliverCallbacksDrainPending()
	}
}

func (q *evictCallbackQueue[V]) deliverMultiCallbacks(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	defer q.reset()
	inlineCount := min(q.n, len(q.buf))
	for i := range inlineCount {
		ev := q.buf[i]
		q.buf[i] = evictEvent[V]{}
		if onVal != nil {
			onVal(ev.value, ev.reason)
		}
		if onEntry != nil {
			onEntry(ev.key, ev.value, ev.reason)
		}
	}
	for i := range q.overflow {
		ev := q.overflow[i]
		q.overflow[i] = evictEvent[V]{}
		if onVal != nil {
			onVal(ev.value, ev.reason)
		}
		if onEntry != nil {
			onEntry(ev.key, ev.value, ev.reason)
		}
	}
	if q.overflow != nil {
		q.overflow = nil
	}
	q.n = 0
}

func (q *evictCallbackQueue[V]) deliverCallbacks(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	defer p.unlockFastEvictCallback()
	if q.n == 1 {
		ev := q.buf[0]
		q.buf[0] = evictEvent[V]{}
		q.n = 0
		if onVal != nil {
			onVal(ev.value, ev.reason)
		}
		if onEntry != nil {
			onEntry(ev.key, ev.value, ev.reason)
		}
		return
	}
	q.deliverMultiCallbacks(onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacksSlow(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	defer p.unlockEvictCallbackSlow()
	if q.n == 1 {
		ev := q.buf[0]
		q.buf[0] = evictEvent[V]{}
		q.n = 0
		if onVal != nil {
			onVal(ev.value, ev.reason)
		}
		if onEntry != nil {
			onEntry(ev.key, ev.value, ev.reason)
		}
		return
	}
	q.deliverMultiCallbacks(onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacksFast0(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(p, onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacksFast1(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(p, onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacksFast2(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(p, onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacksFast3(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(p, onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacksFast4(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(p, onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacksFast5(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(p, onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacksFast6(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(p, onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacksFast7(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(p, onVal, onEntry)
}

func (q *evictCallbackQueue[V]) dispatchFastCallbacks(p *pressureState, tag uint32, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	switch tag {
	case 0:
		q.deliverCallbacksFast0(p, onVal, onEntry)
	case 1:
		q.deliverCallbacksFast1(p, onVal, onEntry)
	case 2:
		q.deliverCallbacksFast2(p, onVal, onEntry)
	case 3:
		q.deliverCallbacksFast3(p, onVal, onEntry)
	case 4:
		q.deliverCallbacksFast4(p, onVal, onEntry)
	case 5:
		q.deliverCallbacksFast5(p, onVal, onEntry)
	case 6:
		q.deliverCallbacksFast6(p, onVal, onEntry)
	default:
		q.deliverCallbacksFast7(p, onVal, onEntry)
	}
}

func (q *evictCallbackQueue[V]) invoke(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	if q.n == 0 {
		return
	}
	if onVal == nil && onEntry == nil {
		q.reset()
		return
	}
	if p.evictCallbackMu.TryLock() {
		if tag, ok := p.claimFastEvictTag(); ok {
			q.dispatchFastCallbacks(p, tag, onVal, onEntry)
			return
		}
		p.registerSlowEvictHolder(0, 0)
		q.deliverCallbacksSlow(p, onVal, onEntry)
		return
	}
	locked, _ := p.lockEvictCallbackSlow()
	if !locked {
		q.enqueuePendingCallback(p, onVal, onEntry)
		return
	}
	q.deliverCallbacksSlow(p, onVal, onEntry)
}

func appendEvicted[V any](base, extra []V) []V {
	if len(base) == 0 {
		return extra
	}
	if len(extra) > 0 {
		return append(base, extra...)
	}
	return base
}

func (p *pressureState) resetZeroWatermarkBelowTier2(pressure float64) {
	if p.lastReclaimedZeroCount == 0 && p.lastReclaimedLen == 0 {
		return
	}
	if p.options.PressureFunc == nil || (p.hasValidSample && pressure < p.options.EvictionThreshold && !p.isSamplingGoroutine()) {
		p.lastReclaimedZeroCount = 0
		p.lastReclaimedLen = 0
	}
}

func (p *pressureState) recordEviction(reason EvictionReason, weight uint64) {
	switch reason {
	case EvictionReasonCapacity:
		p.evictionsCapacity++
		p.evictedWeightCapacity += weight
	case EvictionReasonPressure:
		p.evictionsPressure++
		p.evictedWeightPressure += weight
	case EvictionReasonDeleted:
		p.evictionsDeleted++
		p.evictedWeightDeleted += weight
	case EvictionReasonReplaced:
		p.evictionsReplaced++
		p.evictedWeightReplaced += weight
	}
}

func (p *pressureState) recordCompactionByPressure(pressure float64) {
	switch {
	case pressure >= p.options.EvictionThreshold:
		p.compactionsPressureTier2++
	case pressure >= p.options.CompactionThreshold:
		p.compactionsPressureTier1++
	default:
		p.compactionsAutoSlack++
	}
}

func (p *pressureState) snapshotBaseStats(backend Backend, currentSize, maxSize uint64, entryLen int) Stats {
	return Stats{
		Backend:                  backend,
		GetHits:                  p.getHits,
		GetMisses:                p.getMisses,
		PeekHits:                 p.peekHits.Load(),
		PeekMisses:               p.peekMisses.Load(),
		EvictionsCapacity:        p.evictionsCapacity,
		EvictionsPressure:        p.evictionsPressure,
		EvictionsDeleted:         p.evictionsDeleted,
		EvictionsReplaced:        p.evictionsReplaced,
		EvictedWeightCapacity:    p.evictedWeightCapacity,
		EvictedWeightPressure:    p.evictedWeightPressure,
		EvictedWeightDeleted:     p.evictedWeightDeleted,
		EvictedWeightReplaced:    p.evictedWeightReplaced,
		CurrentSize:              currentSize,
		MaxSize:                  maxSize,
		Len:                      entryLen,
		ZeroSizeCount:            p.zeroSizeCount,
		PutInserted:              p.putInserted,
		PutUpdated:               p.putUpdated,
		PutRejectedOversized:     p.putRejectedOversized.Load(),
		ReplaceUpdated:           p.replaceUpdated,
		ReplaceNotFound:          p.replaceNotFound,
		ReplaceSelfEvicted:       p.replaceSelfEvicted,
		DeleteDeleted:            p.deleteDeleted,
		DeleteNotFound:           p.deleteNotFound,
		DeletePrefixExecuted:     p.deletePrefixExecuted,
		MemoryPressure:           math.Float64frombits(p.lastSampledPressureBits.Load()),
		CompactionsExplicit:      p.compactionsExplicit,
		CompactionsPressureTier1: p.compactionsPressureTier1,
		CompactionsPressureTier2: p.compactionsPressureTier2,
		CompactionsAutoSlack:     p.compactionsAutoSlack,
		PressureShedsInline:      p.pressureShedsInline,
		PressureShedsExplicit:    p.pressureShedsExplicit,
		ReclaimEpoch:             p.reclaimEpoch.Load(),
		DeletedSinceCompact:      p.deletedSinceCompact,
		PeakEntryLen:             p.peakEntryLen,
	}
}

func (p *pressureState) checkWatermarkInvariants(currentLen int) {
	if p.zeroSizeCount < 0 || p.zeroSizeCount > currentLen {
		panic(fmt.Sprintf("lru invariant violation: invalid zeroSizeCount %d for len %d", p.zeroSizeCount, currentLen))
	}
	if p.deletedSinceCompact < 0 {
		panic(fmt.Sprintf("lru invariant violation: negative deletedSinceCompact %d", p.deletedSinceCompact))
	}
	if p.peakEntryLen < 0 {
		panic(fmt.Sprintf("lru invariant violation: negative peakEntryLen %d", p.peakEntryLen))
	}
	if p.putInserted > 0 && currentLen > p.peakEntryLen {
		panic(fmt.Sprintf("lru invariant violation: currentLen %d exceeds peakEntryLen %d", currentLen, p.peakEntryLen))
	}
	lastPressure := math.Float64frombits(p.lastSampledPressureBits.Load())
	if math.IsNaN(lastPressure) || math.IsInf(lastPressure, 0) || lastPressure < 0.0 {
		panic(fmt.Sprintf("lru invariant violation: invalid lastSampledPressure %v", lastPressure))
	}
}

func (p *pressureState) checkCounterInvariants(currentLen int) {
	totalRemovals := p.evictionsCapacity + p.evictionsPressure + p.evictionsDeleted
	if p.deleteDeleted > p.evictionsDeleted {
		panic(fmt.Sprintf("lru invariant violation: deleteDeleted %d exceeds evictionsDeleted %d", p.deleteDeleted, p.evictionsDeleted))
	}
	if p.replaceSelfEvicted > p.evictionsCapacity {
		panic(fmt.Sprintf("lru invariant violation: replaceSelfEvicted %d exceeds evictionsCapacity %d", p.replaceSelfEvicted, p.evictionsCapacity))
	}
	if p.putUpdated+p.replaceUpdated != p.evictionsReplaced {
		panic(fmt.Sprintf("lru invariant violation: putUpdated (%d) + replaceUpdated (%d) != evictionsReplaced (%d)", p.putUpdated, p.replaceUpdated, p.evictionsReplaced))
	}
	if p.pressureShedsInline+p.pressureShedsExplicit > p.evictionsPressure {
		panic(fmt.Sprintf("lru invariant violation: pressureSheds (%d) exceeds evictionsPressure (%d)", p.pressureShedsInline+p.pressureShedsExplicit, p.evictionsPressure))
	}
	if p.putInserted > 0 && uint64(currentLen)+totalRemovals != p.putInserted {
		panic(fmt.Sprintf("lru invariant violation: currentLen (%d) + totalRemovals (%d) != putInserted (%d)", currentLen, totalRemovals, p.putInserted))
	}
}

func (p *pressureState) checkTelemetryInvariants(currentLen int) {
	p.checkWatermarkInvariants(currentLen)
	p.checkCounterInvariants(currentLen)
}
