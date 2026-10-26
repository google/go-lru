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
	"runtime/metrics"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const goroutinesCreatedMetric = "/sched/goroutines-created:goroutines"

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
	goroutinesCreatedSamplePool = sync.Pool{
		New: func() any {
			return &[1]metrics.Sample{{Name: goroutinesCreatedMetric}}
		},
	}
)

func readGoroutinesCreatedInto(samplePtr *[1]metrics.Sample) uint64 {
	if samplePtr[0].Name == "" {
		samplePtr[0].Name = goroutinesCreatedMetric
	}
	metrics.Read(samplePtr[:])
	if samplePtr[0].Value.Kind() == metrics.KindUint64 {
		return samplePtr[0].Value.Uint64()
	}
	return 0
}

func readGoroutinesCreated() uint64 {
	samplePtr := goroutinesCreatedSamplePool.Get().(*[1]metrics.Sample)
	val := readGoroutinesCreatedInto(samplePtr)
	goroutinesCreatedSamplePool.Put(samplePtr)
	return val
}

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

func skipBalancedBrackets(b []byte) (int, bool) {
	if len(b) == 0 || b[0] != '[' {
		return 0, false
	}
	depth := 0
	for i := range b {
		switch b[i] {
		case '\n':
			return 0, false
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i + 1, true
			}
		}
	}
	return 0, false
}

func isCallFrameBoundary(after []byte) bool {
	if len(after) == 0 {
		return false
	}
	if after[0] == '(' {
		return true
	}
	if after[0] == '[' {
		adv, ok := skipBalancedBrackets(after)
		return ok && adv < len(after) && after[adv] == '('
	}
	return false
}

func matchCreatorLine(line, creatorFunc []byte) bool {
	for len(creatorFunc) > 0 {
		brackIdx := bytes.IndexByte(creatorFunc, '[')
		if brackIdx < 0 {
			if !bytes.HasPrefix(line, creatorFunc) {
				return false
			}
			return isCallFrameBoundary(line[len(creatorFunc):])
		}
		if !bytes.HasPrefix(line, creatorFunc[:brackIdx]) {
			return false
		}
		line = line[brackIdx:]
		creatorFunc = creatorFunc[brackIdx:]
		cAdv, cOK := skipBalancedBrackets(creatorFunc)
		lAdv, lOK := skipBalancedBrackets(line)
		if !cOK || !lOK {
			return false
		}
		creatorFunc = creatorFunc[cAdv:]
		line = line[lAdv:]
	}
	return isCallFrameBoundary(line)
}

func containsCreatorCallFrame(stackPrefix, creatorFunc []byte) bool {
	creatorFunc = trimClosureOffset(creatorFunc)
	if len(creatorFunc) == 0 {
		return false
	}
	brackIdx := bytes.IndexByte(creatorFunc, '[')
	if brackIdx < 0 {
		search := stackPrefix
		for len(search) > len(creatorFunc) {
			idx := bytes.Index(search, creatorFunc)
			if idx < 0 {
				return false
			}
			afterIdx := idx + len(creatorFunc)
			if idx > 0 && search[idx-1] == '\n' && isCallFrameBoundary(search[afterIdx:]) {
				return true
			}
			search = search[idx+1:]
		}
		return false
	}
	if brackIdx == 0 {
		return false
	}
	search := stackPrefix
	for len(search) > brackIdx {
		idx := bytes.Index(search, creatorFunc[:brackIdx])
		if idx < 0 {
			return false
		}
		if idx > 0 && search[idx-1] == '\n' && matchCreatorLine(search[idx:], creatorFunc) {
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
		suffix = suffix[len("func"):]
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
		for plusIdx > 0 && (fn[plusIdx-1] == ' ' || fn[plusIdx-1] == '\t') {
			plusIdx--
		}
		return fn[:plusIdx]
	}
	return fn
}

func lastDotOutsideBrackets(s []byte) int {
	brackDepth := 0
	parenDepth := 0
	for i := len(s) - 1; i >= 0; i-- {
		switch s[i] {
		case ']':
			brackDepth++
		case '[':
			if brackDepth > 0 {
				brackDepth--
			}
		case ')':
			parenDepth++
		case '(':
			if parenDepth > 0 {
				parenDepth--
			}
		case '.':
			if brackDepth == 0 && parenDepth == 0 {
				return i
			}
		}
	}
	return -1
}

func hasDotAfterLastSlash(s []byte) bool {
	slashIdx := bytes.LastIndexByte(s, '/')
	if slashIdx >= 0 {
		s = s[slashIdx+1:]
	}
	return lastDotOutsideBrackets(s) > 0
}

func findClosureOwnerInStack(blk, creatorFunc []byte) []byte {
	curr := trimClosureOffset(creatorFunc)
	dot := lastDotOutsideBrackets(curr)
	if dot <= 0 || !isClosureSuffix(curr[dot+1:]) {
		return nil
	}
	for dot > 0 {
		curr = curr[:dot]
		if !hasDotAfterLastSlash(curr) {
			return nil
		}
		if containsCreatorCallFrame(blk, curr) {
			return curr
		}
		dot = lastDotOutsideBrackets(curr)
	}
	return nil
}

func trimFrameFuncName(fnLine []byte) []byte {
	for i := 0; i < len(fnLine); i++ {
		if fnLine[i] == '.' && i+2 < len(fnLine) && fnLine[i+1] == '(' && fnLine[i+2] == '*' {
			i += 3
			for i < len(fnLine) && fnLine[i] != ')' {
				if fnLine[i] == '[' {
					if adv, ok := skipBalancedBrackets(fnLine[i:]); ok {
						i += adv
						continue
					}
				}
				i++
			}
			continue
		}
		if fnLine[i] == '[' {
			if adv, ok := skipBalancedBrackets(fnLine[i:]); ok {
				i += adv - 1
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

func sameFuncSymbolIgnoringTypeArgs(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	for len(a) > 0 && len(b) > 0 {
		ai := bytes.IndexByte(a, '[')
		bi := bytes.IndexByte(b, '[')
		if ai != bi {
			return false
		}
		if ai < 0 {
			return bytes.Equal(a, b)
		}
		if !bytes.Equal(a[:ai], b[:bi]) {
			return false
		}
		aAdv, aOK := skipBalancedBrackets(a[ai:])
		bAdv, bOK := skipBalancedBrackets(b[bi:])
		if !aOK || !bOK {
			return false
		}
		a = a[ai+aAdv:]
		b = b[bi+bAdv:]
	}
	return len(a) == 0 && len(b) == 0
}

func hasClosureOwnedBy(aboveHook, blk, owner []byte) bool {
	rest := aboveHook
	const maxFrames = 16
	for range maxFrames {
		var fn []byte
		fn, rest = popBottomCallFrame(rest)
		if len(fn) == 0 {
			return false
		}
		if fnOwner := findClosureOwnerInStack(blk, fn); len(fnOwner) > 0 && sameFuncSymbolIgnoringTypeArgs(fnOwner, owner) {
			return true
		}
	}
	return false
}

func isInternalCallbackDispatchFrame(fn []byte) bool {
	return bytes.Contains(fn, []byte("deliverMultiCallbacks")) ||
		bytes.Contains(fn, []byte("enqueuePendingCallback")) ||
		bytes.Contains(fn, []byte("resolveOnEvict"))
}

func isOnlyDirectCallbackEntryAboveHook(blk []byte, hookIdx int, currCreator []byte) bool {
	creator := trimClosureOffset(currCreator)
	if len(creator) == 0 || hookIdx <= 0 {
		return false
	}
	rest := blk[:hookIdx]
	var entryFn []byte
	foundEntry := false
	for range 4 {
		entryFn, rest = popBottomCallFrame(rest)
		if len(entryFn) == 0 {
			return false
		}
		if !isInternalCallbackDispatchFrame(entryFn) {
			foundEntry = true
			break
		}
	}
	if !foundEntry || !sameFuncSymbolIgnoringTypeArgs(trimClosureOffset(entryFn), creator) {
		return false
	}
	return !containsCreatorCallFrame(rest, creator)
}

func isClosureFuncName(fn []byte) bool {
	curr := trimClosureOffset(fn)
	dot := lastDotOutsideBrackets(curr)
	return dot > 0 && isClosureSuffix(curr[dot+1:])
}

func isWrapperEntryFrame(fn, currCreator []byte) bool {
	fn = trimClosureOffset(fn)
	if !isClosureFuncName(fn) {
		return true
	}
	creator := trimClosureOffset(currCreator)
	if len(creator) == 0 || isClosureFuncName(creator) {
		return false
	}
	if dot := lastDotOutsideBrackets(fn); dot > 0 {
		return sameFuncSymbolIgnoringTypeArgs(fn[:dot], creator)
	}
	return false
}

func blockOwnsExitedChild(blk, currCreator, currChildStack []byte) bool {
	if len(currCreator) > 0 {
		if containsCreatorCallFrame(blk, currCreator) || len(findClosureOwnerInStack(blk, currCreator)) > 0 {
			return true
		}
	}
	const createdBy = "\ncreated by "
	end := bytes.LastIndex(currChildStack, []byte(createdBy))
	if end <= 0 {
		return false
	}
	rest := currChildStack[:end]
	for i := range 2 {
		var fn []byte
		fn, rest = popBottomCallFrame(rest)
		if len(fn) == 0 {
			return false
		}
		if containsCreatorCallFrame(blk, fn) || len(findClosureOwnerInStack(blk, fn)) > 0 {
			return true
		}
		if i == 0 && isWrapperEntryFrame(fn, currCreator) {
			continue
		}
		return false
	}
	return false
}

func findLiveIntermediateAncestorInAllStacks(allStacks, currCreator, currChildStack []byte, callerGID uint64, bridgedGIDs []uint64, matchAncestor, rejectGID func(uint64) bool) (bridgeGID, nextParentGID uint64, nextCreator, bridgeBlk []byte) {
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		gid, parentGID, creator := parseStackGoroutineParentAndCreator(blk)
		if gid == 0 || gid == callerGID || parentGID == 0 || matchAncestor(gid) || isRejectedAncestorGID(gid, rejectGID) {
			continue
		}
		if slices.Contains(bridgedGIDs, gid) {
			continue
		}
		if blockOwnsExitedChild(blk, currCreator, currChildStack) {
			return gid, parentGID, creator, blk
		}
	}
	return 0, 0, nil, nil
}

func findGoroutineBlockByID(allStacks []byte, targetGID uint64) (blk []byte, nextParentGID uint64, nextCreator []byte, found bool) {
	for rem := allStacks; len(rem) > 0; {
		blk, rem = nextGoroutineBlock(rem)
		gid, parentGID, creator := parseStackGoroutineParentAndCreator(blk)
		if gid == targetGID {
			return blk, parentGID, creator, true
		}
	}
	return nil, 0, nil, false
}

const maxBridgeHops = 8

type ancestorBridgeState struct {
	callerGID        uint64
	exitedCreator    []byte
	exitedChildStack []byte
	bridgedGIDs      [maxBridgeHops]uint64
	bridgeCount      int
}

func (s *ancestorBridgeState) backtrack(allStacks []byte, matchAncestor, rejectGID func(uint64) bool) (nextParentGID uint64, nextCreator, bridgeBlk []byte, ok bool) {
	if s.bridgeCount == 0 || s.bridgeCount >= maxBridgeHops {
		return 0, nil, nil, false
	}
	bridgeGID, pGID, creator, blk := findLiveIntermediateAncestorInAllStacks(
		allStacks, s.exitedCreator, s.exitedChildStack, s.callerGID, s.bridgedGIDs[:s.bridgeCount], matchAncestor, rejectGID,
	)
	if bridgeGID == 0 {
		return 0, nil, nil, false
	}
	s.bridgedGIDs[s.bridgeCount] = bridgeGID
	s.bridgeCount++
	return pGID, creator, blk, true
}

func (s *ancestorBridgeState) bridgeExited(allStacks, currCreator, currChildStack []byte, matchAncestor, rejectGID func(uint64) bool) (nextParentGID uint64, nextCreator, bridgeBlk []byte, ok bool) {
	if s.bridgeCount >= maxBridgeHops {
		return 0, nil, nil, false
	}
	if bridgeGID, pGID, creator, blk := findLiveIntermediateAncestorInAllStacks(
		allStacks, currCreator, currChildStack, s.callerGID, s.bridgedGIDs[:s.bridgeCount], matchAncestor, rejectGID,
	); bridgeGID != 0 {
		if s.bridgeCount == 0 {
			s.exitedCreator = currCreator
			s.exitedChildStack = currChildStack
		}
		s.bridgedGIDs[s.bridgeCount] = bridgeGID
		s.bridgeCount++
		return pGID, creator, blk, true
	}
	return s.backtrack(allStacks, matchAncestor, rejectGID)
}

func findAncestorCreatorAboveHook(allStacks, callerStack []byte, parentGID uint64, creatorFunc, hookName []byte, matchAncestor func(uint64) bool) (uint64, bool) {
	return findAncestorCreatorAboveHookOpts(allStacks, callerStack, parentGID, creatorFunc, hookName, matchAncestor, nil)
}

func isRejectedAncestorGID(currGID uint64, rejectGID func(uint64) bool) bool {
	return rejectGID != nil && rejectGID(currGID)
}

func handleRejectedAncestorGID(bridge *ancestorBridgeState, allStacks []byte, matchAncestor, rejectGID func(uint64) bool) (uint64, []byte, []byte, bool) {
	if bridge.bridgeCount == 0 {
		return 0, nil, nil, false
	}
	return bridge.backtrack(allStacks, matchAncestor, rejectGID)
}

type ancestorMatchDetail struct {
	gid           uint64
	holderBlk     []byte
	hopCreator    []byte
	hopChildStack []byte
	directHop     bool
}

func matchesTargetBlockAboveHook(blk, hookName []byte) bool {
	hookIdx := bytes.LastIndex(blk, hookName)
	return hookIdx >= 0 && hasCallFrameAboveHook(blk[:hookIdx])
}

func findAncestorCreatorAboveHookOpts(allStacks, callerStack []byte, parentGID uint64, creatorFunc, hookName []byte, matchAncestor, rejectGID func(uint64) bool) (uint64, bool) {
	det, ok := findAncestorCreatorAboveHookDetail(allStacks, callerStack, parentGID, creatorFunc, hookName, matchAncestor, rejectGID)
	return det.gid, ok
}

func findAncestorCreatorAboveHookDetail(allStacks, callerStack []byte, parentGID uint64, creatorFunc, hookName []byte, matchAncestor, rejectGID func(uint64) bool) (ancestorMatchDetail, bool) {
	if parentGID == 0 {
		return ancestorMatchDetail{}, false
	}
	if len(allStacks) == 0 {
		var bufPtr *[16384]byte
		allStacks, bufPtr = captureAllGoroutineStacks()
		defer releaseAllGoroutineStacks(bufPtr)
	}
	callerGID, _ := parseStackGoroutineAndParentID(callerStack)
	currGID := parentGID
	currCreator := creatorFunc
	currChildStack := callerStack
	bridge := ancestorBridgeState{
		callerGID:        callerGID,
		exitedCreator:    creatorFunc,
		exitedChildStack: callerStack,
	}
	const maxAncestorHops = 4096
	for range maxAncestorHops {
		var ok bool
		if currGID == 0 {
			currGID, currCreator, currChildStack, ok = bridge.backtrack(allStacks, matchAncestor, rejectGID)
			if !ok {
				return ancestorMatchDetail{}, false
			}
		}
		if isRejectedAncestorGID(currGID, rejectGID) {
			currGID, currCreator, currChildStack, ok = handleRejectedAncestorGID(&bridge, allStacks, matchAncestor, rejectGID)
			if !ok {
				return ancestorMatchDetail{}, false
			}
			continue
		}
		blk, nextParentGID, nextCreator, foundBlock := findGoroutineBlockByID(allStacks, currGID)
		if !foundBlock {
			if gid, hBlk, matched := matchAncestorFallbackForExactPostHook(allStacks, currCreator, currChildStack, creatorFunc, callerStack, hookName, matchAncestor, rejectGID); matched {
				return ancestorMatchDetail{gid: gid, holderBlk: hBlk, hopCreator: currCreator, hopChildStack: currChildStack}, true
			}
			currGID, currCreator, currChildStack, ok = bridge.bridgeExited(allStacks, currCreator, currChildStack, matchAncestor, rejectGID)
			if !ok {
				return ancestorMatchDetail{}, false
			}
			continue
		}
		if !matchAncestor(currGID) {
			currGID, currCreator, currChildStack = nextParentGID, nextCreator, blk
			continue
		}
		if matchesTargetBlockAboveHook(blk, hookName) {
			return ancestorMatchDetail{gid: currGID, holderBlk: blk, hopCreator: currCreator, hopChildStack: currChildStack, directHop: true}, true
		}
		currGID, currCreator, currChildStack, ok = bridge.backtrack(allStacks, matchAncestor, rejectGID)
		if !ok {
			return ancestorMatchDetail{}, false
		}
	}
	gid, hBlk, matched := matchAncestorFallbackForExactPostHook(allStacks, currCreator, currChildStack, creatorFunc, callerStack, hookName, matchAncestor, rejectGID)
	if !matched {
		return ancestorMatchDetail{}, false
	}
	return ancestorMatchDetail{gid: gid, holderBlk: hBlk, hopCreator: currCreator, hopChildStack: currChildStack}, true
}

func aboveHookOwnsSymbol(blk, aboveHook, sym []byte) bool {
	if len(sym) == 0 {
		return false
	}
	if containsCreatorCallFrame(aboveHook, sym) || len(findClosureOwnerInStack(aboveHook, sym)) > 0 {
		return true
	}
	owner := findClosureOwnerInStack(blk, sym)
	return len(owner) > 0 && hasClosureOwnedBy(aboveHook, blk, owner)
}

func aboveHookOwnsExitedChild(blk []byte, hookIdx int, currCreator, childStack []byte) bool {
	aboveHook := blk[:hookIdx]
	if aboveHookOwnsSymbol(blk, aboveHook, currCreator) {
		return true
	}
	end := bytes.LastIndex(childStack, []byte("\ncreated by "))
	if end <= 0 {
		return false
	}
	rest := childStack[:end]
	const maxBottomFrames = 8
	for range maxBottomFrames {
		var fn []byte
		fn, rest = popBottomCallFrame(rest)
		if len(fn) == 0 {
			return false
		}
		if bytes.Equal(fn, []byte("sync.(*WaitGroup).Go.func1")) {
			continue
		}
		if aboveHookOwnsSymbol(blk, aboveHook, fn) {
			return true
		}
	}
	return false
}

func hasLiveRejectedCreatorInAllStacks(allStacks, creatorFunc []byte, rejectGID func(uint64) bool) bool {
	if len(creatorFunc) == 0 || rejectGID == nil {
		return false
	}
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		gid, _ := parseStackGoroutineAndParentID(blk)
		if gid != 0 && rejectGID(gid) && containsCreatorCallFrame(blk, creatorFunc) {
			return true
		}
	}
	return false
}

func matchAncestorFallbackForExactPostHook(allStacks, currCreator, currChildStack, callerCreator, callerStack, hookName []byte, matchAncestor, rejectGID func(uint64) bool) (uint64, []byte, bool) {
	hasDistinctCaller := len(callerStack) > 0 && (len(currChildStack) == 0 || &callerStack[0] != &currChildStack[0])
	rejectedCreatorLive := hasLiveRejectedCreatorInAllStacks(allStacks, currCreator, rejectGID)
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		gid, _ := parseStackGoroutineAndParentID(blk)
		if gid == 0 || !matchAncestor(gid) {
			continue
		}
		hookIdx := bytes.LastIndex(blk, hookName)
		if hookIdx <= 0 || isOnlyDirectCallbackEntryAboveHook(blk, hookIdx, currCreator) {
			continue
		}
		if rejectedCreatorLive && !containsCreatorCallFrame(blk[:hookIdx], currCreator) {
			continue
		}
		if aboveHookOwnsExitedChild(blk, hookIdx, currCreator, currChildStack) || (hasDistinctCaller && aboveHookOwnsExitedChild(blk, hookIdx, callerCreator, callerStack)) {
			return gid, blk, true
		}
	}
	return 0, nil, false
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
		[]byte("invokeFastEvictCallbacks0("),
		[]byte("invokeFastEvictCallbacks1("),
		[]byte("invokeFastEvictCallbacks2("),
		[]byte("invokeFastEvictCallbacks3("),
		[]byte("invokeFastEvictCallbacks4("),
		[]byte("invokeFastEvictCallbacks5("),
		[]byte("invokeFastEvictCallbacks6("),
		[]byte("invokeFastEvictCallbacks7("),
	}
)

var (
	byteStrings       [256]string
	twoDigitStrings   [100]string
	threeDigitStrings [1000]string
	fourDigitsAll     string
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
	var threeDigitsRaw [3000]byte
	for i := range 1000 {
		threeDigitsRaw[i*3] = byte('0' + i/100)
		threeDigitsRaw[i*3+1] = byte('0' + (i/10)%10)
		threeDigitsRaw[i*3+2] = byte('0' + i%10)
	}
	allThreeDigits := string(threeDigitsRaw[:])
	for i := range 1000 {
		threeDigitStrings[i] = allThreeDigits[i*3 : i*3+3]
	}
	fourDigitsRaw := make([]byte, 40000)
	for i := range 10000 {
		fourDigitsRaw[i*4] = byte('0' + i/1000)
		fourDigitsRaw[i*4+1] = byte('0' + (i/100)%10)
		fourDigitsRaw[i*4+2] = byte('0' + (i/10)%10)
		fourDigitsRaw[i*4+3] = byte('0' + i%10)
	}
	fourDigitsAll = string(fourDigitsRaw)
}

func parseTwoASCIIDigits(c0, c1 byte) (int, bool) {
	d0, d1 := c0-'0', c1-'0'
	if d0 < 10 && d1 < 10 {
		return int(d0)*10 + int(d1), true
	}
	return 0, false
}

func parseThreeASCIIDigits(c0, c1, c2 byte) (int, bool) {
	d0, d1, d2 := c0-'0', c1-'0', c2-'0'
	if d0 < 10 && d1 < 10 && d2 < 10 {
		return int(d0)*100 + int(d1)*10 + int(d2), true
	}
	return 0, false
}

func parseFourASCIIDigits(c0, c1, c2, c3 byte) (int, bool) {
	d0, d1, d2, d3 := c0-'0', c1-'0', c2-'0', c3-'0'
	if d0 < 10 && d1 < 10 && d2 < 10 && d3 < 10 {
		return int(d0)*1000 + int(d1)*100 + int(d2)*10 + int(d3), true
	}
	return 0, false
}

func cloneMultiBytePrefix(s string) string {
	switch len(s) {
	case 2:
		if idx, ok := parseTwoASCIIDigits(s[0], s[1]); ok {
			return twoDigitStrings[idx]
		}
	case 3:
		if idx, ok := parseThreeASCIIDigits(s[0], s[1], s[2]); ok {
			return threeDigitStrings[idx]
		}
	case 4:
		if idx, ok := parseFourASCIIDigits(s[0], s[1], s[2], s[3]); ok {
			return fourDigitsAll[idx*4 : idx*4+4]
		}
	}
	return strings.Clone(s)
}

// clonePrefix returns an independent copy of s that never shares s's underlying backing array,
// using preallocated 1-byte and 2..4-digit strings to avoid heap allocations.
func clonePrefix(s string) string {
	switch len(s) {
	case 0:
		return ""
	case 1:
		return byteStrings[s[0]]
	default:
		return cloneMultiBytePrefix(s)
	}
}

func concatFourDigitPrefix(a, b string) (string, bool) {
	var c0, c1, c2, c3 byte
	switch len(a) {
	case 1:
		c0, c1, c2, c3 = a[0], b[0], b[1], b[2]
	case 2:
		c0, c1, c2, c3 = a[0], a[1], b[0], b[1]
	default:
		c0, c1, c2, c3 = a[0], a[1], a[2], b[0]
	}
	if idx, ok := parseFourASCIIDigits(c0, c1, c2, c3); ok {
		return fourDigitsAll[idx*4 : idx*4+4], true
	}
	return "", false
}

// concatPrefix concatenates two non-empty radix prefix edges, using preallocated
// 2..4-digit strings when applicable to avoid heap allocations during path compression.
func concatPrefix(a, b string) string {
	n := len(a) + len(b)
	switch {
	case n == 2 && len(a) == 1:
		if idx, ok := parseTwoASCIIDigits(a[0], b[0]); ok {
			return twoDigitStrings[idx]
		}
	case n == 3 && len(a) > 0 && len(b) > 0:
		c1 := b[0]
		if len(a) == 2 {
			c1 = a[1]
		}
		if idx, ok := parseThreeASCIIDigits(a[0], c1, b[len(b)-1]); ok {
			return threeDigitStrings[idx]
		}
	case n == 4 && len(a) > 0 && len(b) > 0:
		if s, ok := concatFourDigitPrefix(a, b); ok {
			return s
		}
	}
	return a + b
}

// longestCommonPrefix finds the length of the longest common prefix of a and b.
func longestCommonPrefix(a, b string) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
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

	pressureWriteMu            sync.Mutex
	hookIDVal                  atomic.Uint64
	primaryTagHint             atomic.Uint32
	evictTagHint               atomic.Uint32
	samplingPressure           atomic.Bool
	samplingActive             atomic.Bool
	samplingContended          atomic.Bool
	fallbackSampling           atomic.Bool
	pressureInitialized        atomic.Bool
	pressureNeedsRefresh       atomic.Bool
	anyShardReentrantReclaim   atomic.Bool
	reentrantReclaimCount      atomic.Int32
	overflowSamplingCount      atomic.Int32
	samplingGID                atomic.Uint64
	samplingParentGID          atomic.Uint64
	fallbackGID                atomic.Uint64
	fallbackParentGID          atomic.Uint64
	lastReentrantGID           atomic.Uint64
	nonDescendantParentGID     atomic.Uint64
	nonDescendantSeq           atomic.Uint64
	samplingReentrantReclaim   atomic.Bool
	fallbackReentrantReclaim   atomic.Bool
	overflowSamplingGIDs       sync.Map
	reentrantReclaimGIDs       sync.Map
	pressureSampleSeq          atomic.Uint64
	pressureInvokeSeq          atomic.Uint64
	lastSampledSeq             atomic.Uint64
	cachedPressureBits         atomic.Uint64
	cachedPressureEpoch        atomic.Uint64
	reclaimEpoch               atomic.Uint64
	externalReclaimEpoch       atomic.Uint64
	evictCallbackMu            sync.Mutex
	evictCallbackGID           atomic.Uint64
	evictCallbackParentGID     atomic.Uint64
	evictCallbackActive        atomic.Bool
	evictCallbackContended     atomic.Bool
	evictPendingEnqueues       atomic.Int32
	evictCallbackResolveMu     sync.Mutex
	pendingEvictCallbacks      []pendingEvictCallback
	samplingResolveMu          sync.Mutex
	primaryFastSlot            *atomic.Uint64
	primaryFastIdle            uint64
	primaryFastTag             uint32
	primaryGCreatedSample      [1]metrics.Sample
	evictFastSlot              *atomic.Uint64
	evictFastIdle              uint64
	evictFastTag               uint32
	evictCallbackEverContended atomic.Bool
	evictExactSnap             atomic.Bool
	evictHoldSeq               atomic.Uint64
	evictStartSnapSeq          atomic.Uint64
	ambiguousWaiters           []*ambiguousHookWaiter
	evictNonDescendantParent   atomic.Uint64
	evictNonDescendantHolder   atomic.Uint64
	belowTier1Epoch            atomic.Uint64
	belowTier2Epoch            atomic.Uint64
	cachedBelowTier1Epoch      atomic.Uint64
	cachedBelowTier2Epoch      atomic.Uint64
	cachedPressureSeq          atomic.Uint64
	cachedPublishSeq           atomic.Uint64
	zeroWatermarkTier2Epoch    uint64
	zeroWatermarkSeq           uint64
	sampledCoordEpoch          uint64
	sampledBelowTier1Epoch     uint64
	sampledBelowTier2Epoch     uint64
	sampledPressureSeq         uint64
	lastSampledEpoch           uint64
	childShards                []*pressureState
	exitedSamplerSeq           atomic.Uint64
	exitedSamplerGIDs          [64]atomic.Uint64
	panickedSamplerEpoch       atomic.Uint64
	panickedRootSeq            atomic.Uint64
	panickedDescendantSeq      atomic.Uint64
	primaryStartPanicEpoch     atomic.Uint64
	fallbackStartPanicEpoch    atomic.Uint64
	primaryStartSnapSeq        atomic.Uint64
	fallbackStartSnapSeq       atomic.Uint64
	preHookSnapSeq             atomic.Uint64
	lastObservedGCreated       atomic.Uint64
	preHookMu                  sync.Mutex
	preHookLiveGIDs            map[uint64]uint64
	hasPanickedEntries         atomic.Bool
	panickedMu                 sync.Mutex
	panickedRootMap            map[uint64]uint64
	panickedDescMap            map[uint64]uint64
}

func (p *pressureState) pressureStatePtr() *pressureState {
	return p
}

type pressureSample struct {
	epoch         uint64
	shardEpoch    uint64
	shardExtEpoch uint64
	extEpoch      uint64
	invokeSeq     uint64
	belowTier1    uint64
	belowTier2    uint64
	val           float64
	valid         bool
}

type shardSample struct {
	hasSample  bool
	valid      bool
	pressure   float64
	coordEpoch uint64
	belowTier1 uint64
	belowTier2 uint64
	seq        uint64
}

func (s *shardSample) advanceSelfReclaim(postShardEpoch, preShardEpoch uint64) {
	if s.hasSample && s.valid && postShardEpoch > preShardEpoch {
		s.coordEpoch += postShardEpoch - preShardEpoch
	}
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

func (p *pressureState) coord() *pressureState {
	if p.options.sharedPressure != nil {
		return p.options.sharedPressure
	}
	return p
}

func (p *pressureState) tier2TargetSize(maxSize uint64) uint64 {
	if p.options.hasShardTier2Target {
		return p.options.shardTier2Target
	}
	return computeTargetSize(maxSize, p.options.EvictionRetentionRatio)
}

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
		return id
	}
	return p.hookIDVal.Load()
}

func (p *pressureState) primaryTag(id uint64) uint32 {
	return (uint32(id-1) + p.primaryTagHint.Load()) & (numPressureHookTags - 1)
}

func (p *pressureState) setPrimaryTag(id uint64, tag uint32) {
	p.primaryTagHint.Store((tag - uint32(id-1)) & (numPressureHookTags - 1))
}

func (p *pressureState) evictTag(id uint64) uint32 {
	return (uint32(id-1) + p.evictTagHint.Load()) & (numPressureHookTags - 1)
}

func (p *pressureState) setEvictTag(id uint64, tag uint32) {
	p.evictTagHint.Store((tag - uint32(id-1)) & (numPressureHookTags - 1))
}

func (p *pressureState) cacheFastPrimarySlot(id uint64, base, tag uint32, idleState uint64) (uint32, bool) {
	if tag != base {
		p.setPrimaryTag(id, tag)
	}
	p.primaryFastSlot = &fastPrimaryTagOwner[tag]
	p.primaryFastIdle = idleState
	p.primaryFastTag = tag
	return tag, true
}

func (p *pressureState) claimFastPrimaryTagSlow() (uint32, bool) {
	id := p.hookIDVal.Load()
	if id == 0 {
		id = p.hookID()
	}
	base := p.primaryTag(id)
	idleState := id << 1
	activeState := idleState | 1
	for offset := range uint32(numPressureHookTags) {
		tag := (base + offset) & (numPressureHookTags - 1)
		cur := fastPrimaryTagOwner[tag].Load()
		if (cur == 0 || cur == idleState) && fastPrimaryTagOwner[tag].CompareAndSwap(cur, activeState) {
			return p.cacheFastPrimarySlot(id, base, tag, idleState)
		}
	}
	for offset := range uint32(numPressureHookTags) {
		tag := (base + offset) & (numPressureHookTags - 1)
		cur := fastPrimaryTagOwner[tag].Load()
		if cur&1 == 0 && fastPrimaryTagOwner[tag].CompareAndSwap(cur, activeState) {
			return p.cacheFastPrimarySlot(id, base, tag, idleState)
		}
	}
	return 0, false
}

func (p *pressureState) clearActiveFastPrimaryTag() {
	if slot := p.primaryFastSlot; slot != nil {
		slot.CompareAndSwap(p.primaryFastIdle|1, p.primaryFastIdle)
		return
	}
	if id := p.hookIDVal.Load(); id != 0 {
		activeState := (id << 1) | 1
		idleState := id << 1
		for tag := range uint32(numPressureHookTags) {
			if fastPrimaryTagOwner[tag].CompareAndSwap(activeState, idleState) {
				return
			}
		}
	}
}

func (p *pressureState) activeFastPrimaryTag() (uint32, bool) {
	id := p.hookIDVal.Load()
	if id == 0 {
		return 0, false
	}
	activeState := (id << 1) | 1
	tag := p.primaryTag(id)
	if fastPrimaryTagOwner[tag].Load() == activeState {
		return tag, true
	}
	for t := range uint32(numPressureHookTags) {
		if fastPrimaryTagOwner[t].Load() == activeState {
			return t, true
		}
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

func (p *pressureState) hasOtherActiveCustomSampler(holdsPrimary bool) bool {
	if !p.options.hasCustomPressureFunc {
		return false
	}
	return (!holdsPrimary && p.samplingPressure.Load()) ||
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

func canBeDescendantOf(gid, parentGID, ownerGID, ownerParentGID uint64) bool {
	if parentGID == 0 || ownerGID == 0 {
		return false
	}
	if parentGID == ownerGID {
		return true
	}
	if ownerParentGID != 0 && (gid == ownerParentGID || parentGID == ownerParentGID) {
		return false
	}
	return parentGID > 1
}

type overflowSamplerInfo struct {
	parentGID       uint64
	startPanicEpoch uint64
	startSnapSeq    uint64
}

func extractOverflowSamplerInfo(v any) overflowSamplerInfo {
	switch val := v.(type) {
	case overflowSamplerInfo:
		return val
	case uint64:
		return overflowSamplerInfo{parentGID: val}
	default:
		return overflowSamplerInfo{}
	}
}

func (p *pressureState) canBeDescendantOfActiveSampler(gid, parentGID uint64) bool {
	if parentGID == 0 {
		return false
	}
	if p.isCurrentGoroutineSampling(parentGID) {
		return true
	}
	if canBeDescendantOf(gid, parentGID, p.samplingGID.Load(), p.samplingParentGID.Load()) {
		return true
	}
	if canBeDescendantOf(gid, parentGID, p.fallbackGID.Load(), p.fallbackParentGID.Load()) {
		return true
	}
	if p.overflowSamplingCount.Load() <= 0 {
		return false
	}
	matched := false
	p.overflowSamplingGIDs.Range(func(k, v any) bool {
		oGID, _ := k.(uint64)
		info := extractOverflowSamplerInfo(v)
		if canBeDescendantOf(gid, parentGID, oGID, info.parentGID) {
			matched = true
			return false
		}
		return true
	})
	return matched
}

func (p *pressureState) initCoordinator(hasOnEvict bool) {
	if p.options.sharedPressure != nil {
		return
	}
	if p.options.hasCustomPressureFunc || hasOnEvict {
		p.observePreHookLiveGIDs(false)
	}
}

func collectLiveAndParentGIDs(allStacks []byte, snap uint64, liveGIDs map[uint64]uint64, keep map[uint64]struct{}) {
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		gID, pGID := parseStackGoroutineAndParentID(blk)
		if gID != 0 {
			if _, exists := liveGIDs[gID]; !exists {
				liveGIDs[gID] = snap
			}
			if keep != nil {
				keep[gID] = struct{}{}
			}
		}
		if pGID != 0 && keep != nil {
			keep[pGID] = struct{}{}
		}
	}
}

func pruneUnreferencedKeys(m map[uint64]uint64, keep map[uint64]struct{}) {
	for k := range m {
		if _, ok := keep[k]; !ok {
			delete(m, k)
		}
	}
}

func (p *pressureState) isEvictIdleForPrune(holdsEvictMu bool) bool {
	if p.isEvictCallbackActive() {
		return false
	}
	if holdsEvictMu {
		return true
	}
	if !p.evictCallbackMu.TryLock() {
		return false
	}
	p.evictCallbackMu.Unlock()
	return true
}

func (p *pressureState) shouldPrunePreHookMap(canPrune, holdsEvictMu, holdsPrimarySampler bool) bool {
	return canPrune && len(p.preHookLiveGIDs) > 512 && !p.hasOtherActiveCustomSampler(holdsPrimarySampler) && p.isEvictIdleForPrune(holdsEvictMu)
}

func (p *pressureState) observePreHookLiveGIDsWithSample(canPrune, holdsEvictMu, holdsPrimarySampler bool, sampleBuf *[1]metrics.Sample) uint64 {
	lastG := p.lastObservedGCreated.Load()
	var curG uint64
	if sampleBuf != nil {
		curG = readGoroutinesCreatedInto(sampleBuf)
	} else {
		curG = readGoroutinesCreated()
	}
	if snap := p.preHookSnapSeq.Load(); snap != 0 && curG == lastG {
		return snap
	}
	p.preHookMu.Lock()
	defer p.preHookMu.Unlock()
	curG = readGoroutinesCreated()
	if snap := p.preHookSnapSeq.Load(); snap != 0 && curG == p.lastObservedGCreated.Load() {
		return snap
	}
	if p.preHookLiveGIDs == nil {
		p.preHookLiveGIDs = make(map[uint64]uint64, 64)
	}
	allStacks, bufPtr := captureAllGoroutineStacks()
	defer releaseAllGoroutineStacks(bufPtr)
	newSnap := p.preHookSnapSeq.Load() + 1
	var keep map[uint64]struct{}
	if p.shouldPrunePreHookMap(canPrune, holdsEvictMu, holdsPrimarySampler) {
		keep = make(map[uint64]struct{}, 64)
	}
	collectLiveAndParentGIDs(allStacks, newSnap, p.preHookLiveGIDs, keep)
	if keep != nil {
		pruneUnreferencedKeys(p.preHookLiveGIDs, keep)
	}
	p.preHookSnapSeq.Store(newSnap)
	p.lastObservedGCreated.Store(curG)
	return newSnap
}

func (p *pressureState) observePreHookLiveGIDsOpts(canPrune, holdsEvictMu bool) uint64 {
	return p.observePreHookLiveGIDsWithSample(canPrune, holdsEvictMu, false, nil)
}

func (p *pressureState) observePreHookLiveGIDs(canPrune bool) uint64 {
	return p.observePreHookLiveGIDsWithSample(canPrune, false, false, nil)
}

func (p *pressureState) wasLiveAtOrBeforeSnap(gid, snap uint64) bool {
	if gid == 0 || snap == 0 {
		return false
	}
	p.preHookMu.Lock()
	seenSnap, ok := p.preHookLiveGIDs[gid]
	p.preHookMu.Unlock()
	return ok && seenSnap <= snap
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

func (p *pressureState) recordExitedSamplerGID(gid uint64) {
	if gid == 0 {
		return
	}
	idx := p.exitedSamplerSeq.Add(1) & uint64(len(p.exitedSamplerGIDs)-1)
	p.exitedSamplerGIDs[idx].Store(gid)
}

func recordPanickedMapEntryLocked(m map[uint64]uint64, seq *atomic.Uint64, gid, epoch uint64) bool {
	if gid == 0 {
		return false
	}
	if prev, exists := m[gid]; exists {
		if epoch < prev {
			m[gid] = epoch
		}
		return false
	}
	m[gid] = epoch
	seq.Add(1)
	return true
}

func (p *pressureState) prunePanickedMapsLocked(allStacks []byte) {
	if len(p.panickedRootMap)+len(p.panickedDescMap) <= 64 {
		return
	}
	keep := make(map[uint64]struct{}, 64)
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		gID, pGID := parseStackGoroutineAndParentID(blk)
		if gID != 0 {
			keep[gID] = struct{}{}
		}
		if pGID != 0 {
			keep[pGID] = struct{}{}
		}
	}
	pruneUnreferencedKeys(p.panickedRootMap, keep)
	pruneUnreferencedKeys(p.panickedDescMap, keep)
}

func (p *pressureState) lookupPanickedEpoch(gid uint64) (rootEpoch uint64, inRoot bool, descEpoch uint64, inDesc bool) {
	if gid == 0 || !p.hasPanickedEntries.Load() {
		return 0, false, 0, false
	}
	p.panickedMu.Lock()
	rootEpoch, inRoot = p.panickedRootMap[gid]
	descEpoch, inDesc = p.panickedDescMap[gid]
	p.panickedMu.Unlock()
	return rootEpoch, inRoot, descEpoch, inDesc
}

func (p *pressureState) isPanickedDescendantForEpoch(gid, startPanicEpoch uint64) bool {
	if gid == 0 || p.isCurrentGoroutineSampling(gid) {
		return false
	}
	_, _, descEpoch, inDesc := p.lookupPanickedEpoch(gid)
	return inDesc && descEpoch <= startPanicEpoch && !p.isCurrentGoroutineSampling(gid)
}

func (p *pressureState) isExitedOrPanickedForEpoch(gid, startPanicEpoch uint64) bool {
	if gid == 0 || p.isCurrentGoroutineSampling(gid) {
		return false
	}
	rootEpoch, inRoot, descEpoch, inDesc := p.lookupPanickedEpoch(gid)
	if (inRoot && rootEpoch <= startPanicEpoch) || (inDesc && descEpoch <= startPanicEpoch) {
		return !p.isCurrentGoroutineSampling(gid)
	}
	for i := range len(p.exitedSamplerGIDs) {
		if p.exitedSamplerGIDs[i].Load() == gid {
			return !p.isCurrentGoroutineSampling(gid)
		}
	}
	return false
}

func (p *pressureState) slotStartSnapSeq(gid uint64, slot int) uint64 {
	switch slot {
	case samplerSlotPrimary:
		return p.primaryStartSnapSeq.Load()
	case samplerSlotFallback:
		return p.fallbackStartSnapSeq.Load()
	default:
		if v, ok := p.overflowSamplingGIDs.Load(gid); ok {
			return extractOverflowSamplerInfo(v).startSnapSeq
		}
		return 0
	}
}

func (p *pressureState) seedDirectPanickedChildrenLocked(allStacks, callerStack []byte, hookIdx int, actualGID, startSnap uint64) bool {
	if !hasCallFrameAboveHook(callerStack[:hookIdx]) {
		return false
	}
	added := false
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		gID, pGID := parseStackGoroutineAndParentID(blk)
		if gID == 0 || gID == actualGID || pGID != actualGID || p.isCurrentGoroutineSampling(gID) {
			continue
		}
		if p.wasLiveAtOrBeforeSnap(gID, startSnap) {
			continue
		}
		if recordPanickedMapEntryLocked(p.panickedDescMap, &p.panickedDescendantSeq, gID, 0) {
			added = true
		}
	}
	return added
}

func (p *pressureState) recordDeadParentOrphanLocked(allStacks, callerStack []byte, hookIdx int, gCreator, blk []byte, pGID, gID, startSnap, mapEpoch uint64) bool {
	if pGID == 0 || p.wasLiveAtOrBeforeSnap(pGID, startSnap) {
		return false
	}
	if _, _, _, found := findGoroutineBlockByID(allStacks, pGID); found || !aboveHookOwnsExitedChild(callerStack, hookIdx, gCreator, blk) {
		return false
	}
	c1 := recordPanickedMapEntryLocked(p.panickedDescMap, &p.panickedDescendantSeq, pGID, mapEpoch)
	c2 := recordPanickedMapEntryLocked(p.panickedDescMap, &p.panickedDescendantSeq, gID, mapEpoch)
	return c1 || c2
}

func (p *pressureState) expandPanickedStepLocked(allStacks, callerStack []byte, hookIdx int, actualGID, startSnap, tagEpoch uint64, includeOrphans bool) bool {
	added := false
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		gID, pGID, gCreator := parseStackGoroutineParentAndCreator(blk)
		if gID == 0 || gID == actualGID || p.isCurrentGoroutineSampling(gID) || p.wasLiveAtOrBeforeSnap(gID, startSnap) {
			continue
		}
		if pGID != 0 {
			if parentEpoch, ok := p.panickedDescMap[pGID]; ok && (includeOrphans || parentEpoch == 0) {
				if recordPanickedMapEntryLocked(p.panickedDescMap, &p.panickedDescendantSeq, gID, max(parentEpoch, tagEpoch)) {
					added = true
				}
				continue
			}
		}
		if includeOrphans && p.recordDeadParentOrphanLocked(allStacks, callerStack, hookIdx, gCreator, blk, pGID, gID, startSnap, tagEpoch) {
			added = true
		}
	}
	return added
}

func (p *pressureState) populatePanickedLineageLocked(allStacks, callerStack []byte, hookIdx int, actualGID, startSnap, mapEpoch uint64) {
	if p.panickedRootMap == nil {
		p.panickedRootMap = make(map[uint64]uint64, 8)
		p.panickedDescMap = make(map[uint64]uint64, 16)
	}
	p.prunePanickedMapsLocked(allStacks)
	recordPanickedMapEntryLocked(p.panickedRootMap, &p.panickedRootSeq, actualGID, 0)
	if hookIdx < 0 {
		return
	}
	if p.seedDirectPanickedChildrenLocked(allStacks, callerStack, hookIdx, actualGID, startSnap) {
		for range 15 {
			if !p.expandPanickedStepLocked(allStacks, callerStack, hookIdx, actualGID, startSnap, 0, false) {
				break
			}
		}
	}
	for range 16 {
		if !p.expandPanickedStepLocked(allStacks, callerStack, hookIdx, actualGID, startSnap, mapEpoch, true) {
			break
		}
	}
}

func (p *pressureState) advancePanickedEpochAtLeast(target uint64) {
	for {
		cur := p.panickedSamplerEpoch.Load()
		if cur >= target || p.panickedSamplerEpoch.CompareAndSwap(cur, target) {
			return
		}
	}
}

func (p *pressureState) resolveExitingSamplerActualGID(callerStack []byte, gid uint64, slot int) uint64 {
	actualGID, _ := parseStackGoroutineAndParentID(callerStack)
	if actualGID == 0 {
		actualGID = gid
	}
	if actualGID == 0 && slot == samplerSlotPrimary {
		actualGID = p.samplingGID.Load()
	}
	return actualGID
}

func (p *pressureState) recordAbnormalSamplerExit(gid uint64, slot int) {
	p.panickedSamplerEpoch.Add(1)
	callerStack, callerBufPtr := captureCurrentGoroutineStack()
	defer releaseCurrentGoroutineStack(callerBufPtr)
	actualGID := p.resolveExitingSamplerActualGID(callerStack, gid, slot)
	if actualGID == 0 {
		return
	}
	startSnap := p.slotStartSnapSeq(actualGID, slot)
	hookIdx := bytes.LastIndex(callerStack, []byte("invokeAndStorePressure"))
	var allStacks []byte
	var allBufPtr *[16384]byte
	if hookIdx >= 0 {
		allStacks, allBufPtr = captureAllGoroutineStacks()
		defer releaseAllGoroutineStacks(allBufPtr)
	}
	p.panickedMu.Lock()
	mapEpoch := p.panickedSamplerEpoch.Load() + 1
	p.populatePanickedLineageLocked(allStacks, callerStack, hookIdx, actualGID, startSnap, mapEpoch)
	p.hasPanickedEntries.Store(true)
	p.advancePanickedEpochAtLeast(mapEpoch)
	p.panickedMu.Unlock()
}

func (p *pressureState) preRejectSamplerSlot(gid, parentGID, sGID, sParentGID, sig, panicEpoch, snapSeq uint64) bool {
	if sGID == 0 || !canBeDescendantOf(gid, parentGID, sGID, sParentGID) {
		return true
	}
	if p.isPanickedDescendantForEpoch(gid, panicEpoch) || p.wasLiveAtOrBeforeSnap(gid, snapSeq) {
		return true
	}
	if parentGID == sGID {
		return false
	}
	return p.isCachedNonDescendantParent(parentGID, sig) || p.wasLiveAtOrBeforeSnap(parentGID, snapSeq)
}

func (p *pressureState) isDescendantOfSamplerSlot(gid, parentGID uint64, creatorFunc, callerStack []byte, allStacks *[]byte, allBufPtr **([16384]byte), sGID, sParentGID, sig, panicEpoch, snapSeq uint64) bool {
	if p.preRejectSamplerSlot(gid, parentGID, sGID, sParentGID, sig, panicEpoch, snapSeq) {
		return false
	}
	if len(*allStacks) == 0 {
		*allStacks, *allBufPtr = captureAllGoroutineStacks()
	}
	rejectFn := func(ancGID uint64) bool {
		if ancGID == sGID {
			return false
		}
		if p.wasLiveAtOrBeforeSnap(ancGID, snapSeq) {
			return true
		}
		return p.isExitedOrPanickedForEpoch(ancGID, panicEpoch)
	}
	if _, ok := findAncestorCreatorAboveHookOpts(*allStacks, callerStack, parentGID, creatorFunc, []byte("invokeAndStorePressure"), func(ancGID uint64) bool {
		return ancGID == sGID
	}, rejectFn); ok {
		return true
	}
	if parentGID != sGID && sig != 0 && p.activeSamplerSig() == sig {
		p.nonDescendantParentGID.Store(parentGID)
		p.nonDescendantSeq.Store(sig)
	}
	return false
}

func (p *pressureState) checkOverflowSamplerSlots(gid, parentGID uint64, creatorFunc, callerStack []byte, allStacks *[]byte, allBufPtr **([16384]byte)) (uint64, bool) {
	if p.overflowSamplingCount.Load() <= 0 {
		return 0, false
	}
	var matchedGID uint64
	p.overflowSamplingGIDs.Range(func(k, v any) bool {
		oGID, _ := k.(uint64)
		info := extractOverflowSamplerInfo(v)
		if p.isDescendantOfSamplerSlot(gid, parentGID, creatorFunc, callerStack, allStacks, allBufPtr, oGID, info.parentGID, 0, info.startPanicEpoch, info.startSnapSeq) {
			matchedGID = oGID
			return false
		}
		return true
	})
	return matchedGID, matchedGID != 0
}

func (p *pressureState) checkSamplingGoroutineOrChildSlow() (bool, uint64, uint64) {
	if !p.hasActiveSampler() {
		return false, 0, 0
	}
	gid, parentGID, creatorFunc, callerStack, callerBufPtr, allStacks, allBufPtr := p.inspectCurrentAndResolvePrimary()
	defer releaseCurrentGoroutineStack(callerBufPtr)
	defer func() { releaseAllGoroutineStacks(allBufPtr) }()
	if p.isCurrentGoroutineSampling(gid) {
		return true, gid, parentGID
	}
	if !p.canBeDescendantOfActiveSampler(gid, parentGID) {
		return false, gid, parentGID
	}
	sig := p.activeSamplerSig()
	if sGID := p.samplingGID.Load(); sGID != 0 {
		if p.isDescendantOfSamplerSlot(gid, parentGID, creatorFunc, callerStack, &allStacks, &allBufPtr, sGID, p.samplingParentGID.Load(), sig, p.primaryStartPanicEpoch.Load(), p.primaryStartSnapSeq.Load()) {
			return true, sGID, parentGID
		}
	}
	if fGID := p.fallbackGID.Load(); fGID != 0 {
		if p.isDescendantOfSamplerSlot(gid, parentGID, creatorFunc, callerStack, &allStacks, &allBufPtr, fGID, p.fallbackParentGID.Load(), 0, p.fallbackStartPanicEpoch.Load(), p.fallbackStartSnapSeq.Load()) {
			return true, fGID, parentGID
		}
	}
	if oGID, ok := p.checkOverflowSamplerSlots(gid, parentGID, creatorFunc, callerStack, &allStacks, &allBufPtr); ok {
		return true, oGID, parentGID
	}
	return false, gid, parentGID
}

func (p *pressureState) checkSamplingGoroutineOrChild() (sampling bool, rootGID, parentGID uint64) {
	coord := p.coord()
	if coord.options.hasCustomPressureFunc && coord.hasActiveSampler() {
		return coord.checkSamplingGoroutineOrChildSlow()
	}
	return
}

func (p *pressureState) isSamplingGoroutine() bool {
	coord := p.coord()
	return coord.options.hasCustomPressureFunc && coord.hasActiveSampler() && coord.isSamplingGoroutineActiveSlow()
}

func (p *pressureState) isSamplingGoroutineActiveSlow() bool {
	sampling, _, _ := p.checkSamplingGoroutineOrChildSlow()
	return sampling
}

func (p *pressureState) storeReentrantGID(gid uint64) {
	if _, loaded := p.reentrantReclaimGIDs.LoadOrStore(gid, struct{}{}); !loaded {
		p.reentrantReclaimCount.Add(1)
	}
}

func (p *pressureState) recordChildSamplerReclaimOn(coord *pressureState) {
	p.lastReentrantGID.Store(0)
	if coord.samplingPressure.Load() {
		p.samplingReentrantReclaim.Store(true)
	}
	if coord.fallbackSampling.Load() {
		p.fallbackReentrantReclaim.Store(true)
	}
	if coord.overflowSamplingCount.Load() > 0 {
		coord.overflowSamplingGIDs.Range(func(k, _ any) bool {
			if oGID, ok := k.(uint64); ok && oGID != 0 {
				p.storeReentrantGID(oGID)
			}
			return true
		})
	}
}

func (p *pressureState) recordReentrantReclaimGIDOn(coord *pressureState, gid uint64) bool {
	switch {
	case gid == 0:
		if !coord.hasActiveSampler() {
			return false
		}
		p.recordChildSamplerReclaimOn(coord)
		return true
	case coord.samplingGID.Load() == gid:
		p.samplingReentrantReclaim.Store(true)
		return true
	case coord.fallbackGID.Load() == gid:
		p.fallbackReentrantReclaim.Store(true)
		return true
	case coord.hasOverflowSamplingGID(gid):
		p.storeReentrantGID(gid)
		return true
	default:
		return false
	}
}

func (p *pressureState) clearAllReentrantReclaims() {
	if p.lastReentrantGID.Load() != 0 {
		p.lastReentrantGID.Store(0)
	}
	if p.samplingReentrantReclaim.Load() {
		p.samplingReentrantReclaim.Store(false)
	}
	if p.fallbackReentrantReclaim.Load() {
		p.fallbackReentrantReclaim.Store(false)
	}
	if p.reentrantReclaimCount.Load() > 0 {
		p.reentrantReclaimGIDs.Range(func(k, _ any) bool {
			if _, loaded := p.reentrantReclaimGIDs.LoadAndDelete(k); loaded {
				p.reentrantReclaimCount.Add(-1)
			}
			return true
		})
	}
}

func (p *pressureState) clearReentrantSlot(gid uint64, slot int) bool {
	switch slot {
	case samplerSlotPrimary:
		return p.samplingReentrantReclaim.Load() && p.samplingReentrantReclaim.Swap(false)
	case samplerSlotFallback:
		return p.fallbackReentrantReclaim.Load() && p.fallbackReentrantReclaim.Swap(false)
	default:
		if gid == 0 || p.reentrantReclaimCount.Load() <= 0 {
			return false
		}
		_, reentrant := p.reentrantReclaimGIDs.LoadAndDelete(gid)
		if reentrant {
			p.reentrantReclaimCount.Add(-1)
		}
		return reentrant
	}
}

func (p *pressureState) clearChildShardsReentrantSlot(excludeShard *pressureState, gid uint64, slot int) {
	if len(p.childShards) == 0 || !p.anyShardReentrantReclaim.Load() {
		return
	}
	for _, s := range p.childShards {
		if s != excludeShard {
			s.clearReentrantSlot(gid, slot)
		}
	}
}

// markReclaimedLocked invalidates cached pre-reclamation pressure and increments reclaimEpoch.
// Caller MUST hold the cache's write lock.
func (p *pressureState) markReclaimedLocked() {
	p.coord().markReclaimedFromShard(p)
}

func (p *pressureState) invalidateCachedFieldsOnReclaimLocked() {
	seq := p.cachedPublishSeq.Load()
	if seq&1 == 0 {
		p.cachedPublishSeq.Store(seq + 1)
	}
	if !p.pressureNeedsRefresh.Load() {
		p.pressureNeedsRefresh.Store(true)
	}
	if p.pressureInitialized.Load() {
		p.pressureInitialized.Store(false)
	}
	if p.cachedPressureBits.Load() != 0 {
		p.cachedPressureBits.Store(0)
	}
}

func (p *pressureState) markReclaimedFromShard(shard *pressureState) {
	p.pressureWriteMu.Lock()
	var sampling bool
	var gid uint64
	if p.options.hasCustomPressureFunc && p.hasActiveSampler() {
		sampling, gid = p.checkSamplingGoroutine()
	}
	if sampling && p.recordReentrantReclaimGIDOn(p, gid) {
		p.lastReentrantGID.Store(gid)
	} else {
		sampling = false
		p.clearAllReentrantReclaims()
	}
	p.invalidateCachedFieldsOnReclaimLocked()
	if shard != p {
		shard.reclaimEpoch.Add(1)
		if sampling {
			p.anyShardReentrantReclaim.Store(true)
			shard.lastReentrantGID.Store(gid)
			_ = shard.recordReentrantReclaimGIDOn(p, gid)
		} else {
			shard.clearAllReentrantReclaims()
			shard.externalReclaimEpoch.Add(1)
		}
	}
	p.reclaimEpoch.Add(1)
	if !sampling {
		p.externalReclaimEpoch.Add(1)
	}
	p.pressureWriteMu.Unlock()
}

func (p *pressureState) hasElevatedPressureToInvalidate(pressure float64) bool {
	coord := p.coord()
	thresh := coord.options.CompactionThreshold
	if pressure >= thresh ||
		math.Float64frombits(coord.cachedPressureBits.Load()) >= thresh ||
		coord.pressureNeedsRefresh.Load() {
		return true
	}
	if !coord.hasActiveSampler() {
		return false
	}
	return !coord.isSamplingGoroutine()
}

func (p *pressureState) shouldInvalidatePressureOnReclaim(pressure float64) bool {
	coord := p.coord()
	thresh := coord.options.CompactionThreshold
	return pressure >= thresh ||
		math.Float64frombits(coord.cachedPressureBits.Load()) >= thresh ||
		coord.pressureNeedsRefresh.Load() ||
		coord.hasActiveSampler()
}

const (
	sampleStoreNormal = iota
	sampleStoreAdoptedCached
	sampleStoreAdoptedLast
)

func (p *pressureState) publishCachedPressureLocked(epoch, belowTier1, belowTier2, invokeSeq uint64, val float64, needsRefresh bool) {
	seq := p.cachedPublishSeq.Load()
	if seq&1 == 0 {
		seq++
		p.cachedPublishSeq.Store(seq)
	}
	p.cachedPressureEpoch.Store(epoch)
	if p.cachedBelowTier1Epoch.Load() != belowTier1 {
		p.cachedBelowTier1Epoch.Store(belowTier1)
	}
	if p.cachedBelowTier2Epoch.Load() != belowTier2 {
		p.cachedBelowTier2Epoch.Store(belowTier2)
	}
	p.cachedPressureSeq.Store(invokeSeq)
	bits := math.Float64bits(val)
	if p.cachedPressureBits.Load() != bits {
		p.cachedPressureBits.Store(bits)
	}
	if !p.pressureInitialized.Load() {
		p.pressureInitialized.Store(true)
	}
	if p.pressureNeedsRefresh.Load() != needsRefresh {
		p.pressureNeedsRefresh.Store(needsRefresh)
	}
	p.cachedPublishSeq.Store(seq + 1)
}

func (p *pressureState) invalidateCachedPressureLocked() {
	seq := p.cachedPublishSeq.Load()
	if seq&1 == 0 {
		seq++
		p.cachedPublishSeq.Store(seq)
	}
	p.pressureNeedsRefresh.Store(true)
	p.cachedPublishSeq.Store(seq + 1)
}

func (p *pressureState) republishCachedRecoveryEpochsLocked(belowTier1, belowTier2 uint64) {
	oldTier1 := p.cachedBelowTier1Epoch.Load()
	oldTier2 := p.cachedBelowTier2Epoch.Load()
	if belowTier1 <= oldTier1 && belowTier2 <= oldTier2 {
		return
	}
	seq := p.cachedPublishSeq.Load()
	if seq&1 == 0 {
		seq++
		p.cachedPublishSeq.Store(seq)
	}
	if belowTier1 > oldTier1 {
		p.cachedBelowTier1Epoch.Store(belowTier1)
	}
	if belowTier2 > oldTier2 {
		p.cachedBelowTier2Epoch.Store(belowTier2)
	}
	p.cachedPublishSeq.Store(seq + 1)
}

func (p *pressureState) syncCachedRecoveryEpochsLocked(belowTier1, belowTier2 uint64) {
	if !p.pressureInitialized.Load() || p.pressureNeedsRefresh.Load() {
		return
	}
	cachedVal := math.Float64frombits(p.cachedPressureBits.Load())
	if (belowTier2 > p.cachedBelowTier2Epoch.Load() && cachedVal >= p.options.EvictionThreshold) ||
		(belowTier1 > p.cachedBelowTier1Epoch.Load() && cachedVal >= p.options.CompactionThreshold) {
		p.invalidateCachedPressureLocked()
		return
	}
	p.republishCachedRecoveryEpochsLocked(belowTier1, belowTier2)
}

func (p *pressureState) readValidCachedSampleForEpoch(epoch uint64) (float64, uint64, uint64, uint64, bool) {
	seq1 := p.cachedPublishSeq.Load()
	if seq1 == 0 || seq1&1 != 0 || !p.pressureInitialized.Load() || p.pressureNeedsRefresh.Load() {
		return 0.0, 0, 0, 0, false
	}
	if p.cachedPressureEpoch.Load() != epoch {
		return 0.0, 0, 0, 0, false
	}
	bits := p.cachedPressureBits.Load()
	belowTier1 := p.cachedBelowTier1Epoch.Load()
	belowTier2 := p.cachedBelowTier2Epoch.Load()
	sampleSeq := p.cachedPressureSeq.Load()
	if p.cachedPublishSeq.Load() != seq1 || !p.isCachedSampleStillValid(epoch) {
		return 0.0, 0, 0, 0, false
	}
	return math.Float64frombits(bits), belowTier1, belowTier2, sampleSeq, true
}

func (p *pressureState) advanceRecoveryEpochsForValLocked(val float64, preBelowTier1, preBelowTier2 uint64) (uint64, uint64) {
	belowTier1 := preBelowTier1
	if val < p.options.CompactionThreshold {
		belowTier1 = p.belowTier1Epoch.Add(1)
	}
	belowTier2 := preBelowTier2
	if val < p.options.EvictionThreshold {
		belowTier2 = p.belowTier2Epoch.Add(1)
	}
	return belowTier1, belowTier2
}

func (p *pressureState) isPreSampleElevatedCausallyStaleLocked(val float64, preBelowTier1, preBelowTier2 uint64) bool {
	if val >= p.options.EvictionThreshold {
		return preBelowTier2 < p.belowTier2Epoch.Load()
	}
	if val >= p.options.CompactionThreshold {
		return preBelowTier1 < p.belowTier1Epoch.Load()
	}
	return false
}

func (p *pressureState) recordAcceptedFallbackSample(sampleEpoch, invokeSeq, preBelowTier1, preBelowTier2 uint64, val float64) (uint64, uint64) {
	p.pressureWriteMu.Lock()
	defer p.pressureWriteMu.Unlock()
	if p.isPreSampleElevatedCausallyStaleLocked(val, preBelowTier1, preBelowTier2) {
		return preBelowTier1, preBelowTier2
	}
	if invokeSeq != 0 {
		lastSeq := p.lastSampledSeq.Load()
		if invokeSeq > lastSeq {
			p.lastSampledSeq.Store(invokeSeq)
			p.lastSampledEpoch = sampleEpoch
			p.lastSampledPressureBits.Store(math.Float64bits(val))
			belowTier1, belowTier2 := p.advanceRecoveryEpochsForValLocked(val, preBelowTier1, preBelowTier2)
			p.syncCachedRecoveryEpochsLocked(belowTier1, belowTier2)
			return belowTier1, belowTier2
		}
		if invokeSeq == lastSeq {
			p.syncCachedRecoveryEpochsLocked(preBelowTier1, preBelowTier2)
		}
	}
	return preBelowTier1, preBelowTier2
}

func (p *pressureState) isSampleCausallyStaleLocked(val float64, belowTier1, belowTier2, sampleSeq uint64) bool {
	coord := p.coord()
	if val >= p.options.EvictionThreshold {
		return belowTier2 < coord.belowTier2Epoch.Load()
	}
	if val >= p.options.CompactionThreshold {
		return belowTier1 < coord.belowTier1Epoch.Load()
	}
	return sampleSeq != 0 && sampleSeq < p.sampledPressureSeq
}

func (p *pressureState) isElevatedSampleStaleLocked(val float64, belowTier1, belowTier2, sampleSeq uint64) bool {
	return val >= p.options.CompactionThreshold && p.isSampleCausallyStaleLocked(val, belowTier1, belowTier2, sampleSeq)
}

func (p *pressureState) adoptNewerCachedSampleLocked(minSeq uint64) (float64, uint64, uint64, uint64, bool) {
	coord := p.coord()
	val, belowTier1, belowTier2, seq, ok := coord.readValidCachedSampleForEpoch(coord.reclaimEpoch.Load())
	if !ok || seq <= minSeq || seq < p.sampledPressureSeq {
		return 0, 0, 0, 0, false
	}
	return val, belowTier1, belowTier2, seq, true
}

func (p *pressureState) resolvePostRetryPressureLocked(s pressureSample) (uint64, float64) {
	if !s.valid && p.isSamplingGoroutine() {
		p.hasValidSample = false
		return p.reclaimEpoch.Load(), 0.0
	}
	coord := p.coord()
	currentEpoch := coord.reclaimEpoch.Load()
	acceptedCoordEpoch := currentEpoch
	belowTier1 := s.belowTier1
	belowTier2 := s.belowTier2
	sampleSeq := s.invokeSeq
	var resolvedPressure float64
	shardUnreclaimed := s.valid && p.reclaimEpoch.Load() == s.shardEpoch
	if s.valid && s.epoch == currentEpoch {
		p.hasValidSample = true
		resolvedPressure = s.val
	} else if cachedVal, cachedBelowTier1, cachedBelowTier2, cachedSeq, ok := coord.readValidCachedSampleForEpoch(currentEpoch); ok && (!shardUnreclaimed || cachedSeq >= s.invokeSeq) {
		p.hasValidSample = true
		belowTier1 = cachedBelowTier1
		belowTier2 = cachedBelowTier2
		sampleSeq = cachedSeq
		resolvedPressure = cachedVal
	} else if shardUnreclaimed && !p.isElevatedSampleStaleLocked(s.val, s.belowTier1, s.belowTier2, s.invokeSeq) {
		p.hasValidSample = true
		acceptedCoordEpoch = s.epoch
		belowTier1, belowTier2 = coord.recordAcceptedFallbackSample(s.epoch, s.invokeSeq, s.belowTier1, s.belowTier2, s.val)
		resolvedPressure = s.val
	} else {
		p.hasValidSample = false
		resolvedPressure = 0.0
	}
	if p.hasValidSample && p.isElevatedSampleStaleLocked(resolvedPressure, belowTier1, belowTier2, sampleSeq) {
		p.hasValidSample = false
		resolvedPressure = 0.0
	}
	p.sampledCoordEpoch = acceptedCoordEpoch
	p.syncZeroWatermarkLocked(belowTier1, belowTier2, sampleSeq, resolvedPressure)
	return p.reclaimEpoch.Load(), resolvedPressure
}

func (p *pressureState) sampleForLock(fresh bool) pressureSample {
	if fresh {
		return p.coord().samplePressureFreshForShard(p)
	}
	return p.samplePressureWithEpoch()
}

func (p *pressureState) tryFastForwardSampledEpochLocked(targetShard *pressureState, s pressureSample) pressureSample {
	if !s.valid || !p.options.hasCustomPressureFunc {
		return s
	}
	coordCanFF := p.externalReclaimEpoch.Load() == s.extEpoch
	shardNeedsFF := targetShard != p && targetShard.reclaimEpoch.Load() != s.shardEpoch && targetShard.externalReclaimEpoch.Load() == s.shardExtEpoch
	if !coordCanFF && !shardNeedsFF {
		return s
	}
	p.pressureWriteMu.Lock()
	defer p.pressureWriteMu.Unlock()
	if p.canFastForwardLowPressureEpochLocked(s.val) {
		if p.externalReclaimEpoch.Load() == s.extEpoch {
			s.epoch = p.reclaimEpoch.Load()
			s.shardEpoch = targetShard.reclaimEpoch.Load()
		} else if targetShard != p && targetShard.externalReclaimEpoch.Load() == s.shardExtEpoch {
			s.shardEpoch = targetShard.reclaimEpoch.Load()
		}
	}
	return s
}

func (p *pressureState) advanceLockRetryBudgetLocked(sampledShardEpoch uint64, sameShardRetries, crossShardRetries *int) bool {
	if p.reclaimEpoch.Load() != sampledShardEpoch {
		*sameShardRetries++
	} else {
		*crossShardRetries++
	}
	return *sameShardRetries <= 2 && *crossShardRetries <= 4
}

func (p *pressureState) resolveLockLoopStepLocked(s pressureSample, fresh bool, sameShardRetries, crossShardRetries *int) (pressureSample, bool) {
	coord := p.coord()
	if s.epoch != coord.reclaimEpoch.Load() {
		s = coord.tryFastForwardSampledEpochLocked(p, s)
	}
	if s.epoch == coord.reclaimEpoch.Load() {
		if !s.valid || !p.isElevatedSampleStaleLocked(s.val, s.belowTier1, s.belowTier2, s.invokeSeq) {
			return s, false
		}
		if !fresh {
			if val, belowTier1, belowTier2, seq, ok := p.adoptNewerCachedSampleLocked(s.invokeSeq); ok {
				s.val = val
				s.belowTier1 = belowTier1
				s.belowTier2 = belowTier2
				s.invokeSeq = seq
				return s, false
			}
		}
	}
	if !p.advanceLockRetryBudgetLocked(s.shardEpoch, sameShardRetries, crossShardRetries) {
		return s, false
	}
	return s, true
}

func (p *pressureState) lockWithPressureSlow(mu *sync.RWMutex, fresh bool) (uint64, float64) {
	s := p.sampleForLock(fresh)
	mu.Lock()
	coord := p.coord()
	if s.valid && s.epoch == coord.reclaimEpoch.Load() && !p.isElevatedSampleStaleLocked(s.val, s.belowTier1, s.belowTier2, s.invokeSeq) {
		p.hasValidSample = true
		p.sampledCoordEpoch = s.epoch
		p.syncZeroWatermarkLocked(s.belowTier1, s.belowTier2, s.invokeSeq, s.val)
		return p.reclaimEpoch.Load(), s.val
	}
	var sameShardRetries, crossShardRetries int
	for {
		var retry bool
		s, retry = p.resolveLockLoopStepLocked(s, fresh, &sameShardRetries, &crossShardRetries)
		if !retry {
			break
		}
		mu.Unlock()
		s = p.sampleForLock(fresh)
		mu.Lock()
	}
	return p.resolvePostRetryPressureLocked(s)
}

func (p *pressureState) canUseCachedSampleForEpoch(epoch, seq uint64) bool {
	return (seq&pressureSampleWindowMask) != 1 &&
		p.pressureInitialized.Load() &&
		!p.pressureNeedsRefresh.Load() &&
		p.cachedPressureEpoch.Load() == epoch
}

func (p *pressureState) isCachedSampleStillValid(epoch uint64) bool {
	return p.pressureInitialized.Load() &&
		!p.pressureNeedsRefresh.Load() &&
		p.cachedPressureEpoch.Load() == epoch &&
		p.reclaimEpoch.Load() == epoch
}

func (p *pressureState) lockWithPressureFastMiss(mu *sync.RWMutex) (uint64, float64) {
	coord := p.coord()
	s := coord.samplePressureFreshForShard(p)
	mu.Lock()
	if s.epoch == coord.reclaimEpoch.Load() && !(s.valid && p.isElevatedSampleStaleLocked(s.val, s.belowTier1, s.belowTier2, s.invokeSeq)) {
		if !s.valid && p.isSamplingGoroutine() {
			p.hasValidSample = false
			return p.reclaimEpoch.Load(), 0.0
		}
		p.hasValidSample = s.valid
		p.sampledCoordEpoch = s.epoch
		p.syncZeroWatermarkLocked(s.belowTier1, s.belowTier2, s.invokeSeq, s.val)
		return p.reclaimEpoch.Load(), s.val
	}
	mu.Unlock()
	return p.lockWithPressureSlow(mu, false)
}

func (p *pressureState) lockWithPressure(mu *sync.RWMutex, fresh bool) (uint64, float64) {
	coord := p.coord()
	if !fresh && !coord.options.hasCustomPressureFunc {
		epoch := coord.reclaimEpoch.Load()
		if coord.canUseCachedSampleForEpoch(epoch, p.pressureSampleSeq.Add(1)) {
			if pressure, belowTier1, belowTier2, sampleSeq, ok := coord.readValidCachedSampleForEpoch(epoch); ok {
				mu.Lock()
				if epoch == coord.reclaimEpoch.Load() && !p.isElevatedSampleStaleLocked(pressure, belowTier1, belowTier2, sampleSeq) {
					p.hasValidSample = true
					p.sampledCoordEpoch = epoch
					p.syncZeroWatermarkLocked(belowTier1, belowTier2, sampleSeq, pressure)
					return p.reclaimEpoch.Load(), pressure
				}
				mu.Unlock()
				return p.lockWithPressureSlow(mu, false)
			}
		}
		return p.lockWithPressureFastMiss(mu)
	}
	return p.lockWithPressureSlow(mu, fresh)
}

func (p *pressureState) canReuseShardSample(validSample bool) bool {
	return validSample || p.isSamplingGoroutine()
}

func (p *pressureState) adoptCachedShardSampleLocked(minSeq uint64, sample *shardSample) bool {
	coord := p.coord()
	curCoordEpoch := coord.reclaimEpoch.Load()
	val, belowTier1, belowTier2, seq, ok := coord.readValidCachedSampleForEpoch(curCoordEpoch)
	if !ok || seq < minSeq || seq < p.sampledPressureSeq {
		return false
	}
	if p.isElevatedSampleStaleLocked(val, belowTier1, belowTier2, seq) {
		return false
	}
	*sample = shardSample{
		hasSample:  true,
		valid:      true,
		pressure:   val,
		coordEpoch: curCoordEpoch,
		belowTier1: belowTier1,
		belowTier2: belowTier2,
		seq:        seq,
	}
	p.hasValidSample = true
	p.sampledCoordEpoch = curCoordEpoch
	p.syncZeroWatermarkLocked(sample.belowTier1, sample.belowTier2, sample.seq, sample.pressure)
	return true
}

func (p *pressureState) tryApplyShardSampleLocked(fresh bool, sample *shardSample) bool {
	if !sample.hasSample {
		return false
	}
	if !sample.valid {
		if !p.isSamplingGoroutine() {
			return false
		}
		p.hasValidSample = false
		return true
	}
	coord := p.coord()
	epochMismatch := coord.reclaimEpoch.Load() != sample.coordEpoch
	causallyStale := p.isSampleCausallyStaleLocked(sample.pressure, sample.belowTier1, sample.belowTier2, sample.seq)
	newerOnShard := !fresh && sample.seq != 0 && sample.seq < p.sampledPressureSeq
	if epochMismatch || causallyStale || newerOnShard {
		if !fresh {
			minSeq := sample.seq
			if !epochMismatch {
				minSeq++
			}
			if p.adoptCachedShardSampleLocked(minSeq, sample) {
				return true
			}
		}
		if epochMismatch || (causallyStale && sample.pressure >= p.options.CompactionThreshold) {
			return false
		}
	}
	p.hasValidSample = true
	p.sampledCoordEpoch = sample.coordEpoch
	p.syncZeroWatermarkLocked(sample.belowTier1, sample.belowTier2, sample.seq, sample.pressure)
	return true
}

func (p *pressureState) lockShardWithSample(mu *sync.RWMutex, fresh bool, sample shardSample) (uint64, shardSample) {
	if sample.hasSample {
		mu.Lock()
		if p.tryApplyShardSampleLocked(fresh, &sample) {
			return p.reclaimEpoch.Load(), sample
		}
		mu.Unlock()
	}
	sampledEpoch, resolvedPressure := p.lockWithPressure(mu, fresh)
	valid := p.hasValidSample
	var coordEpoch, belowTier1, belowTier2, seq uint64
	if valid {
		coordEpoch = p.sampledCoordEpoch
		belowTier1 = p.sampledBelowTier1Epoch
		belowTier2 = p.sampledBelowTier2Epoch
		seq = p.sampledPressureSeq
	}
	return sampledEpoch, shardSample{
		hasSample:  p.canReuseShardSample(valid),
		valid:      valid,
		pressure:   resolvedPressure,
		coordEpoch: coordEpoch,
		belowTier1: belowTier1,
		belowTier2: belowTier2,
		seq:        seq,
	}
}

func (p *pressureState) recordLastSampledTelemetryLocked(originalEpoch, epoch, invokeSeq uint64, val float64, willStore, elevatedStale bool) bool {
	if elevatedStale {
		return false
	}
	if !willStore && p.reclaimEpoch.Load() != epoch {
		return false
	}
	if !willStore && p.pressureInitialized.Load() && p.cachedPressureEpoch.Load() == p.reclaimEpoch.Load() {
		return false
	}
	lastSeq := p.lastSampledSeq.Load()
	if invokeSeq >= lastSeq || (willStore && originalEpoch > p.lastSampledEpoch) {
		if invokeSeq > lastSeq {
			p.lastSampledSeq.Store(invokeSeq)
		}
		p.lastSampledEpoch = epoch
		p.lastSampledPressureBits.Store(math.Float64bits(val))
		return true
	}
	return false
}

func (p *pressureState) canFastForwardLowPressureEpochLocked(val float64) bool {
	if val >= p.options.CompactionThreshold {
		return false
	}
	curEpoch := p.reclaimEpoch.Load()
	return !p.pressureInitialized.Load() ||
		p.cachedPressureEpoch.Load() != curEpoch ||
		math.Float64frombits(p.cachedPressureBits.Load()) < p.options.CompactionThreshold
}

func (p *pressureState) isSoleReentrantReclaimerLocked(reentrant, noPeerPanic bool, resolvedGID uint64) bool {
	if !reentrant || !noPeerPanic {
		return false
	}
	lastGID := p.lastReentrantGID.Load()
	return lastGID == 0 || lastGID == resolvedGID
}

func (p *pressureState) resolveTargetShardEpochLocked(targetShard *pressureState, shardEpoch, shardExtEpoch uint64, shardReentrant, noPeerPanic bool, resolvedGID uint64, val float64) uint64 {
	if targetShard == p || targetShard.reclaimEpoch.Load() == shardEpoch || targetShard.externalReclaimEpoch.Load() != shardExtEpoch {
		return shardEpoch
	}
	if targetShard.isSoleReentrantReclaimerLocked(shardReentrant, noPeerPanic, resolvedGID) || p.canFastForwardLowPressureEpochLocked(val) {
		return targetShard.reclaimEpoch.Load()
	}
	return shardEpoch
}

func (p *pressureState) resolveSampleEpochsLocked(targetShard *pressureState, shardEpoch, shardExtEpoch, epoch, extEpoch uint64, val float64, gid uint64, slot int, noPeerPanic bool) (uint64, uint64, bool) {
	curEpoch := p.reclaimEpoch.Load()
	allowStore := curEpoch == epoch
	if !p.options.hasCustomPressureFunc {
		return epoch, shardEpoch, allowStore
	}

	reentrant := p.clearReentrantSlot(gid, slot)
	shardReentrant := targetShard != p && targetShard.clearReentrantSlot(gid, slot)
	p.clearChildShardsReentrantSlot(targetShard, gid, slot)
	resolvedGID := gid
	if resolvedGID == 0 && slot == samplerSlotPrimary {
		resolvedGID = p.samplingGID.Load()
	}

	if !allowStore && p.externalReclaimEpoch.Load() == extEpoch {
		switch {
		case p.isSoleReentrantReclaimerLocked(reentrant, noPeerPanic, resolvedGID):
			epoch = curEpoch
			shardEpoch = targetShard.reclaimEpoch.Load()
			allowStore = true
		case p.canFastForwardLowPressureEpochLocked(val):
			epoch = curEpoch
			shardEpoch = targetShard.reclaimEpoch.Load()
		}
	}
	shardEpoch = p.resolveTargetShardEpochLocked(targetShard, shardEpoch, shardExtEpoch, shardReentrant, noPeerPanic, resolvedGID, val)
	return epoch, shardEpoch, allowStore
}

func (p *pressureState) decideOlderInvokeSeqLocked(originalEpoch, epoch, invokeSeq uint64, val float64, hasCachedAtEpoch bool) (uint64, float64, bool, int) {
	if p.lastSampledEpoch == epoch {
		if !hasCachedAtEpoch {
			p.pressureNeedsRefresh.Store(true)
		}
		return p.lastSampledSeq.Load(), math.Float64frombits(p.lastSampledPressureBits.Load()), false, sampleStoreAdoptedLast
	}
	if originalEpoch <= p.lastSampledEpoch {
		if !hasCachedAtEpoch {
			p.pressureNeedsRefresh.Store(true)
		}
		return invokeSeq, val, false, sampleStoreNormal
	}
	return invokeSeq, val, true, sampleStoreNormal
}

func (p *pressureState) decideSampleStoreLocked(originalEpoch, epoch, curEpoch, invokeSeq uint64, val float64, allowStore bool) (uint64, float64, bool, int) {
	if !allowStore {
		if !p.pressureInitialized.Load() || p.cachedPressureEpoch.Load() != curEpoch {
			p.pressureNeedsRefresh.Store(true)
		}
		return invokeSeq, val, false, sampleStoreNormal
	}
	hasCachedAtEpoch := p.pressureInitialized.Load() && !p.pressureNeedsRefresh.Load() && p.cachedPressureEpoch.Load() == epoch
	if hasCachedAtEpoch && invokeSeq != 0 && invokeSeq < p.cachedPressureSeq.Load() {
		return p.cachedPressureSeq.Load(), math.Float64frombits(p.cachedPressureBits.Load()), false, sampleStoreAdoptedCached
	}
	if invokeSeq != 0 && invokeSeq < p.lastSampledSeq.Load() {
		return p.decideOlderInvokeSeqLocked(originalEpoch, epoch, invokeSeq, val, hasCachedAtEpoch)
	}
	return invokeSeq, val, true, sampleStoreNormal
}

func (p *pressureState) finishFastPrimaryLocked() {
	exitingGID := p.resolveExitingPrimaryGID(0, true)
	p.recordExitedSamplerGID(exitingGID)
	p.samplingActive.Store(false)
	p.clearActiveFastPrimaryTag()
	if p.samplingContended.Load() || p.samplingGID.Load() != 0 {
		p.samplingResolveMu.Lock()
		if lateGID := p.samplingGID.Load(); lateGID != 0 && lateGID != exitingGID {
			p.recordExitedSamplerGID(lateGID)
		}
		p.samplingGID.Store(0)
		p.samplingParentGID.Store(0)
		p.nonDescendantSeq.Store(0)
		p.samplingContended.Store(false)
		p.samplingResolveMu.Unlock()
	}
	p.samplingPressure.Store(false)
}

func (p *pressureState) storeSampledPressureWithSeq(targetShard *pressureState, shardEpoch, shardExtEpoch, epoch, extEpoch, preBelowTier1, preBelowTier2, invokeSeq uint64, val float64, gid uint64, slot int, noPeerPanic, releaseFastPrimary bool) (uint64, uint64, uint64, uint64, uint64, float64) {
	p.pressureWriteMu.Lock()

	originalEpoch := epoch
	curEpoch := p.reclaimEpoch.Load()
	elevatedStale := p.isPreSampleElevatedCausallyStaleLocked(val, preBelowTier1, preBelowTier2)
	epoch, shardEpoch, allowStore := p.resolveSampleEpochsLocked(targetShard, shardEpoch, shardExtEpoch, epoch, extEpoch, val, gid, slot, noPeerPanic)
	invokeSeq, val, willStore, storeMode := p.decideSampleStoreLocked(originalEpoch, epoch, curEpoch, invokeSeq, val, allowStore)

	var belowTier1, belowTier2 uint64
	switch {
	case storeMode == sampleStoreAdoptedCached:
		belowTier1 = p.cachedBelowTier1Epoch.Load()
		belowTier2 = p.cachedBelowTier2Epoch.Load()
	case storeMode == sampleStoreAdoptedLast:
		belowTier1 = p.belowTier1Epoch.Load()
		belowTier2 = p.belowTier2Epoch.Load()
	case p.recordLastSampledTelemetryLocked(originalEpoch, epoch, invokeSeq, val, willStore, elevatedStale):
		belowTier1, belowTier2 = p.advanceRecoveryEpochsForValLocked(val, preBelowTier1, preBelowTier2)
	default:
		belowTier1 = preBelowTier1
		belowTier2 = preBelowTier2
	}

	if willStore {
		p.publishCachedPressureLocked(epoch, belowTier1, belowTier2, invokeSeq, val, elevatedStale)
	}
	if releaseFastPrimary {
		p.finishFastPrimaryLocked()
	}
	p.pressureWriteMu.Unlock()
	return epoch, shardEpoch, belowTier1, belowTier2, invokeSeq, val
}

func (p *pressureState) sanitizePressureValue(val float64) float64 {
	if math.IsNaN(val) || val < 0.0 {
		return 0.0
	}
	if math.IsInf(val, 1) {
		return max(1.0, p.options.CompactionThreshold, p.options.EvictionThreshold)
	}
	return val
}

//go:noinline
func (p *pressureState) invokeAndStorePressure(targetShard *pressureState, gid uint64, slot int, fastPrimary bool, tag uint32) pressureSample {
	completed := false
	releasedPrimary := false
	prePanicEpoch := p.panickedSamplerEpoch.Load()
	if p.options.hasCustomPressureFunc {
		p.clearReentrantSlot(gid, slot)
		p.clearChildShardsReentrantSlot(nil, gid, slot)
		defer func() {
			if !completed {
				p.recordAbnormalSamplerExit(gid, slot)
			}
			if fastPrimary && !releasedPrimary {
				p.releasePrimarySamplerSlot(gid, true, completed)
			}
		}()
	}
	shardExtEpoch := targetShard.externalReclaimEpoch.Load()
	shardEpoch := targetShard.reclaimEpoch.Load()
	extEpoch := p.externalReclaimEpoch.Load()
	epoch := p.reclaimEpoch.Load()
	preBelowTier1 := p.belowTier1Epoch.Load()
	preBelowTier2 := p.belowTier2Epoch.Load()
	invokeSeq := p.pressureInvokeSeq.Add(1)
	var val float64
	if fastPrimary {
		val = p.dispatchFastPrimaryPressure(tag)
	} else {
		val = p.options.PressureFunc()
	}
	completed = true
	val = p.sanitizePressureValue(val)
	noPeerPanic := p.panickedSamplerEpoch.Load() == prePanicEpoch
	var belowTier1, belowTier2 uint64
	epoch, shardEpoch, belowTier1, belowTier2, invokeSeq, val = p.storeSampledPressureWithSeq(targetShard, shardEpoch, shardExtEpoch, epoch, extEpoch, preBelowTier1, preBelowTier2, invokeSeq, val, gid, slot, noPeerPanic, fastPrimary)
	releasedPrimary = fastPrimary
	return pressureSample{
		epoch:         epoch,
		shardEpoch:    shardEpoch,
		shardExtEpoch: shardExtEpoch,
		extEpoch:      extEpoch,
		invokeSeq:     invokeSeq,
		belowTier1:    belowTier1,
		belowTier2:    belowTier2,
		val:           val,
		valid:         true,
	}
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure0() float64 {
	return p.options.PressureFunc()
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure1() float64 {
	return p.options.PressureFunc()
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure2() float64 {
	return p.options.PressureFunc()
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure3() float64 {
	return p.options.PressureFunc()
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure4() float64 {
	return p.options.PressureFunc()
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure5() float64 {
	return p.options.PressureFunc()
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure6() float64 {
	return p.options.PressureFunc()
}

//go:noinline
func (p *pressureState) invokeFastPrimaryPressure7() float64 {
	return p.options.PressureFunc()
}

func (p *pressureState) dispatchFastPrimaryPressure(tag uint32) float64 {
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

func (p *pressureState) resolveExitingPrimaryGID(gid uint64, completed bool) uint64 {
	if gid == 0 {
		gid = p.samplingGID.Load()
	}
	if gid == 0 && (!completed || p.fallbackSampling.Load() || p.overflowSamplingCount.Load() > 0) {
		gid, _ = p.ensureSamplingGID(0, 0)
	}
	return gid
}

func (p *pressureState) releasePrimarySamplerSlot(gid uint64, fastPrimary, completed bool) {
	p.pressureWriteMu.Lock()
	exitingGID := p.resolveExitingPrimaryGID(gid, completed)
	p.recordExitedSamplerGID(exitingGID)
	p.clearReentrantSlot(exitingGID, samplerSlotPrimary)
	p.clearChildShardsReentrantSlot(nil, exitingGID, samplerSlotPrimary)
	p.samplingActive.Store(false)
	if fastPrimary {
		p.clearActiveFastPrimaryTag()
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
	p.pressureWriteMu.Unlock()
}

func (p *pressureState) prepareSamplerLineage(canPrune bool) (uint64, uint64) {
	return p.panickedSamplerEpoch.Load(), p.observePreHookLiveGIDs(canPrune)
}

func (p *pressureState) preparePrimarySamplerLineage(canPrune bool) {
	panicEpoch := p.panickedSamplerEpoch.Load()
	snapSeq := p.observePreHookLiveGIDsWithSample(canPrune, false, true, &p.primaryGCreatedSample)
	if p.primaryStartPanicEpoch.Load() != panicEpoch {
		p.primaryStartPanicEpoch.Store(panicEpoch)
	}
	if p.primaryStartSnapSeq.Load() != snapSeq {
		p.primaryStartSnapSeq.Store(snapSeq)
	}
}

//go:noinline
func (p *pressureState) sampleWithSlot(targetShard *pressureState, gid, parentGID uint64, canPrune bool) pressureSample {
	if !p.options.hasCustomPressureFunc {
		defer p.samplingPressure.Store(false)
		return p.invokeAndStorePressure(targetShard, 0, samplerSlotPrimary, false, 0)
	}
	p.preparePrimarySamplerLineage(canPrune)
	if gid == 0 {
		if slot := p.primaryFastSlot; slot != nil && slot.CompareAndSwap(p.primaryFastIdle, p.primaryFastIdle|1) {
			p.samplingActive.Store(true)
			return p.invokeAndStorePressure(targetShard, 0, samplerSlotPrimary, true, p.primaryFastTag)
		}
		if tag, ok := p.claimFastPrimaryTagSlow(); ok {
			p.samplingActive.Store(true)
			return p.invokeAndStorePressure(targetShard, 0, samplerSlotPrimary, true, tag)
		}
	}
	gid, parentGID = p.ensureSamplingGID(gid, parentGID)
	p.samplingResolveMu.Lock()
	p.samplingParentGID.Store(parentGID)
	p.samplingGID.Store(gid)
	p.samplingActive.Store(true)
	p.samplingResolveMu.Unlock()
	completed := false
	defer func() {
		p.releasePrimarySamplerSlot(gid, false, completed)
	}()
	s := p.invokeAndStorePressure(targetShard, gid, samplerSlotPrimary, false, 0)
	completed = true
	return s
}

//go:noinline
func (p *pressureState) sampleWithFallbackSlot(targetShard *pressureState, gid, parentGID uint64) pressureSample {
	if p.options.hasCustomPressureFunc {
		panicEpoch, snapSeq := p.prepareSamplerLineage(false)
		gid, parentGID = p.ensureSamplingGID(gid, parentGID)
		p.fallbackStartPanicEpoch.Store(panicEpoch)
		p.fallbackStartSnapSeq.Store(snapSeq)
		p.fallbackParentGID.Store(parentGID)
		p.fallbackGID.Store(gid)
		defer func() {
			p.pressureWriteMu.Lock()
			p.recordExitedSamplerGID(gid)
			p.clearReentrantSlot(gid, samplerSlotFallback)
			p.clearChildShardsReentrantSlot(nil, gid, samplerSlotFallback)
			p.fallbackGID.Store(0)
			p.fallbackParentGID.Store(0)
			p.fallbackSampling.Store(false)
			p.pressureWriteMu.Unlock()
		}()
	} else {
		defer p.fallbackSampling.Store(false)
	}
	return p.invokeAndStorePressure(targetShard, gid, samplerSlotFallback, false, 0)
}

func (p *pressureState) sampleWithOverflowSlot(targetShard *pressureState, gid, parentGID uint64) pressureSample {
	p.overflowSamplingCount.Add(1)
	var storedGID uint64
	defer func() {
		if p.options.hasCustomPressureFunc {
			p.pressureWriteMu.Lock()
			p.recordExitedSamplerGID(storedGID)
			if storedGID != 0 {
				p.overflowSamplingGIDs.Delete(storedGID)
			}
			p.overflowSamplingCount.Add(-1)
			p.clearReentrantSlot(storedGID, samplerSlotOverflow)
			p.clearChildShardsReentrantSlot(nil, storedGID, samplerSlotOverflow)
			p.pressureWriteMu.Unlock()
			return
		}
		p.overflowSamplingCount.Add(-1)
	}()
	if p.options.hasCustomPressureFunc {
		panicEpoch, snapSeq := p.prepareSamplerLineage(false)
		gid, parentGID = p.ensureSamplingGID(gid, parentGID)
		storedGID = gid
		p.overflowSamplingGIDs.Store(gid, overflowSamplerInfo{
			parentGID:       parentGID,
			startPanicEpoch: panicEpoch,
			startSnapSeq:    snapSeq,
		})
	}
	return p.invokeAndStorePressure(targetShard, gid, samplerSlotOverflow, false, 0)
}

func (p *pressureState) reentrantFallbackPressureSample(targetShard *pressureState) pressureSample {
	shardEpoch := targetShard.reclaimEpoch.Load()
	return pressureSample{
		epoch:      p.reclaimEpoch.Load(),
		shardEpoch: shardEpoch,
		belowTier1: p.belowTier1Epoch.Load(),
		belowTier2: p.belowTier2Epoch.Load(),
	}
}

func (p *pressureState) samplePressureFreshOnCoord(targetShard *pressureState) pressureSample {
	if p.options.PressureFunc == nil {
		shardEpoch := targetShard.reclaimEpoch.Load()
		extEpoch := p.externalReclaimEpoch.Load()
		return pressureSample{
			epoch:      p.reclaimEpoch.Load(),
			shardEpoch: shardEpoch,
			extEpoch:   extEpoch,
			belowTier1: p.belowTier1Epoch.Load(),
			belowTier2: p.belowTier2Epoch.Load(),
			valid:      true,
		}
	}
	hadActive := p.hasActiveSampler()
	var sampling bool
	var gid, parentGID uint64
	if p.options.hasCustomPressureFunc {
		sampling, gid, parentGID = p.checkSamplingGoroutineOrChildSlow()
		if sampling {
			return p.reentrantFallbackPressureSample(targetShard)
		}
	}
	if p.samplingPressure.CompareAndSwap(false, true) {
		return p.sampleWithSlot(targetShard, gid, parentGID, true)
	}
	if p.options.hasCustomPressureFunc {
		if !hadActive {
			sampling, gid, parentGID = p.checkSamplingGoroutineOrChildSlow()
			if sampling {
				return p.reentrantFallbackPressureSample(targetShard)
			}
		}
		gid, parentGID = p.ensureSamplingGID(gid, parentGID)
	}
	if p.fallbackSampling.CompareAndSwap(false, true) {
		return p.sampleWithFallbackSlot(targetShard, gid, parentGID)
	}
	return p.sampleWithOverflowSlot(targetShard, gid, parentGID)
}

// samplePressureFreshForShard evaluates p.options.PressureFunc lock-free outside the cache lock
// with a goroutine-aware re-entrancy guard and cold-start fallback.
func (p *pressureState) samplePressureFreshForShard(targetShard *pressureState) pressureSample {
	return p.coord().samplePressureFreshOnCoord(targetShard)
}

// samplePressureWithEpoch returns the current memory pressure and epoch for foreground cache operations.
// Custom PressureFunc callbacks are invoked on every call; the default runtime/metrics probe is amortized
// across a 256-operation window (and immediately refreshed after any reclamation/compaction cycle).
func (p *pressureState) samplePressureWithEpoch() pressureSample {
	coord := p.coord()
	if coord.options.hasCustomPressureFunc {
		return coord.samplePressureFreshOnCoord(p)
	}
	epoch := coord.reclaimEpoch.Load()
	if !coord.canUseCachedSampleForEpoch(epoch, p.pressureSampleSeq.Add(1)) {
		return coord.samplePressureFreshOnCoord(p)
	}
	shardEpoch := p.reclaimEpoch.Load()
	val, belowTier1, belowTier2, sampleSeq, ok := coord.readValidCachedSampleForEpoch(epoch)
	if !ok {
		return coord.samplePressureFreshOnCoord(p)
	}
	return pressureSample{
		epoch:      epoch,
		shardEpoch: shardEpoch,
		invokeSeq:  sampleSeq,
		belowTier1: belowTier1,
		belowTier2: belowTier2,
		val:        val,
		valid:      true,
	}
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

func (p *pressureState) shouldAutoCompactMap(dirty, isBackground bool, currentLen int) bool {
	return p.shouldAutoCompactEntryCounts(dirty && (currentLen < p.peakEntryLen || p.deletedSinceCompact >= minChurnCompactDeletes), isBackground, currentLen)
}

func (p *pressureState) needsMutationReclaim(reclaimedPre bool, currentSize, sizeBefore uint64, pressure float64) bool {
	return reclaimedPre || currentSize < sizeBefore || pressure >= p.options.CompactionThreshold || pressure >= p.options.EvictionThreshold
}

func (p *pressureState) hasEmptyDeleteSlack(extraSlack bool) bool {
	return extraSlack || p.peakEntryLen > minPeakSlackEntries || p.deletedSinceCompact >= minChurnCompactDeletes
}

func (p *pressureState) shouldReclaimSingleSurvivorOnDelete(currentLen int, isDirty, extraChurn bool) bool {
	return currentLen == 1 && isDirty && (extraChurn || p.peakEntryLen >= minChurnCompactDeletes || p.deletedSinceCompact >= minChurnCompactDeletes)
}

//go:noinline
func (p *pressureState) shouldReclaimEmptyPrePutSlow(extraChurn, evictedPre bool, newSize, sizeBefore uint64, pressure float64) bool {
	return extraChurn || p.peakEntryLen >= minChurnCompactDeletes || p.deletedSinceCompact >= minChurnCompactDeletes || (evictedPre && newSize < sizeBefore && p.hasElevatedPressureToInvalidate(pressure))
}

func (p *pressureState) shouldReclaimEmptyPrePut(currentLen int, extraChurn, evictedPre bool, newSize, sizeBefore uint64, pressure float64) bool {
	if currentLen == 0 {
		return p.shouldReclaimEmptyPrePutSlow(extraChurn, evictedPre, newSize, sizeBefore, pressure)
	}
	return false
}

//go:noinline
func (p *pressureState) shouldReclaimSingleSurvivorSlow(currentLen int, extraChurn, evictedPre bool, postSize, sizeBefore uint64, pressure float64) bool {
	if currentLen != 1 && (!evictedPre || currentLen-p.zeroSizeCount > 1) {
		return false
	}
	return extraChurn || p.peakEntryLen >= minChurnCompactDeletes || p.deletedSinceCompact >= minChurnCompactDeletes ||
		(currentLen == 1 && postSize < sizeBefore && p.hasElevatedPressureToInvalidate(pressure))
}

func (p *pressureState) shouldReclaimSingleSurvivorOnMutation(currentLen int, isDirty, extraChurn, evictedPre bool, postSize, sizeBefore uint64, pressure float64) bool {
	if isDirty && (currentLen <= 1 || p.zeroSizeCount > 0) {
		return p.shouldReclaimSingleSurvivorSlow(currentLen, extraChurn, evictedPre, postSize, sizeBefore, pressure)
	}
	return false
}

func (p *pressureState) shouldCompactAfterMutation(reclaimedPre, netByteReduced, isDirty bool, pressure float64) bool {
	return isDirty && (reclaimedPre || (netByteReduced && pressure >= p.options.CompactionThreshold && uint64(p.deletedSinceCompact)*slackQuarterMultiplier >= uint64(p.peakEntryLen)))
}

func (p *pressureState) shouldMarkReclaimedAfterMutation(reclaimedPre, netByteReduced bool, sampledEpoch uint64, pressure float64) bool {
	return (reclaimedPre || netByteReduced) && p.shouldInvalidatePressureOnReclaim(pressure) && p.reclaimEpoch.Load() == sampledEpoch
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
	if p.zeroSizeCount > 0 {
		if currentLen > 0 {
			targetLen = int(float64(currentLen) * retention)
			if (!hasProtected || protectedSize <= targetSize) && p.lastReclaimedLen > targetLen {
				targetLen = p.lastReclaimedLen
			}
		}
		targetZeroCount, unprotectedZeroTarget = p.computeZeroShedTargets(retention, hasProtected, protectedSize)
	}
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
		p.zeroWatermarkTier2Epoch = p.sampledBelowTier2Epoch
		p.zeroWatermarkSeq = p.sampledPressureSeq
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
	return uint64(p.zeroSizeCount) > maxSize && uint64(p.zeroSizeCount)-maxSize > maxSize-currentSize-valueSize
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
	if reclaimedOrReduced && p.shouldInvalidatePressureOnReclaim(pressure) && p.reclaimEpoch.Load() == sampledEpoch {
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
	buf       [2]evictEvent[V]
	overflow  []evictEvent[V]
	n         int
	exactSnap bool
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
	tag := p.evictTag(id)
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

func (p *pressureState) isCachedNonDescendantEvictParent(parentGID, holdSeq uint64) bool {
	return parentGID != 0 && holdSeq != 0 &&
		p.evictNonDescendantHolder.Load() == holdSeq &&
		p.evictNonDescendantParent.Load() == parentGID
}

func (p *pressureState) refreshEvictSnapLocked() {
	snap := p.observePreHookLiveGIDsOpts(true, true)
	p.evictStartSnapSeq.Store(snap)
	p.evictExactSnap.Store(true)
}

func (p *pressureState) beginEvictCallbackHold(exact bool) {
	seq := p.evictHoldSeq.Load() + 1
	if exact || seq <= 64 || (seq&255) == 0 || p.evictCallbackEverContended.Load() {
		p.refreshEvictSnapLocked()
	} else {
		p.evictExactSnap.Store(false)
	}
	p.evictHoldSeq.Store(seq)
}

func (p *pressureState) preRejectEvictDescendant(gid, parentGID, holder, holdSeq, startSnap uint64) bool {
	if !canBeDescendantOf(gid, parentGID, holder, p.evictCallbackParentGID.Load()) {
		return true
	}
	if p.wasLiveAtOrBeforeSnap(gid, startSnap) {
		return true
	}
	if parentGID == holder {
		return false
	}
	return p.isCachedNonDescendantEvictParent(parentGID, holdSeq) || p.wasLiveAtOrBeforeSnap(parentGID, startSnap)
}

func (p *pressureState) cacheNonDescendantEvictParent(parentGID, holder, holdSeq uint64) {
	if holdSeq != 0 && parentGID != holder && p.evictHoldSeq.Load() == holdSeq && p.evictCallbackGID.Load() == holder {
		p.evictNonDescendantParent.Store(parentGID)
		p.evictNonDescendantHolder.Store(holdSeq)
	}
}

func (p *pressureState) findEvictHolderDetail(allStacks, stack []byte, gid, parentGID uint64, creatorFunc []byte, holder, holdSeq, startSnap uint64) (ancestorMatchDetail, bool) {
	if p.preRejectEvictDescendant(gid, parentGID, holder, holdSeq, startSnap) {
		return ancestorMatchDetail{}, false
	}
	rejectFn := func(ancGID uint64) bool {
		return ancGID != holder && p.wasLiveAtOrBeforeSnap(ancGID, startSnap)
	}
	det, ok := findAncestorCreatorAboveHookDetail(allStacks, stack, parentGID, creatorFunc, []byte("deliverCallbacks"), func(ancGID uint64) bool {
		return ancGID == holder
	}, rejectFn)
	if ok {
		return det, true
	}
	p.cacheNonDescendantEvictParent(parentGID, holder, holdSeq)
	return ancestorMatchDetail{}, false
}

func (p *pressureState) isDescendantOfEvictHolder(allStacks, stack []byte, gid, parentGID uint64, creatorFunc []byte, holder, holdSeq, startSnap uint64) bool {
	_, ok := p.findEvictHolderDetail(allStacks, stack, gid, parentGID, creatorFunc, holder, holdSeq, startSnap)
	return ok
}

func callbackEntryFrameAboveHook(aboveHook []byte) []byte {
	rest := aboveHook
	for range 4 {
		var entryFn []byte
		entryFn, rest = popBottomCallFrame(rest)
		if len(entryFn) == 0 {
			return nil
		}
		if !isInternalCallbackDispatchFrame(entryFn) {
			return trimClosureOffset(entryFn)
		}
	}
	return nil
}

func isOwnedByCallbackEntry(sym, cbEntryFn []byte) bool {
	sym = trimClosureOffset(sym)
	if len(sym) == 0 || len(cbEntryFn) == 0 {
		return false
	}
	if sameFuncSymbolIgnoringTypeArgs(sym, cbEntryFn) {
		return true
	}
	for {
		dot := lastDotOutsideBrackets(sym)
		if dot <= 0 || !isClosureSuffix(sym[dot+1:]) {
			return false
		}
		sym = sym[:dot]
		if sameFuncSymbolIgnoringTypeArgs(sym, cbEntryFn) {
			return true
		}
	}
}

func firstUserEntryFrame(childStack []byte) []byte {
	end := bytes.LastIndex(childStack, []byte("\ncreated by "))
	if end <= 0 {
		return nil
	}
	rest := childStack[:end]
	for range 4 {
		var fn []byte
		fn, rest = popBottomCallFrame(rest)
		if len(fn) == 0 {
			return nil
		}
		if bytes.Equal(fn, []byte("sync.(*WaitGroup).Go.func1")) {
			continue
		}
		return trimClosureOffset(fn)
	}
	return nil
}

func isStrictlyCallbackOwnedHop(belowHook, hopCreator, entryFn, cbEntryFn []byte) bool {
	if len(cbEntryFn) == 0 || containsCreatorCallFrame(belowHook, cbEntryFn) {
		return false
	}
	return isOwnedByCallbackEntry(hopCreator, cbEntryFn) || isOwnedByCallbackEntry(entryFn, cbEntryFn)
}

func isDirectBelowHookNonChild(aboveHook, belowHook, hopCreator []byte, directHop, isWGGo bool) bool {
	if !directHop || isWGGo || len(hopCreator) == 0 {
		return false
	}
	return containsCreatorCallFrame(belowHook, hopCreator) && !containsCreatorCallFrame(aboveHook, hopCreator)
}

func isStrictBelowHookClosure(aboveHook, belowHook, fn, cbEntryFn []byte) bool {
	if isOwnedByCallbackEntry(fn, cbEntryFn) {
		return false
	}
	owner := findClosureOwnerInStack(belowHook, fn)
	return len(owner) > 0 && !containsCreatorCallFrame(aboveHook, owner)
}

func hasBelowHookOwnedClosureEntry(aboveHook, belowHook, childStack, cbEntryFn []byte) bool {
	end := bytes.LastIndex(childStack, []byte("\ncreated by "))
	if end <= 0 {
		return false
	}
	rest := childStack[:end]
	for range 4 {
		var fn []byte
		fn, rest = popBottomCallFrame(rest)
		if len(fn) == 0 {
			return false
		}
		if isStrictBelowHookClosure(aboveHook, belowHook, fn, cbEntryFn) {
			return true
		}
	}
	return false
}

func classifyHolderChildHop(det ancestorMatchDetail) (int, bool) {
	hookIdx := bytes.LastIndex(det.holderBlk, []byte("deliverCallbacks"))
	if hookIdx <= 0 {
		return evictContendLockDirect, false
	}
	aboveHook := det.holderBlk[:hookIdx]
	belowHook := det.holderBlk[hookIdx:]
	hopCreator := trimClosureOffset(det.hopCreator)
	cbEntryFn := callbackEntryFrameAboveHook(aboveHook)
	entryFn := firstUserEntryFrame(det.hopChildStack)
	if isStrictlyCallbackOwnedHop(belowHook, hopCreator, entryFn, cbEntryFn) {
		return evictContendEnqueueChild, false
	}
	isWGGo := det.directHop && bytes.Equal(hopCreator, []byte("sync.(*WaitGroup).Go"))
	if isDirectBelowHookNonChild(aboveHook, belowHook, hopCreator, det.directHop, isWGGo) {
		return evictContendLockDirect, false
	}
	belowClosureViaHelper := det.directHop && !containsCreatorCallFrame(aboveHook, hopCreator) &&
		hasBelowHookOwnedClosureEntry(aboveHook, belowHook, det.hopChildStack, cbEntryFn)
	return evictContendWaitAmbiguous, isWGGo || belowClosureViaHelper
}

const (
	evictContendLockDirect = iota
	evictContendEnqueueChild
	evictContendWaitAmbiguous
)

type pendingEvictCallback struct {
	fn              func()
	preserveOnPanic bool
}

type ambiguousHookWaiter struct {
	holder          uint64
	holdSeq         uint64
	waiterGID       uint64
	fromWaitGroupGo bool
	gateMu          sync.Mutex
	released        atomic.Bool
	enqueueAsChild  bool
	done            chan struct{}
}

func (w *ambiguousHookWaiter) release(asChild bool) {
	if w.released.CompareAndSwap(false, true) {
		w.enqueueAsChild = asChild
		close(w.done)
		w.gateMu.Unlock()
	}
}

func (p *pressureState) releaseAllAmbiguousWaitersLocked(asChild bool) {
	s := p.ambiguousWaiters
	for i, w := range s {
		s[i] = nil
		w.release(asChild)
	}
	p.ambiguousWaiters = s[:0]
}

func (p *pressureState) releaseAmbiguousHookWaiter(w *ambiguousHookWaiter, asChild bool) {
	p.evictCallbackResolveMu.Lock()
	s := p.ambiguousWaiters
	for i, cur := range s {
		if cur == w {
			last := len(s) - 1
			s[i] = s[last]
			s[last] = nil
			p.ambiguousWaiters = s[:last]
			break
		}
	}
	p.evictCallbackResolveMu.Unlock()
	w.release(asChild)
}

var (
	syncWaitStatusPrefixes = [...][]byte{
		[]byte("chan receive"),
		[]byte("chan send"),
		[]byte("select"),
		[]byte("semacquire"),
		[]byte("sync."),
		[]byte("synctest."),
	}
	systemIdleStatusPrefixes = [...][]byte{
		[]byte("force gc (idle)"),
		[]byte("GC sweep wait"),
		[]byte("GC scavenge wait"),
		[]byte("finalizer wait"),
		[]byte("cleanup wait"),
		[]byte("GOMAXPROCS updater (idle)"),
		[]byte("GC worker (idle)"),
		[]byte("trace reader (blocked)"),
		[]byte("wait for GC cycle"),
		[]byte("IO wait"),
		[]byte("coroutine"),
	}
)

func goroutineHeaderStatus(blk []byte) []byte {
	nl := bytes.IndexByte(blk, '\n')
	if nl <= 0 {
		return nil
	}
	header := blk[:nl]
	open := bytes.IndexByte(header, '[')
	closeIdx := bytes.LastIndexByte(header, ']')
	if open < 0 || closeIdx <= open {
		return nil
	}
	return header[open+1 : closeIdx]
}

func isGoroutineInSyncWait(status []byte) bool {
	for _, prefix := range syncWaitStatusPrefixes {
		if bytes.HasPrefix(status, prefix) {
			return true
		}
	}
	return false
}

func isSystemIdleGoroutine(status []byte) bool {
	for _, prefix := range systemIdleStatusPrefixes {
		if bytes.HasPrefix(status, prefix) {
			return true
		}
	}
	return false
}

type ambiguousQuiescenceScan struct {
	holderAlive           bool
	holderInNonWGSyncWait bool
	procQuiescent         bool
	subtreeQuiescent      bool
}

func inspectAmbiguousHolderBlock(blk, status []byte, st *ambiguousQuiescenceScan) {
	hookIdx := bytes.LastIndex(blk, []byte("deliverCallbacks"))
	st.holderAlive = hookIdx >= 0
	inSyncWait := isGoroutineInSyncWait(status)
	if hookIdx > 0 && inSyncWait {
		st.holderInNonWGSyncWait = !containsCreatorCallFrame(blk[:hookIdx], []byte("sync.(*WaitGroup).Wait"))
	}
	if !inSyncWait {
		st.procQuiescent = false
		st.subtreeQuiescent = false
	}
}

func (p *pressureState) scanAmbiguousGoroutineBlock(allStacks, blk []byte, w *ambiguousHookWaiter, startSnap uint64, st *ambiguousQuiescenceScan) {
	if bytes.Contains(blk, []byte("monitorAmbiguousHookWaiter")) {
		return
	}
	gID, pGID, creator := parseStackGoroutineParentAndCreator(blk)
	if gID == 0 || gID == w.waiterGID {
		return
	}
	status := goroutineHeaderStatus(blk)
	if gID == w.holder {
		inspectAmbiguousHolderBlock(blk, status, st)
		return
	}
	if isGoroutineInSyncWait(status) || isSystemIdleGoroutine(status) {
		return
	}
	st.procQuiescent = false
	if st.subtreeQuiescent && p.isDescendantOfEvictHolder(allStacks, blk, gID, pGID, creator, w.holder, 0, startSnap) {
		st.subtreeQuiescent = false
	}
}

func (p *pressureState) inspectAmbiguousWaiterQuiescence(w *ambiguousHookWaiter, startSnap uint64) ambiguousQuiescenceScan {
	allStacks, bufPtr := captureAllGoroutineStacks()
	defer releaseAllGoroutineStacks(bufPtr)
	st := ambiguousQuiescenceScan{procQuiescent: true, subtreeQuiescent: true}
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		p.scanAmbiguousGoroutineBlock(allStacks, blk, w, startSnap, &st)
	}
	return st
}

func advanceStreak(cond bool, streak *int) int {
	if cond {
		*streak++
		return *streak
	}
	*streak = 0
	return 0
}

type ambiguousMonitorStreaks struct {
	proc    int
	subtree int
	nonWG   int
	total   int
}

func (p *pressureState) stepAmbiguousHookWaiterTick(w *ambiguousHookWaiter, startSnap uint64, streaks *ambiguousMonitorStreaks) (done, asChild bool) {
	if !p.isEvictCallbackActive() || p.evictCallbackGID.Load() != w.holder || p.evictHoldSeq.Load() != w.holdSeq {
		return true, false
	}
	st := p.inspectAmbiguousWaiterQuiescence(w, startSnap)
	if !st.holderAlive {
		return true, false
	}
	if w.fromWaitGroupGo && advanceStreak(st.holderInNonWGSyncWait, &streaks.nonWG) >= 10 {
		return true, false
	}
	pStreak := advanceStreak(st.procQuiescent, &streaks.proc)
	sStreak := advanceStreak(st.subtreeQuiescent, &streaks.subtree)
	streaks.total++
	if pStreak >= 60 || sStreak >= 100 || streaks.total >= 160 {
		return true, true
	}
	return false, false
}

func (p *pressureState) monitorAmbiguousHookWaiter(w *ambiguousHookWaiter, startSnap uint64) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	var streaks ambiguousMonitorStreaks
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			if done, asChild := p.stepAmbiguousHookWaiterTick(w, startSnap, &streaks); done {
				p.releaseAmbiguousHookWaiter(w, asChild)
				return
			}
		}
	}
}

func (p *pressureState) registerAmbiguousEvictWaiter(holder, holdSeq, waiterGID, startSnap uint64, fromWGGo bool) *ambiguousHookWaiter {
	p.evictCallbackResolveMu.Lock()
	p.evictCallbackContended.Store(true)
	if !p.isEvictCallbackActive() || p.evictCallbackGID.Load() != holder || p.evictHoldSeq.Load() != holdSeq {
		p.evictCallbackResolveMu.Unlock()
		return nil
	}
	w := &ambiguousHookWaiter{
		holder:          holder,
		holdSeq:         holdSeq,
		waiterGID:       waiterGID,
		fromWaitGroupGo: fromWGGo,
		done:            make(chan struct{}),
	}
	w.gateMu.Lock()
	p.ambiguousWaiters = append(p.ambiguousWaiters, w)
	p.evictCallbackResolveMu.Unlock()
	go p.monitorAmbiguousHookWaiter(w, startSnap)
	return w
}

func (p *pressureState) classifyInexactContendedEvictCaller(stack []byte, gid, parentGID uint64, creatorFunc []byte, holder, holdSeq, startSnap uint64, exactSnap bool) (int, *ambiguousHookWaiter) {
	if p.preRejectEvictDescendant(gid, parentGID, holder, holdSeq, startSnap) {
		return evictContendLockDirect, nil
	}
	allStacks, bufPtr := captureAllGoroutineStacks()
	det, ok := p.findEvictHolderDetail(allStacks, stack, gid, parentGID, creatorFunc, holder, holdSeq, startSnap)
	if !ok {
		releaseAllGoroutineStacks(bufPtr)
		return evictContendLockDirect, nil
	}
	if exactSnap {
		releaseAllGoroutineStacks(bufPtr)
		return evictContendEnqueueChild, nil
	}
	mode, fromWGGo := classifyHolderChildHop(det)
	releaseAllGoroutineStacks(bufPtr)
	if mode != evictContendWaitAmbiguous {
		return mode, nil
	}
	w := p.registerAmbiguousEvictWaiter(holder, holdSeq, gid, startSnap, fromWGGo)
	if w == nil {
		return evictContendLockDirect, nil
	}
	return evictContendWaitAmbiguous, w
}

func (p *pressureState) classifyContendedEvictCaller(stack []byte, gid, parentGID uint64, creatorFunc []byte, holder uint64) (int, *ambiguousHookWaiter) {
	if holder == 0 {
		return evictContendLockDirect, nil
	}
	if holder == gid {
		return evictContendEnqueueChild, nil
	}
	holdSeq := p.evictHoldSeq.Load()
	exactSnap := p.evictExactSnap.Load()
	startSnap := p.evictStartSnapSeq.Load()
	if p.evictCallbackGID.Load() != holder || p.evictHoldSeq.Load() != holdSeq {
		return evictContendLockDirect, nil
	}
	return p.classifyInexactContendedEvictCaller(stack, gid, parentGID, creatorFunc, holder, holdSeq, startSnap, exactSnap)
}

//go:noinline
func (p *pressureState) lockEvictCallbackSlow() (bool, bool) {
	p.evictCallbackEverContended.Store(true)
	stack, bufPtr := captureCurrentGoroutineStack()
	gid, parentGID, creatorFunc := parseStackGoroutineParentAndCreator(stack)
	fastTag, hasFastTag := p.activeFastEvictTag()
	isMyTagFrame := hasFastTag && bytes.Contains(stack, fastEvictCallbackMarkers[fastTag])

	holder := p.resolveActiveEvictCallbackHolder(gid, parentGID, isMyTagFrame)
	mode, waiter := p.classifyContendedEvictCaller(stack, gid, parentGID, creatorFunc, holder)
	releaseCurrentGoroutineStack(bufPtr)

	switch mode {
	case evictContendEnqueueChild:
		return false, false
	case evictContendWaitAmbiguous:
		waiter.gateMu.Lock()
		asChild := waiter.enqueueAsChild
		waiter.gateMu.Unlock()
		if asChild {
			return false, true
		}
	}

	p.evictCallbackMu.Lock()
	p.beginEvictCallbackHold(true)
	p.registerSlowEvictHolder(gid, parentGID)
	return true, false
}

//go:noinline
func (p *pressureState) deliverCallbacksPendingBatch(queue []pendingEvictCallback) {
	defer clear(queue)
	for i, item := range queue {
		queue[i] = pendingEvictCallback{}
		item.fn()
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
			p.deliverCallbacksPendingBatch(queue)
		}
	}
}

func (p *pressureState) cacheFastEvictSlot(id uint64, base, tag uint32, idleState uint64) (uint32, bool) {
	if tag != base {
		p.setEvictTag(id, tag)
	}
	p.evictFastSlot = &fastEvictTagState[tag]
	p.evictFastIdle = idleState
	p.evictFastTag = tag
	return tag, true
}

func (p *pressureState) claimFastEvictTagSlow() (uint32, bool) {
	id := p.hookIDVal.Load()
	if id == 0 {
		id = p.hookID()
	}
	base := p.evictTag(id)
	idleState := id << 1
	activeState := idleState | 1
	for offset := range uint32(numPressureHookTags) {
		tag := (base + offset) & (numPressureHookTags - 1)
		cur := fastEvictTagState[tag].Load()
		if (cur == 0 || cur == idleState) && fastEvictTagState[tag].CompareAndSwap(cur, activeState) {
			return p.cacheFastEvictSlot(id, base, tag, idleState)
		}
	}
	for offset := range uint32(numPressureHookTags) {
		tag := (base + offset) & (numPressureHookTags - 1)
		cur := fastEvictTagState[tag].Load()
		if cur&1 == 0 && fastEvictTagState[tag].CompareAndSwap(cur, activeState) {
			return p.cacheFastEvictSlot(id, base, tag, idleState)
		}
	}
	return 0, false
}

func (p *pressureState) clearActiveFastEvictTag() {
	if slot := p.evictFastSlot; slot != nil {
		slot.CompareAndSwap(p.evictFastIdle|1, p.evictFastIdle)
		return
	}
	if id := p.hookIDVal.Load(); id != 0 {
		tag := p.evictTag(id)
		fastEvictTagState[tag].CompareAndSwap((id<<1)|1, id<<1)
	}
}

//go:noinline
func (p *pressureState) drainPendingEvictCallbacksSlow() {
	p.clearActiveFastEvictTag()
	p.unlockEvictCallbackSlow(true)
}

func (p *pressureState) tryDrainPostUnlock() {
	if p.evictPendingEnqueues.Load() != 0 && p.evictCallbackMu.TryLock() {
		p.beginEvictCallbackHold(true)
		p.drainPendingEvictCallbacksSlow()
	}
}

func (p *pressureState) prepareSlowEvictDrain() {
	if p.evictCallbackGID.Load() == 0 {
		p.registerSlowEvictHolder(0, 0)
		return
	}
	p.evictCallbackResolveMu.Lock()
	p.evictCallbackActive.Store(true)
	p.evictCallbackResolveMu.Unlock()
}

func (p *pressureState) detachPreservedOnPanicLocked() []pendingEvictCallback {
	if len(p.pendingEvictCallbacks) == 0 {
		p.evictPendingEnqueues.Store(0)
		return nil
	}
	var preserved []pendingEvictCallback
	for i, item := range p.pendingEvictCallbacks {
		p.pendingEvictCallbacks[i] = pendingEvictCallback{}
		if item.preserveOnPanic {
			item.preserveOnPanic = false
			preserved = append(preserved, item)
		}
	}
	p.pendingEvictCallbacks = nil
	p.evictPendingEnqueues.Store(0)
	return preserved
}

func invokePreservedCallbackSafely(fn func()) (panicked bool) {
	panicked = true
	defer func() {
		if panicked {
			_ = recover()
		}
	}()
	fn()
	panicked = false
	return false
}

//go:noinline
func (p *pressureState) deliverCallbacksPreservedBatch(preserved []pendingEvictCallback) {
	defer clear(preserved)
	for i, item := range preserved {
		preserved[i] = pendingEvictCallback{}
		if invokePreservedCallbackSafely(item.fn) {
			p.evictCallbackResolveMu.Lock()
			sub := p.detachPreservedOnPanicLocked()
			p.evictCallbackResolveMu.Unlock()
			if len(sub) > 0 {
				p.deliverCallbacksPreservedBatch(sub)
			}
		}
	}
}

func (p *pressureState) drainPreservedCallbacksOnPanic(preserved []pendingEvictCallback) {
	p.evictCallbackMu.Lock()
	p.beginEvictCallbackHold(true)
	p.registerSlowEvictHolder(0, 0)
	p.deliverCallbacksPreservedBatch(preserved)
	for p.evictPendingEnqueues.Load() != 0 {
		p.evictCallbackResolveMu.Lock()
		queue := p.pendingEvictCallbacks
		p.pendingEvictCallbacks = nil
		p.evictPendingEnqueues.Store(0)
		p.evictCallbackResolveMu.Unlock()
		if len(queue) > 0 {
			p.deliverCallbacksPreservedBatch(queue)
		}
	}
	p.unlockEvictCallbackSlow(true)
}

func (p *pressureState) finishEvictCallbackUnlock(panicked bool) {
	p.clearActiveFastEvictTag()
	p.evictCallbackResolveMu.Lock()
	var preserved []pendingEvictCallback
	if panicked {
		preserved = p.detachPreservedOnPanicLocked()
	}
	p.releaseAllAmbiguousWaitersLocked(false)
	p.evictCallbackActive.Store(false)
	p.evictCallbackGID.Store(0)
	p.evictCallbackParentGID.Store(0)
	p.evictNonDescendantHolder.Store(0)
	p.evictCallbackContended.Store(false)
	p.evictCallbackResolveMu.Unlock()
	p.evictCallbackMu.Unlock()
	if len(preserved) > 0 {
		p.drainPreservedCallbacksOnPanic(preserved)
		return
	}
	if !panicked {
		p.tryDrainPostUnlock()
	}
}

func (p *pressureState) unlockEvictCallbackSlow(completed bool) {
	panicked := true
	defer func() {
		p.finishEvictCallbackUnlock(panicked)
	}()
	if !completed {
		return
	}
	if p.evictPendingEnqueues.Load() != 0 {
		if p.evictCallbackGID.Load() == 0 {
			p.registerSlowEvictHolder(0, 0)
		} else if !p.evictCallbackActive.Load() {
			p.evictCallbackActive.Store(true)
		}
		p.drainPendingEvictCallbacks()
	}
	panicked = false
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

func (q *evictCallbackQueue[V]) enqueuePendingCallback(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.enqueuePendingCallbackOpts(p, onVal, onEntry, false)
}

//go:noinline
func (q *evictCallbackQueue[V]) enqueuePendingCallbackOpts(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason), preserveOnPanic bool) {
	events := make([]evictEvent[V], 0, q.n)
	inlineCount := min(q.n, len(q.buf))
	for i := range inlineCount {
		events = append(events, q.buf[i])
	}
	events = append(events, q.overflow...)
	q.reset()

	p.evictCallbackResolveMu.Lock()
	p.pendingEvictCallbacks = append(p.pendingEvictCallbacks, pendingEvictCallback{
		preserveOnPanic: preserveOnPanic,
		fn: func() {
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
		},
	})
	p.evictPendingEnqueues.Store(1)
	p.evictCallbackResolveMu.Unlock()

	if p.evictPendingEnqueues.Load() != 0 && p.evictCallbackMu.TryLock() {
		p.beginEvictCallbackHold(true)
		p.drainPendingEvictCallbacksSlow()
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

func (p *pressureState) finishFastEvictDelivery() bool {
	if p.evictCallbackContended.Load() || p.evictPendingEnqueues.Load() != 0 {
		p.prepareSlowEvictDrain()
		return true
	}
	p.evictFastSlot.Store(p.evictFastIdle)
	if p.evictCallbackContended.Load() || p.evictPendingEnqueues.Load() != 0 {
		p.prepareSlowEvictDrain()
		return true
	}
	p.evictCallbackMu.Unlock()
	if p.evictPendingEnqueues.Load() != 0 && p.evictCallbackMu.TryLock() {
		p.beginEvictCallbackHold(true)
		p.prepareSlowEvictDrain()
		return true
	}
	return false
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacks(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
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
	panicked := true
	defer func() {
		if panicked {
			p.unlockEvictCallbackSlow(false)
		}
	}()
	q.deliverMultiCallbacks(onVal, onEntry)
	panicked = false
}

//go:noinline
func (q *evictCallbackQueue[V]) invokeFastEvictCallbacks0(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) invokeFastEvictCallbacks1(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) invokeFastEvictCallbacks2(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) invokeFastEvictCallbacks3(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) invokeFastEvictCallbacks4(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) invokeFastEvictCallbacks5(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) invokeFastEvictCallbacks6(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(onVal, onEntry)
}

//go:noinline
func (q *evictCallbackQueue[V]) invokeFastEvictCallbacks7(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	q.deliverCallbacks(onVal, onEntry)
}

func (q *evictCallbackQueue[V]) dispatchFastCallbacks(p *pressureState, tag uint32, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	panicked := true
	defer func() {
		if panicked {
			p.unlockEvictCallbackSlow(false)
		}
	}()
	switch tag {
	case 0:
		q.invokeFastEvictCallbacks0(onVal, onEntry)
	case 1:
		q.invokeFastEvictCallbacks1(onVal, onEntry)
	case 2:
		q.invokeFastEvictCallbacks2(onVal, onEntry)
	case 3:
		q.invokeFastEvictCallbacks3(onVal, onEntry)
	case 4:
		q.invokeFastEvictCallbacks4(onVal, onEntry)
	case 5:
		q.invokeFastEvictCallbacks5(onVal, onEntry)
	case 6:
		q.invokeFastEvictCallbacks6(onVal, onEntry)
	default:
		q.invokeFastEvictCallbacks7(onVal, onEntry)
	}
	panicked = false
	if p.finishFastEvictDelivery() {
		p.drainPendingEvictCallbacksSlow()
	}
}

func (q *evictCallbackQueue[V]) invokeLockedSlow(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	if tag, ok := p.claimFastEvictTagSlow(); ok {
		q.dispatchFastCallbacks(p, tag, onVal, onEntry)
		return
	}
	p.registerSlowEvictHolder(0, 0)
	q.deliverCallbacksSlow(p, onVal, onEntry)
	p.unlockEvictCallbackSlow(true)
}

func (q *evictCallbackQueue[V]) invokeSlow(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	if onVal == nil && onEntry == nil {
		q.reset()
		return
	}
	locked, preserveOnPanic := p.lockEvictCallbackSlow()
	if !locked {
		q.enqueuePendingCallbackOpts(p, onVal, onEntry, preserveOnPanic)
		return
	}
	q.deliverCallbacksSlow(p, onVal, onEntry)
	p.unlockEvictCallbackSlow(true)
}

func (q *evictCallbackQueue[V]) invokeNonEmpty(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	if (onVal != nil || onEntry != nil) && p.evictCallbackMu.TryLock() {
		p.beginEvictCallbackHold(q.exactSnap)
		if slot := p.evictFastSlot; slot != nil && slot.CompareAndSwap(p.evictFastIdle, p.evictFastIdle|1) {
			q.dispatchFastCallbacks(p, p.evictFastTag, onVal, onEntry)
			return
		}
		q.invokeLockedSlow(p, onVal, onEntry)
		return
	}
	q.invokeSlow(p, onVal, onEntry)
}

func (q *evictCallbackQueue[V]) invoke(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	if q.n == 0 {
		return
	}
	q.invokeNonEmpty(p.coord(), onVal, onEntry)
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

func (p *pressureState) resetZeroWatermarkBelowTier2Slow(pressure float64) {
	if p.options.PressureFunc == nil ||
		(p.hasValidSample &&
			pressure < p.options.EvictionThreshold &&
			p.sampledPressureSeq >= p.zeroWatermarkSeq &&
			!p.isSamplingGoroutine()) {
		p.lastReclaimedZeroCount = 0
		p.lastReclaimedLen = 0
	}
}

func (p *pressureState) resetZeroWatermarkBelowTier2(pressure float64) {
	if p.lastReclaimedZeroCount != 0 || p.lastReclaimedLen != 0 {
		p.resetZeroWatermarkBelowTier2Slow(pressure)
	}
}

func (p *pressureState) syncZeroWatermarkLocked(belowTier1, belowTier2, sampleSeq uint64, pressure float64) {
	if !p.hasValidSample || p.isSamplingGoroutine() {
		return
	}
	if sampleSeq != 0 && sampleSeq < p.sampledPressureSeq {
		p.hasValidSample = false
		return
	}
	p.sampledBelowTier1Epoch = max(p.sampledBelowTier1Epoch, belowTier1)
	p.sampledBelowTier2Epoch = max(p.sampledBelowTier2Epoch, belowTier2)
	p.sampledPressureSeq = sampleSeq
	if (p.lastReclaimedZeroCount != 0 || p.lastReclaimedLen != 0) && belowTier2 > p.zeroWatermarkTier2Epoch {
		p.zeroWatermarkTier2Epoch = belowTier2
		p.lastReclaimedZeroCount = 0
		p.lastReclaimedLen = 0
	}
	p.resetZeroWatermarkBelowTier2(pressure)
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
	coord := p.coord()
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
		MemoryPressure:           math.Float64frombits(coord.lastSampledPressureBits.Load()),
		CompactionsExplicit:      p.compactionsExplicit,
		CompactionsPressureTier1: p.compactionsPressureTier1,
		CompactionsPressureTier2: p.compactionsPressureTier2,
		CompactionsAutoSlack:     p.compactionsAutoSlack,
		PressureShedsInline:      p.pressureShedsInline,
		PressureShedsExplicit:    p.pressureShedsExplicit,
		ReclaimEpoch:             coord.reclaimEpoch.Load(),
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
	lastPressure := math.Float64frombits(p.coord().lastSampledPressureBits.Load())
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
