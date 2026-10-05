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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

var goroutineIDBufPool = sync.Pool{
	New: func() any {
		return new([32]byte)
	},
}

var goroutineStackBufPool = sync.Pool{
	New: func() any {
		return new([4096]byte)
	},
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

func currentGoroutineID() uint64 {
	bufPtr := goroutineIDBufPool.Get().(*[32]byte)
	n := runtime.Stack(bufPtr[:32], false)
	const prefix = "goroutine "
	var id uint64
	if n > len(prefix) {
		id, _ = parseDecimalUint64(bufPtr[len(prefix):n])
	}
	goroutineIDBufPool.Put(bufPtr)
	return id
}

func parseStackGoroutineAndParentID(stack []byte) (gid, parentGID uint64) {
	const prefix = "goroutine "
	if len(stack) > len(prefix) {
		gid, _ = parseDecimalUint64(stack[len(prefix):])
	}
	const createdIn = " in goroutine "
	if idx := bytes.LastIndex(stack, []byte(createdIn)); idx >= 0 {
		parentGID, _ = parseDecimalUint64(stack[idx+len(createdIn):])
	}
	return gid, parentGID
}

var (
	globalActiveEvictCallbacks atomic.Int32
	fastEvictCallbackOwner     atomic.Pointer[pressureState]
	multiEvictCallbackSentinel = new(pressureState)
	multiCacheEvictMu          sync.Mutex
	multiCacheEvictOwners      = make(map[uint64][]*pressureState)

	globalActivePrimarySamplers atomic.Int32
	fastPrimarySamplerOwner     atomic.Pointer[pressureState]
	multiPrimarySamplerSentinel = new(pressureState)
	multiCacheSamplerMu         sync.Mutex
	multiCacheSamplerOwners     = make(map[uint64][]*pressureState)
)

func addMultiCacheOwner(owners map[uint64][]*pressureState, gid uint64, p *pressureState) {
	if gid == 0 || p == nil {
		return
	}
	list := owners[gid]
	if !slices.Contains(list, p) {
		owners[gid] = append(list, p)
	}
}

func removeMultiCacheOwner(owners map[uint64][]*pressureState, gid uint64, p *pressureState) {
	if gid == 0 || p == nil {
		return
	}
	list := owners[gid]
	for i, item := range list {
		if item == p {
			list = slices.Delete(list, i, i+1)
			if len(list) == 0 {
				delete(owners, gid)
			} else {
				owners[gid] = list
			}
			return
		}
	}
}

var byteStrings [256]string

func init() {
	var raw [256]byte
	for i := range 256 {
		raw[i] = byte(i)
	}
	all := string(raw[:])
	for i := range 256 {
		byteStrings[i] = all[i : i+1]
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
	samplingPressure         atomic.Bool
	fallbackSampling         atomic.Bool
	pressureInitialized      atomic.Bool
	pressureNeedsRefresh     atomic.Bool
	overflowSamplingCount    atomic.Int32
	samplingGID              atomic.Uint64
	fallbackGID              atomic.Uint64
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
	evictCallbackActive      atomic.Bool
	evictCallbackResolveMu   sync.Mutex
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

func isCandidatePrimarySamplerStack(blk []byte) bool {
	if bytes.Contains(blk, []byte("invokeAndStorePressure")) {
		return true
	}
	return bytes.Contains(blk, []byte("sampleWithSlot")) &&
		!bytes.Contains(blk, []byte("registerMultiCachePrimarySampler")) &&
		!bytes.Contains(blk, []byte("releasePrimarySamplerSlot"))
}

func (p *pressureState) resolvePrimaryFromAllStacksLocked() uint64 {
	p.samplingResolveMu.Lock()
	defer p.samplingResolveMu.Unlock()
	if !p.samplingPressure.Load() {
		return 0
	}
	if sGID := p.samplingGID.Load(); sGID != 0 {
		return sGID
	}
	for size := 16384; size <= 1<<20; size *= 2 {
		buf := make([]byte, size)
		n := runtime.Stack(buf, true)
		if n >= len(buf) {
			continue
		}
		for _, blk := range bytes.Split(buf[:n], []byte("\n\n")) {
			if !isCandidatePrimarySamplerStack(blk) {
				continue
			}
			gid, _ := parseStackGoroutineAndParentID(blk)
			if gid == 0 || gid == p.fallbackGID.Load() || p.hasOverflowSamplingGID(gid) {
				continue
			}
			if owners := multiCacheSamplerOwners[gid]; len(owners) > 0 && !slices.Contains(owners, p) {
				continue
			}
			if p.samplingPressure.Load() {
				p.samplingGID.Store(gid)
				return gid
			}
		}
		break
	}
	return p.samplingGID.Load()
}

func promoteFastPrimarySamplerLocked(firstP *pressureState, callerGID uint64, isNestedSampler bool) {
	if firstP == nil || firstP == multiPrimarySamplerSentinel {
		return
	}
	if isNestedSampler && len(multiCacheSamplerOwners) == 0 && firstP.samplingGID.Load() == 0 {
		firstP.samplingResolveMu.Lock()
		if firstP.samplingPressure.Load() && firstP.samplingGID.Load() == 0 {
			firstP.samplingGID.Store(callerGID)
		}
		firstP.samplingResolveMu.Unlock()
		addMultiCacheOwner(multiCacheSamplerOwners, callerGID, firstP)
		return
	}
	if firstGID := firstP.resolvePrimaryFromAllStacksLocked(); firstGID != 0 {
		addMultiCacheOwner(multiCacheSamplerOwners, firstGID, firstP)
	}
}

func (p *pressureState) resolvePrimaryFromAllStacks(callerGID uint64, isNestedSampler bool) {
	multiCacheSamplerMu.Lock()
	defer multiCacheSamplerMu.Unlock()
	firstP := fastPrimarySamplerOwner.Swap(multiPrimarySamplerSentinel)
	promoteFastPrimarySamplerLocked(firstP, callerGID, isNestedSampler)
	if p.samplingPressure.Load() && p.samplingGID.Load() == 0 {
		if candGID := p.resolvePrimaryFromAllStacksLocked(); candGID != 0 {
			addMultiCacheOwner(multiCacheSamplerOwners, candGID, p)
		}
	}
}

func (p *pressureState) inspectCurrentAndResolvePrimary() (gid, parentGID uint64) {
	bufPtr := goroutineStackBufPool.Get().(*[4096]byte)
	n := runtime.Stack(bufPtr[:], false)
	gid, parentGID = parseStackGoroutineAndParentID(bufPtr[:n])
	isNestedSampler := bytes.Contains(bufPtr[:n], []byte("invokeAndStorePressure")) &&
		gid != p.fallbackGID.Load() && !p.hasOverflowSamplingGID(gid)
	goroutineStackBufPool.Put(bufPtr)

	if p.samplingPressure.Load() && p.samplingGID.Load() == 0 {
		p.resolvePrimaryFromAllStacks(gid, isNestedSampler)
	}
	return gid, parentGID
}

func (p *pressureState) hasActiveSampler() bool {
	return p.samplingPressure.Load() ||
		p.fallbackSampling.Load() ||
		p.overflowSamplingCount.Load() > 0
}

func (p *pressureState) checkSamplingGoroutine() (bool, uint64) {
	if !p.options.hasCustomPressureFunc || !p.hasActiveSampler() {
		return false, 0
	}
	gid, _ := p.inspectCurrentAndResolvePrimary()
	return p.isCurrentGoroutineSampling(gid), gid
}

func (p *pressureState) checkSamplingGoroutineOrChild() (bool, uint64) {
	if !p.options.hasCustomPressureFunc || !p.hasActiveSampler() {
		return false, 0
	}
	gid, parentGID := p.inspectCurrentAndResolvePrimary()
	if p.isCurrentGoroutineSampling(gid) || p.isCurrentGoroutineSampling(parentGID) {
		return true, gid
	}
	return false, gid
}

func (p *pressureState) isSamplingGoroutine() bool {
	sampling, _ := p.checkSamplingGoroutine()
	return sampling
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
	}
}

func (p *pressureState) clearReentrantSlot(gid uint64, slot int) bool {
	switch slot {
	case samplerSlotPrimary:
		return p.samplingReentrantReclaim.Load() && p.samplingReentrantReclaim.Swap(false)
	case samplerSlotFallback:
		return p.fallbackReentrantReclaim.Load() && p.fallbackReentrantReclaim.Swap(false)
	default:
		if gid == 0 {
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
		p.samplingReentrantReclaim.Store(false)
		p.fallbackReentrantReclaim.Store(false)
		p.reentrantReclaimGIDs.Clear()
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

func (p *pressureState) ensureSamplingGID(gid uint64) uint64 {
	if p.options.hasCustomPressureFunc && gid == 0 {
		return currentGoroutineID()
	}
	return gid
}

//go:noinline
func (p *pressureState) registerMultiCachePrimarySampler(gid uint64) uint64 {
	bufPtr := goroutineStackBufPool.Get().(*[4096]byte)
	n := runtime.Stack(bufPtr[:], false)
	if gid == 0 {
		gid, _ = parseStackGoroutineAndParentID(bufPtr[:n])
	}
	isNestedSampler := bytes.Contains(bufPtr[:n], []byte("invokeAndStorePressure"))
	goroutineStackBufPool.Put(bufPtr)

	multiCacheSamplerMu.Lock()
	firstP := fastPrimarySamplerOwner.Swap(multiPrimarySamplerSentinel)
	promoteFastPrimarySamplerLocked(firstP, gid, isNestedSampler)
	p.samplingResolveMu.Lock()
	if p.samplingPressure.Load() {
		p.samplingGID.Store(gid)
	}
	p.samplingResolveMu.Unlock()
	addMultiCacheOwner(multiCacheSamplerOwners, gid, p)
	multiCacheSamplerMu.Unlock()
	return gid
}

//go:noinline
func (p *pressureState) releasePrimarySamplerSlot(gid uint64, registeredMulti bool) {
	p.samplingPressure.Store(false)
	if registeredMulti || p.samplingGID.Load() != 0 || !fastPrimarySamplerOwner.CompareAndSwap(p, nil) {
		multiCacheSamplerMu.Lock()
		if g := p.samplingGID.Load(); g != 0 {
			removeMultiCacheOwner(multiCacheSamplerOwners, g, p)
		} else if gid != 0 {
			removeMultiCacheOwner(multiCacheSamplerOwners, gid, p)
		}
		p.samplingResolveMu.Lock()
		p.samplingGID.Store(0)
		p.samplingResolveMu.Unlock()
		multiCacheSamplerMu.Unlock()
	}
	if globalActivePrimarySamplers.Add(-1) == 0 {
		multiCacheSamplerMu.Lock()
		if globalActivePrimarySamplers.Load() == 0 {
			fastPrimarySamplerOwner.CompareAndSwap(multiPrimarySamplerSentinel, nil)
		}
		multiCacheSamplerMu.Unlock()
	}
}

//go:noinline
func (p *pressureState) sampleWithSlot(gid uint64, slot int, slotGID *atomic.Uint64, slotFlag *atomic.Bool) (uint64, float64, bool) {
	if p.options.hasCustomPressureFunc {
		var registeredMulti bool
		if slot == samplerSlotPrimary {
			if globalActivePrimarySamplers.Add(1) != 1 || gid != 0 || !fastPrimarySamplerOwner.CompareAndSwap(nil, p) {
				gid = p.registerMultiCachePrimarySampler(gid)
				registeredMulti = true
			}
		} else {
			gid = p.ensureSamplingGID(gid)
			slotGID.Store(gid)
		}
		defer func() {
			if slot == samplerSlotPrimary {
				p.releasePrimarySamplerSlot(gid, registeredMulti)
			} else {
				slotGID.Store(0)
				slotFlag.Store(false)
			}
		}()
	} else {
		defer slotFlag.Store(false)
	}
	return p.invokeAndStorePressure(gid, slot)
}

func (p *pressureState) sampleWithOverflowSlot(gid uint64) (uint64, float64, bool) {
	if p.options.hasCustomPressureFunc {
		p.overflowSamplingGIDs.Store(gid, struct{}{})
	}
	p.overflowSamplingCount.Add(1)
	defer func() {
		if p.options.hasCustomPressureFunc {
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
	sampling, gid := p.checkSamplingGoroutineOrChild()
	if sampling {
		return p.reclaimEpoch.Load(), 0.0, false
	}
	if p.samplingPressure.CompareAndSwap(false, true) {
		return p.sampleWithSlot(gid, samplerSlotPrimary, &p.samplingGID, &p.samplingPressure)
	}
	if p.options.hasCustomPressureFunc {
		sampling, gid = p.checkSamplingGoroutineOrChild()
		if sampling {
			return p.reclaimEpoch.Load(), 0.0, false
		}
		gid = p.ensureSamplingGID(gid)
	}
	if p.fallbackSampling.CompareAndSwap(false, true) {
		return p.sampleWithSlot(gid, samplerSlotFallback, &p.fallbackGID, &p.fallbackSampling)
	}
	return p.sampleWithOverflowSlot(gid)
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
		if hasProtected && targetLen < 1 {
			targetLen = 1
		}
	}
	targetZeroCount, unprotectedZeroTarget = p.computeZeroShedTargets(retention, hasProtected, protectedSize)
	return effectiveTarget, targetLen, targetZeroCount, unprotectedZeroTarget
}

func (p *pressureState) updateZeroWatermarkAfterShed(retention float64, currentLen int, unprotectedLenAllowed bool, unprotectedZeroTarget, targetLen int) {
	if retention > 0.0 && p.zeroSizeCount > 0 {
		p.lastReclaimedZeroCount = min(p.zeroSizeCount, unprotectedZeroTarget)
		if unprotectedLenAllowed {
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
	return valueSize == 0 && currentLen > 0 &&
		((currentSize >= maxSize && uint64(currentLen) >= maxSize) || uint64(p.zeroSizeCount) >= maxSize)
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

func isCandidateEvictCallbackStack(blk []byte) bool {
	if bytes.Contains(blk, []byte("deliverCallbacks")) {
		return true
	}
	return bytes.Contains(blk, []byte("evictCallbackQueue")) &&
		!bytes.Contains(blk, []byte("lockEvictCallbackSlow")) &&
		!bytes.Contains(blk, []byte("registerMultiCacheEvictHolder")) &&
		!bytes.Contains(blk, []byte("unlockEvictCallback"))
}

func (p *pressureState) resolveEvictCallbackHolderFromAllStacksLocked() uint64 {
	p.evictCallbackResolveMu.Lock()
	defer p.evictCallbackResolveMu.Unlock()
	if !p.evictCallbackActive.Load() {
		return 0
	}
	if gid := p.evictCallbackGID.Load(); gid != 0 {
		return gid
	}
	for size := 16384; size <= 1<<20; size *= 2 {
		buf := make([]byte, size)
		n := runtime.Stack(buf, true)
		if n >= len(buf) {
			continue
		}
		for _, blk := range bytes.Split(buf[:n], []byte("\n\n")) {
			if !isCandidateEvictCallbackStack(blk) {
				continue
			}
			gid, _ := parseStackGoroutineAndParentID(blk)
			if gid == 0 {
				continue
			}
			if owners := multiCacheEvictOwners[gid]; len(owners) > 0 && !slices.Contains(owners, p) {
				continue
			}
			if p.evictCallbackActive.Load() {
				p.evictCallbackGID.Store(gid)
				return gid
			}
		}
		break
	}
	return p.evictCallbackGID.Load()
}

func promoteFastEvictCallbackLocked(firstP *pressureState, callerGID uint64, isNestedCallback bool) {
	if firstP == nil || firstP == multiEvictCallbackSentinel {
		return
	}
	if isNestedCallback && len(multiCacheEvictOwners) == 0 && firstP.evictCallbackGID.Load() == 0 {
		firstP.evictCallbackResolveMu.Lock()
		if firstP.evictCallbackActive.Load() && firstP.evictCallbackGID.Load() == 0 {
			firstP.evictCallbackGID.Store(callerGID)
		}
		firstP.evictCallbackResolveMu.Unlock()
		addMultiCacheOwner(multiCacheEvictOwners, callerGID, firstP)
		return
	}
	if firstGID := firstP.resolveEvictCallbackHolderFromAllStacksLocked(); firstGID != 0 {
		addMultiCacheOwner(multiCacheEvictOwners, firstGID, firstP)
	}
}

//go:noinline
func (p *pressureState) registerMultiCacheEvictHolder(gid uint64) {
	bufPtr := goroutineStackBufPool.Get().(*[4096]byte)
	n := runtime.Stack(bufPtr[:], false)
	if gid == 0 {
		gid, _ = parseStackGoroutineAndParentID(bufPtr[:n])
	}
	isNestedCallback := bytes.Contains(bufPtr[:n], []byte("deliverCallbacks"))
	goroutineStackBufPool.Put(bufPtr)

	multiCacheEvictMu.Lock()
	firstP := fastEvictCallbackOwner.Swap(multiEvictCallbackSentinel)
	promoteFastEvictCallbackLocked(firstP, gid, isNestedCallback)
	p.evictCallbackResolveMu.Lock()
	if p.evictCallbackActive.Load() {
		p.evictCallbackGID.Store(gid)
	}
	p.evictCallbackResolveMu.Unlock()
	addMultiCacheOwner(multiCacheEvictOwners, gid, p)
	multiCacheEvictMu.Unlock()
}

func (p *pressureState) lockEvictCallback() bool {
	if p.evictCallbackMu.TryLock() {
		p.evictCallbackActive.Store(true)
		if globalActiveEvictCallbacks.Add(1) == 1 && fastEvictCallbackOwner.CompareAndSwap(nil, p) {
			return true
		}
		p.registerMultiCacheEvictHolder(0)
		return true
	}
	return p.lockEvictCallbackSlow()
}

//go:noinline
func (p *pressureState) lockEvictCallbackSlow() bool {
	bufPtr := goroutineStackBufPool.Get().(*[4096]byte)
	n := runtime.Stack(bufPtr[:], false)
	gid, parentGID := parseStackGoroutineAndParentID(bufPtr[:n])
	isNestedCallback := bytes.Contains(bufPtr[:n], []byte("deliverCallbacks"))
	goroutineStackBufPool.Put(bufPtr)

	if holder := p.evictCallbackGID.Load(); holder != 0 {
		if holder == gid || holder == parentGID {
			return false
		}
	} else {
		multiCacheEvictMu.Lock()
		firstP := fastEvictCallbackOwner.Swap(multiEvictCallbackSentinel)
		promoteFastEvictCallbackLocked(firstP, gid, isNestedCallback)
		if p.evictCallbackGID.Load() == 0 {
			if candGID := p.resolveEvictCallbackHolderFromAllStacksLocked(); candGID != 0 {
				addMultiCacheOwner(multiCacheEvictOwners, candGID, p)
			}
		}
		multiCacheEvictMu.Unlock()
		if holder := p.evictCallbackGID.Load(); holder != 0 && (holder == gid || holder == parentGID) {
			return false
		}
	}

	p.evictCallbackMu.Lock()
	p.evictCallbackActive.Store(true)
	globalActiveEvictCallbacks.Add(1)
	p.registerMultiCacheEvictHolder(gid)
	return true
}

func (p *pressureState) unlockEvictCallback() {
	p.evictCallbackActive.Store(false)
	if p.evictCallbackGID.Load() != 0 || !fastEvictCallbackOwner.CompareAndSwap(p, nil) {
		multiCacheEvictMu.Lock()
		if g := p.evictCallbackGID.Load(); g != 0 {
			removeMultiCacheOwner(multiCacheEvictOwners, g, p)
		}
		p.evictCallbackResolveMu.Lock()
		p.evictCallbackGID.Store(0)
		p.evictCallbackResolveMu.Unlock()
		multiCacheEvictMu.Unlock()
	}
	if globalActiveEvictCallbacks.Add(-1) == 0 {
		multiCacheEvictMu.Lock()
		if globalActiveEvictCallbacks.Load() == 0 {
			fastEvictCallbackOwner.CompareAndSwap(multiEvictCallbackSentinel, nil)
		}
		multiCacheEvictMu.Unlock()
	}
	p.evictCallbackMu.Unlock()
}

//go:noinline
func (q *evictCallbackQueue[V]) deliverCallbacks(onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	inlineCount := min(q.n, len(q.buf))
	for i := range inlineCount {
		ev := q.buf[i]
		if onVal != nil {
			onVal(ev.value, ev.reason)
		}
		if onEntry != nil {
			onEntry(ev.key, ev.value, ev.reason)
		}
	}
	for _, ev := range q.overflow {
		if onVal != nil {
			onVal(ev.value, ev.reason)
		}
		if onEntry != nil {
			onEntry(ev.key, ev.value, ev.reason)
		}
	}
}

//go:noinline
func (q *evictCallbackQueue[V]) invoke(p *pressureState, onVal func(V, EvictionReason), onEntry func(string, V, EvictionReason)) {
	if q.n == 0 || (onVal == nil && onEntry == nil) {
		return
	}
	if p.lockEvictCallback() {
		defer p.unlockEvictCallback()
	}
	q.deliverCallbacks(onVal, onEntry)
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
