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

var goroutineIDBufPool = sync.Pool{
	New: func() any {
		return new([32]byte)
	},
}

func currentGoroutineID() uint64 {
	bufPtr := goroutineIDBufPool.Get().(*[32]byte)
	n := runtime.Stack(bufPtr[:32], false)
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
}

const (
	samplerSlotPrimary = iota
	samplerSlotFallback
	samplerSlotOverflow
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

func (p *pressureState) checkSamplingGoroutine() (bool, uint64) {
	if !p.options.hasCustomPressureFunc {
		return false, 0
	}
	sGID := p.samplingGID.Load()
	fGID := p.fallbackGID.Load()
	oCount := p.overflowSamplingCount.Load()
	if sGID == 0 && fGID == 0 && oCount <= 0 {
		return false, 0
	}
	gid := currentGoroutineID()
	return p.isCurrentGoroutineSampling(gid), gid
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

func (p *pressureState) storeSampledPressureWithSeq(epoch, extEpoch, invokeSeq uint64, val float64, gid uint64, slot int) (uint64, float64, bool) {
	p.pressureWriteMu.Lock()
	defer p.pressureWriteMu.Unlock()

	if invokeSeq == 0 || invokeSeq >= p.lastSampledSeq.Load() {
		if invokeSeq > p.lastSampledSeq.Load() {
			p.lastSampledSeq.Store(invokeSeq)
		}
		p.lastSampledPressureBits.Store(math.Float64bits(val))
	}

	var stored bool
	if p.reclaimEpoch.Load() == epoch {
		if invokeSeq != 0 && invokeSeq < p.pressureMaxStoredSeq.Load() {
			if p.pressureInitialized.Load() && !p.pressureNeedsRefresh.Load() && p.cachedPressureEpoch.Load() == epoch {
				val = math.Float64frombits(p.cachedPressureBits.Load())
			}
		} else {
			if invokeSeq > p.pressureMaxStoredSeq.Load() {
				p.pressureMaxStoredSeq.Store(invokeSeq)
			}
			bits := math.Float64bits(val)
			p.pressureInitialized.Store(false)
			p.cachedPressureEpoch.Store(epoch)
			p.cachedPressureBits.Store(bits)
			p.pressureInitialized.Store(true)
			p.pressureNeedsRefresh.Store(false)
			stored = true
		}
	} else if !p.pressureInitialized.Load() || p.cachedPressureEpoch.Load() != p.reclaimEpoch.Load() {
		p.pressureNeedsRefresh.Store(true)
	}

	if gid != 0 {
		var reentrant bool
		switch slot {
		case samplerSlotPrimary:
			reentrant = p.samplingReentrantReclaim.Swap(false)
		case samplerSlotFallback:
			reentrant = p.fallbackReentrantReclaim.Swap(false)
		default:
			_, reentrant = p.reentrantReclaimGIDs.LoadAndDelete(gid)
		}
		if reentrant && !stored && p.externalReclaimEpoch.Load() == extEpoch {
			epoch = p.reclaimEpoch.Load()
		}
	}

	return epoch, val, stored
}

func (p *pressureState) invokeAndStorePressure(gid uint64, slot int) (uint64, float64, bool) {
	if p.options.hasCustomPressureFunc && gid != 0 {
		switch slot {
		case samplerSlotPrimary:
			p.samplingReentrantReclaim.Store(false)
		case samplerSlotFallback:
			p.fallbackReentrantReclaim.Store(false)
		default:
			p.reentrantReclaimGIDs.Delete(gid)
		}
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

// samplePressureFreshWithEpoch evaluates p.options.PressureFunc lock-free outside the cache lock
// with a goroutine-aware re-entrancy guard and cold-start fallback.
func (p *pressureState) samplePressureFreshWithEpoch() (uint64, float64, bool) {
	if p.options.PressureFunc == nil {
		return p.reclaimEpoch.Load(), 0.0, true
	}
	sampling, gid := p.checkSamplingGoroutine()
	if sampling {
		return p.reclaimEpoch.Load(), 0.0, false
	}
	if p.samplingPressure.CompareAndSwap(false, true) {
		if p.options.hasCustomPressureFunc {
			if gid == 0 {
				gid = currentGoroutineID()
			}
			p.samplingGID.Store(gid)
			defer func() {
				p.samplingGID.Store(0)
				p.samplingPressure.Store(false)
			}()
		} else {
			defer p.samplingPressure.Store(false)
		}
		return p.invokeAndStorePressure(gid, samplerSlotPrimary)
	}
	if p.options.hasCustomPressureFunc {
		if gid == 0 {
			gid = currentGoroutineID()
		}
		if p.isCurrentGoroutineSampling(gid) {
			return p.reclaimEpoch.Load(), 0.0, false
		}
	}
	if p.fallbackSampling.CompareAndSwap(false, true) {
		if p.options.hasCustomPressureFunc {
			p.fallbackGID.Store(gid)
			defer func() {
				p.fallbackGID.Store(0)
				p.fallbackSampling.Store(false)
			}()
		} else {
			defer p.fallbackSampling.Store(false)
		}
		return p.invokeAndStorePressure(gid, samplerSlotFallback)
	}
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

// samplePressureWithEpoch returns the current memory pressure and epoch for foreground cache operations.
// Custom PressureFunc callbacks are invoked on every call; the default runtime/metrics probe is amortized
// across a 256-operation window (and immediately refreshed after any reclamation/compaction cycle).
func (p *pressureState) samplePressureWithEpoch() (uint64, float64, bool) {
	if p.options.hasCustomPressureFunc {
		return p.samplePressureFreshWithEpoch()
	}
	epoch := p.reclaimEpoch.Load()
	seq := p.pressureSampleSeq.Add(1)
	if (seq&255) == 1 || !p.pressureInitialized.Load() || p.pressureNeedsRefresh.Load() || p.cachedPressureEpoch.Load() != epoch {
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
	if p.peakEntryLen > 8 && p.peakEntryLen > currentLen && p.peakEntryLen-currentLen >= 2 && uint64(p.peakEntryLen-currentLen)*4 >= uint64(p.peakEntryLen) {
		return true
	}
	return p.deletedSinceCompact >= 64 && p.deletedSinceCompact >= currentLen
}

func (p *pressureState) hasEmptyDeleteSlack(extraSlack bool) bool {
	return extraSlack || p.peakEntryLen > 8 || p.deletedSinceCompact >= 64
}

func (p *pressureState) shouldReclaimSingleSurvivorOnDelete(currentLen int, isDirty, extraChurn bool) bool {
	return currentLen == 1 && isDirty && (extraChurn || p.peakEntryLen >= 64 || p.deletedSinceCompact >= 64)
}

func (p *pressureState) shouldReclaimEmptyPrePut(currentLen int, extraChurn, evictedPre bool, newSize, sizeBefore uint64, pressure float64) bool {
	return currentLen == 0 && (extraChurn || p.peakEntryLen >= 64 || p.deletedSinceCompact >= 64 || (evictedPre && newSize < sizeBefore && p.hasElevatedPressureToInvalidate(pressure)))
}

func (p *pressureState) shouldReclaimSingleSurvivorOnMutation(currentLen int, isDirty, extraChurn, evictedPre bool, postSize, sizeBefore uint64, pressure float64) bool {
	return (currentLen == 1 || (evictedPre && currentLen-p.zeroSizeCount <= 1)) && isDirty &&
		(extraChurn || p.peakEntryLen >= 64 || p.deletedSinceCompact >= 64 || (currentLen == 1 && postSize < sizeBefore && p.hasElevatedPressureToInvalidate(pressure)))
}

func (p *pressureState) shouldCompactAfterMutation(reclaimedPre, netByteReduced, isDirty bool, pressure float64) bool {
	return isDirty && (reclaimedPre || (netByteReduced && pressure >= p.options.CompactionThreshold && uint64(p.deletedSinceCompact)*4 >= uint64(p.peakEntryLen)))
}

func (p *pressureState) shouldMarkReclaimedAfterMutation(reclaimedPre, netByteReduced bool, sampledEpoch uint64, pressure float64) bool {
	return (reclaimedPre || netByteReduced) && p.reclaimEpoch.Load() == sampledEpoch && p.hasElevatedPressureToInvalidate(pressure)
}

func (p *pressureState) computeShedTargets(targetSize uint64, retention float64, currentLen int, hasProtected bool, protectedSize uint64) (effectiveTarget uint64, targetLen, targetZeroCount, unprotectedZeroTarget int) {
	effectiveTarget = targetSize
	if hasProtected && retention > 0.0 && protectedSize > effectiveTarget {
		effectiveTarget = protectedSize
	}
	if retention > 0.0 {
		if currentLen > 0 {
			targetLen = int(float64(currentLen) * retention)
			if (!hasProtected || protectedSize <= targetSize) && p.lastReclaimedLen > targetLen {
				targetLen = p.lastReclaimedLen
			}
			if hasProtected && targetLen < 1 {
				targetLen = 1
			}
		}
		if p.zeroSizeCount > 0 {
			unprotectedZeroTarget = max(int(float64(p.zeroSizeCount)*retention), p.lastReclaimedZeroCount)
			targetZeroCount = unprotectedZeroTarget
			if hasProtected && protectedSize == 0 && targetZeroCount < 1 {
				targetZeroCount = 1
			}
		}
	}
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

func (p *pressureState) checkTelemetryInvariants(currentLen int) {
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
