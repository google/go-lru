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

package lru_test

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-lru"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type diffValue struct {
	id   string
	size uint64
}

func (v *diffValue) weight() uint64 {
	if v == nil {
		return 0
	}
	return v.size
}

func (v *diffValue) String() string {
	if v == nil {
		return "<nil>"
	}
	return fmt.Sprintf("val{id:%s, sz:%d}", v.id, v.size)
}

var diffWeigher = lru.WithWeigher(func(_ string, v *diffValue) uint64 {
	if v == nil {
		return 0
	}
	return v.size
})

type diffInstance struct {
	name       string
	invariants bool
	cache      lru.Cache[*diffValue]
}

type differentialHarness struct {
	t          *testing.T
	maxSize    uint64
	unitWeight bool
	instances  []diffInstance
}

func newDifferentialHarness(t *testing.T, maxSize uint64, opts ...lru.Option) *differentialHarness {
	t.Helper()
	return newDifferentialHarnessWithOpts(t, maxSize, false, slices.Concat([]lru.Option{diffWeigher}, opts)...)
}

func newDifferentialHarnessDefaultWeigher(t *testing.T, maxSize uint64, opts ...lru.Option) *differentialHarness {
	t.Helper()
	return newDifferentialHarnessWithOpts(t, maxSize, true, opts...)
}

func newDifferentialHarnessWithOpts(t *testing.T, maxSize uint64, unitWeight bool, opts ...lru.Option) *differentialHarness {
	t.Helper()
	h := &differentialHarness{
		t:          t,
		maxSize:    maxSize,
		unitWeight: unitWeight,
	}

	configs := []struct {
		name        string
		constructor func(uint64, ...lru.Option) lru.Cache[*diffValue]
	}{
		{"MapCache", lru.NewMapCache[*diffValue]},
		{"RadixCache", lru.NewRadixCache[*diffValue]},
		{"ArenaRadixCache", lru.NewArenaRadixCache[*diffValue]},
	}

	baseOpts := slices.Concat([]lru.Option{
		lru.WithPressureFunc(func() float64 { return 0.0 }),
	}, opts)

	for _, cfg := range configs {
		for _, inv := range []bool{false, true} {
			c := cfg.constructor(maxSize, slices.Concat([]lru.Option{lru.WithInvariantChecking(inv)}, baseOpts)...)
			h.instances = append(h.instances, diffInstance{
				name:       cfg.name,
				invariants: inv,
				cache:      c,
			})
		}
	}

	return h
}

func (h *differentialHarness) compareErrors(op string, baseErr, targetErr error, instName string, invariants bool) {
	h.t.Helper()
	if baseErr == nil {
		require.NoErrorf(h.t, targetErr, "[%s] error parity mismatch with %s (inv=%v)", op, instName, invariants)
		return
	}
	require.Errorf(h.t, targetErr, "[%s] error parity mismatch with %s (inv=%v): base err = %v", op, instName, invariants, baseErr)
	require.EqualErrorf(h.t, targetErr, baseErr.Error(), "[%s] error message mismatch with %s (inv=%v)", op, instName, invariants)
}

func (h *differentialHarness) compareValues(op string, baseVal *diffValue, baseOK bool, targetVal *diffValue, targetOK bool, instName string, invariants bool) {
	h.t.Helper()
	require.Equalf(h.t, baseOK, targetOK, "[%s] ok parity mismatch with %s (inv=%v)", op, instName, invariants)
	if !baseOK {
		require.Nilf(h.t, targetVal, "[%s] absent value must be zero/nil on %s (inv=%v)", op, instName, invariants)
		return
	}
	if baseVal == nil {
		require.Nilf(h.t, targetVal, "[%s] nil value parity mismatch with %s (inv=%v)", op, instName, invariants)
		return
	}
	require.NotNilf(h.t, targetVal, "[%s] value nil parity mismatch with %s (inv=%v): base = %v", op, instName, invariants, baseVal)
	require.Equalf(h.t, baseVal.id, targetVal.id, "[%s] value id mismatch with %s (inv=%v)", op, instName, invariants)
	require.Equalf(h.t, baseVal.size, targetVal.size, "[%s] value size mismatch with %s (inv=%v)", op, instName, invariants)
}

func (h *differentialHarness) compareEvicted(op string, baseEvicted, targetEvicted []*diffValue, instName string, invariants bool) {
	h.t.Helper()
	require.Lenf(h.t, targetEvicted, len(baseEvicted), "[%s] evicted slice length mismatch with %s (inv=%v)", op, instName, invariants)
	for i := range baseEvicted {
		h.compareValues(fmt.Sprintf("%s.evicted[%d]", op, i), baseEvicted[i], true, targetEvicted[i], true, instName, invariants)
	}
}

func (h *differentialHarness) Put(key string, val *diffValue) []*diffValue {
	h.t.Helper()
	var baseEvicted []*diffValue
	var baseErr error

	for i, inst := range h.instances {
		evicted, err := inst.cache.Put(key, val)
		if i == 0 {
			baseEvicted = evicted
			baseErr = err
		} else {
			h.compareErrors(fmt.Sprintf("Put(%q)", key), baseErr, err, inst.name, inst.invariants)
			h.compareEvicted(fmt.Sprintf("Put(%q)", key), baseEvicted, evicted, inst.name, inst.invariants)
		}
	}
	return baseEvicted
}

func (h *differentialHarness) Delete(key string) *diffValue {
	h.t.Helper()
	var baseVal *diffValue
	var baseOK bool

	for i, inst := range h.instances {
		val, ok := inst.cache.Delete(key)
		if i == 0 {
			baseVal = val
			baseOK = ok
		} else {
			h.compareValues(fmt.Sprintf("Delete(%q)", key), baseVal, baseOK, val, ok, inst.name, inst.invariants)
		}
	}
	return baseVal
}

func (h *differentialHarness) Get(key string) *diffValue {
	h.t.Helper()
	var baseVal *diffValue
	var baseOK bool

	for i, inst := range h.instances {
		val, ok := inst.cache.Get(key)
		if i == 0 {
			baseVal = val
			baseOK = ok
		} else {
			h.compareValues(fmt.Sprintf("Get(%q)", key), baseVal, baseOK, val, ok, inst.name, inst.invariants)
		}
	}
	return baseVal
}

func (h *differentialHarness) Peek(key string) *diffValue {
	h.t.Helper()
	var baseVal *diffValue
	var baseOK bool

	for i, inst := range h.instances {
		val, ok := inst.cache.Peek(key)
		if i == 0 {
			baseVal = val
			baseOK = ok
		} else {
			h.compareValues(fmt.Sprintf("Peek(%q)", key), baseVal, baseOK, val, ok, inst.name, inst.invariants)
		}
	}
	return baseVal
}

func (h *differentialHarness) Replace(key string, val *diffValue) {
	h.t.Helper()
	var baseErr error

	for i, inst := range h.instances {
		err := inst.cache.Replace(key, val)
		if i == 0 {
			baseErr = err
		} else {
			h.compareErrors(fmt.Sprintf("Replace(%q)", key), baseErr, err, inst.name, inst.invariants)
		}
	}
}

func (h *differentialHarness) DeletePrefix(prefix string) {
	h.t.Helper()
	for _, inst := range h.instances {
		inst.cache.DeletePrefix(prefix)
	}
}

func (h *differentialHarness) VerifyIterators() {
	h.t.Helper()
	var baseKeys []string
	var baseVals []*diffValue

	for i, inst := range h.instances {
		var allKeys []string
		var allVals []*diffValue
		for k, v := range inst.cache.All() {
			allKeys = append(allKeys, k)
			allVals = append(allVals, v)
		}

		keysSeq := slices.Collect(inst.cache.Keys())
		valsSeq := slices.Collect(inst.cache.Values())

		require.Equalf(h.t, allKeys, keysSeq, "[VerifyIterators] All() keys vs Keys() mismatch on %s (inv=%v)", inst.name, inst.invariants)
		require.Lenf(h.t, valsSeq, len(allVals), "[VerifyIterators] All() values vs Values() len mismatch on %s (inv=%v)", inst.name, inst.invariants)
		for j := range allVals {
			h.compareValues(fmt.Sprintf("VerifyIterators.Values[%d]", j), allVals[j], true, valsSeq[j], true, inst.name, inst.invariants)
		}

		if i == 0 {
			baseKeys = allKeys
			baseVals = allVals
		} else {
			require.Equalf(h.t, baseKeys, allKeys, "[VerifyIterators] MRU-to-LRU key order mismatch on %s (inv=%v)", inst.name, inst.invariants)
			require.Lenf(h.t, allVals, len(baseVals), "[VerifyIterators] MRU-to-LRU value count mismatch on %s (inv=%v)", inst.name, inst.invariants)
			for j := range baseVals {
				h.compareValues(fmt.Sprintf("VerifyIterators.All[%d]", j), baseVals[j], true, allVals[j], true, inst.name, inst.invariants)
			}
		}

		for _, limit := range []int{1, len(allKeys) / 2} {
			if limit <= 0 || limit > len(allKeys) {
				continue
			}
			var prefixAllKeys []string
			var prefixAllVals []*diffValue
			for k, v := range inst.cache.All() {
				prefixAllKeys = append(prefixAllKeys, k)
				prefixAllVals = append(prefixAllVals, v)
				if len(prefixAllKeys) == limit {
					break
				}
			}
			require.Equalf(h.t, baseKeys[:limit], prefixAllKeys, "[VerifyIterators] early break All() keys mismatch on %s (inv=%v)", inst.name, inst.invariants)
			for j := range limit {
				h.compareValues(fmt.Sprintf("VerifyIterators.EarlyBreakAll[%d]", j), baseVals[j], true, prefixAllVals[j], true, inst.name, inst.invariants)
			}

			var prefixKeys []string
			for k := range inst.cache.Keys() {
				prefixKeys = append(prefixKeys, k)
				if len(prefixKeys) == limit {
					break
				}
			}
			require.Equalf(h.t, baseKeys[:limit], prefixKeys, "[VerifyIterators] early break Keys() mismatch on %s (inv=%v)", inst.name, inst.invariants)

			var prefixVals []*diffValue
			for v := range inst.cache.Values() {
				prefixVals = append(prefixVals, v)
				if len(prefixVals) == limit {
					break
				}
			}
			for j := range limit {
				h.compareValues(fmt.Sprintf("VerifyIterators.EarlyBreakValues[%d]", j), baseVals[j], true, prefixVals[j], true, inst.name, inst.invariants)
			}
		}
	}
	h.VerifyStatsParity("VerifyIterators")
}

func (h *differentialHarness) VerifyStatsParity(op string) {
	h.t.Helper()
	var base lru.Stats
	var prevSameBackend lru.Stats

	for i, inst := range h.instances {
		st := inst.cache.Stats()
		var provider lru.StatsProvider = inst.cache
		require.Equalf(h.t, st, provider.Stats(), "[%s] StatsProvider.Stats() mismatch on %s (inv=%v)", op, inst.name, inst.invariants)

		// Verify Evictions(reason) and EvictedWeight(reason) helper methods match flat fields.
		require.Equalf(h.t, st.EvictionsCapacity, st.Evictions(lru.EvictionReasonCapacity), "[%s] Evictions(Capacity) mismatch on %s", op, inst.name)
		require.Equalf(h.t, st.EvictionsPressure, st.Evictions(lru.EvictionReasonPressure), "[%s] Evictions(Pressure) mismatch on %s", op, inst.name)
		require.Equalf(h.t, st.EvictionsDeleted, st.Evictions(lru.EvictionReasonDeleted), "[%s] Evictions(Deleted) mismatch on %s", op, inst.name)
		require.Equalf(h.t, st.EvictionsReplaced, st.Evictions(lru.EvictionReasonReplaced), "[%s] Evictions(Replaced) mismatch on %s", op, inst.name)
		require.Zero(h.t, st.Evictions(lru.EvictionReason(99)))

		require.Equalf(h.t, st.EvictedWeightCapacity, st.EvictedWeight(lru.EvictionReasonCapacity), "[%s] EvictedWeight(Capacity) mismatch on %s", op, inst.name)
		require.Equalf(h.t, st.EvictedWeightPressure, st.EvictedWeight(lru.EvictionReasonPressure), "[%s] EvictedWeight(Pressure) mismatch on %s", op, inst.name)
		require.Equalf(h.t, st.EvictedWeightDeleted, st.EvictedWeight(lru.EvictionReasonDeleted), "[%s] EvictedWeight(Deleted) mismatch on %s", op, inst.name)
		require.Equalf(h.t, st.EvictedWeightReplaced, st.EvictedWeight(lru.EvictionReasonReplaced), "[%s] EvictedWeight(Replaced) mismatch on %s", op, inst.name)
		require.Zero(h.t, st.EvictedWeight(lru.EvictionReason(99)))

		// Verify backend identity and backend-specific arena fields.
		switch inst.name {
		case "MapCache":
			require.Equalf(h.t, lru.BackendMap, st.Backend, "[%s] Backend mismatch on %s", op, inst.name)
			require.Zerof(h.t, st.ArenaLiveNodes, "[%s] ArenaLiveNodes must be 0 on %s", op, inst.name)
			require.Zerof(h.t, st.ArenaFreeNodes, "[%s] ArenaFreeNodes must be 0 on %s", op, inst.name)
			require.Zerof(h.t, st.ArenaUnallocatedCap, "[%s] ArenaUnallocatedCap must be 0 on %s", op, inst.name)
			require.Zerof(h.t, st.ArenaHashFallbacks, "[%s] ArenaHashFallbacks must be 0 on %s", op, inst.name)
		case "RadixCache":
			require.Equalf(h.t, lru.BackendRadix, st.Backend, "[%s] Backend mismatch on %s", op, inst.name)
			require.Zerof(h.t, st.ArenaLiveNodes, "[%s] ArenaLiveNodes must be 0 on %s", op, inst.name)
			require.Zerof(h.t, st.ArenaFreeNodes, "[%s] ArenaFreeNodes must be 0 on %s", op, inst.name)
			require.Zerof(h.t, st.ArenaUnallocatedCap, "[%s] ArenaUnallocatedCap must be 0 on %s", op, inst.name)
			require.Zerof(h.t, st.ArenaHashFallbacks, "[%s] ArenaHashFallbacks must be 0 on %s", op, inst.name)
		case "ArenaRadixCache":
			require.Equalf(h.t, lru.BackendArenaRadix, st.Backend, "[%s] Backend mismatch on %s", op, inst.name)
			if st.Len == 0 {
				require.Zerof(h.t, st.ArenaLiveNodes, "[%s] ArenaLiveNodes must be 0 when Len==0 on %s", op, inst.name)
			} else {
				// Non-empty keys allocate at least 1 node in c.nodes[1:], whereas the empty key "" is stored
				// directly on the root node (index 0, excluded from ArenaLiveNodes).
				require.GreaterOrEqualf(h.t, st.ArenaLiveNodes, max(st.Len-1, 0), "[%s] ArenaLiveNodes (%d) must be >= Len-1 (%d) on %s", op, st.ArenaLiveNodes, st.Len-1, inst.name)
			}
			require.GreaterOrEqualf(h.t, st.ArenaFreeNodes, 0, "[%s] ArenaFreeNodes must be >= 0 on %s", op, inst.name)
			require.GreaterOrEqualf(h.t, st.ArenaUnallocatedCap, 0, "[%s] ArenaUnallocatedCap must be >= 0 on %s", op, inst.name)
		}

		// Instances come in (inv=false, inv=true) pairs per backend; both must produce identical Stats structs.
		if i%2 == 0 {
			prevSameBackend = st
		} else {
			require.Equalf(h.t, prevSameBackend, st, "[%s] full Stats mismatch between inv=false and inv=true on %s", op, inst.name)
		}

		if i == 0 {
			base = st
			continue
		}

		// Cross-backend parity on all backend-independent fields:
		require.Equalf(h.t, base.GetHits, st.GetHits, "[%s] GetHits mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.GetMisses, st.GetMisses, "[%s] GetMisses mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.PeekHits, st.PeekHits, "[%s] PeekHits mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.PeekMisses, st.PeekMisses, "[%s] PeekMisses mismatch with %s (inv=%v)", op, inst.name, inst.invariants)

		require.Equalf(h.t, base.EvictionsCapacity, st.EvictionsCapacity, "[%s] EvictionsCapacity mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.EvictionsPressure, st.EvictionsPressure, "[%s] EvictionsPressure mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.EvictionsDeleted, st.EvictionsDeleted, "[%s] EvictionsDeleted mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.EvictionsReplaced, st.EvictionsReplaced, "[%s] EvictionsReplaced mismatch with %s (inv=%v)", op, inst.name, inst.invariants)

		require.Equalf(h.t, base.EvictedWeightCapacity, st.EvictedWeightCapacity, "[%s] EvictedWeightCapacity mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.EvictedWeightPressure, st.EvictedWeightPressure, "[%s] EvictedWeightPressure mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.EvictedWeightDeleted, st.EvictedWeightDeleted, "[%s] EvictedWeightDeleted mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.EvictedWeightReplaced, st.EvictedWeightReplaced, "[%s] EvictedWeightReplaced mismatch with %s (inv=%v)", op, inst.name, inst.invariants)

		require.Equalf(h.t, base.CurrentSize, st.CurrentSize, "[%s] CurrentSize mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.MaxSize, st.MaxSize, "[%s] MaxSize mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.Len, st.Len, "[%s] Len mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.ZeroSizeCount, st.ZeroSizeCount, "[%s] ZeroSizeCount mismatch with %s (inv=%v)", op, inst.name, inst.invariants)

		require.Equalf(h.t, base.PutInserted, st.PutInserted, "[%s] PutInserted mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.PutUpdated, st.PutUpdated, "[%s] PutUpdated mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.PutRejectedOversized, st.PutRejectedOversized, "[%s] PutRejectedOversized mismatch with %s (inv=%v)", op, inst.name, inst.invariants)

		require.Equalf(h.t, base.ReplaceUpdated, st.ReplaceUpdated, "[%s] ReplaceUpdated mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.ReplaceNotFound, st.ReplaceNotFound, "[%s] ReplaceNotFound mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.ReplaceSelfEvicted, st.ReplaceSelfEvicted, "[%s] ReplaceSelfEvicted mismatch with %s (inv=%v)", op, inst.name, inst.invariants)

		require.Equalf(h.t, base.DeleteDeleted, st.DeleteDeleted, "[%s] DeleteDeleted mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.DeleteNotFound, st.DeleteNotFound, "[%s] DeleteNotFound mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.DeletePrefixExecuted, st.DeletePrefixExecuted, "[%s] DeletePrefixExecuted mismatch with %s (inv=%v)", op, inst.name, inst.invariants)

		require.Equalf(h.t, base.PressureShedsInline, st.PressureShedsInline, "[%s] PressureShedsInline mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.PressureShedsExplicit, st.PressureShedsExplicit, "[%s] PressureShedsExplicit mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.InDeltaf(h.t, base.MemoryPressure, st.MemoryPressure, 0.0, "[%s] MemoryPressure mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
	}
}

func (h *differentialHarness) VerifyFullCompactionStatsParity(op string) {
	h.t.Helper()
	h.VerifyStatsParity(op)
	base := h.instances[0].cache.Stats()
	for _, inst := range h.instances[1:] {
		st := inst.cache.Stats()
		require.Equalf(h.t, base.CompactionsExplicit, st.CompactionsExplicit, "[%s] CompactionsExplicit mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.CompactionsPressureTier1, st.CompactionsPressureTier1, "[%s] CompactionsPressureTier1 mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.CompactionsPressureTier2, st.CompactionsPressureTier2, "[%s] CompactionsPressureTier2 mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.CompactionsAutoSlack, st.CompactionsAutoSlack, "[%s] CompactionsAutoSlack mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.ReclaimEpoch, st.ReclaimEpoch, "[%s] ReclaimEpoch mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.DeletedSinceCompact, st.DeletedSinceCompact, "[%s] DeletedSinceCompact mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
		require.Equalf(h.t, base.PeakEntryLen, st.PeakEntryLen, "[%s] PeakEntryLen mismatch with %s (inv=%v)", op, inst.name, inst.invariants)
	}
}

func (h *differentialHarness) DrainAndVerifyEvictionOrder(allKnownKeys []string) {
	h.t.Helper()
	h.VerifyIterators()
	preDrainVals := slices.Collect(h.instances[0].cache.Values())
	var expectedLRUOrder []*diffValue
	for _, v := range slices.Backward(preDrainVals) {
		expectedLRUOrder = append(expectedLRUOrder, v)
	}

	for _, k := range allKnownKeys {
		h.Peek(k)
	}
	if h.unitWeight {
		var allEvicted []*diffValue
		for i := range h.maxSize {
			drainKey := fmt.Sprintf("__DRAIN_KEY_%d__", i)
			evicted := h.Put(drainKey, &diffValue{id: fmt.Sprintf("__DRAIN_ITEM_%d__", i), size: 1})
			allEvicted = append(allEvicted, evicted...)
		}
		h.compareEvicted("DrainAndVerifyEvictionOrder.unitWeight", expectedLRUOrder, allEvicted, h.instances[0].name, h.instances[0].invariants)
		h.VerifyIterators()
		return
	}
	drainVal := &diffValue{id: "__DRAIN_ITEM__", size: h.maxSize}
	evicted := h.Put("__DRAIN_KEY__", drainVal)
	require.LessOrEqual(h.t, len(evicted), len(expectedLRUOrder))
	h.compareEvicted("DrainAndVerifyEvictionOrder", expectedLRUOrder[:len(evicted)], evicted, h.instances[0].name, h.instances[0].invariants)
	h.VerifyIterators()
}

func TestDifferential_FlatWorkload(t *testing.T) {
	// Arrange
	r := rand.New(rand.NewPCG(1337, 0))
	const (
		numOps        = 5000
		keyPoolSize   = 200
		cacheCapacity = 2000
	)

	keys := make([]string, keyPoolSize)
	for i := range keyPoolSize {
		keys[i] = fmt.Sprintf("flat_key_%04d", i)
	}

	h := newDifferentialHarness(t, cacheCapacity)

	// Act
	for op := range numOps {
		k := keys[r.IntN(keyPoolSize)]
		dice := r.IntN(100)

		switch {
		case dice < 35:
			sz := uint64(r.IntN(50) + 1)
			h.Put(k, &diffValue{id: fmt.Sprintf("%s_v%d", k, op), size: sz})
		case dice < 60:
			h.Get(k)
		case dice < 75:
			h.Peek(k)
		case dice < 85:
			existing := h.Peek(k)
			if existing != nil {
				h.Replace(k, &diffValue{id: fmt.Sprintf("%s_upd_%d", k, op), size: existing.weight()})
			} else {
				h.Replace(k, &diffValue{id: fmt.Sprintf("%s_upd_%d", k, op), size: 10})
			}
		case dice < 90:
			newSz := uint64(r.IntN(80))
			h.Replace(k, &diffValue{id: fmt.Sprintf("%s_szupd_%d", k, op), size: newSz})
		case dice < 95:
			h.Delete(k)
		default:
			prefix := fmt.Sprintf("flat_key_%02d", r.IntN(20))
			h.DeletePrefix(prefix)
		}
		if (op+1)%250 == 0 {
			h.VerifyIterators()
		}
	}

	// Assert
	h.DrainAndVerifyEvictionOrder(keys)
}

func TestDifferential_HierarchicalDirectoryWorkload(t *testing.T) {
	// Arrange
	r := rand.New(rand.NewPCG(4242, 0))
	const (
		numOps        = 5000
		cacheCapacity = 3000
	)

	var paths []string
	topDirs := []string{"/var", "/usr", "/home", "/opt"}
	subDirs := []string{"log", "local", "bin", "lib", "data"}

	for _, top := range topDirs {
		for _, sub1 := range subDirs {
			for _, sub2 := range subDirs {
				for f := range 3 {
					paths = append(paths, fmt.Sprintf("%s/%s/%s/file_%02d.dat", top, sub1, sub2, f))
				}
			}
		}
	}

	h := newDifferentialHarness(t, cacheCapacity)

	// Act
	for op := range numOps {
		path := paths[r.IntN(len(paths))]
		dice := r.IntN(100)

		switch {
		case dice < 30:
			sz := uint64(r.IntN(80) + 1)
			h.Put(path, &diffValue{id: fmt.Sprintf("file_%d", op), size: sz})
		case dice < 55:
			h.Get(path)
		case dice < 70:
			h.Peek(path)
		case dice < 80:
			existing := h.Peek(path)
			if existing != nil {
				h.Replace(path, &diffValue{id: fmt.Sprintf("upd_%d", op), size: existing.weight()})
			}
		case dice < 85:
			newSz := uint64(r.IntN(120))
			h.Replace(path, &diffValue{id: fmt.Sprintf("szupd_%d", op), size: newSz})
		case dice < 90:
			h.Delete(path)
		default:
			prefix := fmt.Sprintf("%s/%s/", topDirs[r.IntN(len(topDirs))], subDirs[r.IntN(len(subDirs))])
			h.DeletePrefix(prefix)
		}
		if (op+1)%250 == 0 {
			h.VerifyIterators()
		}
	}

	// Assert
	h.DrainAndVerifyEvictionOrder(paths)
}

func TestDifferential_CapacityThrashingAndSizeUpdates(t *testing.T) {
	// Arrange
	r := rand.New(rand.NewPCG(7777, 0))
	const (
		numOps        = 5000
		cacheCapacity = 200
	)

	keys := make([]string, 50)
	for i := range len(keys) {
		keys[i] = fmt.Sprintf("thrash_%d", i)
	}

	h := newDifferentialHarness(t, cacheCapacity)

	// Act: Explicitly verify self-eviction when updating entry weight beyond maxSize across all engines.
	h.Put("overflow_key", &diffValue{id: "ovf", size: 20})
	h.Replace("overflow_key", &diffValue{id: "ovf_max", size: math.MaxUint64})
	h.Replace("overflow_key", &diffValue{id: "ovf_miss", size: math.MaxUint64 - 10})

	for op := range numOps {
		k := keys[r.IntN(len(keys))]
		dice := r.IntN(100)

		switch {
		case dice < 40:
			sz := uint64(r.IntN(170) + 10)
			h.Put(k, &diffValue{id: fmt.Sprintf("thrash_v%d", op), size: sz})
		case dice < 60:
			h.Get(k)
		case dice < 75:
			h.Peek(k)
		case dice < 85:
			newSz := uint64(r.IntN(230))
			h.Replace(k, &diffValue{id: fmt.Sprintf("thrash_upd_%d", op), size: newSz})
		case dice < 95:
			h.Delete(k)
		default:
			h.DeletePrefix("thrash_")
		}
		if (op+1)%250 == 0 {
			h.VerifyIterators()
		}
	}

	// Assert
	h.DrainAndVerifyEvictionOrder(keys)
}

func TestDifferential_BoundaryAndEdgeCases(t *testing.T) {
	// Arrange
	r := rand.New(rand.NewPCG(12345, 0))
	const (
		numOps        = 3000
		cacheCapacity = 1000
	)

	adversarialKeys := []string{
		"",
		"a", "b", "c", "/", ".", "~",
		"key\x00with\x00nulls",
		"\x00\x01\x02\x03\x04",
		"📁/documents/報告書_2026.pdf",
		"🚀/starship/alpha",
		"مرحبا/عالم/ملف",
		"latin/café/naïve/résumé.txt",
		strings.Repeat("long_key_segment/", 20),
		"\xff\xfe\xfd\xfc",
	}

	h := newDifferentialHarness(t, cacheCapacity)

	// Act
	for op := range numOps {
		k := adversarialKeys[r.IntN(len(adversarialKeys))]
		dice := r.IntN(100)

		switch {
		case dice < 30:
			szRoll := r.IntN(10)
			var sz uint64
			switch szRoll {
			case 0:
				sz = 0
			case 1:
				sz = cacheCapacity + 100
			default:
				sz = uint64(r.IntN(100) + 1)
			}
			h.Put(k, &diffValue{id: fmt.Sprintf("adv_%d", op), size: sz})
		case dice < 35:
			h.Put(k, nil)
		case dice < 55:
			h.Get(k)
		case dice < 70:
			h.Peek(k)
		case dice < 80:
			existing := h.Peek(k)
			if existing != nil {
				switch r.IntN(3) {
				case 0:
					h.Replace(k, &diffValue{id: fmt.Sprintf("upd_%d", op), size: existing.weight()})
				case 1:
					h.Replace(k, &diffValue{id: fmt.Sprintf("grow_%d", op), size: existing.weight() + 5})
				default:
					var shrunk uint64
					if existing.weight() > 5 {
						shrunk = existing.weight() - 5
					}
					h.Replace(k, &diffValue{id: fmt.Sprintf("shrink_%d", op), size: shrunk})
				}
			} else {
				h.Replace(k, &diffValue{id: fmt.Sprintf("noent_%d", op), size: 10})
			}
		case dice < 88:
			h.Replace(k, &diffValue{id: fmt.Sprintf("rand_sz_%d", op), size: uint64(r.IntN(150))})
		case dice < 94:
			h.Delete(k)
		default:
			prefix := adversarialKeys[r.IntN(len(adversarialKeys))]
			h.DeletePrefix(prefix)
		}
		if (op+1)%250 == 0 {
			h.VerifyIterators()
		}
	}

	// Assert
	h.DrainAndVerifyEvictionOrder(adversarialKeys)
}

func TestDifferential_ReplaceSelfEvictionPreservesPreviousValue(t *testing.T) {
	// Arrange
	h := newDifferentialHarness(t, 100)
	dv := &diffValue{id: "self_evict", size: 50}
	h.Put("k1", dv)

	// Act: Update k1 to a new value of size 110 (> maxSize 100), triggering self-eviction across all backends.
	h.Replace("k1", &diffValue{id: "self_evict_2", size: 110})

	// Assert: k1 is evicted and original dv.size remains 50.
	require.Nil(t, h.Peek("k1"))
	require.Equal(t, uint64(50), dv.weight())
}

func TestDifferential_PressureAwareAndCompactionParity(t *testing.T) {
	// Arrange
	r := rand.New(rand.NewPCG(20260925, 0))
	const (
		numOps        = 2000
		cacheCapacity = 1000
	)
	pressure := 0.10

	h := newDifferentialHarness(
		t,
		cacheCapacity,
		lru.WithPressureFunc(func() float64 { return pressure }),
		lru.WithCompactionThreshold(0.75),
		lru.WithEvictionThreshold(0.90),
		lru.WithEvictionRetentionRatio(0.50),
	)

	keys := make([]string, 80)
	for i := range keys {
		keys[i] = fmt.Sprintf("bucket_%d/dir_%d/item_%03d", i%4, (i/4)%4, i)
	}

	// Act
	for op := range numOps {
		switch op % 25 {
		case 0:
			pressure = 0.80 // Tier 1 moderate pressure
		case 10:
			pressure = 0.95 // Tier 2 critical pressure
		case 15:
			pressure = 0.10 // Normal pressure
		}

		k := keys[r.IntN(len(keys))]
		dice := r.IntN(100)
		switch {
		case dice < 35:
			sz := uint64(r.IntN(45) + 5)
			h.Put(k, &diffValue{id: fmt.Sprintf("pv_%d", op), size: sz})
		case dice < 55:
			h.Get(k)
		case dice < 68:
			h.Peek(k)
		case dice < 78:
			h.Delete(k)
		case dice < 86:
			newSz := uint64(r.IntN(60))
			h.Replace(k, &diffValue{id: fmt.Sprintf("pu_%d", op), size: newSz})
		case dice < 93:
			for _, inst := range h.instances {
				inst.cache.(lru.PressureAwareCache[*diffValue]).Compact()
			}
		default:
			var baseEvicted []*diffValue
			for i, inst := range h.instances {
				ev := inst.cache.(lru.PressureAwareCache[*diffValue]).EvaluateMemoryPressure()
				if i == 0 {
					baseEvicted = ev
				} else {
					h.compareEvicted("EvaluateMemoryPressure()", baseEvicted, ev, inst.name, inst.invariants)
				}
			}
		}
		if (op+1)%250 == 0 {
			h.VerifyIterators()
		}
	}

	// Assert
	pressure = 0.10
	h.DrainAndVerifyEvictionOrder(keys)
}

func TestDifferential_ReplaceSingleEntryExceedsMaxSizePreservesOtherEntries(t *testing.T) {
	// Arrange
	h := newDifferentialHarness(t, 100)
	h.Put("k1", &diffValue{id: "v1", size: 20})
	h.Put("k2", &diffValue{id: "v2", size: 20})
	h.Put("k3", &diffValue{id: "v3", size: 30})

	// Act: grow k3 from 30 to 110 (> maxSize 100). Only k3 should be evicted.
	h.Replace("k3", &diffValue{id: "v3_big", size: 110})

	// Assert
	assert.Nil(t, h.Peek("k3"), "k3 exceeds maxSize and must be evicted")
	assert.NotNil(t, h.Peek("k1"), "k1 must not be collateral-evicted")
	assert.NotNil(t, h.Peek("k2"), "k2 must not be collateral-evicted")
}

func TestDifferential_PutNearMaxUint64EvictsWithoutOverflow(t *testing.T) {
	// Arrange
	const maxCap = uint64(math.MaxUint64)
	const halfPlus = uint64(math.MaxUint64/2) + 100
	h := newDifferentialHarness(t, maxCap)

	h.Put("k1", &diffValue{id: "v1", size: halfPlus})

	// Act: inserting k2 of size halfPlus would overflow uint64 if added before evicting k1.
	evicted := h.Put("k2", &diffValue{id: "v2", size: halfPlus})

	// Assert
	require.Len(t, evicted, 1)
	assert.Nil(t, h.Peek("k1"))
	assert.NotNil(t, h.Peek("k2"))

	// Subsequent Delete must not underflow currentSize or panic.
	erased := h.Delete("k2")
	require.NotNil(t, erased)
}

func TestDifferential_ReplaceMiddleEntrySelfEvictionAndMaxUint64(t *testing.T) {
	// Arrange 1: k1 (10B, LRU), k2 (20B, mid), k3 (20B, MRU) in maxSize = 50.
	h := newDifferentialHarness(t, 50)
	h.Put("k1", &diffValue{id: "v1", size: 10})
	h.Put("k2", &diffValue{id: "v2", size: 20})
	h.Put("k3", &diffValue{id: "v3", size: 20})

	// Act 1: Grow k2 from 20 to 40. Since k2 (40) + k3 (20) = 60 > 50, k2 cannot fit alongside newer k3.
	h.Replace("k2", &diffValue{id: "v2_grow", size: 40})

	// Assert 1: k2 is self-evicted while older entry k1 (10B) and newer entry k3 (20B) both survive.
	assert.Nil(t, h.Peek("k2"))
	assert.NotNil(t, h.Peek("k1"))
	assert.NotNil(t, h.Peek("k3"))

	// Arrange 2: Near math.MaxUint64, k1 (MaxUint64 - 50) + k2 (50) == MaxUint64.
	maxH := newDifferentialHarness(t, math.MaxUint64)
	maxH.Put("k1", &diffValue{id: "v1", size: math.MaxUint64 - 50})
	maxH.Put("k2", &diffValue{id: "v2", size: 50})

	// Act 2: Grow k2 from 50 to 150. Evicting k1 first prevents uint64 overflow.
	maxH.Replace("k2", &diffValue{id: "v2_grow", size: 150})

	// Assert 2
	assert.Nil(t, maxH.Peek("k1"))
	assert.NotNil(t, maxH.Peek("k2"))
}

func TestDifferential_ReplaceLockstepParity(t *testing.T) {
	// Updating the MRU head when it requires evicting multiple older entries
	h := newDifferentialHarness(t, 100)
	h.Put("k1", &diffValue{id: "v1", size: 10})
	h.Put("k2", &diffValue{id: "v2", size: 20})
	h.Put("k3", &diffValue{id: "v3", size: 30})
	h.Put("k4", &diffValue{id: "v4", size: 30})

	// cache: k1 (10), k2 (20), k3 (30), k4 (30) -> grow k4 to 70
	h.Replace("k4", &diffValue{id: "v4_grow", size: 70})
	assert.Nil(t, h.Peek("k1"))
	assert.Nil(t, h.Peek("k2"))
	assert.NotNil(t, h.Peek("k3"))
	assert.NotNil(t, h.Peek("k4"))

	// Updating the second-most-recent entry (head.next) when head.size + newSize > maxSize (self-evicts)
	h2 := newDifferentialHarness(t, 100)
	h2.Put("k1", &diffValue{id: "v1", size: 10})
	h2.Put("k2", &diffValue{id: "v2", size: 20})
	h2.Put("k3", &diffValue{id: "v3", size: 30}) // head.next
	h2.Put("k4", &diffValue{id: "v4", size: 40}) // head
	h2.Replace("k3", &diffValue{id: "v3_grow", size: 65})
	assert.Nil(t, h2.Peek("k3")) // self-evicted
	assert.NotNil(t, h2.Peek("k1"))
	assert.NotNil(t, h2.Peek("k2"))
	assert.NotNil(t, h2.Peek("k4"))

	// Updating the second-most-recent entry (head.next) when head.size + newSize <= maxSize (evicts older)
	h3 := newDifferentialHarness(t, 100)
	h3.Put("k1", &diffValue{id: "v1", size: 10})
	h3.Put("k2", &diffValue{id: "v2", size: 20})
	h3.Put("k3", &diffValue{id: "v3", size: 30}) // head.next
	h3.Put("k4", &diffValue{id: "v4", size: 30}) // head
	h3.Replace("k3", &diffValue{id: "v3_grow", size: 60})
	assert.Nil(t, h3.Peek("k1"))
	assert.Nil(t, h3.Peek("k2"))
	assert.NotNil(t, h3.Peek("k3"))
	assert.NotNil(t, h3.Peek("k4"))

	// Updating the LRU tail entry when it fits (sizeDelta <= maxSize - currentSize)
	h4 := newDifferentialHarness(t, 100)
	h4.Put("k1", &diffValue{id: "v1", size: 10}) // tail
	h4.Put("k2", &diffValue{id: "v2", size: 50}) // head
	h4.Replace("k1", &diffValue{id: "v1_grow", size: 40})
	assert.NotNil(t, h4.Peek("k1"))
	assert.NotNil(t, h4.Peek("k2"))

	// Updating the LRU tail entry when it self-evicts (sizeDelta > maxSize - currentSize)
	h5 := newDifferentialHarness(t, 100)
	h5.Put("k1", &diffValue{id: "v1", size: 10}) // tail
	h5.Put("k2", &diffValue{id: "v2", size: 80}) // head
	h5.Replace("k1", &diffValue{id: "v1_grow", size: 30})
	assert.Nil(t, h5.Peek("k1"))
	assert.NotNil(t, h5.Peek("k2"))

	// Shrinking an entry's weight in-place frees capacity for subsequent inserts without changing LRU order
	h6 := newDifferentialHarness(t, 100)
	h6.Put("k1", &diffValue{id: "v1", size: 50}) // tail
	h6.Put("k2", &diffValue{id: "v2", size: 50}) // head
	h6.Replace("k1", &diffValue{id: "v1_shrunk", size: 10})
	ev := h6.Put("k3", &diffValue{id: "v3", size: 40})
	assert.Empty(t, ev)
	assert.NotNil(t, h6.Peek("k1"))
	assert.NotNil(t, h6.Peek("k2"))
	assert.NotNil(t, h6.Peek("k3"))
}

func TestDifferential_DefaultWeigherWorkload(t *testing.T) {
	// Arrange: 6 instances (MapCache, RadixCache, ArenaRadixCache x invariants={false,true})
	// configured WITHOUT WithWeigher so every entry has default weight 1.
	r := rand.New(rand.NewPCG(20260930, 0))
	const (
		numOps        = 5000
		cacheCapacity = 50
	)
	pressure := 0.10
	h := newDifferentialHarnessDefaultWeigher(
		t,
		cacheCapacity,
		lru.WithPressureFunc(func() float64 { return pressure }),
		lru.WithCompactionThreshold(0.75),
		lru.WithEvictionThreshold(0.90),
		lru.WithEvictionRetentionRatio(0.50),
	)

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("ns_%d/dir_%d/key_%03d", i%4, (i/4)%5, i)
	}

	// Act
	for op := range numOps {
		switch op % 40 {
		case 0:
			pressure = 0.80
		case 15:
			pressure = 0.95
		case 20:
			pressure = 0.10
		}

		k := keys[r.IntN(len(keys))]
		dice := r.IntN(100)
		switch {
		case dice < 35:
			if r.IntN(10) == 0 {
				h.Put(k, nil) // nil pointer value still weighs 1 under default weigher
			} else {
				h.Put(k, &diffValue{id: fmt.Sprintf("def_%d", op), size: uint64(r.IntN(10000))})
			}
		case dice < 55:
			h.Get(k)
		case dice < 70:
			h.Peek(k)
		case dice < 82:
			h.Replace(k, &diffValue{id: fmt.Sprintf("def_upd_%d", op), size: uint64(r.IntN(10000))})
		case dice < 90:
			h.Delete(k)
		case dice < 95:
			prefix := fmt.Sprintf("ns_%d/dir_%d/", r.IntN(4), r.IntN(5))
			h.DeletePrefix(prefix)
		case dice < 98:
			for _, inst := range h.instances {
				inst.cache.(lru.PressureAwareCache[*diffValue]).Compact()
			}
		default:
			var baseEvicted []*diffValue
			for i, inst := range h.instances {
				ev := inst.cache.(lru.PressureAwareCache[*diffValue]).EvaluateMemoryPressure()
				if i == 0 {
					baseEvicted = ev
				} else {
					h.compareEvicted("EvaluateMemoryPressure()", baseEvicted, ev, inst.name, inst.invariants)
				}
			}
		}
		if (op+1)%250 == 0 {
			h.VerifyIterators()
		}
	}

	// Assert
	pressure = 0.10
	h.DrainAndVerifyEvictionOrder(keys)
}

func TestDifferential_CustomWeigherGrowAndShrinkReplace(t *testing.T) {
	// Arrange: Custom key+value weigher exercising shrinking (including to 0), equal, growing, and self-evicting updates.
	r := rand.New(rand.NewPCG(987654321, 0))
	const (
		numOps        = 5000
		cacheCapacity = 300
	)
	h := newDifferentialHarnessWithOpts(
		t,
		cacheCapacity,
		false,
		lru.WithWeigher(func(key string, v *diffValue) uint64 {
			if v == nil {
				return 0
			}
			if v.size == 0 {
				return 0
			}
			return uint64(len(key)) + v.size
		}),
	)

	keys := make([]string, 60)
	for i := range keys {
		keys[i] = fmt.Sprintf("tree/%02d/branch/%02d/leaf_%02d", i%3, (i/3)%4, i)
	}

	// Act
	for op := range numOps {
		k := keys[r.IntN(len(keys))]
		dice := r.IntN(100)
		switch {
		case dice < 35:
			var sz uint64
			switch r.IntN(8) {
			case 0:
				sz = 0
			case 1:
				sz = cacheCapacity + 50
			default:
				sz = uint64(r.IntN(80) + 1)
			}
			h.Put(k, &diffValue{id: fmt.Sprintf("ins_%d", op), size: sz})
		case dice < 50:
			h.Get(k)
		case dice < 60:
			h.Peek(k)
		case dice < 88:
			// Exercise all 4 Replace weight transitions:
			// 0: shrink to 0; 1: shrink to smaller positive; 2: same size; 3: grow within capacity; 4: grow > maxSize.
			var targetSz uint64
			switch r.IntN(5) {
			case 0:
				targetSz = 0
			case 1:
				targetSz = uint64(r.IntN(15) + 1)
			case 2:
				if cur := h.Peek(k); cur != nil {
					targetSz = cur.size
				} else {
					targetSz = 20
				}
			case 3:
				targetSz = uint64(r.IntN(180) + 30)
			default:
				targetSz = cacheCapacity + uint64(r.IntN(100)+1)
			}
			h.Replace(k, &diffValue{id: fmt.Sprintf("upd_%d", op), size: targetSz})
		case dice < 95:
			h.Delete(k)
		default:
			prefix := fmt.Sprintf("tree/%02d/", r.IntN(3))
			h.DeletePrefix(prefix)
		}
		if (op+1)%250 == 0 {
			h.VerifyIterators()
		}
	}

	// Assert
	h.DrainAndVerifyEvictionOrder(keys)
}

type diffEvictEvent struct {
	key    string
	valID  string
	valSz  uint64
	isNil  bool
	reason lru.EvictionReason
}

func TestDifferential_EvictionCallbacksParity(t *testing.T) {
	// Arrange: 6 instances (MapCache, RadixCache, ArenaRadixCache x invariants={false,true})
	// configured with both WithOnEvictValue and WithOnEvictEntry under oscillating memory pressure.
	r := rand.New(rand.NewPCG(20261002, 0))
	const (
		numOps        = 6000
		cacheCapacity = 300
	)
	pressure := 0.10

	configs := []struct {
		name        string
		constructor func(uint64, ...lru.Option) lru.Cache[*diffValue]
	}{
		{"MapCache", lru.NewMapCache[*diffValue]},
		{"RadixCache", lru.NewRadixCache[*diffValue]},
		{"ArenaRadixCache", lru.NewArenaRadixCache[*diffValue]},
	}

	type callbackInstance struct {
		diffInstance
		valEvents   []diffEvictEvent
		entryEvents []diffEvictEvent
		orderCheck  []string
	}

	var instances []*callbackInstance
	h := &differentialHarness{
		t:       t,
		maxSize: cacheCapacity,
	}

	for _, cfg := range configs {
		for _, inv := range []bool{false, true} {
			ci := &callbackInstance{}
			c := cfg.constructor(
				cacheCapacity,
				lru.WithInvariantChecking(inv),
				diffWeigher,
				lru.WithPressureFunc(func() float64 { return pressure }),
				lru.WithCompactionThreshold(0.75),
				lru.WithEvictionThreshold(0.90),
				lru.WithEvictionRetentionRatio(0.50),
				lru.WithOnEvictValue(func(v *diffValue, reason lru.EvictionReason) {
					ev := diffEvictEvent{reason: reason, isNil: v == nil}
					if v != nil {
						ev.valID = v.id
						ev.valSz = v.size
					}
					ci.valEvents = append(ci.valEvents, ev)
					ci.orderCheck = append(ci.orderCheck, "V")
				}),
				lru.WithOnEvictEntry(func(k string, v *diffValue, reason lru.EvictionReason) {
					ev := diffEvictEvent{key: k, reason: reason, isNil: v == nil}
					if v != nil {
						ev.valID = v.id
						ev.valSz = v.size
					}
					ci.entryEvents = append(ci.entryEvents, ev)
					ci.orderCheck = append(ci.orderCheck, "E")
				}),
			)
			ci.diffInstance = diffInstance{
				name:       cfg.name,
				invariants: inv,
				cache:      c,
			}
			instances = append(instances, ci)
			h.instances = append(h.instances, ci.diffInstance)
		}
	}

	verifyStepCallbacks := func(opDesc string, isDeletePrefix bool) {
		t.Helper()
		base := instances[0]
		for idx, ci := range instances {
			require.Lenf(t, ci.valEvents, len(ci.entryEvents), "[%s] valEvents/entryEvents len mismatch on %s (inv=%v)", opDesc, ci.name, ci.invariants)
			require.Lenf(t, ci.orderCheck, 2*len(ci.entryEvents), "[%s] orderCheck len mismatch on %s (inv=%v)", opDesc, ci.name, ci.invariants)
			for j := range ci.entryEvents {
				require.Equalf(t, "V", ci.orderCheck[2*j], "[%s] OnEvictValue must precede OnEvictEntry on %s", opDesc, ci.name)
				require.Equalf(t, "E", ci.orderCheck[2*j+1], "[%s] OnEvictEntry must follow OnEvictValue on %s", opDesc, ci.name)
				expectedValEv := ci.entryEvents[j]
				expectedValEv.key = ""
				require.Equalf(t, expectedValEv, ci.valEvents[j], "[%s] valEvent != entryEvent on %s", opDesc, ci.name)
			}

			if idx > 0 {
				if !isDeletePrefix {
					require.Equalf(t, base.entryEvents, ci.entryEvents, "[%s] entryEvents sequence mismatch on %s (inv=%v)", opDesc, ci.name, ci.invariants)
				} else {
					var baseDeleted, basePressure []diffEvictEvent
					for _, e := range base.entryEvents {
						if e.reason == lru.EvictionReasonDeleted {
							baseDeleted = append(baseDeleted, e)
						} else {
							basePressure = append(basePressure, e)
						}
					}
					var ciDeleted, ciPressure []diffEvictEvent
					for _, e := range ci.entryEvents {
						if e.reason == lru.EvictionReasonDeleted {
							ciDeleted = append(ciDeleted, e)
						} else {
							ciPressure = append(ciPressure, e)
						}
					}
					require.ElementsMatchf(t, baseDeleted, ciDeleted, "[%s] DeletePrefix Deleted multiset mismatch on %s (inv=%v)", opDesc, ci.name, ci.invariants)
					require.Equalf(t, basePressure, ciPressure, "[%s] DeletePrefix Pressure sequence mismatch on %s (inv=%v)", opDesc, ci.name, ci.invariants)
				}
			}
		}
		for _, ci := range instances {
			ci.valEvents = ci.valEvents[:0]
			ci.entryEvents = ci.entryEvents[:0]
			ci.orderCheck = ci.orderCheck[:0]
		}
	}

	keys := make([]string, 64)
	for i := range keys {
		keys[i] = fmt.Sprintf("svc/%02d/mod/%02d/item_%02d", i%4, (i/4)%4, i)
	}
	keys = append(keys, "")

	var reasonTotals [4]int

	// Act
	for op := range numOps {
		switch op % 45 {
		case 0:
			pressure = 0.80
		case 15:
			pressure = 0.95
		case 22:
			pressure = 0.10
		}

		k := keys[r.IntN(len(keys))]
		dice := r.IntN(100)
		isDeletePrefix := false
		switch {
		case dice < 35:
			var sz uint64
			switch r.IntN(8) {
			case 0:
				sz = 0
			case 1:
				sz = cacheCapacity + 20
			default:
				sz = uint64(r.IntN(80) + 1)
			}
			h.Put(k, &diffValue{id: fmt.Sprintf("p_%d", op), size: sz})
		case dice < 50:
			h.Get(k)
		case dice < 60:
			h.Peek(k)
		case dice < 80:
			var sz uint64
			switch r.IntN(5) {
			case 0:
				sz = 0
			case 1:
				sz = uint64(r.IntN(20) + 1)
			case 2:
				sz = uint64(r.IntN(120) + 20)
			default:
				sz = cacheCapacity + 20
			}
			h.Replace(k, &diffValue{id: fmt.Sprintf("r_%d", op), size: sz})
		case dice < 90:
			h.Delete(k)
		case dice < 95:
			isDeletePrefix = true
			if r.IntN(12) == 0 {
				h.DeletePrefix("")
			} else {
				prefix := fmt.Sprintf("svc/%02d/mod/%02d/", r.IntN(4), r.IntN(4))
				h.DeletePrefix(prefix)
			}
		case dice < 97:
			for _, inst := range h.instances {
				inst.cache.(lru.PressureAwareCache[*diffValue]).Compact()
			}
		default:
			var baseEvicted []*diffValue
			for i, inst := range h.instances {
				ev := inst.cache.(lru.PressureAwareCache[*diffValue]).EvaluateMemoryPressure()
				if i == 0 {
					baseEvicted = ev
				} else {
					h.compareEvicted("EvaluateMemoryPressure()", baseEvicted, ev, inst.name, inst.invariants)
				}
			}
		}

		if (op+1)%250 == 0 {
			h.VerifyIterators()
		}
		for _, e := range instances[0].entryEvents {
			if int(e.reason) < len(reasonTotals) {
				reasonTotals[e.reason]++
			}
		}
		verifyStepCallbacks(fmt.Sprintf("op=%d", op), isDeletePrefix)
	}

	// Assert: every single EvictionReason (Capacity, Pressure, Deleted, Replaced) was exercised and verified
	assert.Positive(t, reasonTotals[lru.EvictionReasonCapacity], "EvictionReasonCapacity should be exercised")
	assert.Positive(t, reasonTotals[lru.EvictionReasonPressure], "EvictionReasonPressure should be exercised")
	assert.Positive(t, reasonTotals[lru.EvictionReasonDeleted], "EvictionReasonDeleted should be exercised")
	assert.Positive(t, reasonTotals[lru.EvictionReasonReplaced], "EvictionReasonReplaced should be exercised")

	pressure = 0.10
	h.DrainAndVerifyEvictionOrder(keys)
}

func TestDifferential_IteratorsParity(t *testing.T) {
	// Arrange
	pressure := 0.10
	h := newDifferentialHarness(
		t,
		100,
		lru.WithPressureFunc(func() float64 { return pressure }),
		lru.WithCompactionThreshold(0.75),
		lru.WithEvictionThreshold(0.90),
		lru.WithEvictionRetentionRatio(0.50),
	)

	// 1. Empty cache state: All, Keys, Values yield nothing; early break is a no-op.
	h.VerifyIterators()
	for _, inst := range h.instances {
		require.Nil(t, slices.Collect(inst.cache.Keys()))
		require.Nil(t, slices.Collect(inst.cache.Values()))
	}

	// 2. Single-entry states: empty root key "" and nil *diffValue.
	h.Put("", &diffValue{id: "root_val", size: 10})
	h.VerifyIterators()
	require.Equal(t, []string{""}, slices.Collect(h.instances[0].cache.Keys()))

	h.Replace("", nil)
	h.VerifyIterators()
	require.Equal(t, []*diffValue{nil}, slices.Collect(h.instances[0].cache.Values()))

	h.Delete("")
	h.VerifyIterators()
	require.Empty(t, slices.Collect(h.instances[0].cache.Keys()))

	// 3. Multi-entry hierarchical keys with Get, Peek, Replace, Delete, DeletePrefix, Compact, and EvaluateMemoryPressure.
	allKeys := []string{
		"",
		"dir/a/file1",
		"dir/a/file2",
		"dir/b/sub/file3",
		"dir/b/sub/deep/leaf/file4",
		"other/item",
	}
	for i, k := range allKeys {
		h.Put(k, &diffValue{id: fmt.Sprintf("v_%d", i), size: 10})
		h.VerifyIterators()
	}

	// Expected MRU-to-LRU order after sequential Put: reverse of allKeys.
	expectedAfterPut := slices.Clone(allKeys)
	slices.Reverse(expectedAfterPut)
	require.Equal(t, expectedAfterPut, slices.Collect(h.instances[0].cache.Keys()))

	// Peek does not alter recency order.
	h.Peek("dir/a/file1")
	h.Peek("")
	h.VerifyIterators()
	require.Equal(t, expectedAfterPut, slices.Collect(h.instances[0].cache.Keys()))

	// Get promotes accessed entries to MRU head.
	h.Get("dir/a/file1")
	h.Get("")
	h.VerifyIterators()
	require.Equal(t, []string{
		"",
		"dir/a/file1",
		"other/item",
		"dir/b/sub/deep/leaf/file4",
		"dir/b/sub/file3",
		"dir/a/file2",
	}, slices.Collect(h.instances[0].cache.Keys()))

	// Replace in-place updates value/size without altering recency order.
	h.Replace("other/item", &diffValue{id: "v_other_updated", size: 15})
	h.VerifyIterators()
	require.Equal(t, []string{
		"",
		"dir/a/file1",
		"other/item",
		"dir/b/sub/deep/leaf/file4",
		"dir/b/sub/file3",
		"dir/a/file2",
	}, slices.Collect(h.instances[0].cache.Keys()))

	// Replace with size > maxSize self-evicts only the target entry.
	h.Replace("dir/b/sub/file3", &diffValue{id: "too_big", size: 200})
	h.VerifyIterators()
	require.Equal(t, []string{
		"",
		"dir/a/file1",
		"other/item",
		"dir/b/sub/deep/leaf/file4",
		"dir/a/file2",
	}, slices.Collect(h.instances[0].cache.Keys()))

	// Delete removes specific key.
	h.Delete("")
	h.VerifyIterators()
	require.Equal(t, []string{
		"dir/a/file1",
		"other/item",
		"dir/b/sub/deep/leaf/file4",
		"dir/a/file2",
	}, slices.Collect(h.instances[0].cache.Keys()))

	// DeletePrefix removes "dir/a/" subtree while preserving relative MRU-to-LRU order of survivors.
	h.DeletePrefix("dir/a/")
	h.VerifyIterators()
	require.Equal(t, []string{
		"other/item",
		"dir/b/sub/deep/leaf/file4",
	}, slices.Collect(h.instances[0].cache.Keys()))

	// Repopulate and exercise Compact() and EvaluateMemoryPressure().
	h.Put("dir/c/item1", &diffValue{id: "c1", size: 20})
	h.Put("dir/c/item2", &diffValue{id: "c2", size: 20})
	for _, inst := range h.instances {
		inst.cache.(lru.PressureAwareCache[*diffValue]).Compact()
	}
	h.VerifyIterators()

	// Tier 2 critical pressure sheds from LRU tail until <= 50 bytes.
	pressure = 0.95
	for _, inst := range h.instances {
		_ = inst.cache.(lru.PressureAwareCache[*diffValue]).EvaluateMemoryPressure()
	}
	pressure = 0.10
	h.VerifyIterators()

	// 4. Verify DrainAndVerifyEvictionOrder (including reverse Values() == eviction order).
	h.DrainAndVerifyEvictionOrder(append(allKeys, "dir/c/item1", "dir/c/item2"))
}

func TestDifferential_StatsFullLifecycleParity(t *testing.T) {
	// Arrange
	pressure := 0.10
	h := newDifferentialHarness(
		t,
		100,
		lru.WithPressureFunc(func() float64 { return pressure }),
		lru.WithCompactionThreshold(0.75),
		lru.WithEvictionThreshold(0.90),
		lru.WithEvictionRetentionRatio(0.50),
	)

	// 1. Initial empty state
	h.VerifyStatsParity("initial")
	st := h.instances[0].cache.Stats()
	assert.Equal(t, uint64(100), st.MaxSize)
	assert.Zero(t, st.CurrentSize)
	assert.Zero(t, st.Len)
	assert.Zero(t, st.ZeroSizeCount)

	// 2. Put new entries (including a zero-weight entry)
	h.Put("dir/a", &diffValue{id: "a", size: 20})
	h.Put("dir/b", &diffValue{id: "b0", size: 0})
	h.Put("dir/c", &diffValue{id: "c", size: 30})
	h.VerifyStatsParity("after_initial_puts")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(3), st.PutInserted)
	assert.Equal(t, 3, st.Len)
	assert.Equal(t, 1, st.ZeroSizeCount)
	assert.Equal(t, uint64(50), st.CurrentSize)

	// 3. Put updates (zero -> non-zero -> zero weight transitions)
	h.Put("dir/b", &diffValue{id: "b10", size: 10})
	h.VerifyStatsParity("after_put_update_nonzero")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(1), st.PutUpdated)
	assert.Zero(t, st.ZeroSizeCount)
	assert.Equal(t, uint64(60), st.CurrentSize)
	assert.Equal(t, uint64(1), st.EvictionsReplaced)
	assert.Zero(t, st.EvictedWeightReplaced)

	h.Put("dir/b", &diffValue{id: "b0_again", size: 0})
	h.VerifyStatsParity("after_put_update_zero")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(2), st.PutUpdated)
	assert.Equal(t, 1, st.ZeroSizeCount)
	assert.Equal(t, uint64(50), st.CurrentSize)
	assert.Equal(t, uint64(2), st.EvictionsReplaced)
	assert.Equal(t, uint64(10), st.EvictedWeightReplaced)

	// 4. Put rejected oversized (> maxSize)
	h.Put("oversized", &diffValue{id: "big", size: 101})
	h.VerifyStatsParity("after_put_rejected_oversized")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(1), st.PutRejectedOversized)
	assert.Equal(t, 3, st.Len)

	// 5. Put triggering capacity eviction (evicts LRU "dir/a" of size 20)
	h.Put("dir/d", &diffValue{id: "d", size: 60})
	h.VerifyStatsParity("after_put_capacity_eviction")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(4), st.PutInserted)
	assert.Equal(t, uint64(1), st.EvictionsCapacity)
	assert.Equal(t, uint64(20), st.EvictedWeightCapacity)
	assert.Equal(t, uint64(90), st.CurrentSize)
	assert.Equal(t, 3, st.Len)

	// 6. Get and Peek hits and misses
	h.Get("dir/c")
	h.Get("missing")
	h.Peek("dir/d")
	h.Peek("missing")
	h.VerifyStatsParity("after_get_peek")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(1), st.GetHits)
	assert.Equal(t, uint64(1), st.GetMisses)
	assert.Equal(t, uint64(1), st.PeekHits)
	assert.Equal(t, uint64(1), st.PeekMisses)

	// 7. Replace: not found, shrink, grow with older eviction, self-eviction (> maxSize and !canFit)
	h.Replace("missing", &diffValue{id: "m", size: 10})
	h.Replace("dir/d", &diffValue{id: "d50", size: 50})
	h.Replace("dir/c", &diffValue{id: "c60", size: 60}) // evicts older "dir/b" (0) and "dir/d" (50)
	h.Replace("dir/c", &diffValue{id: "c150", size: 150})
	h.VerifyStatsParity("after_replace_self_evict_max")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(1), st.ReplaceNotFound)
	assert.Equal(t, uint64(2), st.ReplaceUpdated)
	assert.Equal(t, uint64(1), st.ReplaceSelfEvicted)
	assert.Zero(t, st.Len)
	assert.Zero(t, st.CurrentSize)

	// Replace !canFit self-eviction (LRU entry grows and cannot fit alongside newer entry)
	h.Put("k1", &diffValue{id: "k1", size: 40})
	h.Put("k2", &diffValue{id: "k2", size: 40})
	h.Replace("k1", &diffValue{id: "k1_grow", size: 70})
	h.VerifyStatsParity("after_replace_self_evict_canfit")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(2), st.ReplaceSelfEvicted)
	assert.Equal(t, 1, st.Len)
	assert.Equal(t, uint64(40), st.CurrentSize)

	// 8. Delete hit and miss, then DeletePrefix
	h.Delete("missing")
	h.Delete("k2")
	h.Put("p/1", &diffValue{id: "p1", size: 10})
	h.Put("p/2", &diffValue{id: "p2", size: 15})
	h.Put("q/1", &diffValue{id: "q1", size: 20})
	h.DeletePrefix("p/")
	h.DeletePrefix("nonexistent/")
	h.VerifyStatsParity("after_delete_and_prefix")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(1), st.DeleteNotFound)
	assert.Equal(t, uint64(1), st.DeleteDeleted)
	assert.Equal(t, uint64(2), st.DeletePrefixExecuted)
	assert.Equal(t, uint64(3), st.EvictionsDeleted)
	assert.Equal(t, uint64(65), st.EvictedWeightDeleted)

	// 9. Explicit Compact() on dirty state followed by clean no-op Compact()
	for _, inst := range h.instances {
		inst.cache.(lru.PressureAwareCache[*diffValue]).Compact()
	}
	h.VerifyFullCompactionStatsParity("after_explicit_compact_dirty")
	assert.Equal(t, uint64(1), h.instances[0].cache.Stats().CompactionsExplicit)
	assert.Zero(t, h.instances[0].cache.Stats().DeletedSinceCompact)

	for _, inst := range h.instances {
		inst.cache.(lru.PressureAwareCache[*diffValue]).Compact()
	}
	h.VerifyFullCompactionStatsParity("after_explicit_compact_clean_noop")
	assert.Equal(t, uint64(1), h.instances[0].cache.Stats().CompactionsExplicit)

	// 10. EvaluateMemoryPressure: Tier 1 compaction and Tier 2 explicit shed + compaction
	h.Put("m/1", &diffValue{id: "m1", size: 30})
	h.Put("m/2", &diffValue{id: "m2", size: 20})
	h.Put("m/3", &diffValue{id: "m3", size: 20})
	h.Delete("m/3") // all backends now have deletion slack and CurrentSize == 70 ("q/1"=20, "m/1"=30, "m/2"=20)
	assert.Equal(t, 1, h.instances[0].cache.Stats().DeletedSinceCompact)
	assert.Equal(t, 4, h.instances[0].cache.Stats().PeakEntryLen)

	pressure = 0.80
	for _, inst := range h.instances {
		ev := inst.cache.(lru.PressureAwareCache[*diffValue]).EvaluateMemoryPressure()
		require.Empty(t, ev)
	}
	h.VerifyFullCompactionStatsParity("after_tier1_pressure_eval")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(1), st.CompactionsPressureTier1)
	assert.InDelta(t, 0.80, st.MemoryPressure, 1e-9)

	pressure = 0.95
	for _, inst := range h.instances {
		ev := inst.cache.(lru.PressureAwareCache[*diffValue]).EvaluateMemoryPressure()
		require.Len(t, ev, 1)
	}
	h.VerifyFullCompactionStatsParity("after_tier2_pressure_eval")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(1), st.PressureShedsExplicit)
	assert.Equal(t, uint64(1), st.CompactionsPressureTier2)
	assert.Equal(t, uint64(1), st.EvictionsPressure)
	assert.Equal(t, uint64(20), st.EvictedWeightPressure)
	assert.InDelta(t, 0.95, st.MemoryPressure, 1e-9)

	// 11. Inline Tier 2 pressure shedding during Put (inserting 15 brings CurrentSize from 50 to 65 > 50,
	// shedding "m/1" of size 30 so post-shed CurrentSize == 35 < sizeBefore 50, triggering inline Tier 2 compaction!)
	h.Put("m/4", &diffValue{id: "m4", size: 15})
	h.VerifyFullCompactionStatsParity("after_inline_tier2_shed")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, uint64(1), st.PressureShedsInline)
	assert.Equal(t, uint64(2), st.CompactionsPressureTier2)
	assert.Equal(t, uint64(2), st.EvictionsPressure)
	assert.Equal(t, uint64(50), st.EvictedWeightPressure)

	// 12. Auto-slack compaction on callback-free DeletePrefix("") fast path when > 64 entries are removed
	pressure = 0.10
	for _, inst := range h.instances {
		_ = inst.cache.(lru.PressureAwareCache[*diffValue]).EvaluateMemoryPressure()
	}
	h.DeletePrefix("")
	for _, inst := range h.instances {
		inst.cache.(lru.PressureAwareCache[*diffValue]).Compact()
	}
	baseAutoSlack := h.instances[0].cache.Stats().CompactionsAutoSlack
	for i := range 70 {
		h.Put(fmt.Sprintf("slack/item_%03d", i), &diffValue{id: "s", size: 1})
	}
	h.DeletePrefix("")
	h.VerifyFullCompactionStatsParity("after_delete_prefix_all_auto_slack")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, baseAutoSlack+1, st.CompactionsAutoSlack)
	assert.Zero(t, st.Len)
	assert.Zero(t, st.CurrentSize)

	// 13. Auto-slack compaction on DeletePrefix("") when peakEntryLen <= 8 and len(nodes) < 64,
	// crossing deletedSinceCompact >= 64 via the deleted live entries.
	h.Put("anchor/1", &diffValue{id: "a1", size: 1})
	h.Put("anchor/2", &diffValue{id: "a2", size: 1})
	h.Put("anchor/3", &diffValue{id: "a3", size: 1})
	h.Put("anchor/4", &diffValue{id: "a4", size: 1})
	for range 60 {
		h.Put("churn/k", &diffValue{id: "ck", size: 1})
		h.Delete("churn/k")
	}
	h.Put("tail/1", &diffValue{id: "t1", size: 1})
	h.Put("tail/2", &diffValue{id: "t2", size: 1})
	h.Put("tail/3", &diffValue{id: "t3", size: 1})
	h.VerifyFullCompactionStatsParity("before_delete_prefix_all_small_peak")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, 7, st.PeakEntryLen)
	assert.Equal(t, 60, st.DeletedSinceCompact)
	baseAutoSlack = st.CompactionsAutoSlack

	h.DeletePrefix("")
	h.VerifyFullCompactionStatsParity("after_delete_prefix_all_small_peak")
	st = h.instances[0].cache.Stats()
	assert.Equal(t, baseAutoSlack+1, st.CompactionsAutoSlack)
	assert.Zero(t, st.Len)
	assert.Zero(t, st.DeletedSinceCompact)
	assert.Zero(t, st.PeakEntryLen)
}
