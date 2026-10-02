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
	"math/rand"
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
	allOpts := make([]lru.Option, 0, len(opts)+1)
	allOpts = append(allOpts, diffWeigher)
	allOpts = append(allOpts, opts...)
	return newDifferentialHarnessWithOpts(t, maxSize, false, allOpts...)
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

	for _, cfg := range configs {
		for _, inv := range []bool{false, true} {
			instanceOpts := make([]lru.Option, 0, len(opts)+1)
			instanceOpts = append(instanceOpts, lru.WithInvariantChecking(inv))
			instanceOpts = append(instanceOpts, opts...)
			c := cfg.constructor(maxSize, instanceOpts...)
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

func (h *differentialHarness) Insert(key string, val *diffValue) []*diffValue {
	h.t.Helper()
	var baseEvicted []*diffValue
	var baseErr error

	for i, inst := range h.instances {
		evicted, err := inst.cache.Insert(key, val)
		if i == 0 {
			baseEvicted = evicted
			baseErr = err
		} else {
			h.compareErrors(fmt.Sprintf("Insert(%q)", key), baseErr, err, inst.name, inst.invariants)
			h.compareEvicted(fmt.Sprintf("Insert(%q)", key), baseEvicted, evicted, inst.name, inst.invariants)
		}
	}
	return baseEvicted
}

func (h *differentialHarness) Erase(key string) *diffValue {
	h.t.Helper()
	var baseVal *diffValue
	var baseOK bool

	for i, inst := range h.instances {
		val, ok := inst.cache.Erase(key)
		if i == 0 {
			baseVal = val
			baseOK = ok
		} else {
			h.compareValues(fmt.Sprintf("Erase(%q)", key), baseVal, baseOK, val, ok, inst.name, inst.invariants)
		}
	}
	return baseVal
}

func (h *differentialHarness) LookUp(key string) *diffValue {
	h.t.Helper()
	var baseVal *diffValue
	var baseOK bool

	for i, inst := range h.instances {
		val, ok := inst.cache.LookUp(key)
		if i == 0 {
			baseVal = val
			baseOK = ok
		} else {
			h.compareValues(fmt.Sprintf("LookUp(%q)", key), baseVal, baseOK, val, ok, inst.name, inst.invariants)
		}
	}
	return baseVal
}

func (h *differentialHarness) LookUpWithoutChangingOrder(key string) *diffValue {
	h.t.Helper()
	var baseVal *diffValue
	var baseOK bool

	for i, inst := range h.instances {
		val, ok := inst.cache.LookUpWithoutChangingOrder(key)
		if i == 0 {
			baseVal = val
			baseOK = ok
		} else {
			h.compareValues(fmt.Sprintf("LookUpWithoutChangingOrder(%q)", key), baseVal, baseOK, val, ok, inst.name, inst.invariants)
		}
	}
	return baseVal
}

func (h *differentialHarness) UpdateWithoutChangingOrder(key string, val *diffValue) {
	h.t.Helper()
	var baseErr error

	for i, inst := range h.instances {
		err := inst.cache.UpdateWithoutChangingOrder(key, val)
		if i == 0 {
			baseErr = err
		} else {
			h.compareErrors(fmt.Sprintf("UpdateWithoutChangingOrder(%q)", key), baseErr, err, inst.name, inst.invariants)
		}
	}
}

func (h *differentialHarness) EraseEntriesWithGivenPrefix(prefix string) {
	h.t.Helper()
	for _, inst := range h.instances {
		inst.cache.EraseEntriesWithGivenPrefix(prefix)
	}
}

func (h *differentialHarness) DrainAndVerifyEvictionOrder(allKnownKeys []string) {
	h.t.Helper()
	for _, k := range allKnownKeys {
		h.LookUpWithoutChangingOrder(k)
	}
	if h.unitWeight {
		for i := uint64(0); i < h.maxSize; i++ {
			drainKey := fmt.Sprintf("__DRAIN_KEY_%d__", i)
			h.Insert(drainKey, &diffValue{id: fmt.Sprintf("__DRAIN_ITEM_%d__", i), size: 1})
		}
		return
	}
	drainVal := &diffValue{id: "__DRAIN_ITEM__", size: h.maxSize}
	h.Insert("__DRAIN_KEY__", drainVal)
}

func TestDifferential_FlatWorkload(t *testing.T) {
	// Arrange
	r := rand.New(rand.NewSource(1337))
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
		k := keys[r.Intn(keyPoolSize)]
		dice := r.Intn(100)

		switch {
		case dice < 35:
			sz := uint64(r.Intn(50) + 1)
			h.Insert(k, &diffValue{id: fmt.Sprintf("%s_v%d", k, op), size: sz})
		case dice < 60:
			h.LookUp(k)
		case dice < 75:
			h.LookUpWithoutChangingOrder(k)
		case dice < 85:
			existing := h.LookUpWithoutChangingOrder(k)
			if existing != nil {
				h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("%s_upd_%d", k, op), size: existing.weight()})
			} else {
				h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("%s_upd_%d", k, op), size: 10})
			}
		case dice < 90:
			newSz := uint64(r.Intn(80))
			h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("%s_szupd_%d", k, op), size: newSz})
		case dice < 95:
			h.Erase(k)
		default:
			prefix := fmt.Sprintf("flat_key_%02d", r.Intn(20))
			h.EraseEntriesWithGivenPrefix(prefix)
		}
	}

	// Assert
	h.DrainAndVerifyEvictionOrder(keys)
}

func TestDifferential_HierarchicalDirectoryWorkload(t *testing.T) {
	// Arrange
	r := rand.New(rand.NewSource(4242))
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
		path := paths[r.Intn(len(paths))]
		dice := r.Intn(100)

		switch {
		case dice < 30:
			sz := uint64(r.Intn(80) + 1)
			h.Insert(path, &diffValue{id: fmt.Sprintf("file_%d", op), size: sz})
		case dice < 55:
			h.LookUp(path)
		case dice < 70:
			h.LookUpWithoutChangingOrder(path)
		case dice < 80:
			existing := h.LookUpWithoutChangingOrder(path)
			if existing != nil {
				h.UpdateWithoutChangingOrder(path, &diffValue{id: fmt.Sprintf("upd_%d", op), size: existing.weight()})
			}
		case dice < 85:
			newSz := uint64(r.Intn(120))
			h.UpdateWithoutChangingOrder(path, &diffValue{id: fmt.Sprintf("szupd_%d", op), size: newSz})
		case dice < 90:
			h.Erase(path)
		default:
			prefix := fmt.Sprintf("%s/%s/", topDirs[r.Intn(len(topDirs))], subDirs[r.Intn(len(subDirs))])
			h.EraseEntriesWithGivenPrefix(prefix)
		}
	}

	// Assert
	h.DrainAndVerifyEvictionOrder(paths)
}

func TestDifferential_CapacityThrashingAndSizeUpdates(t *testing.T) {
	// Arrange
	r := rand.New(rand.NewSource(7777))
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
	h.Insert("overflow_key", &diffValue{id: "ovf", size: 20})
	h.UpdateWithoutChangingOrder("overflow_key", &diffValue{id: "ovf_max", size: math.MaxUint64})
	h.UpdateWithoutChangingOrder("overflow_key", &diffValue{id: "ovf_miss", size: math.MaxUint64 - 10})

	for op := range numOps {
		k := keys[r.Intn(len(keys))]
		dice := r.Intn(100)

		switch {
		case dice < 40:
			sz := uint64(r.Intn(170) + 10)
			h.Insert(k, &diffValue{id: fmt.Sprintf("thrash_v%d", op), size: sz})
		case dice < 60:
			h.LookUp(k)
		case dice < 75:
			h.LookUpWithoutChangingOrder(k)
		case dice < 85:
			newSz := uint64(r.Intn(230))
			h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("thrash_upd_%d", op), size: newSz})
		case dice < 95:
			h.Erase(k)
		default:
			h.EraseEntriesWithGivenPrefix("thrash_")
		}
	}

	// Assert
	h.DrainAndVerifyEvictionOrder(keys)
}

func TestDifferential_BoundaryAndEdgeCases(t *testing.T) {
	// Arrange
	r := rand.New(rand.NewSource(12345))
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
		k := adversarialKeys[r.Intn(len(adversarialKeys))]
		dice := r.Intn(100)

		switch {
		case dice < 30:
			szRoll := r.Intn(10)
			var sz uint64
			switch szRoll {
			case 0:
				sz = 0
			case 1:
				sz = cacheCapacity + 100
			default:
				sz = uint64(r.Intn(100) + 1)
			}
			h.Insert(k, &diffValue{id: fmt.Sprintf("adv_%d", op), size: sz})
		case dice < 35:
			h.Insert(k, nil)
		case dice < 55:
			h.LookUp(k)
		case dice < 70:
			h.LookUpWithoutChangingOrder(k)
		case dice < 80:
			existing := h.LookUpWithoutChangingOrder(k)
			if existing != nil {
				switch r.Intn(3) {
				case 0:
					h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("upd_%d", op), size: existing.weight()})
				case 1:
					h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("grow_%d", op), size: existing.weight() + 5})
				default:
					var shrunk uint64
					if existing.weight() > 5 {
						shrunk = existing.weight() - 5
					}
					h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("shrink_%d", op), size: shrunk})
				}
			} else {
				h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("noent_%d", op), size: 10})
			}
		case dice < 88:
			h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("rand_sz_%d", op), size: uint64(r.Intn(150))})
		case dice < 94:
			h.Erase(k)
		default:
			prefix := adversarialKeys[r.Intn(len(adversarialKeys))]
			h.EraseEntriesWithGivenPrefix(prefix)
		}
	}

	// Assert
	h.DrainAndVerifyEvictionOrder(adversarialKeys)
}

func TestDifferential_UpdateWithoutChangingOrderSelfEvictionPreservesPreviousValue(t *testing.T) {
	// Arrange
	h := newDifferentialHarness(t, 100)
	dv := &diffValue{id: "self_evict", size: 50}
	h.Insert("k1", dv)

	// Act: Update k1 to a new value of size 110 (> maxSize 100), triggering self-eviction across all backends.
	h.UpdateWithoutChangingOrder("k1", &diffValue{id: "self_evict_2", size: 110})

	// Assert: k1 is evicted and original dv.size remains 50.
	require.Nil(t, h.LookUpWithoutChangingOrder("k1"))
	require.Equal(t, uint64(50), dv.weight())
}

func TestDifferential_PressureAwareAndCompactionParity(t *testing.T) {
	// Arrange
	r := rand.New(rand.NewSource(20260925))
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

		k := keys[r.Intn(len(keys))]
		dice := r.Intn(100)
		switch {
		case dice < 35:
			sz := uint64(r.Intn(45) + 5)
			h.Insert(k, &diffValue{id: fmt.Sprintf("pv_%d", op), size: sz})
		case dice < 55:
			h.LookUp(k)
		case dice < 68:
			h.LookUpWithoutChangingOrder(k)
		case dice < 78:
			h.Erase(k)
		case dice < 86:
			newSz := uint64(r.Intn(60))
			h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("pu_%d", op), size: newSz})
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
	}

	// Assert
	pressure = 0.10
	h.DrainAndVerifyEvictionOrder(keys)
}

func TestDifferential_UpdateWithoutChangingOrderSingleEntryExceedsMaxSizePreservesOtherEntries(t *testing.T) {
	// Arrange
	h := newDifferentialHarness(t, 100)
	h.Insert("k1", &diffValue{id: "v1", size: 20})
	h.Insert("k2", &diffValue{id: "v2", size: 20})
	h.Insert("k3", &diffValue{id: "v3", size: 30})

	// Act: grow k3 from 30 to 110 (> maxSize 100). Only k3 should be evicted.
	h.UpdateWithoutChangingOrder("k3", &diffValue{id: "v3_big", size: 110})

	// Assert
	assert.Nil(t, h.LookUpWithoutChangingOrder("k3"), "k3 exceeds maxSize and must be evicted")
	assert.NotNil(t, h.LookUpWithoutChangingOrder("k1"), "k1 must not be collateral-evicted")
	assert.NotNil(t, h.LookUpWithoutChangingOrder("k2"), "k2 must not be collateral-evicted")
}

func TestDifferential_InsertNearMaxUint64EvictsWithoutOverflow(t *testing.T) {
	// Arrange
	const maxCap = uint64(math.MaxUint64)
	const halfPlus = uint64(math.MaxUint64/2) + 100
	h := newDifferentialHarness(t, maxCap)

	h.Insert("k1", &diffValue{id: "v1", size: halfPlus})

	// Act: inserting k2 of size halfPlus would overflow uint64 if added before evicting k1.
	evicted := h.Insert("k2", &diffValue{id: "v2", size: halfPlus})

	// Assert
	require.Len(t, evicted, 1)
	assert.Nil(t, h.LookUpWithoutChangingOrder("k1"))
	assert.NotNil(t, h.LookUpWithoutChangingOrder("k2"))

	// Subsequent Erase must not underflow currentSize or panic.
	erased := h.Erase("k2")
	require.NotNil(t, erased)
}

func TestDifferential_UpdateWithoutChangingOrderMiddleEntrySelfEvictionAndMaxUint64(t *testing.T) {
	// Arrange 1: k1 (10B, LRU), k2 (20B, mid), k3 (20B, MRU) in maxSize = 50.
	h := newDifferentialHarness(t, 50)
	h.Insert("k1", &diffValue{id: "v1", size: 10})
	h.Insert("k2", &diffValue{id: "v2", size: 20})
	h.Insert("k3", &diffValue{id: "v3", size: 20})

	// Act 1: Grow k2 from 20 to 40. Since k2 (40) + k3 (20) = 60 > 50, k2 cannot fit alongside newer k3.
	h.UpdateWithoutChangingOrder("k2", &diffValue{id: "v2_grow", size: 40})

	// Assert 1: k2 is self-evicted while older entry k1 (10B) and newer entry k3 (20B) both survive.
	assert.Nil(t, h.LookUpWithoutChangingOrder("k2"))
	assert.NotNil(t, h.LookUpWithoutChangingOrder("k1"))
	assert.NotNil(t, h.LookUpWithoutChangingOrder("k3"))

	// Arrange 2: Near math.MaxUint64, k1 (MaxUint64 - 50) + k2 (50) == MaxUint64.
	maxH := newDifferentialHarness(t, math.MaxUint64)
	maxH.Insert("k1", &diffValue{id: "v1", size: math.MaxUint64 - 50})
	maxH.Insert("k2", &diffValue{id: "v2", size: 50})

	// Act 2: Grow k2 from 50 to 150. Evicting k1 first prevents uint64 overflow.
	maxH.UpdateWithoutChangingOrder("k2", &diffValue{id: "v2_grow", size: 150})

	// Assert 2
	assert.Nil(t, maxH.LookUpWithoutChangingOrder("k1"))
	assert.NotNil(t, maxH.LookUpWithoutChangingOrder("k2"))
}

func TestDifferential_UpdateWithoutChangingOrderLockstepParity(t *testing.T) {
	// Updating the MRU head when it requires evicting multiple older entries
	h := newDifferentialHarness(t, 100)
	h.Insert("k1", &diffValue{id: "v1", size: 10})
	h.Insert("k2", &diffValue{id: "v2", size: 20})
	h.Insert("k3", &diffValue{id: "v3", size: 30})
	h.Insert("k4", &diffValue{id: "v4", size: 30})

	// cache: k1 (10), k2 (20), k3 (30), k4 (30) -> grow k4 to 70
	h.UpdateWithoutChangingOrder("k4", &diffValue{id: "v4_grow", size: 70})
	assert.Nil(t, h.LookUpWithoutChangingOrder("k1"))
	assert.Nil(t, h.LookUpWithoutChangingOrder("k2"))
	assert.NotNil(t, h.LookUpWithoutChangingOrder("k3"))
	assert.NotNil(t, h.LookUpWithoutChangingOrder("k4"))

	// Updating the second-most-recent entry (head.next) when head.size + newSize > maxSize (self-evicts)
	h2 := newDifferentialHarness(t, 100)
	h2.Insert("k1", &diffValue{id: "v1", size: 10})
	h2.Insert("k2", &diffValue{id: "v2", size: 20})
	h2.Insert("k3", &diffValue{id: "v3", size: 30}) // head.next
	h2.Insert("k4", &diffValue{id: "v4", size: 40}) // head
	h2.UpdateWithoutChangingOrder("k3", &diffValue{id: "v3_grow", size: 65})
	assert.Nil(t, h2.LookUpWithoutChangingOrder("k3")) // self-evicted
	assert.NotNil(t, h2.LookUpWithoutChangingOrder("k1"))
	assert.NotNil(t, h2.LookUpWithoutChangingOrder("k2"))
	assert.NotNil(t, h2.LookUpWithoutChangingOrder("k4"))

	// Updating the second-most-recent entry (head.next) when head.size + newSize <= maxSize (evicts older)
	h3 := newDifferentialHarness(t, 100)
	h3.Insert("k1", &diffValue{id: "v1", size: 10})
	h3.Insert("k2", &diffValue{id: "v2", size: 20})
	h3.Insert("k3", &diffValue{id: "v3", size: 30}) // head.next
	h3.Insert("k4", &diffValue{id: "v4", size: 30}) // head
	h3.UpdateWithoutChangingOrder("k3", &diffValue{id: "v3_grow", size: 60})
	assert.Nil(t, h3.LookUpWithoutChangingOrder("k1"))
	assert.Nil(t, h3.LookUpWithoutChangingOrder("k2"))
	assert.NotNil(t, h3.LookUpWithoutChangingOrder("k3"))
	assert.NotNil(t, h3.LookUpWithoutChangingOrder("k4"))

	// Updating the LRU tail entry when it fits (sizeDelta <= maxSize - currentSize)
	h4 := newDifferentialHarness(t, 100)
	h4.Insert("k1", &diffValue{id: "v1", size: 10}) // tail
	h4.Insert("k2", &diffValue{id: "v2", size: 50}) // head
	h4.UpdateWithoutChangingOrder("k1", &diffValue{id: "v1_grow", size: 40})
	assert.NotNil(t, h4.LookUpWithoutChangingOrder("k1"))
	assert.NotNil(t, h4.LookUpWithoutChangingOrder("k2"))

	// Updating the LRU tail entry when it self-evicts (sizeDelta > maxSize - currentSize)
	h5 := newDifferentialHarness(t, 100)
	h5.Insert("k1", &diffValue{id: "v1", size: 10}) // tail
	h5.Insert("k2", &diffValue{id: "v2", size: 80}) // head
	h5.UpdateWithoutChangingOrder("k1", &diffValue{id: "v1_grow", size: 30})
	assert.Nil(t, h5.LookUpWithoutChangingOrder("k1"))
	assert.NotNil(t, h5.LookUpWithoutChangingOrder("k2"))

	// Shrinking an entry's weight in-place frees capacity for subsequent inserts without changing LRU order
	h6 := newDifferentialHarness(t, 100)
	h6.Insert("k1", &diffValue{id: "v1", size: 50}) // tail
	h6.Insert("k2", &diffValue{id: "v2", size: 50}) // head
	h6.UpdateWithoutChangingOrder("k1", &diffValue{id: "v1_shrunk", size: 10})
	ev := h6.Insert("k3", &diffValue{id: "v3", size: 40})
	assert.Empty(t, ev)
	assert.NotNil(t, h6.LookUpWithoutChangingOrder("k1"))
	assert.NotNil(t, h6.LookUpWithoutChangingOrder("k2"))
	assert.NotNil(t, h6.LookUpWithoutChangingOrder("k3"))
}

func TestDifferential_DefaultWeigherWorkload(t *testing.T) {
	// Arrange: 6 instances (MapCache, RadixCache, ArenaRadixCache x invariants={false,true})
	// configured WITHOUT WithWeigher so every entry has default weight 1.
	r := rand.New(rand.NewSource(20260930))
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

		k := keys[r.Intn(len(keys))]
		dice := r.Intn(100)
		switch {
		case dice < 35:
			if r.Intn(10) == 0 {
				h.Insert(k, nil) // nil pointer value still weighs 1 under default weigher
			} else {
				h.Insert(k, &diffValue{id: fmt.Sprintf("def_%d", op), size: uint64(r.Intn(10000))})
			}
		case dice < 55:
			h.LookUp(k)
		case dice < 70:
			h.LookUpWithoutChangingOrder(k)
		case dice < 82:
			h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("def_upd_%d", op), size: uint64(r.Intn(10000))})
		case dice < 90:
			h.Erase(k)
		case dice < 95:
			prefix := fmt.Sprintf("ns_%d/dir_%d/", r.Intn(4), r.Intn(5))
			h.EraseEntriesWithGivenPrefix(prefix)
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
	}

	// Assert
	pressure = 0.10
	h.DrainAndVerifyEvictionOrder(keys)
}

func TestDifferential_CustomWeigherGrowAndShrinkUpdateWithoutChangingOrder(t *testing.T) {
	// Arrange: Custom key+value weigher exercising shrinking (including to 0), equal, growing, and self-evicting updates.
	r := rand.New(rand.NewSource(987654321))
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
		k := keys[r.Intn(len(keys))]
		dice := r.Intn(100)
		switch {
		case dice < 35:
			var sz uint64
			switch r.Intn(8) {
			case 0:
				sz = 0
			case 1:
				sz = cacheCapacity + 50
			default:
				sz = uint64(r.Intn(80) + 1)
			}
			h.Insert(k, &diffValue{id: fmt.Sprintf("ins_%d", op), size: sz})
		case dice < 50:
			h.LookUp(k)
		case dice < 60:
			h.LookUpWithoutChangingOrder(k)
		case dice < 88:
			// Exercise all 4 UpdateWithoutChangingOrder weight transitions:
			// 0: shrink to 0; 1: shrink to smaller positive; 2: same size; 3: grow within capacity; 4: grow > maxSize.
			var targetSz uint64
			switch r.Intn(5) {
			case 0:
				targetSz = 0
			case 1:
				targetSz = uint64(r.Intn(15) + 1)
			case 2:
				if cur := h.LookUpWithoutChangingOrder(k); cur != nil {
					targetSz = cur.size
				} else {
					targetSz = 20
				}
			case 3:
				targetSz = uint64(r.Intn(180) + 30)
			default:
				targetSz = cacheCapacity + uint64(r.Intn(100)+1)
			}
			h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("upd_%d", op), size: targetSz})
		case dice < 95:
			h.Erase(k)
		default:
			prefix := fmt.Sprintf("tree/%02d/", r.Intn(3))
			h.EraseEntriesWithGivenPrefix(prefix)
		}
	}

	// Assert
	h.DrainAndVerifyEvictionOrder(keys)
}
