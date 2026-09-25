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
	"github.com/stretchr/testify/require"
)

type diffValue struct {
	id   string
	size uint64
}

func (v *diffValue) Size() uint64 {
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

type diffInstance struct {
	name       string
	invariants bool
	cache      lru.Cache
}

type differentialHarness struct {
	t         *testing.T
	maxSize   uint64
	instances []diffInstance
}

func newDifferentialHarness(t *testing.T, maxSize uint64) *differentialHarness {
	t.Helper()
	h := &differentialHarness{
		t:       t,
		maxSize: maxSize,
	}

	configs := []struct {
		name        string
		constructor func(uint64, ...lru.Option) lru.Cache
	}{
		{"MapCache", lru.NewMapCache},
		{"RadixCache", lru.NewRadixCache},
		{"ArenaRadixCache", lru.NewArenaRadixCache},
	}

	for _, cfg := range configs {
		for _, inv := range []bool{false, true} {
			c := cfg.constructor(maxSize, lru.WithInvariantChecking(inv))
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

func (h *differentialHarness) compareValues(op string, baseVal, targetVal lru.ValueType, instName string, invariants bool) {
	h.t.Helper()
	if baseVal == nil {
		require.Nilf(h.t, targetVal, "[%s] value nil parity mismatch with %s (inv=%v)", op, instName, invariants)
		return
	}
	require.NotNilf(h.t, targetVal, "[%s] value nil parity mismatch with %s (inv=%v): base = %v", op, instName, invariants, baseVal)

	bv, ok1 := baseVal.(*diffValue)
	tv, ok2 := targetVal.(*diffValue)
	if ok1 && ok2 {
		require.Equalf(h.t, bv.id, tv.id, "[%s] value id mismatch with %s (inv=%v)", op, instName, invariants)
		require.Equalf(h.t, bv.size, tv.size, "[%s] value size mismatch with %s (inv=%v)", op, instName, invariants)
	} else {
		require.Equalf(h.t, baseVal.Size(), targetVal.Size(), "[%s] value size mismatch with %s (inv=%v)", op, instName, invariants)
	}
}

func (h *differentialHarness) compareEvicted(op string, baseEvicted, targetEvicted []lru.ValueType, instName string, invariants bool) {
	h.t.Helper()
	require.Lenf(h.t, targetEvicted, len(baseEvicted), "[%s] evicted slice length mismatch with %s (inv=%v)", op, instName, invariants)
	for i := range baseEvicted {
		h.compareValues(fmt.Sprintf("%s.evicted[%d]", op, i), baseEvicted[i], targetEvicted[i], instName, invariants)
	}
}

func (h *differentialHarness) Insert(key string, val lru.ValueType) []lru.ValueType {
	h.t.Helper()
	var baseEvicted []lru.ValueType
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

func (h *differentialHarness) Erase(key string) lru.ValueType {
	h.t.Helper()
	var baseVal lru.ValueType

	for i, inst := range h.instances {
		val := inst.cache.Erase(key)
		if i == 0 {
			baseVal = val
		} else {
			h.compareValues(fmt.Sprintf("Erase(%q)", key), baseVal, val, inst.name, inst.invariants)
		}
	}
	return baseVal
}

func (h *differentialHarness) LookUp(key string) lru.ValueType {
	h.t.Helper()
	var baseVal lru.ValueType

	for i, inst := range h.instances {
		val := inst.cache.LookUp(key)
		if i == 0 {
			baseVal = val
		} else {
			h.compareValues(fmt.Sprintf("LookUp(%q)", key), baseVal, val, inst.name, inst.invariants)
		}
	}
	return baseVal
}

func (h *differentialHarness) LookUpWithoutChangingOrder(key string) lru.ValueType {
	h.t.Helper()
	var baseVal lru.ValueType

	for i, inst := range h.instances {
		val := inst.cache.LookUpWithoutChangingOrder(key)
		if i == 0 {
			baseVal = val
		} else {
			h.compareValues(fmt.Sprintf("LookUpWithoutChangingOrder(%q)", key), baseVal, val, inst.name, inst.invariants)
		}
	}
	return baseVal
}

func (h *differentialHarness) UpdateWithoutChangingOrder(key string, val lru.ValueType) {
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

func (h *differentialHarness) UpdateSize(key string, delta uint64) {
	h.t.Helper()
	var baseErr error

	val := h.LookUpWithoutChangingOrder(key)

	for i, inst := range h.instances {
		err := inst.cache.UpdateSize(key, delta)
		if i == 0 {
			baseErr = err
		} else {
			h.compareErrors(fmt.Sprintf("UpdateSize(%q, %d)", key, delta), baseErr, err, inst.name, inst.invariants)
		}
	}

	if baseErr == nil && val != nil && h.LookUpWithoutChangingOrder(key) != nil {
		if tv, ok := val.(*diffValue); ok {
			tv.size += delta
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
				h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("%s_upd_%d", k, op), size: existing.Size()})
			} else {
				h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("%s_upd_%d", k, op), size: 10})
			}
		case dice < 90:
			h.UpdateSize(k, uint64(r.Intn(20)+1))
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
				h.UpdateWithoutChangingOrder(path, &diffValue{id: fmt.Sprintf("upd_%d", op), size: existing.Size()})
			}
		case dice < 85:
			h.UpdateSize(path, uint64(r.Intn(30)+1))
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

	// Act: Explicitly verify uint64 overflow rejection parity across all engines.
	h.Insert("overflow_key", &diffValue{id: "ovf", size: 20})
	h.UpdateSize("overflow_key", math.MaxUint64)
	h.UpdateSize("overflow_key", math.MaxUint64-10)

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
			h.UpdateSize(k, uint64(r.Intn(100)+1))
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
				if r.Intn(2) == 0 {
					h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("upd_%d", op), size: existing.Size()})
				} else {
					h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("bad_sz_%d", op), size: existing.Size() + 5})
				}
			} else {
				h.UpdateWithoutChangingOrder(k, &diffValue{id: fmt.Sprintf("noent_%d", op), size: 10})
			}
		case dice < 88:
			h.UpdateSize(k, uint64(r.Intn(50)))
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

func TestDifferential_UpdateSizeSelfEvictionPreservesDiffValueSize(t *testing.T) {
	// Arrange
	h := newDifferentialHarness(t, 100)
	dv := &diffValue{id: "self_evict", size: 50}
	h.Insert("k1", dv)

	// Act: Grow k1 by +60 (50 + 60 = 110 > maxSize 100), triggering self-eviction across all backends.
	h.UpdateSize("k1", 60)

	// Assert: k1 is evicted and dv.size remains 50 (not mutated to 110).
	require.Nil(t, h.LookUpWithoutChangingOrder("k1"))
	require.Equal(t, uint64(50), dv.Size())
}
