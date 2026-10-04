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
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testGoroutineID parses the calling goroutine's numeric ID from runtime.Stack.
func testGoroutineID() uint64 {
	var buf [32]byte
	n := runtime.Stack(buf[:], false)
	const prefix = "goroutine "
	var id uint64
	if n > len(prefix) {
		for i := len(prefix); i < n; i++ {
			b := buf[i]
			if b < '0' || b > '9' {
				break
			}
			id = id*10 + uint64(b-'0')
		}
	}
	return id
}

// assertAlreadyCompacted verifies that pac.Compact() allocates 0 heap objects on its very first invocation,
// and when a pressureProbe is provided, also verifies via !observeEpochAdvance(tb, probe, pac, compactOnce)
// that the first Compact() call itself did not advance reclaimEpoch (deterministically verifying RadixCache
// was already compacted too).
func assertAlreadyCompacted[V any](tb testing.TB, pac PressureAwareCache[V], probes ...*pressureProbe) {
	tb.Helper()
	var before, after runtime.MemStats
	compactOnce := func() {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
		runtime.Gosched()
		runtime.ReadMemStats(&before)
		pac.Compact()
		runtime.ReadMemStats(&after)
	}
	hasProbe := false
	for _, probe := range probes {
		if probe != nil {
			assert.False(tb, observeEpochAdvance(tb, probe, pac, compactOnce), "expected cache to already be compacted (first Compact call must not advance reclaimEpoch)")
			hasProbe = true
		}
	}
	if !hasProbe {
		compactOnce()
	}
	assert.Equal(tb, uint64(0), after.Mallocs-before.Mallocs, "expected cache to already be compacted (0 allocations on Compact)")
}

// pressureProbe provides a public-API PressureFunc callback that allows tests to dynamically
// update simulated memory pressure and observe via observeEpochAdvance whether an
// operation advanced the cache's reclamation epoch.
type pressureProbe struct {
	pressureBits atomic.Uint64

	mu         sync.Mutex
	hookActive bool
	bgGID      uint64
	bgCalls    int
	bgEntered  chan struct{}
	bgRelease  chan struct{}
}

func newPressureProbe(initial float64) *pressureProbe {
	p := &pressureProbe{}
	p.pressureBits.Store(math.Float64bits(initial))
	return p
}

func (p *pressureProbe) Set(val float64) {
	p.pressureBits.Store(math.Float64bits(val))
}

func (p *pressureProbe) Option() Option {
	return WithPressureFunc(p.PressureFunc)
}

func (p *pressureProbe) PressureFunc() float64 {
	p.mu.Lock()
	if p.hookActive && testGoroutineID() == p.bgGID {
		p.bgCalls++
		callNum := p.bgCalls
		entered := p.bgEntered
		release := p.bgRelease
		p.mu.Unlock()
		if callNum == 1 {
			close(entered)
			<-release
		}
		return 0.0
	}
	p.mu.Unlock()
	return math.Float64frombits(p.pressureBits.Load())
}

// observeEpochAdvance executes op() while a background EvaluateMemoryPressure() call
// holds a pre-operation epoch snapshot inside PressureFunc, returning true if and only if op()
// advanced the cache's reclamation epoch (causing lockWithPressure to re-sample PressureFunc).
func observeEpochAdvance[V any](tb testing.TB, p *pressureProbe, pac PressureAwareCache[V], op func()) bool {
	tb.Helper()

	bgEntered := make(chan struct{})
	bgRelease := make(chan struct{})
	bgDone := make(chan struct{})

	go func() {
		defer close(bgDone)
		p.mu.Lock()
		p.hookActive = true
		p.bgGID = testGoroutineID()
		p.bgCalls = 0
		p.bgEntered = bgEntered
		p.bgRelease = bgRelease
		p.mu.Unlock()

		_ = pac.EvaluateMemoryPressure()

		p.mu.Lock()
		p.hookActive = false
		p.bgGID = 0
		p.mu.Unlock()
	}()

	<-bgEntered
	op()
	close(bgRelease)
	<-bgDone

	p.mu.Lock()
	calls := p.bgCalls
	p.mu.Unlock()
	return calls > 1
}

func allBackends[V any]() []struct {
	name string
	fn   func(uint64, ...Option) Cache[V]
} {
	return []struct {
		name string
		fn   func(uint64, ...Option) Cache[V]
	}{
		{"MapCache", NewMapCache[V]},
		{"RadixCache", NewRadixCache[V]},
		{"ArenaRadixCache", NewArenaRadixCache[V]},
	}
}

type backendDef struct {
	name string
	fn   func(uint64, ...Option) Cache[testData]
}

func testBackends() []backendDef {
	return []backendDef{
		{"MapCache", func(maxSize uint64, opts ...Option) Cache[testData] {
			return NewMapCache[testData](maxSize, slices.Concat([]Option{testDataWeigher}, opts)...)
		}},
		{"RadixCache", func(maxSize uint64, opts ...Option) Cache[testData] {
			return NewRadixCache[testData](maxSize, slices.Concat([]Option{testDataWeigher}, opts)...)
		}},
		{"ArenaRadixCache", func(maxSize uint64, opts ...Option) Cache[testData] {
			return NewArenaRadixCache[testData](maxSize, slices.Concat([]Option{testDataWeigher}, opts)...)
		}},
	}
}

func TestCompaction_EmptyDrainAndPrePutSlackReclamation(t *testing.T) {
	t.Run("SequentialDeleteDrainToEmptyReleasesPeakSlack", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.10)
				pac := b.fn(100000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

				for i := range 200 {
					k := fmt.Sprintf("item/sub/%04d", i)
					_, err := pac.Put(k, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Drain all entries via individual Delete(key) calls down to 0 entries.
				for i := range 200 {
					k := fmt.Sprintf("item/sub/%04d", i)
					_, ok := pac.Delete(k)
					require.True(t, ok)
				}

				// Assert: Peak structures are released upon reaching empty state.
				_, ok := pac.Peek("item/sub/0000")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})

	t.Run("PrePutDrainReleasesPeakSlackAtNormalPressure", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				// Arrange: Populate 100 hierarchical keys (1000B total in 1000B cache) at normal pressure (0.10).
				var sampleCount atomic.Int32
				c := b.fn(
					1000,
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						sampleCount.Add(1)
						return 0.10
					}),
				).(PressureAwareCache[testData])

				for i := range 100 {
					key := fmt.Sprintf("dir_%02d/sub_%02d/file_%03d", i%10, (i/10)%10, i)
					_, err := c.Put(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Put a single 1000B jumbo entry that pre-evicts all 100 entries down to empty before inserting.
				samplesBefore := sampleCount.Load()
				evicted, err := c.Put("jumbo", testData{value: 999, dataSize: 1000})

				// Assert: All 100 entries were evicted, peak slack was released, and only 1 pressure sample ran.
				require.NoError(t, err)
				assert.Len(t, evicted, 100)
				assert.Equal(t, int32(1), sampleCount.Load()-samplesBefore)
				_, ok := c.Peek("dir_00/sub_00/file_000")
				assert.False(t, ok)
				_, ok = c.Peek("jumbo")
				assert.True(t, ok)
				assertAlreadyCompacted(t, c)
			})
		}
	})

	t.Run("PrePutEmptyDrainUnderTier1PressureAdvancesReclaimEpochAcrossBackends", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				// Arrange: Populate 70 keys (10B each = 700B in 1000B cache) at normal pressure (0.10).
				probe := newPressureProbe(0.10)
				pac := b.fn(
					1000,
					WithInvariantChecking(true),
					probe.Option(),
				).(PressureAwareCache[testData])

				for i := range 70 {
					_, err := pac.Put(fmt.Sprintf("dir_%02d/k_%03d", i%10, i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Raise pressure to Tier 1 (0.80) and insert a 1000B entry that pre-evicts all 70 keys.
				probe.Set(0.80)
				var evicted []testData
				var err error
				advanced := observeEpochAdvance(t, probe, pac, func() {
					evicted, err = pac.Put("jumbo", testData{value: 999, dataSize: 1000})
				})

				// Assert
				require.NoError(t, err)
				assert.Len(t, evicted, 70)
				assert.True(t, advanced)
				_, ok := pac.Peek("jumbo")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})

	t.Run("NetByteReductionPrePutEvictionUnderTier1PressureAdvancesReclaimEpoch", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				// Arrange: Fill a 100B cache with "k1" (60B) and "k2" (40B) = 100B at Tier 1 pressure (0.80).
				probe := newPressureProbe(0.80)
				pac := b.fn(
					100,
					WithInvariantChecking(true),
					probe.Option(),
				).(PressureAwareCache[testData])
				_, err := pac.Put("k1", testData{value: 1, dataSize: 60})
				require.NoError(t, err)
				_, err = pac.Put("k2", testData{value: 2, dataSize: 40})
				require.NoError(t, err)

				// Act: Put "k3" (50B), which pre-evicts "k1" (60B) so net currentSize decreases from 100B to 90B.
				var evicted []testData
				advanced := observeEpochAdvance(t, probe, pac, func() {
					evicted, err = pac.Put("k3", testData{value: 3, dataSize: 50})
				})

				// Assert: Net byte reduction via pre-insert eviction under elevated pressure advances reclamation epoch and compacts.
				require.NoError(t, err)
				require.Len(t, evicted, 1)
				assert.True(t, advanced)
				_, ok := pac.Peek("k1")
				assert.False(t, ok)
				_, ok = pac.Peek("k2")
				assert.True(t, ok)
				_, ok = pac.Peek("k3")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})

	t.Run("LargeCacheFullDrainAvoidsPeakOldToNewAllocation", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				// Arrange: Populate 200 entries via public API, then insert a single entry of size == maxSize (200).
				const n = 200
				probe := newPressureProbe(0.10)
				pac := b.fn(n, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range n {
					_, err := pac.Put(fmt.Sprintf("k/%06d", i), testData{value: int64(i), dataSize: 1})
					require.NoError(t, err)
				}

				// Act
				evicted, err := pac.Put("jumbo", testData{value: 999, dataSize: n})
				require.NoError(t, err)

				// Assert: All 200 entries were evicted, only "jumbo" remains, and cache is already compacted without peak slack.
				assert.Len(t, evicted, n)
				_, ok := pac.Get("k/000000")
				assert.False(t, ok)
				_, ok = pac.Get("jumbo")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})
}

func TestCompaction_DeleteEmptyPrefixResetsSlack(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			// Arrange
			probe := newPressureProbe(0.10)
			pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
			for i := range 100 {
				_, err := pac.Put(fmt.Sprintf("dir/sub/%03d", i), testData{value: int64(i), dataSize: 10})
				require.NoError(t, err)
			}

			// Act
			pac.DeletePrefix("")

			// Assert: All entries are removed, backing structures are reset/compacted, and full capacity is available.
			_, ok := pac.Peek("dir/sub/000")
			assert.False(t, ok)
			_, ok = pac.Peek("dir/sub/099")
			assert.False(t, ok)
			assertAlreadyCompacted(t, pac, probe)

			evicted, err := pac.Put("full_capacity", testData{value: 999, dataSize: 1000})
			require.NoError(t, err)
			assert.Empty(t, evicted)
		})
	}
}

func TestCompaction_SingleSurvivorAndOverwriteSlackReclamation(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("OverwriteEvictingAllOtherEntriesReleasesPeakSlack", func(t *testing.T) {
				// Arrange: Populate 70 keys (10B each = 700B in 1000B cache).
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range 70 {
					key := fmt.Sprintf("k-%03d", i)
					_, err := pac.Put(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Overwrite existing key "k-000" with a 1000B value, evicting all 69 other entries down to 1 entry.
				evicted, err := pac.Put("k-000", testData{value: 999, dataSize: 1000})
				require.NoError(t, err)

				// Assert
				assert.Len(t, evicted, 69)
				_, ok := pac.Peek("k-000")
				assert.True(t, ok)
				_, ok = pac.Peek("k-001")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})

			t.Run("ReplaceGrowEvictingAllOtherEntriesReleasesPeakSlack", func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range 70 {
					key := fmt.Sprintf("k-%03d", i)
					_, err := pac.Put(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Grow MRU key "k-069" to 1000B, evicting all 69 older entries down to 1 entry.
				err := pac.Replace("k-069", testData{value: 690, dataSize: 1000})

				// Assert
				require.NoError(t, err)
				_, ok := pac.Peek("k-069")
				assert.True(t, ok)
				_, ok = pac.Peek("k-000")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})

			t.Run("PutNewKeyWithSurvivingZeroSizeMRUEntryReclaimsPeakSlack", func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range 70 {
					key := fmt.Sprintf("k-%03d", i)
					_, err := pac.Put(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				_, err := pac.Put("z_head", testData{value: 0, dataSize: 0})
				require.NoError(t, err)

				// Act
				_, err = pac.Put("jumbo", testData{value: 999, dataSize: 1000})
				require.NoError(t, err)

				// Assert
				_, ok := pac.Peek("z_head")
				assert.True(t, ok)
				_, ok = pac.Peek("jumbo")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})

			t.Run("DeletePrefixDrainingToOneSurvivorReclaimsPeakSlack", func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range 70 {
					key := fmt.Sprintf("batch/k-%03d", i)
					_, err := pac.Put(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				_, err := pac.Put("keep/1", testData{value: 1, dataSize: 10})
				require.NoError(t, err)

				// Act
				pac.DeletePrefix("batch/")

				// Assert
				_, ok := pac.Peek("keep/1")
				assert.True(t, ok)
				_, ok = pac.Peek("batch/k-000")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})

			t.Run("DeleteAndReplaceSelfEvictionDrainingToOneSurvivorReclaimPeakSlack", func(t *testing.T) {
				// Arrange: Put "item-069" first (at LRU tail) followed by "item-000".."item-068" (so item-068 is MRU).
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				_, err := pac.Put("item-069", testData{value: 69, dataSize: 10})
				require.NoError(t, err)
				for i := range 69 {
					key := fmt.Sprintf("item-%03d", i)
					_, err := pac.Put(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				for i := range 68 {
					key := fmt.Sprintf("item-%03d", i)
					_, ok := pac.Delete(key)
					require.True(t, ok)
				}

				// Act: Grow tail entry "item-069" to 1000B while "item-068" (10B) is newer -> self-evicts "item-069".
				err = pac.Replace("item-069", testData{value: 690, dataSize: 1000})
				require.NoError(t, err)

				// Assert
				_, ok := pac.Peek("item-068")
				assert.True(t, ok)
				_, ok = pac.Peek("item-069")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})

			t.Run("Exact64EntriesDrainedToOneSurvivorAndHighChurnTombstoneDrain", func(t *testing.T) {
				// Arrange 1: 64 entries drained to 1 survivor via overwrite.
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range 64 {
					key := string([]byte{byte(i + 1), 'k'})
					_, err := pac.Put(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				firstKey := string([]byte{1, 'k'})
				_, err := pac.Put(firstKey, testData{value: 999, dataSize: 1000})
				require.NoError(t, err)

				_, ok := pac.Peek(firstKey)
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)

				// Arrange 2: High-churn tombstone drain to empty or single survivor.
				probeEmpty := newPressureProbe(0.10)
				pacEmpty := b.fn(300, WithInvariantChecking(true), probeEmpty.Option()).(PressureAwareCache[testData])
				for i := range 100 {
					key := string([]byte{byte(i + 1), 'k'})
					_, err = pacEmpty.Put(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				probeOne := newPressureProbe(0.10)
				pacOne := b.fn(1000, WithInvariantChecking(true), probeOne.Option()).(PressureAwareCache[testData])
				_, err = pacOne.Put("survivor", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				for i := range 50 {
					key := fmt.Sprintf("batch1-%02d", i)
					_, err = pacOne.Put(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				for i := range 50 {
					key := fmt.Sprintf("batch1-%02d", i)
					_, ok := pacOne.Delete(key)
					require.True(t, ok)
				}
				for i := range 50 {
					key := fmt.Sprintf("batch2-%02d", i)
					_, err = pacOne.Put(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				_, err = pacEmpty.Put("jumbo", testData{value: 999, dataSize: 300})
				require.NoError(t, err)

				pacOne.DeletePrefix("batch2-")

				_, ok = pacEmpty.Peek("jumbo")
				assert.True(t, ok)
				_, ok = pacOne.Peek("survivor")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pacEmpty, probeEmpty)
				assertAlreadyCompacted(t, pacOne, probeOne)
			})
		})
	}
}

func TestCompaction_CrossBackendParityOnSingleSurvivorAndShrinkageWatermarks(t *testing.T) {
	t.Run("DeleteDrainingPeakEntriesToSingleSurvivorAdvancesReclaimEpoch", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

				for i := range 70 {
					_, err := pac.Put(fmt.Sprintf("z-%03d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}
				for i := range 68 {
					_, ok := pac.Delete(fmt.Sprintf("z-%03d", i))
					require.True(t, ok)
				}

				// Act: Raise pressure to Tier 1 (0.80) and erase "z-068", draining from 70 peak entries to 1 survivor.
				probe.Set(0.80)
				advanced := observeEpochAdvance(t, probe, pac, func() {
					_, ok := pac.Delete("z-068")
					require.True(t, ok)
				})

				// Assert
				assert.True(t, advanced)
				_, ok := pac.Peek("z-069")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})

	t.Run("ReplaceSelfEvictionDrainingPeakEntriesToSingleSurvivorClampsZeroWatermark", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				// Arrange: Populate 10 zero-size entries, shed 5 under critical pressure (setting watermark to 5),
				// then add "p-069" at tail, 64 zero-size entries, and 1 positive entry "keep-p" at MRU, and drain down to 1 surviving zero-size entry + keep-p via self-eviction.
				probe := newPressureProbe(0.10)
				pac := b.fn(
					1000,
					WithInvariantChecking(true),
					WithEvictionRetentionRatio(0.50),
					probe.Option(),
				).(PressureAwareCache[testData])

				for i := range 10 {
					_, err := pac.Put(fmt.Sprintf("init-z-%02d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}
				probe.Set(0.95)
				require.Len(t, pac.EvaluateMemoryPressure(), 5)

				probe.Set(0.10)
				_, err := pac.Put("p-069", testData{value: 69, dataSize: 10})
				require.NoError(t, err)
				for i := range 64 {
					_, err = pac.Put(fmt.Sprintf("z-%03d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}

				for i := 5; i < 10; i++ {
					_, ok := pac.Delete(fmt.Sprintf("init-z-%02d", i))
					require.True(t, ok)
				}
				for i := range 63 {
					_, ok := pac.Delete(fmt.Sprintf("z-%03d", i))
					require.True(t, ok)
				}

				// Delete "p-069" under Tier 1 pressure, leaving only 1 zero-size entry ("z-063") from 70 peak entries.
				probe.Set(0.80)
				advanced := observeEpochAdvance(t, probe, pac, func() {
					_, ok := pac.Delete("p-069")
					require.True(t, ok)
				})
				assert.True(t, advanced)
				assertAlreadyCompacted(t, pac, probe)

				// Put 1 more zero-size entry ("z-064", total 2 zero-size entries) and evaluate under Tier 2 (0.95).
				// Because the zero-size watermark was clamped to 1 during single-survivor drain, 1 of the 2 zero-size entries is shed.
				_, err = pac.Put("z-064", testData{value: 64, dataSize: 0})
				require.NoError(t, err)
				probe.Set(0.95)
				evicted := pac.EvaluateMemoryPressure()
				assert.Len(t, evicted, 1)
				_, ok := pac.Peek("z-063")
				assert.False(t, ok)
				_, ok = pac.Peek("z-064")
				assert.True(t, ok)
			})
		}
	})

	t.Run("SmallSheddingPreservesCompactionWatermarksForSubsequentSingleSurvivorDrain", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(
					700,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.90),
					probe.Option(),
				).(PressureAwareCache[testData])

				for i := range 64 {
					_, err := pac.Put(fmt.Sprintf("k-%03d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Trigger small Tier 2 shedding (evicts 2 of 65 entries, < 25% shrinkage),
				// then erase remaining entries down to 1 survivor under Tier 1 (0.80).
				probe.Set(0.95)
				_, err := pac.Put("k-064", testData{value: 64, dataSize: 10})
				require.NoError(t, err)

				probe.Set(0.10)
				for i := 2; i < 63; i++ {
					_, ok := pac.Delete(fmt.Sprintf("k-%03d", i))
					require.True(t, ok)
				}

				probe.Set(0.80)
				advanced := observeEpochAdvance(t, probe, pac, func() {
					_, ok := pac.Delete("k-063")
					require.True(t, ok)
				})

				// Assert
				assert.True(t, advanced)
				_, ok := pac.Peek("k-064")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})

	t.Run("ValueBearingInternalNodeShrinkage", testValueBearingInternalNodeShrinkage)
	t.Run("Tier2ShedAndAutoCompactAdvancesReclaimEpochOncePerOperation", testTier2ShedAndAutoCompactAdvancesReclaimEpochOnce)
	t.Run("Exact25PercentShrinkageAutoCompactionParityAcrossBackends", testExact25PercentShrinkageAutoCompactionParity)
	t.Run("SmallCacheWithRoutingNodesRespects8EntryHysteresis", testSmallCacheWithRoutingNodesRespects8EntryHysteresis)
	t.Run("Tier1AndTier2AutoCompactionParityAcrossBackends", testTier1AndTier2AutoCompactionParityAcrossBackends)
}

func testValueBearingInternalNodeShrinkage(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			probe := newPressureProbe(0.10)
			pac := b.fn(10000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

			for i := range 20 {
				parentKey := fmt.Sprintf("p%02d", i)
				childA := fmt.Sprintf("p%02d/a", i)
				childB := fmt.Sprintf("p%02d/b", i)
				for _, k := range []string{parentKey, childA, childB} {
					_, err := pac.Put(k, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
			}

			for i := range 20 {
				_, ok := pac.Delete(fmt.Sprintf("p%02d", i))
				require.True(t, ok)
			}

			// Act: Raise pressure to Tier 1 (0.80) and call EvaluateMemoryPressure().
			probe.Set(0.80)
			advanced := observeEpochAdvance(t, probe, pac, func() {
				pac.EvaluateMemoryPressure()
			})

			// Assert
			assert.True(t, advanced)
			assertAlreadyCompacted(t, pac, probe)
		})
	}
}

func testTier2ShedAndAutoCompactAdvancesReclaimEpochOnce(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			probe := newPressureProbe(0.10)
			pac := b.fn(
				1000,
				WithInvariantChecking(true),
				WithEvictionThreshold(0.90),
				WithEvictionRetentionRatio(0.50),
				probe.Option(),
			).(PressureAwareCache[testData])

			for i := range 20 {
				_, err := pac.Put(fmt.Sprintf("k-%02d", i), testData{value: int64(i), dataSize: 50})
				require.NoError(t, err)
			}

			// Act
			probe.Set(0.95)
			var evicted []testData
			advanced := observeEpochAdvance(t, probe, pac, func() {
				evicted = pac.EvaluateMemoryPressure()
			})

			// Assert: Tier 2 shed+compact evicted 10 entries, compacted slack, and an immediate repeat is a no-op.
			require.Len(t, evicted, 10)
			assert.True(t, advanced)
			assertAlreadyCompacted(t, pac, probe)

			advancedRepeat := observeEpochAdvance(t, probe, pac, func() {
				assert.Empty(t, pac.EvaluateMemoryPressure())
			})
			assert.False(t, advancedRepeat)
		})
	}
}

func testExact25PercentShrinkageAutoCompactionParity(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			probe := newPressureProbe(0.0)
			pac := b.fn(
				1000,
				WithInvariantChecking(true),
				probe.Option(),
				WithCompactionThreshold(0.75),
				WithEvictionThreshold(0.90),
			).(PressureAwareCache[testData])

			for i := range 20 {
				_, err := pac.Put(fmt.Sprintf("k%02d", i), testData{value: int64(i), dataSize: 0})
				require.NoError(t, err)
			}
			for i := range 4 {
				_, ok := pac.Delete(fmt.Sprintf("k%02d", i))
				require.True(t, ok)
			}
			probe.Set(0.80)

			// Act 1: 5th deletion under Tier 1 pressure reaches exact 25% shrinkage from peak (20 -> 15).
			advancedFifth := observeEpochAdvance(t, probe, pac, func() {
				_, ok := pac.Delete("k04")
				require.True(t, ok)
			})

			// Assert 1
			assert.True(t, advancedFifth)
			assertAlreadyCompacted(t, pac, probe)

			// Act 2: Delete 1 more zero-byte key ("k05", 15 -> 14, 1/15 < 25% shrinkage) at Tier 1 pressure;
			// because watermarks were reset to 15 on the 5th deletion, it does not re-compact.
			advancedSixth := observeEpochAdvance(t, probe, pac, func() {
				_, ok := pac.Delete("k05")
				require.True(t, ok)
			})
			assert.False(t, advancedSixth)
		})
	}
}

func testSmallCacheWithRoutingNodesRespects8EntryHysteresis(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			// Arrange: Put 6 zero-byte hierarchical keys (6 entries <= 8 hysteresis floor, even though the radix tree
			// allocates 10 arena nodes > 8 due to root + 3 intermediate routing nodes "a/", "b/", "c/").
			probe := newPressureProbe(0.10)
			pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

			for _, k := range []string{"a/1", "a/2", "b/1", "b/2", "c/1", "c/2"} {
				_, err := pac.Put(k, testData{value: 1, dataSize: 0})
				require.NoError(t, err)
			}

			// Act: Under Tier 1 pressure (0.80), erase 2 zero-byte keys ("a/1", "a/2") while observing epoch advancement.
			probe.Set(0.80)
			advanced := observeEpochAdvance(t, probe, pac, func() {
				_, ok := pac.Delete("a/1")
				require.True(t, ok)
				_, ok = pac.Delete("a/2")
				require.True(t, ok)
			})

			// Assert: Because peak entry count is 6 <= 8 (below the 8-entry small-cache hysteresis floor) and 0 bytes were freed,
			// all backends refrain from auto-compacting or advancing reclamation epoch.
			assert.False(t, advanced)
			_, ok := pac.Peek("a/1")
			assert.False(t, ok)
			_, ok = pac.Peek("a/2")
			assert.False(t, ok)
			for _, k := range []string{"b/1", "b/2", "c/1", "c/2"} {
				_, ok := pac.Peek(k)
				assert.True(t, ok)
			}
		})
	}
}

func testTier1AndTier2AutoCompactionParityAcrossBackends(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("Tier1InlineShrinkageAndExplicitEvaluateAdvanceEpochAndResetSlack", func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(10000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

				for i := range 20 {
					_, err := pac.Put(fmt.Sprintf("k/%02d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				for i := range 5 {
					_, ok := pac.Delete(fmt.Sprintf("k/%02d", i))
					require.True(t, ok)
				}

				// Act 1: Delete the 6th of 20 entries (30% >= 25% shrinkage) under Tier 1 pressure (0.80).
				probe.Set(0.80)
				advancedInline := observeEpochAdvance(t, probe, pac, func() {
					_, ok := pac.Delete("k/05")
					require.True(t, ok)
				})
				assert.True(t, advancedInline)
				assertAlreadyCompacted(t, pac, probe)

				advancedFollowUp := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedFollowUp)

				// Act 2: Delete 1 more entry at normal pressure (0.10), then invoke EvaluateMemoryPressure() under Tier 1 (0.80).
				probe.Set(0.10)
				_, ok := pac.Delete("k/06")
				require.True(t, ok)

				probe.Set(0.80)
				advancedExplicit := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.True(t, advancedExplicit)
				assertAlreadyCompacted(t, pac, probe)

				advancedSecondEval := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedSecondEval)
			})

			t.Run("Tier2NoShedEvaluateCompactsPendingSlack", func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(
					1000,
					WithInvariantChecking(true),
					WithEvictionRetentionRatio(0.50),
					probe.Option(),
				).(PressureAwareCache[testData])

				for i := range 20 {
					_, err := pac.Put(fmt.Sprintf("k/%02d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				for i := range 6 {
					_, ok := pac.Delete(fmt.Sprintf("k/%02d", i))
					require.True(t, ok)
				}

				// Act: Spike pressure to Tier 2 (0.95) and call EvaluateMemoryPressure() while currentSize (140B) <= targetSize (500B).
				probe.Set(0.95)
				advanced := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.True(t, advanced)
				assertAlreadyCompacted(t, pac, probe)

				advancedRepeat := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedRepeat)
			})
		})
	}
}

func testEmptyCacheDrainDoesNotSpuriouslyAdvanceEpoch(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("ZeroByteLastEntryUnderTier1Pressure", func(t *testing.T) {
				probeDelete := newPressureProbe(0.80)
				probeSelfEvict := newPressureProbe(0.80)
				probePrefixEmpty := newPressureProbe(0.80)
				cacheDelete := b.fn(100, WithInvariantChecking(true), probeDelete.Option()).(PressureAwareCache[testData])
				cacheSelfEvict := b.fn(100, WithInvariantChecking(true), probeSelfEvict.Option()).(PressureAwareCache[testData])
				cachePrefixEmpty := b.fn(100, WithInvariantChecking(true), probePrefixEmpty.Option()).(PressureAwareCache[testData])

				_, err := cacheDelete.Put("z1", testData{value: 1, dataSize: 0})
				require.NoError(t, err)
				_, err = cacheSelfEvict.Put("z1", testData{value: 1, dataSize: 0})
				require.NoError(t, err)
				_, err = cacheSelfEvict.Put("mru_full", testData{value: 2, dataSize: 100})
				require.NoError(t, err)
				_, err = cachePrefixEmpty.Put("z1", testData{value: 1, dataSize: 0})
				require.NoError(t, err)

				advancedDelete := observeEpochAdvance(t, probeDelete, cacheDelete, func() {
					_, ok := cacheDelete.Delete("z1")
					require.True(t, ok)
				})
				advancedSelfEvict := observeEpochAdvance(t, probeSelfEvict, cacheSelfEvict, func() {
					require.NoError(t, cacheSelfEvict.Replace("z1", testData{value: 11, dataSize: 10}))
				})
				advancedPrefixEmpty := observeEpochAdvance(t, probePrefixEmpty, cachePrefixEmpty, func() {
					cachePrefixEmpty.DeletePrefix("")
				})

				assert.False(t, advancedDelete)
				assert.False(t, advancedSelfEvict)
				assert.False(t, advancedPrefixEmpty)
			})

			t.Run("NormalPressureLastEntryDeleteAndEmptyPrefixClearDoNotAdvanceEpoch", func(t *testing.T) {
				var eraseSamples, prefixSamples, emptyPrefixSamples atomic.Int32
				cacheDelete := b.fn(100, WithInvariantChecking(true), WithPressureFunc(func() float64 {
					eraseSamples.Add(1)
					return 0.10
				})).(PressureAwareCache[testData])
				cachePrefix := b.fn(100, WithInvariantChecking(true), WithPressureFunc(func() float64 {
					prefixSamples.Add(1)
					return 0.10
				})).(PressureAwareCache[testData])
				cachePrefixEmpty := b.fn(100, WithInvariantChecking(true), WithPressureFunc(func() float64 {
					emptyPrefixSamples.Add(1)
					return 0.10
				})).(PressureAwareCache[testData])

				_, err := cacheDelete.Put("k1", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				_, err = cachePrefix.Put("k1", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				_, err = cachePrefixEmpty.Put("k1", testData{value: 1, dataSize: 10})
				require.NoError(t, err)

				eraseBefore := eraseSamples.Load()
				_, ok := cacheDelete.Delete("k1")
				require.True(t, ok)
				assert.Equal(t, int32(1), eraseSamples.Load()-eraseBefore)

				prefixBefore := prefixSamples.Load()
				cachePrefix.DeletePrefix("k")
				assert.Equal(t, int32(1), prefixSamples.Load()-prefixBefore)

				emptyBefore := emptyPrefixSamples.Load()
				cachePrefixEmpty.DeletePrefix("")
				assert.Equal(t, int32(0), emptyPrefixSamples.Load()-emptyBefore)

				_, ok = cacheDelete.Peek("k1")
				assert.False(t, ok)
				_, ok = cachePrefix.Peek("k1")
				assert.False(t, ok)
				_, ok = cachePrefixEmpty.Peek("k1")
				assert.False(t, ok)
				assertAlreadyCompacted(t, cacheDelete)
				assertAlreadyCompacted(t, cachePrefix)
				assertAlreadyCompacted(t, cachePrefixEmpty)
			})

			t.Run("FreshCacheAndEmptyPrefixClearAreAlreadyCompactedAndDoNotAdvanceEpoch", func(t *testing.T) {
				probe := newPressureProbe(0.80)
				pac := b.fn(100, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

				assertAlreadyCompacted(t, pac, probe)
				advancedFreshEval := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedFreshEval)

				// Populate and clear via DeletePrefix(""); resulting empty cache must already be compacted.
				probe.Set(0.10)
				for i := range 10 {
					_, err := pac.Put(fmt.Sprintf("k-%02d", i), testData{value: int64(i), dataSize: 5})
					require.NoError(t, err)
				}
				probe.Set(0.80)
				advancedClear := observeEpochAdvance(t, probe, pac, func() {
					pac.DeletePrefix("")
				})
				assert.True(t, advancedClear)
				assertAlreadyCompacted(t, pac, probe)

				advancedPostClearEval := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedPostClearEval)
			})

			t.Run("Draining20ZeroSizeEntriesToEmptyUnderTier1AdvancesEpoch", func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range 20 {
					_, err := pac.Put(fmt.Sprintf("z/%02d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}

				probe.Set(0.80)
				advanced := observeEpochAdvance(t, probe, pac, func() {
					pac.DeletePrefix("z/")
				})
				assert.True(t, advanced)
				_, ok := pac.Peek("z/00")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		})
	}
}

func TestCompaction_PreReclaimSlackAndEpochConsistencyAcrossMutations(t *testing.T) {
	t.Run("SingleSurvivorDrainDuringPutDoesNotDoubleAllocateOrRecompactOnEvaluate", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

				for i := range 64 {
					_, err := pac.Put(fmt.Sprintf("k-%03d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act
				probe.Set(0.80)
				advancedPut := observeEpochAdvance(t, probe, pac, func() {
					_, err := pac.Put("big", testData{value: 999, dataSize: 990})
					require.NoError(t, err)
				})
				assert.True(t, advancedPut)
				assertAlreadyCompacted(t, pac, probe)

				advancedEval := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEval)
			})
		}
	})

	t.Run("EmptyCacheDrainDoesNotSpuriouslyAdvanceEpochOnZeroByteOrNormalPressure", testEmptyCacheDrainDoesNotSpuriouslyAdvanceEpoch)

	t.Run("PutNewKeySingleSurvivorPreCompactionDirtiedByTier2Shed", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(
					1000,
					WithInvariantChecking(true),
					WithEvictionRetentionRatio(0.50),
					probe.Option(),
				).(PressureAwareCache[testData])

				for i := range 64 {
					_, err := pac.Put(fmt.Sprintf("e/%02d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				_, err := pac.Put("survivor", testData{value: 1, dataSize: 5})
				require.NoError(t, err)

				probe.Set(0.95)
				advancedPut := observeEpochAdvance(t, probe, pac, func() {
					_, err = pac.Put("jumbo", testData{value: 999, dataSize: 995})
					require.NoError(t, err)
				})
				assert.True(t, advancedPut)
				assertAlreadyCompacted(t, pac, probe)

				probe.Set(0.80)
				advancedEval := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEval)
			})
		}
	})

	t.Run("OverwriteAndReplaceGrowPreReclaimSlackAndZeroSizeSurvivor", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				t.Run("OverwriteAndReplaceGrowToSingleSurvivorUnderTier1DoNotDoubleAdvanceEpoch", func(t *testing.T) {
					probeOverwrite := newPressureProbe(0.10)
					probeUpdate := newPressureProbe(0.10)
					cacheOverwrite := b.fn(100, WithInvariantChecking(true), probeOverwrite.Option()).(PressureAwareCache[testData])
					cacheUpdateSize := b.fn(100, WithInvariantChecking(true), probeUpdate.Option()).(PressureAwareCache[testData])

					for _, c := range []Cache[testData]{cacheOverwrite, cacheUpdateSize} {
						_, err := c.Put("b", testData{value: 2, dataSize: 50})
						require.NoError(t, err)
						_, err = c.Put("a", testData{value: 1, dataSize: 50})
						require.NoError(t, err)
					}

					probeOverwrite.Set(0.80)
					probeUpdate.Set(0.80)

					advancedOverwrite := observeEpochAdvance(t, probeOverwrite, cacheOverwrite, func() {
						_, err := cacheOverwrite.Put("a", testData{value: 11, dataSize: 90})
						require.NoError(t, err)
					})
					advancedUpdate := observeEpochAdvance(t, probeUpdate, cacheUpdateSize, func() {
						require.NoError(t, cacheUpdateSize.Replace("a", testData{value: 11, dataSize: 90}))
					})
					assert.True(t, advancedOverwrite)
					assert.True(t, advancedUpdate)
					assertAlreadyCompacted(t, cacheOverwrite, probeOverwrite)
					assertAlreadyCompacted(t, cacheUpdateSize, probeUpdate)

					advancedEvalOverwrite := observeEpochAdvance(t, probeOverwrite, cacheOverwrite, func() {
						cacheOverwrite.EvaluateMemoryPressure()
					})
					advancedEvalUpdate := observeEpochAdvance(t, probeUpdate, cacheUpdateSize, func() {
						cacheUpdateSize.EvaluateMemoryPressure()
					})
					assert.False(t, advancedEvalOverwrite)
					assert.False(t, advancedEvalUpdate)
				})

				t.Run("OverwriteAndReplaceGrowWithSurvivingZeroSizeEntryReclaim64DeletedSlack", func(t *testing.T) {
					probeOverwrite := newPressureProbe(0.10)
					probeUpdate := newPressureProbe(0.10)
					cacheOverwrite := b.fn(650, WithInvariantChecking(true), probeOverwrite.Option()).(PressureAwareCache[testData])
					cacheUpdateSize := b.fn(650, WithInvariantChecking(true), probeUpdate.Option()).(PressureAwareCache[testData])

					for _, c := range []Cache[testData]{cacheOverwrite, cacheUpdateSize} {
						for i := range 64 {
							_, err := c.Put(fmt.Sprintf("e/%02d", i), testData{value: int64(i), dataSize: 10})
							require.NoError(t, err)
						}
						_, err := c.Put("target", testData{value: 100, dataSize: 10})
						require.NoError(t, err)
						_, err = c.Put("zero", testData{value: 0, dataSize: 0})
						require.NoError(t, err)
					}

					_, err := cacheOverwrite.Put("target", testData{value: 200, dataSize: 650})
					require.NoError(t, err)
					require.NoError(t, cacheUpdateSize.Replace("target", testData{value: 200, dataSize: 650}))

					for _, item := range []struct {
						pac   PressureAwareCache[testData]
						probe *pressureProbe
					}{
						{cacheOverwrite, probeOverwrite},
						{cacheUpdateSize, probeUpdate},
					} {
						_, ok := item.pac.Peek("target")
						assert.True(t, ok)
						_, ok = item.pac.Peek("zero")
						assert.True(t, ok)
						assertAlreadyCompacted(t, item.pac, item.probe)

						item.probe.Set(0.80)
						advancedEval := observeEpochAdvance(t, item.probe, item.pac, func() {
							item.pac.EvaluateMemoryPressure()
						})
						assert.False(t, advancedEval)
					}
				})
			})
		}
	})

	t.Run("PutNewKeyMassPrePutEvictionWithTwoZeroSizeSurvivorsCompacts", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(
					640,
					WithInvariantChecking(true),
					probe.Option(),
				).(PressureAwareCache[testData])
				for i := range 64 {
					_, err := pac.Put(fmt.Sprintf("p%02d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				_, err := pac.Put("z1", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				_, err = pac.Put("z2", testData{value: 0, dataSize: 0})
				require.NoError(t, err)

				evicted, err := pac.Put("jumbo", testData{value: 999, dataSize: 640})
				require.NoError(t, err)
				require.Len(t, evicted, 64)

				_, ok := pac.Peek("z1")
				assert.True(t, ok)
				_, ok = pac.Peek("z2")
				assert.True(t, ok)
				_, ok = pac.Peek("jumbo")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)

				probe.Set(0.80)
				advancedEval := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEval)
			})
		}
	})
}

func TestCompaction_NetByteReductionBelowChurnFloor(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("FullDrainBelow64EntriesWithNetByteReductionAdvancesEpochOnceAndCompacts", func(t *testing.T) {
				// Arrange: Populate cache (maxSize = 100B) with 10 entries of 10B each (100B total, peakLen = 10 < 64)
				// at healthy pressure (0.10).
				probe := newPressureProbe(0.10)
				pac := b.fn(
					100,
					WithInvariantChecking(true),
					probe.Option(),
				).(PressureAwareCache[testData])
				for i := range 10 {
					_, err := pac.Put(fmt.Sprintf("k-%02d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Raise pressure to Tier 1 (0.80) and insert a new 95B entry ("new-large").
				probe.Set(0.80)
				var evicted []testData
				var err error
				advancedPut := observeEpochAdvance(t, probe, pac, func() {
					evicted, err = pac.Put("new-large", testData{value: 99, dataSize: 95})
				})
				require.NoError(t, err)
				require.Len(t, evicted, 10)
				assert.True(t, advancedPut)
				assertAlreadyCompacted(t, pac, probe)

				advancedEval := observeEpochAdvance(t, probe, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEval)
			})

			t.Run("PartialDrainToTwoSurvivorsWithNetByteReductionCompactsDirtyStateInPutAndReplaceGrow", func(t *testing.T) {
				// Arrange: Populate two caches (maxSize = 100B) with 4 entries:
				// "old1" (30B), "old2" (30B), "keep1" (20B), "keep2" (20B) = 100B total (peakLen = 4 <= 8).
				probePut := newPressureProbe(0.10)
				probeUpdate := newPressureProbe(0.10)
				cachePut := b.fn(100, WithInvariantChecking(true), probePut.Option()).(PressureAwareCache[testData])
				cacheUpdate := b.fn(100, WithInvariantChecking(true), probeUpdate.Option()).(PressureAwareCache[testData])

				for _, c := range []Cache[testData]{cachePut, cacheUpdate} {
					_, err := c.Put("old1", testData{value: 1, dataSize: 30})
					require.NoError(t, err)
					_, err = c.Put("old2", testData{value: 2, dataSize: 30})
					require.NoError(t, err)
					_, err = c.Put("keep1", testData{value: 3, dataSize: 20})
					require.NoError(t, err)
					_, err = c.Put("keep2", testData{value: 4, dataSize: 20})
					require.NoError(t, err)
				}

				// Act: Under Tier 1 pressure (0.80), achieve net byte reduction (100B -> 90B) via Put and Replace.
				probePut.Set(0.80)
				probeUpdate.Set(0.80)

				advancedPut := observeEpochAdvance(t, probePut, cachePut, func() {
					_, err := cachePut.Put("new", testData{value: 5, dataSize: 50})
					require.NoError(t, err)
				})
				advancedUpdate := observeEpochAdvance(t, probeUpdate, cacheUpdate, func() {
					require.NoError(t, cacheUpdate.Replace("keep2", testData{value: 44, dataSize: 70}))
				})

				// Assert: Both Put and Replace advanced reclamation epoch AND compacted dirty state.
				assert.True(t, advancedPut)
				assert.True(t, advancedUpdate)
				assertAlreadyCompacted(t, cachePut, probePut)
				assertAlreadyCompacted(t, cacheUpdate, probeUpdate)

				advancedEvalPut := observeEpochAdvance(t, probePut, cachePut, func() {
					cachePut.EvaluateMemoryPressure()
				})
				advancedEvalUpdate := observeEpochAdvance(t, probeUpdate, cacheUpdate, func() {
					cacheUpdate.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEvalPut)
				assert.False(t, advancedEvalUpdate)
			})

			t.Run("Tier2SmallCachePartialDrainCompactsDirtyStateWhileLargeCacheSub25PercentEvictionDefersCompaction", func(t *testing.T) {
				// Part 1: Small cache (peakEntryLen = 4 <= 8) under Tier 2 (0.95) with RetentionRatio = 0.90 (targetSize = 90B).
				// Evicting 2 of 4 entries (50% >= 25% shrinkage) during net-byte-reducing Put / Replace (100B -> 90B)
				// must compact dirty state inline so subsequent Tier 1 EvaluateMemoryPressure() is a no-op.
				probePut := newPressureProbe(0.10)
				probeUpdate := newPressureProbe(0.10)
				cachePut := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.90),
					probePut.Option(),
				).(PressureAwareCache[testData])
				cacheUpdate := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.90),
					probeUpdate.Option(),
				).(PressureAwareCache[testData])

				for _, c := range []Cache[testData]{cachePut, cacheUpdate} {
					_, err := c.Put("old1", testData{value: 1, dataSize: 30})
					require.NoError(t, err)
					_, err = c.Put("old2", testData{value: 2, dataSize: 30})
					require.NoError(t, err)
					_, err = c.Put("keep1", testData{value: 3, dataSize: 20})
					require.NoError(t, err)
					_, err = c.Put("keep2", testData{value: 4, dataSize: 20})
					require.NoError(t, err)
				}

				probePut.Set(0.95)
				probeUpdate.Set(0.95)

				advancedPut := observeEpochAdvance(t, probePut, cachePut, func() {
					_, err := cachePut.Put("new", testData{value: 5, dataSize: 50})
					require.NoError(t, err)
				})
				advancedUpdate := observeEpochAdvance(t, probeUpdate, cacheUpdate, func() {
					require.NoError(t, cacheUpdate.Replace("keep2", testData{value: 44, dataSize: 70}))
				})
				assert.True(t, advancedPut)
				assert.True(t, advancedUpdate)
				assertAlreadyCompacted(t, cachePut, probePut)
				assertAlreadyCompacted(t, cacheUpdate, probeUpdate)

				probePut.Set(0.80)
				probeUpdate.Set(0.80)
				advancedEvalPut := observeEpochAdvance(t, probePut, cachePut, func() {
					cachePut.EvaluateMemoryPressure()
				})
				advancedEvalUpdate := observeEpochAdvance(t, probeUpdate, cacheUpdate, func() {
					cacheUpdate.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEvalPut)
				assert.False(t, advancedEvalUpdate)

				// Part 2: Large cache (100 entries: one 10B tail entry + 99 entries of 1B = 109B in 109B maxSize,
				// RetentionRatio = 0.99 -> targetSize = 107B) under Tier 2 (0.95).
				// Evicting only the single 10B tail entry (1 of 100 entries = 1% < 25% shrinkage) to insert an 8B entry
				// reduces live bytes (109B -> 107B) and advances reclaimEpoch, but must NOT thrash O(N) compaction inline.
				probeLarge := newPressureProbe(0.10)
				cacheLarge := b.fn(
					109,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.99),
					probeLarge.Option(),
				).(PressureAwareCache[testData])

				_, err := cacheLarge.Put("tail-10", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				for i := range 99 {
					_, err = cacheLarge.Put(fmt.Sprintf("k-%03d", i), testData{value: int64(i), dataSize: 1})
					require.NoError(t, err)
				}

				probeLarge.Set(0.95)
				advancedLargePut := observeEpochAdvance(t, probeLarge, cacheLarge, func() {
					evicted, err := cacheLarge.Put("new-8", testData{value: 99, dataSize: 8})
					require.NoError(t, err)
					require.Len(t, evicted, 1)
				})
				assert.True(t, advancedLargePut)

				// Because 1% eviction (< 25%) did not run O(N) compaction inline, an explicit Compact() reclaims the
				// 1 deleted slot and advances reclaimEpoch.
				probeLarge.Set(0.10)
				advancedExplicitCompact := observeEpochAdvance(t, probeLarge, cacheLarge, func() {
					cacheLarge.Compact()
				})
				assert.True(t, advancedExplicitCompact)
			})

			t.Run("PutSmallerOverwriteInvalidatesConcurrentInFlightSampleAndCompacts", func(t *testing.T) {
				// Part 1: Overwriting an existing key with a smaller value under Tier 1 (0.80) or Tier 2 (0.95)
				// without evicting any entries (["k1": 30B, "k2": 50B] -> ["k1": 30B, "k2": 10B], 80B -> 40B <= targetSize 50B)
				// must advance reclaimEpoch so an in-flight EvaluateMemoryPressure() re-samples pressure.
				for _, pressure := range []float64{0.80, 0.95} {
					probe := newPressureProbe(0.10)
					pac := b.fn(
						100,
						WithInvariantChecking(true),
						WithEvictionThreshold(0.90),
						WithEvictionRetentionRatio(0.50),
						probe.Option(),
					).(PressureAwareCache[testData])

					_, err := pac.Put("k1", testData{value: 1, dataSize: 30})
					require.NoError(t, err)
					_, err = pac.Put("k2", testData{value: 2, dataSize: 50})
					require.NoError(t, err)

					probe.Set(pressure)
					advanced := observeEpochAdvance(t, probe, pac, func() {
						evicted, err := pac.Put("k2", testData{value: 22, dataSize: 10})
						require.NoError(t, err)
						assert.Empty(t, evicted)
					})
					assert.True(t, advanced)
					_, ok := pac.Peek("k1")
					assert.True(t, ok)
					_, ok = pac.Peek("k2")
					assert.True(t, ok)
				}

				// Part 2: When >= 25% churn exists (4 entries inserted, 1 erased -> 3 survivors, deletedSinceCompact*4 >= peakEntryLen)
				// under Tier 1 (0.80), overwriting one of the surviving keys with a smaller value compacts dirty state inline.
				probeChurn := newPressureProbe(0.10)
				cacheChurn := b.fn(1000, WithInvariantChecking(true), probeChurn.Option()).(PressureAwareCache[testData])
				for _, k := range []string{"del/00", "keep/01", "keep/02"} {
					_, err := cacheChurn.Put(k, testData{value: 1, dataSize: 10})
					require.NoError(t, err)
				}
				_, err := cacheChurn.Put("k1", testData{value: 1, dataSize: 50})
				require.NoError(t, err)
				_, ok := cacheChurn.Delete("del/00")
				require.True(t, ok)

				probeChurn.Set(0.80)
				advancedChurn := observeEpochAdvance(t, probeChurn, cacheChurn, func() {
					evicted, err := cacheChurn.Put("k1", testData{value: 11, dataSize: 10})
					require.NoError(t, err)
					assert.Empty(t, evicted)
				})
				assert.True(t, advancedChurn)
				assertAlreadyCompacted(t, cacheChurn, probeChurn)

				// Part 3: When a small cache (peakEntryLen = 2 <= 8) has dirty slack down to a sole surviving
				// 50B entry "k1" at normal pressure (0.10), subsequently shrinking "k1" in place to 10B via
				// Put("k1", 10B) (with 0 pre-insert evictions) under Tier 1 (0.80) triggers
				// shouldReclaimSingleSurvivorOnMutation and compacts the dirty slack inline.
				probeSingle := newPressureProbe(0.10)
				cacheSingle := b.fn(1000, WithInvariantChecking(true), probeSingle.Option()).(PressureAwareCache[testData])

				_, err = cacheSingle.Put("del/00", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				_, err = cacheSingle.Put("k1", testData{value: 1, dataSize: 50})
				require.NoError(t, err)
				_, ok = cacheSingle.Delete("del/00")
				require.True(t, ok)

				probeSingle.Set(0.80)
				advancedSingle := observeEpochAdvance(t, probeSingle, cacheSingle, func() {
					evicted, err := cacheSingle.Put("k1", testData{value: 11, dataSize: 10})
					require.NoError(t, err)
					assert.Empty(t, evicted)
				})

				assert.True(t, advancedSingle)
				assertAlreadyCompacted(t, cacheSingle, probeSingle)
			})
		})
	}
}

func TestCompaction_SteadyStateTurnoverAndShrinkageAutoCompaction(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			t.Run("LowChurnTurnoverDoesNotThrashWhileShrinkageAutoCompacts", func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.80)
				c := b.fn(
					100,
					WithInvariantChecking(true),
					probe.Option(),
					WithCompactionThreshold(0.75),
					WithEvictionThreshold(0.90),
				).(PressureAwareCache[testData])

				for i := range 10 {
					_, err := c.Put(fmt.Sprintf("init_%02d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act 1: Perform 30 steady-state capacity-turnover inserts (< 64 churn deletions).
				advancedTurnover := observeEpochAdvance(t, probe, c, func() {
					for i := range 30 {
						_, err := c.Put(fmt.Sprintf("turn_%02d", i), testData{value: int64(i), dataSize: 10})
						require.NoError(t, err)
					}
				})

				// Assert 1: Zero compactions occurred during low-churn capacity turnover.
				assert.False(t, advancedTurnover)

				// Act 2: Delete 3 entries so live entry count drops from 10 to 7 (30% shrinkage >= 25%).
				_, ok := c.Delete("turn_27")
				require.True(t, ok)
				_, ok = c.Delete("turn_28")
				require.True(t, ok)
				advancedShrinkage := observeEpochAdvance(t, probe, c, func() {
					_, ok := c.Delete("turn_29")
					require.True(t, ok)
				})

				// Assert 2: Auto-compaction triggered on >= 25% shrinkage, leaving 7 entries and clean state.
				assert.True(t, advancedShrinkage)
				_, ok = c.Peek("turn_20")
				assert.True(t, ok)
				_, ok = c.Peek("turn_29")
				assert.False(t, ok)
				assertAlreadyCompacted(t, c, probe)
			})

			t.Run("HighChurnDeletedSinceCompactTriggersTier1AutoCompaction", func(t *testing.T) {
				// Arrange: Populate 100 keys (10B each = 1000B in 1000B cache) at low pressure (0.10),
				// then churn 100 new keys so live count stays 100 while accumulating 100 churn deletions (>= 64).
				probe := newPressureProbe(0.10)
				c := b.fn(
					1000,
					WithInvariantChecking(true),
					WithCompactionThreshold(0.75),
					WithEvictionThreshold(0.90),
					probe.Option(),
				).(PressureAwareCache[testData])

				for i := range 100 {
					_, err := c.Put(fmt.Sprintf("init-%03d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				for i := range 100 {
					_, err := c.Put(fmt.Sprintf("churn-%03d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Raise pressure to Tier 1 (0.80) and perform a write.
				probe.Set(0.80)
				var err error
				advanced := observeEpochAdvance(t, probe, c, func() {
					_, err = c.Put("trigger", testData{value: 999, dataSize: 10})
				})

				// Assert: High-churn tombstone bloat triggers Tier 1 auto-compaction.
				require.NoError(t, err)
				assert.True(t, advanced)
				assertAlreadyCompacted(t, c, probe)
			})
		})
	}
}

func TestObserveEpochAdvance_ArbitraryValueType(t *testing.T) {
	for _, b := range allBackends[uint64]() {
		t.Run(b.name, func(t *testing.T) {
			// Arrange: Instantiate a generic cache with a custom value type (uint64) not previously enumerated.
			probe := newPressureProbe(0.10)
			pac := b.fn(10, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[uint64])
			_, err := pac.Put("k1", 42)
			require.NoError(t, err)
			_, ok := pac.Delete("k1")
			require.True(t, ok)

			// Act: Put a new entry and compact via generic observeEpochAdvance[uint64] and assertAlreadyCompacted[uint64].
			_, err = pac.Put("k2", 100)
			require.NoError(t, err)
			_, err = pac.Put("k3", 200)
			require.NoError(t, err)
			_, ok = pac.Delete("k2")
			require.True(t, ok)

			advanced := observeEpochAdvance(t, probe, pac, func() {
				pac.Compact()
			})

			// Assert
			assert.True(t, advanced)
			assertAlreadyCompacted(t, pac, probe)
		})
	}
}
