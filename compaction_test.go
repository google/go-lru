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
// and when a pressureProbe is provided, also verifies via !probe.ObserveEpochAdvance(tb, pac, compactOnce)
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
			assert.False(tb, probe.ObserveEpochAdvance(tb, pac, compactOnce), "expected cache to already be compacted (first Compact call must not advance reclaimEpoch)")
			hasProbe = true
		}
	}
	if !hasProbe {
		compactOnce()
	}
	assert.Equal(tb, uint64(0), after.Mallocs-before.Mallocs, "expected cache to already be compacted (0 allocations on Compact)")
}

// pressureProbe provides a public-API PressureFunc callback that allows tests to dynamically
// update simulated memory pressure and observe via ObserveEpochAdvance whether an
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

// ObserveEpochAdvance executes op() while a background EvaluateMemoryPressure() call
// holds a pre-operation epoch snapshot inside PressureFunc, returning true if and only if op()
// advanced the cache's reclamation epoch (causing lockWithPressure to re-sample PressureFunc).
func (p *pressureProbe) ObserveEpochAdvance(tb testing.TB, pac any, op func()) bool {
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

		switch c := pac.(type) {
		case PressureAwareCache[testData]:
			_ = c.EvaluateMemoryPressure()
		case PressureAwareCache[string]:
			_ = c.EvaluateMemoryPressure()
		case PressureAwareCache[int]:
			_ = c.EvaluateMemoryPressure()
		default:
			panic(fmt.Sprintf("unsupported PressureAwareCache type %T", pac))
		}

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

func testBackends() []struct {
	name string
	fn   func(uint64, ...Option) Cache[testData]
} {
	return []struct {
		name string
		fn   func(uint64, ...Option) Cache[testData]
	}{
		{"MapCache", func(maxSize uint64, opts ...Option) Cache[testData] {
			return NewMapCache[testData](maxSize, append([]Option{testDataWeigher}, opts...)...)
		}},
		{"RadixCache", func(maxSize uint64, opts ...Option) Cache[testData] {
			return NewRadixCache[testData](maxSize, append([]Option{testDataWeigher}, opts...)...)
		}},
		{"ArenaRadixCache", func(maxSize uint64, opts ...Option) Cache[testData] {
			return NewArenaRadixCache[testData](maxSize, append([]Option{testDataWeigher}, opts...)...)
		}},
	}
}

func TestCompaction_EmptyDrainAndPreInsertSlackReclamation(t *testing.T) {
	t.Run("SequentialEraseDrainToEmptyReleasesPeakSlack", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.10)
				pac := b.fn(100000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

				for i := range 200 {
					k := fmt.Sprintf("item/sub/%04d", i)
					_, err := pac.Insert(k, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Drain all entries via individual Erase(key) calls down to 0 entries.
				for i := range 200 {
					k := fmt.Sprintf("item/sub/%04d", i)
					_, ok := pac.Erase(k)
					require.True(t, ok)
				}

				// Assert: Peak structures are released upon reaching empty state.
				_, ok := pac.LookUpWithoutChangingOrder("item/sub/0000")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})

	t.Run("PreInsertDrainReleasesPeakSlackAtNormalPressure", func(t *testing.T) {
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
					_, err := c.Insert(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Insert a single 1000B jumbo entry that pre-evicts all 100 entries down to empty before inserting.
				samplesBefore := sampleCount.Load()
				evicted, err := c.Insert("jumbo", testData{value: 999, dataSize: 1000})

				// Assert: All 100 entries were evicted, peak slack was released, and only 1 pressure sample ran.
				require.NoError(t, err)
				assert.Len(t, evicted, 100)
				assert.Equal(t, int32(1), sampleCount.Load()-samplesBefore)
				_, ok := c.LookUpWithoutChangingOrder("dir_00/sub_00/file_000")
				assert.False(t, ok)
				_, ok = c.LookUpWithoutChangingOrder("jumbo")
				assert.True(t, ok)
				assertAlreadyCompacted(t, c)
			})
		}
	})

	t.Run("PreInsertEmptyDrainUnderTier1PressureAdvancesReclaimEpochAcrossBackends", func(t *testing.T) {
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
					_, err := pac.Insert(fmt.Sprintf("dir_%02d/k_%03d", i%10, i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Raise pressure to Tier 1 (0.80) and insert a 1000B entry that pre-evicts all 70 keys.
				probe.Set(0.80)
				var evicted []testData
				var err error
				advanced := probe.ObserveEpochAdvance(t, pac, func() {
					evicted, err = pac.Insert("jumbo", testData{value: 999, dataSize: 1000})
				})

				// Assert
				require.NoError(t, err)
				assert.Len(t, evicted, 70)
				assert.True(t, advanced)
				_, ok := pac.LookUpWithoutChangingOrder("jumbo")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})

	t.Run("NetByteReductionPreInsertEvictionUnderTier1PressureAdvancesReclaimEpoch", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				// Arrange: Fill a 100B cache with "k1" (60B) and "k2" (40B) = 100B at Tier 1 pressure (0.80).
				probe := newPressureProbe(0.80)
				pac := b.fn(
					100,
					WithInvariantChecking(true),
					probe.Option(),
				).(PressureAwareCache[testData])
				_, err := pac.Insert("k1", testData{value: 1, dataSize: 60})
				require.NoError(t, err)
				_, err = pac.Insert("k2", testData{value: 2, dataSize: 40})
				require.NoError(t, err)

				// Act: Insert "k3" (50B), which pre-evicts "k1" (60B) so net currentSize decreases from 100B to 90B.
				var evicted []testData
				advanced := probe.ObserveEpochAdvance(t, pac, func() {
					evicted, err = pac.Insert("k3", testData{value: 3, dataSize: 50})
				})

				// Assert: Net byte reduction via pre-insert eviction under elevated pressure advances reclamation epoch and compacts.
				require.NoError(t, err)
				require.Len(t, evicted, 1)
				assert.True(t, advanced)
				_, ok := pac.LookUpWithoutChangingOrder("k1")
				assert.False(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("k2")
				assert.True(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("k3")
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
					_, err := pac.Insert(fmt.Sprintf("k/%06d", i), testData{value: int64(i), dataSize: 1})
					require.NoError(t, err)
				}

				// Act
				evicted, err := pac.Insert("jumbo", testData{value: 999, dataSize: n})
				require.NoError(t, err)

				// Assert: All 200 entries were evicted, only "jumbo" remains, and cache is already compacted without peak slack.
				assert.Len(t, evicted, n)
				_, ok := pac.LookUp("k/000000")
				assert.False(t, ok)
				_, ok = pac.LookUp("jumbo")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})
}

func TestCompaction_EraseEmptyPrefixResetsSlack(t *testing.T) {
	for _, b := range testBackends() {
		t.Run(b.name, func(t *testing.T) {
			// Arrange
			probe := newPressureProbe(0.10)
			pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
			for i := range 100 {
				_, err := pac.Insert(fmt.Sprintf("dir/sub/%03d", i), testData{value: int64(i), dataSize: 10})
				require.NoError(t, err)
			}

			// Act
			pac.EraseEntriesWithGivenPrefix("")

			// Assert: All entries are removed, backing structures are reset/compacted, and full capacity is available.
			_, ok := pac.LookUpWithoutChangingOrder("dir/sub/000")
			assert.False(t, ok)
			_, ok = pac.LookUpWithoutChangingOrder("dir/sub/099")
			assert.False(t, ok)
			assertAlreadyCompacted(t, pac, probe)

			evicted, err := pac.Insert("full_capacity", testData{value: 999, dataSize: 1000})
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
					_, err := pac.Insert(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Overwrite existing key "k-000" with a 1000B value, evicting all 69 other entries down to 1 entry.
				evicted, err := pac.Insert("k-000", testData{value: 999, dataSize: 1000})
				require.NoError(t, err)

				// Assert
				assert.Len(t, evicted, 69)
				_, ok := pac.LookUpWithoutChangingOrder("k-000")
				assert.True(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("k-001")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})

			t.Run("UpdateGrowEvictingAllOtherEntriesReleasesPeakSlack", func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range 70 {
					key := fmt.Sprintf("k-%03d", i)
					_, err := pac.Insert(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Grow MRU key "k-069" to 1000B, evicting all 69 older entries down to 1 entry.
				err := pac.UpdateWithoutChangingOrder("k-069", testData{value: 690, dataSize: 1000})

				// Assert
				require.NoError(t, err)
				_, ok := pac.LookUpWithoutChangingOrder("k-069")
				assert.True(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("k-000")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})

			t.Run("InsertNewKeyWithSurvivingZeroSizeMRUEntryReclaimsPeakSlack", func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range 70 {
					key := fmt.Sprintf("k-%03d", i)
					_, err := pac.Insert(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				_, err := pac.Insert("z_head", testData{value: 0, dataSize: 0})
				require.NoError(t, err)

				// Act
				_, err = pac.Insert("jumbo", testData{value: 999, dataSize: 1000})
				require.NoError(t, err)

				// Assert
				_, ok := pac.LookUpWithoutChangingOrder("z_head")
				assert.True(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("jumbo")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})

			t.Run("EraseEntriesWithGivenPrefixDrainingToOneSurvivorReclaimsPeakSlack", func(t *testing.T) {
				// Arrange
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range 70 {
					key := fmt.Sprintf("batch/k-%03d", i)
					_, err := pac.Insert(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				_, err := pac.Insert("keep/1", testData{value: 1, dataSize: 10})
				require.NoError(t, err)

				// Act
				pac.EraseEntriesWithGivenPrefix("batch/")

				// Assert
				_, ok := pac.LookUpWithoutChangingOrder("keep/1")
				assert.True(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("batch/k-000")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})

			t.Run("EraseAndUpdateSelfEvictionDrainingToOneSurvivorReclaimPeakSlack", func(t *testing.T) {
				// Arrange: Insert "item-069" first (at LRU tail) followed by "item-000".."item-068" (so item-068 is MRU).
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				_, err := pac.Insert("item-069", testData{value: 69, dataSize: 10})
				require.NoError(t, err)
				for i := range 69 {
					key := fmt.Sprintf("item-%03d", i)
					_, err := pac.Insert(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				for i := range 68 {
					key := fmt.Sprintf("item-%03d", i)
					_, ok := pac.Erase(key)
					require.True(t, ok)
				}

				// Act: Grow tail entry "item-069" to 1000B while "item-068" (10B) is newer -> self-evicts "item-069".
				err = pac.UpdateWithoutChangingOrder("item-069", testData{value: 690, dataSize: 1000})
				require.NoError(t, err)

				// Assert
				_, ok := pac.LookUpWithoutChangingOrder("item-068")
				assert.True(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("item-069")
				assert.False(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})

			t.Run("Exact64EntriesDrainedToOneSurvivorAndHighChurnTombstoneDrain", func(t *testing.T) {
				// Arrange 1: 64 entries drained to 1 survivor via overwrite.
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
				for i := range 64 {
					key := string([]byte{byte(i + 1), 'k'})
					_, err := pac.Insert(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				firstKey := string([]byte{1, 'k'})
				_, err := pac.Insert(firstKey, testData{value: 999, dataSize: 1000})
				require.NoError(t, err)

				_, ok := pac.LookUpWithoutChangingOrder(firstKey)
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)

				// Arrange 2: High-churn tombstone drain to empty or single survivor.
				probeEmpty := newPressureProbe(0.10)
				pacEmpty := b.fn(300, WithInvariantChecking(true), probeEmpty.Option()).(PressureAwareCache[testData])
				for i := range 100 {
					key := string([]byte{byte(i + 1), 'k'})
					_, err = pacEmpty.Insert(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				probeOne := newPressureProbe(0.10)
				pacOne := b.fn(1000, WithInvariantChecking(true), probeOne.Option()).(PressureAwareCache[testData])
				_, err = pacOne.Insert("survivor", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				for i := range 50 {
					key := fmt.Sprintf("batch1-%02d", i)
					_, err = pacOne.Insert(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				for i := range 50 {
					key := fmt.Sprintf("batch1-%02d", i)
					_, ok := pacOne.Erase(key)
					require.True(t, ok)
				}
				for i := range 50 {
					key := fmt.Sprintf("batch2-%02d", i)
					_, err = pacOne.Insert(key, testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				_, err = pacEmpty.Insert("jumbo", testData{value: 999, dataSize: 300})
				require.NoError(t, err)

				pacOne.EraseEntriesWithGivenPrefix("batch2-")

				_, ok = pacEmpty.LookUpWithoutChangingOrder("jumbo")
				assert.True(t, ok)
				_, ok = pacOne.LookUpWithoutChangingOrder("survivor")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pacEmpty, probeEmpty)
				assertAlreadyCompacted(t, pacOne, probeOne)
			})
		})
	}
}

func TestCompaction_CrossBackendParityOnSingleSurvivorAndShrinkageWatermarks(t *testing.T) {
	t.Run("EraseDrainingPeakEntriesToSingleSurvivorAdvancesReclaimEpoch", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

				for i := range 70 {
					_, err := pac.Insert(fmt.Sprintf("z-%03d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}
				for i := range 68 {
					_, ok := pac.Erase(fmt.Sprintf("z-%03d", i))
					require.True(t, ok)
				}

				// Act: Raise pressure to Tier 1 (0.80) and erase "z-068", draining from 70 peak entries to 1 survivor.
				probe.Set(0.80)
				advanced := probe.ObserveEpochAdvance(t, pac, func() {
					_, ok := pac.Erase("z-068")
					require.True(t, ok)
				})

				// Assert
				assert.True(t, advanced)
				_, ok := pac.LookUpWithoutChangingOrder("z-069")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})

	t.Run("UpdateSelfEvictionDrainingPeakEntriesToSingleSurvivorClampsZeroWatermark", func(t *testing.T) {
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
					_, err := pac.Insert(fmt.Sprintf("init-z-%02d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}
				probe.Set(0.95)
				require.Len(t, pac.EvaluateMemoryPressure(), 5)

				probe.Set(0.10)
				_, err := pac.Insert("p-069", testData{value: 69, dataSize: 10})
				require.NoError(t, err)
				for i := range 64 {
					_, err = pac.Insert(fmt.Sprintf("z-%03d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}

				for i := 5; i < 10; i++ {
					_, ok := pac.Erase(fmt.Sprintf("init-z-%02d", i))
					require.True(t, ok)
				}
				for i := range 63 {
					_, ok := pac.Erase(fmt.Sprintf("z-%03d", i))
					require.True(t, ok)
				}

				// Erase "p-069" under Tier 1 pressure, leaving only 1 zero-size entry ("z-063") from 70 peak entries.
				probe.Set(0.80)
				advanced := probe.ObserveEpochAdvance(t, pac, func() {
					_, ok := pac.Erase("p-069")
					require.True(t, ok)
				})
				assert.True(t, advanced)
				assertAlreadyCompacted(t, pac, probe)

				// Insert 1 more zero-size entry ("z-064", total 2 zero-size entries) and evaluate under Tier 2 (0.95).
				// Because the zero-size watermark was clamped to 1 during single-survivor drain, 1 of the 2 zero-size entries is shed.
				_, err = pac.Insert("z-064", testData{value: 64, dataSize: 0})
				require.NoError(t, err)
				probe.Set(0.95)
				evicted := pac.EvaluateMemoryPressure()
				assert.Len(t, evicted, 1)
				_, ok := pac.LookUpWithoutChangingOrder("z-063")
				assert.False(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("z-064")
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
					_, err := pac.Insert(fmt.Sprintf("k-%03d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Trigger small Tier 2 shedding (evicts 2 of 65 entries, < 25% shrinkage),
				// then erase remaining entries down to 1 survivor under Tier 1 (0.80).
				probe.Set(0.95)
				_, err := pac.Insert("k-064", testData{value: 64, dataSize: 10})
				require.NoError(t, err)

				probe.Set(0.10)
				for i := 2; i < 63; i++ {
					_, ok := pac.Erase(fmt.Sprintf("k-%03d", i))
					require.True(t, ok)
				}

				probe.Set(0.80)
				advanced := probe.ObserveEpochAdvance(t, pac, func() {
					_, ok := pac.Erase("k-063")
					require.True(t, ok)
				})

				// Assert
				assert.True(t, advanced)
				_, ok := pac.LookUpWithoutChangingOrder("k-064")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})

	t.Run("ValueBearingInternalNodeShrinkage", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(10000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

				for i := range 20 {
					parentKey := fmt.Sprintf("p%02d", i)
					childA := fmt.Sprintf("p%02d/a", i)
					childB := fmt.Sprintf("p%02d/b", i)
					for _, k := range []string{parentKey, childA, childB} {
						_, err := pac.Insert(k, testData{value: int64(i), dataSize: 10})
						require.NoError(t, err)
					}
				}

				for i := range 20 {
					_, ok := pac.Erase(fmt.Sprintf("p%02d", i))
					require.True(t, ok)
				}

				// Act: Raise pressure to Tier 1 (0.80) and call EvaluateMemoryPressure().
				probe.Set(0.80)
				advanced := probe.ObserveEpochAdvance(t, pac, func() {
					pac.EvaluateMemoryPressure()
				})

				// Assert
				assert.True(t, advanced)
				assertAlreadyCompacted(t, pac, probe)
			})
		}
	})

	t.Run("Tier2ShedAndAutoCompactAdvancesReclaimEpochOncePerOperation", func(t *testing.T) {
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
					_, err := pac.Insert(fmt.Sprintf("k-%02d", i), testData{value: int64(i), dataSize: 50})
					require.NoError(t, err)
				}

				// Act
				probe.Set(0.95)
				var evicted []testData
				advanced := probe.ObserveEpochAdvance(t, pac, func() {
					evicted = pac.EvaluateMemoryPressure()
				})

				// Assert: Tier 2 shed+compact evicted 10 entries, compacted slack, and an immediate repeat is a no-op.
				require.Len(t, evicted, 10)
				assert.True(t, advanced)
				assertAlreadyCompacted(t, pac, probe)

				advancedRepeat := probe.ObserveEpochAdvance(t, pac, func() {
					assert.Empty(t, pac.EvaluateMemoryPressure())
				})
				assert.False(t, advancedRepeat)
			})
		}
	})

	t.Run("Exact25PercentShrinkageAutoCompactionParityAcrossBackends", func(t *testing.T) {
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
					_, err := pac.Insert(fmt.Sprintf("k%02d", i), testData{value: int64(i), dataSize: 0})
					require.NoError(t, err)
				}
				for i := range 4 {
					_, ok := pac.Erase(fmt.Sprintf("k%02d", i))
					require.True(t, ok)
				}
				probe.Set(0.80)

				// Act 1: 5th deletion under Tier 1 pressure reaches exact 25% shrinkage from peak (20 -> 15).
				advancedFifth := probe.ObserveEpochAdvance(t, pac, func() {
					_, ok := pac.Erase("k04")
					require.True(t, ok)
				})

				// Assert 1
				assert.True(t, advancedFifth)
				assertAlreadyCompacted(t, pac, probe)

				// Act 2: Erase 1 more zero-byte key ("k05", 15 -> 14, 1/15 < 25% shrinkage) at Tier 1 pressure;
				// because watermarks were reset to 15 on the 5th deletion, it does not re-compact.
				advancedSixth := probe.ObserveEpochAdvance(t, pac, func() {
					_, ok := pac.Erase("k05")
					require.True(t, ok)
				})
				assert.False(t, advancedSixth)
			})
		}
	})

	t.Run("SmallCacheWithRoutingNodesRespects8EntryHysteresis", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				// Arrange: Insert 6 zero-byte hierarchical keys (6 entries <= 8 hysteresis floor, even though the radix tree
				// allocates 10 arena nodes > 8 due to root + 3 intermediate routing nodes "a/", "b/", "c/").
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

				for _, k := range []string{"a/1", "a/2", "b/1", "b/2", "c/1", "c/2"} {
					_, err := pac.Insert(k, testData{value: 1, dataSize: 0})
					require.NoError(t, err)
				}

				// Act: Under Tier 1 pressure (0.80), erase 2 zero-byte keys ("a/1", "a/2") while observing epoch advancement.
				probe.Set(0.80)
				advanced := probe.ObserveEpochAdvance(t, pac, func() {
					_, ok := pac.Erase("a/1")
					require.True(t, ok)
					_, ok = pac.Erase("a/2")
					require.True(t, ok)
				})

				// Assert: Because peak entry count is 6 <= 8 (below the 8-entry small-cache hysteresis floor) and 0 bytes were freed,
				// all backends refrain from auto-compacting or advancing reclamation epoch.
				assert.False(t, advanced)
				_, ok := pac.LookUpWithoutChangingOrder("a/1")
				assert.False(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("a/2")
				assert.False(t, ok)
				for _, k := range []string{"b/1", "b/2", "c/1", "c/2"} {
					_, ok := pac.LookUpWithoutChangingOrder(k)
					assert.True(t, ok)
				}
			})
		}
	})

	t.Run("Tier1AndTier2AutoCompactionParityAcrossBackends", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				t.Run("Tier1InlineShrinkageAndExplicitEvaluateAdvanceEpochAndResetSlack", func(t *testing.T) {
					probe := newPressureProbe(0.10)
					pac := b.fn(10000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

					for i := range 20 {
						_, err := pac.Insert(fmt.Sprintf("k/%02d", i), testData{value: int64(i), dataSize: 10})
						require.NoError(t, err)
					}
					for i := range 5 {
						_, ok := pac.Erase(fmt.Sprintf("k/%02d", i))
						require.True(t, ok)
					}

					// Act 1: Erase the 6th of 20 entries (30% >= 25% shrinkage) under Tier 1 pressure (0.80).
					probe.Set(0.80)
					advancedInline := probe.ObserveEpochAdvance(t, pac, func() {
						_, ok := pac.Erase("k/05")
						require.True(t, ok)
					})
					assert.True(t, advancedInline)
					assertAlreadyCompacted(t, pac, probe)

					advancedFollowUp := probe.ObserveEpochAdvance(t, pac, func() {
						pac.EvaluateMemoryPressure()
					})
					assert.False(t, advancedFollowUp)

					// Act 2: Erase 1 more entry at normal pressure (0.10), then invoke EvaluateMemoryPressure() under Tier 1 (0.80).
					probe.Set(0.10)
					_, ok := pac.Erase("k/06")
					require.True(t, ok)

					probe.Set(0.80)
					advancedExplicit := probe.ObserveEpochAdvance(t, pac, func() {
						pac.EvaluateMemoryPressure()
					})
					assert.True(t, advancedExplicit)
					assertAlreadyCompacted(t, pac, probe)

					advancedSecondEval := probe.ObserveEpochAdvance(t, pac, func() {
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
						_, err := pac.Insert(fmt.Sprintf("k/%02d", i), testData{value: int64(i), dataSize: 10})
						require.NoError(t, err)
					}
					for i := range 6 {
						_, ok := pac.Erase(fmt.Sprintf("k/%02d", i))
						require.True(t, ok)
					}

					// Act: Spike pressure to Tier 2 (0.95) and call EvaluateMemoryPressure() while currentSize (140B) <= targetSize (500B).
					probe.Set(0.95)
					advanced := probe.ObserveEpochAdvance(t, pac, func() {
						pac.EvaluateMemoryPressure()
					})
					assert.True(t, advanced)
					assertAlreadyCompacted(t, pac, probe)

					advancedRepeat := probe.ObserveEpochAdvance(t, pac, func() {
						pac.EvaluateMemoryPressure()
					})
					assert.False(t, advancedRepeat)
				})
			})
		}
	})
}

func TestCompaction_PreReclaimSlackAndEpochConsistencyAcrossMutations(t *testing.T) {
	t.Run("SingleSurvivorDrainDuringInsertDoesNotDoubleAllocateOrRecompactOnEvaluate", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

				for i := range 64 {
					_, err := pac.Insert(fmt.Sprintf("k-%03d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act
				probe.Set(0.80)
				advancedInsert := probe.ObserveEpochAdvance(t, pac, func() {
					_, err := pac.Insert("big", testData{value: 999, dataSize: 990})
					require.NoError(t, err)
				})
				assert.True(t, advancedInsert)
				assertAlreadyCompacted(t, pac, probe)

				advancedEval := probe.ObserveEpochAdvance(t, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEval)
			})
		}
	})

	t.Run("EmptyCacheDrainDoesNotSpuriouslyAdvanceEpochOnZeroByteOrNormalPressure", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				t.Run("ZeroByteLastEntryUnderTier1Pressure", func(t *testing.T) {
					probeErase := newPressureProbe(0.80)
					probeSelfEvict := newPressureProbe(0.80)
					probePrefixEmpty := newPressureProbe(0.80)
					cacheErase := b.fn(100, WithInvariantChecking(true), probeErase.Option()).(PressureAwareCache[testData])
					cacheSelfEvict := b.fn(100, WithInvariantChecking(true), probeSelfEvict.Option()).(PressureAwareCache[testData])
					cachePrefixEmpty := b.fn(100, WithInvariantChecking(true), probePrefixEmpty.Option()).(PressureAwareCache[testData])

					_, err := cacheErase.Insert("z1", testData{value: 1, dataSize: 0})
					require.NoError(t, err)
					_, err = cacheSelfEvict.Insert("z1", testData{value: 1, dataSize: 0})
					require.NoError(t, err)
					_, err = cacheSelfEvict.Insert("mru_full", testData{value: 2, dataSize: 100})
					require.NoError(t, err)
					_, err = cachePrefixEmpty.Insert("z1", testData{value: 1, dataSize: 0})
					require.NoError(t, err)

					advancedErase := probeErase.ObserveEpochAdvance(t, cacheErase, func() {
						_, ok := cacheErase.Erase("z1")
						require.True(t, ok)
					})
					advancedSelfEvict := probeSelfEvict.ObserveEpochAdvance(t, cacheSelfEvict, func() {
						require.NoError(t, cacheSelfEvict.UpdateWithoutChangingOrder("z1", testData{value: 11, dataSize: 10}))
					})
					advancedPrefixEmpty := probePrefixEmpty.ObserveEpochAdvance(t, cachePrefixEmpty, func() {
						cachePrefixEmpty.EraseEntriesWithGivenPrefix("")
					})

					assert.False(t, advancedErase)
					assert.False(t, advancedSelfEvict)
					assert.False(t, advancedPrefixEmpty)
				})

				t.Run("NormalPressureLastEntryEraseAndEmptyPrefixClearDoNotAdvanceEpoch", func(t *testing.T) {
					var eraseSamples, prefixSamples, emptyPrefixSamples atomic.Int32
					cacheErase := b.fn(100, WithInvariantChecking(true), WithPressureFunc(func() float64 {
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

					_, err := cacheErase.Insert("k1", testData{value: 1, dataSize: 10})
					require.NoError(t, err)
					_, err = cachePrefix.Insert("k1", testData{value: 1, dataSize: 10})
					require.NoError(t, err)
					_, err = cachePrefixEmpty.Insert("k1", testData{value: 1, dataSize: 10})
					require.NoError(t, err)

					eraseBefore := eraseSamples.Load()
					_, ok := cacheErase.Erase("k1")
					require.True(t, ok)
					assert.Equal(t, int32(1), eraseSamples.Load()-eraseBefore)

					prefixBefore := prefixSamples.Load()
					cachePrefix.EraseEntriesWithGivenPrefix("k")
					assert.Equal(t, int32(1), prefixSamples.Load()-prefixBefore)

					emptyBefore := emptyPrefixSamples.Load()
					cachePrefixEmpty.EraseEntriesWithGivenPrefix("")
					assert.Equal(t, int32(0), emptyPrefixSamples.Load()-emptyBefore)

					_, ok = cacheErase.LookUpWithoutChangingOrder("k1")
					assert.False(t, ok)
					_, ok = cachePrefix.LookUpWithoutChangingOrder("k1")
					assert.False(t, ok)
					_, ok = cachePrefixEmpty.LookUpWithoutChangingOrder("k1")
					assert.False(t, ok)
					assertAlreadyCompacted(t, cacheErase)
					assertAlreadyCompacted(t, cachePrefix)
					assertAlreadyCompacted(t, cachePrefixEmpty)
				})

				t.Run("FreshCacheAndEmptyPrefixClearAreAlreadyCompactedAndDoNotAdvanceEpoch", func(t *testing.T) {
					probe := newPressureProbe(0.80)
					pac := b.fn(100, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])

					assertAlreadyCompacted(t, pac, probe)
					advancedFreshEval := probe.ObserveEpochAdvance(t, pac, func() {
						pac.EvaluateMemoryPressure()
					})
					assert.False(t, advancedFreshEval)

					// Populate and clear via EraseEntriesWithGivenPrefix(""); resulting empty cache must already be compacted.
					probe.Set(0.10)
					for i := range 10 {
						_, err := pac.Insert(fmt.Sprintf("k-%02d", i), testData{value: int64(i), dataSize: 5})
						require.NoError(t, err)
					}
					probe.Set(0.80)
					advancedClear := probe.ObserveEpochAdvance(t, pac, func() {
						pac.EraseEntriesWithGivenPrefix("")
					})
					assert.True(t, advancedClear)
					assertAlreadyCompacted(t, pac, probe)

					advancedPostClearEval := probe.ObserveEpochAdvance(t, pac, func() {
						pac.EvaluateMemoryPressure()
					})
					assert.False(t, advancedPostClearEval)
				})

				t.Run("Draining20ZeroSizeEntriesToEmptyUnderTier1AdvancesEpoch", func(t *testing.T) {
					probe := newPressureProbe(0.10)
					pac := b.fn(1000, WithInvariantChecking(true), probe.Option()).(PressureAwareCache[testData])
					for i := range 20 {
						_, err := pac.Insert(fmt.Sprintf("z/%02d", i), testData{value: int64(i), dataSize: 0})
						require.NoError(t, err)
					}

					probe.Set(0.80)
					advanced := probe.ObserveEpochAdvance(t, pac, func() {
						pac.EraseEntriesWithGivenPrefix("z/")
					})
					assert.True(t, advanced)
					_, ok := pac.LookUpWithoutChangingOrder("z/00")
					assert.False(t, ok)
					assertAlreadyCompacted(t, pac, probe)
				})
			})
		}
	})

	t.Run("InsertNewKeySingleSurvivorPreCompactionDirtiedByTier2Shed", func(t *testing.T) {
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
					_, err := pac.Insert(fmt.Sprintf("e/%02d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				_, err := pac.Insert("survivor", testData{value: 1, dataSize: 5})
				require.NoError(t, err)

				probe.Set(0.95)
				advancedInsert := probe.ObserveEpochAdvance(t, pac, func() {
					_, err = pac.Insert("jumbo", testData{value: 999, dataSize: 995})
					require.NoError(t, err)
				})
				assert.True(t, advancedInsert)
				assertAlreadyCompacted(t, pac, probe)

				probe.Set(0.80)
				advancedEval := probe.ObserveEpochAdvance(t, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEval)
			})
		}
	})

	t.Run("OverwriteAndUpdateGrowPreReclaimSlackAndZeroSizeSurvivor", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				t.Run("OverwriteAndUpdateGrowToSingleSurvivorUnderTier1DoNotDoubleAdvanceEpoch", func(t *testing.T) {
					probeOverwrite := newPressureProbe(0.10)
					probeUpdate := newPressureProbe(0.10)
					cacheOverwrite := b.fn(100, WithInvariantChecking(true), probeOverwrite.Option()).(PressureAwareCache[testData])
					cacheUpdateSize := b.fn(100, WithInvariantChecking(true), probeUpdate.Option()).(PressureAwareCache[testData])

					for _, c := range []Cache[testData]{cacheOverwrite, cacheUpdateSize} {
						_, err := c.Insert("b", testData{value: 2, dataSize: 50})
						require.NoError(t, err)
						_, err = c.Insert("a", testData{value: 1, dataSize: 50})
						require.NoError(t, err)
					}

					probeOverwrite.Set(0.80)
					probeUpdate.Set(0.80)

					advancedOverwrite := probeOverwrite.ObserveEpochAdvance(t, cacheOverwrite, func() {
						_, err := cacheOverwrite.Insert("a", testData{value: 11, dataSize: 90})
						require.NoError(t, err)
					})
					advancedUpdate := probeUpdate.ObserveEpochAdvance(t, cacheUpdateSize, func() {
						require.NoError(t, cacheUpdateSize.UpdateWithoutChangingOrder("a", testData{value: 11, dataSize: 90}))
					})
					assert.True(t, advancedOverwrite)
					assert.True(t, advancedUpdate)
					assertAlreadyCompacted(t, cacheOverwrite, probeOverwrite)
					assertAlreadyCompacted(t, cacheUpdateSize, probeUpdate)

					advancedEvalOverwrite := probeOverwrite.ObserveEpochAdvance(t, cacheOverwrite, func() {
						cacheOverwrite.EvaluateMemoryPressure()
					})
					advancedEvalUpdate := probeUpdate.ObserveEpochAdvance(t, cacheUpdateSize, func() {
						cacheUpdateSize.EvaluateMemoryPressure()
					})
					assert.False(t, advancedEvalOverwrite)
					assert.False(t, advancedEvalUpdate)
				})

				t.Run("OverwriteAndUpdateGrowWithSurvivingZeroSizeEntryReclaim64DeletedSlack", func(t *testing.T) {
					probeOverwrite := newPressureProbe(0.10)
					probeUpdate := newPressureProbe(0.10)
					cacheOverwrite := b.fn(650, WithInvariantChecking(true), probeOverwrite.Option()).(PressureAwareCache[testData])
					cacheUpdateSize := b.fn(650, WithInvariantChecking(true), probeUpdate.Option()).(PressureAwareCache[testData])

					for _, c := range []Cache[testData]{cacheOverwrite, cacheUpdateSize} {
						for i := range 64 {
							_, err := c.Insert(fmt.Sprintf("e/%02d", i), testData{value: int64(i), dataSize: 10})
							require.NoError(t, err)
						}
						_, err := c.Insert("target", testData{value: 100, dataSize: 10})
						require.NoError(t, err)
						_, err = c.Insert("zero", testData{value: 0, dataSize: 0})
						require.NoError(t, err)
					}

					_, err := cacheOverwrite.Insert("target", testData{value: 200, dataSize: 650})
					require.NoError(t, err)
					require.NoError(t, cacheUpdateSize.UpdateWithoutChangingOrder("target", testData{value: 200, dataSize: 650}))

					for _, item := range []struct {
						pac   PressureAwareCache[testData]
						probe *pressureProbe
					}{
						{cacheOverwrite, probeOverwrite},
						{cacheUpdateSize, probeUpdate},
					} {
						_, ok := item.pac.LookUpWithoutChangingOrder("target")
						assert.True(t, ok)
						_, ok = item.pac.LookUpWithoutChangingOrder("zero")
						assert.True(t, ok)
						assertAlreadyCompacted(t, item.pac, item.probe)

						item.probe.Set(0.80)
						advancedEval := item.probe.ObserveEpochAdvance(t, item.pac, func() {
							item.pac.EvaluateMemoryPressure()
						})
						assert.False(t, advancedEval)
					}
				})
			})
		}
	})

	t.Run("InsertNewKeyMassPreInsertEvictionWithTwoZeroSizeSurvivorsCompacts", func(t *testing.T) {
		for _, b := range testBackends() {
			t.Run(b.name, func(t *testing.T) {
				probe := newPressureProbe(0.10)
				pac := b.fn(
					640,
					WithInvariantChecking(true),
					probe.Option(),
				).(PressureAwareCache[testData])
				for i := range 64 {
					_, err := pac.Insert(fmt.Sprintf("p%02d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				_, err := pac.Insert("z1", testData{value: 0, dataSize: 0})
				require.NoError(t, err)
				_, err = pac.Insert("z2", testData{value: 0, dataSize: 0})
				require.NoError(t, err)

				evicted, err := pac.Insert("jumbo", testData{value: 999, dataSize: 640})
				require.NoError(t, err)
				require.Len(t, evicted, 64)

				_, ok := pac.LookUpWithoutChangingOrder("z1")
				assert.True(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("z2")
				assert.True(t, ok)
				_, ok = pac.LookUpWithoutChangingOrder("jumbo")
				assert.True(t, ok)
				assertAlreadyCompacted(t, pac, probe)

				probe.Set(0.80)
				advancedEval := probe.ObserveEpochAdvance(t, pac, func() {
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
					_, err := pac.Insert(fmt.Sprintf("k-%02d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Raise pressure to Tier 1 (0.80) and insert a new 95B entry ("new-large").
				probe.Set(0.80)
				var evicted []testData
				var err error
				advancedInsert := probe.ObserveEpochAdvance(t, pac, func() {
					evicted, err = pac.Insert("new-large", testData{value: 99, dataSize: 95})
				})
				require.NoError(t, err)
				require.Len(t, evicted, 10)
				assert.True(t, advancedInsert)
				assertAlreadyCompacted(t, pac, probe)

				advancedEval := probe.ObserveEpochAdvance(t, pac, func() {
					pac.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEval)
			})

			t.Run("PartialDrainToTwoSurvivorsWithNetByteReductionCompactsDirtyStateInInsertAndUpdateGrow", func(t *testing.T) {
				// Arrange: Populate two caches (maxSize = 100B) with 4 entries:
				// "old1" (30B), "old2" (30B), "keep1" (20B), "keep2" (20B) = 100B total (peakLen = 4 <= 8).
				probeInsert := newPressureProbe(0.10)
				probeUpdate := newPressureProbe(0.10)
				cacheInsert := b.fn(100, WithInvariantChecking(true), probeInsert.Option()).(PressureAwareCache[testData])
				cacheUpdate := b.fn(100, WithInvariantChecking(true), probeUpdate.Option()).(PressureAwareCache[testData])

				for _, c := range []Cache[testData]{cacheInsert, cacheUpdate} {
					_, err := c.Insert("old1", testData{value: 1, dataSize: 30})
					require.NoError(t, err)
					_, err = c.Insert("old2", testData{value: 2, dataSize: 30})
					require.NoError(t, err)
					_, err = c.Insert("keep1", testData{value: 3, dataSize: 20})
					require.NoError(t, err)
					_, err = c.Insert("keep2", testData{value: 4, dataSize: 20})
					require.NoError(t, err)
				}

				// Act: Under Tier 1 pressure (0.80), achieve net byte reduction (100B -> 90B) via Insert and UpdateWithoutChangingOrder.
				probeInsert.Set(0.80)
				probeUpdate.Set(0.80)

				advancedInsert := probeInsert.ObserveEpochAdvance(t, cacheInsert, func() {
					_, err := cacheInsert.Insert("new", testData{value: 5, dataSize: 50})
					require.NoError(t, err)
				})
				advancedUpdate := probeUpdate.ObserveEpochAdvance(t, cacheUpdate, func() {
					require.NoError(t, cacheUpdate.UpdateWithoutChangingOrder("keep2", testData{value: 44, dataSize: 70}))
				})

				// Assert: Both Insert and UpdateWithoutChangingOrder advanced reclamation epoch AND compacted dirty state.
				assert.True(t, advancedInsert)
				assert.True(t, advancedUpdate)
				assertAlreadyCompacted(t, cacheInsert, probeInsert)
				assertAlreadyCompacted(t, cacheUpdate, probeUpdate)

				advancedEvalInsert := probeInsert.ObserveEpochAdvance(t, cacheInsert, func() {
					cacheInsert.EvaluateMemoryPressure()
				})
				advancedEvalUpdate := probeUpdate.ObserveEpochAdvance(t, cacheUpdate, func() {
					cacheUpdate.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEvalInsert)
				assert.False(t, advancedEvalUpdate)
			})

			t.Run("Tier2SmallCachePartialDrainCompactsDirtyStateWhileLargeCacheSub25PercentEvictionDefersCompaction", func(t *testing.T) {
				// Part 1: Small cache (peakEntryLen = 4 <= 8) under Tier 2 (0.95) with RetentionRatio = 0.90 (targetSize = 90B).
				// Evicting 2 of 4 entries (50% >= 25% shrinkage) during net-byte-reducing Insert / UpdateWithoutChangingOrder (100B -> 90B)
				// must compact dirty state inline so subsequent Tier 1 EvaluateMemoryPressure() is a no-op.
				probeInsert := newPressureProbe(0.10)
				probeUpdate := newPressureProbe(0.10)
				cacheInsert := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.90),
					probeInsert.Option(),
				).(PressureAwareCache[testData])
				cacheUpdate := b.fn(
					100,
					WithInvariantChecking(true),
					WithEvictionThreshold(0.90),
					WithEvictionRetentionRatio(0.90),
					probeUpdate.Option(),
				).(PressureAwareCache[testData])

				for _, c := range []Cache[testData]{cacheInsert, cacheUpdate} {
					_, err := c.Insert("old1", testData{value: 1, dataSize: 30})
					require.NoError(t, err)
					_, err = c.Insert("old2", testData{value: 2, dataSize: 30})
					require.NoError(t, err)
					_, err = c.Insert("keep1", testData{value: 3, dataSize: 20})
					require.NoError(t, err)
					_, err = c.Insert("keep2", testData{value: 4, dataSize: 20})
					require.NoError(t, err)
				}

				probeInsert.Set(0.95)
				probeUpdate.Set(0.95)

				advancedInsert := probeInsert.ObserveEpochAdvance(t, cacheInsert, func() {
					_, err := cacheInsert.Insert("new", testData{value: 5, dataSize: 50})
					require.NoError(t, err)
				})
				advancedUpdate := probeUpdate.ObserveEpochAdvance(t, cacheUpdate, func() {
					require.NoError(t, cacheUpdate.UpdateWithoutChangingOrder("keep2", testData{value: 44, dataSize: 70}))
				})
				assert.True(t, advancedInsert)
				assert.True(t, advancedUpdate)
				assertAlreadyCompacted(t, cacheInsert, probeInsert)
				assertAlreadyCompacted(t, cacheUpdate, probeUpdate)

				probeInsert.Set(0.80)
				probeUpdate.Set(0.80)
				advancedEvalInsert := probeInsert.ObserveEpochAdvance(t, cacheInsert, func() {
					cacheInsert.EvaluateMemoryPressure()
				})
				advancedEvalUpdate := probeUpdate.ObserveEpochAdvance(t, cacheUpdate, func() {
					cacheUpdate.EvaluateMemoryPressure()
				})
				assert.False(t, advancedEvalInsert)
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

				_, err := cacheLarge.Insert("tail-10", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				for i := range 99 {
					_, err = cacheLarge.Insert(fmt.Sprintf("k-%03d", i), testData{value: int64(i), dataSize: 1})
					require.NoError(t, err)
				}

				probeLarge.Set(0.95)
				advancedLargeInsert := probeLarge.ObserveEpochAdvance(t, cacheLarge, func() {
					evicted, err := cacheLarge.Insert("new-8", testData{value: 99, dataSize: 8})
					require.NoError(t, err)
					require.Len(t, evicted, 1)
				})
				assert.True(t, advancedLargeInsert)

				// Because 1% eviction (< 25%) did not run O(N) compaction inline, an explicit Compact() reclaims the
				// 1 deleted slot and advances reclaimEpoch.
				probeLarge.Set(0.10)
				advancedExplicitCompact := probeLarge.ObserveEpochAdvance(t, cacheLarge, func() {
					cacheLarge.Compact()
				})
				assert.True(t, advancedExplicitCompact)
			})

			t.Run("InsertSmallerOverwriteInvalidatesConcurrentInFlightSampleAndCompacts", func(t *testing.T) {
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

					_, err := pac.Insert("k1", testData{value: 1, dataSize: 30})
					require.NoError(t, err)
					_, err = pac.Insert("k2", testData{value: 2, dataSize: 50})
					require.NoError(t, err)

					probe.Set(pressure)
					advanced := probe.ObserveEpochAdvance(t, pac, func() {
						evicted, err := pac.Insert("k2", testData{value: 22, dataSize: 10})
						require.NoError(t, err)
						assert.Empty(t, evicted)
					})
					assert.True(t, advanced)
					_, ok := pac.LookUpWithoutChangingOrder("k1")
					assert.True(t, ok)
					_, ok = pac.LookUpWithoutChangingOrder("k2")
					assert.True(t, ok)
				}

				// Part 2: When >= 25% churn exists (4 entries inserted, 1 erased -> 3 survivors, deletedSinceCompact*4 >= peakEntryLen)
				// under Tier 1 (0.80), overwriting one of the surviving keys with a smaller value compacts dirty state inline.
				probeChurn := newPressureProbe(0.10)
				cacheChurn := b.fn(1000, WithInvariantChecking(true), probeChurn.Option()).(PressureAwareCache[testData])
				for _, k := range []string{"del/00", "keep/01", "keep/02"} {
					_, err := cacheChurn.Insert(k, testData{value: 1, dataSize: 10})
					require.NoError(t, err)
				}
				_, err := cacheChurn.Insert("k1", testData{value: 1, dataSize: 50})
				require.NoError(t, err)
				_, ok := cacheChurn.Erase("del/00")
				require.True(t, ok)

				probeChurn.Set(0.80)
				advancedChurn := probeChurn.ObserveEpochAdvance(t, cacheChurn, func() {
					evicted, err := cacheChurn.Insert("k1", testData{value: 11, dataSize: 10})
					require.NoError(t, err)
					assert.Empty(t, evicted)
				})
				assert.True(t, advancedChurn)
				assertAlreadyCompacted(t, cacheChurn, probeChurn)

				// Part 3: When a small cache (peakEntryLen = 2 <= 8) has dirty slack down to a sole surviving
				// 50B entry "k1" at normal pressure (0.10), subsequently shrinking "k1" in place to 10B via
				// Insert("k1", 10B) (with 0 pre-insert evictions) under Tier 1 (0.80) triggers
				// shouldReclaimSingleSurvivorOnMutation and compacts the dirty slack inline.
				probeSingle := newPressureProbe(0.10)
				cacheSingle := b.fn(1000, WithInvariantChecking(true), probeSingle.Option()).(PressureAwareCache[testData])

				_, err = cacheSingle.Insert("del/00", testData{value: 1, dataSize: 10})
				require.NoError(t, err)
				_, err = cacheSingle.Insert("k1", testData{value: 1, dataSize: 50})
				require.NoError(t, err)
				_, ok = cacheSingle.Erase("del/00")
				require.True(t, ok)

				probeSingle.Set(0.80)
				advancedSingle := probeSingle.ObserveEpochAdvance(t, cacheSingle, func() {
					evicted, err := cacheSingle.Insert("k1", testData{value: 11, dataSize: 10})
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
					_, err := c.Insert(fmt.Sprintf("init_%02d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act 1: Perform 30 steady-state capacity-turnover inserts (< 64 churn deletions).
				advancedTurnover := probe.ObserveEpochAdvance(t, c, func() {
					for i := range 30 {
						_, err := c.Insert(fmt.Sprintf("turn_%02d", i), testData{value: int64(i), dataSize: 10})
						require.NoError(t, err)
					}
				})

				// Assert 1: Zero compactions occurred during low-churn capacity turnover.
				assert.False(t, advancedTurnover)

				// Act 2: Erase 3 entries so live entry count drops from 10 to 7 (30% shrinkage >= 25%).
				_, ok := c.Erase("turn_27")
				require.True(t, ok)
				_, ok = c.Erase("turn_28")
				require.True(t, ok)
				advancedShrinkage := probe.ObserveEpochAdvance(t, c, func() {
					_, ok := c.Erase("turn_29")
					require.True(t, ok)
				})

				// Assert 2: Auto-compaction triggered on >= 25% shrinkage, leaving 7 entries and clean state.
				assert.True(t, advancedShrinkage)
				_, ok = c.LookUpWithoutChangingOrder("turn_20")
				assert.True(t, ok)
				_, ok = c.LookUpWithoutChangingOrder("turn_29")
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
					_, err := c.Insert(fmt.Sprintf("init-%03d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}
				for i := range 100 {
					_, err := c.Insert(fmt.Sprintf("churn-%03d", i), testData{value: int64(i), dataSize: 10})
					require.NoError(t, err)
				}

				// Act: Raise pressure to Tier 1 (0.80) and perform a write.
				probe.Set(0.80)
				var err error
				advanced := probe.ObserveEpochAdvance(t, c, func() {
					_, err = c.Insert("trigger", testData{value: 999, dataSize: 10})
				})

				// Assert: High-churn tombstone bloat triggers Tier 1 auto-compaction.
				require.NoError(t, err)
				assert.True(t, advanced)
				assertAlreadyCompacted(t, c, probe)
			})
		})
	}
}
