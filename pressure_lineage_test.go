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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPressureLineage_WaitGroupGoAndExitedIntermediateReentrancy verifies that:
//   - Child goroutines spawned via sync.WaitGroup.Go or returning helper functions inside
//     OnEvict* and PressureFunc are recognized as descendants and never deadlock or re-sample.
//   - Multi-hop child -> grandchild chains where the intermediate child goroutine exits
//     before the grandchild accesses the cache are still recognized via the parent's creatorFunc.
//   - Reclamations performed by a child goroutine inside PressureFunc are attributed to the
//     active root sampler so the parent sampler does not false-positive retry 3 times and over-evict.
//   - Re-entrant OnEvict* callbacks triggered by child goroutines are strictly serialized
//     (maxConcurrent == 1) and delivered in FIFO order without deadlocking wg.Wait().
func TestPressureLineage_WaitGroupGoAndExitedIntermediateReentrancy(t *testing.T) {
	t.Run("WaitGroupGoAndHelperSpawnedChildInOnEvictAndPressureFunc", testCI01WaitGroupGoAndHelperChild)
	t.Run("ExitedIntermediateGoroutineGrandchildDetection", testCI02ExitedIntermediateGrandchild)
	t.Run("ChildGoroutineReclaimInsidePressureFuncPreservesSurvivors", testCI03ChildReclaimPreservesSurvivors)
	t.Run("ReentrantChildGoroutineEvictCallbacksAreStrictlySerialized", testCI04ReentrantChildEvictSerialized)
}

func spawnViaReturningHelper(fn func()) {
	go fn()
}

func testCI01WaitGroupGoAndHelperChild(t *testing.T) {
	t.Helper()
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		// Arrange
		var cache Cache[int]
		var pressureSamples, evictCount atomic.Int32
		var firstPressure, firstEvict atomic.Bool
		firstPressure.Store(true)
		firstEvict.Store(true)

		cache = New[int](1,
			WithBackend(b),
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				pressureSamples.Add(1)
				if firstPressure.CompareAndSwap(true, false) {
					var wg sync.WaitGroup
					wg.Go(func() {
						_, _ = cache.Delete("nonexistent_from_wg_go")
					})
					wg.Wait()
				}
				return 0.10
			}),
			WithOnEvictEntry(func(_ string, _ int, _ EvictionReason) {
				evictCount.Add(1)
				if firstEvict.CompareAndSwap(true, false) {
					var wg sync.WaitGroup
					wg.Go(func() {
						_, putErr := cache.Put("k3_from_wg_go", 3)
						assert.NoError(t, putErr)
					})
					doneHelper := make(chan struct{})
					spawnViaReturningHelper(func() {
						defer close(doneHelper)
						_, _ = cache.Delete("missing_from_helper")
					})
					wg.Wait()
					<-doneHelper
				}
			}),
		)

		// Act
		_, err := cache.Put("k1", 1)
		require.NoError(t, err)
		require.Equal(t, int32(1), pressureSamples.Load(), "wg.Go child inside PressureFunc must not re-sample")

		done := make(chan struct{})
		go func() {
			_, putErr := cache.Put("k2", 2)
			assert.NoError(t, putErr)
			close(done)
		}()

		// Assert
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			require.Failf(t, "deadlock", "backend %v deadlocked on wg.Go child inside OnEvictEntry", b)
		}
		assert.Equal(t, int32(2), evictCount.Load())
		val, ok := cache.Peek("k3_from_wg_go")
		require.True(t, ok)
		assert.Equal(t, 3, val)
	}
}

func testCI02ExitedIntermediateGrandchild(t *testing.T) {
	t.Helper()
	// Arrange: G1 (in OnEvictValue and PressureFunc) spawns intermediate child G2 via wg.Go;
	// G2 spawns grandchild G3 and exits immediately (`childExited` closes before G3 touches cache).
	var cache Cache[int]
	var pressureCalls, evictCalls atomic.Int32

	cache = NewArenaRadixCache[int](1,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			if pressureCalls.Add(1) == 1 {
				grandchildDone := make(chan struct{})
				childExited := make(chan struct{})
				var wg sync.WaitGroup
				wg.Go(func() {
					go func() {
						defer close(grandchildDone)
						<-childExited
						runtime.Gosched()
						_, _ = cache.Delete("from_orphan_grandchild")
					}()
				})
				wg.Wait()
				close(childExited)
				<-grandchildDone
			}
			return 0.10
		}),
		WithOnEvictValue(func(_ int, _ EvictionReason) {
			if evictCalls.Add(1) == 1 {
				grandchildDone := make(chan struct{})
				childExited := make(chan struct{})
				var wg sync.WaitGroup
				wg.Go(func() {
					go func() {
						defer close(grandchildDone)
						<-childExited
						runtime.Gosched()
						_, putErr := cache.Put("k3_from_orphan_grandchild", 30)
						assert.NoError(t, putErr)
					}()
				})
				wg.Wait()
				close(childExited)
				<-grandchildDone
			}
		}),
	)

	// Act
	_, err := cache.Put("k1", 1)
	require.NoError(t, err)
	assert.Equal(t, int32(1), pressureCalls.Load(), "orphan grandchild inside PressureFunc must not re-sample")

	_, err = cache.Put("k2", 2)
	require.NoError(t, err)

	// Assert
	assert.Equal(t, int32(2), evictCalls.Load())
	val, ok := cache.Peek("k3_from_orphan_grandchild")
	require.True(t, ok)
	assert.Equal(t, 30, val)
}

func testCI03ChildReclaimPreservesSurvivors(t *testing.T) {
	t.Helper()
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		// Arrange: Populate 4 entries of 25B each at 0.10 pressure (100B total, retention = 0.50 -> targetSize = 50B).
		// When EvaluateMemoryPressure() runs at 0.95, its PressureFunc spawns a child goroutine via wg.Go
		// that reclaims 1 entry ("k1", 25B) via DeletePrefix, triggering markReclaimedLocked on the child goroutine.
		var pac PressureAwareCache[int]
		var pressureBits atomic.Uint64
		pressureBits.Store(math.Float64bits(0.10))
		var triggerChildDelete atomic.Bool
		var sampleCount atomic.Int32

		c := New[int](100,
			WithBackend(b),
			WithInvariantChecking(true),
			WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
			WithEvictionThreshold(0.90),
			WithEvictionRetentionRatio(0.50),
			WithPressureFunc(func() float64 {
				sampleCount.Add(1)
				if triggerChildDelete.CompareAndSwap(true, false) {
					var wg sync.WaitGroup
					wg.Go(func() {
						pac.DeletePrefix("k1")
					})
					wg.Wait()
				}
				return math.Float64frombits(pressureBits.Load())
			}),
		)
		pac = c.(PressureAwareCache[int])

		for _, k := range []string{"k1", "k2", "k3", "k4"} {
			_, err := c.Put(k, 25)
			require.NoError(t, err)
		}

		// Act
		pressureBits.Store(math.Float64bits(0.95))
		sampleCount.Store(0)
		triggerChildDelete.Store(true)
		evicted := pac.EvaluateMemoryPressure()

		// Assert: Parent sampler sampled only once (did not retry 3 times) and shed only "k2" (25B)
		// to reach 50B (k3 + k4), rather than over-evicting k3 on a post-retry pass.
		assert.Equal(t, int32(1), sampleCount.Load(), "backend %v parent sampler must not retry when child goroutine reclaims", b)
		assert.Len(t, evicted, 1, "backend %v must shed only 1 entry (k2) to reach 50%% retention", b)
		assert.Equal(t, 2, c.Stats().Len, "backend %v must preserve 2 survivors (k3, k4)", b)
	}
}

func testCI04ReentrantChildEvictSerialized(t *testing.T) {
	t.Helper()
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		// Arrange
		var cache Cache[int]
		var inFlight, maxConcurrent atomic.Int32
		var mu sync.Mutex
		var evictedOrder []string

		cache = New[int](10,
			WithBackend(b),
			WithInvariantChecking(true),
			WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
				cur := inFlight.Add(1)
				for {
					prev := maxConcurrent.Load()
					if cur <= prev || maxConcurrent.CompareAndSwap(prev, cur) {
						break
					}
				}
				mu.Lock()
				evictedOrder = append(evictedOrder, key)
				mu.Unlock()

				if key == "k0" {
					var wg sync.WaitGroup
					wg.Go(func() {
						_, _ = cache.Delete("k1")
					})
					wg.Go(func() {
						_, _ = cache.Delete("k2")
					})
					wg.Wait()
				}
				time.Sleep(2 * time.Millisecond)
				inFlight.Add(-1)
			}),
		)

		_, err := cache.Put("k0", 0)
		require.NoError(t, err)
		_, err = cache.Put("k1", 1)
		require.NoError(t, err)
		_, err = cache.Put("k2", 2)
		require.NoError(t, err)

		// Act
		_, ok := cache.Delete("k0")
		require.True(t, ok)

		// Assert: All 3 callbacks ran with strict mutual exclusion (maxConcurrent == 1), with "k0" first.
		assert.Equal(t, int32(1), maxConcurrent.Load(), "backend %v OnEvictEntry must never execute concurrently (maxConcurrent == 1)", b)
		mu.Lock()
		assert.Len(t, evictedOrder, 3)
		assert.Equal(t, "k0", evictedOrder[0])
		assert.ElementsMatch(t, []string{"k0", "k1", "k2"}, evictedOrder)
		mu.Unlock()
	}
}

// TestPressureLineage_UnrelatedGoroutineWithExitedParentNotMisclassified verifies that:
//  1. An unrelated worker goroutine whose parent goroutine has already exited is not
//     misclassified as a re-entrant child of an active OnEvict* or PressureFunc holder.
//  2. A pre-existing worker goroutine spawned via sync.WaitGroup.Go before cache.Put
//     is not misclassified as a re-entrant child of PressureFunc.
func TestPressureLineage_UnrelatedGoroutineWithExitedParentNotMisclassified(t *testing.T) {
	t.Run("UnrelatedWorkerWithExitedParentReturnsFromDeleteOnlyAfterOnEvictRuns", func(t *testing.T) {
		// Arrange: Spawn a worker from a parent goroutine that immediately exits.
		parentExited := make(chan struct{})
		startWorker := make(chan struct{})
		workerDone := make(chan struct{})
		var parentGID atomic.Uint64

		var cache Cache[int]
		inK1Evict := make(chan struct{})
		releaseK1Evict := make(chan struct{})
		var k2EvictCompleted atomic.Bool
		var k2CompletedBeforeDeleteReturn atomic.Bool

		go func() {
			parentGID.Store(currentGoroutineID())
			go func() {
				<-startWorker
				_, ok := cache.Delete("k2")
				assert.True(t, ok)
				k2CompletedBeforeDeleteReturn.Store(k2EvictCompleted.Load())
				close(workerDone)
			}()
			close(parentExited)
		}()
		<-parentExited
		waitForGoroutineExit(t, parentGID.Load())

		var k1Once sync.Once
		cache = NewMapCache[int](10, WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
			switch key {
			case "k1":
				k1Once.Do(func() {
					close(inK1Evict)
					<-releaseK1Evict
				})
			case "k2":
				k2EvictCompleted.Store(true)
			}
		}))
		_, err := cache.Put("k1", 1)
		require.NoError(t, err)
		_, err = cache.Put("k2", 2)
		require.NoError(t, err)

		// Act: Hold G1 inside OnEvict("k1"), trigger the orphan-parent worker to call Delete("k2"),
		// then release OnEvict("k1") once the worker is contending on evictCallbackMu.
		g1Done := make(chan struct{})
		go func() {
			_, _ = cache.Delete("k1")
			close(g1Done)
		}()
		<-inK1Evict
		close(startWorker)
		waitForEvictContentionOrDone(t, extractPressureStateForIter2Test(cache), workerDone)
		close(releaseK1Evict)
		<-g1Done
		<-workerDone

		// Assert: Unrelated worker waited for its own OnEvict("k2") to finish before Delete("k2") returned.
		assert.True(t, k2CompletedBeforeDeleteReturn.Load(), "unrelated worker's Delete(k2) returned before OnEvict(k2) executed")
	})

	t.Run("UnrelatedWorkerSpawnedViaWaitGroupGoBeforePutNotMisclassifiedAsChildOfSampler", func(t *testing.T) {
		// Arrange: Spawn a background worker via wg.Go BEFORE calling cache.Put on the same parent goroutine.
		var cache Cache[int]
		var calls atomic.Int32
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})
		var workerErr error
		var wg sync.WaitGroup

		wg.Go(func() {
			<-workerTrigger
			_, workerErr = cache.Put("worker_key", 2)
			close(workerDone)
		})

		var primaryOnce sync.Once
		var timedOut atomic.Bool
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			c := calls.Add(1)
			if c == 1 {
				primaryOnce.Do(func() {
					close(workerTrigger)
				})
				select {
				case <-workerDone:
				case <-time.After(5 * time.Second):
					timedOut.Store(true)
				}
			}
			return 0.10
		}))

		// Act
		_, err := cache.Put("parent_key", 1)
		require.NoError(t, err)
		require.False(t, timedOut.Load(), "timed out waiting for worker goroutine inside PressureFunc")
		<-workerDone
		wg.Wait()
		require.NoError(t, workerErr)

		// Assert: Both the parent goroutine and the pre-existing wg.Go worker evaluated PressureFunc.
		assert.Equal(t, int32(2), calls.Load(), "pre-existing worker spawned via wg.Go before cache.Put must not be misclassified as a child of PressureFunc")
	})

	t.Run("UnrelatedWorkerWithExitedParentSamplesPressureAndIncrementsExternalReclaimEpoch", func(t *testing.T) {
		// Arrange: Spawn an unrelated worker from a parent goroutine that immediately exits.
		parentExited := make(chan struct{})
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})
		var parentGID atomic.Uint64
		var cache *mapCache[int]
		var calls atomic.Int32
		var workerErr error

		go func() {
			parentGID.Store(currentGoroutineID())
			go func() {
				<-workerTrigger
				_, _ = cache.Delete("k_reclaim")
				_, workerErr = cache.Put("worker_key", 2)
				close(workerDone)
			}()
			close(parentExited)
		}()
		<-parentExited
		waitForGoroutineExit(t, parentGID.Load())

		var armOnce sync.Once
		var armed atomic.Bool
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.95
			}
			c := calls.Add(1)
			if c == 1 {
				armOnce.Do(func() {
					close(workerTrigger)
				})
				<-workerDone
			}
			return 0.95
		})).(*mapCache[int])

		_, err := cache.Put("k_reclaim", 1)
		require.NoError(t, err)
		epochBefore := cache.externalReclaimEpoch.Load()
		armed.Store(true)

		// Act
		_, err = cache.Put("parent_key", 1)
		require.NoError(t, err)
		require.NoError(t, workerErr)

		// Assert: Unrelated worker with exited parent evaluated PressureFunc and incremented externalReclaimEpoch.
		assert.GreaterOrEqual(t, calls.Load(), int32(2), "unrelated worker with exited parent must evaluate PressureFunc")
		assert.Greater(t, cache.externalReclaimEpoch.Load(), epochBefore, "unrelated worker Delete must increment externalReclaimEpoch")
	})
}

// TestPressureLineage_SiblingHelperClosureInSameOuterFuncNotMisclassified verifies that when
// an outer function defines both a helper closure (`spawn := func(fn func()) { go fn() }`)
// and PressureFunc / OnEvict* closures, child goroutines spawned via `spawn` inside PressureFunc
// or OnEvict* are recognized as descendants rather than rejected because the outer function
// frame lies below the hook.
func TestPressureLineage_SiblingHelperClosureInSameOuterFuncNotMisclassified(t *testing.T) {
	// Arrange
	spawnHelper := func(fn func()) {
		go fn()
	}

	var cache Cache[int]
	var pressureCalls, evictCalls atomic.Int32
	var firstPressure, firstEvict atomic.Bool
	firstPressure.Store(true)
	firstEvict.Store(true)

	cache = NewMapCache[int](1,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			pressureCalls.Add(1)
			if firstPressure.CompareAndSwap(true, false) {
				done := make(chan struct{})
				spawnHelper(func() {
					defer close(done)
					_, _ = cache.Delete("missing_from_sibling_helper")
				})
				<-done
			}
			return 0.10
		}),
		WithOnEvictValue(func(_ int, _ EvictionReason) {
			evictCalls.Add(1)
			if firstEvict.CompareAndSwap(true, false) {
				done := make(chan struct{})
				spawnHelper(func() {
					defer close(done)
					_, err := cache.Put("k3_from_sibling_helper", 3)
					assert.NoError(t, err)
				})
				<-done
			}
		}),
	)

	// Act: First Put triggers PressureFunc (where child spawned via spawnHelper must not re-sample);
	// second Put evicts k1 and triggers OnEvictValue (where child spawned via spawnHelper must not deadlock).
	_, err := cache.Put("k1", 1)
	require.NoError(t, err)
	callsAfterFirstPut := pressureCalls.Load()

	putDone := make(chan struct{})
	go func() {
		_, putErr := cache.Put("k2", 2)
		assert.NoError(t, putErr)
		close(putDone)
	}()

	// Assert
	select {
	case <-putDone:
	case <-time.After(2 * time.Second):
		require.Fail(t, "OnEvictValue deadlocked on evictCallbackMu when child was spawned via sibling helper closure")
	}
	assert.Equal(t, int32(1), callsAfterFirstPut, "child spawned via sibling helper closure inside PressureFunc must not re-sample")
	assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and k2 eviction callbacks must complete")
}

//go:noinline
func runWithWaitGroupWrapper(wg *sync.WaitGroup, fn func()) {
	wg.Go(func() {
		fn()
	})
}

// TestPressureLineage_WaitGroupGoWrapperHelperPreservesCallerAttribution verifies that when
// sync.WaitGroup.Go is wrapped inside a helper function (pushing the caller's closure to the
// third bottom stack frame fn2 above "created by"), child and grandchild goroutines spawned
// inside OnEvict* or PressureFunc are still recognized as descendants (even when an intermediate
// goroutine has exited), while pre-existing workers spawned via the same wrapper before cache.Put
// are not misclassified as children.
func TestPressureLineage_WaitGroupGoWrapperHelperPreservesCallerAttribution(t *testing.T) {
	t.Run("ExitedIntermediateGoroutineSpawningViaWaitGroupGoWrapperDeadlocksOnEvictAndBypassesPressureFunc", func(t *testing.T) {
		// Arrange
		var cache Cache[int]
		var pressureCalls, evictCalls atomic.Int32
		var firstPressure, firstEvict atomic.Bool
		firstPressure.Store(true)
		firstEvict.Store(true)

		cache = NewMapCache[int](1,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				pressureCalls.Add(1)
				if firstPressure.CompareAndSwap(true, false) {
					g2Exited := make(chan struct{})
					g3Done := make(chan struct{})
					var g2GID atomic.Uint64
					var wg sync.WaitGroup
					go func() {
						g2GID.Store(currentGoroutineID())
						runWithWaitGroupWrapper(&wg, func() {
							defer close(g3Done)
							<-g2Exited
							waitForGoroutineExit(t, g2GID.Load())
							_, _ = cache.Delete("missing_from_wg_wrapper")
						})
						close(g2Exited)
					}()
					<-g3Done
					wg.Wait()
				}
				return 0.10
			}),
			WithOnEvictValue(func(_ int, _ EvictionReason) {
				evictCalls.Add(1)
				if firstEvict.CompareAndSwap(true, false) {
					g2Exited := make(chan struct{})
					g3Done := make(chan struct{})
					var g2GID atomic.Uint64
					var wg sync.WaitGroup
					go func() {
						g2GID.Store(currentGoroutineID())
						runWithWaitGroupWrapper(&wg, func() {
							defer close(g3Done)
							<-g2Exited
							waitForGoroutineExit(t, g2GID.Load())
							_, putErr := cache.Put("k3_from_wg_wrapper", 3)
							assert.NoError(t, putErr)
						})
						close(g2Exited)
					}()
					<-g3Done
					wg.Wait()
				}
			}),
		)

		// Act
		_, err := cache.Put("k1", 1)
		require.NoError(t, err)
		callsAfterFirstPut := pressureCalls.Load()

		putDone := make(chan struct{})
		go func() {
			_, putErr := cache.Put("k2", 2)
			assert.NoError(t, putErr)
			close(putDone)
		}()

		// Assert
		select {
		case <-putDone:
		case <-time.After(2 * time.Second):
			require.Fail(t, "deadlock: OnEvictValue deadlocked on evictCallbackMu when exited child spawned grandchild via wg.Go wrapper")
		}
		assert.Equal(t, int32(1), callsAfterFirstPut, "grandchild spawned via wg.Go wrapper after intermediate child exited must not bypass PressureFunc guard")
		assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and k2 eviction callbacks must complete")
	})

	t.Run("PreExistingWorkerViaWaitGroupGoWrapperMisclassifiedWhenCallbackUsesWaitGroup", func(t *testing.T) {
		// Arrange: Spawn a pre-existing worker via runWithWaitGroupWrapper before cache.Put,
		// and also use a local sync.WaitGroup inside PressureFunc so "\nsync.(*WaitGroup)."
		// is present in aboveHook while the pre-existing worker accesses the cache.
		var cache Cache[int]
		var calls atomic.Int32
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})
		var workerErr error
		var preWG sync.WaitGroup

		runWithWaitGroupWrapper(&preWG, func() {
			<-workerTrigger
			_, workerErr = cache.Put("worker_key", 2)
			close(workerDone)
		})

		var primaryOnce sync.Once
		var timedOut atomic.Bool
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			c := calls.Add(1)
			if c == 1 {
				var innerWG sync.WaitGroup
				innerWG.Go(func() {
					primaryOnce.Do(func() {
						close(workerTrigger)
					})
					select {
					case <-workerDone:
					case <-time.After(5 * time.Second):
						timedOut.Store(true)
					}
				})
				innerWG.Wait()
			}
			return 0.10
		}))

		// Act
		_, err := cache.Put("parent_key", 1)
		require.NoError(t, err)
		require.False(t, timedOut.Load(), "timed out waiting for worker goroutine inside PressureFunc")
		<-workerDone
		preWG.Wait()
		require.NoError(t, workerErr)

		// Assert
		assert.Equal(t, int32(2), calls.Load(), "pre-existing worker spawned via wg.Go wrapper before cache.Put was misclassified as child of PressureFunc")
	})
}

//go:noinline
func startPreExistingWorkerHelper(trigger <-chan struct{}, fn func()) {
	go func() {
		<-trigger
		fn()
	}()
}

// TestPressureLineage_PreExistingWorkerSpawnedViaHelperOrSetupClosureExcluded verifies
// that pre-existing background goroutines spawned by the same parent goroutine before
// cache.Delete or cache.Put via a helper function or local setup closure are not
// misclassified as re-entrant children of OnEvict* or PressureFunc.
func TestPressureLineage_PreExistingWorkerSpawnedViaHelperOrSetupClosureExcluded(t *testing.T) {
	t.Run("PreExistingWorkerStartedViaHelperBeforeDeleteReturnsBeforeOnEvictRuns", func(t *testing.T) {
		// Arrange
		startWorker := make(chan struct{})
		workerDone := make(chan struct{})

		var cache Cache[int]
		var k2EvictCompleted atomic.Bool
		var k2CompletedBeforeDeleteReturn atomic.Bool

		startPreExistingWorkerHelper(startWorker, func() {
			_, ok := cache.Delete("k2")
			assert.True(t, ok)
			k2CompletedBeforeDeleteReturn.Store(k2EvictCompleted.Load())
			close(workerDone)
		})

		var k1Once sync.Once
		cache = NewMapCache[int](10, WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
			switch key {
			case "k1":
				k1Once.Do(func() {
					close(startWorker)
					waitForEvictContentionOrDone(t, extractPressureStateForIter2Test(cache), workerDone)
				})
			case "k2":
				k2EvictCompleted.Store(true)
			}
		}))
		_, err := cache.Put("k1", 1)
		require.NoError(t, err)
		_, err = cache.Put("k2", 2)
		require.NoError(t, err)

		// Act: Call Delete("k1") directly on the same parent goroutine that started the helper worker.
		_, ok := cache.Delete("k1")
		require.True(t, ok)
		<-workerDone

		// Assert
		assert.True(t, k2CompletedBeforeDeleteReturn.Load(), "pre-existing worker's Delete(k2) returned BEFORE OnEvict(k2) executed (misclassified as child of OnEvict(k1))")
	})

	t.Run("PreExistingWorkerStartedViaHelperBeforePutMisclassifiedAsChildOfSampler", func(t *testing.T) {
		// Arrange
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})
		var cache *mapCache[int]
		var calls atomic.Int32
		var workerErr error

		startPreExistingWorkerHelper(workerTrigger, func() {
			_, _ = cache.Delete("k_reclaim")
			_, workerErr = cache.Put("worker_key", 2)
			close(workerDone)
		})

		var armOnce sync.Once
		var armed atomic.Bool
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			if !armed.Load() {
				return 0.95
			}
			c := calls.Add(1)
			if c == 1 {
				armOnce.Do(func() {
					close(workerTrigger)
				})
				<-workerDone
			}
			return 0.95
		})).(*mapCache[int])

		_, err := cache.Put("k_reclaim", 1)
		require.NoError(t, err)
		epochBefore := cache.externalReclaimEpoch.Load()
		armed.Store(true)

		// Act
		_, err = cache.Put("parent_key", 1)
		require.NoError(t, err)
		require.NoError(t, workerErr)

		// Assert
		assert.GreaterOrEqual(t, calls.Load(), int32(2), "pre-existing worker spawned via helper before cache.Put was misclassified as a child of PressureFunc")
		assert.Greater(t, cache.externalReclaimEpoch.Load(), epochBefore, "pre-existing worker Delete was misclassified as child sampler reclaim and failed to increment externalReclaimEpoch")
	})

	t.Run("PreExistingWorkerStartedViaSetupClosureBeforePutMisclassifiedAsChildOfSampler", func(t *testing.T) {
		// Arrange
		workerTrigger := make(chan struct{})
		workerDone := make(chan struct{})
		var cache Cache[int]
		var calls atomic.Int32
		var workerErr error

		startSetup := func() {
			go func() {
				<-workerTrigger
				_, workerErr = cache.Put("worker_key", 2)
				close(workerDone)
			}()
		}
		startSetup()

		var primaryOnce sync.Once
		var timedOut atomic.Bool
		cache = NewMapCache[int](10, WithPressureFunc(func() float64 {
			c := calls.Add(1)
			if c == 1 {
				primaryOnce.Do(func() {
					close(workerTrigger)
				})
				select {
				case <-workerDone:
				case <-time.After(5 * time.Second):
					timedOut.Store(true)
				}
			}
			return 0.10
		}))

		// Act
		_, err := cache.Put("parent_key", 1)
		require.NoError(t, err)
		require.False(t, timedOut.Load(), "timed out waiting for worker goroutine inside PressureFunc")
		<-workerDone
		require.NoError(t, workerErr)

		// Assert
		assert.Equal(t, int32(2), calls.Load(), "pre-existing worker spawned via setup closure before cache.Put was misclassified as a child of PressureFunc")
	})
}

//go:noinline
func runGenericWaitGroupWorkerScenario[T any](t *testing.T, val T) {
	t.Helper()

	// Arrange: Both the pre-existing wg.Go worker (.func1) and PressureFunc (.func2)
	// are closures inside the same generic function runGenericWaitGroupWorkerScenario[...].
	var cache Cache[T]
	var calls atomic.Int32
	workerTrigger := make(chan struct{})
	workerDone := make(chan struct{})
	var workerErr error
	var wg sync.WaitGroup

	wg.Go(func() {
		<-workerTrigger
		_, workerErr = cache.Put("worker_key", val)
		close(workerDone)
	})

	var primaryOnce sync.Once
	var timedOut atomic.Bool
	cache = NewMapCache[T](10, WithPressureFunc(func() float64 {
		c := calls.Add(1)
		if c == 1 {
			var childWG sync.WaitGroup
			childWG.Go(func() {
				_, _ = cache.Delete("missing_generic_child")
			})
			childWG.Wait()

			primaryOnce.Do(func() {
				close(workerTrigger)
			})
			select {
			case <-workerDone:
			case <-time.After(5 * time.Second):
				timedOut.Store(true)
			}
		}
		return 0.10
	}))

	// Act
	_, err := cache.Put("parent_key", val)
	require.NoError(t, err)
	require.False(t, timedOut.Load(), "timed out waiting for worker goroutine inside PressureFunc")
	<-workerDone
	wg.Wait()
	require.NoError(t, workerErr)

	// Assert
	assert.Equal(t, int32(2), calls.Load(), "pre-existing wg.Go worker in generic function must not be misclassified as child of PressureFunc due to '[' truncation in trimFrameFuncName")
}

// TestPressureLineage_GenericFunctionClosureFrameParsing verifies that trimFrameFuncName
// skips generic type parameter brackets "[...]" rather than truncating at '[', so a
// pre-existing wg.Go worker closure (fn[...].func1) is not confused with a PressureFunc
// closure (fn[...].func2) defined in the same generic function.
func TestPressureLineage_GenericFunctionClosureFrameParsing(t *testing.T) {
	runGenericWaitGroupWorkerScenario(t, 42)
}

type methodChildWorker struct {
	cache   Cache[int]
	putDone chan struct{}
	putErr  error
}

//go:noinline
func (w *methodChildWorker) deleteFromChild() {
	_, _ = w.cache.Delete("missing_from_named_method")
}

//go:noinline
func (w *methodChildWorker) putFromChild() {
	defer close(w.putDone)
	_, w.putErr = w.cache.Put("k3_from_named_method", 3)
}

// TestPressureLineage_NamedMethodAndOuterWrapperChildGoroutines verifies that:
// (1) spawning child goroutines inside OnEvict* and PressureFunc via named methods or method
// values (go w.putFromChild() and wg.Go(w.deleteFromChild)) does not match shared go-lru
// entry frames in belowHook, and (2) spawning child goroutines via a returning outer helper
// closure that wraps go func() { fn() }() does not abort at Frame 0 before matching the
// callback's closure owner at Frame 1 in aboveHook.
func TestPressureLineage_NamedMethodAndOuterWrapperChildGoroutines(t *testing.T) {
	t.Run("MethodValueInWaitGroupGoAndGoStatementInsideOnEvictAndPressureFunc", func(t *testing.T) {
		// Arrange
		var pressureCalls, evictCalls atomic.Int32
		var firstPressure, firstEvict, evictDeadlocked atomic.Bool
		firstPressure.Store(true)
		firstEvict.Store(true)

		w := &methodChildWorker{
			putDone: make(chan struct{}),
		}

		w.cache = NewMapCache[int](1,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				pressureCalls.Add(1)
				if firstPressure.CompareAndSwap(true, false) {
					var wg sync.WaitGroup
					wg.Go(w.deleteFromChild)
					wg.Wait()
				}
				return 0.10
			}),
			WithOnEvictValue(func(_ int, _ EvictionReason) {
				evictCalls.Add(1)
				if firstEvict.CompareAndSwap(true, false) {
					go w.putFromChild()
					select {
					case <-w.putDone:
					case <-time.After(5 * time.Second):
						evictDeadlocked.Store(true)
					}
					if !evictDeadlocked.Load() {
						var wg sync.WaitGroup
						wg.Go(w.deleteFromChild)
						wg.Wait()
					}
				}
			}),
		)

		// Act
		_, err := w.cache.Put("k1", 1)
		require.NoError(t, err)
		callsAfterFirstPut := pressureCalls.Load()

		_, err = w.cache.Put("k2", 2)
		require.NoError(t, err)

		// Assert
		require.False(t, evictDeadlocked.Load(), "deadlock: OnEvictValue deadlocked when spawning child via go w.putFromChild()")
		<-w.putDone
		require.NoError(t, w.putErr)
		assert.Equal(t, int32(1), callsAfterFirstPut, "child spawned via wg.Go(w.deleteFromChild) must not bypass PressureFunc re-entrancy guard")
		assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and k2 eviction callbacks must complete")
	})

	t.Run("ReturningOuterClosureSpawningInnerGoFuncWrapper", func(t *testing.T) {
		// Arrange
		spawnReturningInnerGoFunc := func(fn func()) <-chan struct{} {
			done := make(chan struct{})
			go func() {
				defer close(done)
				fn()
			}()
			return done
		}

		var cache Cache[int]
		var pressureCalls, evictCalls atomic.Int32
		var firstPressure, firstEvict, evictDeadlocked atomic.Bool
		var childDone <-chan struct{}
		var childPutErr error
		firstPressure.Store(true)
		firstEvict.Store(true)

		cache = NewMapCache[int](1,
			WithInvariantChecking(true),
			WithPressureFunc(func() float64 {
				pressureCalls.Add(1)
				if firstPressure.CompareAndSwap(true, false) {
					<-spawnReturningInnerGoFunc(func() {
						_, _ = cache.Delete("missing_from_outer_closure_wrapper")
					})
				}
				return 0.10
			}),
			WithOnEvictValue(func(_ int, _ EvictionReason) {
				evictCalls.Add(1)
				if firstEvict.CompareAndSwap(true, false) {
					childDone = spawnReturningInnerGoFunc(func() {
						_, childPutErr = cache.Put("k3_from_outer_closure_wrapper", 3)
					})
					select {
					case <-childDone:
					case <-time.After(5 * time.Second):
						evictDeadlocked.Store(true)
					}
				}
			}),
		)

		// Act
		_, err := cache.Put("k1", 1)
		require.NoError(t, err)
		callsAfterFirstPut := pressureCalls.Load()

		_, err = cache.Put("k2", 2)
		require.NoError(t, err)

		// Assert
		require.False(t, evictDeadlocked.Load(), "deadlock: OnEvictValue deadlocked when spawning child via returning outer helper closure")
		<-childDone
		require.NoError(t, childPutErr)
		assert.Equal(t, int32(1), callsAfterFirstPut, "returning outer helper closure using go func() { fn() }() must not bypass PressureFunc re-entrancy guard")
		assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and k2 eviction callbacks must complete")
	})
}

//go:noinline
func runSyncGoroutineChain(depth int, leafFn func()) {
	if depth <= 0 {
		leafFn()
		return
	}
	if depth%2 == 0 {
		var wg sync.WaitGroup
		wg.Go(func() {
			runSyncGoroutineChain(depth-1, leafFn)
		})
		wg.Wait()
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runSyncGoroutineChain(depth-1, leafFn)
	}()
	<-done
}

//go:noinline
func runSyncGoroutineChainWithExitedMidHop(t *testing.T, depth, exitAtDepth int, leafFn func()) {
	t.Helper()
	leafDone := make(chan struct{})
	var step func(rem int)
	step = func(rem int) {
		if rem <= 0 {
			defer close(leafDone)
			leafFn()
			return
		}
		if rem == exitAtDepth {
			var midGID atomic.Uint64
			midGID.Store(currentGoroutineID())
			midExited := make(chan struct{})
			go func() {
				<-midExited
				waitForGoroutineExit(t, midGID.Load())
				step(rem - 1)
			}()
			close(midExited)
			return
		}
		childDone := make(chan struct{})
		go func() {
			defer close(childDone)
			step(rem - 1)
		}()
		if rem > exitAtDepth {
			<-leafDone
		} else {
			<-childDone
		}
	}
	step(depth)
	<-leafDone
}

func verifyDeepOnEvictChainForBackendAndDepth(t *testing.T, b Backend, depth int) {
	t.Helper()

	var cache Cache[int]
	var traceMu sync.Mutex
	var trace []string
	var k1ValRan, k1EntryRan atomic.Bool
	var leafPutErr error
	var leafDeletedOK bool

	recordTrace := func(ev string) {
		traceMu.Lock()
		trace = append(trace, ev)
		traceMu.Unlock()
	}

	cache = New[int](3,
		WithBackend(b),
		WithInvariantChecking(true),
		WithOnEvictValue(func(v int, r EvictionReason) {
			recordTrace(fmt.Sprintf("val:%d:%v", v, r))
			if v == 10 && k1ValRan.CompareAndSwap(false, true) {
				runSyncGoroutineChain(depth, func() {
					_, leafDeletedOK = cache.Delete("k2")
				})
			}
		}),
		WithOnEvictEntry(func(k string, v int, r EvictionReason) {
			recordTrace(fmt.Sprintf("entry:%s=%d:%v", k, v, r))
			if k == "k1" && k1EntryRan.CompareAndSwap(false, true) {
				runSyncGoroutineChain(depth, func() {
					_, leafPutErr = cache.Put("k3", 40)
				})
			}
		}),
	)

	_, err := cache.Put("k1", 10)
	require.NoError(t, err)
	_, err = cache.Put("k2", 20)
	require.NoError(t, err)
	_, err = cache.Put("k3", 30)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		v, ok := cache.Delete("k1")
		assert.True(t, ok)
		assert.Equal(t, 10, v)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		require.Failf(t, "deadlock", "backend %v deadlocked on OnEvict* synchronous goroutine chain at depth %d", b, depth)
	}

	require.True(t, leafDeletedOK)
	require.NoError(t, leafPutErr)

	expected := []string{
		fmt.Sprintf("val:10:%v", EvictionReasonDeleted),
		fmt.Sprintf("entry:k1=10:%v", EvictionReasonDeleted),
		fmt.Sprintf("val:20:%v", EvictionReasonDeleted),
		fmt.Sprintf("entry:k2=20:%v", EvictionReasonDeleted),
		fmt.Sprintf("val:30:%v", EvictionReasonReplaced),
		fmt.Sprintf("entry:k3=30:%v", EvictionReasonReplaced),
	}
	assert.Equal(t, expected, trace, "all OnEvictValue and OnEvictEntry callbacks must fire in strict FIFO order at depth %d on %v", depth, b)
}

func verifyDeepPressureFuncChainForBackendAndDepth(t *testing.T, b Backend, depth int) {
	t.Helper()

	var cache Cache[int]
	var pressureCalls atomic.Int32
	var leafPutErr error
	var leafDeletedOK bool

	cache = New[int](10,
		WithBackend(b),
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			if pressureCalls.Add(1) == 1 {
				runSyncGoroutineChain(depth, func() {
					_, leafPutErr = cache.Put("leaf_temp", 100)
					_, leafDeletedOK = cache.Delete("leaf_temp")
				})
			}
			return 0.10
		}),
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := cache.Put("root_key", 1)
		assert.NoError(t, err)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		require.Failf(t, "deadlock", "backend %v deadlocked on PressureFunc synchronous goroutine chain at depth %d", b, depth)
	}

	require.NoError(t, leafPutErr)
	require.True(t, leafDeletedOK)
	assert.Equal(t, int32(1), pressureCalls.Load(),
		"depth-%d descendant Put/Delete inside PressureFunc must bypass re-entrant PressureFunc on %v", depth, b)
	assert.Equal(t, 1, cache.Stats().Len)
}

func verifyDeepExitedMidHopForBackend(t *testing.T, b Backend) {
	t.Helper()

	var cache Cache[int]
	var pressureCalls, evictCalls atomic.Int32
	var leafPutErr error

	cache = New[int](1,
		WithBackend(b),
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			if pressureCalls.Add(1) == 1 {
				runSyncGoroutineChainWithExitedMidHop(t, 12, 6, func() {
					_, _ = cache.Delete("missing_from_depth12_exited_midhop")
				})
			}
			return 0.10
		}),
		WithOnEvictEntry(func(_ string, _ int, _ EvictionReason) {
			if evictCalls.Add(1) == 1 {
				runSyncGoroutineChainWithExitedMidHop(t, 12, 6, func() {
					_, leafPutErr = cache.Put("k3_from_depth12_exited_midhop", 3)
				})
			}
		}),
	)

	_, err := cache.Put("k1", 1)
	require.NoError(t, err)
	assert.Equal(t, int32(1), pressureCalls.Load(), "depth-12 chain with exited intermediate hop must bypass PressureFunc on %v", b)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, putErr := cache.Put("k2", 2)
		assert.NoError(t, putErr)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		require.Failf(t, "deadlock", "backend %v deadlocked on depth-12 OnEvictEntry chain with exited intermediate hop", b)
	}
	require.NoError(t, leafPutErr)
	assert.Equal(t, int32(2), evictCalls.Load())
}

// TestPressureLineage_DeepDescendantReentrancyChainBeyondEightHops verifies deep descendant
// attribution across depths 1, 8, 12, and 16 and all three backends (BackendMap, BackendRadix,
// BackendArenaRadix) for both OnEvictValue/OnEvictEntry and WithPressureFunc,
// including chains where an intermediate ancestor goroutine has already exited.
func TestPressureLineage_DeepDescendantReentrancyChainBeyondEightHops(t *testing.T) {
	backends := []Backend{BackendMap, BackendRadix, BackendArenaRadix}
	depths := []int{1, 8, 12, 16}

	t.Run("OnEvictEntryAndOnEvictValue_Depths_1_8_12_16_AllBackends", func(t *testing.T) {
		for _, b := range backends {
			for _, d := range depths {
				t.Run(fmt.Sprintf("%s_Depth%d", b.String(), d), func(t *testing.T) {
					verifyDeepOnEvictChainForBackendAndDepth(t, b, d)
				})
			}
		}
	})

	t.Run("WithPressureFunc_Depths_1_8_12_16_AllBackends", func(t *testing.T) {
		for _, b := range backends {
			for _, d := range depths {
				t.Run(fmt.Sprintf("%s_Depth%d", b.String(), d), func(t *testing.T) {
					verifyDeepPressureFuncChainForBackendAndDepth(t, b, d)
				})
			}
		}
	})

	t.Run("ExitedIntermediateAncestorAtDepth12_AllBackends", func(t *testing.T) {
		for _, b := range backends {
			t.Run(b.String(), func(t *testing.T) {
				verifyDeepExitedMidHopForBackend(t, b)
			})
		}
	})
}

//go:noinline
func spawnNamedTargetViaReturningHelper(fn func()) {
	go fn()
}

// TestPressureLineage_NamedMethodChildViaReturningHelperAndChannelSync verifies that spawning
// a synchronous child goroutine inside OnEvict* or PressureFunc by passing a named method
// value to a returning helper function (or wg.Go synchronized via a channel before wg.Wait)
// recognizes the child goroutine as a descendant of the callback/sampler holder.
func TestPressureLineage_NamedMethodChildViaReturningHelperAndChannelSync(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			var pressureCalls, evictCalls atomic.Int32
			var evictArmed atomic.Bool

			wPress := &methodChildWorker{
				putDone: make(chan struct{}),
			}
			wPressWG := &methodChildWorker{
				putDone: make(chan struct{}),
			}
			w := &methodChildWorker{
				putDone: make(chan struct{}),
			}
			wWG := &methodChildWorker{
				putDone: make(chan struct{}),
			}

			cache := New[int](4,
				WithBackend(b),
				WithInvariantChecking(true),
				WithPressureFunc(func() float64 {
					if pressureCalls.Add(1) == 1 {
						spawnNamedTargetViaReturningHelper(wPress.putFromChild)
						<-wPress.putDone

						var wg sync.WaitGroup
						wg.Go(wPressWG.putFromChild)
						<-wPressWG.putDone
						wg.Wait()
					}
					return 0.10
				}),
				WithOnEvictValue(func(_ int, _ EvictionReason) {
					if !evictArmed.Load() {
						return
					}
					switch evictCalls.Add(1) {
					case 1:
						spawnNamedTargetViaReturningHelper(w.putFromChild)
						<-w.putDone
					case 2:
						var wg sync.WaitGroup
						wg.Go(wWG.putFromChild)
						<-wWG.putDone
						wg.Wait()
					}
				}),
			)
			wPress.cache = cache
			wPressWG.cache = cache
			w.cache = cache
			wWG.cache = cache

			_, err := cache.Put("k1", 1)
			require.NoError(t, err)
			require.NoError(t, wPress.putErr)
			require.NoError(t, wPressWG.putErr)
			assert.Equal(t, int32(1), pressureCalls.Load(), "named method child spawned via returning helper and wg.Go inside PressureFunc must not re-sample on %v", b)

			evictArmed.Store(true)

			done := make(chan struct{})
			go func() {
				defer close(done)
				_, ok := cache.Delete("k1")
				assert.True(t, ok)
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				require.Failf(t, "deadlock", "backend %v deadlocked on named method child spawned via returning helper / wg.Go inside OnEvictValue", b)
			}

			require.NoError(t, w.putErr)
			require.NoError(t, wWG.putErr)
			assert.Equal(t, int32(3), evictCalls.Load(), "Delete(k1) and both child Put replacements of k3_from_named_method must fire OnEvictValue on %v", b)
		})
	}
}

type genericCallbackBox[T any] struct {
	val T
}

//go:noinline
func (b *genericCallbackBox[T]) spawnWithExitedIntermediate(t *testing.T, fn func()) {
	t.Helper()
	midExited := make(chan struct{})
	leafDone := make(chan struct{})
	var midGID atomic.Uint64
	go func() {
		midGID.Store(currentGoroutineID())
		go func() {
			defer close(leafDone)
			<-midExited
			waitForGoroutineExit(t, midGID.Load())
			fn()
		}()
		close(midExited)
	}()
	<-leafDone
}

//go:noinline
func runGenericSetupAndExitedChildScenario[T any](t *testing.T, val T) {
	t.Helper()

	workerTrigger := make(chan struct{})
	workerDone := make(chan struct{})
	var cache Cache[T]
	var calls atomic.Int32
	var evictCalls atomic.Int32
	var workerErr error

	// Setup closure inside generic function runGenericSetupAndExitedChildScenario[T].
	startSetup := func() {
		go func() {
			<-workerTrigger
			_, workerErr = cache.Put("worker_key", val)
			close(workerDone)
		}()
	}
	startSetup()

	box := &genericCallbackBox[T]{val: val}
	var timedOut atomic.Bool
	cache = NewMapCache[T](2,
		WithInvariantChecking(true),
		WithPressureFunc(func() float64 {
			c := calls.Add(1)
			if c == 1 {
				box.spawnWithExitedIntermediate(t, func() {
					_, _ = cache.Delete("missing_generic_box_child")
				})
				close(workerTrigger)
				select {
				case <-workerDone:
				case <-time.After(5 * time.Second):
					timedOut.Store(true)
				}
			}
			return 0.10
		}),
		WithOnEvictValue(func(_ T, _ EvictionReason) {
			if evictCalls.Add(1) == 1 {
				box.spawnWithExitedIntermediate(t, func() {
					_, _ = cache.Delete("worker_key")
				})
			}
		}),
	)

	_, err := cache.Put("parent_key", val)
	require.NoError(t, err)
	require.False(t, timedOut.Load(), "timed out waiting for worker goroutine inside PressureFunc")
	<-workerDone
	require.NoError(t, workerErr)

	// Assert 1: Pre-existing setup closure worker in generic function sampled PressureFunc independently,
	// while the exited-intermediate child inside generic method box.spawnWithExitedIntermediate did not re-sample.
	assert.Equal(t, int32(2), calls.Load(), "pre-existing setup closure worker in generic function must sample while exited-intermediate generic method child must not")

	// Act 2: Trigger OnEvictValue, which uses generic method box.spawnWithExitedIntermediate to delete worker_key.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, ok := cache.Delete("parent_key")
		assert.True(t, ok)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "deadlock in generic receiver method child inside OnEvictValue")
	}
	assert.Equal(t, int32(2), evictCalls.Load())
}

// TestPressureLineage_SiblingClosureWorkerAndGenericClosureOwnerAttribution verifies:
//  1. When an outer function defines a worker closure (outer.func1) and
//     OnEvict* / PressureFunc (outer.func2) spawns that sibling closure directly via
//     `go worker()` or via an inner closure, or calls a sibling helper closure to spawn
//     a named method, the spawned goroutine is recognized as a descendant rather than
//     deadlocking on evictCallbackMu or bypassing PressureFunc re-entrancy guards.
//  2. Closures compiled inside generic functions (`fn[go.shape...].func1`)
//     and generic receiver methods (`(*Box[go.shape...]).Method.func1`) resolve their
//     generic owner frames (`fn[...]` and `(*Box[...]).Method`) in runtime stacks.
func TestPressureLineage_SiblingClosureWorkerAndGenericClosureOwnerAttribution(t *testing.T) {
	t.Run("DirectGoAndInnerClosureSpawningOuterWorkerClosure", func(t *testing.T) {
		for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			t.Run(b.String(), func(t *testing.T) {
				var cache Cache[int]
				var pressureCalls, evictCalls atomic.Int32
				var k1EvictRan atomic.Bool

				evictWorkerDone := make(chan struct{})
				evictWorker := func() {
					defer close(evictWorkerDone)
					_, _ = cache.Delete("k2")
				}

				pressureWorkerDone := make(chan struct{})
				pressureWorker := func() {
					defer close(pressureWorkerDone)
					_, _ = cache.Delete("missing_sibling_worker")
				}

				spawnSiblingHelper := func(fn func()) {
					go fn()
				}

				w := &methodChildWorker{
					putDone: make(chan struct{}),
				}

				cache = New[int](10,
					WithBackend(b),
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						if pressureCalls.Add(1) == 1 {
							go pressureWorker()
							<-pressureWorkerDone
						}
						return 0.10
					}),
					WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
						evictCalls.Add(1)
						if key == "k1" && k1EvictRan.CompareAndSwap(false, true) {
							go evictWorker()
							<-evictWorkerDone
							spawnSiblingHelper(w.putFromChild)
							<-w.putDone
						}
					}),
				)
				w.cache = cache

				_, err := cache.Put("k1", 1)
				require.NoError(t, err)
				assert.Equal(t, int32(1), pressureCalls.Load(), "sibling closure worker spawned via go pressureWorker() inside PressureFunc must not re-sample")

				_, err = cache.Put("k2", 2)
				require.NoError(t, err)

				delDone := make(chan struct{})
				go func() {
					defer close(delDone)
					_, ok := cache.Delete("k1")
					assert.True(t, ok)
				}()

				select {
				case <-delDone:
				case <-time.After(5 * time.Second):
					require.Failf(t, "deadlock", "backend %v deadlocked when OnEvictEntry spawned outer sibling closure worker", b)
				}
				require.NoError(t, w.putErr)
				assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and k2 eviction callbacks must fire on %v", b)
			})
		}
	})

	t.Run("GenericFunctionAndGenericMethodClosureOwnerResolution", func(t *testing.T) {
		runGenericSetupAndExitedChildScenario(t, [2]func(int){nil, nil})
	})
}

func makeInlinedClosureTask(fn func()) func() {
	return func() {
		fn()
	}
}

func makeInlinedGenericClosureTask[T any](v T, fn func(T)) func() {
	return func() {
		fn(v)
	}
}

type inlinedGenericTaskBox[T any] struct {
	val T
}

func (b *inlinedGenericTaskBox[T]) makeMethodTask(fn func(T)) func() {
	return func() {
		fn(b.val)
	}
}

//go:noinline
func spawnViaCustomWrapper(fn func()) {
	go func() {
		fn()
	}()
}

type liveIntermediateStageRunner struct {
	t            *testing.T
	cache        Cache[int]
	key          string
	decoyEntered chan struct{}
	decoyRelease <-chan struct{}
}

//go:noinline
func (r *liveIntermediateStageRunner) runLiveStageWithExitedMidHop() {
	if r.decoyRelease != nil {
		close(r.decoyEntered)
		<-r.decoyRelease
		return
	}
	g3Done := make(chan struct{})
	g2Started := make(chan uint64, 1)
	go func() {
		g2Started <- currentGoroutineID()
		spawnViaCustomWrapper(func() {
			defer close(g3Done)
			g2ID := <-g2Started
			waitForGoroutineExit(r.t, g2ID)
			_, _ = r.cache.Delete(r.key)
		})
	}()
	<-g3Done
}

// TestPressureLineage_InlinedClosureOwnersAndLiveIntermediateExitedChildBridge verifies:
//  1. findClosureOwnerInStack resolves closures created inside inlined helper functions,
//     inlined generic functions, and inlined methods on generic types (e.g.,
//     pkg.Outer.makeInlinedClosureTask.func1, pkg.Outer.makeInlinedGenericClosureTask[...].func1,
//     and pkg.Outer.(*inlinedGenericTaskBox[...]).makeMethodTask.func1), as well as
//     trimClosureOffset with whitespace before "+0x" and hasClosureOwnedBy across generic bracket styles.
//  2. findAncestorCreatorAboveHook bridges across exited intermediate closure goroutines (G2)
//     to live intermediate named-function/method ancestor goroutines (G1) on the path up to G0
//     (G0 -> G1 [live named method] -> G2 [exited closure] -> G3 [closure calling cache]).
func TestPressureLineage_InlinedClosureOwnersAndLiveIntermediateExitedChildBridge(t *testing.T) {
	t.Run("Unit_InlinedClosureOwnerAndGenericSymbolHelpers", func(t *testing.T) {
		blk := []byte("goroutine 10 [running]:\ngithub.com/google/go-lru.outerCaller(0x1)\n\t/path/file.go:10 +0x20\ngithub.com/google/go-lru.(*genericCallbackBox[...]).outerMethod(0x1)\n\t/path/file.go:20 +0x30\n")

		assert.Equal(t,
			[]byte("github.com/google/go-lru.outerCaller"),
			findClosureOwnerInStack(blk, []byte("github.com/google/go-lru.outerCaller.makeInlinedClosureTask.func1 +0x40")),
		)
		assert.Equal(t,
			[]byte("github.com/google/go-lru.outerCaller"),
			findClosureOwnerInStack(blk, []byte("github.com/google/go-lru.outerCaller.makeInlinedGenericClosureTask[...].func1")),
		)
		assert.Equal(t,
			[]byte("github.com/google/go-lru.(*genericCallbackBox[...]).outerMethod"),
			findClosureOwnerInStack(blk, []byte("github.com/google/go-lru.(*genericCallbackBox[...]).outerMethod.(*inlinedGenericTaskBox[...]).makeMethodTask.func1")),
		)
		assert.Nil(t, findClosureOwnerInStack(blk, []byte("github.com/google/go-lru.outerCaller.nonClosureHelper")))

		aboveHook := []byte("goroutine 10 [running]:\ngithub.com/google/go-lru.(*genericCallbackBox[...]).outerMethod.func2()\n\t/path/file.go:15 +0x10\n")
		assert.True(t, hasClosureOwnedBy(
			aboveHook,
			blk,
			[]byte("github.com/google/go-lru.(*genericCallbackBox[int]).outerMethod"),
		))
	})

	t.Run("InlinedHelperClosuresInPreExistingWorkerAndCallbackExitedHop", func(t *testing.T) {
		for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			t.Run(b.String(), func(t *testing.T) {
				var cache Cache[int]
				var calls atomic.Int32
				var evictCalls atomic.Int32
				workerTrigger := make(chan struct{})
				workerDone := make(chan struct{})
				var workerErr error
				var timedOut atomic.Bool

				box := &inlinedGenericTaskBox[int]{val: 7}
				// Spawn pre-existing worker before Put using an inlined generic method returning a closure.
				go box.makeMethodTask(func(v int) {
					<-workerTrigger
					_, workerErr = cache.Put("worker_key", v)
					close(workerDone)
				})()

				spawnExitedViaInlinedHelpers := func(action func()) {
					g3Done := make(chan struct{})
					g2Started := make(chan uint64, 1)
					go makeInlinedClosureTask(func() {
						g2Started <- currentGoroutineID()
						go makeInlinedGenericClosureTask(1, func(_ int) {
							defer close(g3Done)
							g2ID := <-g2Started
							waitForGoroutineExit(t, g2ID)
							action()
						})()
					})()
					<-g3Done
				}

				cache = New[int](2,
					WithBackend(b),
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						c := calls.Add(1)
						if c == 1 {
							spawnExitedViaInlinedHelpers(func() {
								_, _ = cache.Delete("missing_inlined_child")
							})
							close(workerTrigger)
							select {
							case <-workerDone:
							case <-time.After(5 * time.Second):
								timedOut.Store(true)
							}
						}
						return 0.10
					}),
					WithOnEvictValue(func(_ int, _ EvictionReason) {
						if evictCalls.Add(1) == 1 {
							spawnExitedViaInlinedHelpers(func() {
								_, _ = cache.Delete("worker_key")
							})
						}
					}),
				)

				_, err := cache.Put("parent_key", 1)
				require.NoError(t, err)
				require.False(t, timedOut.Load(), "timed out waiting for pre-existing worker in PressureFunc")
				<-workerDone
				require.NoError(t, workerErr)
				assert.Equal(t, int32(2), calls.Load(), "pre-existing worker from inlined generic method closure must sample independently while callback child must not")

				done := make(chan struct{})
				go func() {
					defer close(done)
					_, _ = cache.Delete("parent_key")
				}()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					require.Failf(t, "deadlock", "backend %v deadlocked in OnEvictValue with inlined closure helpers and exited intermediate hop", b)
				}
				assert.Equal(t, int32(2), evictCalls.Load(), "both parent_key and worker_key eviction callbacks must fire on %v", b)
			})
		}
	})

	t.Run("LiveIntermediateNamedMethodAncestorWithExitedClosureChildInHooks", func(t *testing.T) {
		for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			t.Run(b.String(), func(t *testing.T) {
				var cache Cache[int]
				var pressureCalls atomic.Int32
				var callsDuringPressure atomic.Int32
				var evictCalls atomic.Int32
				var armed atomic.Bool

				// Spawn an earlier decoy goroutine parked inside runLiveStageWithExitedMidHop
				// whose ancestor chain walks up to main (currGID == 0) without passing through G0.
				decoyEntered := make(chan struct{})
				decoyRelease := make(chan struct{})
				var decoyWG sync.WaitGroup
				decoyRunner := &liveIntermediateStageRunner{
					t:            t,
					decoyEntered: decoyEntered,
					decoyRelease: decoyRelease,
				}
				decoyWG.Go(decoyRunner.runLiveStageWithExitedMidHop)
				<-decoyEntered
				defer func() {
					close(decoyRelease)
					decoyWG.Wait()
				}()

				cache = New[int](3,
					WithBackend(b),
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						if !armed.Load() {
							return 0.10
						}
						if pressureCalls.Add(1) == 1 {
							runner := &liveIntermediateStageRunner{
								t:     t,
								cache: cache,
								key:   "missing_live_stage_key",
							}
							var wg sync.WaitGroup
							wg.Go(runner.runLiveStageWithExitedMidHop)
							wg.Wait()
							callsDuringPressure.Store(pressureCalls.Load())
						}
						return 0.10
					}),
					WithOnEvictValue(func(_ int, _ EvictionReason) {
						if !armed.Load() {
							return
						}
						if evictCalls.Add(1) == 1 {
							runner := &liveIntermediateStageRunner{
								t:     t,
								cache: cache,
								key:   "k2",
							}
							var wg sync.WaitGroup
							wg.Go(runner.runLiveStageWithExitedMidHop)
							wg.Wait()
						}
					}),
				)

				_, err := cache.Put("k1", 1)
				require.NoError(t, err)
				_, err = cache.Put("k2", 2)
				require.NoError(t, err)
				_, err = cache.Put("k3", 3)
				require.NoError(t, err)

				armed.Store(true)

				done := make(chan struct{})
				go func() {
					defer close(done)
					_, putErr := cache.Put("k4", 4) // samples PressureFunc, then evicts k1 -> OnEvictValue deletes k2 via G1 (live) -> G2 (exited) -> G3
					assert.NoError(t, putErr)
				}()

				select {
				case <-done:
				case <-time.After(5 * time.Second):
					require.Failf(t, "deadlock", "backend %v deadlocked when G0 spawned live intermediate named method G1 -> exited closure G2 -> G3", b)
				}

				assert.Equal(t, int32(1), callsDuringPressure.Load(), "G3 Delete under live intermediate named method G1 and exited G2 must not re-sample PressureFunc on %v", b)
				assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and k2 eviction callbacks must fire on %v", b)
			})
		}
	})
}
