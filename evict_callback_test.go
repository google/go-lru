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
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEvictCallback_PanicRecoveryPreservesCacheInvariantsAndReleasesSlots verifies that:
//  1. User OnEvict* callbacks are invoked after internal state transitions and c.mu.Unlock() complete,
//     so a recovered panic in OnEvict* never leaves the cache in a corrupted state.
//  2. WithInvariantChecking(true) runs checkInvariants() before OnEvict* and with defer c.mu.Unlock(),
//     never masking user callback panics or leaking c.mu.
//  3. evictCallbackQueue zeroes inline buf and overflow slots both on normal completion and mid-batch panic.
func TestEvictCallback_PanicRecoveryPreservesCacheInvariantsAndReleasesSlots(t *testing.T) {
	t.Run("MapCache_DeletePrefixEmpty_PreservesStateOnCallbackPanic", func(t *testing.T) {
		// Arrange
		panicNow := false
		c := NewMapCache[string](100,
			WithInvariantChecking(true),
			WithOnEvictValue(func(_ string, _ EvictionReason) {
				if panicNow {
					panic("simulated user callback panic")
				}
			}),
		)
		_, _ = c.Put("k1", "v1")
		_, _ = c.Put("k2", "v2")

		// Act
		panicNow = true
		require.PanicsWithValue(t, "simulated user callback panic", func() {
			c.DeletePrefix("")
		})
		panicNow = false

		// Assert
		st := c.Stats()
		keys := slices.Collect(c.Keys())
		assert.Equal(t, 0, st.Len)
		assert.Equal(t, uint64(0), st.CurrentSize)
		assert.Empty(t, keys)
	})

	t.Run("ArenaRadixCache_DeletePrefix_PreservesTreeAndSubsequentEvictionAfterCallbackPanic", func(t *testing.T) {
		// Arrange
		panicNow := false
		c := NewArenaRadixCache[int](20,
			WithInvariantChecking(true),
			WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
			WithOnEvictEntry(func(_ string, _ int, _ EvictionReason) {
				if panicNow {
					panic("simulated arena callback panic")
				}
			}),
		)
		_, _ = c.Put("app/db/1", 10)
		_, _ = c.Put("app/db/2", 10)

		// Act
		panicNow = true
		require.PanicsWithValue(t, "simulated arena callback panic", func() {
			c.DeletePrefix("app/db/")
		})
		panicNow = false

		// Assert
		require.NotPanics(t, func() {
			_, err := c.Put("new_entry", 15)
			require.NoError(t, err)
		})
		st := c.Stats()
		assert.Equal(t, 1, st.Len)
		assert.Equal(t, uint64(15), st.CurrentSize)
	})

	t.Run("AllBackends_PutOverwrite_PreservesCurrentSizeOnCallbackPanic", func(t *testing.T) {
		for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
			// Arrange
			panicOnCap := false
			c := New[int](100,
				WithBackend(b),
				WithInvariantChecking(true),
				WithWeigher(func(_ string, v int) uint64 { return uint64(v) }),
				WithOnEvictValue(func(_ int, r EvictionReason) {
					if panicOnCap && r == EvictionReasonCapacity {
						panic("simulated capacity eviction panic")
					}
				}),
			)
			_, _ = c.Put("lru_victim", 50)
			_, _ = c.Put("updated_key", 40) // total = 90

			// Act
			panicOnCap = true
			require.PanicsWithValue(t, "simulated capacity eviction panic", func() {
				_, _ = c.Put("updated_key", 80)
			})
			panicOnCap = false

			// Assert
			st := c.Stats()
			assert.Equal(t, 1, st.Len)
			assert.Equal(t, uint64(80), st.CurrentSize)
			val, ok := c.Get("updated_key")
			assert.True(t, ok)
			assert.Equal(t, 80, val)
		}
	})

	t.Run("InvariantChecking_PreservesOriginalPanicAndReleasesMutexLock", func(t *testing.T) {
		// Arrange
		panicNow := false
		c := NewArenaRadixCache[string](100,
			WithInvariantChecking(true),
			WithOnEvictValue(func(_ string, _ EvictionReason) {
				if panicNow {
					panic("original user callback panic")
				}
			}),
		).(*arenaRadix[string])
		_, _ = c.Put("dir/a", "v1")
		_, _ = c.Put("dir/b", "v2")

		// Act
		panicNow = true
		var recovered any
		func() {
			defer func() {
				recovered = recover()
			}()
			c.DeletePrefix("dir/")
		}()
		panicNow = false

		// Assert
		require.Equal(t, "original user callback panic", recovered)

		done := make(chan struct{})
		go func() {
			c.mu.RLock()
			remaining := c.len
			c.mu.RUnlock()
			assert.Zero(t, remaining)
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			require.Fail(t, "c.mu remained locked after callback panic")
		}
	})

	t.Run("EvictCallbackQueue_ZeroesBufAndOverflowOnCompletionAndMidBatchPanic", func(t *testing.T) {
		// Arrange
		var p pressureState
		var q evictCallbackQueue[*int]
		vals := [4]int{10, 20, 30, 40}
		for i := range vals {
			q.enqueue(fmt.Sprintf("key_%d", i), &vals[i], EvictionReasonDeleted)
		}
		require.Equal(t, 4, q.n)
		require.Len(t, q.overflow, 2)
		overflowBacking := q.overflow[:len(q.overflow)]

		// Act: Normal completion across inline buf and overflow slice.
		callIdx := 0
		q.invoke(&p, func(v *int, _ EvictionReason) {
			require.NotNil(t, v)
			if callIdx < len(q.buf) {
				assert.Equal(t, evictEvent[*int]{}, q.buf[callIdx], "inline buf slot must be zeroed before callback runs")
			} else {
				assert.Equal(t, evictEvent[*int]{}, overflowBacking[callIdx-len(q.buf)], "overflow slot must be zeroed before callback runs")
			}
			callIdx++
		}, nil)

		// Assert: Normal completion zeroes all inline and overflow slots.
		assert.Equal(t, 4, callIdx)
		assert.Zero(t, q.n)
		assert.Nil(t, q.overflow)
		assert.Equal(t, [2]evictEvent[*int]{}, q.buf)
		assert.Equal(t, []evictEvent[*int]{{}, {}}, overflowBacking)

		// Arrange: Re-populate queue and panic mid-batch on the first event.
		for i := range vals {
			q.enqueue(fmt.Sprintf("panic_key_%d", i), &vals[i], EvictionReasonDeleted)
		}
		overflowBackingPanic := q.overflow[:len(q.overflow)]

		// Act: Panic on the first callback while inline buf[1] and overflow[0..1] remain unconsumed.
		require.PanicsWithValue(t, "mid-batch callback panic", func() {
			q.invoke(&p, nil, func(_ string, _ *int, _ EvictionReason) {
				panic("mid-batch callback panic")
			})
		})

		// Assert: Deferred reset clears all remaining inline buf and overflow slots.
		assert.Zero(t, q.n)
		assert.Nil(t, q.overflow)
		assert.Equal(t, [2]evictEvent[*int]{}, q.buf)
		assert.Equal(t, []evictEvent[*int]{{}, {}}, overflowBackingPanic)
	})
}

// TestEvictCallback_FastTagDoesNotRetainCacheAcrossGC verifies that invoking OnEvict* and
// PressureFunc on a cache instance never retains a strong pointer to the cache or its
// cached entries in package-global tag state after the caller drops the cache.
func TestEvictCallback_FastTagDoesNotRetainCacheAcrossGC(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange: Create a cache inside a non-inlined helper, trigger both PressureFunc
			// and OnEvictValue on the fast path, and keep a weak.Pointer to a surviving value.
			wp := createAndDropCacheForGCLeakCheck(t, b)

			// Act: Force garbage collection cycles after the cache reference has been dropped.
			for range 6 {
				runtime.GC()
				runtime.Gosched()
			}

			// Assert: Neither the cache struct nor its surviving cached entries remain reachable.
			assert.Nil(t, wp.Value(), "dropped %v instance and its cached entries must be garbage-collected", b)
		})
	}
}

//go:noinline
func createAndDropCacheForGCLeakCheck(t *testing.T, b Backend) weak.Pointer[[32]byte] {
	t.Helper()
	c := New[*[32]byte](2,
		WithBackend(b),
		WithPressureFunc(func() float64 { return 0.10 }),
		WithOnEvictValue(func(_ *[32]byte, _ EvictionReason) {}),
	)
	v1 := new([32]byte)
	v2 := new([32]byte)
	v3 := new([32]byte)
	v2[0] = 42
	_, err := c.Put("k1", v1)
	require.NoError(t, err)
	_, err = c.Put("k2", v2)
	require.NoError(t, err)
	_, err = c.Put("k3", v3) // Evicts k1 on the fast path while k2 and k3 remain in c.
	require.NoError(t, err)
	return weak.Make(v2)
}

// TestEvictCallback_PendingChildCallbackPanicReleasesMutexAndClearsQueue verifies that
// if a child goroutine spawned inside OnEvict* triggers a nested eviction whose queued
// callback panics inside drainPendingEvictCallbacks, evictCallbackMu is unlocked, all
// queued events/closures are zeroed, and subsequent evictions do not deadlock.
func TestEvictCallback_PendingChildCallbackPanicReleasesMutexAndClearsQueue(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange
			var cache Cache[*[4]int]
			var panicOnBatch atomic.Bool
			var evictedKeys []string
			var mu sync.Mutex

			cache = New[*[4]int](10,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictEntry(func(key string, _ *[4]int, _ EvictionReason) {
					mu.Lock()
					evictedKeys = append(evictedKeys, key)
					mu.Unlock()

					if key == "k1" {
						var wg sync.WaitGroup
						wg.Go(func() {
							pac := cache.(PressureAwareCache[*[4]int])
							pac.DeletePrefix("batch/")
						})
						wg.Wait()
					}
					if panicOnBatch.Load() && strings.HasPrefix(key, "batch/") {
						panic("simulated pending child callback panic")
					}
				}),
			)

			wpBatch2 := populateBug2Cache(t, cache)

			// Act: Delete k1 so its OnEvictEntry spawns a child that deletes batch/1 and batch/2,
			// and batch/1 panics when drained inside unlockEvictCallbackSlow.
			panicOnBatch.Store(true)
			require.PanicsWithValue(t, "simulated pending child callback panic", func() {
				_, _ = cache.Delete("k1")
			})
			panicOnBatch.Store(false)

			done := make(chan struct{})
			go func() {
				_, delOk := cache.Delete("k3")
				assert.True(t, delOk)
				close(done)
			}()

			// Assert: Subsequent eviction completes without deadlocking on evictCallbackMu,
			// and unconsumed events in the panicking child batch were zeroed for GC.
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				require.Failf(t, "deadlock", "backend %v permanently deadlocked on evictCallbackMu after pending child callback panic", b)
			}
			for range 6 {
				runtime.GC()
				runtime.Gosched()
			}
			assert.Nil(t, wpBatch2.Value(), "unconsumed event value in panicking pending callback batch must be zeroed")
			mu.Lock()
			assert.Contains(t, evictedKeys, "k3")
			mu.Unlock()
		})
	}
}

//go:noinline
func populateBug2Cache(t *testing.T, cache Cache[*[4]int]) weak.Pointer[[4]int] {
	t.Helper()
	v1, vBatch1, vBatch2, v3 := new([4]int), new([4]int), new([4]int), new([4]int)
	wpBatch2 := weak.Make(vBatch2)
	_, err := cache.Put("k1", v1)
	require.NoError(t, err)
	_, err = cache.Put("batch/1", vBatch1)
	require.NoError(t, err)
	_, err = cache.Put("batch/2", vBatch2)
	require.NoError(t, err)
	_, err = cache.Put("k3", v3)
	require.NoError(t, err)
	return wpBatch2
}

func currentGoroutineID() uint64 {
	stack, bufPtr := captureCurrentGoroutineStack()
	defer releaseCurrentGoroutineStack(bufPtr)
	gid, _ := parseStackGoroutineAndParentID(stack)
	return gid
}

func isGoroutineParkedInLockEvictCallbackSlow() bool {
	allStacks, bufPtr := captureAllGoroutineStacks()
	defer releaseAllGoroutineStacks(bufPtr)
	for rem := allStacks; len(rem) > 0; {
		var blk []byte
		blk, rem = nextGoroutineBlock(rem)
		if bytes.Contains(blk, []byte("lockEvictCallbackSlow(")) &&
			bytes.Contains(blk, []byte("sync.(*Mutex).Lock(")) &&
			!bytes.Contains(blk, []byte("isDescendantOfEvictHolder(")) &&
			!bytes.Contains(blk, []byte("resolveActiveEvictCallbackHolder(")) {
			return true
		}
	}
	return false
}

func waitForGoroutineExit(t *testing.T, targetGID uint64) {
	t.Helper()
	if targetGID == 0 {
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allStacks, bufPtr := captureAllGoroutineStacks()
		found := false
		for rem := allStacks; len(rem) > 0; {
			var blk []byte
			blk, rem = nextGoroutineBlock(rem)
			gid, _ := parseStackGoroutineAndParentID(blk)
			if gid == targetGID {
				found = true
				break
			}
		}
		releaseAllGoroutineStacks(bufPtr)
		if !found {
			return
		}
		runtime.Gosched()
	}
	assert.Failf(t, "timeout", "goroutine %d did not exit within 5s", targetGID)
}

func waitForEvictContentionOrDone(t *testing.T, p *pressureState, done <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			return
		default:
		}
		if p != nil && (p.evictPendingEnqueues.Load() != 0 || (p.evictCallbackGID.Load() != 0 && isGoroutineParkedInLockEvictCallbackSlow())) {
			return
		}
		runtime.Gosched()
	}
	assert.Fail(t, "timed out waiting for worker eviction callback contention")
}

// TestEvictCallback_DetachedChildDrainNestedEvictionSerializesFIFO verifies that when
// pending callbacks are drained (either via unlockEvictCallbackSlow or via the post-unlock
// TryLock path in enqueuePendingCallback), the draining goroutine is registered as the active
// callback holder with "deliverCallbacks" on the stack so nested synchronous and child-goroutine
// evictions inside the drained callback never self-deadlock on evictCallbackMu.
func TestEvictCallback_DetachedChildDrainNestedEvictionSerializesFIFO(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange
			var cache Cache[int]
			var evicted []string
			var mu sync.Mutex
			allDone := make(chan struct{})

			onEntry := func(key string, _ int, _ EvictionReason) {
				mu.Lock()
				evicted = append(evicted, key)
				n := len(evicted)
				mu.Unlock()

				switch key {
				case "k1":
					k2Enqueued := make(chan struct{})
					go func() {
						defer close(k2Enqueued)
						_, _ = cache.Delete("k2")
					}()
					<-k2Enqueued
				case "k2":
					// Trigger both a synchronous nested eviction and a wg.Go child nested eviction
					// while draining pending callbacks.
					_, _ = cache.Delete("k3")
					var wg sync.WaitGroup
					wg.Go(func() {
						_, _ = cache.Delete("k4")
					})
					wg.Wait()
				}
				if n == 4 {
					close(allDone)
				}
			}

			cache = New[int](10,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictEntry(onEntry),
			)
			for _, k := range []string{"k1", "k2", "k3", "k4"} {
				_, err := cache.Put(k, 1)
				require.NoError(t, err)
			}

			// Act: First verify the end-to-end detached child chain (k1 -> detached k2 -> k3 & k4).
			act1Done := make(chan struct{})
			go func() {
				defer close(act1Done)
				_, ok := cache.Delete("k1")
				assert.True(t, ok)
			}()

			select {
			case <-act1Done:
			case <-time.After(2 * time.Second):
				require.Failf(t, "deadlock", "backend %v deadlocked during detached child pending callback nested eviction", b)
			}
			select {
			case <-allDone:
			case <-time.After(2 * time.Second):
				require.Failf(t, "incomplete callbacks", "backend %v did not deliver all 4 eviction callbacks", b)
			}

			// Also deterministically exercise enqueuePendingCallback's direct TryLock() drain path
			// when evictCallbackMu is currently unlocked.
			_, err := cache.Put("k2", 2)
			require.NoError(t, err)
			_, err = cache.Put("k3", 3)
			require.NoError(t, err)
			_, err = cache.Put("k4", 4)
			require.NoError(t, err)

			var p *pressureState
			switch impl := cache.(type) {
			case *mapCache[int]:
				p = &impl.pressureState
			case *radixCache[int]:
				p = &impl.pressureState
			case *arenaRadix[int]:
				p = &impl.pressureState
			}

			directDone := make(chan struct{})
			go func() {
				var q evictCallbackQueue[int]
				q.enqueue("k2", 2, EvictionReasonDeleted)
				q.enqueuePendingCallback(p, nil, onEntry)
				close(directDone)
			}()

			// Assert
			select {
			case <-directDone:
			case <-time.After(2 * time.Second):
				require.Failf(t, "deadlock", "backend %v enqueuePendingCallback TryLock drain self-deadlocked on nested eviction", b)
			}
		})
	}
}

// TestEvictCallback_FastTagCollisionFallsBackWithoutStackDumpAllocs verifies that when
// more than 8 caches exist (e.g. caches[0] and caches[8]), holding caches[0] inside OnEvict*
// and PressureFunc does not force an uncontended caches[8] onto the slow stack-capture path
// or mark caches[8] as contended.
func TestEvictCallback_FastTagCollisionFallsBackWithoutStackDumpAllocs(t *testing.T) {
	// Arrange: Create 17 caches so caches[0], caches[8], and caches[16] would collide under modulo-8 indexing.
	const numCaches = 17
	inEvict0 := make(chan struct{})
	releaseEvict0 := make(chan struct{})
	var releaseEvict0Once sync.Once
	defer releaseEvict0Once.Do(func() { close(releaseEvict0) })
	inPressure0 := make(chan struct{})
	releasePressure0 := make(chan struct{})
	var releasePressure0Once sync.Once
	defer releasePressure0Once.Do(func() { close(releasePressure0) })
	var armEvict0, armPressure0 atomic.Bool

	caches := make([]*mapCache[int], numCaches)
	for i := range numCaches {
		idx := i
		c := NewMapCache[int](2,
			WithPressureFunc(func() float64 {
				if idx == 0 && armPressure0.CompareAndSwap(true, false) {
					close(inPressure0)
					<-releasePressure0
				}
				return 0.10
			}),
			WithOnEvictValue(func(_ int, _ EvictionReason) {
				if idx == 0 && armEvict0.CompareAndSwap(true, false) {
					close(inEvict0)
					<-releaseEvict0
				}
			}),
		).(*mapCache[int])
		_, err := c.Put("k1", 1)
		require.NoError(t, err)
		_, err = c.Put("k2", 2)
		require.NoError(t, err)
		caches[i] = c
	}

	armEvict0.Store(true)
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = caches[0].Put("k3", 3) // Evicts k1 on caches[0] and blocks inside OnEvictValue.
	})
	<-inEvict0

	// Act: Perform uncontended evictions and replacements on caches[8] and caches[16] while caches[0] is blocked.
	_, err := caches[8].Put("k3", 3)
	require.NoError(t, err)
	_, err = caches[16].Put("k3", 3)
	require.NoError(t, err)

	allocs8 := testing.AllocsPerRun(50, func() {
		_ = caches[8].Replace("k2", 20)
	})

	releaseEvict0Once.Do(func() { close(releaseEvict0) })
	wg.Wait()

	armPressure0.Store(true)
	wg.Go(func() {
		_ = caches[0].Replace("k2", 30) // Blocks caches[0] inside PressureFunc.
	})
	<-inPressure0

	_ = caches[8].Replace("k2", 40)
	_ = caches[16].Replace("k2", 40)

	releasePressure0Once.Do(func() { close(releasePressure0) })
	wg.Wait()

	// Assert: Neither caches[8] nor caches[16] was forced onto the slow stack-capture path or marked contended.
	assert.Zero(t, allocs8)
	assert.False(t, caches[8].evictCallbackContended.Load(), "uncontended caches[8] must not be marked evictCallbackContended")
	assert.Zero(t, caches[8].evictCallbackGID.Load(), "uncontended caches[8] must not capture evictCallbackGID")
	assert.False(t, caches[8].samplingContended.Load(), "uncontended caches[8] must not be marked samplingContended")
	assert.Zero(t, caches[8].samplingGID.Load(), "uncontended caches[8] must not capture samplingGID")
	assert.False(t, caches[16].evictCallbackContended.Load(), "uncontended caches[16] must not be marked evictCallbackContended")
	assert.False(t, caches[16].samplingContended.Load(), "uncontended caches[16] must not be marked samplingContended")
}

// TestEvictCallback_PanickingPendingCallbackDrainsNestedChildQueue verifies that when a
// drained pending child callback spawns another nested child goroutine that enqueues
// a second-generation callback into pendingEvictCallbacks and the drained callback then panics,
// unlockEvictCallbackSlow clears pendingEvictCallbacks and resets evictPendingEnqueues to 0.
func TestEvictCallback_PanickingPendingCallbackDrainsNestedChildQueue(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange
			var cache Cache[*[4]int]
			cache = New[*[4]int](10,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictEntry(func(key string, _ *[4]int, _ EvictionReason) {
					switch key {
					case "k1":
						var wg sync.WaitGroup
						wg.Go(func() {
							_, _ = cache.Delete("k2")
						})
						wg.Wait()
					case "k2":
						var wg sync.WaitGroup
						wg.Go(func() {
							_, _ = cache.Delete("k3")
						})
						wg.Wait()
						panic("simulated panic after nested child enqueue")
					}
				}),
			)

			wpV3 := populateIter2Bug4Cache(t, cache)
			p := extractPressureStateForIter2Test(cache)

			// Act
			require.PanicsWithValue(t, "simulated panic after nested child enqueue", func() {
				_, _ = cache.Delete("k1")
			})

			done := make(chan struct{})
			go func() {
				_, ok := cache.Delete("k4")
				assert.True(t, ok)
				close(done)
			}()

			// Assert
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				require.Failf(t, "deadlock", "backend %v deadlocked on subsequent Delete after panicking pending callback", b)
			}
			for range 6 {
				runtime.GC()
				runtime.Gosched()
			}
			assert.Zero(t, p.evictPendingEnqueues.Load(), "evictPendingEnqueues must be cleared when drainPendingEvictCallbacks unwinds on panic")
			assert.Nil(t, wpV3.Value(), "nested pending callback closure and its captured value v3 must be cleared on panic")
		})
	}
}

//go:noinline
func populateIter2Bug4Cache(t *testing.T, cache Cache[*[4]int]) weak.Pointer[[4]int] {
	t.Helper()
	v1, v2, v3, v4 := new([4]int), new([4]int), new([4]int), new([4]int)
	wpV3 := weak.Make(v3)
	_, err := cache.Put("k1", v1)
	require.NoError(t, err)
	_, err = cache.Put("k2", v2)
	require.NoError(t, err)
	_, err = cache.Put("k3", v3)
	require.NoError(t, err)
	_, err = cache.Put("k4", v4)
	require.NoError(t, err)
	return wpV3
}

func extractPressureStateForIter2Test[V any](cache Cache[V]) *pressureState {
	switch impl := cache.(type) {
	case *mapCache[V]:
		return &impl.pressureState
	case *radixCache[V]:
		return &impl.pressureState
	case *arenaRadix[V]:
		return &impl.pressureState
	default:
		return nil
	}
}

// TestEvictCallback_PrimaryCallbackPanicStillDrainsPendingChildCallbacks verifies
// that when the primary OnEvict* callback panics after a spawned child goroutine
// enqueued a pending callback, unlockEvictCallbackSlow discards the pending queue
// rather than executing pending callbacks during panic unwind, zeros all queued
// closures for GC, and releases evictCallbackMu cleanly.
func TestEvictCallback_PrimaryCallbackPanicStillDrainsPendingChildCallbacks(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			// Arrange
			var cache Cache[*[4]int]
			var childCallbackRan atomic.Bool
			cache = New[*[4]int](10,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictEntry(func(key string, _ *[4]int, _ EvictionReason) {
					switch key {
					case "k1":
						var wg sync.WaitGroup
						wg.Go(func() {
							_, _ = cache.Delete("k2")
						})
						wg.Wait()
						panic("simulated primary callback panic after child enqueue")
					case "k2":
						childCallbackRan.Store(true)
					}
				}),
			)

			wpV2 := populateIter3Cache(t, cache)
			p := extractPressureStateForIter2Test(cache)

			// Act
			require.PanicsWithValue(t, "simulated primary callback panic after child enqueue", func() {
				_, _ = cache.Delete("k1")
			})

			// Assert: k2's pending callback was NOT executed during k1's panic unwind,
			// evictPendingEnqueues is reset, and subsequent evictions succeed.
			assert.False(t, childCallbackRan.Load(), "pending child callback must not run during primary callback panic unwind")
			assert.Zero(t, p.evictPendingEnqueues.Load(), "evictPendingEnqueues must be reset to 0 after primary callback panic")

			_, ok := cache.Delete("k4")
			assert.True(t, ok)
			for range 6 {
				runtime.GC()
				runtime.Gosched()
			}
			assert.Nil(t, wpV2.Value(), "discarded pending callback closure must release captured value references")
		})
	}
}

//go:noinline
func populateIter3Cache(t *testing.T, cache Cache[*[4]int]) weak.Pointer[[4]int] {
	t.Helper()
	v1, v2, v3, v4 := new([4]int), new([4]int), new([4]int), new([4]int)
	wpV2 := weak.Make(v2)
	_, err := cache.Put("k1", v1)
	require.NoError(t, err)
	_, err = cache.Put("k2", v2)
	require.NoError(t, err)
	_, err = cache.Put("k3", v3)
	require.NoError(t, err)
	_, err = cache.Put("k4", v4)
	require.NoError(t, err)
	return wpV2
}

// TestEvictCallback_CrossCacheFastTagDrainDoesNotPoisonHolderGID verifies that
// when cacheA drains a pending re-entrant eviction callback after its fast-path callback,
// deliverCallbacksFast<tag> is already popped off cacheA's stack before fastEvictTagState[tag]
// is released. When cacheB claims the same tag on a newer goroutine and spawns a child
// goroutine inside OnEvict*, resolveEvictCallbackHolderFromAllStacksLocked resolves cacheB's
// own goroutine rather than matching cacheA's goroutine and deadlocking.
func TestEvictCallback_CrossCacheFastTagDrainDoesNotPoisonHolderGID(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			cacheAEnteredPending := make(chan struct{})
			releaseCacheAPending := make(chan struct{})

			var cacheA, cacheB Cache[int]
			stepA := 0
			cacheA = New[int](2,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictValue(func(_ int, _ EvictionReason) {
					stepA++
					if stepA == 1 {
						_, _ = cacheA.Delete("a2")
						return
					}
					if stepA == 2 {
						close(cacheAEnteredPending)
						<-releaseCacheAPending
					}
				}),
			)
			_, err := cacheA.Put("prime", 1)
			require.NoError(t, err)
			_, _ = cacheA.Delete("prime")
			stepA = 0

			for range numPressureHookTags - 1 {
				d := New[int](2, WithBackend(b), WithOnEvictValue(func(_ int, _ EvictionReason) {}))
				_, putErr := d.Put("p", 1)
				require.NoError(t, putErr)
				_, _ = d.Delete("p")
			}

			var bStarted atomic.Bool
			var bEvicted atomic.Int32
			cacheB = New[int](2,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictValue(func(_ int, _ EvictionReason) {
					bEvicted.Add(1)
					if !bStarted.CompareAndSwap(false, true) {
						return
					}
					var wg sync.WaitGroup
					wg.Go(func() {
						_, _ = cacheB.Delete("b2")
					})
					wg.Wait()
				}),
			)

			_, err = cacheA.Put("a1", 1)
			require.NoError(t, err)
			_, err = cacheA.Put("a2", 2)
			require.NoError(t, err)

			g1Done := make(chan struct{})
			go func() {
				defer close(g1Done)
				_, _ = cacheA.Delete("a1")
			}()

			<-cacheAEnteredPending

			_, err = cacheB.Put("b1", 1)
			require.NoError(t, err)
			_, err = cacheB.Put("b2", 2)
			require.NoError(t, err)

			g2Done := make(chan struct{})
			go func() {
				defer close(g2Done)
				_, _ = cacheB.Delete("b1")
			}()

			select {
			case <-g2Done:
			case <-time.After(2 * time.Second):
				close(releaseCacheAPending)
				require.Failf(t, "deadlock", "backend %v deadlocked due to cross-cache fast-evict tag poisoning", b)
			}

			close(releaseCacheAPending)
			<-g1Done
			assert.Equal(t, int32(2), bEvicted.Load(), "both b1 and child-deleted b2 eviction callbacks must complete on %v", b)
		})
	}
	t.Run("FastEvictContentionClearsEvictCallbackGIDAndHookBoundary", func(t *testing.T) {
		testFastEvictContentionClearsEvictCallbackGID(t)
	})
}

func testFastEvictContentionClearsEvictCallbackGID(t *testing.T) {
	t.Helper()
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			var cache Cache[int]
			inEvict1 := make(chan struct{})
			releaseEvict1 := make(chan struct{})
			var evictCount atomic.Int32

			cache = New[int](10,
				WithBackend(b),
				WithInvariantChecking(true),
				WithOnEvictEntry(func(key string, _ int, _ EvictionReason) {
					evictCount.Add(1)
					switch key {
					case "k1":
						close(inEvict1)
						<-releaseEvict1
					case "k3":
						_, _ = cache.Delete("k4")
					}
				}),
			)

			for _, k := range []string{"k1", "k2", "k3", "k4"} {
				_, err := cache.Put(k, 1)
				require.NoError(t, err)
			}

			g1Done := make(chan struct{})
			go func() {
				defer close(g1Done)
				_, ok := cache.Delete("k1")
				assert.True(t, ok)
			}()
			<-inEvict1

			g2Done := make(chan struct{})
			go func() {
				defer close(g2Done)
				_, ok := cache.Delete("k2")
				assert.True(t, ok)
			}()

			// Wait until G2 resolves the holder via lockEvictCallbackSlow and sets evictCallbackContended.
			ps := extractPressureStateForTest(cache)
			deadline := time.Now().Add(5 * time.Second)
			for !ps.evictCallbackContended.Load() && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			require.True(t, ps.evictCallbackContended.Load())

			close(releaseEvict1)
			<-g1Done
			<-g2Done
			assert.Zero(t, ps.evictCallbackGID.Load(), "evictCallbackGID must be cleared after contended fast delivery")

			// Subsequent fast-path eviction with re-entrant Delete("k4") inside OnEvictEntry must not self-deadlock.
			g3Done := make(chan struct{})
			go func() {
				defer close(g3Done)
				_, ok := cache.Delete("k3")
				assert.True(t, ok)
			}()
			select {
			case <-g3Done:
			case <-time.After(5 * time.Second):
				require.Failf(t, "deadlock", "backend %v self-deadlocked on stale evictCallbackGID", b)
			}
			assert.Equal(t, int32(4), evictCount.Load())
		})
	}
}

func extractPressureStateForTest(cache Cache[int]) *pressureState {
	switch c := cache.(type) {
	case *mapCache[int]:
		return &c.pressureState
	case *radixCache[int]:
		return &c.pressureState
	case *arenaRadix[int]:
		return &c.pressureState
	default:
		return nil
	}
}

// TestEvictCallback_ConcurrentHookIDInitPreservesTagHints verifies that concurrent
// initialization of hookID() and fast-tag claiming across PressureFunc and OnEvict*
// never clobbers primaryTagHint or evictTagHint across all 8 tag modulo values.
func TestEvictCallback_ConcurrentHookIDInitPreservesTagHints(t *testing.T) {
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		t.Run(b.String(), func(t *testing.T) {
			for range numPressureHookTags * 2 {
				var cache Cache[int]
				var pressureArmed, evictArmed atomic.Bool
				var pressureCalls, evictCalls atomic.Int32

				cache = New[int](4,
					WithBackend(b),
					WithInvariantChecking(true),
					WithPressureFunc(func() float64 {
						if !pressureArmed.Load() {
							return 0.10
						}
						if pressureCalls.Add(1) == 1 {
							var wg sync.WaitGroup
							wg.Go(func() {
								_, _ = cache.Delete("missing_pressure_child")
							})
							wg.Wait()
						}
						return 0.10
					}),
					WithOnEvictValue(func(_ int, _ EvictionReason) {
						if !evictArmed.Load() {
							return
						}
						if evictCalls.Add(1) == 1 {
							var wg sync.WaitGroup
							wg.Go(func() {
								_, _ = cache.Delete("k2")
							})
							wg.Wait()
						}
					}),
				)

				_, err := cache.Put("k1", 1)
				require.NoError(t, err)
				_, err = cache.Put("k2", 2)
				require.NoError(t, err)

				pressureArmed.Store(true)
				evictArmed.Store(true)

				start := make(chan struct{})
				done := make(chan struct{})
				go func() {
					defer close(done)
					var wg sync.WaitGroup
					wg.Go(func() {
						<-start
						_, putErr := cache.Put("k3", 3)
						assert.NoError(t, putErr)
					})
					wg.Go(func() {
						<-start
						_, _ = cache.Delete("k1")
					})
					close(start)
					wg.Wait()
				}()

				select {
				case <-done:
				case <-time.After(2 * time.Second):
					require.Failf(t, "deadlock", "backend %v deadlocked during concurrent hookID and fast-tag initialization", b)
				}

				assert.Equal(t, int32(2), evictCalls.Load(), "both k1 and child-deleted k2 eviction callbacks must complete on %v", b)
			}
		})
	}
}
