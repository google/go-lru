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

package lrus_test

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	lrus "github.com/googlecloudplatform/gcsfuse/v3/internal/cache/lru"
)

type concValue struct {
	id   string
	size uint64
}

func (v concValue) Size() uint64 {
	return v.size
}

func allEngines() []struct {
	name        string
	constructor func(maxSize uint64, opts ...lrus.Option) lrus.Cache
} {
	return []struct {
		name        string
		constructor func(maxSize uint64, opts ...lrus.Option) lrus.Cache
	}{
		{"MapCache", lrus.NewMapCache},
		{"RadixCache", lrus.NewRadixCache},
		{"ArenaRadixCache", lrus.NewArenaRadixCache},
	}
}

// TestConcurrency_MixedOperations exercises all Cache methods concurrently across
// multiple goroutines on all three cache engines.
func TestConcurrency_MixedOperations(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			const (
				numGoroutines = 16
				opsPerWorker  = 300
				numKeys       = 100
				capacity      = 100000
			)

			cache := eng.constructor(capacity)

			for i := range numKeys {
				key := fmt.Sprintf("dir_%02d/sub_%02d/file_%03d.txt", i%5, (i/5)%10, i)
				_, err := cache.Insert(key, concValue{id: key, size: 10})
				if err != nil {
					t.Fatalf("unexpected pre-population error: %v", err)
				}
			}

			var wg sync.WaitGroup
			for g := range numGoroutines {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					r := rand.New(rand.NewSource(int64(workerID*10007 + 42)))

					for range opsPerWorker {
						op := r.Intn(100)
						kIdx := r.Intn(numKeys * 2)
						dirIdx := kIdx % 5
						subIdx := (kIdx / 5) % 10
						key := fmt.Sprintf("dir_%02d/sub_%02d/file_%03d.txt", dirIdx, subIdx, kIdx)

						switch {
						case op < 30:
							_, err := cache.Insert(key, concValue{id: key, size: 10})
							if err != nil && !errors.Is(err, lrus.ErrInvalidEntrySize) {
								t.Errorf("worker %d: unexpected insert error: %v", workerID, err)
							}
						case op < 55:
							_ = cache.LookUp(key)
						case op < 70:
							_ = cache.LookUpWithoutChangingOrder(key)
						case op < 80:
							err := cache.UpdateWithoutChangingOrder(key, concValue{id: key + "_upd", size: 10})
							if err != nil && !errors.Is(err, lrus.ErrEntryNotExist) && !errors.Is(err, lrus.ErrInvalidUpdateEntrySize) {
								t.Errorf("worker %d: unexpected update error: %v", workerID, err)
							}
						case op < 88:
							err := cache.UpdateSize(key, 0)
							if err != nil && !errors.Is(err, lrus.ErrEntryNotExist) {
								t.Errorf("worker %d: unexpected update size error: %v", workerID, err)
							}
						case op < 95:
							_ = cache.Erase(key)
						default:
							prefix := fmt.Sprintf("dir_%02d/", dirIdx)
							cache.EraseEntriesWithGivenPrefix(prefix)
						}
					}
				}(g)
			}

			wg.Wait()
		})
	}
}

// TestConcurrency_PrefixErasureAtomicity verifies that EraseEntriesWithGivenPrefix
// atomically removes all matching keys without affecting unrelated prefixes under
// concurrent writer load.
func TestConcurrency_PrefixErasureAtomicity(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			const (
				numWriters   = 8
				opsPerWriter = 100
			)

			cache := eng.constructor(100000)

			// Pre-populate target and retained entries.
			for i := 0; i < 20; i++ {
				kTarget := fmt.Sprintf("/target/init_%d", i)
				_, _ = cache.Insert(kTarget, concValue{id: kTarget, size: 10})
				kKeep := fmt.Sprintf("/keep/init_%d", i)
				_, _ = cache.Insert(kKeep, concValue{id: kKeep, size: 10})
			}

			var writerWg sync.WaitGroup
			for w := 0; w < numWriters; w++ {
				writerWg.Add(1)
				go func(workerID int) {
					defer writerWg.Done()
					for op := 0; op < opsPerWriter; op++ {
						key := fmt.Sprintf("/target/w%d_%d", workerID, op)
						_, _ = cache.Insert(key, concValue{id: key, size: 10})
					}
				}(w)
			}

			// Concurrent erasures during writes.
			for i := 0; i < 10; i++ {
				cache.EraseEntriesWithGivenPrefix("/target/")
			}

			writerWg.Wait()

			// Final prefix erase after writers finish must remove all /target/ keys.
			cache.EraseEntriesWithGivenPrefix("/target/")

			for i := 0; i < 20; i++ {
				kTarget := fmt.Sprintf("/target/init_%d", i)
				if val := cache.LookUp(kTarget); val != nil {
					t.Fatalf("[%s] pre-existing target key %s survived prefix erasure", eng.name, kTarget)
				}
				kKeep := fmt.Sprintf("/keep/init_%d", i)
				if val := cache.LookUp(kKeep); val == nil {
					t.Fatalf("[%s] retained key %s was erroneously erased", eng.name, kKeep)
				}
			}

			for w := 0; w < numWriters; w++ {
				for op := 0; op < opsPerWriter; op++ {
					key := fmt.Sprintf("/target/w%d_%d", w, op)
					if val := cache.LookUp(key); val != nil {
						t.Fatalf("[%s] writer target key %s survived final prefix erasure", eng.name, key)
					}
				}
			}
		})
	}
}

// TestConcurrency_ParallelReadersWithoutChangingOrder verifies concurrent
// LookUpWithoutChangingOrder calls execute safely and preserve LRU eviction order.
func TestConcurrency_ParallelReadersWithoutChangingOrder(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			const (
				totalKeys  = 200
				numReaders = 16
				readsPerG  = 500
				capacity   = 50000
			)

			cache := eng.constructor(capacity)

			for i := 0; i < totalKeys; i++ {
				k := fmt.Sprintf("key_%04d", i)
				_, err := cache.Insert(k, concValue{id: k, size: 10})
				if err != nil {
					t.Fatalf("[%s] pre-population failed: %v", eng.name, err)
				}
			}

			var readerWg sync.WaitGroup
			for r := 0; r < numReaders; r++ {
				readerWg.Add(1)
				go func(readerID int) {
					defer readerWg.Done()
					for i := 0; i < readsPerG; i++ {
						targetKey := "key_0000"
						if i%2 == 1 {
							targetKey = fmt.Sprintf("key_%04d", (readerID*17+i)%totalKeys)
						}
						if val := cache.LookUpWithoutChangingOrder(targetKey); val == nil {
							t.Errorf("[%s] reader %d: unexpected nil lookup for key %s", eng.name, readerID, targetKey)
							return
						}
					}
				}(r)
			}
			readerWg.Wait()

			// Fill remaining capacity (50,000 - 2,000 = 48,000 bytes).
			for i := 0; i < 48; i++ {
				k := fmt.Sprintf("filler_%03d", i)
				_, err := cache.Insert(k, concValue{id: k, size: 1000})
				if err != nil {
					t.Fatalf("[%s] failed to insert filler: %v", eng.name, err)
				}
			}

			// Next 10-byte insert must evict key_0000 (the untouched LRU tail).
			evicted, err := cache.Insert("overflow_trigger", concValue{id: "overflow", size: 10})
			if err != nil {
				t.Fatalf("[%s] overflow insert failed: %v", eng.name, err)
			}
			if len(evicted) != 1 || evicted[0].(concValue).id != "key_0000" {
				t.Fatalf("[%s] expected oldest entry key_0000 to be evicted, got %v", eng.name, evicted)
			}
		})
	}
}

// TestConcurrency_EvictionThrashingWithInvariants stresses concurrent evictions under
// tight capacity with invariant checking enabled.
func TestConcurrency_EvictionThrashingWithInvariants(t *testing.T) {
	for _, eng := range allEngines() {
		t.Run(eng.name, func(t *testing.T) {
			const (
				numGoroutines = 8
				opsPerWorker  = 150
				capacity      = 300
			)

			cache := eng.constructor(capacity, lrus.WithInvariantChecking(true))
			var wg sync.WaitGroup
			var totalEvictions atomic.Int64

			for g := range numGoroutines {
				wg.Add(1)
				go func(workerID int) {
					defer wg.Done()
					for i := range opsPerWorker {
						key := fmt.Sprintf("inv/p%d/item_%d", i%5, i)
						evicted, err := cache.Insert(key, concValue{id: key, size: 10})
						if err != nil {
							t.Errorf("worker %d: unexpected insert error: %v", workerID, err)
						}
						totalEvictions.Add(int64(len(evicted)))
						if i%20 == 0 {
							cache.EraseEntriesWithGivenPrefix(fmt.Sprintf("inv/p%d/", i%5))
						}
					}
				}(g)
			}

			wg.Wait()
		})
	}
}
