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
	"math/rand/v2"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type backendFactory[V any] struct {
	name string
	make func(maxSize uint64, opts ...Option) Cache[V]
}

func allBackendFactories[V any]() []backendFactory[V] {
	return []backendFactory[V]{
		{name: "MapCache", make: NewMapCache[V]},
		{name: "RadixCache", make: NewRadixCache[V]},
		{name: "ArenaRadixCache", make: NewArenaRadixCache[V]},
	}
}

// ============================================================================
// Suite 2.1: Key Distributions (Zipfian vs Uniform vs Scan vs Prefix Burst)
// ============================================================================

func BenchmarkKeyDistributions(b *testing.B) {
	const cacheCap = 4096
	const keySpace = 65536
	keys := make([]string, keySpace)
	for i := range keySpace {
		keys[i] = fmt.Sprintf("ns_%02d/region_%02d/item_%06d", i%16, (i/16)%16, i)
	}

	b.Run("Zipfian_90Get_10Put", func(b *testing.B) {
		for _, bf := range allBackendFactories[uint64]() {
			b.Run(bf.name, func(b *testing.B) {
				c := bf.make(cacheCap)
				for i := range cacheCap {
					_, _ = c.Put(keys[i], uint64(i))
				}
				r := rand.New(rand.NewPCG(42, 1))
				zipf := rand.NewZipf(r, 1.07, 1.0, keySpace-1)
				b.ReportAllocs()
				b.ResetTimer()
				step := 0
				for b.Loop() {
					k := keys[zipf.Uint64()]
					if step%10 == 0 {
						_, _ = c.Put(k, uint64(step))
					} else {
						_, _ = c.Get(k)
					}
					step++
				}
			})
		}
	})

	b.Run("Uniform_90Get_10Put", func(b *testing.B) {
		for _, bf := range allBackendFactories[uint64]() {
			b.Run(bf.name, func(b *testing.B) {
				c := bf.make(cacheCap)
				for i := range cacheCap {
					_, _ = c.Put(keys[i], uint64(i))
				}
				var pcg rand.PCG
				pcg.Seed(42, 2)
				b.ReportAllocs()
				b.ResetTimer()
				step := 0
				for b.Loop() {
					k := keys[pcg.Uint64()%keySpace]
					if step%10 == 0 {
						_, _ = c.Put(k, uint64(step))
					} else {
						_, _ = c.Get(k)
					}
					step++
				}
			})
		}
	})

	b.Run("SequentialScan_100Put_Evict", func(b *testing.B) {
		for _, bf := range allBackendFactories[uint64]() {
			b.Run(bf.name, func(b *testing.B) {
				c := bf.make(cacheCap)
				for i := range cacheCap {
					_, _ = c.Put(keys[i], uint64(i))
				}
				b.ReportAllocs()
				b.ResetTimer()
				step := cacheCap
				for b.Loop() {
					_, _ = c.Put(keys[step%keySpace], uint64(step))
					step++
				}
			})
		}
	})

	b.Run("PrefixLocalityBurst_PutAndDeletePrefix", func(b *testing.B) {
		runPrefixLocalityBurstBench(b, cacheCap)
	})
}

func runPrefixLocalityBurstBench(b *testing.B, cacheCap uint64) {
	b.Helper()
	const numBatches = 64
	const batchSize = 64
	batchPrefixes := make([]string, numBatches)
	batchKeys := make([][]string, numBatches)
	for bi := range numBatches {
		pfx := fmt.Sprintf("batch_%04d/svc/items/", bi)
		batchPrefixes[bi] = pfx
		batchKeys[bi] = make([]string, batchSize)
		for j := range batchSize {
			batchKeys[bi][j] = fmt.Sprintf("%sk_%03d", pfx, j)
		}
	}

	for _, bf := range allBackendFactories[uint64]() {
		b.Run(bf.name, func(b *testing.B) {
			c := bf.make(cacheCap)
			// Pre-populate batches 0..31 (2048 keys)
			for bi := range numBatches / 2 {
				for _, k := range batchKeys[bi] {
					_, _ = c.Put(k, 1)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			step := 0
			for b.Loop() {
				insertBatch := (step + numBatches/2) % numBatches
				purgeBatch := step % numBatches
				for _, k := range batchKeys[insertBatch] {
					_, _ = c.Put(k, 1)
				}
				c.DeletePrefix(batchPrefixes[purgeBatch])
				step++
			}
		})
	}
}

// ============================================================================
// Suite 2.2: Payload Sizes & Pointer Density
// ============================================================================

func runPayloadSizeBench[V any](b *testing.B, val1, val2 V, weigher Option) {
	b.Helper()
	const capEntries = 2048
	const poolSize = 4096
	keys := make([]string, poolSize)
	for i := range poolSize {
		keys[i] = fmt.Sprintf("dir_%02d/sub_%02d/k_%04d", i%16, (i/16)%16, i)
	}

	var opts []Option
	maxCap := uint64(capEntries)
	if weigher != nil {
		opts = append(opts, weigher)
	}

	b.Run("Get_Hit", func(b *testing.B) {
		for _, bf := range allBackendFactories[V]() {
			b.Run(bf.name, func(b *testing.B) {
				c := bf.make(maxCap, opts...)
				for i := range capEntries {
					_, _ = c.Put(keys[i], val1)
				}
				b.ReportAllocs()
				b.ResetTimer()
				i := 0
				for b.Loop() {
					_, _ = c.Get(keys[i%capEntries])
					i++
				}
			})
		}
	})

	b.Run("Put_Update", func(b *testing.B) {
		for _, bf := range allBackendFactories[V]() {
			b.Run(bf.name, func(b *testing.B) {
				c := bf.make(maxCap, opts...)
				for i := range capEntries {
					_, _ = c.Put(keys[i], val1)
				}
				b.ReportAllocs()
				b.ResetTimer()
				i := 0
				for b.Loop() {
					_, _ = c.Put(keys[i%capEntries], val2)
					i++
				}
			})
		}
	})

	b.Run("Put_Evict", func(b *testing.B) {
		for _, bf := range allBackendFactories[V]() {
			b.Run(bf.name, func(b *testing.B) {
				c := bf.make(maxCap, opts...)
				for i := range capEntries {
					_, _ = c.Put(keys[i], val1)
				}
				b.ReportAllocs()
				b.ResetTimer()
				i := capEntries
				for b.Loop() {
					_, _ = c.Put(keys[i%poolSize], val2)
					i++
				}
			})
		}
	})
}

func BenchmarkPayloadSizes(b *testing.B) {
	b.Run("Scalar64B", func(b *testing.B) {
		var v1, v2 [64]byte
		v1[0], v2[0] = 1, 2
		runPayloadSizeBench(b, v1, v2, nil)
	})

	b.Run("Scalar512B", func(b *testing.B) {
		var v1, v2 [512]byte
		v1[0], v2[0] = 1, 2
		runPayloadSizeBench(b, v1, v2, nil)
	})

	b.Run("ByteSlice_1KB", func(b *testing.B) {
		v1 := make([]byte, 1024)
		v2 := make([]byte, 1024)
		v1[0], v2[0] = 1, 2
		runPayloadSizeBench(b, v1, v2, nil)
	})

	b.Run("ByteSlice_64KB", func(b *testing.B) {
		v1 := make([]byte, 65536)
		v2 := make([]byte, 65536)
		v1[0], v2[0] = 1, 2
		runPayloadSizeBench(b, v1, v2, nil)
	})
}

// ============================================================================
// Suite 2.3: Concurrency Scaling & Read/Write Ratios
// ============================================================================

func BenchmarkConcurrencyScaling(b *testing.B) {
	const capEntries = 4096
	const poolSize = 8192
	keys := make([]string, poolSize)
	for i := range poolSize {
		keys[i] = fmt.Sprintf("svc_%02d/ep_%02d/item_%04d", i%16, (i/16)%16, i)
	}

	b.Run("ReadWriteRatios", func(b *testing.B) {
		runReadWriteRatiosBench(b, keys, capEntries, poolSize)
	})

	b.Run("GoroutineScaling", func(b *testing.B) {
		runGoroutineScalingBench(b, keys, capEntries)
	})
}

func runReadWriteRatiosBench(b *testing.B, keys []string, capEntries, poolSize uint64) {
	b.Helper()
	ratios := []struct {
		name    string
		usePeek bool
		putPct  int
	}{
		{name: "100Get_0Put", usePeek: false, putPct: 0},
		{name: "100Peek_0Put", usePeek: true, putPct: 0},
		{name: "95Get_5Put", usePeek: false, putPct: 5},
		{name: "80Get_20Put", usePeek: false, putPct: 20},
		{name: "50Get_50Put", usePeek: false, putPct: 50},
		{name: "0Get_100Put", usePeek: false, putPct: 100},
	}

	for _, rt := range ratios {
		b.Run(rt.name, func(b *testing.B) {
			for _, bf := range allBackendFactories[uint64]() {
				b.Run(bf.name, func(b *testing.B) {
					c := bf.make(capEntries)
					for i := range capEntries {
						_, _ = c.Put(keys[i], i)
					}
					var seq atomic.Uint64
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						wid := seq.Add(1)
						var pcg rand.PCG
						pcg.Seed(42+wid*10007, 1)
						for pb.Next() {
							r := pcg.Uint64()
							k := keys[r%capEntries]
							switch {
							case int(r%100) < rt.putPct:
								if rt.putPct == 100 {
									k = keys[r%poolSize]
								}
								_, _ = c.Put(k, r)
							case rt.usePeek:
								_, _ = c.Peek(k)
							default:
								_, _ = c.Get(k)
							}
						}
					})
				})
			}
		})
	}
}

func runGoroutineScalingBench(b *testing.B, keys []string, capEntries uint64) {
	b.Helper()
	modes := []struct {
		name    string
		usePeek bool
		putPct  int
	}{
		{name: "Get_Hit", usePeek: false, putPct: 0},
		{name: "Peek_Hit", usePeek: true, putPct: 0},
		{name: "Mixed_95Get_5Put", usePeek: false, putPct: 5},
	}

	for _, mode := range modes {
		b.Run(mode.name, func(b *testing.B) {
			for _, gCount := range []int{1, 2, 4, 8, 16, 32, 64} {
				b.Run(fmt.Sprintf("G=%d", gCount), func(b *testing.B) {
					for _, bf := range allBackendFactories[uint64]() {
						b.Run(bf.name, func(b *testing.B) {
							c := bf.make(capEntries)
							for i := range capEntries {
								_, _ = c.Put(keys[i], i)
							}

							b.ReportAllocs()
							b.ResetTimer()

							var totalOps atomic.Int64
							totalOps.Store(int64(b.N))
							var wg sync.WaitGroup
							for g := range gCount {
								wg.Go(func() {
									var pcg rand.PCG
									pcg.Seed(uint64(1000+g*7919), 99)
									for {
										if totalOps.Add(-1) < 0 {
											return
										}
										r := pcg.Uint64()
										k := keys[r%capEntries]
										switch {
										case int(r%100) < mode.putPct:
											_, _ = c.Put(k, r)
										case mode.usePeek:
											_, _ = c.Peek(k)
										default:
											_, _ = c.Get(k)
										}
									}
								})
							}
							wg.Wait()
						})
					}
				})
			}
		})
	}
}

// ============================================================================
// Suite 2.4: Compaction & Tier 2 Pressure Shedding Pause Latency
// ============================================================================

func BenchmarkCompactionAndPressureShedLatency(b *testing.B) {
	const maxScale = 50000
	keys := make([]string, maxScale)
	for i := range maxScale {
		keys[i] = fmt.Sprintf("tenant_%02d/svc_%02d/key_%05d", i%32, (i/32)%32, i)
	}

	for _, n := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprintf("Compact_After75PercentDelete/N=%d", n), func(b *testing.B) {
			for _, bf := range allBackendFactories[uint64]() {
				b.Run(bf.name, func(b *testing.B) {
					c := bf.make(uint64(n * 2)).(PressureAwareCache[uint64])
					for i := range n {
						_, _ = c.Put(keys[i], uint64(i))
					}
					// Delete 75% of entries (leaving 25% > 1 survivors at pressure 0.0) so the cache holds genuine
					// uncompacted free-list holes / deleted map slots when explicit Compact() is called.
					delCount := (n * 3) / 4
					for i := range delCount {
						_, _ = c.Delete(keys[i])
					}

					// Snapshot the uncompacted backing state (Compact() allocates new maps/slices
					// without mutating the old backing arrays) so each iteration measures 100% genuine
					// Compact() execution without spending minutes in untimed StopTimer setup.
					var restorePreCompact func()
					switch impl := c.(type) {
					case *mapCache[uint64]:
						savedIndex := impl.index
						savedDirty := impl.dirtyIndex
						savedDel := impl.deletedSinceCompact
						savedPeak := impl.peakEntryLen
						restorePreCompact = func() {
							impl.index = savedIndex
							impl.dirtyIndex = savedDirty
							impl.deletedSinceCompact = savedDel
							impl.peakEntryLen = savedPeak
						}
					case *radixCache[uint64]:
						savedDel := impl.deletedSinceCompact
						savedPeak := impl.peakEntryLen
						restorePreCompact = func() {
							impl.deletedSinceCompact = savedDel
							impl.peakEntryLen = savedPeak
						}
					case *arenaRadix[uint64]:
						savedNodes := impl.nodes
						savedNodeMap := impl.nodeMap
						savedCollisionPeers := impl.collisionPeers
						savedCollisionCount := impl.collisionCount
						savedRoot := impl.root
						savedHead := impl.head
						savedTail := impl.tail
						savedFreeHead := impl.freeHead
						savedFreeCount := impl.freeCount
						savedDirty := impl.nodeMapDirty
						savedDel := impl.deletedSinceCompact
						savedPeak := impl.peakEntryLen
						restorePreCompact = func() {
							impl.nodes = savedNodes
							impl.nodeMap = savedNodeMap
							impl.collisionPeers = savedCollisionPeers
							impl.collisionCount = savedCollisionCount
							impl.root = savedRoot
							impl.head = savedHead
							impl.tail = savedTail
							impl.freeHead = savedFreeHead
							impl.freeCount = savedFreeCount
							impl.nodeMapDirty = savedDirty
							impl.deletedSinceCompact = savedDel
							impl.peakEntryLen = savedPeak
						}
					}

					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						restorePreCompact()
						c.Compact()
					}
				})
			}
		})

		b.Run(fmt.Sprintf("EvaluateMemoryPressure_Tier2_Shed50Percent/N=%d", n), func(b *testing.B) {
			for _, bf := range allBackendFactories[uint64]() {
				b.Run(bf.name, func(b *testing.B) {
					b.ReportAllocs()
					for range b.N {
						b.StopTimer()
						pressure := 0.10
						c := bf.make(uint64(n),
							WithPressureFunc(func() float64 { return pressure }),
							WithCompactionThreshold(0.75),
							WithEvictionThreshold(0.90),
							WithEvictionRetentionRatio(0.50),
						).(PressureAwareCache[uint64])
						for i := range n {
							_, _ = c.Put(keys[i], uint64(i))
						}
						pressure = 0.95
						b.StartTimer()
						_ = c.EvaluateMemoryPressure()
					}
				})
			}
		})
	}
}

// ============================================================================
// Suite 2.5: GC Scan Pause & Memory Overhead Per Entry
// ============================================================================

func runGCScanBench[V any](b *testing.B, n int, makeVal func(int) V) {
	b.Helper()
	keys := make([]string, n*2)
	for i := range len(keys) {
		keys[i] = fmt.Sprintf("ns_%02d/dir_%02d/object_%06d", i%32, (i/32)%32, i)
	}

	for _, bf := range allBackendFactories[V]() {
		for _, afterCompact := range []bool{false, true} {
			mode := "BeforeCompact_50PctChurn"
			if afterCompact {
				mode = "AfterCompact_Contiguous"
			}
			b.Run(fmt.Sprintf("%s/%s", bf.name, mode), func(b *testing.B) {
				runtime.GC()
				runtime.GC()
				var mBase runtime.MemStats
				runtime.ReadMemStats(&mBase)

				c := bf.make(uint64(n * 4)).(PressureAwareCache[V])
				// Insert 2*n keys then delete n keys so 50% of slots are churned/freed, leaving n live entries.
				for i := range n * 2 {
					_, _ = c.Put(keys[i], makeVal(i))
				}
				for i := range n {
					_, _ = c.Delete(keys[i])
				}
				if afterCompact {
					c.Compact()
				}

				runtime.GC()
				runtime.GC()
				var mLive runtime.MemStats
				runtime.ReadMemStats(&mLive)

				var heapBytesPerEntry float64
				if mLive.HeapAlloc > mBase.HeapAlloc {
					heapBytesPerEntry = float64(mLive.HeapAlloc-mBase.HeapAlloc) / float64(n)
				}

				b.ReportAllocs()
				b.ResetTimer()
				var totalPauseNS int64
				for range b.N {
					start := time.Now()
					runtime.GC()
					totalPauseNS += time.Since(start).Nanoseconds()
				}
				b.StopTimer()
				runtime.KeepAlive(c)

				b.ReportMetric(float64(totalPauseNS)/float64(b.N), "gc_pause_ns/op")
				b.ReportMetric(heapBytesPerEntry, "heap_bytes/entry")
			})
		}
	}
}

func BenchmarkGCPauseAndScanOverhead(b *testing.B) {
	const numLiveEntries = 50000

	b.Run("Scalar64B", func(b *testing.B) {
		runGCScanBench(b, numLiveEntries, func(i int) [64]byte {
			var v [64]byte
			v[0] = byte(i)
			return v
		})
	})

	b.Run("ByteSlice_1KB", func(b *testing.B) {
		runGCScanBench(b, numLiveEntries, func(i int) []byte {
			buf := make([]byte, 1024)
			buf[0] = byte(i)
			return buf
		})
	})
}

// ============================================================================
// Suite 2.6: Pressure Sampling Overhead (H-01 runtime.Stack) & Eviction Callbacks
// ============================================================================

func BenchmarkCustomPressureFuncOverhead(b *testing.B) {
	for _, bf := range allBackendFactories[int]() {
		b.Run(bf.name+"/DefaultRuntimePressureFunc_Amortized256", func(b *testing.B) {
			c := bf.make(10000, WithMemoryBudget(512<<20))
			_, _ = c.Put("hot_key", 1)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = c.Put("hot_key", 1)
			}
		})

		b.Run(bf.name+"/CustomPressureFunc_UnamortizedRuntimeStack", func(b *testing.B) {
			c := bf.make(10000, WithPressureFunc(func() float64 { return 0.10 }))
			_, _ = c.Put("hot_key", 1)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_, _ = c.Put("hot_key", 1)
			}
		})
	}
}

func BenchmarkPressureSamplingAndCallbacks(b *testing.B) {
	b.Run("PressureProbeModes", func(b *testing.B) {
		BenchmarkCustomPressureFuncOverhead(b)
	})

	b.Run("EvictionCallbacks_ShallowVsDeepTree", func(b *testing.B) {
		const capEntries = 1024
		const poolSize = 2048

		makeKeysAtDepth := func(depth int) []string {
			var pfx strings.Builder
			for d := 1; d < depth; d++ {
				fmt.Fprintf(&pfx, "d%02d/", d)
			}
			base := pfx.String()
			out := make([]string, poolSize)
			for i := range poolSize {
				out[i] = fmt.Sprintf("%sk_%04d", base, i)
			}
			return out
		}

		for _, depth := range []int{4, 32} {
			keys := makeKeysAtDepth(depth)
			b.Run(fmt.Sprintf("Depth=%d", depth), func(b *testing.B) {
				for _, cbMode := range []string{"OnEvictValue", "OnEvictEntry"} {
					b.Run(cbMode, func(b *testing.B) {
						for _, bf := range allBackendFactories[uint64]() {
							b.Run(bf.name, func(b *testing.B) {
								var opt Option
								if cbMode == "OnEvictValue" {
									opt = WithOnEvictValue(func(_ uint64, _ EvictionReason) {})
								} else {
									opt = WithOnEvictEntry(func(_ string, _ uint64, _ EvictionReason) {})
								}
								c := bf.make(capEntries, opt)
								// Ensure every level of the prefix has a branching sibling so the radix tree does not collapse it into 1 edge!
								var branchSB strings.Builder
								for d := 1; d < depth; d++ {
									fmt.Fprintf(&branchSB, "d%02d/", d)
									_, _ = c.Put(branchSB.String()+"branch_pin", 1)
								}
								for i := range capEntries {
									_, _ = c.Put(keys[i], uint64(i))
								}

								b.ReportAllocs()
								b.ResetTimer()
								step := capEntries
								for b.Loop() {
									// Periodically refresh branch pins so tree depth remains at `depth`
									if step%capEntries == 0 {
										var sb strings.Builder
										for d := 1; d < depth; d++ {
											fmt.Fprintf(&sb, "d%02d/", d)
											_, _ = c.Get(sb.String() + "branch_pin")
										}
									}
									_, _ = c.Put(keys[step%poolSize], uint64(step))
									step++
								}
							})
						}
					})
				}
			})
		}
	})
}
