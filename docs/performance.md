# Empirical Performance & Heap Footprint

This document records empirical latency, allocation, subtree prefix eviction, and 1,000,000-key live heap memory footprint benchmarks across `MapCache`, `RadixCache`, and `ArenaRadixCache`.

All reference benchmarks were collected on an **Intel Xeon CPU @ 2.60GHz (96 vCPUs, `linux/amd64`)** using `go test -bench=. -benchmem`.

---

## 1. Point Operation Latency & Allocations

| Operation | `MapCache` | `RadixCache` | `ArenaRadixCache` |
| :--- | :--- | :--- | :--- |
| **Lookup (Flat)** | **50.6 ns/op** (0 B, 0 allocs) | 174.0 ns/op (0 B, 0 allocs) | 100.7 ns/op (0 B, 0 allocs) |
| **Lookup (Nested, Depth 2)** | **49.5 ns/op** (0 B, 0 allocs) | 169.6 ns/op (0 B, 0 allocs) | 107.8 ns/op (0 B, 0 allocs) |
| **Lookup (Deeply Nested, Depth 10)** | **64.3 ns/op** (0 B, 0 allocs) | 185.2 ns/op (0 B, 0 allocs) | 194.1 ns/op (0 B, 0 allocs) |
| **Lookup (`LookUpWithoutChangingOrder`)** | **33.2 ns/op** (0 B, 0 allocs) | 163.3 ns/op (0 B, 0 allocs) | 103.8 ns/op (0 B, 0 allocs) |
| **Insert (Existing-Key Overwrite, In-Place)**¹ | **97.7 ns/op** (0 B, 0 allocs in-place) | 178.2 ns/op (0 B, 0 allocs in-place) | 270.6 ns/op (0 B, 0 allocs in-place) |
| **Insert (Nested, Depth 2, In-Place Overwrite)**¹ | **98.1 ns/op** (0 B, 0 allocs in-place) | 172.3 ns/op (0 B, 0 allocs in-place) | 270.9 ns/op (0 B, 0 allocs in-place) |
| **Insert (Deeply Nested, Depth 10, In-Place Overwrite)**¹ | **119.0 ns/op** (0 B, 0 allocs in-place) | 229.9 ns/op (0 B, 0 allocs in-place) | 471.8 ns/op (0 B, 0 allocs in-place) |
| **Insert (`Benchmark_Insert_*`, 50%-Capacity Turnover)**¹ | **383–466 ns/op** (96–144 B, 3 allocs/op) | 472–511 ns/op (132–135 B, 3 allocs/op) | 515–822 ns/op (49–65 B, 2 allocs/op) |
| **Update Value (`UpdateWithoutChangingOrder`)**² | **82.5 ns/op** (0 B internal, 0 allocs) | 97.3 ns/op (0 B, 0 allocs) | 88.6 ns/op (0 B, 0 allocs) |
| **Individual Erase (`Erase`)** | 237.9 ns/op (0 B, 0 allocs) | **147.7 ns/op** (0 B, 0 allocs) | 310.6 ns/op (0 B, 0 allocs) |

> ¹ **Existing-key overwrite `Insert`** (when the working set fits within `maxSize`) updates the entry in place with **0 B, 0 allocs/op** across `MapCache`, `RadixCache`, and `ArenaRadixCache`. By contrast, `Benchmark_Insert_*` configures `capacity := uint64(len(keys) * 5)` (50,000 B for a 10,000-key × 10 B working set, holding 5,000 of the 10,000 keys), exercising **50%-capacity turnover `Insert`** where every `Insert` after warmup is a new-key insert paired with an LRU tail eviction: `MapCache` and `RadixCache` perform **3 allocs/op** (`strings.Clone` + `*list.Element`/`*entry` or `*radixNode` + evicted `Entry`), whereas `ArenaRadixCache` performs **2 allocs/op** (`strings.Clone` + evicted `Entry`, with **0 node allocations** thanks to intrusive free-list node recycling).
>
> ² `UpdateWithoutChangingOrder` performs **0 heap allocations** across all three backends (`MapCache`, `RadixCache`, and `ArenaRadixCache`), storing the generic value `V` directly in the node/entry without interface boxing.

### Key Takeaways
- **`MapCache`** achieves the lowest single-key point lookup (~50 ns) and insertion (~98 ns in-place overwrite; ~383–466 ns under 50%-capacity turnover) latency when keys are flat and prefix operations are rare.
- **`ArenaRadixCache`** accelerates radix lookups by **1.6x–1.7x** over `RadixCache` (~100 ns vs ~170 ns) via its 64-bit FNV-1a index and zero-allocation bottom-up key verifier (`verifyKey`).
- All three backends (**`MapCache`**, **`RadixCache`**, and **`ArenaRadixCache`**) perform in-place value updates (`UpdateWithoutChangingOrder` and existing-key `Insert` overwrites) with **0 internal heap allocations per operation**, and `ArenaRadixCache` recycles deleted node indices in steady state via its intrusive free-list (`2 allocs/op` vs `3 allocs/op` during 50%-capacity turnover).

---

## 2. Subtree Prefix Deletion Latency (`EraseEntriesWithGivenPrefix`)

| Prefix Topology | `MapCache` (`O(N)` Scan) | `RadixCache` (`O(P + S)` Subtree) | `ArenaRadixCache` (`O(P + S)` Subtree) | Speedup vs `MapCache` |
| :--- | :--- | :--- | :--- | :--- |
| **Flat Prefix (100 items)** | 160.6 µs/op | **2.25 µs/op** | ~7.5 µs/op | **71.3x (`Radix`) / ~21.4x (`Arena`) faster** |
| **Nested Prefix (100 items)** | 151.2 µs/op | **2.30 µs/op** | ~7.5 µs/op | **65.7x (`Radix`) / ~20.2x (`Arena`) faster** |
| **Deeply Nested (100 items)** | 219.9 µs/op | **2.50 µs/op** | ~7.6 µs/op | **88.0x (`Radix`) / ~28.9x (`Arena`) faster** |
| **100K Scale (50,000 purged items)** | 21.6 ms/op | **1.05 ms/op** | ~5.1 ms/op | **20.5x (`Radix`) / ~4.2x (`Arena`) faster** |

---

## 3. True Heap Memory Footprint (100K & 1,000,000 Keys)

Measured via `runtime.ReadMemStats` (`HeapAlloc`) after an isolated garbage collection sweep (`runtime.GC()` + `debug.FreeOSMemory()`):

| Scale & Key Topology | `MapCache` Heap | `RadixCache` Heap | `ArenaRadixCache` Heap | Memory Reduction vs `MapCache` |
| :--- | :--- | :--- | :--- | :--- |
| **100K Nested (`Benchmark_LargeScale_Insert_100K`)** | ~16.3 MB (~163.0 `heap-B/entry`) | **~9.6 MB** (~96.0 `heap-B/entry`) | ~11.1 MB (~111.2 `heap-B/entry`) | **41.1% less (`Radix`) / 31.8% less (`Arena`)** |
| **1M Flat** (`file_%d.txt`) | 129.6 MB (135.9 B/entry) | **89.8 MB** (94.2 B/entry) | 102.0 MB (106.9 B/entry) | **30.7% less (`Radix`) / 21.4% less (`Arena`)** |
| **1M Nested** (`dir_%04d/file_%04d.txt`) | 129.6 MB (135.9 B/entry) | **90.5 MB** (94.9 B/entry) | 101.9 MB (106.9 B/entry) | **30.2% less (`Radix`) / 21.4% less (`Arena`)** |
| **1M Deeply Nested** (`projects/...`) | 129.6 MB (135.9 B/entry) | **90.9 MB** (95.4 B/entry) | 101.9 MB (106.9 B/entry) | **29.9% less (`Radix`) / 21.4% less (`Arena`)** |

In addition to reported `b.ReportAllocs()` (`B/op` and `allocs/op`), `Benchmark_LargeScale_Insert_100K` reports `heap-B/entry` (net live heap bytes per entry at 100K scale, where Go map bucket power-of-two sizing accounts for the higher per-entry overhead in `MapCache` relative to 1M scale) and `Benchmark_ArenaRadixCache_Compact` reports `reclaimed-B/op` (live heap bytes reclaimed per compaction pass).

---

## 4. Reproducing Benchmarks & Memory Profiles Locally

```bash
# Run the complete benchmark suite with memory & custom resource metrics
go test -run=^$ -bench=. -benchmem ./...

# Run point lookup, insert, update, and erase benchmarks
go test -run=^$ -bench="Benchmark_(Insert|LookUp|Update|Erase)" -benchmem ./...

# Run multi-core parallel throughput benchmarks (Mixed, ReadHeavy, WriteHeavy)
go test -run=^$ -bench="Benchmark_ParallelThroughput" -benchmem ./...

# Run 100K-entry scale & ArenaRadixCache compaction/pressure benchmarks
go test -run=^$ -bench="Benchmark_(LargeScale|ArenaRadixCache)" -benchmem ./...

# Run live heap footprint (heap-B/entry) and compaction reclamation (reclaimed-B/op) benchmarks
go test -run=^$ -bench="Benchmark_(LargeScale_Insert_100K|ArenaRadixCache_Compact)" -benchmem ./...
```
