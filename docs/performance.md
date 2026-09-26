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
| **Insert (Flat)** | **97.7 ns/op** (32 B, 1 alloc) | 178.2 ns/op (0 B, 0 allocs) | 270.6 ns/op (1 B, 0 allocs) |
| **Insert (Nested, Depth 2)** | **98.1 ns/op** (32 B, 1 alloc) | 172.3 ns/op (0 B, 0 allocs) | 270.9 ns/op (1 B, 0 allocs) |
| **Insert (Deeply Nested, Depth 10)** | **119.0 ns/op** (32 B, 1 alloc) | 229.9 ns/op (0 B, 0 allocs) | 471.8 ns/op (2 B, 0 allocs) |
| **Update Value (`UpdateWithoutChangingOrder`)** | **82.5 ns/op** (32 B, 1 alloc) | 97.3 ns/op (0 B, 0 allocs) | 88.6 ns/op (0 B, 0 allocs) |
| **Individual Erase (`Erase`)** | 237.9 ns/op (0 B, 0 allocs) | **147.7 ns/op** (0 B, 0 allocs) | 310.6 ns/op (0 B, 0 allocs) |

### Key Takeaways
- **`MapCache`** achieves the lowest single-key point lookup (~50 ns) and insertion (~98 ns) latency when keys are flat and prefix operations are rare.
- **`ArenaRadixCache`** accelerates radix lookups by **1.6x–1.7x** over `RadixCache` (~100 ns vs ~170 ns) via its 64-bit FNV-1a index and zero-allocation bottom-up key verifier (`verifyKey`).
- Both **`RadixCache`** and **`ArenaRadixCache`** perform in-place value updates (`UpdateWithoutChangingOrder`) and steady-state node recycling with **0 heap allocations per operation**.

---

## 2. Subtree Prefix Deletion Latency (`EraseEntriesWithGivenPrefix`)

| Prefix Topology | `MapCache` (`O(N)` Scan) | `RadixCache` (`O(P + S)` Subtree) | `ArenaRadixCache` (`O(P + S)` Subtree) | Speedup vs `MapCache` |
| :--- | :--- | :--- | :--- | :--- |
| **Flat Prefix (100 items)** | 160.6 µs/op | **2.25 µs/op** | 27.6 µs/op | **71.3x faster** (`RadixCache`) |
| **Nested Prefix (100 items)** | 151.2 µs/op | **2.30 µs/op** | 27.8 µs/op | **65.7x faster** (`RadixCache`) |
| **Deeply Nested (100 items)** | 219.9 µs/op | **2.50 µs/op** | 28.4 µs/op | **88.0x faster** (`RadixCache`) |
| **100K Scale (50,000 purged items)** | 21.6 ms/op | **1.05 ms/op** | 13.6 ms/op | **20.5x faster** (`RadixCache`) |

---

## 3. True Heap Memory Footprint (1,000,000 Keys)

Measured via `runtime.ReadMemStats` (`HeapAlloc`) after an isolated garbage collection sweep (`runtime.GC()` + `debug.FreeOSMemory()`):

| Key Topology (1,000,000 Entries) | `MapCache` Heap | `RadixCache` Heap | `ArenaRadixCache` Heap | Memory Reduction vs `MapCache` |
| :--- | :--- | :--- | :--- | :--- |
| **Flat** (`file_%d.txt`) | 129.6 MB (135.9 B/entry) | **89.8 MB** (94.2 B/entry) | 102.0 MB (106.9 B/entry) | **30.7% less (`Radix`) / 21.4% less (`Arena`)** |
| **Nested** (`dir_%04d/file_%04d.txt`) | 129.6 MB (135.9 B/entry) | **90.5 MB** (94.9 B/entry) | 101.9 MB (106.9 B/entry) | **30.2% less (`Radix`) / 21.4% less (`Arena`)** |
| **Deeply Nested** (`projects/...`) | 129.6 MB (135.9 B/entry) | **90.9 MB** (95.4 B/entry) | 101.9 MB (106.9 B/entry) | **29.9% less (`Radix`) / 21.4% less (`Arena`)** |

In addition to reported `b.ReportAllocs()` (`B/op` and `allocs/op`), `Benchmark_LargeScale_Insert_100K` reports `heap-B/entry` (net live heap bytes per entry) and `Benchmark_ArenaRadixCache_Compact` reports `reclaimed-B/op` (live heap bytes reclaimed per compaction pass).

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

# Run 1M-key heap memory footprint profiling tests
go test -v -run=TestMemoryFootprint_ ./...
```
