# Empirical Performance & Heap Footprint

This document records empirical latency, allocation, subtree prefix eviction, and 1,000,000-key live heap memory footprint benchmarks across `MapCache`, `RadixCache`, and `ArenaRadixCache`.

All reference benchmarks were collected on an **Intel Xeon CPU @ 2.60GHz (96 vCPUs, `linux/amd64`)** using `go test -bench=. -benchmem`.

---

## 1. Point Operation Latency & Allocations

| Operation | `MapCache` | `RadixCache` | `ArenaRadixCache` |
| :--- | :--- | :--- | :--- |
| **Get (Flat)** | **50.6 ns/op** (0 B, 0 allocs) | 174.0 ns/op (0 B, 0 allocs) | 100.7 ns/op (0 B, 0 allocs) |
| **Get (Nested, Depth 2)** | **49.5 ns/op** (0 B, 0 allocs) | 169.6 ns/op (0 B, 0 allocs) | 107.8 ns/op (0 B, 0 allocs) |
| **Get (Deeply Nested, Depth 10)** | **64.3 ns/op** (0 B, 0 allocs) | 185.2 ns/op (0 B, 0 allocs) | 194.1 ns/op (0 B, 0 allocs) |
| **Peek (`Peek`)** | **33.2 ns/op** (0 B, 0 allocs) | 163.3 ns/op (0 B, 0 allocs) | 103.8 ns/op (0 B, 0 allocs) |
| **Put (Existing-Key Overwrite, In-Place)**¹ | **97.7 ns/op** (0 B, 0 allocs in-place) | 178.2 ns/op (0 B, 0 allocs in-place) | 270.6 ns/op (0 B, 0 allocs in-place) |
| **Put (Nested, Depth 2, In-Place Overwrite)**¹ | **98.1 ns/op** (0 B, 0 allocs in-place) | 172.3 ns/op (0 B, 0 allocs in-place) | 270.9 ns/op (0 B, 0 allocs in-place) |
| **Put (Deeply Nested, Depth 10, In-Place Overwrite)**¹ | **119.0 ns/op** (0 B, 0 allocs in-place) | 229.9 ns/op (0 B, 0 allocs in-place) | 471.8 ns/op (0 B, 0 allocs in-place) |
| **Put (`Benchmark_Put_*`, 50%-Capacity Turnover)**¹ | **383–466 ns/op** (96–144 B, 2–3 allocs/op) | 472–511 ns/op (132–135 B, 2–3 allocs/op) | 515–822 ns/op (~30–35 B, <= 1 allocs/op) |
| **Replace Value (`Replace`)**² | **71.3 ns/op** (0 B internal, 0 allocs) | 121.4 ns/op (0 B, 0 allocs) | 124.4 ns/op (0 B, 0 allocs) |
| **Individual Delete (`Delete`)** | 237.9 ns/op (0 B, 0 allocs) | **147.7 ns/op** (0 B, 0 allocs) | 310.6 ns/op (0 B, 0 allocs) |
| **Stats Snapshot (`Stats()`)** | **~16 ns/op** (0 B, 0 allocs) | **~16 ns/op** (0 B, 0 allocs) | **~17 ns/op** (0 B, 0 allocs) |

> ¹ **Existing-key overwrite `Put`** (when the working set fits within `maxSize`) updates the entry in place with **0 B, 0 allocs/op** across `MapCache`, `RadixCache`, and `ArenaRadixCache`, and a new-entry `Put` into a warm `MapCache` during initial growth allocates **1 heap node (`*entry[V]`)** thanks to its generic intrusive doubly-linked list (`entryList[V]`). To prevent caller substring backing arrays from being pinned by leaf or routing nodes, `MapCache` and `RadixCache` defensively clone retained multi-byte non-digit key/prefix strings via `clonePrefix` (`strings.Clone`), whereas `ArenaRadixCache` stores edge prefixes in a contiguous `prefixArena []byte` (with 32-slot `freePrefixSpans` reuse, eliminating per-node heap string headers). By contrast, `Benchmark_Put_*` configures `capacity := uint64(len(keys) * 5)` (50,000 B for a 10,000-key × 10 B working set, holding 5,000 of the 10,000 keys), exercising **50%-capacity turnover `Put`** where every `Put` after warmup is a new-key write paired with an LRU tail eviction: during steady-state capacity turnover in `Put`, `MapCache` and `RadixCache` recycle evicted `*entry[V]` / `*radixNode[V]` structs in-place (`recycledEntry` in `MapCache`, `recycleNodes [2]*radixNode[V]` in `RadixCache`) while allocating `clonePrefix` (`strings.Clone`) + the returned evicted `[]V` slice (**2–3 allocs/op** across the combined warmup + turnover run), whereas `ArenaRadixCache` recycles both arena node slots via its intrusive free-list and prefix bytes via `prefixArena` / `freePrefixSpans` (**<= 1 allocs/op** after integer truncation across the warmup phase, with **0 node allocations**).
>
> ² `Replace` performs **0 heap allocations** across all three backends (`MapCache`, `RadixCache`, and `ArenaRadixCache`), storing the generic value `V` directly in the node/entry without interface boxing.

### Key Takeaways
- **`MapCache`** achieves the lowest single-key point lookup (~50 ns) and write (~98 ns in-place overwrite; ~383–466 ns under 50%-capacity turnover) latency when keys are flat and prefix operations are rare.
- **`ArenaRadixCache`** accelerates radix lookups by **1.6x–1.7x** over `RadixCache` (~100 ns vs ~170 ns) via its seeded 64-bit `hash/maphash` open-addressed flat hash table (`hashSlots []arenaHashSlot`) and zero-allocation `keyLen` + suffix verifier (`verifyKey`).
- All three backends (**`MapCache`**, **`RadixCache`**, and **`ArenaRadixCache`**) perform in-place value updates (`Replace` and existing-key `Put` overwrites) and telemetry snapshots (`Stats()`) with **0 internal heap allocations per operation**; during steady-state capacity turnover in `Put`, `MapCache` (`recycledEntry`) and `RadixCache` (`recycleNodes [2]*radixNode[V]`) recycle evicted node structs in-place, while `ArenaRadixCache` recycles both node indices via its intrusive free-list and prefix spans in `prefixArena []byte` (`<= 1 allocs/op` vs `2–3 allocs/op` during 50%-capacity turnover).

---

## 2. Subtree Prefix Deletion Latency (`DeletePrefix`)

| Prefix Topology | `MapCache` (`O(N)` Scan) | `RadixCache` (`O(P + S)` Subtree) | `ArenaRadixCache` (`O(P + S)` Subtree) | Speedup vs `MapCache` |
| :--- | :--- | :--- | :--- | :--- |
| **Flat Prefix (100 items)** | 160.6 µs/op | **2.25 µs/op** | ~4.7 µs/op | **71.3x (`Radix`) / ~34.2x (`Arena`) faster** |
| **Nested Prefix (100 items)** | 151.2 µs/op | **2.30 µs/op** | ~4.7 µs/op | **65.7x (`Radix`) / ~32.2x (`Arena`) faster** |
| **Deeply Nested (100 items)** | 219.9 µs/op | **2.50 µs/op** | ~4.8 µs/op | **88.0x (`Radix`) / ~45.8x (`Arena`) faster** |
| **100K Scale (50,000 purged items)** | 21.6 ms/op | **1.05 ms/op** | ~3.45 ms/op | **20.5x (`Radix`) / ~6.3x (`Arena`) faster** |

---

## 3. True Heap Memory Footprint (100K & 1,000,000 Keys)

Measured via `runtime.ReadMemStats` (`HeapAlloc`) after an isolated garbage collection sweep (`runtime.GC()` + `debug.FreeOSMemory()`):

| Scale & Key Topology | `MapCache` Heap | `RadixCache` Heap | `ArenaRadixCache` Heap | Memory Reduction vs `MapCache` |
| :--- | :--- | :--- | :--- | :--- |
| **100K Nested (`Benchmark_LargeScale_Put_100K`)** | ~11.5 MB (~115.0 `heap-B/entry`) | ~9.6 MB (~96.0 `heap-B/entry`) | **~7.9 MB** (~78.9 `heap-B/entry`) | **16.5% less (`Radix`) / 31.4% less (`Arena`)** |
| **1M Flat** (`file_%d.txt`) | 129.6 MB (135.9 B/entry) | **89.8 MB** (94.2 B/entry) | 102.0 MB (106.9 B/entry) | **30.7% less (`Radix`) / 21.4% less (`Arena`)** |
| **1M Nested** (`dir_%04d/file_%04d.txt`) | 129.6 MB (135.9 B/entry) | **90.5 MB** (94.9 B/entry) | 101.9 MB (106.9 B/entry) | **30.2% less (`Radix`) / 21.4% less (`Arena`)** |
| **1M Deeply Nested** (`projects/...`) | 129.6 MB (135.9 B/entry) | **90.9 MB** (95.4 B/entry) | 101.9 MB (106.9 B/entry) | **29.9% less (`Radix`) / 21.4% less (`Arena`)** |

In addition to reported `b.ReportAllocs()` (`B/op` and `allocs/op`), `Benchmark_LargeScale_Put_100K` reports `heap-B/entry` (net live heap bytes per entry at 100K scale, where Go map bucket power-of-two sizing accounts for the higher per-entry overhead in `MapCache` relative to 1M scale) and `Benchmark_ArenaRadixCache_Compact` reports `reclaimed-B/op` (live heap bytes reclaimed per compaction pass).

---

## 4. Reproducing Benchmarks & Memory Profiles Locally

```bash
# Run the complete benchmark suite with memory & custom resource metrics
go test -run=^$ -bench=. -benchmem ./...

# Run point get, peek, put, replace, delete, and stats snapshot benchmarks
go test -run=^$ -bench="Benchmark_(Put|Get|Peek|Replace|Delete|Stats)" -benchmem ./...

# Run multi-core parallel throughput benchmarks (Mixed, ReadHeavy, WriteHeavy)
go test -run=^$ -bench="Benchmark_ParallelThroughput" -benchmem ./...

# Run 100K-entry scale & ArenaRadixCache compaction/pressure benchmarks
go test -run=^$ -bench="Benchmark_(LargeScale|ArenaRadixCache)" -benchmem ./...

# Run live heap footprint (heap-B/entry) and compaction reclamation (reclaimed-B/op) benchmarks
go test -run=^$ -bench="Benchmark_(LargeScale_Put_100K|ArenaRadixCache_Compact)" -benchmem ./...

# Run extended production characterization benchmarks (Zipfian/Uniform, payload sizes, concurrency scaling, GC pause)
go test -run=^$ -bench="Benchmark(KeyDistributions|PayloadSizes|ConcurrencyScaling|CompactionAndPressureShedLatency|GCPauseAndScanOverhead|PressureSamplingAndCallbacks|CustomPressureFuncOverhead)" -benchmem .
```

---

## 5. Enforcing Performance Targets & Regression Thresholds (`./cmd/benchcheck`)

`./cmd/benchcheck` is a zero-dependency Go command that parses `go test -bench=^Benchmark_ -benchmem` output (including multi-sample `-count=N` runs and custom metrics `heap-B/entry` and `reclaimed-B/op`), enforces calibrated absolute performance and zero-allocation targets across all 92 target-enforced `Benchmark_*` sub-benchmarks, and detects relative regressions between a baseline and candidate run.

### Local Target & Regression Verification

```bash
# 1. Run target-enforced benchmarks and verify absolute performance & zero-allocation targets directly via stdin
go test -run=^$ -bench=^Benchmark_ -benchmem -benchtime=10ms ./... | go run ./cmd/benchcheck -head -

# 2. Save a baseline benchmark run (e.g., on main) and compare a candidate branch against it
go test -run=^$ -bench=^Benchmark_ -benchmem -benchtime=10ms -count=6 ./... | tee benchmark-base.txt
# ... apply code changes ...
go test -run=^$ -bench=^Benchmark_ -benchmem -benchtime=10ms -count=6 ./... | tee benchmark-head.txt
go run ./cmd/benchcheck -base benchmark-base.txt -head benchmark-head.txt -format text
```

### CLI Flags & Threshold Configuration

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-head`, `-candidate` | *(required)* | Path to candidate benchmark output file (`-` for `stdin`, or pass as positional arg) |
| `-base`, `-baseline` | `""` | Optional path to baseline benchmark output file for relative regression comparison |
| `-check-targets` | `true` | Enforce calibrated absolute latency, memory, zero-allocation, and custom metric targets |
| `-targets` | `""` | Optional path to a custom JSON target rules file overriding `DefaultTargets()` |
| `-agg` | `"median"` | Multi-sample (`-count=N`) aggregation mode (`"median"` or `"mean"`) |
| `-max-ns-regression` | `0.35` (`+35%`) | Maximum allowed relative increase in `ns/op` |
| `-max-bytes-regression` | `0.25` (`+25%`) | Maximum allowed relative increase in `B/op` |
| `-max-allocs-regression` | `0.10` (`+10%`) | Maximum allowed relative increase in `allocs/op` (`0 -> >=1` always fails) |
| `-max-custom-regression` | `0.15` (`15%`) | Maximum allowed relative regression in custom metrics (`+15%` for `heap-B/entry`, `-15%` for `reclaimed-B/op`) |
| `-min-ns-delta` | `15.0` | Minimum absolute `ns/op` increase required to flag a regression |
| `-min-bytes-delta` | `16.0` | Minimum absolute `B/op` increase required to flag a regression |
| `-min-allocs-delta` | `1.0` | Minimum absolute `allocs/op` increase required to flag a regression |
| `-min-custom-delta` | `1.0` | Minimum absolute custom metric delta required to flag a regression |
| `-strict-missing` | `false` | Fail if any baseline benchmark is missing from candidate or unmatched by target rules |
| `-allow-within-targets` | `true` | Allow relative increases that remain within configured absolute target budgets |
| `-format` | `"markdown"` | Output report format (`"markdown"` or `"text"`) |
| `-summary` | `""` | Optional path (e.g., `$GITHUB_STEP_SUMMARY`) to append the Markdown report to |

### Exit Codes & CI Integration (`.github/workflows/benchmarks.yml`)

- **`0`**: All enabled target budgets and relative regression checks passed.
- **`1`**: One or more absolute performance/allocation targets were violated or a relative regression exceeded the configured threshold.
- **`2`**: Invalid CLI arguments, unreadable/malformed benchmark output, or test failure (`FAIL`) detected in the benchmark log.

In GitHub Actions (`.github/workflows/benchmarks.yml`), every `push`, `pull_request`, and `workflow_dispatch` build runs `./cmd/benchcheck`, appends the structured pass/fail Markdown report to `$GITHUB_STEP_SUMMARY`, uploads `benchcheck-report.md` as a build artifact, and fails the workflow job whenever any target or regression threshold is violated.
