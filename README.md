# go-lru

[![Go Reference](https://pkg.go.dev/badge/github.com/google/go-lru.svg)](https://pkg.go.dev/github.com/google/go-lru)
[![CI](https://github.com/google/go-lru/actions/workflows/ci.yml/badge.svg)](https://github.com/google/go-lru/actions/workflows/ci.yml)
[![Benchmarks](https://github.com/google/go-lru/actions/workflows/benchmarks.yml/badge.svg)](https://github.com/google/go-lru/actions/workflows/benchmarks.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)]()

`package lru` (`github.com/google/go-lru`) is a high-concurrency, zero-dependency Go library providing size-aware Least Recently Used (LRU) cache implementations behind a unified `Cache` interface. Adapted and evolved from Google Cloud Storage FUSE ([GCSFuse](https://github.com/GoogleCloudPlatform/gcsfuse)), `go-lru` supports flat key-value caching (`MapCache`), hierarchical prefix-eviction trees (`RadixCache`), and arena-allocated `uint32`-indexed trees with two-tier memory-pressure reclamation (`ArenaRadixCache`).

---

## Installation

```bash
go get github.com/google/go-lru
```

---

## Quickstart

Use `lru.New` with built-in `ValueType` wrappers (`lru.StringValue`, `lru.BytesValue`, `lru.NewValue`)—no custom struct boilerplate required:

```go
package main

import (
	"fmt"

	"github.com/google/go-lru"
)

func main() {
	// 1. Initialize a 64 MiB LRU cache (defaults to MapCache; use WithBackend for Radix/Arena).
	cache := lru.New(
		64*1024*1024,
		lru.WithBackend(lru.BackendArenaRadix),
	)

	// 2. Insert common types directly using built-in ValueType helpers.
	_, _ = cache.Insert("bucket/dirA/config.json", lru.StringValue(`{"version":1}`))
	_, _ = cache.Insert("bucket/dirA/chunk.bin", lru.BytesValue([]byte{0xDE, 0xAD, 0xBE, 0xEF}))
	_, _ = cache.Insert("bucket/dirB/inode-42", lru.NewValue(uint64(42), 128))

	// 3. Point Lookup (promotes entry to MRU).
	if v := cache.LookUp("bucket/dirA/config.json"); v != nil {
		fmt.Printf("config: %s (%d bytes)\n", v.(lru.StringValue), v.Size())
	}

	// 4. Inspect or update without altering LRU recency order.
	_ = cache.LookUpWithoutChangingOrder("bucket/dirB/inode-42")
	_ = cache.UpdateWithoutChangingOrder("bucket/dirB/inode-42", lru.NewValue(uint64(43), 128))

	// 5. Incrementally grow entry size (automatically evicts LRU entries if capacity is exceeded).
	_ = cache.UpdateSize("bucket/dirA/chunk.bin", 4096)

	// 6. Fast O(prefix + subtree) prefix eviction and individual key erasure.
	cache.EraseEntriesWithGivenPrefix("bucket/dirA/")
	_ = cache.Erase("bucket/dirB/inode-42")
}
```

Custom domain types can also implement `lru.ValueType` directly by defining `Size() uint64`.

---

## Choosing a Cache Backend

Select an engine via `lru.New(maxSize, lru.WithBackend(...))` or call its dedicated constructor directly:

| Backend | Constructor / Option | Lookup / Insert | Prefix Erase (`EraseEntriesWithGivenPrefix`) | Memory & GC Profile | Best Workload Fit |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`MapCache`** *(default)* | `lru.NewMapCache` / `WithBackend(BackendMap)` | `O(1)` (~50 ns / ~98 ns) | `O(N)` full map scan | ~136 B/entry, standard GC pointers | Flat keys, maximum point read/write throughput |
| **`RadixCache`** | `lru.NewRadixCache` / `WithBackend(BackendRadix)` | `O(K)` (~170 ns / ~175 ns) | `O(P + S)` subtree (**20x–88x faster**) | ~94 B/entry (**~30% less heap**), 0-alloc updates | File paths, object storage namespaces, frequent prefix purges |
| **`ArenaRadixCache`** | `lru.NewArenaRadixCache` / `WithBackend(BackendArenaRadix)` | `O(1)` hash-accelerated (~100 ns) | `O(P + S)` subtree | ~107 B/entry, `uint32` slice indices, **two-tier pressure compaction** | 1M+ hierarchical entries, strict GC latency & `GOMEMLIMIT` budgets |

When `ArenaRadixCache` is used, the returned `Cache` also implements `lru.PressureAwareCache`, exposing `Compact()` and `EvaluateMemoryPressure()`.

---

## Configuration Options

| Option | Default | Applies To | Description |
| :--- | :--- | :--- | :--- |
| `WithBackend(backend)` | `BackendMap` | `lru.New` | Selects underlying cache engine (`BackendMap`, `BackendRadix`, `BackendArenaRadix`) |
| `WithInvariantChecking(bool)` | `false` | All engines | Enables runtime structural & size parity invariant verification (for tests/debugging) |
| `WithMemoryBudget(bytes)` | `0` (`GOMEMLIMIT`) | `ArenaRadixCache` | Explicit memory ceiling in bytes for the built-in `runtime/metrics` pressure probe |
| `WithPressureFunc(fn)` | `DefaultRuntimePressureFunc` | `ArenaRadixCache` | Custom callback `func() float64` returning normalized memory pressure in `[0.0, 1.0+]` |
| `WithCompactionThreshold(float64)` | `0.75` (`DefaultCompactionThreshold`) | `ArenaRadixCache` | Tier 1 Moderate Pressure threshold triggering lossless node arena & hash map compaction |
| `WithEvictionThreshold(float64)` | `0.90` (`DefaultEvictionThreshold`) | `ArenaRadixCache` | Tier 2 Critical Pressure threshold triggering proactive LRU tail shedding + compaction |
| `WithEvictionRetentionRatio(float64)` | `0.50` (`DefaultEvictionRetentionRatio`) | `ArenaRadixCache` | Target fraction `[0.0, 1.0]` of `maxSize` retained during Tier 2 Critical Pressure shedding |

---

## Documentation & Verification

- **[Architecture & Memory-Pressure Reclamation (`docs/architecture.md`)](docs/architecture.md)**: Deep dive into `MapCache`, `RadixCache`, and `ArenaRadixCache` node layouts, 32-bit slice arena indexing, FNV-1a lookup acceleration, and Two-Tier Memory-Pressure Reclamation.
- **[Benchmarks & Heap Footprint (`docs/performance.md`)](docs/performance.md)**: Empirical latency, prefix deletion speedups, 1M-key true heap footprint tables, and CLI reproduction commands.
- **[Changelog & Versioning (`CHANGELOG.md`)](CHANGELOG.md)**: Release history and semantic versioning guide.

```bash
# Run unit, differential, concurrency (-race), and runnable Example tests
go test -race ./...
go test -v -run=^Example ./...
```

---

## License & Disclaimer

Released under the [Apache 2.0 License](LICENSE). See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines.
This is not an officially supported Google product. This project is not eligible for the [Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security).

<!-- b06a -->
