# go-lru

[![Go Reference](https://pkg.go.dev/badge/github.com/google/go-lru.svg)](https://pkg.go.dev/github.com/google/go-lru)
[![CI](https://github.com/google/go-lru/actions/workflows/ci.yml/badge.svg)](https://github.com/google/go-lru/actions/workflows/ci.yml)
[![Benchmarks](https://github.com/google/go-lru/actions/workflows/benchmarks.yml/badge.svg)](https://github.com/google/go-lru/actions/workflows/benchmarks.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)]()

`package lru` (`github.com/google/go-lru`) is a high-concurrency, zero-dependency Go library providing size-aware Least Recently Used (LRU) cache implementations behind a unified generic `Cache[V any]` interface. `go-lru` supports flat key-value caching (`MapCache`), hierarchical prefix-eviction trees (`RadixCache`), and arena-allocated `uint32`-indexed trees with two-tier memory-pressure reclamation (`ArenaRadixCache`).

---

## Installation

```bash
go get github.com/google/go-lru
```

---

## Quickstart

Use `lru.New[V]` with any Go type `V`—by default each entry weighs `1`, or pass `lru.WithWeigher` for custom byte/resource accounting:

```go
package main

import (
	"fmt"

	"github.com/google/go-lru"
)

func main() {
	// 1. Initialize a byte-weighted 64 MiB LRU cache (defaults to MapCache; use WithBackend for Radix/Arena).
	cache := lru.New[[]byte](
		64*1024*1024,
		lru.WithBackend(lru.BackendArenaRadix),
		lru.WithWeigher(func(key string, value []byte) uint64 {
			return uint64(len(value))
		}),
	)

	// 2. Insert typed values directly—no wrapper interfaces required.
	_, _ = cache.Insert("bucket/dirA/config.json", []byte(`{"version":1}`))
	_, _ = cache.Insert("bucket/dirA/chunk.bin", []byte{0xDE, 0xAD, 0xBE, 0xEF})
	_, _ = cache.Insert("bucket/dirB/inode-42", make([]byte, 128))

	// 3. Point Lookup (promotes entry to MRU).
	if v, ok := cache.LookUp("bucket/dirA/config.json"); ok {
		fmt.Printf("config: %s (%d bytes)\n", string(v), len(v))
	}

	// 4. Inspect or update in-place without altering LRU recency order.
	//    If the new value weighs more, older entries are automatically evicted if capacity is exceeded.
	_, _ = cache.LookUpWithoutChangingOrder("bucket/dirB/inode-42")
	_ = cache.UpdateWithoutChangingOrder("bucket/dirA/chunk.bin", make([]byte, 4096))

	// 5. Fast O(prefix + subtree) prefix eviction and individual key erasure.
	cache.EraseEntriesWithGivenPrefix("bucket/dirA/")
	_ = cache.Erase("bucket/dirB/inode-42")
}
```

When `WithWeigher` is omitted, each entry defaults to weight `1`, making `maxSize` an entry-count capacity (e.g., `lru.New[string](10_000)` holds up to 10,000 strings).

---

## Choosing a Cache Backend

Select an engine via `lru.New[V](maxSize, lru.WithBackend(...))` or call its dedicated generic constructor directly:

| Backend | Constructor / Option | Lookup / Insert | Prefix Erase (`EraseEntriesWithGivenPrefix`) | Memory & GC Profile | Best Workload Fit |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`MapCache`** *(default)* | `lru.NewMapCache[V]` / `WithBackend(BackendMap)` | `O(1)` (~50 ns / ~98 ns in-place, ~383–466 ns turnover) | `O(N)` full map scan | ~136–163 B/entry, standard GC pointers | Flat keys, maximum point read/write throughput |
| **`RadixCache`** | `lru.NewRadixCache[V]` / `WithBackend(BackendRadix)` | `O(K)` (~170 ns / ~172–230 ns in-place, ~472–511 ns turnover) | `O(P + S)` subtree (**20x–88x faster**) | ~94–96 B/entry (**~30–41% less heap**), 0-alloc updates | File paths, object storage namespaces, frequent prefix purges |
| **`ArenaRadixCache`** | `lru.NewArenaRadixCache[V]` / `WithBackend(BackendArenaRadix)` | `O(1)` hash-accelerated (~100 ns / ~271–472 ns in-place, ~515–822 ns turnover) | `O(P + S)` subtree (**4x–29x faster**) | ~107–111 B/entry, `uint32` slice indices, **two-tier pressure compaction** | 1M+ hierarchical entries, strict GC latency & `GOMEMLIMIT` budgets |

All three backends (`MapCache`, `RadixCache`, and `ArenaRadixCache`) also implement `lru.PressureAwareCache[V]`, exposing `Compact()` and `EvaluateMemoryPressure()`.

---

## Configuration Options

| Option | Default | Applies To | Description |
| :--- | :--- | :--- | :--- |
| `WithBackend(backend)` | `BackendMap` | `lru.New` | Selects underlying cache engine (`BackendMap`, `BackendRadix`, `BackendArenaRadix`) |
| `WithWeigher(fn)` | `1` per entry | All engines | Custom entry-weight callback `func(key string, value V) uint64` for byte/resource-bounded eviction |
| `WithInvariantChecking(bool)` | `false` | All engines | Enables runtime structural & size parity invariant verification (for tests/debugging) |
| `WithMemoryBudget(bytes)` | `0` (`GOMEMLIMIT`) | All engines | Explicit memory ceiling in bytes for the built-in `runtime/metrics` pressure probe |
| `WithPressureFunc(fn)` | `DefaultRuntimePressureFunc` | All engines | Custom callback `func() float64` returning normalized memory pressure in `[0.0, 1.0+]` |
| `WithCompactionThreshold(float64)` | `0.75` (`DefaultCompactionThreshold`) | All engines | Tier 1 Moderate Pressure threshold triggering lossless backing structure & hash map compaction |
| `WithEvictionThreshold(float64)` | `0.90` (`DefaultEvictionThreshold`) | All engines | Tier 2 Critical Pressure threshold triggering proactive LRU tail shedding + compaction |
| `WithEvictionRetentionRatio(float64)` | `0.50` (`DefaultEvictionRetentionRatio`) | All engines | Target fraction `[0.0, 1.0]` of `maxSize` retained during Tier 2 Critical Pressure shedding |

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
