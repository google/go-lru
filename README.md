# High-Performance Go LRU Cache Suite

[![Go Reference](https://pkg.go.dev/badge/github.com/googlecloudplatform/gcsfuse/v3/internal/cache/lru.svg)](https://pkg.go.dev/github.com/googlecloudplatform/gcsfuse/v3/internal/cache/lru)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)]()

`lrus` is a standalone, high-concurrency, zero-dependency Go library providing three specialized Least Recently Used (LRU) cache engines satisfying a unified interface. Directly adapted and evolved from Google Cloud Storage FUSE ([GCSFuse](https://github.com/GoogleCloudPlatform/gcsfuse)), `lrus` delivers production-grade caching optimized for diverse workloads: flat key-value lookups, hierarchical file system trees, and massive scale in-memory caches.

---

## Key Features

- **Zero External Dependencies**: Implemented strictly using the Go standard library (`sync`, `container/list`, `math`, `strings`).
- **Unified Interface**: Seamlessly switch between cache implementations (`MapCache`, `RadixCache`, `ArenaRadixCache`) without changing application logic.
- **Size-Aware Eviction**: Eviction is driven by actual byte or logical size (`ValueType.Size()`), not just entry count.
- **Subtree Prefix Eviction**: $O(\text{prefix\_len} + \text{subtree\_size})$ prefix deletion via radix tree engines—up to **20x–70x faster** than map scans.
- **Memory Compactness**: Radix engines reduce heap footprint by **20% to 31%** compared to standard map-based LRUs.
- **Zero GC Pointer Scanning**: The arena-backed engine allocates nodes in contiguous slice memory with 32-bit indices, eliminating garbage collector pointer graph traversals.
- **Concurrent Safety**: Full multi-goroutine thread safety protected by standard synchronization (`sync.RWMutex` / `sync.Mutex`).
- **Invariant Checking**: Configurable debug integrity checks validate tree consistency, bidirectional pointers, and size parity.

---

## Architectural Comparison Matrix

| Dimension | `MapCache` (`NewMapCache`) | `RadixCache` (`NewRadixCache`) | `ArenaRadixCache` (`NewArenaRadixCache`) |
| :--- | :--- | :--- | :--- |
| **Primary Data Structure** | `map[string]*list.Element` + `container/list.List` | Compressed Radix Tree (LCRS) + Intrusive Pointer LRU List | Contiguous Slice Arena `[]arenaRadixNode` + Freelist + Hash Index |
| **Node Representation** | `list.Element` (48 B) + `entry` (32 B) + Map Buckets | `radixNode` struct (72 B heap object) | `arenaRadixNode` struct (56 B contiguous array entry) |
| **Per-Node Heap Allocations** | 2–3 allocations per entry | 1 allocation per node | **0 allocations** (recycled via free-list) |
| **Node Pointer Width** | 64-bit pointers | 64-bit pointers (5 pointers / node) | **32-bit indices** (`uint32`) |
| **Max Entry Capacity** | Memory / Heap limited | Memory / Heap limited | **$2^{32}-1$ entries** (~4.29 billion nodes) |
| **Point Lookup Latency** | **~50 ns/op** ($O(1)$ Hash Map) | ~170 ns/op ($O(K)$ Trie Descent) | **~100 ns/op** ($O(1)$ Hash Map + Zero-Alloc Path Check) |
| **Sequential Insert Latency** | ~98 ns/op | ~175 ns/op (0 allocs) | ~270 ns/op |
| **Point Update Latency** | ~82 ns/op | ~97 ns/op (0 allocs) | ~88 ns/op (0 allocs) |
| **Prefix Erase (100 items)** | ~160 µs ($O(N)$ full scan) | **~2.2 µs** (**71x faster**) | **~27.6 µs** (**6x faster**) |
| **Prefix Erase (50K/100K items)** | ~21.6 ms | **~1.05 ms** (**20.5x faster**) | ~13.6 ms |
| **1M Items Heap Memory** | ~129.6 MB (~135.9 B/entry) | **~89.8 MB** (**~30.7% reduction**) | **~101.9 MB** (**~21.4% reduction**) |
| **GC Pressure & Overhead** | High (millions of distinct heap objects) | Moderate (heap nodes with pointers) | **Ultra-Low** (flat slice; indices invisible to GC) |
| **Concurrency Lock** | `sync.RWMutex` | `sync.RWMutex` | `sync.Mutex` |
| **Best Used For** | Flat keys, maximum read/write throughput | File systems, directory trees, prefix purges | Large-scale hierarchical caches (1M+ items) |

---

## Engine Deep Dive

### 1. `MapCache` (Standard Map LRU)
The classic LRU implementation combining Go's built-in `map[string]*list.Element` with `container/list.List`.
- **Strengths**: Fastest point lookups (~50 ns) and inserts (~98 ns).
- **Trade-offs**: Prefix deletion requires a linear $O(N)$ scan across all map keys. Highest heap overhead per entry (~136 B/entry).

### 2. `RadixCache` (Pointer-Based Radix Tree LRU)
A compact Radix Tree with Left-Child Right-Sibling (LCRS) tree layout and embedded intrusive doubly-linked list pointers (`prev`, `next`, `parent`, `child`, `sibling`).
- **Strengths**: Subtree detachment allows deleting entire directory subtrees in microseconds. Uses ~30.7% less memory than `MapCache` due to prefix path compression. Zero-allocation node updates.
- **Trade-offs**: Point lookups require traversing tree edges ($O(K)$ where $K$ is key length).

### 3. `ArenaRadixCache` (Flat-Slice Arena Radix Tree LRU)
An arena-allocated radix tree storing all nodes in a contiguous slice `[]arenaRadixNode` indexed by 32-bit integers (`uint32`). Integrates an $O(1)$ FNV-1a hash map accelerator with zero-allocation bottom-up key verification (`verifyKey`).
- **Strengths**: Constant-time lookup acceleration (~100 ns). Zero per-node heap allocations after warm-up via $O(1)$ singly-linked `freeHead` free-list reuse. Zero garbage collector pointer tracing overhead.
- **Trade-offs**: Slightly higher insert latency due to hash table maintenance and array index lookups.

---

## Quickstart

### Installation

```bash
go get github.com/googlecloudplatform/gcsfuse/v3/internal/cache/lru
```

### Implementing `ValueType`

Any cached item must implement the `lrus.ValueType` interface by exposing `Size() uint64`:

```go
package main

import (
	"fmt"
	"github.com/googlecloudplatform/gcsfuse/v3/internal/cache/lru"
)

type FileMetadata struct {
	Inode    uint64
	ByteSize uint64
}

func (f *FileMetadata) Size() uint64 {
	return f.ByteSize
}
```

### Basic Usage

```go
func main() {
	// Create a Radix LRU cache with a 100 MB capacity
	cache := lrus.NewRadixCache(100 * 1024 * 1024)

	// Insert entries
	file := &FileMetadata{Inode: 1001, ByteSize: 4096}
	evicted, err := cache.Insert("projects/app/logs/server.log", file)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Inserted entry; evicted %d items\n", len(evicted))

	// Point Lookup (updates LRU order)
	if val := cache.LookUp("projects/app/logs/server.log"); val != nil {
		meta := val.(*FileMetadata)
		fmt.Printf("Found inode: %d\n", meta.Inode)
	}

	// Read without altering LRU order
	_ = cache.LookUpWithoutChangingOrder("projects/app/logs/server.log")

	// Update existing value payload without modifying LRU order
	newFile := &FileMetadata{Inode: 1001, ByteSize: 4096}
	_ = cache.UpdateWithoutChangingOrder("projects/app/logs/server.log", newFile)

	// Increment size for growing files (evicts LRU items if capacity exceeded)
	_ = cache.UpdateSize("projects/app/logs/server.log", 1024)

	// Subtree Prefix Deletion: instantaneous purging of all logs under projects/app/
	cache.EraseEntriesWithGivenPrefix("projects/app/logs/")

	// Individual Erase
	_ = cache.Erase("projects/app/logs/server.log")
}
```

---

## Unified `Cache` Interface

```go
type Cache interface {
	// Insert inserts or updates a key-value entry in the cache.
	// Returns evicted entries if capacity was exceeded, or an error.
	Insert(key string, value ValueType) ([]ValueType, error)

	// Erase removes the entry associated with key, returning its value (or nil).
	Erase(key string) (value ValueType)

	// LookUp retrieves the value for key and updates its LRU position.
	LookUp(key string) (value ValueType)

	// LookUpWithoutChangingOrder retrieves the value for key without modifying LRU order.
	LookUpWithoutChangingOrder(key string) (value ValueType)

	// UpdateWithoutChangingOrder updates the value of an existing key without modifying LRU order.
	UpdateWithoutChangingOrder(key string, value ValueType) error

	// UpdateSize updates the size of an existing key by sizeDelta and evicts excess if needed.
	UpdateSize(key string, sizeDelta uint64) error

	// EraseEntriesWithGivenPrefix deletes all entries whose keys start with prefix.
	EraseEntriesWithGivenPrefix(prefix string)
}
```

---

## Configuration & Invariant Checking

You can configure internal debugging and data integrity invariants via functional options:

```go
// Create a MapCache with runtime invariant verification enabled
cache := lrus.NewMapCache(1000000, lrus.WithInvariantChecking(true))
```

When `WithInvariantChecking(true)` is enabled:
- **`MapCache`**: Verifies `len(map) == entries.Len()`, total aggregated size matches `currentSize <= maxSize`, and bidirectional linked list pointer bijection.
- **`RadixCache`**: Executes an $O(1)$-space non-recursive tree walk validating pre-order LCRS child/sibling links, LRU linked list consistency, and size accounting.
- **`ArenaRadixCache`**: Validates 32-bit array index references, free-list integrity, hash index 1:1 mapping, and tree-to-LRU bijection.

*Note: Invariant validation is designed for testing/debugging and should be disabled (`false`, the default) in production for maximum throughput.*

---

## Empirical Benchmark Results

Benchmarks executed on an **Intel Xeon CPU @ 2.60GHz (96 cores)**:

### 1. Point Operation Latency & Throughput

| Operation | `MapCache` | `RadixCache` | `ArenaRadixCache` |
| :--- | :--- | :--- | :--- |
| **Lookup (Flat)** | **50.6 ns/op** (0 B, 0 allocs) | 174.0 ns/op (0 B, 0 allocs) | 100.7 ns/op (0 B, 0 allocs) |
| **Lookup (Nested)** | **49.5 ns/op** (0 B, 0 allocs) | 169.6 ns/op (0 B, 0 allocs) | 107.8 ns/op (0 B, 0 allocs) |
| **Lookup (Deeply Nested)** | **64.3 ns/op** (0 B, 0 allocs) | 185.2 ns/op (0 B, 0 allocs) | 194.1 ns/op (0 B, 0 allocs) |
| **Lookup (No Order Change)** | **33.2 ns/op** (0 B, 0 allocs) | 163.3 ns/op (0 B, 0 allocs) | 103.8 ns/op (0 B, 0 allocs) |
| **Insert (Flat)** | **97.7 ns/op** (32 B, 1 alloc) | 178.2 ns/op (0 B, 0 allocs) | 270.6 ns/op (1 B, 0 allocs) |
| **Insert (Nested)** | **98.1 ns/op** (32 B, 1 alloc) | 172.3 ns/op (0 B, 0 allocs) | 270.9 ns/op (1 B, 0 allocs) |
| **Insert (Deeply Nested)** | **119.0 ns/op** (32 B, 1 alloc) | 229.9 ns/op (0 B, 0 allocs) | 471.8 ns/op (2 B, 0 allocs) |
| **Update Value** | **82.5 ns/op** (32 B, 1 alloc) | 97.3 ns/op (0 B, 0 allocs) | 88.6 ns/op (0 B, 0 allocs) |
| **Individual Erase** | 237.9 ns/op (0 B, 0 allocs) | **147.7 ns/op** (0 B, 0 allocs) | 310.6 ns/op (0 B, 0 allocs) |

### 2. Prefix Deletion Latency (Subtree Eviction)

| Prefix Topology | `MapCache` ($O(N)$ Scan) | `RadixCache` ($O(P+S)$ Subtree) | Speedup vs Map |
| :--- | :--- | :--- | :--- |
| **Flat Prefix (100 items)** | 160.6 µs/op | **2.25 µs/op** | **71.3x faster** |
| **Nested Prefix (100 items)** | 151.2 µs/op | **2.30 µs/op** | **65.7x faster** |
| **Deeply Nested (100 items)** | 219.9 µs/op | **2.50 µs/op** | **88.0x faster** |
| **100K Scale (50K purged items)** | 21.6 ms/op | **1.05 ms/op** | **20.5x faster** |

### 3. True Heap Memory Footprint (1,000,000 Keys)

Measured via `runtime.ReadMemStats` with isolated GC sweep (`debug.FreeOSMemory()`):

| Topology (1M Items) | `MapCache` Heap | `RadixCache` Heap | `ArenaRadixCache` Heap | Memory Reduction |
| :--- | :--- | :--- | :--- | :--- |
| **Flat** (`file_%d.txt`) | 129.6 MB (135.9 B/entry) | **89.8 MB** (94.2 B/entry) | 102.0 MB (106.9 B/entry) | **30.7% less memory** |
| **Nested** (`dir_%04d/file_%04d.txt`) | 129.6 MB (135.9 B/entry) | **90.5 MB** (94.9 B/entry) | 101.9 MB (106.9 B/entry) | **30.2% less memory** |
| **Deeply Nested** (`projects/...`) | 129.6 MB (135.9 B/entry) | **90.9 MB** (95.4 B/entry) | 101.9 MB (106.9 B/entry) | **29.9% less memory** |

---

## Testing & Verification

Run the entire test suite including functional tests, parameterized tests, and high-concurrency race stress tests:

```bash
# Run all tests
go test -v ./...

# Run race detector stress tests (0 data races)
go test -v -race -run=TestConcurrency_ ./...

# Run static analysis
go vet ./...

# Run memory footprint profiling
go test -v -run=TestMemoryFootprint_ ./...

# Run full benchmark suite
go test -bench=. -benchmem -run=^$ ./...
```

---

## License

`lrus` is released under the Apache 2.0 License. See [LICENSE](LICENSE) for details.
Originally developed by Google LLC as part of [GoogleCloudPlatform/gcsfuse](https://github.com/GoogleCloudPlatform/gcsfuse).

---

## Disclaimer

This is not an officially supported Google product. This project is not eligible for the [Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security)

