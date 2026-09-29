# Architecture & Memory-Pressure Reclamation

This document details the internal data structures, concurrency models, and memory-pressure reclamation mechanics of the three cache engines in `github.com/google/go-lru` (`package lru`).

---

## 1. Architectural Comparison Matrix

| Dimension | `MapCache` (`NewMapCache`) | `RadixCache` (`NewRadixCache`) | `ArenaRadixCache` (`NewArenaRadixCache`) |
| :--- | :--- | :--- | :--- |
| **Primary Data Structure** | `map[string]*list.Element` + `container/list.List` | Compressed Radix Tree (LCRS) + Intrusive Pointer LRU List | Contiguous Slice Arena `[]arenaRadixNode` + Freelist + Hash Index |
| **Node Representation** | `list.Element` (48 B) + `entry` (40 B) + Map Buckets | `radixNode` struct (80 B heap object) | `arenaRadixNode` struct (64 B contiguous array entry) |
| **Per-Node Heap Allocations** | 2 allocations per entry (0 on update) | 1 allocation per node | **0 allocations** (recycled via intrusive free-list) |
| **Node Pointer Width** | 64-bit pointers | 64-bit pointers (5 pointers / node) | **32-bit indices** (`uint32`, sentinel `nilNode = math.MaxUint32`) |
| **Max Entry Capacity** | Memory / Heap limited | Memory / Heap limited | **`2^32 - 2` total arena nodes** (`0..math.MaxUint32-2`; `nilNode = MaxUint32`, `foregroundNoProtect = MaxUint32-1`) |
| **Point Lookup Latency** | **~50 ns/op** (`O(1)` Hash Map) | ~170 ns/op (`O(K)` Trie Descent) | **~100 ns/op** (`O(1)` FNV-1a Hash Map + Zero-Alloc Path Check) |
| **Sequential Insert Latency** | ~98 ns/op in-place (~383 ns/op turnover) | ~175 ns/op in-place (~472 ns/op turnover) | ~270 ns/op in-place (~515 ns/op turnover, 0 node allocs) |
| **Point Update Latency** | ~82 ns/op (0 allocs) | ~97 ns/op (0 allocs) | ~88 ns/op (0 allocs) |
| **Prefix Erase (100 items)** | ~151.2–219.9 µs (`O(N)` full scan) | **~2.25–2.50 µs** (**65.7x–88.0x faster**) | **~7.3–7.6 µs** (**20.7x–28.9x faster**) |
| **Prefix Erase (50K/100K items)** | ~21.6 ms | **~1.05 ms** (**20.5x faster**) | **~5.1 ms** (**4.2x faster**) |
| **Live Heap Memory** | ~135.9 B/entry at 1M (~163.0 `heap-B/entry` at 100K) | **~94.2 B/entry at 1M** (~96.0 `heap-B/entry` at 100K, **~30.7%–41.1% reduction**) | **~106.9 B/entry at 1M** (~111.2 `heap-B/entry` at 100K, **~21.4%–31.8% reduction**) |
| **GC Pressure & Overhead** | High (millions of distinct heap objects) | Moderate (heap nodes with pointers) | **Ultra-Low** (flat slice; internal tree indices invisible to GC) |
| **Concurrency Lock** | `sync.RWMutex` | `sync.RWMutex` | `sync.RWMutex` |
| **Best Used For** | Flat keys, maximum read/write throughput | File systems, directory trees, prefix purges | Large-scale hierarchical caches (1M+ items) under strict memory limits |

---

## 2. Engine Data Structure Layouts

### 2.1 `MapCache` (`map_lru.go`)
`MapCache` pairs Go's standard hash map (`map[string]*list.Element`) with a doubly-linked recency list (`container/list.List`) guarded by a `sync.RWMutex`.
- **Operations**: `Insert`, `LookUp`, `LookUpWithoutChangingOrder`, `UpdateWithoutChangingOrder`, `UpdateSize`, and `Erase` execute in `O(1)` time.
- **Prefix Eviction Trade-off**: `EraseEntriesWithGivenPrefix(prefix)` must iterate over all `N` entries in the map (`O(N)`), making it unsuitable for frequent directory-tree purges on large caches.

### 2.2 `RadixCache` (`radix_lru.go`)
`RadixCache` stores keys in a compressed **Left-Child Right-Sibling (LCRS)** radix tree (`radixNode`, 80 bytes per node).
- **Intrusive Doubly-Linked LRU List**: Each `radixNode` embeds `prev` and `next` pointers alongside tree topology links (`parent`, `child`, `sibling`), avoiding separate `list.Element` wrapper allocations.
- **Subtree Detachment**: `EraseEntriesWithGivenPrefix(prefix)` descends `O(P)` characters to the prefix root, detaches the subtree in `O(1)`, and unlinks the `S` descendant value nodes from the intrusive LRU list in `O(S)` (`O(P + S)` total work).
- **Path Compression**: When an internal node loses children after `Erase` or eviction, `compressPathUpwards` merges single-child routing nodes with their parent to keep tree depth minimal.

### 2.3 `ArenaRadixCache` (`arena_radix.go`, `arena_radix_lru.go`)
`ArenaRadixCache` eliminates per-node heap objects and pointer-graph scanning by storing all nodes in a contiguous slice `[]arenaRadixNode` (`64 bytes` per node) indexed by 32-bit integers (`uint32`, with `nilNode = math.MaxUint32`).
- **Intrusive O(1) Free-List**: Erased or evicted node slots are pushed onto an intrusive singly-linked free-list (`freeHead`, linked via `next` index with `parent = nilNode`, `child = nilNode`, `sibling = nilNode`, `prev = nilNode`) and recycled on subsequent insertions with zero heap allocations.
- **O(1) 64-Bit FNV-1a Lookup Accelerator**:
  - `nodeMap map[uint64]uint32` maps the 64-bit FNV-1a hash of full keys directly to their arena node index.
  - `verifyKey(nodeID, key)` walks `parent` indices from `nodeID` up to `root`, verifying byte-for-byte suffix equality against `key` in zero allocations.
  - If two distinct keys collide on the same 64-bit FNV-1a hash, `ArenaRadixCache` preserves full correctness by falling back to `O(K)` trie descent (`getNodeKeyWithHash`) and healing `nodeMap` entries on write-locked lookups (`LookUp`, `Insert`, `UpdateWithoutChangingOrder`, `UpdateSize`) or `compactLocked()`.

---

## 3. Two-Tier Memory-Pressure Reclamation (`PressureAwareCache`, `pressure.go`)

Go's built-in `map` never shrinks its bucket array after deletions, pointer-based trees retain fragmented prefix chains, and `[]arenaRadixNode` retains its peak backing array capacity in the free-list. All three backends (`MapCache`, `RadixCache`, and `ArenaRadixCache`) embed a shared `pressureState` (`pressure.go`) and implement `PressureAwareCache`, providing a unified **two-tier memory-pressure reclamation architecture** configurable via `WithPressureFunc`, `WithMemoryBudget`, `WithCompactionThreshold`, `WithEvictionThreshold`, and `WithEvictionRetentionRatio`.

### 3.1 Lock-Free Amortized Pressure Sampling
- **Built-In Probe (`DefaultRuntimePressureFunc`)**: Reads `/memory/classes/total:bytes`, `/memory/classes/heap/released:bytes`, `/memory/classes/heap/free:bytes`, and `/memory/classes/heap/objects:bytes` from `runtime/metrics` using a `sync.Pool` of `[4]metrics.Sample` arrays (`metricsSamplePool`, `0 allocs/op`, zero Stop-The-World pauses), with an `O(1)` fast-path when `memoryBudget == 0` that reads `/gc/gomemlimit:bytes` via a pooled `[1]metrics.Sample` array (`gomemlimitSamplePool`) and returns `0.0` immediately when `GOMEMLIMIT` is unbounded without acquiring `worldsema` or sweeping per-P heap stats.
- **Amortized Window (`samplePressureWithEpoch`)**: When using the built-in `DefaultRuntimePressureFunc`, foreground write operations (`Insert`, `UpdateSize`, `Erase`, `EraseEntriesWithGivenPrefix`) sample the runtime metrics probe once every 256 writes (`seq&255 == 1`) or immediately following a reclamation epoch (`pressureNeedsRefresh`), whereas custom `WithPressureFunc` callbacks are sampled on every foreground write and `EvaluateMemoryPressure()` call.
- **Re-Entrancy Guard**: Atomic sampler tracking (`samplingPressure`, `fallbackSampling`, `overflowSamplingGIDs`) ensures that even if a user-supplied `PressureFunc` re-entrantly calls methods on the same `Cache`, it will never deadlock or recurse infinitely.

### 3.2 Tier 1 — Moderate Pressure (`pressure >= CompactionThreshold`, default `0.75`)
When normalized memory pressure reaches `CompactionThreshold`:
- Triggered automatically on foreground writes whenever backing-structure slack or post-peak entry deletion exceeds 25%, or unconditionally via `Compact()` / `EvaluateMemoryPressure()`.
- `MapCache` reallocates its `map[string]*list.Element` index (`make(map[string]*list.Element, len(c.index))` + `maps.Copy`) when `dirtyIndex` is true to reclaim Go map bucket slack; `RadixCache` eagerly clones prefixes at insertion/split time and zeroes detached nodes upon deletion while resetting compaction watermarks (`onCompacted`); `ArenaRadixCache` allocates a fresh, tightly-sized `newNodes` slice (`len == cap == liveCount`), remaps all 8 `uint32` index fields via `oldToNew`, resets `freeHead = nilNode`, and rebuilds `nodeMap` (`make(map[uint64]uint32, c.len)`) from scratch to release Go hash-map bucket slack.

### 3.3 Tier 2 — Critical Pressure (`pressure >= EvictionThreshold`, default `0.90`)
When normalized memory pressure reaches `EvictionThreshold`:
- `shedAndCompactLocked()` proactively evicts least-recently-used (`tail`) entries until `currentSize <= maxSize * EvictionRetentionRatio` (default `50%` of `maxSize`), and also proportionally sheds zero-size entries (`size == 0`) down to `EvictionRetentionRatio` (bounded by `targetLen` and `lastReclaimedZeroCount` / `lastReclaimedLen` watermarks so repeated evaluations at sustained critical pressure remain idempotent).
- Foreground insertions pass the MRU head entry as a protected reference so the entry currently being inserted or overwritten is never self-evicted during inline pressure shedding, while foreground size updates (`UpdateSize`) pass an unprotected sentinel (`nil` / `foregroundNoProtect`) and protect the MRU head entry when `tail == head`.
- After shedding LRU entries, `compactDataStructuresLocked()` executes immediately to return both the evicted entries' backing structures and any prior map/tree/arena slack to the Go runtime heap.

---

## 4. O(1)-Space Iterative Invariant Verification

Enabling `WithInvariantChecking(true)` validates structural integrity across every cache operation:
1. **Capacity Bounds**: `maxSize > 0` and `currentSize <= maxSize`.
2. **Iterative Tree Walk**: Uses parent/sibling pointers (`radixCache`) or `uint32` index links (`ArenaRadixCache`) to traverse the entire LCRS tree in `O(1)` auxiliary space without recursion, verifying parent-child symmetry and absence of cycles.
3. **LRU List Bijection**: Verifies that every value-bearing tree node appears in the doubly-linked `head`/`tail` list with matching forward and reverse traversal counts and exact `currentSize` sum parity.
4. **Free-List & Hash Index Integrity (`ArenaRadixCache`)**: Confirms `len(nodes) - freeCount == treeNodeCount`, verifies zero overlap between free-list slots and live tree nodes, and checks `nodeMap` index consistency.
