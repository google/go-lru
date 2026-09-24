# Architecture & Memory-Pressure Reclamation

This document details the internal data structures, concurrency models, and memory-pressure reclamation mechanics of the three cache engines in `github.com/google/go-lru` (`package lru`).

---

## 1. Architectural Comparison Matrix

| Dimension | `MapCache` (`NewMapCache`) | `RadixCache` (`NewRadixCache`) | `ArenaRadixCache` (`NewArenaRadixCache`) |
| :--- | :--- | :--- | :--- |
| **Primary Data Structure** | `map[string]*list.Element` + `container/list.List` | Compressed Radix Tree (LCRS) + Intrusive Pointer LRU List | Contiguous Slice Arena `[]arenaRadixNode` + Freelist + Hash Index |
| **Node Representation** | `list.Element` (48 B) + `entry` (32 B) + Map Buckets | `radixNode` struct (72 B heap object) | `arenaRadixNode` struct (56 B contiguous array entry) |
| **Per-Node Heap Allocations** | 2–3 allocations per entry | 1 allocation per node | **0 allocations** (recycled via intrusive free-list) |
| **Node Pointer Width** | 64-bit pointers | 64-bit pointers (5 pointers / node) | **32-bit indices** (`uint32`, sentinel `nilNode = math.MaxUint32`) |
| **Max Entry Capacity** | Memory / Heap limited | Memory / Heap limited | **`2^32 - 1` entries** (~4.29 billion nodes) |
| **Point Lookup Latency** | **~50 ns/op** (`O(1)` Hash Map) | ~170 ns/op (`O(K)` Trie Descent) | **~100 ns/op** (`O(1)` FNV-1a Hash Map + Zero-Alloc Path Check) |
| **Sequential Insert Latency** | ~98 ns/op | ~175 ns/op (0 allocs) | ~270 ns/op |
| **Point Update Latency** | ~82 ns/op | ~97 ns/op (0 allocs) | ~88 ns/op (0 allocs) |
| **Prefix Erase (100 items)** | ~160 µs (`O(N)` full scan) | **~2.2 µs** (**71x faster**) | **~27.6 µs** (**6x faster**) |
| **Prefix Erase (50K/100K items)** | ~21.6 ms | **~1.05 ms** (**20.5x faster**) | ~13.6 ms |
| **1M Items Heap Memory** | ~129.6 MB (~135.9 B/entry) | **~89.8 MB** (**~30.7% reduction**) | **~101.9 MB** (**~21.4% reduction**) |
| **GC Pressure & Overhead** | High (millions of distinct heap objects) | Moderate (heap nodes with pointers) | **Ultra-Low** (flat slice; internal tree indices invisible to GC) |
| **Concurrency Lock** | `sync.RWMutex` | `sync.RWMutex` | `sync.Mutex` |
| **Best Used For** | Flat keys, maximum read/write throughput | File systems, directory trees, prefix purges | Large-scale hierarchical caches (1M+ items) under strict memory limits |

---

## 2. Engine Data Structure Layouts

### 2.1 `MapCache` (`map_lru.go`)
`MapCache` pairs Go's standard hash map (`map[string]*list.Element`) with a doubly-linked recency list (`container/list.List`) guarded by a `sync.RWMutex`.
- **Operations**: `Insert`, `LookUp`, `LookUpWithoutChangingOrder`, `UpdateWithoutChangingOrder`, `UpdateSize`, and `Erase` execute in `O(1)` time.
- **Prefix Eviction Trade-off**: `EraseEntriesWithGivenPrefix(prefix)` must iterate over all `N` entries in the map (`O(N)`), making it unsuitable for frequent directory-tree purges on large caches.

### 2.2 `RadixCache` (`radix_lru.go`)
`RadixCache` stores keys in a compressed **Left-Child Right-Sibling (LCRS)** radix tree (`radixNode`, 72 bytes per node).
- **Intrusive Doubly-Linked LRU List**: Each `radixNode` embeds `prev` and `next` pointers alongside tree topology links (`parent`, `child`, `sibling`), avoiding separate `list.Element` wrapper allocations.
- **Subtree Detachment**: `EraseEntriesWithGivenPrefix(prefix)` descends `O(P)` characters to the prefix root, detaches the subtree in `O(1)`, and unlinks the `S` descendant value nodes from the intrusive LRU list in `O(S)` (`O(P + S)` total work).
- **Path Compression**: When an internal node loses children after `Erase` or eviction, `compressPathUpwards` merges single-child routing nodes with their parent to keep tree depth minimal.

### 2.3 `ArenaRadixCache` (`arena_radix.go`, `arena_radix_lru.go`)
`ArenaRadixCache` eliminates per-node heap objects and pointer-graph scanning by storing all nodes in a contiguous slice `[]arenaRadixNode` (`56 bytes` per node) indexed by 32-bit integers (`uint32`, with `nilNode = math.MaxUint32`).
- **Intrusive O(1) Free-List**: Erased or evicted node slots are pushed onto an intrusive singly-linked free-list (`freeHead`, linked via `next` index with `parent = nilNode`, `child = nilNode`, `sibling = nilNode`, `prev = nilNode`) and recycled on subsequent insertions with zero heap allocations.
- **O(1) 64-Bit FNV-1a Lookup Accelerator**:
  - `nodeMap map[uint64]uint32` maps the 64-bit FNV-1a hash of full keys directly to their arena node index.
  - `verifyKey(nodeID, key)` walks `parent` indices from `nodeID` up to `root`, verifying byte-for-byte suffix equality against `key` in zero allocations.
  - If two distinct keys collide on the same 64-bit FNV-1a hash, `ArenaRadixCache` preserves full correctness by falling back to `O(K)` trie descent (`findNode`) and re-indexing surviving colliding keys (`reindexCollidingKey`) upon deletion or compaction.

---

## 3. Two-Tier Memory-Pressure Reclamation (`PressureAwareCache`)

Go's built-in `map` never shrinks its bucket array after deletions, and `[]arenaRadixNode` retains its peak backing array capacity in the free-list. `ArenaRadixCache` addresses this via a **two-tier memory-pressure reclamation architecture** configurable via `WithPressureFunc`, `WithMemoryBudget`, `WithCompactionThreshold`, `WithEvictionThreshold`, and `WithEvictionRetentionRatio`.

### 3.1 Lock-Free Amortized Pressure Sampling
- **Built-In Probe (`DefaultRuntimePressureFunc`)**: Reads `/memory/classes/total:bytes`, `/memory/classes/heap/released:bytes`, `/memory/classes/heap/free:bytes`, `/memory/classes/heap/objects:bytes`, and `/gc/gomemlimit:bytes` from `runtime/metrics` using a `sync.Pool` of `[5]metrics.Sample` arrays (`0 allocs/op`, zero Stop-The-World pauses).
- **Amortized Window (`samplePressure`)**: Foreground write operations (`Insert`, `UpdateSize`, `Erase`, `EraseEntriesWithGivenPrefix`) sample the pressure probe once every 256 writes (`seq&255 == 1`) or immediately following a reclamation epoch (`pressureNeedsRefresh`).
- **Re-Entrancy Guard**: An atomic compare-and-swap flag (`samplingPressure`) ensures that even if a user-supplied `PressureFunc` re-entrantly calls methods on the same `Cache`, it will never deadlock or recurse infinitely.

### 3.2 Tier 1 — Moderate Pressure (`pressure >= CompactionThreshold`, default `0.75`)
When normalized memory pressure reaches `CompactionThreshold`:
- Triggered automatically on foreground writes whenever arena slack exceeds 25% (`freeCount*4 >= len(nodes)`), or unconditionally via `Compact()` / `EvaluateMemoryPressure()`.
- `compactLocked()` allocates a fresh, tightly-sized `newNodes` slice (`len == cap == liveCount`), copies only live nodes, remaps all 8 `uint32` index references (`root`, `head`, `tail`, `parent`, `child`, `sibling`, `prev`, `next`) via an `oldToNew` translation table, resets `freeHead = nilNode`, and rebuilds `nodeMap` from scratch to release Go hash-map bucket slack.

### 3.3 Tier 2 — Critical Pressure (`pressure >= EvictionThreshold`, default `0.90`)
When normalized memory pressure reaches `EvictionThreshold`:
- `shedAndCompactLocked()` proactively evicts least-recently-used (`tail`) entries until `currentSize <= maxSize * EvictionRetentionRatio` (default `50%` of `maxSize`).
- Foreground insertions and updates pass a `protectedNodeID` so the entry currently being inserted or resized is never self-evicted during inline pressure shedding.
- After shedding LRU entries, `compactLocked()` executes immediately to return both the evicted entries' nodes and any prior arena/map slack to the Go runtime heap.

---

## 4. O(1)-Space Iterative Invariant Verification

Enabling `WithInvariantChecking(true)` validates structural integrity across every cache operation:
1. **Capacity Bounds**: `maxSize > 0` and `currentSize <= maxSize`.
2. **Iterative Tree Walk**: Uses parent/sibling pointers (`radixCache`) or `uint32` index links (`ArenaRadixCache`) to traverse the entire LCRS tree in `O(1)` auxiliary space without recursion, verifying parent-child symmetry and absence of cycles.
3. **LRU List Bijection**: Verifies that every value-bearing tree node appears in the doubly-linked `head`/`tail` list with matching forward and reverse traversal counts and exact `currentSize` sum parity.
4. **Free-List & Hash Index Integrity (`ArenaRadixCache`)**: Confirms `len(nodes) - freeCount == treeNodeCount`, verifies zero overlap between free-list slots and live tree nodes, and checks `nodeMap` index consistency.

<!-- 333d5 -->
