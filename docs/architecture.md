# Architecture & Memory-Pressure Reclamation

This document details the internal data structures, concurrency models, and memory-pressure reclamation mechanics of the three cache engines in `github.com/google/go-lru` (`package lru`).

---

## 1. Architectural Comparison Matrix

| Dimension | `MapCache` (`NewMapCache`) | `RadixCache` (`NewRadixCache`) | `ArenaRadixCache` (`NewArenaRadixCache`) |
| :--- | :--- | :--- | :--- |
| **Primary Data Structure** | `map[string]*entry[V]` + Intrusive Generic Doubly-Linked List (`entryList[V]`) | Compressed Radix Tree (LCRS) + Intrusive Pointer LRU List | Contiguous Slice Arena `[]arenaRadixNode[V]` + `prefixArena []byte` + Freelist + Flat Hash Index (`[]arenaHashSlot`) |
| **Node Representation** | Intrusive `entry[V]` struct (32 B + `sizeof(V)`) + Map Buckets | `radixNode[V]` struct (80 B heap object) | `arenaRadixNode[V]` struct (48 B for 8-byte `V`; 40 B base + `sizeof(V)` contiguous array entry, 100% pointer-free `noscan` for scalar/pointer-free `V`) |
| **Per-Node Heap Allocations** | 1 allocation per new entry during growth (0 on update; capacity turnover in `Put` recycles evicted `*entry[V]` in-place via `recycledEntry`) | 1 allocation per node during growth (capacity turnover in `Put` recycles evicted `*radixNode[V]` in-place via `recycleNodes [2]*radixNode[V]`) | **0 node allocations** (recycled via intrusive free-list; prefixes stored in contiguous `prefixArena []byte` with `freePrefixSpans` reuse) |
| **Node Pointer Width** | 64-bit pointers | 64-bit pointers (5 pointers / node) | **32-bit indices** (`uint32`, sentinel `nilNode = math.MaxUint32`) |
| **Max Entry Capacity** | Memory / Heap limited | Memory / Heap limited | **`2^32 - 2` total arena nodes** (`0..math.MaxUint32-2`; `nilNode = MaxUint32`, `foregroundNoProtect = MaxUint32-1`) |
| **Point Get Latency** | **~50 ns/op** (`O(1)` Hash Map) | ~170 ns/op (`O(K)` Trie Descent) | **~100 ns/op** (`O(1)` Seeded `maphash` Flat Hash Index + Zero-Alloc `keyLen` & Suffix Check) |
| **Sequential Put Latency** | ~98 ns/op in-place (~383 ns/op turnover) | ~175 ns/op in-place (~472 ns/op turnover) | ~270 ns/op in-place (~515 ns/op turnover, 0 node allocs) |
| **Point Replace Latency** | ~71 ns/op (0 allocs) | ~121 ns/op (0 allocs) | ~124 ns/op (0 allocs) |
| **Prefix Delete (100 items)** | ~151.2–219.9 µs (`O(N)` full scan) | **~2.25–2.50 µs** (**65.7x–88.0x faster**) | **~7.3–7.6 µs** (**20.7x–28.9x faster**) |
| **Prefix Delete (50K/100K items)** | ~21.6 ms | **~1.05 ms** (**20.5x faster**) | **~5.1 ms** (**4.2x faster**) |
| **Live Heap Memory** | ~135.9 B/entry at 1M (~163.0 `heap-B/entry` at 100K) | **~94.2 B/entry at 1M** (~96.0 `heap-B/entry` at 100K, **~30.7%–41.1% reduction**) | **~106.9 B/entry at 1M** (~111.2 `heap-B/entry` at 100K, **~21.4%–31.8% reduction**) |
| **GC Pressure & Overhead** | High (millions of distinct heap objects) | Moderate (heap nodes with pointers) | **Ultra-Low** (flat slice; `arenaRadixNode[V]` is 100% pointer-free `noscan` for scalar/pointer-free `V`) |
| **Telemetry & Introspection** | `Stats()` (`0 allocs/op`) + `otellru` (14 instruments) | `Stats()` (`0 allocs/op`) + `otellru` (14 instruments) | `Stats()` (`0 allocs/op`) + `otellru` (**16 instruments**, incl. arena node states & hash fallbacks) |
| **Concurrency Lock** | `sync.RWMutex` | `sync.RWMutex` | `sync.RWMutex` |
| **Best Used For** | Flat keys, maximum read/write throughput | File systems, directory trees, prefix purges | Large-scale hierarchical caches (1M+ items) under strict memory limits |

---

## 2. Engine Data Structure Layouts

### 2.1 `MapCache` (`map_lru.go`)
`MapCache` pairs Go's standard hash map (`map[string]*entry[V]`) with a strongly typed generic intrusive doubly-linked recency list (`entryList[V]` tracking `head, tail *entry[V]`) guarded by a `sync.RWMutex`.
- **Intrusive Doubly-Linked LRU List & In-Place Entry Recycling**: Each `entry[V]` embeds `prev` and `next` pointers directly alongside `key`, `size`, and `value`, eliminating `container/list` `any` interface boxing and cutting per-entry heap allocations during initial growth in half (from 2 allocations to 1 allocation per new entry, and 0 allocations on existing-key updates). During steady-state capacity turnover in `Put`, the first evicted `*entry[V]` node is captured in `recycledEntry` and reused in-place without allocating a new heap struct.
- **Operations**: `Put`, `Get`, `Peek`, `Replace`, and `Delete` execute in `O(1)` time.
- **Prefix Deletion Trade-off**: `DeletePrefix(prefix)` must iterate over all `N` entries in the map (`O(N)`), making it unsuitable for frequent directory-tree purges on large caches.

### 2.2 `RadixCache` (`radix_lru.go`)
`RadixCache` stores keys in a compressed **Left-Child Right-Sibling (LCRS)** radix tree (`radixNode[V]`, 80 bytes per node for 8-byte `V`).
- **Intrusive Doubly-Linked LRU List & Node Recycling**: Each `radixNode[V]` embeds `prev` and `next` pointers alongside tree topology links (`parent`, `child`, `sibling`) and a `hasValue` discriminator, avoiding separate `list.Element` wrapper allocations. While initial growth allocates `*radixNode[V]` per node, capacity turnover in `Put` caches up to 2 detached nodes in `recycleNodes [2]*radixNode[V]` so `insertNode` reuses them in-place without heap allocations.
- **Subtree Detachment**: `DeletePrefix(prefix)` descends `O(P)` characters to the prefix root, detaches the subtree in `O(1)`, and unlinks the `S` descendant value nodes from the intrusive LRU list in `O(S)` (`O(P + S)` total work).
- **Path Compression**: When an internal node loses children after `Delete` or eviction, `compressPathUpwards` merges single-child routing nodes with their parent to keep tree depth minimal.

### 2.3 `ArenaRadixCache` (`arena_radix.go`, `arena_radix_lru.go`)
`ArenaRadixCache` eliminates per-node heap objects and pointer-graph scanning by storing all nodes in a contiguous slice `[]arenaRadixNode[V]` (`48 B for 8-byte V; 40 B base + sizeof(V)`, `56 bytes for 16-byte V`) indexed by 32-bit integers (`uint32`, with `nilNode = math.MaxUint32`). Because edge prefixes live in a contiguous `prefixArena []byte` rather than per-node `string` headers, the `arenaRadixNode[V]` struct itself is 100% pointer-free (`noscan`) for scalar/pointer-free `V`.
- **Contiguous Byte Prefix Arena (`prefixArena []byte`)**:
  - Edge prefixes are stored in `prefixArena []byte` and referenced by each node via `(prefixOff uint32, prefixLen uint16, firstByte uint8, prefixFlags uint8)` at bytes 0..7, detaching both leaf and routing nodes from caller key backing arrays without per-node heap string headers.
  - Freed prefix spans are recycled via a 32-slot span table (`freePrefixSpans [32]prefixSpan`, with adjacent-span coalescing and tail trimming), and prefixes `>= 65,535` bytes store `largePrefixSentinel` (`math.MaxUint16`) in `prefixLen` with a 4-byte little-endian length header at `prefixOff - 4` in `prefixArena`.
- **Intrusive O(1) Free-List & Bit-Packed Node Metadata**:
  - Deleted or evicted node slots are pushed onto an intrusive singly-linked free-list (`freeHead`, linked via `next` index with `parent = nilNode`, `child = nilNode`, `sibling = nilNode`, `prev = nilNode`, `prefixFlags = 0`, `keyLenAndFlags = 0`) and recycled on subsequent insertions with zero node heap allocations.
  - Each node packs `keyLenAndFlags uint32` with `hasValue` (bit 31), the low 14 bits of a 21-bit `hashTag` (bits 17..30, with the upper 7 bits stored in `prefixFlags` bits 1..7 and bit 0 reserved for `prefixFlagPinned`), and a 17-bit saturating full-key byte length `keyLen` (bits 0..16, `nodeKeyLenMask = 131,071`; keys `>= 131,071` bytes store `nodeKeyLenMask` and resolve exact length by ancestor walk). This enables `O(1)` key-length rejection and zero-allocation suffix verification in `verifyKey(nodeID, key)`, fast-path hash slot removal via the 21-bit `hashTag`, zero-allocation stack-buffered `maphash` re-hashing in `hashNodeKey(nodeID)`, and single-pass backward key reconstruction in `reconstructKey(nodeID)`.
- **O(1) Seeded 64-Bit `hash/maphash` Open-Addressed Flat Hash Table (`hashSlots []arenaHashSlot`)**:
  - `hashSlots []arenaHashSlot` is an open-addressed power-of-two flat hash table of 8-byte slots (`tag uint32`, `nodeID uint32`, indexed via `hashMask`) that maps the non-zero 32-bit `arenaHashTag` of the seeded 64-bit `maphash.String(seed, key)` hash (`c.hashKey(key)`) directly to its arena node index, using Robin Hood-style backshift deletion (`backshiftDeleteSlot`) to eliminate tombstones.
  - If two distinct keys collide on the same 64-bit `maphash`, `ArenaRadixCache` preserves full correctness and `O(1)` cache-miss fast-path guarantees (`hashCount + collisionCount == len` when `!nodeMapDirty`) by tracking displaced same-hash peers in `collisionPeers` (`collisionCount`), falling back to `O(K)` trie descent (`getNodeKeyWithHash` / `lookupNodeKeyWithHash`) on hash collisions, and immediately promoting surviving peers into `hashSlots` when a colliding key is deleted or evicted.

### 2.4 Sharded Cache Composition (`sharded_lru.go`)
`NewShardedCache[V]` (and `WithShards(n)` / `WithShardFunc(fn)`) composes `n` (`1 <= n <= maxSize`) independent instances of `MapCache`, `RadixCache`, or `ArenaRadixCache` behind `Cache[V]` and `PressureAwareCache[V]`:
- **Capacity & Tier 2 Retention Partitioning**: Total `maxSize` is split evenly (`maxSize / n`, with `+1` assigned to the first `maxSize % n` shards so `Stats().MaxSize == maxSize`), and the cache-wide Tier 2 retention quota (`computeTargetSize(maxSize, EvictionRetentionRatio)`) is similarly partitioned across shards so critical-pressure shedding matches the configured retention ratio even when `n == maxSize` or `maxSize / n` has fractional retention.
- **Custom & Default Routing (`ShardFunc`) & Single-Hash `arenaShards` Fast-Path**: Keys route via `ShardFunc(key, numShards) int` (defaulting to `DefaultShardFunc`, a zero-allocation seeded 64-bit `hash/maphash` hash with bitmask fast-path on power-of-two shard counts, or custom tenant/prefix routers that preserve subtree locality on `RadixCache` / `ArenaRadixCache`). When `DefaultShardFunc` is paired with `BackendArenaRadix`, `ShardedCache` populates `arenaShards []*arenaRadix[V]` sharing `defaultShardSeed` and performs single-hash routing (`arenaShardAndHash`), computing `maphash.String` once per point operation and forwarding the 64-bit hash directly to the target shard (`getWithHash`, `peekWithHash`, `putWithHash`, `replaceWithHash`, `deleteWithHash`). Out-of-bounds indices panic via a single unsigned check (`uint(idx) >= uint(len(shards))`).
- **Per-Shard Lock Isolation & Cache-Wide Callback/Pressure Coordination**: Point operations (`Get`, `Peek`, `Put`, `Replace`, `Delete`) lock only one shard, while a cache-level coordinator (`c.coord`) unifies active `PressureFunc` re-entrancy tracking, reclamation epochs, latest sampled pressure telemetry, and per-cache `evictCallbackMu` serialization. Multi-shard operations (`DeletePrefix`, `Compact`, `EvaluateMemoryPressure`, `All`/`Keys`/`Values`, `Stats`) visit shards sequentially holding at most one shard's lock at a time, sampling custom `PressureFunc` once per operation and delivering queued eviction callbacks after all shard mutations complete.

---

## 3. Two-Tier Memory-Pressure Reclamation (`PressureAwareCache[V]`, `pressure.go`)

Go's built-in `map` never shrinks its bucket array after deletions, pointer-based trees retain fragmented prefix chains, and `[]arenaRadixNode[V]` retains its peak backing array capacity in the free-list. All three backends (`MapCache`, `RadixCache`, and `ArenaRadixCache`) embed a shared `pressureState` (`pressure.go`) and implement `PressureAwareCache[V]`, providing a unified **two-tier memory-pressure reclamation architecture** configurable via `WithPressureFunc`, `WithMemoryBudget`, `WithCompactionThreshold`, `WithEvictionThreshold`, and `WithEvictionRetentionRatio`.

### 3.1 Lock-Free Amortized Pressure Sampling
- **Built-In Probe (`DefaultRuntimePressureFunc`)**: Reads `/memory/classes/total:bytes`, `/memory/classes/heap/released:bytes`, `/memory/classes/heap/free:bytes`, and `/memory/classes/heap/objects:bytes` from `runtime/metrics` using a `sync.Pool` of `[4]metrics.Sample` arrays (`metricsSamplePool`, `0 allocs/op`, zero Stop-The-World pauses), with an `O(1)` fast-path when `memoryBudget == 0` that reads `/gc/gomemlimit:bytes` via a pooled `[1]metrics.Sample` array (`gomemlimitSamplePool`) and returns `0.0` immediately when `GOMEMLIMIT` is unbounded without acquiring `worldsema` or sweeping per-P heap stats.
- **Amortized Window (`samplePressureWithEpoch`)**: When using the built-in `DefaultRuntimePressureFunc`, foreground write operations (`Put`, `Replace`, `Delete`, `DeletePrefix`) sample the runtime metrics probe once every 256 writes (`seq&255 == 1`) or immediately following a reclamation epoch (`pressureNeedsRefresh`), whereas custom `WithPressureFunc` callbacks are sampled on every foreground write and `EvaluateMemoryPressure()` call.
- **Re-Entrancy Guard**: Lock-free multi-tag primary sampler tracking (`samplingPressure`, `fastPrimaryTagOwner`, `fallbackSampling`, `overflowSamplingGIDs`) with lazy contended stack resolution (`resolvePrimaryFromAllStacksLocked` and `findAncestorCreatorAboveHook`) ensures zero `runtime.Stack()` overhead on uncontended custom `PressureFunc` writes while guaranteeing that even if a user-supplied `PressureFunc` (or a child goroutine spawned inside it) re-entrantly calls methods on the same `Cache`, it will never deadlock or recurse infinitely.

### 3.2 Tier 1 — Moderate Pressure (`pressure >= CompactionThreshold`, default `0.75`)
When normalized memory pressure reaches `CompactionThreshold`:
- Triggered automatically on foreground writes whenever backing-structure slack or post-peak entry deletion exceeds 25%, or unconditionally via `Compact()` / `EvaluateMemoryPressure()`.
- `MapCache` reallocates its `map[string]*entry[V]` index (`make(map[string]*entry[V], len(c.index))` + `maps.Copy`) when `dirtyIndex` is true to reclaim Go map bucket slack; `RadixCache` eagerly clones prefixes at insertion/split time and zeroes detached nodes upon deletion while resetting compaction watermarks (`onCompacted`); `ArenaRadixCache` allocates a fresh, tightly-sized `newNodes` slice (`len == cap == liveCount`), remaps all 8 `uint32` index fields via `oldToNew`, resets `freeHead = nilNode`, compacts `prefixArena` to exact live byte capacity via `compactPrefixArenaExactLocked()`, and rebuilds `hashSlots` (`make([]arenaHashSlot, hashSlotsCapFor(c.len))`) via `rebuildHashSlotsLocked()`.

### 3.3 Tier 2 — Critical Pressure (`pressure >= EvictionThreshold`, default `0.90`)
When normalized memory pressure reaches `EvictionThreshold`:
- `shedAndCompactLocked()` proactively evicts least-recently-used (`tail`) entries until `currentSize <= maxSize * EvictionRetentionRatio` (default `50%` of `maxSize`), and also proportionally sheds zero-size entries (`size == 0`) down to `EvictionRetentionRatio` (bounded by `targetLen` and `lastReclaimedZeroCount` / `lastReclaimedLen` watermarks so repeated evaluations at sustained critical pressure remain idempotent).
- Foreground `Put` and `Replace` operations pass the inserted, overwritten, or replaced entry as a protected reference so the entry currently being mutated is never self-evicted during inline pressure shedding (regardless of whether a `Replace` target resides at the MRU head or deeper in the LRU list).
- After shedding LRU entries, `compactDataStructuresLocked()` executes immediately to return both the evicted entries' backing structures and any prior map/tree/arena slack to the Go runtime heap.

---

## 4. O(1)-Space Iterative Invariant Verification

Enabling `WithInvariantChecking(true)` validates structural integrity across every cache operation:
1. **Capacity Bounds**: `maxSize > 0` and `currentSize <= maxSize`.
2. **Iterative Tree Walk**: Uses parent/sibling pointers (`radixCache`) or `uint32` index links (`ArenaRadixCache`) to traverse the entire LCRS tree in `O(1)` auxiliary space without recursion, verifying parent-child symmetry and absence of cycles.
3. **LRU List Bijection**: Verifies that every value-bearing tree node appears in the doubly-linked `head`/`tail` list with matching forward and reverse traversal counts and exact `currentSize` sum parity.
4. **Free-List & Hash Index Integrity (`ArenaRadixCache`)**: Confirms `len(nodes) - freeCount == treeNodeCount`, verifies zero overlap between free-list slots and live tree nodes, and checks `hashSlots []arenaHashSlot` index bounds, `arenaHashTag` consistency, `collisionPeers` parity, and `hashCount + collisionCount <= len`.
5. **Telemetry & Watermark Parity**: Verifies `deletedSinceCompact >= 0`, `peakEntryLen >= len`, and counter-to-entry-balance parity (`PutInserted - EvictionsCapacity - EvictionsPressure - EvictionsDeleted == len`, `EvictionsDeleted >= DeleteDeleted`, `EvictionsReplaced == PutUpdated + ReplaceUpdated`).

---

## 5. Eviction Callbacks, Range-Over-Function Iterators & Zero-Overhead Key Reconstruction

All three engines support synchronous entry removal/replacement callbacks via `WithOnEvictValue[V any]` (`func(value V, reason EvictionReason)`) and `WithOnEvictEntry[V any]` (`func(key string, value V, reason EvictionReason)`), as well as Go 1.23+ range-over-function iterators (`All() iter.Seq2[string, V]`, `Keys() iter.Seq[string]`, and `Values() iter.Seq[V]`):
- **Four Removal Causes (`EvictionReason`)**: `EvictionReasonCapacity` (LRU eviction on `Put`/`Replace`, or `Replace` self-eviction when the updated weight exceeds `maxSize` or cannot fit alongside newer entries), `EvictionReasonPressure` (Tier 2 critical memory-pressure shedding), `EvictionReasonDeleted` (`Delete` or `DeletePrefix`), and `EvictionReasonReplaced` (in-place value overwrite on `Put` or `Replace`). Eviction events are buffered on a stack-backed `evictCallbackQueue[V]` and delivered synchronously **after** `c.mu.Unlock()` (serialized per cache via `evictCallbackMu`, safely supporting re-entrant calls into the same or other `Cache` instances from the callback goroutine or awaited child goroutines — whereas detached goroutines that outlive the callback serialize as independent callers — and ensuring callback panics never corrupt cache state or leak mutex locks). When both callbacks are configured, `OnEvictValue` is invoked before `OnEvictEntry`. Capacity and pressure evictions fire in strict LRU-to-MRU order; `DeletePrefix("")` fires in MRU-to-LRU order across all three backends, and non-empty `DeletePrefix(prefix)` invokes callbacks once per removed entry (in lexicographical pre-order DFS order on `RadixCache` and `ArenaRadixCache`).
- **Range-Over-Function Iterators (`All`, `Keys`, `Values`)**:
  - `All() iter.Seq2[string, V]`, `Keys() iter.Seq[string]`, and `Values() iter.Seq[V]` acquire the cache's read lock (`RLock`, released via `defer rUnlock()`) and traverse the intrusive doubly-linked LRU list from `head` (MRU) to `tail` (LRU) without altering entry recency order, supporting early termination (`break`).
  - `Values()` delegates to an unexported method (`return c.values`) and skips key reconstruction completely on `RadixCache` and `ArenaRadixCache`, executing with **0 heap allocations (`0 allocs/op`)** across all three backends.
- **Zero Key-Reconstruction Overhead**:
  - Because `RadixCache` and `ArenaRadixCache` store compressed prefix segments across tree ancestors rather than full key strings per node, `WithOnEvictValue` and `Values()` never reconstruct keys (`0` string allocations).
  - Even when `WithOnEvictEntry` is configured, operations that already have `key` in hand (`Delete(key)`, `Put(key, ...)` overwrite, `Replace(key, ...)` overwrite, and `Replace` self-eviction) forward `key` directly without walking the tree (and `MapCache` always passes the `clonePrefix`-cloned `entry.key`).
  - When `WithOnEvictEntry`, `All()`, or `Keys()` needs to materialize a node's full key on `RadixCache` or `ArenaRadixCache`, `reconstructKey` returns single-segment keys (`node.parent == root`) in `0` allocations; for deeper nodes, `RadixCache` performs a two-pass upward parent walk that sums `len(curr.prefix)`, allocates `make([]byte, totalLen)`, copies ancestor prefixes backward, and converts to `string` via `unsafe.String` (`1` allocation at any tree depth), while `ArenaRadixCache` pre-sizes a `make([]byte, node.keyLen())` buffer (or sums `c.nodePrefixLen(curr)` when `keyLen` saturates at `nodeKeyLenMask`), copies ancestor prefixes backward in a single upward walk (`fillNodeKeyBuf`), and converts to `string` via `unsafe.String` (`1` allocation at any tree depth).

---

## 6. Zero-Allocation Telemetry (`Stats()`) & Asynchronous OpenTelemetry Instrumentation (`otellru`)

### 6.1 Hybrid Synchronization in `pressureState` (`Stats()`)
Every cache instance embeds telemetry counters inside `pressureState` (`pressure.go`) and exposes a value-type `Stats() Stats` snapshot on `Cache[V]` (`0 allocs/op`):
- **Write-Locked Scalar Counters**: Operations that already hold the cache's exclusive write lock (`Get`, `Put` insertions/updates, `Replace`, `Delete`, `DeletePrefix`, eviction counts/weights across all four `EvictionReason` values, compaction trigger counters, and Tier 2 pressure shed counters) increment plain `uint64` fields with zero atomic bus-lock overhead.
- **`atomic.Uint64` Counters for `RLock` & Pre-Lock Paths**: Operations that execute under `RLock` (`Peek` hits/misses and `ArenaRadixCache` `maphash` collision/displacement trie fallbacks in `lookupNodeKeyWithHash`), pre-lock oversized rejections (`PutRejectedOversized`), and lock-free pressure probes (`lastSampledPressureBits`) use `atomic.Uint64` so `Peek` never upgrades to a write lock.
- **Compaction & Shed Trigger Classification**:
  - Compactions are attributed to `CompactionsExplicit` (`Compact()`), `CompactionsPressureTier1` (Tier 1 moderate pressure), `CompactionsPressureTier2` (Tier 2 critical pressure), or `CompactionsAutoSlack` (normal-pressure $\ge 25\%$ post-peak shrinkage or `ArenaRadixCache` 64-free-node churn compaction).
  - Tier 2 critical-pressure shedding events (incremented when $\ge 1$ entry is evicted due to pressure) are attributed to `PressureShedsInline` (triggered inline during foreground `Put`, `Replace`, `Delete`, or `DeletePrefix`) or `PressureShedsExplicit` (triggered via `EvaluateMemoryPressure()`).

### 6.2 Purely Asynchronous OpenTelemetry Submodule (`otellru`)
`github.com/google/go-lru/otellru` isolates OpenTelemetry dependencies (`go.opentelemetry.io/otel/metric`) in a separate `otellru/go.mod` submodule:
- **Single Batch Callback**: `otellru.Register(cache, opts...)` registers a single `Meter.RegisterCallback` closure that calls `cache.Stats()` once per collection cycle and records all 14 general instruments (plus 2 `ArenaRadixCache`-only instruments: `lru.cache.arena.nodes` and `lru.cache.arena.hash_fallbacks`).
- **Pre-Allocated `attribute.Set` & `[]metric.ObserveOption` Slices**: At `Register()` time, `otellru` pre-computes every immutable `attribute.Set` (combining `cache.backend`, optional `cache.name`, user-supplied `WithAttributes`, and metric-specific attributes `operation`, `result`, `reason`, `outcome`, `trigger`, `state`) and wraps each in a 1-element `[]metric.ObserveOption{metric.WithAttributeSet(set)}` slice. Unpacking these slices (`opts...`) inside the scrape callback achieves **`0 allocs/op` per collection cycle**.

