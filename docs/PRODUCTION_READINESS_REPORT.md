# `go-lru` Production Readiness & Gap Analysis Report

**Repository**: `github.com/google/go-lru` (`/usr/local/google/home/kislayk/gitproj/go-lru2`)  
**Submodules Evaluated**: Root package `lru` (`cache.go`, `map_lru.go`, `radix_lru.go`, `arena_radix.go`, `arena_radix_lru.go`, `pressure.go`, `options.go`, `errors.go`) and `otellru` (`otellru/otellru.go`)  
**Verification Suite**: 15 `*_test.go` files (12 baseline + 3 newly authored adversarial/benchmark suites: `adversarial_production_test.go`, `production_benchmarks_test.go`, `otellru/adversarial_otel_test.go`)  
**Hardware / Runtime Environment**: `linux/amd64`, Intel(R) Xeon(R) CPU @ 2.60GHz (96 logical CPUs), Go 1.26 (`go test -race ./...`)

---

## 1. Executive Summary & Production Readiness Verdict

### 1.1 Overall Verdict: **GO**

`go-lru` is a generic (`Cache[V any]`), zero-external-dependency in-memory LRU cache library featuring three interchangeable storage engines (`MapCache`, `RadixCache`, `ArenaRadixCache`), two-tier memory pressure management (`pressure.go`), and zero-allocation OpenTelemetry metric callbacks (`otellru`).

Following full remediation of all **Bucket 1 real bugs** (`C-02`, `H-01`, `H-03`, `H-06`, `M-01..M-04`, `L-04`) and **Bucket 2 sharp edges** (`C-01`, `C-03`, `H-05`), `go-lru` and `otellru` receive an unrestricted **GO** production readiness verdict for single-shard, in-memory LRU caching workloads:

1. **100% Verification & Strict-Assertion Pass Rate**: All unit, differential, fuzz, adversarial, and empirical defect verification suites pass 100% with zero failures and zero data races under `go test -race -count=1 ./...` across `.` and `./otellru`, with `0` `golangci-lint` issues.
2. **Zero-Allocation Hot Paths Preserved**: All three backends maintain **zero heap allocations (`0 B/op`, `0 allocs/op`)** on steady-state `Get`, `Peek`, in-place `Put`, `Replace`, `Values()`, `Stats()`, custom `WithPressureFunc` writes, and `otellru` metric scrape callbacks.
3. **All Critical & High Defects Remediated**:
   - **Remediated (`C-01`) — Bounded Zero-Weight (`Weigher == 0`) Entry Eviction**: Inserting zero-weight entries into a cache at full capacity (`c.currentSize >= c.maxSize && uint64(c.len) >= c.maxSize`) or when `uint64(c.zeroSizeCount) >= c.maxSize` now evicts at most 1 LRU tail entry per insert (`shouldEvictZeroWeightOnInsert` gated by `!evictedPrePut` in `map_lru.go`, `radix_lru.go`, `arena_radix_lru.go`), bounding entry count by `maxSize` without wiping out positive-weight tail entries.
   - **Remediated (`C-02` & `C-03a`) — Post-Unlock Eviction Callback Queue & Deferred Mutex Release**: All mutating methods queue evicted `(key, value, reason)` entries into a stack-backed `evictCallbackQueue[V]` (`buf [2]evictEvent[V]` with `0 allocs/op` on steady-state writes) and invoke `OnEvictValue` and `OnEvictEntry` via `defer evictQ.invoke(...)` **after** `c.unlock()` releases `c.mu.Lock()`. Inside `unlock()` and `rUnlock()`, `defer c.mu.Unlock()` / `defer c.mu.RUnlock()` is registered before `c.checkInvariants()`. Panicking callbacks never corrupt cache state or leak mutex locks, and re-entrant cache calls inside `OnEvict*` execute without deadlock across both single-cache and multi-cache callback chains.
   - **Remediated (`H-01` & `C-03b`) — Lazy Primary GID Resolution & Child-Goroutine Creator Frame Check**: Uncontended single-cache primary pressure sampling (`pressure.go`) leaves `samplingGID = 0` without calling `runtime.Stack()` unless a concurrent caller contends on an active sampler, lazily resolving the primary holder's GID via `resolvePrimaryFromAllStacks()` (with `invokeFastPrimaryPressure` frame matching and multi-cache GID ownership tracking) and checking `parseStackGoroutineParentAndCreator()` + `isAncestorCreatorAboveHook()` so child goroutines spawned inside `PressureFunc` are detected as re-entrant (`C-03b`). Custom `WithPressureFunc` write latency improved from **`7,888–8,074 ns/op`** down to **`149.8–159.4 ns/op`** (**`50.7x–53.1x` speedup**, `0 B/op`, `0 allocs/op`).
   - **Remediated (`H-03`) — Non-MRU Entry Protection in `Replace()` Under Tier 2 Pressure**: `Replace()` now passes the updated entry unconditionally to `finishMutationReclaimLocked` / `shedAndCompactLocked`, protecting non-MRU replaced entries from immediate self-eviction when other entries can be shed.
   - **Remediated (`H-05`) — Active Collision Counting (`collisionCount` & `collisionPeers`) in `ArenaRadixCache`**: `ArenaRadixCache` tracks active hash collisions in `c.collisionCount` and displaced same-hash node IDs in `c.collisionPeers`, promoting surviving colliding peers into `c.nodeMap` and restoring the $O(1)$ cache-miss fast-path (`len(c.nodeMap)+c.collisionCount == c.len`) immediately upon deletion or eviction of any colliding key.
   - **Remediated (`H-06`, `M-01..M-04`, `L-04`)**: `MapCache` only reallocates `c.index` when `hasSlackLocked()` (`c.dirtyIndex && (len(c.index) < c.peakEntryLen || c.deletedSinceCompact >= minChurnCompactDeletes)`, while foreground `shouldAutoCompactLocked(false)` requires `>= 25%` shrinkage from `peakEntryLen` or `>= 64` churn deletes `>= currentLen`) holds (`H-06`); `DeletePrefix("")` samples pressure via `lockWithPressure` and attributes Tier 1/2 compactions (`M-01`); `ArenaRadixCache.Compact()` on a pure-insert cache is a no-op when `!isDirtyLocked()`, while `DeletePrefix("")` iterates in MRU-to-LRU order across all three backends and non-empty `DeletePrefix(prefix)` iterates in lexicographical pre-order DFS across both radix backends (`M-02`); `RadixCache.Compact()` returns `false` without inflating compaction telemetry (`M-03`); `reconcileThresholdWindow` clamps `advanceEvictionThreshold` to `<= 1.0` and honors explicit valid threshold pairs (`M-04`); and `otellru` clamps counters via `uint64ToCounterInt64` to `maxOtelCounterInt64 = (1<<63) - 1024` (`L-04`).
4. **Conscious Single-Shard Zero-Allocation Architectural Trade-Offs (Bucket 3)**: Exclusive write lock on `Get()` (`H-02`), string headers in `arenaRadixNode[V]` (`H-04`), opt-in memory budget (`M-05`), `[64]` stack buffers (`M-06`), opt-in `otellru.WithName` (`L-01`), and core API surface (`L-02`, `L-03`) are preserved by design.

---

### 1.2 Per-Backend & Subsystem Production Readiness Matrix

| Component / Backend | Verdict | Optimal / Suited Production Workloads | Conscious Architectural Trade-Offs (Bucket 3) | Key Quantitative Profile (`10K–100K` Entries) |
| :--- | :--- | :--- | :--- | :--- |
| **Overall `go-lru` Library** | **Go** | All single-shard in-memory LRU caching workloads, including 0-weight entries (`C-01`), panicking/re-entrant `OnEvict*` callbacks (`C-02`, `C-03`), and custom `WithPressureFunc` (`H-01`). | Single-shard `sync.RWMutex` write lock on `Get()` (`H-02`); callers must not invoke cache methods inside range-over-func iterator bodies. | Passes 100% of unit, differential, fuzz, adversarial, and `-race` tests with `0` lint issues. |
| **`MapCache[V]`** (`BackendMap`, `map_lru.go`) | **Go** | General-purpose flat or hierarchical key-value caching where point lookup latency (`Get`: `52 ns`, `Peek`: `39 ns`), `Put` throughput (`89–344 ns`), and fast $O(N)$ `All()`/`Keys()` iteration (`4.1 µs` for 1K keys, `0 allocs/op`) are primary, and `DeletePrefix` is infrequent. | Frequent `DeletePrefix` on large caches ($O(N)$ full map scan: `14.5 ms` at 100K keys); high-core `Get()` fan-out (`H-02`—use `Peek()` for lock-shared reads). | **Fastest point ops**: `Get`: `52.0 ns/op`, `Peek`: `39.0 ns/op`, `Put` (evict): `336 ns/op` (`2 allocs/op`), `115.1 B/entry` heap footprint. |
| **`RadixCache[V]`** (`BackendRadix`, `radix_lru.go`) | **Go** | Hierarchical URI/path/namespace caches requiring fast $O(P+S)$ subtree purges (`DeletePrefix`: `2.86–3.17 µs` at 10K keys, `1.63 ms` at 100K keys—**`30x` faster than `MapCache`**) and fast Tier 2 pressure shedding (`134 µs` at 10K keys). | Pointer-based tree relies on Go GC for node reclamation (`Compact()` is a no-op returning `0` compactions, `M-03`); `All()`/`Keys()` reconstruct keys (`1 alloc/entry` at depth $\ge 2$). | **Fastest `DeletePrefix`**: `3.09 µs/op` (100/10K keys), `Get`: `195.5 ns/op`, `Peek`: `185.2 ns/op`, `96.2 B/entry` heap footprint. |
| **`ArenaRadixCache[V]`** (`BackendArenaRadix`, `arena_radix.go`, `arena_radix_lru.go`) | **Go** | Hierarchical caches needing both fast $O(P+S)$ `DeletePrefix` (`6.58–7.46 µs`) AND faster point lookups than `RadixCache` (`Get`: `108.9–117.5 ns`, `Peek`: `108.9 ns` via FNV-1a `nodeMap`), plus true slab defragmentation after bulk deletions (`Compact()` reclaims `53.8%` heap slack). | `arenaRadixNode[V]` embeds `prefix string` (`H-04`); `[64]` stack buffers spill to heap at tree `depth > 64` (`M-06`). | **Lowest steady-state allocs & compact footprint**: `Get`: `108.9 ns/op`, `Peek`: `108.9 ns/op`, `DeletePrefix`: `7.46 µs/op`, `95.4–154.2 B/entry` post-compact. |
| **Memory Pressure Subsystem** (`pressure.go`, `options.go`) | **Go** | Caches configured with `WithMemoryBudget(bytes)`, runtime `GOMEMLIMIT`, or custom `WithPressureFunc` closures (`149.8–159.4 ns/op`, `0 allocs/op` after `H-01` remediation). | Default pressure probe requires either `WithMemoryBudget` or `GOMEMLIMIT` to be set (`M-05`, otherwise returns `0.0` by design). | Default amortized probe: `63–74 ns/op` (`0 allocs/op`); custom `WithPressureFunc`: **`149.8–159.4 ns/op`** (`0 allocs/op`, **>50x faster** than pre-fix baseline). |
| **OpenTelemetry Integration** (`otellru/otellru.go`) | **Go** | Asynchronous OTel metric export across all cache backends with overflow-safe counter and gauge clamping (`L-04` fixed). | When registering multiple caches of the same backend on one `MeterProvider`, pass distinct `otellru.WithName(...)` or `otellru.WithAttributes(...)` (`L-01`). | **Zero-alloc callback**: `181.7–198.2 ns/op`, `0 B/op`, `0 allocs/op` (`31.6 µs/op` via full `sdkmetric.ManualReader.Collect`). |

---

## 2. Architecture & Subsystem Overview

### 2.1 Core Interfaces & Contracts (`cache.go`, `options.go`, `errors.go`)
- **`Cache[V any]` (`cache.go:31-99`)**: Exposes 10 concurrency-safe methods: `Put(key, value) ([]V, error)`, `Get(key) (V, bool)`, `Peek(key) (V, bool)`, `Replace(key, value) error`, `Delete(key) (V, bool)`, `DeletePrefix(prefix)`, Go 1.23 range-over-func iterators `All() iter.Seq2[string, V]`, `Keys() iter.Seq[string]`, `Values() iter.Seq[V]`, and `Stats() Stats`.
- **`PressureAwareCache[V any]` (`cache.go:201-211`)**: Extends `Cache[V]` with `Compact()` and `EvaluateMemoryPressure() []V`. All three concrete constructors (`New[V]`, `NewMapCache[V]`, `NewRadixCache[V]`, `NewArenaRadixCache[V]`) return implementations satisfying both `PressureAwareCache[V]` and `StatsProvider`.
- **`Stats` (`cache.go:109-163`)**: 40-field value struct returned in $O(1)$ time with `0 allocs/op` under `c.mu.RLock()`. Tracks `Backend`, lookup hits/misses (`GetHits`, `GetMisses`, `PeekHits`, `PeekMisses`), eviction counts and weights by `EvictionReason` (`Capacity`, `Pressure`, `Deleted`, `Replaced`), capacity gauges (`CurrentSize`, `MaxSize`, `Len`, `ZeroSizeCount`), mutation outcomes (`PutInserted`, `PutUpdated`, `PutRejectedOversized`, `ReplaceUpdated`, `ReplaceNotFound`, `ReplaceSelfEvicted`, `DeleteDeleted`, `DeleteNotFound`, `DeletePrefixExecuted`), pressure/compaction counters (`MemoryPressure`, `CompactionsExplicit`, `CompactionsPressureTier1`, `CompactionsPressureTier2`, `CompactionsAutoSlack`, `PressureShedsInline`, `PressureShedsExplicit`, `ReclaimEpoch`, `DeletedSinceCompact`, `PeakEntryLen`), and arena slab telemetry (`ArenaLiveNodes`, `ArenaFreeNodes`, `ArenaUnallocatedCap`, `ArenaHashFallbacks`).

### 2.2 Backend Data Structures & Complexity Comparison

| Dimension | `MapCache[V]` (`map_lru.go`) | `RadixCache[V]` (`radix_lru.go`) | `ArenaRadixCache[V]` (`arena_radix.go`, `arena_radix_lru.go`) |
| :--- | :--- | :--- | :--- |
| **Primary Index** | `map[string]*entry[V]` (`map_lru.go:127`) | Pointer-based compressed LCRS radix tree (`*radixNode[V]`, `radix_lru.go:53`) | Contiguous slab `[]arenaRadixNode[V]` indexed by `uint32` + `nodeMap map[uint64]uint32` FNV-1a accelerator (`arena_radix.go:49,53`) |
| **LRU Ordering** | Intrusive doubly-linked list (`entryList[V]` with `head`, `tail *entry[V]`, `map_lru.go:37-102`) | Intrusive doubly-linked pointers (`head`, `tail`, `prev`, `next *radixNode[V]`, `radix_lru.go:37-38,56-57`) | Intrusive doubly-linked `uint32` indices (`head`, `tail`, `prev`, `next uint32`, `nilNode = math.MaxUint32`, `arena_radix.go:25,37-38,60-61`) |
| **Node Memory Layout (64-bit, `V = uint64`)** | `entry[uint64]` = **48 bytes** (`prev`, `next *entry`, `key string`, `size uint64`, `value uint64`) + map bucket slot | `radixNode[uint64]` = **80 bytes** (`prefix string`, `value uint64`, `size uint64`, `parent`, `child`, `sibling`, `prev`, `next *radixNode`, `hasValue bool`) | `arenaRadixNode[uint64]` = **56 bytes** (`prefix string` [16B], `size uint64` [8B], `parent`, `child`, `sibling`, `prev`, `next uint32` [20B], `hasValue bool` + 3B pad [4B], `value uint64` [8B]) |
| **GC Pointer Fields per Node (`V` scalar)** | **3 pointers/entry** (`prev`, `next`, `key.ptr`) + 2 pointers/map slot = **$5N$ pointers** | **6 pointers/node** (`prefix.ptr`, `parent`, `child`, `sibling`, `prev`, `next`) $\times$ up to $2N$ nodes = **up to $12N$ pointers** | **1 pointer/node** (`prefix.ptr` at offset 0 of `arenaRadixNode[V]`) across `cap(c.nodes)` + `0` pointers in `map[uint64]uint32` = **$\le 2N$ pointers** |
| **`Get(key)` / `Peek(key)` Complexity** | $O(1)$ hash lookup (`c.mu.Lock()` / `c.mu.RLock()`) | $O(K)$ top-down trie walk with lexicographical sibling scan (`c.mu.Lock()` / `c.mu.RLock()`) | $O(\text{depth})$ upward `verifyKey` on `nodeMap[fnv1a(key)]` hit; $O(1)$ miss when `len(nodeMap)+collisionCount == c.len`; $O(K)$ trie fallback on unhealed displacement |
| **`DeletePrefix(prefix)` Complexity** | $O(N)$ full map iteration (`map_lru.go`) | $O(P + S)$ subtree detachment & pre-order DFS sweep (`radix_lru.go`) | $O(P + S)$ iterative subtree detachment, incremental FNV-1a `hashStack [64]uint64` `nodeMap` purge, & free-list push (`arena_radix_lru.go`) |
| **`All()` / `Keys()` Iteration** | $O(N)$ time, **`0 allocs/op`** (full `key` stored on `entry[V]`) | $O(N \cdot \text{depth})$ time, **$1\text{ alloc/entry}$** at depth $\ge 2$ (`reconstructKey`) | $O(N \cdot \text{depth})$ time, **$1\text{ alloc/entry}$** at depth $\ge 2$ (`reconstructKey`) |
| **`Compact()` Mechanism** | Allocates `make(map[string]*entry[V], len)` & `maps.Copy` when `hasSlackLocked()` (`c.dirtyIndex && (len(c.index) < c.peakEntryLen || c.deletedSinceCompact >= minChurnCompactDeletes)`) | Returns `false` (`0` compactions) while resetting `deletedSinceCompact = 0` and `peakEntryLen = c.len` | Full `oldToNew []uint32` index-remapping into contiguous `newNodes` slice + `newNodeMap` rebuild when `freeCount > 0` or `deletedSinceCompact > 0` |

---

## 3. Correctness, Concurrency, Memory-Safety & Operational Gap Catalog (R1.2 & R2)

### 3.1 Master Ranked Defect & Gap Table

| ID | Severity | Status | Affected Components & Files | Defect / Gap Summary & Resolution | Empirical Verification Test |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`C-01`** | **Critical** | **REMEDIATED (Bucket 2)** | `pressure.go`, `map_lru.go`, `radix_lru.go`, `arena_radix_lru.go` | **Unbounded Entry & Memory Growth on Zero-Weight (`Weigher == 0`) Entries**: Fixed by evicting the LRU tail entry when inserting a new zero-weight entry while `(c.currentSize >= c.maxSize && uint64(c.len) >= c.maxSize)` or `uint64(c.zeroSizeCount) >= c.maxSize`, and enforcing `currentSize + max(0, zeroSizeCount - maxSize) <= maxSize` across positive-to-zero updates and weighted inserts. | `TestDefect_C01_ZeroWeightUnboundedGrowth` (PASS) |
| **`C-02`** | **Critical** | **REMEDIATED (Bucket 1)** | `pressure.go`, `map_lru.go`, `radix_lru.go`, `arena_radix_lru.go` | **State Corruption, Subsequent Panics, & Permanent Mutex Lock Leak on `OnEvict*` Callback Panic**: Fixed by completing all structural/accounting updates first, queuing callbacks in `evictCallbackQueue[V]`, registering `defer c.mu.Unlock()` inside `unlock()`, and invoking callbacks after `c.unlock()`. | `TestDefect_C02_OnEvictPanicStateCorruptionAndLockLeak` (PASS) |
| **`C-03`** | **Critical** | **REMEDIATED (Bucket 2)** | `pressure.go`, `map_lru.go`, `radix_lru.go`, `arena_radix.go`, `arena_radix_lru.go` | **Synchronous Callback Re-Entrancy Deadlock (`OnEvict*` Under `c.mu.Lock()`) & Child-Goroutine `PressureFunc` Recursion**: Fixed by running `OnEvict*` after `c.mu.Unlock()` with multi-cache GID tracking (`C-03a`) and inspecting `parseStackGoroutineAndParentID()` (`created by ... in goroutine N`) in `checkSamplingGoroutineOrChild` (`C-03b`). | `TestDefect_C03_ReentrancyHazards` (PASS) |
| **`H-01`** | **High** | **REMEDIATED (Bucket 1)** | `pressure.go` | **Unamortized `runtime.Stack()` on 100% of Writes with Custom `WithPressureFunc`**: Fixed via lazy primary GID resolution (`resolvePrimaryFromAllStacks` with multi-cache isolation), avoiding `runtime.Stack()` completely on uncontended primary sampling (`50.7x–53.1x` speedup, `0 allocs/op`). | `TestDefect_H01_CustomPressureFuncPerWriteRuntimeStackOverhead` & `BenchmarkCustomPressureFuncOverhead` (PASS) |
| **`H-02`** | **High** | **Preserved by Design (Bucket 3)** | `map_lru.go`, `radix_lru.go`, `arena_radix_lru.go` | **Global Exclusive Write Lock (`c.mu.Lock()`) on `Get()` + Iterator `RWMutex` Contract**: Conscious single-shard trade-off for strict LRU promotion on `Get()`; `Peek()` provides shared `RLock` reads. | `BenchmarkConcurrencyScaling` (`100Get_0Put` vs `100Peek_0Put`, `G=1..64`) |
| **`H-03`** | **High** | **REMEDIATED (Bucket 1)** | `map_lru.go`, `radix_lru.go`, `arena_radix.go`, `arena_radix_lru.go` | **`Replace()` Immediately Evicts Non-MRU Updated Entry Under Tier 2 Pressure**: Fixed by passing the replaced entry unconditionally as protected to `finishMutationReclaimLocked` / `shedAndCompactLocked`. | `TestDefect_H03_ReplaceEvictsNonMRUUnderTier2Pressure` (PASS) |
| **`H-04`** | **High** | **Preserved by Design (Bucket 3)** | `cache.go`, `arena_radix.go` | **`prefix string` Header in `arenaRadixNode[V]`**: Conscious trade-off keeping simple Go `string` slices per node while reducing pointer count to `1` per node (`<= 2N` pointers vs `12N` in `RadixCache`). | `TestAdversarial_CacheLineLayoutAndStructAlignment`, `BenchmarkGCPauseAndScanOverhead` |
| **`H-05`** | **High** | **REMEDIATED (Bucket 2)** | `arena_radix.go`, `arena_radix_lru.go` | **64-Bit FNV-1a Collision Permanently Disables $O(1)$ Cache-Miss Fast-Path Globally**: Fixed by tracking active hash collisions in `c.collisionCount` and displaced same-hash peers in `c.collisionPeers`, promoting surviving peers into `c.nodeMap` immediately when any colliding key is deleted or evicted. | `TestDefect_H05_FNV1aCollisionDisablesO1MissGlobally` (PASS) |
| **`H-06`** | **High** | **REMEDIATED (Bucket 1)** | `map_lru.go`, `pressure.go` | **Unnecessary 2x Map Reallocations Without Slack in `MapCache.Compact()`**: Fixed by requiring `c.hasSlackLocked()` (`c.dirtyIndex && (len(c.index) < c.peakEntryLen || c.deletedSinceCompact >= minChurnCompactDeletes)`, while foreground `shouldAutoCompactLocked(false)` requires `>= 25%` shrinkage from `peakEntryLen` or `>= 64` churn deletes `>= currentLen`) before reallocating `c.index` on explicit or Tier 1 compaction. | `TestDefect_H06_MapCompactReallocatesWithoutSlack` (PASS) |
| **`M-01`** | **Medium** | **REMEDIATED (Bucket 1)** | `map_lru.go`, `radix_lru.go`, `arena_radix_lru.go`, `pressure.go` | **`DeletePrefix("")` Bypasses Pressure Sampling & Misattributes Compaction Telemetry**: Fixed by acquiring `c.lockWithPressure(&c.mu, false)` in `DeletePrefix("")` and passing `(hadCompactionSlack, hadReclaimable, sampledEpoch, pressure)` to `finishClearAllLocked`. | `TestDefect_M01_to_M06_MediumDefectsAndDivergences/M01_*` (PASS) |
| **`M-02`** | **Medium** | **REMEDIATED (Bucket 1)** | `arena_radix.go`, `arena_radix_lru.go`, `map_lru.go`, `radix_lru.go` | **Cross-Backend Compaction Trigger & `DeletePrefix` Callback Ordering Divergences**: Fixed by gating `ArenaRadixCache.isDirtyLocked()` on prior deletions (`freeHead != nilNode || deletedSinceCompact > 0 || nodeMapDirty`) and ensuring `DeletePrefix("")` iterates in MRU-to-LRU order across all three backends while non-empty `DeletePrefix(prefix)` iterates in pre-order DFS across both radix backends. | `TestDefect_M01_to_M06_MediumDefectsAndDivergences/M02_*` (PASS) |
| **`M-03`** | **Medium** | **REMEDIATED (Bucket 1)** | `radix_lru.go` | **`RadixCache.Compact()` Fake Compaction Telemetry**: Fixed by returning `false` from `radixCache.compactDataStructuresLocked()` after resetting metadata counters so compaction and reclaim-epoch counters are not falsely incremented. | `TestDefect_M01_to_M06_MediumDefectsAndDivergences/M03_*` (PASS) |
| **`M-04`** | **Medium** | **REMEDIATED (Bucket 1)** | `options.go` | **Counter-Intuitive Threshold Reconciliation (`EvictionThreshold > 1.0` or Silent Reset)**: Fixed by clamping `advanceEvictionThreshold` to `<= 1.0` and preserving valid explicit threshold pairs when both `WithCompactionThreshold` and `WithEvictionThreshold` are set. | `TestDefect_M01_to_M06_MediumDefectsAndDivergences/M04_*` (PASS) |
| **`M-05`** | **Medium** | **Preserved by Design (Bucket 3)** | `options.go`, `pressure.go` | **`DefaultRuntimePressureFunc` Opt-In Budget Behavior**: Returns `0.0` when both `WithMemoryBudget` and `GOMEMLIMIT` are unset; refreshes pressure sample after reclamation. | Verified in `options.go` & `pressure.go` |
| **`M-06`** | **Medium** | **Preserved by Design (Bucket 3)** | `pressure.go`, `radix_lru.go`, `arena_radix.go`, `arena_radix_lru.go` | **`pressureState` Atomic Layout & `[64]` Stack Buffer Heap Spill at `depth > 64`**: Compact struct layout and 64-level zero-alloc stack buffer with safe heap fallback above depth 64. | `TestAdversarial_CacheLineLayoutAndStructAlignment` & `TestAdversarial_AllocationCharacterization_PathCompressionAndDeepKeys` (PASS) |
| **`L-01`** | **Low** | **Preserved by Design (Bucket 3)** | `otellru/otellru.go` | **`otellru` Multi-Cache Registration Requires `WithName` / `WithAttributes`**: Callers registering multiple caches on one `MeterProvider` distinguish series via `otellru.WithName`. | `TestDefect_L01_DuplicateRegistrationWithoutNameCollision` (`./otellru`, PASS) |
| **`L-02`** | **Low** | **Preserved by Design (Bucket 3)** | `cache.go`, `arena_radix.go`, `options.go` | **Focused Core LRU API Surface**: Minimal `Cache[V]` interface without TTL/clock complexity; evicted values delivered via `Put` return slice and `OnEvict*` callbacks. | `Benchmark_LargeScale_Put_100K` |
| **`L-03`** | **Low** | **Preserved by Design (Bucket 3)** | `arena_radix_lru.go`, `map_lru.go`, `radix_lru.go` | **Pre-Insert `uint32` Node-Limit Guard & Debug `WithInvariantChecking(true)`**: Debug invariant checking thoroughly validates full cache state on lock release. | Verified in `arena_radix_lru.go` |
| **`L-04`** | **Low** | **REMEDIATED (Bucket 1)** | `otellru/otellru.go` | **`uint64ToInt64` Clamping to `math.MaxInt64` Triggers `float64` Precision Wrap in `sdkmetric` `Sum[int64]`**: Fixed by clamping cumulative counter observations via `uint64ToCounterInt64` to `maxOtelCounterInt64 = (1<<63) - 1024`. | `TestAdversarial_Uint64OverflowClampingAllInstruments` (`./otellru`, PASS) |

---

### 3.2 Pre-Remediation Root-Cause Analysis of Critical & High Findings (All Remediated in Section 5.1)

#### `C-01` (Critical): Unbounded Entry & Memory Growth on Zero-Weight (`Weigher == 0`) Entries
- **Locations**: `map_lru.go:344-354,393-398,600-603`, `radix_lru.go:740-750,790-795,957-960`, `arena_radix_lru.go:450-459,490-495,695-698`, `pressure.go:1041-1068`, `README.md:42-44`.
- **Pre-Remediation Mechanism**:
  In all three backends, `Put(key, value)` computes `valueSize := c.weigh(key, value)` before acquiring `c.mu.Lock()`. Prior to remediation, when inserting a new key, capacity eviction was governed solely by:
  ```go
  for valueSize > c.maxSize-c.currentSize && c.entries.Len() > 0 {
      c.evictOne()
  }
  ```
  `README.md:42-44` explicitly recommends configuring byte-weighted caches via:
  ```go
  lru.WithWeigher(func(key string, value []byte) uint64 { return uint64(len(value)) })
  ```
  When a caller caches empty byte slices (`[]byte{}` or `nil`, e.g., negative cache entries or empty payloads), `valueSize` is `0`. Because `c.currentSize <= c.maxSize` is an invariant, `c.maxSize - c.currentSize >= 0`, so `0 > c.maxSize - c.currentSize` is **unconditionally `false`** even when the cache is at 100% byte capacity (`c.currentSize == c.maxSize`).
- **Production Blast Radius (Pre-Remediation) & Resolution**:
  Whenever memory pressure is below `EvictionThreshold` (or when neither `GOMEMLIMIT` nor `WithMemoryBudget` is set, so `DefaultRuntimePressureFunc` returns `0.0`), zero-weight entries previously bypassed capacity eviction forever. Remediated in `shouldEvictZeroWeightOnInsert` (`pressure.go:1041-1054`), `shouldEvictPreInsertOnPut` (`pressure.go:1056-1064`), and `shouldEvictOnZeroWeightShrink` (`pressure.go:1066-1068`) so `currentSize + max(0, zeroSizeCount - maxSize) <= maxSize` and `zeroSizeCount` remain strictly bounded.
- **Verification Command**:
  ```bash
  go test -v -run TestDefect_C01_ZeroWeightUnboundedGrowth .
  ```

#### `C-02` (Critical): State Corruption, Subsequent Panics, & Permanent Lock Leak on `OnEvict*` Callback Panic
- **Locations**: `map_lru.go:274-286,335-418,553-693`, `radix_lru.go:628-674,680-692,730-814,910-1101`, `arena_radix_lru.go:373-385,445-548,647-884`.
- **Pre-Remediation Mechanism**:
  Prior to remediation, user-supplied `OnEvictValue` and `OnEvictEntry` callbacks were invoked synchronously mid-operation while internal data structures were partially mutated:
  1. **`MapCache.DeletePrefix("")`**: `deleteAllPrefixLocked()` iterated over `c.entries`, zeroing node fields and subtracting `c.currentSize` before calling `c.entries.Init()` or `c.clearEmptyIndexStateLocked()`. A panicking `OnEvict*` left `c.entries` severed while `c.index` retained all keys.
  2. **`ArenaRadixCache.DeletePrefix(prefix)` & `RadixCache.DeletePrefix(prefix)`**: `DeletePrefix` detached the target child from its parent before sweeping value nodes in the detached subtree, leaving unvisited subtree nodes linked in the LRU list with dangling parent pointers.
  3. **`Put` Overwrite**: `updateExistingOnPutLocked` decremented `c.currentSize -= oldSize` before the capacity eviction loop while the existing entry's `.size` field still held `oldSize`.
  4. **Double-Panic & Permanent Lock Leak in `unlock()`**: All three `unlock()` methods ran `c.checkInvariants()` before `c.mu.Unlock()` without a `defer c.mu.Unlock()`, masking callback panics and leaking `c.mu`.
- **Verification Command**:
  ```bash
  go test -v -run TestDefect_C02_OnEvictPanicStateCorruptionAndLockLeak .
  ```

#### `C-03` (Critical): Synchronous Callback Re-Entrancy Deadlock (`OnEvict*` & Descendant-Goroutine `PressureFunc`)
- **Locations**: `map_lru.go:378-380`, `radix_lru.go:773-777`, `arena_radix_lru.go:527-531`, `pressure.go:121-155,462-513,1108-1287`.
- **Pre-Remediation Mechanism & Resolution**:
  1. **`OnEvictValue` / `OnEvictEntry` under `c.mu.Lock()`**: Previously invoked while holding `c.mu.Lock()`, deadlocking on any re-entrant cache call. Remediated by buffering events in `evictCallbackQueue[V]` and invoking callbacks after `c.unlock()` with per-cache `evictCallbackMu` serialization and descendant-goroutine (`isAncestorCreatorAboveHook`) deadlock bypass.
  2. **`PressureFunc` Descendant-Goroutine Re-Entrancy**: Previously checked only `currentGoroutineID()` against `samplingGID`. Remediated via `checkSamplingGoroutineOrChild` (`pressure.go:501-513`) and `isAncestorCreatorAboveHook` (`pressure.go:121-155`), detecting child and multi-hop descendant goroutines spawned inside `PressureFunc`.
- **Verification Command**:
  ```bash
  go test -v -run TestDefect_C03_ReentrancyHazards .
  ```

#### `H-01` (High): Unamortized `runtime.Stack()` & `pressureWriteMu.Lock()` on 100% of Writes with Custom `WithPressureFunc`
- **Locations**: `pressure.go:359-446,695-859`.
- **Pre-Remediation Mechanism & Resolution**:
  Prior to remediation, `samplePressureWithEpoch()` called `ensureSamplingGID` $\rightarrow$ `currentGoroutineID()` $\rightarrow$ `runtime.Stack(buf[:32], false)` on every foreground write when `hasCustomPressureFunc == true`, slowing `Put` by `>50x`. Remediated via `invokeFastPrimaryPressure` (`pressure.go:695-704`) and lazy stack resolution (`resolvePrimaryFromAllStacksLocked`, `pressure.go:380-395`), leaving `samplingGID == 0` on uncontended single-cache sampling and achieving `0 allocs/op` and a **`50.7x–53.1x` speedup**.
- **Verification Command**:
  ```bash
  go test -v -run TestDefect_H01_CustomPressureFuncPerWriteRuntimeStackOverhead .
  go test -run='^$' -bench=BenchmarkCustomPressureFuncOverhead -benchmem .
  ```

#### `H-02` (High): Global Exclusive Write Lock (`c.mu.Lock()`) on `Get()` & Iterator `RWMutex` Deadlock
- **Locations**: `map_lru.go:485-498`, `radix_lru.go:843-856`, `arena_radix_lru.go:578-592`.
- **Mechanism (Preserved by Design — Bucket 3)**:
  - `Get(key)` acquires `c.mu.Lock()` to maintain strict LRU promotion ordering in a single-shard structure; `Peek(key)` acquires `c.mu.RLock()` for shared lock-free-order reads.
  - Range-over-func iterators (`All()`, `Keys()`, `Values()`) hold `c.mu.RLock()` while invoking `yield(...)`; callers must not invoke write methods on the same cache instance inside the loop body.

#### `H-03` (High): `Replace()` Immediately Self-Evicts Non-MRU Updated Entry Under Tier 2 Pressure
- **Locations**: `map_lru.go:617`, `radix_lru.go:973-977`, `arena_radix_lru.go:711`.
- **Pre-Remediation Mechanism & Resolution**:
  Previously, `Replace(key, value)` only protected the replaced entry during Tier 2 shedding if it happened to be at the MRU head (`Front()` / `head`). Remediated by passing the replaced entry unconditionally (`e` / `protectedNode` / `nodeID`) to `finishMutationReclaimLocked` across all three backends.
- **Verification Command**:
  ```bash
  go test -v -run TestDefect_H03_ReplaceEvictsNonMRUUnderTier2Pressure .
  ```

#### `H-04` (High): `prefix string` Header in `arenaRadixNode[V]` (`ArenaRadixCache`)
- **Locations**: `cache.go:20-22`, `arena_radix.go:31-41`.
- **Mechanism (Preserved by Design — Bucket 3)**:
  `arenaRadixNode[V]` embeds `prefix string` (16-byte string header at offset 0) and `uint32` tree/LRU indices (`parent`, `child`, `sibling`, `prev`, `next`), reducing pointer fields per node to `1` (`<= 2N` pointers vs `12N` in `RadixCache`).
- **Verification Command**:
  ```bash
  go test -v -run TestAdversarial_AllocationCharacterization_PathCompressionAndDeepKeys .
  ```

#### `H-05` (High): Single 64-Bit FNV-1a Collision Permanently Disables $O(1)$ Cache-Miss Fast-Path Globally
- **Locations**: `arena_radix.go:54-56,364-396,820-924`, `arena_radix_lru.go:254-298`.
- **Pre-Remediation Mechanism & Resolution**:
  Previously, when two live keys shared the same 64-bit FNV-1a hash, `len(c.nodeMap) < c.len` disabled the $O(1)$ miss fast-path until full compaction. Remediated by tracking active collisions in `c.collisionCount` and displaced same-hash peers in `c.collisionPeers`, promoting surviving peers into `c.nodeMap` immediately upon deletion or eviction and restoring `len(c.nodeMap)+c.collisionCount == c.len`.
- **Verification Command**:
  ```bash
  go test -v -run TestDefect_H05_FNV1aCollisionDisablesO1MissGlobally .
  ```

#### `H-06` (High): Unnecessary 2x Map Reallocations Without Bucket Slack in `MapCache.Compact()`
- **Locations**: `map_lru.go:187-189,750-771,832-849`.
- **Pre-Remediation Mechanism & Resolution**:
  Previously, `MapCache` set `c.dirtyIndex = true` on every deletion or capacity eviction and reallocated `c.index` on `Compact()` or Tier 1 `EvaluateMemoryPressure()` even when `len(c.index) == c.peakEntryLen`. Remediated by gating map reallocation on `hasSlackLocked()` (`c.dirtyIndex && (len(c.index) < c.peakEntryLen || c.deletedSinceCompact >= minChurnCompactDeletes)`).
- **Verification Command**:
  ```bash
  go test -v -run TestDefect_H06_MapCompactReallocatesWithoutSlack .
  ```

---

### 3.3 Medium & Low Findings (`M-01..M-06`, `L-01..L-04`)

- **`M-01` (Remediated — `DeletePrefix("")` Samples Pressure & Attributes Compaction Telemetry)**:
  In `map_lru.go`, `radix_lru.go`, and `arena_radix_lru.go`, `DeletePrefix("")` on a non-empty or dirty cache acquires `c.lockWithPressure(&c.mu, false)` and calls `c.finishClearAllLocked(hadCompactionSlack, hadReclaimable, sampledEpoch, pressure)`, attributing Tier 1/2 compactions and `ReclaimEpoch` accurately while preserving a fast no-op path when the cache is already empty and clean.
- **`M-02` (Remediated — Cross-Backend Compaction & Callback Ordering Parity)**:
  1. `ArenaRadixCache.isDirtyLocked()` (`arena_radix.go:515-517`) requires prior deletions (`c.freeHead != nilNode || (c.deletedSinceCompact > 0 && len(c.nodes) < cap(c.nodes)) || c.nodeMapDirty`), so `Compact()` on a pure-insert cache is a no-op across all three backends.
  2. Deleting a leaf in `ArenaRadixCache` that collapses a single-child routing node frees **2** arena nodes (`c.freeCount += 2`), which is characterized in `TestAdversarial_CrossBackendDifferentialAndEdgeCases/ArenaNodeSlack_vs_EntrySlack_CompactionDivergenceCharacterization`.
  3. `DeletePrefix("")` invokes `OnEvict*` in MRU-to-LRU order across all three backends, and non-empty `DeletePrefix("non_empty")` invokes `OnEvict*` in lexicographical pre-order DFS order across `RadixCache` and `ArenaRadixCache`.
- **`M-03` (Remediated — `RadixCache.Compact()` Resets Watermarks Without False Compaction Telemetry)**:
  `radixCache.compactDataStructuresLocked()` (`radix_lru.go:1166-1169`) calls `c.onCompacted(c.len)` and returns `false`, resetting `deletedSinceCompact` and `peakEntryLen` without inflating `CompactionsExplicit`, `CompactionsPressureTier1`, or `ReclaimEpoch`.
- **`M-04` (Remediated — `reconcileThresholdWindow` Clamps Derived `EvictionThreshold` to `<= 1.0` and Honors Explicit Pairs)**:
  In `options.go:534-577`, `advanceEvictionThreshold` clamps the derived `EvictionThreshold` to `<= 1.0`, and `reconcileThresholdWindow` preserves explicit valid `(CompactionThreshold, EvictionThreshold)` pairs whenever `CompactionThreshold < EvictionThreshold`.
- **`M-05` (Preserved by Design — `DefaultRuntimePressureFunc` Opt-In Budget Behavior)**:
  `DefaultRuntimePressureFunc(0)` (`options.go:490-492`) returns `0.0` if `GOMEMLIMIT` is not configured (`math.MaxInt64`), and `markReclaimedLocked()` refreshes the pressure sample after reclamation.
- **`M-06` (Preserved by Design — `pressureState` Atomic Layout & `[64]` Stack Buffer Spill at `depth > 64`)**:
  Verified via `unsafe.Offsetof` in `TestAdversarial_CacheLineLayoutAndStructAlignment`: `peekHits`, `peekMisses`, `putRejectedOversized`, and `pressureWriteMu` sit in `pressureState` at byte offsets `488..528` in `mapCache` (within a single 64-byte cache line `[448, 512)` / `[512, 576)`), while `arenaRadix` isolates `c.mu` on its own 64-byte cache line (`arena_radix.go:72`). In `RadixCache` and `ArenaRadixCache`, keys with `> 64` branching levels spill fixed `[64]` stack buffers to the heap in `reconstructKey`, `hashNodeKey`, and `freeSubtree`.
- **`L-01` (Preserved by Design — `otellru` Multi-Cache Registration Requires `WithName` / `WithAttributes`)**:
  Registering two caches of the same backend on the same `MeterProvider` without unique `WithName` or `WithAttributes` emits identical attribute sets (`{"cache.backend": "map"}`), characterized in `TestDefect_L01_DuplicateRegistrationWithoutNameCollision`.
- **`L-02` (Preserved by Design — Focused Core LRU API Surface)**:
  Core `Cache[V]` interface without TTL/clock complexity; evicted values delivered via `Put` return slice and `OnEvict*` callbacks.
- **`L-03` (Preserved by Design — Pre-Insert `uint32` Node-Limit Guard & Debug `WithInvariantChecking(true)`)**:
  In `arena_radix_lru.go:481-486`, the pre-insert node-limit loop guards against `uint32` node index exhaustion, and `WithInvariantChecking(true)` validates full structural, `nodeMap`/`collisionPeers`, free-list, and telemetry invariants on lock release.
- **`L-04` (Remediated — `otellru` Counter & Gauge Clamping)**:
  `uint64ToCounterInt64` (`otellru/otellru.go:177-182`) clamps cumulative counter observations to `maxOtelCounterInt64 = (1<<63) - 1024` (`9223372036854774784`) so `sdkmetric` v1.46.0 never wraps `math.MaxInt64` to `math.MinInt64`, while `uint64ToInt64`, `clampNonNegativeInt64`, and `clampNormalizedPressure` clamp gauge observations to valid non-negative / `[0.0, 1.0]` ranges.

---

## 4. Efficiency, Throughput & Memory Footprint Evaluation (R3)

All benchmarks were executed on `linux/amd64` (Intel Xeon @ 2.60GHz, 96 logical CPUs) using `go test -bench=... -benchmem`.

### 4.1 Baseline Point, Bulk, Iterator & Scale Benchmarks (`benchmarks_test.go`, `N = 10,000`–`100,000`)

| Operation / Benchmark | `MapCache` (`ns/op` / `B/op` / `allocs`) | `RadixCache` (`ns/op` / `B/op` / `allocs`) | `ArenaRadixCache` (`ns/op` / `B/op` / `allocs`) | Comparative Analysis |
| :--- | :--- | :--- | :--- | :--- |
| **`Get` Hit (`Flat`, depth 0)** | **`51.95 ns`** / `0 B` / `0` | `201.7 ns` / `0 B` / `0` | `108.9 ns` / `0 B` / `0` | `MapCache` is `2.1x` faster than `ArenaRadixCache` and `3.9x` faster than `RadixCache`. |
| **`Get` Hit (`Nested_Depth2`)** | **`52.55 ns`** / `0 B` / `0` | `195.5 ns` / `0 B` / `0` | `117.5 ns` / `0 B` / `0` | `ArenaRadixCache`'s FNV-1a `nodeMap` + upward `verifyKey` is `1.66x` faster than `RadixCache` trie walk. |
| **`Get` Hit (`DeeplyNested_Depth10`)** | **`72.51 ns`** / `0 B` / `0` | `213.8 ns` / `0 B` / `0` | `195.5 ns` / `0 B` / `0` | At depth 10, upward `verifyKey` in `ArenaRadixCache` approaches `RadixCache` top-down walk time. |
| **`Peek` Hit (`Nested_Depth2`)** | **`38.95 ns`** / `0 B` / `0` | `185.2 ns` / `0 B` / `0` | `108.9 ns` / `0 B` / `0` | `Peek` (`RLock` + no LRU move) is `26%` faster than `Get` on `MapCache`. |
| **`Put` With Eviction (`Flat`)** | **`344.3 ns`** / `108 B` / `2` | `500.1 ns` / `119 B` / `2` | `563.4 ns` / `51 B` / `1` | `ArenaRadixCache` halves heap allocations (`1 alloc`, `51 B/op` vs `2 allocs`, `108–119 B/op`). |
| **`Put` With Eviction (`Nested_Depth2`)** | **`336.1 ns`** / `106 B` / `2` | `493.9 ns` / `119 B` / `2` | `576.9 ns` / `52 B` / `1` | `MapCache` has lowest CPU time; `ArenaRadixCache` has lowest allocation rate (`52 B/op`). |
| **`Put` With Eviction (`DeeplyNested_Depth10`)** | **`405.6 ns`** / `164 B` / `2` | `615.8 ns` / `119 B` / `2` | `841.5 ns` / `65 B` / `1` | Deep radix path compression increases `ArenaRadixCache` CPU cost while keeping `B/op` `2.5x` lower. |
| **`Put` In-Place Update (`UnitWeight`)** | **`89.56 ns`** / `4 B` / `0` | `227.3 ns` / `7 B` / `0` | `169.4 ns` / `17 B` / `0` | `0 allocs/op` steady-state across all 3 backends; `ArenaRadixCache` is `1.34x` faster than `RadixCache`. |
| **`Replace` In-Place** | **`94.13 ns`** / `0 B` / `0` | `123.5 ns` / `0 B` / `0` | `129.6 ns` / `0 B` / `0` | `0 allocs/op` across all 3 backends. |
| **`Delete` Single Key** | `113.1 ns` / `0 B` / `0` | **`99.89 ns`** / `0 B` / `0` | `188.6 ns` / `4 B` / `0` | `ArenaRadixCache` updates both the LCRS radix tree/free-list and `nodeMap`. |
| **`DeletePrefix` (100 of 10K keys, `Depth2`)** | `96,070 ns` / `0 B` / `0` | **`3,089 ns`** / `3 B` / `0` | `7,457 ns` / `4 B` / `0` | **`RadixCache` is `31.1x` faster** and **`ArenaRadixCache` is `12.9x` faster** than `MapCache` ($O(N)$ map scan). |
| **`DeletePrefix` (50K of 100K keys, `LargeScale`)** | `14,531,309 ns` (`14.5 ms`) | **`1,627,886 ns`** (`1.63 ms`) | `4,736,156 ns` (`4.74 ms`) | At 100K keys, `RadixCache` is **`8.9x` faster** and `ArenaRadixCache` is **`3.1x` faster** than `MapCache`. |
| **`All()` Iterator (1,000 entries, `Depth2`)** | **`4,110 ns`** / `0 B` / `0` | `66,240 ns` / `24,000 B` / `1,000` | `67,930 ns` / `24,000 B` / `1,000` | `MapCache` is **`16.5x` faster** with `0 allocs/op`; both radix backends allocate 1 string/entry via `reconstructKey`. |
| **`Keys()` Iterator (1,000 entries, `Depth2`)** | **`3,855 ns`** / `0 B` / `0` | `66,089 ns` / `24,000 B` / `1,000` | `66,673 ns` / `24,000 B` / `1,000` | Same `reconstructKey` allocation overhead (`24 B/entry`, `1,000 allocs/op`) on both radix backends. |
| **`Values()` Iterator (1,000 entries, `Depth2`)** | **`4,094 ns`** / `0 B` / `0` | `4,361 ns` / `0 B` / `0` | `4,126 ns` / `0 B` / `0` | **`0 allocs/op` and `~4.1 ns/entry`** across all 3 backends (`Values()` skips `reconstructKey`). |
| **`Stats()` Snapshot (`Sequential` / `Parallel`)** | `74.3 ns` / `59.1 ns` (`0 B`) | `74.6 ns` / `69.9 ns` (`0 B`) | `76.3 ns` / `72.2 ns` (`0 B`) | `0 allocs/op` across all 3 backends. |
| **`LargeScale_Put_100K` (Cold-Load 100K Keys)** | `39.1 ms` / `115.1 heap-B/entry` (`15.0 MB` alloc, `200,540` allocs) | **`32.7 ms`** / `96.15 heap-B/entry` (`9.6 MB` alloc, `100,011` allocs) | `48.8 ms` / **`95.39 heap-B/entry`** (`39.3 MB` alloc, **`575` allocs**) | `ArenaRadixCache` reduces heap object count by **`348x`** (`575` vs `200,540` allocs) and achieves the lowest live heap footprint (`95.39 B/entry`), though cold slice doubling allocates `39.3 MB` transiently (`L-02`). |

---

### 4.2 Access Distributions (`BenchmarkKeyDistributions`, Capacity = `4,096`, Key Space = `65,536`)

| Workload Distribution | `MapCache` (`ns/op` / `B/op` / `allocs`) | `RadixCache` (`ns/op` / `B/op` / `allocs`) | `ArenaRadixCache` (`ns/op` / `B/op` / `allocs`) | Key Insight |
| :--- | :--- | :--- | :--- | :--- |
| **`Zipfian_90Get_10Put`** ($s=1.07$, high hit rate) | **`156.6 ns`** / `1 B` / `0` | `229.4 ns` / `2 B` / `0` | `227.2 ns` / `1 B` / `0` | Under skewed hot-key traffic, all 3 backends run at `0 allocs/op`; `MapCache` is `1.45x` faster. |
| **`Uniform_90Get_10Put`** (~6.25% hit rate, miss-heavy) | **`109.3 ns`** / `8 B` / `0` | `217.9 ns` / `12 B` / `0` | `168.2 ns` / `2 B` / `0` | On miss-heavy uniform traffic, `ArenaRadixCache`'s $O(1)$ `nodeMap` miss check beats `RadixCache` by `29.5%` and reduces `B/op` by `6x` (`2 B/op` vs `12 B/op`). |
| **`SequentialScan_100Put_Evict`** (0% hit rate, 100% eviction churn) | **`296.0 ns`** / `88 B` / `3` | `377.7 ns` / `121 B` / `3` | `481.6 ns` / **`19 B`** / **`2`** | During streaming scan eviction, `ArenaRadixCache` recycles nodes via its free-list, cutting heap bytes allocated per op by **`4.6x–6.4x`** (`19 B/op` vs `88–121 B/op`). |
| **`PrefixLocalityBurst_PutAndDeletePrefix`** (64-key batch insert + `DeletePrefix`) | `47,194 ns` / `5,144 B` / `128` | **`16,371 ns`** / `5,872 B` / `81` | `20,342 ns` / **`132 B`** / **`9`** | `RadixCache` (`2.88x` faster) and `ArenaRadixCache` (`2.32x` faster) vastly outperform `MapCache` on batch prefix lifecycle; `ArenaRadixCache` cuts allocations by **`14.2x`** (`9 allocs` / `132 B` vs `128 allocs` / `5,144 B`) via free-list node reuse! |

---

### 4.3 Payload Sizes & Pointer Density (`BenchmarkPayloadSizes`, Capacity = `2,048`)

| Payload Type | Operation | `MapCache` (`ns/op` / `B/op` / `allocs`) | `RadixCache` (`ns/op` / `B/op` / `allocs`) | `ArenaRadixCache` (`ns/op` / `B/op` / `allocs`) | Key Insight |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`Scalar64B` (`[64]byte`)** | `Get_Hit` | **`50.28 ns`** / `0 B` / `0` | `123.8 ns` / `0 B` / `0` | `111.2 ns` / `0 B` / `0` | Inline 64B value copy adds negligible overhead (`~3 ns`). |
| **`Scalar64B` (`[64]byte`)** | `Put_Update` | **`79.50 ns`** / `0 B` / `0` | `146.9 ns` / `0 B` / `0` | `147.5 ns` / `0 B` / `0` | `0 allocs/op` across all 3 backends. |
| **`Scalar64B` (`[64]byte`)** | `Put_Evict` | **`351.8 ns`** / `201 B` / `3` | `468.5 ns` / `250 B` / `3` | `494.4 ns` / **`69 B`** / **`2`** | `ArenaRadixCache` avoids allocating a new 112B node struct on every eviction (`69 B/op` vs `201–250 B/op`). |
| **`Scalar512B` (`[512]byte`)** | `Get_Hit` | **`129.9 ns`** / `0 B` / `0` | `227.1 ns` / `0 B` / `0` | `192.6 ns` / `0 B` / `0` | Copying a 512B value by value on `Get` adds `~80 ns` across all backends. |
| **`Scalar512B` (`[512]byte`)** | `Put_Update` | **`130.5 ns`** / `0 B` / `0` | `234.3 ns` / `0 B` / `0` | `218.5 ns` / `0 B` / `0` | `0 allocs/op` in-place 512B overwrite. |
| **`Scalar512B` (`[512]byte`)** | `Put_Evict` | **`831.0 ns`** / `1,116 B` / `3` | `1,073 ns` / `1,324 B` / `3` | `842.6 ns` / **`519 B`** / **`2`** | For large inline structs (`[512]byte`), `ArenaRadixCache` matches `MapCache` speed (`842 ns` vs `831 ns`) and is **`27%` faster than `RadixCache`** while cutting allocations in half (`519 B/op` for the returned `[]V` slice only vs `1,116–1,324 B/op`). |
| **`ByteSlice_1KB` (`[]byte`)** | `Get_Hit` | **`46.70 ns`** / `0 B` / `0` | `109.9 ns` / `0 B` / `0` | `104.2 ns` / `0 B` / `0` | Only the 24-byte slice header is copied. |
| **`ByteSlice_1KB` (`[]byte`)** | `Put_Evict` | **`317.8 ns`** / `112 B` / `3` | `426.4 ns` / `150 B` / `3` | `480.0 ns` / **`29 B`** / **`2`** | `ArenaRadixCache` allocates only `29 B/op` (`3.9x–5.2x` less heap churn). |
| **`ByteSlice_64KB` (`[]byte`)** | `Get_Hit` | **`46.83 ns`** / `0 B` / `0` | `109.0 ns` / `0 B` / `0` | `104.0 ns` / `0 B` / `0` | Latency is independent of `[]byte` backing array size when slice header is stored. |
| **`ByteSlice_64KB` (`[]byte`)** | `Put_Evict` | **`314.5 ns`** / `112 B` / `3` | `426.5 ns` / `150 B` / `3` | `473.2 ns` / **`29 B`** / **`2`** | Identical header-copy efficiency as `1KB`. |

---

### 4.4 Concurrency Scaling & Read/Write Contention (`BenchmarkConcurrencyScaling`)

#### A. Read/Write Ratio Sweep at Full Parallelism (`GOMAXPROCS = 96`)

| Workload Mix (`b.RunParallel`) | `MapCache` (`ns/op` / `B/op` / `allocs`) | `RadixCache` (`ns/op` / `B/op` / `allocs`) | `ArenaRadixCache` (`ns/op` / `B/op` / `allocs`) | Contention Analysis |
| :--- | :--- | :--- | :--- | :--- |
| **`100Peek_0Put`** (`100% Peek`, shared `RLock`) | `73.14 ns` / `0 B` / `0` | **`57.16 ns`** / `0 B` / `0` | `58.22 ns` / `0 B` / `0` | Shared `RLock` scales across cores; only bottleneck is atomic `peekHits.Add(1)` cache-line bouncing (`M-06`). |
| **`100Get_0Put`** (`100% Get`, exclusive `Lock`) | `497.6 ns` / `0 B` / `0` | **`394.3 ns`** / `0 B` / `0` | `468.5 ns` / `0 B` / `0` | **Finding `H-02` proven**: `100% Get` is **`6.8x–8.0x` slower** than `100% Peek` because every `Get` takes `c.mu.Lock()`. |
| **`95Get_5Put`** (`95% Get`, `5% Put`) | `467.8 ns` / `0 B` / `0` | `506.5 ns` / `0 B` / `0` | **`417.6 ns`** / `0 B` / `0` | `ArenaRadixCache` outperforms `MapCache` and `RadixCache` under mixed contention due to cache-line isolation of `c.mu` (`arena_radix.go:72`). |
| **`80Get_20Put`** (`80% Get`, `20% Put`) | `548.0 ns` / `0 B` / `0` | `470.7 ns` / `0 B` / `0` | **`392.5 ns`** / `0 B` / `0` | `ArenaRadixCache` is **`1.40x` faster** than `MapCache` under 80/20 contention. |
| **`50Get_50Put`** (`50% Get`, `50% Put`) | `597.6 ns` / `0 B` / `0` | `519.0 ns` / `0 B` / `0` | **`449.5 ns`** / `0 B` / `0` | `ArenaRadixCache` is **`1.33x` faster** than `MapCache` under 50/50 write contention. |
| **`0Get_100Put`** (`100% Put` with eviction) | **`576.9 ns`** / `40 B` / `1` | `840.4 ns` / `64 B` / `2` | `1,051 ns` / **`13 B`** / **`1`** | Under 100% eviction churn, `MapCache` has the highest throughput while `ArenaRadixCache` has `3x–5x` lower `B/op`. |

#### B. Goroutine Scaling Curve (`G = 1, 2, 4, 8, 16, 32, 64` Concurrent Workers)

| Goroutines (`G`) | `Get_Hit` (`Map` / `Radix` / `Arena` `ns/op`) | `Peek_Hit` (`Map` / `Radix` / `Arena` `ns/op`) | `Mixed_95Get_5Put` (`Map` / `Radix` / `Arena` `ns/op`) | Scaling Behavior |
| :--- | :--- | :--- | :--- | :--- |
| **`G = 1`** | `56.7` / `154.3` / `122.6 ns` | `42.6` / `145.3` / `116.3 ns` | `57.3` / `155.2` / `133.5 ns` | Uncontended baseline: `MapCache` is fastest (`42.6–57.3 ns/op`). |
| **`G = 2`** | `171.9` / `447.4` / `307.1 ns` | `216.3` / `197.7` / `254.1 ns` | `144.5` / `449.6` / `305.7 ns` | Cross-core cache-line transfer begins on `c.mu` and `peekHits`. |
| **`G = 4`** | `350.0` / `437.5` / **`329.8 ns`** | **`159.3`** / `174.7` / `172.4 ns` | `352.7` / `474.6` / `377.1 ns` | `Peek_Hit` begins scaling downward in amortized `ns/op`, while `Get_Hit` climbs due to `c.mu.Lock()` serialization. |
| **`G = 8`** | `538.3` / `452.0` / **`420.8 ns`** | `146.0` / `151.6` / **`142.1 ns`** | `522.3` / `447.6` / **`379.4 ns`** | At `G = 8`, `ArenaRadixCache` overtakes `MapCache` on both `Get_Hit` (`420.8` vs `538.3 ns`) and `Mixed_95Get_5Put` (`379.4` vs `522.3 ns`) because `ArenaRadixCache` isolates `c.mu` on its own 64-byte cache line (`arena_radix.go:72`) and touches contiguous slab memory. |
| **`G = 16`** | `545.7` / `285.8` / `436.6 ns` | `119.7` / **`115.0`** / `126.4 ns` | `629.1` / `448.7` / **`375.0 ns`** | `Peek_Hit` is **`3.5x–4.6x` faster** than `Get_Hit`. |
| **`G = 32`** | `614.6` / **`390.7`** / `458.6 ns` | **`102.8`** / `106.9` / `121.4 ns` | `610.0` / `483.7` / **`419.5 ns`** | `MapCache` `Get_Hit` plateaus at `~615 ns/op` (`10.8x` slower than `G=1`). |
| **`G = 64`** | `611.5` / `495.9` / **`416.3 ns`** | **`79.50`** / `81.15` / `93.98 ns` | `630.3` / `537.9` / **`422.1 ns`** | At `G = 64`, `Peek_Hit` achieves `79.5–94.0 ns/op` across all backends, whereas `Get_Hit` is serialized at `416.3–611.5 ns/op`. |

---

### 4.5 Compaction & Tier 2 Pressure Shedding Pause Latency (`BenchmarkCompactionAndPressureShedLatency`)

Because both `Compact()` and `EvaluateMemoryPressure()` execute synchronously under `c.mu.Lock()`, their `ns/op` represents a **stop-the-world pause** for all concurrent cache operations on that instance:

| Operation & Scale (`N` Entries) | `MapCache` (Pause / `B/op` / `allocs`) | `RadixCache` (Pause / `B/op` / `allocs`) | `ArenaRadixCache` (Pause / `B/op` / `allocs`) | Operational Impact |
| :--- | :--- | :--- | :--- | :--- |
| **`Compact()` after 75% Delete (`N = 1,000`)** | `15.68 µs` / `13,976 B` / `6` | `1.38 µs` / `320 B` / `2` *(no-op)* | `37.88 µs` / `33,176 B` / `8` | Sub-50µs pause at `N = 1,000`. |
| **`Compact()` after 75% Delete (`N = 10,000`)** | `162.5 µs` / `109,584 B` / `12` | `1.87 µs` / `320 B` / `2` *(no-op)* | `252.6 µs` / `336,400 B` / `14` | `0.16–0.25 ms` pause at `N = 10,000`. |
| **`Compact()` after 75% Delete (`N = 50,000`)** | `965.6 µs` (`0.97 ms`) / `437 KB` / `36` | `3.75 µs` / `320 B` / `2` *(no-op)* | **`2,468.7 µs` (`2.47 ms`)** / `1.41 MB` / `38` | At `N = 50,000`, `ArenaRadixCache.Compact()` holds `c.mu.Lock()` for **`2.47 ms`** and allocates `1.41 MB` transiently (`oldToNew` + `newNodes` + `newNodeMap`). |
| **Tier 2 `EvaluateMemoryPressure()` Shed 50% (`N = 1,000`)** | `74.68 µs` / `35,800 B` / `13` | **`22.61 µs`** / `8,960 B` / `41` | `114.6 µs` / `73,304 B` / `47` | `RadixCache` sheds fastest because its `compactDataStructuresLocked()` is a no-op (`M-03`). |
| **Tier 2 `EvaluateMemoryPressure()` Shed 50% (`N = 10,000`)** | `740.8 µs` / `347.0 KB` / `33` | **`133.6 µs`** / `128.5 KB` / `15` | `1,037.2 µs` (`1.04 ms`) / `677.7 KB` / `35` | `MapCache` (`0.74 ms`) and `ArenaRadixCache` (`1.04 ms`) both evict 5,000 entries AND reallocate their index/slab under `c.mu.Lock()`. |
| **Tier 2 `EvaluateMemoryPressure()` Shed 50% (`N = 50,000`)** | `6,321.2 µs` (`6.32 ms`) / `1.76 MB` / `87` | **`793.9 µs` (`0.79 ms`)** / `893.1 KB` / `2,069` | **`6,896.5 µs` (`6.90 ms`)** / `3.36 MB` / `2,137` | At `N = 50,000`, inline or explicit Tier 2 shedding stalls all callers for **`6.32 ms` (`MapCache`)** and **`6.90 ms` (`ArenaRadixCache`)** while transiently allocating `1.76–3.36 MB`. |

---

### 4.6 GC Scan Pause & Memory Footprint Per Entry (`BenchmarkGCPauseAndScanOverhead`, `N = 50,000` Live Entries After 50% Churn)

`BenchmarkGCPauseAndScanOverhead` inserts `100,000` entries and deletes `50,000` entries (leaving `50,000` live entries with 50% churn slack), measuring `runtime.GC()` pause duration and live `heap_bytes/entry` both **before** and **after** `Compact()`:

| Payload & Backend | State (`N = 50,000` Live Entries) | `gc_pause_ns/op` (`runtime.GC()`) | `heap_bytes/entry` (Live Heap) | Heap Reclaimed by `Compact()` | Analysis (`H-04` & `M-03`) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`Scalar64B` (`[64]byte`) — `MapCache`** | Before `Compact()` (50% churn) | `2,551,206 ns` (`2.55 ms`) | `213.9 B/entry` | — | Retains empty map buckets for 100K peak keys. |
| **`Scalar64B` (`[64]byte`) — `MapCache`** | After `Compact()` | `2,727,029 ns` (`2.73 ms`) | `179.0 B/entry` | **`-34.9 B/entry` (`-16.3%`)** | Reallocating `c.index` shrinks map bucket overhead by `34.9 B/entry`. |
| **`Scalar64B` (`[64]byte`) — `RadixCache`** | Before `Compact()` (50% churn) | `1,987,704 ns` (`1.99 ms`) | `166.7 B/entry` | — | Freed `radixNode` structs are already unreachable by GC. |
| **`Scalar64B` (`[64]byte`) — `RadixCache`** | After `Compact()` | `2,120,256 ns` (`2.12 ms`) | `166.5 B/entry` | **`0.0 B/entry` (`0%`)** | **Proves `M-03`**: `RadixCache.Compact()` is a pure metadata no-op. |
| **`Scalar64B` (`[64]byte`) — `ArenaRadixCache`** | Before `Compact()` (50% churn) | `1,551,572 ns` (`1.55 ms`) | `334.1 B/entry` | — | Retains 50K freed node slots inside `c.nodes` slab (`ArenaFreeNodes`). |
| **`Scalar64B` (`[64]byte`) — `ArenaRadixCache`** | After `Compact()` | **`1,567,217 ns` (`1.57 ms`)** | **`154.2 B/entry`** | **`-179.9 B/entry` (`-53.8%`)** | `ArenaRadixCache.Compact()` reclaims `53.8%` of heap memory and achieves the lowest memory footprint (`154.2 B/entry`) and lowest GC pause (`1.57 ms`). However, **proves `H-04`**: because `arenaRadixNode[[64]byte]` embeds `prefix string` (`*byte`), `c.nodes` is still scanned by GC (`1.57 ms` vs `2.12–2.73 ms`, rather than ~`0 ms` for a `noscan` byte slab). |
| **`ByteSlice_1KB` (`[]byte`) — `MapCache`** | Before / After `Compact()` | `5.28 ms` / `5.32 ms` | `1,190 B` $\rightarrow$ `1,155 B/entry` | `-35.0 B/entry` (`-2.9%`) | Dominated by the 1KB `[]byte` backing arrays. |
| **`ByteSlice_1KB` (`[]byte`) — `RadixCache`** | Before / After `Compact()` | `4.97 ms` / `4.28 ms` | `1,136 B` $\rightarrow$ `1,137 B/entry` | `0.0 B/entry` (`0%`) | `0%` structural reclamation. |
| **`ByteSlice_1KB` (`[]byte`) — `ArenaRadixCache`** | Before / After `Compact()` | **`4.13 ms`** / **`4.54 ms`** | `1,244 B` $\rightarrow$ **`1,133 B/entry`** | **`-111.0 B/entry` (`-8.9%`)** | Lowest post-compaction footprint (`1,133 B/entry` = `1,024 B` payload + `109 B` total cache overhead). |

---

### 4.7 Pressure Probe Overhead (`H-01` Remediated) & Eviction Callback Tree-Depth Scaling (`BenchmarkPressureSamplingAndCallbacks`)

#### A. Built-in Amortized `DefaultRuntimePressureFunc` vs Custom `WithPressureFunc` (`H-01` Before & After Remediation)

| Backend | `DefaultRuntimePressureFunc` (`WithMemoryBudget`, 256-op amortized) | Custom `WithPressureFunc` Pre-Fix Baseline (`runtime.Stack()` per write) | Custom `WithPressureFunc` Post-Fix (`resolvePrimaryFromAllStacks`) | Remediation Speedup (`H-01`) |
| :--- | :--- | :--- | :--- | :--- |
| **`MapCache`** | **`65.97 ns/op`** (`0 B/op`, `0 allocs/op`) | `7,958 ns/op` (`0 B/op`, `0 allocs/op`) | **`149.8 ns/op`** (`0 B/op`, `0 allocs/op`) | **`53.1x` faster** (`-7,808 ns/write`, `0 allocs/op`) |
| **`RadixCache`** | **`62.96 ns/op`** (`0 B/op`, `0 allocs/op`) | `7,888 ns/op` (`0 B/op`, `0 allocs/op`) | **`152.9 ns/op`** (`0 B/op`, `0 allocs/op`) | **`51.6x` faster** (`-7,735 ns/write`, `0 allocs/op`) |
| **`ArenaRadixCache`** | **`74.33 ns/op`** (`0 B/op`, `0 allocs/op`) | `8,074 ns/op` (`0 B/op`, `0 allocs/op`) | **`159.4 ns/op`** (`0 B/op`, `0 allocs/op`) | **`50.7x` faster** (`-7,915 ns/write`, `0 allocs/op`) |

#### B. `OnEvictValue` vs `OnEvictEntry` at Radix Tree `Depth = 4` vs `Depth = 32`

| Tree Depth & Callback Type | `MapCache` (`ns/op` / `B/op` / `allocs`) | `RadixCache` (`ns/op` / `B/op` / `allocs`) | `ArenaRadixCache` (`ns/op` / `B/op` / `allocs`) | Analysis |
| :--- | :--- | :--- | :--- | :--- |
| **`Depth = 4` — `OnEvictValue`** | **`289.7 ns`** / `81 B` / `3` | `326.2 ns` / `99 B` / `2` | `399.9 ns` / **`9 B`** / **`1`** | `OnEvictValue` does not reconstruct the evicted key on `RadixCache` or `ArenaRadixCache`. |
| **`Depth = 4` — `OnEvictEntry`** | **`289.3 ns`** / `81 B` / `3` | `415.6 ns` / `124 B` / `3` | `450.7 ns` / **`34 B`** / **`2`** | `OnEvictEntry` adds `+1 alloc/op` (`25 B/op`) on `RadixCache` and `ArenaRadixCache` to reconstruct the 4-level key string (`reconstructKey`). |
| **`Depth = 32` — `OnEvictValue`** | **`376.4 ns`** / `204 B` / `3` | `397.2 ns` / `102 B` / `2` | `860.0 ns` / **`12 B`** / **`1`** | At depth 32, `ArenaRadixCache`'s 32-level upward `verifyKey` and `hashNodeKey` walks add CPU time (`860 ns/op`), while keeping `B/op` `17x` lower than `MapCache` (`12 B` vs `204 B`). |
| **`Depth = 32` — `OnEvictEntry`** | **`369.1 ns`** / `204 B` / `3` | `524.6 ns` / `248 B` / `3` | `976.2 ns` / `157 B` / `2` | `MapCache` is unaffected by key depth on `OnEvictEntry` (`0` extra allocs because `e.key` is stored in full); `RadixCache` and `ArenaRadixCache` allocate the 145-byte reconstructed key on every capacity eviction. |

---

### 4.8 OpenTelemetry Scrape Overhead (`otellru/adversarial_otel_test.go`)

| Benchmark | Latency (`ns/op`) | Bytes (`B/op`) | Allocations (`allocs/op`) | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **`BenchmarkOtelScrape_DirectCallback/MapCache`** | **`183.4 ns/op`** | **`0 B/op`** | **`0 allocs/op`** | Acquires `c.mu.RLock()`, copies `Stats`, and emits 35 observations (`14` instruments). |
| **`BenchmarkOtelScrape_DirectCallback/RadixCache`** | **`181.7 ns/op`** | **`0 B/op`** | **`0 allocs/op`** | `0 allocs/op` via pre-allocated `[]metric.ObserveOption` slices (`otellru.go:204-210`). |
| **`BenchmarkOtelScrape_DirectCallback/ArenaRadixCache`** | **`198.2 ns/op`** | **`0 B/op`** | **`0 allocs/op`** | Emits all 39 observations (`16` instruments, including `arena.nodes` and `arena.hash_fallbacks`). |
| **`BenchmarkOtelScrape_FullSDKCollect/Caches_1`** | `31,597 ns/op` (`31.6 µs`) | `9,500 B/op` | `169 allocs/op` | Full `sdkmetric.ManualReader.Collect` aggregation overhead inside OpenTelemetry SDK for 1 cache. |
| **`BenchmarkOtelScrape_FullSDKCollect/Caches_10`** | `278,980 ns/op` (`279.0 µs`) | `78,626 B/op` | `1,281 allocs/op` | Linear scaling (`~27.9 µs/cache`) when scraping 10 registered caches on one `MeterProvider`. |

---

## 5. Completed Remediations & Production Adoption Guide

### 5.1 Completed Bucket 1 (Real Bugs) & Bucket 2 (Sharp Edges) Remediations

1. **Remediated `C-02` & `C-03a` — Deferred Eviction Callbacks Outside `c.mu.Lock()` and Deferred `c.mu.Unlock()`**:
   - **Files**: `pressure.go`, `map_lru.go`, `radix_lru.go`, `arena_radix.go`, `arena_radix_lru.go`.
   - **Resolution**:
     - In `unlock()` and `rUnlock()`, `defer c.mu.Unlock()` / `defer c.mu.RUnlock()` is registered **before** calling `c.checkInvariants()`.
     - All mutating methods (`Put`, `Replace`, `Delete`, `DeletePrefix`, `EvaluateMemoryPressure`) declare `var evictQ evictCallbackQueue[V]` (`buf [2]evictEvent[V]`) and `defer evictQ.invoke(&c.pressureState, c.onEvictValue, c.onEvictEntry)` **before** `defer c.unlock()` so Go's LIFO `defer` order guarantees `c.checkInvariants()` and `c.mu.Unlock()` complete before any user `OnEvict*` callback runs.
   - **Verified Impact**: Eliminates callback panic state corruption (`C-02a..c`), invariant double-panic lock leaks (`C-02d`), and callback re-entrancy deadlocks (`C-03a`) while preserving `0 allocs/op` on steady-state hot paths.

2. **Remediated `C-01` — Bounded Zero-Weight (`Weigher == 0`) Entry Accumulation**:
   - **Files**: `pressure.go`, `map_lru.go`, `radix_lru.go`, `arena_radix_lru.go`.
   - **Resolution**:
     - When inserting a new entry with `valueSize == 0`, if `!evictedPrePut && c.shouldEvictZeroWeightOnInsert(valueSize, c.maxSize, c.currentSize, c.len)` (`(currentSize >= maxSize && uint64(currentLen) >= maxSize) || uint64(p.zeroSizeCount) >= maxSize`), the cache evicts at most 1 LRU tail entry (`evictOne(&evictQ)`) before inserting the new zero-weight entry; positive-to-zero updates (`Put`/`Replace`) and weighted inserts with excess zero-weight entries also enforce `currentSize + max(0, zeroSizeCount - maxSize) <= maxSize`.
   - **Verified Impact**: Entry count and `ZeroSizeCount` remain strictly bounded without wiping out positive-weight tail entries (`TestDefect_C01_ZeroWeightUnboundedGrowth`).

3. **Remediated `H-01` & `C-03b` — Lazy Primary GID Resolution & Descendant-Goroutine Creator Frame Guard**:
   - **Files**: `pressure.go`.
   - **Resolution**:
     - When a goroutine acquires the uncontended single-cache primary sampler slot (`samplingPressure: false -> true`, `fastPrimarySamplerOwner: nil -> p`), it leaves `samplingGID = 0` without calling `runtime.Stack()` and invokes `invokeFastPrimaryPressure`. Only if a second goroutine concurrently contends does `resolvePrimaryFromAllStacks` / `promoteFastPrimarySamplerLocked` lazily resolve the primary holder's GID and inspect `parseStackGoroutineParentAndCreator()` + `isAncestorCreatorAboveHook()` (`created by <creatorFunc> in goroutine N`) so both direct re-entrancy and child/descendant goroutines spawned inside `PressureFunc` (`C-03b`) are detected.
   - **Verified Impact**: Custom `WithPressureFunc` write latency dropped from `7,888–8,074 ns/op` to **`149.8–159.4 ns/op`** (**`50.7x–53.1x` speedup**, `0 allocs/op`).

4. **Remediated `H-03` — Non-MRU Entry Protection in `Replace()` Under Tier 2 Pressure**:
   - **Files**: `map_lru.go`, `radix_lru.go`, `arena_radix.go`, `arena_radix_lru.go`.
   - **Resolution**:
     - `Replace()` passes the updated entry (`e` / `node` / `nodeID`) unconditionally to `finishMutationReclaimLocked` and `shedAndCompactLocked`. During Tier 2 shedding, `shedAndCompactLocked` skips the protected entry regardless of its LRU position while shedding other entries when `EvictionRetentionRatio > 0`.

5. **Remediated `H-05` — Active Collision Tracking (`collisionCount` & `collisionPeers`) in `ArenaRadixCache`**:
   - **Files**: `arena_radix.go`, `arena_radix_lru.go`.
   - **Resolution**:
     - `arenaRadix[V]` tracks `collisionCount int` and `collisionPeers map[uint64][]uint32` across insertions, promotions, single-key deletions/evictions, `DeletePrefix`, and `Compact()`. The $O(1)$ cache-miss fast-path (`len(c.nodeMap)+c.collisionCount == c.len`) is restored immediately when a colliding key is deleted or evicted.

6. **Remediated `H-06`, `M-01..M-04`, & `L-04` — Compaction Slack Gating, Telemetry Unification, Threshold Clamping, & OTel Overflow Protection**:
   - **Files**: `map_lru.go`, `radix_lru.go`, `arena_radix.go`, `arena_radix_lru.go`, `pressure.go`, `options.go`, `otellru/otellru.go`.
   - **Resolution**:
     - `H-06`: `MapCache` gates `compactDataStructuresLocked()` and `shouldAutoCompactLocked()` on `hasSlackLocked()` (`c.dirtyIndex && (len(c.index) < c.peakEntryLen || c.deletedSinceCompact >= minChurnCompactDeletes)`, while foreground `shouldAutoCompactLocked(false)` requires `>= 25%` shrinkage from `peakEntryLen` or `>= 64` churn deletes `>= currentLen`), preventing 2x map reallocations when bucket slack is zero.
     - `M-01`: `DeletePrefix("")` acquires `c.lockWithPressure(&c.mu, false)` and passes `(hadCompactionSlack, hadReclaimable, sampledEpoch, pressure)` to `finishClearAllLocked`, sampling pressure and attributing Tier 1/2 compactions and `ReclaimEpoch` consistently.
     - `M-02`: `ArenaRadixCache.isDirtyLocked()` requires prior deletions (`freeHead != nilNode || (deletedSinceCompact > 0 && len(nodes) < cap(nodes)) || nodeMapDirty`), while `DeletePrefix("")` iterates in MRU-to-LRU order across all three backends and non-empty `DeletePrefix(prefix)` iterates in lexicographical pre-order DFS order across both radix backends.
     - `M-03`: `RadixCache.compactDataStructuresLocked()` resets `deletedSinceCompact` and `peakEntryLen` via `onCompacted(c.len)` and returns `false` so compaction and `ReclaimEpoch` counters are not incremented without structural compaction.
     - `M-04`: `reconcileThresholdWindow` clamps `advanceEvictionThreshold` to `<= 1.0` and preserves explicit valid `(CompactionThreshold, EvictionThreshold)` pairs.
     - `L-04`: `otellru` clamps cumulative counter observations via `uint64ToCounterInt64` to `maxOtelCounterInt64 = (1<<63) - 1024` (`9223372036854774784`) so `sdkmetric` v1.46.0 never wraps `math.MaxInt64` to `math.MinInt64`.

---

### 5.2 Conscious Architectural Trade-Offs (Bucket 3) & Operational Best Practices

To preserve `go-lru`'s single-shard, zero-allocation steady-state architecture, the following characteristics are maintained by design:
1. **Use `Peek()` on high-core read-only paths where LRU promotion is not required (`H-02`)**: `Get()` acquires `c.mu.Lock()` to maintain strict single-shard LRU promotion ordering; callers inside `for range cache.All()` / `Keys()` / `Values()` loops (which hold `c.mu.RLock()`) must not call write methods on the same cache instance inside the loop body.
2. **Configure `WithMemoryBudget(bytes)` or `GOMEMLIMIT` when using `DefaultRuntimePressureFunc` (`M-05`)**: When neither a memory budget nor `GOMEMLIMIT` is configured, the default runtime probe returns `0.0` (no pressure).
3. **Pass a unique `otellru.WithName("...")` when registering multiple caches on the same `MeterProvider` (`L-01`)**: Distinguishes OpenTelemetry metric series across cache instances sharing a backend type.

---

## 6. Reproducible Verification Commands

All unit, differential, fuzz, adversarial, and empirical defect-verification suites pass 100% under the Go race detector from `/usr/local/google/home/kislayk/gitproj/go-lru2`:

```bash
# 1. Run full unit and adversarial test suites under the Go race detector (100% pass, 0 data races):
go test -race -count=1 ./...
(cd otellru && go test -race -count=1 ./...)

# 2. Run empirical defect & adversarial verification suites (100% pass across all TestDefect_* and TestAdversarial_* tests):
go test -race -count=1 -v -run "TestDefect_|TestAdversarial_" .
(cd otellru && go test -race -count=1 -v -run "TestDefect_|TestAdversarial_" .)

# 3. Verify zero steady-state heap allocations on all hot paths (0 allocs/op):
go test -v -run "TestAdversarial_AllocationCharacterization_PathCompressionAndDeepKeys/ZeroAllocSteadyStateHotPaths" .

# 4. Verify custom PressureFunc write latency speedup (>50x faster than pre-fix baseline, 0 allocs/op):
go test -run='^$' -bench='^BenchmarkCustomPressureFuncOverhead' -benchmem -benchtime=25ms .

# 5. Run golangci-lint across root and otellru (0 issues):
/usr/local/google/home/kislayk/go/bin/golangci-lint run ./...
(cd otellru && /usr/local/google/home/kislayk/go/bin/golangci-lint run ./...)
```

