# Changelog

All notable changes to `github.com/google/go-lru` (`package lru`) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **Eviction & Removal Callbacks (`WithOnEvictValue` & `WithOnEvictEntry`)**:
  - Added `EvictionReason` (`EvictionReasonCapacity`, `EvictionReasonPressure`, `EvictionReasonDeleted`, `EvictionReasonReplaced`) with `String()`.
  - Added `WithOnEvictValue[V any](func(value V, reason EvictionReason))` for zero-key-reconstruction value lifecycle notifications (e.g., buffer pool recycling) and `WithOnEvictEntry[V any](func(key string, value V, reason EvictionReason))` for full key+value notifications across `MapCache`, `RadixCache`, and `ArenaRadixCache`.

### Changed
- **Generic `Cache[V any]` & `PressureAwareCache[V any]` API**: Parameterized `Cache[V any]`, `PressureAwareCache[V any]`, `New[V any]`, `NewMapCache[V any]`, `NewRadixCache[V any]`, and `NewArenaRadixCache[V any]` by value type `V any`, eliminating wrapper interface boilerplate.
- **Configurable Entry Weighing (`WithWeigher`)**: Added `WithWeigher[V any](func(key string, value V) uint64)` with a default unit weight of `1` per entry when omitted.
- **Idiomatic Cache Method Vocabulary**: Standardized `Cache[V any]` operations to `Put`, `Delete`, `Get`, `Peek`, `Replace`, and `DeletePrefix`.

---

## [0.0.1] - 2026-04-20

### Added
- **Standalone Module & Package Identity**: Published under `module github.com/google/go-lru` with idiomatic `package lru` import surface.
- **Unified Constructor & Configurable Backends**:
  - `lru.New(maxSize uint64, opts ...Option) Cache` entry point supporting `lru.WithBackend(backend)` across `BackendMap` (default), `BackendRadix`, and `BackendArenaRadix`.
  - Direct engine constructors `lru.NewMapCache`, `lru.NewRadixCache`, and `lru.NewArenaRadixCache`, with differential state-machine tests, multi-goroutine race stress tests, and `WithInvariantChecking` runtime structural verification.
- **Ergonomic `ValueType` Wrappers (`values.go`)**:
  - `StringValue` (`NewStringValue`), `BytesValue` (`NewBytesValue`), and generic `SizedValue[T]` (`NewSizedValue[T]`, `NewValue[T]`) so callers can cache `string`, `[]byte`, and arbitrary types without boilerplate struct definitions.
- **Two-Tier Memory-Pressure Reclamation (`PressureAwareCache`)**:
  - All three backends (`MapCache`, `RadixCache`, and `ArenaRadixCache`) implement `PressureAwareCache` (`Compact()` and `EvaluateMemoryPressure()`).
  - Configurable via `WithPressureFunc`, `WithMemoryBudget`, `WithCompactionThreshold` (Tier 1 lossless compaction), `WithEvictionThreshold` (Tier 2 proactive LRU tail shedding + compaction), `WithEvictionRetentionRatio`, and `DefaultRuntimePressureFunc`.
- **Documentation & Runnable Examples**:
  - `example_test.go` with `pkg.go.dev` runnable examples (`ExampleNew`, `ExampleNew_eviction`, `ExampleNew_backendsAndDeletePrefix`, `ExampleNew_memoryPressure`).
  - Concise user guide in `README.md` backed by `docs/architecture.md` and `docs/performance.md`.
- **Automated GitHub Actions CI**:
  - `.github/workflows/ci.yml`: formatting (`gofmt -s`, `goimports`), static analysis (`go vet`, `golangci-lint`), `go mod tidy` verification, `-race` unit/differential/concurrency tests, runnable examples, and statement coverage reporting.
  - `.github/workflows/benchmarks.yml`: automated performance and resource footprint benchmarking (`ns/op`, `B/op`, `allocs/op`, `heap-B/entry`, `reclaimed-B/op`) with `benchstat` PR regression comparison.

---

## Release & Versioning Workflow

To cut a new semantic version release (e.g. `v0.0.1`):

```bash
# 1. Verify formatting, static analysis, race-enabled test suite, and examples
test -z "$(gofmt -s -l .)"
test -z "$(goimports -l .)"
go vet ./...
golangci-lint run
go test -race ./...
go test -v -run=^Example ./...

# 2. Verify benchmark & resource usage suite
go test -run=^$ -bench=. -benchmem -benchtime=10ms ./...

# 3. Create and push annotated semantic version tag
git tag -a v0.0.1 -m "Release v0.0.1"
git push origin --tags
```
