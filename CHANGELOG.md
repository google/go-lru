# Changelog

All notable changes to `github.com/google/go-lru` (`package lru`) are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] - 2026-09-24

### Added
- **Standalone Module & Package Identity**: Published under `module github.com/google/go-lru` with idiomatic `package lru` import surface.
- **Unified Constructor & Configurable Backends**:
  - `lru.New(maxSize uint64, opts ...Option) Cache` entry point supporting `lru.WithBackend(backend)` across `BackendMap` (default), `BackendRadix`, and `BackendArenaRadix`.
  - Direct engine constructors `lru.NewMapCache`, `lru.NewRadixCache`, and `lru.NewArenaRadixCache`.
- **Ergonomic `ValueType` Wrappers (`values.go`)**:
  - `StringValue` (`NewStringValue`), `BytesValue` (`NewBytesValue`), and generic `SizedValue[T]` (`NewSizedValue[T]`, `NewValue[T]`) so callers can cache `string`, `[]byte`, and arbitrary types without boilerplate struct definitions.
- **Two-Tier Memory-Pressure Reclamation (`PressureAwareCache`)**:
  - `ArenaRadixCache` implements `PressureAwareCache` (`Compact()` and `EvaluateMemoryPressure()`).
  - Configurable via `WithPressureFunc`, `WithMemoryBudget`, `WithCompactionThreshold` (Tier 1 lossless compaction), `WithEvictionThreshold` (Tier 2 proactive LRU tail shedding + compaction), `WithEvictionRetentionRatio`, and `DefaultRuntimePressureFunc`.
- **Documentation & Runnable Examples**:
  - `example_test.go` with `pkg.go.dev` runnable examples (`ExampleNew`, `ExampleNew_eviction`, `ExampleNew_backendsAndPrefixErase`, `ExampleNew_memoryPressure`).
  - Concise user guide in `README.md` backed by `docs/architecture.md` and `docs/performance.md`.
- **Automated GitHub Actions CI**:
  - `.github/workflows/ci.yml`: formatting (`gofmt -s`), static analysis (`go vet`), `go mod tidy` verification, `-race` unit/differential/concurrency tests, runnable examples, and statement coverage reporting.
  - `.github/workflows/benchmarks.yml`: automated performance and resource footprint benchmarking (`ns/op`, `B/op`, `allocs/op`, `heap-B/entry`, `reclaimed-B/op`) with `benchstat` PR regression comparison.

---

## [0.1.0] - 2026-09-22

### Added
- Initial extraction of `MapCache`, `RadixCache`, and `ArenaRadixCache` from Google Cloud Storage FUSE (`gcsfuse`), including differential state-machine tests, multi-goroutine race stress tests, and `WithInvariantChecking` runtime structural verification.

---

## Release & Versioning Workflow

To cut a new semantic version release (`v0.1.0`, `v1.0.0`, etc.):

```bash
# 1. Verify formatting, static analysis, race-enabled test suite, and examples
test -z "$(gofmt -s -l .)"
go vet ./...
go test -race ./...
go test -v -run=^Example ./...

# 2. Verify benchmark & resource usage suite
go test -run=^$ -bench=. -benchmem -benchtime=10ms ./...

# 3. Create and push annotated semantic version tags
git tag -a v0.1.0 -m "Release v0.1.0"
git tag -a v1.0.0 -m "Release v1.0.0"
git push origin --tags
```

<!-- 202f0 -->
