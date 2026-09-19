// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lrus

import (
	"math"
	"runtime/metrics"
)

// Default thresholds for two-tier memory-pressure reclamation.
const (
	// DefaultCompactionThreshold is the normalized memory pressure [0.0, 1.0+]
	// at or above which Moderate Pressure (Tier 1) lossless arena & map compaction is triggered.
	DefaultCompactionThreshold = 0.75

	// DefaultEvictionThreshold is the normalized memory pressure [0.0, 1.0+]
	// at or above which Critical Pressure (Tier 2) proactive LRU shedding + compaction is triggered.
	DefaultEvictionThreshold = 0.90

	// DefaultEvictionRetentionRatio is the target fraction [0.0, 1.0] of currentSize
	// to retain when shedding LRU entries under Critical Pressure.
	DefaultEvictionRetentionRatio = 0.50
)

// PressureFunc returns a normalized memory pressure reading in [0.0, 1.0+],
// where 0.0 indicates no memory pressure, values in [CompactionThreshold, EvictionThreshold)
// indicate moderate pressure, and values >= EvictionThreshold indicate critical pressure.
type PressureFunc func() float64

// Options contains configuration parameters for Cache instances.
type Options struct {
	// EnableInvariantChecking enables internal data structure integrity and invariant validation.
	// When enabled, cache operations execute comprehensive validation checks (e.g. bidirectional pointer
	// consistency, tree structure validity, size accounting parity) and panic if corruption is detected.
	// Intended primarily for testing and debugging.
	// Note: Fundamental constructor preconditions (such as requiring maxSize > 0) are enforced
	// unconditionally regardless of this setting.
	EnableInvariantChecking bool

	// PressureFunc is an optional custom callback returning normalized memory pressure in [0.0, 1.0+].
	// If nil, DefaultRuntimePressureFunc(MemoryBudget) is used.
	PressureFunc PressureFunc

	// MemoryBudget specifies an optional memory limit in bytes for the default runtime/metrics
	// pressure probe. If 0, the probe falls back to the Go runtime's GOMEMLIMIT (/gc/gomemlimit:bytes).
	MemoryBudget uint64

	// CompactionThreshold specifies the normalized pressure threshold for Tier 1 lossless compaction.
	// Defaults to DefaultCompactionThreshold (0.75) if <= 0.
	CompactionThreshold float64

	// EvictionThreshold specifies the normalized pressure threshold for Tier 2 LRU shedding + compaction.
	// Defaults to DefaultEvictionThreshold (0.90) if <= 0.
	EvictionThreshold float64

	// EvictionRetentionRatio specifies the fraction [0.0, 1.0] of tracked currentSize to retain
	// when Critical Pressure (EvictionThreshold) is reached.
	// Defaults to DefaultEvictionRetentionRatio (0.50) if unset.
	EvictionRetentionRatio float64
}

// Option is a functional option for configuring a Cache instance.
type Option func(*Options)

// WithInvariantChecking returns an Option that enables or disables internal invariant checking.
func WithInvariantChecking(enabled bool) Option {
	return func(o *Options) {
		o.EnableInvariantChecking = enabled
	}
}

// WithPressureFunc configures a custom memory-pressure probe function.
func WithPressureFunc(fn PressureFunc) Option {
	return func(o *Options) {
		o.PressureFunc = fn
	}
}

// WithMemoryBudget configures a custom memory budget (in bytes) for the built-in runtime/metrics probe.
func WithMemoryBudget(bytes uint64) Option {
	return func(o *Options) {
		o.MemoryBudget = bytes
	}
}

// WithCompactionThreshold configures the Moderate Pressure threshold for lossless arena/map compaction.
func WithCompactionThreshold(threshold float64) Option {
	return func(o *Options) {
		o.CompactionThreshold = threshold
	}
}

// WithEvictionThreshold configures the Critical Pressure threshold for proactive LRU shedding.
func WithEvictionThreshold(threshold float64) Option {
	return func(o *Options) {
		o.EvictionThreshold = threshold
	}
}

// WithEvictionRetentionRatio configures the target retention ratio [0.0, 1.0] of currentSize
// retained during Critical Pressure LRU shedding.
func WithEvictionRetentionRatio(ratio float64) Option {
	return func(o *Options) {
		o.EvictionRetentionRatio = ratio
	}
}

// DefaultRuntimePressureFunc returns a lock-free, STW-free PressureFunc backed by runtime/metrics.
// It compares active Go runtime memory (/memory/classes/total:bytes - /memory/classes/heap/released:bytes)
// against memoryBudget (if > 0) or /gc/gomemlimit:bytes (if configured < math.MaxInt64).
func DefaultRuntimePressureFunc(memoryBudget uint64) PressureFunc {
	return func() float64 {
		var samples = [4]metrics.Sample{
			{Name: "/memory/classes/total:bytes"},
			{Name: "/memory/classes/heap/released:bytes"},
			{Name: "/memory/classes/heap/objects:bytes"},
			{Name: "/gc/gomemlimit:bytes"},
		}
		metrics.Read(samples[:])

		totalBytes := samples[0].Value.Uint64()
		releasedBytes := samples[1].Value.Uint64()
		heapObjectsBytes := samples[2].Value.Uint64()
		gomemlimit := samples[3].Value.Uint64()

		limitBytes := memoryBudget
		if limitBytes == 0 {
			if gomemlimit == 0 || gomemlimit >= uint64(math.MaxInt64) {
				return 0.0
			}
			limitBytes = gomemlimit
		}

		var usedBytes uint64
		if totalBytes > releasedBytes {
			usedBytes = totalBytes - releasedBytes
		} else {
			usedBytes = heapObjectsBytes
		}

		pressure := float64(usedBytes) / float64(limitBytes)
		if math.IsNaN(pressure) || pressure < 0.0 {
			return 0.0
		}
		return pressure
	}
}

// ApplyOptions parses and applies the provided slice of Option functions onto a default Options configuration.
func ApplyOptions(opts ...Option) Options {
	options := Options{
		CompactionThreshold:    DefaultCompactionThreshold,
		EvictionThreshold:      DefaultEvictionThreshold,
		EvictionRetentionRatio: DefaultEvictionRetentionRatio,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	if options.CompactionThreshold <= 0 {
		options.CompactionThreshold = DefaultCompactionThreshold
	}
	if options.EvictionThreshold <= 0 {
		options.EvictionThreshold = DefaultEvictionThreshold
	}
	if options.EvictionRetentionRatio < 0 {
		options.EvictionRetentionRatio = DefaultEvictionRetentionRatio
	} else if options.EvictionRetentionRatio > 1.0 {
		options.EvictionRetentionRatio = 1.0
	}
	if options.PressureFunc == nil {
		options.PressureFunc = DefaultRuntimePressureFunc(options.MemoryBudget)
	}
	return options
}

