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

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
)

// TargetRule defines absolute performance and resource allocation bounds for one or more benchmarks.
// Pattern supports exact benchmark names, trailing wildcard prefixes ("Benchmark_Get_MapCache/*"),
// or regular expressions starting with "^".
type TargetRule struct {
	Pattern             string   `json:"pattern"`
	Description         string   `json:"description,omitempty"`
	MaxNsPerOp          *float64 `json:"max_ns_per_op,omitempty"`
	MaxBytesPerOp       *float64 `json:"max_bytes_per_op,omitempty"`
	MaxAllocsPerOp      *float64 `json:"max_allocs_per_op,omitempty"`
	MaxHeapBytesPerEnt  *float64 `json:"max_heap_bytes_per_entry,omitempty"`
	MinReclaimedBytesOp *float64 `json:"min_reclaimed_bytes_per_op,omitempty"`
}

// TargetViolation records a single absolute target failure for a benchmark metric.
type TargetViolation struct {
	Benchmark   string
	RulePattern string
	Metric      string
	Operator    string
	Limit       float64
	Actual      float64
	Missing     bool
	Message     string
}

// BenchmarkTargetEval summarizes target rule matching and violations for a single benchmark.
type BenchmarkTargetEval struct {
	Benchmark     string
	MatchedRules  []string
	BudgetSummary string
	Violations    []TargetViolation
}

// TargetReport holds the aggregate result of checking a benchmark suite against target rules.
type TargetReport struct {
	Evaluated       int
	Matched         int
	Unmatched       []string
	StrictUnmatched bool
	Evaluations     map[string]*BenchmarkTargetEval
	Violations      []TargetViolation
}

// Passed reports whether all target rules were satisfied (and no unmatched benchmarks
// occurred when StrictUnmatched is enabled).
func (r *TargetReport) Passed() bool {
	if r == nil {
		return true
	}
	if len(r.Violations) > 0 {
		return false
	}
	if r.StrictUnmatched && len(r.Unmatched) > 0 {
		return false
	}
	return true
}

// DefaultTargets returns the calibrated performance and allocation target rules
// grounded in docs/performance.md and covering all 89 benchmarks in benchmarks_test.go.
func DefaultTargets() []TargetRule {
	return []TargetRule{
		// 1–3. Point Get (9 benchmarks: strict 0 B/op, 0 allocs/op)
		{
			Pattern:        "Benchmark_Get_MapCache/*",
			Description:    "MapCache point Get (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(500)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_Get_RadixCache/*",
			Description:    "RadixCache point Get (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(1200)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_Get_ArenaRadixCache/*",
			Description:    "ArenaRadixCache point Get (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(1200)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		// 4–6. Point Peek (3 benchmarks: strict 0 B/op, 0 allocs/op)
		{
			Pattern:        "Benchmark_Peek_MapCache",
			Description:    "MapCache point Peek (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(400)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_Peek_RadixCache",
			Description:    "RadixCache point Peek (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(1000)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_Peek_ArenaRadixCache",
			Description:    "ArenaRadixCache point Peek (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(800)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		// 7. Replace (12 benchmarks: weighted, unit-weight, WithOnEvictValue, WithOnEvictEntry: strict 0 B/op, 0 allocs/op)
		{
			Pattern:        "Benchmark_Replace_*",
			Description:    "In-place Replace across all backends and callbacks (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(1000)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		// 8–13. Sequential Put (18 benchmarks: 50% capacity turnover, unit-weight warmup, and eviction callbacks)
		{
			Pattern:        "Benchmark_Put_MapCache/*",
			Description:    "MapCache 50%-turnover Put (<= 3 allocs/op)",
			MaxNsPerOp:     new(float64(2500)),
			MaxBytesPerOp:  new(float64(256)),
			MaxAllocsPerOp: new(float64(3)),
		},
		{
			Pattern:        "Benchmark_Put_RadixCache/*",
			Description:    "RadixCache 50%-turnover Put (<= 3 allocs/op)",
			MaxNsPerOp:     new(float64(3000)),
			MaxBytesPerOp:  new(float64(256)),
			MaxAllocsPerOp: new(float64(3)),
		},
		{
			Pattern:        "Benchmark_Put_ArenaRadixCache/*",
			Description:    "ArenaRadixCache 50%-turnover Put with free-list node recycling (<= 2 allocs/op)",
			MaxNsPerOp:     new(float64(4000)),
			MaxBytesPerOp:  new(float64(256)),
			MaxAllocsPerOp: new(float64(2)),
		},
		{
			Pattern:        "Benchmark_Put_UnitWeight_*",
			Description:    "Unit-weight in-place Put after 10K warmup (0 allocs/op)",
			MaxNsPerOp:     new(float64(2000)),
			MaxBytesPerOp:  new(float64(256)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_Put_WithOnEvictValue/*",
			Description:    "50%-turnover Put with OnEvictValue callback (<= 3 allocs/op)",
			MaxNsPerOp:     new(float64(3500)),
			MaxBytesPerOp:  new(float64(256)),
			MaxAllocsPerOp: new(float64(3)),
		},
		{
			Pattern:        "Benchmark_Put_WithOnEvictEntry/*",
			Description:    "50%-turnover Put with OnEvictEntry callback (<= 4 allocs/op)",
			MaxNsPerOp:     new(float64(4000)),
			MaxBytesPerOp:  new(float64(256)),
			MaxAllocsPerOp: new(float64(4)),
		},
		// 14–16. Individual Delete (3 benchmarks: 0 allocs/op)
		{
			Pattern:        "Benchmark_Delete_MapCache",
			Description:    "MapCache Delete (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(2000)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_Delete_RadixCache",
			Description:    "RadixCache Delete (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(2000)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_Delete_ArenaRadixCache",
			Description:    "ArenaRadixCache Delete (0 allocs/op)",
			MaxNsPerOp:     new(float64(2000)),
			MaxBytesPerOp:  new(float64(32)),
			MaxAllocsPerOp: new(float64(0)),
		},
		// 17–19. Subtree DeletePrefix (9 benchmarks: 0 allocs/op, O(P+S) radix speedup)
		{
			Pattern:        "Benchmark_DeletePrefix_MapCache/*",
			Description:    "MapCache O(N) DeletePrefix (0 allocs/op)",
			MaxNsPerOp:     new(float64(1000000)),
			MaxBytesPerOp:  new(float64(64)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_DeletePrefix_RadixCache/*",
			Description:    "RadixCache O(P+S) subtree DeletePrefix (0 allocs/op)",
			MaxNsPerOp:     new(float64(50000)),
			MaxBytesPerOp:  new(float64(64)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_DeletePrefix_ArenaRadixCache/*",
			Description:    "ArenaRadixCache O(P+S) subtree DeletePrefix (0 allocs/op)",
			MaxNsPerOp:     new(float64(80000)),
			MaxBytesPerOp:  new(float64(64)),
			MaxAllocsPerOp: new(float64(0)),
		},
		// 20–23. Parallel Throughput (12 benchmarks: 0 allocs/op)
		{
			Pattern:        "Benchmark_ParallelThroughput_PeekOnly/*",
			Description:    "Parallel RLock PeekOnly throughput (0 allocs/op)",
			MaxNsPerOp:     new(float64(1000)),
			MaxBytesPerOp:  new(float64(16)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_ParallelThroughput_ReadHeavy/*",
			Description:    "Parallel ReadHeavy throughput (0 allocs/op)",
			MaxNsPerOp:     new(float64(5000)),
			MaxBytesPerOp:  new(float64(64)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_ParallelThroughput_Mixed/*",
			Description:    "Parallel Mixed throughput (0 allocs/op)",
			MaxNsPerOp:     new(float64(5000)),
			MaxBytesPerOp:  new(float64(128)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_ParallelThroughput_WriteHeavy/*",
			Description:    "Parallel WriteHeavy throughput (0 allocs/op)",
			MaxNsPerOp:     new(float64(6000)),
			MaxBytesPerOp:  new(float64(512)),
			MaxAllocsPerOp: new(float64(0)),
		},
		// 24–26. LargeScale Put 100K (3 benchmarks with heap-B/entry bounds)
		{
			Pattern:            "Benchmark_LargeScale_Put_100K/MapCache",
			Description:        "100K MapCache Put scale & heap-B/entry budget",
			MaxNsPerOp:         new(float64(250000000)),
			MaxBytesPerOp:      new(float64(25000000)),
			MaxAllocsPerOp:     new(float64(250000)),
			MaxHeapBytesPerEnt: new(220.0),
		},
		{
			Pattern:            "Benchmark_LargeScale_Put_100K/RadixCache",
			Description:        "100K RadixCache Put scale & heap-B/entry budget",
			MaxNsPerOp:         new(float64(250000000)),
			MaxBytesPerOp:      new(float64(16000000)),
			MaxAllocsPerOp:     new(float64(120000)),
			MaxHeapBytesPerEnt: new(140.0),
		},
		{
			Pattern:            "Benchmark_LargeScale_Put_100K/ArenaRadixCache",
			Description:        "100K ArenaRadixCache Put scale, chunk allocs & heap-B/entry budget",
			MaxNsPerOp:         new(float64(300000000)),
			MaxBytesPerOp:      new(float64(55000000)),
			MaxAllocsPerOp:     new(float64(2000)),
			MaxHeapBytesPerEnt: new(150.0),
		},
		// 27–29. LargeScale DeletePrefix 100K (3 benchmarks)
		{
			Pattern:        "Benchmark_LargeScale_DeletePrefix_100K/MapCache",
			Description:    "100K MapCache DeletePrefix",
			MaxNsPerOp:     new(float64(150000000)),
			MaxBytesPerOp:  new(float64(1024)),
			MaxAllocsPerOp: new(float64(10)),
		},
		{
			Pattern:        "Benchmark_LargeScale_DeletePrefix_100K/RadixCache",
			Description:    "100K RadixCache O(P+S) DeletePrefix (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(25000000)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_LargeScale_DeletePrefix_100K/ArenaRadixCache",
			Description:    "100K ArenaRadixCache O(P+S) DeletePrefix (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(40000000)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		// 30–31. Arena Compaction & Pressure (2 benchmarks, including reclaimed-B/op floor)
		{
			Pattern:             "Benchmark_ArenaRadixCache_Compact",
			Description:         "ArenaRadixCache Compact latency and minimum reclaimed-B/op floor",
			MaxNsPerOp:          new(float64(10000000)),
			MaxBytesPerOp:       new(float64(1500000)),
			MaxAllocsPerOp:      new(float64(25000)),
			MinReclaimedBytesOp: new(400000.0),
		},
		{
			Pattern:        "Benchmark_ArenaRadixCache_PutUnderPressure",
			Description:    "ArenaRadixCache Put under critical memory pressure",
			MaxNsPerOp:     new(float64(80000)),
			MaxBytesPerOp:  new(float64(1024)),
			MaxAllocsPerOp: new(float64(4)),
		},
		// 32–38. Range Iterators (9 benchmarks: MapCache & Values 0 allocs/op; Radix/Arena All/Keys <= 1000 allocs/op)
		{
			Pattern:        "Benchmark_All_MapCache",
			Description:    "MapCache All() iterator over 1,000 entries (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(40000)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_All_RadixCache",
			Description:    "RadixCache All() iterator over 1,000 entries (1 key alloc/entry)",
			MaxNsPerOp:     new(float64(400000)),
			MaxBytesPerOp:  new(float64(32000)),
			MaxAllocsPerOp: new(float64(1000)),
		},
		{
			Pattern:        "Benchmark_All_ArenaRadixCache",
			Description:    "ArenaRadixCache All() iterator over 1,000 entries (1 key alloc/entry)",
			MaxNsPerOp:     new(float64(400000)),
			MaxBytesPerOp:  new(float64(32000)),
			MaxAllocsPerOp: new(float64(1000)),
		},
		{
			Pattern:        "Benchmark_Keys_MapCache",
			Description:    "MapCache Keys() iterator over 1,000 entries (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(40000)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		{
			Pattern:        "Benchmark_Keys_RadixCache",
			Description:    "RadixCache Keys() iterator over 1,000 entries (1 key alloc/entry)",
			MaxNsPerOp:     new(float64(400000)),
			MaxBytesPerOp:  new(float64(32000)),
			MaxAllocsPerOp: new(float64(1000)),
		},
		{
			Pattern:        "Benchmark_Keys_ArenaRadixCache",
			Description:    "ArenaRadixCache Keys() iterator over 1,000 entries (1 key alloc/entry)",
			MaxNsPerOp:     new(float64(400000)),
			MaxBytesPerOp:  new(float64(32000)),
			MaxAllocsPerOp: new(float64(1000)),
		},
		{
			Pattern:        "Benchmark_Values_*",
			Description:    "Values() iterator across all backends (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(40000)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
		// 39. Stats() Snapshot (6 benchmarks: Sequential & Parallel across all 3 backends, 0 allocs/op, 0 B/op)
		{
			Pattern:        "Benchmark_Stats_*",
			Description:    "Stats() snapshot across all backends (0 allocs/op, 0 B/op)",
			MaxNsPerOp:     new(float64(600)),
			MaxBytesPerOp:  new(float64(0)),
			MaxAllocsPerOp: new(float64(0)),
		},
	}
}

// LoadTargets reads and validates a custom JSON target rules file.
func LoadTargets(path string) ([]TargetRule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read targets file %q: %w", path, err)
	}
	var rules []TargetRule
	if err := json.Unmarshal(data, &rules); err != nil {
		return nil, fmt.Errorf("failed to parse targets JSON %q: %w", path, err)
	}
	if len(rules) == 0 {
		return nil, errors.New("targets file contains no rules")
	}
	for i, r := range rules {
		if strings.TrimSpace(r.Pattern) == "" {
			return nil, fmt.Errorf("rule %d has empty pattern", i)
		}
		if strings.HasPrefix(r.Pattern, "^") {
			if _, err := regexp.Compile(r.Pattern); err != nil {
				return nil, fmt.Errorf("rule %d has invalid regex pattern %q: %w", i, r.Pattern, err)
			}
		}
	}
	return rules, nil
}

// CheckTargets evaluates all benchmarks in suite against rules.
func CheckTargets(suite *Suite, rules []TargetRule, strictUnmatched bool) (*TargetReport, error) {
	if suite == nil || len(suite.Results) == 0 {
		return nil, ErrNoBenchmarksFound
	}
	if len(rules) == 0 {
		return nil, errors.New("no target rules provided")
	}

	compiledRegex := make(map[string]*regexp.Regexp)
	for _, r := range rules {
		if strings.HasPrefix(r.Pattern, "^") {
			re, err := regexp.Compile(r.Pattern)
			if err != nil {
				return nil, fmt.Errorf("invalid regex pattern %q: %w", r.Pattern, err)
			}
			compiledRegex[r.Pattern] = re
		}
	}

	report := &TargetReport{
		Evaluated:       len(suite.Order),
		StrictUnmatched: strictUnmatched,
		Evaluations:     make(map[string]*BenchmarkTargetEval, len(suite.Order)),
	}

	for _, name := range suite.Order {
		res := suite.Results[name]
		eval := &BenchmarkTargetEval{
			Benchmark: name,
		}
		report.Evaluations[name] = eval

		var budgetParts []string
		for _, rule := range rules {
			if !matchTargetPattern(name, rule.Pattern, compiledRegex[rule.Pattern]) {
				continue
			}
			eval.MatchedRules = append(eval.MatchedRules, rule.Pattern)
			if s := summarizeRuleBudget(rule); s != "" {
				budgetParts = append(budgetParts, s)
			}

			violations := evaluateRule(res, rule)
			eval.Violations = append(eval.Violations, violations...)
			report.Violations = append(report.Violations, violations...)
		}

		eval.BudgetSummary = strings.Join(budgetParts, "; ")
		if len(eval.MatchedRules) > 0 {
			report.Matched++
		} else {
			report.Unmatched = append(report.Unmatched, name)
		}
	}

	return report, nil
}

func matchTargetPattern(benchName, pattern string, compiled *regexp.Regexp) bool {
	if strings.HasPrefix(pattern, "^") {
		if compiled != nil {
			return compiled.MatchString(benchName)
		}
		matched, err := regexp.MatchString(pattern, benchName)
		return err == nil && matched
	}
	if before, ok := strings.CutSuffix(pattern, "*"); ok {
		prefix := before
		return strings.HasPrefix(benchName, prefix)
	}
	return benchName == pattern
}

func evaluateRule(res *BenchmarkResult, rule TargetRule) []TargetViolation {
	var violations []TargetViolation

	checkUpper := func(metric string, limit *float64) {
		if limit == nil {
			return
		}
		actual, ok := res.Metrics[metric]
		if !ok {
			violations = append(violations, TargetViolation{
				Benchmark:   res.Name,
				RulePattern: rule.Pattern,
				Metric:      metric,
				Operator:    "<=",
				Limit:       *limit,
				Actual:      math.NaN(),
				Missing:     true,
				Message:     fmt.Sprintf("missing required metric %s (target <= %s)", metric, formatMetricValue(metric, *limit)),
			})
			return
		}
		if actual > *limit {
			violations = append(violations, TargetViolation{
				Benchmark:   res.Name,
				RulePattern: rule.Pattern,
				Metric:      metric,
				Operator:    "<=",
				Limit:       *limit,
				Actual:      actual,
				Message:     fmt.Sprintf("%s %s exceeds target <= %s", formatMetricValue(metric, actual), metric, formatMetricValue(metric, *limit)),
			})
		}
	}

	checkLower := func(metric string, limit *float64) {
		if limit == nil {
			return
		}
		actual, ok := res.Metrics[metric]
		if !ok {
			violations = append(violations, TargetViolation{
				Benchmark:   res.Name,
				RulePattern: rule.Pattern,
				Metric:      metric,
				Operator:    ">=",
				Limit:       *limit,
				Actual:      math.NaN(),
				Missing:     true,
				Message:     fmt.Sprintf("missing required metric %s (target >= %s)", metric, formatMetricValue(metric, *limit)),
			})
			return
		}
		if actual < *limit {
			violations = append(violations, TargetViolation{
				Benchmark:   res.Name,
				RulePattern: rule.Pattern,
				Metric:      metric,
				Operator:    ">=",
				Limit:       *limit,
				Actual:      actual,
				Message:     fmt.Sprintf("%s %s is below target >= %s", formatMetricValue(metric, actual), metric, formatMetricValue(metric, *limit)),
			})
		}
	}

	checkUpper(UnitNsPerOp, rule.MaxNsPerOp)
	checkUpper(UnitBytesPerOp, rule.MaxBytesPerOp)
	checkUpper(UnitAllocsPerOp, rule.MaxAllocsPerOp)
	checkUpper(UnitHeapBytesPerEnt, rule.MaxHeapBytesPerEnt)
	checkLower(UnitReclaimedBytesOp, rule.MinReclaimedBytesOp)

	return violations
}

func summarizeRuleBudget(rule TargetRule) string {
	var parts []string
	if rule.MaxNsPerOp != nil {
		parts = append(parts, fmt.Sprintf("<= %s ns/op", formatMetricValue(UnitNsPerOp, *rule.MaxNsPerOp)))
	}
	if rule.MaxBytesPerOp != nil {
		parts = append(parts, fmt.Sprintf("<= %s B/op", formatMetricValue(UnitBytesPerOp, *rule.MaxBytesPerOp)))
	}
	if rule.MaxAllocsPerOp != nil {
		parts = append(parts, fmt.Sprintf("<= %s allocs/op", formatMetricValue(UnitAllocsPerOp, *rule.MaxAllocsPerOp)))
	}
	if rule.MaxHeapBytesPerEnt != nil {
		parts = append(parts, fmt.Sprintf("<= %s heap-B/entry", formatMetricValue(UnitHeapBytesPerEnt, *rule.MaxHeapBytesPerEnt)))
	}
	if rule.MinReclaimedBytesOp != nil {
		parts = append(parts, fmt.Sprintf(">= %s reclaimed-B/op", formatMetricValue(UnitReclaimedBytesOp, *rule.MinReclaimedBytesOp)))
	}
	return strings.Join(parts, ", ")
}
