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
	"fmt"
	"math"
	"slices"
	"strings"
)

func formatMetricValue(unit string, val float64) string {
	if math.IsNaN(val) {
		return "missing"
	}
	if unit == UnitAllocsPerOp || unit == UnitBytesPerOp || unit == UnitReclaimedBytesOp {
		if math.Abs(val-math.Round(val)) < 1e-6 {
			return fmt.Sprintf("%.0f", math.Round(val))
		}
	}
	if math.Abs(val-math.Round(val)) < 1e-9 {
		return fmt.Sprintf("%.0f", math.Round(val))
	}
	if math.Abs(val) >= 100 {
		return fmt.Sprintf("%.1f", val)
	}
	return fmt.Sprintf("%.2f", val)
}

func overallPassed(targetRep *TargetReport, cmpRep *ComparisonReport) bool {
	return targetRep.Passed() && cmpRep.Passed()
}

// FormatMarkdown renders the target and regression evaluation results as GitHub-Flavored Markdown.
func FormatMarkdown(
	cand *Suite,
	agg AggregationMode,
	targetRep *TargetReport,
	cmpRep *ComparisonReport,
) string {
	var b strings.Builder
	passed := overallPassed(targetRep, cmpRep)

	writeMarkdownSummary(&b, passed, cand, agg, targetRep, cmpRep)
	if !passed {
		writeMarkdownFailures(&b, targetRep, cmpRep)
	}
	writeMarkdownCandidateTable(&b, cand, targetRep)
	if cmpRep != nil {
		writeMarkdownComparisonTable(&b, cmpRep)
	}

	return b.String()
}

func writeMarkdownSummary(
	b *strings.Builder,
	passed bool,
	cand *Suite,
	agg AggregationMode,
	targetRep *TargetReport,
	cmpRep *ComparisonReport,
) {
	if passed {
		b.WriteString("## ✅ Performance Benchmark & Target Check: PASSED\n\n")
	} else {
		b.WriteString("## ❌ Performance Benchmark & Target Check: FAILED\n\n")
	}

	_, _ = fmt.Fprintf(b, "- **Candidate Benchmarks Evaluated**: `%d` (aggregation: `%s`)\n", len(cand.Order), agg)
	if targetRep != nil {
		_, _ = fmt.Fprintf(
			b,
			"- **Absolute Target Enforcement**: `%d/%d` matched, `%d` violation(s)\n",
			targetRep.Matched, targetRep.Evaluated, len(targetRep.Violations),
		)
	}
	if cmpRep != nil {
		_, _ = fmt.Fprintf(
			b,
			"- **Relative Regression Check**: `%d` matched, `%d` regression(s), `%d` new, `%d` missing\n",
			len(cmpRep.MatchedBenchmarks),
			len(cmpRep.Regressions),
			len(cmpRep.NewInCandidate),
			len(cmpRep.MissingInCandidate),
		)
	}
	b.WriteString("\n")
}

func writeMarkdownFailures(b *strings.Builder, targetRep *TargetReport, cmpRep *ComparisonReport) {
	b.WriteString("### ❌ Detected Failures\n\n")
	b.WriteString("| Check Type | Benchmark | Metric | Baseline / Target | Candidate | Details |\n")
	b.WriteString("|---|---|---|---:|---:|---|\n")

	if targetRep != nil {
		for _, v := range targetRep.Violations {
			targetStr := fmt.Sprintf("%s %s", v.Operator, formatMetricValue(v.Metric, v.Limit))
			candStr := formatMetricValue(v.Metric, v.Actual)
			_, _ = fmt.Fprintf(
				b,
				"| Target Violation | `%s` | `%s` | `%s` | `%s` | %s |\n",
				v.Benchmark, v.Metric, targetStr, candStr, v.Message,
			)
		}
		if targetRep.StrictUnmatched {
			for _, name := range targetRep.Unmatched {
				_, _ = fmt.Fprintf(
					b,
					"| Unmatched Target | `%s` | `-` | `matched rule` | `none` | benchmark did not match any target rule |\n",
					name,
				)
			}
		}
	}

	if cmpRep != nil {
		for _, reg := range cmpRep.Regressions {
			baseStr := formatMetricValue(reg.Unit, reg.BaseValue)
			candStr := formatMetricValue(reg.Unit, reg.CandValue)
			_, _ = fmt.Fprintf(
				b,
				"| Relative Regression | `%s` | `%s` | `%s` | `%s` | %s |\n",
				reg.Benchmark, reg.Unit, baseStr, candStr, reg.Message,
			)
		}
		if cmpRep.Config.StrictMissing {
			for _, name := range cmpRep.MissingInCandidate {
				_, _ = fmt.Fprintf(
					b,
					"| Missing Benchmark | `%s` | `-` | `present` | `missing` | baseline benchmark missing from candidate |\n",
					name,
				)
			}
		}
	}
	b.WriteString("\n")
}

func writeMarkdownCandidateTable(b *strings.Builder, cand *Suite, targetRep *TargetReport) {
	b.WriteString("### 🎯 Candidate Benchmark & Target Summary\n\n")
	b.WriteString("| Status | Benchmark | Samples | Throughput (`ns/op`) | Memory (`B/op`) | Allocations (`allocs/op`) | Custom Metrics | Target Budget |\n")
	b.WriteString("|---|---|---:|---:|---:|---:|---|---|\n")

	for _, name := range cand.Order {
		res := cand.Results[name]
		status, budget := candidateRowStatusAndBudget(name, targetRep)
		nsStr := formatOptionalMetric(res.Metrics, UnitNsPerOp)
		bytesStr := formatOptionalMetric(res.Metrics, UnitBytesPerOp)
		allocsStr := formatOptionalMetric(res.Metrics, UnitAllocsPerOp)
		customStr := formatCustomMetrics(res.Metrics)

		_, _ = fmt.Fprintf(
			b,
			"| %s | `%s` | %d | %s | %s | %s | %s | %s |\n",
			status, name, len(res.Samples), nsStr, bytesStr, allocsStr, customStr, budget,
		)
	}
	b.WriteString("\n")
}

func candidateRowStatusAndBudget(name string, targetRep *TargetReport) (status, budget string) {
	status = "✅ PASS"
	budget = "-"
	if targetRep == nil {
		return status, budget
	}
	eval, ok := targetRep.Evaluations[name]
	if !ok {
		return status, budget
	}
	if eval.BudgetSummary != "" {
		budget = eval.BudgetSummary
	}
	if len(eval.Violations) > 0 || (targetRep.StrictUnmatched && len(eval.MatchedRules) == 0) {
		status = "❌ FAIL"
	}
	return status, budget
}

func writeMarkdownComparisonTable(b *strings.Builder, cmpRep *ComparisonReport) {
	b.WriteString("### 🔍 Baseline vs. Candidate Regression Comparison\n\n")
	b.WriteString("| Status | Benchmark | `ns/op` (Base → Head) | `B/op` (Base → Head) | `allocs/op` (Base → Head) | Custom Metrics (Base → Head) |\n")
	b.WriteString("|---|---|---|---|---|---|\n")

	for _, name := range cmpRep.MatchedBenchmarks {
		cmp := cmpRep.Comparisons[name]
		status := "✅ PASS"
		if cmp.Regressed {
			status = "❌ REGRESSED"
		}
		nsCell := formatDeltaCell(cmp.Deltas, UnitNsPerOp)
		bytesCell := formatDeltaCell(cmp.Deltas, UnitBytesPerOp)
		allocsCell := formatDeltaCell(cmp.Deltas, UnitAllocsPerOp)
		customCell := formatCustomDeltaCells(cmp)

		_, _ = fmt.Fprintf(
			b,
			"| %s | `%s` | %s | %s | %s | %s |\n",
			status, name, nsCell, bytesCell, allocsCell, customCell,
		)
	}
	b.WriteString("\n")
}

// FormatText renders the target and regression evaluation results as plain text.
func FormatText(
	cand *Suite,
	agg AggregationMode,
	targetRep *TargetReport,
	cmpRep *ComparisonReport,
) string {
	var b strings.Builder
	passed := overallPassed(targetRep, cmpRep)

	verdict := "PASSED"
	if !passed {
		verdict = "FAILED"
	}
	_, _ = fmt.Fprintf(&b, "Performance Benchmark & Target Check: %s\n", verdict)
	_, _ = fmt.Fprintf(&b, "Candidate benchmarks evaluated: %d (aggregation: %s)\n", len(cand.Order), agg)

	if targetRep != nil {
		_, _ = fmt.Fprintf(
			&b,
			"Target check: %d/%d matched, %d violation(s)\n",
			targetRep.Matched, targetRep.Evaluated, len(targetRep.Violations),
		)
		for _, v := range targetRep.Violations {
			_, _ = fmt.Fprintf(&b, "  [TARGET FAIL] %s (%s): %s\n", v.Benchmark, v.Metric, v.Message)
		}
		if targetRep.StrictUnmatched {
			for _, name := range targetRep.Unmatched {
				_, _ = fmt.Fprintf(&b, "  [UNMATCHED TARGET] %s: did not match any target rule\n", name)
			}
		}
	}

	if cmpRep != nil {
		_, _ = fmt.Fprintf(
			&b,
			"Regression check: %d matched, %d regression(s), %d new, %d missing\n",
			len(cmpRep.MatchedBenchmarks),
			len(cmpRep.Regressions),
			len(cmpRep.NewInCandidate),
			len(cmpRep.MissingInCandidate),
		)
		for _, reg := range cmpRep.Regressions {
			_, _ = fmt.Fprintf(&b, "  [REGRESSION] %s (%s): %s\n", reg.Benchmark, reg.Unit, reg.Message)
		}
		if cmpRep.Config.StrictMissing {
			for _, name := range cmpRep.MissingInCandidate {
				_, _ = fmt.Fprintf(&b, "  [MISSING BENCHMARK] %s: missing from candidate\n", name)
			}
		}
	}

	return b.String()
}

func formatOptionalMetric(metrics map[string]float64, unit string) string {
	v, ok := metrics[unit]
	if !ok {
		return "-"
	}
	return fmt.Sprintf("%s %s", formatMetricValue(unit, v), unit)
}

func formatCustomMetrics(metrics map[string]float64) string {
	var keys []string
	for k := range metrics {
		if k == UnitNsPerOp || k == UnitBytesPerOp || k == UnitAllocsPerOp {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return "-"
	}
	slices.Sort(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s %s", formatMetricValue(k, metrics[k]), k))
	}
	return strings.Join(parts, ", ")
}

func formatDeltaCell(deltas map[string]MetricDelta, unit string) string {
	d, ok := deltas[unit]
	if !ok {
		return "-"
	}
	pctStr := formatRelChange(d)
	return fmt.Sprintf(
		"%s → %s (%s)",
		formatMetricValue(unit, d.BaseValue),
		formatMetricValue(unit, d.CandValue),
		pctStr,
	)
}

func formatCustomDeltaCells(cmp *BenchmarkComparison) string {
	var parts []string
	for _, unit := range cmp.Order {
		if unit == UnitNsPerOp || unit == UnitBytesPerOp || unit == UnitAllocsPerOp {
			continue
		}
		d := cmp.Deltas[unit]
		parts = append(parts, fmt.Sprintf(
			"%s: %s → %s (%s)",
			unit,
			formatMetricValue(unit, d.BaseValue),
			formatMetricValue(unit, d.CandValue),
			formatRelChange(d),
		))
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, "; ")
}

func formatRelChange(d MetricDelta) string {
	if math.IsInf(d.RelChange, 1) {
		return fmt.Sprintf("+%s", formatMetricValue(d.Unit, d.AbsDelta))
	}
	if math.Abs(d.RelChange) < 1e-6 {
		return "0.0%"
	}
	return fmt.Sprintf("%+.1f%%", d.RelChange*100)
}
