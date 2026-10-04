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
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
)

// Standard and custom Go benchmark metric units produced by testing.B.
const (
	UnitNsPerOp          = "ns/op"
	UnitBytesPerOp       = "B/op"
	UnitAllocsPerOp      = "allocs/op"
	UnitHeapBytesPerEnt  = "heap-B/entry"
	UnitReclaimedBytesOp = "reclaimed-B/op"
)

// AggregationMode specifies how multiple samples of the same benchmark (-count=N)
// are summarized into a single representative metric value.
type AggregationMode string

const (
	// AggregateMedian summarizes multi-sample runs using the sample median (default).
	AggregateMedian AggregationMode = "median"
	// AggregateMean summarizes multi-sample runs using the arithmetic mean.
	AggregateMean AggregationMode = "mean"
)

// ErrNoBenchmarksFound is returned when the input contains no valid benchmark result lines.
var ErrNoBenchmarksFound = errors.New("no benchmark results found in input")

// Sample represents a single raw Go benchmark output line.
type Sample struct {
	RawName    string
	Name       string
	Iterations int64
	Metrics    map[string]float64
	LineNumber int
}

// BenchmarkResult holds all parsed samples and aggregated metrics for a canonical benchmark name.
type BenchmarkResult struct {
	Name       string
	Samples    []Sample
	Iterations int64
	Metrics    map[string]float64
	RawMetrics map[string][]float64
}

// Suite holds the parsed metadata and aggregated benchmark results from a benchmark output stream.
type Suite struct {
	Metadata map[string]string
	Order    []string
	Results  map[string]*BenchmarkResult
}

// NormalizeBenchmarkName strips a trailing "-<procs>" CPU count suffix (such as "-96" or "-4")
// added by Go's testing package when GOMAXPROCS > 1, returning the canonical benchmark name.
func NormalizeBenchmarkName(raw string) string {
	idx := strings.LastIndexByte(raw, '-')
	if idx <= len("Benchmark") || idx+1 >= len(raw) {
		return raw
	}
	suffix := raw[idx+1:]
	hasNonZero := false
	for i := 0; i < len(suffix); i++ {
		ch := suffix[i]
		if ch < '0' || ch > '9' {
			return raw
		}
		if ch != '0' {
			hasNonZero = true
		}
	}
	if !hasNonZero {
		return raw
	}
	return raw[:idx]
}

// Parse reads standard `go test -bench` output from r and aggregates multi-sample
// results according to agg (defaults to AggregateMedian when agg is empty).
func Parse(r io.Reader, agg AggregationMode) (*Suite, error) {
	if agg == "" {
		agg = AggregateMedian
	}
	if agg != AggregateMedian && agg != AggregateMean {
		return nil, fmt.Errorf("unsupported aggregation mode %q (expected %q or %q)", agg, AggregateMedian, AggregateMean)
	}

	suite := &Suite{
		Metadata: make(map[string]string),
		Results:  make(map[string]*BenchmarkResult),
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		trimmed := strings.TrimSpace(scanner.Text())
		if trimmed == "" {
			continue
		}

		if isTestFailureLine(trimmed) {
			return nil, fmt.Errorf("line %d: benchmark input contains test failure: %s", lineNum, trimmed)
		}

		if key, val, ok := parseMetadataLine(trimmed); ok {
			suite.Metadata[key] = val
			continue
		}

		if !strings.HasPrefix(trimmed, "Benchmark") {
			continue
		}

		sample, err := parseBenchmarkLine(trimmed, lineNum)
		if err != nil {
			return nil, err
		}
		recordBenchmarkSample(suite, sample)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning benchmark input: %w", err)
	}

	if len(suite.Results) == 0 {
		return nil, ErrNoBenchmarksFound
	}

	finalizeSuiteAggregates(suite, agg)
	return suite, nil
}

func recordBenchmarkSample(suite *Suite, sample Sample) {
	res, exists := suite.Results[sample.Name]
	if !exists {
		res = &BenchmarkResult{
			Name:       sample.Name,
			Metrics:    make(map[string]float64),
			RawMetrics: make(map[string][]float64),
		}
		suite.Results[sample.Name] = res
		suite.Order = append(suite.Order, sample.Name)
	}
	res.Samples = append(res.Samples, sample)
	for unit, val := range sample.Metrics {
		res.RawMetrics[unit] = append(res.RawMetrics[unit], val)
	}
}

func finalizeSuiteAggregates(suite *Suite, agg AggregationMode) {
	for _, name := range suite.Order {
		res := suite.Results[name]
		iters := make([]float64, len(res.Samples))
		for i, s := range res.Samples {
			iters[i] = float64(s.Iterations)
		}
		res.Iterations = int64(math.Round(aggregateValues(iters, agg)))
		for unit, vals := range res.RawMetrics {
			res.Metrics[unit] = aggregateValues(vals, agg)
		}
	}
}

func isTestFailureLine(trimmed string) bool {
	return trimmed == "FAIL" ||
		strings.HasPrefix(trimmed, "FAIL\t") ||
		strings.HasPrefix(trimmed, "FAIL ") ||
		strings.HasPrefix(trimmed, "--- FAIL:")
}

func parseMetadataLine(trimmed string) (key, val string, ok bool) {
	colonIdx := strings.IndexByte(trimmed, ':')
	if colonIdx <= 0 {
		return "", "", false
	}
	k := trimmed[:colonIdx]
	switch k {
	case "goos", "goarch", "pkg", "cpu":
		return k, strings.TrimSpace(trimmed[colonIdx+1:]), true
	default:
		return "", "", false
	}
}

func parseBenchmarkLine(trimmed string, lineNum int) (Sample, error) {
	fields := strings.Fields(trimmed)
	if len(fields) < 4 || len(fields)%2 != 0 {
		return Sample{}, fmt.Errorf("line %d: malformed benchmark line (expected even number of fields >= 4, got %d): %q", lineNum, len(fields), trimmed)
	}

	rawName := fields[0]
	if len(rawName) <= len("Benchmark") {
		return Sample{}, fmt.Errorf("line %d: invalid benchmark name %q", lineNum, rawName)
	}

	iters, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || iters <= 0 {
		return Sample{}, fmt.Errorf("line %d: invalid iteration count %q in benchmark %q", lineNum, fields[1], rawName)
	}

	metrics := make(map[string]float64, (len(fields)-2)/2)
	for i := 2; i < len(fields); i += 2 {
		valStr := fields[i]
		unit := fields[i+1]

		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil || math.IsNaN(val) || math.IsInf(val, 0) || val < 0 {
			return Sample{}, fmt.Errorf("line %d: invalid metric value %q for unit %q in benchmark %q", lineNum, valStr, unit, rawName)
		}
		if unit == "" {
			return Sample{}, fmt.Errorf("line %d: empty metric unit in benchmark %q", lineNum, rawName)
		}
		if _, numErr := strconv.ParseFloat(unit, 64); numErr == nil {
			return Sample{}, fmt.Errorf("line %d: invalid numeric metric unit %q in benchmark %q", lineNum, unit, rawName)
		}
		metrics[unit] = val
	}

	if _, hasNs := metrics[UnitNsPerOp]; !hasNs {
		return Sample{}, fmt.Errorf("line %d: benchmark %q missing required %s metric", lineNum, rawName, UnitNsPerOp)
	}

	return Sample{
		RawName:    rawName,
		Name:       NormalizeBenchmarkName(rawName),
		Iterations: iters,
		Metrics:    metrics,
		LineNumber: lineNum,
	}, nil
}

func aggregateValues(values []float64, mode AggregationMode) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) == 1 {
		return values[0]
	}
	if mode == AggregateMean {
		var sum float64
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
	}

	sorted := slices.Clone(values)
	slices.Sort(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2.0
}
