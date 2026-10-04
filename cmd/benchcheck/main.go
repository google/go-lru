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

// Command benchcheck parses Go benchmark output, enforces absolute performance
// and allocation targets grounded in docs/performance.md, and detects relative
// regressions between a baseline and candidate benchmark run.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	exitOK        = 0
	exitViolation = 1
	exitUsageErr  = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("benchcheck", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		candidateFlag string
		headFlag      string
		baselineFlag  string
		baseFlag      string
		checkTargets  bool
		targetsPath   string
		aggStr        string
		formatStr     string
		summaryPath   string
	)

	cfg := DefaultComparisonConfig()

	fs.StringVar(&candidateFlag, "candidate", "", "Path to candidate (head) benchmark output file ('-' for stdin)")
	fs.StringVar(&headFlag, "head", "", "Alias for -candidate: path to candidate (head) benchmark output file")
	fs.StringVar(&baselineFlag, "baseline", "", "Optional path to baseline (base) benchmark output file")
	fs.StringVar(&baseFlag, "base", "", "Alias for -baseline: optional path to baseline (base) benchmark output file")
	fs.BoolVar(&checkTargets, "check-targets", true, "Enforce absolute performance and allocation targets")
	fs.StringVar(&targetsPath, "targets", "", "Optional path to custom JSON target rules file (overrides DefaultTargets)")
	fs.StringVar(&aggStr, "agg", string(AggregateMedian), "Multi-sample aggregation mode: 'median' or 'mean'")
	fs.Float64Var(&cfg.MaxNsRegression, "max-ns-regression", cfg.MaxNsRegression, "Max allowed relative ns/op increase (e.g. 0.35 = +35%)")
	fs.Float64Var(&cfg.MaxBytesRegression, "max-bytes-regression", cfg.MaxBytesRegression, "Max allowed relative B/op increase (e.g. 0.25 = +25%)")
	fs.Float64Var(&cfg.MaxAllocsRegression, "max-allocs-regression", cfg.MaxAllocsRegression, "Max allowed relative allocs/op increase (e.g. 0.10 = +10%)")
	fs.Float64Var(&cfg.MaxCustomRegression, "max-custom-regression", cfg.MaxCustomRegression, "Max allowed relative custom metric regression (e.g. 0.15 = 15%)")
	fs.Float64Var(&cfg.MinNsDelta, "min-ns-delta", cfg.MinNsDelta, "Minimum absolute ns/op increase required to flag a regression")
	fs.Float64Var(&cfg.MinBytesDelta, "min-bytes-delta", cfg.MinBytesDelta, "Minimum absolute B/op increase required to flag a regression")
	fs.Float64Var(&cfg.MinAllocsDelta, "min-allocs-delta", cfg.MinAllocsDelta, "Minimum absolute allocs/op increase required to flag a regression")
	fs.Float64Var(&cfg.MinCustomDelta, "min-custom-delta", cfg.MinCustomDelta, "Minimum absolute custom metric delta required to flag a regression")
	fs.BoolVar(&cfg.StrictMissing, "strict-missing", false, "Fail if any baseline benchmark is missing from candidate or unmatched by target rules")
	fs.StringVar(&formatStr, "format", "markdown", "Output report format: 'markdown' or 'text'")
	fs.StringVar(&summaryPath, "summary", "", "Optional file path (e.g. $GITHUB_STEP_SUMMARY) to append Markdown report to")

	if err := fs.Parse(args); err != nil {
		return exitUsageErr
	}

	candidatePath := firstNonEmpty(candidateFlag, headFlag)
	baselinePath := firstNonEmpty(baselineFlag, baseFlag)

	posArgs := fs.Args()
	if candidatePath == "" {
		switch len(posArgs) {
		case 1:
			candidatePath = posArgs[0]
		case 2:
			if baselinePath == "" {
				baselinePath = posArgs[0]
				candidatePath = posArgs[1]
			} else {
				_, _ = fmt.Fprintf(stderr, "benchcheck: unexpected positional arguments %v\n", posArgs)
				return exitUsageErr
			}
		default:
			if len(posArgs) > 2 {
				_, _ = fmt.Fprintf(stderr, "benchcheck: too many positional arguments %v\n", posArgs)
				return exitUsageErr
			}
		}
	} else if len(posArgs) > 0 {
		_, _ = fmt.Fprintf(stderr, "benchcheck: unexpected positional arguments %v when -head/-candidate is set\n", posArgs)
		return exitUsageErr
	}

	if candidatePath == "" {
		_, _ = fmt.Fprintln(stderr, "benchcheck: missing required -head/-candidate benchmark file path")
		return exitUsageErr
	}
	if !checkTargets && baselinePath == "" {
		_, _ = fmt.Fprintln(stderr, "benchcheck: nothing to verify (-check-targets=false and no -base/-baseline provided)")
		return exitUsageErr
	}

	format := strings.ToLower(strings.TrimSpace(formatStr))
	if format != "markdown" && format != "text" {
		_, _ = fmt.Fprintf(stderr, "benchcheck: unsupported -format %q (expected 'markdown' or 'text')\n", formatStr)
		return exitUsageErr
	}

	if err := cfg.Validate(); err != nil {
		_, _ = fmt.Fprintf(stderr, "benchcheck: %v\n", err)
		return exitUsageErr
	}

	agg := AggregationMode(strings.ToLower(strings.TrimSpace(aggStr)))
	candSuite, err := parseBenchmarkSource(candidatePath, stdin, agg)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "benchcheck: failed to parse candidate %q: %v\n", candidatePath, err)
		return exitUsageErr
	}

	var targetRep *TargetReport
	if checkTargets {
		rules := DefaultTargets()
		if targetsPath != "" {
			rules, err = LoadTargets(targetsPath)
			if err != nil {
				_, _ = fmt.Fprintf(stderr, "benchcheck: %v\n", err)
				return exitUsageErr
			}
		}
		targetRep, err = CheckTargets(candSuite, rules, cfg.StrictMissing)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "benchcheck: target check error: %v\n", err)
			return exitUsageErr
		}
	}

	var cmpRep *ComparisonReport
	if baselinePath != "" {
		baseSuite, err := parseBenchmarkSource(baselinePath, stdin, agg)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "benchcheck: failed to parse baseline %q: %v\n", baselinePath, err)
			return exitUsageErr
		}
		cmpRep, err = CompareSuites(baseSuite, candSuite, cfg)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "benchcheck: comparison error: %v\n", err)
			return exitUsageErr
		}
	}

	markdownReport := FormatMarkdown(candSuite, agg, targetRep, cmpRep)
	if format == "text" {
		_, _ = fmt.Fprint(stdout, FormatText(candSuite, agg, targetRep, cmpRep))
	} else {
		_, _ = fmt.Fprint(stdout, markdownReport)
	}

	if summaryPath != "" {
		if err := appendSummaryFile(summaryPath, markdownReport); err != nil {
			_, _ = fmt.Fprintf(stderr, "benchcheck: failed to write summary %q: %v\n", summaryPath, err)
			return exitUsageErr
		}
	}

	if !overallPassed(targetRep, cmpRep) {
		return exitViolation
	}
	return exitOK
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func parseBenchmarkSource(path string, stdin io.Reader, agg AggregationMode) (*Suite, error) {
	if path == "-" {
		if stdin == nil {
			return nil, fmt.Errorf("stdin reader is nil")
		}
		return Parse(stdin, agg)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	return Parse(f, agg)
}

func appendSummaryFile(path, content string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = f.WriteString(content)
	return err
}
