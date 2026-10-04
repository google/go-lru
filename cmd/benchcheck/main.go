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

type cliOptions struct {
	candidatePath string
	baselinePath  string
	checkTargets  bool
	targetsPath   string
	agg           AggregationMode
	format        string
	summaryPath   string
	cfg           ComparisonConfig
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	opts, ok := parseCLIOptions(args, stderr)
	if !ok {
		return exitUsageErr
	}

	candSuite, err := parseBenchmarkSource(opts.candidatePath, stdin, opts.agg)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "benchcheck: failed to parse candidate %q: %v\n", opts.candidatePath, err)
		return exitUsageErr
	}

	targetRep, err := evaluateTargetsIfEnabled(candSuite, opts)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "benchcheck: %v\n", err)
		return exitUsageErr
	}

	cmpRep, err := evaluateComparisonIfEnabled(candSuite, stdin, opts)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "benchcheck: %v\n", err)
		return exitUsageErr
	}

	markdownReport := FormatMarkdown(candSuite, opts.agg, targetRep, cmpRep)
	if opts.format == "text" {
		_, _ = fmt.Fprint(stdout, FormatText(candSuite, opts.agg, targetRep, cmpRep))
	} else {
		_, _ = fmt.Fprint(stdout, markdownReport)
	}

	if opts.summaryPath != "" {
		if err := appendSummaryFile(opts.summaryPath, markdownReport); err != nil {
			_, _ = fmt.Fprintf(stderr, "benchcheck: failed to write summary %q: %v\n", opts.summaryPath, err)
			return exitUsageErr
		}
	}

	if !overallPassed(targetRep, cmpRep) {
		return exitViolation
	}
	return exitOK
}

func parseCLIOptions(args []string, stderr io.Writer) (cliOptions, bool) {
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
		return cliOptions{}, false
	}

	candidatePath, baselinePath, ok := resolveBenchmarkPaths(
		firstNonEmpty(candidateFlag, headFlag),
		firstNonEmpty(baselineFlag, baseFlag),
		fs.Args(),
		stderr,
	)
	if !ok {
		return cliOptions{}, false
	}

	opts := cliOptions{
		candidatePath: candidatePath,
		baselinePath:  baselinePath,
		checkTargets:  checkTargets,
		targetsPath:   targetsPath,
		agg:           AggregationMode(strings.ToLower(strings.TrimSpace(aggStr))),
		format:        strings.ToLower(strings.TrimSpace(formatStr)),
		summaryPath:   summaryPath,
		cfg:           cfg,
	}
	if !validateCLIOptions(opts, formatStr, stderr) {
		return cliOptions{}, false
	}
	return opts, true
}

func resolveBenchmarkPaths(
	candidatePath, baselinePath string,
	posArgs []string,
	stderr io.Writer,
) (cand, base string, ok bool) {
	if candidatePath != "" {
		if len(posArgs) > 0 {
			_, _ = fmt.Fprintf(stderr, "benchcheck: unexpected positional arguments %v when -head/-candidate is set\n", posArgs)
			return "", "", false
		}
		return candidatePath, baselinePath, true
	}

	switch len(posArgs) {
	case 0:
		return "", baselinePath, true
	case 1:
		return posArgs[0], baselinePath, true
	case 2:
		if baselinePath != "" {
			_, _ = fmt.Fprintf(stderr, "benchcheck: unexpected positional arguments %v\n", posArgs)
			return "", "", false
		}
		return posArgs[1], posArgs[0], true
	default:
		_, _ = fmt.Fprintf(stderr, "benchcheck: too many positional arguments %v\n", posArgs)
		return "", "", false
	}
}

func validateCLIOptions(opts cliOptions, rawFormat string, stderr io.Writer) bool {
	if opts.candidatePath == "" {
		_, _ = fmt.Fprintln(stderr, "benchcheck: missing required -head/-candidate benchmark file path")
		return false
	}
	if !opts.checkTargets && opts.baselinePath == "" {
		_, _ = fmt.Fprintln(stderr, "benchcheck: nothing to verify (-check-targets=false and no -base/-baseline provided)")
		return false
	}
	if opts.format != "markdown" && opts.format != "text" {
		_, _ = fmt.Fprintf(stderr, "benchcheck: unsupported -format %q (expected 'markdown' or 'text')\n", rawFormat)
		return false
	}
	if err := opts.cfg.Validate(); err != nil {
		_, _ = fmt.Fprintf(stderr, "benchcheck: %v\n", err)
		return false
	}
	return true
}

func evaluateTargetsIfEnabled(candSuite *Suite, opts cliOptions) (*TargetReport, error) {
	if !opts.checkTargets {
		return nil, nil
	}
	rules := DefaultTargets()
	if opts.targetsPath != "" {
		var err error
		rules, err = LoadTargets(opts.targetsPath)
		if err != nil {
			return nil, err
		}
	}
	targetRep, err := CheckTargets(candSuite, rules, opts.cfg.StrictMissing)
	if err != nil {
		return nil, fmt.Errorf("target check error: %w", err)
	}
	return targetRep, nil
}

func evaluateComparisonIfEnabled(candSuite *Suite, stdin io.Reader, opts cliOptions) (*ComparisonReport, error) {
	if opts.baselinePath == "" {
		return nil, nil
	}
	baseSuite, err := parseBenchmarkSource(opts.baselinePath, stdin, opts.agg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse baseline %q: %w", opts.baselinePath, err)
	}
	cmpRep, err := CompareSuites(baseSuite, candSuite, opts.cfg)
	if err != nil {
		return nil, fmt.Errorf("comparison error: %w", err)
	}
	return cmpRep, nil
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
