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
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const full92BenchmarkSampleOutput = `goos: linux
goarch: amd64
pkg: github.com/google/go-lru
cpu: Intel(R) Xeon(R) CPU @ 2.60GHz
Benchmark_Put_MapCache/Flat-96                                	  264638	       249.6 ns/op	      42 B/op	       2 allocs/op
Benchmark_Put_MapCache/Nested_Depth2-96                       	  289612	       257.8 ns/op	      42 B/op	       2 allocs/op
Benchmark_Put_MapCache/DeeplyNested_Depth10-96                	  187424	       321.1 ns/op	      99 B/op	       2 allocs/op
Benchmark_Put_RadixCache/Flat-96                              	  158702	       396.1 ns/op	      25 B/op	       2 allocs/op
Benchmark_Put_RadixCache/Nested_Depth2-96                     	  159285	       400.1 ns/op	      25 B/op	       2 allocs/op
Benchmark_Put_RadixCache/DeeplyNested_Depth10-96              	  152318	       433.3 ns/op	      26 B/op	       2 allocs/op
Benchmark_Put_ArenaRadixCache/Flat-96                         	  122172	       447.5 ns/op	      30 B/op	       0 allocs/op
Benchmark_Put_ArenaRadixCache/Nested_Depth2-96                	  129919	       445.1 ns/op	      30 B/op	       0 allocs/op
Benchmark_Put_ArenaRadixCache/DeeplyNested_Depth10-96         	  126817	       484.3 ns/op	      30 B/op	       0 allocs/op
Benchmark_Put_UnitWeight_Map-96                               	  728718	        68.79 ns/op	       2 B/op	       0 allocs/op
Benchmark_Put_UnitWeight_Radix-96                             	  287838	       203.9 ns/op	       3 B/op	       0 allocs/op
Benchmark_Put_UnitWeight_ArenaRadix-96                        	  453702	       116.3 ns/op	       6 B/op	       0 allocs/op
Benchmark_Get_MapCache/Flat-96                                	 1245382	        48.94 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_MapCache/Nested_Depth2-96                       	 1232906	        48.12 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_MapCache/DeeplyNested_Depth10-96                	  750001	        69.33 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_RadixCache/Flat-96                              	  284599	       185.4 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_RadixCache/Nested_Depth2-96                     	  294753	       185.2 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_RadixCache/DeeplyNested_Depth10-96              	  282874	       198.4 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_ArenaRadixCache/Flat-96                         	  665546	        84.16 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_ArenaRadixCache/Nested_Depth2-96                	  602142	       100.2 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_ArenaRadixCache/DeeplyNested_Depth10-96         	  525322	       103.9 ns/op	       0 B/op	       0 allocs/op
Benchmark_Peek_MapCache-96                                    	 1669360	        35.67 ns/op	       0 B/op	       0 allocs/op
Benchmark_Peek_RadixCache-96                                  	  326901	       179.1 ns/op	       0 B/op	       0 allocs/op
Benchmark_Peek_ArenaRadixCache-96                             	  652372	        85.22 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_MapCache-96                                 	  954112	        54.76 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_RadixCache-96                               	  547815	       107.9 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_ArenaRadixCache-96                          	  682686	        85.65 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_UnitWeight_Map-96                           	 1098070	        54.09 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_UnitWeight_Radix-96                         	  551451	       106.9 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_UnitWeight_ArenaRadix-96                    	  683961	        85.27 ns/op	       0 B/op	       0 allocs/op
Benchmark_Delete_MapCache-96                                  	  515457	       111.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Delete_RadixCache-96                                	  800307	        79.81 ns/op	       0 B/op	       0 allocs/op
Benchmark_Delete_ArenaRadixCache-96                           	  396787	       149.4 ns/op	       4 B/op	       0 allocs/op
Benchmark_DeletePrefix_MapCache/Flat-96                       	     637	     94729 ns/op	       0 B/op	       0 allocs/op
Benchmark_DeletePrefix_MapCache/Nested_Depth2-96              	     650	     93748 ns/op	       0 B/op	       0 allocs/op
Benchmark_DeletePrefix_MapCache/DeeplyNested_Depth10-96       	     570	    106419 ns/op	       0 B/op	       0 allocs/op
Benchmark_DeletePrefix_RadixCache/Flat-96                     	   20422	      2841 ns/op	       0 B/op	       0 allocs/op
Benchmark_DeletePrefix_RadixCache/Nested_Depth2-96            	   20250	      2964 ns/op	       3 B/op	       0 allocs/op
Benchmark_DeletePrefix_RadixCache/DeeplyNested_Depth10-96     	   21032	      2688 ns/op	      14 B/op	       0 allocs/op
Benchmark_DeletePrefix_ArenaRadixCache/Flat-96                	   12432	      4864 ns/op	       1 B/op	       0 allocs/op
Benchmark_DeletePrefix_ArenaRadixCache/Nested_Depth2-96       	   12604	      4812 ns/op	       1 B/op	       0 allocs/op
Benchmark_DeletePrefix_ArenaRadixCache/DeeplyNested_Depth10-96	   12475	      4823 ns/op	       1 B/op	       0 allocs/op
Benchmark_ParallelThroughput_Mixed/MapCache-96                	  130876	       511.5 ns/op	      14 B/op	       0 allocs/op
Benchmark_ParallelThroughput_Mixed/RadixCache-96              	  116593	       664.4 ns/op	       9 B/op	       0 allocs/op
Benchmark_ParallelThroughput_Mixed/ArenaRadixCache-96         	  148051	       489.9 ns/op	      13 B/op	       0 allocs/op
Benchmark_ParallelThroughput_ReadHeavy/MapCache-96            	  107302	       568.7 ns/op	       2 B/op	       0 allocs/op
Benchmark_ParallelThroughput_ReadHeavy/RadixCache-96          	  164427	       494.9 ns/op	       2 B/op	       0 allocs/op
Benchmark_ParallelThroughput_ReadHeavy/ArenaRadixCache-96     	  127917	       549.7 ns/op	       0 B/op	       0 allocs/op
Benchmark_ParallelThroughput_WriteHeavy/MapCache-96           	  101486	       585.6 ns/op	      19 B/op	       0 allocs/op
Benchmark_ParallelThroughput_WriteHeavy/RadixCache-96         	   82234	       747.6 ns/op	      14 B/op	       0 allocs/op
Benchmark_ParallelThroughput_WriteHeavy/ArenaRadixCache-96    	  158835	       516.5 ns/op	      14 B/op	       0 allocs/op
Benchmark_ParallelThroughput_PeekOnly/MapCache-96             	  739455	        77.81 ns/op	       0 B/op	       0 allocs/op
Benchmark_ParallelThroughput_PeekOnly/RadixCache-96           	  845473	        70.21 ns/op	       0 B/op	       0 allocs/op
Benchmark_ParallelThroughput_PeekOnly/ArenaRadixCache-96      	  925633	        63.58 ns/op	       0 B/op	       0 allocs/op
Benchmark_LargeScale_Put_100K/MapCache-96                     	       2	  27190649 ns/op	       115.0 heap-B/entry	15017000 B/op	  200538 allocs/op
Benchmark_LargeScale_Put_100K/RadixCache-96                   	       2	  26215560 ns/op	        96.03 heap-B/entry	 9627424 B/op	  100010 allocs/op
Benchmark_LargeScale_Put_100K/ArenaRadixCache-96              	       2	  32823904 ns/op	        78.92 heap-B/entry	34659320 B/op	      62 allocs/op
Benchmark_LargeScale_DeletePrefix_100K/MapCache-96            	       6	  10670921 ns/op	       0 B/op	       0 allocs/op
Benchmark_LargeScale_DeletePrefix_100K/RadixCache-96          	      40	   1394869 ns/op	       0 B/op	       0 allocs/op
Benchmark_LargeScale_DeletePrefix_100K/ArenaRadixCache-96     	      16	   3489437 ns/op	       0 B/op	       0 allocs/op
Benchmark_ArenaRadixCache_Compact-96                          	     100	    505357 ns/op	    729144 reclaimed-B/op	  587008 B/op	       4 allocs/op
Benchmark_ArenaRadixCache_PutUnderPressure-96                 	   51706	      1408 ns/op	     243 B/op	       0 allocs/op
Benchmark_Put_WithOnEvictValue/MapCache-96                    	  247870	       306.2 ns/op	      43 B/op	       2 allocs/op
Benchmark_Put_WithOnEvictValue/RadixCache-96                  	  140259	       495.1 ns/op	      26 B/op	       2 allocs/op
Benchmark_Put_WithOnEvictValue/ArenaRadixCache-96             	  137508	       524.5 ns/op	      28 B/op	       0 allocs/op
Benchmark_Put_WithOnEvictEntry/MapCache-96                    	  258296	       312.3 ns/op	      42 B/op	       2 allocs/op
Benchmark_Put_WithOnEvictEntry/RadixCache-96                  	  129181	       568.9 ns/op	      49 B/op	       3 allocs/op
Benchmark_Put_WithOnEvictEntry/ArenaRadixCache-96             	  129796	       632.1 ns/op	      52 B/op	       1 allocs/op
Benchmark_Replace_WithOnEvictValue/MapCache-96                	  494034	       114.7 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_WithOnEvictValue/RadixCache-96              	  355470	       165.7 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_WithOnEvictValue/ArenaRadixCache-96         	  408204	       141.8 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_WithOnEvictEntry/MapCache-96                	  481054	       115.7 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_WithOnEvictEntry/RadixCache-96              	  354140	       166.5 ns/op	       0 B/op	       0 allocs/op
Benchmark_Replace_WithOnEvictEntry/ArenaRadixCache-96         	  410338	       143.6 ns/op	       0 B/op	       0 allocs/op
Benchmark_All_MapCache-96                                     	   15080	      3978 ns/op	       0 B/op	       0 allocs/op
Benchmark_All_RadixCache-96                                   	    1213	     53382 ns/op	   24000 B/op	    1000 allocs/op
Benchmark_All_ArenaRadixCache-96                              	    1099	     59765 ns/op	   24000 B/op	    1000 allocs/op
Benchmark_Keys_MapCache-96                                    	   14859	      4020 ns/op	       0 B/op	       0 allocs/op
Benchmark_Keys_RadixCache-96                                  	    1269	     53821 ns/op	   24000 B/op	    1000 allocs/op
Benchmark_Keys_ArenaRadixCache-96                             	    1062	     61030 ns/op	   24000 B/op	    1000 allocs/op
Benchmark_Values_MapCache-96                                  	   14641	      4096 ns/op	       0 B/op	       0 allocs/op
Benchmark_Values_RadixCache-96                                	   13774	      4356 ns/op	       0 B/op	       0 allocs/op
Benchmark_Values_ArenaRadixCache-96                           	   14412	      4158 ns/op	       0 B/op	       0 allocs/op
Benchmark_Stats_MapCache/Sequential-96                        	  838533	        67.35 ns/op	       0 B/op	       0 allocs/op
Benchmark_Stats_MapCache/Parallel-96                          	  818098	        72.86 ns/op	       0 B/op	       0 allocs/op
Benchmark_Stats_RadixCache/Sequential-96                      	  870392	        67.23 ns/op	       0 B/op	       0 allocs/op
Benchmark_Stats_RadixCache/Parallel-96                        	  828441	        72.14 ns/op	       0 B/op	       0 allocs/op
Benchmark_Stats_ArenaRadixCache/Sequential-96                 	  904075	        66.20 ns/op	       0 B/op	       0 allocs/op
Benchmark_Stats_ArenaRadixCache/Parallel-96                   	  926281	        65.08 ns/op	       0 B/op	       0 allocs/op
Benchmark_Production_MemoryFootprint/MapCache-96              	       4	  16130198 ns/op	       293.4 heap-B/entry	11381834 B/op	   65817 allocs/op
Benchmark_Production_MemoryFootprint/RadixCache-96            	       4	  14018621 ns/op	       145.4 heap-B/entry	 4819954 B/op	   67103 allocs/op
Benchmark_Production_MemoryFootprint/ArenaRadixCache-96       	       4	  13804221 ns/op	       162.1 heap-B/entry	14993524 B/op	      56 allocs/op
PASS
ok  	github.com/google/go-lru	12.345s
`

func writeTempDataFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	err := os.WriteFile(path, []byte(content), 0o600)
	require.NoError(t, err)
	return path
}

func TestNormalizeBenchmarkName(t *testing.T) {
	// Arrange
	cases := []struct {
		name     string
		raw      string
		expected string
	}{
		{
			name:     "strips multi-core suffix",
			raw:      "Benchmark_Get_MapCache/Flat-96",
			expected: "Benchmark_Get_MapCache/Flat",
		},
		{
			name:     "strips single-digit core suffix",
			raw:      "Benchmark_Peek_MapCache-4",
			expected: "Benchmark_Peek_MapCache",
		},
		{
			name:     "strips -1 suffix",
			raw:      "Benchmark_Peek-1",
			expected: "Benchmark_Peek",
		},
		{
			name:     "preserves name without suffix",
			raw:      "Benchmark_LargeScale_Put_100K/MapCache",
			expected: "Benchmark_LargeScale_Put_100K/MapCache",
		},
		{
			name:     "preserves name ending in digits after underscore",
			raw:      "Benchmark_Get_MapCache/DeeplyNested_Depth10",
			expected: "Benchmark_Get_MapCache/DeeplyNested_Depth10",
		},
		{
			name:     "preserves non-numeric dash suffix",
			raw:      "Benchmark_Custom-subtest",
			expected: "Benchmark_Custom-subtest",
		},
		{
			name:     "preserves all-zero suffix",
			raw:      "Benchmark_Custom-00",
			expected: "Benchmark_Custom-00",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			actual := NormalizeBenchmarkName(tc.raw)

			// Assert
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestParse_StandardAndCustomMetrics_MultiSampleMedianAndMean(t *testing.T) {
	// Arrange
	input := `goos: linux
goarch: amd64
pkg: github.com/google/go-lru
cpu: Intel(R) Xeon(R) CPU @ 2.60GHz
Benchmark_Get_MapCache/Flat-96                 	 1000000	        50.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_MapCache/Flat-96                 	 1000000	        52.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_MapCache/Flat-96                 	 1000000	        48.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_MapCache/Flat-96                 	 1000000	        51.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_MapCache/Flat-96                 	 1000000	        49.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Get_MapCache/Flat-96                 	  200000	       250.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_LargeScale_Put_100K/RadixCache-4     	       2	  27000000 ns/op	        96.0 heap-B/entry	 9600000 B/op	  100010 allocs/op
Benchmark_LargeScale_Put_100K/RadixCache-4     	       4	  29000000 ns/op	        98.0 heap-B/entry	 9600200 B/op	  100012 allocs/op
Benchmark_ArenaRadixCache_Compact-4            	      16	    600000 ns/op	    810000 reclaimed-B/op	  720000 B/op	     220 allocs/op
PASS
ok  	github.com/google/go-lru	4.200s
`

	// Act
	medianSuite, errMedian := Parse(strings.NewReader(input), AggregateMedian)
	meanSuite, errMean := Parse(strings.NewReader(input), AggregateMean)

	// Assert
	require.NoError(t, errMedian)
	require.NoError(t, errMean)

	assert.Equal(t, "linux", medianSuite.Metadata["goos"])
	assert.Equal(t, "amd64", medianSuite.Metadata["goarch"])
	assert.Equal(t, "github.com/google/go-lru", medianSuite.Metadata["pkg"])
	assert.Equal(t, []string{
		"Benchmark_Get_MapCache/Flat",
		"Benchmark_LargeScale_Put_100K/RadixCache",
		"Benchmark_ArenaRadixCache_Compact",
	}, medianSuite.Order)

	flatMedian := medianSuite.Results["Benchmark_Get_MapCache/Flat"]
	require.NotNil(t, flatMedian)
	assert.Len(t, flatMedian.Samples, 6)
	// Sorted ns/op: [48, 49, 50, 51, 52, 250] -> median is (50+51)/2 = 50.5 (rejects the 250 outlier).
	assert.InDelta(t, 50.5, flatMedian.Metrics[UnitNsPerOp], 1e-6)
	assert.InDelta(t, 0.0, flatMedian.Metrics[UnitBytesPerOp], 1e-6)
	assert.InDelta(t, 0.0, flatMedian.Metrics[UnitAllocsPerOp], 1e-6)

	flatMean := meanSuite.Results["Benchmark_Get_MapCache/Flat"]
	require.NotNil(t, flatMean)
	// Mean of [48, 49, 50, 51, 52, 250] = 500 / 6 = 83.333333...
	assert.InDelta(t, 500.0/6.0, flatMean.Metrics[UnitNsPerOp], 1e-4)

	largeScale := medianSuite.Results["Benchmark_LargeScale_Put_100K/RadixCache"]
	require.NotNil(t, largeScale)
	assert.Equal(t, int64(3), largeScale.Iterations)
	assert.InDelta(t, 28000000.0, largeScale.Metrics[UnitNsPerOp], 1e-6)
	assert.InDelta(t, 97.0, largeScale.Metrics[UnitHeapBytesPerEnt], 1e-6)
	assert.InDelta(t, 9600100.0, largeScale.Metrics[UnitBytesPerOp], 1e-6)
	assert.InDelta(t, 100011.0, largeScale.Metrics[UnitAllocsPerOp], 1e-6)

	compact := medianSuite.Results["Benchmark_ArenaRadixCache_Compact"]
	require.NotNil(t, compact)
	assert.InDelta(t, 600000.0, compact.Metrics[UnitNsPerOp], 1e-6)
	assert.InDelta(t, 810000.0, compact.Metrics[UnitReclaimedBytesOp], 1e-6)
	assert.InDelta(t, 720000.0, compact.Metrics[UnitBytesPerOp], 1e-6)
	assert.InDelta(t, 220.0, compact.Metrics[UnitAllocsPerOp], 1e-6)
}

func TestParse_MalformedAndFailingInputs(t *testing.T) {
	// Arrange
	cases := []struct {
		name        string
		input       string
		agg         AggregationMode
		expectedErr string
	}{
		{
			name:        "empty input",
			input:       "   \n\n  ",
			agg:         AggregateMedian,
			expectedErr: "no benchmark results found",
		},
		{
			name:        "headers only without benchmarks",
			input:       "goos: linux\ngoarch: amd64\nPASS\n",
			agg:         AggregateMedian,
			expectedErr: "no benchmark results found",
		},
		{
			name:        "test failure line FAIL",
			input:       "Benchmark_Get-4\t1000\t50 ns/op\nFAIL\tgithub.com/google/go-lru\t0.12s\n",
			agg:         AggregateMedian,
			expectedErr: "benchmark input contains test failure",
		},
		{
			name:        "subtest failure line --- FAIL:",
			input:       "--- FAIL: Benchmark_Get (0.01s)\n",
			agg:         AggregateMedian,
			expectedErr: "benchmark input contains test failure",
		},
		{
			name:        "odd token count on benchmark line",
			input:       "Benchmark_Get_MapCache/Flat-4 1000 50.0 ns/op 0\n",
			agg:         AggregateMedian,
			expectedErr: "malformed benchmark line",
		},
		{
			name:        "zero iteration count",
			input:       "Benchmark_Get_MapCache/Flat-4 0 50.0 ns/op\n",
			agg:         AggregateMedian,
			expectedErr: "invalid iteration count",
		},
		{
			name:        "non-numeric metric value",
			input:       "Benchmark_Get_MapCache/Flat-4 1000 abc ns/op\n",
			agg:         AggregateMedian,
			expectedErr: "invalid metric value",
		},
		{
			name:        "NaN metric value",
			input:       "Benchmark_Get_MapCache/Flat-4 1000 NaN ns/op\n",
			agg:         AggregateMedian,
			expectedErr: "invalid metric value",
		},
		{
			name:        "negative metric value",
			input:       "Benchmark_Get_MapCache/Flat-4 1000 -5.0 ns/op\n",
			agg:         AggregateMedian,
			expectedErr: "invalid metric value",
		},
		{
			name:        "numeric unit token",
			input:       "Benchmark_Get_MapCache/Flat-4 1000 50.0 123\n",
			agg:         AggregateMedian,
			expectedErr: "invalid numeric metric unit",
		},
		{
			name:        "missing ns/op metric",
			input:       "Benchmark_Get_MapCache/Flat-4 1000 0 B/op 0 allocs/op\n",
			agg:         AggregateMedian,
			expectedErr: "missing required ns/op metric",
		},
		{
			name:        "unsupported aggregation mode",
			input:       "Benchmark_Get_MapCache/Flat-4 1000 50.0 ns/op\n",
			agg:         AggregationMode("p99"),
			expectedErr: "unsupported aggregation mode",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			suite, err := Parse(strings.NewReader(tc.input), tc.agg)

			// Assert
			require.Error(t, err)
			assert.Nil(t, suite)
			assert.ErrorContains(t, err, tc.expectedErr)
		})
	}
}

func TestCompareSuites_NoRegressions_Passes(t *testing.T) {
	// Arrange
	baseInput := `
Benchmark_Get_MapCache/Flat-96                 	 1000000	        50.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Put_MapCache/Flat-96                 	  350000	       340.0 ns/op	     106 B/op	       2 allocs/op
Benchmark_LargeScale_Put_100K/RadixCache-96    	       2	  27000000 ns/op	        96.0 heap-B/entry	 9627472 B/op	  100011 allocs/op
Benchmark_ArenaRadixCache_Compact-96           	     100	    550000 ns/op	    810000 reclaimed-B/op	  720000 B/op	     215 allocs/op
`
	candInput := `
Benchmark_Get_MapCache/Flat-4                  	 1000000	        52.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Put_MapCache/Flat-4                  	  360000	       325.0 ns/op	     106 B/op	       2 allocs/op
Benchmark_LargeScale_Put_100K/RadixCache-4     	       2	  26500000 ns/op	        95.5 heap-B/entry	 9620000 B/op	  100010 allocs/op
Benchmark_ArenaRadixCache_Compact-4            	     105	    520000 ns/op	    835000 reclaimed-B/op	  715000 B/op	     215 allocs/op
`
	baseSuite, err := Parse(strings.NewReader(baseInput), AggregateMedian)
	require.NoError(t, err)
	candSuite, err := Parse(strings.NewReader(candInput), AggregateMedian)
	require.NoError(t, err)

	// Act
	rep, err := CompareSuites(baseSuite, candSuite, DefaultComparisonConfig())

	// Assert
	require.NoError(t, err)
	assert.True(t, rep.Passed())
	assert.Empty(t, rep.Regressions)
	assert.Len(t, rep.MatchedBenchmarks, 4)
}

func TestCompareSuites_DetectsLatencyBytesAndAllocsRegressions(t *testing.T) {
	// Arrange
	baseInput := `
Benchmark_Get_MapCache/Flat-96       	 1000000	        50.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Peek_RadixCache-96         	  300000	       170.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Put_MapCache/Flat-96       	  350000	       330.0 ns/op	     100 B/op	       2 allocs/op
`
	candInput := `
Benchmark_Get_MapCache/Flat-96       	 1000000	        51.0 ns/op	       8 B/op	       1 allocs/op
Benchmark_Peek_RadixCache-96         	  200000	       260.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Put_MapCache/Flat-96       	  300000	       340.0 ns/op	     160 B/op	       3 allocs/op
`
	baseSuite, err := Parse(strings.NewReader(baseInput), AggregateMedian)
	require.NoError(t, err)
	candSuite, err := Parse(strings.NewReader(candInput), AggregateMedian)
	require.NoError(t, err)

	// Act
	rep, err := CompareSuites(baseSuite, candSuite, DefaultComparisonConfig())

	// Assert
	require.NoError(t, err)
	assert.False(t, rep.Passed())
	require.Len(t, rep.Regressions, 4)

	// 1. 0 -> 1 allocs/op on Benchmark_Get_MapCache/Flat (+Inf transition)
	assert.Equal(t, "Benchmark_Get_MapCache/Flat", rep.Regressions[0].Benchmark)
	assert.Equal(t, UnitAllocsPerOp, rep.Regressions[0].Unit)
	assert.True(t, math.IsInf(rep.Regressions[0].RelChange, 1))

	// 2. ns/op regression on Benchmark_Peek_RadixCache (170 -> 260 = +52.9% > 35%)
	assert.Equal(t, "Benchmark_Peek_RadixCache", rep.Regressions[1].Benchmark)
	assert.Equal(t, UnitNsPerOp, rep.Regressions[1].Unit)
	assert.InDelta(t, (260.0-170.0)/170.0, rep.Regressions[1].RelChange, 1e-6)

	// 3. B/op regression on Benchmark_Put_MapCache/Flat (100 -> 160 = +60% > 25%)
	assert.Equal(t, "Benchmark_Put_MapCache/Flat", rep.Regressions[2].Benchmark)
	assert.Equal(t, UnitBytesPerOp, rep.Regressions[2].Unit)
	assert.InDelta(t, 0.60, rep.Regressions[2].RelChange, 1e-6)

	// 4. allocs/op regression on Benchmark_Put_MapCache/Flat (2 -> 3 = +50% > 10%)
	assert.Equal(t, "Benchmark_Put_MapCache/Flat", rep.Regressions[3].Benchmark)
	assert.Equal(t, UnitAllocsPerOp, rep.Regressions[3].Unit)
	assert.InDelta(t, 0.50, rep.Regressions[3].RelChange, 1e-6)
}

func TestCompareSuites_DetectsCustomMetricRegressions_BothDirections(t *testing.T) {
	// Arrange
	baseInput := `
Benchmark_LargeScale_Put_100K/RadixCache-96    	       2	  27000000 ns/op	        96.0 heap-B/entry	 9627472 B/op	  100011 allocs/op
Benchmark_ArenaRadixCache_Compact-96           	     100	    520000 ns/op	    816000 reclaimed-B/op	  718000 B/op	     215 allocs/op
`
	candInput := `
Benchmark_LargeScale_Put_100K/RadixCache-96    	       2	  27500000 ns/op	       125.0 heap-B/entry	 9630000 B/op	  100011 allocs/op
Benchmark_ArenaRadixCache_Compact-96           	     100	    530000 ns/op	    500000 reclaimed-B/op	  719000 B/op	     215 allocs/op
`
	baseSuite, err := Parse(strings.NewReader(baseInput), AggregateMedian)
	require.NoError(t, err)
	candSuite, err := Parse(strings.NewReader(candInput), AggregateMedian)
	require.NoError(t, err)

	// Act
	rep, err := CompareSuites(baseSuite, candSuite, DefaultComparisonConfig())

	// Assert
	require.NoError(t, err)
	assert.False(t, rep.Passed())
	require.Len(t, rep.Regressions, 2)

	// heap-B/entry is lower-is-better: 96.0 -> 125.0 (+30.2% > +15%)
	heapReg := rep.Regressions[0]
	assert.Equal(t, "Benchmark_LargeScale_Put_100K/RadixCache", heapReg.Benchmark)
	assert.Equal(t, UnitHeapBytesPerEnt, heapReg.Unit)
	assert.Equal(t, DirectionLowerIsBetter, heapReg.Direction)
	assert.InDelta(t, (125.0-96.0)/96.0, heapReg.RelChange, 1e-6)
	assert.Contains(t, heapReg.Message, "increased by +30.2%")

	// reclaimed-B/op is higher-is-better: 816000 -> 500000 (-38.7% drop > -15%)
	reclaimReg := rep.Regressions[1]
	assert.Equal(t, "Benchmark_ArenaRadixCache_Compact", reclaimReg.Benchmark)
	assert.Equal(t, UnitReclaimedBytesOp, reclaimReg.Unit)
	assert.Equal(t, DirectionHigherIsBetter, reclaimReg.Direction)
	assert.InDelta(t, (500000.0-816000.0)/816000.0, reclaimReg.RelChange, 1e-6)
	assert.Contains(t, reclaimReg.Message, "decreased by -38.7%")
}

func TestCompareSuites_IgnoresNoisyMicroDeltasBelowMinimumFloor(t *testing.T) {
	// Arrange
	baseInput := `
Benchmark_Get_MapCache/Flat-96                              	 1000000	        50.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Put_ArenaRadixCache/Flat-96                       	  120000	       445.0 ns/op	      30 B/op	       0 allocs/op
Benchmark_ArenaRadixCache_PutUnderPressure-96               	   51000	      1390.0 ns/op	     230 B/op	       0 allocs/op
Benchmark_DeletePrefix_RadixCache/Flat-96                   	   22000	      2500.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_ParallelThroughput_WriteHeavy/ArenaRadixCache-96  	  101574	       590.0 ns/op	      42 B/op	       0 allocs/op
Benchmark_LargeScale_DeletePrefix_100K/MapCache-96          	       2	   6142951.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_LargeScale_DeletePrefix_100K/RadixCache-96        	      16	    681404.5 ns/op	       0 B/op	       0 allocs/op
Benchmark_LargeScale_DeletePrefix_100K/ArenaRadixCache-96   	       3	   4376141.5 ns/op	       0 B/op	       0 allocs/op
Benchmark_ArenaRadixCache_Compact-96                        	      18	    519373.5 ns/op	  810000 reclaimed-B/op	  587008 B/op	       4 allocs/op
`
	candInput := `
Benchmark_Get_MapCache/Flat-96                              	 1000000	        58.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_Put_ArenaRadixCache/Flat-96                       	   60000	       455.0 ns/op	      81 B/op	       0 allocs/op
Benchmark_ArenaRadixCache_PutUnderPressure-96               	   30000	      1420.0 ns/op	     295 B/op	       0 allocs/op
Benchmark_DeletePrefix_RadixCache/Flat-96                   	   21000	      2550.0 ns/op	       4 B/op	       0 allocs/op
Benchmark_ParallelThroughput_WriteHeavy/ArenaRadixCache-96  	   65000	       620.0 ns/op	     130 B/op	       0 allocs/op
Benchmark_LargeScale_DeletePrefix_100K/MapCache-96          	       1	  11011284.0 ns/op	     192 B/op	       2 allocs/op
Benchmark_LargeScale_DeletePrefix_100K/RadixCache-96        	      14	    932033.0 ns/op	      14 B/op	       0 allocs/op
Benchmark_LargeScale_DeletePrefix_100K/ArenaRadixCache-96   	       2	   5700000.0 ns/op	      28 B/op	       0 allocs/op
Benchmark_ArenaRadixCache_Compact-96                        	      14	    736689.0 ns/op	  805000 reclaimed-B/op	  587008 B/op	       4 allocs/op
`
	baseSuite, err := Parse(strings.NewReader(baseInput), AggregateMedian)
	require.NoError(t, err)
	candSuite, err := Parse(strings.NewReader(candInput), AggregateMedian)
	require.NoError(t, err)

	// Act
	rep, err := CompareSuites(baseSuite, candSuite, DefaultComparisonConfig())

	// Assert
	require.NoError(t, err)
	assert.True(t, rep.Passed())
	assert.Empty(t, rep.Regressions)
}

func TestCompareSuites_AllowsIntentionalTradeoffsWithinTargetBudgets(t *testing.T) {
	// Arrange
	baseInput := `
Benchmark_Put_ArenaRadixCache/Flat-96             	  220000	       500.3 ns/op	      80 B/op	       0 allocs/op
Benchmark_Get_ArenaRadixCache/Flat-96             	 1000000	        57.8 ns/op	       0 B/op	       0 allocs/op
Benchmark_All_ArenaRadixCache-96                  	   20000	      3447.5 ns/op	       0 B/op	       0 allocs/op
`
	candInput := `
Benchmark_Put_ArenaRadixCache/Flat-96             	  200000	       539.8 ns/op	      91 B/op	       1 allocs/op
Benchmark_Get_ArenaRadixCache/Flat-96             	  600000	        96.1 ns/op	       0 B/op	       0 allocs/op
Benchmark_All_ArenaRadixCache-96                  	     900	     57554.0 ns/op	   24000 B/op	    1000 allocs/op
`
	baseSuite, err := Parse(strings.NewReader(baseInput), AggregateMedian)
	require.NoError(t, err)
	candSuite, err := Parse(strings.NewReader(candInput), AggregateMedian)
	require.NoError(t, err)

	cfgWithTargets := DefaultComparisonConfig()
	cfgWithTargets.TargetRules = DefaultTargets()

	cfgStrictRelative := DefaultComparisonConfig()
	cfgStrictRelative.AllowWithinTargets = false
	cfgStrictRelative.TargetRules = DefaultTargets()

	// Act
	repAllowed, err1 := CompareSuites(baseSuite, candSuite, cfgWithTargets)
	repStrict, err2 := CompareSuites(baseSuite, candSuite, cfgStrictRelative)

	// Assert
	require.NoError(t, err1)
	assert.True(t, repAllowed.Passed())
	assert.Empty(t, repAllowed.Regressions)

	require.NoError(t, err2)
	assert.False(t, repStrict.Passed())
	assert.Len(t, repStrict.Regressions, 5)
}

func TestCompareSuites_MissingAndRenamedBenchmarks(t *testing.T) {
	t.Run("disjoint suites return ErrNoMatchingBenchmarks", func(t *testing.T) {
		// Arrange
		baseSuite, err := Parse(strings.NewReader("Benchmark_OldName-4 1000 50.0 ns/op 0 B/op 0 allocs/op\n"), AggregateMedian)
		require.NoError(t, err)
		candSuite, err := Parse(strings.NewReader("Benchmark_NewName-4 1000 50.0 ns/op 0 B/op 0 allocs/op\n"), AggregateMedian)
		require.NoError(t, err)

		// Act
		rep, err := CompareSuites(baseSuite, candSuite, DefaultComparisonConfig())

		// Assert
		require.ErrorIs(t, err, ErrNoMatchingBenchmarks)
		assert.Nil(t, rep)
	})

	t.Run("partial overlap tracks missing and new benchmarks", func(t *testing.T) {
		// Arrange
		baseInput := `
Benchmark_Shared-4  1000 100.0 ns/op 0 B/op 0 allocs/op
Benchmark_Removed-4 1000 100.0 ns/op 0 B/op 0 allocs/op
`
		candInput := `
Benchmark_Shared-4  1000 102.0 ns/op 0 B/op 0 allocs/op
Benchmark_Added-4   1000 100.0 ns/op 0 B/op 0 allocs/op
`
		baseSuite, err := Parse(strings.NewReader(baseInput), AggregateMedian)
		require.NoError(t, err)
		candSuite, err := Parse(strings.NewReader(candInput), AggregateMedian)
		require.NoError(t, err)

		cfgNonStrict := DefaultComparisonConfig()
		cfgNonStrict.StrictMissing = false

		cfgStrict := DefaultComparisonConfig()
		cfgStrict.StrictMissing = true

		// Act
		repNonStrict, err1 := CompareSuites(baseSuite, candSuite, cfgNonStrict)
		repStrict, err2 := CompareSuites(baseSuite, candSuite, cfgStrict)

		// Assert
		require.NoError(t, err1)
		assert.True(t, repNonStrict.Passed())
		assert.Equal(t, []string{"Benchmark_Removed"}, repNonStrict.MissingInCandidate)
		assert.Equal(t, []string{"Benchmark_Added"}, repNonStrict.NewInCandidate)

		require.NoError(t, err2)
		assert.False(t, repStrict.Passed())
	})
}

func TestCheckTargets_DefaultTargetsCoverAllRepositoryBenchmarks(t *testing.T) {
	// Arrange
	suite, err := Parse(strings.NewReader(full92BenchmarkSampleOutput), AggregateMedian)
	require.NoError(t, err)
	rules := DefaultTargets()

	// Act
	rep, err := CheckTargets(suite, rules, true)

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 92, rep.Evaluated)
	assert.Equal(t, 92, rep.Matched)
	assert.Empty(t, rep.Unmatched)
	assert.Empty(t, rep.Violations)
	assert.True(t, rep.Passed())

	// Also verify that every top-level Benchmark_ function in ../../benchmarks_test.go is represented.
	benchSrc, readErr := os.ReadFile(filepath.Join("..", "..", "benchmarks_test.go"))
	require.NoError(t, readErr)
	topLevelCount := 0
	for line := range strings.SplitSeq(string(benchSrc), "\n") {
		if strings.HasPrefix(line, "func Benchmark_") {
			topLevelCount++
			fnName := strings.TrimPrefix(line, "func ")
			fnName = strings.Split(fnName, "(")[0]
			matchedAny := false
			for _, benchName := range suite.Order {
				if benchName == fnName || strings.HasPrefix(benchName, fnName+"/") {
					matchedAny = true
					break
				}
			}
			assert.True(t, matchedAny, "benchmark function %s in benchmarks_test.go must be covered", fnName)
		}
	}
	assert.Equal(t, 48, topLevelCount)
}

func TestCheckTargets_DetectsZeroAllocLatencyBytesAndCustomViolations(t *testing.T) {
	// Arrange
	violatingInput := `
Benchmark_Get_MapCache/Flat-96                    	 1000000	        55.0 ns/op	       8 B/op	       1 allocs/op
Benchmark_Peek_MapCache-96                        	  100000	       950.0 ns/op	       0 B/op	       0 allocs/op
Benchmark_LargeScale_Put_100K/RadixCache-96       	       2	  27000000 ns/op	       185.0 heap-B/entry	 9627472 B/op	  100011 allocs/op
Benchmark_ArenaRadixCache_Compact-96              	     100	    520000 ns/op	    200000 reclaimed-B/op	  587008 B/op	       4 allocs/op
`
	suite, err := Parse(strings.NewReader(violatingInput), AggregateMedian)
	require.NoError(t, err)

	// Act
	rep, err := CheckTargets(suite, DefaultTargets(), false)

	// Assert
	require.NoError(t, err)
	assert.False(t, rep.Passed())
	require.Len(t, rep.Violations, 5)

	// 1 & 2: Benchmark_Get_MapCache/Flat violates 0 B/op and 0 allocs/op
	assert.Equal(t, "Benchmark_Get_MapCache/Flat", rep.Violations[0].Benchmark)
	assert.Equal(t, UnitBytesPerOp, rep.Violations[0].Metric)
	assert.Equal(t, "Benchmark_Get_MapCache/Flat", rep.Violations[1].Benchmark)
	assert.Equal(t, UnitAllocsPerOp, rep.Violations[1].Metric)

	// 3: Benchmark_Peek_MapCache violates MaxNsPerOp (950 > 75)
	assert.Equal(t, "Benchmark_Peek_MapCache", rep.Violations[2].Benchmark)
	assert.Equal(t, UnitNsPerOp, rep.Violations[2].Metric)

	// 4: Benchmark_LargeScale_Put_100K/RadixCache violates MaxHeapBytesPerEnt (185 > 102)
	assert.Equal(t, "Benchmark_LargeScale_Put_100K/RadixCache", rep.Violations[3].Benchmark)
	assert.Equal(t, UnitHeapBytesPerEnt, rep.Violations[3].Metric)

	// 5: Benchmark_ArenaRadixCache_Compact violates MinReclaimedBytesOp (200000 < 680000)
	assert.Equal(t, "Benchmark_ArenaRadixCache_Compact", rep.Violations[4].Benchmark)
	assert.Equal(t, UnitReclaimedBytesOp, rep.Violations[4].Metric)

	// Also verify missing required custom metric is flagged as a violation.
	missingCustomInput := "Benchmark_ArenaRadixCache_Compact-96 100 520000 ns/op 587008 B/op 4 allocs/op\n"
	missingSuite, err := Parse(strings.NewReader(missingCustomInput), AggregateMedian)
	require.NoError(t, err)
	missingRep, err := CheckTargets(missingSuite, DefaultTargets(), false)
	require.NoError(t, err)
	assert.False(t, missingRep.Passed())
	require.Len(t, missingRep.Violations, 1)
	assert.True(t, missingRep.Violations[0].Missing)
	assert.Equal(t, UnitReclaimedBytesOp, missingRep.Violations[0].Metric)
}

func TestLoadTargets_CustomJSONConfig(t *testing.T) {
	// Arrange
	tmpDir := t.TempDir()
	jsonConfig := `[
		{
			"pattern": "^Benchmark_Custom_[A-Za-z]+$",
			"description": "Regex custom benchmark target",
			"max_ns_per_op": 150,
			"max_bytes_per_op": 0,
			"max_allocs_per_op": 0
		}
	]`
	jsonPath := writeTempDataFile(t, tmpDir, "custom_targets.json", jsonConfig)
	suite, err := Parse(strings.NewReader("Benchmark_Custom_Alpha-8 1000 120.0 ns/op 0 B/op 0 allocs/op\n"), AggregateMedian)
	require.NoError(t, err)

	// Act
	rules, err := LoadTargets(jsonPath)
	require.NoError(t, err)
	rep, err := CheckTargets(suite, rules, true)

	// Assert
	require.NoError(t, err)
	assert.True(t, rep.Passed())
	assert.Equal(t, 1, rep.Matched)

	// Verify invalid JSON and invalid regex errors
	badJSONPath := writeTempDataFile(t, tmpDir, "bad.json", "{not-valid-json")
	_, err = LoadTargets(badJSONPath)
	require.Error(t, err)

	badRegexPath := writeTempDataFile(t, tmpDir, "bad_regex.json", `[{"pattern": "^[unclosed"}]`)
	_, err = LoadTargets(badRegexPath)
	require.Error(t, err)

	_, err = LoadTargets(filepath.Join(tmpDir, "nonexistent.json"))
	require.Error(t, err)
}

func TestCLI_EndToEndExitCodesAndStepSummary(t *testing.T) {
	tmpDir := t.TempDir()
	validDataPath := writeTempDataFile(t, tmpDir, "valid_run.golden", full92BenchmarkSampleOutput)

	t.Run("exit 0 when targets and baseline comparison pass and writes step summary", func(t *testing.T) {
		// Arrange
		summaryPath := filepath.Join(tmpDir, "step_summary.md")
		var stdout, stderr bytes.Buffer

		// Act
		code := run(
			[]string{
				"-base", validDataPath,
				"-head", validDataPath,
				"-strict-missing",
				"-format", "markdown",
				"-summary", summaryPath,
			},
			nil,
			&stdout,
			&stderr,
		)

		// Assert
		assert.Equal(t, exitOK, code)
		assert.Empty(t, stderr.String())
		assert.Contains(t, stdout.String(), "## ✅ Performance Benchmark & Target Check: PASSED")

		summaryBytes, err := os.ReadFile(summaryPath)
		require.NoError(t, err)
		assert.Equal(t, stdout.String(), string(summaryBytes))
	})

	t.Run("exit 0 with positional args, stdin candidate, and text format", func(t *testing.T) {
		// Arrange
		var stdout, stderr bytes.Buffer

		// Act
		code := run(
			[]string{"-format", "text", validDataPath, "-"},
			strings.NewReader(full92BenchmarkSampleOutput),
			&stdout,
			&stderr,
		)

		// Assert
		assert.Equal(t, exitOK, code)
		assert.Empty(t, stderr.String())
		assert.Contains(t, stdout.String(), "Performance Benchmark & Target Check: PASSED")
	})

	t.Run("exit 1 when relative regression exceeds threshold", func(t *testing.T) {
		// Arrange
		regressedOutput := strings.Replace(
			full92BenchmarkSampleOutput,
			"Benchmark_Get_MapCache/Flat-96                                \t 1245382\t        48.94 ns/op\t       0 B/op\t       0 allocs/op",
			"Benchmark_Get_MapCache/Flat-96                                \t  500000\t       180.00 ns/op\t       0 B/op\t       0 allocs/op",
			1,
		)
		regressedPath := writeTempDataFile(t, tmpDir, "regressed_run.golden", regressedOutput)
		var stdout, stderr bytes.Buffer

		// Act
		code := run(
			[]string{"-base", validDataPath, "-head", regressedPath},
			nil,
			&stdout,
			&stderr,
		)

		// Assert
		assert.Equal(t, exitViolation, code)
		assert.Contains(t, stdout.String(), "## ❌ Performance Benchmark & Target Check: FAILED")
		assert.Contains(t, stdout.String(), "Relative Regression")
		assert.Contains(t, stdout.String(), "Benchmark_Get_MapCache/Flat")
	})

	t.Run("exit 1 when absolute target is violated without baseline", func(t *testing.T) {
		// Arrange
		targetViolating := "Benchmark_Get_MapCache/Flat-96 1000000 50.0 ns/op 16 B/op 1 allocs/op\n"
		violatingPath := writeTempDataFile(t, tmpDir, "target_violation.golden", targetViolating)
		var stdout, stderr bytes.Buffer

		// Act
		code := run(
			[]string{"-head", violatingPath, "-format", "text"},
			nil,
			&stdout,
			&stderr,
		)

		// Assert
		assert.Equal(t, exitViolation, code)
		assert.Contains(t, stdout.String(), "Performance Benchmark & Target Check: FAILED")
		assert.Contains(t, stdout.String(), "[TARGET FAIL] Benchmark_Get_MapCache/Flat")
	})

	t.Run("exit 2 on CLI flag errors, negative thresholds, or malformed input", func(t *testing.T) {
		// Arrange
		malformedPath := writeTempDataFile(t, tmpDir, "malformed.golden", "not a benchmark\n")
		var stdout, stderr bytes.Buffer

		// Act & Assert: missing candidate
		assert.Equal(t, exitUsageErr, run([]string{}, nil, &stdout, &stderr))

		// Act & Assert: negative threshold
		assert.Equal(t, exitUsageErr, run([]string{"-head", validDataPath, "-max-ns-regression", "-0.1"}, nil, &stdout, &stderr))

		// Act & Assert: unsupported format
		assert.Equal(t, exitUsageErr, run([]string{"-head", validDataPath, "-format", "xml"}, nil, &stdout, &stderr))

		// Act & Assert: malformed benchmark file
		assert.Equal(t, exitUsageErr, run([]string{"-head", malformedPath}, nil, &stdout, &stderr))
	})
}
