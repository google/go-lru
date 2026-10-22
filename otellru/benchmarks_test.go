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

package otellru_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-lru"
	"github.com/google/go-lru/otellru"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func BenchmarkOtelScrape_DirectCallback(b *testing.B) {
	backends := []struct {
		name string
		make func() lru.StatsProvider
	}{
		{name: "MapCache", make: func() lru.StatsProvider { return lru.NewMapCache[int](1000) }},
		{name: "RadixCache", make: func() lru.StatsProvider { return lru.NewRadixCache[int](1000) }},
		{name: "ArenaRadixCache", make: func() lru.StatsProvider { return lru.NewArenaRadixCache[int](1000) }},
	}

	for _, bk := range backends {
		b.Run(bk.name, func(b *testing.B) {
			cache := bk.make()
			capturingMeter := &callbackCapturingMeter{}
			reg, err := otellru.Register(
				cache,
				otellru.WithMeter(capturingMeter),
				otellru.WithName("bench-cache"),
				otellru.WithAttributes(attribute.String("env", "bench")),
			)
			require.NoError(b, err)
			b.Cleanup(func() { _ = reg.Unregister() })

			obs := &zeroAllocObserver{}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = capturingMeter.cb(ctx, obs)
			}
		})
	}
}

func BenchmarkOtelScrape_FullSDKCollect(b *testing.B) {
	for _, numCaches := range []int{1, 10} {
		b.Run(fmt.Sprintf("Caches_%d", numCaches), func(b *testing.B) {
			reader := sdkmetric.NewManualReader()
			mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			b.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

			for i := range numCaches {
				var c lru.Cache[int]
				switch i % 3 {
				case 0:
					c = lru.NewArenaRadixCache[int](1000)
				case 1:
					c = lru.NewMapCache[int](1000)
				default:
					c = lru.NewRadixCache[int](1000)
				}
				_, _ = c.Put("dir/k1", 1)
				reg, err := otellru.Register(c, otellru.WithMeterProvider(mp), otellru.WithName(fmt.Sprintf("cache-%d", i)))
				require.NoError(b, err)
				b.Cleanup(func() { _ = reg.Unregister() })
			}

			ctx := context.Background()
			var rm metricdata.ResourceMetrics
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = reader.Collect(ctx, &rm)
			}
		})
	}
}
