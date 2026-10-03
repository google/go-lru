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

	"github.com/google/go-lru"
	"github.com/google/go-lru/otellru"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// ExampleRegister demonstrates registering asynchronous OpenTelemetry observable
// counters and gauges for an lru.Cache instance via otellru.Register.
func ExampleRegister() {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	cache := lru.New[string](2, lru.WithBackend(lru.BackendArenaRadix))

	reg, err := otellru.Register(
		cache,
		otellru.WithMeterProvider(mp),
		otellru.WithName("session-cache"),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = reg.Unregister() }()

	_, _ = cache.Put("user:1", "alice")
	_, _ = cache.Put("user:2", "bob")
	_, _ = cache.Get("user:1")            // hit
	_, _ = cache.Get("user:absent")       // miss
	_, _ = cache.Put("user:3", "charlie") // evicts "user:2"

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		panic(err)
	}

	st := cache.Stats()
	fmt.Printf("scope=%s metrics=%d\n", rm.ScopeMetrics[0].Scope.Name, len(rm.ScopeMetrics[0].Metrics))
	fmt.Printf("entries=%d hits=%d misses=%d capacity_evictions=%d\n",
		st.Len, st.GetHits, st.GetMisses, st.EvictionsCapacity)
	// Output:
	// scope=github.com/google/go-lru/otellru metrics=16
	// entries=2 hits=1 misses=1 capacity_evictions=1
}
