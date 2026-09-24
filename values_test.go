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

package lru

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackendSelectionAndUnifiedNew(t *testing.T) {
	// Arrange & Act
	defaultCache := New(1024)
	mapCacheInstance := New(1024, WithBackend(BackendMap))
	radixCacheInstance := New(1024, WithBackend(BackendRadix))
	arenaCacheInstance := New(1024, WithBackend(BackendArenaRadix))
	unknownBackendCache := New(1024, WithBackend(Backend(99)))

	// Assert concrete backend types & String() representations
	assert.Equal(t, "MapCache", BackendMap.String())
	assert.Equal(t, "RadixCache", BackendRadix.String())
	assert.Equal(t, "ArenaRadixCache", BackendArenaRadix.String())
	assert.Equal(t, "UnknownBackend", Backend(99).String())

	_, isDefaultMap := defaultCache.(*mapCache)
	assert.True(t, isDefaultMap, "default New() should construct *mapCache")

	_, isMap := mapCacheInstance.(*mapCache)
	assert.True(t, isMap, "WithBackend(BackendMap) should construct *mapCache")

	_, isRadix := radixCacheInstance.(*radixCache)
	assert.True(t, isRadix, "WithBackend(BackendRadix) should construct *radixCache")

	_, isArena := arenaCacheInstance.(*arenaRadix)
	assert.True(t, isArena, "WithBackend(BackendArenaRadix) should construct *arenaRadix")
	_, implementsPressureAware := arenaCacheInstance.(PressureAwareCache)
	assert.True(t, implementsPressureAware, "ArenaRadixCache returned by New should implement PressureAwareCache")

	_, isUnknownNormalizedToMap := unknownBackendCache.(*mapCache)
	assert.True(t, isUnknownNormalizedToMap, "unrecognized Backend value should normalize to BackendMap")
}

func TestValueWrappers(t *testing.T) {
	// Arrange
	sv := NewStringValue("hello-lru")
	bv := NewBytesValue([]byte{0x01, 0x02, 0x03, 0x04})
	type customPayload struct {
		ID   int
		Name string
	}
	sized := NewSizedValue(customPayload{ID: 7, Name: "node"}, 128)
	shorthand := NewValue("inline-payload", 64)

	// Assert ValueType interface compliance & helper methods
	assert.Equal(t, uint64(9), sv.Size())
	assert.Equal(t, "hello-lru", sv.String())

	assert.Equal(t, uint64(4), bv.Size())
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, bv.Bytes())

	assert.Equal(t, uint64(128), sized.Size())
	assert.Equal(t, customPayload{ID: 7, Name: "node"}, sized.Unwrap())

	assert.Equal(t, uint64(64), shorthand.Size())
	assert.Equal(t, "inline-payload", shorthand.Unwrap())

	// Verify caching across all three backends
	for _, b := range []Backend{BackendMap, BackendRadix, BackendArenaRadix} {
		cache := New(512, WithBackend(b), WithInvariantChecking(true))
		evicted, err := cache.Insert("k1", sv)
		require.NoError(t, err)
		assert.Empty(t, evicted)

		evicted, err = cache.Insert("k2", bv)
		require.NoError(t, err)
		assert.Empty(t, evicted)

		evicted, err = cache.Insert("k3", sized)
		require.NoError(t, err)
		assert.Empty(t, evicted)

		assert.Equal(t, sv, cache.LookUp("k1"))
		assert.Equal(t, bv, cache.LookUp("k2"))
		assert.Equal(t, sized, cache.LookUp("k3"))
	}
}
