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

import "errors"

// Predefined sentinel errors returned by Cache operations.
var (
	// ErrInvalidEntrySize is returned when the weight/size of an entry exceeds the cache's maxSize.
	ErrInvalidEntrySize = errors.New("size of the entry is more than the cache's maxSize")

	// ErrEntryNotExist is returned when attempting to update or modify an entry that does not exist in the cache.
	ErrEntryNotExist = errors.New("entry with given key does not exist")
)
