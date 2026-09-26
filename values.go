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

import "strings"

// StringValue is a ValueType wrapper for a standard Go string whose Size() is its byte length (len(s)).
type StringValue string

// NewStringValue clones s and wraps it as a StringValue implementing ValueType.
// Cloning prevents substring values from pinning large underlying caller backing arrays in memory.
func NewStringValue(s string) StringValue {
	return StringValue(strings.Clone(s))
}

// Size returns the byte length of the string (uint64(len(s))).
func (s StringValue) Size() uint64 {
	return uint64(len(s))
}

// String returns the underlying string.
func (s StringValue) String() string {
	return string(s)
}

// BytesValue is an immutable, comparable ValueType wrapper for a byte slice whose Size() is its byte length (len(b)).
type BytesValue struct {
	data   string
	nonNil bool
}

// NewBytesValue clones b and wraps it as an immutable BytesValue implementing ValueType.
// Cloning prevents caller mutations from racing with cached readers and prevents sub-slices
// from pinning large underlying backing arrays in memory.
func NewBytesValue(b []byte) BytesValue {
	if b == nil {
		return BytesValue{}
	}
	return BytesValue{
		data:   string(b),
		nonNil: true,
	}
}

// Size returns the byte length of the slice (uint64(len(b))).
func (b BytesValue) Size() uint64 {
	return uint64(len(b.data))
}

// Bytes returns a defensive copy of the underlying []byte slice.
func (b BytesValue) Bytes() []byte {
	if len(b.data) == 0 {
		if !b.nonNil {
			return nil
		}
		return []byte{}
	}
	return []byte(b.data)
}

// SizedValue wraps an arbitrary value of type T with an explicit logical or byte size,
// allowing any type to be cached without defining a custom struct implementing ValueType.
type SizedValue[T any] struct {
	Value    T
	ByteSize uint64
}

// NewSizedValue wraps val with the specified logical or byte size as a SizedValue[T].
func NewSizedValue[T any](val T, size uint64) SizedValue[T] {
	return SizedValue[T]{
		Value:    val,
		ByteSize: size,
	}
}

// NewValue is a shorthand constructor alias for NewSizedValue.
func NewValue[T any](val T, size uint64) SizedValue[T] {
	return NewSizedValue(val, size)
}

// Size returns the logical or byte size configured for this entry.
func (v SizedValue[T]) Size() uint64 {
	return v.ByteSize
}

// Unwrap returns the underlying value of type T.
func (v SizedValue[T]) Unwrap() T {
	return v.Value
}
