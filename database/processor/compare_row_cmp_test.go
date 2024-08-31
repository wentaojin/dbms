/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package processor

import (
	"reflect"
	"runtime"
	"strconv"
	"testing"
)

// TestCmp tests the Cmp function with correctness cases.
func TestCmp(t *testing.T) {
	tests := []struct {
		name    string
		src     map[string]int64
		dest    map[string]int64
		wantAdd map[string]int64
		wantDel map[string]int64
	}{
		{"empty maps", make(map[string]int64), make(map[string]int64), make(map[string]int64), make(map[string]int64)},
		{"identical maps", map[string]int64{"key1": 10}, map[string]int64{"key1": 10}, make(map[string]int64), make(map[string]int64)},
		{"src one maps", map[string]int64{"key1": 10}, map[string]int64{}, map[string]int64{"key1": 10}, make(map[string]int64)},
		{"dest one maps", map[string]int64{}, map[string]int64{"key1": 10}, make(map[string]int64), map[string]int64{"key1": 10}},
		{"partial match maps", map[string]int64{"key1": 10, "key2": 20}, map[string]int64{"key1": 10, "key2": 15}, map[string]int64{"key2": 5}, make(map[string]int64)},
		{"no match maps", map[string]int64{"key1": 10}, map[string]int64{"key2": 20}, map[string]int64{"key1": 10}, map[string]int64{"key2": 20}},
		{"multiple keys", map[string]int64{"key1": 10, "key2": 20, "key3": 30, "key5": 10}, map[string]int64{"key1": 10, "key2": 15, "key4": 5, "key5": 20}, map[string]int64{"key2": 5, "key3": 30}, map[string]int64{"key4": 5, "key5": 10}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAdd, gotDel := Cmp(tt.src, tt.dest)
			if !reflect.DeepEqual(gotAdd, tt.wantAdd) {
				t.Errorf("Cmp(%v, %v) added = %v, want %v", tt.src, tt.dest, gotAdd, tt.wantAdd)
			}
			if !reflect.DeepEqual(gotDel, tt.wantDel) {
				t.Errorf("Cmp(%v, %v) deleted = %v, want %v", tt.src, tt.dest, gotDel, tt.wantDel)
			}
		})
	}
}

// BenchmarkCmp tests the performance of the Cmp function.
func BenchmarkCmp(b *testing.B) {
	runtime.GOMAXPROCS(1)

	// Generate test data.
	src := make(map[string]int64)
	dest := make(map[string]int64)

	// Populate the maps with random data.
	for i := 0; i < 1000000; i++ {
		key := "key" + strconv.Itoa(i)
		src[key] = int64(i)
		dest[key] = int64(i + 1)
	}

	// Run the benchmark.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Cmp(src, dest)
	}
}
