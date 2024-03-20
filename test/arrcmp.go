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
package main

import (
	"fmt"
	"maps"
)

func main() {
	srcM := map[string]int64{
		"a": 6,
		"b": 7,
		"d": 9,
	}
	destM := map[string]int64{
		"a": 2,
		"b": 10,
		"c": 8,
	}
	added, removed := arrcmp(destM, srcM)
	fmt.Printf("add: %v\nrem: %v\n", added, removed)
}

// src and dest store key-value pair , and key data row and value data row counts
func arrcmp(src map[string]int64, dest map[string]int64) (map[string]int64, map[string]int64) {
	// source element map mall
	columnDataMall := maps.Clone(src) // source and target element map temp malls

	addedSrcSets := make(map[string]int64)
	delSrcSets := make(map[string]int64)

	// data intersection
	intersectionSets := make(map[string]int64)

	// data comparison through set operations
	for dk, dv := range dest {
		if val, exist := columnDataMall[dk]; exist {
			if dv == val {
				intersectionSets[dk] = dv
			} else if dv < val {
				// target records is less than source records
				delSrcSets[dk] = val - dv
				delete(src, dk)
				delete(columnDataMall, dk)
			} else {
				// target records is more than source records
				addedSrcSets[dk] = dv - val
				delete(src, dk)
				delete(columnDataMall, dk)
			}
		} else {
			columnDataMall[dk] = dv
		}
	}

	for dk, _ := range intersectionSets {
		delete(columnDataMall, dk)
	}

	for dk, dv := range columnDataMall {
		if _, exist := src[dk]; exist {
			delSrcSets[dk] = dv
		} else {
			addedSrcSets[dk] = dv
		}
	}
	return addedSrcSets, delSrcSets
}
