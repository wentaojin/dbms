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
	"unsafe"
)

func main() {
	batchSize := 2
	columnNameOrders := []string{"a", "b", "c", "d", "e"}
	columnNameOrdersCounts := len(columnNameOrders)
	rowData := make([]interface{}, columnNameOrdersCounts)

	argsNums := batchSize * columnNameOrdersCounts

	batchRowsData := make([]interface{}, 0, argsNums)

	columnNameOrderIndexMap := make(map[string]int, columnNameOrdersCounts)

	for i, c := range columnNameOrders {
		columnNameOrderIndexMap[c] = i
	}
	addr := uintptr(unsafe.Pointer(&batchRowsData))
	fmt.Printf("Slice : %p\n", addr)

	rawResult := []map[string]interface{}{
		{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
		{"a": 1, "b": 2, "c": 8, "d": 2, "e": 6},
		{"a": 1, "b": 2, "c": 8, "d": 2, "e": 6},
		{"a": 1, "b": 2, "c": 8, "d": 2, "e": 6},
		{"a": 1, "b": 2, "c": 8, "d": 2, "e": 6},
		{"a": 1, "b": 2, "c": 8, "d": 2, "e": 6},
	}

	for _, val := range rawResult {
		//fmt.Printf("column counts [%d], batch size [%d], args size [%d], row init size [%d], current init size [%d]\n", columnNameOrdersCounts, batchSize, argsNums, len(rowData), len(batchRowsData))
		for k, v := range val {
			rowData[columnNameOrderIndexMap[k]] = v
		}
		//fmt.Printf("column counts [%d], batch size [%d], args size [%d], row append size [%d], current init size [%d]\n", columnNameOrdersCounts, batchSize, argsNums, len(rowData), len(batchRowsData))

		// temporary array
		batchRowsData = append(batchRowsData, rowData...)
		//fmt.Printf("column counts [%d], batch size [%d], args size [%d], row current size [%d], current append size [%d]\n", columnNameOrdersCounts, batchSize, argsNums, len(rowData), len(batchRowsData))

		// clear
		rowData = make([]interface{}, columnNameOrdersCounts)

		//fmt.Printf("column counts [%d], batch size [%d], args size [%d], row clear size [%d], current current size [%d]\n", columnNameOrdersCounts, batchSize, argsNums, len(rowData), len(batchRowsData))
		// batch
		if len(batchRowsData) == argsNums {
			addr01 := uintptr(unsafe.Pointer(&batchRowsData))
			fmt.Printf("Slice : %p\n", addr01)
			fmt.Printf("column counts [%d], batch size [%d], args size [%d], row clear size [%d], current rows size [%d]\n", columnNameOrdersCounts, batchSize, argsNums, len(rowData), len(batchRowsData))
			// clear
			batchRowsData = make([]interface{}, 0, argsNums)
			fmt.Printf("column counts [%d], batch size [%d], args size [%d], row clear size [%d], current clear size [%d]\n", columnNameOrdersCounts, batchSize, argsNums, len(rowData), len(batchRowsData))
		}

	}
}
