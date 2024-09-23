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
	"github.com/wentaojin/dbms/database/processor"
)

func main() {
	srcM := map[string]int64{
		"1|#|'2024-06-12 23:50:10.000000'": 1,
		"2|#|'2024-06-12 23:50:10.000000'": 1,
		"3|#|'2024-06-12 23:50:10.123000'": 1,
		"4|#|'2024-06-12 23:50:10.123456'": 1,
		"5|#|'2024-06-12 12:12:12.000000'": 1,
	}
	destM := map[string]int64{
		"1|#|'2024-06-12 23:50:10.000000'": 1,
		"2|#|'2024-06-12 23:50:10.000000'": 1,
		"3|#|'2024-06-12 23:50:10.123000'": 1,
		"4|#|'2024-06-12 23:50:10.123456'": 1,
		"5|#|'2024-06-12 23:50:10.000000'": 1,
	}
	added, removed := processor.Cmp(destM, srcM)

	fmt.Printf("add: %v\nrem: %v\n", added, removed)
}
