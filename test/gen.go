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
	"strings"
)

// GenerateRangeCondition 生成范围查询条件
func GenerateRangeCondition(columns []string, values [][]interface{}) string {
	var conditions []string

	for i, column := range columns {
		condition := fmt.Sprintf("(%s > %v OR (%s = %v", column, values[i][0], column, values[i][0])
		for j := 1; j < len(values[i]); j++ {
			condition += fmt.Sprintf(" AND %s >= %v", column, values[i][j])
		}
		condition += ")"
		conditions = append(conditions, condition)
	}

	return "WHERE " + strings.Join(conditions, " AND ")
}

func main() {
	// 定义联合索引的列名
	columns := []string{"ID", "NV", "D"}
	// 定义范围的起始点和结束点

	values := [][]interface{}{
		{605, 574},
		{"'VVFQoPVmST'", "'GIilRfWPPe'"},
		{"TO_DATE('2010-11-26 07:32:19','YYYY-MM-DD HH24:MI:SS')", "TO_DATE('2006-03-10 06:49:44','YYYY-MM-DD HH24:MI:SS')"},
	}

	// 生成 SQL 查询条件
	condition := GenerateRangeCondition(columns, values)
	fmt.Println(condition)
}
