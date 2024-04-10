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
	"context"
	"fmt"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model/datasource"
)

func main() {
	ctx := context.Background()

	databaseS, err := database.NewDatabase(ctx, &datasource.Datasource{
		DbType:         "tidb",
		Username:       "root",
		Password:       "",
		Host:           "120.92.77.145",
		Port:           4000,
		ConnectCharset: "utf8mb4",
	}, "")
	if err != nil {
		panic(err)
	}

	rows, err := databaseS.QueryContext(ctx, `SELECT  * FROM STEVEN.MARVIN_COLUMN_T LIMIT 1`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}

	var (
		columnNames []string
		scanTypes   []string
	)
	for _, c := range colTypes {
		columnNames = append(columnNames, c.Name())
		//fmt.Println(c.Name(), c.ScanType(), c.DatabaseTypeName())
		scanTypes = append(scanTypes, c.ScanType().String())
	}

	fmt.Println(scanTypes)
	// data scan
	columnNums := len(columnNames)
	rawResult := make([][]byte, columnNums)
	valuePtrs := make([]interface{}, columnNums)
	for i := range rawResult {
		valuePtrs[i] = &rawResult[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			panic(err)
		}

		//for _, raw := range rawResult {
		//if stringutil.IsValueNil(raw) {
		//	fmt.Println("nil")
		//} else {
		//	fmt.Println(reflect.TypeOf(raw))
		//}

		for i, _ := range columnNames {
			val := rawResult[i]
			if val == nil {
				fmt.Printf("%v\n", `NULL`)
			} else if stringutil.BytesToString(val) == "" {
				fmt.Printf("%v\n", `NULL`)
			} else {
				switch scanTypes[i] {
				case "sql.NullInt16":
					r, err := stringutil.StrconvIntBitSize(stringutil.BytesToString(val), 16)
					if err != nil {
						panic(err)
					}
					fmt.Println(r)
				case "sql.NullInt32":
					r, err := stringutil.StrconvIntBitSize(stringutil.BytesToString(val), 32)
					if err != nil {
						panic(err)
					}
					fmt.Println(r)
				case "sql.NullInt64":
					r, err := stringutil.StrconvIntBitSize(stringutil.BytesToString(val), 64)
					if err != nil {
						panic(err)
					}
					fmt.Println(r)
				case "sql.NullFloat64":
					r, err := stringutil.StrconvFloatBitSize(stringutil.BytesToString(val), 64)
					if err != nil {
						panic(err)
					}
					fmt.Println(r)
				case "sql.NullString":
					fmt.Println("xxx", stringutil.BytesToString(val))
				default:
					fmt.Printf("'%v'\n", stringutil.BytesToString(val))
				}
			}
		}
		//}

		if err = rows.Err(); err != nil {
			panic(err)
		}

	}
}
