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
	"reflect"

	"github.com/wentaojin/dbms/database/postgresql"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func main() {
	ctx := context.Background()
	d, err := postgresql.NewDatabase(ctx, &datasource.Datasource{
		DatasourceName: "postgres",
		DbType:         "postgres",
		Username:       "marvin",
		Password:       "marvin",
		Host:           "192.16.201.89",
		Port:           54321,
		ConnectCharset: "UTF8",
		DbName:         "marvin",
	}, 300)
	if err != nil {
		panic(err)
	}

	rows, err := d.QueryContext(d.Ctx, `select * from marvin.marvin04 limit 1`)

	if err != nil {
		panic(err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}

	var columnNameOrders []string
	for _, ct := range colTypes {
		columnNameOrders = append(columnNameOrders, ct.Name())
	}

	// data scan
	values := make([]interface{}, len(columnNameOrders))
	valuePtrs := make([]interface{}, len(columnNameOrders))
	for i, _ := range columnNameOrders {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			panic(err)
		}

		for i, _ := range columnNameOrders {
			valRes := values[i]
			if stringutil.IsValueNil(valRes) {
				//rowsMap[cols[i]] = `NULL` -> sql
				fmt.Println("nil")
			} else {
				value := reflect.ValueOf(valRes).Interface()
				switch val := value.(type) {
				case interface{}:
					fmt.Println(1111)
				default:
					fmt.Println(val)
				}
			}
		}
	}

	if err = rows.Err(); err != nil {
		panic(err)
	}
	fmt.Println("success")
}
