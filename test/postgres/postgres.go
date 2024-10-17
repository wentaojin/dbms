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
	"strconv"

	"github.com/greatcloak/decimal"
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
		Password:       "xyzn",
		Host:           "192.16.201.89",
		Port:           54321,
		ConnectCharset: "UTF8",
		DbName:         "marvin",
	}, 300)
	if err != nil {
		panic(err)
	}

	rows, err := d.QueryContext(d.Ctx, `select 
    i00, i01, i02, b00, b01, b02, r00, n00, n01, n02, 
    d00, d01, d02, m00, c00, c01, c02, c03, nc00, nc01, 
    nc02, nc03, v00, v01, v02, v03, nv00, nv01, nv02, 
    nv03, to_char(de00,'yyyy-mm-dd'), to_char(t00,'yyyy-mm-dd'), to_char(t01,'yyyy-mm-dd hh24:mi:ss.us'), to_char(t02,'yyyy-mm-dd hh24:mi:ss.us'), it00, in01, by00, tx00, 
    cr00, in00, mc00, ud00, x00, js, vec00, tq00, ar00, 
    po00, li00, ls00, bx00, ph, py, cl00, txd00 from marvin.marvin00`)

	if err != nil {
		panic(err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}

	var (
		columnNameOrders []string
		dbTypeNames      []string
	)
	for _, ct := range colTypes {
		columnNameOrders = append(columnNameOrders, ct.Name())
		//fmt.Printf("column: %v, type: %v, scan: %v\n", ct.Name(), ct.DatabaseTypeName(), ct.ScanType())
		dbTypeNames = append(dbTypeNames, ct.DatabaseTypeName())
	}

	// data scan
	values := make([]interface{}, len(columnNameOrders))
	valuePtrs := make([]interface{}, len(columnNameOrders))
	for i, _ := range columnNameOrders {
		valuePtrs[i] = &values[i]
	}

	rowDatas := make([]string, len(columnNameOrders))

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			panic(err)
		}

		for i, c := range columnNameOrders {
			valRes := values[i]
			if stringutil.IsValueNil(valRes) {
				//rowsMap[cols[i]] = `NULL` -> sql
				fmt.Printf("column: %v, value: %v\n", c, `NULL`)
			} else {
				if err := iter(i, c, dbTypeNames[i], valRes, rowDatas); err != nil {
					panic(err)
				}
			}
		}
		fmt.Println(rowDatas)
	}

	if err = rows.Err(); err != nil {
		panic(err)
	}
	fmt.Println("success")
}

func iter(idx int, c string, dbTypes string, valRes interface{}, rowDatas []string) error {
	v := reflect.ValueOf(valRes)

	switch v.Kind() {
	case reflect.Int16, reflect.Int32, reflect.Int64:
		fmt.Printf("column: %v, type: %v, value: %v\n", c, dbTypes, v.Int())
		rowDatas[idx] = decimal.NewFromInt(v.Int()).String()
	case reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fmt.Printf("column: %v, type: %v, value: %v\n", c, dbTypes, v.Uint())
		rowDatas[idx] = strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		fmt.Printf("column: %v, type: %v, value: %v\n", c, dbTypes, v.Float())
		rowDatas[idx] = decimal.NewFromFloat(v.Float()).String()
	case reflect.Bool:
		fmt.Printf("column: %v, type: %v, value: %v\n", c, dbTypes, v.Bool())
		rowDatas[idx] = strconv.FormatBool(v.Bool())
	case reflect.String:
		fmt.Printf("column: %v, type: %v, value: %v\n", c, dbTypes, v.String())
		rowDatas[idx] = v.String()
	case reflect.Array, reflect.Slice:
		fmt.Printf("column: %v, type: %v, value: %v\n", c, dbTypes, stringutil.BytesToString(v.Bytes()))
		rowDatas[idx] = stringutil.BytesToString(v.Bytes())
	case reflect.Interface:
		if err := iter(idx, c, dbTypes, v.Elem().Interface(), rowDatas); err != nil {
			return err
		}
	default:
		fmt.Printf("column: %v, value: %v, kind: %v\n", c, v.String(), v.Kind())
		panic("not supported")
	}
	return nil
}
