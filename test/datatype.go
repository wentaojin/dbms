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
	"strings"

	"github.com/godror/godror"

	"github.com/greatcloak/decimal"
	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func main() {
	ctx := context.Background()

	databaseS, err := database.NewDatabase(ctx, &datasource.Datasource{
		DbType:         "oracle",
		Username:       "findpt",
		Password:       "findpt",
		Host:           "10.12.13.33",
		Port:           1521,
		ConnectCharset: "zhs16gbk",
		ServiceName:    "gbk",
	}, "MARVIN")
	if err != nil {
		panic(err)
	}

	//rows, err := databaseS.QueryContext(ctx, `SELECT  "N1","N2","N3","N4","N5","N6","N7","N8","N9","N10","NBFILE","VCHAR1","VCHAR2","VCHAR3","VCHAR4","CHAR1","CHAR2","CHAR3","CHAR4","CHAR5","CHAR6","CHAR7","CHAR8","CHAR9","CHAR10","DLOB","CFLOB",TO_CHAR("NDATE",'yyyy-MM-dd HH24:mi:ss') AS "NDATE","NDECIMAL1","NDECIMAL2","NDECIMAL3","NDECIMAL4","DP1","FP1","FP2","FY2","FY4","FY5",TO_CHAR("YT") AS "YT",TO_CHAR("YU") AS "YU","HP","RW1","RW2","RL","RD1","RD2",TO_CHAR("TP1",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP1",TO_CHAR("TP2",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP2",TO_CHAR("TP3",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP3",TO_CHAR("TP4",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP4", XMLSERIALIZE(CONTENT "XT" AS CLOB) AS "XT" FROM MARVIN_COLUMN_T WHERE ROWNUM = 1`)
	//if err != nil {
	//	panic(err)
	//}
	//defer rows.Close()
	//
	//colTypes, err := rows.ColumnTypes()
	//if err != nil {
	//	panic(err)
	//}
	//
	//var columnNames []string
	//for _, c := range colTypes {
	//	columnNames = append(columnNames, c.Name())
	//	fmt.Println(c.Name(), c.ScanType(), c.DatabaseTypeName())
	//}
	//
	//// data scan
	//columnNums := len(columnNames)
	//rawResult := make([]interface{}, columnNums)
	//valuePtrs := make([]interface{}, columnNums)
	//
	//for rows.Next() {
	//	for i := 0; i < columnNums; i++ {
	//		valuePtrs[i] = &rawResult[i]
	//	}
	//	err = rows.Scan(valuePtrs...)
	//	if err != nil {
	//		panic(err)
	//	}
	//
	//	for i, colName := range columnNames {
	//		val := rawResult[i]
	//		if stringutil.IsValueNil(val) {
	//			fmt.Println(colName, "nil", val)
	//		} else {
	//			fmt.Println(colName, reflect.TypeOf(val).String(), val)
	//		}
	//	}
	//}
	//
	//if err = rows.Err(); err != nil {
	//	panic(err)
	//}

	datas, err := GetDatabaseTableChunkData(ctx, databaseS, `SELECT  "N1","N2","N3","N4","N5","N6","N7","N8","N9","N10","NBFILE","VCHAR1","VCHAR2","VCHAR3","VCHAR4","CHAR1","CHAR2","CHAR3","CHAR4","CHAR5","CHAR6","CHAR7","CHAR8","CHAR9","CHAR10","DLOB","CFLOB",TO_CHAR("NDATE",'yyyy-MM-dd HH24:mi:ss') AS "NDATE","NDECIMAL1","NDECIMAL2","NDECIMAL3","NDECIMAL4","DP1","FP1","FP2","FY2","FY4","FY5",TO_CHAR("YT") AS "YT",TO_CHAR("YU") AS "YU","HP","RW1","RW2","RL","RD1","RD2",TO_CHAR("TP1",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP1",TO_CHAR("TP2",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP2",TO_CHAR("TP3",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP3",TO_CHAR("TP4",'yyyy-mm-dd hh24:mi:ss.ff6') AS "TP4", XMLSERIALIZE(CONTENT "XT" AS CLOB) AS "XT" FROM MARVIN_COLUMN_T WHERE ROWNUM=1`, 50, "GBK", "UTF8MB4", "N1,N2,N3,N4,N5,N6,N7,N8,N9,N10,NBFILE,VCHAR1,VCHAR2,VCHAR3,VCHAR4,CHAR1,CHAR2,CHAR3,CHAR4,CHAR5,CHAR6,CHAR7,CHAR8,CHAR9,CHAR10,DLOB,CFLOB,NDATE,NDECIMAL1,NDECIMAL2,NDECIMAL3,NDECIMAL4,DP1,FP1,FP2,FY2,FY4,FY5,YT,YU,HP,RW1,RW2,RL,RD1,RD2,TP1,TP2,TP3,TP4,XT")
	if err != nil {
		panic(err)
	}
	fmt.Println(datas)
}

func GetDatabaseTableChunkData(ctx context.Context, databaseS database.IDatabase, querySQL string, batchSize int, dbCharsetS, dbCharsetT, columnDetailO string) ([][]interface{}, error) {
	var (
		dataChan      [][]interface{}
		columnNames   []string
		databaseTypes []string
		err           error
	)
	columnNameOrders := stringutil.StringSplit(columnDetailO, constant.StringSeparatorComma)
	columnNameOrdersCounts := len(columnNameOrders)
	rowData := make([]interface{}, columnNameOrdersCounts)

	argsNums := batchSize * columnNameOrdersCounts

	batchRowsData := make([]interface{}, 0, argsNums)

	columnNameOrderIndexMap := make(map[string]int, columnNameOrdersCounts)

	for i, c := range columnNameOrders {
		columnNameOrderIndexMap[c] = i
	}

	rows, err := databaseS.QueryContext(ctx, querySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	for _, ct := range colTypes {
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(ct.Name()), dbCharsetS, constant.CharsetUTF8MB4)
		if err != nil {
			return nil, fmt.Errorf("column [%s] charset convert failed, %v", ct.Name(), err)
		}
		columnNames = append(columnNames, stringutil.BytesToString(convertUtf8Raw))
		databaseTypes = append(databaseTypes, ct.DatabaseTypeName())
	}

	// data scan
	columnNums := len(columnNames)
	values := make([]interface{}, columnNums)
	valuePtrs := make([]interface{}, columnNums)

	for rows.Next() {
		for i := 0; i < columnNums; i++ {
			valuePtrs[i] = &values[i]
		}
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		for i, colName := range columnNames {
			valRes := values[i]
			if stringutil.IsValueNil(valRes) {
				//rowsMap[cols[i]] = `NULL` -> sql
				rowData[columnNameOrderIndexMap[colName]] = nil
			} else {
				value := reflect.ValueOf(valRes).Interface()
				switch val := value.(type) {
				case godror.Number:
					r, err := decimal.NewFromString(val.String())
					if err != nil {
						return nil, fmt.Errorf("column [%s] NewFromString strconv failed, %v", colName, err)
					}
					rowData[columnNameOrderIndexMap[colName]] = r
				case *godror.Lob:
					if strings.EqualFold(databaseTypes[i], "BFILE") {
						lobD, err := val.Hijack()
						if err != nil {
							return nil, fmt.Errorf("column [%s] NewFromString strconv failed, %v", colName, err)
						}
						lobSize, err := lobD.Size()
						if err != nil {
							return nil, err
						}

						buf := make([]byte, lobSize)

						var (
							res    strings.Builder
							offset int64
						)
						for {
							count, err := lobD.ReadAt(buf, offset)
							if err != nil {
								return nil, err
							}
							if int64(count) > lobSize/int64(4) {
								count = int(lobSize / 4)
							}
							offset += int64(count)
							res.Write(buf[:count])
							if count == 0 {
								break
							}
						}
						fmt.Println(res.String())

						dir, file, err := lobD.GetFileName()
						if err != nil {
							return nil, fmt.Errorf("column [%s] NewFromString strconv failed, %v", colName, err)
						}

						rowData[columnNameOrderIndexMap[colName]] = fmt.Sprintf("bfilename('%s', '%s')", dir, file)

						err = lobD.Close()
						if err != nil {
							return nil, err
						}
					} else {
						rowData[columnNameOrderIndexMap[colName]] = val
					}
				case string:
					if strings.EqualFold(val, "") {
						//rowsMap[cols[i]] = `NULL` -> sql
						rowData[columnNameOrderIndexMap[colName]] = nil
					} else {
						convertUtf8Raw, err := stringutil.CharsetConvert([]byte(val), dbCharsetS, constant.CharsetUTF8MB4)
						if err != nil {
							return nil, fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
						}
						convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
						if err != nil {
							return nil, fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
						}
						rowData[columnNameOrderIndexMap[colName]] = stringutil.BytesToString(convertTargetRaw)
					}
				default:
					rowData[columnNameOrderIndexMap[colName]] = val
				}
			}
		}

		// temporary array
		batchRowsData = append(batchRowsData, rowData...)

		// clear
		rowData = make([]interface{}, columnNameOrdersCounts)

		// batch
		if len(batchRowsData) == argsNums {
			dataChan = append(dataChan, batchRowsData)
			// clear
			batchRowsData = make([]interface{}, 0, argsNums)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	// non-batch batch
	if len(batchRowsData) > 0 {
		dataChan = append(dataChan, batchRowsData)
	}

	return dataChan, nil
}
