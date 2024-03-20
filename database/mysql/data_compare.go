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
package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func (d *Database) FindDatabaseTableBestColumnName(schemaNameS, tableNameS string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) FilterDatabaseTableBestColumnDatatype(columnType string) bool {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableBestColumnBucket(schemaNameS, tableNameS string, columnNameS, datatypeS string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableBestColumnAttribute(schemaNameS, tableNameS, columnNameS string) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
		TABLE_SCHEMA AS OWNER,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE,
		NUMERIC_SCALE,
		DATETIME_PRECISION
	FROM
		INFORMATION_SCHEMA.COLUMNS
	WHERE
		TABLE_SCHEMA = '%s' 
		AND TABLE_NAME = '%s'
		AND COLUMN_NAME = '%s'`, schemaNameS, tableNameS, columnNameS)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableBestColumnCompareData(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string) ([]string, map[string]int64, error) {
	var (
		columnNames []string
		columnTypes []string
		err         error
	)
	// record repeat counts
	batchRowsM := make(map[string]int64)

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, querySQL)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	for _, ct := range colTypes {
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(ct.Name()), dbCharsetS, constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("column [%s] charset convert failed, %v", ct.Name(), err)
		}
		columnNames = append(columnNames, stringutil.BytesToString(convertUtf8Raw))
		// database field type DatabaseTypeName() maps go type ScanType()
		columnTypes = append(columnTypes, ct.ScanType().String())
	}

	columnNameOrdersCounts := len(columnNames)
	rowData := make([]string, columnNameOrdersCounts)
	columnNameOrderIndexMap := make(map[string]int, columnNameOrdersCounts)

	for i, c := range columnNames {
		columnNameOrderIndexMap[c] = i
	}

	// data scan
	columnNums := len(columnNames)
	rawResult := make([][]byte, columnNums)
	dest := make([]interface{}, columnNums)
	for i := range rawResult {
		dest[i] = &rawResult[i]
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			return nil, nil, err
		}

		for i, raw := range rawResult {
			if raw == nil {
				rowData[columnNameOrderIndexMap[columnNames[i]]] = `NULL`
			} else if stringutil.BytesToString(raw) == "" {
				rowData[columnNameOrderIndexMap[columnNames[i]]] = `NULL`
			} else {
				switch columnTypes[i] {
				case "int64":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "uint64":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "float32":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "float64":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "rune":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "[]uint8":
					// binary data -> raw、long raw、blob
					rowData[columnNameOrderIndexMap[columnNames[i]]] = fmt.Sprintf("'%v'", stringutil.BytesToString(raw))
				default:
					convertUtf8Raw, err := stringutil.CharsetConvert(raw, dbCharsetS, constant.CharsetUTF8MB4)
					if err != nil {
						return nil, nil, fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}

					convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
					if err != nil {
						return nil, nil, fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}
					rowData[columnNameOrderIndexMap[columnNames[i]]] = fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw))
				}
			}
		}

		// append
		batchKey := stringutil.StringJoin(rowData, constant.StringSeparatorComma)
		if val, ok := batchRowsM[batchKey]; ok {
			batchRowsM[batchKey] = val + 1
		} else {
			batchRowsM[batchKey] = 1
		}
		// clear
		rowData = make([]string, columnNameOrdersCounts)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, err
	}

	return columnNames, batchRowsM, nil
}
