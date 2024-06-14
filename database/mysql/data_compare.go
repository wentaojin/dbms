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
	"hash/crc32"
	"strings"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func (d *Database) FindDatabaseTableBestColumn(schemaNameS, tableNameS, columnNameS string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnBucket(schemaNameS, tableNameS string, columnNameS, datatypeS string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableColumnAttribute(schemaNameS, tableNameS, columnNameS string, collationS bool) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT
		TABLE_SCHEMA AS OWNER,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE,
		NUMERIC_SCALE,
		DATETIME_PRECISION,
		COLLATION_NAME AS COLLATION
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

func (d *Database) GetDatabaseTableCompareData(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string) ([]string, uint32, map[string]int64, error) {
	var (
		rowData        []string
		columnNames    []string
		databaaseTypes []string
		scanTypes      []string
		err            error
		crc32Sum       uint32
	)

	var crc32Val uint32 = 0

	// record repeat counts
	batchRowsM := make(map[string]int64)

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, querySQL)
	if err != nil {
		return nil, crc32Sum, nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, crc32Sum, nil, err
	}

	for _, ct := range colTypes {
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(ct.Name()), dbCharsetS, constant.CharsetUTF8MB4)
		if err != nil {
			return nil, crc32Sum, nil, fmt.Errorf("column [%s] charset convert failed, %v", ct.Name(), err)
		}
		columnNames = append(columnNames, stringutil.BytesToString(convertUtf8Raw))
		databaaseTypes = append(databaaseTypes, ct.DatabaseTypeName())
		scanTypes = append(scanTypes, ct.ScanType().String())
	}

	columnNums := len(columnNames)

	// data scan
	rawResult := make([][]byte, columnNums)
	valuePtrs := make([]interface{}, columnNums)
	for i, _ := range columnNames {
		valuePtrs[i] = &rawResult[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, crc32Sum, nil, err
		}

		for i, colName := range columnNames {
			val := rawResult[i]
			// ORACLE database NULL and "" are the same
			if val == nil {
				rowData = append(rowData, `NULL`)
			} else if stringutil.BytesToString(val) == "" {
				rowData = append(rowData, `NULL`)
			} else {
				switch scanTypes[i] {
				case "sql.NullInt16":
					rowData = append(rowData, stringutil.BytesToString(val))
				case "sql.NullInt32":
					rowData = append(rowData, stringutil.BytesToString(val))
				case "sql.NullInt64":
					rowData = append(rowData, stringutil.BytesToString(val))
				case "sql.NullFloat64":
					rowData = append(rowData, stringutil.BytesToString(val))
				default:
					if strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeDecimal) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeBigint) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeDouble) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeDoublePrecision) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeFloat) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeInt) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeInteger) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeMediumint) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeNumeric) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeReal) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeSmallint) ||
						strings.EqualFold(databaaseTypes[i], constant.BuildInMySQLDatatypeTinyint) {
						rfs, err := decimal.NewFromString(stringutil.BytesToString(val))
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] NewFromString strconv failed, %v", colName, databaaseTypes[i], val, err)
						}
						rowData = append(rowData, rfs.String())
					} else {
						convertUtf8Raw, err := stringutil.CharsetConvert(val, dbCharsetS, constant.CharsetUTF8MB4)
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}
						convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}
						rowData = append(rowData, fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw)))
					}
				}
			}
		}

		// append
		batchKey := stringutil.StringJoin(rowData, constant.StringSeparatorComma)

		crc32Sum = atomic.AddUint32(&crc32Val, crc32.ChecksumIEEE([]byte(batchKey)))

		if val, ok := batchRowsM[batchKey]; ok {
			batchRowsM[batchKey] = val + 1
		} else {
			batchRowsM[batchKey] = 1
		}
		// clear
		rowData = rowData[0:0]
	}

	if err = rows.Err(); err != nil {
		return nil, crc32Sum, nil, err
	}

	return columnNames, crc32Sum, batchRowsM, nil
}
