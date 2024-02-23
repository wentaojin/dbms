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
package oracle

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/greatcloak/decimal"
)

func (d *Database) GetDatabaseCurrentSCN() (uint64, error) {
	_, res, err := d.GeneralQuery("SELECT MIN(CURRENT_SCN) CURRENT_SCN FROM GV$DATABASE")
	var globalSCN uint64
	if err != nil {
		return globalSCN, err
	}
	globalSCN, err = strconv.ParseUint(res[0]["CURRENT_SCN"], 10, 64)
	if err != nil {
		return globalSCN, fmt.Errorf("get oracle current snapshot scn %s parseUint failed: %v", res[0]["CURRENT_SCN"], err)
	}
	return globalSCN, nil
}

func (d *Database) GetDatabaseTableColumnNameTableDimensions(schemaName, tableName string) ([]string, error) {
	rows, err := d.QueryContext(d.Ctx, fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE ROWNUM = 1`, schemaName, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return columns, err
	}
	return columns, nil
}

func (d *Database) GetDatabaseTableColumnNameSqlDimensions(sqlStr string) ([]string, map[string]string, map[string]string, error) {
	rows, err := d.QueryContext(d.Ctx, fmt.Sprintf(`SELECT * FROM (%v) WHERE ROWNUM = 1`, sqlStr))
	if err != nil {
		return nil, nil, nil, err
	}
	defer rows.Close()

	var columns []string
	columnTypeMap := make(map[string]string)
	columnScaleMap := make(map[string]string)

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return columns, columnTypeMap, columnScaleMap, err
	}

	for _, c := range columnTypes {
		columns = append(columns, c.Name())
		columnTypeMap[c.Name()] = c.DatabaseTypeName()
		_, dataScale, ok := c.DecimalSize()
		if ok {
			columnScaleMap[c.Name()] = strconv.FormatInt(dataScale, 10)
		}
	}
	return columns, columnTypeMap, columnScaleMap, nil
}

func (d *Database) GetDatabaseTableRowsByStatistics(schemaName, tableName string) (uint64, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT NVL(NUM_ROWS,0) AS NUM_ROWS
  FROM DBA_TABLES
 WHERE OWNER = '%s'
   AND TABLE_NAME = '%s'`, schemaName, tableName))
	if err != nil {
		return 0, err
	}
	if len(res) != 1 {
		return 0, fmt.Errorf("get oracle schema table [%v] rows by statistics falied, results: [%v]",
			fmt.Sprintf("%s.%s", schemaName, tableName), res)
	}
	numRows, err := strconv.ParseUint(res[0]["NUM_ROWS"], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("get oracle schema table [%v] rows [%s] by statistics strconv.Atoi falied: %v",
			fmt.Sprintf("%s.%s", schemaName, tableName), res[0]["NUM_ROWS"], err)
	}
	return numRows, nil
}

func (d *Database) GetDatabaseTableSizeBySegment(schemaName, tableName string) (float64, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT ROUND(bytes/1024/1024,2) AS SIZE_MB
  FROM DBA_SEGMENTS
 WHERE OWNER = '%s'
   AND SEGMENT_TYPE = 'TABLE'
   AND SEGMENT_NAME = '%s'`, schemaName, tableName))
	if err != nil {
		return 0, err
	}
	if len(res) == 0 {
		return 0, nil
	}
	if len(res) != 1 {
		return 0, fmt.Errorf("get oracle schema table [%v] size by segment falied, results: [%v]",
			fmt.Sprintf("%s.%s", schemaName, tableName), res)
	}
	sizeMB, err := strconv.ParseFloat(res[0]["SIZE_MB"], 64)
	if err != nil {
		return 0, fmt.Errorf("get oracle schema table [%v] size(MB) [%s] by segment strconv.Atoi falied: %v",
			fmt.Sprintf("%s.%s", schemaName, tableName), res[0]["SIZE_MB"], err)
	}
	return sizeMB, nil
}

func (d *Database) CreateDatabaseTableChunkTask(taskName string) error {
	sqlStr := fmt.Sprintf(`BEGIN
  DBMS_PARALLEL_EXECUTE.CREATE_TASK (TASK_NAME => '%s');
END;`, taskName)
	_, err := d.ExecContext(d.Ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create task failed: %v, sql: %v", err, sqlStr)
	}
	return nil
}

func (d *Database) StartDatabaseTableChunkTask(taskName, schemaName, tableName string, chunkSize uint64, callTimeout uint64) error {
	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)
	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()
	sqlStr := fmt.Sprintf(`BEGIN
  DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID (TASK_NAME   => '%s',
                                               TABLE_OWNER => '%s',
                                               TABLE_NAME  => '%s',
                                               BY_ROW      => TRUE,
                                               CHUNK_SIZE  => %v);
END;`, taskName, schemaName, tableName, chunkSize)
	_, err := d.ExecContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create_chunks_by_rowid task failed: %v, sql: %v", err, sqlStr)
	}
	return nil
}

func (d *Database) GetDatabaseTableChunkData(taskName string) ([]map[string]string, error) {
	sqlStr := fmt.Sprintf(`SELECT 'ROWID BETWEEN ''' || START_ROWID || ''' AND ''' || END_ROWID || '''' CMD FROM DBA_PARALLEL_EXECUTE_CHUNKS WHERE  TASK_NAME = '%s' ORDER BY chunk_id`, taskName)
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) CloseDatabaseTableChunkTask(taskName string) error {
	sqlStr := fmt.Sprintf(`BEGIN
  DBMS_PARALLEL_EXECUTE.DROP_TASK ('%s');
END;`, taskName)
	_, err := d.ExecContext(d.Ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE drop task failed: %v, sql: %v", err, sqlStr)
	}
	return nil
}

func (d *Database) QueryDatabaseTableChunkData(querySQL string, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailS string, dataChan chan []interface{}) error {
	var (
		columnNames []string
		columnTypes []string
		err         error
	)
	columnNameOrders := stringutil.StringSplit(columnDetailS, constant.StringSeparatorComma)
	columnNameOrdersCounts := len(columnNameOrders)
	rowData := make([]interface{}, columnNameOrdersCounts)

	argsNums := batchSize * columnNameOrdersCounts

	batchRowsData := make([]interface{}, 0, argsNums)

	columnNameOrderIndexMap := make(map[string]int, columnNameOrdersCounts)

	for i, c := range columnNameOrders {
		columnNameOrderIndexMap[c] = i
	}

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, querySQL)
	if err != nil {
		return err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for _, ct := range colTypes {
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(ct.Name()), dbCharsetS, constant.CharsetUTF8MB4)
		if err != nil {
			return fmt.Errorf("column [%s] charset convert failed, %v", ct.Name(), err)
		}
		columnNames = append(columnNames, stringutil.BytesToString(convertUtf8Raw))
		// database field type DatabaseTypeName() maps go type ScanType()
		columnTypes = append(columnTypes, ct.ScanType().String())
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
			return err
		}

		for i, raw := range rawResult {
			if raw == nil {
				//rowsMap[cols[i]] = `NULL` -> sql
				rowData[columnNameOrderIndexMap[columnNames[i]]] = nil
			} else if stringutil.BytesToString(raw) == "" {
				//rowsMap[cols[i]] = `NULL` -> sql
				rowData[columnNameOrderIndexMap[columnNames[i]]] = nil
			} else {
				switch columnTypes[i] {
				case "int64":
					r, err := stringutil.StrconvIntBitSize(stringutil.BytesToString(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[columnNameOrderIndexMap[columnNames[i]]] = r
				case "uint64":
					r, err := stringutil.StrconvUintBitSize(stringutil.BytesToString(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[columnNameOrderIndexMap[columnNames[i]]] = r
				case "float32":
					r, err := stringutil.StrconvFloatBitSize(stringutil.BytesToString(raw), 32)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[columnNameOrderIndexMap[columnNames[i]]] = r
				case "float64":
					r, err := stringutil.StrconvFloatBitSize(stringutil.BytesToString(raw), 64)
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[columnNameOrderIndexMap[columnNames[i]]] = r
				case "rune":
					r, err := stringutil.StrconvRune(stringutil.BytesToString(raw))
					if err != nil {
						return fmt.Errorf("column [%s] strconv failed, %v", columnNames[i], err)
					}
					rowData[columnNameOrderIndexMap[columnNames[i]]] = r
				case "godror.Number":
					r, err := decimal.NewFromString(stringutil.BytesToString(raw))
					if err != nil {
						return fmt.Errorf("column [%s] NewFromString strconv failed, %v", columnNames[i], err)
					}
					rowData[columnNameOrderIndexMap[columnNames[i]]] = r
				case "[]uint8":
					// binary data -> raw、long raw、blob
					rowData[columnNameOrderIndexMap[columnNames[i]]] = raw
				default:
					convertUtf8Raw, err := stringutil.CharsetConvert(raw, dbCharsetS, constant.CharsetUTF8MB4)
					if err != nil {
						return fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}

					convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
					if err != nil {
						return fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(convertTargetRaw)
				}
			}
		}

		// temporary array
		batchRowsData = append(batchRowsData, rowData...)

		// clear
		rowData = make([]interface{}, columnNameOrdersCounts)

		// batch
		if len(batchRowsData) == argsNums {
			dataChan <- batchRowsData

			// clear
			batchRowsData = make([]interface{}, 0, argsNums)
		}
	}

	if err = rows.Err(); err != nil {
		return err
	}

	// non-batch batch
	if len(batchRowsData) > 0 {
		dataChan <- batchRowsData
	}

	return nil
}
