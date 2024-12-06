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
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/godror/godror"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/greatcloak/decimal"
)

func (d *Database) GetDatabaseRole() (string, error) {
	_, res, err := d.GeneralQuery("SELECT DATABASE_ROLE FROM GV$DATABASE")
	if err != nil {
		return "", err
	}
	return res[0]["DATABASE_ROLE"], nil
}

func (d *Database) GetDatabaseConsistentPos(ctx context.Context, tx *sql.Tx) (string, error) {
	var globalSCN string
	err := tx.QueryRowContext(ctx, "SELECT TO_CHAR(MIN(CURRENT_SCN)) CURRENT_SCN FROM GV$DATABASE").Scan(&globalSCN)
	if err != nil {
		return globalSCN, err
	}
	return globalSCN, nil
}

func (d *Database) GetDatabaseTableColumnNameTableDimensions(schemaName, tableName string) ([]string, error) {
	deadline := time.Now().Add(time.Duration(d.CallTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE ROWNUM = 1`, schemaName, tableName))
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
	deadline := time.Now().Add(time.Duration(d.CallTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM (%v) WHERE ROWNUM = 1`, sqlStr))
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

func (d *Database) GetDatabaseTableRows(schemaName, tableName string) (uint64, error) {
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

func (d *Database) GetDatabaseTableSize(schemaName, tableName string) (float64, error) {
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

func (d *Database) GetDatabaseDirectoryName(directory string) (string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
       DIRECTORY_PATH
  FROM DBA_DIRECTORIES
 WHERE DIRECTORY_NAME = '%s'`, directory))
	if err != nil {
		return "", err
	}
	if len(res) > 1 || len(res) == 0 {
		return "", fmt.Errorf("get oracle database directory [%v] path falied, results: [%v]", directory, res)
	}
	return res[0]["DIRECTORY_PATH"], nil
}

func (d *Database) GetDatabaseTableChunkTask(taskName, schemaName, tableName string, chunkSize uint64, callTimeout uint64, batchSize int, dataChan chan []map[string]string) error {
	sqlStr00 := fmt.Sprintf(`BEGIN
  DBMS_PARALLEL_EXECUTE.CREATE_TASK (TASK_NAME => '%s');
END;`, taskName)
	_, err := d.ExecContext(d.Ctx, sqlStr00)
	if err != nil {
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create task failed: %v, sql: %v", err, sqlStr00)
	}

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)
	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	sqlStr01 := fmt.Sprintf(`BEGIN
  DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID (TASK_NAME   => '%s',
                                               TABLE_OWNER => '%s',
                                               TABLE_NAME  => '%s',
                                               BY_ROW      => TRUE,
                                               CHUNK_SIZE  => %v);
END;`, taskName, schemaName, tableName, chunkSize)

	sqlStr02 := fmt.Sprintf(`BEGIN
  DBMS_PARALLEL_EXECUTE.DROP_TASK ('%s');
END;`, taskName)

	_, err = d.ExecContext(ctx, sqlStr01)
	if err != nil {
		_, err = d.ExecContext(d.Ctx, sqlStr02)
		if err != nil {
			return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create_chunks_by_rowid drop task failed: %v, sql: %v", err, sqlStr02)
		}
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create_chunks_by_rowid task failed: %v, sql: %v", err, sqlStr01)
	}

	sqlStr03 := fmt.Sprintf(`SELECT 'ROWID BETWEEN ''' || START_ROWID || ''' AND ''' || END_ROWID || '''' CMD FROM DBA_PARALLEL_EXECUTE_CHUNKS WHERE  TASK_NAME = '%s' ORDER BY CHUNK_ID`, taskName)

	batchRowsData := make([]map[string]string, 0, batchSize)

	qdeadline := time.Now().Add(time.Duration(d.CallTimeout) * time.Second)

	qctx, qcancel := context.WithDeadline(d.Ctx, qdeadline)
	defer qcancel()

	rows, err := d.QueryContext(qctx, sqlStr03)
	if err != nil {
		_, err = d.ExecContext(d.Ctx, sqlStr02)
		if err != nil {
			return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE query_chunks_rowid drop task failed: %v, sql: %v", err, sqlStr03)
		}
		return err
	}
	defer rows.Close()

	// general query, automatic get column name
	columns, err := rows.Columns()
	if err != nil {
		_, err = d.ExecContext(d.Ctx, sqlStr02)
		if err != nil {
			return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE query_chunks_rowid drop task failed: %v, sql: %v", err, sqlStr03)
		}
		return fmt.Errorf("query rows.Columns failed, sql: [%v], error: [%v]", sqlStr03, err)
	}

	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return fmt.Errorf("query rows.Scan failed, sql: [%v], error: [%v]", sqlStr03, err)
		}

		row := make(map[string]string)
		for k, v := range values {
			// Notes: oracle database NULL and ""
			//	1, if the return value is NULLABLE, it represents the value is NULL, oracle sql query statement had be required the field NULL judgement, and if the filed is NULL, it returns that the value is NULLABLE
			//	2, if the return value is nil, it represents the value is NULL
			//	3, if the return value is "", it represents the value is "" string
			//	4, if the return value is 'NULL' or 'null', it represents the value is NULL or null string
			if v == nil {
				row[columns[k]] = "NULLABLE"
			} else {
				// Handling empty string and other values, the return value output string
				row[columns[k]] = stringutil.BytesToString(v)
			}
		}

		// temporary array
		batchRowsData = append(batchRowsData, row)

		// batch
		if len(batchRowsData) == batchSize {
			dataChan <- batchRowsData
			// clear
			batchRowsData = make([]map[string]string, 0, batchSize)
		}
	}

	if err = rows.Err(); err != nil {
		_, err = d.ExecContext(d.Ctx, sqlStr02)
		if err != nil {
			return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE query_chunks_rowid drop task failed: %v, sql: %v", err, sqlStr03)
		}
		return fmt.Errorf("query rows.Next failed, sql: [%v], error: [%v]", sqlStr03, err)
	}

	// non-batch batch
	if len(batchRowsData) > 0 {
		dataChan <- batchRowsData
	}
	return nil
}

func (d *Database) GetDatabaseTableStmtData(querySQL string, queryArgs []interface{}, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailO string, dataChan chan []interface{}) error {
	var (
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

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for _, ct := range colTypes {
		databaseTypes = append(databaseTypes, ct.DatabaseTypeName())
	}

	// data scan
	values := make([]interface{}, columnNameOrdersCounts)
	valuePtrs := make([]interface{}, columnNameOrdersCounts)
	for i, _ := range columnNameOrders {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return err
		}

		for i, colName := range columnNameOrders {
			valRes := values[i]
			if stringutil.IsValueNil(valRes) {
				//rowsMap[cols[i]] = `NULL` -> sql mode
				rowData[columnNameOrderIndexMap[colName]] = nil
			} else {
				value := reflect.ValueOf(valRes).Interface()
				switch val := value.(type) {
				case godror.Number:
					rfs, err := decimal.NewFromString(val.String())
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] NewFromString strconv failed, %v", colName, databaseTypes[i], val, err)
					}
					rowData[columnNameOrderIndexMap[colName]] = rfs
				case *godror.Lob:
					lobD, err := val.Hijack()
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack failed, %v", colName, databaseTypes[i], val, err)
					}
					if strings.EqualFold(databaseTypes[i], "BFILE") {
						dir, file, err := lobD.GetFileName()
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack getfilename failed, %v", colName, databaseTypes[i], val, err)
						}
						dirPath, err := d.GetDatabaseDirectoryName(dir)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack get directory name failed, %v", colName, databaseTypes[i], val, err)
						}
						rowData[columnNameOrderIndexMap[colName]] = filepath.Join(dirPath, file)
					} else {
						// get actual data
						lobSize, err := lobD.Size()
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack size failed, %v", colName, databaseTypes[i], val, err)
						}

						buf := make([]byte, lobSize)

						var (
							res    strings.Builder
							offset int64
						)
						for {
							count, err := lobD.ReadAt(buf, offset)
							if err != nil {
								return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack readAt failed, %v", colName, databaseTypes[i], val, err)
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
						rowData[columnNameOrderIndexMap[colName]] = res.String()
					}
					err = lobD.Close()
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack close failed, %v", colName, databaseTypes[i], val, err)
					}
				case string:
					if strings.EqualFold(val, "") {
						//rowsMap[cols[i]] = `NULL` -> sql
						rowData[columnNameOrderIndexMap[colName]] = nil
					} else {
						convertUtf8Raw, err := stringutil.CharsetConvert([]byte(val), dbCharsetS, constant.CharsetUTF8MB4)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
						}
						convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
						}
						rowData[columnNameOrderIndexMap[colName]] = stringutil.BytesToString(convertTargetRaw)
					}
				case []uint8:
					// binary data -> raw、long raw、blob
					convertUtf8Raw, err := stringutil.CharsetConvert(val, dbCharsetS, constant.CharsetUTF8MB4)
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
					}
					convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
					if err != nil {
						return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
					}

					rowData[columnNameOrderIndexMap[colName]] = convertTargetRaw
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

func (d *Database) GetDatabaseTableNonStmtData(taskFlow, querySQL string, queryArgs []interface{}, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailO string, dataChan chan []interface{}) error {
	var (
		databaseTypes []string
		err           error
	)
	columnNameOrders := stringutil.StringSplit(columnDetailO, constant.StringSeparatorComma)
	columnNameOrdersCounts := len(columnNameOrders)
	rowData := make([]string, columnNameOrdersCounts)

	batchRowsData := make([]string, 0, batchSize)

	batchRowsDataChanTemp := make([]interface{}, 0, 1)

	columnNameOrderIndexMap := make(map[string]int, columnNameOrdersCounts)

	for i, c := range columnNameOrders {
		columnNameOrderIndexMap[c] = i
	}

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for _, ct := range colTypes {
		databaseTypes = append(databaseTypes, ct.DatabaseTypeName())
	}

	// data scan
	values := make([]interface{}, columnNameOrdersCounts)
	valuePtrs := make([]interface{}, columnNameOrdersCounts)
	for i, _ := range columnNameOrders {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return err
		}

		for i, colName := range columnNameOrders {
			valRes := values[i]
			if stringutil.IsValueNil(valRes) {
				rowData[columnNameOrderIndexMap[colName]] = `NULL`
			} else {
				value := reflect.ValueOf(valRes).Interface()
				switch val := value.(type) {
				case godror.Number:
					rfs, err := decimal.NewFromString(val.String())
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] NewFromString strconv failed, %v", colName, databaseTypes[i], val, err)
					}
					rowData[columnNameOrderIndexMap[colName]] = rfs.String()
				case *godror.Lob:
					lobD, err := val.Hijack()
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack failed, %v", colName, databaseTypes[i], val, err)
					}
					if strings.EqualFold(databaseTypes[i], "BFILE") {
						dir, file, err := lobD.GetFileName()
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack getfilename failed, %v", colName, databaseTypes[i], val, err)
						}
						dirPath, err := d.GetDatabaseDirectoryName(dir)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack get directory name failed, %v", colName, databaseTypes[i], val, err)
						}
						rowData[columnNameOrderIndexMap[colName]] = fmt.Sprintf("'%v'", filepath.Join(dirPath, file))
					} else {
						// get actual data
						lobSize, err := lobD.Size()
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack size failed, %v", colName, databaseTypes[i], val, err)
						}

						buf := make([]byte, lobSize)

						var (
							res    strings.Builder
							offset int64
						)
						for {
							count, err := lobD.ReadAt(buf, offset)
							if err != nil {
								return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack readAt failed, %v", colName, databaseTypes[i], val, err)
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
						switch {
						case strings.EqualFold(taskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(taskFlow, constant.TaskFlowOracleToMySQL):
							convertTargetRaw, err := stringutil.CharsetConvert([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase([]byte(res.String()))), constant.CharsetUTF8MB4, dbCharsetT)
							if err != nil {
								return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
							}
							rowData[columnNameOrderIndexMap[colName]] = fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw))
						default:
							return fmt.Errorf("the task_flow [%s] isn't support, please contact author or reselect, %v", taskFlow, err)
						}
					}
					err = lobD.Close()
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack close failed, %v", colName, databaseTypes[i], val, err)
					}
				case string:
					if strings.EqualFold(val, "") {
						rowData[columnNameOrderIndexMap[colName]] = `NULL`
					} else {
						convertUtf8Raw, err := stringutil.CharsetConvert([]byte(val), dbCharsetS, constant.CharsetUTF8MB4)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
						}

						switch {
						case strings.EqualFold(taskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(taskFlow, constant.TaskFlowOracleToMySQL):
							convertTargetRaw, err := stringutil.CharsetConvert([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase(convertUtf8Raw)), constant.CharsetUTF8MB4, dbCharsetT)
							if err != nil {
								return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
							}
							rowData[columnNameOrderIndexMap[colName]] = fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw))
						default:
							return fmt.Errorf("the task_flow [%s] isn't support, please contact author or reselect, %v", taskFlow, err)
						}
					}
				case []uint8:
					// binary data -> raw、long raw、blob
					switch {
					case strings.EqualFold(taskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(taskFlow, constant.TaskFlowOracleToMySQL):
						convertUtf8Raw, err := stringutil.CharsetConvert(val, dbCharsetS, constant.CharsetUTF8MB4)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
						}
						convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
						if err != nil {
							return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}

						rowData[columnNameOrderIndexMap[colName]] = fmt.Sprintf("'%v'", stringutil.SpecialLettersMySQLCompatibleDatabase(convertTargetRaw))
					default:
						return fmt.Errorf("the task_flow [%s] isn't support, please contact author or reselect, %v", taskFlow, err)
					}
				case int64:
					rowData[columnNameOrderIndexMap[colName]] = decimal.NewFromInt(val).String()
				case uint64:
					rowData[columnNameOrderIndexMap[colName]] = strconv.FormatUint(val, 10)
				case float32:
					rowData[columnNameOrderIndexMap[colName]] = decimal.NewFromFloat32(val).String()
				case float64:
					rowData[columnNameOrderIndexMap[colName]] = decimal.NewFromFloat(val).String()
				case int32:
					rowData[columnNameOrderIndexMap[colName]] = decimal.NewFromInt32(val).String()
				default:
					return fmt.Errorf("the task_flow [%s] query [%s] column [%s] unsupported type: %T", taskFlow, querySQL, colName, value)
				}
			}
		}

		// temporary array
		batchRowsData = append(batchRowsData, stringutil.StringJoin(rowData, constant.StringSeparatorComma))

		// clear
		rowData = make([]string, columnNameOrdersCounts)

		// batch
		if len(batchRowsData) == batchSize {
			batchRowsDataChanTemp = append(batchRowsDataChanTemp, batchRowsData)

			dataChan <- batchRowsDataChanTemp

			// clear
			batchRowsDataChanTemp = make([]interface{}, 0, 1)
			batchRowsData = make([]string, 0, batchSize)
		}
	}

	if err = rows.Err(); err != nil {
		return err
	}

	// non-batch batch
	if len(batchRowsData) > 0 {
		batchRowsDataChanTemp = append(batchRowsDataChanTemp, batchRowsData)
		dataChan <- batchRowsDataChanTemp
	}

	return nil
}

func (d *Database) GetDatabaseTableCsvData(querySQL string, queryArgs []interface{}, callTimeout int, taskFlow, dbCharsetS, dbCharsetT, columnDetailO string, escapeBackslash bool, nullValue, separator, delimiter string, dataChan chan []string) error {
	var (
		databaseTypes []string
		err           error
	)
	columnNameOrders := stringutil.StringSplit(columnDetailO, constant.StringSeparatorComma)
	columnNameOrdersCounts := len(columnNameOrders)
	rowData := make([]string, columnNameOrdersCounts)

	columnNameOrderIndexMap := make(map[string]int, columnNameOrdersCounts)

	for i, c := range columnNameOrders {
		columnNameOrderIndexMap[c] = i
	}

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for _, ct := range colTypes {
		databaseTypes = append(databaseTypes, ct.DatabaseTypeName())
	}

	// data scan
	values := make([]interface{}, columnNameOrdersCounts)
	valuePtrs := make([]interface{}, columnNameOrdersCounts)
	for i, _ := range columnNameOrders {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return err
		}
		for i, colName := range columnNameOrders {
			valRes := values[i]
			// ORACLE database NULL and "" are the same
			if stringutil.IsValueNil(valRes) {
				if !strings.EqualFold(nullValue, "") {
					rowData[columnNameOrderIndexMap[colName]] = nullValue
				} else {
					rowData[columnNameOrderIndexMap[colName]] = `NULL`
				}
			} else {
				value := reflect.ValueOf(valRes).Interface()
				switch val := value.(type) {
				case godror.Number:
					rfs, err := decimal.NewFromString(val.String())
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] NewFromString strconv failed, %v", colName, databaseTypes[i], val, err)
					}
					rowData[columnNameOrderIndexMap[colName]] = rfs.String()
				case *godror.Lob:
					lobD, err := val.Hijack()
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack failed, %v", colName, databaseTypes[i], val, err)
					}

					if strings.EqualFold(databaseTypes[i], "BFILE") {
						dir, file, err := lobD.GetFileName()
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack getfilename failed, %v", colName, databaseTypes[i], val, err)
						}
						dirPath, err := d.GetDatabaseDirectoryName(dir)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack get directory name failed, %v", colName, databaseTypes[i], val, err)
						}
						rowData[columnNameOrderIndexMap[colName]] = filepath.Join(dirPath, file)
					} else {
						// get actual data
						lobSize, err := lobD.Size()
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack size failed, %v", colName, databaseTypes[i], val, err)
						}

						buf := make([]byte, lobSize)

						var (
							res    strings.Builder
							offset int64
						)
						for {
							count, err := lobD.ReadAt(buf, offset)
							if err != nil {
								return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack readAt failed, %v", colName, databaseTypes[i], val, err)
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
						rowData[columnNameOrderIndexMap[colName]] = res.String()
					}
					err = lobD.Close()
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] hijack close failed, %v", colName, databaseTypes[i], val, err)
					}
				case []uint8:
					// binary data -> raw、long raw、blob
					convertUtf8Raw, err := stringutil.CharsetConvert(val, dbCharsetS, constant.CharsetUTF8MB4)
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
					}
					convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
					if err != nil {
						return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
					}
					rowData[columnNameOrderIndexMap[colName]] = stringutil.EscapeBinaryCSV(convertTargetRaw, escapeBackslash, delimiter, separator)
				case int64:
					rowData[columnNameOrderIndexMap[colName]] = decimal.NewFromInt(val).String()
				case uint64:
					rowData[columnNameOrderIndexMap[colName]] = strconv.FormatUint(val, 10)
				case float32:
					rowData[columnNameOrderIndexMap[colName]] = decimal.NewFromFloat32(val).String()
				case float64:
					rowData[columnNameOrderIndexMap[colName]] = decimal.NewFromFloat(val).String()
				case int32:
					rowData[columnNameOrderIndexMap[colName]] = decimal.NewFromInt32(val).String()
				case string:
					if strings.EqualFold(val, "") {
						if !strings.EqualFold(nullValue, "") {
							rowData[columnNameOrderIndexMap[colName]] = nullValue
						} else {
							rowData[columnNameOrderIndexMap[colName]] = `NULL`
						}
					} else {
						var convertTargetRaw []byte
						convertUtf8Raw, err := stringutil.CharsetConvert([]byte(val), dbCharsetS, constant.CharsetUTF8MB4)
						if err != nil {
							return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}

						// Handling character sets, special character escapes, string reference delimiters
						if escapeBackslash {
							switch {
							case strings.EqualFold(taskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(taskFlow, constant.TaskFlowOracleToMySQL):
								convertTargetRaw, err = stringutil.CharsetConvert([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase(convertUtf8Raw)), constant.CharsetUTF8MB4, dbCharsetT)
								if err != nil {
									return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
								}
							default:
								return fmt.Errorf("the task_flow [%s] isn't support, please contact author or reselect, %v", taskFlow, err)
							}

						} else {
							convertTargetRaw, err = stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
							if err != nil {
								return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
							}
						}
						if delimiter == "" {
							rowData[columnNameOrderIndexMap[colName]] = stringutil.BytesToString(convertTargetRaw)
						} else {
							rowData[columnNameOrderIndexMap[colName]] = stringutil.StringBuilder(delimiter, stringutil.BytesToString(convertTargetRaw), delimiter)
						}
					}
				default:
					return fmt.Errorf("the task_flow [%s] column [%s] unsupported type: %T", taskFlow, colName, value)
				}
			}
		}

		// temporary array
		dataChan <- rowData
		// clear
		rowData = make([]string, columnNameOrdersCounts)
	}

	if err = rows.Err(); err != nil {
		return err
	}
	return nil
}
