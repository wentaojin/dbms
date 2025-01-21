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

func (d *Database) GetDatabaseSchemaPrimaryTables(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	TABLE_NAME,
	CONSTRAINT_NAME,
	LTRIM(MAX(SYS_CONNECT_BY_PATH(COLUMN_NAME, '|+|'))
    KEEP (DENSE_RANK LAST ORDER BY POSITION), '|+|') AS COLUMN_LIST
FROM
	(
	SELECT
	CU.TABLE_NAME,
	CU.CONSTRAINT_NAME,
	CU.COLUMN_NAME,
	CU.POSITION,
	ROW_NUMBER() OVER (PARTITION BY CU.CONSTRAINT_NAME ORDER BY CU.POSITION)-1 RN_LAG
FROM
	DBA_CONS_COLUMNS CU,
	DBA_CONSTRAINTS AU
WHERE
	CU.CONSTRAINT_NAME = AU.CONSTRAINT_NAME
	AND AU.CONSTRAINT_TYPE = 'P'
	AND AU.STATUS = 'ENABLED'
	AND CU.OWNER = AU.OWNER
	AND CU.TABLE_NAME = AU.TABLE_NAME
	AND CU.OWNER = '%s'
	AND NOT EXISTS (
		SELECT 1 FROM
			DBA_RECYCLEBIN RC WHERE CU.OWNER=RC.OWNER AND CU.TABLE_NAME = RC.OBJECT_NAME
		)
    )
GROUP BY
	TABLE_NAME,CONSTRAINT_NAME
CONNECT BY
	RN_LAG = PRIOR POSITION
	AND CONSTRAINT_NAME = PRIOR CONSTRAINT_NAME
START WITH
	POSITION = 1
ORDER BY TABLE_NAME,CONSTRAINT_NAME`, schemaName))
	if err != nil {
		return nil, err
	}
	var tables []string
	for _, r := range res {
		tables = append(tables, r["TABLE_NAME"])
	}
	return tables, nil
}

func (d *Database) GetDatabaseSchemaTableValidIndex(schemaName string, tableName string) (bool, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`WITH unique_constraints AS (
		SELECT 
			cons.CONSTRAINT_NAME,
			cons.CONSTRAINT_TYPE,
			cols.COLUMN_NAME
		FROM 
			DBA_CONSTRAINTS cons
		JOIN 
			DBA_CONS_COLUMNS cols ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME
		WHERE 
			cons.OWNER = '%s' AND
			cons.TABLE_NAME = '%s' AND
			cons.CONSTRAINT_TYPE IN ('U')
	),
	unique_indexes AS (
		SELECT 
			ind.INDEX_NAME,
			cols.COLUMN_NAME
		FROM 
			DBA_INDEXES ind
		JOIN 
			DBA_IND_COLUMNS cols ON ind.INDEX_NAME = cols.INDEX_NAME
		WHERE 
			ind.OWNER = '%s' AND
			ind.TABLE_NAME = '%s' AND
			ind.UNIQUENESS = 'UNIQUE' AND
			ind.INDEX_TYPE = 'NORMAL' -- 排除其他类型的索引，如位图索引等
	)
	SELECT 
		uc.CONSTRAINT_NAME AS CONSTRAINT_OR_INDEX_NAME,
		uc.CONSTRAINT_TYPE,
		uc.COLUMN_NAME,
		CASE 
			WHEN tabc.NULLABLE = 'N' THEN 'NOT NULL'
			ELSE 'NULLABLE'
		END AS IS_NULLABLE
	FROM 
		unique_constraints uc
	JOIN 
		DBA_TAB_COLUMNS tabc ON uc.COLUMN_NAME = tabc.COLUMN_NAME
	WHERE 
		tabc.OWNER = '%s' AND
		tabc.TABLE_NAME = '%s'
	UNION ALL
	SELECT 
		ui.INDEX_NAME AS CONSTRAINT_OR_INDEX_NAME,
		'INDEX' AS CONSTRAINT_TYPE,
		ui.COLUMN_NAME,
		CASE 
			WHEN tabc.NULLABLE = 'N' THEN 'NOT NULL'
			ELSE 'NULLABLE'
		END AS IS_NULLABLE
	FROM 
		unique_indexes ui
	JOIN 
		DBA_TAB_COLUMNS tabc ON ui.COLUMN_NAME = tabc.COLUMN_NAME
	WHERE 
		tabc.OWNER = '%s' AND
		tabc.TABLE_NAME = '%s'`, schemaName, tableName, schemaName, tableName,
		schemaName, tableName, schemaName, tableName))
	if err != nil {
		return false, err
	}

	indexNameColumns := make(map[string][]string)
	indexNameNulls := make(map[string]int)

	for _, r := range res {
		if vals, ok := indexNameColumns[r["CONSTRAINT_OR_INDEX_NAME"]]; ok {
			indexNameColumns[r["CONSTRAINT_OR_INDEX_NAME"]] = append(vals, r["COLUMN_NAME"])
		} else {
			indexNameColumns[r["CONSTRAINT_OR_INDEX_NAME"]] = append(indexNameColumns[r["CONSTRAINT_OR_INDEX_NAME"]], r["COLUMN_NAME"])
		}

		if r["IS_NULLABLE"] == "NOT NULL" {
			if vals, ok := indexNameNulls[r["CONSTRAINT_OR_INDEX_NAME"]]; ok {
				indexNameNulls[r["CONSTRAINT_OR_INDEX_NAME"]] = vals + 1
			} else {
				indexNameNulls[r["CONSTRAINT_OR_INDEX_NAME"]] = 1
			}
		}
	}

	for k, v := range indexNameColumns {
		if val, ok := indexNameNulls[k]; ok && val == len(v) {
			return true, nil
		}
	}
	return false, nil
}

func (d *Database) GetDatabaseTableColumnMetadata(schemaName string, tableName string) ([]map[string]string, error) {
	version, err := d.GetDatabaseVersion()
	if err != nil {
		return nil, err
	}

	virtualC := false
	if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal(constant.OracleDatabaseTableAndColumnVirtualSupportVersion) {
		virtualC = true
	}

	var sqlStr string
	if virtualC {
		sqlStr = fmt.Sprintf(`
		SELECT 
			T.COLUMN_NAME,
			T.DATA_TYPE,		
			NVL(T.DATA_LENGTH, 0) AS DATA_LENGTH,
			T.VIRTUAL_COLUMN AS IS_GENERATED
		FROM
			DBA_TAB_COLS T,
		WHERE
			T.OWNER = '%s'
			AND T.TABLE_NAME = '%s'
		ORDER BY
			T.COLUMN_ID`, schemaName, tableName)
	} else {
		sqlStr = fmt.Sprintf(`
		SELECT 
			T.COLUMN_NAME,
			T.DATA_TYPE,		
			NVL(T.DATA_LENGTH, 0) AS DATA_LENGTH
			'NO' AS IS_GENERATED
		FROM
			DBA_TAB_COLUMNS T,
		WHERE
			T.OWNER = '%s'
			AND T.TABLE_NAME = '%s'
		ORDER BY
			T.COLUMN_ID`, schemaName, tableName)
	}

	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
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
		_, errMsg := d.ExecContext(d.Ctx, sqlStr02)
		if errMsg != nil {
			return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create_chunks_by_rowid drop task failed, sql: [%v], exec error: [%v], drop error: [%v]", sqlStr02, err, errMsg)
		}
		return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE create_chunks_by_rowid task failed, sql: [%v], error: [%v]", sqlStr01, err)
	}

	sqlStr03 := fmt.Sprintf(`SELECT 'ROWID BETWEEN ''' || START_ROWID || ''' AND ''' || END_ROWID || '''' CMD FROM DBA_PARALLEL_EXECUTE_CHUNKS WHERE  TASK_NAME = '%s' ORDER BY CHUNK_ID`, taskName)

	batchRowsData := make([]map[string]string, 0, batchSize)

	qdeadline := time.Now().Add(time.Duration(d.CallTimeout) * time.Second)

	qctx, qcancel := context.WithDeadline(d.Ctx, qdeadline)
	defer qcancel()

	rows, err := d.QueryContext(qctx, sqlStr03)
	if err != nil {
		_, errMsg := d.ExecContext(d.Ctx, sqlStr02)
		if errMsg != nil {
			return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE query_chunks_rowid drop task failed, sql: [%v], exec error: [%v], drop error: [%v]", sqlStr02, err, errMsg)
		}
		return err
	}
	defer rows.Close()

	// general query, automatic get column name
	columns, err := rows.Columns()
	if err != nil {
		_, errMsg := d.ExecContext(d.Ctx, sqlStr02)
		if errMsg != nil {
			return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE query_chunks_rowid drop task failed, sql: [%v], exec error: [%v], drop error: [%v]", sqlStr02, err, errMsg)
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
		_, errMsg := d.ExecContext(d.Ctx, sqlStr02)
		if errMsg != nil {
			return fmt.Errorf("oracle DBMS_PARALLEL_EXECUTE query_chunks_rowid exec failed [%v] drop task failed: %v, sql: %v", err, errMsg, sqlStr03)
		}
		return fmt.Errorf("query rows.Next failed, sql: [%v], error: [%v]", sqlStr03, err)
	}

	// non-batch batch
	if len(batchRowsData) > 0 {
		dataChan <- batchRowsData
	}
	return nil
}

func (d *Database) GetDatabaseTableStmtData(querySQL string, queryArgs []interface{}, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailO, garbledReplace string, dataChan chan []interface{}) error {
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
						convertUtf8Raw, err := stringutil.CharsetConvertReplace([]byte(val), dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
						}
						convertTargetRaw, err := stringutil.CharsetConvertReplace(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
						}
						rowData[columnNameOrderIndexMap[colName]] = stringutil.BytesToString(convertTargetRaw)
					}
				case []uint8:
					// binary data -> raw、long raw、blob
					convertUtf8Raw, err := stringutil.CharsetConvertReplace(val, dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
					}
					convertTargetRaw, err := stringutil.CharsetConvertReplace(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
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

func (d *Database) GetDatabaseTableNonStmtData(taskFlow, querySQL string, queryArgs []interface{}, batchSize, callTimeout int, dbCharsetS, dbCharsetT, columnDetailO, garbledReplace string, dataChan chan []interface{}) error {
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
							convertTargetRaw, err := stringutil.CharsetConvertReplace([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase([]byte(res.String()))), constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
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
						convertUtf8Raw, err := stringutil.CharsetConvertReplace([]byte(val), dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
						}

						switch {
						case strings.EqualFold(taskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(taskFlow, constant.TaskFlowOracleToMySQL):
							convertTargetRaw, err := stringutil.CharsetConvertReplace([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase(convertUtf8Raw)), constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
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
						convertUtf8Raw, err := stringutil.CharsetConvertReplace(val, dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
						if err != nil {
							return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
						}
						convertTargetRaw, err := stringutil.CharsetConvertReplace(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
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

func (d *Database) GetDatabaseTableCsvData(querySQL string, queryArgs []interface{}, callTimeout int, taskFlow, dbCharsetS, dbCharsetT, columnDetailO string, escapeBackslash bool, nullValue, separator, delimiter, garbledReplace string, dataChan chan []string) error {
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
					convertUtf8Raw, err := stringutil.CharsetConvertReplace(val, dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
					}
					convertTargetRaw, err := stringutil.CharsetConvertReplace(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
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
						convertUtf8Raw, err := stringutil.CharsetConvertReplace([]byte(val), dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
						if err != nil {
							return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}

						// Handling character sets, special character escapes, string reference delimiters
						if escapeBackslash {
							switch {
							case strings.EqualFold(taskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(taskFlow, constant.TaskFlowOracleToMySQL):
								convertTargetRaw, err = stringutil.CharsetConvertReplace([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase(convertUtf8Raw)), constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
								if err != nil {
									return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
								}
							default:
								return fmt.Errorf("the task_flow [%s] isn't support, please contact author or reselect, %v", taskFlow, err)
							}

						} else {
							convertTargetRaw, err = stringutil.CharsetConvertReplace(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
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
