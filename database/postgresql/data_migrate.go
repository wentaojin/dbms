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
package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/greatcloak/decimal"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func (d *Database) GetDatabaseSchemaPrimaryTables(schemaName string) ([]string, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT 
    cons.conname AS "CONSTRAINT_NAME",
	tbl.relname AS "TABLE_NAME",
    string_agg(a.attname, '|+|') AS "COLUMN_LIST"
FROM 
    pg_constraint cons
JOIN 
    pg_class tbl ON cons.conrelid = tbl.oid
JOIN 
    pg_namespace ns ON tbl.relnamespace = ns.oid
JOIN 
    pg_attribute a ON a.attrelid = cons.conrelid AND a.attnum = ANY(cons.conkey)
WHERE 
    ns.nspname = '%s' AND contype = 'p'
GROUP BY 
    cons.conname,tbl.relname`, schemaName))
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
        cons.conname AS constraint_or_index_name,
        tbl.relname AS table_name,
        a.attname AS column_name,
        CASE WHEN a.attnotnull THEN 'YES' ELSE 'NO' END AS is_not_null
    FROM 
        pg_constraint cons
    JOIN 
        pg_class tbl ON cons.conrelid = tbl.oid
    JOIN 
        pg_namespace ns ON tbl.relnamespace = ns.oid
    JOIN 
        pg_attribute a ON a.attrelid = cons.conrelid AND a.attnum = ANY(cons.conkey)
    WHERE 
        ns.nspname = '%s'  -- 替换为您的模式名称
        AND cons.contype IN ('u')
        AND tbl.relname = '%s'  -- 替换为您的表名
),
unique_indexes AS (
    SELECT 
        idx.indexrelid::regclass AS constraint_or_index_name,
        tbl.relname AS table_name,
        a.attname AS column_name,
        CASE WHEN a.attnotnull THEN 'YES' ELSE 'NO' END AS is_not_null
    FROM 
        pg_index idx
    JOIN 
        pg_class tbl ON idx.indrelid = tbl.oid
    JOIN 
        pg_namespace ns ON tbl.relnamespace = ns.oid
    JOIN 
        pg_attribute a ON a.attrelid = idx.indrelid AND a.attnum = ANY(idx.indkey)
    WHERE 
        ns.nspname = '%s'
        AND idx.indisunique 
        AND tbl.relname = '%s'
)
SELECT * FROM unique_constraints
UNION ALL
SELECT * FROM unique_indexes
ORDER BY table_name, column_name`, schemaName, tableName, schemaName, tableName))
	if err != nil {
		return false, err
	}
	indexNameColumns := make(map[string][]string)
	indexNameNulls := make(map[string]int)

	for _, r := range res {
		if vals, ok := indexNameColumns[r["constraint_or_index_name"]]; ok {
			indexNameColumns[r["constraint_or_index_name"]] = append(vals, r["column_name"])
		} else {
			indexNameColumns[r["constraint_or_index_name"]] = append(indexNameColumns[r["constraint_or_index_name"]], r["column_name"])
		}

		if r["is_not_null"] == "YES" {
			if vals, ok := indexNameNulls[r["constraint_or_index_name"]]; ok {
				indexNameNulls[r["constraint_or_index_name"]] = vals + 1
			} else {
				indexNameNulls[r["constraint_or_index_name"]] = 1
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
	if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal(constant.PostgresqlDatabaseVirtualGeneratedColumnSupportVersionRequire) {
		virtualC = true
	}

	var sqlStr string
	if virtualC {
		sqlStr = fmt.Sprintf(`SELECT
		column_name AS "COLUMN_NAME",
		data_type AS "DATA_TYPE",
		COALESCE(character_maximum_length,0) AS "DATA_LENGTH",
		CASE 
			WHEN COALESCE(is_generated, '') = '' THEN 'NO'
			WHEN is_generated = 'NEVER' THEN 'NO'
			ELSE 'YES'
		END AS "IS_GENERATED"
	FROM
	information_schema.columns
	WHERE
		table_schema = '%s'
		AND table_name = '%s'
		ORDER BY ordinal_position`, schemaName, tableName)
	} else {
		sqlStr = fmt.Sprintf(`SELECT
		column_name AS "COLUMN_NAME",
		data_type AS "DATA_TYPE",
		COALESCE(character_maximum_length,0) AS "DATA_LENGTH",
		'NO' AS "IS_GENERATED"
	FROM
	information_schema.columns
	WHERE
		table_schema = '%s'
		AND table_name = '%s'
		ORDER BY ordinal_position`, schemaName, tableName)
	}
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableChunkTask(taskName, schemaName, tableName string, chunkSize uint64, callTimeout uint64, batchSize int, dataChan chan []map[string]string) error {
	//TODO implement me
	panic("implement me")
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

	sqlSlis := stringutil.StringSplit(querySQL, constant.StringSeparatorSemicolon)
	sliLen := len(sqlSlis)

	var (
		txn  *sql.Tx
		rows *sql.Rows
	)

	if sliLen == 1 {
		rows, err = d.QueryContext(ctx, sqlSlis[0], queryArgs...)
		if err != nil {
			return err
		}
		defer rows.Close()
	} else if sliLen == 2 {
		txn, err = d.BeginTxn(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return err
		}
		// SET TRANSACTION SNAPSHOT '000003A1-1';
		_, err = txn.ExecContext(ctx, sqlSlis[0])
		if err != nil {
			return err
		}
		rows, err = txn.QueryContext(ctx, sqlSlis[1], queryArgs...)
		if err != nil {
			return err
		}
		defer rows.Close()
	} else {
		return fmt.Errorf("the query sql [%v] cannot be over two values, please contact author or reselect", querySQL)
	}

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
				//rowsMap[cols[i]] = `NULL` -> sql
				rowData[columnNameOrderIndexMap[colName]] = nil
			} else {
				value := reflect.ValueOf(valRes).Interface()
				switch val := value.(type) {
				case int16, int32, int64:
					rowData[columnNameOrderIndexMap[colName]] = val
				case string:
					convertUtf8Raw, err := stringutil.CharsetConvertReplace([]byte(val), dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
					}
					convertTargetRaw, err := stringutil.CharsetConvertReplace(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
					}
					rowData[columnNameOrderIndexMap[colName]] = stringutil.BytesToString(convertTargetRaw)
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

	// transaction commit
	if sliLen == 2 {
		if err = d.CommitTxn(txn); err != nil {
			return err
		}
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

	batchRowsDataChanTemp := make([]interface{}, 0, 1)

	batchRowsData := make([]string, 0, batchSize)

	columnNameOrderIndexMap := make(map[string]int, columnNameOrdersCounts)

	for i, c := range columnNameOrders {
		columnNameOrderIndexMap[c] = i
	}

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	sqlSlis := stringutil.StringSplit(querySQL, constant.StringSeparatorSemicolon)
	sliLen := len(sqlSlis)

	var (
		txn  *sql.Tx
		rows *sql.Rows
	)

	if sliLen == 1 {
		rows, err = d.QueryContext(ctx, sqlSlis[0], queryArgs...)
		if err != nil {
			return err
		}
		defer rows.Close()
	} else if sliLen == 2 {
		txn, err = d.BeginTxn(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return err
		}
		// SET TRANSACTION SNAPSHOT '000003A1-1';
		_, err = txn.ExecContext(ctx, sqlSlis[0])
		if err != nil {
			return err
		}
		rows, err = txn.QueryContext(ctx, sqlSlis[1], queryArgs...)
		if err != nil {
			return err
		}
		defer rows.Close()
	} else {
		return fmt.Errorf("the query sql [%v] cannot be over two values, please contact author or reselect", querySQL)
	}

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
				case int16, int32, int64:
					rowData[columnNameOrderIndexMap[colName]] = fmt.Sprintf("%v", val)
				case string:
					convertUtf8Raw, err := stringutil.CharsetConvertReplace([]byte(val), dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
					if err != nil {
						return fmt.Errorf("column [%s] datatype [%s] value [%v] charset convert failed, %v", colName, databaseTypes[i], val, err)
					}
					switch {
					case strings.EqualFold(taskFlow, constant.TaskFlowPostgresToTiDB) || strings.EqualFold(taskFlow, constant.TaskFlowPostgresToMySQL):
						convertTargetRaw, err := stringutil.CharsetConvertReplace([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase(convertUtf8Raw)), constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
						if err != nil {
							return fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}
						rowData[columnNameOrderIndexMap[colName]] = fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw))
					default:
						return fmt.Errorf("the task_flow [%s] isn't support, please contact author or reselect, %v", taskFlow, err)
					}
				default:
					str, err := d.RecursiveNonStmt(taskFlow, colName, dbCharsetS, dbCharsetT, garbledReplace, valRes)
					if err != nil {
						return err
					}
					rowData[columnNameOrderIndexMap[colName]] = str
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

	// transaction commit
	if sliLen == 2 {
		if err = d.CommitTxn(txn); err != nil {
			return err
		}
	}
	return nil
}

func (d *Database) RecursiveNonStmt(taskFlow, columnName, dbCharsetS, dbCharsetT, garbledReplace string, valRes interface{}) (string, error) {
	v := reflect.ValueOf(valRes)
	switch v.Kind() {
	case reflect.Int16, reflect.Int32, reflect.Int64:
		return decimal.NewFromInt(v.Int()).String(), nil
	case reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return decimal.NewFromFloat(v.Float()).String(), nil
	case reflect.Bool:
		return strconv.FormatBool(v.Bool()), nil
	case reflect.String:
		convertUtf8Raw, err := stringutil.CharsetConvertReplace([]byte(v.String()), dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
		if err != nil {
			return "", fmt.Errorf("column [%s] charset convert failed, %v", columnName, err)
		}
		switch {
		case strings.EqualFold(taskFlow, constant.TaskFlowPostgresToTiDB) || strings.EqualFold(taskFlow, constant.TaskFlowPostgresToMySQL):
			convertTargetRaw, err := stringutil.CharsetConvertReplace([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase(convertUtf8Raw)), constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
			if err != nil {
				return "", fmt.Errorf("column [%s] charset convert failed, %v", columnName, err)
			}
			return fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw)), nil
		default:
			return "", fmt.Errorf("the task_flow [%s] isn't support, please contact author or reselect, %v", taskFlow, err)
		}
	case reflect.Array, reflect.Slice:
		return fmt.Sprintf("'%v'", stringutil.BytesToString(v.Bytes())), nil
	case reflect.Interface:
		str, err := d.RecursiveCRC(columnName, dbCharsetS, dbCharsetT, v.Elem().Interface())
		if err != nil {
			return str, err
		}
		return str, nil
	default:
		return "", fmt.Errorf("column [%v] column_value [%v] column_kind [%v] not support, please contact author or exclude", columnName, v.String(), v.Kind())
	}
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

	sqlSlis := stringutil.StringSplit(querySQL, constant.StringSeparatorSemicolon)
	sliLen := len(sqlSlis)

	var (
		txn  *sql.Tx
		rows *sql.Rows
	)

	if sliLen == 1 {
		rows, err = d.QueryContext(ctx, sqlSlis[0], queryArgs...)
		if err != nil {
			return err
		}
		defer rows.Close()
	} else if sliLen == 2 {
		txn, err = d.BeginTxn(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return err
		}
		// SET TRANSACTION SNAPSHOT '000003A1-1';
		_, err = txn.ExecContext(ctx, sqlSlis[0])
		if err != nil {
			return err
		}
		rows, err = txn.QueryContext(ctx, sqlSlis[1], queryArgs...)
		if err != nil {
			return err
		}
		defer rows.Close()
	} else {
		return fmt.Errorf("the query sql [%v] cannot be over two values, please contact author or reselect", querySQL)
	}

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
			// postgres database NULL and "" are differently
			if stringutil.IsValueNil(valRes) {
				if !strings.EqualFold(nullValue, "") {
					rowData[columnNameOrderIndexMap[colName]] = nullValue
				} else {
					rowData[columnNameOrderIndexMap[colName]] = `NULL`
				}
			} else {
				if err = d.RecursiveCSV(
					columnNameOrderIndexMap[colName],
					taskFlow,
					colName,
					dbCharsetS,
					dbCharsetT,
					databaseTypes[i],
					valRes,
					rowData,
					separator,
					delimiter,
					garbledReplace,
					escapeBackslash); err != nil {
					return err
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

	// transaction commit
	if sliLen == 2 {
		if err = d.CommitTxn(txn); err != nil {
			return err
		}
	}

	return nil
}

func (d *Database) RecursiveCSV(columnOrder int, taskFlow, columnName, dbCharsetS, dbCharsetT, dbTypes string, valRes interface{}, rowData []string, separator, delimiter, garbledReplace string, escapeBackslash bool) error {
	v := reflect.ValueOf(valRes)
	switch v.Kind() {
	case reflect.Int16, reflect.Int32, reflect.Int64:
		rowData[columnOrder] = decimal.NewFromInt(v.Int()).String()
	case reflect.Uint16, reflect.Uint32, reflect.Uint64:
		rowData[columnOrder] = strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		rowData[columnOrder] = decimal.NewFromFloat(v.Float()).String()
	case reflect.Bool:
		rowData[columnOrder] = strconv.FormatBool(v.Bool())
	case reflect.String:
		// postgres database NULL and "" are differently, so "" no special attention is required
		var convertTargetRaw []byte
		convertUtf8Raw, err := stringutil.CharsetConvertReplace([]byte(v.String()), dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
		if err != nil {
			return fmt.Errorf("column [%s] charset convert failed, %v", columnName, err)
		}
		// Handling character sets, special character escapes, string reference delimiters
		if escapeBackslash {
			switch taskFlow {
			case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL, constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
				convertTargetRaw, err = stringutil.CharsetConvertReplace([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase(convertUtf8Raw)), constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
				if err != nil {
					return fmt.Errorf("column [%s] charset convert failed, %v", columnName, err)
				}
			default:
				return fmt.Errorf("the task_flow [%s] isn't support, please contact author or reselect, %v", taskFlow, err)
			}
		} else {
			convertTargetRaw, err = stringutil.CharsetConvertReplace(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
			if err != nil {
				return fmt.Errorf("column [%s] charset convert failed, %v", columnName, err)
			}
		}
		if delimiter == "" {
			rowData[columnOrder] = stringutil.BytesToString(convertTargetRaw)
		} else {
			rowData[columnOrder] = stringutil.StringBuilder(delimiter, stringutil.BytesToString(convertTargetRaw), delimiter)
		}
	case reflect.Array, reflect.Slice:
		if strings.EqualFold(dbTypes, constant.BuildInPostgresDatatypeBytea) {
			// bytea -> []uint8
			// binary
			rowData[columnOrder] = stringutil.EscapeBinaryCSV(v.Bytes(), escapeBackslash, delimiter, separator)
		} else {
			// postgres database NULL and "" are differently, so "" no special attention is required
			var convertTargetRaw []byte
			convertUtf8Raw, err := stringutil.CharsetConvertReplace(v.Bytes(), dbCharsetS, constant.CharsetUTF8MB4, garbledReplace)
			if err != nil {
				return fmt.Errorf("column [%s] charset convert failed, %v", columnName, err)
			}
			// Handling character sets, special character escapes, string reference delimiters
			if escapeBackslash {
				switch taskFlow {
				case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL, constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
					convertTargetRaw, err = stringutil.CharsetConvertReplace([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase(convertUtf8Raw)), constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
					if err != nil {
						return fmt.Errorf("column [%s] charset convert failed, %v", columnName, err)
					}
				default:
					return fmt.Errorf("the task_flow [%s] isn't support, please contact author or reselect, %v", taskFlow, err)
				}
			} else {
				convertTargetRaw, err = stringutil.CharsetConvertReplace(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT, garbledReplace)
				if err != nil {
					return fmt.Errorf("column [%s] charset convert failed, %v", columnName, err)
				}
			}
			if delimiter == "" {
				rowData[columnOrder] = stringutil.BytesToString(convertTargetRaw)
			} else {
				rowData[columnOrder] = stringutil.StringBuilder(delimiter, stringutil.BytesToString(convertTargetRaw), delimiter)
			}
		}
	case reflect.Interface:
		if err := d.RecursiveCSV(columnOrder, taskFlow, columnName, dbCharsetS, dbCharsetT, dbTypes, v.Elem().Interface(), rowData, separator, delimiter, garbledReplace, escapeBackslash); err != nil {
			return err
		}
	default:
		return fmt.Errorf("column [%v] column_value [%v] column_kind [%v] not support, please contact author or exclude", columnName, v.String(), v.Kind())
	}
	return nil
}
