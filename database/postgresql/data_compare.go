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
	"hash/crc32"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/greatcloak/decimal"
	"github.com/lib/pq"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"github.com/wentaojin/dbms/utils/structure"
	"go.uber.org/zap"
)

func (d *Database) GetDatabaseTableConstraintIndexColumn(schemaNameS, tableNameS string) (map[string]string, error) {
	ci := make(map[string]string)
	pkRes, err := d.GetDatabaseTablePrimaryKey(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	if len(pkRes) > 1 {
		return nil, fmt.Errorf("the database schema [%s] table [%s] has more than one primary key", schemaNameS, tableNameS)
	}
	if len(pkRes) == 1 {
		ci[pkRes[0]["CONSTRAINT_NAME"]] = pkRes[0]["COLUMN_LIST"]
	}
	ukRes, err := d.GetDatabaseTableUniqueKey(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	for _, uk := range ukRes {
		ci[uk["CONSTRAINT_NAME"]] = uk["COLUMN_LIST"]
	}
	uniqueRes, err := d.GetDatabaseTableUniqueIndex(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	for _, ur := range uniqueRes {
		ci[ur["INDEX_NAME"]] = ur["COLUMN_LIST"]
	}
	// when the database table has a primary key, unique key, or unique index, simply ignore the stringutil index and select only from the primary key, unique key, or unique index options.
	// there is a problem with stats_histograms in the tidb database: the NDV of a stringutil field may be higher than that of the PK. This problem may cause the DBMS to select a stringutil field (such as a Chinese field) to divide the chunk, because the sorting of Chinese in GBK and UTF8 is inconsistent, and a large number of FIX
	// issue: https://github.com/wentaojin/dbms/issues/70
	if len(ci) == 0 {
		normalRes, err := d.GetDatabaseTableNormalIndex(schemaNameS, tableNameS)
		if err != nil {
			return nil, err
		}
		for _, nr := range normalRes {
			ci[nr["INDEX_NAME"]] = nr["COLUMN_LIST"]
		}
	}
	return ci, nil
}

func (d *Database) GetDatabaseTableStatisticsBucket(schemaNameS, tableNameS string, consColumns map[string]string) (map[string][]structure.Bucket, map[string]string, error) {
	columnDists, columnBs, err := d.getDatabaseTableColumnStatisticsBucket(schemaNameS, tableNameS)
	if err != nil {
		return nil, nil, err
	}
	estRows, err := d.GetDatabaseTableRows(schemaNameS, tableNameS)
	if err != nil {
		return nil, nil, err
	}

	buckets := make(map[string][]structure.Bucket)
	newConsColumns := make(map[string]string)

	for k, v := range consColumns {
		columns := stringutil.StringSplit(v, constant.StringSeparatorComplexSymbol)

		var (
			columnBuckets [][]string
			newColumns    []string
		)
		for i, c := range columns {
			// prefix column index need exist, otherwise break, continue next index
			if i == 0 && len(columnBs[c]) == 0 {
				break
			} else if i > 0 && len(columnBs[c]) == 0 {
				// when statistics exist for the join index, but the suffix index does not exist, you can ignore this field and continue
				continue
			} else {
				columnBuckets = append(columnBuckets, columnBs[c])
				newColumns = append(newColumns, c)
			}
		}

		// skip
		if columnBuckets == nil {
			continue
		}

		// order merge, return (a,b)  (a,c)
		columnValues, columnValuesLen := stringutil.StringSliceAlignLen(columnBuckets)

		var newColumnsBs []string
		for j := 0; j < columnValuesLen; j++ {
			var bs []string
			for i := 0; i < len(columnValues); i++ {
				bs = append(bs, columnValues[i][j])
			}
			newColumnsBs = append(newColumnsBs, stringutil.StringJoin(bs, constant.StringSeparatorComplexSymbol))
		}

		// estimated number of rows per index based on the first field of the index
		buckets[k] = structure.StringSliceCreateBuckets(newColumnsBs, int64(math.Round(float64(estRows)*columnDists[columns[0]])))

		newConsColumns[k] = stringutil.StringJoin(newColumns, constant.StringSeparatorComplexSymbol)
	}

	return buckets, newConsColumns, nil
}

func (d *Database) GetDatabaseTableStatisticsHistogram(schemaNameS, tableNameS string, consColumns map[string]string) (map[string]structure.Histogram, error) {
	columnDists, _, err := d.getDatabaseTableColumnStatisticsBucket(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	estRows, err := d.GetDatabaseTableRows(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}

	hist := make(map[string]structure.Histogram)
	for k, v := range consColumns {
		columns := stringutil.StringSplit(v, constant.StringSeparatorComplexSymbol)
		hist[k] = structure.Histogram{
			DistinctCount: int64(math.Round(float64(estRows) * columnDists[columns[0]])),
		}
	}
	return hist, nil
}

func (d *Database) GetDatabaseTableHighestSelectivityIndex(schemaNameS, tableNameS string, compareCondField string, ignoreCondFields []string) (*structure.Selectivity, error) {
	oldConsColumns, err := d.GetDatabaseTableConstraintIndexColumn(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	// find max histogram indexName -> columnName
	buckets, consColumns, err := d.GetDatabaseTableStatisticsBucket(schemaNameS, tableNameS, oldConsColumns)
	if err != nil {
		return nil, err
	}
	if len(buckets) == 0 {
		// not found bucket
		return nil, nil
	}

	// query := fmt.Sprintf("SELECT COUNT(DISTINCT %s)/COUNT(1) as SEL FROM %s.%s", columns, schemaNameS,tableNameS)
	histograms, err := d.GetDatabaseTableStatisticsHistogram(schemaNameS, tableNameS, consColumns)
	if err != nil {
		return nil, err
	}
	// the database table histogram not found
	if len(histograms) == 0 {
		return nil, nil
	}

	var sortHists structure.SortHistograms

	var newIgnoreFields []string
	if len(ignoreCondFields) > 0 && stringutil.IsContainedString(ignoreCondFields, compareCondField) {
		newIgnoreFields = stringutil.StringSliceRemoveElement(ignoreCondFields, compareCondField)
	} else {
		newIgnoreFields = ignoreCondFields
	}

	if !strings.EqualFold(compareCondField, "") || len(newIgnoreFields) > 0 {
		matchIndexes := structure.FindColumnMatchConstraintIndexNames(consColumns, compareCondField, newIgnoreFields)
		matchIndexHists := structure.ExtractColumnMatchHistogram(matchIndexes, histograms)

		if len(matchIndexes) == 0 || len(matchIndexHists) == 0 {
			// not found, the custom column not match index or not match histogram
			return nil, nil
		}
		sortHists = structure.SortDistinctCountHistogram(matchIndexHists, consColumns)
	} else {
		sortHists = structure.SortDistinctCountHistogram(histograms, consColumns)
	}

	Selectivity, err := structure.FindMatchDistinctCountBucket(sortHists, buckets, consColumns)
	if err != nil {
		return nil, err
	}

	properties, err := d.GetDatabaseTableColumnProperties(schemaNameS, tableNameS, Selectivity.IndexColumn)
	if err != nil {
		return nil, err
	}
	var (
		columnProps       []string
		columnCollations  []string
		datetimePrecision []string
	)
	for _, c := range Selectivity.IndexColumn {
		for _, p := range properties {
			if strings.EqualFold(p["COLUMN_NAME"], c) {
				columnProps = append(columnProps, p["DATA_TYPE"])
				if stringutil.IsContainedStringIgnoreCase(append([]string{}, append(
					constant.DataComparePostgresCompatibleDatabaseColumnTimeSubtypes,
					constant.DataComparePostgresCompatibleDatabaseColumnDateSubtypes...)...), p["DATA_TYPE"]) {
					datetimePrecision = append(datetimePrecision, p["DATETIME_PRECISION"])
				} else {
					// the column datatype isn't supported, fill ""
					datetimePrecision = append(datetimePrecision, constant.DataCompareDisabledCollationSettingFillEmptyString)
				}
				if stringutil.IsContainedStringIgnoreCase(constant.DataComparePostgresCompatibleDatabaseColumnDatatypeSupportCollation, p["DATA_TYPE"]) {
					columnCollations = append(columnCollations, p["COLLATION"])
				} else {
					// the column datatype isn't supported, fill ""
					columnCollations = append(columnCollations, constant.DataCompareDisabledCollationSettingFillEmptyString)
				}
			}
		}
	}

	Selectivity.ColumnDatatype = columnProps
	Selectivity.ColumnCollation = columnCollations
	Selectivity.DatetimePrecision = datetimePrecision
	return Selectivity, nil
}

func (d *Database) GetDatabaseTableColumnProperties(schemaNameS, tableNameS string, columnNameSli []string) ([]map[string]string, error) {
	var (
		sqlStr  string
		columns []string
	)
	for _, c := range columnNameSli {
		columns = append(columns, fmt.Sprintf("'%s'", c))
	}

	sqlStr = fmt.Sprintf(`select
	col.table_schema AS "TABLE_OWNER",
	col.table_name AS "TABLE_NAME",
	col.column_name AS "COLUMN_NAME",
	col.data_type AS "DATA_TYPE",
	COALESCE(col.character_maximum_length,0) AS "DATA_LENGTH",
	COALESCE(col.numeric_precision,0) AS "DATA_PRECISION",
	COALESCE(col.numeric_scale,0) AS "DATA_SCALE",
	COALESCE(col.datetime_precision,0) AS "DATETIME_PRECISION",
	case
		col.is_nullable when 'YES' then 'Y'
		when 'NO' then 'N'
		else 'UNKNOWN'
	end AS "NULLABLE",
	COALESCE(col.column_default,'NULLSTRING') AS "DATA_DEFAULT",
	COALESCE(col.character_set_name,'UNKNOWN') AS "CHARSET",
	COALESCE(col.collation_name,'UNKNOWN') AS "COLLATION",
	temp.column_comment as "COMMENTS"
from
	information_schema.columns col
join (
	select
		a.attname as column_name,
		d.description as column_comment
	from
		pg_attribute a
	join pg_class c on
		c.oid = a.attrelid
	join pg_namespace n on n.oid = c.relnamespace
	left join pg_description d on
		d.objoid = a.attrelid
		and d.objsubid = a.attnum
		and d.classoid = 'pg_class'::regclass
	where
		n.nspname = '%s'
		and c.relname = '%s'
		and a.attnum > 0
		and not a.attisdropped
	order by
		a.attnum
) temp on
	col.column_name = temp.column_name
where
	col.table_schema = '%s'
	and col.table_name = '%s'
	and col.column_name in (%s)
order by
	col.ordinal_position`, schemaNameS, tableNameS, schemaNameS, tableNameS, stringutil.StringJoin(columns, constant.StringSeparatorComma))
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableRandomValues(schemaNameS, tableNameS string, columns []string, conditions string, condArgs []interface{}, limit int, collations []string) ([][]string, error) {
	if conditions == "" {
		conditions = "1 = 1"
		condArgs = nil
	}

	columnNames := make([]string, 0, len(columns))
	columnOrders := make([]string, 0, len(collations))
	for i, col := range columns {
		columnNames = append(columnNames, fmt.Sprintf("%s", col))
		if !strings.EqualFold(collations[i], "") {
			columnOrders = append(columnOrders, fmt.Sprintf(`%s COLLATE "%s"`, col, collations[i]))
		} else {
			columnOrders = append(columnOrders, fmt.Sprintf("%s", col))
		}
	}

	query := fmt.Sprintf("SELECT %[1]s FROM (SELECT %[1]s, random() as rand_value FROM %[2]s WHERE %[3]s ORDER BY rand_value LIMIT %[4]d) rand_tmp ORDER BY %[5]s",
		strings.Join(columnNames, ", "), fmt.Sprintf("`%s`.`%s`", schemaNameS, tableNameS), conditions, limit, strings.Join(columnOrders, ", "))

	logger.Debug("divide database bucket value query",
		zap.Strings("columns", columnNames),
		zap.Strings("collations", collations),
		zap.String("query", query),
		zap.Reflect("args", condArgs))

	deadline := time.Now().Add(time.Duration(d.CallTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.DBConn.QueryContext(ctx, query, condArgs...)
	if err != nil {
		return nil, fmt.Errorf("the database table random values query [%v] failed: %w", query, err)
	}
	defer rows.Close()

	randomValues := make([][]string, 0, limit)
NEXTROW:
	for rows.Next() {
		colVals := make([][]byte, len(columns))
		colValsI := make([]interface{}, len(colVals))
		for i := range colValsI {
			colValsI[i] = &colVals[i]
		}
		err = rows.Scan(colValsI...)
		if err != nil {
			return nil, err
		}

		randomValue := make([]string, len(columns))

		for i, col := range colVals {
			if col == nil {
				continue NEXTROW
			}
			randomValue[i] = string(col)
		}
		randomValues = append(randomValues, randomValue)
	}

	return randomValues, err

}

func (d *Database) GetDatabaseTableCompareRow(querySQL string, queryArgs ...any) ([]string, []map[string]string, error) {
	var (
		columns []string
		results []map[string]string
	)

	deadline := time.Now().Add(time.Duration(d.CallTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	sqlSlis := stringutil.StringSplit(querySQL, constant.StringSeparatorSemicolon)
	sliLen := len(sqlSlis)

	var (
		txn  *sql.Tx
		rows *sql.Rows
		err  error
	)

	if sliLen == 1 {
		rows, err = d.QueryContext(ctx, sqlSlis[0], queryArgs...)
		if err != nil {
			return columns, results, err
		}
		defer rows.Close()
	} else if sliLen == 2 {
		txn, err = d.BeginTxn(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return columns, results, err
		}
		// SET TRANSACTION SNAPSHOT '000003A1-1';
		_, err = txn.ExecContext(ctx, sqlSlis[0])
		if err != nil {
			return columns, results, err
		}
		rows, err = txn.QueryContext(ctx, sqlSlis[1], queryArgs...)
		if err != nil {
			return columns, results, err
		}
		defer rows.Close()
	} else {
		return columns, results, fmt.Errorf("the query sql [%v] cannot be over two values, please contact author or reselect", querySQL)
	}

	// general query, automatic get column name
	columns, err = rows.Columns()
	if err != nil {
		return columns, results, fmt.Errorf("query rows.Columns failed, sql: [%v], error: [%v]", querySQL, err)
	}

	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return columns, results, fmt.Errorf("query rows.Scan failed, sql: [%v], error: [%v]", querySQL, err)
		}

		row := make(map[string]string)
		for k, v := range values {
			if v == nil {
				row[columns[k]] = "NULLABLE"
			} else {
				// Handling empty string and other values, the return value output string
				row[columns[k]] = stringutil.BytesToString(v)
			}
		}
		results = append(results, row)
	}

	if err = rows.Err(); err != nil {
		return columns, results, fmt.Errorf("query rows.Next failed, sql: [%v], error: [%v]", querySQL, err.Error())
	}

	// txn commit
	if sliLen == 2 {
		if err := d.CommitTxn(txn); err != nil {
			return columns, results, err
		}
	}
	return columns, results, nil
}

func (d *Database) GetDatabaseTableCompareCrc(querySQL string, callTimeout int, dbCharsetS, dbCharsetT, separator string, queryArgs []interface{}) ([]string, uint32, map[string]int64, error) {
	var (
		rowData     []string
		columnNames []string
		err         error
		crc32Sum    uint32
	)

	var crc32Val uint32 = 0

	// record repeat counts
	batchRowsM := make(map[string]int64)

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
			return nil, crc32Sum, nil, err
		}
		defer rows.Close()
	} else if sliLen == 2 {
		txn, err = d.BeginTxn(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return nil, crc32Sum, nil, err
		}
		// SET TRANSACTION SNAPSHOT '000003A1-1';
		_, err = txn.ExecContext(ctx, sqlSlis[0])
		if err != nil {
			return nil, crc32Sum, nil, err
		}
		rows, err = txn.QueryContext(ctx, sqlSlis[1], queryArgs...)
		if err != nil {
			return nil, crc32Sum, nil, err
		}
		defer rows.Close()
	} else {
		return nil, crc32Sum, nil, fmt.Errorf("the query sql [%v] cannot be over two values, please contact author or reselect", querySQL)
	}

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
	}

	columnNums := len(columnNames)

	// data scan
	rawResult := make([]interface{}, columnNums)
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
			valRes := rawResult[i]
			// postgres database NULL and "" are differently
			if stringutil.IsValueNil(valRes) {
				// ORACLE database NULL and "" are the same, but postgres database NULL and "" are the different
				rowData = append(rowData, `NULL`)
			} else {
				str, err := d.RecursiveCRC(colName, dbCharsetS, dbCharsetT, valRes)
				if err != nil {
					return nil, crc32Sum, nil, err
				}
				rowData = append(rowData, str)
			}
		}

		// append
		batchKey := stringutil.StringJoin(rowData, separator)

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

	// txn commit
	if sliLen == 2 {
		if err := d.CommitTxn(txn); err != nil {
			return nil, crc32Sum, nil, err
		}
	}

	return columnNames, crc32Sum, batchRowsM, nil
}

func (d *Database) RecursiveCRC(columnName, dbCharsetS, dbCharsetT string, valRes interface{}) (string, error) {
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
		// postgres database NULL and "" are differently, so "" no special attention is required
		var convertTargetRaw []byte
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(v.String()), dbCharsetS, constant.CharsetUTF8MB4)
		if err != nil {
			return "", fmt.Errorf("column [%s] charset convert failed, %v", columnName, err)
		}
		convertTargetRaw, err = stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
		if err != nil {
			return "", fmt.Errorf("column [%s] charset convert failed, %v", columnName, err)
		}
		return fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw)), nil
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

func (d *Database) getDatabaseTableColumnStatisticsBucket(schemaNameS, tableNameS string) (map[string]float64, map[string][]string, error) {
	var sqlStr string

	sqlStr = fmt.Sprintf(`SELECT
	attname AS "COLUMN_NAME",
	abs(n_distinct) AS "DISTINCT_KYES",
	histogram_bounds AS "BOUNDS"	
FROM pg_stats
WHERE schemaname = '%s' 
  AND tablename = '%s'`, schemaNameS, tableNameS)

	rows, err := d.QueryContext(d.Ctx, sqlStr)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columnDistKeys := make(map[string]float64)
	columnBounds := make(map[string][]string)

	for rows.Next() {
		var (
			column string
			keys   float64
			bs     pq.StringArray
		)
		err := rows.Scan(&column, &keys, &bs)
		if err != nil {
			return nil, nil, err
		}
		columnDistKeys[column] = keys
		columnBounds[column] = bs
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return columnDistKeys, columnBounds, nil
}

func (d *Database) GetDatabaseTableSeekAbnormalData(taskFlow, querySQL string, queryArgs []interface{}, callTimeout int, dbCharsetS, dbCharsetT string, chunkColumns []string) ([][]string, []map[string]string, error) {
	var (
		columnNames []string

		databaseTypes       []string
		err                 error
		chunkColumnDatas    [][]string
		abnormalColumnDatas []map[string]string
	)

	chunkColumnDataOrderIndex := make(map[string]int)
	for i, c := range chunkColumns {
		chunkColumnDataOrderIndex[c] = i
	}

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	chunkColumnNameIndex := make(map[int]string)
	asciiColumnNameIndex := make(map[int]string)
	originColumnNameIndex := make(map[int]string)
	asciiOriginColumnNameMap := make(map[string]string)

	re := regexp.MustCompile(fmt.Sprintf(`%s([^%s]*)%s`, constant.StringSeparatorDoubleQuotes, constant.StringSeparatorDoubleQuotes, constant.StringSeparatorDoubleQuotes))

	for i, ct := range colTypes {
		columnNames = append(columnNames, ct.Name())

		matcheColumnS := re.FindAllStringSubmatch(ct.Name(), -1)
		originColumnSliS := make(map[string]struct{})
		for _, match := range matcheColumnS {
			if len(match) > 1 {
				originColumnSliS[match[1]] = struct{}{}
			}
		}
		if strings.HasPrefix(strings.ToUpper(ct.Name()), constant.DataCompareSeekAsciiColumnPrefix) {
			asciiColumnNameIndex[i] = ct.Name()
			for originC, _ := range originColumnSliS {
				asciiOriginColumnNameMap[ct.Name()] = originC
			}
		} else {
			for originC, _ := range originColumnSliS {
				originColumnNameIndex[i] = originC
			}
		}

		databaseTypes = append(databaseTypes, ct.DatabaseTypeName())

		for _, c := range chunkColumns {
			if strings.EqualFold(ct.Name(), c) {
				chunkColumnNameIndex[i] = c
				break
			}
		}
	}

	// data scan
	columnNums := len(columnNames)

	rawResult := make([][]byte, columnNums)
	valuePtrs := make([]interface{}, columnNums)
	for i, _ := range columnNames {
		valuePtrs[i] = &rawResult[i]
	}

	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, nil, err
		}

		var rowData []string

		for i, colName := range columnNames {
			valRes := rawResult[i]
			// postgres database NULL and "" are differently
			if stringutil.IsValueNil(valRes) {
				// ORACLE database NULL and "" are the same, but postgres database NULL and "" are the different
				rowData = append(rowData, `NULL`)
			} else {
				str, err := d.RecursiveCRC(colName, dbCharsetS, dbCharsetT, valRes)
				if err != nil {
					return nil, nil, err
				}
				rowData = append(rowData, str)
			}
		}

		// resaon -> garbled、uncommonWords(60159 <= ascii <= 66000)
		// columnName[reason][columnData]
		abnormalRowColumnData := make(map[string]string)
		chunkColumnDataSli := make([]string, len(chunkColumns))

		for i, data := range rowData {
			if chunkColName, exist := chunkColumnNameIndex[i]; exist {
				chunkColumnDataSli[chunkColumnDataOrderIndex[chunkColName]] = data
			} else if asciiColumnName, isAsciiCol := asciiColumnNameIndex[i]; isAsciiCol {
				asciiValue, err := stringutil.StrconvIntBitSize(data, 64)
				if err != nil {
					return nil, nil, err
				}
				if asciiValue >= 60159 && asciiValue <= 66000 {
					if val, exist := abnormalRowColumnData[asciiOriginColumnNameMap[asciiColumnName]]; exist {
						abnormalRowColumnData[asciiOriginColumnNameMap[asciiColumnName]] = fmt.Sprintf("%s/%s", val, constant.DataCompareSeekUncommonWordsAbnormalData)
					} else {
						abnormalRowColumnData[asciiOriginColumnNameMap[asciiColumnName]] = constant.DataCompareSeekUncommonWordsAbnormalData
					}
				}
			} else if strings.ContainsRune(data, utf8.RuneError) {
				if val, exist := abnormalRowColumnData[asciiOriginColumnNameMap[asciiColumnName]]; exist {
					abnormalRowColumnData[originColumnNameIndex[i]] = fmt.Sprintf("%s/%s", val, constant.DataCompareSeekGarbledAbnormalData)
				} else {
					abnormalRowColumnData[originColumnNameIndex[i]] = constant.DataCompareSeekGarbledAbnormalData
				}
			}
		}
		if len(abnormalRowColumnData) > 0 {
			chunkColumnDatas = append(chunkColumnDatas, chunkColumnDataSli)
			abnormalColumnDatas = append(abnormalColumnDatas, abnormalRowColumnData)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, nil, err
	}

	return chunkColumnDatas, abnormalColumnDatas, nil
}
