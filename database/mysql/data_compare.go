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
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/shopspring/decimal"
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
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return nil, nil, err
	}

	buckets := make(map[string][]structure.Bucket)
	if strings.Contains(stringutil.StringUpper(res[0]["VERSION"]), constant.DatabaseTypeTiDB) {
		/*
			example in tidb:
			mysql> SHOW STATS_BUCKETS WHERE db_name= "test" AND table_name="testa";
			+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
			| Db_name | Table_name | Partition_name | Column_name | Is_index | Bucket_id | Count | Repeats | Lower_Bound         | Upper_Bound         |
			+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
			| test    | testa      |                | PRIMARY     |        1 |         0 |    64 |       1 | 1846693550524203008 | 1846838686059069440 |
			| test    | testa      |                | PRIMARY     |        1 |         1 |   128 |       1 | 1846840885082324992 | 1847056389361369088 |
			+---------+------------+----------------+-------------+----------+-----------+-------+---------+---------------------+---------------------+
		*/
		_, res, err = d.GeneralQuery(fmt.Sprintf(`SHOW STATS_BUCKETS WHERE db_name= '%s' AND table_name= '%s'`, schemaNameS, tableNameS))
		if err != nil {
			return nil, nil, err
		}

		var pkIntegerColumnName string
		if pkValus, ok := consColumns["PRIMARY"]; ok {
			pkSlis := stringutil.StringSplit(pkValus, constant.StringSeparatorComplexSymbol)
			if len(pkSlis) == 1 {
				columnInfo, err := d.GetDatabaseTableColumnProperties(schemaNameS, tableNameS, []string{pkSlis[0]})
				if err != nil {
					return nil, nil, err
				}
				if stringutil.IsContainedStringIgnoreCase(constant.TiDBDatabaseIntegerColumnDatatypePrimaryKey, stringutil.StringUpper(columnInfo[0]["DATA_TYPE"])) {
					pkIntegerColumnName = pkSlis[0]
				}
			}
		}

		for _, r := range res {
			count, err := strconv.ParseInt(r["Count"], 10, 64)
			if err != nil {
				return nil, nil, fmt.Errorf("the database [%s] table [%s] statistics bucket error parsing integer: %v", schemaNameS, tableNameS, err)
			}
			// filter index column
			if strings.EqualFold(r["Is_index"], "1") {
				// transform index name to index column
				// nonclustered index
				if _, ok := consColumns[r["Column_name"]]; ok {
					if strings.EqualFold(r["Column_name"], "PRIMARY") {
						buckets["PRIMARY"] = append(buckets["PRIMARY"], structure.Bucket{
							Count:      count,
							LowerBound: r["Lower_Bound"],
							UpperBound: r["Upper_Bound"],
						})
					} else {
						buckets[r["Column_name"]] = append(buckets[r["Column_name"]], structure.Bucket{
							Count:      count,
							LowerBound: r["Lower_Bound"],
							UpperBound: r["Upper_Bound"],
						})
					}
				}
			}
			// when primary key is int type, the columnName will be column's name, not `PRIMARY`, check and transform here.
			if !strings.EqualFold(pkIntegerColumnName, "") && strings.EqualFold(r["Column_name"], pkIntegerColumnName) {
				buckets["PRIMARY"] = append(buckets["PRIMARY"], structure.Bucket{
					Count:      count,
					LowerBound: r["Lower_Bound"],
					UpperBound: r["Upper_Bound"],
				})
			}
		}
		return buckets, nil, nil
	}
	return nil, nil, fmt.Errorf("the database [%s] table [%s] statistics bucket doesn't supported, only support tidb database, version: [%v]", schemaNameS, tableNameS, res[0]["VERSION"])
}

func (d *Database) GetDatabaseTableStatisticsHistogram(schemaNameS, tableNameS string, consColumns map[string]string) (map[string]structure.Histogram, error) {
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return nil, err
	}

	hists := make(map[string]structure.Histogram)
	if strings.Contains(stringutil.StringUpper(res[0]["VERSION"]), constant.DatabaseTypeTiDB) {
		_, res, err = d.GeneralQuery(fmt.Sprintf(`SHOW STATS_HISTOGRAMS WHERE db_name='%s' AND table_name='%s'`, schemaNameS, tableNameS))
		if err != nil {
			return nil, err
		}
		var pkIntegerColumnName string
		if pkValus, ok := consColumns["PRIMARY"]; ok {
			pkSlis := stringutil.StringSplit(pkValus, constant.StringSeparatorComplexSymbol)
			if len(pkSlis) == 1 {
				columnInfo, err := d.GetDatabaseTableColumnProperties(schemaNameS, tableNameS, []string{pkSlis[0]})
				if err != nil {
					return nil, err
				}

				if stringutil.IsContainedStringIgnoreCase(constant.TiDBDatabaseIntegerColumnDatatypePrimaryKey, stringutil.StringUpper(columnInfo[0]["DATA_TYPE"])) {
					pkIntegerColumnName = pkSlis[0]
				}
			}
		}

		for _, r := range res {
			disCount, err := strconv.ParseInt(r["Distinct_count"], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing distinct_count [%s] integer: %v", r["Distinct_count"], err)
			}
			nullCount, err := strconv.ParseInt(r["Null_count"], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing null_count [%s] integer: %v", r["Null_count"], err)
			}
			// filter index column
			if strings.EqualFold(r["Is_index"], "1") {
				// transform index name to index column
				// nonclustered index
				if _, ok := consColumns[r["Column_name"]]; ok {
					if strings.EqualFold(r["Column_name"], "PRIMARY") {
						hists["PRIMARY"] = structure.Histogram{
							DistinctCount: disCount,
							NullCount:     nullCount,
						}
					} else {
						hists[r["Column_name"]] = structure.Histogram{
							DistinctCount: disCount,
							NullCount:     nullCount,
						}
					}
				}
			}
			// int clustered
			// when primary key is int type, the columnName will be column's name, not `PRIMARY`, check and transform here.
			if !strings.EqualFold(pkIntegerColumnName, "") && strings.EqualFold(r["Column_name"], pkIntegerColumnName) {
				hists["PRIMARY"] = structure.Histogram{
					DistinctCount: disCount,
					NullCount:     nullCount,
				}
			}
		}
		return hists, nil
	}
	return nil, fmt.Errorf("the database table statistics histograms doesn't supported, only support tidb database, version: [%v]", res[0]["VERSION"])
}

func (d *Database) GetDatabaseTableHighestSelectivityIndex(schemaNameS, tableNameS string, compareCondField string, ignoreCondFields []string) (*structure.Selectivity, error) {
	consColumns, err := d.GetDatabaseTableConstraintIndexColumn(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	// find max histogram indexName -> columnName
	buckets, _, err := d.GetDatabaseTableStatisticsBucket(schemaNameS, tableNameS, consColumns)
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
				if stringutil.IsContainedStringIgnoreCase(constant.DataCompareMYSQLCompatibleDatabaseColumnTimeSubtypes, p["DATA_TYPE"]) {
					datetimePrecision = append(datetimePrecision, p["DATETIME_PRECISION"])
				} else {
					// the column datatype isn't supported, fill ""
					datetimePrecision = append(datetimePrecision, constant.DataCompareDisabledCollationSettingFillEmptyString)
				}
				if stringutil.IsContainedStringIgnoreCase(constant.DataCompareMYSQLCompatibleDatabaseColumnDatatypeSupportCollation, p["DATA_TYPE"]) {
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

func (d *Database) GetDatabaseTableRandomValues(schemaNameS, tableNameS string, columns []string, conditions string, condArgs []interface{}, limit int, collations []string) ([][]string, error) {
	/*
		example: there is one index consists of `id`, `a`, `b`.
		mysql> SELECT `id`, `a`, `b` FROM (SELECT `id`, `a`, `b`, rand() rand_value FROM `test`.`test`  WHERE `id` COLLATE "latin1_bin" > 0 AND `id` COLLATE "latin1_bin" < 100 ORDER BY rand_value LIMIT 5) rand_tmp ORDER BY `id` COLLATE "latin1_bin";
		+------+------+------+
		| id   | a    | b    |
		+------+------+------+
		|    1 |    2 |    3 |
		|    2 |    3 |    4 |
		|    3 |    4 |    5 |
		+------+------+------+
	*/
	if conditions == "" {
		conditions = "1 = 1"
		condArgs = nil
	}

	columnNames := make([]string, 0, len(columns))
	columnOrders := make([]string, 0, len(collations))
	for i, col := range columns {
		columnNames = append(columnNames, fmt.Sprintf("`%s`", col))
		if !strings.EqualFold(collations[i], "") {
			columnOrders = append(columnOrders, fmt.Sprintf("`%s` COLLATE '%s'", col, collations[i]))
		} else {
			columnOrders = append(columnOrders, fmt.Sprintf("`%s`", col))
		}
	}

	query := fmt.Sprintf("SELECT %[1]s FROM (SELECT %[1]s, rand() rand_value FROM %[2]s WHERE %[3]s ORDER BY rand_value LIMIT %[4]d)rand_tmp ORDER BY %[5]s",
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

func (d *Database) GetDatabaseTableColumnProperties(schemaNameS, tableNameS string, columnNameS []string) ([]map[string]string, error) {
	var columns []string
	for _, c := range columnNameS {
		columns = append(columns, fmt.Sprintf("'%s'", c))
	}
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
		AND COLUMN_NAME IN (%s)`, schemaNameS, tableNameS, stringutil.StringJoin(columns, constant.StringSeparatorComma))
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableCompareRow(query string, args ...any) ([]string, []map[string]string, error) {
	var (
		columns []string
		results []map[string]string
	)

	deadline := time.Now().Add(time.Duration(d.CallTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// general query, automatic get column name
	columns, err = rows.Columns()
	if err != nil {
		return columns, results, fmt.Errorf("query rows.Columns failed, sql: [%v], error: [%v]", query, err)
	}

	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scans...)
		if err != nil {
			return columns, results, fmt.Errorf("query rows.Scan failed, sql: [%v], error: [%v]", query, err)
		}

		row := make(map[string]string)
		for k, v := range values {
			// Notes: oracle database NULL and ""
			//	1, if the return value is NULLABLE, it represents the value is NULL, oracle sql query statement had be required the field NULL judgement, and if the filed is NULL, it returns that the value is NULLABLE
			//	2, if the return value is nil, it represents the value is NULL
			//	3, if the return value is "", it represents the value is '' string
			//	4, if the return value is 'NULL' or 'null', it represents the value is NULL or null string
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
		return columns, results, fmt.Errorf("query rows.Next failed, sql: [%v], error: [%v]", query, err.Error())
	}
	return columns, results, nil
}

func (d *Database) GetDatabaseTableCompareCrc(querySQL string, callTimeout int, dbCharsetS, dbCharsetT, separator string, queryArgs []interface{}) ([]string, uint32, map[string]int64, error) {
	var (
		rowData       []string
		columnNames   []string
		databaseTypes []string
		scanTypes     []string
		err           error
		crc32Sum      uint32
	)

	var crc32Val uint32 = 0

	// record repeat counts
	batchRowsM := make(map[string]int64)

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, querySQL, queryArgs...)
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
		databaseTypes = append(databaseTypes, ct.DatabaseTypeName())
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
			// ORACLE database NULL and '' are the same, but mysql database NULL and '' are the different
			if val == nil {
				rowData = append(rowData, `NULL`)
			} else if stringutil.BytesToString(val) == "" {
				rowData = append(rowData, "")
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
					if strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeDecimal) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeBigint) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeDouble) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeDoublePrecision) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeFloat) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeInt) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeInteger) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeMediumint) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeNumeric) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeReal) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeSmallint) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeTinyint) {
						rfs, err := decimal.NewFromString(stringutil.BytesToString(val))
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] NewFromString strconv failed, %v", colName, databaseTypes[i], val, err)
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

	return columnNames, crc32Sum, batchRowsM, nil
}

func (d *Database) GetDatabaseTableSeekAbnormalData(taskFlow, querySQL string, queryArgs []interface{}, callTimeout int, dbCharsetS, dbCharsetT string, chunkColumns []string) ([][]string, []map[string]string, error) {
	var (
		columnNames         []string
		scanTypes           []string
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

	re := regexp.MustCompile(fmt.Sprintf(`%s([^%s]*)%s`, constant.StringSeparatorBacktick, constant.StringSeparatorBacktick, constant.StringSeparatorBacktick))

	for i, ct := range colTypes {
		columnNames = append(columnNames, ct.Name())
		scanTypes = append(scanTypes, ct.ScanType().String())

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
			val := rawResult[i]
			// ORACLE database NULL and '' are the same, but mysql database NULL and '' are the different
			if val == nil {
				rowData = append(rowData, `NULL`)
			} else if stringutil.BytesToString(val) == "" {
				rowData = append(rowData, "")
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
					if strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeDecimal) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeBigint) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeDouble) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeDoublePrecision) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeFloat) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeInt) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeInteger) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeMediumint) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeNumeric) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeReal) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeSmallint) ||
						strings.EqualFold(databaseTypes[i], constant.BuildInMySQLDatatypeTinyint) {
						rfs, err := decimal.NewFromString(stringutil.BytesToString(val))
						if err != nil {
							return nil, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] NewFromString strconv failed, %v", colName, databaseTypes[i], val, err)
						}
						rowData = append(rowData, rfs.String())
					} else {
						convertUtf8Raw, err := stringutil.CharsetConvert(val, dbCharsetS, constant.CharsetUTF8MB4)
						if err != nil {
							return nil, nil, fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}
						convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
						if err != nil {
							return nil, nil, fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}
						rowData = append(rowData, fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw)))
					}
				}
			}
		}

		// resaon -> garbled、uncommonWords(60159 <= ascii <= 66000)、unknown
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
