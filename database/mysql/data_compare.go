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
	"github.com/shopspring/decimal"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"github.com/wentaojin/dbms/utils/structure"
	"go.uber.org/zap"
	"hash/crc32"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
	normalRes, err := d.GetDatabaseTableNormalIndex(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	for _, nr := range normalRes {
		ci[nr["INDEX_NAME"]] = nr["COLUMN_LIST"]
	}
	uniqueRes, err := d.GetDatabaseTableUniqueIndex(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	for _, ur := range uniqueRes {
		ci[ur["INDEX_NAME"]] = ur["COLUMN_LIST"]
	}
	return ci, nil
}

func (d *Database) GetDatabaseTableStatisticsBucket(schemaNameS, tableNameS string, consColumns map[string]string) (map[string][]structure.Bucket, error) {
	_, res, err := d.GeneralQuery(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		var pkIntegerColumnName string
		if pkValus, ok := consColumns["PRIMARY"]; ok {
			pkSlis := stringutil.StringSplit(pkValus, constant.StringSeparatorComplexSymbol)
			if len(pkSlis) == 1 {
				columnInfo, err := d.GetDatabaseTableColumnInfo(schemaNameS, tableNameS)
				if err != nil {
					return nil, err
				}
				for _, column := range columnInfo {
					if strings.EqualFold(pkSlis[0], column["Column_name"]) && stringutil.IsContainedString(constant.TiDBDatabaseIntegerColumnDatatypePrimaryKey, stringutil.StringUpper(column["DATA_TYPE"])) {
						pkIntegerColumnName = pkSlis[0]
						break
					}
				}
			}
		}

		for _, r := range res {
			count, err := strconv.ParseInt(r["Count"], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("the database [%s] table [%s] statistics bucket error parsing integer: %v", schemaNameS, tableNameS, err)
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
		return buckets, nil
	}
	return nil, fmt.Errorf("the database [%s] table [%s] statistics bucket doesn't supported, only support tidb database, version: [%v]", schemaNameS, tableNameS, res[0]["VERSION"])
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
				columnInfo, err := d.GetDatabaseTableColumnInfo(schemaNameS, tableNameS)
				if err != nil {
					return nil, err
				}
				for _, column := range columnInfo {
					if strings.EqualFold(pkSlis[0], column["Column_name"]) && stringutil.IsContainedString(constant.TiDBDatabaseIntegerColumnDatatypePrimaryKey, stringutil.StringUpper(column["DATA_TYPE"])) {
						pkIntegerColumnName = pkSlis[0]
						break
					}
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

func (d *Database) GetDatabaseTableHighestSelectivityIndex(schemaNameS, tableNameS string, compareCondField string, ignoreCondFields []string) (*structure.HighestBucket, error) {
	consColumns, err := d.GetDatabaseTableConstraintIndexColumn(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
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

	// find max histogram indexName -> columnName
	buckets, err := d.GetDatabaseTableStatisticsBucket(schemaNameS, tableNameS, consColumns)
	if err != nil {
		return nil, err
	}

	if len(buckets) == 0 {
		// not found bucket
		return nil, fmt.Errorf("the schema [%s] table [%s] not found buckets, please contact author or reselect", schemaNameS, tableNameS)
	}

	highestBucket, err := structure.FindMatchDistinctCountBucket(sortHists, buckets, consColumns)
	if err != nil {
		return nil, err
	}

	properties, err := d.GetDatabaseTableColumnProperties(schemaNameS, tableNameS, highestBucket.IndexColumn)
	if err != nil {
		return nil, err
	}
	var (
		columnProps       []string
		columnCollations  []string
		datetimePrecision []string
	)
	for _, c := range highestBucket.IndexColumn {
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

	highestBucket.ColumnDatatype = columnProps
	highestBucket.ColumnCollation = columnCollations
	highestBucket.DatetimePrecision = datetimePrecision
	return highestBucket, nil
}

func (d *Database) GetDatabaseTableRandomValues(schemaNameS, tableNameS string, columns []string, conditions string, limit int, collations []string) ([][]string, error) {
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
	}

	columnNames := make([]string, 0, len(columns))
	columnOrders := make([]string, 0, len(collations))
	for i, col := range columns {
		columnNames = append(columnNames, fmt.Sprintf("`%s`", col))
		if !strings.EqualFold(collations[i], "") {
			columnOrders = append(columnOrders, fmt.Sprintf("`%s` COLLATE '%s'", col, collations[i]))
		}
	}

	query := fmt.Sprintf("SELECT %[1]s FROM (SELECT %[1]s, rand() rand_value FROM %[2]s WHERE %[3]s ORDER BY rand_value LIMIT %[4]d)rand_tmp ORDER BY %[5]s",
		strings.Join(columnNames, ", "), fmt.Sprintf("`%s`.`%s`", schemaNameS, tableNameS), conditions, limit, strings.Join(columnOrders, ", "))

	logger.Debug("divide database bucket value by query", zap.Strings("chunk", collations), zap.String("query", query))

	rows, err := d.DBConn.QueryContext(d.Ctx, query)
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
			// ORACLE database NULL and "" are the same, but mysql database NULL and "" are the different
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
