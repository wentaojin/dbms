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
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/utils/structure"
	"go.uber.org/zap"
	"hash/crc32"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/godror/godror"
	"github.com/shopspring/decimal"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
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
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	INDEX_NAME,
	DISTINCT_KEYS,
	NUM_ROWS
FROM DBA_IND_STATISTICS
WHERE TABLE_OWNER = '%s' 
  AND TABLE_NAME = '%s' 
  AND OBJECT_TYPE = 'INDEX'`, schemaNameS, tableNameS))
	if err != nil {
		return nil, err
	}
	// estimate avg rows per key
	bsEstimateCounts := make(map[string]int64)
	for _, r := range res {
		numRows, err := strconv.ParseInt(r["NUM_ROWS"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing null_rows [%s] integer: %v", r["NUM_ROWS"], err)
		}
		bsEstimateCounts[r["INDEX_NAME"]] = numRows
	}

	buckets := make(map[string][]structure.Bucket)
	for k, v := range consColumns {
		columns := stringutil.StringSplit(v, constant.StringSeparatorComplexSymbol)

		columnProps := make(map[string]string)
		props, err := d.GetDatabaseTableColumnProperties(schemaNameS, tableNameS, columns)
		if err != nil {
			return nil, err
		}
		for _, p := range props {
			columnProps[p["COLUMN_NAME"]] = p["DATA_TYPE"]
		}

		var columnBuckets [][]string
		for i, c := range columns {
			bts, err := d.getDatabaseTableColumnStatisticsBucket(schemaNameS, tableNameS, c, columnProps[c])
			if err != nil {
				return nil, err
			}

			// prefix column index need exist or match support datatype, otherwise break, continue next index
			if i == 0 && len(bts) == 0 {
				if !d.filterDatabaseTableColumnBucketSupportDatatype(columnProps[c]) {
					break
				} else {
					columnBuckets = append(columnBuckets, bts)
				}
			} else if i > 0 && len(bts) == 0 {
				continue
			} else {
				if d.filterDatabaseTableColumnBucketSupportDatatype(columnProps[c]) {
					columnBuckets = append(columnBuckets, bts)
				}
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

		buckets[k] = structure.StringSliceCreateBuckets(newColumnsBs, bsEstimateCounts[k])
	}

	return buckets, nil
}

func (d *Database) GetDatabaseTableStatisticsHistogram(schemaNameS, tableNameS string, consColumns map[string]string) (map[string]structure.Histogram, error) {
	_, res, err := d.GeneralQuery(fmt.Sprintf(`SELECT
	INDEX_NAME,
	DISTINCT_KEYS
FROM DBA_IND_STATISTICS
WHERE TABLE_OWNER = '%s' 
  AND TABLE_NAME = '%s' 
  AND OBJECT_TYPE = 'INDEX'`, schemaNameS, tableNameS))
	if err != nil {
		return nil, err
	}
	hist := make(map[string]structure.Histogram)
	for _, r := range res {
		disCount, err := strconv.ParseInt(r["DISTINCT_KEYS"], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing distinct_keys [%s] integer: %v", r["DISTINCT_KEYS"], err)
		}
		hist[r["INDEX_NAME"]] = structure.Histogram{
			DistinctCount: disCount,
		}
	}
	return hist, nil
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
				if stringutil.IsContainedStringIgnoreCase(append([]string{},
					append(constant.DataCompareOracleDatabaseSupportDateSubtypes,
						constant.DataCompareOracleDatabaseSupportTimestampSubtypes...)...), p["DATA_TYPE"]) {
					datetimePrecision = append(datetimePrecision, p["DATA_SCALE"])
				} else {
					// the column datatype isn't supported, fill ""
					datetimePrecision = append(datetimePrecision, constant.DataCompareDisabledCollationSettingFillEmptyString)
				}
				if stringutil.IsContainedStringIgnoreCase(constant.DataCompareORACLECompatibleDatabaseColumnDatatypeSupportCollation, p["DATA_TYPE"]) {
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

func (d *Database) GetDatabaseTableRandomValues(schemaNameS, tableNameS string, columns []string, conditions string, condArgs []interface{}, limit int, collations []string) ([][]string, error) {
	if conditions == "" {
		conditions = "1 = 1"
		condArgs = nil
	}

	columnNames := make([]string, 0, len(columns))
	columnOrders := make([]string, 0, len(collations))
	for i, col := range columns {
		columnNames = append(columnNames, fmt.Sprintf(`"%s"`, col))
		if !strings.EqualFold(collations[i], "") {
			columnOrders = append(columnOrders, fmt.Sprintf(`NLSSORT("%s", 'NLS_SORT = %s')`, col, collations[i]))
		} else {
			columnOrders = append(columnOrders, fmt.Sprintf(`"%s"`, col))
		}
	}

	query := fmt.Sprintf(`WITH RandomizedRows AS (
  SELECT %[1]s,
         DBMS_RANDOM.VALUE AS random_value
  FROM %[2]s
  WHERE %[3]s ORDER BY %[4]s
)
SELECT %[1]s
FROM (
  SELECT rr.*,
         ROW_NUMBER() OVER (ORDER BY random_value) AS rn
  FROM RandomizedRows rr
)
WHERE rn <= %[5]d`, stringutil.StringJoin(columnNames, constant.StringSeparatorComma), fmt.Sprintf(`"%s"."%s"`, schemaNameS, tableNameS), conditions, stringutil.StringJoin(columnOrders, constant.StringSeparatorComma), limit)

	logger.Debug("divide database bucket value by query", zap.Strings("columns", columnNames), zap.Strings("collations", collations), zap.String("query", query))

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

func (d *Database) GetDatabaseTableColumnProperties(schemaNameS, tableNameS string, columnNameSli []string) ([]map[string]string, error) {
	var (
		sqlStr  string
		columns []string
	)
	for _, c := range columnNameSli {
		columns = append(columns, fmt.Sprintf("'%s'", c))
	}

	version, err := d.GetDatabaseVersion()
	if err != nil {
		return nil, err
	}

	collationS := false
	if stringutil.VersionOrdinal(version) >= stringutil.VersionOrdinal(constant.OracleDatabaseTableAndColumnSupportVersion) {
		collationS = true
	} else {
		collationS = false
	}

	if collationS {
		sqlStr = fmt.Sprintf(`SELECT 
		T.OWNER,
		T.TABLE_NAME,
		T.COLUMN_NAME,
	    T.DATA_TYPE,
		T.CHAR_LENGTH,
		NVL(T.CHAR_USED, 'UNKNOWN') CHAR_USED,
	    NVL(T.DATA_LENGTH, 0) AS DATA_LENGTH,
	    DECODE(NVL(TO_CHAR(T.DATA_PRECISION), '*'), '*', '38', TO_CHAR(T.DATA_PRECISION)) AS DATA_PRECISION,
	    DECODE(NVL(TO_CHAR(T.DATA_SCALE), '*'), '*', '127', TO_CHAR(T.DATA_SCALE)) AS DATA_SCALE,
		DECODE(T.COLLATION, 'USING_NLS_COMP',(SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_COMP'), T.COLLATION) COLLATION
FROM
	DBA_TAB_COLUMNS T
WHERE
    T.OWNER = '%s'
	AND T.TABLE_NAME = '%s'
	AND T.COLUMN_NAME IN (%s)
ORDER BY
	T.COLUMN_ID`, schemaNameS, tableNameS, stringutil.StringJoin(columns, constant.StringSeparatorComma))
	} else {
		sqlStr = fmt.Sprintf(`SELECT 
		T.OWNER,
		T.TABLE_NAME,
		T.COLUMN_NAME,
	    T.DATA_TYPE,
		T.CHAR_LENGTH,
		NVL(T.CHAR_USED, 'UNKNOWN') CHAR_USED,
	    NVL(T.DATA_LENGTH, 0) AS DATA_LENGTH,
	    DECODE(NVL(TO_CHAR(T.DATA_PRECISION), '*'), '*', '38', TO_CHAR(T.DATA_PRECISION)) AS DATA_PRECISION,
	    DECODE(NVL(TO_CHAR(T.DATA_SCALE), '*'), '*', '127', TO_CHAR(T.DATA_SCALE)) AS DATA_SCALE,
		(SELECT VALUE FROM NLS_DATABASE_PARAMETERS WHERE PARAMETER = 'NLS_COMP') AS COLLATION
FROM
	DBA_TAB_COLUMNS T
WHERE
    T.OWNER = '%s'
	AND T.TABLE_NAME = '%s'
	AND T.COLUMN_NAME IN (%s)
ORDER BY
	T.COLUMN_ID`, schemaNameS, tableNameS, stringutil.StringJoin(columns, constant.StringSeparatorComma))
	}
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableCompareData(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string, queryArgs []interface{}) ([]string, uint32, map[string]int64, error) {
	var (
		rowData       []string
		columnNames   []string
		databaseTypes []string
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
			// ORACLE database NULL and "" are the same
			if stringutil.IsValueNil(valRes) {
				rowData = append(rowData, `NULL`)
			} else {
				value := reflect.ValueOf(valRes).Interface()
				switch val := value.(type) {
				case godror.Number:
					rfs, err := decimal.NewFromString(val.String())
					if err != nil {
						return nil, crc32Sum, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] NewFromString strconv failed, %v", colName, databaseTypes[i], val, err)
					}
					rowData = append(rowData, rfs.String())
				case *godror.Lob:
					lobD, err := val.Hijack()
					if err != nil {
						return nil, crc32Sum, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] hijack failed, %v", colName, databaseTypes[i], val, err)
					}

					if strings.EqualFold(databaseTypes[i], "BFILE") {
						dir, file, err := lobD.GetFileName()
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] hijack getfilename failed, %v", colName, databaseTypes[i], val, err)
						}
						dirPath, err := d.GetDatabaseDirectoryName(dir)
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] hijack get directory name failed, %v", colName, databaseTypes[i], val, err)
						}
						rowData = append(rowData, fmt.Sprintf("'%v'", filepath.Join(dirPath, file)))
					} else {
						// get actual data
						lobSize, err := lobD.Size()
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] hijack size failed, %v", colName, databaseTypes[i], val, err)
						}

						buf := make([]byte, lobSize)

						var (
							res    strings.Builder
							offset int64
						)
						for {
							count, err := lobD.ReadAt(buf, offset)
							if err != nil {
								return nil, crc32Sum, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] hijack readAt failed, %v", colName, databaseTypes[i], val, err)
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
						rowData = append(rowData, fmt.Sprintf("'%v'", res.String()))
					}
					err = lobD.Close()
					if err != nil {
						return nil, crc32Sum, nil, fmt.Errorf("column [%s] datatype [%s] value [%v] hijack close failed, %v", colName, databaseTypes[i], val, err)
					}
				case []uint8:
					// binary data -> raw、long raw、blob
					rowData = append(rowData, fmt.Sprintf("'%v'", stringutil.BytesToString(val)))
				case int64:
					rowData = append(rowData, decimal.NewFromInt(val).String())
				case uint64:
					rowData = append(rowData, strconv.FormatUint(val, 10))
				case float32:
					rowData = append(rowData, decimal.NewFromFloat32(val).String())
				case float64:
					rowData = append(rowData, decimal.NewFromFloat(val).String())
				case int32:
					rowData = append(rowData, decimal.NewFromInt32(val).String())
				case string:
					if strings.EqualFold(val, "") {
						rowData = append(rowData, `NULL`)
					} else {
						convertUtf8Raw, err := stringutil.CharsetConvert([]byte(val), dbCharsetS, constant.CharsetUTF8MB4)
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}

						convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
						if err != nil {
							return nil, crc32Sum, nil, fmt.Errorf("column [%s] charset convert failed, %v", colName, err)
						}
						rowData = append(rowData, fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw)))
					}
				default:
					return nil, crc32Sum, nil, fmt.Errorf("column [%s] unsupported type: %T, need query sql select column format string", colName, value)
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

func (d *Database) getDatabaseTableColumnStatisticsBucket(schemaNameS, tableNameS string, columnNameS, datatypeS string) ([]string, error) {
	var (
		sqlStr string
		vals   []string
	)
	/*
	 * Statistical information query
	 * ONLY SUPPORT NUMBER and NUMBER subdata types, DATE, TIMESTAMP and Subtypes, VARCHAR character data types
	 * 	1, frequency histogram, TOP frequency histogram, mixed histogram endpoint_numbers represents the cumulative frequency of all values ​​contained in the current and previous buckets, endpoint_value/endpoint_actual_value represents the current data (the data type depends on which field is used)
	 * 	2, Contour histogram endpoint_numbers represents the bucket number, endpoint_value represents the maximum value within the value range of the bucket
	 */
	if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportNumberSubtypes, datatypeS) {
		sqlStr = fmt.Sprintf(`SELECT
	ENDPOINT_VALUE
	--h.ENDPOINT_NUMBER AS CUMMULATIVE_FREQUENCY,
	--h.ENDPOINT_NUMBER - LAG(h.ENDPOINT_NUMBER, 1, 0) OVER (ORDER BY h.ENDPOINT_NUMBER) AS FREQUENCY,
	--h.ENDPOINT_REPEAT_COUNT
FROM
	DBA_HISTOGRAMS
WHERE
 	OWNER = '%s'
	AND TABLE_NAME = '%s'
	AND COLUMN_NAME = '%s'
ORDER BY ENDPOINT_VALUE`, schemaNameS, tableNameS, columnNameS)
	}
	if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportVarcharSubtypes, datatypeS) {
		sqlStr = fmt.Sprintf(`SELECT
	ENDPOINT_ACTUAL_VALUE AS ENDPOINT_VALUE	
	--h.ENDPOINT_NUMBER AS CUMMULATIVE_FREQUENCY,
	--h.ENDPOINT_NUMBER - LAG(h.ENDPOINT_NUMBER, 1, 0) OVER (ORDER BY h.ENDPOINT_NUMBER) AS FREQUENCY,
	--h.ENDPOINT_REPEAT_COUNT
FROM
	DBA_HISTOGRAMS
WHERE
 	OWNER = '%s'
	AND TABLE_NAME = '%s'
	AND COLUMN_NAME = '%s'
ORDER BY ENDPOINT_VALUE`, schemaNameS, tableNameS, columnNameS)
	}
	if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, datatypeS) || stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, datatypeS) {
		sqlStr = fmt.Sprintf(`SELECT
		TO_CHAR(TO_DATE(TRUNC(TO_CHAR(ENDPOINT_VALUE)),'J'),'YYYY-MM-DD')||' '||TO_CHAR(TRUNC(SYSDATE)+TO_NUMBER(SUBSTR(TO_CHAR(ENDPOINT_VALUE),INSTR(TO_CHAR(ENDPOINT_VALUE),'.'))),'HH24:MI:SS') AS ENDPOINT_VALUE	
	--h.ENDPOINT_NUMBER AS CUMMULATIVE_FREQUENCY,
	--h.ENDPOINT_NUMBER - LAG(h.ENDPOINT_NUMBER, 1, 0) OVER (ORDER BY h.ENDPOINT_NUMBER) AS FREQUENCY,
	--h.ENDPOINT_REPEAT_COUNT
FROM
	DBA_HISTOGRAMS
WHERE
	OWNER = '%s'
	AND TABLE_NAME = '%s'
	AND COLUMN_NAME = '%s'
ORDER BY ENDPOINT_VALUE`, schemaNameS, tableNameS, columnNameS)
	}
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return vals, err
	}

	for _, r := range res {
		vals = append(vals, r["ENDPOINT_VALUE"])
	}
	return vals, nil
}

func (d *Database) filterDatabaseTableColumnBucketSupportDatatype(columnType string) bool {
	if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportNumberSubtypes, columnType) {
		return true
	}
	if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportVarcharSubtypes, columnType) {
		return true
	}
	if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, columnType) {
		return true
	}
	if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, columnType) {
		return true
	}
	return false
}

//func (d *Database) findDatabaseTableBestColumn(schemaNameS, tableNameS, columnNameS string) ([]string, error) {
//	var err error
//
//	//sqlStr := fmt.Sprintf(`SELECT
//	//	OWNER,
//	//	TABLE_NAME,
//	//	COLUMN_NAME,
//	//	DATA_TYPE,
//	//	NUM_DISTINCT,
//	//	CASE
//	//		-- IF IT IS TIMESTAMP WITH TIMEZONE, CONVERT FROM UTC TO LOCAL TIME
//	//		WHEN DATA_TYPE LIKE 'TIMESTAMP%%WITH TIME ZONE' THEN
//	//			TO_CHAR(
//	//			FROM_TZ(
//	//				TO_TIMESTAMP(LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(LOW_VALUE, 1, 2), 'XX')
//	//+ TO_NUMBER(SUBSTR(LOW_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
//	//|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
//	//|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
//	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
//	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
//	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
//	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
//	//				, 'UTC' ) AT LOCAL
//	//			, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
//	//		WHEN DATA_TYPE LIKE 'TIMESTAMP%%' THEN
//	//			TO_CHAR(
//	//			TO_TIMESTAMP( LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(LOW_VALUE, 1, 2), 'XX')
//	//+ TO_NUMBER(SUBSTR(LOW_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
//	//|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
//	//|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
//	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
//	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
//	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
//	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
//	//			, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
//	//		WHEN DATA_TYPE = 'NUMBER' THEN
//	//			TO_CHAR(UTL_RAW.CAST_TO_NUMBER(LOW_VALUE))
//	//		WHEN DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN
//	//			TO_CHAR(UTL_RAW.CAST_TO_VARCHAR2(LOW_VALUE))
//	//		WHEN DATA_TYPE = 'NVARCHAR2' THEN
//	//			TO_CHAR(UTL_RAW.CAST_TO_NVARCHAR2(LOW_VALUE))
//	//		WHEN DATA_TYPE = 'BINARY_DOUBLE' THEN
//	//			TO_CHAR(UTL_RAW.CAST_TO_BINARY_DOUBLE(LOW_VALUE))
//	//		WHEN DATA_TYPE = 'BINARY_FLOAT' THEN
//	//			TO_CHAR(UTL_RAW.CAST_TO_BINARY_FLOAT(LOW_VALUE))
//	//		WHEN DATA_TYPE = 'DATE' THEN
//	//			TO_CHAR(DBMS_STATS.CONVERT_RAW_TO_DATE(LOW_VALUE), 'YYYY-MM-DD HH24:MI:SS')
//	//		ELSE
//	//			'UNKNOWN DATA_TYPE'
//	//	END LOW_VALUE,
//	//	CASE
//	//		-- IF IT IS TIMESTAMP WITH TIMEZONE, CONVERT FROM UTC TO LOCAL TIME
//	//		WHEN DATA_TYPE LIKE 'TIMESTAMP%%WITH TIME ZONE' THEN
//	//			TO_CHAR(
//	//			FROM_TZ(
//	//				TO_TIMESTAMP(LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(HIGH_VALUE, 1, 2), 'XX')
//	//+ TO_NUMBER(SUBSTR(HIGH_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
//	//|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
//	//|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
//	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
//	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
//	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
//	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
//	//				, 'UTC' ) AT LOCAL
//	//			, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
//	//		WHEN DATA_TYPE LIKE 'TIMESTAMP%%' THEN
//	//			TO_CHAR(
//	//			TO_TIMESTAMP( LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(HIGH_VALUE, 1, 2), 'XX')
//	//+ TO_NUMBER(SUBSTR(HIGH_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
//	//|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
//	//|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
//	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
//	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
//	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
//	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
//	//			, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
//	//		WHEN DATA_TYPE = 'NUMBER' THEN
//	//			TO_CHAR(UTL_RAW.CAST_TO_NUMBER(HIGH_VALUE))
//	//		WHEN DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN
//	//			TO_CHAR(UTL_RAW.CAST_TO_VARCHAR2(HIGH_VALUE))
//	//		WHEN DATA_TYPE = 'NVARCHAR2' THEN
//	//			TO_CHAR(UTL_RAW.CAST_TO_NVARCHAR2(HIGH_VALUE))
//	//		WHEN DATA_TYPE = 'BINARY_DOUBLE' THEN
//	//			TO_CHAR(UTL_RAW.CAST_TO_BINARY_DOUBLE(HIGH_VALUE))
//	//		WHEN DATA_TYPE = 'BINARY_FLOAT' THEN
//	//			TO_CHAR(UTL_RAW.CAST_TO_BINARY_FLOAT(HIGH_VALUE))
//	//		WHEN DATA_TYPE = 'DATE' THEN
//	//			TO_CHAR(DBMS_STATS.CONVERT_RAW_TO_DATE(HIGH_VALUE), 'YYYY-MM-DD HH24:MI:SS')
//	//		ELSE
//	//			'UNKNOWN DATA_TYPE'
//	//	END HIGH_VALUE
//	//FROM
//	//	DBA_TAB_COLUMNS
//	//WHERE
//	//	OWNER = '%s'
//	//	AND TABLE_NAME = '%s'`, schemaNameS, tableNameS)
//
//	sqlStr := fmt.Sprintf(`SELECT
//		OWNER,
//		TABLE_NAME,
//		COLUMN_NAME,
//		DATA_TYPE
//	FROM
//		DBA_TAB_COLUMNS
//	WHERE
//		OWNER = '%s'
//		AND TABLE_NAME = '%s'`, schemaNameS, tableNameS)
//	_, results, err := d.GeneralQuery(sqlStr)
//	if err != nil {
//		return nil, err
//	}
//
//	var columnNameSli []string
//	columnDatatypeMap := make(map[string]string)
//	for _, res := range results {
//		columnDatatypeMap[res["COLUMN_NAME"]] = res["DATA_TYPE"]
//		columnNameSli = append(columnNameSli, res["COLUMN_NAME"])
//	}
//
//	if !strings.EqualFold(columnNameS, "") && d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[columnNameS]) {
//		return []string{columnNameS}, nil
//	}
//
//	pkSlis, err := d.GetDatabaseTablePrimaryKey(schemaNameS, tableNameS)
//	if err != nil {
//		return nil, err
//	}
//	if len(pkSlis) == 1 {
//		var pkColumns []string
//		pkArrs := stringutil.StringSplit(pkSlis[0]["COLUMN_LIST"], ",")
//		for _, s := range pkArrs {
//			brackets := stringutil.StringExtractorWithinBrackets(s)
//			if len(brackets) == 0 {
//				if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[s]) {
//					break
//				} else {
//					pkColumns = append(pkColumns, s)
//				}
//			} else {
//				marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
//				for _, m := range marks {
//					if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[m]) {
//						break
//					} else {
//						pkColumns = append(pkColumns, m)
//					}
//				}
//			}
//		}
//		if len(pkColumns) == len(stringutil.StringExtractorWithoutQuotationMarks(pkSlis[0]["COLUMN_LIST"], columnNameSli...)) {
//			return pkColumns, nil
//		}
//	} else if len(pkSlis) > 1 {
//		return nil, fmt.Errorf("schema table primary key aren't exist mutiple")
//	}
//
//	ukSlis, err := d.GetDatabaseTableUniqueKey(schemaNameS, tableNameS)
//	if err != nil {
//		return nil, err
//	}
//	if len(ukSlis) > 0 {
//		for _, uk := range ukSlis {
//			var ukColumns []string
//			ukArrs := stringutil.StringSplit(uk["COLUMN_LIST"], ",")
//			for _, s := range ukArrs {
//				brackets := stringutil.StringExtractorWithinBrackets(s)
//				if len(brackets) == 0 {
//					if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[s]) {
//						break
//					} else {
//						ukColumns = append(ukColumns, s)
//					}
//				} else {
//					marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
//					for _, m := range marks {
//						if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[m]) {
//							break
//						} else {
//							ukColumns = append(ukColumns, m)
//						}
//					}
//				}
//			}
//			if len(ukColumns) == len(stringutil.StringExtractorWithoutQuotationMarks(uk["COLUMN_LIST"], columnNameSli...)) {
//				return ukColumns, nil
//			}
//		}
//	}
//
//	uiSlis, err := d.GetDatabaseTableUniqueIndex(schemaNameS, tableNameS)
//	if err != nil {
//		return nil, err
//	}
//	if len(uiSlis) > 0 {
//		for _, ui := range uiSlis {
//			var uiColumns []string
//			uiArrs := stringutil.StringSplit(ui["COLUMN_LIST"], "|+|")
//			for _, s := range uiArrs {
//				brackets := stringutil.StringExtractorWithinBrackets(s)
//				if len(brackets) == 0 {
//					if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[s]) {
//						break
//					} else {
//						uiColumns = append(uiColumns, s)
//					}
//				} else {
//					marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
//					for _, m := range marks {
//						if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[m]) {
//							break
//						} else {
//							uiColumns = append(uiColumns, m)
//						}
//					}
//				}
//			}
//			if len(uiColumns) == len(stringutil.StringExtractorWithoutQuotationMarks(ui["COLUMN_LIST"], columnNameSli...)) {
//				return uiColumns, nil
//			}
//		}
//	}
//
//	niSlis, err := d.GetDatabaseTableNormalIndex(schemaNameS, tableNameS)
//	if err != nil {
//		return nil, err
//	}
//	if len(niSlis) > 0 {
//		for _, ni := range niSlis {
//			var niColumns []string
//			niArrs := stringutil.StringSplit(ni["COLUMN_LIST"], "|+|")
//			for _, s := range niArrs {
//				brackets := stringutil.StringExtractorWithinBrackets(s)
//				if len(brackets) == 0 {
//					if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[s]) {
//						break
//					} else {
//						niColumns = append(niColumns, s)
//					}
//				} else {
//					marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
//					for _, m := range marks {
//						if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[m]) {
//							break
//						} else {
//							niColumns = append(niColumns, m)
//						}
//					}
//				}
//			}
//			if len(niColumns) == len(stringutil.StringExtractorWithoutQuotationMarks(ni["COLUMN_LIST"], columnNameSli...)) {
//				return niColumns, nil
//			}
//		}
//	}
//	return nil, nil
//}
