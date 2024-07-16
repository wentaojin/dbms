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
	"github.com/wentaojin/dbms/utils/structure"
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

func (d *Database) GetDatabaseTableStatisticsBucket(schemeNameS, tableNameS string) (map[string][]structure.Bucket, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) GetDatabaseTableStatisticsHistogram(schemeNameS, tableNameS string) (map[string][]structure.Histogram, error) {
	//TODO implement me
	panic("implement me")
}

func (d *Database) FindDatabaseTableBestColumn(schemaNameS, tableNameS, columnNameS string) ([]string, error) {
	var err error

	//sqlStr := fmt.Sprintf(`SELECT
	//	OWNER,
	//	TABLE_NAME,
	//	COLUMN_NAME,
	//	DATA_TYPE,
	//	NUM_DISTINCT,
	//	CASE
	//		-- IF IT IS TIMESTAMP WITH TIMEZONE, CONVERT FROM UTC TO LOCAL TIME
	//		WHEN DATA_TYPE LIKE 'TIMESTAMP%%WITH TIME ZONE' THEN
	//			TO_CHAR(
	//			FROM_TZ(
	//				TO_TIMESTAMP(LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(LOW_VALUE, 1, 2), 'XX')
	//+ TO_NUMBER(SUBSTR(LOW_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
	//|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
	//|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
	//				, 'UTC' ) AT LOCAL
	//			, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
	//		WHEN DATA_TYPE LIKE 'TIMESTAMP%%' THEN
	//			TO_CHAR(
	//			TO_TIMESTAMP( LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(LOW_VALUE, 1, 2), 'XX')
	//+ TO_NUMBER(SUBSTR(LOW_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
	//|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
	//|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
	//|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
	//			, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
	//		WHEN DATA_TYPE = 'NUMBER' THEN
	//			TO_CHAR(UTL_RAW.CAST_TO_NUMBER(LOW_VALUE))
	//		WHEN DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN
	//			TO_CHAR(UTL_RAW.CAST_TO_VARCHAR2(LOW_VALUE))
	//		WHEN DATA_TYPE = 'NVARCHAR2' THEN
	//			TO_CHAR(UTL_RAW.CAST_TO_NVARCHAR2(LOW_VALUE))
	//		WHEN DATA_TYPE = 'BINARY_DOUBLE' THEN
	//			TO_CHAR(UTL_RAW.CAST_TO_BINARY_DOUBLE(LOW_VALUE))
	//		WHEN DATA_TYPE = 'BINARY_FLOAT' THEN
	//			TO_CHAR(UTL_RAW.CAST_TO_BINARY_FLOAT(LOW_VALUE))
	//		WHEN DATA_TYPE = 'DATE' THEN
	//			TO_CHAR(DBMS_STATS.CONVERT_RAW_TO_DATE(LOW_VALUE), 'YYYY-MM-DD HH24:MI:SS')
	//		ELSE
	//			'UNKNOWN DATA_TYPE'
	//	END LOW_VALUE,
	//	CASE
	//		-- IF IT IS TIMESTAMP WITH TIMEZONE, CONVERT FROM UTC TO LOCAL TIME
	//		WHEN DATA_TYPE LIKE 'TIMESTAMP%%WITH TIME ZONE' THEN
	//			TO_CHAR(
	//			FROM_TZ(
	//				TO_TIMESTAMP(LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(HIGH_VALUE, 1, 2), 'XX')
	//+ TO_NUMBER(SUBSTR(HIGH_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
	//|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
	//|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
	//				, 'UTC' ) AT LOCAL
	//			, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
	//		WHEN DATA_TYPE LIKE 'TIMESTAMP%%' THEN
	//			TO_CHAR(
	//			TO_TIMESTAMP( LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(HIGH_VALUE, 1, 2), 'XX')
	//+ TO_NUMBER(SUBSTR(HIGH_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
	//|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
	//|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
	//|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
	//			, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
	//		WHEN DATA_TYPE = 'NUMBER' THEN
	//			TO_CHAR(UTL_RAW.CAST_TO_NUMBER(HIGH_VALUE))
	//		WHEN DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN
	//			TO_CHAR(UTL_RAW.CAST_TO_VARCHAR2(HIGH_VALUE))
	//		WHEN DATA_TYPE = 'NVARCHAR2' THEN
	//			TO_CHAR(UTL_RAW.CAST_TO_NVARCHAR2(HIGH_VALUE))
	//		WHEN DATA_TYPE = 'BINARY_DOUBLE' THEN
	//			TO_CHAR(UTL_RAW.CAST_TO_BINARY_DOUBLE(HIGH_VALUE))
	//		WHEN DATA_TYPE = 'BINARY_FLOAT' THEN
	//			TO_CHAR(UTL_RAW.CAST_TO_BINARY_FLOAT(HIGH_VALUE))
	//		WHEN DATA_TYPE = 'DATE' THEN
	//			TO_CHAR(DBMS_STATS.CONVERT_RAW_TO_DATE(HIGH_VALUE), 'YYYY-MM-DD HH24:MI:SS')
	//		ELSE
	//			'UNKNOWN DATA_TYPE'
	//	END HIGH_VALUE
	//FROM
	//	DBA_TAB_COLUMNS
	//WHERE
	//	OWNER = '%s'
	//	AND TABLE_NAME = '%s'`, schemaNameS, tableNameS)

	sqlStr := fmt.Sprintf(`SELECT
		OWNER,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE 
	FROM
		DBA_TAB_COLUMNS
	WHERE
		OWNER = '%s' 
		AND TABLE_NAME = '%s'`, schemaNameS, tableNameS)
	_, results, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}

	var columnNameSli []string
	columnDatatypeMap := make(map[string]string)
	for _, res := range results {
		columnDatatypeMap[res["COLUMN_NAME"]] = res["DATA_TYPE"]
		columnNameSli = append(columnNameSli, res["COLUMN_NAME"])
	}

	if !strings.EqualFold(columnNameS, "") && d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[columnNameS]) {
		return []string{columnNameS}, nil
	}

	pkSlis, err := d.GetDatabaseTablePrimaryKey(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	if len(pkSlis) == 1 {
		var pkColumns []string
		pkArrs := stringutil.StringSplit(pkSlis[0]["COLUMN_LIST"], ",")
		for _, s := range pkArrs {
			brackets := stringutil.StringExtractorWithinBrackets(s)
			if len(brackets) == 0 {
				if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[s]) {
					break
				} else {
					pkColumns = append(pkColumns, s)
				}
			} else {
				marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
				for _, m := range marks {
					if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[m]) {
						break
					} else {
						pkColumns = append(pkColumns, m)
					}
				}
			}
		}
		if len(pkColumns) == len(stringutil.StringExtractorWithoutQuotationMarks(pkSlis[0]["COLUMN_LIST"], columnNameSli...)) {
			return pkColumns, nil
		}
	} else if len(pkSlis) > 1 {
		return nil, fmt.Errorf("schema table primary key aren't exist mutiple")
	}

	ukSlis, err := d.GetDatabaseTableUniqueKey(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	if len(ukSlis) > 0 {
		for _, uk := range ukSlis {
			var ukColumns []string
			ukArrs := stringutil.StringSplit(uk["COLUMN_LIST"], ",")
			for _, s := range ukArrs {
				brackets := stringutil.StringExtractorWithinBrackets(s)
				if len(brackets) == 0 {
					if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[s]) {
						break
					} else {
						ukColumns = append(ukColumns, s)
					}
				} else {
					marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
					for _, m := range marks {
						if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[m]) {
							break
						} else {
							ukColumns = append(ukColumns, m)
						}
					}
				}
			}
			if len(ukColumns) == len(stringutil.StringExtractorWithoutQuotationMarks(uk["COLUMN_LIST"], columnNameSli...)) {
				return ukColumns, nil
			}
		}
	}

	uiSlis, err := d.GetDatabaseTableUniqueIndex(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	if len(uiSlis) > 0 {
		for _, ui := range uiSlis {
			var uiColumns []string
			uiArrs := stringutil.StringSplit(ui["COLUMN_LIST"], "|+|")
			for _, s := range uiArrs {
				brackets := stringutil.StringExtractorWithinBrackets(s)
				if len(brackets) == 0 {
					if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[s]) {
						break
					} else {
						uiColumns = append(uiColumns, s)
					}
				} else {
					marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
					for _, m := range marks {
						if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[m]) {
							break
						} else {
							uiColumns = append(uiColumns, m)
						}
					}
				}
			}
			if len(uiColumns) == len(stringutil.StringExtractorWithoutQuotationMarks(ui["COLUMN_LIST"], columnNameSli...)) {
				return uiColumns, nil
			}
		}
	}

	niSlis, err := d.GetDatabaseTableNormalIndex(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	if len(niSlis) > 0 {
		for _, ni := range niSlis {
			var niColumns []string
			niArrs := stringutil.StringSplit(ni["COLUMN_LIST"], "|+|")
			for _, s := range niArrs {
				brackets := stringutil.StringExtractorWithinBrackets(s)
				if len(brackets) == 0 {
					if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[s]) {
						break
					} else {
						niColumns = append(niColumns, s)
					}
				} else {
					marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
					for _, m := range marks {
						if !d.FilterDatabaseTableColumnDatatype(columnDatatypeMap[m]) {
							break
						} else {
							niColumns = append(niColumns, m)
						}
					}
				}
			}
			if len(niColumns) == len(stringutil.StringExtractorWithoutQuotationMarks(ni["COLUMN_LIST"], columnNameSli...)) {
				return niColumns, nil
			}
		}
	}
	return nil, nil
}

func (d *Database) FilterDatabaseTableColumnDatatype(columnType string) bool {
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

func (d *Database) GetDatabaseTableColumnProperties(schemaNameS, tableNameS, columnNameS string, collationS bool) ([]map[string]string, error) {
	var sqlStr string

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
	AND T.COLUMN_NAME = '%s'
ORDER BY
	T.COLUMN_ID`, schemaNameS, tableNameS, columnNameS)
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
	AND T.COLUMN_NAME = '%s'
ORDER BY
	T.COLUMN_ID`, schemaNameS, tableNameS, columnNameS)
	}
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableColumnBucket(schemaNameS, tableNameS string, columnNameS, datatypeS string) ([]string, error) {
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
ORDER BY ENDPOINT_VALUE ASC`, schemaNameS, tableNameS, columnNameS)
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
ORDER BY ENDPOINT_VALUE ASC`, schemaNameS, tableNameS, columnNameS)
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
ORDER BY ENDPOINT_VALUE ASC`, schemaNameS, tableNameS, columnNameS)
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

func (d *Database) GetDatabaseTableCompareData(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string) ([]string, uint32, map[string]int64, error) {
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
					return nil, crc32Sum, nil, fmt.Errorf("column [%s] unsupported type: %T", colName, value)
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
