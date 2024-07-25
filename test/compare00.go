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
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/labstack/gommon/log"
	"go.uber.org/zap"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func main() {
	ctx := context.Background()

	databaseS, err := database.NewDatabase(ctx, &datasource.Datasource{
		DbType:         "oracle",
		Username:       "finpt",
		Password:       "findt",
		Host:           "10.21.13.33",
		Port:           1521,
		ConnectCharset: "zhs16gbk",
		ServiceName:    "gbk",
	}, "MARVIN", 600)
	if err != nil {
		panic(err)
	}

	schemaNameS := "MARVIN"
	tableNameS := "MARVIN05"

	columns, err := FindDatabaseTableColumnName(schemaNameS, tableNameS, databaseS)
	if err != nil {
		panic(err)
	}
	if len(columns) == 0 {
		fmt.Println("null")
	}

	bucketRanges, err := GetDatabaseTableColumnBucketRange(schemaNameS, tableNameS, databaseS, columns)
	if err != nil {
		panic(err)
	}

	if len(bucketRanges) == 0 {
		fmt.Println("null")
	}

	var recs int64
	recs = 0
	for _, br := range bucketRanges {
		sqlStr := fmt.Sprintf(`SELECT COUNT(*) AS COUNT FROM "%s"."%s" T WHERE %v`, schemaNameS, tableNameS, br.toStringS("ORACLE"))
		fmt.Printf("source condition: %v\n", sqlStr)

		_, results, errM := databaseS.GeneralQuery(sqlStr)
		if errM != nil {
			panic(errM)
		}
		fmt.Println(results)

		size, err := stringutil.StrconvIntBitSize(results[0]["COUNT"], 64)
		if err != nil {
			panic(err)
		}
		recs = recs + size
		fmt.Printf("结果：%d\n", recs)
		//ts, tt := br.toStringT(constant.TaskFlowOracleToTiDB, "zhs16GBK", "BINARY")
		//fmt.Printf("target condition: %v, args: %v\n", ts, tt)
	}

}

func GetDatabaseTableColumnBucketRange(schemaNameS, tableNameS string, databaseS database.IDatabase, columnNameS []string) ([]*Range, error) {
	var (
		chunks         []*Range
		randomValues   [][]string
		newColumnNameS []string
	)

	for i, column := range columnNameS {
		columnBuckets, err := GetDatabaseTableColumnBucket(schemaNameS, tableNameS, column, databaseS)
		if err != nil {
			return nil, err
		}
		// first elems
		if i == 0 && len(columnBuckets) == 0 {
			return chunks, nil
		} else if i > 0 && len(columnBuckets) == 0 {
			continue
		} else {
			randomValues = append(randomValues, columnBuckets)
			newColumnNameS = append(newColumnNameS, column)
		}
	}

	randomValues, randomValuesLen := SlicesLengthAlign(randomValues)

	for i := 0; i <= randomValuesLen; i++ {
		newChunk := NewChunkRange()

		for j, column := range newColumnNameS {
			if i == 0 {
				if len(randomValues[j]) == 0 {
					break
				}
				newChunk.update(column, "", randomValues[j][i], false, true)
			} else if i == len(randomValues[0]) {
				newChunk.update(column, randomValues[j][i-1], "", true, false)
			} else {
				newChunk.update(column, randomValues[j][i-1], randomValues[j][i], true, true)
			}
		}
		chunks = append(chunks, newChunk)
	}

	return chunks, nil
}

func GetDatabaseTableColumnBucket(schemaNameS, tableNameS string, columnNameS string, databaseS database.IDatabase) ([]string, error) {
	var vals []string
	/*
	 * 统计信息查询
	 * ONLY SUPPORT NUMBER 以及 NUMBER 子数据类型、DATE、TIMESTAMP 以及 Subtypes 、VARCHAR 字符数据类型
	 * 1, 频率直方图、TOP 频率直方图、混合直方图 endpoint_numbers 代表当前和之前桶中包含所有值的累计频率，endpoint_value/endpoint_actual_value 代表当前数据（数据类型取决于用哪个字段）
	 * 2, 等高直方图 endpoint_numbers 代表桶号，endpoint_value 代表桶中值范围内的最大值
	 */
	sqlStr := fmt.Sprintf(`SELECT
		OWNER,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE,
		DATA_SCALE
	FROM
		DBA_TAB_COLUMNS
	WHERE
		OWNER = '%s' 
		AND TABLE_NAME = '%s'
		AND COLUMN_NAME = '%s'`, schemaNameS, tableNameS, columnNameS)
	_, res, err := databaseS.GeneralQuery(sqlStr)
	if err != nil {
		panic(err)
	}

	datatypeS := res[0]["DATA_TYPE"]
	dataScaleS := res[0]["DATA_SCALE"]

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
	_, res, err = databaseS.GeneralQuery(sqlStr)
	if err != nil {
		panic(err)
	}

	for _, r := range res {
		if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportNumberSubtypes, datatypeS) {
			vals = append(vals, r["ENDPOINT_VALUE"])
		}
		if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportVarcharSubtypes, datatypeS) {
			vals = append(vals, stringutil.StringBuilder(`'`, r["ENDPOINT_VALUE"], `'`))
		}
		if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportDateSubtypes, datatypeS) {
			vals = append(vals, stringutil.StringBuilder(`TO_DATE('`, r["ENDPOINT_VALUE"], `','YYYY-MM-DD HH24:MI:SS')`))
		}
		if stringutil.IsContainedString(constant.DataCompareOracleDatabaseSupportTimestampSubtypes, datatypeS) {
			vals = append(vals, stringutil.StringBuilder(`TO_TIMESTAMP('`, r["ENDPOINT_VALUE"], `','YYYY-MM-DD HH24:MI:SS.FF`, dataScaleS, `')`))
		}
	}
	return vals, nil
}

func FindDatabaseTableColumnName(schemaNameS, tableNameS string, databaseS database.IDatabase) ([]string, error) {
	var err error

	sqlStr := fmt.Sprintf(`SELECT
		OWNER,
		TABLE_NAME,
		COLUMN_NAME,
		DATA_TYPE,
		NUM_DISTINCT,
		CASE
			-- IF IT IS TIMESTAMP WITH TIMEZONE, CONVERT FROM UTC TO LOCAL TIME
			WHEN DATA_TYPE LIKE 'TIMESTAMP%%WITH TIME ZONE' THEN 
				TO_CHAR(
				FROM_TZ(
					TO_TIMESTAMP(LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(LOW_VALUE, 1, 2), 'XX')
	+ TO_NUMBER(SUBSTR(LOW_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
	|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
	|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
	|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
	|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
	|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
	|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
					, 'UTC' ) AT LOCAL
				, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
			WHEN DATA_TYPE LIKE 'TIMESTAMP%%' THEN
				TO_CHAR(
				TO_TIMESTAMP( LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(LOW_VALUE, 1, 2), 'XX')
	+ TO_NUMBER(SUBSTR(LOW_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
	|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
	|| LPAD( TO_NUMBER(SUBSTR(LOW_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
	|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
	|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
	|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
	|| LPAD((TO_NUMBER(SUBSTR(LOW_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
				, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
			WHEN DATA_TYPE = 'NUMBER' THEN
				TO_CHAR(UTL_RAW.CAST_TO_NUMBER(LOW_VALUE))
			WHEN DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN
				TO_CHAR(UTL_RAW.CAST_TO_VARCHAR2(LOW_VALUE))
			WHEN DATA_TYPE = 'NVARCHAR2' THEN
				TO_CHAR(UTL_RAW.CAST_TO_NVARCHAR2(LOW_VALUE))
			WHEN DATA_TYPE = 'BINARY_DOUBLE' THEN
				TO_CHAR(UTL_RAW.CAST_TO_BINARY_DOUBLE(LOW_VALUE))
			WHEN DATA_TYPE = 'BINARY_FLOAT' THEN
				TO_CHAR(UTL_RAW.CAST_TO_BINARY_FLOAT(LOW_VALUE))
			WHEN DATA_TYPE = 'DATE' THEN
				TO_CHAR(DBMS_STATS.CONVERT_RAW_TO_DATE(LOW_VALUE), 'YYYY-MM-DD HH24:MI:SS')
			ELSE
				'UNKNOWN DATA_TYPE'
		END LOW_VALUE,
		CASE
			-- IF IT IS TIMESTAMP WITH TIMEZONE, CONVERT FROM UTC TO LOCAL TIME
			WHEN DATA_TYPE LIKE 'TIMESTAMP%%WITH TIME ZONE' THEN 
				TO_CHAR(
				FROM_TZ(
					TO_TIMESTAMP(LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(HIGH_VALUE, 1, 2), 'XX')
	+ TO_NUMBER(SUBSTR(HIGH_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
	|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
	|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
	|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
	|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
	|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
	|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
					, 'UTC' ) AT LOCAL
				, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
			WHEN DATA_TYPE LIKE 'TIMESTAMP%%' THEN
				TO_CHAR(
				TO_TIMESTAMP( LPAD(TO_CHAR(100 * TO_NUMBER(SUBSTR(HIGH_VALUE, 1, 2), 'XX')
	+ TO_NUMBER(SUBSTR(HIGH_VALUE, 3, 2), 'XX')) - 10100, 4, '0') || '-'
	|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 5, 2), 'XX'), 2, '0')|| '-'
	|| LPAD( TO_NUMBER(SUBSTR(HIGH_VALUE, 7, 2), 'XX'), 2, '0')|| ' '
	|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 9, 2), 'XX')-1), 2, '0')|| ':'
	|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 11, 2), 'XX')-1), 2, '0')|| ':'
	|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 13, 2), 'XX')-1), 2, '0')|| '.'
	|| LPAD((TO_NUMBER(SUBSTR(HIGH_VALUE, 15, 8), 'XXXXXXXX')-1), 8, '0'), 'YYYY-MM-DD HH24:MI:SS.FF9')
				, 'YYYY-MM-DD HH24:MI:SS.FF'||DATA_SCALE)
			WHEN DATA_TYPE = 'NUMBER' THEN
				TO_CHAR(UTL_RAW.CAST_TO_NUMBER(HIGH_VALUE))
			WHEN DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN
				TO_CHAR(UTL_RAW.CAST_TO_VARCHAR2(HIGH_VALUE))
			WHEN DATA_TYPE = 'NVARCHAR2' THEN
				TO_CHAR(UTL_RAW.CAST_TO_NVARCHAR2(HIGH_VALUE))
			WHEN DATA_TYPE = 'BINARY_DOUBLE' THEN
				TO_CHAR(UTL_RAW.CAST_TO_BINARY_DOUBLE(HIGH_VALUE))
			WHEN DATA_TYPE = 'BINARY_FLOAT' THEN
				TO_CHAR(UTL_RAW.CAST_TO_BINARY_FLOAT(HIGH_VALUE))
			WHEN DATA_TYPE = 'DATE' THEN
				TO_CHAR(DBMS_STATS.CONVERT_RAW_TO_DATE(HIGH_VALUE), 'YYYY-MM-DD HH24:MI:SS')
			ELSE
				'UNKNOWN DATA_TYPE'
		END HIGH_VALUE
	FROM
		DBA_TAB_COLUMNS
	WHERE
		OWNER = '%s' 
		AND TABLE_NAME = '%s'`, schemaNameS, tableNameS)
	_, results, err := databaseS.GeneralQuery(sqlStr)
	if err != nil {
		panic(err)
	}

	var columnNameSli []string
	columnDatatypeMap := make(map[string]string)
	for _, res := range results {
		columnDatatypeMap[res["COLUMN_NAME"]] = res["DATA_TYPE"]
		columnNameSli = append(columnNameSli, res["COLUMN_NAME"])
	}

	pkSlis, err := databaseS.GetDatabaseTablePrimaryKey(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	if len(pkSlis) == 1 {
		var pkColumns []string
		pkArrs := stringutil.StringSplit(pkSlis[0]["COLUMN_LIST"], constant.StringSeparatorComplexSymbol)
		for _, s := range pkArrs {
			brackets := stringutil.StringExtractorWithinBrackets(s)
			if len(brackets) == 0 {
				if !FilterDatabaseTableColumnCompareDatatype(columnDatatypeMap[stringutil.RemovePrefixSuffixOnce(s, "\"", "\"")]) {
					break
				} else {
					pkColumns = append(pkColumns, stringutil.RemovePrefixSuffixOnce(s, "\"", "\""))
				}
			} else {
				marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
				for _, m := range marks {
					if !FilterDatabaseTableColumnCompareDatatype(columnDatatypeMap[stringutil.RemovePrefixSuffixOnce(m, "\"", "\"")]) {
						break
					} else {
						pkColumns = append(pkColumns, stringutil.RemovePrefixSuffixOnce(m, "\"", "\""))
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

	ukSlis, err := databaseS.GetDatabaseTableUniqueKey(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	if len(ukSlis) > 0 {
		for _, uk := range ukSlis {
			var ukColumns []string
			ukArrs := stringutil.StringSplit(uk["COLUMN_LIST"], constant.StringSeparatorComplexSymbol)
			for _, s := range ukArrs {
				brackets := stringutil.StringExtractorWithinBrackets(s)
				if len(brackets) == 0 {
					if !FilterDatabaseTableColumnCompareDatatype(columnDatatypeMap[stringutil.RemovePrefixSuffixOnce(s, "\"", "\"")]) {
						break
					} else {
						ukColumns = append(ukColumns, stringutil.RemovePrefixSuffixOnce(s, "\"", "\""))
					}
				} else {
					marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
					for _, m := range marks {
						if !FilterDatabaseTableColumnCompareDatatype(columnDatatypeMap[stringutil.RemovePrefixSuffixOnce(m, "\"", "\"")]) {
							break
						} else {
							ukColumns = append(ukColumns, stringutil.RemovePrefixSuffixOnce(m, "\"", "\""))
						}
					}
				}
			}
			if len(ukColumns) == len(stringutil.StringExtractorWithoutQuotationMarks(uk["COLUMN_LIST"], columnNameSli...)) {
				return ukColumns, nil
			}
		}
	}

	uiSlis, err := databaseS.GetDatabaseTableUniqueIndex(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	if len(uiSlis) > 0 {
		for _, ui := range uiSlis {
			var uiColumns []string
			uiArrs := stringutil.StringSplit(ui["COLUMN_LIST"], constant.StringSeparatorComplexSymbol)
			for _, s := range uiArrs {
				brackets := stringutil.StringExtractorWithinBrackets(s)
				if len(brackets) == 0 {
					if !FilterDatabaseTableColumnCompareDatatype(columnDatatypeMap[stringutil.RemovePrefixSuffixOnce(s, "\"", "\"")]) {
						break
					} else {
						uiColumns = append(uiColumns, stringutil.RemovePrefixSuffixOnce(s, "\"", "\""))
					}
				} else {
					marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
					for _, m := range marks {
						if !FilterDatabaseTableColumnCompareDatatype(columnDatatypeMap[stringutil.RemovePrefixSuffixOnce(m, "\"", "\"")]) {
							break
						} else {
							uiColumns = append(uiColumns, stringutil.RemovePrefixSuffixOnce(m, "\"", "\""))
						}
					}
				}
			}
			if len(uiColumns) == len(stringutil.StringExtractorWithoutQuotationMarks(ui["COLUMN_LIST"], columnNameSli...)) {
				return uiColumns, nil
			}
		}
	}

	niSlis, err := databaseS.GetDatabaseTableNormalIndex(schemaNameS, tableNameS)
	if err != nil {
		return nil, err
	}
	if len(niSlis) > 0 {
		for _, ni := range niSlis {
			var niColumns []string
			niArrs := stringutil.StringSplit(ni["COLUMN_LIST"], constant.StringSeparatorComplexSymbol)
			for _, s := range niArrs {
				brackets := stringutil.StringExtractorWithinBrackets(s)
				if len(brackets) == 0 {
					if !FilterDatabaseTableColumnCompareDatatype(columnDatatypeMap[stringutil.RemovePrefixSuffixOnce(s, "\"", "\"")]) {
						break
					} else {
						niColumns = append(niColumns, stringutil.RemovePrefixSuffixOnce(s, "\"", "\""))
					}
				} else {
					marks := stringutil.StringExtractorWithinQuotationMarks(s, columnNameSli...)
					for _, m := range marks {
						if !FilterDatabaseTableColumnCompareDatatype(columnDatatypeMap[stringutil.RemovePrefixSuffixOnce(m, "\"", "\"")]) {
							break
						} else {
							niColumns = append(niColumns, stringutil.RemovePrefixSuffixOnce(m, "\"", "\""))
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

func FilterDatabaseTableColumnCompareDatatype(columnType string) bool {
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

func SlicesLengthAlign(slices [][]string) ([][]string, int) {
	minL := 0
	for i, slice := range slices {
		if i == 0 || len(slice) < minL {
			minL = len(slice)
		}
	}

	// align
	alignSlices := make([][]string, len(slices))
	for i, slice := range slices {
		alignSlices[i] = slice[:minL]
	}
	return alignSlices, minL
}

// Bound represents a bound for a column
type Bound struct {
	Column string `json:"column"`
	Lower  string `json:"lower"`
	Upper  string `json:"upper"`

	HasLower bool `json:"has-lower"`
	HasUpper bool `json:"has-upper"`
}

// Range represents chunk range
type Range struct {
	Bounds      []*Bound       `json:"bounds"`
	BoundOffset map[string]int `json:"boundOffset"`
}

// NewChunkRange return a Range.
func NewChunkRange() *Range {
	return &Range{
		Bounds:      make([]*Bound, 0, 2),
		BoundOffset: make(map[string]int),
	}
}

// String returns the string of Range, used for log.
func (c *Range) String() string {
	chunkBytes, err := json.Marshal(c)
	if err != nil {
		log.Warn("fail to encode chunk into string", zap.Error(err))
		return ""
	}

	return string(chunkBytes)
}

func (c *Range) toStringS(dbTypeS string) string {
	/* for example:
	there is a bucket, the lowerbound and upperbound are (v1, v3), (v2, v4), and the columns are `a` and `b`,
	this bucket's data range is (a > v1 or (a == v1 and b > v3)) and (a < v2 or (a == v2 and b <= v4))
	*/

	lowerConditionS := make([]string, 0, 1)
	upperConditionS := make([]string, 0, 1)

	preConditionForLowerS := make([]string, 0, 1)
	preConditionForUpperS := make([]string, 0, 1)
	preConditionArgsForLowerS := make([]string, 0, 1)
	preConditionArgsForUpperS := make([]string, 0, 1)

	for i, bound := range c.Bounds {
		lowerSymbol := constant.DataCompareSymbolGt
		upperSymbol := constant.DataCompareSymbolLt
		if i == len(c.Bounds)-1 {
			upperSymbol = constant.DataCompareSymbolLte
		}

		if bound.HasLower {
			if strings.EqualFold(dbTypeS, constant.DatabaseTypeOracle) {
				if len(preConditionForLowerS) > 0 {
					lowerConditionS = append(lowerConditionS, fmt.Sprintf("(%s AND %s %s %s)", strings.Join(preConditionForLowerS, " AND "), stringutil.StringBuilder("\"", bound.Column, "\""), lowerSymbol, bound.Lower))
				} else {
					lowerConditionS = append(lowerConditionS, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("\"", bound.Column, "\""), lowerSymbol, bound.Lower))
				}
				preConditionForLowerS = append(preConditionForLowerS, fmt.Sprintf("%s = %s", stringutil.StringBuilder("\"", bound.Column, "\""), bound.Lower))
				preConditionArgsForLowerS = append(preConditionArgsForLowerS, bound.Lower)
			}

			if strings.EqualFold(dbTypeS, constant.DatabaseTypeMySQL) || strings.EqualFold(dbTypeS, constant.DatabaseTypeTiDB) {
				if len(preConditionForLowerS) > 0 {
					lowerConditionS = append(lowerConditionS, fmt.Sprintf("(%s AND %s %s %s)", strings.Join(preConditionForLowerS, " AND "), stringutil.StringBuilder("`", bound.Column, "`"), lowerSymbol, bound.Lower))
				} else {
					lowerConditionS = append(lowerConditionS, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("`", bound.Column, "`"), lowerSymbol, bound.Lower))
				}
				preConditionForLowerS = append(preConditionForLowerS, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.Column, "`"), bound.Lower))
				preConditionArgsForLowerS = append(preConditionArgsForLowerS, bound.Lower)
			}
		}

		if bound.HasUpper {
			if strings.EqualFold(dbTypeS, constant.DatabaseTypeOracle) {
				if len(preConditionForUpperS) > 0 {
					upperConditionS = append(upperConditionS, fmt.Sprintf("(%s AND %s %s %s)", strings.Join(preConditionForUpperS, " AND "), stringutil.StringBuilder("\"", bound.Column, "\""), upperSymbol, bound.Upper))
				} else {
					upperConditionS = append(upperConditionS, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("\"", bound.Column, "\""), upperSymbol, bound.Upper))
				}
				preConditionForUpperS = append(preConditionForUpperS, fmt.Sprintf("%s = %s", stringutil.StringBuilder("\"", bound.Column, "\""), bound.Upper))
				preConditionArgsForUpperS = append(preConditionArgsForUpperS, bound.Upper)
			}

			if strings.EqualFold(dbTypeS, constant.DatabaseTypeMySQL) || strings.EqualFold(dbTypeS, constant.DatabaseTypeTiDB) {
				if len(preConditionForUpperS) > 0 {
					upperConditionS = append(upperConditionS, fmt.Sprintf("(%s AND %s %s %s)", strings.Join(preConditionForUpperS, " AND "), stringutil.StringBuilder("`", bound.Column, "`"), upperSymbol, bound.Upper))
				} else {
					upperConditionS = append(upperConditionS, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("`", bound.Column, "`"), upperSymbol, bound.Upper))
				}
				preConditionForUpperS = append(preConditionForUpperS, fmt.Sprintf("%s = ?", stringutil.StringBuilder("`", bound.Column, "`")))
				preConditionArgsForUpperS = append(preConditionArgsForUpperS, bound.Upper)
			}

		}
	}

	if len(upperConditionS) == 0 && len(lowerConditionS) == 0 {
		return "TRUE AND TRUE"
	}

	if len(upperConditionS) == 0 {
		return stringutil.StringJoin(lowerConditionS, " OR ")
	}

	if len(lowerConditionS) == 0 {
		return strings.Join(upperConditionS, " OR ")
	}

	return fmt.Sprintf("(%s) AND (%s)", strings.Join(lowerConditionS, " OR "), strings.Join(upperConditionS, " OR "))
}

func (c *Range) toStringT(taskFlow, dbCharsetS, collationS string) string {
	var collationT string
	if collationS != "" {
		switch {
		case strings.EqualFold(taskFlow, constant.TaskFlowOracleToTiDB):
			collationT = fmt.Sprintf(" COLLATE '%s'", constant.MigrateTableStructureDatabaseCollationMap[constant.TaskFlowOracleToTiDB][stringutil.StringUpper(collationS)][constant.MigrateTableStructureDatabaseCharsetMap[constant.TaskFlowOracleToTiDB][stringutil.StringUpper(dbCharsetS)]])
		case strings.EqualFold(taskFlow, constant.TaskFlowOracleToMySQL):
			collationT = fmt.Sprintf(" COLLATE '%s'", constant.MigrateTableStructureDatabaseCollationMap[constant.TaskFlowOracleToMySQL][stringutil.StringUpper(collationS)][constant.MigrateTableStructureDatabaseCharsetMap[constant.TaskFlowOracleToMySQL][stringutil.StringUpper(dbCharsetS)]])

		case strings.EqualFold(taskFlow, constant.TaskFlowTiDBToOracle):
			collationT = fmt.Sprintf(" 'NLS_SORT = %s'", constant.MigrateTableStructureDatabaseCollationMap[constant.TaskFlowTiDBToOracle][stringutil.StringUpper(collationS)][constant.MigrateTableStructureDatabaseCharsetMap[constant.TaskFlowTiDBToOracle][stringutil.StringUpper(dbCharsetS)]])
		case strings.EqualFold(taskFlow, constant.TaskFlowMySQLToOracle):
			collationT = fmt.Sprintf(" 'NLS_SORT = %s'", constant.MigrateTableStructureDatabaseCollationMap[constant.TaskFlowMySQLToOracle][stringutil.StringUpper(collationS)][constant.MigrateTableStructureDatabaseCharsetMap[constant.TaskFlowMySQLToOracle][stringutil.StringUpper(dbCharsetS)]])

		}
	}

	/* for example:
	there is a bucket, the lowerbound and upperbound are (v1, v3), (v2, v4), and the columns are `a` and `b`,
	this bucket's data range is (a > v1 or (a == v1 and b > v3)) and (a < v2 or (a == v2 and b <= v4))
	*/
	dbTypeSli := stringutil.StringSplit(taskFlow, constant.StringSeparatorAite)
	dbTypeT := dbTypeSli[1]

	lowerConditionT := make([]string, 0, 1)
	upperConditionT := make([]string, 0, 1)
	preConditionForLowerT := make([]string, 0, 1)
	preConditionForUpperT := make([]string, 0, 1)
	preConditionArgsForLowerT := make([]string, 0, 1)
	preConditionArgsForUpperT := make([]string, 0, 1)

	for i, bound := range c.Bounds {
		lowerSymbol := constant.DataCompareSymbolGt
		upperSymbol := constant.DataCompareSymbolLt
		if i == len(c.Bounds)-1 {
			upperSymbol = constant.DataCompareSymbolLte
		}

		if bound.HasLower {
			if strings.EqualFold(dbTypeT, constant.DatabaseTypeOracle) {
				if len(preConditionForLowerT) > 0 {
					lowerConditionT = append(lowerConditionT, fmt.Sprintf("(%s AND %s %s %s)", strings.Join(preConditionForLowerT, " AND "), stringutil.StringBuilder("NLSSORT(", "\"", bound.Column, "\"", collationT, ")"), lowerSymbol, stringutil.StringBuilder("NLSSORT('", bound.Lower, "',", collationT, ")")))
				} else {
					lowerConditionT = append(lowerConditionT, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("NLSSORT(", "\"", bound.Column, "\"", collationT, ")"), lowerSymbol, stringutil.StringBuilder("NLSSORT('", bound.Lower, "',", collationT, ")")))
				}
				preConditionForLowerT = append(preConditionForLowerT, fmt.Sprintf("%s = %s", stringutil.StringBuilder("NLSSORT(", "\"", bound.Column, "\"", collationT, ")"), stringutil.StringBuilder("NLSSORT('", bound.Lower, "',", collationT, ")")))
				preConditionArgsForLowerT = append(preConditionArgsForLowerT, stringutil.StringBuilder("NLSSORT('", bound.Lower, "',", collationT, ")"))
			}

			if strings.EqualFold(dbTypeT, constant.DatabaseTypeMySQL) || strings.EqualFold(dbTypeT, constant.DatabaseTypeTiDB) {
				if len(preConditionForLowerT) > 0 {
					lowerConditionT = append(lowerConditionT, fmt.Sprintf("(%s AND %s%s %s %s)", strings.Join(preConditionForLowerT, " AND "), stringutil.StringBuilder("`", bound.Column, "`"), collationT, lowerSymbol, bound.Lower))
				} else {
					lowerConditionT = append(lowerConditionT, fmt.Sprintf("(%s%s %s %s)", stringutil.StringBuilder("`", bound.Column, "`"), collationT, lowerSymbol, bound.Lower))
				}
				preConditionForLowerT = append(preConditionForLowerT, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.Column, "`"), bound.Lower))
				preConditionArgsForLowerT = append(preConditionArgsForLowerT, bound.Lower)
			}
		}

		if bound.HasUpper {
			if strings.EqualFold(dbTypeT, constant.DatabaseTypeOracle) {
				if len(preConditionForUpperT) > 0 {
					upperConditionT = append(upperConditionT, fmt.Sprintf("(%s AND %s %s %s)", strings.Join(preConditionForUpperT, " AND "), stringutil.StringBuilder("NLSSORT(", "\"", bound.Column, "\"", collationT, ")"), upperSymbol, stringutil.StringBuilder("NLSSORT('", bound.Upper, "',", collationT, ")")))
				} else {
					upperConditionT = append(upperConditionT, fmt.Sprintf("(%s %s %s)", stringutil.StringBuilder("NLSSORT(", "\"", bound.Column, "\"", collationT, ")"), upperSymbol, stringutil.StringBuilder("NLSSORT('", bound.Upper, "',", collationT, ")")))
				}
				preConditionForUpperT = append(preConditionForUpperT, fmt.Sprintf("%s = %s", stringutil.StringBuilder("NLSSORT(", "\"", bound.Column, "\"", collationT, ")"), stringutil.StringBuilder("NLSSORT('", bound.Upper, "',", collationT, ")")))
				preConditionArgsForUpperT = append(preConditionArgsForUpperT, stringutil.StringBuilder("NLSSORT('", bound.Upper, "',", collationT, ")"))
			}

			if strings.EqualFold(dbTypeT, constant.DatabaseTypeMySQL) || strings.EqualFold(dbTypeT, constant.DatabaseTypeTiDB) {
				if len(preConditionForUpperT) > 0 {
					upperConditionT = append(upperConditionT, fmt.Sprintf("(%s AND %s%s %s %s)", strings.Join(preConditionForUpperT, " AND "), stringutil.StringBuilder("`", bound.Column, "`"), collationT, upperSymbol, bound.Upper))
				} else {
					upperConditionT = append(upperConditionT, fmt.Sprintf("(%s%s %s %s)", stringutil.StringBuilder("`", bound.Column, "`"), collationT, upperSymbol, bound.Upper))
				}
				preConditionForUpperT = append(preConditionForUpperT, fmt.Sprintf("%s = %s", stringutil.StringBuilder("`", bound.Column, "`"), bound.Upper))
				preConditionArgsForUpperT = append(preConditionArgsForUpperT, bound.Upper)
			}
		}
	}

	if len(upperConditionT) == 0 && len(lowerConditionT) == 0 {
		return "TRUE AND TRUE"
	}

	if len(upperConditionT) == 0 {
		return stringutil.StringJoin(lowerConditionT, " OR ")
	}

	if len(lowerConditionT) == 0 {
		return strings.Join(upperConditionT, " OR ")
	}

	return fmt.Sprintf("(%s) AND (%s)", strings.Join(lowerConditionT, " OR "), strings.Join(upperConditionT, " OR "))
}

func (c *Range) addBound(bound *Bound) {
	c.Bounds = append(c.Bounds, bound)
	c.BoundOffset[bound.Column] = len(c.Bounds) - 1
}

func (c *Range) updateColumnOffset() {
	c.BoundOffset = make(map[string]int)
	for i, bound := range c.Bounds {
		c.BoundOffset[bound.Column] = i
	}
}

func (c *Range) update(column, lower, upper string, updateLower, updateUpper bool) {
	if offset, ok := c.BoundOffset[column]; ok {
		// update the bound
		if updateLower {
			c.Bounds[offset].Lower = lower
			c.Bounds[offset].HasLower = true
		}
		if updateUpper {
			c.Bounds[offset].Upper = upper
			c.Bounds[offset].HasUpper = true
		}

		return
	}

	// add a new bound
	c.addBound(&Bound{
		Column:   column,
		Lower:    lower,
		Upper:    upper,
		HasLower: updateLower,
		HasUpper: updateUpper,
	})
}

func (c *Range) copy() *Range {
	newChunk := NewChunkRange()
	for _, bound := range c.Bounds {
		newChunk.addBound(&Bound{
			Column:   bound.Column,
			Lower:    bound.Lower,
			Upper:    bound.Upper,
			HasLower: bound.HasLower,
			HasUpper: bound.HasUpper,
		})
	}

	return newChunk
}

func (c *Range) copyAndUpdate(column, lower, upper string, updateLower, updateUpper bool) *Range {
	newChunk := c.copy()
	newChunk.update(column, lower, upper, updateLower, updateUpper)
	return newChunk
}
