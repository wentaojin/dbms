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
	"strings"
	"time"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func (d *Database) FindDatabaseTableBestColumnName(schemaNameS, tableNameS, columnNameS string) ([]string, error) {
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
	_, results, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return nil, err
	}

	columnDatatypeMap := make(map[string]string)
	for _, res := range results {
		columnDatatypeMap[res["COLUMN_NAME"]] = res["DATA_TYPE"]
	}

	if !strings.EqualFold(columnNameS, "") && d.FilterDatabaseTableBestColumnDatatype(columnDatatypeMap[columnNameS]) {
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
			if !d.FilterDatabaseTableBestColumnDatatype(columnDatatypeMap[s]) {
				break
			} else {
				pkColumns = append(pkColumns, s)
			}
		}
		if len(pkColumns) == len(pkArrs) {
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
				if !d.FilterDatabaseTableBestColumnDatatype(columnDatatypeMap[s]) {
					break
				} else {
					ukColumns = append(ukColumns, s)
				}
			}
			if len(ukColumns) == len(ukArrs) {
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
			uiArrs := stringutil.StringSplit(ui["COLUMN_LIST"], ",")
			for _, s := range uiArrs {
				if !d.FilterDatabaseTableBestColumnDatatype(columnDatatypeMap[s]) {
					break
				} else {
					uiColumns = append(uiColumns, s)
				}
			}
			if len(uiColumns) == len(uiArrs) {
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
			niArrs := stringutil.StringSplit(ni["COLUMN_LIST"], ",")
			for _, s := range niArrs {
				if !d.FilterDatabaseTableBestColumnDatatype(columnDatatypeMap[s]) {
					break
				} else {
					niColumns = append(niColumns, s)
				}
			}
			if len(niColumns) == len(niArrs) {
				return niColumns, nil
			}
		}
	}
	return nil, nil
}

func (d *Database) FilterDatabaseTableBestColumnDatatype(columnType string) bool {
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

func (d *Database) GetDatabaseTableBestColumnAttribute(schemaNameS, tableNameS, columnNameS string) ([]map[string]string, error) {
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
	_, res, err := d.GeneralQuery(sqlStr)
	if err != nil {
		return res, err
	}
	return res, nil
}

func (d *Database) GetDatabaseTableBestColumnBucket(schemaNameS, tableNameS string, columnNameS, datatypeS string) ([]string, error) {
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

func (d *Database) GetDatabaseTableBestColumnCompareData(querySQL string, callTimeout int, dbCharsetS, dbCharsetT string) ([]string, map[string]int64, error) {
	var (
		columnNames []string
		columnTypes []string
		err         error
	)
	// record repeat counts
	batchRowsM := make(map[string]int64)

	deadline := time.Now().Add(time.Duration(callTimeout) * time.Second)

	ctx, cancel := context.WithDeadline(d.Ctx, deadline)
	defer cancel()

	rows, err := d.QueryContext(ctx, querySQL)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	for _, ct := range colTypes {
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(ct.Name()), dbCharsetS, constant.CharsetUTF8MB4)
		if err != nil {
			return nil, nil, fmt.Errorf("column [%s] charset convert failed, %v", ct.Name(), err)
		}
		columnNames = append(columnNames, stringutil.BytesToString(convertUtf8Raw))
		// database field type DatabaseTypeName() maps go type ScanType()
		columnTypes = append(columnTypes, ct.ScanType().String())
	}

	columnNameOrdersCounts := len(columnNames)
	rowData := make([]string, columnNameOrdersCounts)
	columnNameOrderIndexMap := make(map[string]int, columnNameOrdersCounts)

	for i, c := range columnNames {
		columnNameOrderIndexMap[c] = i
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
			return nil, nil, err
		}

		for i, raw := range rawResult {
			if raw == nil {
				rowData[columnNameOrderIndexMap[columnNames[i]]] = `NULL`
			} else if stringutil.BytesToString(raw) == "" {
				rowData[columnNameOrderIndexMap[columnNames[i]]] = `NULL`
			} else {
				switch columnTypes[i] {
				case "int64":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "uint64":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "float32":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "float64":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "rune":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "godror.Number":
					rowData[columnNameOrderIndexMap[columnNames[i]]] = stringutil.BytesToString(raw)
				case "[]uint8":
					// binary data -> raw、long raw、blob
					rowData[columnNameOrderIndexMap[columnNames[i]]] = fmt.Sprintf("'%v'", stringutil.BytesToString(raw))
				default:
					convertUtf8Raw, err := stringutil.CharsetConvert(raw, dbCharsetS, constant.CharsetUTF8MB4)
					if err != nil {
						return nil, nil, fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}

					convertTargetRaw, err := stringutil.CharsetConvert(convertUtf8Raw, constant.CharsetUTF8MB4, dbCharsetT)
					if err != nil {
						return nil, nil, fmt.Errorf("column [%s] charset convert failed, %v", columnNames[i], err)
					}
					rowData[columnNameOrderIndexMap[columnNames[i]]] = fmt.Sprintf("'%v'", stringutil.BytesToString(convertTargetRaw))
				}
			}
		}

		// append
		batchKey := stringutil.StringJoin(rowData, constant.StringSeparatorComma)
		if val, ok := batchRowsM[batchKey]; ok {
			batchRowsM[batchKey] = val + 1
		} else {
			batchRowsM[batchKey] = 1
		}
		// clear
		rowData = make([]string, columnNameOrdersCounts)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, err
	}

	return columnNames, batchRowsM, nil
}
