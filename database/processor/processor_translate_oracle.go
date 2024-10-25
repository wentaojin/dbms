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
package processor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func GenOracleCompatibleDatabaseInsertStmtSQL(schemaName, tableName, sqlHint string, columnDetailSlice []string, columnDataString []string, safeMode bool, columnDataCounts ...int) string {
	var (
		prefixSQL                     string
		columnDetailTSli              []string
		onCondsColumnDetailTSlice     []string
		whereCondsColumnDetailT2Slice []string
	)
	for _, c := range columnDetailSlice {
		columnDetailTSli = append(columnDetailTSli, stringutil.StringBuilder(`"`, c, `"`))
		onCondsColumnDetailTSlice = append(onCondsColumnDetailTSlice, stringutil.StringBuilder(`T1."`, c, `" = T2."`, c, `"`))
		whereCondsColumnDetailT2Slice = append(whereCondsColumnDetailT2Slice, stringutil.StringBuilder(`T2."`, c, `"`))
	}

	if safeMode {
		if strings.EqualFold(sqlHint, "") {
			prefixSQL = stringutil.StringBuilder(`MERGE INTO "`, schemaName, `"."`, tableName, `" T1`)
		} else {
			prefixSQL = stringutil.StringBuilder(`MERGE `, sqlHint, ` INTO "`, schemaName, `"."`, tableName, `" T1`)
		}

		var (
			restoreColDatas []string
			usingQueries    []string
		)
		if len(columnDataCounts) > 0 {
			for i := 0; i < columnDataCounts[0]; i++ {
				restoreColDatas = append(restoreColDatas, stringutil.StringJoin(columnDataString, constant.StringSeparatorComma))
			}
		} else {
			restoreColDatas = columnDataString
		}

		for _, c := range restoreColDatas {
			var selectConds []string
			for i, s := range stringutil.StringSplit(c, constant.StringSeparatorComma) {
				selectConds = append(selectConds, stringutil.StringBuilder(s, ` AS `, columnDetailTSli[i]))
			}
			usingQueries = append(usingQueries, stringutil.StringBuilder(`SELECT `, stringutil.StringJoin(selectConds, constant.StringSeparatorComma)), ` FROM DUAL`)
		}

		usingQuery := stringutil.StringJoin(usingQueries, " UNION ")
		onConds := stringutil.StringJoin(onCondsColumnDetailTSlice, " AND ")

		return fmt.Sprintf(`%s
USING (%s) T2
ON (%s)
WHEN MATCHED THEN
	UPDATE SET %s
WHEN NOT MATCHED THEN
	INSERT (%s) VALUES (%s)`, prefixSQL, usingQuery, onConds, onConds, stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), stringutil.StringJoin(whereCondsColumnDetailT2Slice, constant.StringSeparatorComma))

	} else {
		var restoreColDatas []string
		if len(columnDataCounts) > 0 {
			for i := 0; i < columnDataCounts[0]; i++ {
				restoreColDatas = append(restoreColDatas, stringutil.StringJoin(columnDataString, constant.StringSeparatorComma))
			}
		} else {
			restoreColDatas = columnDataString
		}

		if len(restoreColDatas) > 1 {
			if strings.EqualFold(sqlHint, "") {
				prefixSQL = stringutil.StringBuilder("INSERT ALL \n")
			} else {
				prefixSQL = stringutil.StringBuilder("INSERT ALL ", sqlHint, "\n")
			}
			var suffixVal []string
			for _, vals := range restoreColDatas {
				suffixVal = append(suffixVal, stringutil.StringBuilder(` INTO "`, schemaName, `"."`, tableName, `" (`, stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES (`, vals, `)`))
			}
			return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(suffixVal, "\n"), `SELECT * FROM DUAL`, constant.StringSeparatorSemicolon)
		} else if len(restoreColDatas) == 1 {
			if strings.EqualFold(sqlHint, "") {
				prefixSQL = stringutil.StringBuilder(`INSERT INTO "`, schemaName, `"."`, tableName, `" (`, stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
			} else {
				prefixSQL = stringutil.StringBuilder(`INSERT INTO `, sqlHint, `"`, schemaName, `"."`, tableName, `" (`, stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
			}
			var suffixVal []string
			for _, vals := range restoreColDatas {
				suffixVal = append(suffixVal, stringutil.StringBuilder(`(`, vals, `)`))
			}
			return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(suffixVal, constant.StringSeparatorComma), constant.StringSeparatorSemicolon)
		} else {
			panic(fmt.Errorf("the schema_name [%s] table_name [%s] column_data_counts [%v] column_data_string [%v] restore_col_datas [%v], please contact author or reselect",
				schemaName, tableName, columnDataCounts, columnDataString, len(restoreColDatas)))
		}
	}
}

func GenOracleCompatibleDatabaseDeleteStmtSQL(schemaName, tableName, sqlHint string, columnDetailSlice []string, columnDataString []string, columnDataCounts int) string {

	var prefixSQL string
	if strings.EqualFold(sqlHint, "") {
		prefixSQL = stringutil.StringBuilder(`DELETE FROM "`, schemaName, `"."`, tableName, `" WHERE `)
	} else {
		prefixSQL = stringutil.StringBuilder(`DELETE `, sqlHint, ` FROM "`, schemaName, `"."`, tableName, `" WHERE `)
	}

	var columnConds []string
	for i, c := range columnDetailSlice {
		if strings.EqualFold(columnDataString[i], constant.OracleDatabaseTableColumnDefaultValueWithNULL) {
			columnConds = append(columnConds, stringutil.StringBuilder(`"`, c, `"`, ` IS `, columnDataString[i]))
		} else if strings.EqualFold(columnDataString[i], constant.OracleDatabaseTableColumnDefaultValueWithEmptyString) {
			columnConds = append(columnConds, stringutil.StringBuilder(`"`, c, `"`, ` IS NULL`))
		} else {
			columnConds = append(columnConds, stringutil.StringBuilder(`"`, c, `"`, ` = `, columnDataString[i]))
		}
	}

	return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(columnConds, " AND "), ` LIMIT `, strconv.Itoa(columnDataCounts), constant.StringSeparatorSemicolon)
}
