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
	"strconv"
	"strings"

	"github.com/thinkeridea/go-extend/exstrings"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func GenMYSQLCompatibleDatabasePrepareStmt(
	schemaName, tableName string, sqlHint, columnDetailSlice string, insertBatchSize int, safeMode bool) string {

	return stringutil.StringBuilder(
		genMYSQLCompatibleDatabaseInsertSQLStmtPrefix(schemaName, tableName, sqlHint, columnDetailSlice, safeMode),
		genMYSQLCompatibleDatabasePrepareBindVarStmt(len(stringutil.StringSplit(columnDetailSlice, constant.StringSeparatorComma)), insertBatchSize))
}

func genMYSQLCompatibleDatabaseInsertSQLStmtPrefix(schemaName, tableName, sqlHint string, columnDetailSlice string, safeMode bool) string {
	var prefixSQL string
	column := stringutil.StringBuilder(" (", columnDetailSlice, ")")
	if safeMode {
		if strings.EqualFold(sqlHint, "") {
			prefixSQL = stringutil.StringBuilder(`REPLACE INTO `, schemaName, ".", tableName, column, ` VALUES `)
		} else {
			prefixSQL = stringutil.StringBuilder(`REPLACE `, sqlHint, ` INTO `, schemaName, ".", tableName, column, ` VALUES `)
		}
	} else {
		if strings.EqualFold(sqlHint, "") {
			prefixSQL = stringutil.StringBuilder(`INSERT INTO `, schemaName, ".", tableName, column, ` VALUES `)
		} else {
			prefixSQL = stringutil.StringBuilder(`INSERT `, sqlHint, ` INTO `, schemaName, ".", tableName, column, ` VALUES `)
		}
	}
	return prefixSQL
}

func genMYSQLCompatibleDatabasePrepareBindVarStmt(columns, bindVarBatch int) string {
	var (
		bindVars []string
		bindVar  []string
	)
	for i := 0; i < columns; i++ {
		bindVar = append(bindVar, "?")
	}
	singleBindVar := stringutil.StringBuilder("(", exstrings.Join(bindVar, ","), ")")
	for i := 0; i < bindVarBatch; i++ {
		bindVars = append(bindVars, singleBindVar)
	}

	return exstrings.Join(bindVars, ",")
}

func GenMYSQLCompatibleDatabaseInsertStmtSQL(schemaName, tableName, sqlHint string, columnDetailSlice []string, columnDataString []string, safeMode bool, columnDataCounts ...int) string {
	var (
		prefixSQL        string
		columnDetailTSli []string
	)
	for _, c := range columnDetailSlice {
		columnDetailTSli = append(columnDetailTSli, stringutil.StringBuilder("`", c, "`"))
	}

	if safeMode {
		if strings.EqualFold(sqlHint, "") {
			prefixSQL = stringutil.StringBuilder("REPLACE INTO `", schemaName, "`.`", tableName, "` (", stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
		} else {
			prefixSQL = stringutil.StringBuilder("REPLACE ", sqlHint, " INTO `", schemaName, "`.`", tableName, "` (", stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
		}
	} else {
		if strings.EqualFold(sqlHint, "") {
			prefixSQL = stringutil.StringBuilder("INSERT INTO `", schemaName, "`.`", tableName, "` (", stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
		} else {
			prefixSQL = stringutil.StringBuilder("INSERT ", sqlHint, " INTO `", schemaName, "`.`", tableName, "` (", stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
		}
	}

	var suffixVal []string

	// data verify
	if len(columnDataCounts) > 0 {
		var restoreColDatas []string
		for i := 0; i < columnDataCounts[0]; i++ {
			restoreColDatas = append(restoreColDatas, stringutil.StringJoin(columnDataString, constant.StringSeparatorComma))
		}

		for _, vals := range restoreColDatas {
			suffixVal = append(suffixVal, stringutil.StringBuilder(`(`, vals, `)`))
		}
		return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(suffixVal, constant.StringSeparatorComma), constant.StringSeparatorSemicolon)
	}

	for _, vals := range columnDataString {
		suffixVal = append(suffixVal, stringutil.StringBuilder(`(`, vals, `)`))
	}
	return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(suffixVal, constant.StringSeparatorComma), constant.StringSeparatorSemicolon)
}

func GenMYSQLCompatibleDatabaseDeleteStmtSQL(schemaName, tableName, sqlHint string, columnDetailSlice []string, columnDataString []string, columnDataCounts int) string {

	var prefixSQL string
	if strings.EqualFold(sqlHint, "") {
		prefixSQL = stringutil.StringBuilder("DELETE FROM `", schemaName, "`.`", tableName, "` WHERE ")
	} else {
		prefixSQL = stringutil.StringBuilder("DELETE ", sqlHint, " FROM `", schemaName, "`.`", tableName, "` WHERE ")
	}

	var columnConds []string
	for i, c := range columnDetailSlice {
		if strings.EqualFold(columnDataString[i], constant.MYSQLDatabaseTableColumnDefaultValueWithNULL) {
			columnConds = append(columnConds, stringutil.StringBuilder("`", c, "`", ` IS `, columnDataString[i]))
		} else if strings.EqualFold(columnDataString[i], constant.MYSQLDatabaseTableColumnDefaultValueWithEmptyString) {
			columnConds = append(columnConds, stringutil.StringBuilder("`", c, "`", ` = ''`))
		} else {
			columnConds = append(columnConds, stringutil.StringBuilder("`", c, "`", ` = `, columnDataString[i]))
		}
	}

	return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(columnConds, " AND "), ` LIMIT `, strconv.Itoa(columnDataCounts), constant.StringSeparatorSemicolon)
}
