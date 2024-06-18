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
package taskflow

import (
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/thinkeridea/go-extend/exstrings"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func GenMYSQLCompatibleDatabasePrepareStmt(
	targetSchemaName, targetTableName string, sqlHintT, columnDetailT string, insertBatchSize int, safeMode bool) string {

	return stringutil.StringBuilder(
		genMYSQLCompatibleDatabaseInsertSQLStmtPrefix(targetSchemaName, targetTableName, sqlHintT, columnDetailT, safeMode),
		genMYSQLCompatibleDatabasePrepareBindVarStmt(len(stringutil.StringSplit(columnDetailT, constant.StringSeparatorComma)), insertBatchSize))
}

func genMYSQLCompatibleDatabaseInsertSQLStmtPrefix(targetSchemaName, targetTableName, sqlHintT string, columnDetailT string, safeMode bool) string {
	var prefixSQL string
	column := stringutil.StringBuilder(" (", columnDetailT, ")")
	if safeMode {
		if strings.EqualFold(sqlHintT, "") {
			prefixSQL = stringutil.StringBuilder(`REPLACE INTO `, targetSchemaName, ".", targetTableName, column, ` VALUES `)
		} else {
			prefixSQL = stringutil.StringBuilder(`REPLACE `, sqlHintT, ` INTO `, targetSchemaName, ".", targetTableName, column, ` VALUES `)
		}
	} else {
		if strings.EqualFold(sqlHintT, "") {
			prefixSQL = stringutil.StringBuilder(`INSERT INTO `, targetSchemaName, ".", targetTableName, column, ` VALUES `)
		} else {
			prefixSQL = stringutil.StringBuilder(`INSERT `, sqlHintT, ` INTO `, targetSchemaName, ".", targetTableName, column, ` VALUES `)
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

func GenMYSQLCompatibleDatabaseInsertStmtSQL(targetSchemaName, targetTableName, sqlHintT string, columnDetailT []string, columnDataT string, columnDataCounts int, safeMode bool) string {
	var (
		prefixSQL        string
		columnDetailTSli []string
	)
	for _, c := range columnDetailT {
		columnDetailTSli = append(columnDetailTSli, stringutil.StringBuilder("`", c, "`"))
	}

	if safeMode {
		if strings.EqualFold(sqlHintT, "") {
			prefixSQL = stringutil.StringBuilder("REPLACE INTO `", targetSchemaName, "`.`", targetTableName, "` (", stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
		} else {
			prefixSQL = stringutil.StringBuilder("REPLACE ", sqlHintT, " INTO `", targetSchemaName, "`.`", targetTableName, "` (", stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
		}
	} else {
		if strings.EqualFold(sqlHintT, "") {
			prefixSQL = stringutil.StringBuilder("INSERT INTO `", targetSchemaName, "`.`", targetTableName, "` (", stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
		} else {
			prefixSQL = stringutil.StringBuilder("INSERT ", sqlHintT, " INTO `", targetSchemaName, "`.`", targetTableName, "` (", stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
		}
	}

	var restoreColDatas []string
	for i := 0; i < columnDataCounts; i++ {
		restoreColDatas = append(restoreColDatas, columnDataT)
	}

	var suffixVal []string
	for _, vals := range restoreColDatas {
		suffixVal = append(suffixVal, stringutil.StringBuilder(`(`, vals, `)`))
	}

	return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(suffixVal, constant.StringSeparatorComma))
}

func GenMYSQLCompatibleDatabaseDeleteStmtSQL(targetSchemaName, targetTableName, sqlHintT string, columnDetailT []string, columnDataT []string, columnDataCounts int) string {

	var prefixSQL string
	if strings.EqualFold(sqlHintT, "") {
		prefixSQL = stringutil.StringBuilder("DELETE FROM `", targetSchemaName, "`.`", targetTableName, "` WHERE ")
	} else {
		prefixSQL = stringutil.StringBuilder("DELETE ", sqlHintT, " FROM `", targetSchemaName, "`.`", targetTableName, "` WHERE ")
	}

	var columnConds []string
	for i, c := range columnDetailT {
		if strings.EqualFold(columnDataT[i], constant.MYSQLDatabaseTableColumnDefaultValueWithNULL) {
			columnConds = append(columnConds, stringutil.StringBuilder("`", c, "`", ` IS `, columnDataT[i]))
		} else if strings.EqualFold(columnDataT[i], constant.MYSQLDatabaseTableColumnDefaultValueWithStringNull) {
			columnConds = append(columnConds, `''`)
		} else {
			columnConds = append(columnConds, stringutil.StringBuilder("`", c, "`", ` = `, columnDataT[i]))
		}
	}

	return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(columnConds, " AND "), ` LIMIT `, strconv.Itoa(columnDataCounts))
}
