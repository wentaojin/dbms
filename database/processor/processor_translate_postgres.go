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

	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func GenPostgresCompatibleDatabaseInsertStmtSQL(schemaName, tableName, sqlHint string, columnDetailSlice []string, columnDataString []string, columnDataCounts int, safeMode bool) string {
	var (
		prefixSQL        string
		columnDetailTSli []string
	)
	for _, c := range columnDetailSlice {
		columnDetailTSli = append(columnDetailTSli, stringutil.StringBuilder("`", c, "`"))
	}

	if strings.EqualFold(sqlHint, "") {
		prefixSQL = stringutil.StringBuilder("INSERT INTO `", schemaName, "`.`", tableName, "` (", stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
	} else {
		prefixSQL = stringutil.StringBuilder("INSERT ", sqlHint, " INTO `", schemaName, "`.`", tableName, "` (", stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `)`, ` VALUES `)
	}

	var restoreColDatas []string
	for i := 0; i < columnDataCounts; i++ {
		restoreColDatas = append(restoreColDatas, stringutil.StringJoin(columnDataString, constant.StringSeparatorComma))
	}

	var suffixVal []string
	for _, vals := range restoreColDatas {
		suffixVal = append(suffixVal, stringutil.StringBuilder(`(`, vals, `)`))
	}

	if safeMode {
		var doUpdates []string
		for _, c := range columnDetailTSli {
			doUpdates = append(doUpdates, stringutil.StringBuilder(c, ` = excluded.`, c))
		}
		return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(suffixVal, constant.StringSeparatorComma), `ON CONFLICT (`, stringutil.StringJoin(columnDetailTSli, constant.StringSeparatorComma), `) DO UPDATE SET `, stringutil.StringJoin(doUpdates, constant.StringSeparatorComma), constant.StringSeparatorSemicolon)
	}
	return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(suffixVal, constant.StringSeparatorComma), constant.StringSeparatorSemicolon)
}

func GenPostgresCompatibleDatabaseDeleteStmtSQL(schemaName, tableName, sqlHint string, columnDetailSlice []string, columnDataString []string, columnDataCounts int) string {

	var prefixSQL string
	if strings.EqualFold(sqlHint, "") {
		prefixSQL = stringutil.StringBuilder(`DELETE FROM "`, schemaName, `"."`, tableName, `" WHERE `)
	} else {
		prefixSQL = stringutil.StringBuilder(`DELETE `, sqlHint, ` FROM "`, schemaName, `"."`, tableName, `" WHERE `)
	}

	var columnConds []string
	for i, c := range columnDetailSlice {
		if strings.EqualFold(columnDataString[i], constant.PostgresDatabaseTableColumnDefaultValueWithNULL) {
			columnConds = append(columnConds, stringutil.StringBuilder(`"`, c, `"`, ` IS `, columnDataString[i]))
		} else if strings.EqualFold(columnDataString[i], constant.PostgresDatabaseTableColumnDefaultValueWithEmptyString) {
			columnConds = append(columnConds, stringutil.StringBuilder(`"`, c, `"`, ` = ''`))
		} else {
			columnConds = append(columnConds, stringutil.StringBuilder(`"`, c, `"`, ` = `, columnDataString[i]))
		}
	}

	return stringutil.StringBuilder(prefixSQL, stringutil.StringJoin(columnConds, " AND "), ` LIMIT `, strconv.Itoa(columnDataCounts), constant.StringSeparatorSemicolon)
}
