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
	"github.com/wentaojin/dbms/utils/constant"

	"github.com/thinkeridea/go-extend/exstrings"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func GenMYSQLCompatibleDatabasePrepareStmt(
	targetSchemaName, targetTableName string, columnDetailT string, insertBatchSize int, safeMode bool) string {

	return stringutil.StringBuilder(
		genMYSQLCompatibleDatabaseInsertSQLStmtPrefix(targetSchemaName, targetTableName, columnDetailT, safeMode),
		genMYSQLCompatibleDatabasePrepareBindVarStmt(len(stringutil.StringSplit(columnDetailT, constant.StringSeparatorComma)), insertBatchSize))
}

func genMYSQLCompatibleDatabaseInsertSQLStmtPrefix(targetSchemaName, targetTableName string, columnDetailT string, safeMode bool) string {
	var prefixSQL string
	column := stringutil.StringBuilder(" (", columnDetailT, ")")
	if safeMode {
		prefixSQL = stringutil.StringBuilder(`REPLACE INTO `, targetSchemaName, ".", targetTableName, column, ` VALUES `)

	} else {
		prefixSQL = stringutil.StringBuilder(`INSERT INTO `, targetSchemaName, ".", targetTableName, column, ` VALUES `)
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