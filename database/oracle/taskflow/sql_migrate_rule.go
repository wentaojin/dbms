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
	"context"
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type SqlMigrateRule struct {
	Ctx             context.Context    `json:"-"`
	TaskName        string             `json:"taskName"`
	TaskMode        string             `json:"taskMode"`
	TaskFlow        string             `json:"taskFlow"`
	SchemaNameT     string             `json:"schemaNameS"`
	TableNameT      string             `json:"tableNameS"`
	SqlHintT        string             `json:"sqlHintT"`
	GlobalSqlHintT  string             `json:"globalSqlHintT"`
	DatabaseS       database.IDatabase `json:"databaseS"`
	DBCharsetS      string             `json:"DBCharsetS"`
	SqlQueryS       string             `json:"sqlQueryS"`
	ColumnRouteRule map[string]string  `json:"columnRouteRule"`
	CaseFieldRuleS  string             `json:"caseFieldRuleS"`
	CaseFieldRuleT  string             `json:"caseFieldRuleT"`
}

func (r *SqlMigrateRule) GenSchemaNameRule() (string, error) {
	convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.SchemaNameT), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
	if err != nil {
		return "", fmt.Errorf("[GetSchemaNameRule] oracle schema [%s] charset convert failed, %v", r.SchemaNameT, err)
	}
	schemaNameT := stringutil.BytesToString(convertUtf8Raw)

	if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleLower) {
		schemaNameT = strings.ToLower(schemaNameT)
	}
	if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleUpper) {
		schemaNameT = strings.ToUpper(schemaNameT)
	}

	return schemaNameT, nil
}

func (r *SqlMigrateRule) GenTableNameRule() (string, error) {
	convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.TableNameT), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
	if err != nil {
		return "", fmt.Errorf("[GetTableNameRule] oracle schema [%s] table [%v] charset convert failed, %v", r.SchemaNameT, r.TableNameT, err)
	}
	tableNameT := stringutil.BytesToString(convertUtf8Raw)

	if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleLower) {
		tableNameT = strings.ToLower(tableNameT)
	}
	if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleUpper) {
		tableNameT = strings.ToUpper(tableNameT)
	}
	return tableNameT, nil
}

func (r *SqlMigrateRule) GenTableColumnRule() (string, string, error) {
	// column name rule
	columnNames, columnTypeMap, columnScaleMap, err := r.DatabaseS.GetDatabaseTableColumnNameSqlDimensions(r.SqlQueryS)
	if err != nil {
		return "", "", err
	}

	var (
		columnNameSliS []string
		columnNameSliT []string
	)
	for _, c := range columnNames {
		columnNameS, err := optimizerColumnDatatypeS(c, columnTypeMap[c], columnScaleMap[c])
		if err != nil {
			return "", "", err
		}
		columnNameSliS = append(columnNameSliS, columnNameS)

		columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(c), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return "", "", fmt.Errorf("[GenTableColumnRule] oracle data migrate task sql column [%v] charset convert [UTFMB4] failed, error: %v", c, err)
		}

		columnName := stringutil.BytesToString(columnNameUtf8Raw)

		// column name caseFieldRule
		var (
			columnNameSNew string
			columnNameTNew string
		)
		if strings.EqualFold(r.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			columnNameSNew = strings.ToLower(columnName)
		}
		if strings.EqualFold(r.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			columnNameSNew = strings.ToUpper(columnName)
		}
		if strings.EqualFold(r.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
			columnNameSNew = columnName
		}

		if val, ok := r.ColumnRouteRule[columnNameSNew]; ok {
			columnNameTNew = val
		} else {
			columnNameTNew = columnNameSNew
		}
		switch {
		case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
			if strings.EqualFold(r.TaskMode, constant.TaskModeDataMigrate) ||
				strings.EqualFold(r.TaskMode, constant.TaskModeIncrMigrate) {
				columnNameSliT = append(columnNameSliT, fmt.Sprintf("`%s`", columnNameTNew))
			}
			if strings.EqualFold(r.TaskMode, constant.TaskModeCSVMigrate) {
				columnNameSliT = append(columnNameSliT, columnNameTNew)
			}
		default:
			return "", "", fmt.Errorf("oracle current task [%s] taskflow [%s] schema_name_t [%v] data migrate sql column rule isn't support, please contact author", r.TaskName, r.SchemaNameT, r.TaskFlow)
		}
	}
	return stringutil.StringJoin(columnNameSliS, constant.StringSeparatorComma), stringutil.StringJoin(columnNameSliT, constant.StringSeparatorComma), nil
}

func (r *SqlMigrateRule) GenTableCustomRule() (string, string) {
	if strings.EqualFold(r.SqlHintT, "") {
		return r.GlobalSqlHintT, r.SqlQueryS
	}
	return r.SqlHintT, r.SqlQueryS
}
