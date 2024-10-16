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
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/database"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type DataMigrateRule struct {
	Ctx            context.Context    `json:"-"`
	TaskName       string             `json:"taskName"`
	TaskMode       string             `json:"taskMode"`
	TaskFlow       string             `json:"taskFlow"`
	SchemaNameS    string             `json:"schemaNameS"`
	TableNameS     string             `json:"tableNameS"`
	GlobalSqlHintS string             `json:"globalSqlHintS"`
	TableTypeS     map[string]string  `json:"tableTypeS"`
	DatabaseS      database.IDatabase `json:"databaseS"`
	DBCharsetS     string             `json:"DBCharsetS"`
	CaseFieldRuleS string             `json:"caseFieldRuleS"`
	CaseFieldRuleT string             `json:"caseFieldRuleT"`
}

func (r *DataMigrateRule) GenSchemaNameRule() (string, string, error) {
	routeRule, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(r.Ctx, &rule.SchemaRouteRule{
		TaskName: r.TaskName, SchemaNameS: r.SchemaNameS})
	if err != nil {
		return "", "", err
	}

	var schemaNameS string
	switch r.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.SchemaNameS), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] [GetSchemaNameRule] schema [%s] charset convert failed, %v", r.TaskName, r.TaskFlow, r.TaskMode, r.SchemaNameS, err)
		}
		schemaNameS = stringutil.BytesToString(convertUtf8Raw)
	case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.SchemaNameS), constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] [GetSchemaNameRule] schema [%s] charset convert failed, %v", r.TaskName, r.TaskFlow, r.TaskMode, r.SchemaNameS, err)
		}
		schemaNameS = stringutil.BytesToString(convertUtf8Raw)
	default:
		return "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] isn't support, please contact author or reselect", r.TaskName, r.TaskFlow, r.TaskMode)
	}

	var schemaNameTNew string

	if !strings.EqualFold(routeRule.SchemaNameT, "") {
		schemaNameTNew = routeRule.SchemaNameT
	} else {
		schemaNameTNew = schemaNameS
	}

	if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleLower) {
		schemaNameTNew = strings.ToLower(schemaNameTNew)
	}
	if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleUpper) {
		schemaNameTNew = strings.ToUpper(schemaNameTNew)
	}

	return schemaNameS, schemaNameTNew, nil
}

func (r *DataMigrateRule) GenSchemaTableNameRule() (string, string, error) {
	routeRule, err := model.GetIMigrateTableRouteRW().GetTableRouteRule(r.Ctx, &rule.TableRouteRule{
		TaskName: r.TaskName, SchemaNameS: r.SchemaNameS, TableNameS: r.TableNameS})
	if err != nil {
		return "", "", err
	}

	var tableNameS string
	switch r.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.TableNameS), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] [GetTableNameRule] schema [%s] table [%v] charset convert failed, %v", r.TaskName, r.TaskFlow, r.TaskMode, r.SchemaNameS, r.TableNameS, err)
		}
		tableNameS = stringutil.BytesToString(convertUtf8Raw)
	case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
		convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.TableNameS), constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] [GetTableNameRule] schema [%s] table [%v] charset convert failed, %v", r.TaskName, r.TaskFlow, r.TaskMode, r.SchemaNameS, r.TableNameS, err)
		}
		tableNameS = stringutil.BytesToString(convertUtf8Raw)
	default:
		return "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] isn't support, please contact author or reselect", r.TaskName, r.TaskFlow, r.TaskMode)
	}

	var tableNameTNew string
	if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleLower) {
		tableNameTNew = strings.ToLower(tableNameS)
	}
	if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleUpper) {
		tableNameTNew = strings.ToUpper(tableNameS)
	}
	if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleOrigin) {
		tableNameTNew = tableNameS
	}

	if !strings.EqualFold(routeRule.TableNameT, "") {
		tableNameTNew = routeRule.TableNameT
	}
	return tableNameS, tableNameTNew, nil
}

func (r *DataMigrateRule) GetSchemaTableColumnNameRule() (map[string]string, error) {
	// ignore, only GenSchemaTableColumnSelectRule
	return nil, nil
}

func (r *DataMigrateRule) GenSchemaTableColumnSelectRule() (string, string, string, string, error) {
	columnRules := make(map[string]string)

	columnRoutes, err := model.GetIMigrateColumnRouteRW().FindColumnRouteRule(r.Ctx, &rule.ColumnRouteRule{
		TaskName:    r.TaskName,
		SchemaNameS: r.SchemaNameS,
		TableNameS:  r.TableNameS,
	})
	if err != nil {
		return "", "", "", "", err
	}

	sourceColumnNameS, err := r.DatabaseS.GetDatabaseTableColumnNameTableDimensions(r.SchemaNameS, r.TableNameS)
	if err != nil {
		return "", "", "", "", err
	}

	for _, c := range sourceColumnNameS {
		var columnName string
		switch r.TaskFlow {
		case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
			columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(c), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return "", "", "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] [GetTableColumnRule] schema [%s] table [%s] column [%s] charset convert [UTFMB4] failed, error: %v", r.TaskName, r.TaskFlow, r.TaskMode, r.SchemaNameS, r.TableNameS, c, err)
			}
			columnName = stringutil.BytesToString(columnNameUtf8Raw)
		case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
			columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(c), constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return "", "", "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] [GetTableColumnRule] schema [%s] table [%s] column [%s] charset convert [UTFMB4] failed, error: %v", r.TaskName, r.TaskFlow, r.TaskMode, r.SchemaNameS, r.TableNameS, c, err)
			}
			columnName = stringutil.BytesToString(columnNameUtf8Raw)
		default:
			return "", "", "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] isn't support, please contact author or reselect", r.TaskName, r.TaskFlow, r.TaskMode)
		}

		// column name caseFieldRule
		var (
			columnNameSNew string
			columnNameTNew string
		)
		if strings.EqualFold(r.CaseFieldRuleS, constant.ParamValueDataMigrateCaseFieldRuleLower) {
			columnNameSNew = strings.ToLower(columnName)
		}
		if strings.EqualFold(r.CaseFieldRuleS, constant.ParamValueDataMigrateCaseFieldRuleUpper) {
			columnNameSNew = strings.ToUpper(columnName)
		}
		if strings.EqualFold(r.CaseFieldRuleS, constant.ParamValueDataMigrateCaseFieldRuleOrigin) {
			columnNameSNew = columnName
		}

		if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleLower) {
			columnNameTNew = strings.ToLower(columnName)
		}
		if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleUpper) {
			columnNameTNew = strings.ToUpper(columnName)
		}
		if strings.EqualFold(r.CaseFieldRuleT, constant.ParamValueDataMigrateCaseFieldRuleOrigin) {
			columnNameTNew = columnName
		}

		columnRules[columnNameSNew] = columnNameTNew
	}

	for _, c := range columnRoutes {
		if _, exist := columnRules[c.ColumnNameS]; exist {
			columnRules[c.ColumnNameS] = c.ColumnNameT
		}
	}

	var (
		columnNameSilS, columnNameSliT []string
	)

	sourceColumnInfos, err := r.DatabaseS.GetDatabaseTableColumnInfo(r.SchemaNameS, r.TableNameS)
	if err != nil {
		return "", "", "", "", err
	}

	for _, rowCol := range sourceColumnInfos {
		columnName := rowCol["COLUMN_NAME"]

		switch r.TaskFlow {
		case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
			columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(columnName), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return "", "", "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] [GenTableQueryColumnRule] schema [%s] table [%s] column [%s] charset convert [UTFMB4] failed, error: %v", r.TaskName, r.TaskFlow, r.TaskMode, r.SchemaNameS, r.TableNameS, columnName, err)
			}
			columnName = stringutil.BytesToString(columnNameUtf8Raw)

			columnNameS, err := OptimizerOracleDataMigrateColumnS(columnName, rowCol["DATA_TYPE"], rowCol["DATA_SCALE"])
			if err != nil {
				return "", "", "", "", err
			}
			columnNameSilS = append(columnNameSilS, columnNameS)
		case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
			columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(columnName), constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return "", "", "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] [GenTableQueryColumnRule] schema [%s] table [%s] column [%s] charset convert [UTFMB4] failed, error: %v", r.TaskName, r.TaskFlow, r.TaskMode, r.SchemaNameS, r.TableNameS, columnName, err)
			}
			columnName = stringutil.BytesToString(columnNameUtf8Raw)

			columnNameS, err := OptimizerPostgresDataMigrateColumnS(columnName, rowCol["DATA_TYPE"], rowCol["DATETIME_PRECISION"])
			if err != nil {
				return "", "", "", "", err
			}
			columnNameSilS = append(columnNameSilS, columnNameS)
		default:
			return "", "", "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] isn't support, please contact author or reselect", r.TaskName, r.TaskFlow, r.TaskMode)
		}

		var (
			columnNameSNew string
			columnNameT    string
		)
		if strings.EqualFold(r.CaseFieldRuleS, constant.ParamValueDataMigrateCaseFieldRuleLower) {
			columnNameSNew = strings.ToLower(columnName)
		}
		if strings.EqualFold(r.CaseFieldRuleS, constant.ParamValueDataMigrateCaseFieldRuleUpper) {
			columnNameSNew = strings.ToUpper(columnName)
		}
		if strings.EqualFold(r.CaseFieldRuleS, constant.ParamValueDataMigrateCaseFieldRuleOrigin) {
			columnNameSNew = columnName
		}

		if val, ok := columnRules[columnNameSNew]; ok {
			switch r.TaskFlow {
			case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL, constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
				if strings.EqualFold(r.TaskMode, constant.TaskModeStmtMigrate) ||
					strings.EqualFold(r.TaskMode, constant.TaskModeIncrMigrate) {
					columnNameT = fmt.Sprintf("%s%s%s", constant.StringSeparatorBacktick, val, constant.StringSeparatorBacktick)
				}
				if strings.EqualFold(r.TaskMode, constant.TaskModeCSVMigrate) {
					columnNameT = val
				}
			default:
				return "", "", "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] schema [%s] taskflow [%s] column rule isn't support, please contact author", r.TaskName, r.TaskFlow, r.TaskMode, r.SchemaNameS, r.TaskFlow)
			}
		} else {
			return "", "", "", "", fmt.Errorf("the task_name [%s] task_flow [%s] and task_mode [%s] [GetTableColumnRule] schema [%s] table [%s] column [%s] isn't exist, please contact author or double check again", r.TaskName, r.TaskFlow, r.TaskMode, r.SchemaNameS, r.TableNameS, columnName)
		}
		columnNameSliT = append(columnNameSliT, columnNameT)
	}

	return stringutil.StringJoin(sourceColumnNameS, constant.StringSeparatorComma),
		stringutil.StringJoin(columnNameSilS, constant.StringSeparatorComma),
		stringutil.StringJoin(columnNameSliT, constant.StringSeparatorComma),
		stringutil.StringJoin(columnNameSliT, constant.StringSeparatorComma), nil
}

func (r *DataMigrateRule) GenSchemaTableTypeRule() string {
	return r.TableTypeS[r.TableNameS]
}

func (r *DataMigrateRule) GenSchemaTableCustomRule() (bool, string, string, error) {
	isRecord, err := model.GetIDataMigrateRuleRW().IsContainedDataMigrateRuleRecord(r.Ctx, &rule.DataMigrateRule{
		TaskName:    r.TaskName,
		SchemaNameS: r.SchemaNameS,
		TableNameS:  r.TableNameS,
	})
	if err != nil {
		return false, "", "", err
	}
	if !isRecord {
		return false, "", r.GlobalSqlHintS, nil
	}

	migrateTableRule, err := model.GetIDataMigrateRuleRW().GetDataMigrateRule(r.Ctx, &rule.DataMigrateRule{
		TaskName:    r.TaskName,
		SchemaNameS: r.SchemaNameS,
		TableNameS:  r.TableNameS,
	})
	if err != nil {
		return false, "", r.GlobalSqlHintS, err
	}
	enableChunkStrategy, err := strconv.ParseBool(migrateTableRule.EnableChunkStrategy)
	if err != nil {
		return false, "", r.GlobalSqlHintS, err
	}

	if strings.EqualFold(migrateTableRule.SqlHintS, "") {
		return enableChunkStrategy, migrateTableRule.WhereRange, r.GlobalSqlHintS, nil
	}
	return enableChunkStrategy, migrateTableRule.WhereRange, migrateTableRule.SqlHintS, nil
}
