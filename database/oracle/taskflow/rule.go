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

	"github.com/wentaojin/dbms/model/buildin"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/migrate"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type Rule struct {
	Ctx                      context.Context                  `json:"-"`
	TaskName                 string                           `json:"taskName"`
	TaskFlow                 string                           `json:"taskFlow"`
	TaskRuleName             string                           `json:"taskRuleName"`
	SchemaNameS              string                           `json:"schemaNameS"`
	TableNameS               string                           `json:"tableNameS"`
	TablePrimaryAttrs        []map[string]string              `json:"tablePrimaryAttrs"`
	TableColumnsAttrs        []map[string]string              `json:"tableColumnsAttrs"`
	TableCommentAttrs        []map[string]string              `json:"tableCommentAttrs"`
	CaseFieldRule            string                           `json:"caseFieldRule"`
	CreateIfNotExist         bool                             `json:"createIfNotExist"`
	DBCollationS             bool                             `json:"DBCollationS"`
	DBCharsetS               string                           `json:"dbCharsetS"`
	DBCharsetT               string                           `json:"dbCharsetT"`
	BuildinDatatypeRules     []*buildin.BuildinDatatypeRule   `json:"-"`
	BuildinDefaultValueRules []*buildin.BuildinDefaultvalRule `json:"-"`
}

func (r *Rule) GetCreatePrefixRule() string {
	switch {
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
		if r.CreateIfNotExist {
			return `CREATE TABLE IF NOT EXISTS`
		}
		return `CREATE TABLE`
	default:
		return `CREATE TABLE`
	}
}

func (r *Rule) GetSchemaNameRule() (map[string]string, error) {
	schemaRoute := make(map[string]string)

	routeRule, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(r.Ctx, &rule.SchemaRouteRule{
		TaskRuleName: r.TaskRuleName, SchemaNameS: r.SchemaNameS})
	if err != nil {
		return schemaRoute, err
	}
	convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.SchemaNameS), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
	if err != nil {
		return nil, fmt.Errorf("[GetSchemaNameRule] oracle schema [%s] charset convert failed, %v", r.SchemaNameS, err)
	}
	schemaNameS := string(convertUtf8Raw)

	var schemaNameSNew string

	if !strings.EqualFold(routeRule.SchemaNameT, "") {
		schemaNameSNew = routeRule.SchemaNameT
	} else {
		schemaNameSNew = schemaNameS
	}

	if strings.EqualFold(r.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
		schemaNameSNew = strings.ToLower(schemaNameSNew)
	}
	if strings.EqualFold(r.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
		schemaNameSNew = strings.ToUpper(schemaNameSNew)
	}

	schemaRoute[schemaNameS] = schemaNameSNew

	return schemaRoute, nil
}

func (r *Rule) GetTableNameRule() (map[string]string, error) {
	tableRoute := make(map[string]string)
	routeRule, err := model.GetIMigrateTableRouteRW().GetTableRouteRule(r.Ctx, &rule.TableRouteRule{
		TaskRuleName: r.TaskRuleName, SchemaNameS: r.SchemaNameS, TableNameS: r.TableNameS})
	if err != nil {
		return tableRoute, err
	}

	convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.TableNameS), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
	if err != nil {
		return nil, fmt.Errorf("[GetTableNameRule] oracle schema [%s] table [%v] charset convert failed, %v", r.SchemaNameS, r.TableNameS, err)
	}
	tableNameS := string(convertUtf8Raw)

	var tableNameSNew string
	if strings.EqualFold(r.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
		tableNameSNew = strings.ToLower(tableNameS)
	}
	if strings.EqualFold(r.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
		tableNameSNew = strings.ToUpper(tableNameS)
	}
	if strings.EqualFold(r.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
		tableNameSNew = tableNameS
	}

	if !strings.EqualFold(routeRule.TableNameT, "") {
		tableRoute[tableNameS] = routeRule.TableNameT
	} else {
		tableRoute[tableNameS] = tableNameSNew
	}
	return tableRoute, nil
}

func (r *Rule) GetCaseFieldRule() string {
	return r.CaseFieldRule
}

// GetTableColumnRule used for get custom table column rule
// column datatype rule priority:
// - column level
// - table level
// - task level
// - default level
func (r *Rule) GetTableColumnRule() (map[string]string, map[string]string, map[string]string, error) {
	columnRules := make(map[string]string)
	columnDatatypeRules := make(map[string]string)
	columnDefaultValueRules := make(map[string]string)

	columnRoutes, err := model.GetIMigrateColumnRouteRW().FindColumnRouteRule(r.Ctx, &rule.ColumnRouteRule{
		TaskRuleName: r.TaskRuleName,
		SchemaNameS:  r.SchemaNameS,
		TableNameS:   r.TableNameS,
	})
	if err != nil {
		return columnRules, columnDatatypeRules, columnDefaultValueRules, err
	}
	structTaskRules, err := model.GetIStructMigrateTaskRuleRW().QueryTaskStructRule(r.Ctx, &migrate.TaskStructRule{TaskName: r.TaskName})
	if err != nil {
		return columnRules, columnDatatypeRules, columnDefaultValueRules, err
	}
	structSchemaRules, err := model.GetIStructMigrateSchemaRuleRW().QuerySchemaStructRule(r.Ctx, &migrate.SchemaStructRule{
		TaskName:    r.TaskName,
		SchemaNameS: r.SchemaNameS})
	if err != nil {
		return columnRules, columnDatatypeRules, columnDefaultValueRules, err
	}
	structTableRules, err := model.GetIStructMigrateTableRuleRW().QueryTableStructRule(r.Ctx, &migrate.TableStructRule{
		TaskName:    r.TaskName,
		SchemaNameS: r.SchemaNameS,
		TableNameS:  r.TableNameS})
	if err != nil {
		return columnRules, columnDatatypeRules, columnDefaultValueRules, err
	}
	structColumnRules, err := model.GetIStructMigrateColumnRuleRW().QueryColumnStructRule(r.Ctx, &migrate.ColumnStructRule{
		TaskName:    r.TaskName,
		SchemaNameS: r.SchemaNameS,
		TableNameS:  r.TableNameS})
	if err != nil {
		return columnRules, columnDatatypeRules, columnDefaultValueRules, err
	}

	for _, c := range r.TableColumnsAttrs {
		columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(c["COLUMN_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return columnRules, columnDatatypeRules, columnDefaultValueRules, fmt.Errorf("[GetTableColumnRule] oracle schema [%s] table [%s] column [%s] charset convert [UTFMB4] failed, error: %v", r.SchemaNameS, r.TableNameS, c["COLUMN_NAME"], err)
		}

		columnName := string(columnNameUtf8Raw)

		// column name caseFieldRule
		var columnNameNew string

		if strings.EqualFold(r.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
			columnNameNew = strings.ToLower(columnName)
		}
		if strings.EqualFold(r.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
			columnNameNew = strings.ToUpper(columnName)
		}
		if strings.EqualFold(r.CaseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
			columnNameNew = columnName
		}

		columnRules[columnName] = columnNameNew

		defaultValUtf8Raw, err := stringutil.CharsetConvert([]byte(c["DATA_DEFAULT"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return columnRules, columnDatatypeRules, columnDefaultValueRules, fmt.Errorf("[GetTableColumnRule] oracle schema [%s] table [%s] column [%s] default value [%s] charset convert [UTFMB4] failed, error: %v", r.SchemaNameS, r.TableNameS, c["COLUMN_NAME"], c["DATA_DEFAULT"], err)
		}
		columnDefaultValues := string(defaultValUtf8Raw)

		commentUtf8Raw, err := stringutil.CharsetConvert([]byte(c["COMMENTS"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return columnRules, columnDatatypeRules, columnDefaultValueRules, fmt.Errorf("[GetTableColumnRule] oracle schema [%s] table [%s] column [%s] comment [%s] charset convert [UTFMB4] failed, error: %v", r.SchemaNameS, r.TableNameS, c["COLUMN_NAME"], c["COMMENTS"], err)
		}
		columnComment := string(commentUtf8Raw)

		var (
			originColumnType, buildInColumnType string
		)
		// task flow
		switch {
		case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
			originColumnType, buildInColumnType, err = DatabaseTableColumnMapMYSQLDatatypeRule(&Column{
				ColumnName:    columnName,
				Datatype:      c["DATA_TYPE"],
				CharUsed:      c["CHAR_USED"],
				CharLength:    c["CHAR_LENGTH"],
				DataPrecision: c["DATA_PRECISION"],
				DataLength:    c["DATA_LENGTH"],
				DataScale:     c["DATA_SCALE"],
				DataDefault:   columnDefaultValues,
				Nullable:      c["NULLABLE"],
				Comment:       columnComment,
			}, r.BuildinDatatypeRules)
			if err != nil {
				return nil, nil, nil, err
			}
			// priority, return target database table column datatype
			convertColumnDatatype, convertColumnDefaultValue, err := HandleColumnRuleWithPriority(
				columnName,
				originColumnType,
				buildInColumnType,
				columnDefaultValues,
				r.DBCharsetS,
				r.DBCharsetT,
				r.BuildinDefaultValueRules,
				structTaskRules,
				structSchemaRules,
				structTableRules,
				structColumnRules)
			if err != nil {
				return nil, nil, nil, err
			}

			columnDatatypeRules[columnName] = convertColumnDatatype
			columnDefaultValueRules[columnName] = convertColumnDefaultValue
		default:
			return nil, nil, nil, fmt.Errorf("oracle current task [%s] schema [%s] taskflow [%s] column rule isn't support, please contact author", r.TaskName, r.SchemaNameS, r.TaskFlow)
		}
	}

	for _, c := range columnRoutes {
		if _, exist := columnRules[c.ColumnNameS]; exist {
			columnRules[c.ColumnNameS] = c.ColumnNameT
		}
	}

	return columnRules, columnDatatypeRules, columnDefaultValueRules, nil
}

func (r *Rule) GetTableAttributesRule() (string, error) {
	attr, err := model.GetIStructMigrateTableAttrsRuleRW().GetTableAttrsRule(r.Ctx, &migrate.TableAttrsRule{
		TaskName:    r.TaskName,
		SchemaNameS: r.SchemaNameS,
		TableNameS:  r.TableNameS,
	})
	if err != nil {
		return "", err
	}

	if !strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB) {
		logger.Warn("get table rule",
			zap.String("task_name", r.TableNameS),
			zap.String("task_flow", r.TaskFlow),
			zap.String("task_notes",
				fmt.Sprintf("oracle current task [%s] schema [%s] taskflow [%s] attributes rule isn't support, please contact author", r.TaskName, r.SchemaNameS, r.TaskFlow)),
			zap.String("operator", "ignore"))
		return "", nil
	}

	return attr.TableAttrT, nil
}

func (r *Rule) GetTableCommentRule() (string, error) {
	var tableComment string
	if len(r.TableCommentAttrs) > 1 {
		return tableComment, fmt.Errorf("oracle schema [%s] table [%s] comments [%s] records are over one, current value is [%d]", r.SchemaNameS, r.TableNameS, r.TableCommentAttrs[0]["COMMENTS"], len(r.TableCommentAttrs))
	}
	if len(r.TableCommentAttrs) == 0 || strings.EqualFold(r.TableCommentAttrs[0]["COMMENTS"], "") {
		return tableComment, nil
	}

	convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.TableCommentAttrs[0]["COMMENTS"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
	if err != nil {
		return tableComment, fmt.Errorf("[GetTableColumnRule] oracle schema [%s] table [%s] comments [%s] charset convert [UTFMB4] failed, error: %v", r.SchemaNameS, r.TableNameS, r.TableCommentAttrs[0]["COMMENTS"], err)
	}
	tableComment = string(convertUtf8Raw)
	switch {
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
		return tableComment, nil
	default:
		return tableComment, fmt.Errorf("oracle current taskflow [%s] isn't support, please contact author or reselect", r.TaskFlow)
	}
}

func (r *Rule) GetTableColumnCollationRule() (map[string]string, error) {
	columnCollationMap := make(map[string]string)
	for _, rowCol := range r.TableColumnsAttrs {
		var columnCollation string
		// the oracle 12.2 and the above version support column collation
		if r.DBCollationS {
			// check column sort collation
			if collationMapVal, ok := constant.MigrateTableStructureDatabaseCollationMap[r.TaskFlow][strings.ToUpper(rowCol["COLLATION"])][constant.MigrateTableStructureDatabaseCharsetMap[r.TaskFlow][r.DBCharsetS]]; ok {
				columnCollation = collationMapVal
			} else {
				// exclude the column with the integer datatype
				if !strings.EqualFold(rowCol["COLLATION"], "") {
					return columnCollationMap, fmt.Errorf("oracle schema [%s] table [%s] column [%s] collation [%s] check failed", r.SchemaNameS, r.TableNameS, rowCol["COLUMN_NAME"], rowCol["COLLATION"])
				}
				columnCollation = ""
			}
		} else {
			// the oracle 12.2 and the above version isn't support column collation, and set ""
			columnCollation = ""
		}

		columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(rowCol["COLUMN_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return columnCollationMap, fmt.Errorf("[GetTableColumnCollationRule] oracle schema [%s] table [%s] column [%s] charset convert [UTFMB4] failed, error: %v", r.SchemaNameS, r.TableNameS, rowCol["COLUMN_NAME"], err)
		}
		columnName := string(columnNameUtf8Raw)

		columnCollationMap[columnName] = columnCollation
	}
	return columnCollationMap, nil
}

func (r *Rule) GetTableColumnCommentRule() (map[string]string, error) {
	columnCommentMap := make(map[string]string)
	for _, rowCol := range r.TableColumnsAttrs {
		columnNameUtf8Raw, err := stringutil.CharsetConvert([]byte(rowCol["COLUMN_NAME"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return columnCommentMap, fmt.Errorf("[GetTableColumnCommentRule] oracle schema [%s] table [%s] column [%s] charset convert [UTFMB4] failed, error: %v", r.SchemaNameS, r.TableNameS, rowCol["COLUMN_NAME"], err)
		}
		columnName := string(columnNameUtf8Raw)

		commentUtf8Raw, err := stringutil.CharsetConvert([]byte(rowCol["COMMENTS"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
		if err != nil {
			return columnCommentMap, fmt.Errorf("[GetTableColumnCommentRule] oracle schema [%s] table [%s] column [%s] comment [%s] charset convert failed, %v", r.SchemaNameS, r.TableNameS, rowCol["COLUMN_NAME"], rowCol["COMMENTS"], err)
		}

		columnComment := string(commentUtf8Raw)

		if !strings.EqualFold(columnComment, "") {
			switch {
			case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
				columnCommentMap[columnName] = columnComment
			default:
				return columnCommentMap, fmt.Errorf("[GetTableColumnCommentRule] oracle current taskflow [%s] column comment isn't support, please contact author or reselect", r.TaskFlow)
			}
		} else {
			columnCommentMap[columnName] = columnComment
		}
	}
	return columnCommentMap, nil
}

func (r *Rule) String() string {
	jsonStr, _ := stringutil.MarshalJSON(r)
	return jsonStr
}
