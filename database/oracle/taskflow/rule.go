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

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/migrate"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type Rule struct {
	Ctx              context.Context     `json:"-"`
	TaskName         string              `json:"taskName"`
	TaskFlow         string              `json:"taskFlow"`
	TaskRuleName     string              `json:"taskRuleName"`
	SchemaNameS      string              `json:"schemaNameS"`
	TableNameS       string              `json:"tableNameS"`
	TablePrimaryKeyS []map[string]string `json:"tablePrimaryKeyS"`
	TableColumnsS    []map[string]string `json:"tableColumnsS"`
	TableCommentS    []map[string]string `json:"tableCommentS"`
	CaseFieldRule    string              `json:"caseFieldRule"`
	DBCollationS     bool                `json:"DBCollationS"`
	DBCharsetS       string              `json:"dbCharsetS"`
	DBCharsetT       string              `json:"dbCharsetT"`
}

func (r *Rule) GetTableNameRule() (map[string]string, map[string]string, error) {
	schemaRoute := make(map[string]string)
	tableRoute := make(map[string]string)
	routeRule, err := model.GetIMigrateTableRouteRW().GetTableRouteRule(r.Ctx, &rule.TableRouteRule{
		TaskRuleName: r.TaskRuleName, SchemaNameS: r.SchemaNameS, TableNameS: r.TableNameS})
	if err != nil {
		return schemaRoute, tableRoute, err
	}
	schemaRoute[r.SchemaNameS] = routeRule.SchemaNameT
	tableRoute[r.TableNameS] = routeRule.TableNameT
	return schemaRoute, tableRoute, nil
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
	buildInDatatypeRules, err := model.GetIBuildInDatatypeRuleRW().QueryBuildInDatatypeRule(r.Ctx, r.TaskFlow)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, c := range r.TableColumnsS {
		columnName := c["COLUMN_NAME"]
		columnRules[columnName] = columnName

		var (
			originColumnType, buildInColumnType string
		)
		// task flow
		switch {
		case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB):
			originColumnType, buildInColumnType, err = DatabaseTableColumnMapMYSQLDatatypeRule(&Column{
				ColumnName:    c["COLUMN_NAME"],
				Datatype:      c["DATA_TYPE"],
				CharUsed:      c["CHAR_USED"],
				CharLength:    c["CHAR_LENGTH"],
				DataPrecision: c["DATA_PRECISION"],
				DataLength:    c["DATA_LENGTH"],
				DataScale:     c["DATA_SCALE"],
				DataDefault:   c["DATA_DEFAULT"],
				Nullable:      c["NULLABLE"],
				Comment:       c["COMMENTS"],
			}, buildInDatatypeRules)
			if err != nil {
				return nil, nil, nil, err
			}
			// priority, return target database table column datatype
			convertColumnDatatype, convertColumnDefaultValue, err := HandleColumnRuleWithPriority(
				c["COLUMN_NAME"],
				originColumnType, buildInColumnType,
				c["DATA_DEFAULT"],
				r.DBCharsetS,
				r.DBCharsetT,
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

	for _, col := range columnRoutes {
		if _, exist := columnRules[col.ColumnNameS]; exist {
			columnRules[col.ColumnNameS] = col.ColumnNameT
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

	// task flow
	if !strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB) {
		logger.Warn("get table rule",
			zap.String("notes", fmt.Sprintf("oracle current task [%s] schema [%s] taskflow [%s] attributes rule isn't support, please contact author", r.TaskName, r.SchemaNameS, r.TaskFlow)),
			zap.String("operator", "ignore"))
		return "", nil
	}

	return attr.TableAttrT, nil
}

func (r *Rule) GetTableCaseFieldRule() string {
	return r.CaseFieldRule
}

func (r *Rule) GetTableCommentRule() (string, error) {
	var tableComment string
	if len(r.TableCommentS) == 0 || strings.EqualFold(r.TableCommentS[0]["COMMENTS"], "") {
		return tableComment, nil
	}

	convertUtf8Raw, err := stringutil.CharsetConvert([]byte(r.TableCommentS[0]["COMMENTS"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
	if err != nil {
		return tableComment, fmt.Errorf("oracle schema [%s] table [%s] comments [%s] charset convert [UTFMB4] failed, error: %v", r.SchemaNameS, r.TableNameS, r.TableCommentS[0]["COMMENTS"], err)
	}

	switch {
	case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
		convertTargetRaw, err := stringutil.CharsetConvert([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase(convertUtf8Raw)), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetT)])
		if err != nil {
			return tableComment, fmt.Errorf("oracle schema [%s] table [%s] comments [%s] charset convert [%s] failed, error: %v", r.SchemaNameS, r.TableNameS, r.TableCommentS[0]["COMMENTS"], r.DBCharsetT, err)
		}
		tableComment = string(convertTargetRaw)

		return tableComment, nil
	default:
		return tableComment, fmt.Errorf("oracle current taskflow [%s] isn't support, please contact author or reselect", r.TaskFlow)
	}
}

func (r *Rule) GetTableColumnCollationRule() (map[string]string, error) {
	columnCollationMap := make(map[string]string)
	for _, rowCol := range r.TableColumnsS {
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

		columnCollationMap[rowCol["COLUMN_NAME"]] = columnCollation
	}
	return columnCollationMap, nil
}

func (r *Rule) GetTableColumnCommentRule() (map[string]string, error) {
	columnCommentMap := make(map[string]string)
	for _, rowCol := range r.TableColumnsS {
		if !strings.EqualFold(rowCol["COMMENTS"], "") {
			convertUtf8Raw, err := stringutil.CharsetConvert([]byte(rowCol["COMMENTS"]), constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetS)], constant.CharsetUTF8MB4)
			if err != nil {
				return columnCommentMap, fmt.Errorf("oracle schema [%s] table [%s] column [%s] comments charset convert failed, %v", r.SchemaNameS, r.TableNameS, rowCol["COLUMN_NAME"], err)
			}

			switch {
			case strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(r.TaskFlow, constant.TaskFlowOracleToMySQL):
				convertTargetRaw, err := stringutil.CharsetConvert([]byte(stringutil.SpecialLettersMySQLCompatibleDatabase(convertUtf8Raw)), constant.CharsetUTF8MB4, constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(r.DBCharsetT)])
				if err != nil {
					return columnCommentMap, fmt.Errorf("column [%s] comments charset convert failed, %v", rowCol["COLUMN_NAME"], err)
				}

				columnCommentMap[rowCol["COLUMN_NAME"]] = "'" + string(convertTargetRaw) + "'"
			default:
				return columnCommentMap, fmt.Errorf("oracle current taskflow [%s] column comment isn't support, please contact author or reselect", r.TaskFlow)
			}
		} else {
			columnCommentMap[rowCol["COLUMN_NAME"]] = rowCol["COMMENTS"]
		}
	}
	return columnCommentMap, nil
}
