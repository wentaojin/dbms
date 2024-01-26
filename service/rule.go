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
package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/database"

	"github.com/wentaojin/dbms/database/oracle"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func UpsertRule(ctx context.Context, taskName, datasourceNameS string, caseFieldRule *pb.CaseFieldRule, schemaRouteRules []*pb.SchemaRouteRule) error {
	sourceDatasource, err := model.GetIDatasourceRW().GetDatasource(ctx, datasourceNameS)
	if err != nil {
		return err
	}

	// exclude table and include table is whether conflict
	var sourceSchemas []string
	for _, sr := range schemaRouteRules {
		if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
			sourceSchemas = append(sourceSchemas, sr.SchemaNameS)
		}
		if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
			sourceSchemas = append(sourceSchemas, stringutil.StringUpper(sr.SchemaNameS))
		}
		if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
			sourceSchemas = append(sourceSchemas, stringutil.StringLower(sr.SchemaNameS))
		}
		interSection := stringutil.StringItemsFilterIntersection(sr.IncludeTableS, sr.ExcludeTableS)
		if len(interSection) > 0 {
			return fmt.Errorf("there is the same table within source include and exclude table, please check and remove")
		}
	}

	switch {
	case strings.EqualFold(sourceDatasource.DbType, constant.DatabaseTypeOracle):
		sourceDB, errN := database.NewDatabase(ctx, sourceDatasource, "")
		if errN != nil {
			return err
		}
		defer sourceDB.Close()
		allOraSchemas, err := sourceDB.(*oracle.Database).FilterDatabaseSchema()
		if err != nil {
			return err
		}
		for _, s := range sourceSchemas {
			if !stringutil.IsContainedString(allOraSchemas, s) {
				return fmt.Errorf("oracle schema isn't contained in the database with the case field rule, failed schemas: [%v]", s)
			}
		}
	default:
		return fmt.Errorf("the current datasource type [%s] isn't support, please remove and reruning", sourceDatasource.DbType)
	}

	for _, sr := range schemaRouteRules {
		if !strings.EqualFold(sr.String(), "") {
			err = model.Transaction(ctx, func(txnCtx context.Context) error {
				var (
					sourceSchema  string
					targetSchema  string
					includeTableS []string
					excludeTableS []string
				)
				if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
					sourceSchema = sr.SchemaNameS
					includeTableS = sr.IncludeTableS
					excludeTableS = sr.ExcludeTableS
				}
				if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameOrigin) {
					targetSchema = sr.SchemaNameT
				}
				if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
					sourceSchema = stringutil.StringUpper(sr.SchemaNameS)
					for _, t := range sr.IncludeTableS {
						includeTableS = append(includeTableS, stringutil.StringUpper(t))
					}
					for _, t := range sr.ExcludeTableS {
						excludeTableS = append(excludeTableS, stringutil.StringUpper(t))
					}
				}
				if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
					targetSchema = stringutil.StringUpper(sr.SchemaNameT)
				}
				if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
					sourceSchema = stringutil.StringLower(sr.SchemaNameS)
					for _, t := range sr.IncludeTableS {
						includeTableS = append(includeTableS, stringutil.StringLower(t))
					}
					for _, t := range sr.ExcludeTableS {
						excludeTableS = append(excludeTableS, stringutil.StringLower(t))
					}
				}
				if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameLower) {
					targetSchema = stringutil.StringLower(sr.SchemaNameT)
				}

				for _, t := range includeTableS {
					_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
						TaskName:    taskName,
						SchemaNameS: sourceSchema,
						TableNameS:  t,
						IsExclude:   constant.MigrateTaskTableIsNotExclude,
					})
				}

				for _, t := range excludeTableS {
					_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
						TaskName:    taskName,
						SchemaNameS: sourceSchema,
						TableNameS:  t,
						IsExclude:   constant.MigrateTaskTableIsExclude,
					})
				}

				_, err = model.GetIMigrateSchemaRouteRW().CreateSchemaRouteRule(txnCtx, &rule.SchemaRouteRule{
					TaskName:    taskName,
					SchemaNameS: sourceSchema,
					SchemaNameT: targetSchema,
				})
				if err != nil {
					return err
				}

				var (
					tableRules  []*rule.TableRouteRule
					columnRules []*rule.ColumnRouteRule
				)
				for _, st := range sr.TableRouteRules {
					if !strings.EqualFold(st.String(), "") {
						if strings.EqualFold(st.TableNameT, "") {
							st.TableNameT = st.TableNameS
						}

						var (
							sourceTable string
							targetTable string
						)
						columnRouteRuleMap := make(map[string]string)

						if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
							sourceTable = st.TableNameS
						}
						if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameOrigin) {
							targetTable = st.TableNameT
						}

						if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
							sourceTable = stringutil.StringUpper(st.TableNameS)
							for k, v := range st.ColumnRouteRules {
								columnRouteRuleMap[stringutil.StringUpper(k)] = v
							}
						}
						if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
							targetTable = stringutil.StringUpper(st.TableNameT)
							for k, v := range columnRouteRuleMap {
								columnRouteRuleMap[k] = stringutil.StringUpper(v)
							}
						}
						if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
							sourceTable = stringutil.StringLower(st.TableNameS)
							for k, v := range st.ColumnRouteRules {
								columnRouteRuleMap[stringutil.StringLower(k)] = v
							}
						}
						if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameLower) {
							targetTable = stringutil.StringLower(st.TableNameT)
							for k, v := range columnRouteRuleMap {
								columnRouteRuleMap[k] = stringutil.StringLower(v)
							}
						}

						if columnRouteRuleMap == nil {
							columnRouteRuleMap = st.ColumnRouteRules
						}

						for k, v := range columnRouteRuleMap {
							columnRules = append(columnRules, &rule.ColumnRouteRule{
								TaskName:    taskName,
								SchemaNameS: sourceSchema,
								TableNameS:  sourceTable,
								ColumnNameS: k,
								SchemaNameT: targetSchema,
								TableNameT:  targetTable,
								ColumnNameT: v,
							})
						}

						tableRules = append(tableRules, &rule.TableRouteRule{
							TaskName:    taskName,
							SchemaNameS: sourceSchema,
							TableNameS:  sourceTable,
							SchemaNameT: targetSchema,
							TableNameT:  targetTable,
						})
					}
				}

				_, err = model.GetIMigrateTableRouteRW().CreateInBatchTableRouteRule(ctx, tableRules, constant.DefaultRecordCreateBatchSize)
				if err != nil {
					return err
				}

				_, err = model.GetIMigrateColumnRouteRW().CreateInBatchColumnRouteRule(ctx, columnRules, constant.DefaultRecordCreateBatchSize)
				if err != nil {
					return err
				}

				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func DeleteRule(ctx context.Context, taskName []string) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		err := model.GetIMigrateSchemaRouteRW().DeleteSchemaRouteRule(txnCtx, taskName)
		if err != nil {
			return err
		}

		err = model.GetIMigrateTaskTableRW().DeleteMigrateTaskTable(txnCtx, taskName)
		if err != nil {
			return err
		}

		err = model.GetIMigrateTableRouteRW().DeleteTableRouteRule(txnCtx, taskName)
		if err != nil {
			return err
		}

		err = model.GetIMigrateColumnRouteRW().DeleteColumnRouteRule(txnCtx, taskName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func ShowRule(ctx context.Context, taskName string) ([]*pb.SchemaRouteRule, error) {
	var opSchemaRules []*pb.SchemaRouteRule

	tables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(ctx, &rule.MigrateTaskTable{TaskName: taskName})
	if err != nil {
		return opSchemaRules, err
	}

	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		schemaRules, err := model.GetIMigrateSchemaRouteRW().FindSchemaRouteRule(txnCtx, &rule.SchemaRouteRule{TaskName: taskName})
		if err != nil {
			return err
		}

		for _, sr := range schemaRules {
			var (
				opColumnRules []*pb.TableRouteRule

				includeTables []string
				excludeTables []string
			)
			opSchemaRule := &pb.SchemaRouteRule{}
			opColumnRule := &pb.TableRouteRule{}

			opSchemaRule.SchemaNameS = sr.SchemaNameS
			opSchemaRule.SchemaNameT = sr.SchemaNameT

			for _, t := range tables {
				if t.SchemaNameS == sr.SchemaNameS && t.IsExclude == constant.MigrateTaskTableIsNotExclude {
					includeTables = append(includeTables, t.TableNameS)
				}
				if t.SchemaNameS == sr.SchemaNameS && t.IsExclude != constant.MigrateTaskTableIsExclude {
					excludeTables = append(excludeTables, t.TableNameS)
				}
			}

			opSchemaRule.IncludeTableS = includeTables
			opSchemaRule.ExcludeTableS = excludeTables

			tableRules, err := model.GetIMigrateTableRouteRW().FindTableRouteRule(txnCtx, &rule.TableRouteRule{TaskName: taskName, SchemaNameS: sr.SchemaNameS})
			if err != nil {
				return err
			}

			for _, st := range tableRules {
				opColumnRule.TableNameS = st.TableNameS
				opColumnRule.TableNameT = st.TableNameT

				columnRules, err := model.GetIMigrateColumnRouteRW().FindColumnRouteRule(txnCtx, &rule.ColumnRouteRule{TaskName: taskName, SchemaNameS: st.SchemaNameS, TableNameS: st.TableNameS})
				if err != nil {
					return err
				}

				columnRuleMap := make(map[string]string)
				for _, sc := range columnRules {
					columnRuleMap[sc.ColumnNameS] = sc.ColumnNameT
				}

				opColumnRule.ColumnRouteRules = columnRuleMap

				opColumnRules = append(opColumnRules, opColumnRule)
			}

			opSchemaRule.TableRouteRules = opColumnRules

			opSchemaRules = append(opSchemaRules, opSchemaRule)
		}

		return nil
	})
	if err != nil {
		return opSchemaRules, err
	}

	return opSchemaRules, nil
}
