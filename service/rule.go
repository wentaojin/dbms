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

	"github.com/wentaojin/dbms/model/common"

	"github.com/wentaojin/dbms/database/oracle"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func UpsertRule(ctx context.Context, req *pb.UpsertRuleRequest) (string, error) {
	sourceDatasource, err := model.GetIDatasourceRW().GetDatasource(ctx, req.Rule.DatasourceNameS)
	if err != nil {
		return "", err
	}

	sourceDB, errN := database.NewDatabase(ctx, sourceDatasource, "")
	if errN != nil {
		return "", err
	}

	// exclude table and include table is whether conflict
	var sourceSchemas []string
	for _, sr := range req.Rule.SchemaRouteRules {
		if strings.EqualFold(sr.CaseFieldRule, constant.ParamValueRuleCaseFieldNameOrigin) {
			sourceSchemas = append(sourceSchemas, sr.SourceSchema)
		}
		if strings.EqualFold(sr.CaseFieldRule, constant.ParamValueRuleCaseFieldNameUpper) {
			sourceSchemas = append(sourceSchemas, stringutil.StringUpper(sr.SourceSchema))
		}
		if strings.EqualFold(sr.CaseFieldRule, constant.ParamValueRuleCaseFieldNameLower) {
			sourceSchemas = append(sourceSchemas, stringutil.StringLower(sr.SourceSchema))
		}
		interSection := stringutil.StringItemsFilterIntersection(sr.SourceIncludeTable, sr.SourceExcludeTable)
		if len(interSection) > 0 {
			return "", fmt.Errorf("there is the same table within source include and exclude table, please check and remove")
		}
	}

	switch {
	case strings.EqualFold(sourceDatasource.DbType, constant.DatabaseTypeOracle):
		allOraSchemas, err := sourceDB.(*oracle.Database).FilterDatabaseSchema()
		if err != nil {
			return "", err
		}
		for _, s := range sourceSchemas {
			if !stringutil.IsContainedString(allOraSchemas, s) {
				return "", fmt.Errorf("oracle schema isn't contained in the database with the case field rule, failed schemas: [%v]", s)
			}
		}
	default:
		return "", fmt.Errorf("the current datasource type [%s] isn't support, please remove and reruning", sourceDatasource.DbType)
	}

	err = sourceDB.Close()
	if err != nil {
		return "", err
	}

	for _, sr := range req.Rule.SchemaRouteRules {
		if !strings.EqualFold(sr.String(), "") {
			err = model.Transaction(ctx, func(txnCtx context.Context) error {
				_, err = model.GetIRuleRW().CreateRule(txnCtx, &rule.Rule{
					TaskRuleName:    req.Rule.TaskRuleName,
					DatasourceNameS: req.Rule.DatasourceNameS,
					DatasourceNameT: req.Rule.DatasourceNameT,
					Entity:          &common.Entity{Comment: req.Rule.Comment},
				})
				if err != nil {
					return err
				}

				var (
					sourceSchema string
					targetSchema string
				)
				if strings.EqualFold(sr.CaseFieldRule, constant.ParamValueRuleCaseFieldNameOrigin) {
					sourceSchema = sr.SourceSchema
					targetSchema = sr.TargetSchema
					for _, t := range sr.SourceIncludeTable {
						_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
							TaskRuleName:  req.Rule.TaskRuleName,
							SchemaNameS:   sourceSchema,
							TableNameS:    t,
							CaseFieldRule: sr.CaseFieldRule,
							IsExclude:     constant.MigrateTaskTableIsNotExclude,
						})
					}

					for _, t := range sr.SourceExcludeTable {
						_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
							TaskRuleName:  req.Rule.TaskRuleName,
							SchemaNameS:   sourceSchema,
							TableNameS:    t,
							CaseFieldRule: sr.CaseFieldRule,
							IsExclude:     constant.MigrateTaskTableIsExclude,
						})
					}
				}
				if strings.EqualFold(sr.CaseFieldRule, constant.ParamValueRuleCaseFieldNameUpper) {
					sourceSchema = stringutil.StringUpper(sr.SourceSchema)
					targetSchema = stringutil.StringUpper(sr.TargetSchema)
					for _, t := range sr.SourceIncludeTable {
						_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
							TaskRuleName:  req.Rule.TaskRuleName,
							SchemaNameS:   sourceSchema,
							TableNameS:    stringutil.StringUpper(t),
							CaseFieldRule: sr.CaseFieldRule,
							IsExclude:     constant.MigrateTaskTableIsNotExclude,
						})
					}

					for _, t := range sr.SourceExcludeTable {
						_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
							TaskRuleName:  req.Rule.TaskRuleName,
							SchemaNameS:   sourceSchema,
							TableNameS:    stringutil.StringUpper(t),
							CaseFieldRule: sr.CaseFieldRule,
							IsExclude:     constant.MigrateTaskTableIsExclude,
						})
					}
				}
				if strings.EqualFold(sr.CaseFieldRule, constant.ParamValueRuleCaseFieldNameLower) {
					sourceSchema = stringutil.StringLower(sr.SourceSchema)
					targetSchema = stringutil.StringLower(sr.TargetSchema)
					for _, t := range sr.SourceIncludeTable {
						_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
							TaskRuleName:  req.Rule.TaskRuleName,
							SchemaNameS:   sourceSchema,
							TableNameS:    stringutil.StringLower(t),
							CaseFieldRule: sr.CaseFieldRule,
							IsExclude:     constant.MigrateTaskTableIsNotExclude,
						})
					}

					for _, t := range sr.SourceExcludeTable {
						_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
							TaskRuleName:  req.Rule.TaskRuleName,
							SchemaNameS:   sourceSchema,
							TableNameS:    stringutil.StringLower(t),
							CaseFieldRule: sr.CaseFieldRule,
							IsExclude:     constant.MigrateTaskTableIsExclude,
						})
					}
				}

				_, err = model.GetIMigrateSchemaRouteRW().CreateSchemaRouteRule(txnCtx, &rule.SchemaRouteRule{
					TaskRuleName:  req.Rule.TaskRuleName,
					SchemaNameS:   sourceSchema,
					SchemaNameT:   targetSchema,
					CaseFieldRule: sr.CaseFieldRule,
					Entity:        &common.Entity{Comment: req.Rule.Comment},
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
						if strings.EqualFold(st.TargetTable, "") {
							st.TargetTable = st.SourceTable
						}

						var (
							sourceTable string
							targetTable string
						)
						if strings.EqualFold(sr.CaseFieldRule, constant.ParamValueRuleCaseFieldNameOrigin) {
							sourceTable = st.SourceTable
							targetTable = st.TargetTable
							if st.ColumnRouteRules != nil {
								for k, v := range st.ColumnRouteRules {
									columnRules = append(columnRules, &rule.ColumnRouteRule{
										TaskRuleName:  req.Rule.TaskRuleName,
										SchemaNameS:   sourceSchema,
										TableNameS:    sourceTable,
										SchemaNameT:   targetSchema,
										TableNameT:    targetTable,
										ColumnNameS:   k,
										ColumnNameT:   v,
										CaseFieldRule: st.CaseFieldRule,
										Entity:        &common.Entity{Comment: req.Rule.Comment},
									})
								}
							}
						}
						if strings.EqualFold(sr.CaseFieldRule, constant.ParamValueRuleCaseFieldNameUpper) {
							sourceTable = stringutil.StringUpper(st.SourceTable)
							targetTable = stringutil.StringUpper(st.TargetTable)
							if st.ColumnRouteRules != nil {
								for k, v := range st.ColumnRouteRules {
									columnRules = append(columnRules, &rule.ColumnRouteRule{
										TaskRuleName:  req.Rule.TaskRuleName,
										SchemaNameS:   sourceSchema,
										TableNameS:    sourceTable,
										SchemaNameT:   targetSchema,
										TableNameT:    targetTable,
										ColumnNameS:   stringutil.StringUpper(k),
										ColumnNameT:   stringutil.StringUpper(v),
										CaseFieldRule: st.CaseFieldRule,
										Entity:        &common.Entity{Comment: req.Rule.Comment},
									})
								}
							}
						}
						if strings.EqualFold(sr.CaseFieldRule, constant.ParamValueRuleCaseFieldNameLower) {
							sourceTable = stringutil.StringLower(st.SourceTable)
							targetTable = stringutil.StringLower(st.TargetTable)
							if st.ColumnRouteRules != nil {
								for k, v := range st.ColumnRouteRules {
									columnRules = append(columnRules, &rule.ColumnRouteRule{
										TaskRuleName:  req.Rule.TaskRuleName,
										SchemaNameS:   sourceSchema,
										TableNameS:    sourceTable,
										SchemaNameT:   targetSchema,
										TableNameT:    targetTable,
										ColumnNameS:   stringutil.StringLower(k),
										ColumnNameT:   stringutil.StringLower(v),
										CaseFieldRule: st.CaseFieldRule,
										Entity:        &common.Entity{Comment: req.Rule.Comment},
									})
								}
							}
						}

						tableRules = append(tableRules, &rule.TableRouteRule{
							TaskRuleName:  req.Rule.TaskRuleName,
							SchemaNameS:   sourceSchema,
							TableNameS:    sourceTable,
							SchemaNameT:   targetSchema,
							TableNameT:    targetTable,
							CaseFieldRule: st.CaseFieldRule,
							Entity:        &common.Entity{Comment: req.Rule.Comment},
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
				return "", err
			}
		}
	}

	jsonStr, err := stringutil.MarshalJSON(req)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func DeleteRule(ctx context.Context, req *pb.DeleteRuleRequest) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		err := model.GetIRuleRW().DeleteRule(txnCtx, req.TaskRuleName)
		if err != nil {
			return err
		}

		err = model.GetIMigrateSchemaRouteRW().DeleteSchemaRouteRule(txnCtx, req.TaskRuleName)
		if err != nil {
			return err
		}

		err = model.GetIMigrateTaskTableRW().DeleteMigrateTaskTable(txnCtx, req.TaskRuleName)
		if err != nil {
			return err
		}

		err = model.GetIMigrateTableRouteRW().DeleteTableRouteRule(txnCtx, req.TaskRuleName)
		if err != nil {
			return err
		}

		err = model.GetIMigrateColumnRouteRW().DeleteColumnRouteRule(txnCtx, req.TaskRuleName)
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

func ShowRule(ctx context.Context, req *pb.ShowRuleRequest) (string, error) {
	var opSchemaRules []*pb.SchemaRouteRule
	opRules := &pb.Rule{}

	tables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(ctx, &rule.MigrateTaskTable{TaskRuleName: req.TaskRuleName})
	if err != nil {
		return "", err
	}

	err = model.Transaction(ctx, func(txnCtx context.Context) error {

		taskRule, err := model.GetIRuleRW().GetRule(txnCtx, &rule.Rule{TaskRuleName: req.TaskRuleName})
		if err != nil {
			return err
		}
		opRules.TaskRuleName = taskRule.TaskRuleName
		opRules.DatasourceNameS = taskRule.DatasourceNameS
		opRules.DatasourceNameT = taskRule.DatasourceNameT
		opRules.Comment = taskRule.Comment

		schemaRules, err := model.GetIMigrateSchemaRouteRW().FindSchemaRouteRule(txnCtx, &rule.SchemaRouteRule{TaskRuleName: req.TaskRuleName})
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

			opSchemaRule.SourceSchema = sr.SchemaNameS
			opSchemaRule.TargetSchema = sr.SchemaNameT

			for _, t := range tables {
				if t.SchemaNameS == sr.SchemaNameS && t.IsExclude == constant.MigrateTaskTableIsNotExclude {
					includeTables = append(includeTables, t.TableNameS)
				}
				if t.SchemaNameS == sr.SchemaNameS && t.IsExclude != constant.MigrateTaskTableIsExclude {
					excludeTables = append(excludeTables, t.TableNameS)
				}
			}

			opSchemaRule.SourceIncludeTable = includeTables
			opSchemaRule.SourceExcludeTable = excludeTables

			tableRules, err := model.GetIMigrateTableRouteRW().FindTableRouteRule(txnCtx, &rule.TableRouteRule{TaskRuleName: req.TaskRuleName, SchemaNameS: sr.SchemaNameS})
			if err != nil {
				return err
			}

			for _, st := range tableRules {
				opColumnRule.SourceTable = st.TableNameS
				opColumnRule.TargetTable = st.TableNameT
				opColumnRule.CaseFieldRule = st.CaseFieldRule

				columnRules, err := model.GetIMigrateColumnRouteRW().FindColumnRouteRule(txnCtx, &rule.ColumnRouteRule{TaskRuleName: req.TaskRuleName, SchemaNameS: st.SchemaNameS, TableNameS: st.TableNameS})
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

		opRules.SchemaRouteRules = opSchemaRules
		return nil
	})
	if err != nil {
		return "", err
	}

	jsonStr, err := stringutil.MarshalIndentJSON(opRules)
	if err != nil {
		return "", err
	}
	return jsonStr, nil
}
