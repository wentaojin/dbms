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

	"github.com/wentaojin/dbms/database/oracle"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/common"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func UpsertMigrateTaskRule(ctx context.Context, req *pb.UpsertMigrateTaskRuleRequest) (string, error) {
	sourceDatasource, err := model.GetIDatasourceRW().GetDatasource(ctx, req.MigrateTaskRule.DatasourceNameS)
	if err != nil {
		return "", err
	}

	sourceDB, errN := database.NewDatabase(ctx, sourceDatasource, "")
	if errN != nil {
		return "", err
	}

	// exclude table and include table is whether conflict
	var sourceSchemas []string
	for _, sr := range req.MigrateTaskRule.SchemaRouteRules {
		sourceSchemas = append(sourceSchemas, sr.SourceSchema)

		interSection := stringutil.StringItemsFilterIntersection(sr.SourceIncludeTable, sr.SourceExcludeTable)
		if len(interSection) > 0 {
			return "", fmt.Errorf("there is the same table within source include and exclude table, please check and remove")
		}
	}

	switch {
	case strings.EqualFold(sourceDatasource.DbType, constant.DatabaseTypeOracle):
		err = sourceDB.(*oracle.Database).FilterDatabaseSchema(sourceSchemas)
		if err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("the current datasource type [%s] isn't support, please remove and reruning", sourceDatasource.DbType)
	}

	err = sourceDB.Close()
	if err != nil {
		return "", err
	}

	for _, sr := range req.MigrateTaskRule.SchemaRouteRules {
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			_, err = model.GetIMigrateTaskRuleRW().CreateMigrateTaskRule(txnCtx, &rule.MigrateTaskRule{
				TaskRuleName:    req.MigrateTaskRule.TaskRuleName,
				DatasourceNameS: req.MigrateTaskRule.DatasourceNameS,
				DatasourceNameT: req.MigrateTaskRule.DatasourceNameT,
				Entity:          &common.Entity{Comment: req.MigrateTaskRule.Comment},
			})
			if err != nil {
				return err
			}

			_, err = model.GetIMigrateSchemaRouteRW().CreateSchemaRouteRule(txnCtx, &rule.SchemaRouteRule{
				TaskRuleName: req.MigrateTaskRule.TaskRuleName,
				SchemaNameS:  stringutil.StringUpper(sr.SourceSchema),
				SchemaNameT:  stringutil.StringUpper(sr.TargetSchema),
				Entity:       &common.Entity{Comment: req.MigrateTaskRule.Comment},
			})
			if err != nil {
				return err
			}

			for _, t := range sr.SourceIncludeTable {
				_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
					TaskRuleName: req.MigrateTaskRule.TaskRuleName,
					SchemaNameS:  stringutil.StringUpper(sr.SourceSchema),
					TableNameS:   stringutil.StringUpper(t),
					IsExclude:    constant.MigrateTaskTableIsNotExclude,
				})
			}

			for _, t := range sr.SourceExcludeTable {
				_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
					TaskRuleName: req.MigrateTaskRule.TaskRuleName,
					SchemaNameS:  stringutil.StringUpper(sr.SourceSchema),
					TableNameS:   stringutil.StringUpper(t),
					IsExclude:    constant.MigrateTaskTableIsExclude,
				})
			}

			var (
				tableRules  []*rule.TableRouteRule
				columnRules []*rule.ColumnRouteRule
			)
			for _, st := range sr.TableRouteRules {
				if strings.EqualFold(st.TargetTable, "") {
					st.TargetTable = st.SourceTable
				}
				tableRules = append(tableRules, &rule.TableRouteRule{
					TaskRuleName: req.MigrateTaskRule.TaskRuleName,
					SchemaNameS:  stringutil.StringUpper(sr.SourceSchema),
					TableNameS:   stringutil.StringUpper(st.SourceTable),
					SchemaNameT:  stringutil.StringUpper(sr.TargetSchema),
					TableNameT:   stringutil.StringUpper(st.TargetTable),
					Entity:       &common.Entity{Comment: req.MigrateTaskRule.Comment},
				})
				if st.ColumnRouteRules != nil {
					for k, v := range st.ColumnRouteRules {
						columnRules = append(columnRules, &rule.ColumnRouteRule{
							TaskRuleName: req.MigrateTaskRule.TaskRuleName,
							SchemaNameS:  stringutil.StringUpper(sr.SourceSchema),
							TableNameS:   stringutil.StringUpper(st.SourceTable),
							SchemaNameT:  stringutil.StringUpper(sr.TargetSchema),
							TableNameT:   stringutil.StringUpper(st.TargetTable),
							ColumnNameS:  stringutil.StringUpper(k),
							ColumnNameT:  stringutil.StringUpper(v),
							Entity:       &common.Entity{Comment: req.MigrateTaskRule.Comment},
						})
					}
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

	jsonStr, err := stringutil.MarshalJSON(req)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func DeleteMigrateTaskRule(ctx context.Context, req *pb.DeleteMigrateTaskRuleRequest) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		err := model.GetIMigrateTaskRuleRW().DeleteMigrateTaskRule(txnCtx, req.TaskRuleName)
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

func ShowMigrateTaskRule(ctx context.Context, req *pb.ShowMigrateTaskRuleRequest) (string, error) {
	var (
		opRules       *pb.MigrateTaskRule
		opSchemaRules []*pb.SchemaRouteRule
	)

	tables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(ctx, &rule.MigrateTaskTable{TaskRuleName: req.TaskRuleName})
	if err != nil {
		return "", err
	}

	err = model.Transaction(ctx, func(txnCtx context.Context) error {

		taskRule, err := model.GetIMigrateTaskRuleRW().GetMigrateTaskRule(txnCtx, &rule.MigrateTaskRule{TaskRuleName: req.TaskRuleName})
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
				opSchemaRule *pb.SchemaRouteRule
				opColumnRule *pb.TableRouteRule

				opColumnRules []*pb.TableRouteRule

				includeTables []string
				excludeTables []string
			)

			opSchemaRule.SourceSchema = sr.SchemaNameS
			opSchemaRule.TargetSchema = sr.SchemaNameT

			for _, t := range tables {
				if strings.EqualFold(t.SchemaNameS, sr.SchemaNameS) && strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsNotExclude) {
					includeTables = append(includeTables, t.TableNameS)
				}
				if strings.EqualFold(t.SchemaNameS, sr.SchemaNameS) && !strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsExclude) {
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
