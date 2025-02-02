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
	"strconv"
	"strings"

	"github.com/wentaojin/dbms/database"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

func UpsertSchemaRouteRule(ctx context.Context, taskName, datasourceNameS string, caseFieldRule *pb.CaseFieldRule, schemaRouteRule *pb.SchemaRouteRule,
	dataMigrateRules []*pb.DataMigrateRule, dataCompareRules []*pb.DataCompareRule) error {
	datasourceS, err := model.GetIDatasourceRW().GetDatasource(ctx, datasourceNameS)
	if err != nil {
		return err
	}

	if strings.EqualFold(datasourceNameS, "") {
		return fmt.Errorf("the datasource %s is empty, please check the datasource whether is exists", datasourceNameS)
	}

	// exclude table and include table is whether conflict
	var (
		sourceSchemas string
		sourceIncls   []string
		sourceExcls   []string
	)
	if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
		sourceSchemas = schemaRouteRule.SchemaNameS
		sourceIncls = schemaRouteRule.IncludeTableS
		sourceExcls = schemaRouteRule.ExcludeTableS
	}
	if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
		sourceSchemas = stringutil.StringUpper(schemaRouteRule.SchemaNameS)
		for _, t := range schemaRouteRule.IncludeTableS {
			sourceIncls = append(sourceIncls, stringutil.StringUpper(t))
		}
		for _, t := range schemaRouteRule.ExcludeTableS {
			sourceExcls = append(sourceExcls, stringutil.StringUpper(t))
		}
	}
	if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
		sourceSchemas = stringutil.StringLower(schemaRouteRule.SchemaNameS)
		for _, t := range schemaRouteRule.IncludeTableS {
			sourceIncls = append(sourceIncls, stringutil.StringLower(t))
		}
		for _, t := range schemaRouteRule.ExcludeTableS {
			sourceExcls = append(sourceExcls, stringutil.StringLower(t))
		}
	}
	interSection := stringutil.StringItemsFilterIntersection(sourceIncls, sourceExcls)
	if len(interSection) > 0 {
		return fmt.Errorf("there is the same table within source include and exclude table, please check and remove")
	}

	var databaseS database.IDatabase

	switch {
	case strings.EqualFold(datasourceS.DbType, constant.DatabaseTypeOracle):
		databaseS, err = database.NewDatabase(ctx, datasourceS, sourceSchemas, constant.ServiceDatabaseSqlQueryCallTimeout)
		if err != nil {
			return err
		}
	case strings.EqualFold(datasourceS.DbType, constant.DatabaseTypeTiDB) || strings.EqualFold(datasourceS.DbType, constant.DatabaseTypeMySQL) || strings.EqualFold(datasourceS.DbType, constant.DatabaseTypePostgresql) || strings.EqualFold(datasourceS.DbType, constant.DatabaseTypeOceanbaseMYSQL):
		databaseS, err = database.NewDatabase(ctx, datasourceS, "", constant.ServiceDatabaseSqlQueryCallTimeout)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("the task [%s] datasource [%s] database type [%s] is not supported, please contact author or reselect", taskName, datasourceNameS, datasourceS.DbType)
	}

	allOraSchemas, err := databaseS.GetDatabaseSchema()
	if err != nil {
		return err
	}
	if !stringutil.IsContainedString(allOraSchemas, sourceSchemas) {
		return fmt.Errorf("the schema isn't contained in the database with the case field rule, failed schemas: [%v]", sourceSchemas)
	}

	if !strings.EqualFold(schemaRouteRule.String(), "") {
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			var (
				sourceSchema  string
				targetSchema  string
				includeTableS []string
				excludeTableS []string
				includeSeqS   []string
				excludeSeqS   []string
			)
			if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
				sourceSchema = schemaRouteRule.SchemaNameS
				includeTableS = schemaRouteRule.IncludeTableS
				excludeTableS = schemaRouteRule.ExcludeTableS
				includeSeqS = schemaRouteRule.IncludeSequenceS
				excludeSeqS = schemaRouteRule.ExcludeSequenceS
			}
			if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameOrigin) {
				targetSchema = schemaRouteRule.SchemaNameT
			}
			if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
				sourceSchema = stringutil.StringUpper(schemaRouteRule.SchemaNameS)
				for _, t := range schemaRouteRule.IncludeTableS {
					includeTableS = append(includeTableS, stringutil.StringUpper(t))
				}
				for _, t := range schemaRouteRule.ExcludeTableS {
					excludeTableS = append(excludeTableS, stringutil.StringUpper(t))
				}
				for _, t := range schemaRouteRule.IncludeSequenceS {
					includeSeqS = append(includeSeqS, stringutil.StringUpper(t))
				}
				for _, t := range schemaRouteRule.ExcludeSequenceS {
					excludeSeqS = append(excludeSeqS, stringutil.StringUpper(t))
				}
			}
			if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
				targetSchema = stringutil.StringUpper(schemaRouteRule.SchemaNameT)
			}
			if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
				sourceSchema = stringutil.StringLower(schemaRouteRule.SchemaNameS)
				for _, t := range schemaRouteRule.IncludeTableS {
					includeTableS = append(includeTableS, stringutil.StringLower(t))
				}
				for _, t := range schemaRouteRule.ExcludeTableS {
					excludeTableS = append(excludeTableS, stringutil.StringLower(t))
				}
				for _, t := range schemaRouteRule.IncludeSequenceS {
					includeSeqS = append(includeSeqS, stringutil.StringLower(t))
				}
				for _, t := range schemaRouteRule.ExcludeSequenceS {
					excludeSeqS = append(excludeSeqS, stringutil.StringLower(t))
				}
			}
			if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameLower) {
				targetSchema = stringutil.StringLower(schemaRouteRule.SchemaNameT)
			}

			if len(includeTableS) > 0 && len(excludeTableS) > 0 {
				return fmt.Errorf("source config params include-table-s/exclude-table-s cannot exist at the same time")
			}

			if len(includeTableS) == 0 {
				err = model.GetIMigrateTaskTableRW().DeleteMigrateTaskTableByTaskIsExclude(txnCtx, &rule.MigrateTaskTable{
					TaskName:  taskName,
					IsExclude: constant.MigrateTaskTableIsNotExclude,
				})
				if err != nil {
					return err
				}
			}
			for _, t := range includeTableS {
				_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
					TaskName:    taskName,
					SchemaNameS: sourceSchema,
					TableNameS:  t,
					IsExclude:   constant.MigrateTaskTableIsNotExclude,
				})
				if err != nil {
					return err
				}
			}
			if len(excludeTableS) == 0 {
				err = model.GetIMigrateTaskTableRW().DeleteMigrateTaskTableByTaskIsExclude(txnCtx, &rule.MigrateTaskTable{
					TaskName:  taskName,
					IsExclude: constant.MigrateTaskTableIsExclude,
				})
				if err != nil {
					return err
				}
			}
			for _, t := range excludeTableS {
				_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
					TaskName:    taskName,
					SchemaNameS: sourceSchema,
					TableNameS:  t,
					IsExclude:   constant.MigrateTaskTableIsExclude,
				})
			}
			if len(includeSeqS) == 0 {
				err = model.GetIMigrateTaskSequenceRW().DeleteMigrateTaskSequenceByTaskIsExclude(txnCtx, &rule.MigrateTaskSequence{
					TaskName:  taskName,
					IsExclude: constant.MigrateTaskTableIsNotExclude,
				})
				if err != nil {
					return err
				}
			}
			for _, t := range includeSeqS {
				_, err = model.GetIMigrateTaskSequenceRW().CreateMigrateTaskSequence(txnCtx, &rule.MigrateTaskSequence{
					TaskName:      taskName,
					SchemaNameS:   sourceSchema,
					SequenceNameS: t,
					IsExclude:     constant.MigrateTaskTableIsNotExclude,
				})
			}
			if len(excludeSeqS) == 0 {
				err = model.GetIMigrateTaskSequenceRW().DeleteMigrateTaskSequenceByTaskIsExclude(txnCtx, &rule.MigrateTaskSequence{
					TaskName:  taskName,
					IsExclude: constant.MigrateTaskTableIsExclude,
				})
				if err != nil {
					return err
				}
			}
			for _, t := range excludeSeqS {
				_, err = model.GetIMigrateTaskSequenceRW().CreateMigrateTaskSequence(txnCtx, &rule.MigrateTaskSequence{
					TaskName:      taskName,
					SchemaNameS:   sourceSchema,
					SequenceNameS: t,
					IsExclude:     constant.MigrateTaskTableIsExclude,
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
				tableRules        []*rule.TableRouteRule
				columnRules       []*rule.ColumnRouteRule
				tableMigrateRules []*rule.DataMigrateRule
				tableCompareRules []*rule.DataCompareRule
			)
			for _, st := range schemaRouteRule.TableRouteRules {
				if !strings.EqualFold(st.String(), "") {
					if strings.EqualFold(st.TableNameT, "") {
						st.TableNameT = st.TableNameS
					}

					var (
						sourceTable string
						targetTable string
					)

					if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameOrigin) {
						targetTable = st.TableNameT
					}

					if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
						targetTable = stringutil.StringUpper(st.TableNameT)
						for k, v := range st.ColumnRouteRules {
							if _, ok := st.ColumnRouteRules[k]; ok {
								st.ColumnRouteRules[k] = stringutil.StringUpper(v)
							}
						}
					}
					if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameLower) {
						targetTable = stringutil.StringLower(st.TableNameT)
						for k, v := range st.ColumnRouteRules {
							st.ColumnRouteRules[k] = stringutil.StringLower(v)
						}
					}

					if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
						sourceTable = st.TableNameS
					}
					if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
						sourceTable = stringutil.StringUpper(st.TableNameS)
						for k, v := range st.ColumnRouteRules {
							st.ColumnRouteRules[stringutil.StringUpper(k)] = v
							delete(st.ColumnRouteRules, k)
						}
					}
					if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
						sourceTable = stringutil.StringLower(st.TableNameS)
						for k, v := range st.ColumnRouteRules {
							st.ColumnRouteRules[stringutil.StringLower(k)] = v
							delete(st.ColumnRouteRules, k)
						}
					}

					for k, v := range st.ColumnRouteRules {
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

			_, err = model.GetIMigrateTableRouteRW().CreateInBatchTableRouteRule(ctx, tableRules, constant.DefaultRecordCreateWriteThread, constant.DefaultRecordCreateBatchSize)
			if err != nil {
				return err
			}

			_, err = model.GetIMigrateColumnRouteRW().CreateInBatchColumnRouteRule(ctx, columnRules, constant.DefaultRecordCreateWriteThread, constant.DefaultRecordCreateBatchSize)
			if err != nil {
				return err
			}

			for _, st := range dataMigrateRules {
				if !strings.EqualFold(st.String(), "") {
					var sourceTable string

					if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
						sourceTable = st.TableNameS
					}

					if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
						sourceTable = stringutil.StringUpper(st.TableNameS)
					}
					if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
						sourceTable = stringutil.StringLower(st.TableNameS)
					}

					tableMigrateRules = append(tableMigrateRules, &rule.DataMigrateRule{
						TaskName:            taskName,
						SchemaNameS:         sourceSchema,
						TableNameS:          sourceTable,
						EnableChunkStrategy: strconv.FormatBool(st.EnableChunkStrategy),
						WhereRange:          st.WhereRange,
						SqlHintS:            st.SqlHintS,
					})
				}
			}

			if len(tableMigrateRules) > 0 {
				_, err = model.GetIDataMigrateRuleRW().CreateInBatchDataMigrateRule(ctx, tableMigrateRules, constant.DefaultRecordCreateWriteThread, constant.DefaultRecordCreateBatchSize)
				if err != nil {
					return err
				}
			}

			for _, st := range dataCompareRules {
				if !strings.EqualFold(st.String(), "") {
					var (
						sourceTable        string
						compareFiled       string
						ignoreSelectFields []string
						ignoreCondFields   []string
					)

					if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
						sourceTable = st.TableNameS
						compareFiled = st.CompareConditionField
						ignoreSelectFields = st.IgnoreSelectFields
						ignoreCondFields = st.IgnoreConditionFields
					}

					if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
						sourceTable = stringutil.StringUpper(st.TableNameS)
						compareFiled = stringutil.StringUpper(st.CompareConditionField)
						for _, f := range st.IgnoreSelectFields {
							ignoreSelectFields = append(ignoreSelectFields, stringutil.StringUpper(f))
						}
						for _, f := range st.IgnoreConditionFields {
							ignoreCondFields = append(ignoreCondFields, stringutil.StringUpper(f))
						}
					}
					if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
						sourceTable = stringutil.StringLower(st.TableNameS)
						compareFiled = stringutil.StringLower(st.CompareConditionField)
						for _, f := range st.IgnoreSelectFields {
							ignoreSelectFields = append(ignoreSelectFields, stringutil.StringLower(f))
						}
						for _, f := range st.IgnoreConditionFields {
							ignoreCondFields = append(ignoreCondFields, stringutil.StringLower(f))
						}
					}

					tableCompareRules = append(tableCompareRules, &rule.DataCompareRule{
						TaskName:               taskName,
						SchemaNameS:            sourceSchema,
						TableNameS:             sourceTable,
						CompareConditionField:  compareFiled,
						CompareConditionRangeS: st.CompareConditionRangeS,
						CompareConditionRangeT: st.CompareConditionRangeT,
						IgnoreSelectFields:     stringutil.StringJoin(ignoreSelectFields, constant.StringSeparatorComma),
						IgnoreConditionFields:  stringutil.StringJoin(ignoreCondFields, constant.StringSeparatorComma),
						SqlHintS:               st.SqlHintS,
						SqlHintT:               st.SqlHintT,
					})
				}
			}

			if len(tableCompareRules) > 0 {
				_, err = model.GetIDataCompareRuleRW().CreateInBatchDataCompareRule(ctx, tableCompareRules, constant.DefaultRecordCreateWriteThread, constant.DefaultRecordCreateBatchSize)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	err = databaseS.Close()
	if err != nil {
		return err
	}
	return nil
}

func DeleteSchemaRouteRule(ctx context.Context, taskName []string) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		err := model.GetIMigrateSchemaRouteRW().DeleteSchemaRouteRule(txnCtx, taskName)
		if err != nil {
			return err
		}

		err = model.GetIMigrateTaskTableRW().DeleteMigrateTaskTable(txnCtx, taskName)
		if err != nil {
			return err
		}

		err = model.GetIMigrateTaskSequenceRW().DeleteMigrateTaskSequence(txnCtx, taskName)
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

func ShowSchemaRouteRule(ctx context.Context, taskName string) (*pb.SchemaRouteRule, []*pb.DataMigrateRule, []*pb.DataCompareRule, error) {
	var (
		opSchemaRules  *pb.SchemaRouteRule
		opTableRules   []*pb.DataMigrateRule
		opCompareRules []*pb.DataCompareRule
	)

	tables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(ctx, &rule.MigrateTaskTable{TaskName: taskName})
	if err != nil {
		return opSchemaRules, opTableRules, opCompareRules, err
	}

	seqs, err := model.GetIMigrateTaskSequenceRW().FindMigrateTaskSequence(ctx, &rule.MigrateTaskSequence{TaskName: taskName})
	if err != nil {
		return opSchemaRules, opTableRules, opCompareRules, err
	}

	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		sr, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(txnCtx, &rule.SchemaRouteRule{TaskName: taskName})
		if err != nil {
			return err
		}

		var (
			opColumnRules []*pb.TableRouteRule

			includeTables []string
			excludeTables []string
			includeSeqs   []string
			excludeSeqs   []string
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

		for _, s := range seqs {
			if s.SchemaNameS == sr.SchemaNameS && s.IsExclude == constant.MigrateTaskTableIsNotExclude {
				includeSeqs = append(includeSeqs, s.SequenceNameS)
			}
			if s.SchemaNameS == sr.SchemaNameS && s.IsExclude != constant.MigrateTaskTableIsExclude {
				excludeSeqs = append(excludeSeqs, s.SequenceNameS)
			}
		}

		opSchemaRule.IncludeTableS = includeTables
		opSchemaRule.ExcludeTableS = excludeTables
		opSchemaRule.IncludeSequenceS = includeSeqs
		opSchemaRule.ExcludeSequenceS = excludeSeqs

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

		migrateTableRules, err := model.GetIDataMigrateRuleRW().FindDataMigrateRule(txnCtx, &rule.DataMigrateRule{
			TaskName:    taskName,
			SchemaNameS: sr.SchemaNameS,
		})
		if err != nil {
			return err
		}
		for _, st := range migrateTableRules {
			enableChunk, err := strconv.ParseBool(st.EnableChunkStrategy)
			if err != nil {
				return err
			}
			opTableRules = append(opTableRules, &pb.DataMigrateRule{
				TableNameS:          st.TableNameS,
				EnableChunkStrategy: enableChunk,
				WhereRange:          st.WhereRange,
				SqlHintS:            st.SqlHintS,
			})
		}

		migrateCompareRules, err := model.GetIDataCompareRuleRW().FindDataCompareRule(txnCtx, &rule.DataCompareRule{
			TaskName:    taskName,
			SchemaNameS: sr.SchemaNameS,
		})
		if err != nil {
			return err
		}
		for _, st := range migrateCompareRules {
			opCompareRules = append(opCompareRules, &pb.DataCompareRule{
				TableNameS:             st.TableNameS,
				CompareConditionField:  st.CompareConditionField,
				CompareConditionRangeS: st.CompareConditionRangeS,
				CompareConditionRangeT: st.CompareConditionRangeT,
				IgnoreSelectFields:     stringutil.StringSplit(st.IgnoreSelectFields, constant.StringSeparatorComma),
				IgnoreConditionFields:  stringutil.StringSplit(st.IgnoreConditionFields, constant.StringSeparatorComma),
				SqlHintS:               st.SqlHintS,
				SqlHintT:               st.SqlHintT,
			})
		}

		return nil
	})
	if err != nil {
		return opSchemaRules, opTableRules, opCompareRules, err
	}

	return opSchemaRules, opTableRules, opCompareRules, nil
}

func UpsertSqlMigrateRule(ctx context.Context, taskName string, caseFieldRule *pb.CaseFieldRule, sqlRouteRule []*pb.SqlMigrateRule) error {
	var sqlRouteRules []*rule.SqlMigrateRule

	for _, st := range sqlRouteRule {
		if !strings.EqualFold(st.String(), "") {
			var (
				targetSchema string
				targetTable  string
			)

			if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameOrigin) {
				targetSchema = st.SchemaNameT
				targetTable = st.TableNameT
			}

			if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
				targetSchema = stringutil.StringUpper(st.SchemaNameT)
				targetTable = stringutil.StringUpper(st.TableNameT)
			}
			if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameLower) {
				targetSchema = stringutil.StringLower(st.SchemaNameT)
				targetTable = stringutil.StringLower(st.TableNameT)
			}

			if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
				for k, v := range st.ColumnRouteRules {
					st.ColumnRouteRules[stringutil.StringUpper(k)] = v
				}
			}
			if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameUpper) {
				for k, v := range st.ColumnRouteRules {
					st.ColumnRouteRules[k] = stringutil.StringUpper(v)
				}
			}
			if strings.EqualFold(caseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
				for k, v := range st.ColumnRouteRules {
					st.ColumnRouteRules[stringutil.StringLower(k)] = v
				}
			}
			if strings.EqualFold(caseFieldRule.CaseFieldRuleT, constant.ParamValueRuleCaseFieldNameLower) {
				for k, v := range st.ColumnRouteRules {
					st.ColumnRouteRules[k] = stringutil.StringLower(v)
				}
			}

			jsonColumnRulesStr, err := stringutil.MarshalJSON(st.ColumnRouteRules)
			if err != nil {
				return err
			}

			sqlDigest, err := stringutil.Encrypt(st.SqlQueryS, []byte(constant.DefaultDataEncryptDecryptKey))
			if err != nil {
				return err
			}

			sqlRouteRules = append(sqlRouteRules, &rule.SqlMigrateRule{
				TaskName:        taskName,
				SchemaNameT:     targetSchema,
				TableNameT:      targetTable,
				SqlHintT:        st.SqlHintT,
				SqlQueryS:       sqlDigest,
				ColumnRouteRule: jsonColumnRulesStr,
				Entity:          nil,
			})
		}

		_, err := model.GetISqlMigrateRuleRW().CreateInBatchSqlMigrateRule(ctx, sqlRouteRules, constant.DefaultRecordCreateWriteThread, constant.DefaultRecordCreateBatchSize)
		if err != nil {
			return err
		}
	}
	return nil
}

func DeleteSqlMigrateRule(ctx context.Context, taskName []string) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		err := model.GetISqlMigrateRuleRW().DeleteSqlMigrateRule(txnCtx, taskName)
		if err != nil {
			return err
		}
		err = model.GetISqlMigrateSummaryRW().DeleteSqlMigrateSummaryName(txnCtx, taskName)
		if err != nil {
			return err
		}
		err = model.GetISqlMigrateTaskRW().DeleteSqlMigrateTaskName(txnCtx, taskName)
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

func ShowSqlMigrateRule(ctx context.Context, taskName string) ([]*pb.SqlMigrateRule, error) {
	var (
		opSqlRules []*pb.SqlMigrateRule
	)

	migrateSqlRules, err := model.GetISqlMigrateRuleRW().FindSqlMigrateRule(ctx, &rule.SqlMigrateRule{
		TaskName: taskName,
	})
	if err != nil {
		return opSqlRules, err
	}
	for _, st := range migrateSqlRules {
		columnRuleMap := make(map[string]string)
		err = stringutil.UnmarshalJSON([]byte(st.ColumnRouteRule), columnRuleMap)
		if err != nil {
			return opSqlRules, err
		}
		opSqlRules = append(opSqlRules, &pb.SqlMigrateRule{
			SchemaNameT:      st.SchemaNameT,
			TableNameT:       st.TableNameT,
			SqlHintT:         st.SqlHintT,
			SqlQueryS:        st.SqlQueryS,
			ColumnRouteRules: columnRuleMap,
		})
	}

	if err != nil {
		return opSqlRules, err
	}

	return opSqlRules, nil
}
