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
	"time"

	"github.com/wentaojin/dbms/database/processor"

	"github.com/golang/snappy"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/errconcurrent"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type StructCompareTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.StructCompareParam
}

func (dmt *StructCompareTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("struct compare task get schema route",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(dmt.Ctx, &rule.SchemaRouteRule{TaskName: dmt.Task.TaskName})
	if err != nil {
		return err
	}

	dbTypeSli := stringutil.StringSplit(dmt.Task.TaskFlow, constant.StringSeparatorAite)
	buildInDatatypeRulesS, err := model.GetIBuildInDatatypeRuleRW().QueryBuildInDatatypeRule(dmt.Ctx, dbTypeSli[0], dbTypeSli[1])
	if err != nil {
		return err
	}
	buildInDefaultValueRulesS, err := model.GetBuildInDefaultValueRuleRW().QueryBuildInDefaultValueRule(dmt.Ctx, dbTypeSli[0], dbTypeSli[1])
	if err != nil {
		return err
	}
	buildInDatatypeRulesT, err := model.GetIBuildInDatatypeRuleRW().QueryBuildInDatatypeRule(dmt.Ctx, dbTypeSli[1], dbTypeSli[0])
	if err != nil {
		return err
	}
	buildInDefaultValueRulesT, err := model.GetBuildInDefaultValueRuleRW().QueryBuildInDefaultValueRule(dmt.Ctx, dbTypeSli[1], dbTypeSli[0])
	if err != nil {
		return err
	}

	logger.Info("struct compare task init database connection",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))

	sourceDatasource, err := model.GetIDatasourceRW().GetDatasource(dmt.Ctx, dmt.Task.DatasourceNameS)
	if err != nil {
		return err
	}
	databaseS, err := database.NewDatabase(dmt.Ctx, sourceDatasource, schemaRoute.SchemaNameS)
	if err != nil {
		return err
	}
	defer databaseS.Close()
	databaseT, err := database.NewDatabase(dmt.Ctx, dmt.DatasourceT, "")
	if err != nil {
		return err
	}
	defer databaseT.Close()

	logger.Info("struct compare task inspect migrate task",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	_, _, dbCollationS, err := inspectMigrateTask(dmt.Task.TaskName, dmt.Task.TaskFlow, dmt.Task.TaskMode, databaseS, stringutil.StringUpper(dmt.DatasourceS.ConnectCharset), stringutil.StringUpper(dmt.DatasourceT.ConnectCharset))
	if err != nil {
		return err
	}

	logger.Info("struct compare task init task",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	err = initStructCompareTask(dmt.Ctx, dmt.Task, databaseS, databaseT, schemaRoute)
	if err != nil {
		return err
	}

	logger.Info("struct compare task get tables",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	startTableTime := time.Now()
	logger.Info("struct compare task process table",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("schema_name_s", schemaRoute.SchemaNameS))

	var migrateTasks []*task.StructCompareTask
	err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateTasks, err = model.GetIStructCompareTaskRW().FindStructCompareTask(txnCtx,
			&task.StructCompareTask{
				TaskName:    dmt.Task.TaskName,
				SchemaNameS: schemaRoute.SchemaNameS,
				TaskStatus:  constant.TaskDatabaseStatusWaiting,
			})
		if err != nil {
			return err
		}
		migrateFailedTasks, err := model.GetIStructCompareTaskRW().FindStructCompareTask(txnCtx,
			&task.StructCompareTask{
				TaskName:    dmt.Task.TaskName,
				SchemaNameS: schemaRoute.SchemaNameS,
				TaskStatus:  constant.TaskDatabaseStatusFailed})
		if err != nil {
			return err
		}
		migrateRunningTasks, err := model.GetIStructCompareTaskRW().FindStructCompareTask(txnCtx,
			&task.StructCompareTask{
				TaskName:    dmt.Task.TaskName,
				SchemaNameS: schemaRoute.SchemaNameS,
				TaskStatus:  constant.TaskDatabaseStatusRunning})
		if err != nil {
			return err
		}
		migrateStopTasks, err := model.GetIStructCompareTaskRW().FindStructCompareTask(txnCtx,
			&task.StructCompareTask{
				TaskName:    dmt.Task.TaskName,
				SchemaNameS: schemaRoute.SchemaNameS,
				TaskStatus:  constant.TaskDatabaseStatusRunning})
		if err != nil {
			return err
		}
		migrateTasks = append(migrateTasks, migrateFailedTasks...)
		migrateTasks = append(migrateTasks, migrateRunningTasks...)
		migrateTasks = append(migrateTasks, migrateStopTasks...)
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("struct compare task process chunks",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("schema_name_s", schemaRoute.SchemaNameS))

	g := errconcurrent.NewGroup()
	g.SetLimit(int(dmt.TaskParams.CompareThread))
	for _, j := range migrateTasks {
		gTime := time.Now()
		g.Go(j, gTime, func(j interface{}) error {
			select {
			case <-dmt.Ctx.Done():
				return nil
			default:
				dt := j.(*task.StructCompareTask)
				errW := model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
					_, err = model.GetIStructCompareTaskRW().UpdateStructCompareTask(txnCtx,
						&task.StructCompareTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS},
						map[string]interface{}{
							"TaskStatus": constant.TaskDatabaseStatusRunning,
						})
					if err != nil {
						return err
					}
					_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
						TaskName:    dt.TaskName,
						SchemaNameS: dt.SchemaNameS,
						TableNameS:  dt.TableNameS,
						LogDetail: fmt.Sprintf("%v [%v] struct compare task [%v] taskflow [%v] source table [%v.%v] compare start",
							stringutil.CurrentTimeFormatString(),
							stringutil.StringLower(constant.TaskModeDataCompare),
							dt.TaskName,
							dmt.Task.TaskMode,
							dt.SchemaNameS,
							dt.TableNameS),
					})
					if err != nil {
						return err
					}
					return nil
				})
				if errW != nil {
					return errW
				}

				switch {
				case strings.EqualFold(dmt.Task.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(dmt.Task.TaskFlow, constant.TaskFlowOracleToMySQL):
					oracleProcessor, err := database.IStructCompareProcessor(&processor.OracleProcessor{
						Ctx:                      dmt.Ctx,
						TaskName:                 dmt.Task.TaskName,
						TaskFlow:                 dmt.Task.TaskFlow,
						SchemaName:               dt.SchemaNameS,
						TableName:                dt.TableNameS,
						DBCharset:                stringutil.StringUpper(dmt.DatasourceS.ConnectCharset),
						DBCollation:              dbCollationS,
						Database:                 databaseS,
						BuildinDatatypeRules:     buildInDatatypeRulesS,
						BuildinDefaultValueRules: buildInDefaultValueRulesS,
						ColumnRouteRules:         make(map[string]string),
						IsBaseline:               true,
					})
					if err != nil {
						return fmt.Errorf("the struct compare processor database [%s] failed: %v", dmt.DatasourceS.DbType, err)
					}

					// oracle baseline, mysql not configure task and not configure rules
					mysqlProcessor, err := database.IStructCompareProcessor(&processor.MySQLProcessor{
						Ctx:                      dmt.Ctx,
						TaskName:                 dmt.Task.TaskName,
						TaskFlow:                 dmt.Task.TaskFlow,
						SchemaName:               dt.SchemaNameT,
						TableName:                dt.TableNameT,
						DBCharset:                stringutil.StringUpper(dmt.DatasourceT.ConnectCharset),
						Database:                 databaseT,
						BuildinDatatypeRules:     buildInDatatypeRulesT,
						BuildinDefaultValueRules: buildInDefaultValueRulesT,
						ColumnRouteRules:         make(map[string]string),
						IsBaseline:               false,
					})
					if err != nil {
						return fmt.Errorf("the struct compare processor database [%s] failed: %v", dmt.DatasourceT.DbType, err)
					}

					compareDetail, err := database.IStructCompareTable(&processor.Table{
						TaskName: dmt.Task.TaskName,
						TaskFlow: dmt.Task.TaskFlow,
						Source:   oracleProcessor,
						Target:   mysqlProcessor,
					})
					if err != nil {
						return fmt.Errorf("the struct compare table processor failed: %v", err)
					}
					if strings.EqualFold(compareDetail, "") {
						errW = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
							_, err = model.GetIStructCompareTaskRW().UpdateStructCompareTask(txnCtx,
								&task.StructCompareTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS},
								map[string]interface{}{
									"TaskStatus": constant.TaskDatabaseStatusEqual,
									"Duration":   fmt.Sprintf("%f", time.Now().Sub(gTime).Seconds()),
								})
							if err != nil {
								return err
							}
							_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
								TaskName:    dt.TaskName,
								SchemaNameS: dt.SchemaNameS,
								TableNameS:  dt.TableNameS,
								LogDetail: fmt.Sprintf("%v [%v] struct compare task [%v] taskflow [%v] source table [%v.%v] compare equal, please see [struct_compare_task] detail",
									stringutil.CurrentTimeFormatString(),
									stringutil.StringLower(constant.TaskModeStructCompare),
									dt.TaskName,
									dmt.Task.TaskMode,
									dt.SchemaNameS,
									dt.TableNameS),
							})
							if err != nil {
								return err
							}
							return nil
						})
						if errW != nil {
							return errW
						}
						return nil
					}

					originStructS, err := databaseS.GetDatabaseTableOriginStruct(dt.SchemaNameS, dt.TableNameS, "TABLE")
					if err != nil {
						return fmt.Errorf("the struct compare table get source origin struct failed: %v", err)
					}
					originStructT, err := databaseT.GetDatabaseTableOriginStruct(dt.SchemaNameT, dt.TableNameT, "")
					if err != nil {
						return fmt.Errorf("the struct compare table get target origin struct failed: %v", err)
					}

					encOriginS := snappy.Encode(nil, []byte(originStructS))
					encryptOriginS, err := stringutil.Encrypt(stringutil.BytesToString(encOriginS), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					encOriginT := snappy.Encode(nil, []byte(originStructT))
					encryptOriginT, err := stringutil.Encrypt(stringutil.BytesToString(encOriginT), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					encCompareDetail := snappy.Encode(nil, []byte(compareDetail))
					encryptCompareDetail, err := stringutil.Encrypt(stringutil.BytesToString(encCompareDetail), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					errW = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIStructCompareTaskRW().UpdateStructCompareTask(txnCtx,
							&task.StructCompareTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS},
							map[string]interface{}{
								"TaskStatus":      constant.TaskDatabaseStatusNotEqual,
								"SourceSqlDigest": encryptOriginS,
								"TargetSqlDigest": encryptOriginT,
								"CompareDetail":   encryptCompareDetail,
								"Duration":        fmt.Sprintf("%f", time.Now().Sub(gTime).Seconds()),
							})
						if err != nil {
							return err
						}
						_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
							TaskName:    dt.TaskName,
							SchemaNameS: dt.SchemaNameS,
							TableNameS:  dt.TableNameS,
							LogDetail: fmt.Sprintf("%v [%v] struct compare task [%v] taskflow [%v] source table [%v.%v] compare equal, please see [struct_compare_task] detail",
								stringutil.CurrentTimeFormatString(),
								stringutil.StringLower(constant.TaskModeStructCompare),
								dt.TaskName,
								dmt.Task.TaskMode,
								dt.SchemaNameS,
								dt.TableNameS),
						})
						if err != nil {
							return err
						}
						return nil
					})
					if errW != nil {
						return errW
					}
					return nil
				default:
					return fmt.Errorf("oracle current task [%s] schema [%s] taskflow [%s] column rule isn't support, please contact author", dmt.Task.TaskName, dt.SchemaNameS, dmt.Task.TaskFlow)
				}
			}
		})
	}

	for _, r := range g.Wait() {
		if r.Err != nil {
			smt := r.Task.(*task.StructCompareTask)
			logger.Warn("struct compare task process tables",
				zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow),
				zap.String("schema_name_s", smt.SchemaNameS),
				zap.String("table_name_s", smt.TableNameS),
				zap.Error(r.Err))

			errW := model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
				_, err = model.GetIStructCompareTaskRW().UpdateStructCompareTask(txnCtx,
					&task.StructCompareTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS},
					map[string]interface{}{
						"TaskStatus":  constant.TaskDatabaseStatusFailed,
						"Duration":    fmt.Sprintf("%f", time.Now().Sub(r.Time).Seconds()),
						"ErrorDetail": r.Err.Error(),
					})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName:    smt.TaskName,
					SchemaNameS: smt.SchemaNameS,
					TableNameS:  smt.TableNameS,
					LogDetail: fmt.Sprintf("%v [%v] struct compare task [%v] taskflow [%v] source table [%v.%v] failed, please see [struct_compare_task] detail",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(constant.TaskModeStructCompare),
						smt.TaskName,
						dmt.Task.TaskMode,
						smt.SchemaNameS,
						smt.TableNameS),
				})
				if err != nil {
					return err
				}
				return nil
			})
			if errW != nil {
				return errW
			}
		}
	}

	err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
		tableStatusRecs, err := model.GetIStructCompareTaskRW().FindStructCompareTaskGroupByTaskStatus(txnCtx, dmt.Task.TaskName)
		if err != nil {
			return err
		}
		for _, rec := range tableStatusRecs {
			switch rec.TaskStatus {
			case constant.TaskDatabaseStatusEqual:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: schemaRoute.SchemaNameS,
				}, map[string]interface{}{
					"TableEquals": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusNotEqual:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: schemaRoute.SchemaNameS,
				}, map[string]interface{}{
					"TableNotEquals": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusFailed:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: schemaRoute.SchemaNameS,
				}, map[string]interface{}{
					"TableFails": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusWaiting:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: schemaRoute.SchemaNameS,
				}, map[string]interface{}{
					"TableWaits": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusRunning:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: schemaRoute.SchemaNameS,
				}, map[string]interface{}{
					"TableRuns": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusStopped:
				_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
					TaskName:    rec.TaskName,
					SchemaNameS: schemaRoute.SchemaNameS,
				}, map[string]interface{}{
					"TableStops": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("the task [%v] task_mode [%s] task_flow [%v] schema_name_s [%v] task_status [%v] panic, please contact auhtor or reselect", dmt.Task.TaskName, dmt.Task.TaskMode, dmt.Task.TaskFlow, schemaRoute.SchemaNameS, rec.TaskStatus)
			}
		}

		_, err = model.GetIStructCompareSummaryRW().UpdateStructCompareSummary(txnCtx, &task.StructCompareSummary{
			TaskName:    dmt.Task.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
		}, map[string]interface{}{
			"Duration": fmt.Sprintf("%f", time.Now().Sub(startTableTime).Seconds()),
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("struct compare task",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}

func initStructCompareTask(ctx context.Context, taskInfo *task.Task, databaseS, databaseT database.IDatabase, schemaRoute *rule.SchemaRouteRule) error {
	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(ctx, &rule.MigrateTaskTable{
		TaskName:    schemaRoute.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	if err != nil {
		return err
	}
	var (
		includeTables  []string
		excludeTables  []string
		databaseTables []string // task tables
	)
	databaseTableTypeMap := make(map[string]string)

	for _, t := range schemaTaskTables {
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsExclude) {
			excludeTables = append(excludeTables, t.TableNameS)
		}
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsNotExclude) {
			includeTables = append(includeTables, t.TableNameS)
		}
	}

	databaseTables, err = databaseS.FilterDatabaseTable(schemaRoute.SchemaNameS, includeTables, excludeTables)
	if err != nil {
		return err
	}
	databaseTableTypeMap, err = databaseS.GetDatabaseTableType(schemaRoute.SchemaNameS)
	if err != nil {
		return err
	}

	allTablesT, err := databaseT.GetDatabaseTable(schemaRoute.SchemaNameT)
	if err != nil {
		return err
	}
	// get table route rule
	tableRouteRule := make(map[string]string)
	tableRouteRuleT := make(map[string]string)

	tableRoutes, err := model.GetIMigrateTableRouteRW().FindTableRouteRule(ctx, &rule.TableRouteRule{
		TaskName:    schemaRoute.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	for _, tr := range tableRoutes {
		tableRouteRule[tr.TableNameS] = tr.TableNameT
		tableRouteRuleT[tr.TableNameT] = tr.TableNameS
	}

	tableRouteRuleTNew := make(map[string]string)
	for _, t := range allTablesT {
		if v, ok := tableRouteRuleT[t]; ok {
			tableRouteRuleTNew[v] = t
		} else {
			tableRouteRuleTNew[t] = t
		}
	}

	var panicTables []string
	for _, t := range databaseTables {
		if _, ok := tableRouteRuleTNew[t]; !ok {
			panicTables = append(panicTables, t)
		}
	}
	if len(panicTables) > 0 {
		return fmt.Errorf("the task [%v] task_flow [%v] task_mode [%v] source database tables aren't existed in the target database, please create the tables [%v]", taskInfo.TaskName, taskInfo.TaskFlow, taskInfo.TaskMode, stringutil.StringJoin(panicTables, constant.StringSeparatorComma))
	}

	// clear the struct compare task table
	migrateTasks, err := model.GetIStructCompareTaskRW().BatchFindStructCompareTask(ctx, &task.StructCompareTask{TaskName: taskInfo.TaskName})
	if err != nil {
		return err
	}

	// repeatInitTableMap used for store the struct_migrate_task table name has be finished, avoid repeated initialization
	repeatInitTableMap := make(map[string]struct{})
	if len(migrateTasks) > 0 {
		taskTablesMap := make(map[string]struct{})
		for _, t := range databaseTables {
			taskTablesMap[t] = struct{}{}
		}
		for _, smt := range migrateTasks {
			if _, ok := taskTablesMap[smt.TableNameS]; !ok {
				err = model.GetIStructCompareTaskRW().DeleteStructCompareTask(ctx, smt.ID)
				if err != nil {
					return err
				}
			} else {
				repeatInitTableMap[smt.TableNameS] = struct{}{}
			}
		}
	}

	err = model.GetIStructCompareSummaryRW().DeleteStructCompareSummary(ctx, &task.StructCompareSummary{TaskName: taskInfo.TaskName})
	if err != nil {
		return err
	}

	// database tables
	// init database table
	// get table column route rule
	for _, sourceTable := range databaseTables {
		initStructInfos, err := model.GetIStructCompareTaskRW().GetStructCompareTaskTable(ctx, &task.StructCompareTask{
			TaskName:    taskInfo.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
			TableNameS:  sourceTable,
		})
		if err != nil {
			return err
		}
		if len(initStructInfos) > 1 {
			return fmt.Errorf("the struct compare task table is over one, it should be only one")
		}
		// if the table is existed and task_status success, then skip init
		if _, ok := repeatInitTableMap[sourceTable]; ok && strings.EqualFold(initStructInfos[0].TaskStatus, constant.TaskDatabaseStatusSuccess) {
			continue
		}
		var (
			targetTable string
		)
		if val, ok := tableRouteRule[sourceTable]; ok {
			targetTable = val
		} else {
			// the according target case field rule convert
			if strings.EqualFold(taskInfo.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleLower) {
				targetTable = stringutil.StringLower(sourceTable)
			}
			if strings.EqualFold(taskInfo.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
				targetTable = stringutil.StringUpper(sourceTable)
			}
			if strings.EqualFold(taskInfo.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
				targetTable = sourceTable
			}
		}

		_, err = model.GetIStructCompareTaskRW().CreateStructCompareTask(ctx, &task.StructCompareTask{
			TaskName:    taskInfo.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
			TableNameS:  sourceTable,
			TableTypeS:  databaseTableTypeMap[sourceTable],
			SchemaNameT: schemaRoute.SchemaNameT,
			TableNameT:  targetTable,
			TaskStatus:  constant.TaskDatabaseStatusWaiting,
		})
		if err != nil {
			return err
		}
	}

	_, err = model.GetIStructCompareSummaryRW().CreateStructCompareSummary(ctx,
		&task.StructCompareSummary{
			TaskName:    taskInfo.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
			TableTotals: uint64(len(databaseTables)),
		})
	if err != nil {
		return err
	}
	return nil
}
