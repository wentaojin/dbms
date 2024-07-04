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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

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
	"golang.org/x/sync/errgroup"
)

type DataCompareTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.DataCompareParam
}

func (dmt *DataCompareTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("data compare task get schema route",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(dmt.Ctx, &rule.SchemaRouteRule{TaskName: dmt.Task.TaskName})
	if err != nil {
		return err
	}

	logger.Info("data compare task init database connection",
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

	logger.Info("data compare task inspect migrate task",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))

	_, _, dbCollationS, err := inspectMigrateTask(dmt.Task.TaskName, dmt.Task.TaskFlow, dmt.Task.TaskMode, databaseS, stringutil.StringUpper(dmt.DatasourceS.ConnectCharset), stringutil.StringUpper(dmt.DatasourceT.ConnectCharset))
	if err != nil {
		return err
	}

	logger.Info("data compare task init task",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))

	err = dmt.InitDataCompareTask(databaseS, databaseT, dbCollationS, schemaRoute)
	if err != nil {
		return err
	}

	logger.Info("data compare task get tables",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))

	summaries, err := model.GetIDataCompareSummaryRW().FindDataCompareSummary(dmt.Ctx, &task.DataCompareSummary{
		TaskName:    dmt.Task.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	if err != nil {
		return err
	}

	for _, s := range summaries {
		startTableTime := time.Now()
		logger.Info("data compare task process table",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS))

		var migrateTasks []*task.DataCompareTask
		err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
			// get migrate task tables
			migrateTasks, err = model.GetIDataCompareTaskRW().FindDataCompareTask(txnCtx,
				&task.DataCompareTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					TaskStatus:  constant.TaskDatabaseStatusWaiting,
				})
			if err != nil {
				return err
			}
			migrateFailedTasks, err := model.GetIDataCompareTaskRW().FindDataCompareTask(txnCtx,
				&task.DataCompareTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					TaskStatus:  constant.TaskDatabaseStatusFailed})
			if err != nil {
				return err
			}
			migrateRunningTasks, err := model.GetIDataCompareTaskRW().FindDataCompareTask(txnCtx,
				&task.DataCompareTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					TaskStatus:  constant.TaskDatabaseStatusRunning})
			if err != nil {
				return err
			}
			migrateStopTasks, err := model.GetIDataCompareTaskRW().FindDataCompareTask(txnCtx,
				&task.DataCompareTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					TaskStatus:  constant.TaskDatabaseStatusStopped})
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

		logger.Info("data compare task process chunks",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS))

		g := errconcurrent.NewGroup()
		g.SetLimit(int(dmt.TaskParams.SqlThread))
		for _, j := range migrateTasks {
			gTime := time.Now()
			g.Go(j, gTime, func(j interface{}) error {
				select {
				case <-dmt.Ctx.Done():
					return nil
				default:
					dt := j.(*task.DataCompareTask)
					errW := model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
							&task.DataCompareTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS, ChunkDetailS: dt.ChunkDetailS},
							map[string]interface{}{
								"TaskStatus": constant.TaskDatabaseStatusRunning,
							})
						if err != nil {
							return err
						}
						// clear data compare chunk result
						err = model.GetIDataCompareResultRW().DeleteDataCompareResult(txnCtx, &task.DataCompareResult{
							TaskName:     dt.TaskName,
							SchemaNameS:  dt.SchemaNameS,
							TableNameS:   dt.TableNameS,
							ChunkDetailS: dt.ChunkDetailS,
						})
						if err != nil {
							return err
						}
						_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
							TaskName:    dt.TaskName,
							SchemaNameS: dt.SchemaNameS,
							TableNameS:  dt.TableNameS,
							LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] chunk [%s] start",
								stringutil.CurrentTimeFormatString(),
								stringutil.StringLower(constant.TaskModeDataCompare),
								dt.TaskName,
								dmt.Task.TaskMode,
								dt.SchemaNameS,
								dt.TableNameS,
								dt.ChunkDetailS),
						})
						if err != nil {
							return err
						}
						return nil
					})
					if errW != nil {
						return errW
					}

					var dbCharsetT string
					switch {
					case strings.EqualFold(dmt.Task.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(dmt.Task.TaskFlow, constant.TaskFlowOracleToMySQL):
						dbCharsetT = constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DatasourceT.ConnectCharset)]
					default:
						return fmt.Errorf("oracle current task [%s] schema [%s] taskflow [%s] column rule isn't support, please contact author", dmt.Task.TaskName, dt.SchemaNameS, dmt.Task.TaskFlow)
					}
					err = database.IDataCompareProcess(&DataCompareRow{
						Ctx:         dmt.Ctx,
						TaskMode:    dmt.Task.TaskMode,
						TaskFlow:    dmt.Task.TaskFlow,
						StartTime:   gTime,
						Dmt:         dt,
						DatabaseS:   databaseS,
						DatabaseT:   databaseT,
						BatchSize:   int(dmt.TaskParams.BatchSize),
						WriteThread: int(dmt.TaskParams.WriteThread),
						CallTimeout: int(dmt.TaskParams.CallTimeout),
						DBCharsetS:  constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DatasourceS.ConnectCharset)],
						DBCharsetT:  dbCharsetT,
					})
					if err != nil {
						return err
					}
					return nil
				}
			})
		}

		for _, r := range g.Wait() {
			if r.Err != nil {
				smt := r.Task.(*task.DataCompareTask)
				logger.Warn("data compare task process tables",
					zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow),
					zap.String("schema_name_s", smt.SchemaNameS),
					zap.String("table_name_s", smt.TableNameS),
					zap.Error(r.Err))

				errW := model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
					_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
						&task.DataCompareTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS, ChunkDetailS: smt.ChunkDetailS},
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
						LogDetail: fmt.Sprintf("%v [%v] data compare task [%v] taskflow [%v] source table [%v.%v] failed, please see [data_compare_task] detail",
							stringutil.CurrentTimeFormatString(),
							stringutil.StringLower(constant.TaskModeDataCompare),
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

		endTableTime := time.Now()
		err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
			tableStatusRecs, err := model.GetIDataCompareTaskRW().FindDataCompareTaskBySchemaTableChunkStatus(txnCtx, &task.DataCompareTask{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			})
			if err != nil {
				return err
			}
			for _, rec := range tableStatusRecs {
				switch rec.TaskStatus {
				case constant.TaskDatabaseStatusEqual:
					_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(txnCtx, &task.DataCompareSummary{
						TaskName:    rec.TaskName,
						SchemaNameS: rec.SchemaNameS,
						TableNameS:  rec.TableNameS,
					}, map[string]interface{}{
						"ChunkEquals": rec.StatusTotals,
					})
					if err != nil {
						return err
					}
				case constant.TaskDatabaseStatusNotEqual:
					_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(txnCtx, &task.DataCompareSummary{
						TaskName:    rec.TaskName,
						SchemaNameS: rec.SchemaNameS,
						TableNameS:  rec.TableNameS,
					}, map[string]interface{}{
						"ChunkNotEquals": rec.StatusTotals,
					})
					if err != nil {
						return err
					}
				case constant.TaskDatabaseStatusFailed:
					_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(txnCtx, &task.DataCompareSummary{
						TaskName:    rec.TaskName,
						SchemaNameS: rec.SchemaNameS,
						TableNameS:  rec.TableNameS,
					}, map[string]interface{}{
						"ChunkFails": rec.StatusTotals,
					})
					if err != nil {
						return err
					}
				case constant.TaskDatabaseStatusWaiting:
					_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(txnCtx, &task.DataCompareSummary{
						TaskName:    rec.TaskName,
						SchemaNameS: rec.SchemaNameS,
						TableNameS:  rec.TableNameS,
					}, map[string]interface{}{
						"ChunkWaits": rec.StatusTotals,
					})
					if err != nil {
						return err
					}
				case constant.TaskDatabaseStatusRunning:
					_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(txnCtx, &task.DataCompareSummary{
						TaskName:    rec.TaskName,
						SchemaNameS: rec.SchemaNameS,
						TableNameS:  rec.TableNameS,
					}, map[string]interface{}{
						"ChunkRuns": rec.StatusTotals,
					})
					if err != nil {
						return err
					}
				case constant.TaskDatabaseStatusStopped:
					_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(txnCtx, &task.DataCompareSummary{
						TaskName:    rec.TaskName,
						SchemaNameS: rec.SchemaNameS,
						TableNameS:  rec.TableNameS,
					}, map[string]interface{}{
						"ChunkStops": rec.StatusTotals,
					})
					if err != nil {
						return err
					}
				default:
					return fmt.Errorf("the task [%v] task_mode [%s] task_flow [%v] schema_name_s [%v] table_name_s [%v] task_status [%v] panic, please contact auhtor or reselect", s.TaskName, dmt.Task.TaskMode, dmt.Task.TaskFlow, rec.SchemaNameS, rec.TableNameS, rec.TaskStatus)
				}
			}

			_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(txnCtx, &task.DataCompareSummary{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
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

		logger.Info("data compare task process table",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS),
			zap.String("cost", endTableTime.Sub(startTableTime).String()))
	}
	logger.Info("data compare task",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}

func (dmt *DataCompareTask) InitDataCompareTask(databaseS, databaseT database.IDatabase, dbCollationS bool, schemaRoute *rule.SchemaRouteRule) error {
	// delete checkpoint
	initFlags, err := model.GetITaskRW().GetTask(dmt.Ctx, &task.Task{TaskName: dmt.Task.TaskName})
	if err != nil {
		return err
	}
	if !dmt.TaskParams.EnableCheckpoint || strings.EqualFold(initFlags.TaskInit, constant.TaskInitStatusNotFinished) {
		err = model.GetIDataCompareTaskRW().DeleteDataCompareTaskName(dmt.Ctx, []string{schemaRoute.TaskName})
		if err != nil {
			return err
		}
		err = model.GetIDataCompareSummaryRW().DeleteDataCompareSummaryName(dmt.Ctx, []string{schemaRoute.TaskName})
		if err != nil {
			return err
		}
		err = model.GetIDataCompareResultRW().DeleteDataCompareResultName(dmt.Ctx, []string{schemaRoute.TaskName})
		if err != nil {
			return err
		}
	} else if dmt.TaskParams.EnableCheckpoint && strings.EqualFold(initFlags.TaskInit, constant.TaskInitStatusFinished) {
		logger.Warn("data compare task init skip",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("task_init", constant.TaskInitStatusFinished))
		return nil
	}
	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(dmt.Ctx, &rule.MigrateTaskTable{
		TaskName:    schemaRoute.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	if err != nil {
		return err
	}
	var (
		includeTables      []string
		excludeTables      []string
		databaseTaskTables []string // task tables
		globalScnS         string
		globalScnT         string
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

	tableObjs, err := databaseS.FilterDatabaseTable(schemaRoute.SchemaNameS, includeTables, excludeTables)
	if err != nil {
		return err
	}

	// rule case field
	for _, t := range tableObjs.TaskTables {
		var tabName string
		// the according target case field rule convert
		if strings.EqualFold(dmt.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			tabName = stringutil.StringLower(t)
		}
		if strings.EqualFold(dmt.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			tabName = stringutil.StringUpper(t)
		}
		if strings.EqualFold(dmt.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
			tabName = t
		}
		databaseTaskTables = append(databaseTaskTables, tabName)
	}

	// clear the data compare task table
	// repeatInitTableMap used for store the struct_migrate_task table name has be finished, avoid repeated initialization
	migrateGroupTasks, err := model.GetIDataCompareTaskRW().FindDataCompareTaskGroupByTaskSchemaTable(dmt.Ctx, dmt.Task.TaskName)
	if err != nil {
		return err
	}
	repeatInitTableMap := make(map[string]struct{})

	if len(migrateGroupTasks) > 0 {
		taskTablesMap := make(map[string]struct{})
		for _, t := range databaseTaskTables {
			taskTablesMap[t] = struct{}{}
		}
		for _, smt := range migrateGroupTasks {
			if smt.SchemaNameS == schemaRoute.SchemaNameS {
				if _, ok := taskTablesMap[smt.TableNameS]; !ok {
					err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
						err = model.GetIDataCompareSummaryRW().DeleteDataCompareSummary(txnCtx, &task.DataCompareSummary{
							TaskName:    smt.TaskName,
							SchemaNameS: smt.SchemaNameS,
							TableNameS:  smt.TableNameS,
						})
						if err != nil {
							return err
						}
						err = model.GetIDataCompareTaskRW().DeleteDataCompareTask(txnCtx, &task.DataCompareTask{
							TaskName:    smt.TaskName,
							SchemaNameS: smt.SchemaNameS,
							TableNameS:  smt.TableNameS,
						})
						if err != nil {
							return err
						}
						return nil
					})
					if err != nil {
						return err
					}

					continue
				}
				var summary *task.DataCompareSummary

				summary, err = model.GetIDataCompareSummaryRW().GetDataCompareSummary(dmt.Ctx, &task.DataCompareSummary{
					TaskName:    smt.TaskName,
					SchemaNameS: smt.SchemaNameS,
					TableNameS:  smt.TableNameS,
				})
				if err != nil {
					return err
				}

				if int64(summary.ChunkTotals) != smt.ChunkTotals {
					err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
						err = model.GetIDataCompareSummaryRW().DeleteDataCompareSummary(txnCtx, &task.DataCompareSummary{
							TaskName:    smt.TaskName,
							SchemaNameS: smt.SchemaNameS,
							TableNameS:  smt.TableNameS,
						})
						if err != nil {
							return err
						}
						err = model.GetIDataCompareTaskRW().DeleteDataCompareTask(txnCtx, &task.DataCompareTask{
							TaskName:    smt.TaskName,
							SchemaNameS: smt.SchemaNameS,
							TableNameS:  smt.TableNameS,
						})
						if err != nil {
							return err
						}
						return nil
					})
					if err != nil {
						return err
					}

					continue
				}

				repeatInitTableMap[smt.TableNameS] = struct{}{}
			}
		}
	}

	databaseTableTypeMap, err = databaseS.GetDatabaseTableType(schemaRoute.SchemaNameS)
	if err != nil {
		return err
	}

	globalScn, err := databaseS.GetDatabaseConsistentPos()
	if err != nil {
		return err
	}

	if dmt.TaskParams.EnableConsistentRead {
		globalScnS = strconv.FormatUint(globalScn, 10)
	}

	if !dmt.TaskParams.EnableConsistentRead && !strings.EqualFold(dmt.TaskParams.ConsistentReadPointS, "") {
		globalScnS = dmt.TaskParams.ConsistentReadPointS
	}

	if !strings.EqualFold(dmt.TaskParams.ConsistentReadPointT, "") {
		globalScnT = dmt.TaskParams.ConsistentReadPointT
	}

	// database tables
	// init database table
	logger.Info("data compare task init",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))

	g, gCtx := errgroup.WithContext(dmt.Ctx)
	g.SetLimit(int(dmt.TaskParams.TableThread))

	for _, taskJob := range databaseTaskTables {
		sourceTable := taskJob
		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return nil
			default:
				startTime := time.Now()
				if _, ok := repeatInitTableMap[sourceTable]; ok {
					// skip
					return nil
				}
				tableRows, err := databaseS.GetDatabaseTableRows(schemaRoute.SchemaNameS, sourceTable)
				if err != nil {
					return err
				}
				tableSize, err := databaseS.GetDatabaseTableSize(schemaRoute.SchemaNameS, sourceTable)
				if err != nil {
					return err
				}

				dataRule := &DataCompareRule{
					Ctx:            gCtx,
					TaskMode:       dmt.Task.TaskMode,
					TaskName:       dmt.Task.TaskName,
					TaskFlow:       dmt.Task.TaskFlow,
					DatabaseS:      databaseS,
					DBCollationS:   dbCollationS,
					SchemaNameS:    schemaRoute.SchemaNameS,
					TableNameS:     sourceTable,
					TableTypeS:     databaseTableTypeMap,
					OnlyCompareRow: dmt.TaskParams.OnlyCompareRow,
					DBCharsetS:     dmt.DatasourceS.ConnectCharset,
					DBCharsetT:     dmt.DatasourceT.ConnectCharset,
					CaseFieldRuleS: dmt.Task.CaseFieldRuleS,
					CaseFieldRuleT: dmt.Task.CaseFieldRuleT,
					GlobalSqlHintS: dmt.TaskParams.SqlHintS,
				}

				attsRule, err := database.IDataCompareAttributesRule(dataRule)
				if err != nil {
					return err
				}

				// optimizer
				if !strings.EqualFold(attsRule.CompareRangeC, "") {
					encChunk := snappy.Encode(nil, []byte(attsRule.CompareRangeC))
					encryptChunk, err := stringutil.Encrypt(stringutil.BytesToString(encChunk), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						_, err = model.GetIDataCompareTaskRW().CreateDataCompareTask(txnCtx, &task.DataCompareTask{
							TaskName:        dmt.Task.TaskName,
							SchemaNameS:     attsRule.SchemaNameS,
							TableNameS:      attsRule.TableNameS,
							SchemaNameT:     attsRule.SchemaNameT,
							TableNameT:      attsRule.TableNameT,
							TableTypeS:      attsRule.TableTypeS,
							SnapshotPointS:  globalScnS,
							SnapshotPointT:  globalScnT,
							CompareMethod:   attsRule.CompareMethod,
							ColumnDetailSO:  attsRule.ColumnDetailSO,
							ColumnDetailS:   attsRule.ColumnDetailS,
							ColumnDetailTO:  attsRule.ColumnDetailTO,
							ColumnDetailT:   attsRule.ColumnDetailT,
							SqlHintS:        dmt.TaskParams.SqlHintS,
							SqlHintT:        dmt.TaskParams.SqlHintT,
							ChunkDetailS:    encryptChunk,
							ChunkDetailT:    encryptChunk,
							ConsistentReadS: strconv.FormatBool(dmt.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
						})
						if err != nil {
							return err
						}
						_, err = model.GetIDataCompareSummaryRW().CreateDataCompareSummary(txnCtx, &task.DataCompareSummary{
							TaskName:       dmt.Task.TaskName,
							SchemaNameS:    attsRule.SchemaNameS,
							TableNameS:     attsRule.TableNameS,
							SchemaNameT:    attsRule.SchemaNameT,
							TableNameT:     attsRule.TableNameT,
							SnapshotPointS: globalScnS,
							SnapshotPointT: globalScnT,
							TableRowsS:     tableRows,
							TableSizeS:     tableSize,
							ChunkTotals:    1,
						})
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

				var customColumnS string
				if !strings.EqualFold(attsRule.ColumnFieldC, "") {
					convertCharsetColumnS, err := stringutil.CharsetConvert([]byte(attsRule.ColumnFieldC), constant.CharsetUTF8MB4, constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DatasourceS.ConnectCharset)])
					if err != nil {
						return err
					}
					customColumnS = stringutil.BytesToString(convertCharsetColumnS)
				}

				columnNameSlis, err := databaseS.FindDatabaseTableBestColumn(attsRule.SchemaNameS, attsRule.TableNameS, customColumnS)
				if err != nil {
					return err
				}

				if len(columnNameSlis) == 0 {
					encChunk := snappy.Encode(nil, []byte("1 = 1"))
					encryptChunk, err := stringutil.Encrypt(stringutil.BytesToString(encChunk), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						_, err = model.GetIDataCompareTaskRW().CreateDataCompareTask(txnCtx, &task.DataCompareTask{
							TaskName:        dmt.Task.TaskName,
							SchemaNameS:     attsRule.SchemaNameS,
							TableNameS:      attsRule.TableNameS,
							SchemaNameT:     attsRule.SchemaNameT,
							TableNameT:      attsRule.TableNameT,
							TableTypeS:      attsRule.TableTypeS,
							SnapshotPointS:  globalScnS,
							SnapshotPointT:  globalScnT,
							CompareMethod:   attsRule.CompareMethod,
							ColumnDetailSO:  attsRule.ColumnDetailSO,
							ColumnDetailS:   attsRule.ColumnDetailS,
							ColumnDetailTO:  attsRule.ColumnDetailTO,
							ColumnDetailT:   attsRule.ColumnDetailT,
							SqlHintS:        dmt.TaskParams.SqlHintS,
							SqlHintT:        dmt.TaskParams.SqlHintT,
							ChunkDetailS:    encryptChunk,
							ChunkDetailT:    encryptChunk,
							ConsistentReadS: strconv.FormatBool(dmt.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
						})
						if err != nil {
							return err
						}
						_, err = model.GetIDataCompareSummaryRW().CreateDataCompareSummary(txnCtx, &task.DataCompareSummary{
							TaskName:       dmt.Task.TaskName,
							SchemaNameS:    attsRule.SchemaNameS,
							TableNameS:     attsRule.TableNameS,
							SchemaNameT:    attsRule.SchemaNameT,
							TableNameT:     attsRule.TableNameT,
							SnapshotPointS: globalScnS,
							SnapshotPointT: globalScnT,
							TableRowsS:     tableRows,
							TableSizeS:     tableSize,
							ChunkTotals:    1,
						})
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

				// bucket ranges
				bucketRanges, err := getDatabaseTableColumnBucket(dmt.Ctx, databaseS, databaseT, dmt.Task.TaskName, dmt.Task.TaskFlow, attsRule.SchemaNameS, attsRule.SchemaNameT, attsRule.TableNameS, attsRule.TableNameT, dbCollationS, columnNameSlis, dmt.DatasourceS.ConnectCharset, dmt.DatasourceT.ConnectCharset)
				if err != nil {
					return err
				}
				if len(bucketRanges) == 0 {
					encChunk := snappy.Encode(nil, []byte("1 = 1"))
					encryptChunk, err := stringutil.Encrypt(stringutil.BytesToString(encChunk), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						_, err = model.GetIDataCompareTaskRW().CreateDataCompareTask(txnCtx, &task.DataCompareTask{
							TaskName:        dmt.Task.TaskName,
							SchemaNameS:     attsRule.SchemaNameS,
							TableNameS:      attsRule.TableNameS,
							SchemaNameT:     attsRule.SchemaNameT,
							TableNameT:      attsRule.TableNameT,
							TableTypeS:      attsRule.TableTypeS,
							SnapshotPointS:  globalScnS,
							SnapshotPointT:  globalScnT,
							CompareMethod:   attsRule.CompareMethod,
							ColumnDetailSO:  attsRule.ColumnDetailSO,
							ColumnDetailS:   attsRule.ColumnDetailS,
							ColumnDetailTO:  attsRule.ColumnDetailTO,
							ColumnDetailT:   attsRule.ColumnDetailT,
							SqlHintS:        dmt.TaskParams.SqlHintS,
							SqlHintT:        dmt.TaskParams.SqlHintT,
							ChunkDetailS:    encryptChunk,
							ChunkDetailT:    encryptChunk,
							ConsistentReadS: strconv.FormatBool(dmt.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
						})
						if err != nil {
							return err
						}
						_, err = model.GetIDataCompareSummaryRW().CreateDataCompareSummary(txnCtx, &task.DataCompareSummary{
							TaskName:       dmt.Task.TaskName,
							SchemaNameS:    attsRule.SchemaNameS,
							TableNameS:     attsRule.TableNameS,
							SchemaNameT:    attsRule.SchemaNameT,
							TableNameT:     attsRule.TableNameT,
							SnapshotPointS: globalScnS,
							SnapshotPointT: globalScnT,
							TableRowsS:     tableRows,
							TableSizeS:     tableSize,
							ChunkTotals:    1,
						})
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

				// column route rule
				var metas []*task.DataCompareTask
				for _, r := range bucketRanges {
					encChunkS := snappy.Encode(nil, []byte(r.ToStringS()))
					encChunkT := snappy.Encode(nil, []byte(r.ToStringT()))

					encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					encryptChunkT, err := stringutil.Encrypt(stringutil.BytesToString(encChunkT), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					metas = append(metas, &task.DataCompareTask{
						TaskName:        dmt.Task.TaskName,
						SchemaNameS:     attsRule.SchemaNameS,
						TableNameS:      attsRule.TableNameS,
						SchemaNameT:     attsRule.SchemaNameT,
						TableNameT:      attsRule.TableNameT,
						TableTypeS:      attsRule.TableTypeS,
						SnapshotPointS:  globalScnS,
						SnapshotPointT:  globalScnT,
						CompareMethod:   attsRule.CompareMethod,
						ColumnDetailSO:  attsRule.ColumnDetailSO,
						ColumnDetailS:   attsRule.ColumnDetailS,
						ColumnDetailTO:  attsRule.ColumnDetailTO,
						ColumnDetailT:   attsRule.ColumnDetailT,
						SqlHintS:        dmt.TaskParams.SqlHintS,
						SqlHintT:        dmt.TaskParams.SqlHintT,
						ChunkDetailS:    encryptChunkS,
						ChunkDetailT:    encryptChunkT,
						ConsistentReadS: strconv.FormatBool(dmt.TaskParams.EnableConsistentRead),
						TaskStatus:      constant.TaskDatabaseStatusWaiting,
					})
				}

				err = model.Transaction(gCtx, func(txnCtx context.Context) error {
					err = model.GetIDataCompareTaskRW().CreateInBatchDataCompareTask(txnCtx, metas, int(dmt.TaskParams.BatchSize))
					if err != nil {
						return err
					}
					_, err = model.GetIDataCompareSummaryRW().CreateDataCompareSummary(txnCtx, &task.DataCompareSummary{
						TaskName:       dmt.Task.TaskName,
						SchemaNameS:    attsRule.SchemaNameS,
						TableNameS:     attsRule.TableNameS,
						SchemaNameT:    attsRule.SchemaNameT,
						TableNameT:     attsRule.TableNameT,
						SnapshotPointS: globalScnS,
						SnapshotPointT: globalScnT,
						TableRowsS:     tableRows,
						TableSizeS:     tableSize,
						ChunkTotals:    uint64(len(bucketRanges)),
					})
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					return err
				}

				logger.Info("data compare task init",
					zap.String("task_name", dmt.Task.TaskName),
					zap.String("task_mode", dmt.Task.TaskMode),
					zap.String("task_flow", dmt.Task.TaskFlow),
					zap.String("schema_name_s", attsRule.SchemaNameS),
					zap.String("table_name_s", attsRule.TableNameS),
					zap.String("cost", time.Now().Sub(startTime).String()))
				return nil
			}
		})
	}

	// ignore context canceled error
	if err = g.Wait(); !errors.Is(err, context.Canceled) {
		logger.Warn("data compare task init",
			zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("schema_name_s", schemaRoute.SchemaNameS),
			zap.Error(err))
		return err
	}
	_, err = model.GetITaskRW().UpdateTask(dmt.Ctx, &task.Task{TaskName: dmt.Task.TaskName}, map[string]interface{}{"TaskInit": constant.TaskInitStatusFinished})
	if err != nil {
		return err
	}
	return nil
}
