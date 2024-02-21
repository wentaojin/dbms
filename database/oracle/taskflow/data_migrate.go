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
	"strconv"
	"strings"
	"time"

	"github.com/wentaojin/dbms/errconcurrent"

	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"
	"github.com/wentaojin/dbms/database"
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

type DataMigrateTask struct {
	Ctx         context.Context
	TaskName    string
	TaskFlow    string
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.DataMigrateParam
}

func (dmt *DataMigrateTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("data migrate task get schema route",
		zap.String("task_name", dmt.TaskName),
		zap.String("task_flow", dmt.TaskFlow))
	taskInfo, err := model.GetITaskRW().GetTask(dmt.Ctx, &task.Task{TaskName: dmt.TaskName})
	if err != nil {
		return err
	}
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(dmt.Ctx, &rule.SchemaRouteRule{TaskName: dmt.TaskName})
	if err != nil {
		return err
	}

	logger.Info("data migrate task init connection",
		zap.String("task_name", dmt.TaskName),
		zap.String("task_flow", dmt.TaskFlow))
	sourceDatasource, err := model.GetIDatasourceRW().GetDatasource(dmt.Ctx, taskInfo.DatasourceNameS)
	if err != nil {
		return err
	}
	databaseS, err := database.NewDatabase(dmt.Ctx, sourceDatasource, schemaRoute.SchemaNameS)
	if err != nil {
		return err
	}
	databaseT, err := database.NewDatabase(dmt.Ctx, dmt.DatasourceT, "")
	if err != nil {
		return err
	}

	logger.Info("data migrate task inspect task",
		zap.String("task_name", dmt.TaskName),
		zap.String("task_flow", dmt.TaskFlow))
	dbCollationS, err := InspectMigrateTask(databaseS, stringutil.StringUpper(dmt.DatasourceS.ConnectCharset), stringutil.StringUpper(dmt.DatasourceT.ConnectCharset))
	if err != nil {
		return err
	}

	logger.Info("data migrate task init task",
		zap.String("task_name", dmt.TaskName),
		zap.String("task_flow", dmt.TaskFlow))
	err = dmt.InitDataMigrateTask(taskInfo, databaseS, dbCollationS, schemaRoute)
	if err != nil {
		return err
	}

	logger.Info("data migrate task get tables",
		zap.String("task_name", dmt.TaskName),
		zap.String("task_flow", dmt.TaskFlow))
	summaries, err := model.GetIDataMigrateSummaryRW().FindDataMigrateSummary(dmt.Ctx, &task.DataMigrateSummary{
		TaskName:    dmt.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	if err != nil {
		return err
	}

	logger.Info("data migrate task process tables",
		zap.String("task_name", dmt.TaskName),
		zap.String("task_flow", dmt.TaskFlow))
	for _, s := range summaries {
		logger.Info("data migrate task get chunks",
			zap.String("task_name", dmt.TaskName),
			zap.String("task_flow", dmt.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS))

		var migrateTasks []*task.DataMigrateTask
		err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
			// get migrate task tables
			migrateTasks, err = model.GetIDataMigrateTaskRW().FindDataMigrateTask(txnCtx,
				&task.DataMigrateTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					TaskStatus:  constant.TaskDatabaseStatusWaiting,
				})
			if err != nil {
				return err
			}
			migrateFailedTasks, err := model.GetIDataMigrateTaskRW().FindDataMigrateTask(txnCtx,
				&task.DataMigrateTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					TaskStatus:  constant.TaskDatabaseStatusFailed})
			if err != nil {
				return err
			}
			migrateTasks = append(migrateTasks, migrateFailedTasks...)
			return nil
		})
		if err != nil {
			return err
		}

		logger.Info("data migrate task process chunks",
			zap.String("task_name", dmt.TaskName),
			zap.String("task_flow", dmt.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS))

		g := errconcurrent.NewGroup()
		g.SetLimit(int(dmt.TaskParams.SqlThreadS))
		for _, t := range migrateTasks {
			select {
			case <-dmt.Ctx.Done():
				goto BreakLoop
			default:
				g.Go(t, func(t interface{}) error {
					dt := t.(*task.DataMigrateTask)
					switch {
					case strings.EqualFold(dmt.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(dmt.TaskFlow, constant.TaskFlowOracleToMySQL):
						sqlStr := GenMYSQLCompatibleDatabasePrepareStmt(dt.SchemaNameT, dt.TableNameT, dt.ColumnDetailT, int(dmt.TaskParams.BatchSize), true)

						stmt, err := databaseT.PrepareContext(dmt.Ctx, sqlStr)
						if err != nil {
							return err
						}
						defer stmt.Close()

						readQueue := make(chan []map[string]interface{}, constant.DefaultMigrateTaskQueueSize)
						writeQueue := make(chan []interface{}, constant.DefaultMigrateTaskQueueSize)

						err = database.IDataMigrateProcess(&DataMigrateRow{
							Ctx:           dmt.Ctx,
							Dmt:           dt,
							DatabaseS:     databaseS,
							DatabaseT:     databaseT,
							DatabaseTStmt: stmt,
							DBCharsetS:    constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DatasourceS.ConnectCharset)],
							DBCharsetT:    stringutil.StringUpper(dmt.DatasourceT.ConnectCharset),
							SqlThreadT:    int(dmt.TaskParams.SqlThreadT),
							BatchSize:     int(dmt.TaskParams.BatchSize),
							CallTimeout:   int(dmt.TaskParams.CallTimeout),
							SafeMode:      true,
							ReadQueue:     readQueue,
							WriteQueue:    writeQueue,
						})
						if err != nil {
							return err
						}
					default:
						return fmt.Errorf("oracle current task [%s] schema [%s] taskflow [%s] column rule isn't support, please contact author", dmt.TaskName, dt.SchemaNameS, dmt.TaskFlow)
					}

					return nil
				})
			}
		}
	BreakLoop:
		logger.Warn("data migrate task",
			zap.String("task_name", dmt.TaskName),
			zap.String("task_flow", dmt.TaskFlow),
			zap.String("warn", "the task data migrate has be canceled"))

		for _, r := range g.Wait() {
			if r.Err != nil {
				smt := r.Task.(*task.DataMigrateTask)
				logger.Warn("data migrate task",
					zap.String("task_name", smt.TaskName),
					zap.String("task_flow", smt.TaskFlow),
					zap.String("schema_name_s", smt.SchemaNameS),
					zap.String("table_name_s", smt.TableNameS),
					zap.Error(r.Err))

				errW := model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
					_, err = model.GetIDataMigrateTaskRW().UpdateDataMigrateTask(txnCtx,
						&task.DataMigrateTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS},
						map[string]interface{}{
							"TaskStatus":  constant.TaskDatabaseStatusFailed,
							"ErrorDetail": r.Err.Error(),
						})
					if err != nil {
						return err
					}
					_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
						TaskName:    smt.TaskName,
						SchemaNameS: smt.SchemaNameS,
						TableNameS:  smt.TableNameS,
						LogDetail: fmt.Sprintf("%v [%v] data migrate task [%v] taskflow [%v] source table [%v.%v] failed, please see [data_migrate_task] detail",
							stringutil.CurrentTimeFormatString(),
							constant.TaskModeDataMigrate,
							smt.TaskName,
							smt.TaskFlow,
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
	}
	logger.Info("data migrate task",
		zap.String("task_name", dmt.TaskName),
		zap.String("task_flow", dmt.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}

func (dmt *DataMigrateTask) InitDataMigrateTask(taskInfo *task.Task, databaseS database.IDatabase, dbCollationS bool, schemaRoute *rule.SchemaRouteRule) error {
	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(dmt.Ctx, &rule.MigrateTaskTable{
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
		globalScn      uint64
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

	// clear the data migrate task table
	// repeatInitTableMap used for store the struct_migrate_task table name has be finished, avoid repeated initialization
	migrateGroupTasks, err := model.GetIDataMigrateTaskRW().FindDataMigrateTaskGroupByTaskSchemaTable(dmt.Ctx)
	if err != nil {
		return err
	}
	repeatInitTableMap := make(map[string]struct{})

	if len(migrateGroupTasks) > 0 {
		taskTablesMap := make(map[string]struct{})
		for _, t := range databaseTables {
			taskTablesMap[t] = struct{}{}
		}
		for _, smt := range migrateGroupTasks {
			if smt.SchemaNameS == schemaRoute.SchemaNameS {
				if _, ok := taskTablesMap[smt.TableNameS]; !ok {
					err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
						err = model.GetIDataMigrateSummaryRW().DeleteDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
							TaskName:    smt.TaskName,
							SchemaNameS: smt.SchemaNameS,
							TableNameS:  smt.TableNameS,
						})
						if err != nil {
							return err
						}
						err = model.GetIDataMigrateTaskRW().DeleteDataMigrateTask(txnCtx, &task.DataMigrateTask{
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
				var (
					summary       *task.DataMigrateSummary
					currentCounts int64
				)
				err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
					summary, err = model.GetIDataMigrateSummaryRW().GetDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
						TaskName:    smt.TaskName,
						SchemaNameS: smt.SchemaNameS,
						TableNameS:  smt.TableNameS,
					})
					if err != nil {
						return err
					}
					currentCounts, err = model.GetIDataMigrateTaskRW().FindDataMigrateTaskBySchemaTableChunkCounts(txnCtx, &task.DataMigrateTask{
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
				if int64(summary.ChunkTotals) != currentCounts {
					err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
						err = model.GetIDataMigrateSummaryRW().DeleteDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
							TaskName:    smt.TaskName,
							SchemaNameS: smt.SchemaNameS,
							TableNameS:  smt.TableNameS,
						})
						if err != nil {
							return err
						}
						err = model.GetIDataMigrateTaskRW().DeleteDataMigrateTask(txnCtx, &task.DataMigrateTask{
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

	databaseTables, err = databaseS.FilterDatabaseTable(schemaRoute.SchemaNameS, includeTables, excludeTables)
	if err != nil {
		return err
	}
	databaseTableTypeMap, err = databaseS.GetDatabaseTableType(schemaRoute.SchemaNameS)
	if err != nil {
		return err
	}

	globalScn, err = databaseS.GetDatabaseCurrentSCN()
	if err != nil {
		return err
	}

	// database tables
	// init database table
	logger.Info("data migrate task init tables",
		zap.String("task_name", dmt.TaskName),
		zap.String("task_flow", dmt.TaskFlow))
	g, gCtx := errgroup.WithContext(dmt.Ctx)
	g.SetLimit(int(dmt.TaskParams.TableThread))

	for _, taskJob := range databaseTables {
		select {
		case <-gCtx.Done():
			goto BreakLoop
		default:
			sourceTable := taskJob
			g.Go(func() error {
				startTime := time.Now()
				if _, ok := repeatInitTableMap[sourceTable]; ok {
					// skip
					return nil
				}

				tableRows, err := databaseS.GetDatabaseTableRowsByStatistics(schemaRoute.SchemaNameS, sourceTable)
				if err != nil {
					return err
				}
				tableSize, err := databaseS.GetDatabaseTableSizeBySegment(schemaRoute.SchemaNameS, sourceTable)
				if err != nil {
					return err
				}

				dataRule := &DataMigrateRule{
					Ctx:            gCtx,
					TaskMode:       taskInfo.TaskMode,
					TaskName:       dmt.TaskName,
					TaskFlow:       dmt.TaskFlow,
					DatabaseS:      databaseS,
					DBCollationS:   dbCollationS,
					SchemaNameS:    schemaRoute.SchemaNameS,
					TableNameS:     sourceTable,
					TableTypeS:     databaseTableTypeMap,
					DBCharsetS:     dmt.DatasourceS.ConnectCharset,
					CaseFieldRuleS: taskInfo.CaseFieldRuleS,
					CaseFieldRuleT: taskInfo.CaseFieldRuleT,
					GlobalSqlHintS: dmt.TaskParams.SqlHintS,
				}

				attsRule, err := database.IDataMigrateAttributesRule(dataRule)
				if err != nil {
					return err
				}

				chunkTask := uuid.New().String()

				err = databaseS.CreateDatabaseTableChunkTask(chunkTask)
				if err != nil {
					return err
				}

				err = databaseS.StartDatabaseTableChunkTask(chunkTask, schemaRoute.SchemaNameS, sourceTable, dmt.TaskParams.ChunkSize, dmt.TaskParams.CallTimeout)
				if err != nil {
					return err
				}

				chunks, err := databaseS.GetDatabaseTableChunkData(chunkTask)
				if err != nil {
					return err
				}

				var whereRange string

				if len(chunks) == 0 {
					switch {
					case attsRule.EnableChunkStrategy && !strings.EqualFold(attsRule.WhereRange, ""):
						whereRange = stringutil.StringBuilder(`1 = 1 AND `, attsRule.WhereRange)
					default:
						whereRange = `1 = 1`
					}

					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						_, err = model.GetIDataMigrateTaskRW().CreateDataMigrateTask(txnCtx, &task.DataMigrateTask{
							TaskName:        dmt.TaskName,
							TaskFlow:        dmt.TaskFlow,
							SchemaNameS:     attsRule.SchemaNameS,
							TableNameS:      attsRule.TableNameS,
							SchemaNameT:     attsRule.SchemaNameT,
							TableNameT:      attsRule.TableNameT,
							TableTypeS:      attsRule.TableTypeS,
							GlobalScnS:      globalScn,
							ColumnDetailS:   attsRule.ColumnDetailS,
							ColumnDetailT:   attsRule.ColumnDetailT,
							SqlHintS:        attsRule.SqlHintS,
							SqlHintT:        dmt.TaskName,
							ChunkDetailS:    whereRange,
							ConsistentReadS: strconv.FormatBool(dmt.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
						})
						if err != nil {
							return err
						}
						_, err = model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
							TaskName:     dmt.TaskName,
							TaskFlow:     dmt.TaskFlow,
							SchemaNameS:  attsRule.SchemaNameS,
							TableNameS:   attsRule.TableNameS,
							SchemaNameT:  attsRule.SchemaNameT,
							TableNameT:   attsRule.TableNameT,
							TableRowsS:   tableRows,
							TableSizeS:   tableSize,
							ChunkTotals:  1,
							ChunkSuccess: 0,
							ChunkFails:   0,
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

				var metas []*task.DataMigrateTask
				for _, r := range chunks {
					switch {
					case attsRule.EnableChunkStrategy && !strings.EqualFold(attsRule.WhereRange, ""):
						whereRange = stringutil.StringBuilder(r["CMD"], ` AND `, attsRule.WhereRange)
					default:
						whereRange = r["CMD"]
					}

					metas = append(metas, &task.DataMigrateTask{
						TaskName:        dmt.TaskName,
						TaskFlow:        dmt.TaskFlow,
						SchemaNameS:     attsRule.SchemaNameS,
						TableNameS:      attsRule.TableNameS,
						SchemaNameT:     attsRule.SchemaNameT,
						TableNameT:      attsRule.TableNameT,
						TableTypeS:      attsRule.TableTypeS,
						GlobalScnS:      globalScn,
						ColumnDetailS:   attsRule.ColumnDetailS,
						ColumnDetailT:   attsRule.ColumnDetailT,
						SqlHintS:        attsRule.SqlHintS,
						SqlHintT:        dmt.TaskName,
						ChunkDetailS:    whereRange,
						ConsistentReadS: strconv.FormatBool(dmt.TaskParams.EnableConsistentRead),
						TaskStatus:      constant.TaskDatabaseStatusWaiting,
					})
				}

				err = model.Transaction(gCtx, func(txnCtx context.Context) error {
					err = model.GetIDataMigrateTaskRW().CreateInBatchDataMigrateTask(txnCtx, metas, int(dmt.TaskParams.BatchSize))
					if err != nil {
						return err
					}
					_, err = model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
						TaskName:     dmt.TaskName,
						TaskFlow:     dmt.TaskFlow,
						SchemaNameS:  attsRule.SchemaNameS,
						TableNameS:   attsRule.TableNameS,
						SchemaNameT:  attsRule.SchemaNameT,
						TableNameT:   attsRule.TableNameT,
						TableRowsS:   tableRows,
						TableSizeS:   tableSize,
						ChunkTotals:  uint64(len(chunks)),
						ChunkSuccess: 0,
						ChunkFails:   0,
					})
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					return err
				}

				if err = databaseS.CloseDatabaseTableChunkTask(chunkTask); err != nil {
					return err
				}

				logger.Info("data migrate task",
					zap.String("task_name", dmt.TaskName),
					zap.String("task_flow", dmt.TaskFlow),
					zap.String("schema_name_s", attsRule.SchemaNameS),
					zap.String("table_name_s", attsRule.TableNameS),
					zap.String("cost", time.Now().Sub(startTime).String()))
				return nil
			})
		}
	}
BreakLoop:
	logger.Warn("data migrate task",
		zap.String("task_name", dmt.TaskName),
		zap.String("task_flow", dmt.TaskFlow),
		zap.String("warn", "the data migrate task init stage has be canceled"))

	if err = g.Wait(); err != nil {
		logger.Warn("data migrate task",
			zap.String("task_name", dmt.TaskName),
			zap.String("task_flow", dmt.TaskFlow),
			zap.String("schema_name_s", schemaRoute.SchemaNameS),
			zap.Error(err))
		return err
	}

	err = databaseS.Close()
	if err != nil {
		return err
	}
	return nil
}
