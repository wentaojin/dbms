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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"

	"github.com/google/uuid"
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

type CsvMigrateTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.CsvMigrateParam
}

func (cmt *CsvMigrateTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("csv migrate task get schema route",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(cmt.Ctx, &rule.SchemaRouteRule{TaskName: cmt.Task.TaskName})
	if err != nil {
		return err
	}

	logger.Info("csv migrate task init database connection",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))

	sourceDatasource, err := model.GetIDatasourceRW().GetDatasource(cmt.Ctx, cmt.Task.DatasourceNameS)
	if err != nil {
		return err
	}
	databaseS, err := database.NewDatabase(cmt.Ctx, sourceDatasource, schemaRoute.SchemaNameS)
	if err != nil {
		return err
	}
	defer databaseS.Close()
	databaseT, err := database.NewDatabase(cmt.Ctx, cmt.DatasourceT, "")
	if err != nil {
		return err
	}
	defer databaseT.Close()

	logger.Info("csv migrate task inspect migrate task",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))
	dbVersion, _, dbCollationS, err := inspectMigrateTask(cmt.Task.TaskName, cmt.Task.TaskFlow, cmt.Task.TaskMode, databaseS, stringutil.StringUpper(cmt.DatasourceS.ConnectCharset), stringutil.StringUpper(cmt.DatasourceT.ConnectCharset))
	if err != nil {
		return err
	}

	logger.Info("csv migrate task init task",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))
	err = cmt.InitCsvMigrateTask(databaseS, dbVersion, dbCollationS, schemaRoute)
	if err != nil {
		return err
	}

	logger.Info("csv migrate task get tables",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))

	summaries, err := model.GetIDataMigrateSummaryRW().FindDataMigrateSummary(cmt.Ctx, &task.DataMigrateSummary{
		TaskName:    cmt.Task.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	if err != nil {
		return err
	}

	for _, s := range summaries {
		startTableTime := time.Now()

		statfs, err := stringutil.GetDiskUsage(cmt.TaskParams.OutputDir)
		if err != nil {
			return err
		}
		// MB
		diskFactor, err := stringutil.StrconvFloatBitSize(cmt.TaskParams.DiskUsageFactor, 64)
		if err != nil {
			return err
		}
		estmTableSizeMB := s.TableSizeS * diskFactor

		totalSpace := statfs.Blocks * uint64(statfs.Bsize) / 1024 / 1024
		freeSpace := statfs.Bfree * uint64(statfs.Bsize) / 1024 / 1024
		usedSpace := totalSpace - freeSpace

		if freeSpace < uint64(estmTableSizeMB) {
			logger.Warn("csv migrate task disk usage",
				zap.String("task_name", cmt.Task.TaskName),
				zap.String("task_mode", cmt.Task.TaskMode),
				zap.String("task_flow", cmt.Task.TaskFlow),
				zap.String("schema_name_s", s.SchemaNameS),
				zap.String("table_name_s", s.TableNameS),
				zap.String("output_dir", cmt.TaskParams.OutputDir),
				zap.Uint64("disk total space(MB)", totalSpace),
				zap.Uint64("disk used space(MB)", usedSpace),
				zap.Uint64("disk free space(MB)", freeSpace),
				zap.Uint64("estimate table space(MB)", uint64(estmTableSizeMB)))
			_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(cmt.Ctx, &task.DataMigrateSummary{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			}, map[string]interface{}{
				"Refused": fmt.Sprintf("the output [%s] current disk quota isn't enough, total space(MB): [%v], used space(MB): [%v], free space(MB): [%v], estimate space(MB): [%v]", cmt.TaskParams.OutputDir, totalSpace, usedSpace, freeSpace, estmTableSizeMB),
			})
			if err != nil {
				return err
			}
			// skip
			continue
		}

		err = stringutil.PathNotExistOrCreate(filepath.Join(
			cmt.TaskParams.OutputDir,
			s.SchemaNameS,
			s.TableNameS,
		))
		if err != nil {
			return err
		}

		logger.Info("csv migrate task process table",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS),
			zap.String("output_dir", cmt.TaskParams.OutputDir),
			zap.Uint64("disk total space(MB)", totalSpace),
			zap.Uint64("disk used space(MB)", usedSpace),
			zap.Uint64("disk free space(MB)", freeSpace),
			zap.Uint64("estimate table space(MB)", uint64(estmTableSizeMB)))

		var migrateTasks []*task.DataMigrateTask
		err = model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
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
			migrateRunningTasks, err := model.GetIDataMigrateTaskRW().FindDataMigrateTask(txnCtx,
				&task.DataMigrateTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					TaskStatus:  constant.TaskDatabaseStatusRunning})
			if err != nil {
				return err
			}
			migrateStopTasks, err := model.GetIDataMigrateTaskRW().FindDataMigrateTask(txnCtx,
				&task.DataMigrateTask{
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

		logger.Info("csv migrate task process chunks",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS))
		g := errconcurrent.NewGroup()
		g.SetLimit(int(cmt.TaskParams.SqlThreadS))
		for _, j := range migrateTasks {
			gTime := time.Now()
			g.Go(j, gTime, func(j interface{}) error {
				select {
				case <-cmt.Ctx.Done():
					return nil
				default:
					dt := j.(*task.DataMigrateTask)
					errW := model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIDataMigrateTaskRW().UpdateDataMigrateTask(txnCtx,
							&task.DataMigrateTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS, ChunkDetailS: dt.ChunkDetailS},
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
							LogDetail: fmt.Sprintf("%v [%v] csv migrate task [%v] taskflow [%v] source table [%v.%v] chunk [%s] start",
								stringutil.CurrentTimeFormatString(),
								stringutil.StringLower(constant.TaskModeCSVMigrate),
								dt.TaskName,
								cmt.Task.TaskMode,
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

					err = database.IDataMigrateProcess(&CsvMigrateRow{
						Ctx:        cmt.Ctx,
						TaskMode:   cmt.Task.TaskMode,
						TaskFlow:   cmt.Task.TaskFlow,
						BufioSize:  constant.DefaultMigrateTaskBufferIOSize,
						Dmt:        dt,
						DatabaseS:  databaseS,
						DBCharsetS: constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(cmt.DatasourceS.ConnectCharset)],
						DBCharsetT: stringutil.StringUpper(cmt.DatasourceT.ConnectCharset),
						TaskParams: cmt.TaskParams,
						ReadChan:   make(chan []string, constant.DefaultMigrateTaskQueueSize),
						WriteChan:  make(chan string, constant.DefaultMigrateTaskQueueSize),
					})
					if err != nil {
						return err
					}

					errW = model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIDataMigrateTaskRW().UpdateDataMigrateTask(txnCtx,
							&task.DataMigrateTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS, ChunkDetailS: dt.ChunkDetailS},
							map[string]interface{}{
								"TaskStatus": constant.TaskDatabaseStatusSuccess,
								"Duration":   fmt.Sprintf("%f", time.Now().Sub(gTime).Seconds()),
							})
						if err != nil {
							return err
						}
						_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
							TaskName:    dt.TaskName,
							SchemaNameS: dt.SchemaNameS,
							TableNameS:  dt.TableNameS,
							LogDetail: fmt.Sprintf("%v [%v] csv migrate task [%v] taskflow [%v] source table [%v.%v] chunk [%s] success",
								stringutil.CurrentTimeFormatString(),
								stringutil.StringLower(constant.TaskModeCSVMigrate),
								dt.TaskName,
								cmt.Task.TaskMode,
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
					return nil
				}
			})
		}

		for _, r := range g.Wait() {
			if r.Err != nil {
				smt := r.Task.(*task.DataMigrateTask)
				logger.Warn("csv migrate task process tables",
					zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow),
					zap.String("schema_name_s", smt.SchemaNameS),
					zap.String("table_name_s", smt.TableNameS),
					zap.Error(r.Err))

				errW := model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
					_, err = model.GetIDataMigrateTaskRW().UpdateDataMigrateTask(txnCtx,
						&task.DataMigrateTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS, ChunkDetailS: smt.ChunkDetailS},
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
						LogDetail: fmt.Sprintf("%v [%v] csv migrate task [%v] taskflow [%v] source table [%v.%v] failed, please see [data_migrate_task] detail",
							stringutil.CurrentTimeFormatString(),
							stringutil.StringLower(constant.TaskModeStmtMigrate),
							smt.TaskName,
							cmt.Task.TaskMode,
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
		err = model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
			tableStatusRecs, err := model.GetIDataMigrateTaskRW().FindDataMigrateTaskBySchemaTableChunkStatus(txnCtx, &task.DataMigrateTask{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			})
			if err != nil {
				return err
			}
			for _, rec := range tableStatusRecs {
				switch rec.TaskStatus {
				case constant.TaskDatabaseStatusSuccess:
					_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
						TaskName:    rec.TaskName,
						SchemaNameS: rec.SchemaNameS,
						TableNameS:  rec.TableNameS,
					}, map[string]interface{}{
						"ChunkSuccess": rec.StatusTotals,
					})
					if err != nil {
						return err
					}
				case constant.TaskDatabaseStatusFailed:
					_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
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
					_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
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
					_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
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
					_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
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
					return fmt.Errorf("the task [%v] task_mode [%s] task_flow [%v] schema_name_s [%v] table_name_s [%v] task_status [%v] panic, please contact auhtor or reselect", s.TaskName, cmt.Task.TaskMode, cmt.Task.TaskFlow, rec.SchemaNameS, rec.TableNameS, rec.TaskStatus)
				}
			}

			_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			}, map[string]interface{}{
				"Refused":  "", // reset
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

		logger.Info("csv migrate task process table",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS),
			zap.String("cost", endTableTime.Sub(startTableTime).String()))
	}
	logger.Info("csv migrate task",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}

func (cmt *CsvMigrateTask) InitCsvMigrateTask(databaseS database.IDatabase, dbVersion string, dbCollationS bool, schemaRoute *rule.SchemaRouteRule) error {
	// delete checkpoint
	initFlags, err := model.GetITaskRW().GetTask(cmt.Ctx, &task.Task{TaskName: cmt.Task.TaskName})
	if err != nil {
		return err
	}
	if !cmt.TaskParams.EnableCheckpoint || strings.EqualFold(initFlags.TaskInit, constant.TaskInitStatusNotFinished) {
		err := model.GetIDataMigrateSummaryRW().DeleteDataMigrateSummaryName(cmt.Ctx, []string{schemaRoute.TaskName})
		if err != nil {
			return err
		}
		err = model.GetIDataMigrateTaskRW().DeleteDataMigrateTaskName(cmt.Ctx, []string{schemaRoute.TaskName})
		if err != nil {
			return err
		}
	} else if cmt.TaskParams.EnableCheckpoint && strings.EqualFold(initFlags.TaskInit, constant.TaskInitStatusFinished) {
		logger.Warn("csv migrate task init skip",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("task_init", constant.TaskInitStatusFinished))
		return nil
	}

	dbRole, err := databaseS.GetDatabaseRole()
	if err != nil {
		return err
	}

	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(cmt.Ctx, &rule.MigrateTaskTable{
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
		globalScn          string
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

	databaseFilterTables, err := databaseS.FilterDatabaseTable(schemaRoute.SchemaNameS, includeTables, excludeTables)
	if err != nil {
		return err
	}

	// rule case field
	for _, t := range databaseFilterTables {
		var tabName string
		// the according target case field rule convert
		if strings.EqualFold(cmt.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			tabName = stringutil.StringLower(t)
		}
		if strings.EqualFold(cmt.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			tabName = stringutil.StringUpper(t)
		}
		if strings.EqualFold(cmt.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
			tabName = t
		}
		databaseTaskTables = append(databaseTaskTables, tabName)
	}

	// clear the csv migrate task table
	migrateGroupTasks, err := model.GetIDataMigrateTaskRW().FindDataMigrateTaskGroupByTaskSchemaTable(cmt.Ctx, cmt.Task.TaskName)
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
					err = model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
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
				var summary *task.DataMigrateSummary

				summary, err = model.GetIDataMigrateSummaryRW().GetDataMigrateSummary(cmt.Ctx, &task.DataMigrateSummary{
					TaskName:    smt.TaskName,
					SchemaNameS: smt.SchemaNameS,
					TableNameS:  smt.TableNameS,
				})
				if err != nil {
					return err
				}

				if int64(summary.ChunkTotals) != smt.ChunkTotals {
					err = model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
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

	databaseTableTypeMap, err = databaseS.GetDatabaseTableType(schemaRoute.SchemaNameS)
	if err != nil {
		return err
	}

	globalScnS, err := databaseS.GetDatabaseConsistentPos()
	if err != nil {
		return err
	}
	globalScn = strconv.FormatUint(globalScnS, 10)

	// database tables
	// init database table
	logger.Info("csv migrate task init",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))

	g, gCtx := errgroup.WithContext(cmt.Ctx)
	g.SetLimit(int(cmt.TaskParams.TableThread))

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

				dataRule := &DataMigrateRule{
					Ctx:            gCtx,
					TaskMode:       cmt.Task.TaskMode,
					TaskName:       cmt.Task.TaskName,
					TaskFlow:       cmt.Task.TaskFlow,
					DatabaseS:      databaseS,
					DBCollationS:   dbCollationS,
					SchemaNameS:    schemaRoute.SchemaNameS,
					TableNameS:     sourceTable,
					TableTypeS:     databaseTableTypeMap,
					DBCharsetS:     cmt.DatasourceS.ConnectCharset,
					CaseFieldRuleS: cmt.Task.CaseFieldRuleS,
					CaseFieldRuleT: cmt.Task.CaseFieldRuleT,
					GlobalSqlHintS: cmt.TaskParams.SqlHintS,
				}

				attsRule, err := database.IDataMigrateAttributesRule(dataRule)
				if err != nil {
					return err
				}

				// only where range
				if !attsRule.EnableChunkStrategy && !strings.EqualFold(attsRule.WhereRange, "") {
					encChunkS := snappy.Encode(nil, []byte(attsRule.WhereRange))

					encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}
					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						_, err = model.GetIDataMigrateTaskRW().CreateDataMigrateTask(txnCtx, &task.DataMigrateTask{
							TaskName:        cmt.Task.TaskName,
							SchemaNameS:     attsRule.SchemaNameS,
							TableNameS:      attsRule.TableNameS,
							SchemaNameT:     attsRule.SchemaNameT,
							TableNameT:      attsRule.TableNameT,
							TableTypeS:      attsRule.TableTypeS,
							SnapshotPointS:  globalScn,
							ColumnDetailO:   attsRule.ColumnDetailO,
							ColumnDetailS:   attsRule.ColumnDetailS,
							ColumnDetailT:   attsRule.ColumnDetailT,
							SqlHintS:        attsRule.SqlHintS,
							ChunkDetailS:    encryptChunkS,
							ConsistentReadS: strconv.FormatBool(cmt.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
							CsvFile: filepath.Join(cmt.TaskParams.OutputDir, attsRule.SchemaNameS, attsRule.TableNameS,
								stringutil.StringBuilder(attsRule.SchemaNameT, `.`, attsRule.TableNameT, `.0.csv`)),
						})
						if err != nil {
							return err
						}
						_, err = model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
							TaskName:       cmt.Task.TaskName,
							SchemaNameS:    attsRule.SchemaNameS,
							TableNameS:     attsRule.TableNameS,
							SchemaNameT:    attsRule.SchemaNameT,
							TableNameT:     attsRule.TableNameT,
							SnapshotPointS: globalScn,
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

				var whereRange string
				// statistic
				if !strings.EqualFold(dbRole, constant.OracleDatabasePrimaryRole) || (strings.EqualFold(dbRole, constant.OracleDatabasePrimaryRole) && stringutil.VersionOrdinal(dbVersion) < stringutil.VersionOrdinal(constant.OracleDatabaseTableMigrateRowidRequireVersion)) {
					columnNameSlis, err := databaseS.FindDatabaseTableBestColumn(attsRule.SchemaNameS, attsRule.TableNameS, "")
					if err != nil {
						return err
					}
					if len(columnNameSlis) == 0 {
						logger.Warn("csv migrate task table",
							zap.String("task_name", cmt.Task.TaskName),
							zap.String("task_mode", cmt.Task.TaskMode),
							zap.String("task_flow", cmt.Task.TaskFlow),
							zap.String("schema_name_s", attsRule.SchemaNameS),
							zap.String("table_name_s", attsRule.TableNameS),
							zap.String("database_version", dbVersion),
							zap.String("database_role", dbRole),
							zap.String("migrate_method", "scan"))
						switch {
						case attsRule.EnableChunkStrategy && !strings.EqualFold(attsRule.WhereRange, ""):
							whereRange = stringutil.StringBuilder(`1 = 1 AND `, attsRule.WhereRange)
						default:
							whereRange = `1 = 1`
						}

						encChunkS := snappy.Encode(nil, []byte(whereRange))

						encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
						if err != nil {
							return err
						}

						err = model.Transaction(gCtx, func(txnCtx context.Context) error {
							_, err = model.GetIDataMigrateTaskRW().CreateDataMigrateTask(txnCtx, &task.DataMigrateTask{
								TaskName:        cmt.Task.TaskName,
								SchemaNameS:     attsRule.SchemaNameS,
								TableNameS:      attsRule.TableNameS,
								SchemaNameT:     attsRule.SchemaNameT,
								TableNameT:      attsRule.TableNameT,
								TableTypeS:      attsRule.TableTypeS,
								SnapshotPointS:  globalScn,
								ColumnDetailO:   attsRule.ColumnDetailO,
								ColumnDetailS:   attsRule.ColumnDetailS,
								ColumnDetailT:   attsRule.ColumnDetailT,
								SqlHintS:        attsRule.SqlHintS,
								ChunkDetailS:    encryptChunkS,
								ConsistentReadS: strconv.FormatBool(cmt.TaskParams.EnableConsistentRead),
								TaskStatus:      constant.TaskDatabaseStatusWaiting,
								CsvFile: filepath.Join(cmt.TaskParams.OutputDir, attsRule.SchemaNameS, attsRule.TableNameS,
									stringutil.StringBuilder(attsRule.SchemaNameT, `.`, attsRule.TableNameT, `.0.csv`)),
							})
							if err != nil {
								return err
							}
							_, err = model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
								TaskName:       cmt.Task.TaskName,
								SchemaNameS:    attsRule.SchemaNameS,
								TableNameS:     attsRule.TableNameS,
								SchemaNameT:    attsRule.SchemaNameT,
								TableNameT:     attsRule.TableNameT,
								SnapshotPointS: globalScn,
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

					bucketRanges, err := getDatabaseTableColumnBucket(cmt.Ctx, databaseS, nil, cmt.Task.TaskName, cmt.Task.TaskFlow, attsRule.SchemaNameS, attsRule.SchemaNameT, attsRule.TableNameS, attsRule.TableNameT, dbCollationS, columnNameSlis, cmt.DatasourceS.ConnectCharset, cmt.DatasourceT.ConnectCharset)
					if err != nil {
						return err
					}
					logger.Warn("csv migrate task table",
						zap.String("task_name", cmt.Task.TaskName),
						zap.String("task_mode", cmt.Task.TaskMode),
						zap.String("task_flow", cmt.Task.TaskFlow),
						zap.String("schema_name_s", attsRule.SchemaNameS),
						zap.String("table_name_s", attsRule.TableNameS),
						zap.String("database_version", dbVersion),
						zap.String("database_role", dbRole),
						zap.String("migrate_method", "statistic"))
					if len(bucketRanges) == 0 {
						switch {
						case attsRule.EnableChunkStrategy && !strings.EqualFold(attsRule.WhereRange, ""):
							whereRange = stringutil.StringBuilder(`1 = 1 AND `, attsRule.WhereRange)
						default:
							whereRange = `1 = 1`
						}

						encChunkS := snappy.Encode(nil, []byte(whereRange))

						encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
						if err != nil {
							return err
						}

						err = model.Transaction(gCtx, func(txnCtx context.Context) error {
							_, err = model.GetIDataMigrateTaskRW().CreateDataMigrateTask(txnCtx, &task.DataMigrateTask{
								TaskName:        cmt.Task.TaskName,
								SchemaNameS:     attsRule.SchemaNameS,
								TableNameS:      attsRule.TableNameS,
								SchemaNameT:     attsRule.SchemaNameT,
								TableNameT:      attsRule.TableNameT,
								TableTypeS:      attsRule.TableTypeS,
								SnapshotPointS:  globalScn,
								ColumnDetailO:   attsRule.ColumnDetailO,
								ColumnDetailS:   attsRule.ColumnDetailS,
								ColumnDetailT:   attsRule.ColumnDetailT,
								SqlHintS:        attsRule.SqlHintS,
								ChunkDetailS:    encryptChunkS,
								ConsistentReadS: strconv.FormatBool(cmt.TaskParams.EnableConsistentRead),
								TaskStatus:      constant.TaskDatabaseStatusWaiting,
								CsvFile: filepath.Join(cmt.TaskParams.OutputDir, attsRule.SchemaNameS, attsRule.TableNameS,
									stringutil.StringBuilder(attsRule.SchemaNameT, `.`, attsRule.TableNameT, `.0.csv`)),
							})
							if err != nil {
								return err
							}
							_, err = model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
								TaskName:       cmt.Task.TaskName,
								SchemaNameS:    attsRule.SchemaNameS,
								TableNameS:     attsRule.TableNameS,
								SchemaNameT:    attsRule.SchemaNameT,
								TableNameT:     attsRule.TableNameT,
								SnapshotPointS: globalScn,
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

					var metas []*task.DataMigrateTask
					for idx, r := range bucketRanges {
						switch {
						case attsRule.EnableChunkStrategy && !strings.EqualFold(attsRule.WhereRange, ""):
							whereRange = stringutil.StringBuilder(r.ToStringS(), ` AND `, attsRule.WhereRange)
						default:
							whereRange = r.ToStringS()
						}

						encChunkS := snappy.Encode(nil, []byte(whereRange))

						encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
						if err != nil {
							return err
						}
						metas = append(metas, &task.DataMigrateTask{
							TaskName:        cmt.Task.TaskName,
							SchemaNameS:     attsRule.SchemaNameS,
							TableNameS:      attsRule.TableNameS,
							SchemaNameT:     attsRule.SchemaNameT,
							TableNameT:      attsRule.TableNameT,
							TableTypeS:      attsRule.TableTypeS,
							SnapshotPointS:  globalScn,
							ColumnDetailO:   attsRule.ColumnDetailO,
							ColumnDetailS:   attsRule.ColumnDetailS,
							ColumnDetailT:   attsRule.ColumnDetailT,
							SqlHintS:        attsRule.SqlHintS,
							ChunkDetailS:    encryptChunkS,
							ConsistentReadS: strconv.FormatBool(cmt.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
							CsvFile: filepath.Join(cmt.TaskParams.OutputDir, attsRule.SchemaNameS, attsRule.TableNameS,
								stringutil.StringBuilder(attsRule.SchemaNameT, `.`, attsRule.TableNameT, `.`, strconv.Itoa(idx), `.csv`)),
						})
					}

					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						err = model.GetIDataMigrateTaskRW().CreateInBatchDataMigrateTask(txnCtx, metas, int(cmt.TaskParams.BatchSize))
						if err != nil {
							return err
						}
						_, err = model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
							TaskName:       cmt.Task.TaskName,
							SchemaNameS:    attsRule.SchemaNameS,
							TableNameS:     attsRule.TableNameS,
							SchemaNameT:    attsRule.SchemaNameT,
							TableNameT:     attsRule.TableNameT,
							SnapshotPointS: globalScn,
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

					return nil
				}

				chunkTask := uuid.New().String()
				chunks, err := databaseS.GetDatabaseTableChunkTask(chunkTask, schemaRoute.SchemaNameS, sourceTable, cmt.TaskParams.ChunkSize, cmt.TaskParams.CallTimeout)
				if err != nil {
					return err
				}

				if len(chunks) == 0 {
					switch {
					case attsRule.EnableChunkStrategy && !strings.EqualFold(attsRule.WhereRange, ""):
						whereRange = stringutil.StringBuilder(`1 = 1 AND `, attsRule.WhereRange)
					default:
						whereRange = `1 = 1`
					}

					encChunkS := snappy.Encode(nil, []byte(whereRange))

					encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}

					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						_, err = model.GetIDataMigrateTaskRW().CreateDataMigrateTask(txnCtx, &task.DataMigrateTask{
							TaskName:        cmt.Task.TaskName,
							SchemaNameS:     attsRule.SchemaNameS,
							TableNameS:      attsRule.TableNameS,
							SchemaNameT:     attsRule.SchemaNameT,
							TableNameT:      attsRule.TableNameT,
							TableTypeS:      attsRule.TableTypeS,
							SnapshotPointS:  globalScn,
							ColumnDetailO:   attsRule.ColumnDetailO,
							ColumnDetailS:   attsRule.ColumnDetailS,
							ColumnDetailT:   attsRule.ColumnDetailT,
							SqlHintS:        attsRule.SqlHintS,
							ChunkDetailS:    encryptChunkS,
							ConsistentReadS: strconv.FormatBool(cmt.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
							CsvFile: filepath.Join(cmt.TaskParams.OutputDir, attsRule.SchemaNameS, attsRule.TableNameS,
								stringutil.StringBuilder(attsRule.SchemaNameT, `.`, attsRule.TableNameT, `.0.csv`)),
						})
						if err != nil {
							return err
						}
						_, err = model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
							TaskName:       cmt.Task.TaskName,
							SchemaNameS:    attsRule.SchemaNameS,
							TableNameS:     attsRule.TableNameS,
							SchemaNameT:    attsRule.SchemaNameT,
							TableNameT:     attsRule.TableNameT,
							SnapshotPointS: globalScn,
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

				var metas []*task.DataMigrateTask
				for i, r := range chunks {
					csvFile := filepath.Join(cmt.TaskParams.OutputDir, attsRule.SchemaNameS, attsRule.TableNameS,
						stringutil.StringBuilder(attsRule.SchemaNameT, `.`, attsRule.TableNameT, `.`, strconv.Itoa(i), `.csv`))
					switch {
					case attsRule.EnableChunkStrategy && !strings.EqualFold(attsRule.WhereRange, ""):
						whereRange = stringutil.StringBuilder(r["CMD"], ` AND `, attsRule.WhereRange)
					default:
						whereRange = r["CMD"]
					}

					encChunkS := snappy.Encode(nil, []byte(whereRange))

					encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}

					metas = append(metas, &task.DataMigrateTask{
						TaskName:        cmt.Task.TaskName,
						SchemaNameS:     attsRule.SchemaNameS,
						TableNameS:      attsRule.TableNameS,
						SchemaNameT:     attsRule.SchemaNameT,
						TableNameT:      attsRule.TableNameT,
						TableTypeS:      attsRule.TableTypeS,
						SnapshotPointS:  globalScn,
						ColumnDetailO:   attsRule.ColumnDetailO,
						ColumnDetailS:   attsRule.ColumnDetailS,
						ColumnDetailT:   attsRule.ColumnDetailT,
						SqlHintS:        attsRule.SqlHintS,
						ChunkDetailS:    encryptChunkS,
						ConsistentReadS: strconv.FormatBool(cmt.TaskParams.EnableConsistentRead),
						TaskStatus:      constant.TaskDatabaseStatusWaiting,
						CsvFile:         csvFile,
					})
				}

				err = model.Transaction(gCtx, func(txnCtx context.Context) error {
					err = model.GetIDataMigrateTaskRW().CreateInBatchDataMigrateTask(txnCtx, metas, int(cmt.TaskParams.BatchSize))
					if err != nil {
						return err
					}
					_, err = model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
						TaskName:       cmt.Task.TaskName,
						SchemaNameS:    attsRule.SchemaNameS,
						TableNameS:     attsRule.TableNameS,
						SchemaNameT:    attsRule.SchemaNameT,
						TableNameT:     attsRule.TableNameT,
						SnapshotPointS: globalScn,
						TableRowsS:     tableRows,
						TableSizeS:     tableSize,
						ChunkTotals:    uint64(len(chunks)),
					})
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					return err
				}

				logger.Info("csv migrate task init",
					zap.String("task_name", cmt.Task.TaskName),
					zap.String("task_mode", cmt.Task.TaskMode),
					zap.String("task_flow", cmt.Task.TaskFlow),
					zap.String("schema_name_s", attsRule.SchemaNameS),
					zap.String("table_name_s", attsRule.TableNameS),
					zap.String("cost", time.Now().Sub(startTime).String()))
				return nil
			}
		})
	}

	// ignore context canceled error
	if err = g.Wait(); !errors.Is(err, context.Canceled) {
		logger.Warn("csv migrate task init",
			zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", schemaRoute.SchemaNameS),
			zap.Error(err))
		return err
	}

	_, err = model.GetITaskRW().UpdateTask(cmt.Ctx, &task.Task{TaskName: cmt.Task.TaskName}, map[string]interface{}{"TaskInit": constant.TaskInitStatusFinished})
	if err != nil {
		return err
	}
	return nil
}
