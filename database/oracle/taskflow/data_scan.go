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
	"strconv"
	"strings"
	"time"
)

type DataScanTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.DataScanParam
}

func (dst *DataScanTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("data scan task get schema route",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))
	schemaNameRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(dst.Ctx, &rule.SchemaRouteRule{TaskName: dst.Task.TaskName})
	if err != nil {
		return err
	}
	schemaNameS := schemaNameRoute.SchemaNameS

	logger.Info("data scan task init database connection",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))

	datasourceS, err := model.GetIDatasourceRW().GetDatasource(dst.Ctx, dst.Task.DatasourceNameS)
	if err != nil {
		return err
	}
	databaseS, err := database.NewDatabase(dst.Ctx, datasourceS, schemaNameS)
	if err != nil {
		return err
	}
	defer databaseS.Close()

	logger.Info("data scan task inspect migrate task",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))

	dbVersion, _, dbCollationS, err := inspectMigrateTask(dst.Task.TaskName, dst.Task.TaskFlow, dst.Task.TaskMode, databaseS, stringutil.StringUpper(dst.DatasourceS.ConnectCharset), stringutil.StringUpper(dst.DatasourceT.ConnectCharset))
	if err != nil {
		return err
	}

	logger.Info("data scan task init task",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))
	err = dst.initDataScanTask(databaseS, dbVersion, dbCollationS, schemaNameS)
	if err != nil {
		return err
	}

	logger.Info("data scan task get tables",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))

	summaries, err := model.GetIDataScanSummaryRW().FindDataScanSummary(dst.Ctx, &task.DataScanSummary{
		TaskName:    dst.Task.TaskName,
		SchemaNameS: schemaNameS,
	})
	if err != nil {
		return err
	}

	for _, s := range summaries {
		startTableTime := time.Now()
		logger.Info("data scan task process table",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS))

		var migrateTasks []*task.DataScanTask
		err = model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
			// get migrate task tables
			migrateTasks, err = model.GetIDataScanTaskRW().FindDataScanTask(txnCtx,
				&task.DataScanTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					TaskStatus:  constant.TaskDatabaseStatusWaiting,
				})
			if err != nil {
				return err
			}
			migrateFailedTasks, err := model.GetIDataScanTaskRW().FindDataScanTask(txnCtx,
				&task.DataScanTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					TaskStatus:  constant.TaskDatabaseStatusFailed})
			if err != nil {
				return err
			}
			migrateRunningTasks, err := model.GetIDataScanTaskRW().FindDataScanTask(txnCtx,
				&task.DataScanTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					TaskStatus:  constant.TaskDatabaseStatusRunning})
			if err != nil {
				return err
			}
			migrateStopTasks, err := model.GetIDataScanTaskRW().FindDataScanTask(txnCtx,
				&task.DataScanTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
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

		logger.Info("data scan task process chunks",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS))

		g := errconcurrent.NewGroup()
		g.SetLimit(int(dst.TaskParams.SqlThreadS))
		for _, j := range migrateTasks {
			gTime := time.Now()
			g.Go(j, gTime, func(j interface{}) error {
				select {
				case <-dst.Ctx.Done():
					return nil
				default:
					dt := j.(*task.DataScanTask)
					errW := model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIDataScanTaskRW().UpdateDataScanTask(txnCtx,
							&task.DataScanTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS, ChunkDetailS: dt.ChunkDetailS},
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
							LogDetail: fmt.Sprintf("%v [%v] data scan task [%v] taskflow [%v] source table [%v.%v] chunk [%s] start",
								stringutil.CurrentTimeFormatString(),
								stringutil.StringLower(constant.TaskModeDataScan),
								dt.TaskName,
								dst.Task.TaskMode,
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

					err = database.IDataScanProcess(&DataScanRow{
						Ctx:        dst.Ctx,
						StartTime:  gTime,
						TaskName:   dt.TaskName,
						TaskMode:   dst.Task.TaskMode,
						TaskFlow:   dst.Task.TaskFlow,
						Dst:        dt,
						DatabaseS:  databaseS,
						DBCharsetS: stringutil.StringUpper(dst.DatasourceS.ConnectCharset),
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
				mt := r.Task.(*task.DataScanTask)
				logger.Warn("data scan task process tables",
					zap.String("task_name", mt.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow),
					zap.String("schema_name_s", mt.SchemaNameS),
					zap.String("table_name_s", mt.TableNameS),
					zap.Error(r.Err))

				errW := model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
					_, err = model.GetIDataScanTaskRW().UpdateDataScanTask(txnCtx,
						&task.DataScanTask{TaskName: mt.TaskName, SchemaNameS: mt.SchemaNameS, TableNameS: mt.TableNameS, ChunkDetailS: mt.ChunkDetailS},
						map[string]interface{}{
							"TaskStatus":  constant.TaskDatabaseStatusFailed,
							"Duration":    fmt.Sprintf("%f", time.Now().Sub(r.Time).Seconds()),
							"ErrorDetail": r.Err.Error(),
						})
					if err != nil {
						return err
					}
					_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
						TaskName:    mt.TaskName,
						SchemaNameS: mt.SchemaNameS,
						TableNameS:  mt.TableNameS,
						LogDetail: fmt.Sprintf("%v [%v] data scan task [%v] taskflow [%v] source table [%v.%v] failed, please see [data_migrate_task] detail",
							stringutil.CurrentTimeFormatString(),
							stringutil.StringLower(constant.TaskModeDataScan),
							mt.TaskName,
							dst.Task.TaskMode,
							mt.SchemaNameS,
							mt.TableNameS),
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
		err = model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
			tableStatusRecs, err := model.GetIDataScanTaskRW().FindDataScanTaskBySchemaTableChunkStatus(txnCtx, &task.DataScanTask{
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
					_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(txnCtx, &task.DataScanSummary{
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
					_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(txnCtx, &task.DataScanSummary{
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
					_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(txnCtx, &task.DataScanSummary{
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
					_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(txnCtx, &task.DataScanSummary{
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
					_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(txnCtx, &task.DataScanSummary{
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
					return fmt.Errorf("the task [%v] task_mode [%s] task_flow [%v] schema_name_s [%v] table_name_s [%v] task_status [%v] panic, please contact auhtor or reselect", s.TaskName, dst.Task.TaskMode, dst.Task.TaskFlow, rec.SchemaNameS, rec.TableNameS, rec.TaskStatus)
				}
			}

			_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(txnCtx, &task.DataScanSummary{
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

		logger.Info("data scan task process table",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS),
			zap.String("cost", endTableTime.Sub(startTableTime).String()))
	}
	logger.Info("data scan task",
		zap.String("task_name", dst.Task.TaskName),
		zap.String("task_mode", dst.Task.TaskMode),
		zap.String("task_flow", dst.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}

func (dst *DataScanTask) initDataScanTask(databaseS database.IDatabase, dbVersion string, dbCollationS bool, schemaNameS string) error {
	dbRole, err := databaseS.GetDatabaseRole()
	if err != nil {
		return err
	}
	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(dst.Ctx, &rule.MigrateTaskTable{
		TaskName:    dst.Task.TaskName,
		SchemaNameS: schemaNameS,
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

	databaseTables, err = databaseS.FilterDatabaseTable(schemaNameS, includeTables, excludeTables)
	if err != nil {
		return err
	}

	// clear the data scan task table
	// repeatInitTableMap used for store the data_scan_task table name has be finished, avoid repeated initialization
	migrateGroupTasks, err := model.GetIDataScanTaskRW().FindDataScanTaskGroupByTaskSchemaTable(dst.Ctx, dst.Task.TaskName)
	if err != nil {
		return err
	}
	repeatInitTableMap := make(map[string]struct{})

	if len(migrateGroupTasks) > 0 {
		taskTablesMap := make(map[string]struct{})
		for _, t := range databaseTables {
			taskTablesMap[t] = struct{}{}
		}
		for _, mt := range migrateGroupTasks {
			if mt.SchemaNameS == schemaNameS {
				if _, ok := taskTablesMap[mt.TableNameS]; !ok {
					err = model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
						err = model.GetIDataScanSummaryRW().DeleteDataScanSummary(txnCtx, &task.DataScanSummary{
							TaskName:    mt.TaskName,
							SchemaNameS: mt.SchemaNameS,
							TableNameS:  mt.TableNameS,
						})
						if err != nil {
							return err
						}
						err = model.GetIDataScanTaskRW().DeleteDataScanTask(txnCtx, &task.DataScanTask{
							TaskName:    mt.TaskName,
							SchemaNameS: mt.SchemaNameS,
							TableNameS:  mt.TableNameS,
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
				var summary *task.DataScanSummary

				summary, err = model.GetIDataScanSummaryRW().GetDataScanSummary(dst.Ctx, &task.DataScanSummary{
					TaskName:    mt.TaskName,
					SchemaNameS: mt.SchemaNameS,
					TableNameS:  mt.TableNameS,
				})
				if err != nil {
					return err
				}

				if int64(summary.ChunkTotals) != mt.ChunkTotals {
					err = model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
						err = model.GetIDataScanSummaryRW().DeleteDataScanSummary(txnCtx, &task.DataScanSummary{
							TaskName:    mt.TaskName,
							SchemaNameS: mt.SchemaNameS,
							TableNameS:  mt.TableNameS,
						})
						if err != nil {
							return err
						}
						err = model.GetIDataScanTaskRW().DeleteDataScanTask(txnCtx, &task.DataScanTask{
							TaskName:    mt.TaskName,
							SchemaNameS: mt.SchemaNameS,
							TableNameS:  mt.TableNameS,
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

				repeatInitTableMap[mt.TableNameS] = struct{}{}
			}
		}
	}

	databaseTableTypeMap, err = databaseS.GetDatabaseTableType(schemaNameS)
	if err != nil {
		return err
	}

	globalScn, err = databaseS.GetDatabaseConsistentPos()
	if err != nil {
		return err
	}

	// database tables
	// init database table
	logger.Info("data scan task start init",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))

	g, gCtx := errgroup.WithContext(dst.Ctx)
	g.SetLimit(int(dst.TaskParams.TableThread))

	for _, taskJob := range databaseTables {
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

				tableRows, err := databaseS.GetDatabaseTableRows(schemaNameS, sourceTable)
				if err != nil {
					return err
				}
				tableSize, err := databaseS.GetDatabaseTableSize(schemaNameS, sourceTable)
				if err != nil {
					return err
				}

				dataRule := &DataScanRule{
					Ctx:               gCtx,
					TaskName:          dst.Task.TaskName,
					TaskMode:          dst.Task.TaskMode,
					TaskFlow:          dst.Task.TaskFlow,
					SchemaNameS:       schemaNameS,
					TableNameS:        sourceTable,
					TableTypeS:        databaseTableTypeMap,
					DBCollationS:      dbCollationS,
					DatabaseS:         databaseS,
					DBCharsetS:        dst.DatasourceS.ConnectCharset,
					GlobalSqlHintS:    dst.TaskParams.SqlHintS,
					GlobalSamplerateS: strconv.FormatUint(dst.TaskParams.TableSamplerateS, 10),
				}

				attsRule, err := database.IDataScanAttributesRule(dataRule)
				if err != nil {
					return err
				}

				var whereRange string
				size, err := stringutil.StrconvFloatBitSize(attsRule.TableSamplerateS, 64)
				if err != nil {
					return err
				}

				if size > 0.000001 && size < 100 {
					logger.Warn("data scan task table",
						zap.String("task_name", dst.Task.TaskName),
						zap.String("task_mode", dst.Task.TaskMode),
						zap.String("task_flow", dst.Task.TaskFlow),
						zap.String("schema_name_s", attsRule.SchemaNameS),
						zap.String("table_name_s", attsRule.TableNameS),
						zap.String("database_version", dbVersion),
						zap.String("database_role", dbRole),
						zap.String("migrate_method", "scan"))

					whereRange = `sample_scan`
					encChunkS := snappy.Encode(nil, []byte(whereRange))
					encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}

					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						_, err = model.GetIDataScanTaskRW().CreateDataScanTask(txnCtx, &task.DataScanTask{
							TaskName:        dst.Task.TaskName,
							SchemaNameS:     attsRule.SchemaNameS,
							TableNameS:      attsRule.TableNameS,
							TableTypeS:      attsRule.TableTypeS,
							GlobalScnS:      globalScn,
							ColumnDetailS:   attsRule.ColumnDetailS,
							GroupColumnS:    attsRule.GroupColumnS,
							SqlHintS:        attsRule.SqlHintS,
							ChunkDetailS:    encryptChunkS,
							Samplerate:      attsRule.TableSamplerateS,
							ConsistentReadS: strconv.FormatBool(dst.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
						})
						if err != nil {
							return err
						}
						_, err = model.GetIDataScanSummaryRW().CreateDataScanSummary(txnCtx, &task.DataScanSummary{
							TaskName:    dst.Task.TaskName,
							SchemaNameS: attsRule.SchemaNameS,
							TableNameS:  attsRule.TableNameS,
							GlobalScnS:  globalScn,
							TableRowsS:  tableRows,
							TableSizeS:  tableSize,
							ChunkTotals: 1,
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

				// statistic
				if !strings.EqualFold(dbRole, constant.OracleDatabasePrimaryRole) || (strings.EqualFold(dbRole, constant.OracleDatabasePrimaryRole) && stringutil.VersionOrdinal(dbVersion) < stringutil.VersionOrdinal(constant.OracleDatabaseTableMigrateRowidRequireVersion)) {
					columnNameSlis, err := databaseS.FindDatabaseTableBestColumn(attsRule.SchemaNameS, attsRule.TableNameS, "")
					if err != nil {
						return err
					}
					if len(columnNameSlis) == 0 {
						logger.Warn("data scan task table",
							zap.String("task_name", dst.Task.TaskName),
							zap.String("task_mode", dst.Task.TaskMode),
							zap.String("task_flow", dst.Task.TaskFlow),
							zap.String("schema_name_s", attsRule.SchemaNameS),
							zap.String("table_name_s", attsRule.TableNameS),
							zap.String("database_version", dbVersion),
							zap.String("database_role", dbRole),
							zap.String("migrate_method", "scan"))

						whereRange = `1 = 1`

						encChunkS := snappy.Encode(nil, []byte(whereRange))

						encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
						if err != nil {
							return err
						}

						err = model.Transaction(gCtx, func(txnCtx context.Context) error {
							_, err = model.GetIDataScanTaskRW().CreateDataScanTask(txnCtx, &task.DataScanTask{
								TaskName:        dst.Task.TaskName,
								SchemaNameS:     attsRule.SchemaNameS,
								TableNameS:      attsRule.TableNameS,
								TableTypeS:      attsRule.TableTypeS,
								GlobalScnS:      globalScn,
								ColumnDetailS:   attsRule.ColumnDetailS,
								GroupColumnS:    attsRule.GroupColumnS,
								SqlHintS:        attsRule.SqlHintS,
								ChunkDetailS:    encryptChunkS,
								Samplerate:      attsRule.TableSamplerateS,
								ConsistentReadS: strconv.FormatBool(dst.TaskParams.EnableConsistentRead),
								TaskStatus:      constant.TaskDatabaseStatusWaiting,
							})
							if err != nil {
								return err
							}
							_, err = model.GetIDataScanSummaryRW().CreateDataScanSummary(txnCtx, &task.DataScanSummary{
								TaskName:    dst.Task.TaskName,
								SchemaNameS: attsRule.SchemaNameS,
								TableNameS:  attsRule.TableNameS,
								GlobalScnS:  globalScn,
								TableRowsS:  tableRows,
								TableSizeS:  tableSize,
								ChunkTotals: 1,
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

					bucketRanges, err := getDatabaseTableColumnBucket(dst.Ctx, databaseS, nil, dst.Task.TaskName, dst.Task.TaskFlow, attsRule.SchemaNameS, attsRule.SchemaNameS, attsRule.TableNameS, attsRule.TableNameS, dbCollationS, columnNameSlis, dst.DatasourceS.ConnectCharset, dst.DatasourceS.ConnectCharset)
					if err != nil {
						return err
					}
					logger.Warn("data scan task table",
						zap.String("task_name", dst.Task.TaskName),
						zap.String("task_mode", dst.Task.TaskMode),
						zap.String("task_flow", dst.Task.TaskFlow),
						zap.String("schema_name_s", attsRule.SchemaNameS),
						zap.String("table_name_s", attsRule.TableNameS),
						zap.String("database_version", dbVersion),
						zap.String("database_role", dbRole),
						zap.String("migrate_method", "statistic"))
					if len(bucketRanges) == 0 {
						whereRange = `1 = 1`

						encChunkS := snappy.Encode(nil, []byte(whereRange))

						encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
						if err != nil {
							return err
						}

						err = model.Transaction(gCtx, func(txnCtx context.Context) error {
							_, err = model.GetIDataScanTaskRW().CreateDataScanTask(txnCtx, &task.DataScanTask{
								TaskName:        dst.Task.TaskName,
								SchemaNameS:     attsRule.SchemaNameS,
								TableNameS:      attsRule.TableNameS,
								TableTypeS:      attsRule.TableTypeS,
								GlobalScnS:      globalScn,
								ColumnDetailS:   attsRule.ColumnDetailS,
								GroupColumnS:    attsRule.GroupColumnS,
								SqlHintS:        attsRule.SqlHintS,
								ChunkDetailS:    encryptChunkS,
								Samplerate:      attsRule.TableSamplerateS,
								ConsistentReadS: strconv.FormatBool(dst.TaskParams.EnableConsistentRead),
								TaskStatus:      constant.TaskDatabaseStatusWaiting,
							})
							if err != nil {
								return err
							}
							_, err = model.GetIDataScanSummaryRW().CreateDataScanSummary(txnCtx, &task.DataScanSummary{
								TaskName:    dst.Task.TaskName,
								SchemaNameS: attsRule.SchemaNameS,
								TableNameS:  attsRule.TableNameS,
								GlobalScnS:  globalScn,
								TableRowsS:  tableRows,
								TableSizeS:  tableSize,
								ChunkTotals: 1,
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

					var metas []*task.DataScanTask
					for _, r := range bucketRanges {

						whereRange = r.ToStringS()

						encChunkS := snappy.Encode(nil, []byte(whereRange))

						encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
						if err != nil {
							return err
						}
						metas = append(metas, &task.DataScanTask{
							TaskName:        dst.Task.TaskName,
							SchemaNameS:     attsRule.SchemaNameS,
							TableNameS:      attsRule.TableNameS,
							TableTypeS:      attsRule.TableTypeS,
							GlobalScnS:      globalScn,
							ColumnDetailS:   attsRule.ColumnDetailS,
							GroupColumnS:    attsRule.GroupColumnS,
							SqlHintS:        attsRule.SqlHintS,
							ChunkDetailS:    encryptChunkS,
							Samplerate:      attsRule.TableSamplerateS,
							ConsistentReadS: strconv.FormatBool(dst.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
						})
					}

					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						err = model.GetIDataScanTaskRW().CreateInBatchDataScanTask(txnCtx, metas, int(dst.TaskParams.BatchSize))
						if err != nil {
							return err
						}
						_, err = model.GetIDataScanSummaryRW().CreateDataScanSummary(txnCtx, &task.DataScanSummary{
							TaskName:    dst.Task.TaskName,
							SchemaNameS: attsRule.SchemaNameS,
							TableNameS:  attsRule.TableNameS,
							GlobalScnS:  globalScn,
							TableRowsS:  tableRows,
							TableSizeS:  tableSize,
							ChunkTotals: uint64(len(bucketRanges)),
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

				chunks, err := databaseS.GetDatabaseTableChunkTask(chunkTask, schemaNameS, sourceTable, dst.TaskParams.ChunkSize, dst.TaskParams.CallTimeout)
				if err != nil {
					return err
				}

				if len(chunks) == 0 {

					whereRange = `1 = 1`

					encChunkS := snappy.Encode(nil, []byte(whereRange))

					encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}

					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						_, err = model.GetIDataScanTaskRW().CreateDataScanTask(txnCtx, &task.DataScanTask{
							TaskName:        dst.Task.TaskName,
							SchemaNameS:     attsRule.SchemaNameS,
							TableNameS:      attsRule.TableNameS,
							TableTypeS:      attsRule.TableTypeS,
							GlobalScnS:      globalScn,
							ColumnDetailS:   attsRule.ColumnDetailS,
							GroupColumnS:    attsRule.GroupColumnS,
							SqlHintS:        attsRule.SqlHintS,
							ChunkDetailS:    encryptChunkS,
							Samplerate:      attsRule.TableSamplerateS,
							ConsistentReadS: strconv.FormatBool(dst.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
						})
						if err != nil {
							return err
						}
						_, err = model.GetIDataScanSummaryRW().CreateDataScanSummary(txnCtx, &task.DataScanSummary{
							TaskName:    dst.Task.TaskName,
							SchemaNameS: attsRule.SchemaNameS,
							TableNameS:  attsRule.TableNameS,
							GlobalScnS:  globalScn,
							TableRowsS:  tableRows,
							TableSizeS:  tableSize,
							ChunkTotals: 1,
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

				var metas []*task.DataScanTask
				for _, r := range chunks {
					whereRange = r["CMD"]

					encChunkS := snappy.Encode(nil, []byte(whereRange))

					encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
					if err != nil {
						return err
					}

					metas = append(metas, &task.DataScanTask{
						TaskName:        dst.Task.TaskName,
						SchemaNameS:     attsRule.SchemaNameS,
						TableNameS:      attsRule.TableNameS,
						TableTypeS:      attsRule.TableTypeS,
						GlobalScnS:      globalScn,
						ColumnDetailS:   attsRule.ColumnDetailS,
						GroupColumnS:    attsRule.GroupColumnS,
						SqlHintS:        attsRule.SqlHintS,
						ChunkDetailS:    encryptChunkS,
						Samplerate:      attsRule.TableSamplerateS,
						ConsistentReadS: strconv.FormatBool(dst.TaskParams.EnableConsistentRead),
						TaskStatus:      constant.TaskDatabaseStatusWaiting,
					})
				}

				err = model.Transaction(gCtx, func(txnCtx context.Context) error {
					err = model.GetIDataScanTaskRW().CreateInBatchDataScanTask(txnCtx, metas, int(dst.TaskParams.BatchSize))
					if err != nil {
						return err
					}
					_, err = model.GetIDataScanSummaryRW().CreateDataScanSummary(txnCtx, &task.DataScanSummary{
						TaskName:    dst.Task.TaskName,
						SchemaNameS: attsRule.SchemaNameS,
						TableNameS:  attsRule.TableNameS,
						GlobalScnS:  globalScn,
						TableRowsS:  tableRows,
						TableSizeS:  tableSize,
						ChunkTotals: uint64(len(chunks)),
					})
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					return err
				}

				logger.Info("data scan task init success",
					zap.String("task_name", dst.Task.TaskName),
					zap.String("task_mode", dst.Task.TaskMode),
					zap.String("task_flow", dst.Task.TaskFlow),
					zap.String("schema_name_s", attsRule.SchemaNameS),
					zap.String("table_name_s", attsRule.TableNameS),
					zap.String("cost", time.Now().Sub(startTime).String()))
				return nil
			}
		})
	}

	// ignore context canceled error
	if err = g.Wait(); !errors.Is(err, context.Canceled) {
		logger.Warn("data scan task init failed",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", schemaNameS),
			zap.Error(err))
		return err
	}
	return nil
}