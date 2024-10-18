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
package processor

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wentaojin/dbms/errconcurrent"
	"github.com/wentaojin/dbms/utils/structure"

	"github.com/golang/snappy"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
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
	DatabaseS   database.IDatabase
	DatabaseT   database.IDatabase
	SchemaNameS string

	DBCharsetS string
	DBCharsetT string

	GlobalSnapshotS string
	GlobalSnapshotT string
	TaskParams      *pb.DataCompareParam

	WaiterC chan *WaitingRecs
	ResumeC chan *WaitingRecs
}

func (dmt *DataCompareTask) Init() error {
	defer func() {
		close(dmt.WaiterC)
		close(dmt.ResumeC)
	}()
	logger.Info("data compare task init table",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))

	logger.Warn("data compare task checkpoint action",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.Bool("enable_checkpoint", dmt.TaskParams.EnableCheckpoint))
	if !dmt.TaskParams.EnableCheckpoint {
		logger.Warn("data compare task clear task records",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow))

		err := model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
			err := model.GetIDataCompareSummaryRW().DeleteDataCompareSummaryName(txnCtx, []string{dmt.Task.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataCompareTaskRW().DeleteDataCompareTaskName(txnCtx, []string{dmt.Task.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataCompareResultRW().DeleteDataCompareResultName(txnCtx, []string{dmt.Task.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(dmt.Ctx, &rule.MigrateTaskTable{
		TaskName:    dmt.Task.TaskName,
		SchemaNameS: dmt.SchemaNameS,
	})
	if err != nil {
		return err
	}
	var (
		includeTables      []string
		excludeTables      []string
		databaseTaskTables []string // task tables
	)
	databaseTableTypeMap := make(map[string]string)
	databaseTaskTablesMap := make(map[string]struct{})

	for _, t := range schemaTaskTables {
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsExclude) {
			excludeTables = append(excludeTables, t.TableNameS)
		}
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsNotExclude) {
			includeTables = append(includeTables, t.TableNameS)
		}
	}

	tableObjs, err := dmt.DatabaseS.FilterDatabaseTable(dmt.SchemaNameS, includeTables, excludeTables)
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
		databaseTaskTablesMap[tabName] = struct{}{}
	}

	// compare the task table
	// the database task table is exist, and the config task table isn't exist, the clear the database task table
	summaries, err := model.GetIDataCompareSummaryRW().FindDataCompareSummary(dmt.Ctx, &task.DataCompareSummary{TaskName: dmt.Task.TaskName, SchemaNameS: dmt.SchemaNameS})
	if err != nil {
		return err
	}
	for _, s := range summaries {
		_, ok := databaseTaskTablesMap[s.TableNameS]

		if !ok || strings.EqualFold(s.InitFlag, constant.TaskInitStatusNotFinished) {
			err = model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
				err = model.GetIDataCompareSummaryRW().DeleteDataCompareSummary(txnCtx, &task.DataCompareSummary{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
				})
				if err != nil {
					return err
				}
				err = model.GetIDataCompareTaskRW().DeleteDataCompareTask(txnCtx, &task.DataCompareTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
				})
				if err != nil {
					return err
				}
				err = model.GetIDataCompareResultRW().DeleteDataCompareResultTable(txnCtx, &task.DataCompareResult{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
				})
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

	databaseTableTypeMap, err = dmt.DatabaseS.GetDatabaseTableType(dmt.SchemaNameS)
	if err != nil {
		return err
	}

	// database tables
	// init database table
	dbTypeSli := stringutil.StringSplit(dmt.Task.TaskFlow, constant.StringSeparatorAite)
	dbTypeS := dbTypeSli[0]
	dbTypeT := dbTypeSli[1]

	logger.Info("data compare task init",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow), zap.Any("tables", databaseTaskTables))

	g, gCtx := errgroup.WithContext(dmt.Ctx)
	g.SetLimit(int(dmt.TaskParams.TableThread))

	for _, taskJob := range databaseTaskTables {
		sourceTable := taskJob
		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
				startTime := time.Now()
				logger.Info("data compare task init table start",
					zap.String("task_name", dmt.Task.TaskName),
					zap.String("task_mode", dmt.Task.TaskMode),
					zap.String("task_flow", dmt.Task.TaskFlow),
					zap.String("schema_name_s", dmt.SchemaNameS),
					zap.String("table_name_s", sourceTable))
				s, err := model.GetIDataCompareSummaryRW().GetDataCompareSummary(gCtx, &task.DataCompareSummary{
					TaskName:    dmt.Task.TaskName,
					SchemaNameS: dmt.SchemaNameS,
					TableNameS:  sourceTable,
				})
				if err != nil {
					return err
				}
				if strings.EqualFold(s.InitFlag, constant.TaskInitStatusFinished) {
					// the database task has init flag,skip
					select {
					case dmt.ResumeC <- &WaitingRecs{
						TaskName:    s.TaskName,
						SchemaNameS: s.SchemaNameS,
						TableNameS:  s.TableNameS,
					}:
						logger.Info("data compare task resume channel send",
							zap.String("task_name", dmt.Task.TaskName),
							zap.String("task_mode", dmt.Task.TaskMode),
							zap.String("task_flow", dmt.Task.TaskFlow),
							zap.String("schema_name_s", dmt.SchemaNameS),
							zap.String("table_name_s", sourceTable))
					default:
						logger.Warn("data compare task resume channel full",
							zap.String("task_name", dmt.Task.TaskName),
							zap.String("task_mode", dmt.Task.TaskMode),
							zap.String("task_flow", dmt.Task.TaskFlow),
							zap.String("schema_name_s", dmt.SchemaNameS),
							zap.String("table_name_s", sourceTable),
							zap.String("action", "skip send"))
						_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(gCtx, &task.DataCompareSummary{
							TaskName:    dmt.Task.TaskName,
							SchemaNameS: dmt.SchemaNameS,
							TableNameS:  sourceTable}, map[string]interface{}{
							"CompareFlag": constant.TaskCompareStatusSkipped,
						})
						if err != nil {
							return err
						}
					}
					return nil
				}

				tableRows, err := dmt.DatabaseS.GetDatabaseTableRows(dmt.SchemaNameS, sourceTable)
				if err != nil {
					return err
				}
				tableSize, err := dmt.DatabaseS.GetDatabaseTableSize(dmt.SchemaNameS, sourceTable)
				if err != nil {
					return err
				}

				dataRule := &DataCompareRule{
					Ctx:                         gCtx,
					TaskMode:                    dmt.Task.TaskMode,
					TaskName:                    dmt.Task.TaskName,
					TaskFlow:                    dmt.Task.TaskFlow,
					DatabaseS:                   dmt.DatabaseS,
					DatabaseT:                   dmt.DatabaseT,
					SchemaNameS:                 dmt.SchemaNameS,
					TableNameS:                  sourceTable,
					TableTypeS:                  databaseTableTypeMap,
					OnlyDatabaseCompareRow:      dmt.TaskParams.OnlyCompareRow,
					DisableDatabaseCompareMd5:   dmt.TaskParams.DisableMd5Checksum,
					DBCharsetS:                  dmt.DBCharsetS,
					DBCharsetT:                  dmt.DBCharsetT,
					CaseFieldRuleS:              dmt.Task.CaseFieldRuleS,
					CaseFieldRuleT:              dmt.Task.CaseFieldRuleT,
					GlobalSqlHintS:              dmt.TaskParams.SqlHintS,
					GlobalSqlHintT:              dmt.TaskParams.SqlHintT,
					GlobalIgnoreConditionFields: dmt.TaskParams.IgnoreConditionFields,
				}

				attsRule, err := database.IDataCompareAttributesRule(dataRule)
				if err != nil {
					return err
				}

				err = dmt.ProcessStatisticsScan(
					gCtx,
					dbTypeS,
					dbTypeT,
					dmt.GlobalSnapshotS,
					dmt.GlobalSnapshotT,
					tableRows,
					tableSize,
					attsRule)
				if err != nil {
					return err
				}

				logger.Info("data compare task init table finished",
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

	if err = g.Wait(); err != nil {
		logger.Error("data compare task init failed",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("schema_name_s", dmt.SchemaNameS),
			zap.Error(err))
		return err
	}
	return nil
}

func (dmt *DataCompareTask) Run() error {
	logger.Info("data compare task run table",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))

	for s := range dmt.WaiterC {
		err := dmt.Process(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dmt *DataCompareTask) Resume() error {
	logger.Info("data compare task resume table",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))

	for s := range dmt.ResumeC {
		err := dmt.Process(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dmt *DataCompareTask) Last() error {
	logger.Info("data compare task last table",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	flags, err := model.GetIDataCompareSummaryRW().QueryDataCompareSummaryFlag(dmt.Ctx, &task.DataCompareSummary{
		TaskName:    dmt.Task.TaskName,
		SchemaNameS: dmt.SchemaNameS,
		InitFlag:    constant.TaskInitStatusFinished,
		CompareFlag: constant.TaskCompareStatusSkipped,
	})
	if err != nil {
		return err
	}

	for _, f := range flags {
		err = dmt.Process(&WaitingRecs{
			TaskName:    f.TaskName,
			SchemaNameS: f.SchemaNameS,
			TableNameS:  f.TableNameS,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (dmt *DataCompareTask) Process(s *WaitingRecs) error {
	startTime := time.Now()
	logger.Info("data compare task process table",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("schema_name_s", s.SchemaNameS),
		zap.String("table_name_s", s.TableNameS))

	var (
		migrateTasks []*task.DataCompareTask
		err          error
	)
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
			dt := j.(*task.DataCompareTask)
			errW := model.Transaction(dmt.Ctx, func(txnCtx context.Context) error {
				_, err = model.GetIDataCompareTaskRW().UpdateDataCompareTask(txnCtx,
					&task.DataCompareTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS, ChunkID: dt.ChunkID},
					map[string]interface{}{
						"TaskStatus": constant.TaskDatabaseStatusRunning,
					})
				if err != nil {
					return err
				}
				// clear data compare chunk result
				err = model.GetIDataCompareResultRW().DeleteDataCompareResult(txnCtx, &task.DataCompareResult{
					TaskName:    dt.TaskName,
					SchemaNameS: dt.SchemaNameS,
					TableNameS:  dt.TableNameS,
					ChunkID:     dt.ChunkID,
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
						stringutil.StringLower(dmt.Task.TaskMode),
						dt.TaskName,
						dmt.Task.TaskFlow,
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

			var dbCharsetS, dbCharsetT string
			switch dmt.Task.TaskFlow {
			case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
				dbCharsetS = constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DBCharsetS)]
				dbCharsetT = constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DBCharsetT)]
			case constant.TaskFlowTiDBToOracle, constant.TaskFlowMySQLToOracle:
				dbCharsetS = constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DBCharsetS)]
				dbCharsetT = constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DBCharsetT)]
			case constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
				dbCharsetS = constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DBCharsetS)]
				dbCharsetT = constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DBCharsetT)]
			case constant.TaskFlowTiDBToPostgres, constant.TaskFlowMySQLToPostgres:
				dbCharsetS = constant.MigrateMySQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DBCharsetS)]
				dbCharsetT = constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[stringutil.StringUpper(dmt.DBCharsetT)]
			default:
				return fmt.Errorf("the task [%s] schema [%s] taskflow [%s] column rule isn't support, please contact author", dmt.Task.TaskName, dt.SchemaNameS, dmt.Task.TaskFlow)
			}

			err = database.IDataCompareProcess(&DataCompareRow{
				Ctx:            dmt.Ctx,
				TaskMode:       dmt.Task.TaskMode,
				TaskFlow:       dmt.Task.TaskFlow,
				StartTime:      gTime,
				Dmt:            dt,
				DatabaseS:      dmt.DatabaseS,
				DatabaseT:      dmt.DatabaseT,
				BatchSize:      int(dmt.TaskParams.BatchSize),
				WriteThread:    int(dmt.TaskParams.WriteThread),
				CallTimeout:    int(dmt.TaskParams.CallTimeout),
				DBCharsetS:     dbCharsetS,
				DBCharsetT:     dbCharsetT,
				RepairStmtFlow: dmt.TaskParams.RepairStmtFlow,
				Separator:      dmt.TaskParams.Separator,
			})
			if err != nil {
				return err
			}
			return nil
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
					&task.DataCompareTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS, ChunkID: smt.ChunkID},
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
						stringutil.StringLower(dmt.Task.TaskMode),
						smt.TaskName,
						dmt.Task.TaskFlow,
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
		var equalChunks uint64
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
				equalChunks = equalChunks + uint64(rec.StatusTotals)
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

		summary, err := model.GetIDataCompareSummaryRW().GetDataCompareSummary(txnCtx, &task.DataCompareSummary{
			TaskName:    s.TaskName,
			SchemaNameS: s.SchemaNameS,
			TableNameS:  s.TableNameS,
		})
		if err != nil {
			return err
		}
		if summary.ChunkTotals == equalChunks {
			_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(txnCtx, &task.DataCompareSummary{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			}, map[string]interface{}{
				"CompareFlag": constant.TaskCompareStatusFinished,
				"Duration":    fmt.Sprintf("%f", time.Now().Sub(startTime).Seconds()),
			})
			if err != nil {
				return err
			}
		} else {
			_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(txnCtx, &task.DataCompareSummary{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			}, map[string]interface{}{
				"CompareFlag": constant.TaskCompareStatusNotFinished,
				"Duration":    fmt.Sprintf("%f", time.Now().Sub(startTime).Seconds()),
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("data compare task process table finished",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("schema_name_s", s.SchemaNameS),
		zap.String("table_name_s", s.TableNameS),
		zap.String("cost", endTableTime.Sub(startTime).String()))

	return nil
}

func (dmt *DataCompareTask) ProcessStatisticsScan(ctx context.Context, dbTypeS, dbTypeT, globalScnS, globalScnT string, tableRows uint64, tableSize float64, attsRule *database.DataCompareAttributesRule) error {
	h, err := dmt.DatabaseS.GetDatabaseTableHighestSelectivityIndex(
		attsRule.SchemaNameS,
		attsRule.TableNameS,
		attsRule.CompareConditionFieldS,
		attsRule.IgnoreConditionFields)
	if err != nil {
		return err
	}

	logger.Debug("data compare task init table chunk",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("schema_name_s", attsRule.SchemaNameS),
		zap.String("table_name_s", attsRule.TableNameS),
		zap.Any("origin upstream bucket", h))

	if h == nil {
		logger.Warn("data migrate task table",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("schema_name_s", attsRule.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("seletivity", "selectivity is null, skip statistics"),
			zap.String("migrate_method", "scan"))
		err = dmt.ProcessTableScan(ctx, globalScnS, globalScnT, tableRows, tableSize, attsRule)
		if err != nil {
			return err
		}
		return nil
	}

	// upstream bucket ranges
	err = h.TransSelectivity(
		dbTypeS,
		dmt.DBCharsetS,
		dmt.Task.CaseFieldRuleS,
		dmt.TaskParams.EnableCollationSetting)
	if err != nil {
		return err
	}

	columnDatatypeSliT, err := GetDownstreamTableColumnDatatype(attsRule.SchemaNameT, attsRule.TableNameT, dmt.DatabaseT, h.IndexColumn, attsRule.ColumnNameRouteRule)
	if err != nil {
		return err
	}

	// downstream bucket rule
	selecRules, err := h.TransSelectivityRule(dmt.Task.TaskFlow, dbTypeT, dmt.DBCharsetS, columnDatatypeSliT, attsRule.ColumnNameRouteRule)
	if err != nil {
		return err
	}

	logger.Debug("data compare task init table chunk",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("schema_name_s", attsRule.SchemaNameS),
		zap.String("table_name_s", attsRule.TableNameS),
		zap.String("process_method", "statistic"),
		zap.Any("downstream selectivity rule", selecRules),
		zap.Any("new upstream bucket", h))

	rangeC := make(chan []*structure.Range, constant.DefaultMigrateTaskQueueSize)
	chunksC := make(chan int, 1)

	d := &Divide{
		DBTypeS:     dbTypeS,
		DBCharsetS:  dmt.DBCharsetS,
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
		ChunkSize:   int64(dmt.TaskParams.ChunkSize),
		DatabaseS:   dmt.DatabaseS,
		Cons:        h,
		RangeC:      rangeC,
	}

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(rangeC)
		err = d.ProcessUpstreamStatisticsBucket()
		if err != nil {
			return err
		}
		return nil
	})

	g.Go(func() error {
		totalChunks := 0
		for r := range rangeC {
			downRanges, err := ProcessDownstreamStatisticsBucket(dbTypeT, dmt.DBCharsetT, r, selecRules)
			if err != nil {
				return err
			}
			logger.Debug("data compare task init table chunk",
				zap.String("task_name", dmt.Task.TaskName),
				zap.String("task_mode", dmt.Task.TaskMode),
				zap.String("task_flow", dmt.Task.TaskFlow),
				zap.String("schema_name_s", attsRule.SchemaNameS),
				zap.String("table_name_s", attsRule.TableNameS),
				zap.Any("downstream selectivity rule", selecRules),
				zap.Any("new upstream bucket", h),
				zap.Any("current upstream range", r),
				zap.Any("current downstream range", downRanges))
			statsRanges, err := dmt.PrepareStatisticsRange(globalScnS, globalScnT, attsRule, r, downRanges)
			if err != nil {
				return err
			}
			if len(statsRanges) > 0 {
				err = model.GetIDataCompareTaskRW().CreateInBatchDataCompareTask(gCtx, statsRanges, int(dmt.TaskParams.WriteThread), int(dmt.TaskParams.BatchSize))
				if err != nil {
					return err
				}
				totalChunks = totalChunks + len(statsRanges)
			}
		}
		chunksC <- totalChunks
		return nil
	})

	if err = g.Wait(); err != nil {
		return err
	}

	totalChunks := <-chunksC
	if totalChunks == 0 {
		err := dmt.ProcessTableScan(ctx, globalScnS, globalScnT, tableRows, tableSize, attsRule)
		if err != nil {
			return err
		}
		return nil
	}
	_, err = model.GetIDataCompareSummaryRW().CreateDataCompareSummary(ctx, &task.DataCompareSummary{
		TaskName:       dmt.Task.TaskName,
		SchemaNameS:    attsRule.SchemaNameS,
		TableNameS:     attsRule.TableNameS,
		SchemaNameT:    attsRule.SchemaNameT,
		TableNameT:     attsRule.TableNameT,
		SnapshotPointS: globalScnS,
		SnapshotPointT: globalScnT,
		TableRowsS:     tableRows,
		TableSizeS:     tableSize,
		ChunkTotals:    uint64(totalChunks),
		InitFlag:       constant.TaskInitStatusFinished,
		CompareFlag:    constant.TaskCompareStatusNotFinished,
	})
	if err != nil {
		return err
	}

	select {
	case dmt.WaiterC <- &WaitingRecs{
		TaskName:    dmt.Task.TaskName,
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
	}:
		logger.Info("data compare task wait channel send",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("schema_name_s", dmt.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS))
	default:
		logger.Warn("data compare task wait channel full",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("schema_name_s", dmt.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("action", "skip send"))
		_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(ctx, &task.DataCompareSummary{
			TaskName:    dmt.Task.TaskName,
			SchemaNameS: attsRule.SchemaNameS,
			TableNameS:  attsRule.TableNameS}, map[string]interface{}{
			"CompareFlag": constant.TaskCompareStatusSkipped,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (dmt *DataCompareTask) ProcessTableScan(ctx context.Context, globalScnS, globalScnT string, tableRows uint64, tableSize float64, attsRule *database.DataCompareAttributesRule) error {
	var encChunkS, encChunkT []byte
	if !strings.EqualFold(attsRule.CompareConditionRangeS, "") {
		encChunkS = snappy.Encode(nil, []byte(attsRule.CompareConditionRangeS))
	} else {
		encChunkS = snappy.Encode(nil, []byte("1 = 1"))
	}
	if !strings.EqualFold(attsRule.CompareConditionRangeT, "") {
		encChunkT = snappy.Encode(nil, []byte(attsRule.CompareConditionRangeT))
	} else {
		encChunkT = snappy.Encode(nil, []byte("1 = 1"))
	}

	logger.Warn("data compare task init table chunk",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("schema_name_s", attsRule.SchemaNameS),
		zap.String("table_name_s", attsRule.TableNameS),
		zap.Any("upstream bucket range", string(encChunkS)))

	encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	encryptChunkT, err := stringutil.Encrypt(stringutil.BytesToString(encChunkT), []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
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
			SqlHintS:        attsRule.SqlHintS,
			SqlHintT:        attsRule.SqlHintT,
			ChunkID:         uuid.New().String(),
			ChunkDetailS:    encryptChunkS,
			ChunkDetailArgS: "",
			ChunkDetailT:    encryptChunkT,
			ChunkDetailArgT: "",
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
			InitFlag:       constant.TaskInitStatusFinished,
			CompareFlag:    constant.TaskCompareStatusNotFinished,
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	select {
	case dmt.WaiterC <- &WaitingRecs{
		TaskName:    dmt.Task.TaskName,
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
	}:
		logger.Info("data compare task wait send",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("schema_name_s", dmt.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS))
	default:
		logger.Warn("data compare task wait channel full",
			zap.String("task_name", dmt.Task.TaskName),
			zap.String("task_mode", dmt.Task.TaskMode),
			zap.String("task_flow", dmt.Task.TaskFlow),
			zap.String("schema_name_s", dmt.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("action", "skip send"))
		_, err = model.GetIDataCompareSummaryRW().UpdateDataCompareSummary(ctx, &task.DataCompareSummary{
			TaskName:    dmt.Task.TaskName,
			SchemaNameS: attsRule.SchemaNameS,
			TableNameS:  attsRule.TableNameS}, map[string]interface{}{
			"CompareFlag": constant.TaskCompareStatusSkipped,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (dmt *DataCompareTask) PrepareStatisticsRange(globalScnS, globalScnT string, attsRule *database.DataCompareAttributesRule, upRanges, downRanges []*structure.Range) ([]*task.DataCompareTask, error) {
	var metas []*task.DataCompareTask
	for i, r := range upRanges {
		toStringS, toStringArgsS := r.ToString()
		toStringT, toStringArgsT := downRanges[i].ToString()

		if !strings.EqualFold(attsRule.CompareConditionRangeS, "") {
			toStringS = fmt.Sprintf("%s AND (%s)", toStringS, attsRule.CompareConditionRangeS)
		}
		if !strings.EqualFold(attsRule.CompareConditionRangeT, "") {
			toStringT = fmt.Sprintf("%s AND (%s)", toStringT, attsRule.CompareConditionRangeT)
		}

		encChunkS := snappy.Encode(nil, []byte(toStringS))
		encChunkT := snappy.Encode(nil, []byte(toStringT))

		encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return metas, err
		}
		encryptChunkT, err := stringutil.Encrypt(stringutil.BytesToString(encChunkT), []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return metas, err
		}

		var argsS, argsT string
		if toStringArgsS != nil {
			argsS, err = stringutil.MarshalJSON(toStringArgsS)
			if err != nil {
				return metas, err
			}
		}
		if toStringArgsT != nil {
			argsT, err = stringutil.MarshalJSON(toStringArgsT)
			if err != nil {
				return metas, err
			}
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
			SqlHintS:        attsRule.SqlHintS,
			SqlHintT:        attsRule.SqlHintT,
			ChunkID:         uuid.New().String(),
			ChunkDetailS:    encryptChunkS,
			ChunkDetailArgS: argsS,
			ChunkDetailT:    encryptChunkT,
			ChunkDetailArgT: argsT,
			ConsistentReadS: strconv.FormatBool(dmt.TaskParams.EnableConsistentRead),
			TaskStatus:      constant.TaskDatabaseStatusWaiting,
		})
	}
	return metas, nil
}
