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
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/thread"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"github.com/wentaojin/dbms/utils/structure"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type DataScanTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatabaseS   database.IDatabase
	SchemaNameS string
	DBRoleS     string
	DBCharsetS  string
	DBVersionS  string

	TaskParams *pb.DataScanParam
	WaiterC    chan *WaitingRecs
	ResumeC    chan *WaitingRecs

	Progress *Progress
}

func (dst *DataScanTask) Init() error {
	defer func() {
		close(dst.WaiterC)
		close(dst.ResumeC)
	}()
	logger.Info("data scan task init table",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))

	if !dst.TaskParams.EnableCheckpoint {
		err := model.GetIDataScanTaskRW().DeleteDataScanTaskName(dst.Ctx, []string{dst.Task.TaskName})
		if err != nil {
			return err
		}
		err = model.GetIDataScanSummaryRW().DeleteDataScanSummaryName(dst.Ctx, []string{dst.Task.TaskName})
		if err != nil {
			return err
		}
	}

	logger.Warn("data scan task checkpoint skip",
		zap.String("task_name", dst.Task.TaskName),
		zap.String("task_mode", dst.Task.TaskMode),
		zap.String("task_flow", dst.Task.TaskFlow),
		zap.Bool("enable_checkpoint", dst.TaskParams.EnableCheckpoint))

	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(dst.Ctx, &rule.MigrateTaskTable{
		TaskName:    dst.Task.TaskName,
		SchemaNameS: dst.SchemaNameS,
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
	databaseTaskTablesMap := make(map[string]struct{})

	for _, t := range schemaTaskTables {
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsExclude) {
			excludeTables = append(excludeTables, t.TableNameS)
		}
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsNotExclude) {
			includeTables = append(includeTables, t.TableNameS)
		}
	}

	tableObjs, err := dst.DatabaseS.FilterDatabaseTable(dst.SchemaNameS, includeTables, excludeTables)
	if err != nil {
		return err
	}

	// rule case field
	for _, t := range tableObjs.TaskTables {
		var tabName string
		// the according target case field rule convert
		if strings.EqualFold(dst.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			tabName = stringutil.StringLower(t)
		}
		if strings.EqualFold(dst.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			tabName = stringutil.StringUpper(t)
		}
		if strings.EqualFold(dst.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
			tabName = t
		}
		databaseTaskTables = append(databaseTaskTables, tabName)
		databaseTaskTablesMap[tabName] = struct{}{}
	}

	// compare the task table
	// the database task table is exist, and the config task table isn't exist, the clear the database task table
	summaries, err := model.GetIDataScanSummaryRW().FindDataScanSummary(dst.Ctx, &task.DataScanSummary{TaskName: dst.Task.TaskName, SchemaNameS: dst.SchemaNameS})
	if err != nil {
		return err
	}
	for _, s := range summaries {
		_, ok := databaseTaskTablesMap[s.TableNameS]

		if !ok || strings.EqualFold(s.InitFlag, constant.TaskInitStatusNotFinished) {
			err = model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
				err := model.GetIDataScanTaskRW().DeleteDataScanTaskName(txnCtx, []string{dst.Task.TaskName})
				if err != nil {
					return err
				}
				err = model.GetIDataScanSummaryRW().DeleteDataScanSummaryName(txnCtx, []string{dst.Task.TaskName})
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

	databaseTableTypeMap, err = dst.DatabaseS.GetDatabaseTableType(dst.SchemaNameS)
	if err != nil {
		return err
	}

	if err := dst.DatabaseS.Transaction(dst.Ctx, &sql.TxOptions{}, []func(ctx context.Context, tx *sql.Tx) error{
		func(ctx context.Context, tx *sql.Tx) error {
			globalScn, err = dst.DatabaseS.GetDatabaseConsistentPos(ctx, tx)
			if err != nil {
				return err
			}
			return nil
		},
	}); err != nil {
		return err
	}

	// database tables
	// init database table
	dbTypeSli := stringutil.StringSplit(dst.Task.TaskFlow, constant.StringSeparatorAite)
	dbTypeS := dbTypeSli[0]

	logger.Info("data scan task start init",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))

	dst.Progress.SetTableCounts(uint64(len(databaseTaskTables)))

	// print struct migrate progress
	go dst.Progress.PrintProgress()

	g, gCtx := errgroup.WithContext(dst.Ctx)
	g.SetLimit(int(dst.TaskParams.TableThread))

	for _, taskJob := range databaseTaskTables {
		sourceTable := taskJob
		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
				startTime := time.Now()
				s, err := model.GetIDataMigrateSummaryRW().GetDataMigrateSummary(gCtx, &task.DataMigrateSummary{
					TaskName:    dst.Task.TaskName,
					SchemaNameS: dst.SchemaNameS,
					TableNameS:  sourceTable,
				})
				if err != nil {
					return err
				}
				if strings.EqualFold(s.InitFlag, constant.TaskInitStatusFinished) {
					dst.Progress.SetTableRowCounts(s.TableRowsS)
					dst.Progress.SetTableSizeCounts(uint64(s.TableSizeS))
					dst.Progress.SetTableChunkCounts(s.ChunkTotals)
					// the database task has init flag,skip
					select {
					case dst.ResumeC <- &WaitingRecs{
						TaskName:    dst.Task.TaskName,
						SchemaNameS: dst.SchemaNameS,
						TableNameS:  sourceTable,
					}:
						logger.Info("data scan task resume send",
							zap.String("task_name", dst.Task.TaskName),
							zap.String("task_mode", dst.Task.TaskMode),
							zap.String("task_flow", dst.Task.TaskFlow),
							zap.String("schema_name_s", dst.SchemaNameS),
							zap.String("table_name_s", sourceTable))
					default:
						_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(gCtx, &task.DataScanSummary{
							TaskName:    dst.Task.TaskName,
							SchemaNameS: dst.SchemaNameS,
							TableNameS:  sourceTable}, map[string]interface{}{
							"ScanFlag": constant.TaskMigrateStatusSkipped,
						})
						if err != nil {
							return err
						}
						logger.Warn("data scan task resume channel full",
							zap.String("task_name", dst.Task.TaskName),
							zap.String("task_mode", dst.Task.TaskMode),
							zap.String("task_flow", dst.Task.TaskFlow),
							zap.String("schema_name_s", dst.SchemaNameS),
							zap.String("table_name_s", sourceTable),
							zap.String("action", "skip send"))
					}
					return nil
				}

				tableRows, err := dst.DatabaseS.GetDatabaseTableRows(dst.SchemaNameS, sourceTable)
				if err != nil {
					return err
				}
				tableSize, err := dst.DatabaseS.GetDatabaseTableSize(dst.SchemaNameS, sourceTable)
				if err != nil {
					return err
				}

				dataRule := &DataScanRule{
					Ctx:               gCtx,
					TaskName:          dst.Task.TaskName,
					TaskMode:          dst.Task.TaskMode,
					TaskFlow:          dst.Task.TaskFlow,
					SchemaNameS:       dst.SchemaNameS,
					TableNameS:        sourceTable,
					TableTypeS:        databaseTableTypeMap,
					DatabaseS:         dst.DatabaseS,
					DBCharsetS:        dst.DBCharsetS,
					GlobalSqlHintS:    dst.TaskParams.SqlHintS,
					GlobalSamplerateS: strconv.FormatUint(dst.TaskParams.TableSamplerateS, 10),
				}

				attsRule, err := database.IDataScanAttributesRule(dataRule)
				if err != nil {
					return err
				}

				// If the database table ColumnDetailS and GroupColumnS return ""
				// it means that the database table does not have a number data type field, ignore and skip init
				if strings.EqualFold(attsRule.ColumnDetailS, "") && strings.EqualFold(attsRule.GroupColumnS, "") {
					return nil
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
						zap.String("database_version", dst.DBVersionS),
						zap.String("database_role", dst.DBRoleS),
						zap.String("migrate_method", "scan"))

					sampleRows := uint64(float64(tableRows) * size)
					sampleRowSizes := float64(tableSize) * size

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
							SnapshotPointS:  globalScn,
							ColumnDetailS:   attsRule.ColumnDetailS,
							GroupColumnS:    attsRule.GroupColumnS,
							SqlHintS:        attsRule.SqlHintS,
							ChunkID:         uuid.New().String(),
							ChunkDetailS:    encryptChunkS,
							ChunkDetailArgS: "",
							Samplerate:      attsRule.TableSamplerateS,
							ConsistentReadS: strconv.FormatBool(dst.TaskParams.EnableConsistentRead),
							TaskStatus:      constant.TaskDatabaseStatusWaiting,
						})
						if err != nil {
							return err
						}
						_, err = model.GetIDataScanSummaryRW().CreateDataScanSummary(txnCtx, &task.DataScanSummary{
							TaskName:       dst.Task.TaskName,
							SchemaNameS:    attsRule.SchemaNameS,
							TableNameS:     attsRule.TableNameS,
							SnapshotPointS: globalScn,
							TableRowsS:     sampleRows,
							TableSizeS:     sampleRowSizes,
							ChunkTotals:    1,
							InitFlag:       constant.TaskInitStatusFinished,
							ScanFlag:       constant.TaskScanStatusNotFinished,
						})
						if err != nil {
							return err
						}
						return nil
					})
					if err != nil {
						return err
					}

					dst.Progress.SetTableRowCounts(sampleRows)
					dst.Progress.SetTableSizeCounts(uint64(sampleRowSizes))
					dst.Progress.SetTableChunkCounts(1)
					return nil
				}

				dst.Progress.SetTableRowCounts(tableRows)
				dst.Progress.SetTableSizeCounts(uint64(tableSize))

				// statistic
				switch stringutil.StringUpper(dbTypeS) {
				case constant.DatabaseTypeOracle:
					if !strings.EqualFold(dst.DBRoleS, constant.OracleDatabasePrimaryRole) || (strings.EqualFold(dst.DBRoleS, constant.OracleDatabasePrimaryRole) && stringutil.VersionOrdinal(dst.DBVersionS) < stringutil.VersionOrdinal(constant.OracleDatabaseTableMigrateRowidRequireVersion)) {
						err = dst.ProcessStatisticsScan(
							gCtx,
							dbTypeS,
							globalScn,
							tableRows,
							tableSize,
							attsRule)
						if err != nil {
							return err
						}
						return nil
					}
				default:
					return fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] database type [%s] can't support, please contact author or retry", dst.Task.TaskName, dst.Task.TaskFlow, dst.Task.TaskMode, dbTypeS)
				}

				err = dst.ProcessChunkScan(gCtx, dst.SchemaNameS, sourceTable, globalScn, tableRows, tableSize, attsRule)
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

	if err = g.Wait(); err != nil {
		logger.Error("data scan task init failed",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", dst.SchemaNameS),
			zap.Error(err))
		return err
	}
	return nil
}

func (dst *DataScanTask) Run() error {
	logger.Info("data scan task run table",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))

	for s := range dst.WaiterC {
		err := dst.Process(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dst *DataScanTask) Resume() error {
	logger.Info("data scan task resume table",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))

	for s := range dst.ResumeC {
		err := dst.Process(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dst *DataScanTask) Last() error {
	logger.Info("data scan task last table",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))
	flags, err := model.GetIDataScanSummaryRW().QueryDataScanSummaryFlag(dst.Ctx, &task.DataScanSummary{
		TaskName:    dst.Task.TaskName,
		SchemaNameS: dst.SchemaNameS,
		InitFlag:    constant.TaskInitStatusFinished,
		ScanFlag:    constant.TaskScanStatusSkipped,
	})
	if err != nil {
		return err
	}

	for _, f := range flags {
		err = dst.Process(&WaitingRecs{
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

func (dst *DataScanTask) Process(s *WaitingRecs) error {
	startTableTime := time.Now()
	summary, err := model.GetIDataScanSummaryRW().GetDataScanSummary(dst.Ctx, &task.DataScanSummary{
		TaskName:    s.TaskName,
		SchemaNameS: s.SchemaNameS,
		TableNameS:  s.TableNameS,
	})
	if err != nil {
		return err
	}
	if strings.EqualFold(summary.ScanFlag, constant.TaskScanStatusFinished) {
		dst.Progress.UpdateTableNameProcessed(1)
		dst.Progress.UpdateTableRowsProcessed(summary.TableRowsS)
		dst.Progress.UpdateTableBytesProcessed(uint64(summary.TableSizeS))
		dst.Progress.UpdateTableChunksProcessed(summary.ChunkTotals)

		logger.Warn("data scan task init",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS),
			zap.String("scan_flag", summary.ScanFlag),
			zap.String("action", "scan skip"))
		return nil
	}

	if strings.EqualFold(summary.InitFlag, constant.TaskInitStatusNotFinished) {
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema_name_s [%s] table_name_s [%s] init status not finished, disabled scan", s.TableNameS, dst.Task.TaskMode, dst.Task.TaskFlow, s.SchemaNameS, s.TableNameS)
	}

	logger.Info("data scan task process table",
		zap.String("task_name", dst.Task.TaskName),
		zap.String("task_mode", dst.Task.TaskMode),
		zap.String("task_flow", dst.Task.TaskFlow),
		zap.String("schema_name_s", s.SchemaNameS),
		zap.String("table_name_s", s.TableNameS))

	var (
		migrateTasks []*task.DataScanTask
		successTasks []*task.DataScanTask
	)
	err = model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateTasks, err = model.GetIDataScanTaskRW().FindDataScanTask(txnCtx,
			&task.DataScanTask{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			},
			[]string{constant.TaskDatabaseStatusWaiting, constant.TaskDatabaseStatusFailed, constant.TaskDatabaseStatusRunning, constant.TaskDatabaseStatusStopped})
		if err != nil {
			return err
		}
		successTasks, err = model.GetIDataScanTaskRW().FindDataScanTask(txnCtx,
			&task.DataScanTask{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			},
			[]string{constant.TaskDatabaseStatusSuccess})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	successChunkNums := len(successTasks)
	if successChunkNums > 0 {
		dst.Progress.UpdateTableRowsProcessed(calcSuccRatioResult(successChunkNums, int(summary.ChunkTotals), summary.TableRowsS))
		dst.Progress.UpdateTableBytesProcessed(calcSuccRatioResult(successChunkNums, int(summary.ChunkTotals), uint64(summary.TableSizeS)))
		dst.Progress.UpdateTableChunksProcessed(uint64(successChunkNums))
		dst.Progress.SetLastTableRowsProcessed(calcSuccRatioResult(successChunkNums, int(summary.ChunkTotals), summary.TableRowsS))
		dst.Progress.SetLastTableBytesProcessed(calcSuccRatioResult(successChunkNums, int(summary.ChunkTotals), uint64(summary.TableSizeS)))
		dst.Progress.SetLastTableChunksProcessed(uint64(successChunkNums))
	}

	// Round up to an integer and calculate the average number of rows per chunk, which is the same as counting the number of rows per chunk processed.
	avgChunkRows := max(1, (summary.TableRowsS+summary.ChunkTotals-1)/summary.ChunkTotals)

	var avgChunkBytes uint64
	roundedUp := math.Ceil(summary.TableSizeS / float64(summary.ChunkTotals))
	if roundedUp < 1 {
		avgChunkBytes = 1
	} else {
		avgChunkBytes = uint64(roundedUp)
	}

	logger.Info("data scan task process chunks",
		zap.String("task_name", dst.Task.TaskName),
		zap.String("task_mode", dst.Task.TaskMode),
		zap.String("task_flow", dst.Task.TaskFlow),
		zap.String("schema_name_s", s.SchemaNameS),
		zap.String("table_name_s", s.TableNameS))

	g := thread.NewGroup()
	g.SetLimit(int(dst.TaskParams.SqlThreadS))
	go func() {
		for _, j := range migrateTasks {
			g.Go(j, func(j interface{}) error {
				dt := j.(*task.DataScanTask)
				gTime := time.Now()
				errW := model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
					_, err = model.GetIDataScanTaskRW().UpdateDataScanTask(txnCtx,
						&task.DataScanTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS, ChunkID: dt.ChunkID},
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
							stringutil.StringLower(dst.Task.TaskMode),
							dt.TaskName,
							dst.Task.TaskFlow,
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
					DatabaseS:  dst.DatabaseS,
					DBCharsetS: dst.DBCharsetS,
				})
				if err != nil {
					return err
				}
				return nil
			})
		}
		g.Wait()
	}()

	for res := range g.ResultC {
		dst.Progress.UpdateTableChunksProcessed(1)
		dst.Progress.UpdateTableRowsProcessed(avgChunkRows)
		dst.Progress.UpdateTableBytesProcessed(avgChunkBytes)
		if res.Error != nil {
			mt := res.Task.(*task.DataScanTask)
			logger.Error("data scan task process tables",
				zap.String("task_name", mt.TaskName),
				zap.String("task_mode", dst.Task.TaskMode),
				zap.String("task_flow", dst.Task.TaskFlow),
				zap.String("schema_name_s", mt.SchemaNameS),
				zap.String("table_name_s", mt.TableNameS),
				zap.Error(res.Error))

			if err := thread.Retry(
				&thread.RetryConfig{
					MaxRetries: thread.DefaultThreadErrorMaxRetries,
					Delay:      thread.DefaultThreadErrorRereyDelay,
				},
				func(err error) bool {
					return true
				},
				func() error {
					errW := model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIDataScanTaskRW().UpdateDataScanTask(txnCtx,
							&task.DataScanTask{TaskName: mt.TaskName, SchemaNameS: mt.SchemaNameS, TableNameS: mt.TableNameS, ChunkID: mt.ChunkID},
							map[string]interface{}{
								"TaskStatus":  constant.TaskDatabaseStatusFailed,
								"Duration":    res.Duration,
								"ErrorDetail": res.Error.Error(),
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
								stringutil.StringLower(dst.Task.TaskMode),
								mt.TaskName,
								dst.Task.TaskFlow,
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
					return nil
				}); err != nil {
				return err
			}
		}
	}

	endTableTime := time.Now()
	err = model.Transaction(dst.Ctx, func(txnCtx context.Context) error {
		var successChunks int64

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

				successChunks = successChunks + rec.StatusTotals
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

		summar, err := model.GetIDataScanSummaryRW().GetDataScanSummary(txnCtx, &task.DataScanSummary{
			TaskName:    s.TaskName,
			SchemaNameS: s.SchemaNameS,
			TableNameS:  s.TableNameS,
		})
		if err != nil {
			return err
		}

		if int64(summar.ChunkTotals) == successChunks {
			_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(txnCtx, &task.DataScanSummary{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			}, map[string]interface{}{
				"Duration": fmt.Sprintf("%f", time.Now().Sub(startTableTime).Seconds()),
				"ScanFlag": constant.TaskScanStatusFinished,
			})
			if err != nil {
				return err
			}
		} else {
			_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(txnCtx, &task.DataScanSummary{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			}, map[string]interface{}{
				"Duration": fmt.Sprintf("%f", time.Now().Sub(startTableTime).Seconds()),
				"ScanFlag": constant.TaskScanStatusNotFinished,
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

	dst.Progress.UpdateTableNameProcessed(1)

	logger.Info("data scan task process table",
		zap.String("task_name", dst.Task.TaskName),
		zap.String("task_mode", dst.Task.TaskMode),
		zap.String("task_flow", dst.Task.TaskFlow),
		zap.String("schema_name_s", s.SchemaNameS),
		zap.String("table_name_s", s.TableNameS),
		zap.String("cost", endTableTime.Sub(startTableTime).String()))
	return nil
}

func (dst *DataScanTask) ProcessStatisticsScan(ctx context.Context, dbTypeS, globalScn string, tableRows uint64, tableSize float64, attsRule *database.DataScanAttributesRule) error {
	h, err := dst.DatabaseS.GetDatabaseTableHighestSelectivityIndex(
		attsRule.SchemaNameS,
		attsRule.TableNameS,
		"",
		nil)
	if err != nil {
		return err
	}
	if h == nil {
		logger.Warn("data scan task table",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", attsRule.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("database_version", dst.DBVersionS),
			zap.String("database_role", dst.DBRoleS),
			zap.String("seletivity", "selectivity is null, skip statistics"),
			zap.String("migrate_method", "scan"))
		err = dst.ProcessTableScan(ctx, globalScn, tableRows, tableSize, attsRule)
		if err != nil {
			return err
		}
		return nil
	}

	// upstream bucket ranges
	err = h.TransSelectivity(
		dbTypeS,
		stringutil.StringUpper(dst.DBCharsetS),
		dst.Task.CaseFieldRuleS,
		false)
	if err != nil {
		return err
	}

	logger.Warn("data scan task table",
		zap.String("task_name", dst.Task.TaskName),
		zap.String("task_mode", dst.Task.TaskMode),
		zap.String("task_flow", dst.Task.TaskFlow),
		zap.String("schema_name_s", attsRule.SchemaNameS),
		zap.String("table_name_s", attsRule.TableNameS),
		zap.String("database_version", dst.DBVersionS),
		zap.String("database_role", dst.DBRoleS),
		zap.String("migrate_method", "statistic"))

	rangeC := make(chan []*structure.Range, constant.DefaultMigrateTaskQueueSize)
	chunksC := make(chan int, 1)

	d := &Divide{
		DBTypeS:     dbTypeS,
		DBCharsetS:  stringutil.StringUpper(dst.DBCharsetS),
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
		ChunkSize:   int64(dst.TaskParams.ChunkSize),
		DatabaseS:   dst.DatabaseS,
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
			statsRanges, err := dst.PrepareStatisticsRange(globalScn, attsRule, r)
			if err != nil {
				return err
			}
			if len(statsRanges) > 0 {
				err = model.GetIDataScanTaskRW().CreateInBatchDataScanTask(gCtx, statsRanges, int(dst.TaskParams.WriteThread), int(dst.TaskParams.BatchSize))
				if err != nil {
					return err
				}
				totalChunks = totalChunks + len(statsRanges)
			}
			return nil
		}

		chunksC <- totalChunks
		return nil
	})

	if err = g.Wait(); err != nil {
		return err
	}

	totalChunks := <-chunksC

	if totalChunks == 0 {
		err := dst.ProcessTableScan(ctx, globalScn, tableRows, tableSize, attsRule)
		if err != nil {
			return err
		}
		return nil
	}

	_, err = model.GetIDataScanSummaryRW().CreateDataScanSummary(ctx, &task.DataScanSummary{
		TaskName:       dst.Task.TaskName,
		SchemaNameS:    attsRule.SchemaNameS,
		TableNameS:     attsRule.TableNameS,
		SnapshotPointS: globalScn,
		TableRowsS:     tableRows,
		TableSizeS:     tableSize,
		ChunkTotals:    uint64(totalChunks),
		InitFlag:       constant.TaskInitStatusFinished,
		ScanFlag:       constant.TaskScanStatusNotFinished,
	})
	if err != nil {
		return err
	}

	dst.Progress.SetTableChunkCounts(uint64(totalChunks))

	select {
	case dst.WaiterC <- &WaitingRecs{
		TaskName:    dst.Task.TaskName,
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
	}:
		logger.Info("data scan task wait send",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", dst.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS))
	default:
		_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(ctx, &task.DataScanSummary{
			TaskName:    dst.Task.TaskName,
			SchemaNameS: attsRule.SchemaNameS,
			TableNameS:  attsRule.TableNameS}, map[string]interface{}{
			"ScanFlag": constant.TaskMigrateStatusSkipped,
		})
		if err != nil {
			return err
		}
		logger.Warn("data scan task wait channel full",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", attsRule.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("action", "skip send"))
	}
	return nil
}

func (dst *DataScanTask) ProcessTableScan(ctx context.Context, globalScn string, tableRows uint64, tableSize float64, attsRule *database.DataScanAttributesRule) error {
	var whereRange string
	whereRange = `1 = 1`

	encChunkS := snappy.Encode(nil, []byte(whereRange))

	encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}

	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err = model.GetIDataScanTaskRW().CreateDataScanTask(txnCtx, &task.DataScanTask{
			TaskName:        dst.Task.TaskName,
			SchemaNameS:     attsRule.SchemaNameS,
			TableNameS:      attsRule.TableNameS,
			TableTypeS:      attsRule.TableTypeS,
			SnapshotPointS:  globalScn,
			ColumnDetailS:   attsRule.ColumnDetailS,
			GroupColumnS:    attsRule.GroupColumnS,
			SqlHintS:        attsRule.SqlHintS,
			ChunkID:         uuid.New().String(),
			ChunkDetailS:    encryptChunkS,
			ChunkDetailArgS: "",
			Samplerate:      attsRule.TableSamplerateS,
			ConsistentReadS: strconv.FormatBool(dst.TaskParams.EnableConsistentRead),
			TaskStatus:      constant.TaskDatabaseStatusWaiting,
		})
		if err != nil {
			return err
		}
		_, err = model.GetIDataScanSummaryRW().CreateDataScanSummary(txnCtx, &task.DataScanSummary{
			TaskName:       dst.Task.TaskName,
			SchemaNameS:    attsRule.SchemaNameS,
			TableNameS:     attsRule.TableNameS,
			SnapshotPointS: globalScn,
			TableRowsS:     tableRows,
			TableSizeS:     tableSize,
			ChunkTotals:    1,
			InitFlag:       constant.TaskInitStatusFinished,
			ScanFlag:       constant.TaskScanStatusNotFinished,
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	dst.Progress.SetTableChunkCounts(1)

	select {
	case dst.WaiterC <- &WaitingRecs{
		TaskName:    dst.Task.TaskName,
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
	}:
		logger.Info("data scan task wait send",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", dst.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS))
	default:
		_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(ctx, &task.DataScanSummary{
			TaskName:    dst.Task.TaskName,
			SchemaNameS: attsRule.SchemaNameS,
			TableNameS:  attsRule.TableNameS}, map[string]interface{}{
			"ScanFlag": constant.TaskMigrateStatusSkipped,
		})
		if err != nil {
			return err
		}
		logger.Warn("data scan task wait channel full",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", attsRule.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("action", "skip send"))
	}
	return nil
}

func (dst *DataScanTask) ProcessChunkScan(ctx context.Context, schemaNameS, tableNameS, globalScn string, tableRows uint64, tableSize float64, attsRule *database.DataScanAttributesRule) error {
	chunkCh := make(chan []map[string]string, constant.DefaultMigrateTaskQueueSize)
	chunkRecC := make(chan int, 1)

	gC, gCtx := errgroup.WithContext(ctx)

	gC.Go(func() error {
		defer close(chunkCh)
		err := dst.DatabaseS.GetDatabaseTableChunkTask(
			uuid.New().String(), schemaNameS, tableNameS, dst.TaskParams.ChunkSize, dst.TaskParams.CallTimeout, int(dst.TaskParams.BatchSize), chunkCh)
		if err != nil {
			return err
		}
		return nil
	})

	gC.Go(func() error {
		var whereRange string
		totalChunkRecs := 0

		for chunks := range chunkCh {
			// batch commit
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
					SnapshotPointS:  globalScn,
					ColumnDetailS:   attsRule.ColumnDetailS,
					GroupColumnS:    attsRule.GroupColumnS,
					SqlHintS:        attsRule.SqlHintS,
					ChunkDetailS:    encryptChunkS,
					Samplerate:      attsRule.TableSamplerateS,
					ConsistentReadS: strconv.FormatBool(dst.TaskParams.EnableConsistentRead),
					TaskStatus:      constant.TaskDatabaseStatusWaiting,
				})
			}

			chunkRecs := len(metas)
			if chunkRecs > 0 {
				err := model.GetIDataScanTaskRW().CreateInBatchDataScanTask(gCtx, metas, int(dst.TaskParams.WriteThread), int(dst.TaskParams.BatchSize))
				if err != nil {
					return err
				}
				totalChunkRecs = totalChunkRecs + chunkRecs
			}
		}

		chunkRecC <- totalChunkRecs
		return nil
	})

	err := gC.Wait()
	if err != nil {
		return err
	}

	totalChunkRecs := <-chunkRecC

	if totalChunkRecs == 0 {
		err := dst.ProcessTableScan(ctx, globalScn, tableRows, tableSize, attsRule)
		if err != nil {
			return err
		}
		return nil
	}

	_, err = model.GetIDataScanSummaryRW().CreateDataScanSummary(ctx, &task.DataScanSummary{
		TaskName:       dst.Task.TaskName,
		SchemaNameS:    attsRule.SchemaNameS,
		TableNameS:     attsRule.TableNameS,
		SnapshotPointS: globalScn,
		TableRowsS:     tableRows,
		TableSizeS:     tableSize,
		ChunkTotals:    uint64(totalChunkRecs),
		InitFlag:       constant.TaskInitStatusFinished,
		ScanFlag:       constant.TaskScanStatusNotFinished,
	})
	if err != nil {
		return err
	}

	dst.Progress.SetTableChunkCounts(uint64(totalChunkRecs))

	select {
	case dst.WaiterC <- &WaitingRecs{
		TaskName:    dst.Task.TaskName,
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
	}:
		logger.Info("data scan task wait send",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", dst.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS))
	default:
		_, err = model.GetIDataScanSummaryRW().UpdateDataScanSummary(ctx, &task.DataScanSummary{
			TaskName:    dst.Task.TaskName,
			SchemaNameS: attsRule.SchemaNameS,
			TableNameS:  attsRule.TableNameS}, map[string]interface{}{
			"ScanFlag": constant.TaskMigrateStatusSkipped,
		})
		if err != nil {
			return err
		}
		logger.Warn("data scan task wait channel full",
			zap.String("task_name", dst.Task.TaskName),
			zap.String("task_mode", dst.Task.TaskMode),
			zap.String("task_flow", dst.Task.TaskFlow),
			zap.String("schema_name_s", attsRule.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("action", "skip send"))
	}
	return nil
}

func (dst *DataScanTask) PrepareStatisticsRange(globalScn string, attsRule *database.DataScanAttributesRule, ranges []*structure.Range) ([]*task.DataScanTask, error) {
	var (
		metas []*task.DataScanTask
		err   error
	)

	for _, r := range ranges {
		toStringS, toStringSArgs := r.ToString()
		var argsS string
		if toStringSArgs != nil {
			argsS, err = stringutil.MarshalJSON(toStringSArgs)
			if err != nil {
				return nil, err
			}
		}

		encChunkS := snappy.Encode(nil, []byte(toStringS))

		encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
		if err != nil {
			return nil, err
		}
		metas = append(metas, &task.DataScanTask{
			TaskName:        dst.Task.TaskName,
			SchemaNameS:     attsRule.SchemaNameS,
			TableNameS:      attsRule.TableNameS,
			TableTypeS:      attsRule.TableTypeS,
			SnapshotPointS:  globalScn,
			ColumnDetailS:   attsRule.ColumnDetailS,
			GroupColumnS:    attsRule.GroupColumnS,
			SqlHintS:        attsRule.SqlHintS,
			ChunkID:         uuid.New().String(),
			ChunkDetailS:    encryptChunkS,
			ChunkDetailArgS: argsS,
			Samplerate:      attsRule.TableSamplerateS,
			ConsistentReadS: strconv.FormatBool(dst.TaskParams.EnableConsistentRead),
			TaskStatus:      constant.TaskDatabaseStatusWaiting,
		})
	}
	return metas, nil
}
