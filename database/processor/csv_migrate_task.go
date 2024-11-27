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
	"encoding/json"
	"fmt"
	"path/filepath"
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

type CsvMigrateTask struct {
	Ctx             context.Context
	Task            *task.Task
	DBRoleS         string
	DBVersionS      string
	DBCharsetS      string
	DBCharsetT      string
	DatabaseS       database.IDatabase
	DatabaseT       database.IDatabase
	SchemaNameS     string
	SchemaNameT     string
	GlobalSnapshotS string

	CsvParams *pb.CsvMigrateParam

	WaiterC chan *WaitingRecs
	ResumeC chan *WaitingRecs
}

func (cmt *CsvMigrateTask) Init() error {
	defer func() {
		close(cmt.WaiterC)
		close(cmt.ResumeC)
	}()
	startTime := time.Now()
	logger.Info("csv migrate task init start",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("startTime", startTime.String()))

	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(cmt.Ctx, &rule.MigrateTaskTable{
		TaskName:    cmt.Task.TaskName,
		SchemaNameS: cmt.SchemaNameS,
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

	tableObjs, err := cmt.DatabaseS.FilterDatabaseTable(cmt.SchemaNameS, includeTables, excludeTables)
	if err != nil {
		return err
	}

	// rule case field
	for _, t := range tableObjs.TaskTables {
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
		databaseTaskTablesMap[tabName] = struct{}{}
	}

	logger.Warn("csv migrate task checkpoint action",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.Bool("enable_checkpoint", cmt.CsvParams.EnableCheckpoint))

	if !cmt.CsvParams.EnableCheckpoint {
		logger.Warn("stmt migrate task clear task records",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow))

		err = model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
			err := model.GetIDataMigrateSummaryRW().DeleteDataMigrateSummaryName(txnCtx, []string{cmt.Task.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataMigrateTaskRW().DeleteDataMigrateTaskName(txnCtx, []string{cmt.Task.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	logger.Info("csv migrate task compare table",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow))

	// compare the task table
	// the database task table is exist, and the config task table isn't exist, the clear the database task table
	summaries, err := model.GetIDataMigrateSummaryRW().FindDataMigrateSummary(cmt.Ctx, &task.DataMigrateSummary{TaskName: cmt.Task.TaskName, SchemaNameS: cmt.SchemaNameS})
	if err != nil {
		return err
	}
	for _, s := range summaries {
		_, ok := databaseTaskTablesMap[s.TableNameS]

		if !ok || strings.EqualFold(s.InitFlag, constant.TaskInitStatusNotFinished) {
			err = model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
				err = model.GetIDataMigrateSummaryRW().DeleteDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
				})
				if err != nil {
					return err
				}
				err = model.GetIDataMigrateTaskRW().DeleteDataMigrateTask(txnCtx, &task.DataMigrateTask{
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

	databaseTableTypeMap, err = cmt.DatabaseS.GetDatabaseTableType(cmt.SchemaNameS)
	if err != nil {
		return err
	}

	// database tables
	// init database table
	dbTypeSli := stringutil.StringSplit(cmt.Task.TaskFlow, constant.StringSeparatorAite)
	dbTypeS := dbTypeSli[0]

	initTable := time.Now()
	logger.Info("csv migrate task init table",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("startTime", initTable.String()))
	g, gCtx := errgroup.WithContext(cmt.Ctx)
	g.SetLimit(int(cmt.CsvParams.TableThread))

	for _, taskJob := range databaseTaskTables {
		sourceTable := taskJob
		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
				initTableTime := time.Now()
				s, err := model.GetIDataMigrateSummaryRW().GetDataMigrateSummary(gCtx, &task.DataMigrateSummary{
					TaskName:    cmt.Task.TaskName,
					SchemaNameS: cmt.SchemaNameS,
					TableNameS:  sourceTable,
				})
				if err != nil {
					return err
				}
				if strings.EqualFold(s.InitFlag, constant.TaskInitStatusFinished) {
					// the database task has init flag,skip
					select {
					case cmt.ResumeC <- &WaitingRecs{
						TaskName:    s.TaskName,
						SchemaNameS: s.SchemaNameS,
						TableNameS:  s.TableNameS,
						SchemaNameT: s.SchemaNameT,
						TableNameT:  s.TableNameT,
					}:
						logger.Info("csv migrate task resume send",
							zap.String("task_name", cmt.Task.TaskName),
							zap.String("task_mode", cmt.Task.TaskMode),
							zap.String("task_flow", cmt.Task.TaskFlow),
							zap.String("schema_name_s", cmt.SchemaNameS),
							zap.String("table_name_s", sourceTable))
					default:
						_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(gCtx, &task.DataMigrateSummary{
							TaskName:    cmt.Task.TaskName,
							SchemaNameS: cmt.SchemaNameS,
							TableNameS:  sourceTable}, map[string]interface{}{
							"MigrateFlag": constant.TaskMigrateStatusSkipped,
						})
						if err != nil {
							return err
						}
						logger.Warn("csv migrate task resume channel full",
							zap.String("task_name", cmt.Task.TaskName),
							zap.String("task_mode", cmt.Task.TaskMode),
							zap.String("task_flow", cmt.Task.TaskFlow),
							zap.String("schema_name_s", cmt.SchemaNameS),
							zap.String("table_name_s", sourceTable),
							zap.String("action", "skip send"))
					}
					return nil
				}

				tableRows, err := cmt.DatabaseS.GetDatabaseTableRows(cmt.SchemaNameS, sourceTable)
				if err != nil {
					return err
				}
				tableSize, err := cmt.DatabaseS.GetDatabaseTableSize(cmt.SchemaNameS, sourceTable)
				if err != nil {
					return err
				}

				dataRule := &DataMigrateRule{
					Ctx:            gCtx,
					TaskMode:       cmt.Task.TaskMode,
					TaskName:       cmt.Task.TaskName,
					TaskFlow:       cmt.Task.TaskFlow,
					DatabaseS:      cmt.DatabaseS,
					SchemaNameS:    cmt.SchemaNameS,
					TableNameS:     sourceTable,
					TableTypeS:     databaseTableTypeMap,
					DBCharsetS:     cmt.DBCharsetS,
					CaseFieldRuleS: cmt.Task.CaseFieldRuleS,
					CaseFieldRuleT: cmt.Task.CaseFieldRuleT,
					GlobalSqlHintS: cmt.CsvParams.SqlHintS,
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

					migrateTask := &task.DataMigrateTask{
						TaskName:        cmt.Task.TaskName,
						SchemaNameS:     attsRule.SchemaNameS,
						TableNameS:      attsRule.TableNameS,
						SchemaNameT:     attsRule.SchemaNameT,
						TableNameT:      attsRule.TableNameT,
						TableTypeS:      attsRule.TableTypeS,
						SnapshotPointS:  cmt.GlobalSnapshotS,
						ColumnDetailO:   attsRule.ColumnDetailO,
						ColumnDetailS:   attsRule.ColumnDetailS,
						ColumnDetailT:   attsRule.ColumnDetailT,
						SqlHintS:        attsRule.SqlHintS,
						ChunkID:         uuid.New().String(),
						ChunkDetailS:    encryptChunkS,
						ChunkDetailArgS: "",
						ConsistentReadS: strconv.FormatBool(cmt.CsvParams.EnableConsistentRead),
						TaskStatus:      constant.TaskDatabaseStatusWaiting,
						CsvFile:         filepath.Join(cmt.CsvParams.OutputDir, attsRule.SchemaNameS, attsRule.TableNameS, stringutil.StringBuilder(attsRule.SchemaNameT, `.`, attsRule.TableNameT, `.0.csv`)),
					}
					err = model.Transaction(gCtx, func(txnCtx context.Context) error {
						_, err = model.GetIDataMigrateTaskRW().CreateDataMigrateTask(txnCtx, migrateTask)
						if err != nil {
							return err
						}
						_, err = model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
							TaskName:       cmt.Task.TaskName,
							SchemaNameS:    attsRule.SchemaNameS,
							TableNameS:     attsRule.TableNameS,
							SchemaNameT:    attsRule.SchemaNameT,
							TableNameT:     attsRule.TableNameT,
							SnapshotPointS: cmt.GlobalSnapshotS,
							TableRowsS:     tableRows,
							TableSizeS:     tableSize,
							InitFlag:       constant.TaskInitStatusFinished,
							MigrateFlag:    constant.TaskMigrateStatusNotFinished,
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

					select {
					case cmt.WaiterC <- &WaitingRecs{
						TaskName:    s.TaskName,
						SchemaNameS: s.SchemaNameS,
						TableNameS:  s.TableNameS,
						SchemaNameT: s.SchemaNameT,
						TableNameT:  s.TableNameT,
					}:
						logger.Info("csv migrate task wait send",
							zap.String("task_name", cmt.Task.TaskName),
							zap.String("task_mode", cmt.Task.TaskMode),
							zap.String("task_flow", cmt.Task.TaskFlow),
							zap.String("schema_name_s", cmt.SchemaNameS),
							zap.String("table_name_s", sourceTable))
					default:
						_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(gCtx, &task.DataMigrateSummary{
							TaskName:    cmt.Task.TaskName,
							SchemaNameS: cmt.SchemaNameS,
							TableNameS:  sourceTable}, map[string]interface{}{
							"MigrateFlag": constant.TaskMigrateStatusSkipped,
						})
						if err != nil {
							return err
						}
						logger.Warn("csv migrate task wait channel full",
							zap.String("task_name", cmt.Task.TaskName),
							zap.String("task_mode", cmt.Task.TaskMode),
							zap.String("task_flow", cmt.Task.TaskFlow),
							zap.String("schema_name_s", cmt.SchemaNameS),
							zap.String("table_name_s", sourceTable),
							zap.String("action", "skip send"))
					}
					return nil
				}

				// statistic
				switch stringutil.StringUpper(dbTypeS) {
				case constant.DatabaseTypeOracle:
					if !strings.EqualFold(cmt.DBRoleS, constant.OracleDatabasePrimaryRole) || (strings.EqualFold(cmt.DBRoleS, constant.OracleDatabasePrimaryRole) && stringutil.VersionOrdinal(cmt.DBVersionS) < stringutil.VersionOrdinal(constant.OracleDatabaseTableMigrateRowidRequireVersion)) {
						err = cmt.ProcessStatisticsScan(
							gCtx,
							dbTypeS,
							cmt.GlobalSnapshotS,
							tableRows,
							tableSize,
							attsRule)
						if err != nil {
							return err
						}
					} else {
						err = cmt.ProcessChunkScan(
							gCtx,
							cmt.GlobalSnapshotS,
							tableRows,
							tableSize,
							attsRule)
						if err != nil {
							return err
						}
					}
				case constant.DatabaseTypePostgresql:
					err = cmt.ProcessStatisticsScan(
						gCtx,
						dbTypeS,
						cmt.GlobalSnapshotS,
						tableRows,
						tableSize,
						attsRule)
					if err != nil {
						return err
					}
				default:
					return fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] database type [%s] can't support, please contact author or retry", cmt.Task.TaskName, cmt.Task.TaskFlow, cmt.Task.TaskMode, dbTypeS)
				}

				logger.Info("csv migrate task init table finished",
					zap.String("task_name", cmt.Task.TaskName),
					zap.String("task_mode", cmt.Task.TaskMode),
					zap.String("task_flow", cmt.Task.TaskFlow),
					zap.String("schema_name_s", attsRule.SchemaNameS),
					zap.String("table_name_s", attsRule.TableNameS),
					zap.String("cost", time.Now().Sub(initTableTime).String()))
				return nil
			}
		})
	}

	if err = g.Wait(); err != nil {
		logger.Error("csv migrate task init failed",
			zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", cmt.SchemaNameS),
			zap.Error(err))
		return err
	}
	logger.Info("csv migrate task init table",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(initTable).String()))

	logger.Info("csv migrate task init success",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func (cmt *CsvMigrateTask) Run() error {
	startTime := time.Now()
	logger.Info("csv migrate task run starting",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))

	for s := range cmt.WaiterC {
		err := cmt.Process(s)
		if err != nil {
			return err
		}
	}

	logger.Info("csv migrate task run success",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func (cmt *CsvMigrateTask) Resume() error {
	startTime := time.Now()
	logger.Info("csv migrate task resume starting",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))

	for s := range cmt.ResumeC {
		err := cmt.Process(s)
		if err != nil {
			return err
		}
	}
	logger.Info("csv migrate task resume success",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func (cmt *CsvMigrateTask) Last() error {
	logger.Info("csv migrate task last table",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))
	flags, err := model.GetIDataMigrateSummaryRW().QueryDataMigrateSummaryFlag(cmt.Ctx, &task.DataMigrateSummary{
		TaskName:    cmt.Task.TaskName,
		SchemaNameS: cmt.SchemaNameS,
		InitFlag:    constant.TaskInitStatusFinished,
		MigrateFlag: constant.TaskMigrateStatusSkipped,
	})
	if err != nil {
		return err
	}

	for _, f := range flags {
		err = cmt.Process(&WaitingRecs{
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

func (cmt *CsvMigrateTask) Process(s *WaitingRecs) error {
	startTableTime := time.Now()

	summary, err := model.GetIDataMigrateSummaryRW().GetDataMigrateSummary(cmt.Ctx, &task.DataMigrateSummary{
		TaskName:    s.TaskName,
		SchemaNameS: s.SchemaNameS,
		TableNameS:  s.TableNameS,
	})
	if err != nil {
		return err
	}
	if strings.EqualFold(summary.MigrateFlag, constant.TaskMigrateStatusFinished) {
		logger.Warn("csv migrate task process",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS),
			zap.String("migrate_flag", summary.MigrateFlag),
			zap.String("action", "migrate skip"))
		return nil
	}

	if strings.EqualFold(summary.InitFlag, constant.TaskInitStatusNotFinished) {
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema_name_s [%s] table_name_s [%s] init status not finished, disabled migrate", s.TableNameS, cmt.Task.TaskMode, cmt.Task.TaskFlow, s.SchemaNameS, s.TableNameS)
	}

	// if the task table migrate status is importing, then it represent the task table has exported, but it isn't imported or importing failed
	if !strings.EqualFold(summary.MigrateFlag, constant.TaskMigrateStatusImporting) {
		err = stringutil.PathNotExistOrCreate(filepath.Join(cmt.CsvParams.OutputDir, s.SchemaNameS, s.TableNameS))
		if err != nil {
			return err
		}
		totalSpace, freeSpace, err := stringutil.GetDiskUsage(cmt.CsvParams.OutputDir)
		if err != nil {
			return err
		}
		// MB
		diskFactor, err := stringutil.StrconvFloatBitSize(cmt.CsvParams.DiskUsageFactor, 64)
		if err != nil {
			return err
		}
		estmTableSizeMB := summary.TableSizeS * diskFactor

		usedSpace := totalSpace - freeSpace

		if freeSpace < uint64(estmTableSizeMB) {
			logger.Warn("csv migrate task disk usage",
				zap.String("task_name", cmt.Task.TaskName),
				zap.String("task_mode", cmt.Task.TaskMode),
				zap.String("task_flow", cmt.Task.TaskFlow),
				zap.String("schema_name_s", s.SchemaNameS),
				zap.String("table_name_s", s.TableNameS),
				zap.String("output_dir", cmt.CsvParams.OutputDir),
				zap.Uint64("disk total space(MB)", totalSpace),
				zap.Uint64("disk used space(MB)", usedSpace),
				zap.Uint64("disk free space(MB)", freeSpace),
				zap.Uint64("estimate table space(MB)", uint64(estmTableSizeMB)))
			_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(cmt.Ctx, &task.DataMigrateSummary{
				TaskName:    s.TaskName,
				SchemaNameS: s.SchemaNameS,
				TableNameS:  s.TableNameS,
			}, map[string]interface{}{
				"Refused": fmt.Sprintf("the output [%s] current disk quota isn't enough, total space(MB): [%v], used space(MB): [%v], free space(MB): [%v], estimate space(MB): [%v]", cmt.CsvParams.OutputDir, totalSpace, usedSpace, freeSpace, estmTableSizeMB),
			})
			if err != nil {
				return err
			}
			// skip
			return nil
		}

		logger.Info("csv migrate task process table",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS),
			zap.String("output_dir", cmt.CsvParams.OutputDir),
			zap.Uint64("disk total space(MB)", totalSpace),
			zap.Uint64("disk used space(MB)", usedSpace),
			zap.Uint64("disk free space(MB)", freeSpace),
			zap.Uint64("estimate table space(MB)", uint64(estmTableSizeMB)),
			zap.String("startTime", startTableTime.String()))

		var migrateTasks []*task.DataMigrateTask
		migrateTasks, err = model.GetIDataMigrateTaskRW().FindDataMigrateTaskTableStatus(cmt.Ctx,
			s.TaskName,
			s.SchemaNameS,
			s.TableNameS,
			[]string{constant.TaskDatabaseStatusWaiting, constant.TaskDatabaseStatusFailed, constant.TaskDatabaseStatusRunning, constant.TaskDatabaseStatusStopped},
		)
		if err != nil {
			return err
		}

		logger.Info("csv migrate task process chunks",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS))

		var convertDBCharsetS string
		switch cmt.Task.TaskFlow {
		case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
			convertDBCharsetS = constant.MigrateOracleCharsetStringConvertMapping[cmt.DBCharsetS]
		case constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
			convertDBCharsetS = constant.MigratePostgreSQLCompatibleCharsetStringConvertMapping[cmt.DBCharsetS]
		default:
			return fmt.Errorf("the task_name [%s] schema_name_s [%s] task_mode [%s] task_flow [%s] prepare isn't support, please contact author", cmt.Task.TaskName, s.SchemaNameS, cmt.Task.TaskMode, cmt.Task.TaskFlow)
		}

		g := thread.NewGroup()
		g.SetLimit(int(cmt.CsvParams.SqlThreadS))

		go func() {
			for _, j := range migrateTasks {
				g.Go(j, func(j interface{}) error {
					gTime := time.Now()
					dt := j.(*task.DataMigrateTask)
					errW := model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
						_, err := model.GetIDataMigrateTaskRW().UpdateDataMigrateTask(txnCtx,
							&task.DataMigrateTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS, ChunkID: dt.ChunkID},
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
								stringutil.StringLower(cmt.Task.TaskMode),
								dt.TaskName,
								cmt.Task.TaskFlow,
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
						DatabaseS:  cmt.DatabaseS,
						DBCharsetS: convertDBCharsetS,
						DBCharsetT: stringutil.StringUpper(cmt.DBCharsetT),
						TaskParams: cmt.CsvParams,
						ReadChan:   make(chan []string, constant.DefaultMigrateTaskQueueSize),
						WriteChan:  make(chan string, constant.DefaultMigrateTaskQueueSize),
					})
					if err != nil {
						return err
					}

					errW = model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIDataMigrateTaskRW().UpdateDataMigrateTask(txnCtx,
							&task.DataMigrateTask{TaskName: dt.TaskName, SchemaNameS: dt.SchemaNameS, TableNameS: dt.TableNameS, ChunkID: dt.ChunkID},
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
								stringutil.StringLower(cmt.Task.TaskMode),
								dt.TaskName,
								cmt.Task.TaskFlow,
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
				})
			}

			g.Wait()
		}()

		for res := range g.ResultC {
			if res.Error != nil {
				smt := res.Task.(*task.DataMigrateTask)
				logger.Error("csv migrate task process tables",
					zap.String("task_name", cmt.Task.TaskName),
					zap.String("task_mode", cmt.Task.TaskMode),
					zap.String("task_flow", cmt.Task.TaskFlow),
					zap.String("schema_name_s", smt.SchemaNameS),
					zap.String("table_name_s", smt.TableNameS),
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
						if errW := model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
							_, err = model.GetIDataMigrateTaskRW().UpdateDataMigrateTask(txnCtx,
								&task.DataMigrateTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS, ChunkID: smt.ChunkID},
								map[string]interface{}{
									"TaskStatus":  constant.TaskDatabaseStatusFailed,
									"Duration":    res.Duration,
									"ErrorDetail": res.Error.Error(),
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
									stringutil.StringLower(cmt.Task.TaskMode),
									smt.TaskName,
									cmt.Task.TaskFlow,
									smt.SchemaNameS,
									smt.TableNameS),
							})
							if err != nil {
								return err
							}
							return nil
						}); errW != nil {
							return errW
						}
						return nil
					}); err != nil {
					return err
				}
			}
		}
	}

	var (
		successChunks int64
		summar        *task.DataMigrateSummary
	)

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
				successChunks = successChunks + rec.StatusTotals
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

		summar, err = model.GetIDataMigrateSummaryRW().GetDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
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

	logger.Info("csv migrate task process summary",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("schema_name_s", s.SchemaNameS),
		zap.String("table_name_s", s.TableNameS),
		zap.Uint64("chunk_totals", summar.ChunkTotals),
		zap.Int64("success_chunks", successChunks))

	if int64(summar.ChunkTotals) == successChunks {
		logger.Info("csv migrate task process table",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", s.SchemaNameS),
			zap.String("table_name_s", s.TableNameS),
			zap.Bool("enable_import_feature", cmt.CsvParams.EnableImportFeature))
		// the database table export success
		// enable import feature, only the task migrate status isn't finished
		if cmt.CsvParams.EnableImportFeature {
			if strings.EqualFold(summar.MigrateFlag, constant.TaskMigrateStatusFinished) {
				logger.Info("csv migrate task process table",
					zap.String("task_name", cmt.Task.TaskName),
					zap.String("task_mode", cmt.Task.TaskMode),
					zap.String("task_flow", cmt.Task.TaskFlow),
					zap.String("schema_name_s", s.SchemaNameS),
					zap.String("table_name_s", s.TableNameS),
					zap.String("import_table_name", "skip import"),
					zap.Bool("enable_import_feature", cmt.CsvParams.EnableImportFeature),
					zap.String("cost", time.Now().Sub(startTableTime).String()))
				return nil
			}
			err := model.Transaction(cmt.Ctx, func(txnCtx context.Context) error {
				_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(txnCtx, &task.DataMigrateSummary{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
				}, map[string]interface{}{
					"MigrateFlag": constant.TaskMigrateStatusImporting,
				})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
					LogDetail: fmt.Sprintf("%v [%v] csv migrate task [%v] taskflow [%v] source table [%v.%v] import start",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(cmt.Task.TaskMode),
						s.TaskName,
						cmt.Task.TaskFlow,
						s.SchemaNameS,
						s.TableNameS),
				})
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}

			// import
			dbTypeSli := stringutil.StringSplit(cmt.Task.TaskFlow, constant.StringSeparatorAite)
			dbTypeT := dbTypeSli[1]
			switch stringutil.StringUpper(dbTypeT) {
			case constant.DatabaseTypeTiDB:
				// require >=v7.5
				var (
					escape       string
					importParams ImportTiDBParams
				)
				if cmt.CsvParams.EscapeBackslash {
					// sql statement execution \ requires comment \\, the code retains two \\, requires \\\\
					escape = "\\\\"
				}
				if len(cmt.CsvParams.CsvImportParams) > 0 {
					marshalBytes, err := json.Marshal(cmt.CsvParams.CsvImportParams)
					if err != nil {
						return err
					}
					err = stringutil.UnmarshalJSON(marshalBytes, &importParams)
					if err != nil {
						return err
					}
				}
				limitOneRec, err := model.GetIDataMigrateTaskRW().GetDataMigrateTask(cmt.Ctx, &task.DataMigrateTask{
					TaskName:    s.TaskName,
					SchemaNameS: s.SchemaNameS,
					TableNameS:  s.TableNameS,
				})
				if err != nil {
					return err
				}
				importTiDB := &ImportTiDBDatabase{
					CharsetSet:          cmt.CsvParams.DataCharsetT,
					FieldsTerminatedBy:  cmt.CsvParams.Separator,
					FieldsEnclosedBy:    cmt.CsvParams.Delimiter,
					FieldsEscapedBy:     escape,
					FieldsDefinedNullBy: cmt.CsvParams.NullValue,
					LinesTerminatedBy:   cmt.CsvParams.Terminator,
					SkipRows:            1, // skip csv header
					ImportTiDBParams:    importParams,
				}
				outCsvDir := filepath.Join(cmt.CsvParams.OutputDir, s.SchemaNameS, s.TableNameS)

				if err := importTiDB.Import(cmt.Ctx, cmt.DatabaseT, s.SchemaNameT, s.TableNameT, limitOneRec.ColumnDetailT, outCsvDir); err != nil {
					return err
				}

				// clear csv data dir
				err = stringutil.RemoveAllDir(outCsvDir)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] database type [%s] can't support import, please setting enable-import-feature = false and retry", cmt.Task.TaskName, cmt.Task.TaskFlow, cmt.Task.TaskMode, dbTypeT)
			}
		}

		_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(cmt.Ctx, &task.DataMigrateSummary{
			TaskName:    s.TaskName,
			SchemaNameS: s.SchemaNameS,
			TableNameS:  s.TableNameS,
		}, map[string]interface{}{
			"MigrateFlag": constant.TaskMigrateStatusFinished,
			"Duration":    fmt.Sprintf("%f", time.Now().Sub(startTableTime).Seconds()),
		})
		if err != nil {
			return err
		}
	} else {
		_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(cmt.Ctx, &task.DataMigrateSummary{
			TaskName:    s.TaskName,
			SchemaNameS: s.SchemaNameS,
			TableNameS:  s.TableNameS,
		}, map[string]interface{}{
			"MigrateFlag": constant.TaskMigrateStatusNotFinished,
			"Duration":    fmt.Sprintf("%f", time.Now().Sub(startTableTime).Seconds()),
		})
		if err != nil {
			return err
		}
	}

	logger.Info("csv migrate task process table",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("schema_name_s", s.SchemaNameS),
		zap.String("table_name_s", s.TableNameS),
		zap.String("cost", time.Now().Sub(startTableTime).String()))
	return nil
}

func (cmt *CsvMigrateTask) ProcessStatisticsScan(ctx context.Context, dbTypeS, globalScn string, tableRows uint64, tableSize float64, attsRule *database.DataMigrateAttributesRule) error {
	h, err := cmt.DatabaseS.GetDatabaseTableHighestSelectivityIndex(
		attsRule.SchemaNameS,
		attsRule.TableNameS,
		"",
		nil)
	if err != nil {
		return err
	}
	if h == nil {
		logger.Warn("csv migrate task table",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", attsRule.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("database_version", cmt.DBVersionS),
			zap.String("database_role", cmt.DBRoleS),
			zap.String("seletivity", "selectivity is null, skip statistics"),
			zap.String("migrate_method", "scan"))
		err = cmt.ProcessTableScan(ctx, globalScn, tableRows, tableSize, attsRule)
		if err != nil {
			return err
		}
		return nil
	}

	// upstream bucket ranges
	err = h.TransSelectivity(
		dbTypeS,
		stringutil.StringUpper(cmt.DBCharsetS),
		cmt.Task.CaseFieldRuleS,
		false)
	if err != nil {
		return err
	}

	logger.Warn("csv migrate task table",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("schema_name_s", attsRule.SchemaNameS),
		zap.String("table_name_s", attsRule.TableNameS),
		zap.String("database_version", cmt.DBVersionS),
		zap.String("database_role", cmt.DBRoleS),
		zap.String("migrate_method", "statistic"))

	rangeC := make(chan []*structure.Range, constant.DefaultMigrateTaskQueueSize)
	chunksC := make(chan int, 1)
	d := &Divide{
		DBTypeS:     dbTypeS,
		DBCharsetS:  stringutil.StringUpper(cmt.DBCharsetS),
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
		ChunkSize:   int64(cmt.CsvParams.ChunkSize),
		DatabaseS:   cmt.DatabaseS,
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
		chunkID := 0
		for ranges := range rangeC {
			var statsRanges []*task.DataMigrateTask
			for _, r := range ranges {
				statsRange, err := cmt.PrepareStatisticsRange(globalScn, attsRule, r, chunkID)
				if err != nil {
					return err
				}
				statsRanges = append(statsRanges, statsRange)
				chunkID++
			}

			if len(statsRanges) > 0 {
				err = model.GetIDataMigrateTaskRW().CreateInBatchDataMigrateTask(gCtx, statsRanges, int(cmt.CsvParams.WriteThread), int(cmt.CsvParams.BatchSize))
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
		err := cmt.ProcessTableScan(ctx, globalScn, tableRows, tableSize, attsRule)
		if err != nil {
			return err
		}
		return nil
	}
	_, err = model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(ctx, &task.DataMigrateSummary{
		TaskName:       cmt.Task.TaskName,
		SchemaNameS:    attsRule.SchemaNameS,
		TableNameS:     attsRule.TableNameS,
		SchemaNameT:    attsRule.SchemaNameT,
		TableNameT:     attsRule.TableNameT,
		SnapshotPointS: globalScn,
		TableRowsS:     tableRows,
		TableSizeS:     tableSize,
		ChunkTotals:    uint64(totalChunks),
		InitFlag:       constant.TaskInitStatusFinished,
		MigrateFlag:    constant.TaskMigrateStatusNotFinished,
	})
	if err != nil {
		return err
	}

	select {
	case cmt.WaiterC <- &WaitingRecs{
		TaskName:    cmt.Task.TaskName,
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
		SchemaNameT: attsRule.SchemaNameT,
		TableNameT:  attsRule.TableNameT,
	}:
		logger.Info("csv migrate task wait send",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", cmt.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS))
	default:
		_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(ctx, &task.DataMigrateSummary{
			TaskName:    cmt.Task.TaskName,
			SchemaNameS: attsRule.SchemaNameS,
			TableNameS:  attsRule.TableNameS}, map[string]interface{}{
			"MigrateFlag": constant.TaskMigrateStatusSkipped,
		})
		if err != nil {
			return err
		}
		logger.Warn("csv migrate task wait channel full",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", attsRule.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("action", "skip send"))
	}
	return nil
}

func (cmt *CsvMigrateTask) ProcessTableScan(ctx context.Context, globalScn string, tableRows uint64, tableSize float64, attsRule *database.DataMigrateAttributesRule) error {
	var whereRange string
	switch {
	case attsRule.EnableChunkStrategy && !strings.EqualFold(attsRule.WhereRange, ""):
		whereRange = stringutil.StringBuilder(`1 = 1 AND `, attsRule.WhereRange)
	default:
		whereRange = `1 = 1`
	}
	logger.Warn("csv migrate task table",
		zap.String("task_name", cmt.Task.TaskName),
		zap.String("task_mode", cmt.Task.TaskMode),
		zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("schema_name_s", attsRule.SchemaNameS),
		zap.String("table_name_s", attsRule.TableNameS),
		zap.String("database_version", cmt.DBVersionS),
		zap.String("database_role", cmt.DBRoleS),
		zap.String("where_range", whereRange))

	encChunkS := snappy.Encode(nil, []byte(whereRange))

	encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}

	csvFile := filepath.Join(cmt.CsvParams.OutputDir, attsRule.SchemaNameS, attsRule.TableNameS, stringutil.StringBuilder(attsRule.SchemaNameT, `.`, attsRule.TableNameT, `.0.csv`))

	migrateTask := &task.DataMigrateTask{
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
		ChunkID:         uuid.New().String(),
		ChunkDetailS:    encryptChunkS,
		ChunkDetailArgS: "",
		ConsistentReadS: strconv.FormatBool(cmt.CsvParams.EnableConsistentRead),
		TaskStatus:      constant.TaskDatabaseStatusWaiting,
		CsvFile:         csvFile,
	}
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err = model.GetIDataMigrateTaskRW().CreateDataMigrateTask(txnCtx, migrateTask)
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
			InitFlag:       constant.TaskInitStatusFinished,
			MigrateFlag:    constant.TaskMigrateStatusNotFinished,
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
	case cmt.WaiterC <- &WaitingRecs{
		TaskName:    cmt.Task.TaskName,
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
		SchemaNameT: attsRule.SchemaNameT,
		TableNameT:  attsRule.TableNameT,
	}:
		logger.Info("csv migrate task wait send",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", cmt.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS))
	default:
		_, err = model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(ctx, &task.DataMigrateSummary{
			TaskName:    cmt.Task.TaskName,
			SchemaNameS: attsRule.SchemaNameS,
			TableNameS:  attsRule.TableNameS}, map[string]interface{}{
			"MigrateFlag": constant.TaskMigrateStatusSkipped,
		})
		if err != nil {
			return err
		}
		logger.Warn("csv migrate task wait channel full",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", attsRule.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("action", "skip send"))
	}
	return nil
}

func (cmt *CsvMigrateTask) ProcessChunkScan(ctx context.Context, globalScn string, tableRows uint64, tableSize float64, attsRule *database.DataMigrateAttributesRule) error {
	chunkCh := make(chan []map[string]string, constant.DefaultMigrateTaskQueueSize)
	chunkRecC := make(chan int, 1)

	gC, gCtx := errgroup.WithContext(ctx)

	gC.Go(func() error {
		defer close(chunkCh)
		err := cmt.DatabaseS.GetDatabaseTableChunkTask(
			uuid.New().String(), attsRule.SchemaNameS, attsRule.TableNameS, cmt.CsvParams.ChunkSize, cmt.CsvParams.CallTimeout, int(cmt.CsvParams.BatchSize), chunkCh)
		if err != nil {
			return err
		}
		return nil
	})

	gC.Go(func() error {
		var whereRange string
		totalChunkRecs := 0
		chunkID := 0

		for chunks := range chunkCh {
			// batch commit
			var (
				metas []*task.DataMigrateTask
			)
			for _, r := range chunks {
				csvFile := filepath.Join(cmt.CsvParams.OutputDir, attsRule.SchemaNameS, attsRule.TableNameS, stringutil.StringBuilder(attsRule.SchemaNameT, `.`, attsRule.TableNameT, `.`, strconv.Itoa(chunkID), `.csv`))

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
					ChunkID:         uuid.New().String(),
					ChunkDetailS:    encryptChunkS,
					ChunkDetailArgS: "",
					ConsistentReadS: strconv.FormatBool(cmt.CsvParams.EnableConsistentRead),
					TaskStatus:      constant.TaskDatabaseStatusWaiting,
					CsvFile:         csvFile,
				})

				chunkID++
			}

			chunkRecs := len(metas)
			if chunkRecs > 0 {
				err := model.GetIDataMigrateTaskRW().CreateInBatchDataMigrateTask(gCtx, metas, int(cmt.CsvParams.WriteThread), int(cmt.CsvParams.BatchSize))
				if err != nil {
					return err
				}
				totalChunkRecs = totalChunkRecs + chunkRecs
			}
		}

		chunkRecC <- totalChunkRecs
		return nil
	})

	if err := gC.Wait(); err != nil {
		return err
	}

	totalChunkRecs := <-chunkRecC

	if totalChunkRecs == 0 {
		err := cmt.ProcessTableScan(ctx, globalScn, tableRows, tableSize, attsRule)
		if err != nil {
			return err
		}
		return nil
	}

	_, err := model.GetIDataMigrateSummaryRW().CreateDataMigrateSummary(ctx, &task.DataMigrateSummary{
		TaskName:       cmt.Task.TaskName,
		SchemaNameS:    attsRule.SchemaNameS,
		TableNameS:     attsRule.TableNameS,
		SchemaNameT:    attsRule.SchemaNameT,
		TableNameT:     attsRule.TableNameT,
		SnapshotPointS: globalScn,
		TableRowsS:     tableRows,
		TableSizeS:     tableSize,
		ChunkTotals:    uint64(totalChunkRecs),
		InitFlag:       constant.TaskInitStatusFinished,
		MigrateFlag:    constant.TaskMigrateStatusNotFinished,
	})
	if err != nil {
		return err
	}

	select {
	case cmt.WaiterC <- &WaitingRecs{
		TaskName:    cmt.Task.TaskName,
		SchemaNameS: attsRule.SchemaNameS,
		TableNameS:  attsRule.TableNameS,
		SchemaNameT: attsRule.SchemaNameT,
		TableNameT:  attsRule.TableNameT,
	}:
		logger.Info("csv migrate task wait send",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", cmt.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS))
	default:
		_, err := model.GetIDataMigrateSummaryRW().UpdateDataMigrateSummary(ctx, &task.DataMigrateSummary{
			TaskName:    cmt.Task.TaskName,
			SchemaNameS: attsRule.SchemaNameS,
			TableNameS:  attsRule.TableNameS}, map[string]interface{}{
			"MigrateFlag": constant.TaskMigrateStatusSkipped,
		})
		if err != nil {
			return err
		}
		logger.Warn("csv migrate task wait channel full",
			zap.String("task_name", cmt.Task.TaskName),
			zap.String("task_mode", cmt.Task.TaskMode),
			zap.String("task_flow", cmt.Task.TaskFlow),
			zap.String("schema_name_s", attsRule.SchemaNameS),
			zap.String("table_name_s", attsRule.TableNameS),
			zap.String("action", "skip send"))
	}
	return nil
}

func (cmt *CsvMigrateTask) PrepareStatisticsRange(globalScn string, attsRule *database.DataMigrateAttributesRule, r *structure.Range, chunkID int) (*task.DataMigrateTask, error) {
	toStringS, toStringSArgs := r.ToString()
	var (
		argsS string
		err   error
	)
	if toStringSArgs != nil {
		argsS, err = stringutil.MarshalJSON(toStringSArgs)
		if err != nil {
			return nil, err
		}
	}
	var (
		whereRange string
		csvFile    string
	)
	switch {
	case attsRule.EnableChunkStrategy && !strings.EqualFold(attsRule.WhereRange, ""):
		whereRange = stringutil.StringBuilder(`((`, toStringS, `) AND (`, attsRule.WhereRange, `))`)
	default:
		whereRange = toStringS
	}

	encChunkS := snappy.Encode(nil, []byte(whereRange))

	encryptChunkS, err := stringutil.Encrypt(stringutil.BytesToString(encChunkS), []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return nil, err
	}

	csvFile = filepath.Join(cmt.CsvParams.OutputDir, attsRule.SchemaNameS, attsRule.TableNameS, stringutil.StringBuilder(attsRule.SchemaNameT, `.`, attsRule.TableNameT, `.`, strconv.Itoa(chunkID), `.csv`))

	return &task.DataMigrateTask{
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
		ChunkID:         uuid.New().String(),
		ChunkDetailS:    encryptChunkS,
		ChunkDetailArgS: argsS,
		ConsistentReadS: strconv.FormatBool(cmt.CsvParams.EnableConsistentRead),
		TaskStatus:      constant.TaskDatabaseStatusWaiting,
		CsvFile:         csvFile,
	}, nil
}
