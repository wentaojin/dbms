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
	"strconv"
	"strings"
	"time"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/errconcurrent"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type SqlMigrateTask struct {
	Ctx        context.Context
	Task       *task.Task
	DatabaseS  database.IDatabase
	DatabaseT  database.IDatabase
	DBRoleS    string
	DBCharsetS string
	DBCharsetT string
	DBVersionS string

	TaskParams *pb.SqlMigrateParam
}

func (smt *SqlMigrateTask) Init() error {
	logger.Info("sql migrate task init table",
		zap.String("task_name", smt.Task.TaskName), zap.String("task_mode", smt.Task.TaskMode), zap.String("task_flow", smt.Task.TaskFlow))

	if !smt.TaskParams.EnableCheckpoint {
		err := model.GetISqlMigrateTaskRW().DeleteSqlMigrateTaskName(smt.Ctx, []string{smt.Task.TaskName})
		if err != nil {
			return err
		}
		err = model.GetISqlMigrateSummaryRW().DeleteSqlMigrateSummaryName(smt.Ctx, []string{smt.Task.TaskName})
		if err != nil {
			return err
		}
	}
	logger.Warn("sql migrate task init skip",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow),
		zap.Bool("enable_checkpoint", smt.TaskParams.EnableCheckpoint))

	// compare the task table
	// the database task table is exist, and the config task table isn't exist, the clear the database task table
	s, err := model.GetISqlMigrateSummaryRW().GetSqlMigrateSummary(smt.Ctx, &task.SqlMigrateSummary{TaskName: smt.Task.TaskName})
	if err != nil {
		return err
	}

	if strings.EqualFold(s.InitFlag, constant.TaskInitStatusNotFinished) {
		err = model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
			err = model.GetISqlMigrateSummaryRW().DeleteSqlMigrateSummary(txnCtx, &task.SqlMigrateSummary{
				TaskName: s.TaskName,
			})
			if err != nil {
				return err
			}
			err = model.GetISqlMigrateTaskRW().DeleteSqlMigrateTask(txnCtx, &task.SqlMigrateTask{
				TaskName: s.TaskName,
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

	var globalScn string

	if err := smt.DatabaseS.Transaction(smt.Ctx, &sql.TxOptions{}, []func(ctx context.Context, tx *sql.Tx) error{
		func(ctx context.Context, tx *sql.Tx) error {
			globalScn, err = smt.DatabaseS.GetDatabaseConsistentPos(ctx, tx)
			if err != nil {
				return err
			}
			return nil
		},
	}); err != nil {
		return err
	}

	logger.Info("sql migrate task init sql",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow))

	migrateSqlRules, err := model.GetISqlMigrateRuleRW().FindSqlMigrateRule(smt.Ctx, &rule.SqlMigrateRule{
		TaskName: smt.Task.TaskName,
	})
	if err != nil {
		return err
	}

	var sqlMigrateTasks []*task.SqlMigrateTask
	for _, sr := range migrateSqlRules {
		columnRouteRule := make(map[string]string)
		err = stringutil.UnmarshalJSON([]byte(sr.ColumnRouteRule), &columnRouteRule)
		if err != nil {
			return err
		}
		dataRule := &SqlMigrateRule{
			Ctx:             smt.Ctx,
			TaskName:        smt.Task.TaskName,
			TaskMode:        smt.Task.TaskMode,
			TaskFlow:        smt.Task.TaskFlow,
			SchemaNameT:     sr.SchemaNameT,
			TableNameT:      sr.TableNameT,
			SqlHintT:        sr.SqlHintT,
			GlobalSqlHintT:  smt.TaskParams.SqlHintT,
			DatabaseS:       smt.DatabaseS,
			DBCharsetS:      stringutil.StringUpper(smt.DBCharsetS),
			SqlQueryS:       sr.SqlQueryS,
			ColumnRouteRule: columnRouteRule,
			CaseFieldRuleS:  smt.Task.CaseFieldRuleS,
			CaseFieldRuleT:  smt.Task.CaseFieldRuleT,
		}
		attrs, err := database.ISqlMigrateAttributesRule(dataRule)
		if err != nil {
			return err
		}
		sqlMigrateTasks = append(sqlMigrateTasks, &task.SqlMigrateTask{
			TaskName:        smt.Task.TaskName,
			SchemaNameT:     attrs.SchemaNameT,
			TableNameT:      attrs.TableNameT,
			SnapshotPointS:  globalScn,
			ColumnDetailO:   attrs.ColumnDetailO,
			ColumnDetailS:   attrs.ColumnDetailS,
			ColumnDetailT:   attrs.ColumnDetailT,
			SqlHintT:        attrs.SqlHintT,
			ConsistentReadS: strconv.FormatBool(smt.TaskParams.EnableConsistentRead),
			TaskStatus:      constant.TaskDatabaseStatusWaiting,
			SqlQueryS:       attrs.SqlQueryS,
		})
	}

	migrateSqlRuleGroupResults, err := model.GetISqlMigrateRuleRW().FindSqlMigrateRuleGroupBySchemaTable(smt.Ctx)
	if err != nil {
		return err
	}

	err = model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
		err = model.GetISqlMigrateTaskRW().CreateInBatchSqlMigrateTask(txnCtx, sqlMigrateTasks, int(smt.TaskParams.WriteThread), int(smt.TaskParams.BatchSize))
		if err != nil {
			return err
		}
		for _, r := range migrateSqlRuleGroupResults {
			_, err = model.GetISqlMigrateSummaryRW().CreateSqlMigrateSummary(txnCtx, &task.SqlMigrateSummary{
				TaskName:    smt.Task.TaskName,
				SqlTotals:   r.RowTotals,
				InitFlag:    constant.TaskInitStatusFinished,
				MigrateFlag: constant.TaskMigrateStatusNotFinished,
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

	return nil
}

func (smt *SqlMigrateTask) Run() error {
	logger.Info("sql migrate task run table",
		zap.String("task_name", smt.Task.TaskName), zap.String("task_mode", smt.Task.TaskMode), zap.String("task_flow", smt.Task.TaskFlow))
	err := smt.Process()
	if err != nil {
		return err
	}
	return nil
}

func (smt *SqlMigrateTask) Resume() error {
	logger.Info("sql migrate task resume table",
		zap.String("task_name", smt.Task.TaskName), zap.String("task_mode", smt.Task.TaskMode), zap.String("task_flow", smt.Task.TaskFlow))
	return nil
}

func (smt *SqlMigrateTask) Last() error {
	logger.Info("sql migrate task last table",
		zap.String("task_name", smt.Task.TaskName), zap.String("task_mode", smt.Task.TaskMode), zap.String("task_flow", smt.Task.TaskFlow))
	return nil
}

func (smt *SqlMigrateTask) Process() error {
	startTime := time.Now()
	s, err := model.GetISqlMigrateSummaryRW().GetSqlMigrateSummary(smt.Ctx, &task.SqlMigrateSummary{TaskName: smt.Task.TaskName})
	if err != nil {
		return err
	}

	if strings.EqualFold(s.MigrateFlag, constant.TaskMigrateStatusFinished) {
		logger.Warn("sql migrate task migrate skip",
			zap.String("task_name", smt.Task.TaskName),
			zap.String("task_mode", smt.Task.TaskMode),
			zap.String("task_flow", smt.Task.TaskFlow),
			zap.String("init_flag", s.InitFlag),
			zap.String("migrate_flag", s.MigrateFlag),
			zap.String("action", "migrate skip"))
		return nil
	}
	var (
		migrateTasks []*task.SqlMigrateTask
	)
	err = model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateTasks, err = model.GetISqlMigrateTaskRW().FindSqlMigrateTaskByTaskStatus(txnCtx,
			&task.SqlMigrateTask{
				TaskName:   smt.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusWaiting,
			})
		if err != nil {
			return err
		}
		migrateFailedTasks, err := model.GetISqlMigrateTaskRW().FindSqlMigrateTaskByTaskStatus(txnCtx,
			&task.SqlMigrateTask{
				TaskName:   smt.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusFailed})
		if err != nil {
			return err
		}
		migrateRunningTasks, err := model.GetISqlMigrateTaskRW().FindSqlMigrateTaskByTaskStatus(txnCtx,
			&task.SqlMigrateTask{
				TaskName:   smt.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusRunning})
		if err != nil {
			return err
		}
		migrateStopTasks, err := model.GetISqlMigrateTaskRW().FindSqlMigrateTaskByTaskStatus(txnCtx,
			&task.SqlMigrateTask{
				TaskName:   smt.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusStopped})
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

	logger.Info("sql migrate task process sql",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow))

	g := errconcurrent.NewGroup()
	g.SetLimit(int(smt.TaskParams.SqlThreadS))
	for _, job := range migrateTasks {
		gTime := time.Now()
		g.Go(job, gTime, func(job interface{}) error {
			dt := job.(*task.SqlMigrateTask)
			switch {
			case strings.EqualFold(smt.Task.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(smt.Task.TaskFlow, constant.TaskFlowOracleToMySQL):
				errW := model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
					_, err = model.GetISqlMigrateTaskRW().UpdateSqlMigrateTask(txnCtx,
						&task.SqlMigrateTask{TaskName: dt.TaskName, SchemaNameT: dt.SchemaNameT, TableNameT: dt.TableNameT, SqlQueryS: dt.SqlQueryS},
						map[string]interface{}{
							"TaskStatus": constant.TaskDatabaseStatusRunning,
						})
					if err != nil {
						return err
					}
					_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
						TaskName: dt.TaskName,
						LogDetail: fmt.Sprintf("%v [%v] sql migrate task [%v] taskflow [%v] schema_name_t [%v] table_name_t [%v] start",
							stringutil.CurrentTimeFormatString(),
							stringutil.StringLower(smt.Task.TaskMode),
							dt.TaskName,
							smt.Task.TaskFlow,
							dt.SchemaNameT,
							dt.TableNameT),
					})
					if err != nil {
						return err
					}
					return nil
				})
				if errW != nil {
					return errW
				}

				sqlStr := GenMYSQLCompatibleDatabasePrepareStmt(dt.SchemaNameT, dt.TableNameT, smt.TaskParams.SqlHintT, dt.ColumnDetailT, int(smt.TaskParams.BatchSize), true)

				stmt, err := smt.DatabaseT.PrepareContext(smt.Ctx, sqlStr)
				if err != nil {
					return err
				}
				defer stmt.Close()

				err = database.IDataMigrateProcess(&SqlMigrateRow{
					Ctx:           smt.Ctx,
					TaskMode:      smt.Task.TaskMode,
					TaskFlow:      smt.Task.TaskFlow,
					Smt:           dt,
					DatabaseS:     smt.DatabaseS,
					DatabaseT:     smt.DatabaseT,
					DatabaseTStmt: stmt,
					DBCharsetS:    constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(smt.DBCharsetS)],
					DBCharsetT:    stringutil.StringUpper(smt.DBCharsetT),
					SqlThreadT:    int(smt.TaskParams.SqlThreadT),
					BatchSize:     int(smt.TaskParams.BatchSize),
					CallTimeout:   int(smt.TaskParams.CallTimeout),
					SafeMode:      smt.TaskParams.EnableSafeMode,
					ReadChan:      make(chan []interface{}, constant.DefaultMigrateTaskQueueSize),
					WriteChan:     make(chan []interface{}, constant.DefaultMigrateTaskQueueSize),
				})
				if err != nil {
					return err
				}

				errW = model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
					_, err = model.GetISqlMigrateTaskRW().UpdateSqlMigrateTask(txnCtx,
						&task.SqlMigrateTask{TaskName: dt.TaskName, SchemaNameT: dt.SchemaNameT, TableNameT: dt.TableNameT, SqlQueryS: dt.SqlQueryS},
						map[string]interface{}{
							"TaskStatus": constant.TaskDatabaseStatusSuccess,
							"Duration":   fmt.Sprintf("%f", time.Now().Sub(gTime).Seconds()),
						})
					if err != nil {
						return err
					}
					_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
						TaskName: dt.TaskName,
						LogDetail: fmt.Sprintf("%v [%v] sql migrate task [%v] taskflow [%v] schema_name_t [%v] table_name_t [%v] success",
							stringutil.CurrentTimeFormatString(),
							stringutil.StringLower(smt.Task.TaskMode),
							dt.TaskName,
							smt.Task.TaskFlow,
							dt.SchemaNameT,
							dt.TableNameT),
					})
					if err != nil {
						return err
					}
					return nil
				})
				if errW != nil {
					return errW
				}
			default:
				return fmt.Errorf("oracle current task [%s] task_mode [%s] task_flow [%s] column rule isn't support, please contact author", smt.Task.TaskName, smt.Task.TaskMode, smt.Task.TaskFlow)
			}
			return nil
		})
	}

	for _, r := range g.Wait() {
		if r.Err != nil {
			esmt := r.Task.(*task.SqlMigrateTask)
			logger.Warn("sql migrate task process sql",
				zap.String("task_name", smt.Task.TaskName),
				zap.String("task_mode", smt.Task.TaskMode),
				zap.String("task_flow", smt.Task.TaskFlow),
				zap.String("schema_name_t", esmt.SchemaNameT),
				zap.String("table_name_t", esmt.TableNameT),
				zap.Error(r.Err))

			errW := model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
				_, err = model.GetISqlMigrateTaskRW().UpdateSqlMigrateTask(txnCtx,
					&task.SqlMigrateTask{TaskName: esmt.TaskName, SchemaNameT: esmt.SchemaNameT, TableNameT: esmt.TableNameT, SqlQueryS: esmt.SqlQueryS},
					map[string]interface{}{
						"TaskStatus":  constant.TaskDatabaseStatusFailed,
						"Duration":    fmt.Sprintf("%f", time.Now().Sub(r.Time).Seconds()),
						"ErrorDetail": r.Err.Error(),
					})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName: esmt.TaskName,
					LogDetail: fmt.Sprintf("%v [%v] sql migrate task [%v] taskflow [%v] schema_name_t [%v] table_name_t [%v] failed, please see [sql_migrate_task] detail",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(smt.Task.TaskMode),
						esmt.TaskName,
						smt.Task.TaskFlow,
						esmt.SchemaNameT,
						esmt.TableNameT),
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

	var successRecs int64
	err = model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
		tableStatusRecs, err := model.GetISqlMigrateTaskRW().FindSqlMigrateTaskGroupByTaskStatus(txnCtx, smt.Task.TaskName)
		if err != nil {
			return err
		}
		for _, rec := range tableStatusRecs {
			switch rec.TaskStatus {
			case constant.TaskDatabaseStatusSuccess:
				_, err = model.GetISqlMigrateSummaryRW().UpdateSqlMigrateSummary(txnCtx, &task.SqlMigrateSummary{
					TaskName: rec.TaskName,
				}, map[string]interface{}{
					"SqlSuccess": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
				successRecs = successRecs + rec.StatusCounts
			case constant.TaskDatabaseStatusFailed:
				_, err = model.GetISqlMigrateSummaryRW().UpdateSqlMigrateSummary(txnCtx, &task.SqlMigrateSummary{
					TaskName: rec.TaskName,
				}, map[string]interface{}{
					"SqlFails": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusWaiting:
				_, err = model.GetISqlMigrateSummaryRW().UpdateSqlMigrateSummary(txnCtx, &task.SqlMigrateSummary{
					TaskName: rec.TaskName,
				}, map[string]interface{}{
					"SqlWaits": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusRunning:
				_, err = model.GetISqlMigrateSummaryRW().UpdateSqlMigrateSummary(txnCtx, &task.SqlMigrateSummary{
					TaskName: rec.TaskName,
				}, map[string]interface{}{
					"SqlRuns": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			case constant.TaskDatabaseStatusStopped:
				_, err = model.GetISqlMigrateSummaryRW().UpdateSqlMigrateSummary(txnCtx, &task.SqlMigrateSummary{
					TaskName: rec.TaskName,
				}, map[string]interface{}{
					"SqlStops": rec.StatusCounts,
				})
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("the task [%v] task_mode [%s] task_flow [%v] task_status [%v] panic, please contact auhtor or reselect", smt.Task.TaskName, smt.Task.TaskMode, smt.Task.TaskFlow, rec.TaskStatus)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	s, err = model.GetISqlMigrateSummaryRW().GetSqlMigrateSummary(smt.Ctx, &task.SqlMigrateSummary{TaskName: smt.Task.TaskName})
	if err != nil {
		return err
	}
	if int64(s.SqlSuccess) == successRecs {
		_, err = model.GetISqlMigrateSummaryRW().UpdateSqlMigrateSummary(smt.Ctx, &task.SqlMigrateSummary{
			TaskName: smt.Task.TaskName,
		}, map[string]interface{}{
			"MigrateFlag": constant.TaskMigrateStatusFinished,
			"Duration":    fmt.Sprintf("%f", time.Now().Sub(startTime).Seconds()),
		})
		if err != nil {
			return err
		}
	} else {
		_, err = model.GetISqlMigrateSummaryRW().UpdateSqlMigrateSummary(smt.Ctx, &task.SqlMigrateSummary{
			TaskName: smt.Task.TaskName,
		}, map[string]interface{}{
			"MigrateFlag": constant.TaskMigrateStatusNotFinished,
			"Duration":    fmt.Sprintf("%f", time.Now().Sub(startTime).Seconds()),
		})
		if err != nil {
			return err
		}
	}
	return nil
}
