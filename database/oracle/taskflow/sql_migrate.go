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
	"github.com/wentaojin/dbms/database/processor"
	"strconv"
	"strings"
	"time"

	"github.com/wentaojin/dbms/model/rule"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/errconcurrent"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

type SqlMigrateTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.SqlMigrateParam
}

func (smt *SqlMigrateTask) Start() error {
	schemaStartTime := time.Now()
	logger.Info("sql migrate task init database connection",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow))
	databaseS, err := database.NewDatabase(smt.Ctx, smt.DatasourceS, "", int64(smt.TaskParams.CallTimeout))
	if err != nil {
		return err
	}
	defer databaseS.Close()

	databaseT, err := database.NewDatabase(smt.Ctx, smt.DatasourceT, "", int64(smt.TaskParams.CallTimeout))
	if err != nil {
		return err
	}
	defer databaseT.Close()

	logger.Info("sql migrate task inspect migrate task",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow))
	_, _, err = processor.InspectOracleMigrateTask(smt.Task.TaskName, smt.Task.TaskFlow, smt.Task.TaskMode, databaseS, stringutil.StringUpper(smt.DatasourceS.ConnectCharset), stringutil.StringUpper(smt.DatasourceT.ConnectCharset))
	if err != nil {
		return err
	}

	logger.Info("sql migrate task init migrate task",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow))
	err = smt.InitSqlMigrateTask(databaseS)
	if err != nil {
		return err
	}

	var migrateTasks []*task.SqlMigrateTask
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
			select {
			case <-smt.Ctx.Done():
				return nil
			default:
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
								stringutil.StringLower(constant.TaskModeSqlMigrate),
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

					sqlStr := processor.GenMYSQLCompatibleDatabasePrepareStmt(dt.SchemaNameT, dt.TableNameT, smt.TaskParams.SqlHintT, dt.ColumnDetailT, int(smt.TaskParams.BatchSize), true)

					stmt, err := databaseT.PrepareContext(smt.Ctx, sqlStr)
					if err != nil {
						return err
					}
					defer stmt.Close()

					err = database.IDataMigrateProcess(&processor.SqlMigrateRow{
						Ctx:           smt.Ctx,
						TaskMode:      smt.Task.TaskMode,
						TaskFlow:      smt.Task.TaskFlow,
						Smt:           dt,
						DatabaseS:     databaseS,
						DatabaseT:     databaseT,
						DatabaseTStmt: stmt,
						DBCharsetS:    constant.MigrateOracleCharsetStringConvertMapping[stringutil.StringUpper(smt.DatasourceS.ConnectCharset)],
						DBCharsetT:    stringutil.StringUpper(smt.DatasourceT.ConnectCharset),
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
								stringutil.StringLower(constant.TaskModeSqlMigrate),
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
			}
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
						stringutil.StringLower(constant.TaskModeSqlMigrate),
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

	schemaEndTime := time.Now()
	_, err = model.GetISqlMigrateSummaryRW().UpdateSqlMigrateSummary(smt.Ctx, &task.SqlMigrateSummary{
		TaskName: smt.Task.TaskName,
	}, map[string]interface{}{
		"Duration": fmt.Sprintf("%f", schemaEndTime.Sub(schemaStartTime).Seconds()),
	})
	if err != nil {
		return err
	}
	logger.Info("sql migrate task",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow),
		zap.String("cost", schemaEndTime.Sub(schemaStartTime).String()))
	return nil
}

func (smt *SqlMigrateTask) InitSqlMigrateTask(databaseS database.IDatabase) error {
	var (
		initFlags *task.Task
		err       error
	)
	err = model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
		initFlags, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: smt.Task.TaskName})
		if err != nil {
			return err
		}
		if strings.EqualFold(initFlags.TaskInit, constant.TaskInitStatusNotFinished) {
			err = model.GetISqlMigrateTaskRW().DeleteSqlMigrateTaskName(txnCtx, []string{smt.Task.TaskName})
			if err != nil {
				return err
			}
			err = model.GetISqlMigrateSummaryRW().DeleteSqlMigrateSummaryName(txnCtx, []string{smt.Task.TaskName})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// repeatedDoneInfos used for store the sql_migrate_task information has be finished, avoid repeated initialization
	migrateDoneInfos, err := model.GetISqlMigrateTaskRW().FindSqlMigrateTaskByTaskStatus(smt.Ctx, &task.SqlMigrateTask{TaskName: smt.Task.TaskName, TaskStatus: constant.TaskDatabaseStatusSuccess})
	if err != nil {
		return err
	}

	repeatedDoneInfos := make(map[string]struct{})
	for _, m := range migrateDoneInfos {
		repeatedDoneInfos[stringutil.StringBuilder(m.TaskName, m.SchemaNameT, m.TableNameT, m.SqlQueryS)] = struct{}{}
	}

	var globalScn string

	globalScnS, err := databaseS.GetDatabaseConsistentPos()
	if err != nil {
		return err
	}

	globalScn = strconv.FormatUint(globalScnS, 10)

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
	for _, s := range migrateSqlRules {
		if _, ok := repeatedDoneInfos[stringutil.StringBuilder(s.TaskName, s.SchemaNameT, s.TableNameT, s.SqlQueryS)]; ok {
			continue
		}

		columnRouteRule := make(map[string]string)
		err = stringutil.UnmarshalJSON([]byte(s.ColumnRouteRule), &columnRouteRule)
		if err != nil {
			return err
		}
		dataRule := &processor.SqlMigrateRule{
			Ctx:             smt.Ctx,
			TaskName:        smt.Task.TaskName,
			TaskMode:        smt.Task.TaskMode,
			TaskFlow:        smt.Task.TaskFlow,
			SchemaNameT:     s.SchemaNameT,
			TableNameT:      s.TableNameT,
			SqlHintT:        s.SqlHintT,
			GlobalSqlHintT:  smt.TaskParams.SqlHintT,
			DatabaseS:       databaseS,
			DBCharsetS:      smt.DatasourceS.ConnectCharset,
			SqlQueryS:       s.SqlQueryS,
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
		_, err = model.GetITaskRW().UpdateTask(txnCtx, &task.Task{TaskName: smt.Task.TaskName}, map[string]interface{}{"TaskInit": constant.TaskInitStatusFinished})
		if err != nil {
			return err
		}
		err = model.GetISqlMigrateTaskRW().CreateInBatchSqlMigrateTask(txnCtx, sqlMigrateTasks, constant.DefaultRecordCreateBatchSize)
		if err != nil {
			return err
		}
		for _, r := range migrateSqlRuleGroupResults {
			_, err = model.GetISqlMigrateSummaryRW().CreateSqlMigrateSummary(txnCtx, &task.SqlMigrateSummary{
				TaskName:  smt.Task.TaskName,
				SqlTotals: r.RowTotals,
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
