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
	schemaTaskTime := time.Now()
	logger.Info("sql migrate task init database connection",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow))
	databaseS, err := database.NewDatabase(smt.Ctx, smt.DatasourceS, "")
	if err != nil {
		return err
	}
	databaseT, err := database.NewDatabase(smt.Ctx, smt.DatasourceT, "")
	if err != nil {
		return err
	}

	logger.Info("sql migrate task inspect migrate task",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow))
	_, err = InspectMigrateTask(databaseS, stringutil.StringUpper(smt.DatasourceS.ConnectCharset), stringutil.StringUpper(smt.DatasourceT.ConnectCharset))
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
		migrateTasks = append(migrateTasks, migrateFailedTasks...)
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
	for _, t := range migrateTasks {
		g.Go(t, func(t interface{}) error {
			select {
			case <-smt.Ctx.Done():
				return nil
			default:
				dt := t.(*task.SqlMigrateTask)
				switch {
				case strings.EqualFold(smt.Task.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(smt.Task.TaskFlow, constant.TaskFlowOracleToMySQL):
					errW := model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetISqlMigrateTaskRW().UpdateSqlMigrateTask(txnCtx,
							&task.SqlMigrateTask{TaskName: dt.TaskName, SchemaNameT: dt.SchemaNameT, TableNameT: dt.TableNameT},
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

					sqlStr := GenMYSQLCompatibleDatabasePrepareStmt(dt.SchemaNameT, dt.TableNameT, dt.ColumnDetailT, int(smt.TaskParams.BatchSize), true)

					stmt, err := databaseT.PrepareContext(smt.Ctx, sqlStr)
					if err != nil {
						return err
					}
					defer stmt.Close()

					readChan := make(chan []interface{}, constant.DefaultMigrateTaskQueueSize)
					writeChan := make(chan []interface{}, constant.DefaultMigrateTaskQueueSize)
					err = database.IDataMigrateProcess(&SqlMigrateRow{
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
						SafeMode:      true,
						ReadChan:      readChan,
						WriteChan:     writeChan,
					})
					if err != nil {
						return err
					}

					errW = model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetISqlMigrateTaskRW().UpdateSqlMigrateTask(txnCtx,
							&task.SqlMigrateTask{TaskName: dt.TaskName, SchemaNameT: dt.SchemaNameT, TableNameT: dt.TableNameT},
							map[string]interface{}{
								"TaskStatus": constant.TaskDatabaseStatusSuccess,
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
			logger.Warn("sql migrate task prcess sql",
				zap.String("task_name", smt.Task.TaskName),
				zap.String("task_mode", smt.Task.TaskMode),
				zap.String("task_flow", smt.Task.TaskFlow),
				zap.String("schema_name_t", esmt.SchemaNameT),
				zap.String("table_name_t", esmt.TableNameT),
				zap.Error(r.Err))

			errW := model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
				_, err = model.GetISqlMigrateTaskRW().UpdateSqlMigrateTask(txnCtx,
					&task.SqlMigrateTask{TaskName: esmt.TaskName, SchemaNameT: esmt.SchemaNameT, TableNameT: esmt.TableNameT},
					map[string]interface{}{
						"TaskStatus":  constant.TaskDatabaseStatusFailed,
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
	logger.Info("sql migrate task",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}

func (smt *SqlMigrateTask) InitSqlMigrateTask(databaseS database.IDatabase) error {
	// repeatedDoneInfos used for store the sql_migrate_task information has be finished, avoid repeated initialization
	migrateDoneInfos, err := model.GetISqlMigrateTaskRW().FindSqlMigrateTaskByTaskStatus(smt.Ctx, &task.SqlMigrateTask{TaskName: smt.Task.TaskName, TaskStatus: constant.TaskDatabaseStatusSuccess})
	if err != nil {
		return err
	}

	repeatedDoneInfos := make(map[string]struct{})
	for _, m := range migrateDoneInfos {
		repeatedDoneInfos[stringutil.StringBuilder(m.TaskName, m.SchemaNameT, m.TableNameT)] = struct{}{}
	}

	globalScn, err := databaseS.GetDatabaseCurrentSCN()
	if err != nil {
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
	for _, s := range migrateSqlRules {
		if _, ok := repeatedDoneInfos[stringutil.StringBuilder(s.TaskName, s.SchemaNameT, s.TableNameT)]; ok {
			continue
		}

		columnRouteRule := make(map[string]string)
		err = stringutil.UnmarshalJSON([]byte(s.ColumnRouteRule), columnRouteRule)
		if err != nil {
			return err
		}
		dataRule := &SqlMigrateRule{
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
			GlobalScnS:      globalScn,
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
		err = model.GetISqlMigrateTaskRW().CreateInBatchSqlMigrateTask(txnCtx, sqlMigrateTasks, constant.DefaultRecordCreateBatchSize)
		if err != nil {
			return err
		}
		for _, r := range migrateSqlRuleGroupResults {
			_, err = model.GetISqlMigrateSummaryRW().CreateSqlMigrateSummary(txnCtx, &task.SqlMigrateSummary{
				TaskName:    smt.Task.TaskName,
				SchemaNameT: r.SchemaNameT,
				TableNameT:  r.TableNameT,
				SqlTotals:   r.RowTotals,
				SqlSuccess:  0,
				SqlFails:    0,
				Duration:    0,
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

	err = databaseS.Close()
	if err != nil {
		return err
	}
	return nil
}
