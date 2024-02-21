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
	TaskName    string
	TaskFlow    string
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.DataMigrateParam
}

func (smt *SqlMigrateTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("sql migrate task get schema route",
		zap.String("task_name", smt.TaskName),
		zap.String("task_flow", smt.TaskFlow))
	taskInfo, err := model.GetITaskRW().GetTask(smt.Ctx, &task.Task{TaskName: smt.TaskName})
	if err != nil {
		return err
	}

	logger.Info("sql migrate task init connection",
		zap.String("task_name", smt.TaskName),
		zap.String("task_flow", smt.TaskFlow))
	databaseS, err := database.NewDatabase(smt.Ctx, smt.DatasourceS, "")
	if err != nil {
		return err
	}
	databaseT, err := database.NewDatabase(smt.Ctx, smt.DatasourceT, "")
	if err != nil {
		return err
	}

	logger.Info("sql migrate task inspect task",
		zap.String("task_name", smt.TaskName),
		zap.String("task_flow", smt.TaskFlow))
	_, err = InspectMigrateTask(databaseS, stringutil.StringUpper(smt.DatasourceS.ConnectCharset), stringutil.StringUpper(smt.DatasourceT.ConnectCharset))
	if err != nil {
		return err
	}

	logger.Info("sql migrate task init task",
		zap.String("task_name", smt.TaskName),
		zap.String("task_flow", smt.TaskFlow))
	err = smt.InitSqlMigrateTask(taskInfo, databaseS)
	if err != nil {
		return err
	}

	var migrateTasks []*task.SqlMigrateTask
	err = model.Transaction(smt.Ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateTasks, err = model.GetISqlMigrateTaskRW().FindSqlMigrateTaskByTaskStatus(txnCtx,
			&task.SqlMigrateTask{
				TaskName:   smt.TaskName,
				TaskStatus: constant.TaskDatabaseStatusWaiting,
			})
		if err != nil {
			return err
		}
		migrateFailedTasks, err := model.GetISqlMigrateTaskRW().FindSqlMigrateTaskByTaskStatus(txnCtx,
			&task.SqlMigrateTask{
				TaskName:   smt.TaskName,
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
		zap.String("task_name", smt.TaskName),
		zap.String("task_flow", smt.TaskFlow))

	g := errconcurrent.NewGroup()
	g.SetLimit(int(smt.TaskParams.SqlThreadS))
	for _, t := range migrateTasks {
		select {
		case <-smt.Ctx.Done():
			goto BreakLoop
		default:
			g.Go(t, func(t interface{}) error {
				dt := t.(*task.SqlMigrateTask)
				switch {
				case strings.EqualFold(smt.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(smt.TaskFlow, constant.TaskFlowOracleToMySQL):
					sqlStr := GenMYSQLCompatibleDatabasePrepareStmt(dt.SchemaNameT, dt.TableNameT, dt.ColumnDetailT, int(smt.TaskParams.BatchSize), true)

					stmt, err := databaseT.PrepareContext(smt.Ctx, sqlStr)
					if err != nil {
						return err
					}
					defer stmt.Close()

					readQueue := make(chan []map[string]interface{}, constant.DefaultMigrateTaskQueueSize)
					writeQueue := make(chan []interface{}, constant.DefaultMigrateTaskQueueSize)

					err = database.IDataMigrateProcess(&SqlMigrateRow{
						Ctx:           smt.Ctx,
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
						ReadQueue:     readQueue,
						WriteQueue:    writeQueue,
					})
					if err != nil {
						return err
					}

				default:
					return fmt.Errorf("oracle current task [%s] taskflow [%s] column rule isn't support, please contact author", smt.TaskName, smt.TaskFlow)
				}
				return nil
			})
		}
	}
BreakLoop:
	logger.Warn("sql migrate task",
		zap.String("task_name", smt.TaskName),
		zap.String("task_flow", smt.TaskFlow),
		zap.String("warn", "the task sql migrate has be canceled"))

	for _, r := range g.Wait() {
		if r.Err != nil {
			esmt := r.Task.(*task.SqlMigrateTask)
			logger.Warn("sql migrate task",
				zap.String("task_name", esmt.TaskName),
				zap.String("task_flow", esmt.TaskFlow),
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
						constant.TaskModeSqlMigrate,
						esmt.TaskName,
						esmt.TaskFlow,
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
		zap.String("task_name", smt.TaskName),
		zap.String("task_flow", smt.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}

func (smt *SqlMigrateTask) InitSqlMigrateTask(taskInfo *task.Task, databaseS database.IDatabase) error {
	// repeatedDoneInfos used for store the sql_migrate_task information has be finished, avoid repeated initialization
	migrateDoneInfos, err := model.GetISqlMigrateTaskRW().FindSqlMigrateTaskByTaskStatus(smt.Ctx, &task.SqlMigrateTask{TaskName: smt.TaskName, TaskStatus: constant.TaskDatabaseStatusSuccess})
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
		zap.String("task_name", smt.TaskName),
		zap.String("task_flow", smt.TaskFlow))

	migrateSqlRules, err := model.GetIMigrateSqlRuleRW().FindSqlRouteRule(smt.Ctx, &rule.SqlRouteRule{
		TaskName: smt.TaskName,
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
			TaskMode:        taskInfo.TaskMode,
			TaskName:        smt.TaskName,
			TaskFlow:        smt.TaskFlow,
			SchemaNameT:     s.SchemaNameT,
			TableNameT:      s.TableNameT,
			SqlHintT:        s.SqlHintT,
			GlobalSqlHintT:  smt.TaskParams.SqlHintT,
			DatabaseS:       databaseS,
			DBCharsetS:      smt.DatasourceS.ConnectCharset,
			SqlQueryS:       s.SqlQueryS,
			ColumnRouteRule: columnRouteRule,
			CaseFieldRuleS:  taskInfo.CaseFieldRuleS,
			CaseFieldRuleT:  taskInfo.CaseFieldRuleT,
		}
		attrs, err := database.ISqlMigrateAttributesRule(dataRule)
		if err != nil {
			return err
		}
		sqlMigrateTasks = append(sqlMigrateTasks, &task.SqlMigrateTask{
			TaskName:        smt.TaskName,
			TaskFlow:        smt.TaskFlow,
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

	migrateSqlRuleGroupResults, err := model.GetIMigrateSqlRuleRW().FindSqlRouteRuleGroupBySchemaTable(smt.Ctx)
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
				TaskName:    smt.TaskName,
				TaskFlow:    smt.TaskFlow,
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
