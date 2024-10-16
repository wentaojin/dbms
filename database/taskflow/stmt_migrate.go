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
	"database/sql"
	"fmt"
	"time"

	"github.com/wentaojin/dbms/database/processor"

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

type StmtMigrateTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.StatementMigrateParam

	WaiterC chan *processor.WaitingRecs
	ResumeC chan *processor.WaitingRecs
}

func (stm *StmtMigrateTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("stmt migrate task get schema route",
		zap.String("task_name", stm.Task.TaskName), zap.String("task_mode", stm.Task.TaskMode), zap.String("task_flow", stm.Task.TaskFlow))
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(stm.Ctx, &rule.SchemaRouteRule{TaskName: stm.Task.TaskName})
	if err != nil {
		return err
	}

	logger.Info("stmt migrate task init database connection",
		zap.String("task_name", stm.Task.TaskName), zap.String("task_mode", stm.Task.TaskMode), zap.String("task_flow", stm.Task.TaskFlow))

	var (
		databaseS, databaseT database.IDatabase
	)
	switch stm.Task.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
		databaseS, err = database.NewDatabase(stm.Ctx, stm.DatasourceS, schemaRoute.SchemaNameS, int64(stm.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()
		databaseT, err = database.NewDatabase(stm.Ctx, stm.DatasourceT, "", int64(stm.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseT.Close()

		logger.Info("stmt migrate task inspect migrate task",
			zap.String("task_name", stm.Task.TaskName), zap.String("task_mode", stm.Task.TaskMode), zap.String("task_flow", stm.Task.TaskFlow))
		_, err = processor.InspectOracleMigrateTask(stm.Task.TaskName, stm.Task.TaskFlow, stm.Task.TaskMode, databaseS,
			stringutil.StringUpper(stm.DatasourceS.ConnectCharset),
			stringutil.StringUpper(stm.DatasourceT.ConnectCharset))
		if err != nil {
			return err
		}
		dbVersionS, err := databaseS.GetDatabaseVersion()
		if err != nil {
			return err
		}
		dbRoles, err := databaseS.GetDatabaseRole()
		if err != nil {
			return err
		}

		var globalScnS string
		if err := databaseS.Transaction(stm.Ctx, &sql.TxOptions{}, []func(ctx context.Context, tx *sql.Tx) error{
			func(ctx context.Context, tx *sql.Tx) error {
				globalScnS, err = databaseS.GetDatabaseConsistentPos(ctx, tx)
				if err != nil {
					return err
				}
				return nil
			},
		}); err != nil {
			return err
		}
		err = database.IDatabaseRun(&processor.DataMigrateTask{
			Ctx:             stm.Ctx,
			Task:            stm.Task,
			DBRoleS:         dbRoles,
			DBVersionS:      dbVersionS,
			DBCharsetS:      stringutil.StringUpper(stm.DatasourceS.ConnectCharset),
			DBCharsetT:      stringutil.StringUpper(stm.DatasourceT.ConnectCharset),
			DatabaseS:       databaseS,
			DatabaseT:       databaseT,
			SchemaNameS:     schemaRoute.SchemaNameS,
			StmtParams:      stm.TaskParams,
			GlobalSnapshotS: globalScnS,
			WaiterC:         make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
			ResumeC:         make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
		})
		if err != nil {
			return err
		}
	case constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
		databaseS, err = database.NewDatabase(stm.Ctx, stm.DatasourceS, "", int64(stm.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()
		databaseT, err = database.NewDatabase(stm.Ctx, stm.DatasourceT, "", int64(stm.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseT.Close()

		logger.Info("stmt migrate task inspect migrate task",
			zap.String("task_name", stm.Task.TaskName), zap.String("task_mode", stm.Task.TaskMode), zap.String("task_flow", stm.Task.TaskFlow))
		_, err = processor.InspectPostgresMigrateTask(stm.Task.TaskName, stm.Task.TaskFlow, stm.Task.TaskMode, databaseS,
			stringutil.StringUpper(stm.DatasourceS.ConnectCharset),
			stringutil.StringUpper(stm.DatasourceT.ConnectCharset))
		if err != nil {
			return err
		}
		dbVersionS, err := databaseS.GetDatabaseVersion()
		if err != nil {
			return err
		}
		dbRoles, err := databaseS.GetDatabaseRole()
		if err != nil {
			return err
		}

		var (
			globalScnS string
			txn        *sql.Tx
		)
		if stm.TaskParams.EnableConsistentRead {
			txn, err = databaseS.BeginTxn(stm.Ctx, &sql.TxOptions{
				Isolation: sql.LevelRepeatableRead,
			})
			if err != nil {
				return err
			}
			globalScnS, err = databaseS.GetDatabaseConsistentPos(stm.Ctx, txn)
			if err != nil {
				return err
			}
		}

		err = database.IDatabaseRun(&processor.DataMigrateTask{
			Ctx:             stm.Ctx,
			Task:            stm.Task,
			DBRoleS:         dbRoles,
			DBVersionS:      dbVersionS,
			DBCharsetS:      stringutil.StringUpper(stm.DatasourceS.ConnectCharset),
			DBCharsetT:      stringutil.StringUpper(stm.DatasourceT.ConnectCharset),
			DatabaseS:       databaseS,
			DatabaseT:       databaseT,
			SchemaNameS:     schemaRoute.SchemaNameS,
			StmtParams:      stm.TaskParams,
			GlobalSnapshotS: globalScnS,
			WaiterC:         make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
			ResumeC:         make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
		})
		if err != nil {
			return err
		}
		if stm.TaskParams.EnableConsistentRead {
			if err = databaseS.CommitTxn(txn); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema_name_s [%s] isn't support, please contact author or reselect", stm.Task.TaskName, stm.Task.TaskMode, stm.Task.TaskFlow, schemaRoute.SchemaNameS)
	}

	logger.Info("data migrate task",
		zap.String("task_name", stm.Task.TaskName), zap.String("task_mode", stm.Task.TaskMode), zap.String("task_flow", stm.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}
