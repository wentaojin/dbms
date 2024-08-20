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
	"time"

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
	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema_name_s [%s] isn't support, please contact author or reselect", stm.Task.TaskName, stm.Task.TaskMode, stm.Task.TaskFlow, schemaRoute.SchemaNameS)
	}

	dbVersionS, err := databaseS.GetDatabaseVersion()
	if err != nil {
		return err
	}
	dbRoles, err := databaseS.GetDatabaseRole()
	if err != nil {
		return err
	}

	err = database.IDatabaseRun(stm.Ctx, &processor.DataMigrateTask{
		Ctx:                  stm.Ctx,
		Task:                 stm.Task,
		DBRoleS:              dbRoles,
		DBVersionS:           dbVersionS,
		DBCharsetS:           stringutil.StringUpper(stm.DatasourceS.ConnectCharset),
		DBCharsetT:           stringutil.StringUpper(stm.DatasourceT.ConnectCharset),
		DatabaseS:            databaseS,
		DatabaseT:            databaseT,
		SchemaNameS:          schemaRoute.SchemaNameS,
		TableThread:          stm.TaskParams.TableThread,
		GlobalSqlHintS:       stm.TaskParams.SqlHintS,
		GlobalSqlHintT:       stm.TaskParams.SqlHintT,
		EnableCheckpoint:     stm.TaskParams.EnableCheckpoint,
		EnableConsistentRead: stm.TaskParams.EnableConsistentRead,
		ChunkSize:            stm.TaskParams.ChunkSize,
		BatchSize:            stm.TaskParams.BatchSize,
		WriteThread:          stm.TaskParams.WriteThread,
		CallTimeout:          stm.TaskParams.CallTimeout,
		SqlThreadS:           stm.TaskParams.SqlThreadS,
		StmtParams:           stm.TaskParams,
		WaiterC:              make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
		ResumeC:              make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
	})
	if err != nil {
		return err
	}

	logger.Info("data migrate task",
		zap.String("task_name", stm.Task.TaskName), zap.String("task_mode", stm.Task.TaskMode), zap.String("task_flow", stm.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}