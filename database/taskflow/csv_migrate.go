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

type CsvMigrateTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.CsvMigrateParam
}

func (cmt *CsvMigrateTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("data migrate task get schema route",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(cmt.Ctx, &rule.SchemaRouteRule{TaskName: cmt.Task.TaskName})
	if err != nil {
		return err
	}

	logger.Info("data migrate task init database connection",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))

	var (
		databaseS, databaseT database.IDatabase
	)
	switch cmt.Task.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
		databaseS, err = database.NewDatabase(cmt.Ctx, cmt.DatasourceS, schemaRoute.SchemaNameS, int64(cmt.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()
		databaseT, err = database.NewDatabase(cmt.Ctx, cmt.DatasourceT, "", int64(cmt.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseT.Close()

		logger.Info("data migrate task inspect migrate task",
			zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow))
		_, err = processor.InspectOracleMigrateTask(cmt.Task.TaskName, cmt.Task.TaskFlow, cmt.Task.TaskMode, databaseS, stringutil.StringUpper(cmt.DatasourceS.ConnectCharset), stringutil.StringUpper(cmt.DatasourceT.ConnectCharset))
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema_name_s [%s] isn't support, please contact author or reselect", cmt.Task.TaskName, cmt.Task.TaskMode, cmt.Task.TaskFlow, schemaRoute.SchemaNameS)
	}

	dbVersionS, err := databaseS.GetDatabaseVersion()
	if err != nil {
		return err
	}
	dbRoles, err := databaseS.GetDatabaseRole()
	if err != nil {
		return err
	}

	err = database.IDatabaseRun(cmt.Ctx, &processor.DataMigrateTask{
		Ctx:                  cmt.Ctx,
		Task:                 cmt.Task,
		DBRoleS:              dbRoles,
		DBVersionS:           dbVersionS,
		DBCharsetS:           stringutil.StringUpper(cmt.DatasourceS.ConnectCharset),
		DBCharsetT:           stringutil.StringUpper(cmt.DatasourceT.ConnectCharset),
		DatabaseS:            databaseS,
		DatabaseT:            databaseT,
		SchemaNameS:          schemaRoute.SchemaNameS,
		TableThread:          cmt.TaskParams.TableThread,
		GlobalSqlHintS:       cmt.TaskParams.SqlHintS,
		EnableCheckpoint:     cmt.TaskParams.EnableCheckpoint,
		EnableConsistentRead: cmt.TaskParams.EnableConsistentRead,
		ChunkSize:            cmt.TaskParams.ChunkSize,
		BatchSize:            cmt.TaskParams.BatchSize,
		WriteThread:          cmt.TaskParams.WriteThread,
		CallTimeout:          cmt.TaskParams.CallTimeout,
		SqlThreadS:           cmt.TaskParams.SqlThreadS,
		CsvParams:            cmt.TaskParams,
		WaiterC:              make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
		ResumeC:              make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
	})
	if err != nil {
		return err
	}

	logger.Info("data migrate task",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}
