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
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/database/processor"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
	"time"
)

type DataScanTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.DataScanParam
}

func (dst *DataScanTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("data scan task get schema route",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))
	schemaNameRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(dst.Ctx, &rule.SchemaRouteRule{TaskName: dst.Task.TaskName})
	if err != nil {
		return err
	}
	schemaNameS := schemaNameRoute.SchemaNameS

	logger.Info("data scan task init database connection",
		zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))
	var (
		databaseS database.IDatabase
	)
	switch dst.Task.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
		databaseS, err = database.NewDatabase(dst.Ctx, dst.DatasourceS, schemaNameS, int64(dst.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()

		logger.Info("data scan task inspect migrate task",
			zap.String("task_name", dst.Task.TaskName), zap.String("task_mode", dst.Task.TaskMode), zap.String("task_flow", dst.Task.TaskFlow))

		_, err = processor.InspectOracleMigrateTask(dst.Task.TaskName, dst.Task.TaskFlow, dst.Task.TaskMode, databaseS, stringutil.StringUpper(dst.DatasourceS.ConnectCharset), stringutil.StringUpper(dst.DatasourceT.ConnectCharset))
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema_name_s [%s] isn't support, please contact author or reselect", dst.Task.TaskName, dst.Task.TaskMode, dst.Task.TaskFlow, schemaNameS)
	}

	dbVersionS, err := databaseS.GetDatabaseVersion()
	if err != nil {
		return err
	}
	dbRoles, err := databaseS.GetDatabaseRole()
	if err != nil {
		return err
	}
	err = database.IDatabaseRun(&processor.DataScanTask{
		Ctx:         dst.Ctx,
		Task:        dst.Task,
		DatabaseS:   databaseS,
		SchemaNameS: schemaNameS,
		DBVersionS:  dbVersionS,
		DBRoleS:     dbRoles,
		DBCharsetS:  stringutil.StringUpper(dst.DatasourceS.ConnectCharset),
		TaskParams:  dst.TaskParams,
		WaiterC:     make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
		ResumeC:     make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
	})
	if err != nil {
		return err
	}
	logger.Info("data scan task",
		zap.String("task_name", dst.Task.TaskName),
		zap.String("task_mode", dst.Task.TaskMode),
		zap.String("task_flow", dst.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}
