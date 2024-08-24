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

type DataCompareTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.DataCompareParam
}

func (dmt *DataCompareTask) Start() error {
	schemaTaskTime := time.Now()
	logger.Info("data compare task get schema route",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(dmt.Ctx, &rule.SchemaRouteRule{TaskName: dmt.Task.TaskName})
	if err != nil {
		return err
	}

	logger.Info("data compare task init database connection",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))

	var (
		databaseS, databaseT database.IDatabase
	)
	switch dmt.Task.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
		databaseS, err = database.NewDatabase(dmt.Ctx, dmt.DatasourceS, schemaRoute.SchemaNameS, int64(dmt.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()
		databaseT, err = database.NewDatabase(dmt.Ctx, dmt.DatasourceT, "", int64(dmt.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseT.Close()

		logger.Info("data compare task inspect task",
			zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
		_, err = processor.InspectOracleMigrateTask(dmt.Task.TaskName, dmt.Task.TaskFlow, dmt.Task.TaskMode, databaseS, stringutil.StringUpper(dmt.DatasourceS.ConnectCharset), stringutil.StringUpper(dmt.DatasourceT.ConnectCharset))
		if err != nil {
			return err
		}
	case constant.TaskFlowTiDBToOracle:
		databaseS, err = database.NewDatabase(dmt.Ctx, dmt.DatasourceS, "", int64(dmt.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()

		databaseT, err = database.NewDatabase(dmt.Ctx, dmt.DatasourceT, schemaRoute.SchemaNameT, int64(dmt.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseT.Close()

		logger.Info("data compare task inspect task",
			zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
		_, err = processor.InspectOracleMigrateTask(dmt.Task.TaskName, dmt.Task.TaskFlow, dmt.Task.TaskMode, databaseT, stringutil.StringUpper(dmt.DatasourceT.ConnectCharset), stringutil.StringUpper(dmt.DatasourceS.ConnectCharset))
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema_name_s [%s] isn't support, please contact author or reselect", dmt.Task.TaskName, dmt.Task.TaskMode, dmt.Task.TaskFlow, schemaRoute.SchemaNameS)
	}

	err = database.IDatabaseRun(&processor.DataCompareTask{
		Ctx:         dmt.Ctx,
		Task:        dmt.Task,
		DatabaseS:   databaseS,
		DatabaseT:   databaseT,
		SchemaNameS: schemaRoute.SchemaNameS,
		DBCharsetS:  stringutil.StringUpper(dmt.DatasourceS.ConnectCharset),
		DBCharsetT:  stringutil.StringUpper(dmt.DatasourceT.ConnectCharset),
		TaskParams:  dmt.TaskParams,
		WaiterC:     make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
		ResumeC:     make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
	})
	if err != nil {
		return err
	}

	logger.Info("data compare task",
		zap.String("task_name", dmt.Task.TaskName),
		zap.String("task_mode", dmt.Task.TaskMode),
		zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}
