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
	"github.com/wentaojin/dbms/utils/constant"
	"time"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
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

	var (
		databaseS, databaseT database.IDatabase
		err                  error
	)
	switch smt.Task.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB:
		databaseS, err = database.NewDatabase(smt.Ctx, smt.DatasourceS, "", int64(smt.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()

		databaseT, err = database.NewDatabase(smt.Ctx, smt.DatasourceT, "", int64(smt.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseT.Close()

		logger.Info("sql migrate task inspect migrate task",
			zap.String("task_name", smt.Task.TaskName),
			zap.String("task_mode", smt.Task.TaskMode),
			zap.String("task_flow", smt.Task.TaskFlow))
		_, err = processor.InspectOracleMigrateTask(smt.Task.TaskName, smt.Task.TaskFlow, smt.Task.TaskMode, databaseS, stringutil.StringUpper(smt.DatasourceS.ConnectCharset), stringutil.StringUpper(smt.DatasourceT.ConnectCharset))
		if err != nil {
			return err
		}

		logger.Info("sql migrate task init migrate task",
			zap.String("task_name", smt.Task.TaskName),
			zap.String("task_mode", smt.Task.TaskMode),
			zap.String("task_flow", smt.Task.TaskFlow))

	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] isn't support, please contact author or reselect", smt.Task.TaskName, smt.Task.TaskMode, smt.Task.TaskFlow)
	}

	dbVersionS, err := databaseS.GetDatabaseVersion()
	if err != nil {
		return err
	}
	dbRoles, err := databaseS.GetDatabaseRole()
	if err != nil {
		return err
	}

	err = database.IDatabaseRun(smt.Ctx, &processor.SqlMigrateTask{
		Ctx:        smt.Ctx,
		Task:       smt.Task,
		DatabaseS:  databaseS,
		DatabaseT:  databaseT,
		DBRoleS:    dbRoles,
		DBCharsetS: stringutil.StringUpper(smt.DatasourceS.ConnectCharset),
		DBCharsetT: stringutil.StringUpper(smt.DatasourceT.ConnectCharset),
		DBVersionS: dbVersionS,
		TaskParams: smt.TaskParams,
	})
	if err != nil {
		return err
	}
	schemaEndTime := time.Now()
	logger.Info("sql migrate task",
		zap.String("task_name", smt.Task.TaskName),
		zap.String("task_mode", smt.Task.TaskMode),
		zap.String("task_flow", smt.Task.TaskFlow),
		zap.String("cost", schemaEndTime.Sub(schemaStartTime).String()))
	return nil
}
