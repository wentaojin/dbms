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

type StructCompareTask struct {
	Ctx         context.Context
	Task        *task.Task
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.StructCompareParam
}

func (dmt *StructCompareTask) Start() error {
	startTime := time.Now()
	logger.Info("struct compare task get schema route",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(dmt.Ctx, &rule.SchemaRouteRule{TaskName: dmt.Task.TaskName})
	if err != nil {
		return err
	}

	dbTypeSli := stringutil.StringSplit(dmt.Task.TaskFlow, constant.StringSeparatorAite)
	buildInDatatypeRulesS, err := model.GetIBuildInDatatypeRuleRW().QueryBuildInDatatypeRule(dmt.Ctx, dbTypeSli[0], dbTypeSli[1])
	if err != nil {
		return err
	}
	buildInDefaultValueRulesS, err := model.GetBuildInDefaultValueRuleRW().QueryBuildInDefaultValueRule(dmt.Ctx, dbTypeSli[0], dbTypeSli[1])
	if err != nil {
		return err
	}
	buildInDatatypeRulesT, err := model.GetIBuildInDatatypeRuleRW().QueryBuildInDatatypeRule(dmt.Ctx, dbTypeSli[1], dbTypeSli[0])
	if err != nil {
		return err
	}
	buildInDefaultValueRulesT, err := model.GetBuildInDefaultValueRuleRW().QueryBuildInDefaultValueRule(dmt.Ctx, dbTypeSli[1], dbTypeSli[0])
	if err != nil {
		return err
	}

	// interval 10 seconds print
	progress := processor.NewProgresser(dmt.Ctx)
	defer progress.Close()

	progress.Init(
		processor.WithTaskName(dmt.Task.TaskName),
		processor.WithTaskMode(dmt.Task.TaskMode),
		processor.WithTaskFlow(dmt.Task.TaskFlow))

	logger.Info("struct compare task init database connection",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
	var (
		databaseS, databaseT database.IDatabase
	)
	switch dmt.Task.TaskFlow {
	case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
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

		logger.Info("struct compare task inspect migrate task",
			zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
		_, err = processor.InspectOracleMigrateTask(dmt.Task.TaskName, dmt.Task.TaskFlow, dmt.Task.TaskMode, databaseS, stringutil.StringUpper(dmt.DatasourceS.ConnectCharset), stringutil.StringUpper(dmt.DatasourceT.ConnectCharset))
		if err != nil {
			return err
		}
	case constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
		databaseS, err = database.NewDatabase(dmt.Ctx, dmt.DatasourceS, "", int64(dmt.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()
		databaseT, err = database.NewDatabase(dmt.Ctx, dmt.DatasourceT, "", int64(dmt.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseT.Close()

		logger.Info("struct compare task inspect migrate task",
			zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow))
		_, err = processor.InspectPostgresMigrateTask(dmt.Task.TaskName, dmt.Task.TaskFlow, dmt.Task.TaskMode, databaseS, stringutil.StringUpper(dmt.DatasourceS.ConnectCharset), stringutil.StringUpper(dmt.DatasourceT.ConnectCharset))
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema [%s] isn't support, please contact author or reselect", dmt.Task.TaskName, dmt.Task.TaskMode, dmt.Task.TaskFlow, schemaRoute.SchemaNameS)
	}

	err = database.IDatabaseRun(&processor.StructCompareTask{
		Ctx:                       dmt.Ctx,
		Task:                      dmt.Task,
		DBTypeS:                   dbTypeSli[0],
		DBTypeT:                   dbTypeSli[1],
		DatabaseS:                 databaseS,
		DatabaseT:                 databaseT,
		DBCharsetS:                stringutil.StringUpper(dmt.DatasourceS.ConnectCharset),
		DBCharsetT:                stringutil.StringUpper(dmt.DatasourceT.ConnectCharset),
		SchemaNameS:               schemaRoute.SchemaNameS,
		SchemaNameT:               schemaRoute.SchemaNameT,
		StartTime:                 startTime,
		TaskParams:                dmt.TaskParams,
		BuildInDatatypeRulesS:     buildInDatatypeRulesS,
		BuildInDefaultValueRulesS: buildInDefaultValueRulesS,
		BuildInDatatypeRulesT:     buildInDatatypeRulesT,
		BuildInDefaultValueRulesT: buildInDefaultValueRulesT,
		ReadyInit:                 make(chan bool, 1),
		Progress:                  progress,
	})
	if err != nil {
		return err
	}
	logger.Info("struct compare task finished",
		zap.String("task_name", dmt.Task.TaskName), zap.String("task_mode", dmt.Task.TaskMode), zap.String("task_flow", dmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}
