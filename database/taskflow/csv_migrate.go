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

type CsvMigrateTask struct {
	Ctx           context.Context
	Task          *task.Task
	DatasourceS   *datasource.Datasource
	DatasourceT   *datasource.Datasource
	MigrateParams *pb.CsvMigrateParam
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
	case constant.TaskFlowOracleToTiDB:
		databaseS, err = database.NewDatabase(cmt.Ctx, cmt.DatasourceS, schemaRoute.SchemaNameS, int64(cmt.MigrateParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()
		databaseT, err = database.NewDatabase(cmt.Ctx, cmt.DatasourceT, "", int64(cmt.MigrateParams.CallTimeout))
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

		if cmt.MigrateParams.EnableImportFeature {
			dbVersionT, err := databaseT.GetDatabaseVersion()
			if err != nil {
				return err
			}
			if stringutil.VersionOrdinal(dbVersionT) < stringutil.VersionOrdinal(constant.TIDBDatabaseImportIntoSupportVersion) {
				return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] database [tidb] version [%v] isn't support, require version [>=%s], please setting enable-import-feature = false and retry", cmt.Task.TaskName, cmt.Task.TaskMode, cmt.Task.TaskFlow, dbVersionT, constant.TIDBDatabaseImportIntoSupportVersion)
			}
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
		if err := databaseS.Transaction(cmt.Ctx, &sql.TxOptions{}, []func(ctx context.Context, tx *sql.Tx) error{
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
		err = database.IDatabaseRun(&processor.CsvMigrateTask{
			Ctx:             cmt.Ctx,
			Task:            cmt.Task,
			DBRoleS:         dbRoles,
			DBVersionS:      dbVersionS,
			DBCharsetS:      stringutil.StringUpper(cmt.DatasourceS.ConnectCharset),
			DBCharsetT:      stringutil.StringUpper(cmt.DatasourceT.ConnectCharset),
			DatabaseS:       databaseS,
			DatabaseT:       databaseT,
			SchemaNameS:     schemaRoute.SchemaNameS,
			SchemaNameT:     schemaRoute.SchemaNameT,
			GlobalSnapshotS: globalScnS,
			CsvParams:       cmt.MigrateParams,
			WaiterC:         make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
			ResumeC:         make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
		})
		if err != nil {
			return err
		}

	case constant.TaskFlowOracleToMySQL:
		databaseS, err = database.NewDatabase(cmt.Ctx, cmt.DatasourceS, schemaRoute.SchemaNameS, int64(cmt.MigrateParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()
		databaseT, err = database.NewDatabase(cmt.Ctx, cmt.DatasourceT, "", int64(cmt.MigrateParams.CallTimeout))
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

		if cmt.MigrateParams.EnableImportFeature {
			return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] database [mysql] isn't support, please setting enable-import-feature = false and retry", cmt.Task.TaskName, cmt.Task.TaskMode, cmt.Task.TaskFlow)
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
		if err := databaseS.Transaction(cmt.Ctx, &sql.TxOptions{}, []func(ctx context.Context, tx *sql.Tx) error{
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
		err = database.IDatabaseRun(&processor.CsvMigrateTask{
			Ctx:             cmt.Ctx,
			Task:            cmt.Task,
			DBRoleS:         dbRoles,
			DBVersionS:      dbVersionS,
			DBCharsetS:      stringutil.StringUpper(cmt.DatasourceS.ConnectCharset),
			DBCharsetT:      stringutil.StringUpper(cmt.DatasourceT.ConnectCharset),
			DatabaseS:       databaseS,
			DatabaseT:       databaseT,
			SchemaNameS:     schemaRoute.SchemaNameS,
			SchemaNameT:     schemaRoute.SchemaNameT,
			GlobalSnapshotS: globalScnS,
			CsvParams:       cmt.MigrateParams,
			WaiterC:         make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
			ResumeC:         make(chan *processor.WaitingRecs, constant.DefaultMigrateTaskQueueSize),
		})
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema_name_s [%s] isn't support, please contact author or reselect", cmt.Task.TaskName, cmt.Task.TaskMode, cmt.Task.TaskFlow, schemaRoute.SchemaNameS)
	}

	logger.Info("data migrate task",
		zap.String("task_name", cmt.Task.TaskName), zap.String("task_mode", cmt.Task.TaskMode), zap.String("task_flow", cmt.Task.TaskFlow),
		zap.String("cost", time.Now().Sub(schemaTaskTime).String()))
	return nil
}
