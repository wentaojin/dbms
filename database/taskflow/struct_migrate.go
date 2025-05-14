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
	"github.com/wentaojin/dbms/model/datasource"

	"github.com/wentaojin/dbms/logger"
	"go.uber.org/zap"

	"github.com/wentaojin/dbms/database"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type StructMigrateTask struct {
	Ctx         context.Context
	Task        *task.Task
	SchemaNameS string
	SchemaNameT string
	DatasourceS *datasource.Datasource
	DatasourceT *datasource.Datasource
	TaskParams  *pb.StructMigrateParam
}

func (st *StructMigrateTask) Start() error {
	startTime := time.Now()

	if !st.TaskParams.EnableCheckpoint {
		err := model.GetISchemaMigrateTaskRW().DeleteSchemaMigrateTaskName(st.Ctx, []string{st.Task.TaskName})
		if err != nil {
			return err
		}
	}
	var (
		databaseS, databaseT database.IDatabase
		err                  error
	)

	logger.Info("init database connection",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow))

	switch st.Task.TaskFlow {
	case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
		databaseS, err = database.NewDatabase(st.Ctx, st.DatasourceS, st.SchemaNameS, int64(st.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()
		databaseT, err = database.NewDatabase(st.Ctx, st.DatasourceT, "", int64(st.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseT.Close()
	case constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
		databaseS, err = database.NewDatabase(st.Ctx, st.DatasourceS, "", int64(st.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseS.Close()
		databaseT, err = database.NewDatabase(st.Ctx, st.DatasourceT, "", int64(st.TaskParams.CallTimeout))
		if err != nil {
			return err
		}
		defer databaseT.Close()
	default:
		return fmt.Errorf("the task_name [%v] task_mode [%v] task_flow [%s] isn't support, please contact author or reselect", st.Task.TaskName, st.Task.TaskMode, st.Task.TaskFlow)
	}

	logger.Info("query task buildin rule",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow))

	dbTypeSli := stringutil.StringSplit(st.Task.TaskFlow, constant.StringSeparatorAite)

	dbTypeS := dbTypeSli[0]
	dbTypeT := dbTypeSli[1]

	buildInDatatypeRules, err := model.GetIBuildInDatatypeRuleRW().QueryBuildInDatatypeRule(st.Ctx, dbTypeS, dbTypeT)
	if err != nil {
		return err
	}
	buildInDefaultValueRules, err := model.GetBuildInDefaultValueRuleRW().QueryBuildInDefaultValueRule(st.Ctx, dbTypeS, dbTypeT)
	if err != nil {
		return err
	}

	// process schema
	var createSchema string
	schemaCreateTime := time.Now()

	dbVersionS, err := databaseS.GetDatabaseVersion()
	if err != nil {
		return err
	}

	logger.Info("inspect struct migrate task",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow))

	switch st.Task.TaskFlow {
	case constant.TaskFlowOracleToTiDB, constant.TaskFlowOracleToMySQL:
		nlsComp, err := processor.InspectOracleMigrateTask(st.Task.TaskName, st.Task.TaskFlow, st.Task.TaskMode, databaseS, stringutil.StringUpper(st.DatasourceS.ConnectCharset), stringutil.StringUpper(st.DatasourceT.ConnectCharset))
		if err != nil {
			return err
		}

		dbCollationS := false
		if stringutil.VersionOrdinal(dbVersionS) >= stringutil.VersionOrdinal(constant.OracleDatabaseTableAndColumnSupportVersion) {
			dbCollationS = true
		} else {
			dbCollationS = false
		}
		if dbCollationS {
			schemaCollationS, err := databaseS.GetDatabaseSchemaCollation(st.SchemaNameS)
			if err != nil {
				return err
			}
			targetSchemaCollation, ok := constant.MigrateTableStructureDatabaseCollationMap[st.Task.TaskFlow][stringutil.StringUpper(schemaCollationS)][stringutil.StringUpper(st.DatasourceT.ConnectCharset)]
			if !ok {
				return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema [%s] collation [%s] isn't support", st.Task.TaskName, st.Task.TaskMode, st.Task.TaskFlow, st.SchemaNameS, schemaCollationS)
			}
			if st.TaskParams.CreateIfNotExist {
				createSchema = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET %s COLLATE %s;", st.SchemaNameT, stringutil.StringUpper(st.DatasourceT.ConnectCharset), targetSchemaCollation)
			} else {
				createSchema = fmt.Sprintf("CREATE DATABASE `%s` DEFAULT CHARACTER SET %s COLLATE %s;", st.SchemaNameT, stringutil.StringUpper(st.DatasourceT.ConnectCharset), targetSchemaCollation)
			}
		} else {
			targetSchemaCollation, ok := constant.MigrateTableStructureDatabaseCollationMap[st.Task.TaskFlow][stringutil.StringUpper(nlsComp)][stringutil.StringUpper(st.DatasourceT.ConnectCharset)]
			if !ok {
				return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema [%s] nls_comp collation [%s] isn't support", st.Task.TaskName, st.Task.TaskMode, st.Task.TaskFlow, st.SchemaNameS, nlsComp)
			}
			if st.TaskParams.CreateIfNotExist {
				createSchema = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET %s COLLATE %s;", st.SchemaNameT, stringutil.StringUpper(st.DatasourceT.ConnectCharset), targetSchemaCollation)
			} else {
				createSchema = fmt.Sprintf("CREATE DATABASE `%s` DEFAULT CHARACTER SET %s COLLATE %s;", st.SchemaNameT, stringutil.StringUpper(st.DatasourceT.ConnectCharset), targetSchemaCollation)
			}
		}
	case constant.TaskFlowPostgresToTiDB, constant.TaskFlowPostgresToMySQL:
		dbCollation, err := processor.InspectPostgresMigrateTask(st.Task.TaskName, st.Task.TaskFlow, st.Task.TaskMode, databaseS, stringutil.StringUpper(st.DatasourceS.ConnectCharset), stringutil.StringUpper(st.DatasourceT.ConnectCharset))
		if err != nil {
			return err
		}
		targetSchemaCollation, ok := constant.MigrateTableStructureDatabaseCollationMap[st.Task.TaskFlow][stringutil.StringUpper(dbCollation)][stringutil.StringUpper(st.DatasourceT.ConnectCharset)]
		if !ok {
			return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema [%s] collation [%s] isn't support", st.Task.TaskName, st.Task.TaskMode, st.Task.TaskFlow, st.SchemaNameS, dbCollation)
		}
		if st.TaskParams.CreateIfNotExist {
			createSchema = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET %s COLLATE %s;", st.SchemaNameT, stringutil.StringUpper(st.DatasourceT.ConnectCharset), targetSchemaCollation)
		} else {
			createSchema = fmt.Sprintf("CREATE DATABASE `%s` DEFAULT CHARACTER SET %s COLLATE %s;", st.SchemaNameT, stringutil.StringUpper(st.DatasourceT.ConnectCharset), targetSchemaCollation)
		}
	default:
		return fmt.Errorf("the task_name [%s] task_mode [%s] task_flow [%s] schema [%s] isn't support, please contact author or reselect", st.Task.TaskName, st.Task.TaskMode, st.Task.TaskFlow, st.SchemaNameS)
	}

	encryptCreateSchema, err := stringutil.Encrypt(createSchema, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}

	// direct write database -> schema
	if st.TaskParams.EnableDirectCreate {
		_, err = databaseT.ExecContext(st.Ctx, createSchema)
		if err != nil {
			return err
		}
	}

	// schema create failed, return
	_, err = model.GetISchemaMigrateTaskRW().CreateSchemaMigrateTask(st.Ctx, &task.SchemaMigrateTask{
		TaskName:        st.Task.TaskName,
		SchemaNameS:     st.SchemaNameS,
		SchemaNameT:     st.SchemaNameT,
		TaskStatus:      constant.TaskDatabaseStatusSuccess,
		TargetSqlDigest: encryptCreateSchema,
		Duration:        time.Now().Sub(schemaCreateTime).Seconds(),
	})
	if err != nil {
		return err
	}

	logger.Info("generate schema create sql",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("create_schema_sql", createSchema))

	_, err = model.GetITaskLogRW().CreateLog(st.Ctx, &task.Log{
		TaskName: st.Task.TaskName,
		LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] schema create sql gen",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeStructMigrate),
			st.Task.TaskName,
			st.Task.TaskFlow,
			st.Task.WorkerAddr,
		),
	})
	if err != nil {
		return err
	}

	logger.Info("query migrate checkppoint",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.Bool("enable_checkpoint", st.TaskParams.EnableCheckpoint))

	if !st.TaskParams.EnableCheckpoint {
		err := model.Transaction(st.Ctx, func(txnCtx context.Context) error {
			err := model.GetIStructMigrateSummaryRW().DeleteStructMigrateSummaryName(txnCtx, []string{st.Task.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIStructMigrateTaskRW().DeleteStructMigrateTaskName(txnCtx, []string{st.Task.TaskName})
			if err != nil {
				return err
			}
			err = model.GetISequenceMigrateSummaryRW().DeleteSequenceMigrateSummaryName(txnCtx, []string{st.Task.TaskName})
			if err != nil {
				return err
			}
			err = model.GetISequenceMigrateTaskRW().DeleteSequenceMigrateTaskName(txnCtx, []string{st.Task.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	logger.Info("process migrate schema tables",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT))

	// interval 10 seconds print
	progress := processor.NewProgresser(st.Ctx)
	defer progress.Close()

	progress.Init(
		processor.WithTaskName(st.Task.TaskName),
		processor.WithTaskMode(st.Task.TaskMode),
		processor.WithTaskFlow(st.Task.TaskFlow))

	err = database.IDatabaseRun(&processor.StructMigrateTask{
		Ctx:                      st.Ctx,
		Task:                     st.Task,
		SchemaNameS:              st.SchemaNameS,
		SchemaNameT:              st.SchemaNameT,
		DatabaseS:                databaseS,
		DatabaseT:                databaseT,
		DBTypeS:                  dbTypeS,
		DBVersionS:               dbVersionS,
		DBCharsetS:               stringutil.StringUpper(st.DatasourceS.ConnectCharset),
		DBCharsetT:               stringutil.StringUpper(st.DatasourceT.ConnectCharset),
		StartTime:                startTime,
		BuildInDatatypeRules:     buildInDatatypeRules,
		BuildInDefaultValueRules: buildInDefaultValueRules,
		TaskParams:               st.TaskParams,
		StructReadyInit:          make(chan bool, 1),
		SequenceReadyInit:        make(chan bool, 1),
		Progress:                 progress,
	})
	if err != nil {
		return err
	}

	logger.Info("process migrate tables finished",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("cost", time.Since(startTime).String()))
	return nil
}
