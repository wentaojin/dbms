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
	"strings"
	"time"

	"github.com/wentaojin/dbms/errconcurrent"

	"github.com/wentaojin/dbms/model/buildin"

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
	DatabaseS   database.IDatabase
	DatabaseT   database.IDatabase
	DBCharsetS  string
	DBCharsetT  string
	TaskParams  *pb.StructMigrateParam
}

func (st *StructMigrateTask) Start() error {
	schemaStartTime := time.Now()
	logger.Info("struct migrate task get buildin rule",
		zap.String("task_name", st.Task.TaskName), zap.String("task_mode", st.Task.TaskMode), zap.String("task_flow", st.Task.TaskFlow))

	dbTypeSli := stringutil.StringSplit(st.Task.TaskFlow, constant.StringSeparatorAite)
	buildInDatatypeRules, err := model.GetIBuildInDatatypeRuleRW().QueryBuildInDatatypeRule(st.Ctx, dbTypeSli[0], dbTypeSli[1])
	if err != nil {
		return err
	}
	buildInDefaultValueRules, err := model.GetBuildInDefaultValueRuleRW().QueryBuildInDefaultValueRule(st.Ctx, dbTypeSli[0], dbTypeSli[1])
	if err != nil {
		return err
	}

	logger.Info("struct migrate task inspect migrate task",
		zap.String("task_name", st.Task.TaskName), zap.String("task_mode", st.Task.TaskMode), zap.String("task_flow", st.Task.TaskFlow))
	schemaCollationS, nlsComp, tableCollationS, dbCollationS, err := InspectStructMigrateTask(st.Task.TaskName, st.Task.TaskFlow, st.SchemaNameS, st.DatabaseS, st.DBCharsetS, st.DBCharsetT)
	if err != nil {
		return err
	}

	// process schema
	var createSchema string
	schemaCreateTime := time.Now()
	logger.Info("struct migrate task process schema",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT))
	switch {
	case strings.EqualFold(st.Task.TaskFlow, constant.TaskFlowOracleToTiDB) || strings.EqualFold(st.Task.TaskFlow, constant.TaskFlowOracleToMySQL):
		if dbCollationS {
			targetSchemaCollation, ok := constant.MigrateTableStructureDatabaseCollationMap[st.Task.TaskFlow][stringutil.StringUpper(schemaCollationS)][stringutil.StringUpper(st.DBCharsetT)]
			if !ok {
				return fmt.Errorf("oracle current task [%s] task_mode [%s] task_flow [%s] schema [%s] collation [%s] isn't support", st.Task.TaskName, st.Task.TaskMode, st.Task.TaskFlow, st.SchemaNameS, schemaCollationS)
			}
			if st.TaskParams.CreateIfNotExist {
				createSchema = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET %s COLLATE %s;", st.SchemaNameT, stringutil.StringUpper(st.DBCharsetT), targetSchemaCollation)
			} else {
				createSchema = fmt.Sprintf("CREATE DATABASE `%s` DEFAULT CHARACTER SET %s COLLATE %s;", st.SchemaNameT, stringutil.StringUpper(st.DBCharsetT), targetSchemaCollation)
			}
		} else {
			targetSchemaCollation, ok := constant.MigrateTableStructureDatabaseCollationMap[st.Task.TaskFlow][stringutil.StringUpper(nlsComp)][stringutil.StringUpper(st.DBCharsetT)]
			if !ok {
				return fmt.Errorf("oracle current task [%s] task_mode [%s] task_flow [%s] schema [%s] nls_comp collation [%s] isn't support", st.Task.TaskName, st.Task.TaskMode, st.Task.TaskFlow, st.SchemaNameS, nlsComp)
			}
			if st.TaskParams.CreateIfNotExist {
				createSchema = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET %s COLLATE %s;", st.SchemaNameT, stringutil.StringUpper(st.DBCharsetT), targetSchemaCollation)
			} else {
				createSchema = fmt.Sprintf("CREATE DATABASE `%s` DEFAULT CHARACTER SET %s COLLATE %s;", st.SchemaNameT, stringutil.StringUpper(st.DBCharsetT), targetSchemaCollation)
			}
		}
	default:
		return fmt.Errorf("oracle current task [%s] task_mode [%s] task_flow [%s] schema [%s] isn't support, please contact author or reselect", st.Task.TaskName, st.Task.TaskMode, st.Task.TaskFlow, st.SchemaNameS)
	}

	encryptCreateSchema, err := stringutil.Encrypt(createSchema, []byte(constant.DefaultDataEncryptDecryptKey))
	if err != nil {
		return err
	}

	// schema create failed, return
	_, err = model.GetIStructMigrateTaskRW().CreateStructMigrateTask(st.Ctx, &task.StructMigrateTask{
		TaskName:        st.Task.TaskName,
		SchemaNameS:     st.SchemaNameS,
		TableTypeS:      constant.OracleDatabaseTableTypeSchema,
		SchemaNameT:     st.SchemaNameT,
		TaskStatus:      constant.TaskDatabaseStatusSuccess,
		TargetSqlDigest: encryptCreateSchema,
		IsSchemaCreate:  constant.DatabaseIsSchemaCreateSqlYES,
		Duration:        time.Now().Sub(schemaCreateTime).Seconds(),
	})
	if err != nil {
		return err
	}

	// direct write database -> schema
	if st.TaskParams.EnableDirectCreate {
		_, err = st.DatabaseT.ExecContext(st.Ctx, createSchema)
		if err != nil {
			return err
		}
	}

	logger.Info("struct migrate task get migrate tasks",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow))
	var migrateTasks []*task.StructMigrateTask

	err = model.Transaction(st.Ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateTasks, err = model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:       st.Task.TaskName,
				TaskStatus:     constant.TaskDatabaseStatusWaiting,
				IsSchemaCreate: constant.DatabaseIsSchemaCreateSqlNO})
		if err != nil {
			return err
		}
		migrateFailedTasks, err := model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:       st.Task.TaskName,
				TaskStatus:     constant.TaskDatabaseStatusFailed,
				IsSchemaCreate: constant.DatabaseIsSchemaCreateSqlNO})
		if err != nil {
			return err
		}
		migrateRunningTasks, err := model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:       st.Task.TaskName,
				TaskStatus:     constant.TaskDatabaseStatusRunning,
				IsSchemaCreate: constant.DatabaseIsSchemaCreateSqlNO})
		if err != nil {
			return err
		}
		migrateStopTasks, err := model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:       st.Task.TaskName,
				TaskStatus:     constant.TaskDatabaseStatusStopped,
				IsSchemaCreate: constant.DatabaseIsSchemaCreateSqlNO})
		if err != nil {
			return err
		}
		migrateTasks = append(migrateTasks, migrateFailedTasks...)
		migrateTasks = append(migrateTasks, migrateRunningTasks...)
		migrateTasks = append(migrateTasks, migrateStopTasks...)
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("struct migrate task process migrate tables",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow))
	g := errconcurrent.NewGroup()
	g.SetLimit(int(st.TaskParams.MigrateThread))

	for _, job := range migrateTasks {
		gTime := time.Now()
		g.Go(job, gTime, func(job interface{}) error {
			select {
			case <-st.Ctx.Done():
				return nil
			default:
				smt := job.(*task.StructMigrateTask)
				err = st.structMigrateStart(
					st.DatabaseS,
					st.DatabaseT,
					gTime,
					smt,
					buildInDatatypeRules,
					buildInDefaultValueRules,
					dbCollationS,
					schemaCollationS,
					tableCollationS,
					nlsComp)
				if err != nil {
					return err
				}
				return nil
			}
		})
	}
	for _, r := range g.Wait() {
		if r.Err != nil {
			smt := r.Task.(*task.StructMigrateTask)
			logger.Warn("struct migrate task",
				zap.String("task_name", st.Task.TaskName),
				zap.String("task_mode", st.Task.TaskMode),
				zap.String("task_flow", st.Task.TaskFlow),
				zap.String("schema_name_s", smt.SchemaNameS),
				zap.String("table_name_s", smt.TableNameS),
				zap.Error(r.Err))

			errW := model.Transaction(st.Ctx, func(txnCtx context.Context) error {
				_, err = model.GetIStructMigrateTaskRW().UpdateStructMigrateTask(txnCtx,
					&task.StructMigrateTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS},
					map[string]interface{}{
						"TaskStatus":  constant.TaskDatabaseStatusFailed,
						"Duration":    fmt.Sprintf("%f", time.Now().Sub(r.Time).Seconds()),
						"ErrorDetail": r.Err.Error(),
					})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName:    smt.TaskName,
					SchemaNameS: smt.SchemaNameS,
					TableNameS:  smt.TableNameS,
					LogDetail: fmt.Sprintf("%v [%v] struct migrate task [%v] taskflow [%v] source table [%v.%v] failed, please see [struct_migrate_task] detail",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(st.Task.TaskMode),
						smt.TaskName,
						st.Task.TaskMode,
						smt.SchemaNameS,
						smt.TableNameS),
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

	err = st.DatabaseS.Close()
	if err != nil {
		return err
	}
	err = st.DatabaseT.Close()
	if err != nil {
		return err
	}
	schemaEndTime := time.Now()
	_, err = model.GetIStructMigrateSummaryRW().UpdateStructMigrateSummary(st.Ctx,
		&task.StructMigrateSummary{
			TaskName:    st.Task.TaskName,
			SchemaNameS: st.SchemaNameS},
		map[string]interface{}{
			"Duration": fmt.Sprintf("%f", schemaEndTime.Sub(schemaStartTime).Seconds()),
		})
	if err != nil {
		return err
	}
	logger.Info("struct migrate task",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("cost", schemaEndTime.Sub(schemaStartTime).String()))
	return nil
}

func (st *StructMigrateTask) structMigrateStart(
	databaseS,
	databaseT database.IDatabase,
	startTime time.Time,
	smt *task.StructMigrateTask,
	buildInDatatypeRules []*buildin.BuildinDatatypeRule,
	buildInDefaultValueRules []*buildin.BuildinDefaultvalRule,
	dbCollationS bool,
	schemaCollationS string,
	tableCollationS map[string]string,
	nlsComp string) error {
	// if the schema table success, skip
	if strings.EqualFold(smt.TaskStatus, constant.TaskDatabaseStatusSuccess) {
		logger.Info("struct migrate task process",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", smt.SchemaNameS),
			zap.String("table_name_s", smt.TableNameS),
			zap.String("task_status", constant.TaskDatabaseStatusSuccess),
			zap.String("table task had done", "skip migrate"),
			zap.String("cost", time.Now().Sub(startTime).String()))
		return nil
	}
	if strings.EqualFold(smt.TaskStatus, constant.TaskDatabaseStatusRunning) {
		logger.Info("struct migrate task process",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", smt.SchemaNameS),
			zap.String("table_name_s", smt.TableNameS),
			zap.String("task_status", constant.TaskDatabaseStatusRunning),
			zap.String("table task has running", "current status may panic, skip migrate, please double check"),
			zap.String("cost", time.Now().Sub(startTime).String()))
		return nil
	}

	err := model.Transaction(st.Ctx, func(txnCtx context.Context) error {
		_, err := model.GetIStructMigrateTaskRW().UpdateStructMigrateTask(txnCtx, &task.StructMigrateTask{
			TaskName:    smt.TaskName,
			SchemaNameS: smt.SchemaNameS,
			TableNameS:  smt.TableNameS},
			map[string]interface{}{
				"TaskStatus": constant.TaskDatabaseStatusRunning,
			})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName:    smt.TaskName,
			SchemaNameS: smt.SchemaNameS,
			TableNameS:  smt.TableNameS,
			LogDetail: fmt.Sprintf("%v [%v] the worker task [%v] source table [%v.%v] starting",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeStructMigrate),
				smt.TaskName,
				smt.SchemaNameS,
				smt.TableNameS),
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	sourceTime := time.Now()
	dataSource := &Datasource{
		DatabaseS:        databaseS,
		SchemaNameS:      smt.SchemaNameS,
		TableNameS:       smt.TableNameS,
		TableTypeS:       smt.TableTypeS,
		CollationS:       dbCollationS,
		DBCharsetS:       st.DBCharsetS,
		DBCharsetT:       st.DBCharsetT,
		SchemaCollationS: schemaCollationS,
		TableCollationS:  tableCollationS[smt.TableNameS],
		DBNlsCompS:       nlsComp,
	}

	attrs, err := database.IStructMigrateAttributes(dataSource)
	if err != nil {
		return err
	}
	logger.Info("struct migrate task process",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", smt.SchemaNameS),
		zap.String("table_name_s", smt.TableNameS),
		zap.String("task_stage", "datasource"),
		zap.String("datasource", dataSource.String()),
		zap.String("cost", time.Now().Sub(sourceTime).String()))
	ruleTime := time.Now()
	dataRule := &StructMigrateRule{
		Ctx:                      st.Ctx,
		TaskName:                 smt.TaskName,
		TaskFlow:                 st.Task.TaskFlow,
		SchemaNameS:              smt.SchemaNameS,
		TableNameS:               smt.TableNameS,
		TablePrimaryAttrs:        attrs.PrimaryKey,
		TableColumnsAttrs:        attrs.TableColumns,
		TableCommentAttrs:        attrs.TableComment,
		CreateIfNotExist:         st.TaskParams.CreateIfNotExist,
		CaseFieldRuleT:           st.Task.CaseFieldRuleT,
		DBCollationS:             dbCollationS,
		DBCharsetS:               st.DBCharsetS,
		DBCharsetT:               st.DBCharsetT,
		BuildinDatatypeRules:     buildInDatatypeRules,
		BuildinDefaultValueRules: buildInDefaultValueRules,
	}

	rules, err := database.IStructMigrateAttributesRule(dataRule)
	if err != nil {
		return err
	}
	logger.Info("struct migrate task process",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", smt.SchemaNameS),
		zap.String("table_name_s", smt.TableNameS),
		zap.String("task_stage", "rule"),
		zap.String("rule", dataRule.String()),
		zap.String("cost", time.Now().Sub(ruleTime).String()))

	tableTime := time.Now()
	dataTable := &StructMigrateTable{
		TaskName:            smt.TaskName,
		TaskFlow:            st.Task.TaskFlow,
		DatasourceS:         dataSource,
		TableAttributes:     attrs,
		TableAttributesRule: rules,
	}

	tableStruct, err := database.IStructMigrateTableStructure(dataTable)
	if err != nil {
		return err
	}

	logger.Info("struct migrate task process",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", smt.SchemaNameS),
		zap.String("table_name_s", smt.TableNameS),
		zap.String("task_stage", "struct"),
		zap.String("struct", dataTable.String()),
		zap.String("cost", time.Now().Sub(tableTime).String()))

	writerTime := time.Now()
	var w database.IStructMigrateDatabaseWriter
	w = NewStructMigrateDatabase(st.Ctx, smt.TaskName, st.Task.TaskFlow, databaseT, startTime, tableStruct)

	if st.TaskParams.EnableDirectCreate {
		err = w.SyncStructDatabase()
		if err != nil {
			return err
		}
		logger.Info("struct migrate task process",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", smt.SchemaNameS),
			zap.String("table_name_s", smt.TableNameS),
			zap.String("task_stage", "struct sync database"),
			zap.String("cost", time.Now().Sub(writerTime).String()))

		return nil
	}

	err = w.WriteStructDatabase()
	if err != nil {
		return err
	}
	logger.Info("struct migrate task process",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", smt.SchemaNameS),
		zap.String("table_name_s", smt.TableNameS),
		zap.String("task_stage", "struct write database"),
		zap.String("cost", time.Now().Sub(writerTime).String()))
	return nil
}

func InspectStructMigrateTask(taskName, taskFlow string, schemaNameS string, databaseS database.IDatabase, connectDBCharsetS, connectDBCharsetT string) (string, string, map[string]string, bool, error) {
	var (
		schemaCollationS string
	)
	tableCollationS := make(map[string]string)

	dbCharsetS := stringutil.StringUpper(connectDBCharsetS)

	if !strings.EqualFold(connectDBCharsetT, constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][dbCharsetS]) {
		return schemaCollationS, "", tableCollationS, false, fmt.Errorf("oracle current subtask [%s] taskflow [%s] schema [%s] mapping charset [%s] isn't equal with database connect charset [%s], please adjust database connect charset", taskName, taskFlow, schemaNameS, dbCharsetS, connectDBCharsetT)
	}

	dbCharset, err := databaseS.GetDatabaseCharset()
	if err != nil {
		return schemaCollationS, "", tableCollationS, false, err
	}
	nlsComp, nlsSort, err := databaseS.GetDatabaseCharsetCollation()
	if err != nil {
		return schemaCollationS, "", tableCollationS, false, err
	}
	if _, ok := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(nlsComp)][constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][dbCharset]]; !ok {
		return schemaCollationS, nlsComp, tableCollationS, false,
			fmt.Errorf("oracle database nls comp [%s] , mysql db isn't support", nlsComp)
	}
	if _, ok := constant.MigrateTableStructureDatabaseCollationMap[taskFlow][stringutil.StringUpper(nlsSort)][constant.MigrateTableStructureDatabaseCharsetMap[taskFlow][dbCharset]]; !ok {
		return schemaCollationS, nlsComp, tableCollationS, false, fmt.Errorf("oracle database nls sort [%s] , mysql db isn't support", nlsSort)
	}

	if !strings.EqualFold(nlsSort, nlsComp) {
		return schemaCollationS, nlsComp, tableCollationS, false, fmt.Errorf("oracle database nls_sort [%s] and nls_comp [%s] isn't different, need be equal; because mysql db isn't support", nlsSort, nlsComp)
	}

	// whether the oracle version can specify table and field collation，if the oracle database version is 12.2 and the above version, it's specify table and field collation, otherwise can't specify
	// oracle database nls_sort/nls_comp value need to be equal, USING_NLS_COMP value is nls_comp
	oracleDBVersion, err := databaseS.GetDatabaseVersion()
	if err != nil {
		return schemaCollationS, nlsComp, tableCollationS, false, err
	}

	oracleCollation := false
	if stringutil.VersionOrdinal(oracleDBVersion) >= stringutil.VersionOrdinal(constant.OracleDatabaseTableAndColumnSupportVersion) {
		oracleCollation = true
	}

	if oracleCollation {
		schemaCollationS, err = databaseS.GetDatabaseSchemaCollation(schemaNameS)
		if err != nil {
			return schemaCollationS, nlsComp, tableCollationS, false, err
		}
		tableCollationS, err = databaseS.GetDatabaseSchemaTableCollation(schemaNameS, schemaCollationS)
		if err != nil {
			return schemaCollationS, nlsComp, tableCollationS, false, err
		}
	}
	return schemaCollationS, nlsComp, tableCollationS, oracleCollation, nil
}
