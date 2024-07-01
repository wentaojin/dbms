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
	"github.com/wentaojin/dbms/model/rule"
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
	logger.Info("struct migrate task inspect migrate task",
		zap.String("task_name", st.Task.TaskName), zap.String("task_mode", st.Task.TaskMode), zap.String("task_flow", st.Task.TaskFlow))
	_, nlsComp, dbCollationS, err := inspectMigrateTask(st.Task.TaskName, st.Task.TaskFlow, st.Task.TaskMode, st.DatabaseS, st.DBCharsetS, st.DBCharsetT)
	if err != nil {
		return err
	}

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

	logger.Info("struct migrate task init task information",
		zap.String("task_name", st.Task.TaskName), zap.String("task_mode", st.Task.TaskMode), zap.String("task_flow", st.Task.TaskFlow))
	err = st.initStructMigrateTask()
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
			schemaCollationS, err := st.DatabaseS.GetDatabaseSchemaCollation(st.SchemaNameS)
			if err != nil {
				return err
			}
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
		TableTypeS:      constant.DatabaseStructMigrateSqlSchemaCategory,
		SchemaNameT:     st.SchemaNameT,
		TaskStatus:      constant.TaskDatabaseStatusSuccess,
		TargetSqlDigest: encryptCreateSchema,
		Category:        constant.DatabaseStructMigrateSqlSchemaCategory,
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
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusWaiting,
				Category:   constant.DatabaseStructMigrateSqlTableCategory})
		if err != nil {
			return err
		}
		migrateFailedTasks, err := model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusFailed,
				Category:   constant.DatabaseStructMigrateSqlTableCategory})
		if err != nil {
			return err
		}
		migrateRunningTasks, err := model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusRunning,
				Category:   constant.DatabaseStructMigrateSqlTableCategory})
		if err != nil {
			return err
		}
		migrateStopTasks, err := model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusStopped,
				Category:   constant.DatabaseStructMigrateSqlTableCategory})
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
					dbCollationS)
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

	// sequence migrate exclude struct_migrate_summary compute counts
	err = st.sequenceMigrateStart(st.DatabaseS, st.DatabaseT)
	if err != nil {
		return err
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
	dbCollationS bool) error {
	// if the schema table success, skip
	if strings.EqualFold(smt.TaskStatus, constant.TaskDatabaseStatusSuccess) {
		logger.Warn("struct migrate task process",
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
	// if the table is MATERIALIZED VIEW, SKIP
	// MATERIALIZED VIEW isn't support struct migrate
	if strings.EqualFold(smt.TableTypeS, constant.OracleDatabaseTableTypeMaterializedView) {
		logger.Warn("struct migrate task process",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", smt.SchemaNameS),
			zap.String("table_name_s", smt.TableNameS),
			zap.String("table_type_s", smt.TableTypeS),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
		zap.String("cost", time.Now().Sub(startTime).String())
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
		DatabaseS:   databaseS,
		SchemaNameS: smt.SchemaNameS,
		TableNameS:  smt.TableNameS,
		TableTypeS:  smt.TableTypeS,
		CollationS:  dbCollationS,
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
		DBCharsetT:          st.DBCharsetT,
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

func (st *StructMigrateTask) sequenceMigrateStart(databaseS, databaseT database.IDatabase) error {
	startTime := time.Now()
	logger.Info("sequence migrate task process",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("start_time", startTime.String()))
	sequences, err := databaseS.GetDatabaseSequence(st.SchemaNameS)
	if err != nil {
		return err
	}

	var seqCreates []string
	for _, seq := range sequences {
		lastNumber, err := stringutil.StrconvIntBitSize(seq["LAST_NUMBER"], 64)
		if err != nil {
			return err
		}
		cacheSize, err := stringutil.StrconvIntBitSize(seq["CACHE_SIZE"], 64)
		if err != nil {
			return err
		}
		// disable cache
		if cacheSize == 0 {
			lastNumber = lastNumber + 5000
		} else {
			lastNumber = lastNumber + (cacheSize * 2)
		}

		switch {
		case strings.EqualFold(st.Task.TaskFlow, constant.TaskFlowOracleToMySQL) || strings.EqualFold(st.Task.TaskFlow, constant.TaskFlowOracleToTiDB):
			if st.TaskParams.CreateIfNotExist {
				seqCreates = append(seqCreates, fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS %s.%s START %v INCREMENT %v MINVALUE %v MAXVALUE %v CACHE %v CYCLE %v;`, st.SchemaNameT, seq["SEQUENCE_NAME"], lastNumber, seq["INCREMENT_BY"], seq["MIN_VALUE"], seq["MAX_VALUE"], seq["CACHE_SIZE"], seq["CYCLE_FLAG"]))
			} else {
				seqCreates = append(seqCreates, fmt.Sprintf(`CREATE SEQUENCE %s.%s START %v INCREMENT %v MINVALUE %v MAXVALUE %v CACHE %v CYCLE %v;`, st.SchemaNameT, seq["SEQUENCE_NAME"], lastNumber, seq["INCREMENT_BY"], seq["MIN_VALUE"], seq["MAX_VALUE"], seq["CACHE_SIZE"], seq["CYCLE_FLAG"]))
			}
		default:
			return fmt.Errorf("the task [%v] task_flow [%v] isn't support, please contact author or reselect", st.Task.TaskName, st.Task.TaskFlow)
		}
	}

	writerTime := time.Now()
	var w database.ISequenceMigrateDatabaseWriter
	w = NewSequenceMigrateDatabase(st.Ctx, st.Task.TaskName, st.Task.TaskFlow, st.SchemaNameS, st.SchemaNameT, databaseT, startTime, seqCreates)

	if st.TaskParams.EnableDirectCreate {
		err = w.SyncSequenceDatabase()
		if err != nil {
			return err
		}
		logger.Info("sequence migrate task process",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("task_stage", "struct sequence database"),
			zap.String("cost", time.Now().Sub(writerTime).String()))
		return nil
	}

	err = w.WriteSequenceDatabase()
	if err != nil {
		return err
	}
	logger.Info("sequence migrate task process",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("task_stage", "struct sequence database"),
		zap.String("cost", time.Now().Sub(writerTime).String()))
	return nil
}

func (st *StructMigrateTask) initStructMigrateTask() error {
	// delete checkpoint
	initFlags, err := model.GetITaskRW().GetTask(st.Ctx, &task.Task{TaskName: st.Task.TaskName})
	if err != nil {
		return err
	}
	if !st.TaskParams.EnableCheckpoint || strings.EqualFold(initFlags.TaskInit, constant.TaskInitStatusNotFinished) {
		err := model.GetIStructMigrateSummaryRW().DeleteStructMigrateSummaryName(st.Ctx, []string{st.Task.TaskName})
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateTaskRW().DeleteStructMigrateTaskName(st.Ctx, []string{st.Task.TaskName})
		if err != nil {
			return err
		}
	} else if st.TaskParams.EnableCheckpoint && strings.EqualFold(initFlags.TaskInit, constant.TaskInitStatusFinished) {
		logger.Warn("struct migrate task init skip",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("task_init", constant.TaskInitStatusFinished))
		return nil
	}

	schemaRoute, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(st.Ctx,
		&rule.SchemaRouteRule{TaskName: st.Task.TaskName})
	if err != nil {
		return err
	}

	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(st.Ctx, &rule.MigrateTaskTable{
		TaskName:    schemaRoute.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	if err != nil {
		return err
	}
	var (
		includeTables      []string
		excludeTables      []string
		databaseTaskTables []string // task tables
	)
	databaseTableTypeMap := make(map[string]string)

	for _, t := range schemaTaskTables {
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsExclude) {
			excludeTables = append(excludeTables, t.TableNameS)
		}
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsNotExclude) {
			includeTables = append(includeTables, t.TableNameS)
		}
	}

	tableObjs, err := st.DatabaseS.FilterDatabaseTable(schemaRoute.SchemaNameS, includeTables, excludeTables)
	if err != nil {
		return err
	}

	// rule case field
	for _, t := range tableObjs.TaskTables {
		var tabName string
		// the according target case field rule convert
		if strings.EqualFold(st.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			tabName = stringutil.StringLower(t)
		}
		if strings.EqualFold(st.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			tabName = stringutil.StringUpper(t)
		}
		if strings.EqualFold(st.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
			tabName = t
		}
		databaseTaskTables = append(databaseTaskTables, tabName)
	}

	databaseTableTypeMap, err = st.DatabaseS.GetDatabaseTableType(schemaRoute.SchemaNameS)
	if err != nil {
		return err
	}

	// get table route rule
	tableRouteRule := make(map[string]string)

	tableRoutes, err := model.GetIMigrateTableRouteRW().FindTableRouteRule(st.Ctx, &rule.TableRouteRule{
		TaskName:    schemaRoute.TaskName,
		SchemaNameS: schemaRoute.SchemaNameS,
	})
	for _, tr := range tableRoutes {
		tableRouteRule[tr.TableNameS] = tr.TableNameT
	}

	// clear the struct migrate task table
	migrateTasks, err := model.GetIStructMigrateTaskRW().BatchFindStructMigrateTask(st.Ctx, &task.StructMigrateTask{TaskName: st.Task.TaskName})
	if err != nil {
		return err
	}

	// repeatInitTableMap used for store the struct_migrate_task table name has be finished, avoid repeated initialization
	repeatInitTableMap := make(map[string]struct{})
	if len(migrateTasks) > 0 {
		taskTablesMap := make(map[string]struct{})
		for _, t := range databaseTaskTables {
			taskTablesMap[t] = struct{}{}
		}
		for _, smt := range migrateTasks {
			if _, ok := taskTablesMap[smt.TableNameS]; !ok {
				err = model.GetIStructMigrateTaskRW().DeleteStructMigrateTask(st.Ctx, smt.ID)
				if err != nil {
					return err
				}
			} else {
				repeatInitTableMap[smt.TableNameS] = struct{}{}
			}
		}
	}

	// database tables
	// init database table
	// get table column route rule
	for _, sourceTable := range databaseTaskTables {
		initStructInfos, err := model.GetIStructMigrateTaskRW().GetStructMigrateTaskTable(st.Ctx, &task.StructMigrateTask{
			TaskName:    st.Task.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
			TableNameS:  sourceTable,
		})
		if err != nil {
			return err
		}
		if len(initStructInfos) > 1 {
			return fmt.Errorf("the struct migrate task table is over one, it should be only one")
		}
		// if the table is existed, then skip init
		if _, ok := repeatInitTableMap[sourceTable]; ok {
			continue
		}
		var (
			targetTable string
		)
		if val, ok := tableRouteRule[sourceTable]; ok {
			targetTable = val
		} else {
			// the according target case field rule convert
			if strings.EqualFold(st.Task.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleLower) {
				targetTable = stringutil.StringLower(sourceTable)
			}
			if strings.EqualFold(st.Task.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
				targetTable = stringutil.StringUpper(sourceTable)
			}
			if strings.EqualFold(st.Task.CaseFieldRuleT, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
				targetTable = sourceTable
			}
		}

		_, err = model.GetIStructMigrateTaskRW().CreateStructMigrateTask(st.Ctx, &task.StructMigrateTask{
			TaskName:    st.Task.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
			TableNameS:  sourceTable,
			TableTypeS:  databaseTableTypeMap[sourceTable],
			SchemaNameT: schemaRoute.SchemaNameT,
			TableNameT:  targetTable,
			TaskStatus:  constant.TaskDatabaseStatusWaiting,
			Category:    constant.DatabaseStructMigrateSqlTableCategory,
		})
		if err != nil {
			return err
		}
	}

	_, err = model.GetIStructMigrateSummaryRW().CreateStructMigrateSummary(st.Ctx,
		&task.StructMigrateSummary{
			TaskName:    st.Task.TaskName,
			SchemaNameS: schemaRoute.SchemaNameS,
			TableTotals: uint64(len(databaseTaskTables) + 1), // include schema create sql
		})
	if err != nil {
		return err
	}
	_, err = model.GetITaskRW().UpdateTask(st.Ctx, &task.Task{TaskName: st.Task.TaskName}, map[string]interface{}{"TaskInit": constant.TaskInitStatusFinished})
	if err != nil {
		return err
	}
	return nil
}
