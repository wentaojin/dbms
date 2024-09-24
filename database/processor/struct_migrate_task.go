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
package processor

import (
	"context"
	"fmt"
	"github.com/wentaojin/dbms/errconcurrent"
	"github.com/wentaojin/dbms/model/rule"
	"golang.org/x/sync/errgroup"
	"strings"
	"time"

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
	Ctx                      context.Context
	Task                     *task.Task
	SchemaNameS              string
	SchemaNameT              string
	DatabaseS                database.IDatabase
	DatabaseT                database.IDatabase
	DBTypeS                  string
	DBVersionS               string
	DBCharsetS               string
	DBCharsetT               string
	StartTime                time.Time
	BuildInDatatypeRules     []*buildin.BuildinDatatypeRule
	BuildInDefaultValueRules []*buildin.BuildinDefaultvalRule
	TaskParams               *pb.StructMigrateParam

	StructReadyInit   chan bool
	SequenceReadyInit chan bool
}

func (st *StructMigrateTask) Init() error {
	logger.Info("struct migrate task init table",
		zap.String("task_name", st.Task.TaskName), zap.String("task_mode", st.Task.TaskMode), zap.String("task_flow", st.Task.TaskFlow))

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
	logger.Warn("struct migrate task checkpoint skip",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.Bool("enable_checkpoint", st.TaskParams.EnableCheckpoint))

	g := &errgroup.Group{}
	g.SetLimit(2)

	g.Go(func() error {
		err := st.initSequenceMigrate()
		if err != nil {
			return err
		}
		return nil
	})

	g.Go(func() error {
		err := st.initStructMigrate()
		if err != nil {
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (st *StructMigrateTask) Run() error {
	logger.Info("struct migrate task run table",
		zap.String("task_name", st.Task.TaskName), zap.String("task_mode", st.Task.TaskMode), zap.String("task_flow", st.Task.TaskFlow))
	g := &errgroup.Group{}
	g.SetLimit(2)

	g.Go(func() error {
		for ready := range st.SequenceReadyInit {
			if ready {
				err := st.processSequenceMigrate()
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	g.Go(func() error {
		for ready := range st.StructReadyInit {
			if ready {
				err := st.processStructMigrate()
				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func (st *StructMigrateTask) Resume() error {
	logger.Info("struct migrate task resume table",
		zap.String("task_name", st.Task.TaskName), zap.String("task_mode", st.Task.TaskMode), zap.String("task_flow", st.Task.TaskFlow))
	return nil
}

func (st *StructMigrateTask) Last() error {
	logger.Info("struct migrate task last table",
		zap.String("task_name", st.Task.TaskName), zap.String("task_mode", st.Task.TaskMode), zap.String("task_flow", st.Task.TaskFlow))
	return nil
}

func (st *StructMigrateTask) initStructMigrate() error {
	defer close(st.StructReadyInit)

	logger.Info("struct migrate task init",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS))

	s, err := model.GetIStructMigrateSummaryRW().GetStructMigrateSummary(st.Ctx, &task.StructMigrateSummary{TaskName: st.Task.TaskName, SchemaNameS: st.SchemaNameS})
	if err != nil {
		return err
	}

	if strings.EqualFold(s.InitFlag, constant.TaskInitStatusNotFinished) {
		err = model.Transaction(st.Ctx, func(txnCtx context.Context) error {
			err = model.GetIStructMigrateSummaryRW().DeleteStructMigrateSummaryName(txnCtx, []string{st.Task.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIStructMigrateTaskRW().DeleteStructMigrateTaskName(txnCtx, []string{st.Task.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	} else {
		st.StructReadyInit <- true
	}

	// filter database table
	schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(st.Ctx, &rule.MigrateTaskTable{
		TaskName:    st.Task.TaskName,
		SchemaNameS: st.SchemaNameS,
	})
	if err != nil {
		return err
	}

	if len(schemaTaskTables) == 0 {
		logger.Warn("struct migrate task process",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("detail", "config file schema-route-table include-table-s/exclude-table-s params are null, skip struct migrate"),
			zap.String("action", "skip_struct_migrate"),
			zap.String("cost", time.Now().Sub(st.StartTime).String()))

		st.StructReadyInit <- false
		return nil
	}

	var (
		includeTables      []string
		excludeTables      []string
		databaseTaskTables []string // task tables
	)
	databaseTableTypeMap := make(map[string]string)
	taskTablesMap := make(map[string]struct{})

	for _, t := range schemaTaskTables {
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsExclude) {
			excludeTables = append(excludeTables, t.TableNameS)
		}
		if strings.EqualFold(t.IsExclude, constant.MigrateTaskTableIsNotExclude) {
			includeTables = append(includeTables, t.TableNameS)
		}
	}

	tableObjs, err := st.DatabaseS.FilterDatabaseTable(st.SchemaNameS, includeTables, excludeTables)
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
		taskTablesMap[tabName] = struct{}{}
	}

	databaseTableTypeMap, err = st.DatabaseS.GetDatabaseTableType(st.SchemaNameS)
	if err != nil {
		return err
	}

	// get table route rule
	tableRouteRule := make(map[string]string)

	tableRoutes, err := model.GetIMigrateTableRouteRW().FindTableRouteRule(st.Ctx, &rule.TableRouteRule{
		TaskName:    st.Task.TaskName,
		SchemaNameS: st.SchemaNameS,
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
			SchemaNameS: st.SchemaNameS,
			TableNameS:  sourceTable,
			TableTypeS:  databaseTableTypeMap[sourceTable],
			SchemaNameT: st.SchemaNameT,
			TableNameT:  targetTable,
			TaskStatus:  constant.TaskDatabaseStatusWaiting,
		})
		if err != nil {
			return err
		}
	}

	_, err = model.GetIStructMigrateSummaryRW().CreateStructMigrateSummary(st.Ctx,
		&task.StructMigrateSummary{
			TaskName:    st.Task.TaskName,
			SchemaNameS: st.SchemaNameS,
			TableTotals: uint64(len(databaseTaskTables)), // only the database table, exclude the schema and sequence ddl
			InitFlag:    constant.TaskInitStatusFinished,
			MigrateFlag: constant.TaskMigrateStatusNotFinished,
		})
	if err != nil {
		return err
	}

	st.StructReadyInit <- true
	return nil
}

func (st *StructMigrateTask) processStructMigrate() error {
	logger.Info("struct migrate task run table",
		zap.String("task_name", st.Task.TaskName), zap.String("task_mode", st.Task.TaskMode), zap.String("task_flow", st.Task.TaskFlow))
	s, err := model.GetIStructMigrateSummaryRW().GetStructMigrateSummary(st.Ctx, &task.StructMigrateSummary{TaskName: st.Task.TaskName, SchemaNameS: st.SchemaNameS})
	if err != nil {
		return err
	}

	if strings.EqualFold(s.MigrateFlag, constant.TaskMigrateStatusFinished) {
		logger.Warn("struct migrate task migrate skip",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("init_flag", s.InitFlag),
			zap.String("migrate_flag", s.MigrateFlag),
			zap.String("action", "migrate skip"))
		_, err = model.GetIStructMigrateSummaryRW().UpdateStructMigrateSummary(st.Ctx,
			&task.StructMigrateSummary{
				TaskName:    st.Task.TaskName,
				SchemaNameS: st.SchemaNameS},
			map[string]interface{}{
				"MigrateFlag": constant.TaskMigrateStatusFinished,
				"Duration":    fmt.Sprintf("%f", time.Now().Sub(st.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
		return nil
	}

	logger.Info("struct migrate task get migrate tasks",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow))
	var (
		migrateTasks []*task.StructMigrateTask
	)

	err = model.Transaction(st.Ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateTasks, err = model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusWaiting})
		if err != nil {
			return err
		}
		migrateFailedTasks, err := model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusFailed,
			})
		if err != nil {
			return err
		}
		migrateRunningTasks, err := model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusRunning,
			})
		if err != nil {
			return err
		}
		migrateStopTasks, err := model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusStopped,
			})
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
			smt := job.(*task.StructMigrateTask)
			err = st.structMigrateStart(smt)
			if err != nil {
				return err
			}
			return nil
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

	s, err = model.GetIStructMigrateSummaryRW().GetStructMigrateSummary(st.Ctx, &task.StructMigrateSummary{TaskName: st.Task.TaskName, SchemaNameS: st.SchemaNameS})
	if err != nil {
		return err
	}
	if s.TableTotals == s.TableSuccess {
		_, err = model.GetIStructMigrateSummaryRW().UpdateStructMigrateSummary(st.Ctx,
			&task.StructMigrateSummary{
				TaskName:    st.Task.TaskName,
				SchemaNameS: st.SchemaNameS},
			map[string]interface{}{
				"MigrateFlag": constant.TaskMigrateStatusFinished,
				"Duration":    fmt.Sprintf("%f", time.Now().Sub(st.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
	} else {
		_, err = model.GetIStructMigrateSummaryRW().UpdateStructMigrateSummary(st.Ctx,
			&task.StructMigrateSummary{
				TaskName:    st.Task.TaskName,
				SchemaNameS: st.SchemaNameS},
			map[string]interface{}{
				"MigrateFlag": constant.TaskMigrateStatusNotFinished,
				"Duration":    fmt.Sprintf("%f", time.Now().Sub(st.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *StructMigrateTask) structMigrateStart(smt *task.StructMigrateTask) error {
	// if the schema table success, skip
	if strings.EqualFold(smt.TaskStatus, constant.TaskDatabaseStatusSuccess) {
		logger.Warn("struct migrate task process",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("table_name_s", smt.TableNameS),
			zap.String("task_status", constant.TaskDatabaseStatusSuccess),
			zap.String("table task had done", "skip migrate"),
			zap.String("cost", time.Now().Sub(st.StartTime).String()))
		return nil
	}
	// if the table is MATERIALIZED VIEW, SKIP
	// MATERIALIZED VIEW isn't support struct migrate
	if strings.EqualFold(smt.TableTypeS, constant.DatabaseMigrateTableStructDisabledMaterializedView) {
		logger.Warn("struct migrate task process",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", smt.SchemaNameS),
			zap.String("table_name_s", smt.TableNameS),
			zap.String("table_type_s", smt.TableTypeS),
			zap.String("suggest", "if necessary, please manually process the tables in the above list"))
		zap.String("cost", time.Now().Sub(st.StartTime).String())
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
			LogDetail: fmt.Sprintf("%v [%v] the struct migrate task [%v] source table [%v.%v] starting",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(st.Task.TaskMode),
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
	datasourceS := &Datasource{
		DBTypeS:     st.DBTypeS,
		DBVersionS:  st.DBVersionS,
		DatabaseS:   st.DatabaseS,
		SchemaNameS: smt.SchemaNameS,
		TableNameS:  smt.TableNameS,
		TableTypeS:  smt.TableTypeS,
	}

	attrs, err := database.IStructMigrateAttributes(datasourceS)
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
		zap.String("datasource", datasourceS.String()),
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
		DBVersionS:               st.DBVersionS,
		DBCharsetS:               st.DBCharsetS,
		DBCharsetT:               st.DBCharsetT,
		BuildinDatatypeRules:     st.BuildInDatatypeRules,
		BuildinDefaultValueRules: st.BuildInDefaultValueRules,
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
		DatasourceS:         datasourceS,
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
	w = NewStructMigrateDatabase(st.Ctx, smt.TaskName, st.Task.TaskFlow, st.DatabaseT, st.StartTime, tableStruct)

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

func (st *StructMigrateTask) initSequenceMigrate() error {
	defer close(st.SequenceReadyInit)

	logger.Info("sequence migrate task init",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS))

	seq, err := model.GetISequenceMigrateSummaryRW().GetSequenceMigrateSummary(st.Ctx, &task.SequenceMigrateSummary{TaskName: st.Task.TaskName, SchemaNameS: st.SchemaNameS})
	if err != nil {
		return err
	}

	if strings.EqualFold(seq.InitFlag, constant.TaskInitStatusNotFinished) {
		err = model.Transaction(st.Ctx, func(txnCtx context.Context) error {
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
	} else {
		st.SequenceReadyInit <- true
	}

	// filter database sequence
	schemaSeqsTasks, err := model.GetIMigrateTaskSequenceRW().FindMigrateTaskSequence(st.Ctx, &rule.MigrateTaskSequence{
		TaskName:    st.Task.TaskName,
		SchemaNameS: st.SchemaNameS,
	})
	if err != nil {
		return err
	}

	if len(schemaSeqsTasks) == 0 {
		logger.Warn("sequence migrate task process",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("detail", "config file schema-route-table include-sequence-s/exclude-sequence-s params are null, skip sequence migrate"),
			zap.String("action", "skip_sequence_migrate"),
			zap.String("cost", time.Now().Sub(st.StartTime).String()))

		st.SequenceReadyInit <- false
		return nil
	}
	// rule case field
	taskSeqMap := make(map[string]struct{})

	var (
		includeSeqs  []string
		excludeSeqs  []string
		databaseSeqs []string // task seqs

	)
	for _, s := range schemaSeqsTasks {
		// the according target case field rule convert
		if strings.EqualFold(s.IsExclude, constant.MigrateTaskTableIsNotExclude) {
			includeSeqs = append(includeSeqs, s.SequenceNameS)
		}
		if strings.EqualFold(s.IsExclude, constant.MigrateTaskTableIsExclude) {
			excludeSeqs = append(excludeSeqs, s.SequenceNameS)
		}
	}

	seqObjs, err := st.DatabaseS.FilterDatabaseSequence(st.SchemaNameS, includeSeqs, excludeSeqs)
	if err != nil {
		return err
	}

	for _, t := range seqObjs.SequenceNames {
		var seqName string
		// the according target case field rule convert
		if strings.EqualFold(st.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleLower) {
			seqName = stringutil.StringLower(t)
		}
		if strings.EqualFold(st.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleUpper) {
			seqName = stringutil.StringUpper(t)
		}
		if strings.EqualFold(st.Task.CaseFieldRuleS, constant.ParamValueStructMigrateCaseFieldRuleOrigin) {
			seqName = t
		}
		databaseSeqs = append(databaseSeqs, seqName)
		taskSeqMap[seqName] = struct{}{}
	}

	// clear the sequence migrate task
	migrateSeqs, err := model.GetISequenceMigrateTaskRW().BatchFindSequenceMigrateTask(st.Ctx, &task.SequenceMigrateTask{TaskName: st.Task.TaskName})
	if err != nil {
		return err
	}

	// repeatInitSeqMap used for store the sequence_migrate_task table name has be finished, avoid repeated initialization
	repeatInitSeqMap := make(map[string]struct{})
	if len(migrateSeqs) > 0 {
		for _, smt := range migrateSeqs {
			if _, ok := taskSeqMap[smt.SequenceNameS]; !ok {
				err = model.GetISequenceMigrateTaskRW().DeleteSequenceMigrateTask(st.Ctx, smt.ID)
				if err != nil {
					return err
				}
			} else {
				repeatInitSeqMap[smt.SequenceNameS] = struct{}{}
			}
		}
	}

	for _, s := range databaseSeqs {
		// the sequence exist init skip
		if _, ok := repeatInitSeqMap[s]; ok {
			continue
		}
		_, err = model.GetISequenceMigrateTaskRW().CreateSequenceMigrateTask(st.Ctx, &task.SequenceMigrateTask{
			TaskName:      st.Task.TaskName,
			SchemaNameS:   st.SchemaNameS,
			SequenceNameS: s,
			SchemaNameT:   st.SchemaNameT,
			TaskStatus:    constant.TaskDatabaseStatusWaiting,
		})
		if err != nil {
			return err
		}
	}

	_, err = model.GetISequenceMigrateSummaryRW().CreateSequenceMigrateSummary(st.Ctx, &task.SequenceMigrateSummary{
		TaskName:    st.Task.TaskName,
		SchemaNameS: st.SchemaNameS,
		SeqTotals:   uint64(len(seqObjs.SequenceNames)),
		InitFlag:    constant.TaskInitStatusFinished,
		MigrateFlag: constant.TaskMigrateStatusNotFinished,
	})
	if err != nil {
		return err
	}

	st.SequenceReadyInit <- true

	logger.Info("sequence migrate task init finished",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("cost", time.Now().Sub(st.StartTime).String()))
	return nil
}

func (st *StructMigrateTask) processSequenceMigrate() error {
	logger.Info("sequence migrate task run table",
		zap.String("task_name", st.Task.TaskName), zap.String("task_mode", st.Task.TaskMode), zap.String("task_flow", st.Task.TaskFlow))

	s, err := model.GetISequenceMigrateSummaryRW().GetSequenceMigrateSummary(st.Ctx, &task.SequenceMigrateSummary{TaskName: st.Task.TaskName, SchemaNameS: st.SchemaNameS})
	if err != nil {
		return err
	}

	if strings.EqualFold(s.MigrateFlag, constant.TaskMigrateStatusFinished) {
		logger.Warn("sequence migrate task migrate skip",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("init_flag", s.InitFlag),
			zap.String("migrate_flag", s.MigrateFlag),
			zap.String("action", "migrate skip"))
		_, err = model.GetISequenceMigrateSummaryRW().UpdateSequenceMigrateSummary(st.Ctx,
			&task.SequenceMigrateSummary{
				TaskName:    st.Task.TaskName,
				SchemaNameS: st.SchemaNameS},
			map[string]interface{}{
				"MigrateFlag": constant.TaskMigrateStatusFinished,
				"Duration":    fmt.Sprintf("%f", time.Now().Sub(st.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
		return nil
	}

	logger.Info("sequence migrate task get migrate tasks",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow))
	var (
		migrateTasks []*task.SequenceMigrateTask
	)

	err = model.Transaction(st.Ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateTasks, err = model.GetISequenceMigrateTaskRW().QuerySequenceMigrateTask(txnCtx,
			&task.SequenceMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusWaiting})
		if err != nil {
			return err
		}
		migrateFailedTasks, err := model.GetISequenceMigrateTaskRW().QuerySequenceMigrateTask(txnCtx,
			&task.SequenceMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusFailed,
			})
		if err != nil {
			return err
		}
		migrateRunningTasks, err := model.GetISequenceMigrateTaskRW().QuerySequenceMigrateTask(txnCtx,
			&task.SequenceMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusRunning,
			})
		if err != nil {
			return err
		}
		migrateStopTasks, err := model.GetISequenceMigrateTaskRW().QuerySequenceMigrateTask(txnCtx,
			&task.SequenceMigrateTask{
				TaskName:   st.Task.TaskName,
				TaskStatus: constant.TaskDatabaseStatusStopped,
			})
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

	logger.Info("sequence migrate task process migrate sequences",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow))
	g := errconcurrent.NewGroup()
	g.SetLimit(int(st.TaskParams.MigrateThread))

	for _, job := range migrateTasks {
		gTime := time.Now()
		g.Go(job, gTime, func(job interface{}) error {
			smt := job.(*task.SequenceMigrateTask)
			err = model.Transaction(st.Ctx, func(txnCtx context.Context) error {
				_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx, &task.SequenceMigrateTask{
					TaskName:      smt.TaskName,
					SchemaNameS:   smt.SchemaNameS,
					SequenceNameS: smt.SequenceNameS,
				}, map[string]interface{}{
					"TaskStatus": constant.TaskDatabaseStatusRunning,
				})
				if err != nil {
					return err
				}
				_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
					TaskName:    smt.TaskName,
					SchemaNameS: smt.SchemaNameS,
					TableNameS:  smt.SequenceNameS,
					LogDetail: fmt.Sprintf("%v [%v] the struct migrate task [%v] source sequence [%v.%v] starting",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(st.Task.TaskMode),
						smt.TaskName,
						smt.SchemaNameS,
						smt.SequenceNameS),
				})
				if err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}

			seqRes, err := st.DatabaseS.GetDatabaseSequenceName(st.SchemaNameS, smt.SequenceNameS)
			if err != nil {
				return err
			}
			if len(seqRes) == 0 {
				return fmt.Errorf("the database schema_name_s [%s] sequence_name_s [%s] not exist", st.SchemaNameS, s)
			}
			lastNumber, err := stringutil.StrconvIntBitSize(seqRes[0]["LAST_NUMBER"], 64)
			if err != nil {
				return err
			}
			cacheSize, err := stringutil.StrconvIntBitSize(seqRes[0]["CACHE_SIZE"], 64)
			if err != nil {
				return err
			}
			// disable cache
			if cacheSize == 0 {
				lastNumber = lastNumber + 5000
			} else {
				lastNumber = lastNumber + (cacheSize * 2)
			}

			switch st.Task.TaskFlow {
			case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB, constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
				var (
					cycleFlag string
					cacheFlag string
					maxValue  string
				)
				if strings.EqualFold(seqRes[0]["CYCLE_FLAG"], "N") {
					cycleFlag = "NOCYCLE"
				} else {
					cycleFlag = "CYCLE"
				}
				if cacheSize == 0 {
					cacheFlag = "NOCACHE"
				} else {
					cacheFlag = fmt.Sprintf("CACHE %v", seqRes[0]["CACHE_SIZE"])
				}
				if seqRes[0]["MAX_VALUE"] == "9999999999999999999999999999" {
					maxValue = "NOMAXVALUE"
				} else {
					maxValue = fmt.Sprintf("MAXVALUE %v", seqRes[0]["MAX_VALUE"])
				}

				if st.TaskParams.CreateIfNotExist {
					smt.SourceSqlDigest = fmt.Sprintf(`CREATE SEQUENCE %s.%s START %v INCREMENT %v MINVALUE %v %v %v %v;`, st.SchemaNameS, seqRes[0]["SEQUENCE_NAME"], lastNumber, seqRes[0]["INCREMENT_BY"], seqRes[0]["MIN_VALUE"], maxValue, cacheFlag, cycleFlag)
					smt.TargetSqlDigest = fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS %s.%s START %v INCREMENT %v MINVALUE %v %v %v %v;`, st.SchemaNameT, seqRes[0]["SEQUENCE_NAME"], lastNumber, seqRes[0]["INCREMENT_BY"], seqRes[0]["MIN_VALUE"], maxValue, cacheFlag, cycleFlag)
				} else {
					smt.SourceSqlDigest = fmt.Sprintf(`CREATE SEQUENCE %s.%s START %v INCREMENT %v MINVALUE %v %v %v %v;`, st.SchemaNameS, seqRes[0]["SEQUENCE_NAME"], lastNumber, seqRes[0]["INCREMENT_BY"], seqRes[0]["MIN_VALUE"], maxValue, cacheFlag, cycleFlag)
					smt.TargetSqlDigest = fmt.Sprintf(`CREATE SEQUENCE %s.%s START %v INCREMENT %v MINVALUE %v %v %v %v;`, st.SchemaNameT, seqRes[0]["SEQUENCE_NAME"], lastNumber, seqRes[0]["INCREMENT_BY"], seqRes[0]["MIN_VALUE"], maxValue, cacheFlag, cycleFlag)
				}
			default:
				return fmt.Errorf("the task [%v] task_flow [%v] isn't support, please contact author or reselect", st.Task.TaskName, st.Task.TaskFlow)
			}

			err = st.sequenceMigrateStart(smt)
			if err != nil {
				return err
			}
			return nil
		})
	}
	for _, r := range g.Wait() {
		if r.Err != nil {
			smt := r.Task.(*task.SequenceMigrateTask)
			logger.Warn("sequence migrate task",
				zap.String("task_name", st.Task.TaskName),
				zap.String("task_mode", st.Task.TaskMode),
				zap.String("task_flow", st.Task.TaskFlow),
				zap.String("schema_name_s", smt.SchemaNameS),
				zap.String("sequence_name_s", smt.SequenceNameS),
				zap.Error(r.Err))

			errW := model.Transaction(st.Ctx, func(txnCtx context.Context) error {
				_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx,
					&task.SequenceMigrateTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, SequenceNameS: smt.SequenceNameS},
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
					TableNameS:  smt.SequenceNameS,
					LogDetail: fmt.Sprintf("%v [%v] struct migrate task [%v] taskflow [%v] source sequence [%v.%v] failed, please see [sequence_migrate_task] detail",
						stringutil.CurrentTimeFormatString(),
						stringutil.StringLower(st.Task.TaskMode),
						smt.TaskName,
						st.Task.TaskMode,
						smt.SchemaNameS,
						smt.SequenceNameS),
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

	s, err = model.GetISequenceMigrateSummaryRW().GetSequenceMigrateSummary(st.Ctx, &task.SequenceMigrateSummary{TaskName: st.Task.TaskName, SchemaNameS: st.SchemaNameS})
	if err != nil {
		return err
	}
	if s.SeqTotals == s.SeqSuccess {
		_, err = model.GetISequenceMigrateSummaryRW().UpdateSequenceMigrateSummary(st.Ctx,
			&task.SequenceMigrateSummary{
				TaskName:    st.Task.TaskName,
				SchemaNameS: st.SchemaNameS},
			map[string]interface{}{
				"MigrateFlag": constant.TaskMigrateStatusFinished,
				"Duration":    fmt.Sprintf("%f", time.Now().Sub(st.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
	} else {
		_, err = model.GetISequenceMigrateSummaryRW().UpdateSequenceMigrateSummary(st.Ctx,
			&task.SequenceMigrateSummary{
				TaskName:    st.Task.TaskName,
				SchemaNameS: st.SchemaNameS},
			map[string]interface{}{
				"MigrateFlag": constant.TaskMigrateStatusNotFinished,
				"Duration":    fmt.Sprintf("%f", time.Now().Sub(st.StartTime).Seconds()),
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *StructMigrateTask) sequenceMigrateStart(smt *task.SequenceMigrateTask) error {
	startTime := time.Now()
	logger.Info("sequence migrate task process",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("start_time", startTime.String()))

	writerTime := time.Now()
	var w database.ISequenceMigrateDatabaseWriter
	w = NewSequenceMigrateDatabase(st.Ctx, st.Task.TaskName, st.Task.TaskMode, st.Task.TaskFlow, st.DatabaseT, startTime, smt)

	if st.TaskParams.EnableDirectCreate {
		err := w.SyncSequenceDatabase()
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

	err := w.WriteSequenceDatabase()
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
