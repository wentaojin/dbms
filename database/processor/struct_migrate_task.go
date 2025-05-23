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
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/thread"
	"golang.org/x/sync/errgroup"

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

	Progress *Progress
}

func (st *StructMigrateTask) Init() error {
	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "init migrate record"))

	gSeq := &errgroup.Group{}

	gSeq.Go(func() error {
		err := st.initSequenceMigrate()
		if err != nil {
			return err
		}
		return nil
	})
	if err := gSeq.Wait(); err != nil {
		return err
	}

	// print struct migrate progress
	go st.Progress.PrintProgress()

	gStru := &errgroup.Group{}

	gStru.Go(func() error {
		err := st.initStructMigrate()
		if err != nil {
			return err
		}
		return nil
	})

	if err := gStru.Wait(); err != nil {
		return err
	}
	return nil
}

func (st *StructMigrateTask) Run() error {
	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "process migrate record"))

	gSeq := &errgroup.Group{}

	gSeq.Go(func() error {
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

	if err := gSeq.Wait(); err != nil {
		return err
	}

	gStru := &errgroup.Group{}

	gStru.Go(func() error {
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

	if err := gStru.Wait(); err != nil {
		return err
	}
	return nil
}

func (st *StructMigrateTask) Resume() error {
	return nil
}

func (st *StructMigrateTask) Last() error {
	return nil
}

func (st *StructMigrateTask) initStructMigrate() error {
	defer close(st.StructReadyInit)

	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "struct migrate init"))

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
		logger.Info("struct migrate task processor",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("schema_name_t", st.SchemaNameT),
			zap.String("detail", "config file schema-route-table include-table-s/exclude-table-s params are null, skip struct migrate"),
			zap.String("task_stage", "skip struct migrate"),
			zap.String("cost", time.Since(st.StartTime).String()))
		st.StructReadyInit <- false
		return nil
	}

	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "query schema tables"))

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
	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "query table routes"))

	tableRouteRule := make(map[string]string)

	tableRoutes, err := model.GetIMigrateTableRouteRW().FindTableRouteRule(st.Ctx, &rule.TableRouteRule{
		TaskName:    st.Task.TaskName,
		SchemaNameS: st.SchemaNameS,
	})
	for _, tr := range tableRoutes {
		tableRouteRule[tr.TableNameS] = tr.TableNameT
	}

	// clear the struct migrate task table
	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "delete repeated tables"))

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

	st.Progress.SetTableCounts(uint64(len(databaseTaskTables)))

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

	_, err = model.GetITaskLogRW().CreateLog(st.Ctx, &task.Log{
		TaskName: st.Task.TaskName,
		LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] schema_name_s [%s] init struct migrate record finished",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeStructMigrate),
			st.Task.TaskName,
			st.Task.TaskFlow,
			st.SchemaNameS,
			st.Task.WorkerAddr,
		),
	})
	if err != nil {
		return err
	}
	logger.Warn("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "init struct records finished"),
		zap.String("cost", time.Since(st.StartTime).String()))

	return nil
}

func (st *StructMigrateTask) processStructMigrate() error {
	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "process struct migrate"))

	s, err := model.GetIStructMigrateSummaryRW().GetStructMigrateSummary(st.Ctx, &task.StructMigrateSummary{TaskName: st.Task.TaskName, SchemaNameS: st.SchemaNameS})
	if err != nil {
		return err
	}

	if strings.EqualFold(s.MigrateFlag, constant.TaskMigrateStatusFinished) {
		logger.Info("struct migrate task processor",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("schema_name_t", st.SchemaNameT),
			zap.String("init_flag", s.InitFlag),
			zap.String("migrate_flag", s.MigrateFlag),
			zap.String("task_stage", "skip struct migrate"))

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

		st.Progress.UpdateTableNameProcessed(1)
		return nil
	}

	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "query struct records"))
	var (
		migrateTasks []*task.StructMigrateTask
		successTasks []*task.StructMigrateTask
	)

	err = model.Transaction(st.Ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateTasks, err = model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{TaskName: st.Task.TaskName},
			[]string{constant.TaskDatabaseStatusWaiting, constant.TaskDatabaseStatusFailed, constant.TaskDatabaseStatusRunning, constant.TaskDatabaseStatusStopped})
		if err != nil {
			return err
		}
		successTasks, err = model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx,
			&task.StructMigrateTask{TaskName: st.Task.TaskName},
			[]string{constant.TaskDatabaseStatusSuccess})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	successTableNums := len(successTasks)
	if successTableNums > 0 {
		st.Progress.UpdateTableNameProcessed(uint64(successTableNums))
		st.Progress.SetLastTableNameProcessed(uint64(successTableNums))
	}

	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "struct concurrency start"))

	g := thread.NewGroup()
	g.SetLimit(int(st.TaskParams.MigrateThread))

	go func() {
		for _, job := range migrateTasks {
			g.Go(job, func(job interface{}) error {
				smt := job.(*task.StructMigrateTask)
				err = st.structMigrateStart(smt)
				if err != nil {
					return err
				}
				return nil
			})
		}
		g.Wait()
	}()

	for res := range g.ResultC {
		st.Progress.UpdateTableNameProcessed(1)
		if res.Error != nil {
			smt := res.Task.(*task.StructMigrateTask)
			logger.Error("struct migrate task processor",
				zap.String("task_name", st.Task.TaskName),
				zap.String("task_mode", st.Task.TaskMode),
				zap.String("task_flow", st.Task.TaskFlow),
				zap.String("schema_name_s", st.SchemaNameS),
				zap.String("schema_name_t", st.SchemaNameT),
				zap.String("task_stage", "struct concurrency failed"),
				zap.Error(res.Error))

			if err := thread.Retry(
				&thread.RetryConfig{
					MaxRetries: thread.DefaultThreadErrorMaxRetries,
					Delay:      thread.DefaultThreadErrorRereyDelay,
				},
				func(err error) bool {
					return true
				},
				func() error {
					errW := model.Transaction(st.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetIStructMigrateTaskRW().UpdateStructMigrateTask(txnCtx,
							&task.StructMigrateTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, TableNameS: smt.TableNameS},
							map[string]interface{}{
								"TaskStatus":  constant.TaskDatabaseStatusFailed,
								"Duration":    res.Duration,
								"ErrorDetail": res.Error.Error(),
							})
						if err != nil {
							return err
						}
						_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
							TaskName: st.Task.TaskName,
							LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] schema_name_s [%s] table_name_s [%s] migrate failed, please see [struct_migrate_task] detail",
								stringutil.CurrentTimeFormatString(),
								stringutil.StringLower(constant.TaskModeStructMigrate),
								st.Task.TaskName,
								st.Task.TaskFlow,
								st.Task.WorkerAddr,
								st.SchemaNameS,
								smt.TableNameS,
							),
						})
						if err != nil {
							return err
						}
						return nil
					})
					if errW != nil {
						return errW
					}
					return nil
				}); err != nil {
				return err
			}
		}
	}

	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "struct concurrency done"))
	return nil
}

func (st *StructMigrateTask) structMigrateStart(smt *task.StructMigrateTask) error {
	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.Uint64("struct_migrate_id", smt.ID))

	// if the schema table success, skip
	if strings.EqualFold(smt.TaskStatus, constant.TaskDatabaseStatusSuccess) {
		logger.Warn("struct migrate task processor",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("schema_name_t", st.SchemaNameT),
			zap.String("task_stage", "the task migrate has success, skip the struct migrate"),
			zap.String("cost", time.Since(st.StartTime).String()))
		return nil
	}
	// if the table is MATERIALIZED VIEW, SKIP
	// MATERIALIZED VIEW isn't support struct migrate
	if strings.EqualFold(smt.TableTypeS, constant.DatabaseMigrateTableStructDisabledMaterializedView) {
		logger.Error("struct migrate task processor",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("schema_name_t", st.SchemaNameT),
			zap.String("task_stage", "the materialized view not support, skip the migrate, if necessary, please manually process the tables in the above list"),
			zap.String("cost", time.Since(st.StartTime).String()))
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
			TaskName: st.Task.TaskName,
			LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] schema_name_s [%s] table_name_s [%s] migrate start",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeStructMigrate),
				st.Task.TaskName,
				st.Task.TaskFlow,
				st.Task.WorkerAddr,
				st.SchemaNameS,
				smt.TableNameS,
			),
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

	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("datasource", datasourceS.String()),
		zap.String("task_stage", "struct attribute gen"),
		zap.String("cost", time.Since(sourceTime).String()))

	logger.Debug("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("datasource", datasourceS.String()),
		zap.String("arrtibutes", attrs.String()),
		zap.String("task_stage", "struct attribute gen"),
		zap.String("cost", time.Since(sourceTime).String()))

	ruleTime := time.Now()
	dataRule := &StructMigrateRule{
		Ctx:                      st.Ctx,
		TaskName:                 smt.TaskName,
		TaskFlow:                 st.Task.TaskFlow,
		SchemaNameS:              smt.SchemaNameS,
		TableNameS:               smt.TableNameS,
		TableCharsetAttr:         attrs.TableCharset,
		TableCollationAttr:       attrs.TableCollation,
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

	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "struct rule prepare"),
		zap.String("cost", time.Since(ruleTime).String()))

	logger.Debug("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("migrate_rules", dataRule.String()),
		zap.String("task_stage", "struct rule prepare"),
		zap.String("cost", time.Since(ruleTime).String()))

	rules, err := database.IStructMigrateAttributesRule(dataRule)
	if err != nil {
		return err
	}
	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "struct rule gen"),
		zap.String("cost", time.Since(ruleTime).String()))

	logger.Debug("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("struct_rules", rules.String()),
		zap.String("task_stage", "struct rule gen"),
		zap.String("cost", time.Since(ruleTime).String()))

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
	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "struct table gen"),
		zap.String("cost", time.Since(tableTime).String()))

	logger.Debug("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("struct_table", tableStruct.String()),
		zap.String("task_stage", "struct table gen"),
		zap.String("cost", time.Since(tableTime).String()))

	writerTime := time.Now()
	var w database.IStructMigrateDatabaseWriter
	w = NewStructMigrateDatabase(st.Ctx, smt.TaskName, st.Task.TaskFlow, st.DatabaseT, st.StartTime, tableStruct)

	if st.TaskParams.EnableDirectCreate {
		err = w.SyncStructDatabase()
		if err != nil {
			return err
		}
		logger.Info("struct migrate task processor",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("schema_name_t", st.SchemaNameT),
			zap.String("task_stage", "struct sync database"),
			zap.String("cost", time.Since(writerTime).String()))

		return nil
	}

	err = w.WriteStructDatabase()
	if err != nil {
		return err
	}
	logger.Info("struct migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "struct write database"),
		zap.String("cost", time.Since(writerTime).String()))
	return nil
}

func (st *StructMigrateTask) initSequenceMigrate() error {
	defer close(st.SequenceReadyInit)

	logger.Info("sequence migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "sequence migrate init"))

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
		logger.Warn("sequence migrate task processor",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("schema_name_t", st.SchemaNameT),
			zap.String("detail", "config file schema-route-table include-sequence-s/exclude-sequence-s params are null, skip sequence migrate"),
			zap.String("task_stage", "skip sequence migrate"),
			zap.String("cost", time.Since(st.StartTime).String()))

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
	logger.Warn("sequence migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "sequence records clear"))

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

	_, err = model.GetITaskLogRW().CreateLog(st.Ctx, &task.Log{
		TaskName: st.Task.TaskName,
		LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] schema_name_s [%s] sequence migrate record init done",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeStructMigrate),
			st.Task.TaskName,
			st.Task.TaskFlow,
			st.Task.WorkerAddr,
			st.SchemaNameS,
		),
	})
	if err != nil {
		return err
	}
	logger.Warn("sequence migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "sequence records done"),
		zap.String("cost", time.Since(st.StartTime).String()))
	return nil
}

func (st *StructMigrateTask) processSequenceMigrate() error {
	logger.Warn("sequence migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "sequence migrate start"))

	s, err := model.GetISequenceMigrateSummaryRW().GetSequenceMigrateSummary(st.Ctx, &task.SequenceMigrateSummary{TaskName: st.Task.TaskName, SchemaNameS: st.SchemaNameS})
	if err != nil {
		return err
	}

	if strings.EqualFold(s.MigrateFlag, constant.TaskMigrateStatusFinished) {
		logger.Warn("sequence migrate task processor",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("schema_name_t", st.SchemaNameT),
			zap.String("init_flag", s.InitFlag),
			zap.String("migrate_flag", s.MigrateFlag),
			zap.String("task_stage", "sequence migrate skip"))
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

	logger.Warn("sequence migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "sequence records get"))
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

	logger.Warn("sequence migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "sequence concurrency start"))

	g := thread.NewGroup()
	g.SetLimit(int(st.TaskParams.MigrateThread))

	go func() {
		for _, job := range migrateTasks {
			g.Go(job, func(job interface{}) error {
				smt := job.(*task.SequenceMigrateTask)

				logger.Warn("sequence migrate task processor",
					zap.String("task_name", st.Task.TaskName),
					zap.String("task_mode", st.Task.TaskMode),
					zap.String("task_flow", st.Task.TaskFlow),
					zap.String("schema_name_s", st.SchemaNameS),
					zap.String("sequence_name_s", smt.SequenceNameS),
					zap.String("schema_name_t", st.SchemaNameT),
					zap.String("task_stage", "sequence record processing"))

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
						TaskName: st.Task.TaskName,
						LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] schema_name_s [%s] sequence_name_s [%s] migrate starting",
							stringutil.CurrentTimeFormatString(),
							stringutil.StringLower(constant.TaskModeSequenceMigrate),
							st.Task.TaskName,
							st.Task.TaskFlow,
							st.Task.WorkerAddr,
							st.SchemaNameS,
							smt.SequenceNameS,
						),
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
					return fmt.Errorf("the database schema_name_s [%s] sequence_name_s [%s] not exist", st.SchemaNameS, smt.SequenceNameS)
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
					maxVal, err := decimal.NewFromString(seqRes[0]["MAX_VALUE"])
					if err != nil {
						return fmt.Errorf("parse sequence string value [%s] failed: %v", seqRes[0]["MAX_VALUE"], err)
					}
					// mysql compatible sequence max value is math.MaxInt64 (bigint)
					// build report
					// github.com/scylladb/go-set@v1.0.2 cannot use math.MaxInt64 (untyped int constant 9223372036854775807) as int value in assignment (overflows)
					// upgrade github.com/scylladb/go-set@master build normal, but dbms-master glibc version require `GLIBC_2.38' (./dbms-master: /lib64/libc.so.6: version `GLIBC_2.38' not found (required by ./dbms-master))
					// ubuntu build environment 18.04 is OK
					maxBigint, err := decimal.NewFromString(strconv.FormatInt(math.MaxInt64, 10))
					if err != nil {
						return fmt.Errorf("parse max int64 string value [%s] failed: %v", strconv.FormatInt(math.MaxInt64, 10), err)
					}
					if maxVal.GreaterThanOrEqual(maxBigint) {
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

				logger.Warn("sequence migrate task processor",
					zap.String("task_name", st.Task.TaskName),
					zap.String("task_mode", st.Task.TaskMode),
					zap.String("task_flow", st.Task.TaskFlow),
					zap.String("schema_name_s", st.SchemaNameS),
					zap.String("sequence_name_s", smt.SequenceNameS),
					zap.String("schema_name_t", st.SchemaNameT),
					zap.String("task_stage", "sequence record processed"))
				return nil
			})
		}
		g.Wait()
	}()

	for res := range g.ResultC {
		if res.Error != nil {
			smt := res.Task.(*task.SequenceMigrateTask)
			logger.Error("sequence migrate task processor",
				zap.String("task_name", st.Task.TaskName),
				zap.String("task_mode", st.Task.TaskMode),
				zap.String("task_flow", st.Task.TaskFlow),
				zap.String("schema_name_s", st.SchemaNameS),
				zap.String("sequence_name_s", smt.SequenceNameS),
				zap.String("schema_name_t", st.SchemaNameT),
				zap.String("task_stage", "sequence migrate error"),
				zap.Error(res.Error))

			if err := thread.Retry(
				&thread.RetryConfig{
					MaxRetries: thread.DefaultThreadErrorMaxRetries,
					Delay:      thread.DefaultThreadErrorRereyDelay,
				},
				func(err error) bool {
					return true
				},
				func() error {
					errW := model.Transaction(st.Ctx, func(txnCtx context.Context) error {
						_, err = model.GetISequenceMigrateTaskRW().UpdateSequenceMigrateTask(txnCtx,
							&task.SequenceMigrateTask{TaskName: smt.TaskName, SchemaNameS: smt.SchemaNameS, SequenceNameS: smt.SequenceNameS},
							map[string]interface{}{
								"TaskStatus":  constant.TaskDatabaseStatusFailed,
								"Duration":    res.Duration,
								"ErrorDetail": res.Error.Error(),
							})
						if err != nil {
							return err
						}
						_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
							TaskName: st.Task.TaskName,
							LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] schema_name_s [%s] sequence_name_s [%s] migrate failed, please see [sequence_migrate_task] detail",
								stringutil.CurrentTimeFormatString(),
								stringutil.StringLower(constant.TaskModeSequenceMigrate),
								st.Task.TaskName,
								st.Task.TaskFlow,
								st.Task.WorkerAddr,
								st.SchemaNameS,
								smt.SequenceNameS,
							),
						})
						if err != nil {
							return err
						}
						return nil
					})
					if errW != nil {
						return errW
					}
					return nil
				}); err != nil {
				return err
			}
		}
	}

	logger.Warn("sequence migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.String("task_stage", "sequence concurrency done"))
	return nil
}

func (st *StructMigrateTask) sequenceMigrateStart(smt *task.SequenceMigrateTask) error {
	startTime := time.Now()
	var w database.ISequenceMigrateDatabaseWriter
	w = NewSequenceMigrateDatabase(st.Ctx, st.Task.TaskName, st.Task.TaskMode, st.Task.TaskFlow, st.DatabaseT, startTime, smt)

	if st.TaskParams.EnableDirectCreate {
		err := w.SyncSequenceDatabase()
		if err != nil {
			return err
		}

		logger.Warn("sequence migrate task processor",
			zap.String("task_name", st.Task.TaskName),
			zap.String("task_mode", st.Task.TaskMode),
			zap.String("task_flow", st.Task.TaskFlow),
			zap.String("schema_name_s", st.SchemaNameS),
			zap.String("schema_name_t", st.SchemaNameT),
			zap.Bool("enable_direct_create", st.TaskParams.EnableDirectCreate),
			zap.String("task_stage", "sync sequence database"),
			zap.String("cost", time.Since(startTime).String()))
		return nil
	}

	err := w.WriteSequenceDatabase()
	if err != nil {
		return err
	}
	logger.Warn("sequence migrate task processor",
		zap.String("task_name", st.Task.TaskName),
		zap.String("task_mode", st.Task.TaskMode),
		zap.String("task_flow", st.Task.TaskFlow),
		zap.String("schema_name_s", st.SchemaNameS),
		zap.String("schema_name_t", st.SchemaNameT),
		zap.Bool("enable_direct_create", st.TaskParams.EnableDirectCreate),
		zap.String("task_stage", "write sequence database"),
		zap.String("cost", time.Since(startTime).String()))
	return nil
}
