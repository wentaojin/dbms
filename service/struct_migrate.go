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
package service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/database/oracle"
	"github.com/wentaojin/dbms/logger"

	"github.com/wentaojin/dbms/database/oracle/taskflow"

	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/migrate"
	"github.com/wentaojin/dbms/model/rule"

	"github.com/wentaojin/dbms/model/params"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/model/task"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/proto/pb"
)

func UpsertStructMigrateTask(ctx context.Context, req *pb.UpsertStructMigrateTaskRequest) (string, error) {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err := model.GetITaskRW().CreateTask(txnCtx, &task.Task{
			TaskName:     req.TaskName,
			TaskMode:     constant.TaskModeStructMigrate,
			TaskRuleName: req.TaskRuleName,
			TaskStatus:   constant.TaskDatabaseStatusWaiting,
		})
		if err != nil {
			return err
		}

		fieldInfos := stringutil.GetJSONTagFieldValue(req.StructMigrateParam)
		for jsonTag, fieldValue := range fieldInfos {
			_, err = model.GetIParamsRW().CreateTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:   req.TaskName,
				TaskMode:   constant.TaskModeStructMigrate,
				ParamName:  jsonTag,
				ParamValue: fieldValue,
			})
			if err != nil {
				return err
			}
		}

		for _, r := range req.StructMigrateRule.TaskStructRules {
			if !strings.EqualFold(r.String(), "") {
				_, err = model.GetIStructMigrateTaskRuleRW().CreateTaskStructRule(txnCtx, &migrate.TaskStructRule{
					TaskName:      req.TaskName,
					ColumnTypeS:   r.ColumnTypeS,
					ColumnTypeT:   r.ColumnTypeT,
					DefaultValueS: r.DefaultValueS,
					DefaultValueT: r.DefaultValueT,
				})
				if err != nil {
					return err
				}
			}
		}
		for _, r := range req.StructMigrateRule.SchemaStructRules {
			if !strings.EqualFold(r.String(), "") {
				_, err = model.GetIStructMigrateSchemaRuleRW().CreateSchemaStructRule(txnCtx, &migrate.SchemaStructRule{
					TaskName:      req.TaskName,
					SchemaNameS:   r.SourceSchema,
					ColumnTypeS:   r.ColumnTypeS,
					ColumnTypeT:   r.ColumnTypeT,
					DefaultValueS: r.DefaultValueS,
					DefaultValueT: r.DefaultValueT,
				})
				if err != nil {
					return err
				}
			}
		}
		for _, r := range req.StructMigrateRule.TableStructRules {
			if !strings.EqualFold(r.String(), "") {
				_, err = model.GetIStructMigrateTableRuleRW().CreateTableStructRule(txnCtx, &migrate.TableStructRule{
					TaskName:      req.TaskName,
					SchemaNameS:   r.SourceSchema,
					TableNameS:    r.SourceTable,
					ColumnTypeS:   r.ColumnTypeS,
					ColumnTypeT:   r.ColumnTypeT,
					DefaultValueS: r.DefaultValueS,
					DefaultValueT: r.DefaultValueT,
				})
				if err != nil {
					return err
				}
			}
		}
		for _, r := range req.StructMigrateRule.ColumnStructRules {
			if !strings.EqualFold(r.String(), "") {
				_, err = model.GetIStructMigrateColumnRuleRW().CreateColumnStructRule(txnCtx, &migrate.ColumnStructRule{
					TaskName:      req.TaskName,
					SchemaNameS:   r.SourceSchema,
					TableNameS:    r.SourceTable,
					ColumnNameS:   r.SourceColumn,
					ColumnTypeS:   r.ColumnTypeS,
					ColumnTypeT:   r.ColumnTypeT,
					DefaultValueS: r.DefaultValueS,
					DefaultValueT: r.DefaultValueT,
				})
				if err != nil {
					return err
				}
			}
		}

		for _, r := range req.StructMigrateRule.TableAttrsRules {
			if !strings.EqualFold(r.String(), "") {
				for _, t := range r.SourceTables {
					_, err = model.GetIStructMigrateTableAttrsRuleRW().CreateTableAttrsRule(txnCtx, &migrate.TableAttrsRule{
						TaskName:    req.TaskName,
						SchemaNameS: r.SourceSchema,
						TableNameS:  t,
						TableAttrT:  r.TableAttrsT,
					})
				}
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	jsonStr, err := stringutil.MarshalJSON(req)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func DeleteStructMigrateTask(ctx context.Context, req *pb.DeleteStructMigrateTaskRequest) (string, error) {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		err := model.GetITaskRW().DeleteTask(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIParamsRW().DeleteTaskCustomParam(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateTaskRuleRW().DeleteTaskStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateSchemaRuleRW().DeleteSchemaStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateTableRuleRW().DeleteTableStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateColumnRuleRW().DeleteColumnStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIStructMigrateTableAttrsRuleRW().DeleteTableAttrsRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	jsonStr, err := stringutil.MarshalJSON(req)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func ShowStructMigrateTask(ctx context.Context, req *pb.ShowStructMigrateTaskRequest) (string, error) {
	var (
		resp  *pb.UpsertStructMigrateTaskRequest
		param *pb.StructMigrateParam
		rules *pb.StructMigrateRule
	)

	err := model.Transaction(ctx, func(txnCtx context.Context) error {

		taskInfo, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		paramsInfo, err := model.GetIParamsRW().QueryTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName: req.TaskName,
			TaskMode: constant.TaskModeStructMigrate,
		})
		if err != nil {
			return err
		}

		paramMap := make(map[string]string)
		for _, p := range paramsInfo {
			paramMap[p.ParamName] = p.ParamValue
		}

		migrateThread, err := strconv.Atoi(paramMap[constant.ParamNameStructMigrateMigrateThread])
		if err != nil {
			return err
		}

		taskQueueSize, err := strconv.Atoi(paramMap[constant.ParamNameStructMigrateTaskQueueSize])
		if err != nil {
			return err
		}

		directWrite, err := strconv.ParseBool(paramMap[constant.ParamNameStructMigrateDirectWrite])
		if err != nil {
			return err
		}

		createIfNotExist, err := strconv.ParseBool(paramMap[constant.ParamNameStructMigrateCreateIfNotExist])
		if err != nil {
			return err
		}

		param = &pb.StructMigrateParam{
			CaseFieldRule:    paramMap[constant.ParamNameStructMigrateCaseFieldRule],
			MigrateThread:    uint64(migrateThread),
			TaskQueueSize:    uint64(taskQueueSize),
			DirectWrite:      directWrite,
			CreateIfNotExist: createIfNotExist,
			OutputDir:        paramMap[constant.ParamNameStructMigrateOutputDir],
		}

		taskRules, err := model.GetIStructMigrateTaskRuleRW().QueryTaskStructRule(txnCtx, &migrate.TaskStructRule{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		for _, d := range taskRules {
			rules.TaskStructRules = append(rules.TaskStructRules, &pb.TaskStructRule{
				ColumnTypeS:   d.ColumnTypeS,
				ColumnTypeT:   d.ColumnTypeT,
				DefaultValueS: d.DefaultValueS,
				DefaultValueT: d.DefaultValueT,
			})
		}

		schemaRules, err := model.GetIStructMigrateSchemaRuleRW().FindSchemaStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}

		for _, d := range schemaRules {
			rules.SchemaStructRules = append(rules.SchemaStructRules, &pb.SchemaStructRule{
				SourceSchema:  d.SchemaNameS,
				ColumnTypeS:   d.ColumnTypeS,
				ColumnTypeT:   d.ColumnTypeT,
				DefaultValueS: d.DefaultValueS,
				DefaultValueT: d.DefaultValueT,
			})
		}

		tableRules, err := model.GetIStructMigrateTableRuleRW().FindTableStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}

		for _, d := range tableRules {
			rules.TableStructRules = append(rules.TableStructRules, &pb.TableStructRule{
				SourceSchema:  d.SchemaNameS,
				SourceTable:   d.TableNameS,
				ColumnTypeS:   d.ColumnTypeS,
				ColumnTypeT:   d.ColumnTypeT,
				DefaultValueS: d.DefaultValueS,
				DefaultValueT: d.DefaultValueT,
			})
		}

		columnRules, err := model.GetIStructMigrateColumnRuleRW().FindColumnStructRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		for _, d := range columnRules {
			rules.ColumnStructRules = append(rules.ColumnStructRules, &pb.ColumnStructRule{
				SourceSchema:  d.SchemaNameS,
				SourceTable:   d.TableNameS,
				SourceColumn:  d.ColumnNameS,
				ColumnTypeS:   d.ColumnTypeS,
				ColumnTypeT:   d.ColumnTypeT,
				DefaultValueS: d.DefaultValueS,
				DefaultValueT: d.DefaultValueT,
			})
		}

		tableAttrRules, err := model.GetIStructMigrateTableAttrsRuleRW().FindTableAttrsRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}

		schemaUniqMap := make(map[string]struct{})
		for _, t := range tableAttrRules {
			schemaUniqMap[t.SchemaNameS] = struct{}{}
		}

		for s, _ := range schemaUniqMap {
			var (
				sourceTables []string
				tableAttrT   string
			)
			for _, t := range tableAttrRules {
				if strings.EqualFold(s, t.SchemaNameS) {
					sourceTables = append(sourceTables, t.TableNameS)
				}
				tableAttrT = t.TableAttrT
			}
			rules.TableAttrsRules = append(rules.TableAttrsRules, &pb.TableAttrsRule{
				SourceSchema: s,
				SourceTables: sourceTables,
				TableAttrsT:  tableAttrT,
			})
		}
		resp = &pb.UpsertStructMigrateTaskRequest{
			TaskName:           taskInfo.TaskName,
			TaskRuleName:       taskInfo.TaskRuleName,
			StructMigrateParam: param,
			StructMigrateRule:  rules,
		}

		return nil
	})
	if err != nil {
		return "", err
	}

	jsonStr, err := stringutil.MarshalJSON(resp)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func StartStructMigrateTask(ctx context.Context, taskName, workerAddr string) error {
	startTime := time.Now()
	logger.Info("struct migrate task start", zap.String("task_name", taskName))

	_, err := model.GetITaskRW().UpdateTask(ctx, &task.Task{
		TaskName: taskName,
	}, map[string]interface{}{
		"WorkerAddr": workerAddr,
		"TaskStatus": constant.TaskDatabaseStatusRunning,
		"StartTime":  startTime,
	})
	if err != nil {
		return err
	}

	logger.Info("struct migrate task get params", zap.String("task_name", taskName))
	taskParams, err := getStructMigrateTasKParams(ctx, taskName)
	if err != nil {
		return err
	}

	logger.Info("struct migrate task init task", zap.String("task_name", taskName))
	err = initStructMigrateTask(ctx, taskName, taskParams.CaseFieldRule)
	if err != nil {
		return err
	}

	logger.Info("struct migrate task get tasks", zap.String("task_name", taskName))
	var migrateTasks []*task.StructMigrateTask
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateTasks, err = model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx, &task.StructMigrateTask{TaskName: taskName, TaskStatus: constant.TaskDatabaseStatusWaiting, IsSchemaCreate: constant.DatabaseIsSchemaCreateSqlNO})
		if err != nil {
			return err
		}
		migrateFailedTasks, err := model.GetIStructMigrateTaskRW().QueryStructMigrateTask(txnCtx, &task.StructMigrateTask{TaskName: taskName, TaskStatus: constant.TaskDatabaseStatusFailed, IsSchemaCreate: constant.DatabaseIsSchemaCreateSqlNO})
		if err != nil {
			return err
		}
		migrateTasks = append(migrateTasks, migrateFailedTasks...)
		return nil
	})
	if err != nil {
		return err
	}

	// get datasource
	var (
		taskInfo         *task.Task
		sourceDatasource *datasource.Datasource
		targetDatasource *datasource.Datasource
	)
	logger.Info("struct migrate task get datasource", zap.String("task_name", taskName))
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName})
		if err != nil {
			return err
		}
		taskRule, err := model.GetIRuleRW().GetRule(txnCtx, &rule.Rule{TaskRuleName: taskInfo.TaskRuleName})
		if err != nil {
			return err
		}
		sourceDatasource, err = model.GetIDatasourceRW().GetDatasource(txnCtx, taskRule.DatasourceNameS)
		if err != nil {
			return err
		}
		targetDatasource, err = model.GetIDatasourceRW().GetDatasource(txnCtx, taskRule.DatasourceNameT)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	taskFlow := stringutil.StringBuilder(stringutil.StringUpper(sourceDatasource.DbType), constant.StringSeparatorAite, stringutil.StringUpper(targetDatasource.DbType))

	if strings.EqualFold(sourceDatasource.DbType, constant.DatabaseTypeOracle) {
		logger.Info("struct migrate task process task", zap.String("task_name", taskName))
		startTime := time.Now()
		taskStruct := &taskflow.StructMigrateTask{
			Ctx:          ctx,
			TaskName:     taskName,
			TaskRuleName: taskInfo.TaskRuleName,
			TaskFlow:     taskFlow,
			MigrateTasks: migrateTasks,
			DatasourceS:  sourceDatasource,
			DatasourceT:  targetDatasource,
			TaskParams:   taskParams,
		}
		err = taskStruct.Start()
		if err != nil {
			return err
		}
		logger.Info("struct migrate task process task",
			zap.String("task_name", taskName),
			zap.String("cost", time.Now().Sub(startTime).String()))
	} else {
		return fmt.Errorf("current struct migrate task [%s] datasource [%s] source [%s] isn't support, please contact auhtor or reselect", taskName, sourceDatasource.DatasourceName, sourceDatasource.DbType)
	}

	// status
	var (
		migrateFailedResults []*task.StructMigrateTask
		migrateWaitResults   []*task.StructMigrateTask
		migrateRunResults    []*task.StructMigrateTask
	)

	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		// get migrate task tables
		migrateFailedResults, err = model.GetIStructMigrateTaskRW().FindStructMigrateTask(txnCtx, &task.StructMigrateTask{TaskName: taskName,
			TaskStatus: constant.TaskDatabaseStatusFailed})
		if err != nil {
			return err
		}

		migrateWaitResults, err = model.GetIStructMigrateTaskRW().FindStructMigrateTask(txnCtx, &task.StructMigrateTask{TaskName: taskName,
			TaskStatus: constant.TaskDatabaseStatusWaiting})
		if err != nil {
			return err
		}
		migrateRunResults, err = model.GetIStructMigrateTaskRW().FindStructMigrateTask(txnCtx, &task.StructMigrateTask{TaskName: taskName,
			TaskStatus: constant.TaskDatabaseStatusRunning})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	if len(migrateFailedResults) > 0 || len(migrateWaitResults) > 0 || len(migrateRunResults) > 0 {
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			_, err = model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
				TaskName: taskName,
			}, map[string]interface{}{
				"TaskStatus": constant.TaskDatabaseStatusFailed,
				"EndTime":    time.Now(),
			})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName: taskName,
				LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] are exist failed [%d] or waiting [%d] or running [%d] status records during running operation, please see [struct_migrate_task] detail",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeStructMigrate),
					taskInfo.WorkerAddr,
					taskName,
					len(migrateFailedResults),
					len(migrateWaitResults),
					len(migrateRunResults)),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}

		logger.Info("struct migrate task failed",
			zap.String("task_name", taskName),
			zap.Int("total records", len(migrateTasks)),
			zap.Int("failed records", len(migrateFailedResults)),
			zap.Int("wait records", len(migrateWaitResults)),
			zap.Int("running records", len(migrateRunResults)),
			zap.String("detail tips", "please see [struct_migrate_task] detail"),
			zap.String("cost", time.Now().Sub(startTime).String()))
		return nil
	}
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err = model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskName,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusSuccess,
			"EndTime":    time.Now(),
		})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName: taskName,
			LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] running success, cost: [%v]",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeStructMigrate),
				taskInfo.WorkerAddr,
				taskName,
				time.Now().Sub(startTime).String()),
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("struct migrate task success",
		zap.String("task_name", taskName),
		zap.Int("total records", len(migrateTasks)),
		zap.String("detail tips", "please see [struct_migrate_task] detail"),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func StopStructMigrateTask(ctx context.Context, taskName string) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err := model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskName,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusStopped,
		})
		if err != nil {
			return err
		}
		_, err = model.GetIStructMigrateTaskRW().BatchUpdateStructMigrateTask(txnCtx, &task.StructMigrateTask{
			TaskName:   taskName,
			TaskStatus: constant.TaskDatabaseStatusRunning,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusStopped,
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func getStructMigrateTasKParams(ctx context.Context, taskName string) (*pb.StructMigrateParam, error) {
	taskParam := &pb.StructMigrateParam{}

	migrateParams, err := model.GetIParamsRW().QueryTaskCustomParam(ctx, &params.TaskCustomParam{
		TaskName: taskName,
		TaskMode: constant.TaskModeStructMigrate,
	})
	if err != nil {
		return taskParam, err
	}
	for _, p := range migrateParams {
		if strings.EqualFold(p.ParamName, constant.ParamNameStructMigrateCaseFieldRule) {
			taskParam.CaseFieldRule = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStructMigrateOutputDir) {
			taskParam.OutputDir = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStructMigrateMigrateThread) {
			migrateThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.MigrateThread = migrateThread
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStructMigrateTaskQueueSize) {
			taskQueueSize, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.TaskQueueSize = taskQueueSize
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStructMigrateCreateIfNotExist) {
			createIfNotExist, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.CreateIfNotExist = createIfNotExist
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStructMigrateDirectWrite) {
			directBool, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.DirectWrite = directBool
		}
	}
	return taskParam, nil
}

func initStructMigrateTask(ctx context.Context, taskName string, caseFieldRule string) error {
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName})
	if err != nil {
		return err
	}

	schemaRoutes, err := model.GetIMigrateSchemaRouteRW().FindSchemaRouteRule(ctx, &rule.SchemaRouteRule{TaskRuleName: taskInfo.TaskRuleName})
	if err != nil {
		return err
	}

	for _, schemaRoute := range schemaRoutes {
		// get datasource
		taskRule, err := model.GetIRuleRW().GetRule(ctx, &rule.Rule{TaskRuleName: schemaRoute.TaskRuleName})
		if err != nil {
			return err
		}

		// create source database conn
		sourceDatasource, err := model.GetIDatasourceRW().GetDatasource(ctx, taskRule.DatasourceNameS)
		if err != nil {
			return err
		}

		sourceDB, err := database.NewDatabase(ctx, sourceDatasource, schemaRoute.SchemaNameS)
		if err != nil {
			return err
		}

		// filter database table
		schemaTaskTables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(ctx, &rule.MigrateTaskTable{
			TaskRuleName: schemaRoute.TaskRuleName,
			SchemaNameS:  schemaRoute.SchemaNameS,
		})
		if err != nil {
			return err
		}
		var (
			includeTables  []string
			excludeTables  []string
			databaseTables []string // task tables
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

		switch {
		case strings.EqualFold(sourceDatasource.DbType, constant.DatabaseTypeOracle):
			oracleDB := sourceDB.(*oracle.Database)
			databaseTables, err = oracleDB.FilterDatabaseTable(schemaRoute.SchemaNameS, includeTables, excludeTables)
			if err != nil {
				return err
			}
			databaseTableTypeMap, err = oracleDB.GetDatabaseTableType(schemaRoute.SchemaNameS)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("the current datasource type [%s] isn't support, please remove and reruning", sourceDatasource.DbType)
		}

		err = sourceDB.Close()
		if err != nil {
			return err
		}

		// get table route rule
		tableRouteRule := make(map[string]string)

		tableRoutes, err := model.GetIMigrateTableRouteRW().FindTableRouteRule(ctx, &rule.TableRouteRule{
			TaskRuleName: schemaRoute.TaskRuleName,
			SchemaNameS:  schemaRoute.SchemaNameS,
		})
		for _, tr := range tableRoutes {
			tableRouteRule[tr.TableNameS] = tr.TableNameT
		}

		// database tables
		// init database table
		// get table column route rule
		for _, sourceTable := range databaseTables {
			var (
				targetSchema string
				targetTable  string
			)
			if val, ok := tableRouteRule[sourceTable]; ok {
				targetTable = val
			} else {
				targetTable = sourceTable
			}

			if strings.EqualFold(caseFieldRule, constant.ParamValueStructMigrateCaseFieldNameLower) {
				targetSchema = strings.ToLower(schemaRoute.SchemaNameT)
				targetTable = strings.ToLower(targetTable)
			}
			if strings.EqualFold(caseFieldRule, constant.ParamValueStructMigrateCaseFieldNameUpper) {
				targetSchema = strings.ToUpper(schemaRoute.SchemaNameT)
				targetTable = strings.ToUpper(targetTable)
			}
			if strings.EqualFold(caseFieldRule, constant.ParamValueStructMigrateCaseFieldNameOrigin) {
				targetSchema = schemaRoute.SchemaNameT
			}

			_, err = model.GetIStructMigrateTaskRW().CreateStructMigrateTask(ctx, &task.StructMigrateTask{
				TaskName:    taskName,
				SchemaNameS: schemaRoute.SchemaNameS,
				TableNameS:  sourceTable,
				TableTypeS:  databaseTableTypeMap[sourceTable],
				SchemaNameT: targetSchema,
				TableNameT:  targetTable,
				TaskStatus:  constant.TaskDatabaseStatusWaiting,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
