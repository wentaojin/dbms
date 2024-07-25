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
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/database/oracle/taskflow"
	"github.com/wentaojin/dbms/database/processor"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/common"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/params"
	"github.com/wentaojin/dbms/model/rule"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/etcdutil"
	"github.com/wentaojin/dbms/utils/stringutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"
)

func PromptDataScanTask(ctx context.Context, taskName, serverAddr string) error {
	etcdClient, err := etcdutil.CreateClient(ctx, []string{stringutil.WithHostPort(serverAddr)}, nil)
	if err != nil {
		return err
	}
	keyResp, err := etcdutil.GetKey(etcdClient, constant.DefaultMasterDatabaseDBMSKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	switch {
	case len(keyResp.Kvs) > 1:
		return fmt.Errorf("get key [%v] values is over one record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	case len(keyResp.Kvs) == 1:
		// open database conn
		var dbCfg *model.Database
		err = json.Unmarshal(keyResp.Kvs[0].Value, &dbCfg)
		if err != nil {
			return fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", stringutil.BytesToString(keyResp.Kvs[0].Value), err)
		}
		err = model.CreateDatabaseReadWrite(dbCfg)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("get key [%v] values isn't exist record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	}
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName})
	if err != nil {
		return err
	}
	if !strings.EqualFold(taskInfo.TaskMode, constant.TaskModeDataScan) && !strings.EqualFold(taskInfo.TaskMode, "") {
		return fmt.Errorf("the task name [%s] has be existed in the task mode [%s], please rename the global unqiue task name", taskName, taskInfo.TaskMode)
	}

	if strings.EqualFold(taskInfo.TaskMode, constant.TaskModeDataScan) {
		if err = stringutil.PromptForAnswerOrAbortError(
			"Yes, I know my configuration file will be overwrite.",
			fmt.Sprintf("This operation will overwrite the task_mode [%s] task_name [%s] configuration file\n.",
				color.HiYellowString(strings.ToLower(constant.TaskModeDataScan)),
				color.HiYellowString(taskInfo.TaskName),
			)+"\nAre you sure to continue?",
		); err != nil {
			return err
		}
		_, err := DeleteDataScanTask(ctx, &pb.DeleteDataScanTaskRequest{TaskName: []string{taskName}})
		if err != nil {
			return err
		}
	}
	return nil
}

func UpsertDataScanTask(ctx context.Context, req *pb.UpsertDataScanTaskRequest) (string, error) {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		var (
			datasourceS *datasource.Datasource
			datasourceT *datasource.Datasource
			err         error
		)
		datasourceS, err = model.GetIDatasourceRW().GetDatasource(txnCtx, req.DatasourceNameS)
		if err != nil {
			return err
		}
		datasourceT, err = model.GetIDatasourceRW().GetDatasource(txnCtx, req.DatasourceNameT)
		if err != nil {
			return err
		}

		_, err = model.GetITaskRW().CreateTask(txnCtx, &task.Task{
			TaskName:        req.TaskName,
			TaskMode:        constant.TaskModeDataScan,
			TaskFlow:        stringutil.StringBuilder(stringutil.StringUpper(datasourceS.DbType), constant.StringSeparatorAite, stringutil.StringUpper(datasourceT.DbType)),
			DatasourceNameS: req.DatasourceNameS,
			DatasourceNameT: req.DatasourceNameT,
			CaseFieldRuleS:  req.CaseFieldRule.CaseFieldRuleS,
			CaseFieldRuleT:  req.CaseFieldRule.CaseFieldRuleT,
			TaskStatus:      constant.TaskDatabaseStatusWaiting,
			Entity:          &common.Entity{Comment: req.Comment},
		})
		if err != nil {
			return err
		}

		// exclude table and include table is whether conflict
		var sourceSchemas []string
		if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
			sourceSchemas = append(sourceSchemas, req.SchemaRouteRule.SchemaNameS)
		}
		if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
			sourceSchemas = append(sourceSchemas, stringutil.StringUpper(req.SchemaRouteRule.SchemaNameS))
		}
		if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
			sourceSchemas = append(sourceSchemas, stringutil.StringLower(req.SchemaRouteRule.SchemaNameS))
		}
		interSection := stringutil.StringItemsFilterIntersection(req.SchemaRouteRule.IncludeTableS, req.SchemaRouteRule.ExcludeTableS)
		if len(interSection) > 0 {
			return fmt.Errorf("there is the same table within source include and exclude table, please check and remove")
		}

		databaseS, errN := database.NewDatabase(ctx, datasourceS, "", constant.ServiceDatabaseSqlQueryCallTimeout)
		if errN != nil {
			return err
		}
		defer databaseS.Close()
		allOraSchemas, err := databaseS.GetDatabaseSchema()
		if err != nil {
			return err
		}
		for _, s := range sourceSchemas {
			if !stringutil.IsContainedString(allOraSchemas, s) {
				return fmt.Errorf("the schema isn't contained in the database with the case field rule, failed schemas: [%v]", s)
			}
		}

		fieldInfos := stringutil.GetJSONTagFieldValue(req.DataScanParam)
		for jsonTag, fieldValue := range fieldInfos {
			_, err = model.GetIParamsRW().CreateTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:   req.TaskName,
				TaskMode:   constant.TaskModeDataScan,
				ParamName:  jsonTag,
				ParamValue: fieldValue,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	if !strings.EqualFold(req.SchemaRouteRule.String(), "") {
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			var (
				sourceSchema  string
				includeTableS []string
				excludeTableS []string
			)
			if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
				sourceSchema = req.SchemaRouteRule.SchemaNameS
				includeTableS = req.SchemaRouteRule.IncludeTableS
				excludeTableS = req.SchemaRouteRule.ExcludeTableS
			}

			if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
				sourceSchema = stringutil.StringUpper(req.SchemaRouteRule.SchemaNameS)
				for _, t := range req.SchemaRouteRule.IncludeTableS {
					includeTableS = append(includeTableS, stringutil.StringUpper(t))
				}
				for _, t := range req.SchemaRouteRule.ExcludeTableS {
					excludeTableS = append(excludeTableS, stringutil.StringUpper(t))
				}
			}
			if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
				sourceSchema = stringutil.StringLower(req.SchemaRouteRule.SchemaNameS)
				for _, t := range req.SchemaRouteRule.IncludeTableS {
					includeTableS = append(includeTableS, stringutil.StringLower(t))
				}
				for _, t := range req.SchemaRouteRule.ExcludeTableS {
					excludeTableS = append(excludeTableS, stringutil.StringLower(t))
				}
			}

			if len(includeTableS) > 0 && len(excludeTableS) > 0 {
				return fmt.Errorf("source config params include-table-s/exclude-table-s cannot exist at the same time")
			}

			if len(includeTableS) == 0 {
				err = model.GetIMigrateTaskTableRW().DeleteMigrateTaskTableByTaskIsExclude(txnCtx, &rule.MigrateTaskTable{
					TaskName:  req.TaskName,
					IsExclude: constant.MigrateTaskTableIsNotExclude,
				})
				if err != nil {
					return err
				}
			}
			for _, t := range includeTableS {
				_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
					TaskName:    req.TaskName,
					SchemaNameS: sourceSchema,
					TableNameS:  t,
					IsExclude:   constant.MigrateTaskTableIsNotExclude,
				})
				if err != nil {
					return err
				}
			}
			if len(excludeTableS) == 0 {
				err = model.GetIMigrateTaskTableRW().DeleteMigrateTaskTableByTaskIsExclude(txnCtx, &rule.MigrateTaskTable{
					TaskName:  req.TaskName,
					IsExclude: constant.MigrateTaskTableIsExclude,
				})
				if err != nil {
					return err
				}
			}
			for _, t := range excludeTableS {
				_, err = model.GetIMigrateTaskTableRW().CreateMigrateTaskTable(txnCtx, &rule.MigrateTaskTable{
					TaskName:    req.TaskName,
					SchemaNameS: sourceSchema,
					TableNameS:  t,
					IsExclude:   constant.MigrateTaskTableIsExclude,
				})
			}

			_, err = model.GetIMigrateSchemaRouteRW().CreateSchemaRouteRule(txnCtx, &rule.SchemaRouteRule{
				TaskName:    req.TaskName,
				SchemaNameS: sourceSchema,
				SchemaNameT: sourceSchema,
			})
			if err != nil {
				return err
			}

			var tableScanRules []*rule.DataScanRule

			for _, st := range req.DataScanRules {
				if !strings.EqualFold(st.String(), "") {
					var (
						sourceTable string
					)

					if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameOrigin) {
						sourceTable = st.TableNameS
					}

					if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameUpper) {
						sourceTable = stringutil.StringUpper(st.TableNameS)
					}
					if strings.EqualFold(req.CaseFieldRule.CaseFieldRuleS, constant.ParamValueRuleCaseFieldNameLower) {
						sourceTable = stringutil.StringLower(st.TableNameS)
					}

					tableScanRules = append(tableScanRules, &rule.DataScanRule{
						TaskName:         req.TaskName,
						SchemaNameS:      sourceSchema,
						TableNameS:       sourceTable,
						SqlHintS:         st.SqlHintS,
						TableSamplerateS: strconv.FormatUint(st.TableSamplerateS, 10),
					})
				}
			}

			if len(tableScanRules) > 0 {
				_, err = model.GetIDataScanRuleRW().CreateInBatchDataScanRule(ctx, tableScanRules, constant.DefaultRecordCreateBatchSize)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return "", err
		}
	}

	jsonStr, err := stringutil.MarshalJSON(req)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func DeleteDataScanTask(ctx context.Context, req *pb.DeleteDataScanTaskRequest) (string, error) {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		err := model.GetITaskRW().DeleteTask(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIMigrateTaskTableRW().DeleteMigrateTaskTable(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIParamsRW().DeleteTaskCustomParam(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIDataScanRuleRW().DeleteDataScanRule(txnCtx, req.TaskName)
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

func ShowDataScanTask(ctx context.Context, req *pb.ShowDataScanTaskRequest) (string, error) {
	var (
		resp        *pb.UpsertDataScanTaskRequest
		opScanRules []*pb.DataScanRule
		param       *pb.DataScanParam
	)

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		paramsInfo, err := model.GetIParamsRW().QueryTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName: req.TaskName,
			TaskMode: constant.TaskModeDataScan,
		})
		if err != nil {
			return err
		}

		paramMap := make(map[string]string)
		for _, p := range paramsInfo {
			paramMap[p.ParamName] = p.ParamValue
		}

		tableThread, err := strconv.ParseUint(paramMap[constant.ParamNameDataScanTableThread], 10, 64)
		if err != nil {
			return err
		}

		batchSize, err := strconv.ParseUint(paramMap[constant.ParamNameDataScanBatchSize], 10, 64)
		if err != nil {
			return err
		}

		chunkSize, err := strconv.ParseUint(paramMap[constant.ParamNameDataScanChunkSize], 10, 64)
		if err != nil {
			return err
		}

		sqlThreadS, err := strconv.ParseUint(paramMap[constant.ParamNameDataScanSqlThreadS], 10, 64)
		if err != nil {
			return err
		}
		tableSamplerateS, err := strconv.ParseUint(paramMap[constant.ParamNameDataScanTableSamplerateS], 10, 64)
		if err != nil {
			return err
		}
		callTimeout, err := strconv.ParseUint(paramMap[constant.ParamNameDataScanCallTimeout], 10, 64)
		if err != nil {
			return err
		}
		enableCheckpoint, err := strconv.ParseBool(paramMap[constant.ParamNameDataScanEnableCheckpoint])
		if err != nil {
			return err
		}

		enableConsistentRead, err := strconv.ParseBool(paramMap[constant.ParamNameDataScanEnableConsistentRead])
		if err != nil {
			return err
		}

		param = &pb.DataScanParam{
			TableThread:          tableThread,
			BatchSize:            batchSize,
			ChunkSize:            chunkSize,
			SqlThreadS:           sqlThreadS,
			SqlHintS:             paramMap[constant.ParamNameDataScanSqlHintS],
			TableSamplerateS:     tableSamplerateS,
			CallTimeout:          callTimeout,
			EnableCheckpoint:     enableCheckpoint,
			EnableConsistentRead: enableConsistentRead,
		}

		tables, err := model.GetIMigrateTaskTableRW().FindMigrateTaskTable(ctx, &rule.MigrateTaskTable{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		sr, err := model.GetIMigrateSchemaRouteRW().GetSchemaRouteRule(txnCtx, &rule.SchemaRouteRule{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		var (
			includeTables []string
			excludeTables []string
		)
		opSchemaRule := &pb.SchemaRouteRule{}

		opSchemaRule.SchemaNameS = sr.SchemaNameS

		for _, t := range tables {
			if t.SchemaNameS == sr.SchemaNameS && t.IsExclude == constant.MigrateTaskTableIsNotExclude {
				includeTables = append(includeTables, t.TableNameS)
			}
			if t.SchemaNameS == sr.SchemaNameS && t.IsExclude != constant.MigrateTaskTableIsExclude {
				excludeTables = append(excludeTables, t.TableNameS)
			}
		}

		opSchemaRule.IncludeTableS = includeTables
		opSchemaRule.ExcludeTableS = excludeTables

		migrateTableRules, err := model.GetIDataScanRuleRW().FindDataScanRule(txnCtx, &rule.DataScanRule{
			TaskName:    req.TaskName,
			SchemaNameS: sr.SchemaNameS,
		})
		if err != nil {
			return err
		}
		for _, st := range migrateTableRules {
			size, err := stringutil.StrconvUintBitSize(st.TableSamplerateS, 64)
			if err != nil {
				return err
			}
			opScanRules = append(opScanRules, &pb.DataScanRule{
				TableNameS:       st.TableNameS,
				SqlHintS:         st.SqlHintS,
				TableSamplerateS: size,
			})
		}

		resp = &pb.UpsertDataScanTaskRequest{
			TaskName:        taskInfo.TaskName,
			DatasourceNameS: taskInfo.DatasourceNameS,
			DatasourceNameT: taskInfo.DatasourceNameT,
			CaseFieldRule: &pb.CaseFieldRule{
				CaseFieldRuleS: taskInfo.CaseFieldRuleS,
			},
			Comment:         taskInfo.Comment,
			SchemaRouteRule: opSchemaRule,
			DataScanRules:   opScanRules,
			DataScanParam:   param,
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

func StartDataScanTask(ctx context.Context, taskName, workerAddr string) error {
	startTime := time.Now()
	logger.Info("data scan task start", zap.String("task_name", taskName))
	logger.Info("data scan task get task information", zap.String("task_name", taskName))
	var (
		taskInfo                 *task.Task
		datasourceS, datasourceT *datasource.Datasource
		err                      error
	)
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeDataScan})
		if err != nil {
			return err
		}
		datasourceS, err = model.GetIDatasourceRW().GetDatasource(txnCtx, taskInfo.DatasourceNameS)
		if err != nil {
			return err
		}
		datasourceT, err = model.GetIDatasourceRW().GetDatasource(txnCtx, taskInfo.DatasourceNameT)
		if err != nil {
			return err
		}
		err = model.GetITaskLogRW().DeleteLog(txnCtx, []string{taskName})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("data scan task update task status",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	_, err = model.GetITaskRW().UpdateTask(ctx, &task.Task{
		TaskName: taskInfo.TaskName,
	}, map[string]interface{}{
		"WorkerAddr": workerAddr,
		"TaskStatus": constant.TaskDatabaseStatusRunning,
		"StartTime":  startTime,
	})
	if err != nil {
		return err
	}

	logger.Info("data scan task get task params",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	taskParams, err := getDataScanTaskParams(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}

	if !taskParams.EnableCheckpoint {
		logger.Warn("data scan task clear task records",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetIDataScanSummaryRW().DeleteDataScanSummaryName(txnCtx, []string{taskInfo.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataScanTaskRW().DeleteDataScanTaskName(txnCtx, []string{taskInfo.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	if strings.EqualFold(datasourceS.DbType, constant.DatabaseTypeOracle) {
		logger.Info("data scan task process task", zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
		taskTime := time.Now()
		dataScan := &taskflow.DataScanTask{
			Ctx:         ctx,
			Task:        taskInfo,
			DatasourceS: datasourceS,
			DatasourceT: datasourceT,
			TaskParams:  taskParams,
		}
		err = dataScan.Start()
		if err != nil {
			return err
		}
		logger.Info("data scan task process task",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow),
			zap.String("cost", time.Now().Sub(taskTime).String()))
	} else {
		return fmt.Errorf("current data scan task [%s] datasource [%s] source [%s] isn't support, please contact auhtor or reselect", taskName, datasourceS.DatasourceName, datasourceS.DbType)
	}

	// status
	var (
		migrateFailedResults  int64
		migrateWaitResults    int64
		migrateRunResults     int64
		migrateStopResults    int64
		migrateSuccessResults int64
		migrateTotalsResults  int64
	)

	statusRecords, err := model.GetIDataScanTaskRW().FindDataScanTaskGroupByTaskStatus(ctx, taskName)
	if err != nil {
		return err
	}
	for _, rec := range statusRecords {
		switch strings.ToUpper(rec.TaskStatus) {
		case constant.TaskDatabaseStatusFailed:
			migrateFailedResults = rec.StatusCounts
		case constant.TaskDatabaseStatusWaiting:
			migrateWaitResults = rec.StatusCounts
		case constant.TaskDatabaseStatusStopped:
			migrateStopResults = rec.StatusCounts
		case constant.TaskDatabaseStatusRunning:
			migrateRunResults = rec.StatusCounts
		case constant.TaskDatabaseStatusSuccess:
			migrateSuccessResults = rec.StatusCounts
		default:
			return fmt.Errorf("the task [%v] task_mode [%s] task_flow [%v] taskStatus [%v] panic, please contact auhtor or reselect", taskInfo.TaskName, taskInfo.TaskMode, taskInfo.TaskFlow, rec.TaskStatus)
		}
	}

	migrateTotalsResults = migrateFailedResults + migrateWaitResults + migrateStopResults + migrateSuccessResults + migrateRunResults

	if migrateFailedResults > 0 || migrateWaitResults > 0 || migrateRunResults > 0 || migrateStopResults > 0 {
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			_, err = model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
				TaskName: taskInfo.TaskName,
			}, map[string]interface{}{
				"TaskStatus": constant.TaskDatabaseStatusFailed,
				"EndTime":    time.Now(),
			})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName: taskName,
				LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] are exist failed [%d] or waiting [%d] or running [%d] or stopped [%d] status records during running operation, please see [data_scan_task] detail, total records [%d], success records [%d]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeDataScan),
					taskInfo.WorkerAddr,
					taskName,
					migrateFailedResults,
					migrateWaitResults,
					migrateRunResults,
					migrateStopResults,
					migrateTotalsResults,
					migrateSuccessResults),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}

		logger.Info("data scan task failed",
			zap.String("task_name", taskInfo.TaskName),
			zap.String("task_mode", taskInfo.TaskMode),
			zap.String("task_flow", taskInfo.TaskFlow),
			zap.Int64("total records", migrateTotalsResults),
			zap.Int64("failed records", migrateFailedResults),
			zap.Int64("wait records", migrateWaitResults),
			zap.Int64("running records", migrateRunResults),
			zap.Int64("stopped records", migrateStopResults),
			zap.Int64("success records", migrateSuccessResults),
			zap.String("detail tips", "please see [data_scan_task] detail"),
			zap.String("cost", time.Now().Sub(startTime).String()))
		return nil
	}
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err = model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskInfo.TaskName,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusSuccess,
			"EndTime":    time.Now(),
		})
		if err != nil {
			return err
		}
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName: taskName,
			LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] running success, total records [%d], success records [%d], cost: [%v]",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeDataScan),
				taskInfo.WorkerAddr,
				taskName,
				migrateTotalsResults,
				migrateSuccessResults,
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

	if migrateTotalsResults == 0 {
		logger.Info("data scan task success",
			zap.String("task_name", taskInfo.TaskName),
			zap.String("task_mode", taskInfo.TaskMode),
			zap.String("task_flow", taskInfo.TaskFlow),
			zap.Int64("total records", migrateTotalsResults),
			zap.Int64("success records", migrateSuccessResults),
			zap.String("scan action", "the task meets the requirements and does not require scanning"),
			zap.String("detail tips", "please see [data_scan_task] detail"),
			zap.String("cost", time.Now().Sub(startTime).String()))
	} else {
		logger.Info("data scan task success",
			zap.String("task_name", taskInfo.TaskName),
			zap.String("task_mode", taskInfo.TaskMode),
			zap.String("task_flow", taskInfo.TaskFlow),
			zap.Int64("total records", migrateTotalsResults),
			zap.Int64("success records", migrateSuccessResults),
			zap.String("detail tips", "please see [data_scan_task] detail"),
			zap.String("cost", time.Now().Sub(startTime).String()))
	}
	return nil
}

func StopDataScanTask(ctx context.Context, taskName string) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err := model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskName,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusStopped,
		})
		if err != nil {
			return err
		}
		_, err = model.GetIDataScanTaskRW().BatchUpdateDataScanTask(txnCtx, &task.DataScanTask{
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

func GenDataScanTask(ctx context.Context, serverAddr, taskName, outputDir string) error {
	etcdClient, err := etcdutil.CreateClient(ctx, []string{stringutil.WithHostPort(serverAddr)}, nil)
	if err != nil {
		return err
	}
	keyResp, err := etcdutil.GetKey(etcdClient, constant.DefaultMasterDatabaseDBMSKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	switch {
	case len(keyResp.Kvs) > 1:
		return fmt.Errorf("get key [%v] values is over one record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	case len(keyResp.Kvs) == 1:
		// open database conn
		var dbCfg *model.Database
		err = json.Unmarshal(keyResp.Kvs[0].Value, &dbCfg)
		if err != nil {
			return fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", stringutil.BytesToString(keyResp.Kvs[0].Value), err)
		}
		err = model.CreateDatabaseReadWrite(dbCfg)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("get key [%v] values isn't exist record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	}
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeDataScan})
	if err != nil {
		return err
	}

	if !strings.EqualFold(taskInfo.TaskStatus, constant.TaskDatabaseStatusSuccess) {
		return fmt.Errorf("the [%v] task [%v] status [%v] is running in the worker [%v], please waiting success and retry", stringutil.StringLower(taskInfo.TaskMode),
			taskInfo.TaskName, stringutil.StringLower(taskInfo.TaskStatus), taskInfo.WorkerAddr)
	}

	var w database.IFileWriter
	w = processor.NewDataScanFile(ctx, taskInfo.TaskName, taskInfo.TaskMode, taskInfo.TaskFlow, outputDir)
	err = w.InitFile()
	if err != nil {
		return err
	}
	err = w.SyncFile()
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}

	return nil
}

func getDataScanTaskParams(ctx context.Context, taskName string) (*pb.DataScanParam, error) {
	taskParam := &pb.DataScanParam{}

	migrateParams, err := model.GetIParamsRW().QueryTaskCustomParam(ctx, &params.TaskCustomParam{
		TaskName: taskName,
		TaskMode: constant.TaskModeDataScan,
	})
	if err != nil {
		return taskParam, err
	}
	for _, p := range migrateParams {
		if strings.EqualFold(p.ParamName, constant.ParamNameDataScanTableThread) {
			tableThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.TableThread = tableThread
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataScanBatchSize) {
			batchSize, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.BatchSize = batchSize
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataScanChunkSize) {
			chunkSize, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.ChunkSize = chunkSize
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataScanSqlThreadS) {
			sqlThreadS, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.SqlThreadS = sqlThreadS
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataScanSqlHintS) {
			taskParam.SqlHintS = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataScanTableSamplerateS) {
			tableSamplerateS, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.TableSamplerateS = tableSamplerateS
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataScanCallTimeout) {
			callTimeout, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.CallTimeout = callTimeout
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataScanEnableCheckpoint) {
			enableCheckpoint, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableCheckpoint = enableCheckpoint
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataScanEnableConsistentRead) {
			enableConsistentRead, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableConsistentRead = enableConsistentRead
		}
	}
	return taskParam, nil
}
