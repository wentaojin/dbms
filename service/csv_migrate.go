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

	"github.com/wentaojin/dbms/database/oracle/taskflow"
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/common"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/params"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

func UpsertCsvMigrateTask(ctx context.Context, req *pb.UpsertCsvMigrateTaskRequest) (string, error) {
	_, err := DeleteCsvMigrateTask(ctx, &pb.DeleteCsvMigrateTaskRequest{TaskName: []string{req.TaskName}})
	if err != nil {
		return "", err
	}
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(taskInfo.TaskMode, constant.TaskModeCSVMigrate) && !strings.EqualFold(taskInfo.TaskMode, "") {
		return "", fmt.Errorf("the task name [%s] has be existed in the task mode [%s], please rename the global unqiue task name", req.TaskName, taskInfo.TaskMode)
	}

	err = model.Transaction(ctx, func(txnCtx context.Context) error {
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
			TaskMode:        constant.TaskModeCSVMigrate,
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

		err = UpsertSchemaRouteRule(txnCtx, req.TaskName, req.DatasourceNameS, req.CaseFieldRule, req.SchemaRouteRule, req.DataMigrateRules, nil)
		if err != nil {
			return err
		}

		fieldInfos := stringutil.GetJSONTagFieldValue(req.CsvMigrateParam)
		for jsonTag, fieldValue := range fieldInfos {
			_, err = model.GetIParamsRW().CreateTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:   req.TaskName,
				TaskMode:   constant.TaskModeCSVMigrate,
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

	jsonStr, err := stringutil.MarshalJSON(req)
	if err != nil {
		return jsonStr, err
	}
	return jsonStr, nil
}

func DeleteCsvMigrateTask(ctx context.Context, req *pb.DeleteCsvMigrateTaskRequest) (string, error) {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		err := model.GetITaskRW().DeleteTask(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = DeleteSchemaRouteRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIParamsRW().DeleteTaskCustomParam(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIMigrateSchemaRouteRW().DeleteSchemaRouteRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIMigrateTableRouteRW().DeleteTableRouteRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIMigrateColumnRouteRW().DeleteColumnRouteRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIDataMigrateTaskRW().DeleteDataMigrateTaskName(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIDataMigrateRuleRW().DeleteDataMigrateRule(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIDataMigrateSummaryRW().DeleteDataMigrateSummaryName(txnCtx, req.TaskName)
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

func ShowCsvMigrateTask(ctx context.Context, req *pb.ShowCsvMigrateTaskRequest) (string, error) {
	var (
		resp  *pb.UpsertCsvMigrateTaskRequest
		param *pb.CsvMigrateParam
	)

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		paramsInfo, err := model.GetIParamsRW().QueryTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName: req.TaskName,
			TaskMode: constant.TaskModeCSVMigrate,
		})
		if err != nil {
			return err
		}

		paramMap := make(map[string]string)
		for _, p := range paramsInfo {
			paramMap[p.ParamName] = p.ParamValue
		}

		tableThread, err := strconv.ParseUint(paramMap[constant.ParamNameCsvMigrateTableThread], 10, 64)
		if err != nil {
			return err
		}
		batchSize, err := strconv.ParseUint(paramMap[constant.ParamNameCsvMigrateBatchSize], 10, 64)
		if err != nil {
			return err
		}
		chunkSize, err := strconv.ParseUint(paramMap[constant.ParamNameCsvMigrateChunkSize], 10, 64)
		if err != nil {
			return err
		}
		sqlThreadS, err := strconv.ParseUint(paramMap[constant.ParamNameCsvMigrateSqlThreadS], 10, 64)
		if err != nil {
			return err
		}
		callTimeout, err := strconv.ParseUint(paramMap[constant.ParamNameCsvMigrateCallTimeout], 10, 64)
		if err != nil {
			return err
		}
		header, err := strconv.ParseBool(paramMap[constant.ParamNameCsvMigrateHeader])
		if err != nil {
			return err
		}
		escapeBackslash, err := strconv.ParseBool(paramMap[constant.ParamNameCsvMigrateEscapeBackslash])
		if err != nil {
			return err
		}
		enableCheckpoint, err := strconv.ParseBool(paramMap[constant.ParamNameCsvMigrateEnableCheckpoint])
		if err != nil {
			return err
		}
		enableConsistentRead, err := strconv.ParseBool(paramMap[constant.ParamNameCsvMigrateEnableConsistentRead])
		if err != nil {
			return err
		}

		param = &pb.CsvMigrateParam{
			TableThread:          tableThread,
			BatchSize:            batchSize,
			DiskUsageFactor:      paramMap[constant.ParamNameCsvMigrateDiskUsageFactor],
			Header:               header,
			Separator:            paramMap[constant.ParamNameCsvMigrateSeparator],
			Terminator:           paramMap[constant.ParamNameCsvMigrateTerminator],
			DataCharsetT:         paramMap[constant.ParamNameCsvMigrateDataCharsetT],
			Delimiter:            paramMap[constant.ParamNameCsvMigrateDelimiter],
			NullValue:            paramMap[constant.ParamNameCsvMigrateNullValue],
			EscapeBackslash:      escapeBackslash,
			ChunkSize:            chunkSize,
			OutputDir:            paramMap[constant.ParamNameCsvMigrateOutputDir],
			SqlThreadS:           sqlThreadS,
			SqlHintS:             paramMap[constant.ParamNameCsvMigrateSqlHintS],
			CallTimeout:          callTimeout,
			EnableCheckpoint:     enableCheckpoint,
			EnableConsistentRead: enableConsistentRead,
		}

		schemaRouteRule, dataMigrateRules, _, err := ShowSchemaRouteRule(txnCtx, taskInfo.TaskName)
		if err != nil {
			return err
		}

		resp = &pb.UpsertCsvMigrateTaskRequest{
			TaskName:        taskInfo.TaskName,
			DatasourceNameS: taskInfo.DatasourceNameS,
			DatasourceNameT: taskInfo.DatasourceNameT,
			Comment:         taskInfo.Comment,
			CaseFieldRule: &pb.CaseFieldRule{
				CaseFieldRuleS: taskInfo.CaseFieldRuleS,
				CaseFieldRuleT: taskInfo.CaseFieldRuleT,
			},
			SchemaRouteRule:  schemaRouteRule,
			DataMigrateRules: dataMigrateRules,
			CsvMigrateParam:  param,
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

func StartCsvMigrateTask(ctx context.Context, taskName, workerAddr string) error {
	startTime := time.Now()
	logger.Info("csv migrate task start", zap.String("task_name", taskName))
	logger.Info("csv migrate task get task information", zap.String("task_name", taskName))
	var (
		taskInfo         *task.Task
		sourceDatasource *datasource.Datasource
		targetDatasource *datasource.Datasource
		err              error
	)
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeCSVMigrate})
		if err != nil {
			return err
		}
		sourceDatasource, err = model.GetIDatasourceRW().GetDatasource(txnCtx, taskInfo.DatasourceNameS)
		if err != nil {
			return err
		}
		targetDatasource, err = model.GetIDatasourceRW().GetDatasource(txnCtx, taskInfo.DatasourceNameT)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	logger.Info("csv migrate task update task status",
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

	logger.Info("csv migrate task get task params",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	taskParams, err := getCsvMigrateTasKParams(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}

	if !taskParams.EnableCheckpoint {
		logger.Warn("stmt migrate task clear task records",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetIDataMigrateSummaryRW().DeleteDataMigrateSummaryName(txnCtx, []string{taskInfo.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataMigrateTaskRW().DeleteDataMigrateTaskName(txnCtx, []string{taskInfo.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	if strings.EqualFold(sourceDatasource.DbType, constant.DatabaseTypeOracle) {
		logger.Info("csv migrate task process task", zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
		taskTime := time.Now()
		dm := &taskflow.CsvMigrateTask{
			Ctx:         ctx,
			Task:        taskInfo,
			DatasourceS: sourceDatasource,
			DatasourceT: targetDatasource,
			TaskParams:  taskParams,
		}
		err = dm.Start()
		if err != nil {
			return err
		}
		logger.Info("csv migrate task process task",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow),
			zap.String("cost", time.Now().Sub(taskTime).String()))
	} else {
		return fmt.Errorf("current csv migrate task [%s] datasource [%s] source [%s] isn't support, please contact auhtor or reselect", taskName, sourceDatasource.DatasourceName, sourceDatasource.DbType)
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

	statusRecords, err := model.GetIDataMigrateTaskRW().FindDataMigrateTaskGroupByTaskStatus(ctx, taskName)
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
				LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] are exist failed [%d] or waiting [%d] or running [%d] or stopped [%d] status records during running operation, please see [data_migrate_task] detail, total records [%d], success records [%d]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeStmtMigrate),
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

		logger.Info("csv migrate task failed",
			zap.String("task_name", taskInfo.TaskName),
			zap.String("task_mode", taskInfo.TaskMode),
			zap.String("task_flow", taskInfo.TaskFlow),
			zap.Int64("total records", migrateTotalsResults),
			zap.Int64("failed records", migrateFailedResults),
			zap.Int64("wait records", migrateWaitResults),
			zap.Int64("running records", migrateRunResults),
			zap.Int64("stopped records", migrateStopResults),
			zap.Int64("success records", migrateSuccessResults),
			zap.String("detail tips", "please see [data_migrate_task] detail"),
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
				stringutil.StringLower(constant.TaskModeStmtMigrate),
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

	logger.Info("csv migrate task success",
		zap.String("task_name", taskInfo.TaskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow),
		zap.Int64("total records", migrateTotalsResults),
		zap.Int64("success records", migrateSuccessResults),
		zap.String("detail tips", "please see [data_migrate_task] detail"),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func StopCsvMigrateTask(ctx context.Context, taskName string) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err := model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskName,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusStopped,
		})
		if err != nil {
			return err
		}
		_, err = model.GetIDataMigrateTaskRW().BatchUpdateDataMigrateTask(txnCtx, &task.DataMigrateTask{
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

func getCsvMigrateTasKParams(ctx context.Context, taskName string) (*pb.CsvMigrateParam, error) {
	taskParam := &pb.CsvMigrateParam{}

	migrateParams, err := model.GetIParamsRW().QueryTaskCustomParam(ctx, &params.TaskCustomParam{
		TaskName: taskName,
		TaskMode: constant.TaskModeCSVMigrate,
	})
	if err != nil {
		return taskParam, err
	}
	for _, p := range migrateParams {
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateTableThread) {
			tableThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.TableThread = tableThread
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateBatchSize) {
			batchSize, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.BatchSize = batchSize
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateDiskUsageFactor) {
			taskParam.DiskUsageFactor = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateHeader) {
			header, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.Header = header
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateSeparator) {
			taskParam.Separator = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateTerminator) {
			taskParam.Terminator = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateDataCharsetT) {
			taskParam.DataCharsetT = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateDelimiter) {
			taskParam.Delimiter = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateNullValue) {
			taskParam.NullValue = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateEscapeBackslash) {
			escapeBackslash, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EscapeBackslash = escapeBackslash
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateChunkSize) {
			chunkSize, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.ChunkSize = chunkSize
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateOutputDir) {
			taskParam.OutputDir = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateSqlThreadS) {
			sqlThreadS, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.SqlThreadS = sqlThreadS
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateSqlHintS) {
			taskParam.SqlHintS = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateCallTimeout) {
			callTimeout, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.CallTimeout = callTimeout
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateEnableCheckpoint) {
			enableCheckpoint, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableCheckpoint = enableCheckpoint
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateEnableConsistentRead) {
			enableConsistentRead, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableConsistentRead = enableConsistentRead
		}
	}
	return taskParam, nil
}
