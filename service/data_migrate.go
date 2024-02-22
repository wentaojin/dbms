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

func UpsertDataMigrateTask(ctx context.Context, req *pb.UpsertDataMigrateTaskRequest) (string, error) {
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(taskInfo.TaskMode, constant.TaskModeDataMigrate) && !strings.EqualFold(taskInfo.TaskMode, "") {
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
			TaskMode:        constant.TaskModeDataMigrate,
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

		err = UpsertSchemaRouteRule(txnCtx, req.TaskName, req.DatasourceNameS, req.CaseFieldRule, req.SchemaRouteRule)
		if err != nil {
			return err
		}

		fieldInfos := stringutil.GetJSONTagFieldValue(req.DataMigrateParam)
		for jsonTag, fieldValue := range fieldInfos {
			_, err = model.GetIParamsRW().CreateTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:   req.TaskName,
				TaskMode:   constant.TaskModeDataMigrate,
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

func DeleteDataMigrateTask(ctx context.Context, req *pb.DeleteDataMigrateTaskRequest) (string, error) {
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

func ShowDataMigrateTask(ctx context.Context, req *pb.ShowDataMigrateTaskRequest) (string, error) {
	var (
		resp  *pb.UpsertDataMigrateTaskRequest
		param *pb.DataMigrateParam
	)

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		paramsInfo, err := model.GetIParamsRW().QueryTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName: req.TaskName,
			TaskMode: constant.TaskModeDataMigrate,
		})
		if err != nil {
			return err
		}

		paramMap := make(map[string]string)
		for _, p := range paramsInfo {
			paramMap[p.ParamName] = p.ParamValue
		}

		tableThread, err := strconv.ParseUint(paramMap[constant.ParamNameDataMigrateTableThread], 10, 64)
		if err != nil {
			return err
		}

		batchSize, err := strconv.ParseUint(paramMap[constant.ParamNameDataMigrateBatchSize], 10, 64)
		if err != nil {
			return err
		}

		chunkSize, err := strconv.ParseUint(paramMap[constant.ParamNameDataMigrateChunkSize], 10, 64)
		if err != nil {
			return err
		}

		sqlThreadS, err := strconv.ParseUint(paramMap[constant.ParamNameDataMigrateSqlThreadS], 10, 64)
		if err != nil {
			return err
		}
		sqlThreadT, err := strconv.ParseUint(paramMap[constant.ParamNameDataMigrateSqlThreadT], 10, 64)
		if err != nil {
			return err
		}
		callTimeout, err := strconv.ParseUint(paramMap[constant.ParamNameDataMigrateCallTimeout], 10, 64)
		if err != nil {
			return err
		}
		enableCheckpoint, err := strconv.ParseBool(paramMap[constant.ParamNameDataMigrateEnableCheckpoint])
		if err != nil {
			return err
		}

		enableConsistentRead, err := strconv.ParseBool(paramMap[constant.ParamNameDataMigrateEnableConsistentRead])
		if err != nil {
			return err
		}

		param = &pb.DataMigrateParam{
			TableThread:          tableThread,
			BatchSize:            batchSize,
			ChunkSize:            chunkSize,
			SqlThreadS:           sqlThreadS,
			SqlHintS:             paramMap[constant.ParamNameDataMigrateSqlHintS],
			SqlThreadT:           sqlThreadT,
			SqlHintT:             paramMap[constant.ParamNameDataMigrateSqlHintT],
			CallTimeout:          callTimeout,
			EnableCheckpoint:     enableCheckpoint,
			EnableConsistentRead: enableConsistentRead,
		}

		schemaRouteRule, err := ShowSchemaRouteRule(txnCtx, taskInfo.TaskName)
		if err != nil {
			return err
		}

		resp = &pb.UpsertDataMigrateTaskRequest{
			TaskName:        taskInfo.TaskName,
			DatasourceNameS: taskInfo.DatasourceNameS,
			DatasourceNameT: taskInfo.DatasourceNameT,
			CaseFieldRule: &pb.CaseFieldRule{
				CaseFieldRuleS: taskInfo.CaseFieldRuleS,
				CaseFieldRuleT: taskInfo.CaseFieldRuleT,
			},
			Comment:          taskInfo.Comment,
			SchemaRouteRule:  schemaRouteRule,
			DataMigrateParam: param,
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

func StartDataMigrateTask(ctx context.Context, taskName, workerAddr string) error {
	startTime := time.Now()
	logger.Info("data migrate task start", zap.String("task_name", taskName))
	logger.Info("data migrate task get task information", zap.String("task_name", taskName))
	var (
		taskInfo         *task.Task
		sourceDatasource *datasource.Datasource
		targetDatasource *datasource.Datasource
		err              error
	)
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeDataMigrate})
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

	logger.Info("data migrate task update task status",
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

	logger.Info("data migrate task get task params",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	taskParams, err := getDataMigrateTasKParams(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}

	if strings.EqualFold(sourceDatasource.DbType, constant.DatabaseTypeOracle) {
		logger.Info("struct migrate task process task", zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
		taskTime := time.Now()
		dataMigrate := &taskflow.DataMigrateTask{
			Ctx:         ctx,
			Task:        taskInfo,
			DatasourceS: sourceDatasource,
			DatasourceT: targetDatasource,
			TaskParams:  taskParams,
		}
		err = dataMigrate.Start()
		if err != nil {
			return err
		}
		logger.Info("data migrate task process task",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow),
			zap.String("cost", time.Now().Sub(taskTime).String()))
	} else {
		return fmt.Errorf("current data migrate task [%s] datasource [%s] source [%s] isn't support, please contact auhtor or reselect", taskName, sourceDatasource.DatasourceName, sourceDatasource.DbType)
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
					stringutil.StringLower(constant.TaskModeDataMigrate),
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

		logger.Info("data migrate task failed",
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
				stringutil.StringLower(constant.TaskModeDataMigrate),
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

	logger.Info("data migrate task success",
		zap.String("task_name", taskInfo.TaskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow),
		zap.Int64("total records", migrateTotalsResults),
		zap.Int64("success records", migrateSuccessResults),
		zap.String("detail tips", "please see [data_migrate_task] detail"),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func StopDataMigrateTask(ctx context.Context, taskName string) error {
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

func getDataMigrateTasKParams(ctx context.Context, taskName string) (*pb.DataMigrateParam, error) {
	taskParam := &pb.DataMigrateParam{}

	migrateParams, err := model.GetIParamsRW().QueryTaskCustomParam(ctx, &params.TaskCustomParam{
		TaskName: taskName,
		TaskMode: constant.TaskModeDataMigrate,
	})
	if err != nil {
		return taskParam, err
	}
	for _, p := range migrateParams {
		if strings.EqualFold(p.ParamName, constant.ParamNameDataMigrateTableThread) {
			tableThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.TableThread = tableThread
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataMigrateBatchSize) {
			batchSize, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.BatchSize = batchSize
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataMigrateChunkSize) {
			chunkSize, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.ChunkSize = chunkSize
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataMigrateSqlThreadS) {
			sqlThreadS, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.SqlThreadS = sqlThreadS
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataMigrateSqlHintS) {
			taskParam.SqlHintS = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataMigrateSqlThreadT) {
			sqlThreadT, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.SqlThreadT = sqlThreadT
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataMigrateSqlHintT) {
			taskParam.SqlHintT = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataMigrateCallTimeout) {
			callTimeout, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.CallTimeout = callTimeout
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataMigrateEnableCheckpoint) {
			enableCheckpoint, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableCheckpoint = enableCheckpoint
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataMigrateEnableConsistentRead) {
			enableConsistentRead, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableConsistentRead = enableConsistentRead
		}
	}
	return taskParam, nil
}
