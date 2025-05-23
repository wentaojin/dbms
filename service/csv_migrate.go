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
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/wentaojin/dbms/database/taskflow"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/common"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/params"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/stringutil"
	"go.uber.org/zap"
)

func PromptCsvMigrateTask(ctx context.Context, taskName, serverAddr string) error {
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
	if !strings.EqualFold(taskInfo.TaskMode, constant.TaskModeCSVMigrate) && !strings.EqualFold(taskInfo.TaskMode, "") {
		return fmt.Errorf("the task name [%s] has be existed in the task mode [%s], please rename the global unqiue task name", taskName, taskInfo.TaskMode)
	}

	if strings.EqualFold(taskInfo.TaskMode, constant.TaskModeCSVMigrate) {
		if err = stringutil.PromptForAnswerOrAbortError(
			"Yes, I know my configuration file will be overwrite.",
			fmt.Sprintf("This operation will overwrite the task_mode [%s] task_name [%s] configuration file.\n",
				color.HiYellowString(strings.ToLower(constant.TaskModeCSVMigrate)),
				color.HiYellowString(taskInfo.TaskName),
			)+"\nAre you sure to continue?",
		); err != nil {
			return err
		}
		_, err := DeleteCsvMigrateTask(ctx, &pb.DeleteCsvMigrateTaskRequest{TaskName: []string{taskName}})
		if err != nil {
			return err
		}
	}
	return nil
}

func UpsertCsvMigrateTask(ctx context.Context, req *pb.UpsertCsvMigrateTaskRequest) (string, error) {
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

		paramsInfo, err := model.GetIParamsRW().GetTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName:  req.TaskName,
			TaskMode:  constant.TaskModeCSVMigrate,
			ParamName: constant.ParamNameCsvMigrateEnableImportFeature,
		})
		if err != nil {
			return err
		}

		fieldInfos := stringutil.GetJSONTagFieldValue(req.CsvMigrateParam)
		for jsonTag, fieldValue := range fieldInfos {
			if strings.EqualFold(jsonTag, constant.ParamNameCsvMigrateEnableImportFeature) && !strings.EqualFold(paramsInfo.ParamValue, "") && !strings.EqualFold(paramsInfo.ParamValue, fieldValue) {
				return fmt.Errorf("the task_name [%s] task_flow [%s] task_mode [%s] can't support modify params [enable-import-feature], please delete the task and recreating or rename the task_name creating",
					req.TaskName,
					constant.TaskModeCSVMigrate,
					stringutil.StringBuilder(stringutil.StringUpper(datasourceS.DbType), constant.StringSeparatorAite, stringutil.StringUpper(datasourceT.DbType)),
				)
			}
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
			return fmt.Errorf("parse table_thread err: %v", err)
		}
		writeThread, err := strconv.ParseUint(paramMap[constant.ParamNameCsvMigrateWriteThread], 10, 64)
		if err != nil {
			return fmt.Errorf("parse write_thread err: %v", err)
		}
		batchSize, err := strconv.ParseUint(paramMap[constant.ParamNameCsvMigrateBatchSize], 10, 64)
		if err != nil {
			return fmt.Errorf("parse batch_size err: %v", err)
		}
		chunkSize, err := strconv.ParseUint(paramMap[constant.ParamNameCsvMigrateChunkSize], 10, 64)
		if err != nil {
			return fmt.Errorf("parse chunk_size err: %v", err)
		}
		sqlThreadS, err := strconv.ParseUint(paramMap[constant.ParamNameCsvMigrateSqlThreadS], 10, 64)
		if err != nil {
			return fmt.Errorf("parse sql_thread_s err: %v", err)
		}
		callTimeout, err := strconv.ParseUint(paramMap[constant.ParamNameCsvMigrateCallTimeout], 10, 64)
		if err != nil {
			return fmt.Errorf("parse call_timeout err: %v", err)
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
		enableImportFeature, err := strconv.ParseBool(paramMap[constant.ParamNameCsvMigrateEnableImportFeature])
		if err != nil {
			return err
		}
		importPars := make(map[string]string)
		err = json.Unmarshal([]byte(paramMap[constant.ParamNameCsvMigrateImportParams]), &importPars)
		if err != nil {
			return err
		}

		param = &pb.CsvMigrateParam{
			TableThread:          tableThread,
			WriteThread:          writeThread,
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
			EnableImportFeature:  enableImportFeature,
			CsvImportParams:      importPars,
			GarbledCharReplace:   paramMap[constant.ParamNameCsvMigrateGarbledCharReplace],
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
		err = model.GetITaskLogRW().DeleteLog(txnCtx, []string{taskName})
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
	migrateParams, err := getCsvMigrateTasKParams(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}

	switch taskInfo.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB, constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
		dm := &taskflow.CsvMigrateTask{
			Ctx:           ctx,
			Task:          taskInfo,
			DatasourceS:   sourceDatasource,
			DatasourceT:   targetDatasource,
			MigrateParams: migrateParams,
		}
		err = dm.Start()
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("the csv migrate task [%s] task_flow [%s] datasource [%s] source [%s] isn't support, please contact auhtor or reselect", taskName, taskInfo.TaskFlow, sourceDatasource.DatasourceName, sourceDatasource.DbType)
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
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateWriteThread) {
			writeThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.WriteThread = writeThread
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
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateEnableImportFeature) {
			enableImportFeature, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableImportFeature = enableImportFeature
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateImportParams) {
			importPars := make(map[string]string)
			if !strings.EqualFold(p.ParamValue, "") {
				err := json.Unmarshal([]byte(p.ParamValue), &importPars)
				if err != nil {
					return taskParam, err
				}
			}
			taskParam.CsvImportParams = importPars
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCsvMigrateGarbledCharReplace) {
			taskParam.GarbledCharReplace = p.ParamValue
		}
	}
	return taskParam, nil
}
