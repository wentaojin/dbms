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
	"github.com/wentaojin/dbms/utils/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"

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

func PromptStmtMigrateTask(ctx context.Context, taskName, serverAddr string) error {
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
	if !strings.EqualFold(taskInfo.TaskMode, constant.TaskModeStmtMigrate) && !strings.EqualFold(taskInfo.TaskMode, "") {
		return fmt.Errorf("the task name [%s] has be existed in the task mode [%s], please rename the global unqiue task name", taskName, taskInfo.TaskMode)
	}

	if strings.EqualFold(taskInfo.TaskMode, constant.TaskModeStmtMigrate) {
		if err = stringutil.PromptForAnswerOrAbortError(
			"Yes, I know my configuration file will be overwrite.",
			fmt.Sprintf("This operation will overwrite the task_mode [%s] task_name [%s] configuration file\n.",
				color.HiYellowString(strings.ToLower(constant.TaskModeStmtMigrate)),
				color.HiYellowString(taskInfo.TaskName),
			)+"\nAre you sure to continue?",
		); err != nil {
			return err
		}
		_, err := DeleteStmtMigrateTask(ctx, &pb.DeleteStmtMigrateTaskRequest{TaskName: []string{taskName}})
		if err != nil {
			return err
		}
	}
	return nil
}

func UpsertStmtMigrateTask(ctx context.Context, req *pb.UpsertStmtMigrateTaskRequest) (string, error) {
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
			TaskMode:        constant.TaskModeStmtMigrate,
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

		fieldInfos := stringutil.GetJSONTagFieldValue(req.StatementMigrateParam)
		for jsonTag, fieldValue := range fieldInfos {
			_, err = model.GetIParamsRW().CreateTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:   req.TaskName,
				TaskMode:   constant.TaskModeStmtMigrate,
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

func DeleteStmtMigrateTask(ctx context.Context, req *pb.DeleteStmtMigrateTaskRequest) (string, error) {
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

func ShowStmtMigrateTask(ctx context.Context, req *pb.ShowStmtMigrateTaskRequest) (string, error) {
	var (
		resp  *pb.UpsertStmtMigrateTaskRequest
		param *pb.StatementMigrateParam
	)

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		paramsInfo, err := model.GetIParamsRW().QueryTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName: req.TaskName,
			TaskMode: constant.TaskModeStmtMigrate,
		})
		if err != nil {
			return err
		}

		paramMap := make(map[string]string)
		for _, p := range paramsInfo {
			paramMap[p.ParamName] = p.ParamValue
		}

		tableThread, err := strconv.ParseUint(paramMap[constant.ParamNameStmtMigrateTableThread], 10, 64)
		if err != nil {
			return err
		}

		writeThread, err := strconv.ParseUint(paramMap[constant.ParamNameStmtMigrateWriteThread], 10, 64)
		if err != nil {
			return err
		}

		batchSize, err := strconv.ParseUint(paramMap[constant.ParamNameStmtMigrateBatchSize], 10, 64)
		if err != nil {
			return err
		}

		chunkSize, err := strconv.ParseUint(paramMap[constant.ParamNameStmtMigrateChunkSize], 10, 64)
		if err != nil {
			return err
		}

		sqlThreadS, err := strconv.ParseUint(paramMap[constant.ParamNameStmtMigrateSqlThreadS], 10, 64)
		if err != nil {
			return err
		}
		sqlThreadT, err := strconv.ParseUint(paramMap[constant.ParamNameStmtMigrateSqlThreadT], 10, 64)
		if err != nil {
			return err
		}
		callTimeout, err := strconv.ParseUint(paramMap[constant.ParamNameStmtMigrateCallTimeout], 10, 64)
		if err != nil {
			return err
		}
		enableCheckpoint, err := strconv.ParseBool(paramMap[constant.ParamNameStmtMigrateEnableCheckpoint])
		if err != nil {
			return err
		}

		enableConsistentRead, err := strconv.ParseBool(paramMap[constant.ParamNameStmtMigrateEnableConsistentRead])
		if err != nil {
			return err
		}

		enableSafeMode, err := strconv.ParseBool(paramMap[constant.ParamNameStmtMigrateEnableSafeMode])
		if err != nil {
			return err
		}

		enablePrepareStmt, err := strconv.ParseBool(paramMap[constant.ParamNameStmtMigrateEnablePrepareStmt])
		if err != nil {
			return err
		}

		param = &pb.StatementMigrateParam{
			TableThread:          tableThread,
			WriteThread:          writeThread,
			BatchSize:            batchSize,
			ChunkSize:            chunkSize,
			SqlThreadS:           sqlThreadS,
			SqlHintS:             paramMap[constant.ParamNameStmtMigrateSqlHintS],
			SqlThreadT:           sqlThreadT,
			SqlHintT:             paramMap[constant.ParamNameStmtMigrateSqlHintT],
			CallTimeout:          callTimeout,
			EnableCheckpoint:     enableCheckpoint,
			EnableConsistentRead: enableConsistentRead,
			EnableSafeMode:       enableSafeMode,
			EnablePrepareStmt:    enablePrepareStmt,
		}

		schemaRouteRule, dataMigrateRules, _, err := ShowSchemaRouteRule(txnCtx, taskInfo.TaskName)
		if err != nil {
			return err
		}

		resp = &pb.UpsertStmtMigrateTaskRequest{
			TaskName:        taskInfo.TaskName,
			DatasourceNameS: taskInfo.DatasourceNameS,
			DatasourceNameT: taskInfo.DatasourceNameT,
			CaseFieldRule: &pb.CaseFieldRule{
				CaseFieldRuleS: taskInfo.CaseFieldRuleS,
				CaseFieldRuleT: taskInfo.CaseFieldRuleT,
			},
			Comment:               taskInfo.Comment,
			SchemaRouteRule:       schemaRouteRule,
			DataMigrateRules:      dataMigrateRules,
			StatementMigrateParam: param,
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

func StartStmtMigrateTask(ctx context.Context, taskName, workerAddr string) error {
	startTime := time.Now()

	logger.Info("stmt migrate task start",
		zap.String("task_name", taskName),
		zap.String("assign_worker", workerAddr))

	var (
		taskInfo         *task.Task
		sourceDatasource *datasource.Datasource
		targetDatasource *datasource.Datasource
		err              error
	)
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeStmtMigrate})
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
		_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
			TaskName: taskInfo.TaskName,
			LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] starting",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeStmtMigrate),
				taskInfo.TaskName,
				taskInfo.TaskFlow,
				workerAddr,
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

	logger.Info("stmt migrate task running",
		zap.String("task_name", taskInfo.TaskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow),
		zap.String("task_stage", "task params get"))

	_, err = model.GetITaskLogRW().CreateLog(ctx, &task.Log{
		TaskName: taskInfo.TaskName,
		LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] task params get",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeStmtMigrate),
			taskInfo.TaskName,
			taskInfo.TaskFlow,
			workerAddr,
		),
	})
	if err != nil {
		return err
	}

	taskParams, err := getStmtMigrateTasKParams(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}

	switch taskInfo.TaskFlow {
	case constant.TaskFlowOracleToMySQL, constant.TaskFlowOracleToTiDB, constant.TaskFlowPostgresToMySQL, constant.TaskFlowPostgresToTiDB:
		taskTime := time.Now()
		logger.Info("stmt migrate task running",
			zap.String("task_name", taskInfo.TaskName),
			zap.String("task_mode", taskInfo.TaskMode),
			zap.String("task_flow", taskInfo.TaskFlow),
			zap.String("task_stage", "process migrate task"))

		dataMigrate := &taskflow.StmtMigrateTask{
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
		logger.Info("stmt migrate task running",
			zap.String("task_name", taskInfo.TaskName),
			zap.String("task_mode", taskInfo.TaskMode),
			zap.String("task_flow", taskInfo.TaskFlow),
			zap.String("task_stage", "process migrate task"),
			zap.String("cost", time.Now().Sub(taskTime).String()))
	default:
		return fmt.Errorf("current stmt migrate task [%s] datasource [%s] source [%s] isn't support, please contact auhtor or reselect", taskName, sourceDatasource.DatasourceName, sourceDatasource.DbType)
	}

	// status
	logger.Info("stmt migrate task running",
		zap.String("task_name", taskInfo.TaskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow),
		zap.String("task_stage", "task status get"))
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
				TaskName: taskInfo.TaskName,
				LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] are exist failed [%d] or waiting [%d] or running [%d] or stopped [%d] status records during running operation, please see [data_migrate_task] detail, total records [%d], success records [%d]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeStmtMigrate),
					taskInfo.TaskName,
					taskInfo.TaskFlow,
					workerAddr,
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

		logger.Info("struct migrate task failed",
			zap.String("task_name", taskName),
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
			LogDetail: fmt.Sprintf("%v [%v] the task_name [%v] task_flow [%s] worker_addr [%v] running success, total records [%d], success records [%d], cost: [%v]",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeStmtMigrate),
				taskInfo.TaskName,
				taskInfo.TaskFlow,
				workerAddr,
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

	logger.Info("struct migrate task success",
		zap.String("task_name", taskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow),
		zap.Int64("total records", migrateTotalsResults),
		zap.Int64("success records", migrateSuccessResults),
		zap.String("detail tips", "please see [data_migrate_task] detail"),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func StopStmtMigrateTask(ctx context.Context, taskName string) error {
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

func getStmtMigrateTasKParams(ctx context.Context, taskName string) (*pb.StatementMigrateParam, error) {
	taskParam := &pb.StatementMigrateParam{}

	migrateParams, err := model.GetIParamsRW().QueryTaskCustomParam(ctx, &params.TaskCustomParam{
		TaskName: taskName,
		TaskMode: constant.TaskModeStmtMigrate,
	})
	if err != nil {
		return taskParam, err
	}
	for _, p := range migrateParams {
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateTableThread) {
			tableThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.TableThread = tableThread
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateWriteThread) {
			writeThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.WriteThread = writeThread
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateBatchSize) {
			batchSize, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.BatchSize = batchSize
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateChunkSize) {
			chunkSize, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.ChunkSize = chunkSize
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateSqlThreadS) {
			sqlThreadS, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.SqlThreadS = sqlThreadS
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateSqlHintS) {
			taskParam.SqlHintS = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateSqlThreadT) {
			sqlThreadT, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.SqlThreadT = sqlThreadT
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateSqlHintT) {
			taskParam.SqlHintT = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateCallTimeout) {
			callTimeout, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.CallTimeout = callTimeout
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateEnableCheckpoint) {
			enableCheckpoint, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableCheckpoint = enableCheckpoint
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateEnableConsistentRead) {
			enableConsistentRead, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableConsistentRead = enableConsistentRead
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateEnableSafeMode) {
			enableSafeMode, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableSafeMode = enableSafeMode
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameStmtMigrateEnablePrepareStmt) {
			enablePrepareStmt, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnablePrepareStmt = enablePrepareStmt
		}
	}
	return taskParam, nil
}
