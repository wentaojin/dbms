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

	"github.com/wentaojin/dbms/database"
	"github.com/wentaojin/dbms/utils/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"

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

func UpsertDataCompareTask(ctx context.Context, req *pb.UpsertDataCompareTaskRequest) (string, error) {
	_, err := DeleteDataCompareTask(ctx, &pb.DeleteDataCompareTaskRequest{TaskName: []string{req.TaskName}})
	if err != nil {
		return "", err
	}
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(taskInfo.TaskMode, constant.TaskModeDataCompare) && !strings.EqualFold(taskInfo.TaskMode, "") {
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
			TaskMode:        constant.TaskModeDataCompare,
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

		err = UpsertSchemaRouteRule(txnCtx, req.TaskName, req.DatasourceNameS, req.CaseFieldRule, req.SchemaRouteRule, nil, req.DataCompareRules)
		if err != nil {
			return err
		}

		fieldInfos := stringutil.GetJSONTagFieldValue(req.DataCompareParam)
		for jsonTag, fieldValue := range fieldInfos {
			_, err = model.GetIParamsRW().CreateTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:   req.TaskName,
				TaskMode:   constant.TaskModeDataCompare,
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

func DeleteDataCompareTask(ctx context.Context, req *pb.DeleteDataCompareTaskRequest) (string, error) {
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
		err = model.GetIDataCompareTaskRW().DeleteDataCompareTaskName(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIDataCompareSummaryRW().DeleteDataCompareSummaryName(txnCtx, req.TaskName)
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

func ShowDataCompareTask(ctx context.Context, req *pb.ShowDataCompareTaskRequest) (string, error) {
	var (
		resp  *pb.UpsertDataCompareTaskRequest
		param *pb.DataCompareParam
	)

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		paramsInfo, err := model.GetIParamsRW().QueryTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName: req.TaskName,
			TaskMode: constant.TaskModeDataCompare,
		})
		if err != nil {
			return err
		}

		paramMap := make(map[string]string)
		for _, p := range paramsInfo {
			paramMap[p.ParamName] = p.ParamValue
		}

		tableThread, err := strconv.ParseUint(paramMap[constant.ParamNameDataCompareTableThread], 10, 64)
		if err != nil {
			return err
		}

		batchSize, err := strconv.ParseUint(paramMap[constant.ParamNameDataCompareBatchSize], 10, 64)
		if err != nil {
			return err
		}
		sqlThread, err := strconv.ParseUint(paramMap[constant.ParamNameDataCompareSqlThread], 10, 64)
		if err != nil {
			return err
		}

		callTimeout, err := strconv.ParseUint(paramMap[constant.ParamNameDataCompareCallTimeout], 10, 64)
		if err != nil {
			return err
		}
		enableCheckpoint, err := strconv.ParseBool(paramMap[constant.ParamNameDataCompareEnableCheckpoint])
		if err != nil {
			return err
		}

		enableConsistentRead, err := strconv.ParseBool(paramMap[constant.ParamNameDataCompareEnableConsistentRead])
		if err != nil {
			return err
		}

		onlyCompareRow, err := strconv.ParseBool(paramMap[constant.ParamNameDataCompareOnlyCompareRow])
		if err != nil {
			return err
		}

		param = &pb.DataCompareParam{
			TableThread:          tableThread,
			BatchSize:            batchSize,
			SqlThread:            sqlThread,
			SqlHintS:             paramMap[constant.ParamNameDataCompareSqlHintS],
			SqlHintT:             paramMap[constant.ParamNameDataCompareSqlHintT],
			CallTimeout:          callTimeout,
			EnableCheckpoint:     enableCheckpoint,
			EnableConsistentRead: enableConsistentRead,
			OnlyCompareRow:       onlyCompareRow,
		}

		schemaRouteRule, _, dataCompareRules, err := ShowSchemaRouteRule(txnCtx, taskInfo.TaskName)
		if err != nil {
			return err
		}

		resp = &pb.UpsertDataCompareTaskRequest{
			TaskName:        taskInfo.TaskName,
			DatasourceNameS: taskInfo.DatasourceNameS,
			DatasourceNameT: taskInfo.DatasourceNameT,
			CaseFieldRule: &pb.CaseFieldRule{
				CaseFieldRuleS: taskInfo.CaseFieldRuleS,
				CaseFieldRuleT: taskInfo.CaseFieldRuleT,
			},
			Comment:          taskInfo.Comment,
			SchemaRouteRule:  schemaRouteRule,
			DataCompareRules: dataCompareRules,
			DataCompareParam: param,
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

func StartDataCompareTask(ctx context.Context, taskName, workerAddr string) error {
	startTime := time.Now()
	logger.Info("data compare task start", zap.String("task_name", taskName))
	logger.Info("data compare task get task information", zap.String("task_name", taskName))
	var (
		taskInfo         *task.Task
		sourceDatasource *datasource.Datasource
		targetDatasource *datasource.Datasource
		err              error
	)
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeDataCompare})
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

	logger.Info("data compare task update task status",
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

	logger.Info("data compare task get task params",
		zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	taskParams, err := getDataCompareTasKParams(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}

	if !taskParams.EnableCheckpoint {
		logger.Warn("data compare task clear task records",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetIDataCompareSummaryRW().DeleteDataCompareSummaryName(txnCtx, []string{taskInfo.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataCompareTaskRW().DeleteDataCompareTaskName(txnCtx, []string{taskInfo.TaskName})
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
		logger.Info("data compare task process task", zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
		taskTime := time.Now()
		dataMigrate := &taskflow.DataCompareTask{
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
		logger.Info("data compare task process task",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow),
			zap.String("cost", time.Now().Sub(taskTime).String()))
	} else {
		return fmt.Errorf("current data compare task [%s] datasource [%s] source [%s] isn't support, please contact auhtor or reselect", taskName, sourceDatasource.DatasourceName, sourceDatasource.DbType)
	}

	// status
	var (
		failedResults   int64
		waitResults     int64
		runResults      int64
		stopResults     int64
		equalResults    int64
		notEqualResults int64
		totalsResults   int64
	)

	statusRecords, err := model.GetIDataCompareTaskRW().FindDataCompareTaskGroupByTaskStatus(ctx, taskName)
	if err != nil {
		return err
	}
	for _, rec := range statusRecords {
		switch strings.ToUpper(rec.TaskStatus) {
		case constant.TaskDatabaseStatusFailed:
			failedResults = rec.StatusCounts
		case constant.TaskDatabaseStatusWaiting:
			waitResults = rec.StatusCounts
		case constant.TaskDatabaseStatusStopped:
			stopResults = rec.StatusCounts
		case constant.TaskDatabaseStatusRunning:
			runResults = rec.StatusCounts
		case constant.TaskDatabaseStatusEqual:
			equalResults = rec.StatusCounts
		case constant.TaskDatabaseStatusNotEqual:
			notEqualResults = rec.StatusCounts
		default:
			return fmt.Errorf("the task [%v] task_mode [%s] task_flow [%v] taskStatus [%v] panic, please contact auhtor or reselect", taskInfo.TaskName, taskInfo.TaskMode, taskInfo.TaskFlow, rec.TaskStatus)
		}
	}

	totalsResults = failedResults + waitResults + stopResults + equalResults + notEqualResults + runResults

	if failedResults > 0 || waitResults > 0 || runResults > 0 || stopResults > 0 {
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
				LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] are exist failed [%d] or waiting [%d] or running [%d] or stopped [%d] status records during running operation, please see [data_compare_task] detail, total records [%d], qual records [%d], not qual records [%d]",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(constant.TaskModeDataCompare),
					taskInfo.WorkerAddr,
					taskName,
					failedResults,
					waitResults,
					runResults,
					stopResults,
					totalsResults,
					equalResults,
					notEqualResults),
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}

		logger.Info("data compare task failed",
			zap.String("task_name", taskInfo.TaskName),
			zap.String("task_mode", taskInfo.TaskMode),
			zap.String("task_flow", taskInfo.TaskFlow),
			zap.Int64("total records", totalsResults),
			zap.Int64("failed records", failedResults),
			zap.Int64("wait records", waitResults),
			zap.Int64("running records", runResults),
			zap.Int64("stopped records", stopResults),
			zap.Int64("equal records", equalResults),
			zap.Int64("not equal records", notEqualResults),
			zap.String("detail tips", "please see [data_compare_task] detail"),
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
			LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] running success, total records [%d], qual records [%d], not qual records [%d], cost: [%v]",
				stringutil.CurrentTimeFormatString(),
				stringutil.StringLower(constant.TaskModeDataCompare),
				taskInfo.WorkerAddr,
				taskName,
				totalsResults,
				equalResults,
				notEqualResults,
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

	logger.Info("data compare task success",
		zap.String("task_name", taskInfo.TaskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow),
		zap.Int64("total records", totalsResults),
		zap.Int64("equal records", equalResults),
		zap.Int64("not equal records", notEqualResults),
		zap.String("detail tips", "please see [data_compare_task] detail"),
		zap.String("cost", time.Now().Sub(startTime).String()))
	return nil
}

func StopDataCompareTask(ctx context.Context, taskName string) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err := model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskName,
		}, map[string]interface{}{
			"TaskStatus": constant.TaskDatabaseStatusStopped,
		})
		if err != nil {
			return err
		}
		_, err = model.GetIDataCompareTaskRW().BatchUpdateDataCompareTask(txnCtx, &task.DataCompareTask{
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

func GenDataCompareTask(ctx context.Context, serverAddr, taskName, outputDir string) error {
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
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeDataCompare})
	if err != nil {
		return err
	}

	if !strings.EqualFold(taskInfo.TaskStatus, constant.TaskDatabaseStatusSuccess) {
		return fmt.Errorf("the [%v] task [%v] status [%v] is running in the worker [%v], please waiting success and retry", stringutil.StringLower(taskInfo.TaskMode),
			taskInfo.TaskName, stringutil.StringLower(taskInfo.TaskStatus), taskInfo.WorkerAddr)
	}

	var w database.IDataCompareFileWriter
	w = taskflow.NewDataCompareFile(ctx, taskInfo.TaskName, taskInfo.TaskFlow, outputDir)
	err = w.InitOutputFile()
	if err != nil {
		return err
	}
	err = w.SyncCompareFile()
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}

	fmt.Printf("the data compare task table records aren't eqal record sql file had be output to [%v], please forward to view\n", outputDir)
	return nil
}

func getDataCompareTasKParams(ctx context.Context, taskName string) (*pb.DataCompareParam, error) {
	taskParam := &pb.DataCompareParam{}

	migrateParams, err := model.GetIParamsRW().QueryTaskCustomParam(ctx, &params.TaskCustomParam{
		TaskName: taskName,
		TaskMode: constant.TaskModeDataCompare,
	})
	if err != nil {
		return taskParam, err
	}
	for _, p := range migrateParams {
		if strings.EqualFold(p.ParamName, constant.ParamNameDataCompareTableThread) {
			tableThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.TableThread = tableThread
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataCompareBatchSize) {
			batchSize, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.BatchSize = batchSize
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataCompareSqlThread) {
			sqlThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.SqlThread = sqlThread
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataCompareSqlHintS) {
			taskParam.SqlHintS = p.ParamValue
		}

		if strings.EqualFold(p.ParamName, constant.ParamNameDataCompareSqlHintT) {
			taskParam.SqlHintT = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataCompareCallTimeout) {
			callTimeout, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.CallTimeout = callTimeout
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataCompareEnableCheckpoint) {
			enableCheckpoint, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableCheckpoint = enableCheckpoint
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataCompareEnableConsistentRead) {
			enableConsistentRead, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableConsistentRead = enableConsistentRead
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameDataCompareOnlyCompareRow) {
			onlyCompareRow, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.OnlyCompareRow = onlyCompareRow
		}
	}
	return taskParam, nil
}
