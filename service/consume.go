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
	"github.com/wentaojin/dbms/logger"
	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/common"
	"github.com/wentaojin/dbms/model/datasource"
	"github.com/wentaojin/dbms/model/params"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/proto/pb"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/etcdutil"
	"github.com/wentaojin/dbms/utils/stringutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func PromptCdcConsumeTask(ctx context.Context, taskName, serverAddr string) error {
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
	if !strings.EqualFold(taskInfo.TaskMode, constant.TaskModeCdcConsume) && !strings.EqualFold(taskInfo.TaskMode, "") {
		return fmt.Errorf("the task name [%s] has be existed in the task mode [%s], please rename the global unqiue task name", taskName, taskInfo.TaskMode)
	}

	if strings.EqualFold(taskInfo.TaskMode, constant.TaskModeCdcConsume) {
		if err = stringutil.PromptForAnswerOrAbortError(
			"Yes, I know my configuration file will be overwrite.",
			fmt.Sprintf("This operation will overwrite the task_mode [%s] task_name [%s] configuration file\n.",
				color.HiYellowString(strings.ToLower(constant.TaskModeCdcConsume)),
				color.HiYellowString(taskInfo.TaskName),
			)+"\nAre you sure to continue?",
		); err != nil {
			return err
		}
		_, err := DeleteCdcConsumeTask(ctx, &pb.DeleteCdcConsumeTaskRequest{TaskName: []string{taskName}})
		if err != nil {
			return err
		}
	}
	return nil
}

func UpsertCdcConsumeTask(ctx context.Context, req *pb.UpsertCdcConsumeTaskRequest) (string, error) {
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
			TaskMode:        constant.TaskModeCdcConsume,
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

		err = UpsertSchemaRouteRule(txnCtx, req.TaskName, req.DatasourceNameS, req.CaseFieldRule, req.SchemaRouteRule, nil, nil)
		if err != nil {
			return err
		}

		fieldInfos := stringutil.GetJSONTagFieldValue(req.CdcConsumeParam)
		for jsonTag, fieldValue := range fieldInfos {
			_, err = model.GetIParamsRW().CreateTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:   req.TaskName,
				TaskMode:   constant.TaskModeCdcConsume,
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

func DeleteCdcConsumeTask(ctx context.Context, req *pb.DeleteCdcConsumeTaskRequest) (string, error) {
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
		err = model.GetIMsgTopicPartitionRW().DeleteMsgTopicPartition(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIMsgDdlRewriteRW().DeleteMsgDdlRewrite(txnCtx, req.TaskName)
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

func ShowCdcConsumeTask(ctx context.Context, req *pb.ShowCdcConsumeTaskRequest) (string, error) {
	var (
		resp  *pb.UpsertCdcConsumeTaskRequest
		param *pb.CdcConsumeParam
	)

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		paramsInfo, err := model.GetIParamsRW().QueryTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName: req.TaskName,
			TaskMode: constant.TaskModeCdcConsume,
		})
		if err != nil {
			return err
		}

		paramMap := make(map[string]string)
		for _, p := range paramsInfo {
			paramMap[p.ParamName] = p.ParamValue
		}

		tableThread, err := strconv.ParseUint(paramMap[constant.ParamNameCdcConsumeTableThread], 10, 64)
		if err != nil {
			return fmt.Errorf("parse table_thread err: %v", err)
		}
		idleResolvedThreshold, err := strconv.ParseUint(paramMap[constant.ParamNameCdcConsumeIdleResolvedThreshold], 10, 64)
		if err != nil {
			return fmt.Errorf("parse idle_resolved_threshold err: %v", err)
		}
		callTimeout, err := strconv.ParseUint(paramMap[constant.ParamNameCdcConsumeCallTimeout], 10, 64)
		if err != nil {
			return fmt.Errorf("parse call_timeout err: %v", err)
		}
		enableCheckpoint, err := strconv.ParseBool(paramMap[constant.ParamNameCdcConsumeEnableCheckpoint])
		if err != nil {
			return fmt.Errorf("parse enable_checkpoint err: %v", err)
		}

		param = &pb.CdcConsumeParam{
			TableThread:           tableThread,
			MessageCompression:    paramMap[constant.ParamNameCdcConsumeMessageCompression],
			IdleResolvedThreshold: idleResolvedThreshold,
			CallTimeout:           callTimeout,
			SubscribeTopic:        paramMap[constant.ParamNameCdcConsumeSubscribeTopic],
			ServerAddress:         stringutil.StringSplit(paramMap[constant.ParamNameCdcConsumeServerAddress], constant.StringSeparatorComma),
			EnableCheckpoint:      enableCheckpoint,
		}

		schemaRouteRule, _, _, err := ShowSchemaRouteRule(txnCtx, taskInfo.TaskName)
		if err != nil {
			return err
		}

		resp = &pb.UpsertCdcConsumeTaskRequest{
			TaskName:        taskInfo.TaskName,
			DatasourceNameS: taskInfo.DatasourceNameS,
			DatasourceNameT: taskInfo.DatasourceNameT,
			Comment:         taskInfo.Comment,
			CaseFieldRule: &pb.CaseFieldRule{
				CaseFieldRuleS: taskInfo.CaseFieldRuleS,
				CaseFieldRuleT: taskInfo.CaseFieldRuleT,
			},
			SchemaRouteRule: schemaRouteRule,
			CdcConsumeParam: param,
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

func StartCdcConsumeTask(ctx context.Context, taskName, workerAddr string) error {
	startTime := time.Now()
	logger.Info("cdc consume task start", zap.String("task_name", taskName))
	logger.Info("cdc consume task get task information", zap.String("task_name", taskName))
	var (
		taskInfo         *task.Task
		sourceDatasource *datasource.Datasource
		targetDatasource *datasource.Datasource
		err              error
	)
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeCdcConsume})
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

	logger.Info("cdc consume task update task status",
		zap.String("task_name", taskInfo.TaskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow))
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

	_, err = model.GetITaskLogRW().CreateLog(ctx, &task.Log{
		TaskName: taskName,
		LogDetail: fmt.Sprintf("%v [%v] the worker [%v] task [%v] cdc consumer start",
			stringutil.CurrentTimeFormatString(),
			stringutil.StringLower(constant.TaskModeStmtMigrate),
			taskInfo.WorkerAddr,
			taskName,
		),
	})
	if err != nil {
		return err
	}

	logger.Info("cdc consume task get task params",
		zap.String("task_name", taskInfo.TaskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow))
	migrateParams, err := getCdcConsumeTasKParams(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}

	switch taskInfo.TaskFlow {
	case constant.TaskFlowTiDBToOracle, constant.TaskFlowTiDBToPostgres, constant.TaskFlowTiDBToMYSQL, constant.TaskFlowTiDBToTiDB:
		dm := &taskflow.CdcConsumeTask{
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
		return fmt.Errorf("the cdc consume task [%s] datasource [%s] source [%s] isn't support, please contact auhtor or reselect", taskName, sourceDatasource.DatasourceName, sourceDatasource.DbType)
	}
	return nil
}

func StopCdcConsumeTask(ctx context.Context, taskName string) error {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		_, err := model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
			TaskName: taskName,
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

func getCdcConsumeTasKParams(ctx context.Context, taskName string) (*pb.CdcConsumeParam, error) {
	taskParam := &pb.CdcConsumeParam{}

	migrateParams, err := model.GetIParamsRW().QueryTaskCustomParam(ctx, &params.TaskCustomParam{
		TaskName: taskName,
		TaskMode: constant.TaskModeCdcConsume,
	})
	if err != nil {
		return taskParam, err
	}
	for _, p := range migrateParams {
		if strings.EqualFold(p.ParamName, constant.ParamNameCdcConsumeTableThread) {
			tableThread, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.TableThread = tableThread
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCdcConsumeIdleResolvedThreshold) {
			idleResolvedThreshold, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.IdleResolvedThreshold = idleResolvedThreshold
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCdcConsumeCallTimeout) {
			callTimeout, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return taskParam, err
			}
			taskParam.CallTimeout = callTimeout
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCdcConsumeMessageCompression) {
			taskParam.MessageCompression = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCdcConsumeSubscribeTopic) {
			taskParam.SubscribeTopic = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCdcConsumeServerAddress) {
			taskParam.ServerAddress = stringutil.StringSplit(p.ParamValue, constant.StringSeparatorComma)
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameCdcConsumeEnableCheckpoint) {
			enableCheckpoint, err := strconv.ParseBool(p.ParamValue)
			if err != nil {
				return taskParam, err
			}
			taskParam.EnableCheckpoint = enableCheckpoint
		}
	}
	return taskParam, nil
}
