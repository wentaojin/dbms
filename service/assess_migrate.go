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

func PromptAssessMigrateTask(ctx context.Context, taskName, serverAddr string) error {
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
	if !strings.EqualFold(taskInfo.TaskMode, constant.TaskModeAssessMigrate) && !strings.EqualFold(taskInfo.TaskMode, "") {
		return fmt.Errorf("the task name [%s] has be existed in the task mode [%s], please rename the global unqiue task name", taskName, taskInfo.TaskMode)
	}

	if strings.EqualFold(taskInfo.TaskMode, constant.TaskModeAssessMigrate) {
		if err = stringutil.PromptForAnswerOrAbortError(
			"Yes, I know my configuration file will be overwrite.",
			fmt.Sprintf("\nThis operation will overwrite the task_mode [%s] task_name [%s] configuration file.",
				color.HiYellowString(strings.ToLower(constant.TaskModeAssessMigrate)),
				color.HiYellowString(taskInfo.TaskName),
			)+"\nAre you sure to continue?",
		); err != nil {
			return err
		}
		_, err = DeleteAssessMigrateTask(ctx, &pb.DeleteAssessMigrateTaskRequest{TaskName: []string{taskName}})
		if err != nil {
			return err
		}
	}
	return nil
}

func UpsertAssessMigrateTask(ctx context.Context, req *pb.UpsertAssessMigrateTaskRequest) (string, error) {
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
			TaskMode:        constant.TaskModeAssessMigrate,
			TaskFlow:        stringutil.StringBuilder(stringutil.StringUpper(datasourceS.DbType), constant.StringSeparatorAite, stringutil.StringUpper(datasourceT.DbType)),
			DatasourceNameS: req.DatasourceNameS,
			DatasourceNameT: req.DatasourceNameT,
			CaseFieldRuleS:  req.AssessMigrateParam.CaseFieldRuleS,
			TaskStatus:      constant.TaskDatabaseStatusWaiting,
			Entity:          &common.Entity{Comment: req.Comment},
		})
		if err != nil {
			return err
		}

		fieldInfos := stringutil.GetJSONTagFieldValue(req.AssessMigrateParam)
		for jsonTag, fieldValue := range fieldInfos {
			_, err = model.GetIParamsRW().CreateTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:   req.TaskName,
				TaskMode:   constant.TaskModeAssessMigrate,
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

func DeleteAssessMigrateTask(ctx context.Context, req *pb.DeleteAssessMigrateTaskRequest) (string, error) {
	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		err := model.GetITaskRW().DeleteTask(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIParamsRW().DeleteTaskCustomParam(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetIAssessMigrateTaskRW().DeleteAssessMigrateTaskName(txnCtx, req.TaskName)
		if err != nil {
			return err
		}
		err = model.GetITaskLogRW().DeleteLog(txnCtx, req.TaskName)
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

func ShowAssessMigrateTask(ctx context.Context, req *pb.ShowAssessMigrateTaskRequest) (string, error) {
	var (
		resp  *pb.UpsertAssessMigrateTaskRequest
		param *pb.AssessMigrateParam
	)

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}

		paramsInfo, err := model.GetIParamsRW().QueryTaskCustomParam(txnCtx, &params.TaskCustomParam{
			TaskName: req.TaskName,
			TaskMode: constant.TaskModeAssessMigrate,
		})
		if err != nil {
			return err
		}

		paramMap := make(map[string]string)
		for _, p := range paramsInfo {
			paramMap[p.ParamName] = p.ParamValue
		}

		callTimeout, err := strconv.ParseUint(paramMap[constant.ParamNameAssessMigrateCallTimeout], 10, 64)
		if err != nil {
			return err
		}

		param = &pb.AssessMigrateParam{
			CaseFieldRuleS: paramMap[constant.ParamNameAssessMigrateCaseFieldRuleS],
			SchemaNameS:    paramMap[constant.ParamNameAssessMigrateSchemaNameS],
			CallTimeout:    callTimeout,
		}

		resp = &pb.UpsertAssessMigrateTaskRequest{
			TaskName:           taskInfo.TaskName,
			DatasourceNameS:    taskInfo.DatasourceNameS,
			Comment:            taskInfo.Comment,
			AssessMigrateParam: param,
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

func StartAssessMigrateTask(ctx context.Context, taskName, workerAddr string) error {
	startTime := time.Now()
	logger.Info("assess migrate task start", zap.String("task_name", taskName))
	logger.Info("assess migrate task get task information", zap.String("task_name", taskName))

	var (
		taskInfo         *task.Task
		sourceDatasource *datasource.Datasource
		targetDatasource *datasource.Datasource
		err              error
	)
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		taskInfo, err = model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeAssessMigrate})
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

	logger.Info("assess migrate task update task status",
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

	logger.Info("assess migrate task get params", zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))
	taskParams, err := getAssessMigrateTasKParams(ctx, taskInfo.TaskName)
	if err != nil {
		return err
	}

	if strings.EqualFold(sourceDatasource.DbType, constant.DatabaseTypeOracle) {
		logger.Info("assess migrate task process task", zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow))

		taskTime := time.Now()
		dm := &taskflow.AssessMigrateTask{
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
		logger.Info("assess migrate task process task",
			zap.String("task_name", taskInfo.TaskName), zap.String("task_mode", taskInfo.TaskMode), zap.String("task_flow", taskInfo.TaskFlow),
			zap.String("cost", time.Now().Sub(taskTime).String()))
	} else {
		return fmt.Errorf("current assess migrate task [%s] datasource [%s] source [%s] isn't support, please contact auhtor or reselect", taskName, sourceDatasource.DatasourceName, sourceDatasource.DbType)
	}

	endTime := time.Now()
	_, err = model.GetITaskRW().UpdateTask(ctx, &task.Task{
		TaskName: taskInfo.TaskName,
	}, map[string]interface{}{
		"TaskStatus": constant.TaskDatabaseStatusSuccess,
		"EndTime":    endTime,
	})
	logger.Info("assess migrate task success",
		zap.String("task_name", taskInfo.TaskName),
		zap.String("task_mode", taskInfo.TaskMode),
		zap.String("task_flow", taskInfo.TaskFlow),
		zap.String("detail tips", "please see [assess_migrate_task] detail"),
		zap.String("cost", endTime.Sub(startTime).String()))
	return nil
}

func StopAssessMigrateTask(ctx context.Context, taskName string) error {
	_, err := model.GetITaskRW().UpdateTask(ctx, &task.Task{
		TaskName: taskName,
	}, map[string]interface{}{
		"TaskStatus": constant.TaskDatabaseStatusStopped,
	})
	if err != nil {
		return err
	}
	return nil
}

func GenAssessMigrateTask(ctx context.Context, serverAddr, taskName, outputDir string) error {
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
	taskInfo, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName, TaskMode: constant.TaskModeAssessMigrate})
	if err != nil {
		return err
	}

	if !strings.EqualFold(taskInfo.TaskStatus, constant.TaskDatabaseStatusSuccess) {
		return fmt.Errorf("the [%v] task [%v] status [%v] is running in the worker [%v], please waiting success and retry", stringutil.StringLower(taskInfo.TaskMode),
			taskInfo.TaskName, stringutil.StringLower(taskInfo.TaskStatus), taskInfo.WorkerAddr)
	}

	var w database.IFileWriter
	w = taskflow.NewAssessMigrateFile(ctx, taskInfo.TaskName, taskInfo.TaskFlow, outputDir)
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

	fmt.Printf("the data compare task table records aren't eqal record sql file had be output to [%v], please forward to view\n", outputDir)
	return nil
}

func getAssessMigrateTasKParams(ctx context.Context, taskName string) (*pb.AssessMigrateParam, error) {
	taskParam := &pb.AssessMigrateParam{}

	migrateParams, err := model.GetIParamsRW().QueryTaskCustomParam(ctx, &params.TaskCustomParam{
		TaskName: taskName,
		TaskMode: constant.TaskModeAssessMigrate,
	})
	if err != nil {
		return taskParam, err
	}
	for _, p := range migrateParams {
		if strings.EqualFold(p.ParamName, constant.ParamNameAssessMigrateCaseFieldRuleS) {
			taskParam.CaseFieldRuleS = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameAssessMigrateSchemaNameS) {
			taskParam.SchemaNameS = p.ParamValue
		}
		if strings.EqualFold(p.ParamName, constant.ParamNameAssessMigrateCallTimeout) {
			callTimeout, err := strconv.ParseUint(p.ParamValue, 10, 64)
			if err != nil {
				return nil, err
			}
			taskParam.CallTimeout = callTimeout
		}
	}

	if strings.EqualFold(taskParam.CaseFieldRuleS, constant.ParamValueAssessMigrateCaseFieldRuleUpper) {
		taskParam.SchemaNameS = stringutil.StringUpper(taskParam.SchemaNameS)
	}

	if strings.EqualFold(taskParam.CaseFieldRuleS, constant.ParamValueAssessMigrateCaseFieldRuleLower) {
		taskParam.SchemaNameS = stringutil.StringLower(taskParam.SchemaNameS)
	}

	return taskParam, nil
}
