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
	"errors"
	"fmt"
	"github.com/wentaojin/dbms/proto/pb"
	"go.uber.org/zap"
	"strings"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/wentaojin/dbms/logger"

	"github.com/robfig/cron/v3"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/openapi"
	"github.com/wentaojin/dbms/utils/constant"
	"github.com/wentaojin/dbms/utils/etcdutil"
	"github.com/wentaojin/dbms/utils/stringutil"
	"google.golang.org/grpc"
)

func StartTask(ctx context.Context, cli *clientv3.Client, discoveries *etcdutil.Discovery, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	// the non-scheduled task request express is always empty and needs to be judged by branching.
	// when the scheduled task request calls the StartTask function,
	// the crontab mark will be set artificially, and there is no need to go through the branch to judge.
	if !strings.EqualFold(req.Express, "crontab") {
		// according to the configuring prefix key, and get key information
		key := stringutil.StringBuilder(constant.DefaultMasterCrontabExpressPrefixKey, req.TaskName)
		exprResp, err := etcdutil.GetKey(cli, key)
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
		if len(exprResp.Kvs) != 0 {
			errMsg := fmt.Errorf("the task [%s] express [%s] has be submited to crontab job, disable direct start the task, please wait the crontab task running or clear the crontab task and retry start the task", req.TaskName, req.Express)
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: errMsg.Error(),
			}}, errMsg
		}
	}

	workerAddr, err := discoveries.Assign(req.TaskName)
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: workerAddr,
		}}, err
	}

	w := &etcdutil.Instance{
		Addr:     workerAddr,
		Role:     constant.DefaultInstanceRoleWorker,
		State:    constant.DefaultInstanceBoundState,
		TaskName: req.TaskName,
	}
	_, err = etcdutil.PutKey(cli, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, workerAddr), w.String())
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	logger.Info("the worker task start",
		zap.String("task", req.TaskName),
		zap.String("role", constant.DefaultInstanceRoleWorker),
		zap.String("worker", w.String()))
	// send worker request
	grpcConn, err := grpc.NewClient(workerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	defer grpcConn.Close()

	client := pb.NewWorkerClient(grpcConn)

	request := &pb.OperateWorkerRequest{
		Operate:  constant.TaskOperationStart,
		TaskName: req.TaskName,
	}

	ws, err := client.OperateWorker(ctx, request)
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}

	if !strings.EqualFold(ws.Response.Result, openapi.ResponseResultStatusSuccess) {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: ws.Response.Message,
		}}, errors.New(ws.Response.Message)
	}

	return &pb.OperateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: ws.Response.Message,
	}}, nil
}

func StopTask(ctx context.Context, cli *clientv3.Client, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	t, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	if strings.EqualFold(t.TaskStatus, constant.TaskDatabaseStatusSuccess) || strings.EqualFold(t.TaskStatus, constant.TaskDatabaseStatusStopped) {
		errMsg := fmt.Errorf("the task [%v] had finished or stopped in the worker [%v], stop is prohibited", req.TaskName, t.WorkerAddr)
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: errMsg.Error(),
		}}, errMsg
	}

	keyResp, err := etcdutil.GetKey(cli, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, t.WorkerAddr))
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	if len(keyResp.Kvs) == 0 || len(keyResp.Kvs) > 1 {
		errMsg := fmt.Errorf("the task [%v] is not exist or the worker state over one, current worker nums [%d]", req.TaskName, len(keyResp.Kvs))
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: errMsg.Error(),
		}}, errMsg
	}

	// send worker request
	logger.Warn("the worker task stop",
		zap.String("task", req.TaskName),
		zap.String("role", constant.DefaultInstanceRoleWorker),
		zap.String("worker", t.WorkerAddr))

	grpcConn, err := grpc.NewClient(t.WorkerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	defer grpcConn.Close()

	client := pb.NewWorkerClient(grpcConn)

	request := &pb.OperateWorkerRequest{
		Operate:  constant.TaskOperationStop,
		TaskName: req.TaskName,
	}
	w, err := client.OperateWorker(ctx, request)
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.OperateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: w.Response.Message,
	}}, nil
}

func DeleteTask(ctx context.Context, cli *clientv3.Client, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	t, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}

	if strings.EqualFold(t.TaskName, "") {
		errMsg := fmt.Errorf("the task [%s] is not exist, forbid delete, please double check", req.TaskName)
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: errMsg.Error(),
		}}, errMsg
	}

	// exclude crontab task
	// according to the configuring prefix key, and get key information from etcd
	key := stringutil.StringBuilder(constant.DefaultMasterCrontabExpressPrefixKey, req.TaskName)
	exprResp, err := etcdutil.GetKey(cli, key)
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result: openapi.ResponseResultStatusFailed,
			Message: fmt.Sprintf("the task [%s] get key [%s] failed, disable delete task. If need, please retry delete task",
				req.TaskName, key),
		}}, err
	}

	// if an existing task is the same as the new task, skip creating it again
	if len(exprResp.Kvs) != 0 {
		for _, resp := range exprResp.Kvs {
			var expr *Express
			err = stringutil.UnmarshalJSON(resp.Value, &expr)
			if err != nil {
				return &pb.OperateTaskResponse{Response: &pb.Response{
					Result:  openapi.ResponseResultStatusFailed,
					Message: fmt.Sprintf("the task [%s] and express [%s] unmarshal failed, disable delete task", req.TaskName, req.Express),
				}}, err
			}
			if strings.EqualFold(expr.TaskName, req.TaskName) && !strings.EqualFold(expr.Express, "") {
				return &pb.OperateTaskResponse{Response: &pb.Response{
					Result:  openapi.ResponseResultStatusFailed,
					Message: fmt.Sprintf("the task [%s] and express [%v] has be submited, disable delete task. If need, please clear the crontab task [%s] and retry delete task", req.TaskName, req.Express, req.TaskName),
				}}, err
			}
		}
	}

	if strings.EqualFold(t.TaskStatus, constant.TaskDatabaseStatusRunning) {
		errMsg := fmt.Errorf("the task [%s] has running, the operation cann't [%v], the current task status is [%v], running worker addr is [%v], delete is prohibitted, please stop before deleting", req.TaskName, req.Operate, t.TaskStatus, t.WorkerAddr)
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: errMsg.Error(),
		}}, errMsg
	} else {
		keyResp, err := etcdutil.GetKey(cli, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, t.WorkerAddr))
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
		if len(keyResp.Kvs) == 0 || len(keyResp.Kvs) > 1 {
			errMsg := fmt.Errorf("the task [%v] is not exist or the worker state over one, current worker nums [%d]", req.TaskName, len(keyResp.Kvs))
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: errMsg.Error(),
			}}, errMsg
		}

		// send worker request
		logger.Warn("the worker task delete", zap.String("task", req.TaskName), zap.String("role", constant.DefaultInstanceRoleWorker), zap.String("worker", t.WorkerAddr))

		grpcConn, err := grpc.NewClient(t.WorkerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
		defer grpcConn.Close()

		client := pb.NewWorkerClient(grpcConn)

		request := &pb.OperateWorkerRequest{
			Operate:  constant.TaskOperationDelete,
			TaskName: req.TaskName,
		}
		w, err := client.OperateWorker(ctx, request)
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusSuccess,
			Message: w.Response.Message,
		}}, nil
	}
}

type Status struct {
	TaskName        string   `json:"taskName"`
	TaskFlow        string   `json:"taskFlow"`
	TaskMode        string   `json:"taskMode"`
	DatasourceNameS string   `json:"datasourceNameS"`
	DatasourceNameT string   `json:"datasourceNameT"`
	TaskStatus      string   `json:"taskStatus"`
	WorkerAddr      string   `json:"workerAddr"`
	LogDetail       []string `json:"logDetail"`
}

func StatusTask(ctx context.Context, taskName, serverAddr string, last int) (Status, error) {
	etcdClient, err := etcdutil.CreateClient(ctx, []string{stringutil.WithHostPort(serverAddr)}, nil)
	if err != nil {
		return Status{}, err
	}
	keyResp, err := etcdutil.GetKey(etcdClient, constant.DefaultMasterDatabaseDBMSKey, clientv3.WithPrefix())
	if err != nil {
		return Status{}, err
	}

	switch {
	case len(keyResp.Kvs) > 1:
		return Status{}, fmt.Errorf("get key [%v] values is over one record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	case len(keyResp.Kvs) == 1:
		// open database conn
		var dbCfg *model.Database
		err = json.Unmarshal(keyResp.Kvs[0].Value, &dbCfg)
		if err != nil {
			return Status{}, fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", stringutil.BytesToString(keyResp.Kvs[0].Value), err)
		}
		err = model.CreateDatabaseReadWrite(dbCfg)
		if err != nil {
			return Status{}, err
		}
	default:
		return Status{}, fmt.Errorf("get key [%v] values isn't exist record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	}

	resp := Status{}
	err = model.Transaction(ctx, func(txnCtx context.Context) error {
		t, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: taskName})
		if err != nil {
			return err
		}
		resp.TaskName = t.TaskName
		resp.TaskFlow = t.TaskFlow
		resp.TaskMode = t.TaskMode
		resp.DatasourceNameS = t.DatasourceNameS
		resp.DatasourceNameT = t.DatasourceNameT
		resp.TaskStatus = t.TaskStatus
		resp.WorkerAddr = t.WorkerAddr

		logs, err := model.GetITaskLogRW().QueryLog(txnCtx, &task.Log{
			TaskName: taskName,
		}, last)
		if err != nil {
			return err
		}
		if len(logs) != 0 {
			// origin slice desc create_time
			for _, log := range logs {
				resp.LogDetail = append(resp.LogDetail, log.LogDetail)
			}
			// reverse slice
			for left, right := 0, len(resp.LogDetail)-1; left < right; left, right = left+1, right-1 {
				resp.LogDetail[left], resp.LogDetail[right] = resp.LogDetail[right], resp.LogDetail[left]
			}
		}
		return nil
	})
	if err != nil {
		return Status{}, err
	}
	return resp, nil
}

type List struct {
	TaskName        string `json:"taskName"`
	TaskFlow        string `json:"taskFlow"`
	TaskMode        string `json:"taskMode"`
	CaseFieldRuleS  string `json:"caseFieldRuleS"`
	CaseFieldRuleT  string `json:"caseFieldRuleT"`
	DatasourceNameS string `json:"datasourceNameS"`
	DatasourceNameT string `json:"datasourceNameT"`
	TaskStatus      string `json:"taskStatus"`
	TaskInit        string `json:"taskInit"`
	WorkerAddr      string `json:"workerAddr"`
}

func ListTask(ctx context.Context, taskName, serverAddr string) ([]List, error) {
	var lists []List
	etcdClient, err := etcdutil.CreateClient(ctx, []string{stringutil.WithHostPort(serverAddr)}, nil)
	if err != nil {
		return lists, err
	}
	keyResp, err := etcdutil.GetKey(etcdClient, constant.DefaultMasterDatabaseDBMSKey, clientv3.WithPrefix())
	if err != nil {
		return lists, err
	}

	switch {
	case len(keyResp.Kvs) > 1:
		return lists, fmt.Errorf("get key [%v] values is over one record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	case len(keyResp.Kvs) == 1:
		// open database conn
		var dbCfg *model.Database
		err = json.Unmarshal(keyResp.Kvs[0].Value, &dbCfg)
		if err != nil {
			return lists, fmt.Errorf("json unmarshal [%v] to struct database faild: [%v]", stringutil.BytesToString(keyResp.Kvs[0].Value), err)
		}
		err = model.CreateDatabaseReadWrite(dbCfg)
		if err != nil {
			return lists, err
		}
	default:
		return lists, fmt.Errorf("get key [%v] values isn't exist record from etcd server, it's panic, need check and fix, records are [%v]", constant.DefaultMasterDatabaseDBMSKey, keyResp.Kvs)
	}

	if strings.EqualFold(taskName, "") {
		tasks, err := model.GetITaskRW().ListTask(ctx, 0, 0)
		if err != nil {
			return lists, err
		}
		for _, t := range tasks {
			lists = append(lists, List{
				TaskName:        t.TaskName,
				TaskFlow:        t.TaskFlow,
				TaskMode:        t.TaskMode,
				CaseFieldRuleS:  t.CaseFieldRuleS,
				CaseFieldRuleT:  t.CaseFieldRuleT,
				DatasourceNameS: t.DatasourceNameS,
				DatasourceNameT: t.DatasourceNameT,
				TaskStatus:      t.TaskStatus,
				TaskInit:        t.TaskInit,
				WorkerAddr:      t.WorkerAddr,
			})
		}
		return lists, nil
	}

	t, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: taskName})
	if err != nil {
		return lists, err
	}
	if strings.EqualFold(t.TaskName, "") {
		return []List{}, nil
	}
	lists = append(lists, List{
		TaskName:        t.TaskName,
		TaskFlow:        t.TaskFlow,
		TaskMode:        t.TaskMode,
		CaseFieldRuleS:  t.CaseFieldRuleS,
		CaseFieldRuleT:  t.CaseFieldRuleT,
		DatasourceNameS: t.DatasourceNameS,
		DatasourceNameT: t.DatasourceNameT,
		TaskStatus:      t.TaskStatus,
		TaskInit:        t.TaskInit,
		WorkerAddr:      t.WorkerAddr,
	})
	return lists, nil
}

type Cronjob struct {
	cli         *clientv3.Client
	discoveries *etcdutil.Discovery
	taskName    string
}

func NewCronjob(
	cli *clientv3.Client, discoveries *etcdutil.Discovery, taskName string) *Cronjob {
	return &Cronjob{
		cli:         cli,
		discoveries: discoveries,
		taskName:    taskName}
}

func (c *Cronjob) Run() {
	key := stringutil.StringBuilder(constant.DefaultMasterCrontabExpressPrefixKey, c.taskName)
	// rpc error: code = Canceled desc = context canceled
	// resolved using context.TODO()
	_, err := StartTask(context.TODO(), c.cli, c.discoveries, &pb.OperateTaskRequest{
		Operate:  constant.TaskOperationStart,
		TaskName: c.taskName,
		Express:  "crontab", // only avoid start the task error
	})
	if err != nil {
		logger.Warn("the crontab task start failed, If need, please retry submit crontab task",
			zap.String("task", c.taskName),
			zap.String("key", key),
			zap.Error(err))
		panic(fmt.Errorf("the crontab task [%s] start failed, If need, please retry submit crontab task, error: %v", c.taskName, err))
	}
	_, err = etcdutil.DeleteKey(c.cli, key)
	if err != nil {
		logger.Warn("the crontab task delete key failed",
			zap.String("task", c.taskName),
			zap.String("key", key),
			zap.Error(err))
		panic(fmt.Errorf("the crontab task [%s] delete key [%s] failed, error: %v", c.taskName, key, err))
	}
}

// Express used to the store key-value information
type Express struct {
	TaskName string
	EntryID  cron.EntryID
	Express  string
}

func (e *Express) String() string {
	jsonStr, _ := stringutil.MarshalJSON(e)
	return jsonStr
}

func AddCronTask(ctx context.Context, c *cron.Cron, cli *clientv3.Client, req *pb.OperateTaskRequest, job cron.Job) (*pb.OperateTaskResponse, error) {
	t, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	if !strings.EqualFold(t.TaskStatus, constant.TaskDatabaseStatusWaiting) && strings.EqualFold(t.TaskName, req.TaskName) {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result: openapi.ResponseResultStatusFailed,
			Message: fmt.Sprintf("the task [%s] has exist and task_status [%s], disable creating crontab task. If need, please delete the task [%s] and retry crontab task",
				req.TaskName, stringutil.StringLower(t.TaskStatus), req.TaskName),
		}}, err
	}

	// according to the configuring prefix key, and get key information from etcd
	key := stringutil.StringBuilder(constant.DefaultMasterCrontabExpressPrefixKey, req.TaskName)
	expressKeyResp, err := etcdutil.GetKey(cli, key)
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result: openapi.ResponseResultStatusFailed,
			Message: fmt.Sprintf("the task [%s] get key [%s] failed, disable creating crontab task. If need, please retry crontab task",
				req.TaskName, key),
		}}, err
	}

	// if an existing task is the same as the new task, skip creating it again
	if len(expressKeyResp.Kvs) != 0 {
		for _, resp := range expressKeyResp.Kvs {
			var expr *Express
			err = stringutil.UnmarshalJSON(resp.Value, &expr)
			if err != nil {
				return &pb.OperateTaskResponse{Response: &pb.Response{
					Result:  openapi.ResponseResultStatusFailed,
					Message: fmt.Sprintf("the task [%s] and express [%s] unmarshal failed, disable repeat create crontab task. If need, please delete the task [%s] and retry crontab task", req.TaskName, req.Express, req.TaskName),
				}}, err
			}
			if strings.EqualFold(expr.TaskName, req.TaskName) && strings.EqualFold(expr.Express, req.Express) {
				return &pb.OperateTaskResponse{Response: &pb.Response{
					Result:  openapi.ResponseResultStatusFailed,
					Message: fmt.Sprintf("the task [%s] and express [%v] has be submited, disable repeat create crontab task. If need, please delete the task [%s] and retry crontab task", req.TaskName, req.Express, req.TaskName),
				}}, err
			}
		}
	}

	newEntryID, err := c.AddJob(req.Express, cron.NewChain(cron.Recover(logger.NewCronLogger(logger.GetRootLogger()))).Then(job))
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}

	newExprVal := &Express{
		TaskName: req.TaskName,
		EntryID:  newEntryID,
		Express:  req.Express,
	}
	_, err = etcdutil.PutKey(cli, stringutil.StringBuilder(constant.DefaultMasterCrontabExpressPrefixKey, req.TaskName), newExprVal.String())
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.OperateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: fmt.Sprintf("add crontab task [%v] express [%s] success, please waiting schedule running", req.TaskName, req.Express),
	}}, nil

}

func ClearCronTask(ctx context.Context, c *cron.Cron, cli *clientv3.Client, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	t, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	if strings.EqualFold(t.TaskStatus, constant.TaskDatabaseStatusWaiting) && strings.EqualFold(t.TaskName, req.TaskName) {
		key := stringutil.StringBuilder(constant.DefaultMasterCrontabExpressPrefixKey, req.TaskName)
		exprResp, err := etcdutil.GetKey(cli, key)
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result: openapi.ResponseResultStatusFailed,
				Message: fmt.Sprintf("the task [%s] get key [%s] failed, disable clear crontab task. If need, please retry clear crontab task",
					req.TaskName, key),
			}}, err
		}
		if len(exprResp.Kvs) != 1 {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result: openapi.ResponseResultStatusFailed,
				Message: fmt.Sprintf("the task [%s] get key [%s] failed, disable clear crontab task. the current return result counts [%d] are not equal one",
					req.TaskName, key, len(exprResp.Kvs)),
			}}, err
		}

		var expr *Express
		err = stringutil.UnmarshalJSON(exprResp.Kvs[0].Value, &expr)
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: fmt.Sprintf("the task [%s] and express [%s] unmarshal failed, disable clear crontab task. If need, please retry clear the crontab task", req.Express, req.TaskName),
			}}, err
		}
		c.Remove(expr.EntryID)

		_, err = etcdutil.DeleteKey(cli, key)
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}

		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusSuccess,
			Message: fmt.Sprintf("clear crontab task [%v] success", req.TaskName),
		}}, nil
	} else {
		errMsg := fmt.Errorf("the crontab task [%s] has scheduled, the operation cann't [%v], the current task status is [%v], running worker addr is [%v]", req.TaskName, stringutil.StringLower(req.Operate), t.TaskStatus, t.WorkerAddr)
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: errMsg.Error(),
		}}, errMsg
	}
}

func DisplayCronTask(ctx context.Context, serverAddr, taskName string) ([]*Express, error) {
	cli, err := etcdutil.CreateClient(ctx, []string{serverAddr}, nil)
	if err != nil {
		return nil, fmt.Errorf("the server create client failed, disable display crontab task. If need, please retry display crontab task, error: %v", err)
	}

	if !strings.EqualFold(taskName, "") {
		// according to the configuring prefix key, and get key information from etcd
		key := stringutil.StringBuilder(constant.DefaultMasterCrontabExpressPrefixKey, taskName)
		exprKeyResp, err := etcdutil.GetKey(cli, key)
		if err != nil {
			return nil, fmt.Errorf("the task [%s] get result failed, disable display crontab task. If need, please retry display crontab task", taskName)
		}
		if len(exprKeyResp.Kvs) != 1 {
			return nil, fmt.Errorf("the task [%s] get result counts [%d] over than one, disable display crontab task. If need, please retry display crontab task", taskName, len(exprKeyResp.Kvs))
		}

		var expr *Express

		err = stringutil.UnmarshalJSON(exprKeyResp.Kvs[0].Value, &expr)
		if err != nil {
			return nil, err
		}

		return []*Express{expr}, nil
	}

	key := constant.DefaultMasterCrontabExpressPrefixKey
	exprKeyResp, err := etcdutil.GetKey(cli, key, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("get the all tasks results failed, disable display crontab task. If need, please retry display crontab task")
	}
	if len(exprKeyResp.Kvs) == 0 {
		return nil, fmt.Errorf("get the all tasks results counts [%d] are equal zero, disable display crontab task", len(exprKeyResp.Kvs))
	}

	var resps []*Express
	for _, resp := range exprKeyResp.Kvs {
		var expr *Express
		err = stringutil.UnmarshalJSON(resp.Value, &expr)
		if err != nil {
			return nil, err
		}
		resps = append(resps, expr)
	}
	return resps, nil
}
