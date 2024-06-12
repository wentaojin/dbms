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
	"errors"
	"fmt"
	"go.uber.org/zap"
	"strings"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/wentaojin/dbms/logger"

	"github.com/robfig/cron/v3"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/openapi"
	"github.com/wentaojin/dbms/proto/pb"
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

	workerAddr, err := discoveries.GetFreeWorker(req.TaskName)
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: workerAddr,
		}}, err
	}
	// send worker request
	grpcConn, err := grpc.DialContext(ctx, workerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	w, err := client.OperateWorker(ctx, request)
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}

	if !strings.EqualFold(w.Response.Result, openapi.ResponseResultStatusSuccess) {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: w.Response.Message,
		}}, errors.New(w.Response.Message)
	}

	// persistence worker state
	node := &etcdutil.Worker{
		Addr:     workerAddr,
		State:    constant.DefaultWorkerBoundState,
		TaskName: req.TaskName,
	}
	_, err = etcdutil.PutKey(cli, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, workerAddr), node.String())
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

	keyResp, err := etcdutil.GetKey(cli, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, t.WorkerAddr))
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
	grpcConn, err := grpc.DialContext(ctx, t.WorkerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
		keyResp, err := etcdutil.GetKey(cli, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, t.WorkerAddr))
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
		grpcConn, err := grpc.DialContext(ctx, t.WorkerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func GetTask(ctx context.Context, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	resp := struct {
		TaskName        string   `json:"taskName"`
		TaskMode        string   `json:"taskMode"`
		DatasourceNameS string   `json:"datasourceNameS"`
		DatasourceNameT string   `json:"datasourceNameT"`
		TaskStatus      string   `json:"taskStatus"`
		WorkerAddr      string   `json:"workerAddr"`
		LogDetail       []string `json:"logDetail"`
	}{}

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		t, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}
		resp.TaskName = t.TaskName
		resp.TaskMode = t.TaskMode
		resp.DatasourceNameS = t.DatasourceNameS
		resp.DatasourceNameT = t.DatasourceNameT
		resp.TaskStatus = t.TaskStatus
		resp.WorkerAddr = t.WorkerAddr

		l, err := model.GetITaskLogRW().QueryLog(txnCtx, &task.Log{
			TaskName: req.TaskName,
		})
		if err != nil {
			return err
		}
		if len(l) != 0 {
			// DESC 5
			resp.LogDetail = append(resp.LogDetail, l[4].LogDetail)
			resp.LogDetail = append(resp.LogDetail, l[3].LogDetail)
			resp.LogDetail = append(resp.LogDetail, l[2].LogDetail)
			resp.LogDetail = append(resp.LogDetail, l[1].LogDetail)
			resp.LogDetail = append(resp.LogDetail, l[0].LogDetail)
		}
		return nil
	})
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}

	jsonStr, err := stringutil.MarshalJSON(resp)
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	return &pb.OperateTaskResponse{Response: &pb.Response{
		Result:  openapi.ResponseResultStatusSuccess,
		Message: jsonStr,
	}}, nil
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
