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
	workerAddr, err := discoveries.GetFreeWorker()
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
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
	grpcConn, err := grpc.DialContext(ctx, t.WorkerAddr)
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
		Message: w.String(),
	}}, nil
}

type Cronjob struct {
	ctx         context.Context
	cli         *clientv3.Client
	discoveries *etcdutil.Discovery
	taskName    string
}

func NewCronjob(ctx context.Context,
	cli *clientv3.Client, discoveries *etcdutil.Discovery, taskName string) *Cronjob {
	return &Cronjob{
		ctx:         ctx,
		cli:         cli,
		discoveries: discoveries,
		taskName:    taskName}
}

func (c *Cronjob) Run() {
	_, err := StartTask(c.ctx, c.cli, c.discoveries, &pb.OperateTaskRequest{
		Operate:  constant.TaskOperationStart,
		TaskName: c.taskName,
	})
	if err != nil {
		panic(err)
	}
	_, err = etcdutil.TxnKey(c.cli,
		clientv3.OpDelete(
			stringutil.StringBuilder(constant.DefaultMasterCrontabExpressPrefixKey, c.taskName)),
		clientv3.OpDelete(
			stringutil.StringBuilder(constant.DefaultMasterCrontabEntryPrefixKey, c.taskName)))
	if err != nil {
		panic(err)
	}
}

func CrontabTask(ctx context.Context, c *cron.Cron, cli *clientv3.Client, req *pb.OperateTaskRequest, job cron.Job) (*pb.OperateTaskResponse, error) {
	newEntryID, err := c.AddJob(req.Express, cron.NewChain(cron.Recover(logger.NewCronLogger(logger.GetRootLogger()))).Then(job))
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	newEntry := &Entry{EntryID: newEntryID}

	txnResp, err := etcdutil.TxnKey(cli,
		clientv3.OpPut(stringutil.StringBuilder(constant.DefaultMasterCrontabExpressPrefixKey, req.TaskName), req.Express), clientv3.OpPut(stringutil.StringBuilder(constant.DefaultMasterCrontabEntryPrefixKey, req.TaskName), newEntry.String()))
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}

	if txnResp.Succeeded {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result: openapi.ResponseResultStatusSuccess,
		}}, nil
	} else {
		errMsg := fmt.Errorf("transaction failed")
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusSuccess,
			Message: errMsg.Error(),
		}}, errMsg
	}
}

func ClearTask(ctx context.Context, cli *clientv3.Client, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	t, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	if strings.EqualFold(t.TaskName, "") {
		_, err = etcdutil.TxnKey(cli,
			clientv3.OpDelete(
				stringutil.StringBuilder(constant.DefaultMasterCrontabExpressPrefixKey, req.TaskName)),
			clientv3.OpDelete(
				stringutil.StringBuilder(constant.DefaultMasterCrontabEntryPrefixKey, req.TaskName)))
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}

		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result: openapi.ResponseResultStatusSuccess,
		}}, nil
	} else {
		errMsg := fmt.Errorf("the crontab task [%s] has scheduled, the operation cann't [%v], the current task status is [%v], running worker addr is [%v], running rule name is [%v]", req.TaskName, req.Operate, t.TaskStatus, t.WorkerAddr, t.TaskRuleName)
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: errMsg.Error(),
		}}, errMsg
	}
}

func DeleteTask(ctx context.Context, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
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

	switch strings.ToUpper(t.TaskStatus) {
	case constant.TaskDatabaseStatusRunning:
		errMsg := fmt.Errorf("the task [%s] has running, the operation cann't [%v], the current task status is [%v], running worker addr is [%v], running rule name is [%v]", req.TaskName, req.Operate, t.TaskStatus, t.WorkerAddr, t.TaskRuleName)
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: errMsg.Error(),
		}}, errMsg
	default:
		var tasks []string
		tasks = append(tasks, req.TaskName)
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetITaskRW().DeleteTask(txnCtx, tasks)
			if err != nil {
				return err
			}
			err = model.GetIParamsRW().DeleteTaskCustomParam(txnCtx, tasks)
			if err != nil {
				return err
			}
			err = model.GetIStructMigrateTaskRuleRW().DeleteTaskStructRule(txnCtx, tasks)
			if err != nil {
				return err
			}
			err = model.GetIStructMigrateSchemaRuleRW().DeleteSchemaStructRule(txnCtx, tasks)
			if err != nil {
				return err
			}
			err = model.GetIStructMigrateTableRuleRW().DeleteTableStructRule(txnCtx, tasks)
			if err != nil {
				return err
			}
			err = model.GetIStructMigrateColumnRuleRW().DeleteColumnStructRule(txnCtx, tasks)
			if err != nil {
				return err
			}
			err = model.GetIStructMigrateTableAttrsRuleRW().DeleteTableAttrsRule(txnCtx, tasks)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}

		jsonStr, err := stringutil.MarshalJSON(req)
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
}

func GetTask(ctx context.Context, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	resp := struct {
		TaskName     string `json:"taskName"`
		TaskRuleName string `json:"taskRuleName"`
		TaskMode     string `json:"taskMode"`
		TaskStatus   string `json:"taskStatus"`
		WorkerAddr   string `json:"workerAddr"`
		LogDetail    string `json:"logDetail"`
	}{}

	err := model.Transaction(ctx, func(txnCtx context.Context) error {
		t, err := model.GetITaskRW().GetTask(txnCtx, &task.Task{TaskName: req.TaskName})
		if err != nil {
			return err
		}
		resp.TaskName = t.TaskName
		resp.TaskRuleName = t.TaskRuleName
		resp.TaskMode = t.TaskMode
		resp.TaskStatus = t.TaskStatus
		resp.WorkerAddr = t.WorkerAddr

		l, err := model.GetITaskLogRW().QueryLog(txnCtx, &task.Log{
			TaskName: req.TaskName,
		})
		if err != nil {
			return err
		}
		resp.LogDetail = l[len(l)-1].LogDetail

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
