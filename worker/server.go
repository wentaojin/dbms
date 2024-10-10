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
package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/wentaojin/dbms/model/params"
	"github.com/wentaojin/dbms/proto/pb"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/openapi"
	"github.com/wentaojin/dbms/service"
	"google.golang.org/grpc"

	"go.uber.org/zap"

	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/wentaojin/dbms/utils/etcdutil"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/configutil"

	"github.com/wentaojin/dbms/logger"
)

type Server struct {
	*Config

	etcdClient *clientv3.Client

	// the used for database connection ready
	// before database connect success, worker disable service
	dbConnReady *atomic.Bool

	mu         sync.Mutex
	cancelCtx  context.Context
	cancelFunc context.CancelFunc

	grpcServer *grpc.Server

	// UnimplementedWorkerServer
	pb.UnimplementedWorkerServer
}

// NewServer creates a new server
func NewServer(cfg *Config) *Server {
	return &Server{
		Config:      cfg,
		mu:          sync.Mutex{},
		dbConnReady: new(atomic.Bool),
	}
}

// Start starts to serving
func (s *Server) Start(ctx context.Context) error {
	err := s.initOption(configutil.WithWorkerName(s.WorkerOptions.Name),
		configutil.WithWorkerAddr(s.WorkerOptions.WorkerAddr),
		configutil.WithMasterEndpoint(s.WorkerOptions.Endpoint),
		configutil.WithWorkerLease(s.WorkerOptions.KeepaliveTTL),
		configutil.WithEventQueueSize(s.WorkerOptions.EventQueueSize),
		configutil.WithBalanceSleepTime(s.WorkerOptions.BalanceSleepTime))
	if err != nil {
		return err
	}

	s.etcdClient, err = etcdutil.CreateClient(ctx, stringutil.WrapSchemes(s.WorkerOptions.Endpoint, false), nil)
	if err != nil {
		return fmt.Errorf("create etcd client for [%v] failed: [%v]", s.WorkerOptions.Endpoint, err)
	}

	err = s.registerService(ctx)
	if err != nil {
		return err
	}

	s.watchConn()

	err = s.gRPCServe()
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return nil
	}
}

func (s *Server) initOption(opts ...configutil.WorkerOption) (err error) {
	workerCfg := configutil.DefaultWorkerServerConfig()
	for _, opt := range opts {
		opt(workerCfg)
	}

	host, port, err := net.SplitHostPort(workerCfg.WorkerAddr)
	if err != nil {
		return fmt.Errorf("net split addr [%s] host port failed: [%v]", workerCfg.WorkerAddr, err)
	}
	if host == "" || host == "0.0.0.0" || len(port) == 0 {
		return fmt.Errorf("worker-addr (%s) must include the 'host' part (should not be '0.0.0.0')", workerCfg.WorkerAddr)
	}

	if strings.EqualFold(workerCfg.Name, "") || strings.EqualFold(workerCfg.Name, configutil.DefaultWorkerNamePrefix) {
		name := stringutil.WrapPrefixIPName(host, configutil.DefaultWorkerNamePrefix, workerCfg.WorkerAddr)
		if strings.EqualFold(name, "") {
			return fmt.Errorf("worker-addr host ip [%s] is not set", workerCfg.WorkerAddr)
		}
		workerCfg.Name = name
	}

	if workerCfg.Endpoint == "" {
		return fmt.Errorf("worker params join is not set, please set master server addrs")
	}

	if workerCfg.KeepaliveTTL <= 0 {
		workerCfg.KeepaliveTTL = configutil.DefaultWorkerKeepaliveTTL
	}

	s.WorkerOptions = workerCfg

	return nil
}

func (s *Server) registerService(ctx context.Context) error {
	// init register service and binding lease
	workerState := constant.DefaultInstanceFreeState

	stateKeyResp, err := etcdutil.GetKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, s.WorkerOptions.WorkerAddr))
	if err != nil {
		return err
	}

	if len(stateKeyResp.Kvs) > 1 {
		return fmt.Errorf("the dbms-worker instance register service failed: service register records [%s] are [%d], should be one record", stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, s.WorkerOptions.WorkerAddr), len(stateKeyResp.Kvs))
	}

	for _, ev := range stateKeyResp.Kvs {
		var w *etcdutil.Instance
		err = stringutil.UnmarshalJSON(ev.Value, &w)
		if err != nil {
			return err
		}
		workerState = w.State
	}

	n := &etcdutil.Instance{
		Addr:  s.WorkerOptions.WorkerAddr,
		Role:  constant.DefaultInstanceRoleWorker,
		State: workerState,
	}

	r := etcdutil.NewServiceRegister(
		s.etcdClient, s.WorkerOptions.WorkerAddr,
		stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, s.WorkerOptions.WorkerAddr),
		n.String(), s.WorkerOptions.KeepaliveTTL)

	err = r.Register(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) watchConn() {
	conn := etcdutil.NewServiceConnect(s.etcdClient, constant.DefaultInstanceRoleWorker, s.LogConfig.LogLevel, s.dbConnReady)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("dbms-worker create database failed",
					zap.Any("panic", r),
					zap.Any("stack", stringutil.BytesToString(debug.Stack())))
			}
		}()
		err := conn.Watch(constant.DefaultMasterDatabaseDBMSKey)
		if err != nil {
			panic(err)
		}
	}()
}

func (s *Server) gRPCServe() error {
	lis, err := net.Listen("tcp", s.WorkerOptions.WorkerAddr)
	if err != nil {
		return err
	}

	s.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServer(s.grpcServer, s)

	err = s.grpcServer.Serve(lis)
	if err != nil {
		return err
	}
	return nil
}

// Close the server, this function can be called multiple times.
func (s *Server) Close() {
	logger.Info("the dbms-worker closing server")
	defer func() {
		logger.Info("the dbms-worker server closed")
	}()

	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

func (s *Server) OperateWorker(ctx context.Context, req *pb.OperateWorkerRequest) (*pb.OperateWorkerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return &pb.OperateWorkerResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}
	// task check
	if t == nil {
		return &pb.OperateWorkerResponse{Response: &pb.Response{
			Result: openapi.ResponseResultStatusFailed,
		}}, fmt.Errorf("the task_name [%s] is not exist, please upsert the task and rerun start the task", req.TaskName)
	}

	switch stringutil.StringUpper(req.Operate) {
	case constant.TaskOperationStart:
		s.cancelCtx, s.cancelFunc = context.WithCancel(context.Background())

		go func() {
			defer s.handlePanicRecover(context.Background(), t)
			s.OperateStart(s.cancelCtx, t)
		}()

		return &pb.OperateWorkerResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusSuccess,
			Message: fmt.Sprintf("the task [%v] is running asynchronously by the task mode [%v] in the worker [%v], please query the task status and waitting finished", t.TaskName, stringutil.StringLower(t.TaskMode), s.WorkerOptions.WorkerAddr),
		}}, nil

	case constant.TaskOperationStop:
		// the task is exits, stop the task
		if s.cancelFunc != nil {
			s.cancelFunc()
			err = s.OperateStop(context.TODO(), t)
			if err != nil {
				return &pb.OperateWorkerResponse{Response: &pb.Response{
					Result:  openapi.ResponseResultStatusFailed,
					Message: err.Error(),
				}}, err
			}
			s.cancelFunc = nil // reset
		}
		return &pb.OperateWorkerResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusSuccess,
			Message: fmt.Sprintf("the task [%v] and the task_mode [%v] sending operation [%v] in the worker [%v] success, the task_name has [%s], please query the task status", t.TaskName, stringutil.StringLower(t.TaskMode), stringutil.StringLower(req.Operate), stringutil.StringLower(s.WorkerOptions.WorkerAddr), stringutil.StringLower(req.Operate)),
		}}, nil
	case constant.TaskOperationDelete:
		if strings.EqualFold(t.TaskStatus, constant.TaskDatabaseStatusRunning) {
			return &pb.OperateWorkerResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: fmt.Sprintf("the worker [%v] task_name [%v] task_mode [%v] task_flow [%v] is running, disabled sending the delete request, please stop and delete the task", s.WorkerOptions.WorkerAddr, t.TaskName, t.TaskMode, t.TaskFlow),
			}}, fmt.Errorf("the worker [%v] task_name [%v] task_mode [%v] task_flow [%v] is running, disabled sending the delete request, please stop and delete the task", s.WorkerOptions.WorkerAddr, t.TaskName, t.TaskMode, t.TaskFlow)
		}

		if s.cancelFunc != nil {
			s.cancelFunc()
			err = s.OperateStop(context.TODO(), t)
			if err != nil {
				return &pb.OperateWorkerResponse{Response: &pb.Response{
					Result:  openapi.ResponseResultStatusFailed,
					Message: err.Error(),
				}}, err
			}
			s.cancelFunc = nil // reset
		}

		err = s.OperateDelete(context.TODO(), t)
		if err != nil {
			return &pb.OperateWorkerResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
		return &pb.OperateWorkerResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusSuccess,
			Message: fmt.Sprintf("the task [%v] and the task_mode [%v] sending operation [%v] in the worker [%v] success, the task_name has [%s], please query the task status", t.TaskName, stringutil.StringLower(t.TaskMode), stringutil.StringLower(req.Operate), stringutil.StringLower(s.WorkerOptions.WorkerAddr), stringutil.StringLower(req.Operate)),
		}}, nil
	default:
		return &pb.OperateWorkerResponse{Response: &pb.Response{
			Result: openapi.ResponseResultStatusFailed,
		}}, fmt.Errorf("the worker [%v] task [%v] mode [%v] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, t.TaskName, t.TaskMode)
	}

}

func (s *Server) OperateStart(ctx context.Context, t *task.Task) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			switch stringutil.StringUpper(t.TaskMode) {
			case constant.TaskModeAssessMigrate:
				err := service.StartAssessMigrateTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			case constant.TaskModeStructMigrate:
				err := service.StartStructMigrateTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			case constant.TaskModeStmtMigrate:
				err := service.StartStmtMigrateTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			case constant.TaskModeCSVMigrate:
				err := service.StartCsvMigrateTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			case constant.TaskModeSqlMigrate:
				err := service.StartSqlMigrateTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			case constant.TaskModeDataCompare:
				err := service.StartDataCompareTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			case constant.TaskModeStructCompare:
				err := service.StartStructCompareTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			case constant.TaskModeDataScan:
				err := service.StartDataScanTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
				if err != nil && !errors.Is(err, context.Canceled) {
					panic(err)
				}
			default:
				panic(fmt.Errorf("the worker [%v] task [%v] mode [%v] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, t.TaskName, t.TaskMode))
			}

			w := &etcdutil.Instance{
				Addr:     s.WorkerOptions.WorkerAddr,
				Role:     constant.DefaultInstanceRoleWorker,
				State:    constant.DefaultInstanceFreeState,
				TaskName: "",
			}
			_, err := etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, s.WorkerOptions.WorkerAddr), w.String())
			if err != nil {
				panic(fmt.Errorf("the worker task [%v] finished, but the worker instance wirte [%v] value failed: [%v]", t.TaskName, w.String(), err))
			}

			// task status double check
			newTask, err := model.GetITaskRW().GetTask(ctx, &task.Task{TaskName: t.TaskName})
			if err != nil {
				panic(fmt.Errorf("the worker task [%v] finished, but the query worker task the database task_status failed: [%v]", t.TaskName, err))
			}
			if strings.EqualFold(newTask.TaskStatus, constant.TaskDatabaseStatusSuccess) {
				_, err = etcdutil.DeleteKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultInstanceTaskReferencesPrefixKey, t.TaskName))
				if err != nil {
					panic(fmt.Errorf("the worker task [%v] success, but the worker refrenece delete [%v] value failed: [%v]", t.TaskName, stringutil.StringBuilder(constant.DefaultInstanceTaskReferencesPrefixKey, t.TaskName), err))
				}
			}
			s.cancelFunc()
		}
	}
}

func (s *Server) OperateStop(ctx context.Context, t *task.Task) error {
	switch stringutil.StringUpper(t.TaskMode) {
	case constant.TaskModeAssessMigrate:
		err := service.StopAssessMigrateTask(ctx, t.TaskName)
		if err != nil {
			return err
		}
	case constant.TaskModeStructMigrate:
		err := service.StopStructMigrateTask(ctx, t.TaskName)
		if err != nil {
			return err
		}
	case constant.TaskModeStmtMigrate:
		err := service.StopStmtMigrateTask(ctx, t.TaskName)
		if err != nil {
			return err
		}
	case constant.TaskModeCSVMigrate:
		err := service.StopCsvMigrateTask(ctx, t.TaskName)
		if err != nil {
			return err
		}
	case constant.TaskModeSqlMigrate:
		err := service.StopSqlMigrateTask(ctx, t.TaskName)
		if err != nil {
			return err
		}
	case constant.TaskModeDataCompare:
		err := service.StopDataCompareTask(ctx, t.TaskName)
		if err != nil {
			return err
		}
	case constant.TaskModeStructCompare:
		err := service.StopStructCompareTask(ctx, t.TaskName)
		if err != nil {
			return err
		}
	case constant.TaskModeDataScan:
		err := service.StopDataScanTask(ctx, t.TaskName)
		if err != nil {
			return err
		}
	default:
		panic(fmt.Errorf("the worker [%v] task [%v] mode [%v] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, t.TaskName, t.TaskMode))
	}

	w := &etcdutil.Instance{
		Addr:     s.WorkerOptions.WorkerAddr,
		Role:     constant.DefaultInstanceRoleWorker,
		State:    constant.DefaultInstanceStoppedState,
		TaskName: t.TaskName,
	}
	_, err := etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, s.WorkerOptions.WorkerAddr), w.String())
	if err != nil {
		return err
	}
	logger.Error("the worker task stopped",
		zap.String("task_name", t.TaskName),
		zap.String("task_mode", t.TaskMode),
		zap.String("task_flow", t.TaskFlow),
		zap.String("worker", w.String()))
	return nil
}

func (s *Server) OperateDelete(ctx context.Context, t *task.Task) error {
	switch stringutil.StringUpper(t.TaskMode) {
	case constant.TaskModeAssessMigrate:
		_, err := service.DeleteAssessMigrateTask(ctx, &pb.DeleteAssessMigrateTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
	case constant.TaskModeStructMigrate:
		_, err := service.DeleteStructMigrateTask(ctx, &pb.DeleteStructMigrateTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetIStructMigrateSummaryRW().DeleteStructMigrateSummaryName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIStructMigrateTaskRW().DeleteStructMigrateTaskName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			err = model.GetISequenceMigrateSummaryRW().DeleteSequenceMigrateSummaryName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			err = model.GetISequenceMigrateTaskRW().DeleteSequenceMigrateTaskName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	case constant.TaskModeStmtMigrate:
		_, err := service.DeleteStmtMigrateTask(ctx, &pb.DeleteStmtMigrateTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetIDataMigrateSummaryRW().DeleteDataMigrateSummaryName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataMigrateTaskRW().DeleteDataMigrateTaskName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	case constant.TaskModeCSVMigrate:
		_, err := service.DeleteCsvMigrateTask(ctx, &pb.DeleteCsvMigrateTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}

		var paramsInfo *params.TaskCustomParam
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetIDataMigrateSummaryRW().DeleteDataMigrateSummaryName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataMigrateTaskRW().DeleteDataMigrateTaskName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			paramsInfo, err = model.GetIParamsRW().GetTaskCustomParam(txnCtx, &params.TaskCustomParam{
				TaskName:  t.TaskName,
				TaskMode:  constant.TaskModeCSVMigrate,
				ParamName: constant.ParamNameCsvMigrateOutputDir,
			})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		// clear output dir
		err = stringutil.RemoveAllDir(paramsInfo.ParamValue)
		if err != nil {
			return err
		}
	case constant.TaskModeSqlMigrate:
		_, err := service.DeleteSqlMigrateTask(ctx, &pb.DeleteSqlMigrateTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetISqlMigrateSummaryRW().DeleteSqlMigrateSummaryName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			err = model.GetISqlMigrateTaskRW().DeleteSqlMigrateTaskName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	case constant.TaskModeDataCompare:
		_, err := service.DeleteDataCompareTask(ctx, &pb.DeleteDataCompareTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetIDataCompareSummaryRW().DeleteDataCompareSummaryName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataCompareTaskRW().DeleteDataCompareTaskName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataCompareResultRW().DeleteDataCompareResultName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	case constant.TaskModeStructCompare:
		_, err := service.DeleteStructCompareTask(ctx, &pb.DeleteStructCompareTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetIStructCompareSummaryRW().DeleteStructCompareSummaryName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIStructCompareTaskRW().DeleteStructCompareTaskName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	case constant.TaskModeDataScan:
		_, err := service.DeleteDataScanTask(ctx, &pb.DeleteDataScanTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		err = model.Transaction(ctx, func(txnCtx context.Context) error {
			err = model.GetIDataScanSummaryRW().DeleteDataScanSummaryName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			err = model.GetIDataScanTaskRW().DeleteDataScanTaskName(txnCtx, []string{t.TaskName})
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
	default:
		panic(fmt.Errorf("the worker [%v] task [%v] mode [%v] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, t.TaskName, t.TaskMode))
	}

	// persistence worker state
	w := &etcdutil.Instance{
		Addr:     s.WorkerOptions.WorkerAddr,
		Role:     constant.DefaultInstanceRoleWorker,
		State:    constant.DefaultInstanceFreeState,
		TaskName: "",
	}
	_, err := etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, s.WorkerOptions.WorkerAddr), w.String())
	if err != nil {
		return err
	}
	_, err = etcdutil.DeleteKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultInstanceTaskReferencesPrefixKey, t.TaskName))
	if err != nil {
		return err
	}
	err = model.GetITaskLogRW().DeleteLog(ctx, []string{t.TaskName})
	if err != nil {
		return err
	}
	logger.Error("the worker task deleted",
		zap.String("task_name", t.TaskName),
		zap.String("task_mode", t.TaskMode),
		zap.String("task_flow", t.TaskFlow),
		zap.String("worker", w.String()))
	return nil
}

func (s *Server) handlePanicRecover(ctx context.Context, t *task.Task) {
	if r := recover(); r != nil {
		// recover cancel
		if s.cancelFunc != nil {
			s.cancelFunc()
		}

		errTxn := model.Transaction(ctx, func(txnCtx context.Context) error {
			_, err := model.GetITaskRW().UpdateTask(txnCtx, &task.Task{
				TaskName: t.TaskName,
			}, map[string]interface{}{
				"TaskStatus": constant.TaskDatabaseStatusFailed,
				"EndTime":    time.Now(),
			})
			if err != nil {
				return err
			}
			_, err = model.GetITaskLogRW().CreateLog(txnCtx, &task.Log{
				TaskName: t.TaskName,
				LogDetail: fmt.Sprintf("%v [%v] the worker [%s] task [%v] running [%v], error: [%v], stack: %v",
					stringutil.CurrentTimeFormatString(),
					stringutil.StringLower(t.TaskMode),
					t.WorkerAddr,
					stringutil.StringLower(constant.TaskDatabaseStatusFailed),
					t.TaskName,
					r,
					stringutil.BytesToString(debug.Stack())),
			})
			if err != nil {
				return err
			}
			return nil
		})

		w := &etcdutil.Instance{
			Addr:     s.WorkerOptions.WorkerAddr,
			Role:     constant.DefaultInstanceRoleWorker,
			State:    constant.DefaultInstanceFailedState,
			TaskName: t.TaskName,
		}
		_, errPut := etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultInstanceServiceRegisterPrefixKey, s.WorkerOptions.WorkerAddr), w.String())

		logger.Error("the worker running task panic",
			zap.String("task_name", t.TaskName),
			zap.String("worker_addr", s.WorkerOptions.WorkerAddr),
			zap.Any("panic", r),
			zap.Any("stack", stringutil.BytesToString(debug.Stack())),
			zap.Any("errorTxn", errTxn),
			zap.Any("errorPut", errPut))
	}
}
