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
	"fmt"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/wentaojin/dbms/model"
	"github.com/wentaojin/dbms/model/task"
	"github.com/wentaojin/dbms/openapi"
	"github.com/wentaojin/dbms/service"
	"google.golang.org/grpc"

	"github.com/wentaojin/dbms/proto/pb"

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
	cancelFunc context.CancelFunc

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
	n := &etcdutil.Node{
		Addr: s.WorkerOptions.WorkerAddr,
		Role: constant.DefaultInstanceRoleWorker,
	}

	r := etcdutil.NewServiceRegister(
		s.etcdClient, s.WorkerOptions.WorkerAddr,
		stringutil.StringBuilder(constant.DefaultWorkerRegisterPrefixKey, s.WorkerOptions.WorkerAddr),
		n.String(), s.WorkerOptions.KeepaliveTTL)

	err := r.Register(ctx)
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
				logger.Error("dbms-worker create database failed", zap.Any("panic", r))
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
	server := grpc.NewServer()
	pb.RegisterWorkerServer(server, s)

	err = server.Serve(lis)
	if err != nil {
		return err
	}
	return nil
}

// Close the server, this function can be called multiple times.
func (s *Server) Close() {
	logger.Info("dbms-worker closing server")
	defer func() {
		logger.Info("dbms-worker server closed")
	}()
}

func (s *Server) OperateWorker(ctx context.Context, req *pb.OperateWorkerRequest) (*pb.OperateWorkerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cancelCtx, cancelFunc := context.WithCancel(context.TODO())
	s.cancelFunc = cancelFunc

	t, err := model.GetITaskRW().GetTask(cancelCtx, &task.Task{TaskName: req.TaskName})
	if err != nil {
		return &pb.OperateWorkerResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusFailed,
			Message: err.Error(),
		}}, err
	}

	switch stringutil.StringUpper(t.TaskMode) {
	case constant.TaskModeAssessMigrate:
		err = s.operateAssessMigrateTask(cancelCtx, t, req)
		if err != nil {
			return &pb.OperateWorkerResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
	case constant.TaskModeStructMigrate:
		err = s.operateStructMigrateTask(cancelCtx, t, req)
		if err != nil {
			return &pb.OperateWorkerResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
	case constant.TaskModeStmtMigrate:
		err = s.operateStmtMigrateTask(cancelCtx, t, req)
		if err != nil {
			return &pb.OperateWorkerResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
	case constant.TaskModeCSVMigrate:
		err = s.operateCsvMigrateTask(cancelCtx, t, req)
		if err != nil {
			return &pb.OperateWorkerResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
	case constant.TaskModeSqlMigrate:
		err = s.operateSqlMigrateTask(cancelCtx, t, req)
		if err != nil {
			return &pb.OperateWorkerResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
	case constant.TaskModeDataCompare:
		err = s.operateDataCompareTask(cancelCtx, t, req)
		if err != nil {
			return &pb.OperateWorkerResponse{Response: &pb.Response{
				Result:  openapi.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
	default:
		return &pb.OperateWorkerResponse{Response: &pb.Response{
			Result: openapi.ResponseResultStatusFailed,
		}}, fmt.Errorf("current worker [%v] task [%v] mode [%v] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, t.TaskName, t.TaskMode)
	}

	if strings.EqualFold(req.Operate, constant.TaskOperationStart) {
		return &pb.OperateWorkerResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusSuccess,
			Message: fmt.Sprintf("the task [%v] is running asynchronously by the task mode [%v] in the worker [%v], please query the task status and waitting finished", t.TaskName, stringutil.StringLower(t.TaskMode), s.WorkerOptions.WorkerAddr),
		}}, nil
	} else {
		return &pb.OperateWorkerResponse{Response: &pb.Response{
			Result:  openapi.ResponseResultStatusSuccess,
			Message: fmt.Sprintf("the task [%v] operation is [%v] by the task mode [%v] in the worker [%v], please query the task status", t.TaskName, stringutil.StringLower(req.Operate), stringutil.StringLower(t.TaskMode), s.WorkerOptions.WorkerAddr),
		}}, nil
	}
}

func (s *Server) operateAssessMigrateTask(ctx context.Context, t *task.Task, req *pb.OperateWorkerRequest) error {
	switch strings.ToUpper(req.Operate) {
	case constant.TaskOperationStart:
		go func() {
			defer s.handlePanicRecover(ctx, t)
			err := service.StartAssessMigrateTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
			if err != nil {
				w := &etcdutil.Worker{
					Addr:     s.WorkerOptions.WorkerAddr,
					State:    constant.DefaultWorkerFailedState,
					TaskName: t.TaskName,
				}
				_, errPut := etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
				if errPut != nil {
					panic(fmt.Errorf("the worker task [%v] failed, there are two errors, one is the worker write [%v] value failed: [%v], two is the worker task running failed: [%v]", t.TaskName, w.String(), errPut, err))
				} else {
					panic(fmt.Errorf("the worker task [%v] failed: [%v]", t.TaskName, err))
				}
			}
			w := &etcdutil.Worker{
				Addr:     s.WorkerOptions.WorkerAddr,
				State:    constant.DefaultWorkerFreeState,
				TaskName: "",
			}
			_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
			if err != nil {
				panic(fmt.Errorf("the worker task [%v] success, but the worker status wirte [%v] value failed: [%v]", t.TaskName, w.String(), err))
			}
			s.cancelFunc()
		}()
		return nil
	case constant.TaskOperationStop:
		s.cancelFunc()
		err := service.StopAssessMigrateTask(context.TODO(), req.TaskName)
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerStoppedState,
			TaskName: req.TaskName,
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	case constant.TaskOperationDelete:
		s.cancelFunc()
		_, err := service.DeleteAssessMigrateTask(context.TODO(),
			&pb.DeleteAssessMigrateTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerFreeState,
			TaskName: "",
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	default:
		s.cancelFunc()
		return fmt.Errorf("current worker [%s] task [%v] operation [%s] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, req.TaskName, req.Operate)
	}
}

func (s *Server) operateStructMigrateTask(ctx context.Context, t *task.Task, req *pb.OperateWorkerRequest) error {
	switch strings.ToUpper(req.Operate) {
	case constant.TaskOperationStart:
		go func() {
			defer s.handlePanicRecover(ctx, t)
			err := service.StartStructMigrateTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
			if err != nil {
				w := &etcdutil.Worker{
					Addr:     s.WorkerOptions.WorkerAddr,
					State:    constant.DefaultWorkerFailedState,
					TaskName: t.TaskName,
				}
				_, errPut := etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
				if errPut != nil {
					panic(fmt.Errorf("the worker task [%v] failed, there are two errors, one is the worker write [%v] value failed: [%v], two is the worker task running failed: [%v]", t.TaskName, w.String(), errPut, err))
				} else {
					panic(fmt.Errorf("the worker task [%v] failed: [%v]", t.TaskName, err))
				}
			}
			w := &etcdutil.Worker{
				Addr:     s.WorkerOptions.WorkerAddr,
				State:    constant.DefaultWorkerFreeState,
				TaskName: "",
			}
			_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
			if err != nil {
				panic(fmt.Errorf("the worker task [%v] success, but the worker status wirte [%v] value failed: [%v]", t.TaskName, w.String(), err))
			}
			s.cancelFunc()
		}()
		return nil
	case constant.TaskOperationStop:
		s.cancelFunc()
		err := service.StopStructMigrateTask(context.TODO(), req.TaskName)
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerStoppedState,
			TaskName: req.TaskName,
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	case constant.TaskOperationDelete:
		s.cancelFunc()
		_, err := service.DeleteStructMigrateTask(context.TODO(),
			&pb.DeleteStructMigrateTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerFreeState,
			TaskName: "",
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	default:
		s.cancelFunc()
		return fmt.Errorf("current worker [%s] task [%v] operation [%s] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, req.TaskName, req.Operate)
	}
}

func (s *Server) operateStmtMigrateTask(ctx context.Context, t *task.Task, req *pb.OperateWorkerRequest) error {
	switch strings.ToUpper(req.Operate) {
	case constant.TaskOperationStart:
		go func() {
			defer s.handlePanicRecover(ctx, t)
			err := service.StartStmtMigrateTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
			if err != nil {
				w := &etcdutil.Worker{
					Addr:     s.WorkerOptions.WorkerAddr,
					State:    constant.DefaultWorkerFailedState,
					TaskName: t.TaskName,
				}
				_, errPut := etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
				if errPut != nil {
					panic(fmt.Errorf("the worker task [%v] failed, there are two errors, one is the worker write [%v] value failed: [%v], two is the worker task running failed: [%v]", t.TaskName, w.String(), errPut, err))
				} else {
					panic(fmt.Errorf("the worker task [%v] failed: [%v]", t.TaskName, err))
				}
			}
			w := &etcdutil.Worker{
				Addr:     s.WorkerOptions.WorkerAddr,
				State:    constant.DefaultWorkerFreeState,
				TaskName: "",
			}
			_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
			if err != nil {
				panic(fmt.Errorf("the worker task [%v] success, but the worker status wirte [%v] value failed: [%v]", t.TaskName, w.String(), err))
			}
			s.cancelFunc()
		}()
		return nil
	case constant.TaskOperationStop:
		s.cancelFunc()
		err := service.StopStmtMigrateTask(context.TODO(), req.TaskName)
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerStoppedState,
			TaskName: req.TaskName,
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	case constant.TaskOperationDelete:
		s.cancelFunc()
		_, err := service.DeleteStmtMigrateTask(context.TODO(),
			&pb.DeleteStmtMigrateTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerFreeState,
			TaskName: "",
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	default:
		s.cancelFunc()
		return fmt.Errorf("current worker [%s] task [%v] operation [%s] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, req.TaskName, req.Operate)
	}
}

func (s *Server) operateCsvMigrateTask(ctx context.Context, t *task.Task, req *pb.OperateWorkerRequest) error {
	switch strings.ToUpper(req.Operate) {
	case constant.TaskOperationStart:
		go func() {
			defer s.handlePanicRecover(ctx, t)
			err := service.StartCsvMigrateTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
			if err != nil {
				w := &etcdutil.Worker{
					Addr:     s.WorkerOptions.WorkerAddr,
					State:    constant.DefaultWorkerFailedState,
					TaskName: t.TaskName,
				}
				_, errPut := etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
				if errPut != nil {
					panic(fmt.Errorf("the worker task [%v] failed, there are two errors, one is the worker write [%v] value failed: [%v], two is the worker task running failed: [%v]", t.TaskName, w.String(), errPut, err))
				} else {
					panic(fmt.Errorf("the worker task [%v] failed: [%v]", t.TaskName, err))
				}
			}
			w := &etcdutil.Worker{
				Addr:     s.WorkerOptions.WorkerAddr,
				State:    constant.DefaultWorkerFreeState,
				TaskName: "",
			}
			_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
			if err != nil {
				panic(fmt.Errorf("the worker task [%v] success, but the worker status wirte [%v] value failed: [%v]", t.TaskName, w.String(), err))
			}
			s.cancelFunc()
		}()
		return nil
	case constant.TaskOperationStop:
		s.cancelFunc()
		err := service.StopCsvMigrateTask(context.TODO(), req.TaskName)
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerStoppedState,
			TaskName: req.TaskName,
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	case constant.TaskOperationDelete:
		s.cancelFunc()
		_, err := service.DeleteCsvMigrateTask(context.TODO(),
			&pb.DeleteCsvMigrateTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerFreeState,
			TaskName: "",
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	default:
		s.cancelFunc()
		return fmt.Errorf("current worker [%s] task [%v] operation [%s] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, req.TaskName, req.Operate)
	}
}

func (s *Server) operateSqlMigrateTask(ctx context.Context, t *task.Task, req *pb.OperateWorkerRequest) error {
	switch strings.ToUpper(req.Operate) {
	case constant.TaskOperationStart:
		go func() {
			defer s.handlePanicRecover(ctx, t)
			err := service.StartSqlMigrateTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
			if err != nil {
				w := &etcdutil.Worker{
					Addr:     s.WorkerOptions.WorkerAddr,
					State:    constant.DefaultWorkerFailedState,
					TaskName: t.TaskName,
				}
				_, errPut := etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
				if errPut != nil {
					panic(fmt.Errorf("the worker task [%v] failed, there are two errors, one is the worker write [%v] value failed: [%v], two is the worker task running failed: [%v]", t.TaskName, w.String(), errPut, err))
				} else {
					panic(fmt.Errorf("the worker task [%v] failed: [%v]", t.TaskName, err))
				}
			}
			w := &etcdutil.Worker{
				Addr:     s.WorkerOptions.WorkerAddr,
				State:    constant.DefaultWorkerFreeState,
				TaskName: "",
			}
			_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
			if err != nil {
				panic(fmt.Errorf("the worker task [%v] success, but the worker status wirte [%v] value failed: [%v]", t.TaskName, w.String(), err))
			}
			s.cancelFunc()
		}()
		return nil
	case constant.TaskOperationStop:
		s.cancelFunc()
		err := service.StopSqlMigrateTask(context.TODO(), req.TaskName)
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerStoppedState,
			TaskName: req.TaskName,
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	case constant.TaskOperationDelete:
		s.cancelFunc()
		_, err := service.DeleteSqlMigrateTask(context.TODO(),
			&pb.DeleteSqlMigrateTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerFreeState,
			TaskName: "",
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	default:
		s.cancelFunc()
		return fmt.Errorf("current worker [%s] task [%v] operation [%s] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, req.TaskName, req.Operate)
	}
}

func (s *Server) operateDataCompareTask(ctx context.Context, t *task.Task, req *pb.OperateWorkerRequest) error {
	switch strings.ToUpper(req.Operate) {
	case constant.TaskOperationStart:
		go func() {
			defer s.handlePanicRecover(ctx, t)
			err := service.StartDataCompareTask(ctx, t.TaskName, s.WorkerOptions.WorkerAddr)
			if err != nil {
				w := &etcdutil.Worker{
					Addr:     s.WorkerOptions.WorkerAddr,
					State:    constant.DefaultWorkerFailedState,
					TaskName: t.TaskName,
				}
				_, errPut := etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
				if errPut != nil {
					panic(fmt.Errorf("the worker task [%v] failed, there are two errors, one is the worker write [%v] value failed: [%v], two is the worker task running failed: [%v]", t.TaskName, w.String(), errPut, err))
				} else {
					panic(fmt.Errorf("the worker task [%v] failed: [%v]", t.TaskName, err))
				}
			}
			w := &etcdutil.Worker{
				Addr:     s.WorkerOptions.WorkerAddr,
				State:    constant.DefaultWorkerFreeState,
				TaskName: "",
			}
			_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
			if err != nil {
				panic(fmt.Errorf("the worker task [%v] success, but the worker status wirte [%v] value failed: [%v]", t.TaskName, w.String(), err))
			}
			s.cancelFunc()
		}()
		return nil
	case constant.TaskOperationStop:
		s.cancelFunc()
		err := service.StopDataCompareTask(context.TODO(), req.TaskName)
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerStoppedState,
			TaskName: req.TaskName,
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	case constant.TaskOperationDelete:
		s.cancelFunc()
		_, err := service.DeleteDataCompareTask(context.TODO(),
			&pb.DeleteDataCompareTaskRequest{TaskName: []string{t.TaskName}})
		if err != nil {
			return err
		}
		// persistence worker state
		w := &etcdutil.Worker{
			Addr:     s.WorkerOptions.WorkerAddr,
			State:    constant.DefaultWorkerFreeState,
			TaskName: "",
		}
		_, err = etcdutil.PutKey(s.etcdClient, stringutil.StringBuilder(constant.DefaultWorkerStatePrefixKey, s.WorkerOptions.WorkerAddr), w.String())
		if err != nil {
			return err
		}
		return nil
	default:
		s.cancelFunc()
		return fmt.Errorf("current worker [%s] task [%v] operation [%s] is not support, please contact author or reselect", s.WorkerOptions.WorkerAddr, req.TaskName, req.Operate)
	}
}

func (s *Server) handlePanicRecover(ctx context.Context, t *task.Task) {
	if r := recover(); r != nil {
		err := model.Transaction(ctx, func(txnCtx context.Context) error {
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
		logger.Error("the worker running task panic",
			zap.String("task_name", t.TaskName),
			zap.Any("panic", r),
			zap.Any("stack", stringutil.BytesToString(debug.Stack())),
			zap.Error(err))
	}
}
