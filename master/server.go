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
package master

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wentaojin/dbms/service"

	"go.uber.org/zap"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/utils/configutil"

	"github.com/wentaojin/dbms/utils/etcdutil"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/openapi"
	"google.golang.org/grpc"

	"github.com/wentaojin/dbms/proto/pb"

	"github.com/wentaojin/dbms/logger"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Server struct {
	cfg *Config

	// the embed etcd server, and the gRPC/HTTP API server also attached to it.
	etcdSrv etcdutil.Embed

	etcdClient *clientv3.Client

	// the embed etcd election information
	election *etcdutil.Election

	// the used for database connection ready
	// before database connect success, api request disable service
	dbConnReady *atomic.Bool

	errDBConnReady chan error

	// discoveries used for service discovery, watch worker
	discoveries *etcdutil.Discovery

	mutex sync.Mutex

	// UnimplementedMasterServer
	pb.UnimplementedMasterServer
}

// NewServer creates a new server
func NewServer(cfg *Config) *Server {
	return &Server{
		cfg:            cfg,
		etcdSrv:        etcdutil.NewETCDServer(),
		errDBConnReady: make(chan error, 1),
		mutex:          sync.Mutex{},
	}
}

// Start starts to serving
func (s *Server) Start(ctx context.Context) error {
	// gRPC API server
	gRPCSvr := func(gs *grpc.Server) {
		pb.RegisterMasterServer(gs, s)
	}

	// Http API Handler
	// securityOpt is used for rpc client tls in grpc gateway
	apiHandler, err := s.InitOpenAPIHandler()
	if err != nil {
		return err
	}

	// etcd config init
	err = s.etcdSrv.Init(
		configutil.WithMasterName(s.cfg.MasterOptions.Name),
		configutil.WithMasterDir(s.cfg.MasterOptions.DataDir),
		configutil.WithClientAddr(s.cfg.MasterOptions.ClientAddr),
		configutil.WithPeerAddr(s.cfg.MasterOptions.PeerAddr),
		configutil.WithCluster(s.cfg.MasterOptions.InitialCluster),
		configutil.WithClusterState(s.cfg.MasterOptions.InitialClusterState),
		configutil.WithMaxTxnOps(s.cfg.MasterOptions.MaxTxnOps),
		configutil.WithMaxRequestBytes(s.cfg.MasterOptions.MaxRequestBytes),
		configutil.WithAutoCompactionMode(s.cfg.MasterOptions.AutoCompactionMode),
		configutil.WithAutoCompactionRetention(s.cfg.MasterOptions.AutoCompactionRetention),
		configutil.WithQuotaBackendBytes(s.cfg.MasterOptions.QuotaBackendBytes),
		configutil.WithLogLogger(logger.GetRootLogger()),
		configutil.WithLogLevel(s.cfg.LogConfig.LogLevel),
		configutil.WithMasterLease(s.cfg.MasterOptions.KeepaliveTTL),

		configutil.WithGRPCSvr(gRPCSvr),
		configutil.WithHttpHandles(map[string]http.Handler{
			openapi.DBMSAPIBasePath:  apiHandler,
			openapi.DebugAPIBasePath: openapi.GetHTTPDebugHandler(),
		}),
		configutil.WithMasterJoin(s.cfg.MasterOptions.Join),
	)
	if err != nil {
		return err
	}

	// prepare config to join an existing cluster
	err = s.etcdSrv.Join()
	if err != nil {
		return err
	}

	// start embed etcd server, gRPC API server and HTTP (API, status and debug) server.
	err = s.etcdSrv.Run()
	if err != nil {
		return err
	}

	s.cfg.MasterOptions = s.etcdSrv.GetConfig()

	// create an etcd client used in the whole server instance.
	// NOTE: we only use the local member's address now, but we can use all endpoints of the cluster if needed.
	s.etcdClient, err = etcdutil.CreateClient(ctx, []string{stringutil.WithHostPort(s.cfg.MasterOptions.ClientAddr)}, nil)
	if err != nil {
		return err
	}

	// service discovery
	err = s.discovery()
	if err != nil {
		return err
	}

	// service election
	s.election, err = etcdutil.NewElection(&etcdutil.Election{
		EtcdClient: s.etcdClient,
		LeaseTTL:   etcdutil.DefaultLeaderElectionTTLSecond,
		Callbacks: etcdutil.Callbacks{
			OnStartedLeading: func(ctx context.Context) error {
				// we're notified when we start - this is where you would
				// usually put your code
				logger.Info("listening server addr request", zap.String("address", s.cfg.MasterOptions.ClientAddr))

				// init db conn
				s.createDatabaseConn()

				// pending, start receive request
				for {
					select {
					case <-ctx.Done():
						return nil
					case err = <-s.errDBConnReady:
						return err
					}
				}
			},
			OnStoppedLeading: func(ctx context.Context) error {
				// we can do cleanup here
				// reset
				s.dbConnReady.Store(false)

				logger.Info("server leader lost", zap.String("lost leader identity", s.cfg.MasterOptions.ClientAddr))
				return nil
			},
			OnNewLeader: func(identity string) {
				// we're notified when new leader elected
				if strings.EqualFold(s.cfg.MasterOptions.ClientAddr, identity) {
					return
				}
				logger.Info("server new leader elected", zap.String("new node identity", identity))
			},
		},
		Prefix:   etcdutil.DefaultLeaderElectionPrefix,
		Identity: s.cfg.MasterOptions.ClientAddr,
	})
	if err != nil {
		return err
	}

	errs := s.election.Run(ctx)
	if errs != nil {
		var errStrs []string
		for _, e := range errs {
			errStrs = append(errStrs, e.Error())
		}
		return fmt.Errorf("server election run failed, there are [%d] error(s), error: [%v]", len(errs), stringutil.StringJoin(errStrs, "\n"))
	}

	return nil
}

// discovery used for master and worker service discovery
func (s *Server) discovery() error {
	s.discoveries = etcdutil.NewServiceDiscovery(s.etcdClient)

	err := s.discoveries.Discovery(constant.DefaultWorkerRegisterPrefixKey)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) createDatabaseConn() {
	// get meta database conn info
	conn := etcdutil.NewServiceConnect(s.etcdClient, s.cfg.MasterOptions.LogLevel, s.dbConnReady)

	go func() {
		err := conn.Watch(constant.DefaultMasterDatabaseDBMSKey)
		if err != nil {
			s.errDBConnReady <- err
		}
	}()
}

// Close the RPC server, this function can be called multiple times.
func (s *Server) Close() {
	logger.Info("dbms-master closing server")
	defer func() {
		logger.Info("dbms-master server closed")
	}()

	// close the etcd and other attached servers
	if s.etcdSrv != nil {
		s.etcdSrv.Close()
	}
}

// OperateTask implements MasterServer.OperateTask.
func (s *Server) OperateTask(ctx context.Context, req *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	switch strings.ToUpper(req.Operate) {
	case "SUBMIT":
		err := service.UpsertTask(s.etcdClient, req)
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  constant.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
	case "DELETE":
		err := service.DeleteTask(s.etcdClient, req)
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  constant.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
	case "KILL":
		err := service.KillTask(s.etcdClient, req)
		if err != nil {
			return &pb.OperateTaskResponse{Response: &pb.Response{
				Result:  constant.ResponseResultStatusFailed,
				Message: err.Error(),
			}}, err
		}
	default:
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result:  constant.ResponseResultStatusFailed,
			Message: fmt.Sprintf("task [%v] operate [%v] isn't support, current support submit、delete and kill", req.TaskName, req.Operate),
		}}, fmt.Errorf("task [%v] operate [%v] isn't support, current support submit、delete and kill", req.TaskName, req.Operate)
	}
	return &pb.OperateTaskResponse{Response: &pb.Response{
		Result: constant.ResponseResultStatusSuccess,
	}}, nil
}
