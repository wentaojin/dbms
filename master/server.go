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
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/getkin/kin-openapi/openapi3"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/gin"

	middleware "github.com/deepmap/oapi-codegen/pkg/gin-middleware"

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

	"github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Server struct {
	*Config

	// the embed etcd server, and the gRPC/HTTP API server also attached to it.
	etcdSrv etcdutil.Embed

	etcdClient *clientv3.Client

	// the embed etcd election information
	election *etcdutil.Election

	// database connect is whether active, if active , it can service, otherwise disable service
	dbConnReady *atomic.Bool

	// discoveries used for service discovery, watch worker
	discoveries *etcdutil.Discovery

	mutex sync.Mutex

	cron *cron.Cron

	// UnimplementedMasterServer
	pb.UnimplementedMasterServer
}

// NewServer creates a new server
func NewServer(cfg *Config) *Server {
	return &Server{
		Config:      cfg,
		etcdSrv:     etcdutil.NewETCDServer(),
		dbConnReady: new(atomic.Bool),
		cron:        cron.New(cron.WithLogger(logger.NewCronLogger(logger.GetRootLogger()))),
		mutex:       sync.Mutex{},
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
	apiHandler, err := s.initOpenAPIHandler()
	if err != nil {
		return err
	}

	// etcd config init
	err = s.etcdSrv.Init(
		configutil.WithMasterName(s.MasterOptions.Name),
		configutil.WithMasterDir(s.MasterOptions.DataDir),
		configutil.WithClientAddr(s.MasterOptions.ClientAddr),
		configutil.WithPeerAddr(s.MasterOptions.PeerAddr),
		configutil.WithCluster(s.MasterOptions.InitialCluster),
		configutil.WithClusterState(s.MasterOptions.InitialClusterState),
		configutil.WithMaxTxnOps(s.MasterOptions.MaxTxnOps),
		configutil.WithMaxRequestBytes(s.MasterOptions.MaxRequestBytes),
		configutil.WithAutoCompactionMode(s.MasterOptions.AutoCompactionMode),
		configutil.WithAutoCompactionRetention(s.MasterOptions.AutoCompactionRetention),
		configutil.WithQuotaBackendBytes(s.MasterOptions.QuotaBackendBytes),
		configutil.WithLogLogger(logger.GetRootLogger()),
		configutil.WithLogLevel(s.LogConfig.LogLevel),
		configutil.WithMasterLease(s.MasterOptions.KeepaliveTTL),

		configutil.WithGRPCSvr(gRPCSvr),
		configutil.WithHttpHandles(map[string]http.Handler{
			openapi.DBMSAPIBasePath:  apiHandler,
			openapi.DebugAPIBasePath: openapi.GetHTTPDebugHandler(),
		}),
		configutil.WithMasterJoin(s.MasterOptions.Join),
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

	s.MasterOptions = s.etcdSrv.GetConfig()

	// create an etcd client used in the whole server instance.
	// NOTE: we only use the local member's address now, but we can use all endpoints of the cluster if needed.
	s.etcdClient, err = etcdutil.CreateClient(ctx, []string{stringutil.WithHostPort(s.MasterOptions.ClientAddr)}, nil)
	if err != nil {
		return err
	}

	err = s.serviceDiscovery()
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
				logger.Info("listening server addr request", zap.String("address", s.MasterOptions.ClientAddr))

				s.watchConn()

				s.cron.Start()

				s.crontab(ctx)

				// pending, start receive request
				for {
					select {
					case <-ctx.Done():
						return nil
					}
				}
			},
			OnStoppedLeading: func(ctx context.Context) error {
				// we can do cleanup here
				// reset
				s.dbConnReady.Store(false)

				s.cron.Stop()

				logger.Info("server leader lost", zap.String("lost leader identity", s.MasterOptions.ClientAddr))
				return nil
			},
			OnNewLeader: func(identity string) {
				// we're notified when new leader elected
				if strings.EqualFold(s.MasterOptions.ClientAddr, identity) {
					return
				}
				logger.Info("server new leader elected", zap.String("new node identity", identity))
			},
		},
		Prefix:   etcdutil.DefaultLeaderElectionPrefix,
		Identity: s.MasterOptions.ClientAddr,
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

// serviceDiscovery used for master and worker service discovery
func (s *Server) serviceDiscovery() error {
	s.discoveries = etcdutil.NewServiceDiscovery(s.etcdClient)

	err := s.discoveries.Discovery(constant.DefaultWorkerRegisterPrefixKey)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) watchConn() {
	// get meta database conn info
	conn := etcdutil.NewServiceConnect(s.etcdClient, constant.DefaultInstanceRoleMaster, s.MasterOptions.LogLevel, s.dbConnReady)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("dbms-master create database failed", zap.Any("PANIC", r))
			}
		}()
		err := conn.Watch(constant.DefaultMasterDatabaseDBMSKey)
		if err != nil {
			panic(err)
		}
	}()
}

func (s *Server) crontab(ctx context.Context) {
	conn := service.NewServiceCrontab(ctx, s.etcdClient, s.discoveries, s.cron)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("dbms-master crontab running failed", zap.Any("PANIC", r))
			}
		}()
		err := conn.Watch(
			constant.DefaultMasterCrontabExpressPrefixKey,
			constant.DefaultMasterCrontabEntryPrefixKey,
		)
		if err != nil {
			panic(err)
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
	case constant.TaskOperationStart:
		return service.StartTask(ctx, s.etcdClient, s.discoveries, req)
	case constant.TaskOperationStop:
		return service.StopTask(ctx, s.etcdClient, req)
	case constant.TaskOperationCrontab:
		return service.AddCronTask(ctx, s.cron, s.etcdClient, req,
			service.NewCronjob(ctx, s.etcdClient, s.discoveries, req.TaskName))
		// cleanup tasks are used for scheduled job tasks that are running.
	case constant.TaskOperationClear:
		return service.ClearCronTask(ctx, s.etcdClient, req)
		// delete tasks are used to delete tasks that are not running or have stopped running.
	case constant.TaskOperationDelete:
		return service.DeleteTask(ctx, s.etcdClient, req)
	case constant.TaskOperationGet:
		return service.GetTask(ctx, req)
	default:
		return &pb.OperateTaskResponse{Response: &pb.Response{
			Result: openapi.ResponseResultStatusFailed,
			Message: fmt.Sprintf("task [%v] operate [%v] isn't support, current support operation [%v]", req.TaskName, req.Operate,
				stringutil.StringJoin([]string{constant.TaskOperationStart, constant.TaskOperationStop, constant.TaskOperationCrontab, constant.TaskOperationClear, constant.TaskOperationDelete, constant.TaskOperationGet}, ",")),
		}}, fmt.Errorf("task [%v] operate [%v] isn't support, current support operation [%v]", req.TaskName, req.Operate, stringutil.StringJoin([]string{constant.TaskOperationStart, constant.TaskOperationStop, constant.TaskOperationCrontab, constant.TaskOperationClear, constant.TaskOperationDelete, constant.TaskOperationGet}, ","))
	}
}

// initOpenAPIHandler returns a HTTP handler to handle dbms-master apis
func (s *Server) initOpenAPIHandler() (*gin.Engine, error) {
	swagger, err := openapi.GetSwagger()
	if err != nil {
		return nil, fmt.Errorf("openapi get swagger failed: [%v]", err)
	}
	// servers configure sever api base path, avoid gin-middleware request valid failed, report error {"error":"no matching operation was found"}
	swagger.Servers = openapi3.Servers{&openapi3.Server{URL: openapi.DBMSAPIBasePath}}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	// middlewares
	r.Use(s.cors())

	r.Use(s.proxy())

	// add a ginzap middleware, which:
	//   - log requests, like a combined access and error log.
	r.Use(ginzap.GinzapWithConfig(logger.GetRootLogger().With(zap.String("component", "gin")), &ginzap.Config{
		TimeFormat: logger.LogTimeFmt,
		UTC:        false}))

	// logs all panic to error log
	//   - stack means whether output the stack info.
	r.Use(ginzap.RecoveryWithZap(logger.GetRootLogger().With(zap.String("component", "gin")), true))

	// use validation middleware to check all requests against the OpenAPI schema.
	r.Use(middleware.OapiRequestValidatorWithOptions(swagger, &middleware.Options{
		SilenceServersWarning: true, // forbid servers parameter check warn
	}))

	// register handlers
	openapi.RegisterHandlersWithOptions(r, s, openapi.GinServerOptions{BaseURL: openapi.DBMSAPIBasePath})

	return r, nil
}

// proxy used for reverses request to leader
func (s *Server) proxy() gin.HandlerFunc {
	return func(c *gin.Context) {
		isLeader, err := s.election.CurrentIsLeader(context.TODO())
		if err != nil {
			c.JSON(http.StatusOK, openapi.Response{
				Code:  http.StatusBadRequest,
				Error: err.Error(),
			})
			c.Abort()
			return
		}

		switch {
		case isLeader:
			if !s.dbConnReady.Load() && !strings.EqualFold(c.Request.URL.Path, stringutil.StringBuilder(openapi.DBMSAPIBasePath, openapi.APIDatabasePath)) {
				c.JSON(http.StatusOK, openapi.Response{
					Code:  http.StatusBadRequest,
					Error: fmt.Sprintf("database connection is not ready, disable service, please check whether the database connection has been created. if it has been created, please wait 30s and retry sending the request. if it has not beed created, please create the database connection and wait 30s sending the request."),
				})
				c.Abort()
				return
			} else {
				c.Next()
			}
		default:
			leaderAddr, err := s.election.Leader(context.TODO())
			if err != nil {
				c.JSON(http.StatusOK, openapi.Response{
					Code:  http.StatusBadRequest,
					Error: err.Error(),
				})
				logger.Error("api request get leader error",
					zap.String("request URL", c.Request.URL.String()),
					zap.String("current addr", s.MasterOptions.ClientAddr),
					zap.String("current leader", leaderAddr),
					zap.Bool("current is leader", isLeader))

				c.Abort()
				return
			}

			if strings.EqualFold(leaderAddr, "") {
				c.JSON(http.StatusOK, openapi.Response{
					Code:  http.StatusBadRequest,
					Error: fmt.Sprintf("current leader service election action isn't finished, please wait retrying"),
				})
				logger.Error("api request leader not election",
					zap.String("request URL", c.Request.URL.String()),
					zap.String("current addr", s.MasterOptions.ClientAddr),
					zap.String("current leader", leaderAddr),
					zap.Bool("current is leader", isLeader))
				c.Abort()
				return
			}

			// simpleProxy just reverse to leader host
			proxyUrl, err := url.Parse(stringutil.StringBuilder(`http://`, leaderAddr))
			if err != nil {
				c.JSON(http.StatusOK, openapi.Response{
					Code:  http.StatusBadRequest,
					Error: fmt.Sprintf("current leader service election action isn't finished, please wait retrying"),
				})
				logger.Error("api request parse url",
					zap.String("request URL", c.Request.URL.String()),
					zap.String("current addr", s.MasterOptions.ClientAddr),
					zap.String("current leader", leaderAddr),
					zap.Bool("current is leader", isLeader))
				c.Abort()
				return
			}

			proxy := httputil.NewSingleHostReverseProxy(proxyUrl)
			proxy.Director = func(req *http.Request) {
				req.URL.Scheme = proxyUrl.Scheme
				req.URL.Host = proxyUrl.Host
				req.Host = proxyUrl.Host
			}

			logger.Warn("reverse request to leader",
				zap.String("request URL", c.Request.URL.String()),
				zap.String("current addr", s.MasterOptions.ClientAddr),
				zap.String("current leader", leaderAddr),
				zap.Bool("current is leader", isLeader), zap.String("forward leader", leaderAddr))

			proxy.ServeHTTP(c.Writer, c.Request)
			c.Abort()
			return
		}
	}
}

// cors used for support cors request
func (s *Server) cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		method := c.Request.Method
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Headers", "Content-Type,AccessToken,X-CSRF-Token, Authorization, Token")
		c.Header("Access-Control-Allow-Methods", "POST, GET, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Content-Type")
		c.Header("Access-Control-Allow-Credentials", "true")

		// release all OPTIONS methods
		if method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
		}
		c.Next()
	}
}

func (s *Server) APIListDatabase(c *gin.Context) {
	database, err := s.listDatabase(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: database,
	})
}

func (s *Server) APIPutDatabase(c *gin.Context) {
	var req openapi.APIPutDatabaseJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	database, err := s.upsertDatabase(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: database})
}

func (s *Server) APIDeleteDatabase(c *gin.Context) {
	database, err := s.deleteDatabase(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: database,
	})
}

func (s *Server) APIListDatasource(c *gin.Context) {
	var req openapi.APIListDatasourceJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	datasource, err := s.listDatasource(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusCreated, openapi.Response{
		Code: http.StatusCreated,
		Data: datasource,
	})
}

func (s *Server) APIPutDatasource(c *gin.Context) {
	var req openapi.APIPutDatasourceJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}

	datasource, err := s.upsertDatasource(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: datasource,
	})
}

func (s *Server) APIDeleteDatasource(c *gin.Context) {
	var req openapi.APIDeleteDatasourceJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	delMsg, err := s.deleteDatasource(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: delMsg,
	})
}

func (s *Server) APIDeleteAssessMigrate(c *gin.Context) {
	var req openapi.APIDeleteAssessMigrateJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	delMsg, err := s.deleteAssessMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: delMsg,
	})
}

func (s *Server) APIListAssessMigrate(c *gin.Context) {
	var req openapi.APIListAssessMigrateJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	listMsg, err := s.listAssessMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusCreated, openapi.Response{
		Code: http.StatusCreated,
		Data: listMsg,
	})
}

func (s *Server) APIPutAssessMigrate(c *gin.Context) {
	var req openapi.APIPutAssessMigrateJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	upsertMsg, err := s.upsertAssessMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: upsertMsg,
	})
}

func (s *Server) APIDeleteStructMigrate(c *gin.Context) {
	var req openapi.APIDeleteStructMigrateJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	delMsg, err := s.deleteStructMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: delMsg,
	})
}

func (s *Server) APIListStructMigrate(c *gin.Context) {
	var req openapi.APIListStructMigrateJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	listMsg, err := s.listStructMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusCreated, openapi.Response{
		Code: http.StatusCreated,
		Data: listMsg,
	})
}

func (s *Server) APIPutStructMigrate(c *gin.Context) {
	var req openapi.APIPutStructMigrateJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	upsertMsg, err := s.upsertStructMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: upsertMsg,
	})
}

func (s *Server) APIDeleteStmtMigrate(c *gin.Context) {
	var req openapi.APIDeleteStmtMigrateJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	delMsg, err := s.deleteStmtMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: delMsg,
	})
}

func (s *Server) APIListStmtMigrate(c *gin.Context) {
	var req openapi.APIListStmtMigrateJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	listMsg, err := s.listStmtMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusCreated, openapi.Response{
		Code: http.StatusCreated,
		Data: listMsg,
	})
}

func (s *Server) APIPutStmtMigrate(c *gin.Context) {
	var req openapi.APIPutStmtMigrateJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	upsertMsg, err := s.upsertStmtMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: upsertMsg,
	})
}

func (s *Server) APIDeleteDataCompare(c *gin.Context) {
	var req openapi.APIDeleteDataCompareJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	delMsg, err := s.deleteDataCompareTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: delMsg,
	})
}

func (s *Server) APIListDataCompare(c *gin.Context) {
	var req openapi.APIListDataCompareJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	listMsg, err := s.listDataCompareTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusCreated, openapi.Response{
		Code: http.StatusCreated,
		Data: listMsg,
	})
}

func (s *Server) APIPutDataCompare(c *gin.Context) {
	var req openapi.APIPutDataCompareJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	upsertMsg, err := s.upsertDataCompareTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: upsertMsg,
	})
}

func (s *Server) APIDeleteCsvMigrate(c *gin.Context) {
	var req openapi.APIDeleteCsvMigrateJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	delMsg, err := s.deleteCsvMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: delMsg,
	})
}

func (s *Server) APIListCsvMigrate(c *gin.Context) {
	var req openapi.APIListCsvMigrateJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	listMsg, err := s.listCsvMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusCreated, openapi.Response{
		Code: http.StatusCreated,
		Data: listMsg,
	})
}

func (s *Server) APIPutCsvMigrate(c *gin.Context) {
	var req openapi.APIPutCsvMigrateJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	upsertMsg, err := s.upsertCsvMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: upsertMsg,
	})
}

func (s *Server) APIDeleteSqlMigrate(c *gin.Context) {
	var req openapi.APIDeleteSqlMigrateJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	delMsg, err := s.deleteSqlMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusNoContent, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusNoContent, openapi.Response{
		Code: http.StatusNoContent,
		Data: delMsg,
	})
}

func (s *Server) APIListSqlMigrate(c *gin.Context) {
	var req openapi.APIListSqlMigrateJSONRequestBody
	err := c.Bind(&req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	listMsg, err := s.listSqlMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusCreated, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusCreated, openapi.Response{
		Code: http.StatusCreated,
		Data: listMsg,
	})
}

func (s *Server) APIPutSqlMigrate(c *gin.Context) {
	var req openapi.APIPutSqlMigrateJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	upsertMsg, err := s.upsertSqlMigrateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: upsertMsg,
	})
}

func (s *Server) APIPostTask(c *gin.Context) {
	var req openapi.APIPostTaskJSONRequestBody
	if err := c.Bind(&req); err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	task, err := s.operateTask(c.Request.Context(), req)
	if err != nil {
		c.JSON(http.StatusOK, openapi.Response{
			Code:  http.StatusBadRequest,
			Error: err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, openapi.Response{
		Code: http.StatusOK,
		Data: task,
	})
}
