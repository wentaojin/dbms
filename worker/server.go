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
	"strings"

	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/wentaojin/dbms/utils/etcdutil"

	"github.com/wentaojin/dbms/utils/constant"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/configutil"

	"github.com/wentaojin/dbms/logger"
)

type Server struct {
	cfg *Config

	etcdClient *clientv3.Client

	// the used for database connection ready
	// before database connect success, worker disable service
	dbConnReady *atomic.Bool
}

// NewServer creates a new server
func NewServer(cfg *Config) *Server {
	return &Server{
		cfg: cfg,
	}
}

// Start starts to serving
func (s *Server) Start(ctx context.Context) error {
	err := s.initOption(configutil.WithWorkerName(s.cfg.WorkerOptions.Name),
		configutil.WithWorkerAddr(s.cfg.WorkerOptions.WorkerAddr),
		configutil.WithMasterEndpoint(s.cfg.WorkerOptions.Endpoint),
		configutil.WithWorkerLease(s.cfg.WorkerOptions.KeepaliveTTL),
		configutil.WithEventQueueSize(s.cfg.WorkerOptions.EventQueueSize))
	if err != nil {
		return err
	}

	s.etcdClient, err = etcdutil.CreateClient(ctx, stringutil.WrapSchemes(s.cfg.WorkerOptions.Endpoint, false), nil)
	if err != nil {
		return fmt.Errorf("create etcd client for [%v] failed: [%v]", s.cfg.WorkerOptions.Endpoint, err)
	}

	err = s.registerService(ctx)
	if err != nil {
		return err
	}

	// init db conn
	s.createDatabaseConn()

	s.schedule(ctx)

	return nil
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

	s.cfg.WorkerOptions = workerCfg

	return nil
}

func (s *Server) registerService(ctx context.Context) error {
	// init register service and binding lease
	node := &etcdutil.Worker{
		Addr: s.cfg.WorkerOptions.WorkerAddr,
		Role: constant.DefaultInstanceRoleWorker,
	}
	r := etcdutil.NewServiceRegister(
		s.etcdClient, s.cfg.WorkerOptions.WorkerAddr,
		stringutil.StringBuilder(constant.DefaultWorkerRegisterPrefixKey, s.cfg.WorkerOptions.WorkerAddr),
		node.String(), s.cfg.WorkerOptions.KeepaliveTTL)

	err := r.Register(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) createDatabaseConn() {
	conn := etcdutil.NewServiceConnect(s.etcdClient, s.cfg.LogConfig.LogLevel, s.dbConnReady)

	go func() {
		err := conn.Watch(constant.DefaultMasterDatabaseDBMSKey)
		if err != nil {
			panic(err)
		}
	}()
}

func (s *Server) schedule(ctx context.Context) {
	er := NewExecutor()
	er.Execute()

	sr := NewScheduler(ctx, er, s.cfg.WorkerOptions.EventQueueSize)
	sr.Scheduling()

	wr := NewWatcher(ctx, s.cfg.WorkerOptions.WorkerAddr, sr, s.etcdClient)
	wr.Watch()
}

// Close the server, this function can be called multiple times.
func (s *Server) Close() {
	logger.Info("dbms-worker closing server")
	defer func() {
		logger.Info("dbms-worker server closed")
	}()
}
