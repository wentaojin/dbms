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
package etcdutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wentaojin/dbms/utils/configutil"

	"github.com/wentaojin/dbms/utils/stringutil"

	"go.etcd.io/etcd/client/pkg/v3/types"

	"github.com/wentaojin/dbms/logger"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

const (
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode os.FileMode = 0o700
)

// Embed etcd server interface
type Embed interface {
	Init(opts ...configutil.MasterOption) (err error)
	Run() (err error)
	Join() (err error)
	GetConfig() *configutil.MasterOptions
	Close()
}

type Etcd struct {
	MasterOptions *configutil.MasterOptions
	Srv           *embed.Etcd
	Config        *embed.Config
}

func NewETCDServer() Embed {
	return new(Etcd)
}

// Init inits embed etcd start config
func (e *Etcd) Init(opts ...configutil.MasterOption) (err error) {
	e.MasterOptions = configutil.DefaultMasterServerConfig()
	for _, opt := range opts {
		opt(e.MasterOptions)
	}

	cfg := embed.NewConfig()
	cfg.LogLevel = e.MasterOptions.LogLevel

	// embed etcd logger
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(e.MasterOptions.Logger)
	cfg.Logger = "zap"
	err = cfg.ZapLoggerBuilder(cfg)
	if err != nil {
		return fmt.Errorf("embed etcd setup logger failed: [%v]", err)
	}

	host, port, err := net.SplitHostPort(e.MasterOptions.ClientAddr)
	if err != nil {
		return fmt.Errorf("net split addr [%s] host port failed: [%v]", e.MasterOptions.ClientAddr, err)
	}
	if host == "" || host == "0.0.0.0" || len(port) == 0 {
		return fmt.Errorf("master-addr (%s) must include the 'host' part (should not be '0.0.0.0')", e.MasterOptions.ClientAddr)
	}

	if strings.EqualFold(e.MasterOptions.Name, "") || strings.EqualFold(e.MasterOptions.Name, configutil.DefaultMasterNamePrefix) {
		e.MasterOptions.Name = stringutil.WrapPrefixIPName(host, configutil.DefaultMasterNamePrefix, e.MasterOptions.PeerAddr)
	}
	cfg.Name = e.MasterOptions.Name

	if strings.EqualFold(e.MasterOptions.DataDir, "") || strings.EqualFold(e.MasterOptions.DataDir, configutil.DefaultMasterDataDirPrefix) {
		e.MasterOptions.DataDir = fmt.Sprintf("%s.%s", configutil.DefaultMasterDataDirPrefix, cfg.Name)
		cfg.Dir = e.MasterOptions.DataDir
	} else {
		e.MasterOptions.DataDir = filepath.Join(e.MasterOptions.DataDir, fmt.Sprintf("%s.%s", configutil.DefaultMasterDataDirPrefix, cfg.Name))
		cfg.Dir = e.MasterOptions.DataDir
	}

	if strings.EqualFold(e.MasterOptions.InitialCluster, "") {
		cfg.InitialCluster = stringutil.WrapSchemesForInitialCluster(e.MasterOptions.PeerAddr, configutil.DefaultMasterNamePrefix, false)
	} else {
		cfg.InitialCluster = stringutil.WrapSchemesForInitialCluster(e.MasterOptions.InitialCluster, configutil.DefaultMasterNamePrefix, false)
	}

	if e.MasterOptions.KeepaliveTTL <= 0 {
		e.MasterOptions.KeepaliveTTL = configutil.DefaultMasterKeepaliveTTL
	}

	// HostWhitelist lists acceptable hostnames from HTTP client requests.
	// Client origin policy protects against "DNS Rebinding" attacks
	// to insecure etcd servers. That is, any website can simply create
	// an authorized DNS name, and direct DNS to "localhost" (or any
	// other address). Then, all HTTP endpoints of etcd server listening
	// on "localhost" becomes accessible, thus vulnerable to DNS rebinding
	// attacks. See "CVE-2018-5702" for more detail.
	//
	// 1. If client connection is secure via HTTPS, allow any hostnames.
	// 2. If client connection is not secure and "HostWhitelist" is not empty,
	//    only allow HTTP requests whose Host field is listed in whitelist.
	//
	// Note that the client origin policy is enforced whether authentication
	// is enabled or not, for tighter controls.
	//
	// By default, "HostWhitelist" is "*", which allows any hostnames.
	// Note that when specifying hostnames, loopback addresses are not added
	// automatically. To allow loopback interfaces, leave it empty or set it "*",
	// or add them to whitelist manually (e.g. "localhost", "127.0.0.1", etc.).
	//
	// CVE-2018-5702 reference:
	// - https://bugs.chromium.org/p/project-zero/issues/detail?id=1447#c2
	// - https://github.com/transmission/transmission/pull/468
	// - https://github.com/etcd-io/etcd/issues/9353
	cfg.HostWhitelist = stringutil.HostWhiteListForInitialCluster(cfg.InitialCluster)

	if !strings.EqualFold(e.MasterOptions.InitialClusterState, "") {
		cfg.ClusterState = e.MasterOptions.InitialClusterState
	}

	if !strings.EqualFold(e.MasterOptions.AutoCompactionMode, "") {
		cfg.AutoCompactionMode = e.MasterOptions.AutoCompactionMode
	}
	if !strings.EqualFold(e.MasterOptions.AutoCompactionRetention, "") {
		cfg.AutoCompactionRetention = e.MasterOptions.AutoCompactionRetention
	}
	if e.MasterOptions.QuotaBackendBytes != 0 {
		cfg.QuotaBackendBytes = e.MasterOptions.QuotaBackendBytes
	}
	if e.MasterOptions.MaxTxnOps != 0 {
		cfg.MaxTxnOps = e.MasterOptions.MaxTxnOps
	}
	if e.MasterOptions.MaxRequestBytes != 0 {
		cfg.MaxRequestBytes = e.MasterOptions.MaxRequestBytes
	}

	// metrics monitoring
	if e.MasterOptions.MetricsURL != "" {
		cfg.Metrics = e.MasterOptions.Metrics //  "extensive" or "base"
		if cfg.ListenMetricsUrls, err = types.NewURLs(stringutil.WrapSchemes(e.MasterOptions.MetricsURL, false)); err != nil {
			return fmt.Errorf("metrics embed etcd config failed: [%v]", err)
		}
	}

	// attach extra gRPC and HTTP server
	if e.MasterOptions.GRPCSvr != nil {
		cfg.ServiceRegister = e.MasterOptions.GRPCSvr
	}
	if e.MasterOptions.HttpHandles != nil {
		cfg.UserHandlers = e.MasterOptions.HttpHandles
	}

	// reuse the previous addr as the client listening URL.
	if cfg.ListenClientUrls, err = types.NewURLs(stringutil.WrapSchemes(e.MasterOptions.ClientAddr, false)); err != nil {
		return fmt.Errorf("etcd types [%s] listen client urls failed: [%v]", e.MasterOptions.ClientAddr, err)
	}

	if cfg.AdvertiseClientUrls, err = types.NewURLs(stringutil.WrapSchemes(e.MasterOptions.ClientAddr, false)); err != nil {
		return fmt.Errorf("etcd types [%s] advertise client urls failed: [%v]", e.MasterOptions.ClientAddr, err)
	}

	if cfg.ListenPeerUrls, err = types.NewURLs(stringutil.WrapSchemes(e.MasterOptions.PeerAddr, false)); err != nil {
		return fmt.Errorf("etcd types [%s] listen peer urls failed: [%v]", e.MasterOptions.PeerAddr, err)
	}

	if cfg.AdvertisePeerUrls, err = types.NewURLs(stringutil.WrapSchemes(e.MasterOptions.PeerAddr, false)); err != nil {
		return fmt.Errorf("etcd types [%s] advertise peer urls failed: [%v]", e.MasterOptions.PeerAddr, err)
	}

	err = cfg.Validate() // verify & trigger the builder
	if err != nil {
		return fmt.Errorf("validate embed etcd config failed: [%v]", err)
	}

	e.Config = cfg

	return nil
}

// Run starts an embedded etcd server.
func (e *Etcd) Run() (err error) {
	e.Srv, err = embed.StartEtcd(e.Config)
	if err != nil {
		return fmt.Errorf("start etcd server failed: [%v]", err)
	}

	select {
	case <-e.Srv.Server.ReadyNotify():
		logger.Info("start etcd server success, the server is Ready!")
	case <-time.After(time.Duration(e.MasterOptions.StartTimeout) * time.Second):
		// if fail to startup, the etcd server may be still blocking in
		// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/serve.go#L92
		// then `e.Close` will block in
		// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/etcd.go#L377
		// because `close(sctx.serversC)` has not been called in
		// https://github.com/etcd-io/etcd/blob/3cf2f69b5738fb702ba1a935590f36b52b18979b/embed/serve.go#L200.
		// so for `ReadyNotify` timeout, we choose to only call `e.Server.Stop()` now,
		// and we should exit the dbms-master process after returned with error from this function.
		e.Srv.Server.Stop()

		return fmt.Errorf("start etcd server timeout %v", e.MasterOptions.StartTimeout)
	}
	return nil
}

// Join joins an embedded etcd server node
// prepareJoinEtcd prepares config needed to join an existing cluster.
// learn from https://github.com/pingcap/pd/blob/37efcb05f397f26c70cda8dd44acaa3061c92159/server/join/join.go#L44.
//
// when setting `initial-cluster` explicitly to bootstrap a new cluster:
// - if local persistent data exist, just restart the previous cluster (in fact, it's not bootstrapping).
// - if local persistent data not exist, just bootstrap the cluster as a new cluster.
//
// when setting `join` to join an existing cluster (without `initial-cluster` set):
// - if local persistent data exists (in fact, it's not join):
//   - just restart if `member` already exists (already joined before)
//   - read `initial-cluster` back from local persistent data to restart (just like bootstrapping)
//
// - if local persistent data not exist:
//  1. fetch member list from the cluster to check if we can join now.
//  2. call `member add` to add the member info into the cluster.
//  3. generate config for join (`initial-cluster` and `initial-cluster-state`).
//  4. save `initial-cluster` in local persistent data for later restarting.
//
// NOTE: A member can't join to another cluster after it has joined a previous one.
func (e *Etcd) Join() (err error) {
	// no need to join
	if e.MasterOptions.Join == "" {
		return nil
	}

	// try to join self, invalid
	if e.MasterOptions.Join == e.MasterOptions.ClientAddr {
		return fmt.Errorf("embed etcd join node [%v] is forbidden", e.MasterOptions.Join)
	}

	// restart with previous data, no `InitialCluster` need to set
	// ref: https://github.com/etcd-io/etcd/blob/ae9734ed278b7a1a7dfc82e800471ebbf9fce56f/etcdserver/server.go#L313
	if stringutil.IsDirExist(filepath.Join(e.MasterOptions.DataDir, "member", "wal")) {
		e.Config.InitialCluster = ""
		e.Config.ClusterState = embed.ClusterStateFlagExisting
		return nil
	}

	// join with persistent data
	joinFP := filepath.Join(e.MasterOptions.DataDir, "join")
	if s, err := os.ReadFile(joinFP); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("embed etcd read persistent join data failed: [%v]", err)
		}
	} else {
		e.Config.InitialCluster = strings.TrimSpace(stringutil.BytesToString(s))
		e.Config.ClusterState = embed.ClusterStateFlagExisting
		logger.Info("using persistent join data", zap.String("file", joinFP), zap.String("data", e.Config.InitialCluster))
		return nil
	}

	// if without previous data, we need a client to contact with the existing cluster.
	client, err := CreateClient(context.Background(), stringutil.WrapSchemes(e.MasterOptions.Join, false), nil)
	if err != nil {
		return fmt.Errorf("create etcd client for [%v] failed: [%v]", e.MasterOptions.Join, err)
	}
	defer client.Close()

	// `member list`
	listResp, err := ListMembers(client)
	if err != nil {
		return fmt.Errorf("etcd list member for [%v] failed: [%v]", e.MasterOptions.Join, err)
	}

	// check members
	for _, m := range listResp.Members {
		if m.Name == "" { // the previous existing member without name (not complete the join operation)
			// we can't generate `initial-cluster` correctly with empty member name,
			// and if added a member but not started it to complete the join,
			// the later join operation may encounter `etcdserver: re-configuration failed due to not enough started members`.
			return fmt.Errorf("there is a member that has not joined successfully, continue the join or remove it")
		}
		if m.Name == e.MasterOptions.Name {
			// a failed DM-master re-joins the previous cluster.
			return fmt.Errorf("missing data or joining a duplicate member [%s]", m.Name)
		}
	}

	// `member add`, a new/deleted dbms-master joins to an existing cluster.
	addResp, err := AddMember(client, stringutil.WrapSchemes(e.MasterOptions.PeerAddr, false))
	if err != nil {
		return fmt.Errorf("ectd add member [%s] failed: [%v]", e.MasterOptions.PeerAddr, err)
	}

	// generate `--initial-cluster`
	ms := make([]string, 0, len(addResp.Members))
	for _, m := range addResp.Members {
		name := m.Name
		if m.ID == addResp.Member.ID {
			// the member only called `member add`,
			// but has not started the process to complete the join should have an empty name.
			// so, we use the `name` in config instead.
			name = e.Config.Name
		}
		if name == "" {
			// this should be checked in the previous `member list` operation if having only one member is join.
			// if multi join operations exist, the behavior may be unexpected.
			// check again here only to decrease the unexpectedness.
			return fmt.Errorf("there is a member that has not joined successfully, continue the join or remove it")
		}
		for _, url := range m.PeerURLs {
			ms = append(ms, fmt.Sprintf("%s=%s", name, url))
		}
	}
	e.Config.InitialCluster = strings.Join(ms, ",")
	e.Config.ClusterState = embed.ClusterStateFlagExisting

	// save `--initial-cluster` in persist data
	if err = os.MkdirAll(e.Config.Dir, privateDirMode); err != nil && !os.IsExist(err) {
		return fmt.Errorf("etcd make directory [%v] failed: [%v]", e.Config.Dir, err)
	}
	if err = os.WriteFile(joinFP, []byte(e.Config.InitialCluster), privateDirMode); err != nil {
		return fmt.Errorf("etcd write persistent join data for [%v] failed: [%v]", e.Config.Dir, err)
	}

	return nil
}

// GetConfig used for return master finally config
func (e *Etcd) GetConfig() *configutil.MasterOptions {
	return e.MasterOptions
}

func (e *Etcd) Close() {
	e.Srv.Close()
}
