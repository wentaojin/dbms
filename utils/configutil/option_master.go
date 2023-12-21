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
package configutil

import (
	"fmt"
	"net/http"
	"strings"

	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
)

const (
	DefaultMasterNamePrefix              = "master"
	DefaultMasterDataDirPrefix           = "data"
	DefaultMasterClientAddr              = "127.0.0.1:2379"
	DefaultMasterPeerAddr                = "127.0.0.1:2380"
	DefaultMasterInitialClusterState     = embed.ClusterStateFlagNew
	DefaultMasterAutoCompactionMode      = "periodic"
	DefaultMasterAutoCompactionRetention = "1h"
	DefaultMasterMaxTxnOps               = 2048
	DefaultMasterQuotaBackendBytes       = 2 * 1024 * 1024 * 1024 // 2GB
	DefaultMasterMaxRequestBytes         = 1.5 * 1024 * 1024
	DefaultMasterStartTimeoutSecond      = 60
	DefaultMasterLogLevel                = "info"
	// DefaultMasterKeepaliveTTL represented service lease keep alive ttl (seconds)
	DefaultMasterKeepaliveTTL = 5
)

// MasterOptions etcd server relative config items
type MasterOptions struct {
	Name                string `toml:"name" json:"name"`
	DataDir             string `toml:"data-dir" json:"data-dir"`
	ClientAddr          string `toml:"client-addr" json:"client-addr"`
	PeerAddr            string `toml:"peer-addr" json:"peer-addr"`
	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`

	Join string `toml:"join" json:"join"` // cluster's client address (endpoints), not peer address

	MaxTxnOps               uint   `toml:"max-txn-ops" json:"max-txn-ops"`
	MaxRequestBytes         uint   `toml:"max-request-bytes" json:"max-request-bytes"`
	AutoCompactionMode      string `toml:"auto-compaction-mode" json:"auto-compaction-mode"`
	AutoCompactionRetention string `toml:"auto-compaction-retention" json:"auto-compaction-retention"`
	QuotaBackendBytes       int64  `toml:"quota-backend-bytes" json:"quota-backend-bytes"`

	Metrics    string `toml:"metrics" json:"metrics"`
	MetricsURL string `toml:"metrics-url" json:"metrics-url"`

	KeepaliveTTL int64 `toml:"keepalive-ttl" json:"keepalive-ttl"`

	// time waiting for etcd to be started.
	StartTimeout int `toml:"start-timeout" json:"start-timeout"`

	LogLevel string `toml:"log-level" json:"log-level"`

	GRPCSvr     func(*grpc.Server)      `json:"-"`
	HttpHandles map[string]http.Handler `json:"-"`
	Logger      *zap.Logger             `json:"-"`
}

type MasterOption func(opts *MasterOptions)

func DefaultMasterServerConfig() *MasterOptions {
	return &MasterOptions{
		Name:                    DefaultMasterNamePrefix,
		DataDir:                 DefaultMasterDataDirPrefix,
		ClientAddr:              DefaultMasterClientAddr,
		PeerAddr:                DefaultMasterPeerAddr,
		InitialCluster:          fmt.Sprintf("%s=http://%s", DefaultMasterNamePrefix, DefaultMasterPeerAddr),
		InitialClusterState:     DefaultMasterInitialClusterState,
		MaxTxnOps:               DefaultMasterMaxTxnOps,
		MaxRequestBytes:         DefaultMasterMaxRequestBytes,
		AutoCompactionMode:      DefaultMasterAutoCompactionMode,
		AutoCompactionRetention: DefaultMasterAutoCompactionRetention,
		QuotaBackendBytes:       DefaultMasterQuotaBackendBytes,
		StartTimeout:            DefaultMasterStartTimeoutSecond,
		LogLevel:                DefaultMasterLogLevel,
		KeepaliveTTL:            DefaultMasterKeepaliveTTL,
	}
}

func WithMasterName(name string) MasterOption {
	return func(opts *MasterOptions) {
		opts.Name = name
	}
}

func WithMasterDir(dir string) MasterOption {
	return func(opts *MasterOptions) {
		opts.DataDir = dir
	}
}

func WithClientAddr(addr string) MasterOption {
	return func(opts *MasterOptions) {
		opts.ClientAddr = addr
	}
}

func WithPeerAddr(addr string) MasterOption {
	return func(opts *MasterOptions) {
		opts.PeerAddr = addr
	}
}

func WithCluster(cluster string) MasterOption {
	return func(opts *MasterOptions) {
		opts.InitialCluster = cluster
	}
}

func WithClusterState(state string) MasterOption {
	return func(opts *MasterOptions) {
		// "new" or "existing"
		if strings.HasPrefix(state, "exist") {
			opts.InitialClusterState = embed.ClusterStateFlagExisting
		} else {
			opts.InitialClusterState = embed.ClusterStateFlagNew
		}
	}
}

func WithMetrics(addr string, mode string) MasterOption {
	return func(opts *MasterOptions) {
		switch {
		case strings.HasPrefix(mode, "b"):
			opts.Metrics = "base"
		case strings.HasPrefix(mode, "e"):
			opts.Metrics = "extensive"
		default:
			opts.Metrics = "base"
		}
		if addr != "" && !strings.HasPrefix(addr, "http://") {
			opts.MetricsURL = "http://" + addr
			return
		}
		opts.Metrics = addr
	}
}

func WithMasterJoin(addr string) MasterOption {
	return func(opts *MasterOptions) {
		opts.Join = addr
	}
}

func WithMaxTxnOps(maxTxnOps uint) MasterOption {
	return func(opts *MasterOptions) {
		opts.MaxTxnOps = maxTxnOps
	}
}

func WithMaxRequestBytes(maxRequestBytes uint) MasterOption {
	return func(opts *MasterOptions) {
		opts.MaxRequestBytes = maxRequestBytes
	}
}

func WithAutoCompactionMode(autoCompactionMode string) MasterOption {
	return func(opts *MasterOptions) {
		opts.AutoCompactionMode = autoCompactionMode
	}
}

func WithAutoCompactionRetention(autoCompactionRetention string) MasterOption {
	return func(opts *MasterOptions) {
		opts.AutoCompactionRetention = autoCompactionRetention
	}
}

func WithQuotaBackendBytes(quotaBackendBytes int64) MasterOption {
	return func(opts *MasterOptions) {
		opts.QuotaBackendBytes = quotaBackendBytes
	}
}

func WithStartTimeout(timeoutSecond int) MasterOption {
	return func(opts *MasterOptions) {
		opts.StartTimeout = timeoutSecond
	}
}

func WithLogLevel(logLevel string) MasterOption {
	return func(opts *MasterOptions) {
		opts.LogLevel = logLevel
	}
}

func WithLogLogger(logger *zap.Logger) MasterOption {
	return func(opts *MasterOptions) {
		opts.Logger = logger
	}
}

func WithGRPCSvr(gRPCSvr func(*grpc.Server)) MasterOption {
	return func(opts *MasterOptions) {
		opts.GRPCSvr = gRPCSvr
	}
}

func WithHttpHandles(httpHandles map[string]http.Handler) MasterOption {
	return func(opts *MasterOptions) {
		opts.HttpHandles = httpHandles
	}
}

func WithMasterLease(aliveTTL int64) MasterOption {
	return func(opts *MasterOptions) {
		opts.KeepaliveTTL = aliveTTL
	}
}
