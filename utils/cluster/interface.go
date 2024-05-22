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
package cluster

import (
	"context"
	"crypto/tls"

	"github.com/wentaojin/dbms/utils/executor"
)

type IMetadata interface {
	SetUser(user string)
	SetVersion(version string)
	SetTopology(topo *Topology)
	GetTopology() *Topology
	GetUser() string
	GetVersion() string
	GenMetadata() *Metadata
	ScaleOutTopology(topo *Topology)
}

// Component represents a component of the cluster.
type Component interface {
	ComponentName() string
	ComponentRole() string
	Instances() []Instance
}

// Instance represents the instance.
type Instance interface {
	InstanceName() string
	InstanceManageHost() string
	InstanceHost() string
	InstancePort() int
	InstancePeerPort() int
	InstanceSshPort() int
	InstanceDeployDir() string
	InstanceDataDir() string
	InstanceLogDir() string
	InstanceListenHost() string
	InstanceNumaNode() string
	OS() string
	Arch() string
	GetInstanceConfig() map[string]any
	UsedPort() []int
	UsedDir() []string
	ComponentName() string
	ComponentRole() string
	ServiceName() string
	ServiceReady(ctx context.Context, e executor.Executor, timeout uint64) error
	InstanceInitConfig(ctx context.Context, e executor.Executor, deployUser string, cacheDir string) error
	InstanceScaleConfig(ctx context.Context, e executor.Executor, topo *Topology, deployUser, cacheDir string) error
	Status(ctx context.Context, tlsCfg *tls.Config, addrs ...string) (string, error)
	IsPatched() bool
	SetPatched(patch bool)
}

// InstanceTopology represents a instance topology, used to reflect get component instance topology and update instance topology
type InstanceTopology interface {
	InstanceRole() string
	InstanceName() string
}
