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
	"fmt"

	"gopkg.in/yaml.v2"
)

const (
	// DefaultDeployUser is the cluster deploy default user
	DefaultDeployUser = "dbms"
	// metadata
	// MetaFileName is the file name of the cluster topology file, eg. {clusterName}/
	MetaFileName = "meta.yaml"
	// CacheDirName is the directory to save cluster cache file .eg. {clusterName}/{CacheDirName}
	CacheDirName = "cache"
	// PatchDirName is the directory to store patch file, eg. {clusterName}/{PatchDirName}/dbms-hotfix.tar.gz
	PatchDirName = "patch"
	// SshDirName is the directory to store cluster ssh file, eg. {clusterName}/{SshDirName}
	SshDirName = "ssh"
	// AuditDirName is the directory to store cluster operation audit log, eg. {clusterName}/{AuditDirName}
	AuditDirName = "audit"
	// LockDirName is the directory to store cluster operation lock
	LockDirName      = "lock"
	ScaleOutLockName = "scale-out.yaml"

	// cluster
	// BinDirName is the directory to save bin files
	BinDirName = "bin"
	// BackupDirName is the directory to save backup files
	BackupDirName = "backup"
	// ScriptDirName is the directory to save script files
	ScriptDirName = "script"
	// ConfDirName is the directory to save script files
	ConfDirName = "conf"
)

type Metadata struct {
	User     string    `yaml:"user"`
	Version  string    `yaml:"version"`
	Topology *Topology `yaml:"topology"`
}

func (m *Metadata) SetUser(user string) {
	m.User = user
}

func (m *Metadata) GetUser() string {
	return m.User
}

func (m *Metadata) SetVersion(version string) {
	m.Version = version
}

func (m *Metadata) GetVersion() string {
	return m.Version
}

func (m *Metadata) GetTopology() *Topology {
	return m.Topology
}

func (m *Metadata) SetTopology(topo *Topology) {
	m.Topology = topo
}

func (m *Metadata) GenMetadata() *Metadata {
	return m
}

func (m *Metadata) ParseMetadata(file string) (*Metadata, error) {
	yamlFile, err := ReadYamlFile(file)
	if err != nil {
		return nil, err
	}

	if err = yaml.UnmarshalStrict(yamlFile, m); err != nil {
		return nil, fmt.Errorf("please check the syntax of your topology file and try again, parse topology yaml file [%s] failed: %v, ", file, err)
	}
	return m, nil
}

func (m *Metadata) ScaleOutTopology(topo *Topology) {
	m.Topology = &Topology{
		GlobalOptions: m.Topology.GlobalOptions,
		ServerConfigs: m.Topology.ServerConfigs,
		MasterServers: append(m.Topology.MasterServers, topo.MasterServers...),
		WorkerServers: append(m.Topology.WorkerServers, topo.WorkerServers...),
	}
}
