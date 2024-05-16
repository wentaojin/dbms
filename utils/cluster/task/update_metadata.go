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
package task

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/wentaojin/dbms/utils/stringutil"
	"gopkg.in/yaml.v2"

	"github.com/wentaojin/dbms/utils/cluster"
)

// UpdateMetadata is used to maintain the DBMS meta information
type UpdateMetadata struct {
	cluster        string
	basePath       string
	metadata       cluster.IMetadata
	deletedNodesID []string
}

// NewUpdateMetadata create i update dbms meta task.
func NewUpdateMetadata(cluster string, basePath string, metadata cluster.IMetadata, deletedNodesID []string) *UpdateMetadata {
	return &UpdateMetadata{
		cluster:        cluster,
		basePath:       basePath,
		metadata:       metadata,
		deletedNodesID: deletedNodesID,
	}
}

// Execute implements the Task interface
// the metadata especially the topology is in wide use,
// the other callers point to this field by a pointer,
// so we should update the original topology directly, and don't make a copy
func (u *UpdateMetadata) Execute(ctx context.Context) error {
	deleted := stringutil.NewStringSet(u.deletedNodesID...)
	topo := u.metadata.GetTopology()
	masters := make([]*cluster.MasterOptions, 0)
	for i, instance := range (&cluster.DBMSMasterComponent{Topology: topo}).Instances() {
		if deleted.Exist(instance.InstanceName()) {
			continue
		}
		masters = append(masters, topo.MasterServers[i])
	}
	topo.MasterServers = masters

	workers := make([]*cluster.WorkerOptions, 0)
	for i, instance := range (&cluster.DBMSWorkerComponent{Topology: topo}).Instances() {
		if deleted.Exist(instance.InstanceName()) {
			continue
		}
		workers = append(workers, topo.WorkerServers[i])
	}
	topo.WorkerServers = workers

	u.metadata.SetTopology(topo)

	data, err := yaml.Marshal(u.metadata.GenMetadata())
	if err != nil {
		return err
	}

	err = stringutil.SaveFileWithBackup(u.Path(cluster.MetaFileName), data, u.Path(cluster.BackupDirName))
	if err != nil {
		return err
	}
	return nil
}

// Rollback implements the Task interface
func (u *UpdateMetadata) Rollback(ctx context.Context) error {
	data, err := yaml.Marshal(u.metadata.GenMetadata())
	if err != nil {
		return err
	}
	err = stringutil.SaveFileWithBackup(u.Path(cluster.MetaFileName), data, u.Path(cluster.BackupDirName))
	if err != nil {
		return err
	}
	return nil
}

// String implements the fmt.Stringer interface
func (u *UpdateMetadata) String() string {
	return fmt.Sprintf("UpdateMetadata: cluster=%s, deleted=`'%s'`", u.cluster, strings.Join(u.deletedNodesID, "','"))
}

func (u *UpdateMetadata) Path(subpath ...string) string {
	return filepath.Join(append([]string{
		u.basePath,
		u.cluster,
	}, subpath...)...)
}
