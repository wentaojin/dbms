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
package manager

import (
	"fmt"
	"strings"

	"github.com/wentaojin/dbms/utils/cluster"

	"github.com/wentaojin/dbms/utils/stringutil"

	"go.uber.org/zap"
)

// DirAccessor stands for a directory accessor for an instance
type DirAccessor struct {
	dirKind  string
	accessor func(cluster.Instance, *cluster.Topology) string
}

func dirAccessors() []DirAccessor {
	instanceDirAccessor := []DirAccessor{
		{dirKind: "deploy directory", accessor: func(instance cluster.Instance, topo *cluster.Topology) string { return instance.InstanceDeployDir() }},
		{dirKind: "data directory", accessor: func(instance cluster.Instance, topo *cluster.Topology) string { return instance.InstanceDataDir() }},
		{dirKind: "log directory", accessor: func(instance cluster.Instance, topo *cluster.Topology) string { return instance.InstanceLogDir() }},
	}
	return instanceDirAccessor
}

func fixDir(topo *cluster.Topology) func(string) string {
	return func(dir string) string {
		if dir != "" {
			return stringutil.Abs(topo.GlobalOptions.User, dir)
		}
		return dir
	}
}

// DirEntry stands for a directory with attributes and instance
type DirEntry struct {
	clusterName string
	dirKind     string
	dir         string
	instance    cluster.Instance
}

func appendEntries(name string, topo *cluster.Topology, inst cluster.Instance, dirAccessor DirAccessor, targets []DirEntry) []DirEntry {
	for _, dir := range strings.Split(fixDir(topo)(dirAccessor.accessor(inst, topo)), ",") {
		targets = append(targets, DirEntry{
			clusterName: name,
			dirKind:     dirAccessor.dirKind,
			dir:         dir,
			instance:    inst,
		})
	}

	return targets
}

// CheckClusterDirOverlap checks cluster dir overlaps with data or log.
// this should only be used across clusters.
// we don't allow to deploy log under data, and vise versa.
// ref https://github.com/pingcap/tiup/issues/1047#issuecomment-761711508
func CheckClusterDirOverlap(entries []DirEntry) error {
	ignore := func(d1, d2 DirEntry) bool {
		return d1.dir == "" || d2.dir == "" ||
			strings.HasSuffix(d1.dirKind, "deploy directory") ||
			strings.HasSuffix(d2.dirKind, "deploy directory")
	}
	for i := 0; i < len(entries)-1; i++ {
		d1 := entries[i]
		for j := i + 1; j < len(entries); j++ {
			d2 := entries[j]
			if ignore(d1, d2) {
				continue
			}

			if stringutil.IsSubDir(d1.dir, d2.dir) || stringutil.IsSubDir(d2.dir, d1.dir) {
				properties := map[string]string{
					"ThisDirKind":   d1.dirKind,
					"ThisDir":       d1.dir,
					"ThisComponent": d1.instance.ComponentName(),
					"ThisHost":      d1.instance.InstanceHost(),
					"ThatDirKind":   d2.dirKind,
					"ThatDir":       d2.dir,
					"ThatComponent": d2.instance.ComponentName(),
					"ThatHost":      d2.instance.InstanceHost(),
				}
				zap.L().Info("Meet deploy directory overlap", zap.Any("info", properties))
				return fmt.Errorf(`deploy directory overlaps to another instance
The directory you specified in the topology file is:
  Directory: %v %v
  Component: %v %v

It overlaps to another instance:
  Other Directory: %v %v
  Other Component: %v %v

Please modify the topology file and try again.`, properties["ThisDirKind"], properties["ThisDir"], properties["ThisComponent"], properties["ThisHost"], properties["ThatDirKind"], properties["ThatDir"], properties["ThatComponent"], properties["ThatHost"])
			}
		}
	}

	return nil
}

// IterHost iterates one instance for each host
func IterHost(topo *cluster.Topology, fn func(inst cluster.Instance)) {
	hostMap := make(map[string]bool)
	for _, comp := range topo.ComponentsByStartOrder() {
		for _, inst := range comp.Instances() {
			host := inst.InstanceHost()
			_, ok := hostMap[host]
			if !ok {
				hostMap[host] = true
				fn(inst)
			}
		}
	}
}
