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

	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/ctxt"
)

// ScaleConfig is used to copy all configurations to the target directory of path
type ScaleConfig struct {
	clusterName string
	instance    cluster.Instance
	topology    *cluster.Topology
	deployUser  string
	cacheDir    string
}

// Execute implements the Task interface
func (c *ScaleConfig) Execute(ctx context.Context) error {
	// Copy to remote server
	exec, found := ctxt.GetInner(ctx).GetExecutor(c.instance.InstanceHost())
	if !found {
		return ErrNoExecutor
	}

	return c.instance.InstanceScaleConfig(ctx, exec, c.topology, c.deployUser, c.cacheDir)
}

// Rollback implements the Task interface
func (c *ScaleConfig) Rollback(ctx context.Context) error {
	return ErrUnsupportedRollback
}

// String implements the fmt.Stringer interface
func (c *ScaleConfig) String() string {
	return fmt.Sprintf("ScaleConfig: cluster=%s, user=%s, host=%s, service=%s, %s",
		c.clusterName, c.deployUser, c.instance.InstanceManageHost(), c.instance.ServiceName(), c.cacheDir)
}
