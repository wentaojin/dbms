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
package operator

import (
	"context"
	"crypto/tls"

	"github.com/wentaojin/dbms/logger/printer"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/utils/cluster"
)

// Upgrade the cluster. (actually, it's rolling restart)
func Upgrade(
	ctx context.Context,
	topo *cluster.Topology,
	options *Options,
	tlsCfg *tls.Config,
) error {
	roleFilter := stringutil.NewStringSet(options.Roles...)
	nodeFilter := stringutil.NewStringSet(options.Nodes...)
	components := topo.ComponentsByUpdateOrder()
	components = FilterComponent(components, roleFilter)
	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)

	for _, component := range components {
		instances := FilterInstance(component.Instances(), nodeFilter)
		if len(instances) < 1 {
			continue
		}
		logger.Infof("Upgrading component %s", component.ComponentName())

		// some instances are upgraded after others
		deferInstances := make([]cluster.Instance, 0)

		for _, instance := range instances {
			if err := upgradeInstance(ctx, topo, instance, options, tlsCfg); err != nil {
				return err
			}
		}

		// process deferred instances
		for _, instance := range deferInstances {
			logger.Debugf("Upgrading deferred instance %s...", instance.InstanceName())
			if err := upgradeInstance(ctx, topo, instance, options, tlsCfg); err != nil {
				return err
			}
		}
	}
	return nil
}

func upgradeInstance(
	ctx context.Context,
	topo *cluster.Topology,
	instance cluster.Instance,
	options *Options,
	tlsCfg *tls.Config,
) (err error) {
	err = executeSSHCommand(ctx, "Executing pre-upgrade command", instance.InstanceManageHost(), options.SSHCustomScripts.BeforeRestartInstance.Command())
	if err != nil {
		return err
	}

	systemdMode := string(topo.GlobalOptions.SystemdMode)
	if err := restartInstance(ctx, instance, options.OptTimeout, tlsCfg, systemdMode); err != nil && !options.Force {
		return err
	}

	err = executeSSHCommand(ctx, "Executing post-upgrade command", instance.InstanceManageHost(), options.SSHCustomScripts.AfterRestartInstance.Command())
	if err != nil {
		return err
	}

	return nil
}
