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
	"fmt"
	"path/filepath"
	"strings"

	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/wentaojin/dbms/logger/printer"

	"github.com/fatih/color"
	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/ctxt"
	"github.com/wentaojin/dbms/utils/module"
)

// DestroyComponent destroy the instances.
func DestroyComponent(ctx context.Context, instances []cluster.Instance, cls *cluster.Topology, options *Options) error {
	if len(instances) == 0 {
		return nil
	}

	logger := ctx.Value(printer.ContextKeyLogger).(*printer.Logger)
	name := instances[0].ComponentName()
	logger.Infof("Destroying component %s", name)

	retainDataRoles := stringutil.NewStringSet(options.RetainDataRoles...)
	retainDataNodes := stringutil.NewStringSet(options.RetainDataNodes...)

	for _, ins := range instances {
		// Some data of instances will be retained
		dataRetained := retainDataRoles.Exist(ins.ComponentName()) ||
			retainDataNodes.Exist(ins.InstanceName()) || retainDataNodes.Exist(ins.InstanceHost()) || retainDataRoles.Exist(ins.InstanceManageHost())

		e := ctxt.GetInner(ctx).Get(ins.InstanceManageHost())
		logger.Infof("\tDestroying instance %s", ins.InstanceManageHost())

		var dataDirs []string
		if len(ins.InstanceDataDir()) > 0 {
			dataDirs = strings.Split(ins.InstanceDataDir(), ",")
		}

		deployDir := ins.InstanceDeployDir()
		delPaths := stringutil.NewStringSet()

		// Retain the deploy directory if the users want to retain the data directory
		// and the data directory is a sub-directory of deploy directory
		keepDeployDir := false

		for _, dataDir := range dataDirs {
			// Don't delete the parent directory if any sub-directory retained
			keepDeployDir = (dataRetained && strings.HasPrefix(dataDir, deployDir)) || keepDeployDir
			if !dataRetained && cls.CountDir(ins.InstanceManageHost(), dataDir) == 1 {
				// only delete path if it is not used by any other instance in the cluster
				delPaths.Insert(dataDir)
			}
		}

		logDir := ins.InstanceLogDir()

		if keepDeployDir {
			delPaths.Insert(filepath.Join(deployDir, "conf"))
			delPaths.Insert(filepath.Join(deployDir, "bin"))
			delPaths.Insert(filepath.Join(deployDir, "scripts"))
			// only delete path if it is not used by any other instance in the cluster
			if strings.HasPrefix(logDir, deployDir) && cls.CountDir(ins.InstanceManageHost(), logDir) == 1 {
				delPaths.Insert(logDir)
			}
		} else {
			// only delete path if it is not used by any other instance in the cluster
			if cls.CountDir(ins.InstanceManageHost(), logDir) == 1 {
				delPaths.Insert(logDir)
			}
			if cls.CountDir(ins.InstanceManageHost(), ins.InstanceDeployDir()) == 1 {
				delPaths.Insert(ins.InstanceDeployDir())
			}
		}

		// check for deploy dir again, to avoid unused files being left on disk
		dpCnt := 0
		for _, dir := range delPaths.Slice() {
			if strings.HasPrefix(dir, deployDir+"/") { // only check subdir of deploy dir
				dpCnt++
			}
		}
		if cls.CountDir(ins.InstanceManageHost(), deployDir)-dpCnt == 1 {
			delPaths.Insert(deployDir)
		}

		systemdDir := "/etc/systemd/system/"
		sudo := true
		if cls.GlobalOptions.SystemdMode == cluster.UserMode {
			systemdDir = "~/.config/systemd/user/"
			sudo = false
		}

		if svc := ins.ServiceName(); svc != "" {
			delPaths.Insert(fmt.Sprintf("%s%s", systemdDir, svc))
		}
		logger.Debugf("Deleting paths on %s: %s", ins.InstanceManageHost(), strings.Join(delPaths.Slice(), " "))
		for _, delPath := range delPaths.Slice() {
			c := module.ShellModuleConfig{
				Command:  fmt.Sprintf("rm -rf %s;", delPath),
				Sudo:     sudo, // the .service files are in a directory owned by root
				Chdir:    "",
				UseShell: false,
			}
			shell := module.NewShellModule(c)
			_, _, err := shell.Execute(ctx, e)

			if err != nil {
				// Ignore error and continue.For example, deleting a mount point will result in a "Device or resource busy" error.
				logger.Warnf(color.YellowString("Warn: failed to delete path \"%s\" on %s.Please check this error message and manually delete if necessary.\nerrmsg: %s", delPath, ins.InstanceManageHost(), err))
			}
		}

		logger.Infof("Destroy %s finished", ins.InstanceManageHost())
		logger.Infof("- Destroy %s paths: %v", ins.ComponentName(), delPaths.Slice())
	}
	return nil
}
