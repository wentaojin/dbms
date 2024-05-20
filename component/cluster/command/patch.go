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
package command

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/wentaojin/dbms/utils/executor"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/fatih/color"
	"github.com/wentaojin/dbms/utils/cluster/task"
	"github.com/wentaojin/dbms/utils/ctxt"

	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/cluster/manager"

	"github.com/wentaojin/dbms/utils/cluster/operator"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
)

type AppPatch struct {
	*App
	Overwrite bool
}

func (a *App) AppPatch() component.Cmder {
	return &AppPatch{App: a}
}

func (a *AppPatch) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "patch <cluster-name> <package-path>",
		Short: "Replace the remote package with a specified package and restart the service",
		RunE:  a.RunE,
	}
	c.Flags().BoolVar(&a.Overwrite, "overwrite", false, "Use this package in the future scale-out operations")
	c.Flags().StringSliceVarP(&gOpt.Nodes, "node", "N", nil, "Specify the nodes")
	c.Flags().StringSliceVarP(&gOpt.Roles, "role", "R", nil, "Specify the roles")
	return c
}

func (a *AppPatch) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return cmd.Help()
	}

	if len(gOpt.Nodes) == 0 && len(gOpt.Roles) == 0 {
		return fmt.Errorf("the flag -R or -N must be specified at least one")
	}

	clusterName := args[0]
	packagePath := args[1]
	return a.Patch(clusterName, packagePath, gOpt)
}

func (a *AppPatch) Patch(clusterName, packagePath string, gOpt *operator.Options) error {
	mg := manager.New(gOpt.MetaDir, logger)
	metadata, err := cluster.ParseMetadataYaml(mg.GetMetaFilePath(clusterName))
	if err != nil {
		return err
	}
	topo := metadata.GetTopology()

	// valid roles
	var compRoles []string
	topo.IterComponent(func(c cluster.Component) {
		compRoles = append(compRoles, c.ComponentRole())
	})

	for _, r := range gOpt.Roles {
		match := false
		for _, has := range compRoles {
			if r == has {
				match = true
				break
			}
		}
		if !match {
			return fmt.Errorf("not valid role: %s, should be one of: %v", r, compRoles)
		}
	}

	if err := cluster.ValidateClusterNameOrError(clusterName); err != nil {
		return err
	}

	// check locked
	if err = mg.ScaleOutLockedErr(clusterName); err != nil {
		return err
	}

	if !stringutil.IsPathExist(packagePath) {
		return fmt.Errorf("specified package(%s) not exists", packagePath)
	}

	if !a.SkipConfirm {
		if err := stringutil.PromptForConfirmOrAbortError(
			fmt.Sprintf("Will patch the cluster %s with package path is %s, nodes: %s, roles: %s.\nDo you want to continue? [y/N]:",
				color.HiYellowString(clusterName),
				color.HiYellowString(packagePath),
				color.HiRedString(strings.Join(gOpt.Nodes, ",")),
				color.HiRedString(strings.Join(gOpt.Roles, ",")),
			),
		); err != nil {
			return err
		}
	}

	insts, err := instancesToPatch(topo, gOpt)
	if err != nil {
		return err
	}

	var replacePackageTasks []task.Task

	deployUser := metadata.GetUser()
	currentVersion := metadata.GetVersion()
	for _, inst := range insts {
		deployDir := stringutil.Abs(deployUser, inst.InstanceDeployDir())
		tb := task.NewBuilder(mg.Logger)
		tb.BackupComponent(inst.ComponentName(), currentVersion, inst.InstanceManageHost(), deployDir).
			InstallPackage(packagePath, inst.InstanceManageHost(), filepath.Join(deployDir, cluster.BinDirName))
		replacePackageTasks = append(replacePackageTasks, tb.Build())
	}

	var p *operator.SSHConnectionProps = &operator.SSHConnectionProps{}
	if gOpt.SSHType != executor.SSHTypeNone {
		var err error
		if len(gOpt.SSHProxyHost) != 0 {
			if p, err = operator.ReadIdentityFileOrPassword(gOpt.SSHProxyIdentity, gOpt.SSHProxyUsePassword); err != nil {
				return err
			}
		}
	}

	b := task.NewBuilder(mg.Logger).
		SSHKeySet(mg.Path(clusterName, cluster.SshDirName, "id_rsa"), mg.Path(clusterName, cluster.SshDirName, "id_rsa.pub")).
		ClusterSSH(
			topo,
			deployUser,
			gOpt.SSHTimeout,
			gOpt.OptTimeout,
			gOpt.SSHProxyHost,
			gOpt.SSHProxyPort,
			gOpt.SSHProxyUser,
			p.Password,
			p.IdentityFile,
			p.IdentityFilePassphrase,
			gOpt.SSHProxyTimeout,
			gOpt.SSHType,
			executor.SSHType(topo.GlobalOptions.SSHType),
		).Parallel(false, replacePackageTasks...).
		Func("UpgradeCluster", func(ctx context.Context) error {
			return operator.Upgrade(ctx, topo, gOpt, nil)
		}).
		Build()

	ctx := ctxt.New(
		context.Background(),
		gOpt.Concurrency,
		mg.Logger,
	)
	if err = b.Execute(ctx); err != nil {
		return err
	}

	if a.Overwrite {
		if err := overwritePatch(mg, clusterName, insts[0].ComponentName(), packagePath); err != nil {
			return err
		}
	}

	// mark instance as patched in meta
	topo.IterInstance(func(ins cluster.Instance) {
		for _, pachedIns := range insts {
			if ins.InstanceName() == pachedIns.InstanceName() {
				ins.SetPatched(true)
				break
			}
		}
	})

	err = mg.SaveMetadata(clusterName, metadata.GenMetadata())
	if err != nil {
		return err
	}

	mg.Logger.Infof("Patched cluster `%s` successfully", clusterName)
	return nil
}

func overwritePatch(mg *manager.Controller, name, comp, packagePath string) error {
	if err := stringutil.MkdirAll(mg.Path(name, cluster.PatchDirName), 0755); err != nil {
		return err
	}

	if err := stringutil.Copy(packagePath, mg.Path(name, cluster.PatchDirName, comp+".tar.gz")); err != nil {
		return err
	}
	return nil
}

func instancesToPatch(topo *cluster.Topology, options *operator.Options) ([]cluster.Instance, error) {
	roleFilter := stringutil.NewStringSet(options.Roles...)
	nodeFilter := stringutil.NewStringSet(options.Nodes...)
	components := topo.ComponentsByStartOrder()
	components = operator.FilterComponent(components, roleFilter)

	var instances []cluster.Instance
	var comps []string
	for _, com := range components {
		insts := operator.FilterInstance(com.Instances(), nodeFilter)
		if len(insts) > 0 {
			comps = append(comps, com.ComponentName())
		}
		instances = append(instances, insts...)
	}
	if len(comps) > 1 {
		return nil, fmt.Errorf("can't patch more than one component at once: %v", comps)
	}

	if len(instances) == 0 {
		return nil, fmt.Errorf("no instance found on specifid role(%v) and nodes(%v)", options.Roles, options.Nodes)
	}

	return instances, nil
}
