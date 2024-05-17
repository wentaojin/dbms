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
	"os"
	"path/filepath"
	"strings"

	"github.com/wentaojin/dbms/utils/ctxt"

	"github.com/wentaojin/dbms/utils/executor"

	"github.com/wentaojin/dbms/utils/cluster/task"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/cluster/manager"
	"github.com/wentaojin/dbms/utils/cluster/operator"
	"github.com/wentaojin/dbms/utils/stringutil"
)

type AppUpgrade struct {
	*App
}

func (a *App) AppUpgrade() component.Cmder {
	return &AppUpgrade{App: a}
}

func (a *AppUpgrade) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:              "upgrade <cluster-name> <version>",
		Short:            "Upgrade a specified DBMS cluster",
		RunE:             a.RunE,
		TraverseChildren: true,
		SilenceUsage:     true,
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			switch len(args) {
			case 0:
				return shellCompGetClusterName(a.MetaDir, toComplete)
			default:
				return nil, cobra.ShellCompDirectiveNoFileComp
			}
		},
	}
	return c
}

func (a *AppUpgrade) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return cmd.Help()
	}
	clusterName := args[0]
	clusterUpgradeVersion := args[1]
	if strings.EqualFold(gOpt.MirrorDir, "") {
		return fmt.Errorf("the flag parameters cannot be null, please configure --mirror-dir")
	}
	return a.Upgrade(clusterName, clusterUpgradeVersion, gOpt)
}

func (a *AppUpgrade) Upgrade(clusterName string, clusterUpgradeVersion string, gOpt *operator.Options) error {
	if err := cluster.ValidateClusterNameOrError(clusterName); err != nil {
		return err
	}

	mg := manager.New(a.MetaDir, logger)
	// check locked
	if err := mg.ScaleOutLockedErr(clusterName); err != nil {
		return err
	}

	metadata := mg.NewMetadata()
	topo := metadata.GetTopology()

	var (
		copyCompTasks []task.Task // tasks which are used to copy components to remote host
	)

	if err := stringutil.VersionCompare(metadata.GetVersion(), clusterUpgradeVersion); err != nil {
		mg.Logger.Warnf(color.RedString("There is no guarantee that the cluster can be downgraded. Be careful before you continue."))
	}

	compVersionMsg := ""
	var restartComponents []string
	components := topo.ComponentsByUpdateOrder()

	for _, comp := range components {
		restartComponents = append(restartComponents, comp.ComponentName(), comp.ComponentRole())
		if len(comp.Instances()) > 0 {
			compVersionMsg += fmt.Sprintf("\nwill upgrade and restart component \"%19s\" to \"%s\",", comp.ComponentName(), clusterUpgradeVersion)
		}
	}

	components = operator.FilterComponent(components, stringutil.NewStringSet(restartComponents...))

	mg.Logger.Warnf(`DBMS
This operation will upgrade %s %s cluster %s to %s:%s`,
		color.YellowString("Before the upgrade, it is recommended to read the upgrade guide and finish the preparation steps."),
		color.HiYellowString(metadata.GetVersion()),
		color.HiYellowString(clusterName),
		color.HiYellowString(clusterUpgradeVersion),
		compVersionMsg)
	if !gOpt.SkipConfirm {
		if err := stringutil.PromptForConfirmOrAbortError(`Do you want to continue? [y/N]:`); err != nil {
			return err
		}
		mg.Logger.Infof("Upgrading cluster...")
	}

	deployUser := metadata.GetUser()
	currentVersion := metadata.GetVersion()
	for _, comp := range components {
		compName := comp.ComponentName()

		for _, inst := range comp.Instances() {
			deployDir := stringutil.Abs(deployUser, inst.InstanceDeployDir())

			// Deploy component
			tb := task.NewBuilder(mg.Logger).
				BackupComponent(compName, currentVersion, inst.InstanceManageHost(), deployDir).
				CopyComponent(
					compName,
					inst.OS(),
					inst.Arch(),
					clusterUpgradeVersion,
					gOpt.MirrorDir,
					inst.InstanceManageHost(),
					filepath.Join(deployDir, cluster.BinDirName),
				).InitConfig(
				clusterName,
				inst,
				deployUser,
				mg.Path(clusterName, cluster.CacheDirName),
			).Build()
			copyCompTasks = append(copyCompTasks, tb)
		}
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

	ctx := ctxt.New(
		context.Background(),
		gOpt.Concurrency,
		mg.Logger,
	)

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
		).Parallel(gOpt.Force, copyCompTasks...).
		Func("UpgradeCluster", func(ctx context.Context) error {
			nopt := gOpt
			nopt.Roles = restartComponents
			return operator.Upgrade(ctx, topo, nopt, nil)
		}).Build()

	if err := b.Execute(ctx); err != nil {
		return err
	}

	// clear patched packages and tags
	if err := os.RemoveAll(mg.Path(clusterName, cluster.PatchDirName)); err != nil {
		return err
	}
	topo.IterInstance(func(ins cluster.Instance) {
		if ins.IsPatched() {
			ins.SetPatched(false)
		}
	})

	metadata.SetVersion(clusterUpgradeVersion)

	if err := mg.SaveMetadata(clusterName, metadata.GenMetadata()); err != nil {
		return err
	}

	mg.Logger.Infof("Upgraded cluster `%s` successfully", clusterName)
	return nil

}
