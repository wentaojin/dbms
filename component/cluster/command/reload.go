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
	"strings"

	"github.com/wentaojin/dbms/utils/cluster/task"

	"github.com/fatih/color"
	"github.com/wentaojin/dbms/utils/cluster/operator"
	"github.com/wentaojin/dbms/utils/ctxt"
	"github.com/wentaojin/dbms/utils/executor"
	"github.com/wentaojin/dbms/utils/stringutil"

	"github.com/spf13/cobra"
	"github.com/wentaojin/dbms/component"
	"github.com/wentaojin/dbms/utils/cluster"
	"github.com/wentaojin/dbms/utils/cluster/manager"
)

type AppReload struct {
	*App
	SkipRestart bool
}

func (a *App) AppReload() component.Cmder {
	return &AppReload{App: a}
}

func (a *AppReload) Cmd() *cobra.Command {
	c := &cobra.Command{
		Use:              "reload <cluster-name>",
		Short:            "Reload a DBMS cluster's config and restart if needed",
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
	c.Flags().StringSliceVarP(&gOpt.Roles, "role", "R", nil, "Only reload specified roles")
	c.Flags().StringSliceVarP(&gOpt.Nodes, "node", "N", nil, "Specify the nodes (required)")
	c.Flags().BoolVar(&gOpt.Force, "force", false, "Force will ignore remote error while destroy the cluster")
	c.Flags().BoolVar(&a.SkipRestart, "skip-restart", false, "Only refresh configuration to remote and do not restart services")
	return c
}

func (a *AppReload) RunE(cmd *cobra.Command, args []string) error {
	if len(args) != 2 {
		return cmd.Help()
	}
	clusterName := args[0]

	return a.Reload(clusterName, gOpt)
}

func (a *AppReload) Reload(clusterName string, gOpt *operator.Options) error {
	mg := manager.New(gOpt.MetaDir, logger)
	meta := mg.NewMetadata()
	metadata, err := meta.ParseMetadata(mg.GetMetaFilePath(clusterName))
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

	if err = cluster.ValidateClusterNameOrError(clusterName); err != nil {
		return err
	}

	// check locked
	if err := mg.ScaleOutLockedErr(clusterName); err != nil {
		return err
	}

	var p *operator.SSHConnectionProps = &operator.SSHConnectionProps{}
	if gOpt.SSHType != executor.SSHTypeNone && len(gOpt.SSHProxyHost) != 0 {
		var err error
		if p, err = operator.ReadIdentityFileOrPassword(gOpt.SSHProxyIdentity, gOpt.SSHProxyUsePassword); err != nil {
			return err
		}
	}

	if !gOpt.SkipConfirm {
		if err := stringutil.PromptForConfirmOrAbortError(
			fmt.Sprintf("Will reload the cluster %s with restart policy is %s, nodes: %s, roles: %s.\nDo you want to continue? [y/N]:",
				color.HiYellowString(clusterName),
				color.HiRedString(fmt.Sprintf("%v", !a.SkipRestart)),
				color.HiRedString(strings.Join(gOpt.Nodes, ",")),
				color.HiRedString(strings.Join(gOpt.Roles, ",")),
			),
		); err != nil {
			return err
		}
	}

	// Build the scale out tasks
	var (
		refreshConfigTasks []*task.StepDisplay // tasks which are used to refresh config to remote host
	)

	globalOptions := topo.GlobalOptions

	// refresh config
	topo.IterInstance(func(inst cluster.Instance) {
		compName := inst.ComponentName()
		// Download and copy the latest component to remote if the cluster is imported from Ansible
		tb := task.NewBuilder(logger).InitConfig(
			clusterName,
			inst,
			globalOptions.User,
			mg.Path(clusterName, cluster.CacheDirName),
		).
			BuildAsStep(fmt.Sprintf("  - Generate config %s -> %s", compName, inst.InstanceName()))
		refreshConfigTasks = append(refreshConfigTasks, tb)
	})

	b := task.NewBuilder(mg.Logger).
		SSHKeySet(mg.Path(clusterName, cluster.SshDirName, "id_rsa"), mg.Path(clusterName, cluster.SshDirName, "id_rsa.pub")).
		ClusterSSH(
			topo,
			metadata.User,
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
		)

	if !a.SkipRestart {
		b.UpdateMetadata(
			clusterName,
			a.MetaDir,
			metadata,
			nil, /* deleteNodeIds */
		)
	}
	b.ParallelStep("+ Refresh instance configs", gOpt.Force, refreshConfigTasks...)

	if !a.SkipRestart {
		b.Func("Upgrade Cluster", func(ctx context.Context) error {
			return operator.Upgrade(ctx, topo, gOpt, nil)
		})
	}

	t := b.Build()

	ctx := ctxt.New(
		context.Background(),
		gOpt.Concurrency,
		mg.Logger,
	)
	if err := t.Execute(ctx); err != nil {
		return err
	}

	mg.Logger.Infof("Reloaded cluster `%s` successfully", clusterName)

	return nil

}
